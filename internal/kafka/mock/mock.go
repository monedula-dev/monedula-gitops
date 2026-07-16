// Package mock provides an in-memory kafka.AdminClient seeded from a YAML
// state fixture, so the entire v0.1 CLI can run without a live cluster.
// Reads are deterministically ordered to keep golden tests stable.
package mock

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"

	"sigs.k8s.io/yaml"

	"github.com/monedula-dev/monedula-gitops/internal/kafka"
)

var _ kafka.AdminClient = (*Client)(nil)

// Client is an in-memory implementation of kafka.AdminClient.
//
// In addition to the read methods it implements all six mutations. Every
// mutating call is recorded (see Calls) so the apply executor (v0.2 Task 3)
// can be unit-tested without a live cluster. Failures can be injected per call
// via FailOn; an injected failure is recorded but does NOT mutate state.
type Client struct {
	topics []kafka.TopicState
	acls   []kafka.ACLState
	// quotas stores quota entries keyed by a canonical entity string produced by
	// quotaKey. The map is lazily initialised on first write.
	quotas map[string]kafka.QuotaState
	// scramCreds stores SCRAM credential state keyed by scramKey(user,
	// mechanism). The map is lazily initialised on first write.
	scramCreds map[string]*scramEntry

	// calls records each mutating call in invocation order, e.g.
	// "CreateTopic payments.orders", "DeleteACLs 1".
	calls []string

	// failures maps a call signature ("<method> <target>") to the error that
	// should be returned for that call. Populated via FailOn.
	failures map[string]error
}

// scramEntry is the mock's internal record for one (user, mechanism) SCRAM
// credential. It deliberately has NO password/salt/hash field: mirroring the
// real API's observability, the mock cannot leak a secret it never stored.
// UpsertCount is the rotation-assertion primitive -- tests that need to
// confirm "a rotation happened" (T4/T5: e.g. Secret-driven password rotation)
// assert UpsertCount without ever touching a secret value.
type scramEntry struct {
	User        string
	Mechanism   string
	Iterations  int32
	UpsertCount int // incremented on every UpsertScramCredential call for this (user, mechanism)
}

// fileState mirrors the on-disk YAML fixture. JSON tags drive the lowercase
// field mapping via sigs.k8s.io/yaml (YAML -> JSON -> struct).
type fileState struct {
	Topics           []fileTopic   `json:"topics"`
	ACLs             []fileACL     `json:"acls"`
	Quotas           []fileQuota   `json:"quotas"`
	ScramCredentials []fileScram   `json:"scramCredentials"`
	Failures         []fileFailure `json:"failures"`
}

type fileTopic struct {
	Name              string            `json:"name"`
	Partitions        int               `json:"partitions"`
	ReplicationFactor int               `json:"replicationFactor"`
	Config            map[string]string `json:"config"`
}

type fileACL struct {
	Principal    string `json:"principal"`
	Host         string `json:"host"`
	ResourceType string `json:"resourceType"`
	ResourceName string `json:"resourceName"`
	PatternType  string `json:"patternType"`
	Operation    string `json:"operation"`
	Permission   string `json:"permission"`
}

// fileQuota is the YAML shape for a single quota entry in the state file.
// A missing name field maps to a nil *string (= per-type default entity).
//
//	quotas:
//	  - entity:
//	      - type: user
//	        name: svc-checkout      # omit name for the default entity
//	      - type: client-id
//	        name: batch
//	    limits:
//	      producer_byte_rate: 1024
type fileQuota struct {
	Entity []fileQuotaEntity  `json:"entity"`
	Limits map[string]float64 `json:"limits"`
}

type fileQuotaEntity struct {
	Type string  `json:"type"`
	Name *string `json:"name,omitempty"`
}

// fileScram is the YAML shape for a single seeded SCRAM credential. There is
// deliberately no password field: the mock never stores passwords (see
// scramEntry), so the fixture cannot seed one either. Seeding a credential
// this way only establishes the observable (user, mechanism, iterations)
// identity, as if a rotation had already happened.
//
//	scramCredentials:
//	  - user: svc-checkout
//	    mechanism: SCRAM-SHA-512
//	    iterations: 4096
type fileScram struct {
	User       string `json:"user"`
	Mechanism  string `json:"mechanism"`
	Iterations int32  `json:"iterations"`
}

// fileFailure is the YAML shape for an injected failure, wired to FailOn at
// load time. Target is empty for the target-less READ methods (ListTopics,
// ListACLs, ListQuotas); for mutations it uses the same value Calls records
// (e.g. the topic name for CreateTopic). This lets file-seeded CLI tests
// simulate a credential lacking a specific describe permission — a scoped-read
// test proves a read was skipped by injecting a failure that never surfaces.
//
//	failures:
//	  - method: ListQuotas
//	    error: "describing client quotas (user): CLUSTER_AUTHORIZATION_FAILED"
type fileFailure struct {
	Method string `json:"method"`
	Target string `json:"target,omitempty"`
	Error  string `json:"error"`
}

// New constructs a Client from in-memory state. Used by later tasks/tests for
// programmatic construction.
func New(topics []kafka.TopicState, acls []kafka.ACLState) *Client {
	return &Client{topics: topics, acls: acls}
}

// NewWithQuotas constructs a Client with pre-seeded topics, ACLs, and quotas.
// Used by tests that need a programmatic quota starting state.
func NewWithQuotas(topics []kafka.TopicState, acls []kafka.ACLState, quotas []kafka.QuotaState) *Client {
	c := &Client{topics: topics, acls: acls}
	for _, q := range quotas {
		c.upsertQuota(q.Entity, q.Limits)
	}
	return c
}

// NewWithScramCredentials constructs a Client with pre-seeded topics, ACLs,
// and SCRAM credentials. Used by tests (e.g. T4/T5 KafkaUser reconciliation)
// that need a programmatic credential starting state. Seeding sets the
// observable identity (user, mechanism, iterations) only -- the mock never
// stores passwords, so there is nothing password-shaped to seed.
func NewWithScramCredentials(topics []kafka.TopicState, acls []kafka.ACLState, creds []kafka.ScramCredential) *Client {
	c := &Client{topics: topics, acls: acls}
	for _, cr := range creds {
		c.seedScramCredential(cr.User, cr.Mechanism, cr.Iterations)
	}
	return c
}

// FromFile loads cluster state from a YAML fixture at path.
func FromFile(path string) (*Client, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read state file %q: %w", path, err)
	}
	var fs fileState
	if err := yaml.Unmarshal(data, &fs); err != nil {
		return nil, fmt.Errorf("parse state file %q: %w", path, err)
	}

	topics := make([]kafka.TopicState, 0, len(fs.Topics))
	for _, t := range fs.Topics {
		topics = append(topics, kafka.TopicState{
			Name:              t.Name,
			Partitions:        t.Partitions,
			ReplicationFactor: t.ReplicationFactor,
			Config:            t.Config,
		})
	}

	acls := make([]kafka.ACLState, 0, len(fs.ACLs))
	for _, a := range fs.ACLs {
		acls = append(acls, kafka.ACLState{
			Principal:    a.Principal,
			Host:         a.Host,
			ResourceType: a.ResourceType,
			ResourceName: a.ResourceName,
			PatternType:  a.PatternType,
			Operation:    a.Operation,
			Permission:   a.Permission,
		})
	}

	c := New(topics, acls)
	for _, fq := range fs.Quotas {
		entity := make([]kafka.QuotaEntityComponent, 0, len(fq.Entity))
		for _, fe := range fq.Entity {
			entity = append(entity, kafka.QuotaEntityComponent{
				Type: fe.Type,
				Name: fe.Name,
			})
		}
		limits := make(map[string]float64, len(fq.Limits))
		for k, v := range fq.Limits {
			limits[k] = v
		}
		c.upsertQuota(entity, limits)
	}
	for _, fc := range fs.ScramCredentials {
		c.seedScramCredential(fc.User, fc.Mechanism, fc.Iterations)
	}
	for _, ff := range fs.Failures {
		if ff.Method == "" || ff.Error == "" {
			return nil, fmt.Errorf("state file %q: a failures entry needs both method and error", path)
		}
		c.FailOn(ff.Method, ff.Target, errors.New(ff.Error))
	}
	return c, nil
}

// GetTopic returns a copy of the topic named name, or (nil, nil) if absent.
func (c *Client) GetTopic(_ context.Context, name string) (*kafka.TopicState, error) {
	for _, t := range c.topics {
		if t.Name == name {
			cp := t
			cp.Config = copyConfig(t.Config)
			return &cp, nil
		}
	}
	return nil, nil
}

// ListTopics returns all topics sorted by Name.
func (c *Client) ListTopics(_ context.Context) ([]kafka.TopicState, error) {
	if err := c.failures[failKey("ListTopics", "")]; err != nil {
		return nil, err
	}
	out := make([]kafka.TopicState, len(c.topics))
	for i, t := range c.topics {
		out[i] = t
		out[i].Config = copyConfig(t.Config)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Name < out[j].Name
	})
	return out, nil
}

// ListACLs returns all ACLs sorted by a stable composite key.
func (c *Client) ListACLs(_ context.Context) ([]kafka.ACLState, error) {
	if err := c.failures[failKey("ListACLs", "")]; err != nil {
		return nil, err
	}
	out := make([]kafka.ACLState, len(c.acls))
	copy(out, c.acls)
	sort.Slice(out, func(i, j int) bool {
		return aclKey(out[i]) < aclKey(out[j])
	})
	return out, nil
}

// DescribeTopicConfigs returns the topic's config entries sorted by Name. The
// mock treats all seeded config as explicitly set, so every entry has
// Default=false. It errors if the topic is absent.
func (c *Client) DescribeTopicConfigs(_ context.Context, topic string) ([]kafka.ConfigEntry, error) {
	i := c.indexOfTopic(topic)
	if i < 0 {
		return nil, fmt.Errorf("topic %q not found", topic)
	}
	cfg := c.topics[i].Config
	out := make([]kafka.ConfigEntry, 0, len(cfg))
	for k, v := range cfg {
		out = append(out, kafka.ConfigEntry{Name: k, Value: v, Default: false})
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Name < out[j].Name
	})
	return out, nil
}

func aclKey(a kafka.ACLState) string {
	return a.Principal + "\x00" + a.ResourceType + "\x00" + a.ResourceName + "\x00" +
		a.PatternType + "\x00" + a.Operation + "\x00" + a.Host + "\x00" + a.Permission
}

// Calls returns the recorded mutating calls in invocation order. Each entry is
// a stable string of the form "<Method> <target>", where target is the topic
// name (CreateTopic/UpdateTopicConfig/DeleteTopic), "<topic> <count>"
// (CreatePartitions), or the number of ACLs (CreateACLs/DeleteACLs). A call is
// recorded even if it fails (including via FailOn).
func (c *Client) Calls() []string {
	out := make([]string, len(c.calls))
	copy(out, c.calls)
	return out
}

// FailOn configures the mock to return err for the next and subsequent calls to
// method against target. target uses the same value recorded by Calls (e.g. the
// topic name for CreateTopic, or the ACL count as a string for CreateACLs). The
// failing call is still recorded, but state is NOT mutated.
//
// The target-less read methods (ListTopics, ListACLs, ListQuotas) support
// failure injection too, with target "" — they are never recorded in Calls
// (which stays mutations-only), they just return the configured error.
func (c *Client) FailOn(method, target string, err error) {
	if c.failures == nil {
		c.failures = make(map[string]error)
	}
	c.failures[failKey(method, target)] = err
}

// failKey builds the failures map key: "<method> <target>" (matching the
// signature record logs for mutations), or just the method for target-less
// reads.
func failKey(method, target string) string {
	if target == "" {
		return method
	}
	return method + " " + target
}

// record appends sig to the call log and returns any configured failure for it.
func (c *Client) record(sig string) error {
	c.calls = append(c.calls, sig)
	return c.failures[sig]
}

// CreateTopic adds a new topic. It errors if a topic with the same name already
// exists.
func (c *Client) CreateTopic(_ context.Context, t kafka.TopicSpec) error {
	if err := c.record("CreateTopic " + t.Name); err != nil {
		return err
	}
	if c.indexOfTopic(t.Name) >= 0 {
		return fmt.Errorf("topic already exists: %s", t.Name)
	}
	c.topics = append(c.topics, kafka.TopicState{
		Name:              t.Name,
		Partitions:        t.Partitions,
		ReplicationFactor: t.ReplicationFactor,
		Config:            copyConfig(t.Config),
	})
	return nil
}

// UpdateTopicConfig merges set into the topic's Config, overwriting existing
// keys and preserving the rest. It errors if the topic is absent.
func (c *Client) UpdateTopicConfig(_ context.Context, topic string, set map[string]string) error {
	if err := c.record("UpdateTopicConfig " + topic); err != nil {
		return err
	}
	i := c.indexOfTopic(topic)
	if i < 0 {
		return fmt.Errorf("topic not found: %s", topic)
	}
	if c.topics[i].Config == nil {
		c.topics[i].Config = make(map[string]string, len(set))
	}
	for k, v := range set {
		c.topics[i].Config[k] = v
	}
	return nil
}

// CreatePartitions raises the topic's partition count to count. It errors if
// the topic is absent or if count is below the current count (the diff engine
// never requests a decrease, but we guard against it defensively).
func (c *Client) CreatePartitions(_ context.Context, topic string, count int) error {
	if err := c.record(fmt.Sprintf("CreatePartitions %s %d", topic, count)); err != nil {
		return err
	}
	i := c.indexOfTopic(topic)
	if i < 0 {
		return fmt.Errorf("topic not found: %s", topic)
	}
	if count < c.topics[i].Partitions {
		return fmt.Errorf("cannot decrease partitions for %s: %d < %d", topic, count, c.topics[i].Partitions)
	}
	c.topics[i].Partitions = count
	return nil
}

// DeleteTopic removes the topic. Deleting an absent topic is a no-op (idempotent).
func (c *Client) DeleteTopic(_ context.Context, topic string) error {
	if err := c.record("DeleteTopic " + topic); err != nil {
		return err
	}
	i := c.indexOfTopic(topic)
	if i < 0 {
		return nil
	}
	c.topics = append(c.topics[:i], c.topics[i+1:]...)
	return nil
}

// CreateACLs appends the given ACL bindings. Reads stay deterministically
// sorted regardless of insertion order.
func (c *Client) CreateACLs(_ context.Context, acls []kafka.ACLState) error {
	if err := c.record(fmt.Sprintf("CreateACLs %d", len(acls))); err != nil {
		return err
	}
	c.acls = append(c.acls, acls...)
	return nil
}

// DeleteACLs removes every stored ACL that fully matches (all seven fields) any
// of the supplied tuples.
func (c *Client) DeleteACLs(_ context.Context, acls []kafka.ACLState) error {
	if err := c.record(fmt.Sprintf("DeleteACLs %d", len(acls))); err != nil {
		return err
	}
	if len(acls) == 0 {
		return nil
	}
	toDelete := make(map[kafka.ACLState]struct{}, len(acls))
	for _, a := range acls {
		toDelete[a] = struct{}{}
	}
	kept := c.acls[:0]
	for _, a := range c.acls {
		if _, drop := toDelete[a]; drop {
			continue
		}
		kept = append(kept, a)
	}
	c.acls = kept
	return nil
}

// indexOfTopic returns the index of the named topic, or -1 if absent.
func (c *Client) indexOfTopic(name string) int {
	for i := range c.topics {
		if c.topics[i].Name == name {
			return i
		}
	}
	return -1
}

func copyConfig(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}
	cp := make(map[string]string, len(m))
	for k, v := range m {
		cp[k] = v
	}
	return cp
}

// --- Quotas ---

// quotaKey returns a canonical, sortable string key for an entity, matching the
// scheme used by internal/quota.Entity.Key(). Components are sorted by Type
// then rendered as "type=name" (or "type=<default>" for nil Name), joined by
// "|". Re-implemented here to avoid an import cycle between kafka/mock and
// internal/quota.
func quotaKey(entity []kafka.QuotaEntityComponent) string {
	comps := make([]kafka.QuotaEntityComponent, len(entity))
	copy(comps, entity)
	sort.Slice(comps, func(i, j int) bool {
		return comps[i].Type < comps[j].Type
	})
	parts := make([]string, 0, len(comps))
	for _, c := range comps {
		if c.Name == nil {
			parts = append(parts, c.Type+"=<default>")
		} else {
			parts = append(parts, c.Type+"="+*c.Name)
		}
	}
	return strings.Join(parts, "|")
}

// upsertQuota merges limits into the stored quota for entity (internal helper).
// The entity slice is stored in canonical (Type-sorted) order so that
// ListQuotas returns components in a stable, deterministic sequence regardless
// of the caller's insertion order.
func (c *Client) upsertQuota(entity []kafka.QuotaEntityComponent, limits map[string]float64) {
	if c.quotas == nil {
		c.quotas = make(map[string]kafka.QuotaState)
	}
	key := quotaKey(entity)
	existing, ok := c.quotas[key]
	if !ok {
		// Deep-copy and sort the entity slice so the caller can't mutate our
		// store and so that components are in canonical Type order.
		ent := make([]kafka.QuotaEntityComponent, len(entity))
		copy(ent, entity)
		sort.Slice(ent, func(i, j int) bool {
			return ent[i].Type < ent[j].Type
		})
		existing = kafka.QuotaState{
			Entity: ent,
			Limits: make(map[string]float64, len(limits)),
		}
	}
	for k, v := range limits {
		existing.Limits[k] = v
	}
	c.quotas[key] = existing
}

// ListQuotas returns all stored quotas sorted deterministically by entity key.
func (c *Client) ListQuotas(_ context.Context) ([]kafka.QuotaState, error) {
	if err := c.failures[failKey("ListQuotas", "")]; err != nil {
		return nil, err
	}
	if len(c.quotas) == 0 {
		return nil, nil
	}
	keys := make([]string, 0, len(c.quotas))
	for k := range c.quotas {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]kafka.QuotaState, 0, len(keys))
	for _, k := range keys {
		qs := c.quotas[k]
		// Deep-copy limits so callers cannot mutate internal state.
		lim := make(map[string]float64, len(qs.Limits))
		for lk, lv := range qs.Limits {
			lim[lk] = lv
		}
		ent := make([]kafka.QuotaEntityComponent, len(qs.Entity))
		copy(ent, qs.Entity)
		out = append(out, kafka.QuotaState{Entity: ent, Limits: lim})
	}
	return out, nil
}

// SetQuota merges the given limit keys into the entity's quota (upsert).
func (c *Client) SetQuota(_ context.Context, entity []kafka.QuotaEntityComponent, limits map[string]float64) error {
	if err := c.record("SetQuota " + quotaKey(entity)); err != nil {
		return err
	}
	c.upsertQuota(entity, limits)
	return nil
}

// DeleteQuota removes the given limit keys from the entity's quota. If no
// keys remain after deletion, the entity entry is removed entirely.
func (c *Client) DeleteQuota(_ context.Context, entity []kafka.QuotaEntityComponent, keys []string) error {
	key := quotaKey(entity)
	if err := c.record("DeleteQuota " + key); err != nil {
		return err
	}
	if c.quotas == nil {
		return nil
	}
	qs, ok := c.quotas[key]
	if !ok {
		return nil
	}
	for _, k := range keys {
		delete(qs.Limits, k)
	}
	if len(qs.Limits) == 0 {
		delete(c.quotas, key)
	} else {
		c.quotas[key] = qs
	}
	return nil
}

// --- SCRAM credentials ---

// scramKey returns a canonical, sortable map key for a (user, mechanism)
// pair. Kept simple (unlike quotaKey) because a SCRAM credential's identity
// is always exactly these two flat fields, never a variable-component entity.
func scramKey(user, mechanism string) string {
	return user + "\x00" + mechanism
}

// seedScramCredential installs a credential's observable identity directly
// (no upsert-count bump, mirroring how FromFile/NewWithScramCredentials seed
// starting state rather than performing a write). Used by FromFile and
// NewWithScramCredentials only.
func (c *Client) seedScramCredential(user, mechanism string, iterations int32) {
	if c.scramCreds == nil {
		c.scramCreds = make(map[string]*scramEntry)
	}
	c.scramCreds[scramKey(user, mechanism)] = &scramEntry{
		User:       user,
		Mechanism:  mechanism,
		Iterations: iterations,
	}
}

// ListScramCredentials returns the observable identity of every stored
// credential matching usernames (all credentialed users when usernames is
// empty), sorted by user then mechanism. A requested-but-absent user is
// simply missing from the result, matching the franz adapter's semantics
// (never an error).
func (c *Client) ListScramCredentials(_ context.Context, usernames ...string) ([]kafka.ScramCredential, error) {
	want := make(map[string]struct{}, len(usernames))
	for _, u := range usernames {
		want[u] = struct{}{}
	}
	keys := make([]string, 0, len(c.scramCreds))
	for k := range c.scramCreds {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]kafka.ScramCredential, 0, len(keys))
	for _, k := range keys {
		e := c.scramCreds[k]
		if len(want) > 0 {
			if _, ok := want[e.User]; !ok {
				continue
			}
		}
		out = append(out, kafka.ScramCredential{
			User:       e.User,
			Mechanism:  e.Mechanism,
			Iterations: e.Iterations,
		})
	}
	return out, nil
}

// UpsertScramCredential creates or updates the (User, Mechanism) credential.
// The password is NEVER stored: only the observable identity (mechanism,
// iterations) is recorded, plus UpsertCount, which is incremented every call
// so rotation tests can assert "an upsert happened for this (user,
// mechanism)" without the mock ever holding a secret.
func (c *Client) UpsertScramCredential(_ context.Context, u kafka.ScramUpsert) error {
	key := scramKey(u.User, u.Mechanism)
	if err := c.record("UpsertScramCredential " + key); err != nil {
		return err
	}
	if c.scramCreds == nil {
		c.scramCreds = make(map[string]*scramEntry)
	}
	iterations := u.Iterations
	if iterations == 0 {
		iterations = defaultScramIterations
	}
	e, ok := c.scramCreds[key]
	if !ok {
		e = &scramEntry{User: u.User, Mechanism: u.Mechanism}
		c.scramCreds[key] = e
	}
	e.Iterations = iterations
	e.UpsertCount++
	return nil
}

// DeleteScramCredential removes only the (username, mechanism) credential.
// Deleting an absent credential is a no-op (idempotent, mirroring DeleteTopic
// and DeleteQuota).
func (c *Client) DeleteScramCredential(_ context.Context, username, mechanism string) error {
	key := scramKey(username, mechanism)
	if err := c.record("DeleteScramCredential " + key); err != nil {
		return err
	}
	delete(c.scramCreds, key)
	return nil
}

// ScramUpsertCount returns how many times UpsertScramCredential has been
// called for (user, mechanism), or 0 if the credential has never been
// upserted (including if it was never seeded and does not exist). This is the
// rotation-assertion primitive: tests confirm a password rotation occurred by
// asserting the count increased, without the mock ever exposing (or storing)
// the password itself.
func (c *Client) ScramUpsertCount(user, mechanism string) int {
	e, ok := c.scramCreds[scramKey(user, mechanism)]
	if !ok {
		return 0
	}
	return e.UpsertCount
}

// defaultScramIterations mirrors franz.defaultScramIterations: kadm/Kafka
// require a nonzero iteration count (bounded [4096, 16384] broker-side), so
// the mock applies the same documented default (4096, Kafka's own default and
// the lower bound) when a ScramUpsert leaves Iterations unset, keeping mock
// and real-adapter behavior aligned for callers that inspect Iterations after
// an upsert with Iterations: 0.
const defaultScramIterations = 4096
