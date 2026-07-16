package franz

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	"github.com/monedula-dev/monedula-gitops/internal/secrets"
)

// Client is the real franz-go backed implementation of kafka.AdminClient. It is
// a thin translation layer over kadm: it maps kafka.* request/response types to
// and from kadm/kmsg types and contains no business logic.
type Client struct {
	kgo *kgo.Client
	adm *kadm.Client
}

var _ kafka.AdminClient = (*Client)(nil)

// New builds a franz-go client from a KafkaCluster spec and wraps it in a kadm
// admin client. The Resolver resolves secret references (credentials, TLS CA).
// extra kgo options (e.g. a debug logger) are appended after the spec-derived
// ones, so they can extend but not silently override connection settings.
func New(c *v1alpha1.KafkaCluster, r secrets.Resolver, extra ...kgo.Opt) (*Client, error) {
	cc, err := buildConnConfig(c, r)
	if err != nil {
		return nil, err
	}
	kcl, err := kgo.NewClient(append(cc.opts(), extra...)...)
	if err != nil {
		return nil, fmt.Errorf("creating kafka client: %w", err)
	}
	return &Client{kgo: kcl, adm: kadm.NewClient(kcl)}, nil
}

// Close releases the underlying connections.
func (c *Client) Close() {
	if c.kgo != nil {
		c.kgo.Close()
	}
}

// --- Topics (read) ---

// GetTopic returns the topic's observed state, or nil if the topic does not
// exist.
func (c *Client) GetTopic(ctx context.Context, name string) (*kafka.TopicState, error) {
	td, err := c.adm.ListTopics(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("listing topic %q: %w", name, err)
	}
	if !td.Has(name) {
		return nil, nil
	}
	detail := td[name]
	if detail.Err != nil {
		// kadm reports unknown-topic as a per-topic Err rather than omitting it.
		if isUnknownTopic(detail.Err) {
			return nil, nil
		}
		return nil, fmt.Errorf("loading topic %q: %w", name, detail.Err)
	}

	cfg, err := c.topicConfig(ctx, name)
	if err != nil {
		return nil, err
	}
	return &kafka.TopicState{
		Name:              name,
		Partitions:        len(detail.Partitions),
		ReplicationFactor: detail.Partitions.NumReplicas(),
		Config:            cfg,
	}, nil
}

// ListTopics returns the observed state of every (non-internal) topic.
//
// Topic configs are fetched in a single batched DescribeTopicConfigs call
// covering every topic (kadm.Client.DescribeTopicConfigs is variadic and
// shards/sizes the underlying request itself), rather than one round-trip per
// topic. On a real cluster with thousands of topics, per-topic round-trips
// dominate diff latency; batching turns that into a single request. Per-topic
// error semantics and output ordering (by detail.Topic, via td.Sorted()) are
// unchanged from the pre-batching, per-topic implementation.
func (c *Client) ListTopics(ctx context.Context) ([]kafka.TopicState, error) {
	td, err := c.adm.ListTopics(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing topics: %w", err)
	}
	sorted := td.Sorted()

	names := make([]string, 0, len(sorted))
	for _, detail := range sorted {
		if detail.Err != nil {
			return nil, fmt.Errorf("loading topic %q: %w", detail.Topic, detail.Err)
		}
		names = append(names, detail.Topic)
	}

	cfgs, err := c.batchTopicConfigs(ctx, names)
	if err != nil {
		return nil, err
	}

	out := make([]kafka.TopicState, 0, len(sorted))
	for _, detail := range sorted {
		cfg, ok := cfgs[detail.Topic]
		if !ok {
			// Defensive: DescribeTopicConfigs did not return an entry for this
			// topic at all (distinct from a per-resource Err, which is handled in
			// batchTopicConfigs). Should not happen given ListTopics just
			// confirmed the topic exists, but fail loudly rather than silently
			// dropping the topic's config.
			return nil, fmt.Errorf("describing config for topic %q: no result returned", detail.Topic)
		}
		out = append(out, kafka.TopicState{
			Name:              detail.Topic,
			Partitions:        len(detail.Partitions),
			ReplicationFactor: detail.Partitions.NumReplicas(),
			Config:            cfg,
		})
	}
	return out, nil
}

// batchTopicConfigs fetches dynamic config key/values for every topic in
// names via one DescribeTopicConfigs call, mapping results back by topic
// name. Each topic's per-resource error (kadm.ResourceConfig.Err) is
// surfaced individually, matching the per-topic error semantics of the
// former one-request-per-topic implementation. An empty names list issues no
// request.
func (c *Client) batchTopicConfigs(ctx context.Context, names []string) (map[string]map[string]string, error) {
	if len(names) == 0 {
		return map[string]map[string]string{}, nil
	}
	rcs, err := c.adm.DescribeTopicConfigs(ctx, names...)
	if err != nil {
		return nil, fmt.Errorf("describing topic configs: %w", err)
	}
	out := make(map[string]map[string]string, len(names))
	for _, name := range names {
		rc, err := rcs.On(name, nil)
		if err != nil {
			return nil, fmt.Errorf("describing config for topic %q: %w", name, err)
		}
		if rc.Err != nil {
			return nil, fmt.Errorf("describing config for topic %q: %w", name, rc.Err)
		}
		cfg := make(map[string]string, len(rc.Configs))
		for _, cf := range rc.Configs {
			cfg[cf.Key] = cf.MaybeValue()
		}
		out[name] = cfg
	}
	return out, nil
}

// describeTopicConfigs fetches the per-resource config for a single topic,
// returning the resolved kadm configs. It surfaces any per-resource error.
// Used by GetTopic and DescribeTopicConfigs, which operate on one topic at a
// time and so gain nothing from batching.
func (c *Client) describeTopicConfigs(ctx context.Context, topic string) ([]kadm.Config, error) {
	rcs, err := c.adm.DescribeTopicConfigs(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("describing config for topic %q: %w", topic, err)
	}
	rc, err := rcs.On(topic, nil)
	if err != nil {
		return nil, fmt.Errorf("describing config for topic %q: %w", topic, err)
	}
	if rc.Err != nil {
		return nil, fmt.Errorf("describing config for topic %q: %w", topic, rc.Err)
	}
	return rc.Configs, nil
}

// topicConfig fetches the dynamic config key/values for a single topic.
func (c *Client) topicConfig(ctx context.Context, topic string) (map[string]string, error) {
	configs, err := c.describeTopicConfigs(ctx, topic)
	if err != nil {
		return nil, err
	}
	cfg := make(map[string]string, len(configs))
	for _, cf := range configs {
		cfg[cf.Key] = cf.MaybeValue()
	}
	return cfg, nil
}

// DescribeTopicConfigs returns every config entry for the topic, annotated with
// whether the value is an inherited/broker default. Default is true unless the
// config source is the dynamic per-topic source (i.e. explicitly set on the
// topic). Entries are sorted by Name.
func (c *Client) DescribeTopicConfigs(ctx context.Context, topic string) ([]kafka.ConfigEntry, error) {
	configs, err := c.describeTopicConfigs(ctx, topic)
	if err != nil {
		return nil, err
	}
	out := make([]kafka.ConfigEntry, 0, len(configs))
	for _, cf := range configs {
		out = append(out, kafka.ConfigEntry{
			Name:    cf.Key,
			Value:   cf.MaybeValue(),
			Default: cf.Source != kmsg.ConfigSourceDynamicTopicConfig,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Name < out[j].Name
	})
	return out, nil
}

// --- Topics (mutate) ---

// CreateTopic creates a topic with the given partitions, replication factor and
// config.
func (c *Client) CreateTopic(ctx context.Context, t kafka.TopicSpec) error {
	configs := make(map[string]*string, len(t.Config))
	for k, v := range t.Config {
		configs[k] = kadm.StringPtr(v)
	}
	// An unspecified replication factor (or partition count) is sent as -1 so the
	// broker applies its own default (default.replication.factor / num.partitions).
	// We never impose a tool-side default: replication factor is managed only when
	// the manifest -- or the cluster's spec.defaults -- sets it explicitly.
	partitions := int32(t.Partitions)
	if partitions <= 0 {
		partitions = -1
	}
	replication := int16(t.ReplicationFactor)
	if replication <= 0 {
		replication = -1
	}
	resp, err := c.adm.CreateTopics(ctx, partitions, replication, configs, t.Name)
	if err != nil {
		return fmt.Errorf("creating topic %q: %w", t.Name, err)
	}
	if err := resp.Error(); err != nil {
		return fmt.Errorf("creating topic %q: %w", t.Name, err)
	}
	return nil
}

// UpdateTopicConfig incrementally sets the given config keys on a topic.
func (c *Client) UpdateTopicConfig(ctx context.Context, topic string, set map[string]string) error {
	alters := make([]kadm.AlterConfig, 0, len(set))
	for k, v := range set {
		alters = append(alters, kadm.AlterConfig{Op: kadm.SetConfig, Name: k, Value: kadm.StringPtr(v)})
	}
	resp, err := c.adm.AlterTopicConfigs(ctx, alters, topic)
	if err != nil {
		return fmt.Errorf("altering config for topic %q: %w", topic, err)
	}
	if _, err := resp.On(topic, func(r *kadm.AlterConfigsResponse) error { return r.Err }); err != nil {
		return fmt.Errorf("altering config for topic %q: %w", topic, err)
	}
	return nil
}

// CreatePartitions sets the topic's total partition count to count. We use
// kadm.UpdatePartitions, whose semantic is "set the final partition count"
// (matching kafka.AdminClient.CreatePartitions(topic, count)), rather than
// CreatePartitions, which adds a delta.
func (c *Client) CreatePartitions(ctx context.Context, topic string, count int) error {
	resp, err := c.adm.UpdatePartitions(ctx, count, topic)
	if err != nil {
		return fmt.Errorf("updating partitions for topic %q: %w", topic, err)
	}
	if err := resp.Error(); err != nil {
		return fmt.Errorf("updating partitions for topic %q: %w", topic, err)
	}
	return nil
}

// DeleteTopic deletes a topic. Deleting an absent topic is a no-op (idempotent,
// mirroring the mock): kadm surfaces UNKNOWN_TOPIC_OR_PARTITION as the
// per-topic response error, which deleteTopicErr filters to success — the
// desired end state ("topic gone") already holds. Without this, a
// deletionPolicy=Delete resource whose topic was removed out-of-band could
// never finalize (review I7).
func (c *Client) DeleteTopic(ctx context.Context, topic string) error {
	resp, err := c.adm.DeleteTopics(ctx, topic)
	if err != nil {
		return fmt.Errorf("deleting topic %q: %w", topic, err)
	}
	if err := deleteTopicErr(resp.Error()); err != nil {
		return fmt.Errorf("deleting topic %q: %w", topic, err)
	}
	return nil
}

// deleteTopicErr filters kadm's per-topic delete-response error:
// UNKNOWN_TOPIC_OR_PARTITION means the topic is already absent, which is the
// desired end state of a delete, so it is treated as success. It uses the same
// errors.Is(kerr.UnknownTopicOrPartition) detection as GetTopic's
// isUnknownTopic. Every other error propagates.
func deleteTopicErr(err error) error {
	if err == nil || isUnknownTopic(err) {
		return nil
	}
	return err
}

// --- ACLs ---

// CreateACLs creates the given ACL bindings.
//
// This issues one kadm.CreateACLs request per ACL rather than batching all
// acls into a single kadm.ACLBuilder call. kadm.ACLBuilder is a cartesian-
// product builder: every principal x host x resource x operation combination
// set on the builder is expanded into a creation request (see kadm's
// ACLBuilder doc comment). Folding N unrelated kafka.ACLState values (each
// with its own resource, principal, host, and operation) into one builder
// would create the cross product of all of them -- e.g. ACL A's principal
// paired with ACL B's topic -- which is not what was requested and is a
// correctness hazard, not just an inefficiency. Per-ACL requests keep the
// request shape (and CreateACLsResults' 1:1 index alignment with what we
// asked for) unambiguous. Correctness beats throughput here.
func (c *Client) CreateACLs(ctx context.Context, acls []kafka.ACLState) error {
	for _, a := range acls {
		b, err := aclBuilder(a, builderModeCreate)
		if err != nil {
			return err
		}
		results, err := c.adm.CreateACLs(ctx, b)
		if err != nil {
			return fmt.Errorf("creating ACL %+v: %w", a, err)
		}
		for _, r := range results {
			if r.Err != nil {
				return fmt.Errorf("creating ACL %+v: %w", a, r.Err)
			}
		}
	}
	return nil
}

// DeleteACLs deletes the given ACL bindings. Each binding is translated to a
// matching filter.
//
// As with CreateACLs, this deliberately issues one kadm.DeleteACLs request per
// ACL rather than merging all filters into a single kadm.ACLBuilder call: the
// builder's cartesian-product expansion would turn N independent per-ACL
// filters into cross-product filters that could match (and delete) ACLs never
// named in the input. Batched filter deletes are also inherently harder to
// attribute back to individual ops than batched creates, since a single
// filter can match a variable number of broker-side ACLs (DeleteACLsResult.
// Deleted is a slice, not a 1:1 pairing). Keeping one filter per requested
// delete keeps the semantics exact.
func (c *Client) DeleteACLs(ctx context.Context, acls []kafka.ACLState) error {
	for _, a := range acls {
		b, err := aclBuilder(a, builderModeFilter)
		if err != nil {
			return err
		}
		results, err := c.adm.DeleteACLs(ctx, b)
		if err != nil {
			return fmt.Errorf("deleting ACL %+v: %w", a, err)
		}
		for _, r := range results {
			if r.Err != nil {
				return fmt.Errorf("deleting ACL %+v: %w", a, r.Err)
			}
		}
	}
	return nil
}

// ListACLs returns all ACL bindings on the cluster.
func (c *Client) ListACLs(ctx context.Context) ([]kafka.ACLState, error) {
	// An empty "any" filter matches every ACL.
	b := kadm.NewACLs().
		AnyResource().
		ResourcePatternType(kmsg.ACLResourcePatternTypeAny).
		Operations(kmsg.ACLOperationAny).
		Allow().AllowHosts().
		Deny().DenyHosts()

	results, err := c.adm.DescribeACLs(ctx, b)
	if err != nil {
		return nil, fmt.Errorf("describing ACLs: %w", err)
	}
	var out []kafka.ACLState
	for _, r := range results {
		if r.Err != nil {
			return nil, fmt.Errorf("describing ACLs: %w", r.Err)
		}
		for _, d := range r.Described {
			out = append(out, kafka.ACLState{
				Principal:    d.Principal,
				Host:         d.Host,
				ResourceType: canonicalResourceType(d.Type),
				ResourceName: d.Name,
				PatternType:  canonicalPatternType(d.Pattern),
				Operation:    canonicalOperation(d.Operation),
				Permission:   canonicalPermission(d.Permission),
			})
		}
	}
	return out, nil
}

// The canonical* converters map kmsg ACL enums to the tool's canonical string
// forms (the ones manifests use and access.ACL.FullKey compares): lowercase
// resource/pattern types (topic, transactionalId, literal, ...) and CamelCase
// operations/permissions (Write, DescribeConfigs, Allow, ...).
//
// kmsg's .String() returns Kafka's UPPER_SNAKE forms (TOPIC, DESCRIBE_CONFIGS),
// which would never match the desired set's keys -- the diff would re-create
// every ACL on every run. Unknown enum values fall back to .String() so they
// are at least visible (and, being out of canonical form, never silently match).

func canonicalResourceType(t kmsg.ACLResourceType) string {
	switch t {
	case kmsg.ACLResourceTypeTopic:
		return "topic"
	case kmsg.ACLResourceTypeGroup:
		return "group"
	case kmsg.ACLResourceTypeCluster:
		return "cluster"
	case kmsg.ACLResourceTypeTransactionalId:
		return "transactionalId"
	case kmsg.ACLResourceTypeDelegationToken:
		return "delegationToken"
	default:
		return t.String()
	}
}

func canonicalPatternType(p kmsg.ACLResourcePatternType) string {
	switch p {
	case kmsg.ACLResourcePatternTypeLiteral:
		return "literal"
	case kmsg.ACLResourcePatternTypePrefixed:
		return "prefixed"
	default:
		return p.String()
	}
}

func canonicalOperation(o kmsg.ACLOperation) string {
	switch o {
	case kmsg.ACLOperationRead:
		return "Read"
	case kmsg.ACLOperationWrite:
		return "Write"
	case kmsg.ACLOperationCreate:
		return "Create"
	case kmsg.ACLOperationDelete:
		return "Delete"
	case kmsg.ACLOperationAlter:
		return "Alter"
	case kmsg.ACLOperationDescribe:
		return "Describe"
	case kmsg.ACLOperationClusterAction:
		return "ClusterAction"
	case kmsg.ACLOperationDescribeConfigs:
		return "DescribeConfigs"
	case kmsg.ACLOperationAlterConfigs:
		return "AlterConfigs"
	case kmsg.ACLOperationIdempotentWrite:
		return "IdempotentWrite"
	case kmsg.ACLOperationAll:
		return "All"
	default:
		return o.String()
	}
}

func canonicalPermission(p kmsg.ACLPermissionType) string {
	switch p {
	case kmsg.ACLPermissionTypeAllow:
		return "Allow"
	case kmsg.ACLPermissionTypeDeny:
		return "Deny"
	default:
		return p.String()
	}
}

// builderMode distinguishes building an ACL for creation (requires Allow/Deny
// principals, host defaults to wildcard) from building a delete/describe filter
// (host must be explicit).
type builderMode int

const (
	builderModeCreate builderMode = iota
	builderModeFilter
)

// aclBuilder translates one kafka.ACLState into a kadm.ACLBuilder. The kafka.*
// strings are mapped to kmsg enums via kmsg.Parse* (case/format-insensitive:
// dots, dashes, underscores are stripped and the value lowercased), so inputs
// like "transactionalId"/"transactional_id" and "Read"/"READ" both work.
func aclBuilder(a kafka.ACLState, mode builderMode) (*kadm.ACLBuilder, error) {
	rt, err := kmsg.ParseACLResourceType(a.ResourceType)
	if err != nil {
		return nil, fmt.Errorf("invalid ACL resourceType %q: %w", a.ResourceType, err)
	}
	op, err := kmsg.ParseACLOperation(a.Operation)
	if err != nil {
		return nil, fmt.Errorf("invalid ACL operation %q: %w", a.Operation, err)
	}
	perm, err := kmsg.ParseACLPermissionType(a.Permission)
	if err != nil {
		return nil, fmt.Errorf("invalid ACL permission %q: %w", a.Permission, err)
	}
	pat, err := kmsg.ParseACLResourcePatternType(a.PatternType)
	if err != nil {
		return nil, fmt.Errorf("invalid ACL patternType %q: %w", a.PatternType, err)
	}

	b := kadm.NewACLs().ResourcePatternType(pat).Operations(op)

	// Resource selector by type.
	switch rt {
	case kmsg.ACLResourceTypeTopic:
		b.Topics(a.ResourceName)
	case kmsg.ACLResourceTypeGroup:
		b.Groups(a.ResourceName)
	case kmsg.ACLResourceTypeCluster:
		b.Clusters()
	case kmsg.ACLResourceTypeTransactionalId:
		b.TransactionalIDs(a.ResourceName)
	case kmsg.ACLResourceTypeDelegationToken:
		b.DelegationTokens(a.ResourceName)
	default:
		return nil, fmt.Errorf("unsupported ACL resourceType %q", a.ResourceType)
	}

	// Permission -> Allow/Deny principal+host.
	switch perm {
	case kmsg.ACLPermissionTypeAllow:
		b.Allow(a.Principal)
		if mode == builderModeFilter || a.Host != "" {
			b.AllowHosts(a.Host)
		}
	case kmsg.ACLPermissionTypeDeny:
		b.Deny(a.Principal)
		if mode == builderModeFilter || a.Host != "" {
			b.DenyHosts(a.Host)
		}
	default:
		return nil, fmt.Errorf("unsupported ACL permission %q", a.Permission)
	}

	return b, nil
}

// isUnknownTopic reports whether err is Kafka's UNKNOWN_TOPIC_OR_PARTITION,
// which kadm surfaces as a per-topic load error for absent topics.
func isUnknownTopic(err error) bool {
	return errors.Is(err, kerr.UnknownTopicOrPartition)
}

// isResourceNotFound reports whether err is Kafka's RESOURCE_NOT_FOUND
// (kerr.ResourceNotFound, code 91), which the broker returns as a per-user Err
// from DescribeUserSCRAMCredentials when a caller explicitly names a user
// that has no SCRAM credentials configured. See ListScramCredentials.
func isResourceNotFound(err error) bool {
	return errors.Is(err, kerr.ResourceNotFound)
}

// --- Quotas ---

// ListQuotas returns all client quota entries on the cluster.
//
// Confluent Platform 8.x has two constraints on DescribeClientQuotas filters:
//
//  1. "ip" cannot be mixed with "user" or "client-id" in the same request
//     (INVALID_REQUEST: "IP filter component should not be used with user or
//     clientId filter component").
//
//  2. Combining "user" and "client-id" in a single request on CP 8.x returns
//     only entities that have BOTH components (i.e. user+client-id composite
//     quotas), silently omitting user-only or client-id-only entities.
//
// To capture all three entity shapes we therefore issue three separate
// requests — one each for user-only, client-id-only, and ip-only — and merge
// the results. Duplicate-free by construction: a user-only entity only appears
// in the user request, a composite entity appears in both the user and
// client-id requests, but we deduplicate by entity key.
func (c *Client) ListQuotas(ctx context.Context) ([]kafka.QuotaState, error) {
	// describeOne issues a single-component filter and returns the entries
	// (empty on UNSUPPORTED_VERSION so callers don't need to handle that case).
	describeOne := func(compType string) ([]kadm.DescribedClientQuota, error) {
		comps := []kadm.DescribeClientQuotaComponent{
			{Type: compType, MatchType: kmsg.QuotasMatchTypeAny},
		}
		d, err := c.adm.DescribeClientQuotas(ctx, false, comps)
		if err != nil {
			if errors.Is(err, kerr.UnsupportedVersion) {
				return nil, nil
			}
			return nil, fmt.Errorf("describing client quotas (%s): %w", compType, err)
		}
		return d, nil
	}

	userDescribed, err := describeOne("user")
	if err != nil {
		return nil, err
	}
	clientDescribed, err := describeOne("client-id")
	if err != nil {
		return nil, err
	}
	ipDescribed, err := describeOne("ip")
	if err != nil {
		return nil, err
	}

	// Merge, deduplicating by entity key (composite user+client-id entries
	// appear in both userDescribed and clientDescribed).
	seen := make(map[string]struct{})
	var out []kafka.QuotaState
	for _, d := range append(append(userDescribed, clientDescribed...), ipDescribed...) {
		// Entity.String() is a stable per-entity key: the broker returns an
		// entity's components in a fixed wire order, so the same composite entity
		// stringifies identically in the user- and client-id-filter responses.
		key := d.Entity.String()
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		ent := make([]kafka.QuotaEntityComponent, 0, len(d.Entity))
		for _, comp := range d.Entity {
			ent = append(ent, kafka.QuotaEntityComponent{Type: comp.Type, Name: comp.Name})
		}
		limits := make(map[string]float64, len(d.Values))
		for _, v := range d.Values {
			limits[v.Key] = v.Value
		}
		out = append(out, kafka.QuotaState{Entity: ent, Limits: limits})
	}
	return out, nil
}

// SetQuota sets the given quota limits for the entity.
func (c *Client) SetQuota(ctx context.Context, entity []kafka.QuotaEntityComponent, limits map[string]float64) error {
	return c.alterQuota(ctx, entity, limits, false)
}

// DeleteQuota removes the given quota keys from the entity.
func (c *Client) DeleteQuota(ctx context.Context, entity []kafka.QuotaEntityComponent, keys []string) error {
	rm := make(map[string]float64, len(keys))
	for _, k := range keys {
		rm[k] = 0
	}
	return c.alterQuota(ctx, entity, rm, true)
}

// alterQuota is the shared implementation for SetQuota and DeleteQuota: it
// translates the kafka-layer entity into a kadm.ClientQuotaEntity, builds
// AlterClientQuotaOps in deterministic (sorted) key order, and surfaces any
// per-entry broker error.
func (c *Client) alterQuota(ctx context.Context, entity []kafka.QuotaEntityComponent, limits map[string]float64, remove bool) error {
	kent := make(kadm.ClientQuotaEntity, 0, len(entity))
	for _, comp := range entity {
		kent = append(kent, kadm.ClientQuotaEntityComponent{Type: comp.Type, Name: comp.Name})
	}
	keys := make([]string, 0, len(limits))
	for k := range limits {
		keys = append(keys, k)
	}
	sort.Strings(keys) // deterministic op order
	ops := make([]kadm.AlterClientQuotaOp, 0, len(keys))
	for _, k := range keys {
		ops = append(ops, kadm.AlterClientQuotaOp{Key: k, Value: limits[k], Remove: remove})
	}
	res, err := c.adm.AlterClientQuotas(ctx, []kadm.AlterClientQuotaEntry{{Entity: kent, Ops: ops}})
	if err != nil {
		return fmt.Errorf("altering client quota: %w", err)
	}
	for _, r := range res {
		if r.Err != nil {
			return fmt.Errorf("altering client quota %s: %w", r.Entity, r.Err)
		}
	}
	return nil
}

// --- SCRAM credentials ---

// defaultScramIterations is used when a ScramUpsert leaves Iterations unset
// (0). kadm.UpsertSCRAM requires Iterations to be nonzero -- it feeds it
// directly into pbkdf2.Key as the round count, so 0 would silently produce an
// unsalted-strength (zero-round) derived key rather than erroring. Kafka's own
// broker-side bounds for user.scram.credential.iterations are
// [4096, 16384]; 4096 is Kafka's documented default and the lower bound, so
// it is the safest value to assume when the caller expresses no preference.
const defaultScramIterations = 4096

// parseScramMechanism maps our canonical mechanism enum ("SCRAM-SHA-256" /
// "SCRAM-SHA-512") to kadm's ScramMechanism. Any other value is a hard error:
// mechanism strings reach here from parsed manifests, so a typo or an
// unsupported mechanism must fail loudly rather than being coerced to some
// default.
func parseScramMechanism(mechanism string) (kadm.ScramMechanism, error) {
	switch mechanism {
	case "SCRAM-SHA-256":
		return kadm.ScramSha256, nil
	case "SCRAM-SHA-512":
		return kadm.ScramSha512, nil
	default:
		return 0, fmt.Errorf("unsupported SCRAM mechanism %q", mechanism)
	}
}

// ListScramCredentials returns the observable SCRAM credential identities for
// the given usernames (all credentialed users when usernames is empty), via
// kadm.DescribeUserSCRAMs.
//
// A requested-but-absent user is NOT an error: confirmed against a live
// broker (Confluent Local 7.6.1 / KRaft), DescribeUserSCRAMCredentials
// returns RESOURCE_NOT_FOUND as a PER-USER Err (kerr.ResourceNotFound, code
// 91) -- not merely an omission from the response map -- whenever a caller
// explicitly names a user that currently has zero SCRAM credentials (e.g.
// right after its last mechanism was deleted). That is Kafka's documented
// per-resource semantics for this RPC, not a broker error, so it is filtered
// here exactly like isUnknownTopic/deleteTopicErr filter
// UNKNOWN_TOPIC_OR_PARTITION for topics. Any other per-user Err (e.g. an
// authorization failure for that principal) still propagates. Results are
// sorted by user, then by mechanism, for deterministic output.
func (c *Client) ListScramCredentials(ctx context.Context, usernames ...string) ([]kafka.ScramCredential, error) {
	described, err := c.adm.DescribeUserSCRAMs(ctx, usernames...)
	if err != nil {
		return nil, fmt.Errorf("describing user SCRAM credentials: %w", err)
	}
	var out []kafka.ScramCredential
	for _, d := range described.Sorted() {
		if d.Err != nil {
			if isResourceNotFound(d.Err) {
				continue
			}
			return nil, fmt.Errorf("describing SCRAM credentials for user %q: %w", d.User, d.Err)
		}
		for _, ci := range d.CredInfos {
			out = append(out, kafka.ScramCredential{
				User:       d.User,
				Mechanism:  ci.Mechanism.String(),
				Iterations: ci.Iterations,
			})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].User != out[j].User {
			return out[i].User < out[j].User
		}
		return out[i].Mechanism < out[j].Mechanism
	})
	return out, nil
}

// UpsertScramCredential creates or updates the SCRAM credential for
// (u.User, u.Mechanism) via kadm.AlterUserSCRAMs. kadm generates a fresh
// 24-byte salt and derives the salted password with pbkdf2 from u.Password;
// this adapter never computes or stores salt/hash material itself. If
// u.Iterations is 0 (unset), defaultScramIterations is used instead of
// passing 0 through to kadm/the broker.
//
// SECURITY: u.Password is passed to kadm by value and is never included in
// any error or log line below -- errors reference only the user and
// mechanism, matching the SECURITY comment convention in oauth.go.
func (c *Client) UpsertScramCredential(ctx context.Context, u kafka.ScramUpsert) error {
	mech, err := parseScramMechanism(u.Mechanism)
	if err != nil {
		return fmt.Errorf("upserting SCRAM credential for user %q: %w", u.User, err)
	}
	iterations := u.Iterations
	if iterations == 0 {
		iterations = defaultScramIterations
	}
	res, err := c.adm.AlterUserSCRAMs(ctx, nil, []kadm.UpsertSCRAM{{
		User:       u.User,
		Mechanism:  mech,
		Iterations: iterations,
		Password:   u.Password,
	}})
	if err != nil {
		return fmt.Errorf("upserting SCRAM credential for user %q mechanism %q: %w", u.User, u.Mechanism, err)
	}
	if r, ok := res[u.User]; ok && r.Err != nil {
		return fmt.Errorf("upserting SCRAM credential for user %q mechanism %q: %w", u.User, u.Mechanism, r.Err)
	}
	return nil
}

// DeleteScramCredential removes only the (username, mechanism) credential via
// kadm.AlterUserSCRAMs; any other mechanism configured for the user is
// untouched.
func (c *Client) DeleteScramCredential(ctx context.Context, username, mechanism string) error {
	mech, err := parseScramMechanism(mechanism)
	if err != nil {
		return fmt.Errorf("deleting SCRAM credential for user %q: %w", username, err)
	}
	res, err := c.adm.AlterUserSCRAMs(ctx, []kadm.DeleteSCRAM{{
		User:      username,
		Mechanism: mech,
	}}, nil)
	if err != nil {
		return fmt.Errorf("deleting SCRAM credential for user %q mechanism %q: %w", username, mechanism, err)
	}
	if r, ok := res[username]; ok && r.Err != nil {
		return fmt.Errorf("deleting SCRAM credential for user %q mechanism %q: %w", username, mechanism, r.Err)
	}
	return nil
}
