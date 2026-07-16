//go:build integration

package franz

// Build-tagged integration tests that round-trip the real franz/kadm adapter
// (internal/kafka/franz) and the executor against a REAL Kafka broker started
// via testcontainers-go. These are EXCLUDED from the default `go test ./...`
// suite (no build tag) so the default run stays hermetic and Docker-free.
//
// Run them with:
//
//	go test -tags integration ./internal/kafka/franz/ -v
//
// They SKIP cleanly (t.Skip) when Docker is unavailable, via
// testcontainers.SkipIfProviderIsNotHealthy plus a fallback check on the
// container-start error, so a Docker-less environment never sees a failure.
//
// A fresh broker container is started PER test (one call to startKafka per
// test). This is slower than a shared broker but trivially isolates each test;
// topic/principal names additionally include t.Name() for extra clarity.

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/monedula-dev/monedula-gitops/internal/access"
	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/diff"
	"github.com/monedula-dev/monedula-gitops/internal/executor"
	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	"github.com/monedula-dev/monedula-gitops/internal/operations"
	"github.com/monedula-dev/monedula-gitops/internal/secrets"
)

// kafkaImage is the Confluent broker image used by the testcontainers kafka
// module. It speaks plaintext (no TLS/SASL) which matches franz.New(cluster,"")
// with no Spec.TLS/Spec.Auth.
const kafkaImage = "confluentinc/confluent-local:7.6.1"

// startKafka starts a single-broker Kafka container and returns a connected
// franz Client wired to its bootstrap brokers. The container and client are
// torn down via t.Cleanup. If Docker is unavailable the test is skipped (never
// failed): first via testcontainers.SkipIfProviderIsNotHealthy, then as a
// belt-and-braces fallback if the container start itself fails for a
// docker-connectivity reason.
func startKafka(t *testing.T) *Client {
	t.Helper()

	// Primary Docker-availability gate.
	testcontainers.SkipIfProviderIsNotHealthy(t)

	ctx := context.Background()
	container, err := tckafka.Run(ctx, kafkaImage)
	if err != nil {
		if isDockerUnavailable(err) {
			t.Skipf("Docker not available, skipping integration test: %v", err)
		}
		t.Fatalf("starting kafka container: %v", err)
	}
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Logf("terminating kafka container: %v", err)
		}
	})

	brokers, err := container.Brokers(ctx)
	require.NoError(t, err, "getting container bootstrap brokers")
	require.NotEmpty(t, brokers, "container returned no brokers")

	cluster := &v1alpha1.KafkaCluster{
		Spec: v1alpha1.KafkaClusterSpec{
			BootstrapServers: strings.Join(brokers, ","),
			// No TLS, no Auth: the testcontainers kafka module is plaintext.
		},
	}

	client, err := New(cluster, secrets.FileEnvResolver{BaseDir: ""})
	require.NoError(t, err, "constructing franz client")
	t.Cleanup(client.Close)
	return client
}

// isDockerUnavailable heuristically detects a docker-connectivity failure from a
// container-start error so we can t.Skip instead of t.Fatal. This is a fallback
// behind SkipIfProviderIsNotHealthy.
func isDockerUnavailable(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	for _, needle := range []string{
		"cannot connect to the docker daemon",
		"docker daemon",
		"dial unix",
		"no such file or directory",
		"connection refused",
		"docker.sock",
		"rootless docker not found",
		"failed to find a viable docker",
		"docker host",
	} {
		if strings.Contains(s, needle) {
			return true
		}
	}
	return false
}

// ctxT returns a per-test context with a generous timeout, tied to t.Cleanup.
func ctxT(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// --- Topics ---

func TestIntegration_TopicRoundTrip(t *testing.T) {
	client := startKafka(t)
	ctx := ctxT(t)

	name := "rt-" + sanitize(t.Name())
	spec := kafka.TopicSpec{
		Name:              name,
		Partitions:        3,
		ReplicationFactor: 1,
		Config:            map[string]string{"retention.ms": "604800000"},
	}
	require.NoError(t, client.CreateTopic(ctx, spec))

	got, err := client.GetTopic(ctx, name)
	require.NoError(t, err)
	require.NotNil(t, got, "GetTopic returned nil for a topic we just created")
	assert.Equal(t, 3, got.Partitions)
	assert.Equal(t, 1, got.ReplicationFactor)
	assert.Equal(t, "604800000", got.Config["retention.ms"])

	all, err := client.ListTopics(ctx)
	require.NoError(t, err)
	assert.True(t, containsTopic(all, name), "ListTopics did not include %q", name)
}

func TestIntegration_UpdateTopicConfig(t *testing.T) {
	client := startKafka(t)
	ctx := ctxT(t)

	name := "cfg-" + sanitize(t.Name())
	require.NoError(t, client.CreateTopic(ctx, kafka.TopicSpec{
		Name:              name,
		Partitions:        1,
		ReplicationFactor: 1,
		Config:            map[string]string{"retention.ms": "604800000"},
	}))

	require.NoError(t, client.UpdateTopicConfig(ctx, name, map[string]string{"retention.ms": "1209600000"}))

	got, err := client.GetTopic(ctx, name)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "1209600000", got.Config["retention.ms"], "UpdateTopicConfig change not reflected by GetTopic")
}

func TestIntegration_CreatePartitions(t *testing.T) {
	client := startKafka(t)
	ctx := ctxT(t)

	name := "part-" + sanitize(t.Name())
	require.NoError(t, client.CreateTopic(ctx, kafka.TopicSpec{
		Name: name, Partitions: 3, ReplicationFactor: 1,
	}))

	require.NoError(t, client.CreatePartitions(ctx, name, 6))

	got, err := client.GetTopic(ctx, name)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, 6, got.Partitions, "CreatePartitions did not raise partition count")
}

// TestIntegration_ErrorSurfacing is CRITICAL: it pins that broker-rejected
// mutations surface a non-nil error rather than silently succeeding.
func TestIntegration_ErrorSurfacing(t *testing.T) {
	client := startKafka(t)
	ctx := ctxT(t)

	name := "err-" + sanitize(t.Name())
	require.NoError(t, client.CreateTopic(ctx, kafka.TopicSpec{
		Name: name, Partitions: 3, ReplicationFactor: 1,
	}))

	t.Run("CreateTopic on existing topic errors", func(t *testing.T) {
		err := client.CreateTopic(ctx, kafka.TopicSpec{Name: name, Partitions: 3, ReplicationFactor: 1})
		assert.Error(t, err, "re-creating an existing topic must surface a non-nil error")
	})

	t.Run("CreatePartitions shrink errors", func(t *testing.T) {
		err := client.CreatePartitions(ctx, name, 1) // current is 3
		assert.Error(t, err, "shrinking partitions must surface a non-nil error")
	})

	// DeleteTopic on a non-existent topic is IDEMPOTENT (review I7): kadm
	// surfaces UNKNOWN_TOPIC_OR_PARTITION as a per-topic error, and the adapter
	// filters it to success (deleteTopicErr) because "topic absent" is the
	// desired end state of a delete — matching the mock. This used to assert an
	// error (pinning the old non-idempotent behavior), which made a
	// deletionPolicy=Delete CR whose topic was removed out-of-band impossible to
	// finalize.
	t.Run("DeleteTopic on non-existent topic is idempotent (no error)", func(t *testing.T) {
		err := client.DeleteTopic(ctx, "never-created-"+sanitize(t.Name()))
		assert.NoError(t, err, "deleting a non-existent topic must be an idempotent no-op")
	})
}

func TestIntegration_GetTopicAbsent(t *testing.T) {
	client := startKafka(t)
	ctx := ctxT(t)

	got, err := client.GetTopic(ctx, "never-created-"+sanitize(t.Name()))
	require.NoError(t, err, "GetTopic on an absent topic must not error")
	assert.Nil(t, got, "GetTopic on an absent topic must return (nil, nil)")
}

// TestIntegration_ListTopicsExcludesInternal confirms ListTopics uses the
// non-internal listing (not ListTopicsWithInternal): no topic name starting
// with "__" (e.g. __consumer_offsets) is returned. The created topic must be
// present. The internal-topic exclusion is a soft check in that the container
// may not have materialized __consumer_offsets yet, but the "no __ prefix"
// invariant always holds for ListTopics.
func TestIntegration_ListTopicsExcludesInternal(t *testing.T) {
	client := startKafka(t)
	ctx := ctxT(t)

	name := "vis-" + sanitize(t.Name())
	require.NoError(t, client.CreateTopic(ctx, kafka.TopicSpec{Name: name, Partitions: 1, ReplicationFactor: 1}))

	all, err := client.ListTopics(ctx)
	require.NoError(t, err)
	assert.True(t, containsTopic(all, name), "ListTopics did not include the created topic %q", name)
	for _, ts := range all {
		assert.False(t, strings.HasPrefix(ts.Name, "__"),
			"ListTopics returned an internal topic %q (should use non-internal listing)", ts.Name)
		assert.NotEqual(t, "__consumer_offsets", ts.Name)
	}
}

// --- ACLs ---

// TestIntegration_ACLRoundTrip is CRITICAL: it exercises Create -> List ->
// Delete -> List across every supported resource type and both permissions,
// asserting the created tuple appears in ListACLs (confirming
// Type.String()/Pattern.String()/Operation.String()/Permission.String()
// re-parse symmetrically) and disappears after delete.
func TestIntegration_ACLRoundTrip(t *testing.T) {
	client := startKafka(t)
	ctx := ctxT(t)

	resourceTypes := []struct {
		typ  string
		name string
	}{
		{"topic", "acl-topic-" + sanitize(t.Name())},
		{"group", "acl-group-" + sanitize(t.Name())},
		{"cluster", "kafka-cluster"}, // cluster ACLs use the fixed resource name "kafka-cluster"
		{"transactionalId", "acl-txn-" + sanitize(t.Name())},
	}
	permissions := []string{"allow", "deny"}

	for _, rt := range resourceTypes {
		for i, perm := range permissions {
			rt, perm := rt, perm
			// Unique principal per (resourceType, permission) to avoid Allow/Deny
			// collisions on the same subject.
			principal := "User:it-" + sanitize(t.Name()) + "-" + rt.typ + "-" + perm
			acl := kafka.ACLState{
				Principal:    principal,
				Host:         "*",
				ResourceType: rt.typ,
				ResourceName: rt.name,
				PatternType:  "literal",
				Operation:    operationFor(i), // vary operation to keep tuples distinct
				Permission:   perm,
			}

			require.NoError(t, client.CreateACLs(ctx, []kafka.ACLState{acl}),
				"CreateACLs failed for %s/%s", rt.typ, perm)

			list, err := client.ListACLs(ctx)
			require.NoError(t, err)
			assert.True(t, aclPresent(list, acl),
				"ListACLs did not return the created %s/%s ACL (symmetry failure): %+v\nlisted: %+v",
				rt.typ, perm, acl, list)

			require.NoError(t, client.DeleteACLs(ctx, []kafka.ACLState{acl}),
				"DeleteACLs failed for %s/%s", rt.typ, perm)

			list, err = client.ListACLs(ctx)
			require.NoError(t, err)
			assert.False(t, aclPresent(list, acl),
				"ListACLs still returned the %s/%s ACL after delete", rt.typ, perm)
		}
	}
}

// TestIntegration_ACLHostDefaulting confirms that creating an Allow ACL with an
// empty Host results in the broker default host "*" (the adapter omits the host
// on create when empty), and that a delete using the explicit "*" host removes
// it (the filter path requires an explicit host).
func TestIntegration_ACLHostDefaulting(t *testing.T) {
	client := startKafka(t)
	ctx := ctxT(t)

	principal := "User:hostdefault-" + sanitize(t.Name())
	created := kafka.ACLState{
		Principal:    principal,
		Host:         "", // empty -> broker should default to "*"
		ResourceType: "topic",
		ResourceName: "hostdefault-" + sanitize(t.Name()),
		PatternType:  "literal",
		Operation:    "read",
		Permission:   "allow",
	}
	require.NoError(t, client.CreateACLs(ctx, []kafka.ACLState{created}))

	list, err := client.ListACLs(ctx)
	require.NoError(t, err)

	// The same tuple but with host "*" — what the broker should report.
	starHost := created
	starHost.Host = "*"
	assert.True(t, aclPresent(list, starHost),
		"empty-host Allow ACL should be reported with host %q; listed: %+v", "*", list)

	// Delete using the explicit "*" host (filter mode requires explicit host).
	require.NoError(t, client.DeleteACLs(ctx, []kafka.ACLState{starHost}))

	list, err = client.ListACLs(ctx)
	require.NoError(t, err)
	assert.False(t, aclPresent(list, starHost), "ACL not removed after DeleteACLs with host %q", "*")
}

// --- End-to-end converge ---

// TestIntegration_Converge builds a desired topic state, computes ops against
// the live container via diff.Compute, applies them via executor.Apply with all
// approvals, then re-computes and asserts the diff is empty (converged).
func TestIntegration_Converge(t *testing.T) {
	client := startKafka(t)
	ctx := ctxT(t)

	topicName := "converge-" + sanitize(t.Name())
	desired := diff.Desired{
		Topics: []diff.DesiredTopic{{
			Kind:              "KafkaTopic",
			Name:              topicName,
			Partitions:        3,
			ReplicationFactor: 1,
			Config:            map[string]string{"retention.ms": "604800000"},
		}},
		Scope: access.BuildScope(nil),
	}

	live := liveState(t, ctx, client)
	ops := diff.Compute(desired, live)
	require.NotEmpty(t, ops, "expected at least a CreateTopic op against an empty cluster")

	res := executor.Apply(ctx, executor.Clients{Kafka: client}, ops, executor.Approvals{Destructive: true, Delete: true})
	require.True(t, res.OK(), "executor.Apply did not fully succeed: %+v", res.Results)

	// Re-read live state and recompute: must be converged (zero ops).
	live2 := liveState(t, ctx, client)
	ops2 := diff.Compute(desired, live2)
	assert.Empty(t, ops2, "cluster did not converge; residual ops: %s", renderOps(ops2))
}

// liveState reads topics and ACLs from the live cluster into a diff.Live.
func liveState(t *testing.T, ctx context.Context, client *Client) diff.Live {
	t.Helper()
	topics, err := client.ListTopics(ctx)
	require.NoError(t, err)
	aclStates, err := client.ListACLs(ctx)
	require.NoError(t, err)
	acls := make([]access.ACL, 0, len(aclStates))
	for _, a := range aclStates {
		acls = append(acls, access.ACL{
			Principal:    a.Principal,
			Host:         a.Host,
			ResourceType: a.ResourceType,
			ResourceName: a.ResourceName,
			PatternType:  a.PatternType,
			Operation:    a.Operation,
			Permission:   a.Permission,
		})
	}
	return diff.Live{Topics: topics, ACLs: acls}
}

// --- helpers ---

// sanitize turns a test name (which may contain '/' and other characters not
// valid in Kafka topic names) into a Kafka-safe slug.
func sanitize(s string) string {
	r := strings.NewReplacer("/", "-", " ", "-", "_", "-")
	return strings.ToLower(r.Replace(s))
}

func containsTopic(ts []kafka.TopicState, name string) bool {
	for _, t := range ts {
		if t.Name == name {
			return true
		}
	}
	return false
}

// operationFor returns a distinct, valid ACL operation per index so that ACLs
// across permission variants do not collide on the same subject key.
func operationFor(i int) string {
	ops := []string{"read", "write", "describe", "alter"}
	return ops[i%len(ops)]
}

// aclPresent reports whether want is present in the listed ACLs, comparing each
// enum field in its CANONICAL parsed form so the comparison is robust to the
// adapter returning kmsg's canonical String() (e.g. "TOPIC", "READ") regardless
// of the case used on input.
func aclPresent(list []kafka.ACLState, want kafka.ACLState) bool {
	for _, got := range list {
		if aclEqual(got, want) {
			return true
		}
	}
	return false
}

func aclEqual(a, b kafka.ACLState) bool {
	return a.Principal == b.Principal &&
		a.Host == b.Host &&
		a.ResourceName == b.ResourceName &&
		canonResourceType(a.ResourceType) == canonResourceType(b.ResourceType) &&
		canonPattern(a.PatternType) == canonPattern(b.PatternType) &&
		canonOperation(a.Operation) == canonOperation(b.Operation) &&
		canonPermission(a.Permission) == canonPermission(b.Permission)
}

func canonResourceType(s string) kmsg.ACLResourceType {
	v, _ := kmsg.ParseACLResourceType(s)
	return v
}
func canonPattern(s string) kmsg.ACLResourcePatternType {
	v, _ := kmsg.ParseACLResourcePatternType(s)
	return v
}
func canonOperation(s string) kmsg.ACLOperation {
	v, _ := kmsg.ParseACLOperation(s)
	return v
}
func canonPermission(s string) kmsg.ACLPermissionType {
	v, _ := kmsg.ParseACLPermissionType(s)
	return v
}

// --- Quotas ---

// TestIntegration_QuotaRoundTrip exercises SetQuota -> ListQuotas -> DeleteQuota
// -> ListQuotas against the real broker (Confluent Local 7.6.1 supports client
// quotas via the standard AlterClientQuotas / DescribeClientQuotas APIs).
func TestIntegration_QuotaRoundTrip(t *testing.T) {
	client := startKafka(t)
	ctx := ctxT(t)

	entity := []kafka.QuotaEntityComponent{
		{Type: "user", Name: strPtr("itest-quota")},
	}

	// Set producer_byte_rate = 1000 for user=itest-quota.
	require.NoError(t, client.SetQuota(ctx, entity, map[string]float64{
		"producer_byte_rate": 1000,
	}), "SetQuota failed")

	// ListQuotas must include the entry we just set.
	list, err := client.ListQuotas(ctx)
	require.NoError(t, err, "ListQuotas after SetQuota failed")
	assert.True(t, quotaHasLimit(list, "user", "itest-quota", "producer_byte_rate", 1000),
		"ListQuotas did not return the expected producer_byte_rate=1000 for user itest-quota; got: %+v", list)

	// DeleteQuota must remove the key.
	require.NoError(t, client.DeleteQuota(ctx, entity, []string{"producer_byte_rate"}),
		"DeleteQuota failed")

	// ListQuotas must no longer carry that entry (entity absent or limit key removed).
	list2, err := client.ListQuotas(ctx)
	require.NoError(t, err, "ListQuotas after DeleteQuota failed")
	assert.False(t, quotaHasLimit(list2, "user", "itest-quota", "producer_byte_rate", 1000),
		"ListQuotas still returned producer_byte_rate=1000 for user itest-quota after DeleteQuota; got: %+v", list2)
}

// strPtr is a helper that returns a pointer to s, for building QuotaEntityComponent.Name.
func strPtr(s string) *string { return &s }

// quotaHasLimit reports whether the list contains a quota for the given entity
// type+name with the specified limit key at the given value.
func quotaHasLimit(list []kafka.QuotaState, entityType, entityName, limitKey string, value float64) bool {
	for _, q := range list {
		for _, comp := range q.Entity {
			if comp.Type == entityType && comp.Name != nil && *comp.Name == entityName {
				if v, ok := q.Limits[limitKey]; ok && v == value {
					return true
				}
			}
		}
	}
	return false
}

// --- SCRAM credentials ---

// TestIntegration_ScramCredentialRoundTrip exercises
// UpsertScramCredential -> ListScramCredentials -> DeleteScramCredential ->
// ListScramCredentials against the real broker. AlterUserSCRAMCredentials /
// DescribeUserSCRAMCredentials are cluster-management APIs independent of
// which SASL mechanisms (if any) the broker's listeners are configured with,
// so this works against the plaintext, no-SASL testcontainers broker used by
// every other integration test here: the credential is written and read back
// from ZooKeeper-less KRaft metadata, never actually used to authenticate a
// connection.
func TestIntegration_ScramCredentialRoundTrip(t *testing.T) {
	client := startKafka(t)
	ctx := ctxT(t)

	user := "itest-scram-" + sanitize(t.Name())

	// Upsert with Iterations left unset (0): the adapter must apply its
	// default rather than sending 0 to the broker.
	require.NoError(t, client.UpsertScramCredential(ctx, kafka.ScramUpsert{
		User:      user,
		Mechanism: "SCRAM-SHA-512",
		Password:  "s3cr3t-password-value",
	}), "UpsertScramCredential failed")

	list, err := client.ListScramCredentials(ctx, user)
	require.NoError(t, err, "ListScramCredentials after upsert failed")
	require.Len(t, list, 1, "expected exactly one credential for %q", user)
	assert.Equal(t, user, list[0].User)
	assert.Equal(t, "SCRAM-SHA-512", list[0].Mechanism)
	assert.Greater(t, list[0].Iterations, int32(0), "Iterations must resolve to the adapter's default, not 0")

	// A second mechanism for the same user is independent.
	require.NoError(t, client.UpsertScramCredential(ctx, kafka.ScramUpsert{
		User:       user,
		Mechanism:  "SCRAM-SHA-256",
		Iterations: 4096,
		Password:   "another-password-value",
	}), "UpsertScramCredential (second mechanism) failed")

	list, err = client.ListScramCredentials(ctx, user)
	require.NoError(t, err)
	require.Len(t, list, 2, "expected both mechanisms listed for %q", user)

	// DeleteScramCredential removes only the targeted mechanism.
	require.NoError(t, client.DeleteScramCredential(ctx, user, "SCRAM-SHA-256"),
		"DeleteScramCredential failed")

	list, err = client.ListScramCredentials(ctx, user)
	require.NoError(t, err, "ListScramCredentials after delete failed")
	require.Len(t, list, 1, "expected only SCRAM-SHA-512 to remain for %q", user)
	assert.Equal(t, "SCRAM-SHA-512", list[0].Mechanism)

	// Deleting the remaining mechanism leaves the user with none, so a
	// filtered ListScramCredentials for that user returns empty (absent, not
	// an error).
	require.NoError(t, client.DeleteScramCredential(ctx, user, "SCRAM-SHA-512"))
	list, err = client.ListScramCredentials(ctx, user)
	require.NoError(t, err)
	assert.Empty(t, list, "user with no remaining SCRAM credentials must not appear in ListScramCredentials")
}

// TestIntegration_ScramCredentialUnknownMechanismErrors confirms both
// UpsertScramCredential and DeleteScramCredential reject a non-canonical
// mechanism string loudly rather than silently coercing or ignoring it.
func TestIntegration_ScramCredentialUnknownMechanismErrors(t *testing.T) {
	client := startKafka(t)
	ctx := ctxT(t)
	user := "itest-scram-bad-" + sanitize(t.Name())

	err := client.UpsertScramCredential(ctx, kafka.ScramUpsert{
		User: user, Mechanism: "SCRAM-SHA-1", Password: "whatever",
	})
	assert.Error(t, err, "unsupported mechanism must error on upsert")

	err = client.DeleteScramCredential(ctx, user, "SCRAM-SHA-1")
	assert.Error(t, err, "unsupported mechanism must error on delete")
}

func renderOps(ops []operations.Operation) string {
	var b strings.Builder
	for _, op := range ops {
		b.WriteString("\n  ")
		b.WriteString(string(op.Action))
		b.WriteString(" ")
		b.WriteString(op.Target)
		if op.Field != "" {
			b.WriteString(" field=" + op.Field + " from=" + op.From + " to=" + op.To)
		}
	}
	return b.String()
}
