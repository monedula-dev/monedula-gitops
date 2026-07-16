package executor

import (
	"context"
	"errors"
	"testing"

	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	"github.com/monedula-dev/monedula-gitops/internal/kafka/mock"
	mdsmock "github.com/monedula-dev/monedula-gitops/internal/mds/mock"
	"github.com/monedula-dev/monedula-gitops/internal/operations"
	"github.com/monedula-dev/monedula-gitops/internal/rbac"
	srmock "github.com/monedula-dev/monedula-gitops/internal/schemaregistry/mock"
	"github.com/stretchr/testify/require"
)

func createTopicOp(name string, partitions int, config map[string]string) operations.Operation {
	op := operations.New(operations.CreateTopic)
	op.Target = name
	op.Partitions = partitions
	op.ReplicationFactor = 3
	op.Config = config
	return op
}

func TestApplyCreateTopicSucceeds(t *testing.T) {
	client := mock.New(nil, nil)
	op := createTopicOp("payments.orders", 3, map[string]string{"retention.ms": "604800000"})

	res := Apply(context.Background(), Clients{Kafka: client}, []operations.Operation{op}, Approvals{})

	require.Len(t, res.Results, 1)
	require.Equal(t, Succeeded, res.Results[0].Status)
	require.Contains(t, client.Calls(), "CreateTopic payments.orders")

	// Confirm the op payload actually round-trips into the client call, not
	// merely that a call was recorded.
	got, err := client.GetTopic(context.Background(), "payments.orders")
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, 3, got.Partitions)
	require.Equal(t, 3, got.ReplicationFactor)
	require.Equal(t, "604800000", got.Config["retention.ms"])
}

func TestApplyBlocksUngatedDestructive(t *testing.T) {
	client := mock.New([]kafka.TopicState{{Name: "t", Partitions: 3}}, nil)
	op := operations.New(operations.IncreasePartitions)
	op.Target = "t"
	op.Partitions = 6

	res := Apply(context.Background(), Clients{Kafka: client}, []operations.Operation{op}, Approvals{})

	require.Len(t, res.Results, 1)
	require.Equal(t, Blocked, res.Results[0].Status)
	require.Empty(t, client.Calls())
}

func TestApplyExecutesGatedDestructive(t *testing.T) {
	client := mock.New([]kafka.TopicState{{Name: "t", Partitions: 3}}, nil)
	op := operations.New(operations.IncreasePartitions)
	op.Target = "t"
	op.Partitions = 6

	res := Apply(context.Background(), Clients{Kafka: client}, []operations.Operation{op}, Approvals{Destructive: true})

	require.Len(t, res.Results, 1)
	require.Equal(t, Succeeded, res.Results[0].Status)
	require.Contains(t, client.Calls(), "CreatePartitions t 6")
}

func TestApplyDeleteTopicGate(t *testing.T) {
	op := operations.New(operations.DeleteTopic)
	op.Target = "t"

	// Destructive does not satisfy the Delete gate.
	c1 := mock.New([]kafka.TopicState{{Name: "t"}}, nil)
	r1 := Apply(context.Background(), Clients{Kafka: c1}, []operations.Operation{op}, Approvals{Destructive: true})
	require.Equal(t, Blocked, r1.Results[0].Status)
	require.Empty(t, c1.Calls())

	// Delete approval satisfies it.
	c2 := mock.New([]kafka.TopicState{{Name: "t"}}, nil)
	r2 := Apply(context.Background(), Clients{Kafka: c2}, []operations.Operation{op}, Approvals{Delete: true})
	require.Equal(t, Succeeded, r2.Results[0].Status)
	require.Contains(t, c2.Calls(), "DeleteTopic t")
}

func TestApplyBestEffortContinues(t *testing.T) {
	client := mock.New(nil, nil)
	client.FailOn("CreateTopic", "A", errors.New("boom"))
	opA := createTopicOp("A", 1, nil)
	opB := createTopicOp("B", 1, nil)

	res := Apply(context.Background(), Clients{Kafka: client}, []operations.Operation{opA, opB}, Approvals{})

	require.Len(t, res.Results, 2)
	require.Equal(t, Failed, res.Results[0].Status)
	require.Equal(t, "A", res.Results[0].Op.Target)
	require.Equal(t, "boom", res.Results[0].Err)
	require.Equal(t, Succeeded, res.Results[1].Status)
	require.Equal(t, "B", res.Results[1].Op.Target)
	require.Contains(t, client.Calls(), "CreateTopic B")
}

func TestApplySkipsAclOfFailedTopic(t *testing.T) {
	client := mock.New(nil, nil)
	client.FailOn("CreateTopic", "T", errors.New("boom"))
	topicOp := createTopicOp("T", 1, nil)
	aclOp := operations.New(operations.CreateAcl)
	aclOp.Target = "acl"
	aclOp.ACL = &kafka.ACLState{
		Principal:    "User:x",
		Host:         "*",
		ResourceType: "topic",
		ResourceName: "T",
		PatternType:  "literal",
		Operation:    "Write",
		Permission:   "Allow",
	}

	res := Apply(context.Background(), Clients{Kafka: client}, []operations.Operation{topicOp, aclOp}, Approvals{})

	require.Len(t, res.Results, 2)
	require.Equal(t, Failed, res.Results[0].Status)
	require.Equal(t, Skipped, res.Results[1].Status)
	for _, c := range client.Calls() {
		require.NotContains(t, c, "CreateACLs")
	}
}

func TestApplySkipsAclOfFailedTopicInDiffOrder(t *testing.T) {
	// diff.Compute sorts ops for RENDERING by (Action, Target, Field), so
	// "CreateAcl" sorts before "CreateTopic" and a topic's CreateAcl is emitted
	// BEFORE its CreateTopic. Feed that real order: Apply must reorder for
	// execution so CreateTopic runs first, populating failedTopics in time for
	// the prerequisite-skip to fire.
	client := mock.New(nil, nil)
	client.FailOn("CreateTopic", "T", errors.New("boom"))
	topicOp := createTopicOp("T", 1, nil)
	aclOp := operations.New(operations.CreateAcl)
	aclOp.Target = "acl"
	aclOp.ACL = &kafka.ACLState{
		Principal:    "User:x",
		Host:         "*",
		ResourceType: "topic",
		ResourceName: "T",
		PatternType:  "literal",
		Operation:    "Write",
		Permission:   "Allow",
	}

	// CreateAcl listed BEFORE CreateTopic, as Compute would emit it.
	res := Apply(context.Background(), Clients{Kafka: client}, []operations.Operation{aclOp, topicOp}, Approvals{})

	require.Len(t, res.Results, 2)
	// Execution order is topic-first, then ACL.
	require.Equal(t, operations.CreateTopic, res.Results[0].Op.Action)
	require.Equal(t, Failed, res.Results[0].Status)
	require.Equal(t, operations.CreateAcl, res.Results[1].Op.Action)
	require.Equal(t, Skipped, res.Results[1].Status)
	for _, c := range client.Calls() {
		require.NotContains(t, c, "CreateACLs")
	}
}

func TestApplyCreateAclNilACLFails(t *testing.T) {
	client := mock.New(nil, nil)
	op := operations.New(operations.CreateAcl)
	op.Target = "acl"
	// op.ACL intentionally left nil.

	res := Apply(context.Background(), Clients{Kafka: client}, []operations.Operation{op}, Approvals{})

	require.Len(t, res.Results, 1)
	require.Equal(t, Failed, res.Results[0].Status)
	require.NotEmpty(t, res.Results[0].Err)
	for _, c := range client.Calls() {
		require.NotContains(t, c, "CreateACLs")
	}
}

func TestApplyRejectedNeverExecutes(t *testing.T) {
	client := mock.New(nil, nil)
	op := operations.New(operations.Rejected)
	op.Target = "t"

	res := Apply(context.Background(), Clients{Kafka: client}, []operations.Operation{op}, Approvals{Delete: true, Destructive: true})

	require.Len(t, res.Results, 1)
	require.Equal(t, Rejected, res.Results[0].Status)
	require.Empty(t, client.Calls())
}

// TestApplyReplicationFactorBlockedWithoutDestructive pins review I10(a): an
// UpdateReplicationFactor is GateDestructive (§17.1), so WITHOUT
// --allow-destructive it must be Blocked at the gate — not short-circuited to
// Failed before the gate even runs.
func TestApplyReplicationFactorBlockedWithoutDestructive(t *testing.T) {
	client := mock.New([]kafka.TopicState{{Name: "t", ReplicationFactor: 3}}, nil)
	op := operations.New(operations.UpdateReplicationFactor)
	op.Target = "t"
	op.From = "3"
	op.To = "5"

	res := Apply(context.Background(), Clients{Kafka: client}, []operations.Operation{op}, Approvals{})

	require.Len(t, res.Results, 1)
	require.Equal(t, Blocked, res.Results[0].Status)
	require.Empty(t, client.Calls())
}

// TestApplyReplicationFactorUnsupported pins review I10(b): an APPROVED
// UpdateReplicationFactor is recorded with the terminal Unsupported status —
// not Failed (which the operator treats as transient and retries forever) —
// and is never dispatched to the client. The divergence stands: Result.OK()
// must be false.
func TestApplyReplicationFactorUnsupported(t *testing.T) {
	client := mock.New([]kafka.TopicState{{Name: "t", ReplicationFactor: 3}}, nil)
	op := operations.New(operations.UpdateReplicationFactor)
	op.Target = "t"
	op.From = "3"
	op.To = "5"

	res := Apply(context.Background(), Clients{Kafka: client}, []operations.Operation{op}, Approvals{Destructive: true})

	require.Len(t, res.Results, 1)
	require.Equal(t, Unsupported, res.Results[0].Status)
	require.Equal(t, "replication factor changes are not supported; recreate the topic or use kafka-reassign-partitions", res.Results[0].Err)
	require.False(t, res.OK(), "an Unsupported op is a standing divergence; the run must not be OK")
	require.Empty(t, client.Calls())
}

func TestResultOKAndCounts(t *testing.T) {
	client := mock.New(nil, nil)
	client.FailOn("CreateTopic", "A", errors.New("boom"))
	opA := createTopicOp("A", 1, nil) // Failed
	opB := createTopicOp("B", 1, nil) // Succeeded
	rejected := operations.New(operations.Rejected)
	rejected.Target = "r"
	delOp := operations.New(operations.DeleteTopic) // Blocked (no Delete approval)
	delOp.Target = "d"

	res := Apply(context.Background(), Clients{Kafka: client}, []operations.Operation{opA, opB, rejected, delOp}, Approvals{})

	require.False(t, res.OK())
	counts := res.Counts()
	require.Equal(t, 1, counts[Failed])
	require.Equal(t, 1, counts[Succeeded])
	require.Equal(t, 1, counts[Rejected])
	require.Equal(t, 1, counts[Blocked])

	// All succeeded => OK true.
	okRes := Apply(context.Background(), Clients{Kafka: mock.New(nil, nil)}, []operations.Operation{createTopicOp("X", 1, nil)}, Approvals{})
	require.True(t, okRes.OK())

	// Empty => OK true.
	require.True(t, Result{}.OK())
}

func registerSchemaOp(subject, topic string) operations.Operation {
	op := operations.New(operations.RegisterSchema)
	op.Target = subject
	op.Subject = subject
	op.SchemaType = "AVRO"
	op.SchemaDef = `{"type":"record","name":"R","fields":[]}`
	op.Topic = topic
	return op
}

func TestApplyRegisterSchemaSucceeds(t *testing.T) {
	sr := srmock.New()
	op := registerSchemaOp("payments.orders-value", "payments.orders")

	res := Apply(context.Background(), Clients{Kafka: mock.New(nil, nil), Schema: sr}, []operations.Operation{op}, Approvals{})

	require.Len(t, res.Results, 1)
	require.Equal(t, Succeeded, res.Results[0].Status)
	require.Contains(t, sr.Calls(), "RegisterSchema payments.orders-value")
}

func TestApplySchemaWithoutClientFails(t *testing.T) {
	op := registerSchemaOp("payments.orders-value", "payments.orders")

	res := Apply(context.Background(), Clients{Kafka: mock.New(nil, nil)}, []operations.Operation{op}, Approvals{})

	require.Len(t, res.Results, 1)
	require.Equal(t, Failed, res.Results[0].Status)
	require.Equal(t, "schema registry not configured", res.Results[0].Err)
}

func TestApplyLowerCompatibilityBlockedWithoutGate(t *testing.T) {
	newOp := func() operations.Operation {
		op := operations.New(operations.LowerSchemaCompatibility)
		op.Target = "payments.orders-value"
		op.Subject = "payments.orders-value"
		op.Compatibility = "NONE"
		return op
	}

	// Without the destructive gate: Blocked, registry untouched.
	sr1 := srmock.New()
	r1 := Apply(context.Background(), Clients{Kafka: mock.New(nil, nil), Schema: sr1}, []operations.Operation{newOp()}, Approvals{})
	require.Len(t, r1.Results, 1)
	require.Equal(t, Blocked, r1.Results[0].Status)
	require.Empty(t, sr1.Calls())

	// With the destructive gate: Succeeded, SetCompatibility called.
	sr2 := srmock.New()
	r2 := Apply(context.Background(), Clients{Kafka: mock.New(nil, nil), Schema: sr2}, []operations.Operation{newOp()}, Approvals{Destructive: true})
	require.Len(t, r2.Results, 1)
	require.Equal(t, Succeeded, r2.Results[0].Status)
	require.Contains(t, sr2.Calls(), "SetCompatibility payments.orders-value NONE")
}

func TestApplySchemaSkippedForFailedTopic(t *testing.T) {
	client := mock.New(nil, nil)
	client.FailOn("CreateTopic", "T", errors.New("boom"))
	sr := srmock.New()
	topicOp := createTopicOp("T", 1, nil)
	schemaOp := registerSchemaOp("T-value", "T")

	// Fed schema-first to exercise schemas-last ordering as well.
	res := Apply(context.Background(), Clients{Kafka: client, Schema: sr}, []operations.Operation{schemaOp, topicOp}, Approvals{})

	require.Len(t, res.Results, 2)
	require.Equal(t, operations.CreateTopic, res.Results[0].Op.Action)
	require.Equal(t, Failed, res.Results[0].Status)
	require.Equal(t, operations.RegisterSchema, res.Results[1].Op.Action)
	require.Equal(t, Skipped, res.Results[1].Status)
	require.Empty(t, sr.Calls())
}

func TestApplyExecutionOrderSchemasLast(t *testing.T) {
	client := mock.New(nil, nil)
	sr := srmock.New()

	topicOp := createTopicOp("payments.orders", 1, nil)
	aclOp := operations.New(operations.CreateAcl)
	aclOp.Target = "acl"
	aclOp.ACL = &kafka.ACLState{
		Principal:    "User:x",
		Host:         "*",
		ResourceType: "topic",
		ResourceName: "payments.orders",
		PatternType:  "literal",
		Operation:    "Write",
		Permission:   "Allow",
	}
	schemaOp := registerSchemaOp("payments.orders-value", "payments.orders")

	// Feed in a mixed order; execution must be topic, then ACL, then schema.
	res := Apply(context.Background(), Clients{Kafka: client, Schema: sr}, []operations.Operation{schemaOp, aclOp, topicOp}, Approvals{})

	require.Len(t, res.Results, 3)
	require.Equal(t, operations.CreateTopic, res.Results[0].Op.Action)
	require.Equal(t, operations.CreateAcl, res.Results[1].Op.Action)
	require.Equal(t, operations.RegisterSchema, res.Results[2].Op.Action)
	for _, r := range res.Results {
		require.Equal(t, Succeeded, r.Status)
	}
}

// TestApplySchemaSupersededUnsupportedTerminal pins spec §12.1: a
// SchemaSuperseded op is recorded with the terminal Unsupported status (never
// Failed, which callers retry as transient) and is NEVER dispatched to the
// registry — re-registering would dedupe to the old version and loop forever.
func TestApplySchemaSupersededUnsupportedTerminal(t *testing.T) {
	sr := srmock.New()
	op := operations.New(operations.SchemaSuperseded)
	op.Target = "s-value"
	op.Subject = "s-value"
	op.Message = "manifest schema is an older version of subject s-value (registered as v1; latest is v2); update the manifest or roll the registry forward"

	res := Apply(context.Background(), Clients{Kafka: mock.New(nil, nil), Schema: sr},
		[]operations.Operation{op}, Approvals{Delete: true, Destructive: true, Prune: true})

	require.Len(t, res.Results, 1)
	require.Equal(t, Unsupported, res.Results[0].Status)
	require.Equal(t, op.Message, res.Results[0].Err)
	require.False(t, res.OK(), "superseded divergence stands; the run must not be OK")
	require.Empty(t, sr.Calls(), "the registry must never be touched for a superseded schema")
}

// TestApplySchemaSupersededReportOnly: mode gating still wins — a DetectOnly
// resource's superseded schema is reported, never a failure.
func TestApplySchemaSupersededReportOnly(t *testing.T) {
	op := operations.New(operations.SchemaSuperseded)
	op.Target = "s-value"
	op.Mode = operations.ModeDetectOnly

	res := Apply(context.Background(), Clients{Kafka: mock.New(nil, nil)}, []operations.Operation{op}, Approvals{})
	require.Len(t, res.Results, 1)
	require.Equal(t, ReportOnly, res.Results[0].Status)
	require.True(t, res.OK())
}

// --- Quota dispatch tests (spec §39) ---

func strptr(s string) *string { return &s }

// TestApplySetQuotaCallsClientSetQuota verifies that a SetQuota op dispatches to
// the mock's SetQuota and the entity+limits are visible via ListQuotas.
func TestApplySetQuotaCallsClientSetQuota(t *testing.T) {
	client := mock.New(nil, nil)
	entity := []kafka.QuotaEntityComponent{{Type: "user", Name: strptr("svc-checkout")}}
	limits := map[string]float64{"producer_byte_rate": 1048576}

	op := operations.New(operations.SetQuota)
	op.Target = "user=svc-checkout"
	op.QuotaEntity = entity
	op.QuotaLimits = limits

	res := Apply(context.Background(), Clients{Kafka: client}, []operations.Operation{op}, Approvals{})

	require.Len(t, res.Results, 1)
	require.Equal(t, Succeeded, res.Results[0].Status)

	got, err := client.ListQuotas(context.Background())
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, entity, got[0].Entity)
	require.InDelta(t, 1048576.0, got[0].Limits["producer_byte_rate"], 0.001)
}

// TestApplyUpdateQuotaCallsClientSetQuota verifies that an UpdateQuota op (drift
// on an existing entity) also dispatches to SetQuota — both operations share the
// same client call; the distinction is for output readability only.
func TestApplyUpdateQuotaCallsClientSetQuota(t *testing.T) {
	entity := []kafka.QuotaEntityComponent{{Type: "user", Name: strptr("svc-checkout")}}
	client := mock.NewWithQuotas(nil, nil, []kafka.QuotaState{
		{Entity: entity, Limits: map[string]float64{"producer_byte_rate": 500}},
	})
	limits := map[string]float64{"producer_byte_rate": 1000}

	op := operations.New(operations.UpdateQuota)
	op.Target = "user=svc-checkout"
	op.QuotaEntity = entity
	op.QuotaLimits = limits

	res := Apply(context.Background(), Clients{Kafka: client}, []operations.Operation{op}, Approvals{})

	require.Len(t, res.Results, 1)
	require.Equal(t, Succeeded, res.Results[0].Status)

	got, err := client.ListQuotas(context.Background())
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.InDelta(t, 1000.0, got[0].Limits["producer_byte_rate"], 0.001)
}

// TestApplyRemoveQuotaCallsClientDeleteQuota verifies that an APPROVED
// RemoveQuota op (GateDestructive, spec §17.1) dispatches to DeleteQuota with
// the keys extracted from op.QuotaLimits.
func TestApplyRemoveQuotaCallsClientDeleteQuota(t *testing.T) {
	entity := []kafka.QuotaEntityComponent{{Type: "user", Name: strptr("svc-checkout")}}
	client := mock.NewWithQuotas(nil, nil, []kafka.QuotaState{
		{Entity: entity, Limits: map[string]float64{
			"producer_byte_rate": 1000,
			"consumer_byte_rate": 2000,
		}},
	})

	// QuotaLimits carries the keys to remove; values are irrelevant for deletion.
	op := operations.New(operations.RemoveQuota)
	op.Target = "user=svc-checkout"
	op.QuotaEntity = entity
	op.QuotaLimits = map[string]float64{"consumer_byte_rate": 0}

	res := Apply(context.Background(), Clients{Kafka: client}, []operations.Operation{op}, Approvals{Destructive: true})

	require.Len(t, res.Results, 1)
	require.Equal(t, Succeeded, res.Results[0].Status)

	got, err := client.ListQuotas(context.Background())
	require.NoError(t, err)
	require.Len(t, got, 1)
	_, hasCons := got[0].Limits["consumer_byte_rate"]
	require.False(t, hasCons, "consumer_byte_rate should have been removed")
	require.InDelta(t, 1000.0, got[0].Limits["producer_byte_rate"], 0.001)
}

// TestApplyRemoveQuotaBlockedWithoutDestructive pins the RemoveQuota gate
// (spec §17.1): authoritatively deleting a live limit key (unthrottling a
// client) requires --allow-destructive. Without it the op is Blocked, never
// attempted, and the live limit survives untouched.
func TestApplyRemoveQuotaBlockedWithoutDestructive(t *testing.T) {
	entity := []kafka.QuotaEntityComponent{{Type: "user", Name: strptr("svc-checkout")}}
	client := mock.NewWithQuotas(nil, nil, []kafka.QuotaState{
		{Entity: entity, Limits: map[string]float64{
			"producer_byte_rate": 1000,
			"consumer_byte_rate": 2000,
		}},
	})

	op := operations.New(operations.RemoveQuota)
	op.Target = "user=svc-checkout"
	op.QuotaEntity = entity
	op.QuotaLimits = map[string]float64{"consumer_byte_rate": 0}

	res := Apply(context.Background(), Clients{Kafka: client}, []operations.Operation{op}, Approvals{})

	require.Len(t, res.Results, 1)
	require.Equal(t, Blocked, res.Results[0].Status)

	// Not attempted: the live limit key is still there.
	got, err := client.ListQuotas(context.Background())
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.InDelta(t, 2000.0, got[0].Limits["consumer_byte_rate"], 0.001)
}

// --- RBAC role-binding dispatch tests (spec §40) ---

// makeRoleBindingOp builds an AddRoleBinding or RemoveRoleBinding operation
// with a minimal rbac.RoleBinding payload.
func makeRoleBindingOp(action operations.Action, rb rbac.RoleBinding) operations.Operation {
	op := operations.New(action)
	op.Target = rb.Principal + "|" + rb.Role
	op.RoleBinding = &rb
	return op
}

// TestApplyAddRoleBindingCallsMDS verifies that an AddRoleBinding op dispatches
// to MDS.AddRoleBinding and the call is recorded.
func TestApplyAddRoleBindingCallsMDS(t *testing.T) {
	mdsc := mdsmock.New()
	rb := rbac.RoleBinding{
		Principal: "User:alice",
		Role:      "DeveloperRead",
		Scope:     rbac.Scope{Type: "kafka", KafkaCluster: "lkc-abc"},
		Resource: &rbac.ResourcePattern{
			Type:        "Topic",
			Name:        "payments.orders",
			PatternType: "literal",
		},
	}
	op := makeRoleBindingOp(operations.AddRoleBinding, rb)

	res := Apply(context.Background(),
		Clients{Kafka: mock.New(nil, nil), MDS: mdsc},
		[]operations.Operation{op},
		Approvals{})

	require.Len(t, res.Results, 1)
	require.Equal(t, Succeeded, res.Results[0].Status)
	calls := mdsc.Calls()
	require.Len(t, calls, 1)
	require.Contains(t, calls[0], "AddRoleBinding")
	require.Contains(t, calls[0], "User:alice")
}

// TestApplyRemoveRoleBindingCallsMDS verifies that a RemoveRoleBinding op
// dispatches to MDS.RemoveRoleBinding and is gated by GatePrune (PruneDisabled
// without consent, executes with Prune: true).
func TestApplyRemoveRoleBindingCallsMDS(t *testing.T) {
	rb := rbac.RoleBinding{
		Principal: "User:alice",
		Role:      "DeveloperRead",
		Scope:     rbac.Scope{Type: "kafka", KafkaCluster: "lkc-abc"},
	}
	op := makeRoleBindingOp(operations.RemoveRoleBinding, rb)

	// Without prune consent: PruneDisabled, MDS not called.
	mds1 := mdsmock.New()
	r1 := Apply(context.Background(),
		Clients{Kafka: mock.New(nil, nil), MDS: mds1},
		[]operations.Operation{op},
		Approvals{})
	require.Len(t, r1.Results, 1)
	require.Equal(t, PruneDisabled, r1.Results[0].Status)
	require.Empty(t, mds1.Calls())

	// With run-wide prune consent: Succeeded, MDS called.
	mds2 := mdsmock.New()
	r2 := Apply(context.Background(),
		Clients{Kafka: mock.New(nil, nil), MDS: mds2},
		[]operations.Operation{op},
		Approvals{Prune: true})
	require.Len(t, r2.Results, 1)
	require.Equal(t, Succeeded, r2.Results[0].Status)
	calls := mds2.Calls()
	require.Len(t, calls, 1)
	require.Contains(t, calls[0], "RemoveRoleBinding")
}

// TestApplyNilMDSRoleBindingOpFails verifies that a role-binding op with a nil
// MDS client is recorded as Failed (not panicking), mirroring the nil-Schema
// handling for schema ops.
func TestApplyNilMDSRoleBindingOpFails(t *testing.T) {
	rb := rbac.RoleBinding{
		Principal: "User:alice",
		Role:      "DeveloperRead",
		Scope:     rbac.Scope{Type: "kafka", KafkaCluster: "lkc-abc"},
	}
	op := makeRoleBindingOp(operations.AddRoleBinding, rb)

	// MDS is nil (not set in Clients).
	res := Apply(context.Background(),
		Clients{Kafka: mock.New(nil, nil)},
		[]operations.Operation{op},
		Approvals{})

	require.Len(t, res.Results, 1)
	require.Equal(t, Failed, res.Results[0].Status)
	require.Equal(t, "MDS not configured", res.Results[0].Err)
	require.False(t, res.OK())
}

// TestToMDS verifies the rbac→mds converter for both cluster-scoped (nil
// Resource) and resource-scoped bindings.
func TestToMDS(t *testing.T) {
	// Resource-scoped binding.
	rb := rbac.RoleBinding{
		Principal: "User:alice",
		Role:      "DeveloperRead",
		Scope:     rbac.Scope{Type: "kafka", KafkaCluster: "lkc-abc", SubCluster: ""},
		Resource: &rbac.ResourcePattern{
			Type:        "Topic",
			Name:        "payments.orders",
			PatternType: "literal",
		},
	}
	got := toMDS(rb)
	require.Equal(t, "User:alice", got.Principal)
	require.Equal(t, "DeveloperRead", got.Role)
	require.Equal(t, "kafka", got.Scope.Type)
	require.Equal(t, "lkc-abc", got.Scope.KafkaCluster)
	require.Equal(t, "", got.Scope.SubCluster)
	require.NotNil(t, got.Resource)
	require.Equal(t, "Topic", got.Resource.Type)
	require.Equal(t, "payments.orders", got.Resource.Name)
	require.Equal(t, "literal", got.Resource.PatternType)

	// Cluster-scoped binding (nil Resource).
	rbCluster := rbac.RoleBinding{
		Principal: "User:bob",
		Role:      "ClusterAdmin",
		Scope:     rbac.Scope{Type: "schema-registry", KafkaCluster: "lkc-xyz", SubCluster: "sr-001"},
	}
	gotCluster := toMDS(rbCluster)
	require.Equal(t, "User:bob", gotCluster.Principal)
	require.Equal(t, "ClusterAdmin", gotCluster.Role)
	require.Equal(t, "schema-registry", gotCluster.Scope.Type)
	require.Equal(t, "lkc-xyz", gotCluster.Scope.KafkaCluster)
	require.Equal(t, "sr-001", gotCluster.Scope.SubCluster)
	require.Nil(t, gotCluster.Resource)
}
