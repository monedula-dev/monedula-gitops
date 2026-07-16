package diff

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/monedula-dev/monedula-gitops/internal/access"
	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	"github.com/monedula-dev/monedula-gitops/internal/operations"
)

func TestTopicOpsCarryMode(t *testing.T) {
	// One absent topic (CreateTopic) and one drifting topic (config + partition
	// increase + RF change), both DetectOnly: every emitted op carries the mode.
	desired := Desired{Topics: []DesiredTopic{
		{Kind: "KafkaTopic", Namespace: "ns", Name: "new", Partitions: 3, Mode: operations.ModeDetectOnly},
		{Kind: "KafkaTopic", Namespace: "ns", Name: "drift", Partitions: 6, ReplicationFactor: 3,
			Config: map[string]string{"retention.ms": "2"}, Mode: operations.ModeDetectOnly},
	}}
	live := Live{Topics: []kafka.TopicState{
		{Name: "drift", Partitions: 3, ReplicationFactor: 2, Config: map[string]string{"retention.ms": "1"}},
	}}
	ops := Compute(desired, live)
	require.NotEmpty(t, ops)
	for _, op := range ops {
		require.Equal(t, operations.ModeDetectOnly, op.Mode, "op %s %s must carry the owning topic's mode", op.Action, op.Target)
	}
}

func TestRejectedOpCarriesMode(t *testing.T) {
	desired := Desired{Topics: []DesiredTopic{
		{Kind: "KafkaTopic", Namespace: "ns", Name: "t", Partitions: 1, Mode: operations.ModeObserveOnly},
	}}
	live := Live{Topics: []kafka.TopicState{{Name: "t", Partitions: 3}}}
	ops := Compute(desired, live)
	rej := findOp(ops, operations.Rejected)
	require.NotNil(t, rej)
	require.Equal(t, operations.ModeObserveOnly, rej.Mode)
}

func TestSchemaOpsInheritOwningTopicMode(t *testing.T) {
	desired := Desired{Schemas: []DesiredSchema{{
		Subject:       "t-value",
		Topic:         "t",
		Type:          "AVRO",
		Definition:    `{"type":"record","name":"R","fields":[]}`,
		Compatibility: "BACKWARD",
		Mode:          operations.ModeObserveOnly,
	}}}
	ops := Compute(desired, Live{})
	sch := schemaOps(ops)
	require.Len(t, sch, 2) // RegisterSchema + RaiseSchemaCompatibility
	for _, op := range sch {
		require.Equal(t, operations.ModeObserveOnly, op.Mode)
	}
}

func TestCreateAclOpCarriesModeAndOwner(t *testing.T) {
	a := access.ACL{
		Principal: "User:x", Host: "*", ResourceType: "topic", ResourceName: "t",
		PatternType: "literal", Operation: "Write", Permission: "Allow",
		Mode: operations.ModeDetectOnly, SourceKind: "KafkaTopic", SourceNamespace: "payments", SourceName: "orders",
	}
	desired := Desired{ACLs: []access.ACL{a}, Scope: access.BuildScope([]access.ACL{a})}
	ops := Compute(desired, Live{})
	op := findOp(ops, operations.CreateAcl)
	require.NotNil(t, op)
	require.Equal(t, operations.ModeDetectOnly, op.Mode)
	// Owner attribution (review M5, spec §17.5 example): the op carries the
	// owning resource's kind/namespace/name, not the placeholder "ACL".
	require.Equal(t, "KafkaTopic", op.Kind)
	require.Equal(t, "payments", op.Namespace)
	require.Equal(t, "orders", op.Name)
}

func TestCreateAclOpWithoutAttributionKeepsACLKind(t *testing.T) {
	// Operator-path compatibility: ACLs without Source*/Mode keep Kind "ACL"
	// and an empty mode (executor treats empty as Enforce).
	a := access.ACL{Principal: "User:x", Host: "*", ResourceType: "topic", ResourceName: "t",
		PatternType: "literal", Operation: "Write", Permission: "Allow"}
	desired := Desired{ACLs: []access.ACL{a}, Scope: access.BuildScope([]access.ACL{a})}
	ops := Compute(desired, Live{})
	op := findOp(ops, operations.CreateAcl)
	require.NotNil(t, op)
	require.Equal(t, "ACL", op.Kind)
	require.Empty(t, op.Mode)
}

func TestPruneAclModeFromCoveringScope(t *testing.T) {
	// A live ACL within managed scope but not desired: the DeleteAcl op takes
	// the mode (and owner) of the resource whose scope covers the tuple.
	desiredACL := access.ACL{
		Principal: "User:x", Host: "*", ResourceType: "topic", ResourceName: "t",
		PatternType: "literal", Operation: "Read", Permission: "Allow",
		Mode: operations.ModeObserveOnly, SourceKind: "KafkaAccessPolicy", SourceNamespace: "billing", SourceName: "policy",
	}
	liveACL := access.ACL{
		Principal: "User:x", Host: "*", ResourceType: "topic", ResourceName: "t",
		PatternType: "literal", Operation: "Write", Permission: "Allow",
	}
	desired := Desired{ACLs: []access.ACL{desiredACL}, Scope: access.BuildScope([]access.ACL{desiredACL})}
	ops := Compute(desired, Live{ACLs: []access.ACL{desiredACL, liveACL}})
	op := findOp(ops, operations.DeleteAcl)
	require.NotNil(t, op)
	require.Equal(t, operations.ModeObserveOnly, op.Mode)
	require.Equal(t, "KafkaAccessPolicy", op.Kind)
	require.Equal(t, "billing", op.Namespace)
	require.Equal(t, "policy", op.Name)
}

func TestPruneAclWithoutAttributionStaysEnforceable(t *testing.T) {
	// Operator-path compatibility: a scope built from unattributed ACLs yields
	// prune ops with an empty mode (executed by the executor) and Kind "ACL".
	desiredACL := access.ACL{Principal: "User:x", Host: "*", ResourceType: "topic", ResourceName: "t",
		PatternType: "literal", Operation: "Read", Permission: "Allow"}
	liveACL := access.ACL{Principal: "User:x", Host: "*", ResourceType: "topic", ResourceName: "t",
		PatternType: "literal", Operation: "Write", Permission: "Allow"}
	desired := Desired{ACLs: []access.ACL{desiredACL}, Scope: access.BuildScope([]access.ACL{desiredACL})}
	ops := Compute(desired, Live{ACLs: []access.ACL{desiredACL, liveACL}})
	op := findOp(ops, operations.DeleteAcl)
	require.NotNil(t, op)
	require.Empty(t, op.Mode)
	require.Equal(t, "ACL", op.Kind)
}
