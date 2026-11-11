package diff

import (
	"errors"
	"testing"

	"github.com/monedula-dev/monedula-gitops/internal/access"
	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	"github.com/monedula-dev/monedula-gitops/internal/mirror"
	"github.com/monedula-dev/monedula-gitops/internal/operations"
	"github.com/monedula-dev/monedula-gitops/internal/schemaregistry"
	"github.com/stretchr/testify/require"
)

func guardDesired() Desired {
	return Desired{
		Topics: []DesiredTopic{
			{Kind: "KafkaTopic", Namespace: "payments", Name: "orders", Partitions: 3, ReplicationFactor: 3,
				Config: map[string]string{"retention.ms": "1000"}, Mode: operations.ModeEnforce},
			{Kind: "KafkaTopic", Namespace: "payments", Name: "plain", Partitions: 3,
				Config: map[string]string{"retention.ms": "1000"}, Mode: operations.ModeEnforce},
		},
		ACLs: []access.ACL{
			{Principal: "User:a", Host: "*", ResourceType: "topic", ResourceName: "orders", PatternType: "literal", Operation: "Read", Permission: "Allow"},
			{Principal: "User:a", Host: "*", ResourceType: "topic", ResourceName: "plain", PatternType: "literal", Operation: "Read", Permission: "Allow"},
			{Principal: "User:a", Host: "*", ResourceType: "topic", ResourceName: "orders", PatternType: "prefixed", Operation: "Write", Permission: "Allow"},
			{Principal: "User:a", Host: "*", ResourceType: "topic", ResourceName: "ord", PatternType: "prefixed", Operation: "Describe", Permission: "Allow"},
			{Principal: "User:a", Host: "*", ResourceType: "group", ResourceName: "orders", PatternType: "literal", Operation: "Read", Permission: "Allow"},
		},
		Schemas: []DesiredSchema{
			{Subject: "orders-value", Topic: "orders", Type: "AVRO", Definition: `"string"`, Mode: operations.ModeEnforce},
			{Subject: "plain-value", Topic: "plain", Type: "AVRO", Definition: `"string"`, Mode: operations.ModeEnforce},
		},
	}
}

func guardLive(snap *mirror.Snapshot) Live {
	return Live{
		Topics: []kafka.TopicState{
			{Name: "orders", Partitions: 3, ReplicationFactor: 3, Config: map[string]string{"retention.ms": "5"}},
			{Name: "plain", Partitions: 3, Config: map[string]string{"retention.ms": "1000"}},
		},
		Schemas: []schemaregistry.SubjectState{},
		Mirrors: snap,
	}
}

func opsFor(ops []operations.Operation, a operations.Action, target string) []operations.Operation {
	var out []operations.Operation
	for _, op := range ops {
		if op.Action == a && op.Target == target {
			out = append(out, op)
		}
	}
	return out
}

func ordersTargetOps(ops []operations.Operation) []operations.Operation {
	var out []operations.Operation
	for _, op := range ops {
		switch op.Action {
		case operations.CreateAcl:
			if op.ACL != nil && op.ACL.ResourceType == "topic" && op.ACL.PatternType == "literal" && op.ACL.ResourceName == "orders" {
				out = append(out, op)
			}
		case operations.RegisterSchema:
			if op.Topic == "orders" {
				out = append(out, op)
			}
		default:
			if op.Target == "orders" {
				out = append(out, op)
			}
		}
	}
	return out
}

func TestTopicGuardRejectsLiveMirror(t *testing.T) {
	for _, status := range []string{"ACTIVE", "PENDING_STOPPED"} {
		t.Run(status, func(t *testing.T) {
			snap := mirror.NewSnapshot("c1", "c1",
				map[string]kafka.TopicState{"orders": {Name: "orders", Partitions: 3}},
				liveMirror(mtLink, "orders", "orders", status), activeLink(nil))
			ops := Compute(guardDesired(), guardLive(&snap))

			guarded := ordersTargetOps(ops)
			require.Len(t, guarded, 1)
			g := guarded[0]
			require.Equal(t, operations.Rejected, g.Action)
			require.Equal(t, "KafkaTopic", g.Kind)
			require.Equal(t, "payments", g.Namespace)
			require.Equal(t, operations.ModeEnforce, g.Mode)
			require.Contains(t, g.Message, `topic "orders" is a mirror topic on link "dr-link"; manage it with KafkaMirrorTopic`)

			// Other topic's ops and non-literal / non-topic ACLs are untouched.
			var acls int
			for _, op := range ops {
				if op.Action == operations.CreateAcl {
					acls++
				}
			}
			require.Equal(t, 4, acls) // plain literal, orders prefixed, ord prefixed, group orders
			require.Len(t, opsFor(ops, operations.RegisterSchema, "plain-value"), 1)
			require.Empty(t, opsFor(ops, operations.RegisterSchema, "orders-value"))
		})
	}
}

func TestTopicGuardStateReadError(t *testing.T) {
	snap := mirror.NewSnapshot("c1", "c1", nil, nil, nil)
	snap.Err = errors.New("boom")
	live := guardLive(&snap)
	live.Topics = live.Topics[1:] // orders absent: would otherwise CreateTopic
	ops := Compute(guardDesired(), live)

	require.Empty(t, opsFor(ops, operations.CreateTopic, "orders"))
	guarded := ordersTargetOps(ops)
	require.Len(t, guarded, 1)
	require.Equal(t, operations.StateReadError, guarded[0].Action)
	require.Equal(t, "KafkaTopic", guarded[0].Kind)
	// Every desired topic is unknown, so plain is guarded too.
	require.Empty(t, opsFor(ops, operations.UpdateTopicConfig, "plain"))
	require.Len(t, opsFor(ops, operations.StateReadError, "plain"), 1)
}

func TestTopicGuardStoppedFormerMirrorManaged(t *testing.T) {
	snap := mirror.NewSnapshot("c1", "c1",
		map[string]kafka.TopicState{"orders": {Name: "orders", Partitions: 3}},
		liveMirror(mtLink, "orders", "orders", "STOPPED"), activeLink(nil))
	ops := Compute(guardDesired(), guardLive(&snap))
	require.Len(t, opsFor(ops, operations.UpdateTopicConfig, "orders"), 1)
	for _, op := range ops {
		require.NotEqual(t, operations.Rejected, op.Action)
		require.NotEqual(t, operations.StateReadError, op.Action)
	}
}

func TestTopicGuardKeepsPolicyACLsForOtherTopics(t *testing.T) {
	snap := mirror.NewSnapshot("c1", "c1",
		map[string]kafka.TopicState{"orders": {Name: "orders", Partitions: 3}},
		liveMirror(mtLink, "orders", "orders", "ACTIVE"), activeLink(nil))
	ops := Compute(guardDesired(), guardLive(&snap))
	var names []string
	for _, op := range ops {
		if op.Action == operations.CreateAcl {
			names = append(names, op.ACL.ResourceType+"/"+op.ACL.PatternType+"/"+op.ACL.ResourceName)
		}
	}
	require.ElementsMatch(t, []string{"topic/literal/plain", "topic/prefixed/orders", "topic/prefixed/ord", "group/literal/orders"}, names)
}

func TestTopicGuardNilSnapshotUnchanged(t *testing.T) {
	ops := Compute(guardDesired(), guardLive(nil))
	var got []string
	for _, op := range ops {
		got = append(got, string(op.Action)+"|"+op.Target)
	}
	require.Equal(t, []string{
		"CreateAcl|User:a Describe topic:ord Allow",
		"CreateAcl|User:a Read group:orders Allow",
		"CreateAcl|User:a Read topic:orders Allow",
		"CreateAcl|User:a Read topic:plain Allow",
		"CreateAcl|User:a Write topic:orders Allow",
		"RegisterSchema|orders-value",
		"RegisterSchema|plain-value",
		"UpdateTopicConfig|orders",
	}, got)
}

func TestTopicGuardUnknownOnCompleteSnapshotNamesCause(t *testing.T) {
	for _, tc := range []struct {
		name, status string
		topics       map[string]kafka.TopicState
	}{
		{"unrecognized status", "WEIRD", map[string]kafka.TopicState{"orders": {Name: "orders", Partitions: 3}}},
		{"mirror without topic", "ACTIVE", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snap := mirror.NewSnapshot("c1", "c1", tc.topics,
				liveMirror(mtLink, "orders", "orders", tc.status), activeLink(nil))
			require.True(t, snap.Complete())
			ops := Compute(guardDesired(), guardLive(&snap))
			got := opsFor(ops, operations.StateReadError, "orders")
			require.Len(t, got, 1)
			require.Equal(t, `cannot read mirror state for topic "orders": mirror status "`+tc.status+
				`" unrecognized or Kafka/REST state inconsistent`, got[0].Message)
			require.NotContains(t, got[0].Message, "snapshot not read")
		})
	}
}

// A guarded topic's literal topic ACLs are not pruned either: a live in-scope
// ACL no longer desired would otherwise produce DeleteAcl on the mirror topic.
func TestTopicGuardSuppressesPruneOnGuardedTopic(t *testing.T) {
	snap := mirror.NewSnapshot("c1", "c1",
		map[string]kafka.TopicState{"orders": {Name: "orders", Partitions: 3}},
		liveMirror(mtLink, "orders", "orders", "ACTIVE"), activeLink(nil))
	d := guardDesired()
	for i := range d.ACLs {
		d.ACLs[i].Prune = true
	}
	d.Scope = access.BuildScope(d.ACLs)
	live := guardLive(&snap)
	stale := func(resource string) access.ACL {
		return access.ACL{Principal: "User:a", Host: "*", ResourceType: "topic", ResourceName: resource,
			PatternType: "literal", Operation: "Write", Permission: "Allow"}
	}
	live.ACLs = []access.ACL{stale("orders"), stale("plain")}

	// Control: without the guard both stale ACLs are prune candidates.
	unguardedLive := guardLive(nil)
	unguardedLive.ACLs = live.ACLs
	var control []string
	for _, op := range Compute(d, unguardedLive) {
		if op.Action == operations.DeleteAcl {
			control = append(control, op.ACL.ResourceName)
		}
	}
	require.Equal(t, []string{"orders", "plain"}, control)

	ops := Compute(d, live)
	var deleted []string
	for _, op := range ops {
		if op.Action == operations.DeleteAcl {
			deleted = append(deleted, op.ACL.ResourceName)
		}
	}
	require.Equal(t, []string{"plain"}, deleted, "guarded literal topic ACL must not be pruned")
}
