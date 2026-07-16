package webhook

import (
	"context"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
)

// newPolicyFakeClient builds a fake client seeded with objs and with both the
// ClusterRefNameIndex (KafkaTopic) and PolicyClusterRefNameIndex
// (KafkaAccessPolicy) field indexes registered, mirroring RegisterIndexes so
// the validator's MatchingFields List calls work hermetically.
func newPolicyFakeClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	return fake.NewClientBuilder().
		WithScheme(newScheme(t)).
		WithIndex(&v1alpha1.KafkaTopic{}, ClusterRefNameIndex, func(obj client.Object) []string {
			tp, ok := obj.(*v1alpha1.KafkaTopic)
			if !ok {
				return nil
			}
			return []string{tp.Spec.ClusterRef.Name}
		}).
		WithIndex(&v1alpha1.KafkaAccessPolicy{}, PolicyClusterRefNameIndex, func(obj client.Object) []string {
			p, ok := obj.(*v1alpha1.KafkaAccessPolicy)
			if !ok {
				return nil
			}
			return []string{p.Spec.ClusterRef.Name}
		}).
		WithObjects(objs...).
		Build()
}

// policy builds a minimal KafkaAccessPolicy for unit tests. permission defaults
// to "Allow" when empty.
func policy(uid, ns, name, clusterRef, principal, resType, resName, permission string, ops []string) *v1alpha1.KafkaAccessPolicy {
	if permission == "" {
		permission = "Allow"
	}
	return &v1alpha1.KafkaAccessPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, UID: types.UID(uid)},
		Spec: v1alpha1.KafkaAccessPolicySpec{
			ClusterRef: v1alpha1.ClusterRef{Name: clusterRef},
			Rules: []v1alpha1.ACLRule{{
				Principal:  principal,
				Permission: permission,
				Resource:   v1alpha1.ACLResource{Type: resType, Name: resName, PatternType: "literal"},
				Operations: ops,
			}},
		},
	}
}

// clusterObj builds a minimal KafkaCluster for use in the fake client.
func clusterObj(ns, name string) *v1alpha1.KafkaCluster {
	return &v1alpha1.KafkaCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec:       v1alpha1.KafkaClusterSpec{BootstrapServers: "localhost:9092"},
	}
}

// ---- shape tests ----

func TestPolicyValidateCreate_InvalidShape_Denied(t *testing.T) {
	v := &KafkaAccessPolicyValidator{Reader: newPolicyFakeClient(t)}
	// Empty rules -> shape error (at least one rule required).
	bad := &v1alpha1.KafkaAccessPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "bad-pol", Namespace: "default"},
		Spec:       v1alpha1.KafkaAccessPolicySpec{ClusterRef: v1alpha1.ClusterRef{Name: "prod"}},
	}
	_, err := v.ValidateCreate(context.Background(), bad)
	if err == nil {
		t.Fatal("expected rejection for empty-rules policy")
	}
	if !strings.Contains(err.Error(), "rules must not be empty") {
		t.Fatalf("expected shape error mentioning rules: %v", err)
	}
}

func TestPolicyValidateCreate_InvalidPermission_Denied(t *testing.T) {
	v := &KafkaAccessPolicyValidator{Reader: newPolicyFakeClient(t)}
	bad := policy("uid-bad", "default", "bad-perm", "prod", "User:alice", "topic", "orders", "INVALID", []string{"Read"})
	_, err := v.ValidateCreate(context.Background(), bad)
	if err == nil {
		t.Fatal("expected rejection for invalid permission")
	}
	if !strings.Contains(err.Error(), "invalid permission") {
		t.Fatalf("expected shape error mentioning permission: %v", err)
	}
}

func TestPolicyValidateCreate_ValidShape_ClusterMissing_Allowed(t *testing.T) {
	// No cluster object present: admission must not block on a lagging cluster.
	v := &KafkaAccessPolicyValidator{Reader: newPolicyFakeClient(t)}
	pol := policy("uid-a", "default", "pol-a", "prod", "User:alice", "topic", "orders", "Allow", []string{"Read"})
	if _, err := v.ValidateCreate(context.Background(), pol); err != nil {
		t.Fatalf("expected allow when cluster is missing: %v", err)
	}
}

// ---- conflict tests ----

func TestPolicyValidateCreate_ConflictsWithExistingPolicy_Denied(t *testing.T) {
	cl := clusterObj("default", "prod")
	// Existing policy: allows User:alice Read on topic "orders".
	existing := policy("uid-a", "default", "pol-allow", "prod", "User:alice", "topic", "orders", "Allow", []string{"Read"})

	v := &KafkaAccessPolicyValidator{Reader: newPolicyFakeClient(t, cl, existing)}

	// Incoming policy denies the same tuple -> conflict.
	incoming := policy("uid-b", "default", "pol-deny", "prod", "User:alice", "topic", "orders", "Deny", []string{"Read"})
	_, err := v.ValidateCreate(context.Background(), incoming)
	if err == nil {
		t.Fatal("expected rejection: incoming policy conflicts with existing policy")
	}
	if !strings.Contains(err.Error(), "conflict") {
		t.Fatalf("expected conflict error: %v", err)
	}
	if !strings.Contains(err.Error(), "pol-allow") {
		t.Fatalf("error should name the other party (pol-allow): %v", err)
	}
}

func TestPolicyValidateCreate_ConflictsWithExistingTopic_Denied(t *testing.T) {
	cl := clusterObj("default", "prod")
	// Existing KafkaTopic that allows User:alice Read on topic "orders" (via
	// inline producer access). CompileTopic maps Producers to Allow ACLs.
	existingTopic := &v1alpha1.KafkaTopic{
		ObjectMeta: metav1.ObjectMeta{Name: "orders", Namespace: "default", UID: "uid-tp"},
		Spec: v1alpha1.KafkaTopicSpec{
			ClusterRef: v1alpha1.ClusterRef{Name: "prod"},
			TopicName:  "orders",
			Partitions: 1,
			Access: v1alpha1.TopicAccess{
				Producers: []v1alpha1.ProducerAccess{{
					Principal:  "User:alice",
					Operations: []string{"Read"},
				}},
			},
		},
	}

	v := &KafkaAccessPolicyValidator{Reader: newPolicyFakeClient(t, cl, existingTopic)}

	// Incoming policy denies the same tuple -> conflict with the topic.
	incoming := policy("uid-b", "default", "pol-deny", "prod", "User:alice", "topic", "orders", "Deny", []string{"Read"})
	_, err := v.ValidateCreate(context.Background(), incoming)
	if err == nil {
		t.Fatal("expected rejection: incoming policy conflicts with existing topic")
	}
	if !strings.Contains(err.Error(), "conflict") {
		t.Fatalf("expected conflict error: %v", err)
	}
	if !strings.Contains(err.Error(), "KafkaTopic") {
		t.Fatalf("error should name the other party as KafkaTopic: %v", err)
	}
}

func TestPolicyValidateCreate_NoConflict_Allowed(t *testing.T) {
	cl := clusterObj("default", "prod")
	// Existing policy on a different principal -> no conflict.
	existing := policy("uid-a", "default", "pol-bob", "prod", "User:bob", "topic", "orders", "Allow", []string{"Read"})

	v := &KafkaAccessPolicyValidator{Reader: newPolicyFakeClient(t, cl, existing)}

	incoming := policy("uid-b", "default", "pol-alice-deny", "prod", "User:alice", "topic", "orders", "Deny", []string{"Read"})
	if _, err := v.ValidateCreate(context.Background(), incoming); err != nil {
		t.Fatalf("expected allow: different principal, no conflict: %v", err)
	}
}

func TestPolicyValidateCreate_DifferentClusterRef_NoConflict(t *testing.T) {
	cl := clusterObj("default", "prod")
	// Existing policy on cluster "staging" — different clusterRef, no conflict with "prod".
	existing := policy("uid-a", "default", "pol-staging", "staging", "User:alice", "topic", "orders", "Allow", []string{"Read"})

	v := &KafkaAccessPolicyValidator{Reader: newPolicyFakeClient(t, cl, existing)}

	// No cluster "prod" yet, so the validator returns allow-on-cluster-not-found
	// but let's also verify it doesn't conflict with a staging policy.
	incoming := policy("uid-b", "default", "pol-deny", "prod", "User:alice", "topic", "orders", "Deny", []string{"Read"})
	if _, err := v.ValidateCreate(context.Background(), incoming); err != nil {
		t.Fatalf("expected allow: different cluster refs, no conflict: %v", err)
	}
}

func TestPolicyValidateUpdate_IntroducesConflict_Denied(t *testing.T) {
	cl := clusterObj("default", "prod")
	// Existing policy allows.
	existing := policy("uid-a", "default", "pol-allow", "prod", "User:alice", "topic", "orders", "Allow", []string{"Read"})
	// The policy being updated (stored version, no stored conflict).
	stored := policy("uid-b", "default", "pol-updating", "prod", "User:bob", "topic", "orders", "Allow", []string{"Read"})

	v := &KafkaAccessPolicyValidator{Reader: newPolicyFakeClient(t, cl, existing, stored)}

	// Updated policy now denies User:alice on the same tuple — conflict.
	updated := policy("uid-b", "default", "pol-updating", "prod", "User:alice", "topic", "orders", "Deny", []string{"Read"})
	_, err := v.ValidateUpdate(context.Background(), stored, updated)
	if err == nil {
		t.Fatal("expected rejection: update introduces conflict with pol-allow")
	}
	if !strings.Contains(err.Error(), "conflict") {
		t.Fatalf("expected conflict error: %v", err)
	}
}

func TestPolicyValidateUpdate_SelfUpdate_NoConflict_Allowed(t *testing.T) {
	cl := clusterObj("default", "prod")
	stored := policy("uid-a", "default", "pol-allow", "prod", "User:alice", "topic", "orders", "Allow", []string{"Read"})

	v := &KafkaAccessPolicyValidator{Reader: newPolicyFakeClient(t, cl, stored)}

	// Updated with same permission: the stored version is excluded from the view
	// and replaced by the incoming; no conflict with itself.
	updated := policy("uid-a", "default", "pol-allow", "prod", "User:alice", "topic", "orders", "Allow", []string{"Read", "Write"})
	if _, err := v.ValidateUpdate(context.Background(), stored, updated); err != nil {
		t.Fatalf("expected allow for self-update without conflict: %v", err)
	}
}

func TestPolicyValidateUpdate_ClusterRefChange_Denied(t *testing.T) {
	// Repointing the clusterRef orphans the ACLs applied on the previous
	// cluster; the webhook now mirrors the always-on CEL rule on
	// KafkaAccessPolicySpec.
	v := &KafkaAccessPolicyValidator{Reader: newPolicyFakeClient(t)}
	old := policy("uid-a", "team-a", "pol", "prod", "User:svc", "topic", "orders", "Allow", []string{"Read"})
	updated := policy("uid-a", "team-a", "pol", "staging", "User:svc", "topic", "orders", "Allow", []string{"Read"})

	_, err := v.ValidateUpdate(context.Background(), old, updated)
	if err == nil {
		t.Fatal("expected denial for clusterRef change")
	}
	if !strings.Contains(err.Error(), "spec.clusterRef.name is immutable") ||
		!strings.Contains(err.Error(), "prod") || !strings.Contains(err.Error(), "staging") {
		t.Fatalf("error should name the field and both cluster refs: %v", err)
	}
}

func TestPolicyValidateDelete_AlwaysAllowed(t *testing.T) {
	v := &KafkaAccessPolicyValidator{Reader: newPolicyFakeClient(t)}
	pol := policy("uid-a", "default", "pol-a", "prod", "User:alice", "topic", "orders", "Allow", []string{"Read"})
	if _, err := v.ValidateDelete(context.Background(), pol); err != nil {
		t.Fatalf("delete must always be allowed: %v", err)
	}
}
