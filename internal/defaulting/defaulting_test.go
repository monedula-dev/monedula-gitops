package defaulting

import (
	"testing"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestTopicDefaults(t *testing.T) {
	tp := &v1alpha1.KafkaTopic{ObjectMeta: metav1.ObjectMeta{Name: "orders"}}
	rf := 3
	Topic(tp, &v1alpha1.ClusterDefaults{ReplicationFactor: &rf})
	require.Equal(t, "orders", tp.Spec.TopicName)
	require.Equal(t, "Orphan", tp.Spec.DeletionPolicy)
	require.NotNil(t, tp.Spec.Reconciliation)
	require.Equal(t, "Enforce", tp.Spec.Reconciliation.Mode)
	require.NotNil(t, tp.Spec.ReplicationFactor)
	require.Equal(t, 3, *tp.Spec.ReplicationFactor)
}

func TestTopicKeepsExplicitValues(t *testing.T) {
	rf := 5
	tp := &v1alpha1.KafkaTopic{
		ObjectMeta: metav1.ObjectMeta{Name: "orders"},
		Spec: v1alpha1.KafkaTopicSpec{
			TopicName:         "payments.orders",
			DeletionPolicy:    "Delete",
			Reconciliation:    &v1alpha1.Reconciliation{Mode: "DetectOnly"},
			ReplicationFactor: &rf,
		},
	}
	clusterRF := 3
	Topic(tp, &v1alpha1.ClusterDefaults{ReplicationFactor: &clusterRF})
	require.Equal(t, "payments.orders", tp.Spec.TopicName)
	require.Equal(t, "Delete", tp.Spec.DeletionPolicy)
	require.Equal(t, "DetectOnly", tp.Spec.Reconciliation.Mode)
	require.Equal(t, 5, *tp.Spec.ReplicationFactor)
}

func TestTopicNilClusterDefaults(t *testing.T) {
	tp := &v1alpha1.KafkaTopic{ObjectMeta: metav1.ObjectMeta{Name: "orders"}}
	Topic(tp, nil)
	require.Equal(t, "orders", tp.Spec.TopicName)
	require.Nil(t, tp.Spec.ReplicationFactor) // no cluster default => stays nil
}

func TestPolicyRuleDefaults(t *testing.T) {
	pol := &v1alpha1.KafkaAccessPolicy{
		Spec: v1alpha1.KafkaAccessPolicySpec{
			Rules: []v1alpha1.ACLRule{{Principal: "User:x", Resource: v1alpha1.ACLResource{Type: "topic", Name: "t"}}},
		},
	}
	Policy(pol)
	require.Equal(t, "Delete", pol.Spec.DeletionPolicy)
	require.NotNil(t, pol.Spec.Reconciliation)
	require.Equal(t, "Enforce", pol.Spec.Reconciliation.Mode)
	require.Equal(t, "Allow", pol.Spec.Rules[0].Permission)
	require.Equal(t, "*", pol.Spec.Rules[0].Host)
	require.Equal(t, "literal", pol.Spec.Rules[0].Resource.PatternType)
}

func TestUserDefaults(t *testing.T) {
	u := &v1alpha1.KafkaUser{ObjectMeta: metav1.ObjectMeta{Name: "svc-checkout"}}
	User(u)
	require.Equal(t, "svc-checkout", u.Spec.Username)
	require.Equal(t, "SCRAM-SHA-512", u.Spec.Mechanism)
	require.Equal(t, "Delete", u.Spec.DeletionPolicy)
}

func TestUserKeepsExplicitValues(t *testing.T) {
	u := &v1alpha1.KafkaUser{
		ObjectMeta: metav1.ObjectMeta{Name: "svc-checkout"},
		Spec: v1alpha1.KafkaUserSpec{
			Username:       "explicit-user",
			Mechanism:      "SCRAM-SHA-256",
			DeletionPolicy: "Orphan",
		},
	}
	User(u)
	require.Equal(t, "explicit-user", u.Spec.Username)
	require.Equal(t, "SCRAM-SHA-256", u.Spec.Mechanism)
	require.Equal(t, "Orphan", u.Spec.DeletionPolicy)
}

func TestRoleBindingDefaults(t *testing.T) {
	rb := &v1alpha1.KafkaRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: "rb"}}
	RoleBinding(rb)
	require.Equal(t, "Delete", rb.Spec.DeletionPolicy)
}

func TestRoleBindingKeepsExplicitValues(t *testing.T) {
	rb := &v1alpha1.KafkaRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "rb"},
		Spec:       v1alpha1.KafkaRoleBindingSpec{DeletionPolicy: "Orphan"},
	}
	RoleBinding(rb)
	require.Equal(t, "Orphan", rb.Spec.DeletionPolicy)
}

func TestTopicClusterRFAppliedWhenNoPlacement(t *testing.T) {
	rf := 3
	tp := &v1alpha1.KafkaTopic{ObjectMeta: metav1.ObjectMeta{Name: "orders"}}
	Topic(tp, &v1alpha1.ClusterDefaults{ReplicationFactor: &rf})
	require.NotNil(t, tp.Spec.ReplicationFactor)
	require.Equal(t, 3, *tp.Spec.ReplicationFactor)
}

func TestTopicClusterRFSkippedWithPlacementConstraint(t *testing.T) {
	rf := 3
	tp := &v1alpha1.KafkaTopic{
		ObjectMeta: metav1.ObjectMeta{Name: "orders"},
		Spec: v1alpha1.KafkaTopicSpec{
			Config: map[string]string{"confluent.placement.constraints": `{"version":2}`},
		},
	}
	Topic(tp, &v1alpha1.ClusterDefaults{ReplicationFactor: &rf})
	// RF must stay unset so the placement constraint determines replication.
	require.Nil(t, tp.Spec.ReplicationFactor)
}

func TestTopicClusterDefaultDeletionPolicyApplied(t *testing.T) {
	// When the topic has no deletionPolicy and the cluster default is "Delete",
	// the cluster default must win over the hardcoded "Orphan" fallback.
	tp := &v1alpha1.KafkaTopic{ObjectMeta: metav1.ObjectMeta{Name: "orders"}}
	Topic(tp, &v1alpha1.ClusterDefaults{TopicDeletionPolicy: "Delete"})
	require.Equal(t, "Delete", tp.Spec.DeletionPolicy)
}

func TestTopicExplicitDeletionPolicySurvivesClusterDefault(t *testing.T) {
	// An explicit topic deletionPolicy must not be overwritten by the cluster
	// default in either direction.
	tp := &v1alpha1.KafkaTopic{
		ObjectMeta: metav1.ObjectMeta{Name: "orders"},
		Spec:       v1alpha1.KafkaTopicSpec{DeletionPolicy: "Orphan"},
	}
	Topic(tp, &v1alpha1.ClusterDefaults{TopicDeletionPolicy: "Delete"})
	require.Equal(t, "Orphan", tp.Spec.DeletionPolicy)

	tp2 := &v1alpha1.KafkaTopic{
		ObjectMeta: metav1.ObjectMeta{Name: "orders"},
		Spec:       v1alpha1.KafkaTopicSpec{DeletionPolicy: "Delete"},
	}
	Topic(tp2, &v1alpha1.ClusterDefaults{TopicDeletionPolicy: "Orphan"})
	require.Equal(t, "Delete", tp2.Spec.DeletionPolicy)
}

func TestTopicNilClusterDefaultsFallsBackToOrphan(t *testing.T) {
	// nil clusterDefaults must still produce the "Orphan" fallback.
	tp := &v1alpha1.KafkaTopic{ObjectMeta: metav1.ObjectMeta{Name: "orders"}}
	Topic(tp, nil)
	require.Equal(t, "Orphan", tp.Spec.DeletionPolicy)
}

func TestTopicEmptyClusterDefaultDeletionPolicyFallsBackToOrphan(t *testing.T) {
	// A ClusterDefaults with TopicDeletionPolicy unset still falls back to
	// "Orphan" — only a non-empty cluster default overrides the built-in.
	tp := &v1alpha1.KafkaTopic{ObjectMeta: metav1.ObjectMeta{Name: "orders"}}
	Topic(tp, &v1alpha1.ClusterDefaults{})
	require.Equal(t, "Orphan", tp.Spec.DeletionPolicy)
}
