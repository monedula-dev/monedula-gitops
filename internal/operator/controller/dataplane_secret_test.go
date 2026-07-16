package controller

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
)

// newDataPlaneSecretClient is an alias of newSecretTestClient (a fake client
// over v1alpha1+corev1 with the ClusterSecretNamesIndex registered), named for
// readability at the data-plane fan-out call sites.
func newDataPlaneSecretClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	return newSecretTestClient(t, objs...)
}

func TestMapSecretToDataPlane(t *testing.T) {
	ns := "team-a"
	cluster := clusterWithSecret(ns, "prod", "kafka-creds")
	topic := &v1alpha1.KafkaTopic{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "orders"},
		Spec:       v1alpha1.KafkaTopicSpec{ClusterRef: v1alpha1.ClusterRef{Name: "prod"}, Partitions: 1},
	}
	otherTopic := &v1alpha1.KafkaTopic{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "elsewhere"},
		Spec:       v1alpha1.KafkaTopicSpec{ClusterRef: v1alpha1.ClusterRef{Name: "staging"}, Partitions: 1},
	}
	policy := &v1alpha1.KafkaAccessPolicy{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "billing"},
		Spec:       v1alpha1.KafkaAccessPolicySpec{ClusterRef: v1alpha1.ClusterRef{Name: "prod"}},
	}
	quota := &v1alpha1.KafkaQuota{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "q1"},
		Spec:       v1alpha1.KafkaQuotaSpec{ClusterRef: v1alpha1.ClusterRef{Name: "prod"}},
	}
	rb := &v1alpha1.KafkaRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "rb1"},
		Spec:       v1alpha1.KafkaRoleBindingSpec{ClusterRef: v1alpha1.ClusterRef{Name: "prod"}},
	}
	c := newDataPlaneSecretClient(t, cluster, topic, otherTopic, policy, quota, rb)
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "kafka-creds"}}
	ctx := context.Background()

	if got := (&KafkaTopicReconciler{Client: c}).mapSecretToTopics(ctx, secret); len(got) != 1 || got[0].Name != "orders" {
		t.Errorf("mapSecretToTopics = %+v, want [orders]", got)
	}
	if got := (&KafkaAccessPolicyReconciler{Client: c}).mapSecretToPolicies(ctx, secret); len(got) != 1 || got[0].Name != "billing" {
		t.Errorf("mapSecretToPolicies = %+v, want [billing]", got)
	}
	if got := (&KafkaQuotaReconciler{Client: c}).mapSecretToQuotas(ctx, secret); len(got) != 1 || got[0].Name != "q1" {
		t.Errorf("mapSecretToQuotas = %+v, want [q1]", got)
	}
	if got := (&KafkaRoleBindingReconciler{Client: c}).mapSecretToRoleBindings(ctx, secret); len(got) != 1 || got[0].Name != "rb1" {
		t.Errorf("mapSecretToRoleBindings = %+v, want [rb1]", got)
	}
}

func TestMapSecretToDataPlaneNoMatch(t *testing.T) {
	ns := "team-a"
	c := newDataPlaneSecretClient(t, clusterWithSecret(ns, "prod", "kafka-creds"))
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "unrelated"}}
	if got := (&KafkaTopicReconciler{Client: c}).mapSecretToTopics(context.Background(), secret); len(got) != 0 {
		t.Errorf("mapSecretToTopics = %+v, want none", got)
	}
}
