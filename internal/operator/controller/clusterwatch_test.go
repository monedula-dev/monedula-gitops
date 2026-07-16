package controller

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	operatorwebhook "github.com/monedula-dev/monedula-gitops/internal/operator/webhook"
)

// newClusterWatchClient builds a fake client seeded with the per-type
// spec.clusterRef.name field indexes (same extractors as
// operatorwebhook.RegisterIndexes) and the supplied objects.
func newClusterWatchClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	s := topicScheme(t)
	return fake.NewClientBuilder().
		WithScheme(s).
		WithIndex(&v1alpha1.KafkaTopic{}, operatorwebhook.ClusterRefNameIndex, func(o client.Object) []string {
			return []string{o.(*v1alpha1.KafkaTopic).Spec.ClusterRef.Name}
		}).
		WithIndex(&v1alpha1.KafkaRoleBinding{}, operatorwebhook.RoleBindingClusterRefNameIndex, func(o client.Object) []string {
			return []string{o.(*v1alpha1.KafkaRoleBinding).Spec.ClusterRef.Name}
		}).
		WithIndex(&v1alpha1.KafkaAccessPolicy{}, operatorwebhook.PolicyClusterRefNameIndex, func(o client.Object) []string {
			return []string{o.(*v1alpha1.KafkaAccessPolicy).Spec.ClusterRef.Name}
		}).
		WithIndex(&v1alpha1.KafkaQuota{}, operatorwebhook.QuotaClusterRefNameIndex, func(o client.Object) []string {
			return []string{o.(*v1alpha1.KafkaQuota).Spec.ClusterRef.Name}
		}).
		WithObjects(objs...).
		Build()
}

// refTopic returns a minimal KafkaTopic in ns referencing clusterRef.
func refTopic(ns, name, clusterRef string) *v1alpha1.KafkaTopic {
	return &v1alpha1.KafkaTopic{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec: v1alpha1.KafkaTopicSpec{
			ClusterRef: v1alpha1.ClusterRef{Name: clusterRef},
			TopicName:  name,
			Partitions: 1,
		},
	}
}

// assertRequests compares got against want order-independently.
func assertRequests(t *testing.T, got []types.NamespacedName, want ...types.NamespacedName) {
	t.Helper()
	wantSet := make(map[types.NamespacedName]bool, len(want))
	for _, w := range want {
		wantSet[w] = true
	}
	if len(got) != len(want) {
		t.Fatalf("got %d requests %v, want %d %v", len(got), got, len(want), want)
	}
	for _, g := range got {
		if !wantSet[g] {
			t.Errorf("unexpected request %v", g)
		}
		delete(wantSet, g)
	}
	for w := range wantSet {
		t.Errorf("missing expected request %v", w)
	}
}

// TestMapClusterToTopics_NamespaceLocal: in namespace-local mode a KafkaCluster
// event in ns1 maps to topics in ns1 referencing it — not topics in other
// namespaces and not topics referencing another cluster.
func TestMapClusterToTopics_NamespaceLocal(t *testing.T) {
	cl := newClusterWatchClient(t,
		refTopic("ns1", "topic-a", "prod"),
		refTopic("ns1", "topic-b", "other"),
		refTopic("ns2", "topic-c", "prod"),
	)
	r := &KafkaTopicReconciler{Client: cl, Scheme: topicScheme(t)}

	cluster := &v1alpha1.KafkaCluster{ObjectMeta: metav1.ObjectMeta{Namespace: "ns1", Name: "prod"}}
	got := r.mapClusterToTopics(context.Background(), cluster)

	var names []types.NamespacedName
	for _, g := range got {
		names = append(names, g.NamespacedName)
	}
	assertRequests(t, names, types.NamespacedName{Namespace: "ns1", Name: "topic-a"})
}

// TestMapClusterToTopics_ClusterNamespaceMode: with --cluster-namespace=infra,
// a KafkaCluster event in infra fans out to referencing topics in ALL
// namespaces; an event for a same-named cluster in any other namespace maps to
// nothing (dependents never resolve it).
func TestMapClusterToTopics_ClusterNamespaceMode(t *testing.T) {
	cl := newClusterWatchClient(t,
		refTopic("ns1", "topic-a", "prod"),
		refTopic("ns2", "topic-b", "prod"),
		refTopic("ns2", "topic-c", "other"),
	)
	r := &KafkaTopicReconciler{Client: cl, Scheme: topicScheme(t), ClusterNamespace: "infra"}

	infraCluster := &v1alpha1.KafkaCluster{ObjectMeta: metav1.ObjectMeta{Namespace: "infra", Name: "prod"}}
	got := r.mapClusterToTopics(context.Background(), infraCluster)
	var names []types.NamespacedName
	for _, g := range got {
		names = append(names, g.NamespacedName)
	}
	assertRequests(t, names,
		types.NamespacedName{Namespace: "ns1", Name: "topic-a"},
		types.NamespacedName{Namespace: "ns2", Name: "topic-b"},
	)

	elsewhereCluster := &v1alpha1.KafkaCluster{ObjectMeta: metav1.ObjectMeta{Namespace: "ns1", Name: "prod"}}
	if got := r.mapClusterToTopics(context.Background(), elsewhereCluster); len(got) != 0 {
		t.Fatalf("cluster outside --cluster-namespace must map to nothing, got %v", got)
	}
}

// TestMapClusterToRoleBindings_NamespaceLocal covers the shared core through a
// second list type (the MDSNotConfigured un-wedge path).
func TestMapClusterToRoleBindings_NamespaceLocal(t *testing.T) {
	rb := &v1alpha1.KafkaRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns1", Name: "rb-a"},
		Spec: v1alpha1.KafkaRoleBindingSpec{
			ClusterRef: v1alpha1.ClusterRef{Name: "prod"},
			Principal:  "User:alice",
			Role:       "SystemAdmin",
			Scope:      v1alpha1.RoleBindingScope{Type: "kafka"},
		},
	}
	cl := newClusterWatchClient(t, rb)
	r := &KafkaRoleBindingReconciler{Client: cl, Scheme: topicScheme(t)}

	cluster := &v1alpha1.KafkaCluster{ObjectMeta: metav1.ObjectMeta{Namespace: "ns1", Name: "prod"}}
	got := r.mapClusterToRoleBindings(context.Background(), cluster)
	var names []types.NamespacedName
	for _, g := range got {
		names = append(names, g.NamespacedName)
	}
	assertRequests(t, names, types.NamespacedName{Namespace: "ns1", Name: "rb-a"})
}

// TestMapClusterToPolicies_NamespaceLocal covers the shared core through a
// third list type (KafkaAccessPolicy), pinning that mapClusterToPolicies pairs
// KafkaAccessPolicyList with PolicyClusterRefNameIndex — a copy-paste swap
// between controllers (e.g. reusing QuotaClusterRefNameIndex here) would
// return zero requests against a fake client that only has the correct index
// registered, or panic on the list-type assertion, and this test would catch
// either.
func TestMapClusterToPolicies_NamespaceLocal(t *testing.T) {
	matching := &v1alpha1.KafkaAccessPolicy{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns1", Name: "policy-a"},
		Spec: v1alpha1.KafkaAccessPolicySpec{
			ClusterRef: v1alpha1.ClusterRef{Name: "prod"},
			Rules: []v1alpha1.ACLRule{{
				Principal:  "User:alice",
				Permission: "Allow",
				Resource:   v1alpha1.ACLResource{Type: "topic", Name: "orders", PatternType: "literal"},
				Operations: []string{"Read"},
			}},
		},
	}
	nonMatching := &v1alpha1.KafkaAccessPolicy{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns1", Name: "policy-b"},
		Spec: v1alpha1.KafkaAccessPolicySpec{
			ClusterRef: v1alpha1.ClusterRef{Name: "other"},
			Rules: []v1alpha1.ACLRule{{
				Principal:  "User:bob",
				Permission: "Allow",
				Resource:   v1alpha1.ACLResource{Type: "topic", Name: "payments", PatternType: "literal"},
				Operations: []string{"Read"},
			}},
		},
	}
	cl := newClusterWatchClient(t, matching, nonMatching)
	r := &KafkaAccessPolicyReconciler{Client: cl, Scheme: topicScheme(t)}

	cluster := &v1alpha1.KafkaCluster{ObjectMeta: metav1.ObjectMeta{Namespace: "ns1", Name: "prod"}}
	got := r.mapClusterToPolicies(context.Background(), cluster)
	var names []types.NamespacedName
	for _, g := range got {
		names = append(names, g.NamespacedName)
	}
	assertRequests(t, names, types.NamespacedName{Namespace: "ns1", Name: "policy-a"})
}

// TestMapClusterToQuotas_NamespaceLocal covers the shared core through a
// fourth list type (KafkaQuota), pinning that mapClusterToQuotas pairs
// KafkaQuotaList with QuotaClusterRefNameIndex.
func TestMapClusterToQuotas_NamespaceLocal(t *testing.T) {
	rate := 1024.0
	matching := &v1alpha1.KafkaQuota{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns1", Name: "quota-a"},
		Spec: v1alpha1.KafkaQuotaSpec{
			ClusterRef: v1alpha1.ClusterRef{Name: "prod"},
			Entity:     v1alpha1.QuotaEntity{User: "User:alice"},
			Limits:     v1alpha1.QuotaLimits{ProducerByteRate: &rate},
		},
	}
	nonMatching := &v1alpha1.KafkaQuota{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns1", Name: "quota-b"},
		Spec: v1alpha1.KafkaQuotaSpec{
			ClusterRef: v1alpha1.ClusterRef{Name: "other"},
			Entity:     v1alpha1.QuotaEntity{User: "User:bob"},
			Limits:     v1alpha1.QuotaLimits{ProducerByteRate: &rate},
		},
	}
	cl := newClusterWatchClient(t, matching, nonMatching)
	r := &KafkaQuotaReconciler{Client: cl, Scheme: topicScheme(t)}

	cluster := &v1alpha1.KafkaCluster{ObjectMeta: metav1.ObjectMeta{Namespace: "ns1", Name: "prod"}}
	got := r.mapClusterToQuotas(context.Background(), cluster)
	var names []types.NamespacedName
	for _, g := range got {
		names = append(names, g.NamespacedName)
	}
	assertRequests(t, names, types.NamespacedName{Namespace: "ns1", Name: "quota-a"})
}

// TestMapClusterToDependents_NonCluster: a non-KafkaCluster object maps to
// nothing (defensive; the watch source only delivers KafkaClusters).
func TestMapClusterToDependents_NonCluster(t *testing.T) {
	cl := newClusterWatchClient(t, refTopic("ns1", "topic-a", "prod"))
	r := &KafkaTopicReconciler{Client: cl, Scheme: topicScheme(t)}
	if got := r.mapClusterToTopics(context.Background(), refTopic("ns1", "not-a-cluster", "prod")); got != nil {
		t.Fatalf("non-cluster object must map to nil, got %v", got)
	}
}

// TestClusterWatchPredicate pins the fan-out shape (review I2): Create and
// Delete always pass (topic-before-cluster / prompt cluster-missing
// transitions); Updates pass ONLY on a generation change (spec write), so the
// KafkaCluster controller's own status writes never storm dependents.
func TestClusterWatchPredicate(t *testing.T) {
	p := clusterWatchPredicate()

	oldC := &v1alpha1.KafkaCluster{ObjectMeta: metav1.ObjectMeta{Namespace: "ns1", Name: "prod", Generation: 1}}
	specChanged := &v1alpha1.KafkaCluster{ObjectMeta: metav1.ObjectMeta{Namespace: "ns1", Name: "prod", Generation: 2}}
	statusOnly := oldC.DeepCopy() // same generation: status/annotation-only write

	if !p.Create(event.CreateEvent{Object: oldC}) {
		t.Error("Create must pass (topic-before-cluster recovery)")
	}
	if !p.Delete(event.DeleteEvent{Object: oldC}) {
		t.Error("Delete must pass (prompt cluster-missing condition)")
	}
	if !p.Update(event.UpdateEvent{ObjectOld: oldC, ObjectNew: specChanged}) {
		t.Error("generation-changed Update must pass (cluster spec fix)")
	}
	if p.Update(event.UpdateEvent{ObjectOld: oldC, ObjectNew: statusOnly}) {
		t.Error("status-only Update must NOT pass (no dependent storm)")
	}
}
