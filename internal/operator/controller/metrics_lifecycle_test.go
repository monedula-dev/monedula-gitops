package controller

// Metrics lifecycle tests (review I12): per-CR series must be created when a CR
// is reconciled, kept fresh on error paths, and removed when the CR goes away —
// across the finalizer flow AND the post-delete NotFound reconcile (Delete
// watch events always pass the event filter; see predicates.go).
//
// The collectors are process-global (controller-runtime's crmetrics.Registry),
// so every test here uses a UNIQUE namespace/name to avoid colliding with other
// tests touching the same families.

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	kafkamock "github.com/monedula-dev/monedula-gitops/internal/kafka/mock"
	mdsmock "github.com/monedula-dev/monedula-gitops/internal/mds/mock"
	"github.com/monedula-dev/monedula-gitops/internal/operator"
)

// gaugeSeries gathers the global controller-runtime registry and returns the
// value of the series in family name whose labels contain all of want, plus
// whether such a series exists. A nil/empty want matches the first (e.g. only)
// series, which is how the label-less monedula_managed_topics is read.
func gaugeSeries(t *testing.T, name string, want map[string]string) (float64, bool) {
	t.Helper()
	mfs, err := crmetrics.Registry.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
	metric:
		for _, m := range mf.GetMetric() {
			labels := map[string]string{}
			for _, lp := range m.GetLabel() {
				labels[lp.GetName()] = lp.GetValue()
			}
			for k, v := range want {
				if labels[k] != v {
					continue metric
				}
			}
			return m.GetGauge().GetValue(), true
		}
	}
	return 0, false
}

// managedTopicsValue reads monedula_managed_topics (0 when never moved).
func managedTopicsValue(t *testing.T) float64 {
	t.Helper()
	v, _ := gaugeSeries(t, "monedula_managed_topics", nil)
	return v
}

// metricsTopic builds an Enforce-mode topic in the given (unique) namespace
// referencing a same-namespace cluster named clusterName.
func metricsTopic(ns, name, clusterName string) *v1alpha1.KafkaTopic {
	tp := &v1alpha1.KafkaTopic{
		Spec: v1alpha1.KafkaTopicSpec{
			ClusterRef:     v1alpha1.ClusterRef{Name: clusterName},
			TopicName:      name,
			Partitions:     3,
			Reconciliation: &v1alpha1.Reconciliation{Mode: "Enforce"},
		},
	}
	tp.Name = name
	tp.Namespace = ns
	tp.Generation = 1
	return tp
}

func metricsCluster(ns, name string) *v1alpha1.KafkaCluster {
	c := &v1alpha1.KafkaCluster{}
	c.Name = name
	c.Namespace = ns
	c.Generation = 1
	return c
}

func nsReq(ns, name string) ctrl.Request {
	return ctrl.Request{NamespacedName: client.ObjectKey{Namespace: ns, Name: name}}
}

// TestTopicMetrics_LifecycleOnDeletion pins bug 1 + 2 of review I12: the
// managed-topics gauge must DECREMENT when a topic is finalized (not count
// "ever seen"), and the per-CR drift series must be removed — including across
// the post-delete NotFound reconcile, without double-decrementing.
func TestTopicMetrics_LifecycleOnDeletion(t *testing.T) {
	const ns, name = "metrics-life-ns", "metrics-life-orders"
	s := topicScheme(t)
	tp := metricsTopic(ns, name, "prod")
	c := metricsCluster(ns, "prod")
	cl := newTopicFakeClient(t, s, tp, c)
	k := kafkamock.New(nil, nil)
	r := &KafkaTopicReconciler{Client: cl, Scheme: s, Clients: stubFactory{k: k}}

	base := managedTopicsValue(t)

	if _, err := r.Reconcile(context.Background(), nsReq(ns, name)); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got := managedTopicsValue(t); got != base+1 {
		t.Fatalf("managed_topics after first reconcile = %v want %v", got, base+1)
	}
	// Re-reconcile must not double-count.
	if _, err := r.Reconcile(context.Background(), nsReq(ns, name)); err != nil {
		t.Fatalf("Reconcile again: %v", err)
	}
	if got := managedTopicsValue(t); got != base+1 {
		t.Fatalf("managed_topics after re-reconcile = %v want %v (dedupe)", got, base+1)
	}
	if _, ok := gaugeSeries(t, "monedula_kafka_topic_drift_detected",
		map[string]string{"namespace": ns, "name": name}); !ok {
		t.Fatal("drift series missing after reconcile")
	}

	// Delete the CR (deletionPolicy defaults to Orphan): the fake client keeps
	// it alive via the finalizer; the next reconcile finalizes it.
	var live v1alpha1.KafkaTopic
	if err := cl.Get(context.Background(), nsReq(ns, name).NamespacedName, &live); err != nil {
		t.Fatalf("get for delete: %v", err)
	}
	if err := cl.Delete(context.Background(), &live); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := r.Reconcile(context.Background(), nsReq(ns, name)); err != nil {
		t.Fatalf("Reconcile deletion: %v", err)
	}

	if got := managedTopicsValue(t); got != base {
		t.Fatalf("managed_topics after finalization = %v want %v (must decrement)", got, base)
	}
	if _, ok := gaugeSeries(t, "monedula_kafka_topic_drift_detected",
		map[string]string{"namespace": ns, "name": name}); ok {
		t.Fatal("drift series still present after finalization (stale series)")
	}

	// The Delete watch event triggers one more reconcile that hits NotFound; it
	// must be a no-op for the gauge (no double-decrement).
	if _, err := r.Reconcile(context.Background(), nsReq(ns, name)); err != nil {
		t.Fatalf("Reconcile NotFound: %v", err)
	}
	if got := managedTopicsValue(t); got != base {
		t.Fatalf("managed_topics after NotFound reconcile = %v want %v (no double-decrement)", got, base)
	}
}

// TestTopicMetrics_DriftGaugeSetOnApplyError pins bug 2 of review I12: a
// reconcile that ends in a transient apply error STILL computed a fresh status
// (with drift detected), so the drift gauge must be updated — not left at its
// previous value.
func TestTopicMetrics_DriftGaugeSetOnApplyError(t *testing.T) {
	const ns, name = "metrics-err-ns", "metrics-err-orders"
	s := topicScheme(t)
	tp := metricsTopic(ns, name, "prod")
	c := metricsCluster(ns, "prod")
	cl := newTopicFakeClient(t, s, tp, c)
	k := kafkamock.New(nil, nil) // topic absent -> CreateTopic op
	k.FailOn("CreateTopic", name, errReason("broker exploded"))
	r := &KafkaTopicReconciler{Client: cl, Scheme: s, Clients: stubFactory{k: k}}

	if _, err := r.Reconcile(context.Background(), nsReq(ns, name)); err == nil {
		t.Fatal("expected transient apply error")
	}
	v, ok := gaugeSeries(t, "monedula_kafka_topic_drift_detected",
		map[string]string{"namespace": ns, "name": name})
	if !ok {
		t.Fatal("drift gauge not set on the error path (stale)")
	}
	if v != 1 {
		t.Fatalf("drift gauge on apply error = %v want 1", v)
	}
}

// TestTopicMetrics_NotFoundCleansSeries pins the safety net: a reconcile of an
// already-gone topic removes its drift series even if the finalizer-path
// cleanup was missed (e.g. operator restart between finalize and delete).
func TestTopicMetrics_NotFoundCleansSeries(t *testing.T) {
	const ns, name = "metrics-gone-ns", "metrics-gone-orders"
	operator.SetTopicDrift(ns, name, true) // simulate a series from a prior run

	s := topicScheme(t)
	cl := newTopicFakeClient(t, s)
	r := &KafkaTopicReconciler{Client: cl, Scheme: s, Clients: stubFactory{}}
	if _, err := r.Reconcile(context.Background(), nsReq(ns, name)); err != nil {
		t.Fatalf("Reconcile NotFound: %v", err)
	}
	if _, ok := gaugeSeries(t, "monedula_kafka_topic_drift_detected",
		map[string]string{"namespace": ns, "name": name}); ok {
		t.Fatal("drift series not removed by the NotFound reconcile")
	}
}

// metricsPolicy builds an Enforce-mode policy in the given (unique) namespace.
func metricsPolicy(ns, name string) *v1alpha1.KafkaAccessPolicy {
	pol := &v1alpha1.KafkaAccessPolicy{
		Spec: v1alpha1.KafkaAccessPolicySpec{
			ClusterRef: v1alpha1.ClusterRef{Name: "prod"},
			Rules: []v1alpha1.ACLRule{{
				Principal:  "User:svc-metrics",
				Permission: "Allow",
				Host:       "*",
				Resource:   v1alpha1.ACLResource{Type: "topic", Name: "metrics.orders", PatternType: "literal"},
				Operations: []string{"Read"},
			}},
			Reconciliation: &v1alpha1.Reconciliation{Mode: "Enforce"},
		},
	}
	pol.Name = name
	pol.Namespace = ns
	pol.Generation = 1
	return pol
}

// TestPolicyMetrics_DriftSeriesDeletedOnFinalize: the policy drift series must
// be removed when the policy is finalized (Orphan path), and the NotFound
// reconcile is the safety net for an already-gone policy.
func TestPolicyMetrics_DriftSeriesDeletedOnFinalize(t *testing.T) {
	const ns, name = "metrics-pol-ns", "metrics-pol-access"
	s := topicScheme(t)
	pol := metricsPolicy(ns, name)
	pol.Spec.DeletionPolicy = "Orphan"
	c := metricsCluster(ns, "prod")
	cl := newPolicyFakeClient(t, s, pol, c)
	k := kafkamock.New(nil, nil)
	r := &KafkaAccessPolicyReconciler{Client: cl, Scheme: s, Clients: stubFactory{k: k}}

	if _, err := r.Reconcile(context.Background(), nsReq(ns, name)); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if _, ok := gaugeSeries(t, "monedula_access_policy_drift_detected",
		map[string]string{"namespace": ns, "name": name}); !ok {
		t.Fatal("policy drift series missing after reconcile")
	}

	var live v1alpha1.KafkaAccessPolicy
	if err := cl.Get(context.Background(), nsReq(ns, name).NamespacedName, &live); err != nil {
		t.Fatalf("get for delete: %v", err)
	}
	if err := cl.Delete(context.Background(), &live); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := r.Reconcile(context.Background(), nsReq(ns, name)); err != nil {
		t.Fatalf("Reconcile deletion: %v", err)
	}
	if _, ok := gaugeSeries(t, "monedula_access_policy_drift_detected",
		map[string]string{"namespace": ns, "name": name}); ok {
		t.Fatal("policy drift series still present after finalization (stale series)")
	}

	// NotFound safety net for a series left behind by a prior process.
	operator.SetPolicyDrift(ns, name, true)
	if _, err := r.Reconcile(context.Background(), nsReq(ns, name)); err != nil {
		t.Fatalf("Reconcile NotFound: %v", err)
	}
	if _, ok := gaugeSeries(t, "monedula_access_policy_drift_detected",
		map[string]string{"namespace": ns, "name": name}); ok {
		t.Fatal("policy drift series not removed by the NotFound reconcile")
	}
}

// TestPolicyMetrics_DriftGaugeSetOnApplyError: like the topic case, a policy
// reconcile ending in a transient apply error must still update the drift
// gauge from the freshly computed status.
func TestPolicyMetrics_DriftGaugeSetOnApplyError(t *testing.T) {
	const ns, name = "metrics-polerr-ns", "metrics-polerr-access"
	s := topicScheme(t)
	pol := metricsPolicy(ns, name)
	c := metricsCluster(ns, "prod")
	cl := newPolicyFakeClient(t, s, pol, c)
	k := kafkamock.New(nil, nil) // ACLs absent -> CreateACLs op
	k.FailOn("CreateACLs", "1", errReason("broker exploded"))
	r := &KafkaAccessPolicyReconciler{Client: cl, Scheme: s, Clients: stubFactory{k: k}}

	if _, err := r.Reconcile(context.Background(), nsReq(ns, name)); err == nil {
		t.Fatal("expected transient apply error")
	}
	v, ok := gaugeSeries(t, "monedula_access_policy_drift_detected",
		map[string]string{"namespace": ns, "name": name})
	if !ok {
		t.Fatal("policy drift gauge not set on the error path (stale)")
	}
	if v != 1 {
		t.Fatalf("policy drift gauge on apply error = %v want 1", v)
	}
}

// TestClusterMetrics_NamespaceKeyedReachable pins bug 3 of review I12: two
// same-named KafkaClusters in different namespaces must produce two distinct
// reachability series (namespace+name keyed), not overwrite each other.
func TestClusterMetrics_NamespaceKeyedReachable(t *testing.T) {
	const nsA, nsB, name = "metrics-cl-ns-a", "metrics-cl-ns-b", "prod"
	s := newScheme(t)
	ca := metricsCluster(nsA, name)
	cb := metricsCluster(nsB, name)

	// nsA: reachable.
	clA := newFakeClient(t, s, ca)
	rA := &KafkaClusterReconciler{Client: clA, Scheme: s, Clients: stubFactory{k: kafkamock.New(nil, nil)}}
	if _, err := rA.Reconcile(context.Background(), nsReq(nsA, name)); err != nil {
		t.Fatalf("Reconcile nsA: %v", err)
	}
	// nsB: clients fail to build -> not reachable.
	clB := newFakeClient(t, s, cb)
	rB := &KafkaClusterReconciler{Client: clB, Scheme: s, Clients: stubFactory{err: errReason("unreachable")}}
	if _, err := rB.Reconcile(context.Background(), nsReq(nsB, name)); err == nil {
		t.Fatal("expected build error for nsB")
	}

	va, okA := gaugeSeries(t, "monedula_kafka_cluster_reachable",
		map[string]string{"namespace": nsA, "name": name})
	if !okA || va != 1 {
		t.Fatalf("reachable{%s,%s} = (%v, %v) want (1, true)", nsA, name, va, okA)
	}
	vb, okB := gaugeSeries(t, "monedula_kafka_cluster_reachable",
		map[string]string{"namespace": nsB, "name": name})
	if !okB || vb != 0 {
		t.Fatalf("reachable{%s,%s} = (%v, %v) want (0, true)", nsB, name, vb, okB)
	}
}

// TestClusterMetrics_NotFoundDeletesSeries: a KafkaCluster has no finalizer, so
// the post-delete NotFound reconcile (the Delete watch event passes the event
// filter) is where its reachability series is removed.
func TestClusterMetrics_NotFoundDeletesSeries(t *testing.T) {
	const ns, name = "metrics-clgone-ns", "prod"
	s := newScheme(t)
	c := metricsCluster(ns, name)
	cl := newFakeClient(t, s, c)
	r := &KafkaClusterReconciler{Client: cl, Scheme: s, Clients: stubFactory{k: kafkamock.New(nil, nil)}}

	if _, err := r.Reconcile(context.Background(), nsReq(ns, name)); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if _, ok := gaugeSeries(t, "monedula_kafka_cluster_reachable",
		map[string]string{"namespace": ns, "name": name}); !ok {
		t.Fatal("reachable series missing after reconcile")
	}

	if err := cl.Delete(context.Background(), c); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := r.Reconcile(context.Background(), nsReq(ns, name)); err != nil {
		t.Fatalf("Reconcile NotFound: %v", err)
	}
	if _, ok := gaugeSeries(t, "monedula_kafka_cluster_reachable",
		map[string]string{"namespace": ns, "name": name}); ok {
		t.Fatal("reachable series still present after the NotFound reconcile (stale series)")
	}
}

// --- KafkaQuota metrics lifecycle ---

// newQuotaFakeClient builds a fake client with the quota + cluster status
// subresources enabled.
func newQuotaFakeClient(t *testing.T, s *runtime.Scheme, objs ...client.Object) client.Client {
	t.Helper()
	return fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(objs...).
		WithStatusSubresource(&v1alpha1.KafkaQuota{}, &v1alpha1.KafkaCluster{}).
		Build()
}

func metricsQuota(ns, name string) *v1alpha1.KafkaQuota {
	q := &v1alpha1.KafkaQuota{
		Spec: v1alpha1.KafkaQuotaSpec{
			ClusterRef:     v1alpha1.ClusterRef{Name: "prod"},
			Entity:         v1alpha1.QuotaEntity{User: "User:alice"},
			Limits:         v1alpha1.QuotaLimits{ProducerByteRate: float64Ptr(1048576)},
			Reconciliation: &v1alpha1.Reconciliation{Mode: "Enforce"},
		},
	}
	q.Name = name
	q.Namespace = ns
	q.Generation = 1
	return q
}

func float64Ptr(f float64) *float64 { return &f }

// managedQuotasValue reads monedula_managed_quotas (0 when never moved).
func managedQuotasValue(t *testing.T) float64 {
	t.Helper()
	v, _ := gaugeSeries(t, "monedula_managed_quotas", nil)
	return v
}

// TestQuotaMetrics_LifecycleOnDeletion mirrors TestTopicMetrics_LifecycleOnDeletion:
// the managed-quotas gauge must increment on first reconcile, not double-count on
// re-reconcile, decrement on finalization, and the drift series must be removed
// on finalization and (safety net) on a post-delete NotFound reconcile.
func TestQuotaMetrics_LifecycleOnDeletion(t *testing.T) {
	const ns, name = "metrics-quota-ns", "metrics-quota-alice"
	s := topicScheme(t)
	q := metricsQuota(ns, name)
	c := metricsCluster(ns, "prod")
	cl := newQuotaFakeClient(t, s, q, c)
	k := kafkamock.New(nil, nil)
	r := &KafkaQuotaReconciler{Client: cl, Scheme: s, Clients: stubFactory{k: k}}

	base := managedQuotasValue(t)

	if _, err := r.Reconcile(context.Background(), nsReq(ns, name)); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got := managedQuotasValue(t); got != base+1 {
		t.Fatalf("managed_quotas after first reconcile = %v want %v", got, base+1)
	}
	if _, err := r.Reconcile(context.Background(), nsReq(ns, name)); err != nil {
		t.Fatalf("Reconcile again: %v", err)
	}
	if got := managedQuotasValue(t); got != base+1 {
		t.Fatalf("managed_quotas after re-reconcile = %v want %v (dedupe)", got, base+1)
	}
	if _, ok := gaugeSeries(t, "monedula_kafka_quota_drift_detected",
		map[string]string{"namespace": ns, "name": name}); !ok {
		t.Fatal("quota drift series missing after reconcile")
	}

	var live v1alpha1.KafkaQuota
	if err := cl.Get(context.Background(), nsReq(ns, name).NamespacedName, &live); err != nil {
		t.Fatalf("get for delete: %v", err)
	}
	if err := cl.Delete(context.Background(), &live); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := r.Reconcile(context.Background(), nsReq(ns, name)); err != nil {
		t.Fatalf("Reconcile deletion: %v", err)
	}

	if got := managedQuotasValue(t); got != base {
		t.Fatalf("managed_quotas after finalization = %v want %v (must decrement)", got, base)
	}
	if _, ok := gaugeSeries(t, "monedula_kafka_quota_drift_detected",
		map[string]string{"namespace": ns, "name": name}); ok {
		t.Fatal("quota drift series still present after finalization (stale series)")
	}

	// The Delete watch event triggers one more reconcile that hits NotFound; it
	// must be a no-op for the gauge (no double-decrement).
	if _, err := r.Reconcile(context.Background(), nsReq(ns, name)); err != nil {
		t.Fatalf("Reconcile NotFound: %v", err)
	}
	if got := managedQuotasValue(t); got != base {
		t.Fatalf("managed_quotas after NotFound reconcile = %v want %v (no double-decrement)", got, base)
	}
}

// TestQuotaMetrics_NotFoundCleansSeries mirrors TestTopicMetrics_NotFoundCleansSeries:
// a reconcile of an already-gone quota removes its drift series even if the
// finalizer-path cleanup was missed.
func TestQuotaMetrics_NotFoundCleansSeries(t *testing.T) {
	const ns, name = "metrics-quotagone-ns", "metrics-quotagone-alice"
	operator.SetQuotaDrift(ns, name, true) // simulate a series from a prior run

	s := topicScheme(t)
	cl := newQuotaFakeClient(t, s)
	r := &KafkaQuotaReconciler{Client: cl, Scheme: s, Clients: stubFactory{}}
	if _, err := r.Reconcile(context.Background(), nsReq(ns, name)); err != nil {
		t.Fatalf("Reconcile NotFound: %v", err)
	}
	if _, ok := gaugeSeries(t, "monedula_kafka_quota_drift_detected",
		map[string]string{"namespace": ns, "name": name}); ok {
		t.Fatal("quota drift series not removed by the NotFound reconcile")
	}
}

// --- KafkaRoleBinding metrics lifecycle ---

// newRoleBindingFakeClient builds a fake client with the role-binding + cluster
// status subresources enabled.
func newRoleBindingFakeClient(t *testing.T, s *runtime.Scheme, objs ...client.Object) client.Client {
	t.Helper()
	return fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(objs...).
		WithStatusSubresource(&v1alpha1.KafkaRoleBinding{}, &v1alpha1.KafkaCluster{}).
		Build()
}

func metricsMDSCluster(ns, name string) *v1alpha1.KafkaCluster {
	c := &v1alpha1.KafkaCluster{
		Spec: v1alpha1.KafkaClusterSpec{
			BootstrapServers: "localhost:9092",
			Authorization: &v1alpha1.AuthorizationConfig{
				MDS: &v1alpha1.MDSConfig{
					Endpoint: "http://mds:8090",
					Clusters: v1alpha1.MDSClusters{KafkaCluster: "lkc-test"},
				},
			},
		},
	}
	c.Name = name
	c.Namespace = ns
	c.Generation = 1
	return c
}

func metricsRoleBinding(ns, name string) *v1alpha1.KafkaRoleBinding {
	rb := &v1alpha1.KafkaRoleBinding{
		Spec: v1alpha1.KafkaRoleBindingSpec{
			ClusterRef: v1alpha1.ClusterRef{Name: "prod"},
			Principal:  "User:alice",
			Role:       "SystemAdmin",
			Scope:      v1alpha1.RoleBindingScope{Type: "kafka"},
		},
	}
	rb.Name = name
	rb.Namespace = ns
	rb.Generation = 1
	return rb
}

// managedRoleBindingsValue reads monedula_managed_rolebindings (0 when never moved).
func managedRoleBindingsValue(t *testing.T) float64 {
	t.Helper()
	v, _ := gaugeSeries(t, "monedula_managed_rolebindings", nil)
	return v
}

// TestRoleBindingMetrics_LifecycleOnDeletion mirrors
// TestTopicMetrics_LifecycleOnDeletion for KafkaRoleBinding: managed-count
// increments once, dedupes on re-reconcile, decrements on finalization, and the
// drift series (keyed off Phase==Drifted, since KafkaRoleBindingStatus has no
// Drift field) is removed on finalization and the NotFound safety net.
func TestRoleBindingMetrics_LifecycleOnDeletion(t *testing.T) {
	const ns, name = "metrics-rb-ns", "metrics-rb-alice"
	s := topicScheme(t)
	rb := metricsRoleBinding(ns, name)
	c := metricsMDSCluster(ns, "prod")
	cl := newRoleBindingFakeClient(t, s, rb, c)
	mock := mdsmock.New()
	r := &KafkaRoleBindingReconciler{Client: cl, Scheme: s, Clients: stubFactory{mds: mock}}

	base := managedRoleBindingsValue(t)

	if _, err := r.Reconcile(context.Background(), nsReq(ns, name)); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got := managedRoleBindingsValue(t); got != base+1 {
		t.Fatalf("managed_rolebindings after first reconcile = %v want %v", got, base+1)
	}
	if _, err := r.Reconcile(context.Background(), nsReq(ns, name)); err != nil {
		t.Fatalf("Reconcile again: %v", err)
	}
	if got := managedRoleBindingsValue(t); got != base+1 {
		t.Fatalf("managed_rolebindings after re-reconcile = %v want %v (dedupe)", got, base+1)
	}
	if _, ok := gaugeSeries(t, "monedula_kafka_rolebinding_drift_detected",
		map[string]string{"namespace": ns, "name": name}); !ok {
		t.Fatal("rolebinding drift series missing after reconcile")
	}

	var live v1alpha1.KafkaRoleBinding
	if err := cl.Get(context.Background(), nsReq(ns, name).NamespacedName, &live); err != nil {
		t.Fatalf("get for delete: %v", err)
	}
	if err := cl.Delete(context.Background(), &live); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := r.Reconcile(context.Background(), nsReq(ns, name)); err != nil {
		t.Fatalf("Reconcile deletion: %v", err)
	}

	if got := managedRoleBindingsValue(t); got != base {
		t.Fatalf("managed_rolebindings after finalization = %v want %v (must decrement)", got, base)
	}
	if _, ok := gaugeSeries(t, "monedula_kafka_rolebinding_drift_detected",
		map[string]string{"namespace": ns, "name": name}); ok {
		t.Fatal("rolebinding drift series still present after finalization (stale series)")
	}

	if _, err := r.Reconcile(context.Background(), nsReq(ns, name)); err != nil {
		t.Fatalf("Reconcile NotFound: %v", err)
	}
	if got := managedRoleBindingsValue(t); got != base {
		t.Fatalf("managed_rolebindings after NotFound reconcile = %v want %v (no double-decrement)", got, base)
	}
}

// TestRoleBindingMetrics_NotFoundCleansSeries mirrors
// TestTopicMetrics_NotFoundCleansSeries: a reconcile of an already-gone role
// binding removes its drift series even if the finalizer-path cleanup was missed.
func TestRoleBindingMetrics_NotFoundCleansSeries(t *testing.T) {
	const ns, name = "metrics-rbgone-ns", "metrics-rbgone-alice"
	operator.SetRoleBindingDrift(ns, name, true) // simulate a series from a prior run

	s := topicScheme(t)
	cl := newRoleBindingFakeClient(t, s)
	r := &KafkaRoleBindingReconciler{Client: cl, Scheme: s, Clients: stubFactory{}}
	if _, err := r.Reconcile(context.Background(), nsReq(ns, name)); err != nil {
		t.Fatalf("Reconcile NotFound: %v", err)
	}
	if _, ok := gaugeSeries(t, "monedula_kafka_rolebinding_drift_detected",
		map[string]string{"namespace": ns, "name": name}); ok {
		t.Fatal("rolebinding drift series not removed by the NotFound reconcile")
	}
}
