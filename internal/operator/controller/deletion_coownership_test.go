package controller

// Co-ownership tests for the finalizer Delete path: deleting one CR must not
// remove an ACL/role-binding tuple another LIVE CR still desires (the delete-
// path analogue of the §10.4 prune aggregation). Covers the pure subtraction
// helpers, the controller-level shield through Reconcile (fake client), the
// deleting-co-owner rule (a CR mid-deletion does not protect tuples), and the
// view-build-failure mode (fail + retry, never over-delete).

import (
	"context"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/monedula-dev/monedula-gitops/internal/access"
	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	kafkamock "github.com/monedula-dev/monedula-gitops/internal/kafka/mock"
	"github.com/monedula-dev/monedula-gitops/internal/mds"
	mdsmock "github.com/monedula-dev/monedula-gitops/internal/mds/mock"
	"github.com/monedula-dev/monedula-gitops/internal/rbac"
)

// --- pure helper tests ---

func TestSubtractProtectedACLs(t *testing.T) {
	shared := access.ACL{Principal: "User:svc", Host: "*", ResourceType: "topic", ResourceName: "t1", PatternType: "literal", Operation: "Read", Permission: "Allow"}
	own := access.ACL{Principal: "User:svc", Host: "*", ResourceType: "group", ResourceName: "g1", PatternType: "literal", Operation: "Read", Permission: "Allow"}

	// The protecting copy carries DIFFERENT attribution (Source*/Prune are
	// excluded from identity): subtraction must match on FullKey only.
	protecting := shared
	protecting.SourceKind = "KafkaAccessPolicy"
	protecting.SourceName = "other-owner"
	protecting.Prune = true

	got := subtractProtectedACLs([]access.ACL{shared, own}, []access.ACL{protecting})
	if len(got) != 1 || got[0].FullKey() != own.FullKey() {
		t.Fatalf("subtractProtectedACLs = %+v, want only the non-shared tuple %q", got, own.FullKey())
	}

	// Empty protect set: everything is deletable (identical result, no copy needed).
	if got := subtractProtectedACLs([]access.ACL{shared, own}, nil); len(got) != 2 {
		t.Fatalf("empty protect set must keep all tuples, got %+v", got)
	}
	// Empty to-delete set: stays empty.
	if got := subtractProtectedACLs(nil, []access.ACL{protecting}); len(got) != 0 {
		t.Fatalf("empty to-delete set must stay empty, got %+v", got)
	}
	// Everything protected: nothing to delete.
	if got := subtractProtectedACLs([]access.ACL{shared}, []access.ACL{protecting}); len(got) != 0 {
		t.Fatalf("fully protected set must be empty, got %+v", got)
	}
}

func TestSubtractProtectedRoleBindings(t *testing.T) {
	scope := rbac.Scope{Type: "kafka", KafkaCluster: "lkc-1"}
	shared := rbac.RoleBinding{Principal: "User:svc", Role: "DeveloperWrite", Scope: scope,
		Resource: &rbac.ResourcePattern{Type: "Topic", Name: "t1", PatternType: "literal"}}
	own := rbac.RoleBinding{Principal: "User:svc", Role: "DeveloperWrite", Scope: scope,
		Resource: &rbac.ResourcePattern{Type: "Topic", Name: "t2", PatternType: "literal"}}

	// Protecting copy with different attribution (topic-derived): FullKey-only match.
	protecting := shared
	protecting.SourceKind = "KafkaTopic"
	protecting.SourceName = "other-owner"
	protecting.Prune = true

	got := subtractProtectedRoleBindings([]rbac.RoleBinding{shared, own}, []rbac.RoleBinding{protecting})
	if len(got) != 1 || got[0].FullKey() != own.FullKey() {
		t.Fatalf("subtractProtectedRoleBindings = %+v, want only the non-shared binding %q", got, own.FullKey())
	}
	if got := subtractProtectedRoleBindings([]rbac.RoleBinding{shared, own}, nil); len(got) != 2 {
		t.Fatalf("empty protect set must keep all bindings, got %+v", got)
	}
	if got := subtractProtectedRoleBindings([]rbac.RoleBinding{shared}, []rbac.RoleBinding{protecting}); len(got) != 0 {
		t.Fatalf("fully protected set must be empty, got %+v", got)
	}
}

// --- controller-level shield tests (fake client) ---

// coOwnedTopic is topicObj with a consumer whose topic-side ACLs (Read,
// Describe on payments.orders) are EXACTLY the tuples policyObj compiles to;
// the group ACL (Read on group orders-g1) is the topic's own, non-shared tuple.
func coOwnedTopic() *v1alpha1.KafkaTopic {
	tp := topicObj("Enforce")
	tp.Spec.Access = v1alpha1.TopicAccess{Consumers: []v1alpha1.ConsumerAccess{
		{Principal: "User:svc-payments", Group: "orders-g1"},
	}}
	return tp
}

// coOwnedLiveACLs is the converged live state: the shared topic Read+Describe
// tuples plus the topic-only group tuple.
func coOwnedLiveACLs() []kafka.ACLState {
	return append(seededPolicyACLsState(), kafka.ACLState{
		Principal: "User:svc-payments", Host: "*", ResourceType: "group", ResourceName: "orders-g1",
		PatternType: "literal", Operation: "Read", Permission: "Allow",
	})
}

// liveGroupACLPresent reports whether the topic's non-shared group tuple is live.
func liveGroupACLPresent(t *testing.T, k *kafkamock.Client) bool {
	t.Helper()
	acls, err := k.ListACLs(context.Background())
	if err != nil {
		t.Fatalf("ListACLs: %v", err)
	}
	for _, a := range acls {
		if a.ResourceType == "group" && a.ResourceName == "orders-g1" {
			return true
		}
	}
	return false
}

// TestTopicDelete_SharedACLProtectedByLivePolicy is the headline bug fix: topic
// A (deletionPolicy Delete + allow-delete) and live policy B share the
// (User:svc-payments, topic payments.orders, Read/Describe) tuples. Finalizing
// A must delete the broker topic and A's own group ACL but RETAIN the shared
// tuples — B's principal must not lose access.
func TestTopicDelete_SharedACLProtectedByLivePolicy(t *testing.T) {
	s := topicScheme(t)
	tp := coOwnedTopic()
	tp.Spec.DeletionPolicy = "Delete"
	tp.Finalizers = []string{FinalizerName}
	tp.Annotations = map[string]string{"gitops.monedula.dev/allow-delete": "true"}
	pol := policyObj("Enforce") // live co-owner
	c := topicCluster()
	cl := newViewFakeClient(t, s, tp, pol, c)
	if err := cl.Delete(context.Background(), tp); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	k := kafkamock.New(seededTopics("payments.orders"), coOwnedLiveACLs())
	rec := events.NewFakeRecorder(8)
	r := &KafkaTopicReconciler{Client: cl, Scheme: s, Recorder: rec, Clients: stubFactory{k: k}}
	if _, err := r.Reconcile(context.Background(), topicReq()); err != nil {
		t.Fatalf("Reconcile delete: %v", err)
	}

	if !callsContain(k.Calls(), "DeleteTopic payments.orders") {
		t.Fatalf("broker topic must still be deleted (not co-owned), calls %v", k.Calls())
	}
	got := liveOpsFor(t, k)
	if !got["Read"] || !got["Describe"] {
		t.Fatalf("shared ACLs co-owned by the live policy were deleted, live ops = %v (calls %v)", got, k.Calls())
	}
	if liveGroupACLPresent(t, k) {
		t.Fatalf("the topic's own (non-shared) group ACL must be deleted, calls %v", k.Calls())
	}
	var gone v1alpha1.KafkaTopic
	if err := cl.Get(context.Background(), topicReq().NamespacedName, &gone); err == nil {
		t.Fatalf("topic CR still present after finalization: finalizers %v", gone.Finalizers)
	}

	// SharedACLsRetained must name the actual co-owner (the live policy),
	// Kind/namespace/name, not just a bare count.
	select {
	case ev := <-rec.Events:
		if !strings.Contains(ev, "SharedACLsRetained") {
			t.Fatalf("expected a SharedACLsRetained event, got %q", ev)
		}
		if !strings.Contains(ev, "KafkaAccessPolicy/ns1/payments-access") {
			t.Fatalf("event should name the co-owner KafkaAccessPolicy/ns1/payments-access, got %q", ev)
		}
	default:
		t.Fatal("expected a SharedACLsRetained event")
	}
}

// TestTopicDelete_OtherDeletingPolicyDoesNotProtect: a co-owner that is ITSELF
// being deleted (non-zero DeletionTimestamp) must not protect the shared
// tuples — it is going away too, and its own finalizer handles its cleanup.
func TestTopicDelete_OtherDeletingPolicyDoesNotProtect(t *testing.T) {
	s := topicScheme(t)
	tp := coOwnedTopic()
	tp.Spec.DeletionPolicy = "Delete"
	tp.Finalizers = []string{FinalizerName}
	tp.Annotations = map[string]string{"gitops.monedula.dev/allow-delete": "true"}
	pol := policyObj("Enforce")
	pol.Finalizers = []string{FinalizerName} // keep it alive through Delete
	c := topicCluster()
	cl := newViewFakeClient(t, s, tp, pol, c)
	if err := cl.Delete(context.Background(), tp); err != nil {
		t.Fatalf("Delete topic: %v", err)
	}
	if err := cl.Delete(context.Background(), pol); err != nil {
		t.Fatalf("Delete policy: %v", err)
	}

	k := kafkamock.New(seededTopics("payments.orders"), coOwnedLiveACLs())
	r := &KafkaTopicReconciler{Client: cl, Scheme: s, Clients: stubFactory{k: k}}
	if _, err := r.Reconcile(context.Background(), topicReq()); err != nil {
		t.Fatalf("Reconcile delete: %v", err)
	}

	got := liveOpsFor(t, k)
	if got["Read"] || got["Describe"] {
		t.Fatalf("a deleting policy must not protect shared tuples, live ops = %v", got)
	}
}

// TestPolicyDelete_SharedACLProtectedByLiveTopic is the mirror case: policy B
// (Delete + allow-delete) finalizes while topic A still desires the same
// tuples. Every tuple of B is co-owned, so NO DeleteACLs call may happen.
func TestPolicyDelete_SharedACLProtectedByLiveTopic(t *testing.T) {
	s := topicScheme(t)
	pol := policyObj("Enforce")
	pol.Spec.DeletionPolicy = "Delete"
	pol.Finalizers = []string{FinalizerName}
	pol.Annotations = map[string]string{"gitops.monedula.dev/allow-delete": "true"}
	tp := coOwnedTopic() // live co-owner
	c := topicCluster()
	cl := newViewFakeClient(t, s, tp, pol, c)
	if err := cl.Delete(context.Background(), pol); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	k := kafkamock.New(seededTopics("payments.orders"), coOwnedLiveACLs())
	rec := events.NewFakeRecorder(8)
	r := &KafkaAccessPolicyReconciler{Client: cl, Scheme: s, Recorder: rec, Clients: stubFactory{k: k}}
	if _, err := r.Reconcile(context.Background(), policyReq()); err != nil {
		t.Fatalf("Reconcile delete: %v", err)
	}

	if callsContain(k.Calls(), "DeleteACLs") {
		t.Fatalf("all policy tuples are co-owned by the live topic; DeleteACLs must not run, calls %v", k.Calls())
	}
	got := liveOpsFor(t, k)
	if !got["Read"] || !got["Describe"] {
		t.Fatalf("shared ACLs were deleted despite the live topic co-owner, live ops = %v", got)
	}
	var gone v1alpha1.KafkaAccessPolicy
	if err := cl.Get(context.Background(), policyReq().NamespacedName, &gone); err == nil {
		t.Fatalf("policy CR still present after finalization: finalizers %v", gone.Finalizers)
	}

	select {
	case ev := <-rec.Events:
		if !strings.Contains(ev, "SharedACLsRetained") {
			t.Fatalf("expected a SharedACLsRetained event, got %q", ev)
		}
		if !strings.Contains(ev, "KafkaTopic/ns1/orders") {
			t.Fatalf("event should name the co-owner KafkaTopic/ns1/orders, got %q", ev)
		}
	default:
		t.Fatal("expected a SharedACLsRetained event")
	}
}

// listFailClient wraps a client and fails every KafkaTopicList List call,
// simulating a transient cache/list failure during the view build.
type listFailClient struct {
	client.Client
}

func (c *listFailClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if _, ok := list.(*v1alpha1.KafkaTopicList); ok {
		return errReason("simulated list failure")
	}
	return c.Client.List(ctx, list, opts...)
}

// TestTopicDelete_ViewBuildFailureBlocksDeletion pins the over-delete guard:
// when the cluster ACL view cannot be built, the deletion attempt must FAIL
// (finalizer retained, requeue) with NO broker mutation — never fall back to
// deleting the CR's full compiled set.
func TestTopicDelete_ViewBuildFailureBlocksDeletion(t *testing.T) {
	s := topicScheme(t)
	tp := coOwnedTopic()
	tp.Spec.DeletionPolicy = "Delete"
	tp.Finalizers = []string{FinalizerName}
	tp.Annotations = map[string]string{"gitops.monedula.dev/allow-delete": "true"}
	c := topicCluster()
	cl := newViewFakeClient(t, s, tp, c)
	if err := cl.Delete(context.Background(), tp); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	k := kafkamock.New(seededTopics("payments.orders"), coOwnedLiveACLs())
	r := &KafkaTopicReconciler{Client: &listFailClient{Client: cl}, Scheme: s, Clients: stubFactory{k: k}}
	if _, err := r.Reconcile(context.Background(), topicReq()); err == nil {
		t.Fatal("expected a transient error (retry) when the view build fails during deletion")
	}

	if len(k.Calls()) != 0 {
		t.Fatalf("view-build failure must not mutate the broker, calls %v", k.Calls())
	}
	var got v1alpha1.KafkaTopic
	if err := cl.Get(context.Background(), topicReq().NamespacedName, &got); err != nil {
		t.Fatalf("topic CR should still exist (finalizer retained): %v", err)
	}
	if !controllerutil.ContainsFinalizer(&got, FinalizerName) {
		t.Fatal("finalizer must be retained when the view build fails")
	}
}

// --- role-binding co-ownership: SharedRoleBindingsRetained co-owner naming ---

// mdsCluster builds a KafkaCluster with authorization.mds configured, suitable
// for the role-binding co-ownership tests below (plain fake client — the view
// builder does no field-indexed List, so envtest is not needed here).
func mdsCluster(ns, name string) *v1alpha1.KafkaCluster {
	return &v1alpha1.KafkaCluster{
		Spec: v1alpha1.KafkaClusterSpec{
			BootstrapServers: "localhost:9092",
			Authorization: &v1alpha1.AuthorizationConfig{
				MDS: &v1alpha1.MDSConfig{
					Endpoint: "http://mds:8090",
					Clusters: v1alpha1.MDSClusters{KafkaCluster: "lkc-test"},
				},
			},
		},
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
	}
}

// clusterScopedRoleBinding builds a minimal cluster-scoped KafkaRoleBinding
// (no spec.resources, so it compiles to exactly one tuple with a nil
// Resource — see rbac.Compile) referencing clusterName.
func clusterScopedRoleBinding(ns, name, clusterName string) *v1alpha1.KafkaRoleBinding {
	return &v1alpha1.KafkaRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec: v1alpha1.KafkaRoleBindingSpec{
			ClusterRef: v1alpha1.ClusterRef{Name: clusterName},
			Principal:  "User:alice",
			Role:       "SystemAdmin",
			Scope:      v1alpha1.RoleBindingScope{Type: "kafka"},
		},
	}
}

// TestRoleBindingDelete_SharedBindingProtectedByLiveRoleBinding pins the
// SharedRoleBindingsRetained co-owner naming: two KafkaRoleBinding CRs with
// the identical (principal, role, scope) tuple. Finalizing the Delete-policy
// one must retain the MDS binding (the live co-owner still desires it) and
// the emitted event must name that co-owner as KafkaRoleBinding/ns/name.
func TestRoleBindingDelete_SharedBindingProtectedByLiveRoleBinding(t *testing.T) {
	s := newScheme(t)
	rb := clusterScopedRoleBinding("ns1", "rb-delete", "prod")
	rb.Spec.DeletionPolicy = "Delete"
	rb.Finalizers = []string{FinalizerName}
	rb.Annotations = map[string]string{"gitops.monedula.dev/allow-delete": "true"}
	live := clusterScopedRoleBinding("ns1", "rb-live", "prod") // live co-owner, same tuple
	c := mdsCluster("ns1", "prod")
	cl := newViewFakeClient(t, s, rb, live, c)
	if err := cl.Delete(context.Background(), rb); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	seeded := mds.RoleBinding{Principal: "User:alice", Role: "SystemAdmin", Scope: mds.Scope{Type: "kafka", KafkaCluster: "lkc-test"}}
	mock := mdsmock.New(seeded)
	rec := events.NewFakeRecorder(8)
	r := &KafkaRoleBindingReconciler{Client: cl, Scheme: s, Recorder: rec, Clients: stubFactory{mds: mock}}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKey{Namespace: "ns1", Name: "rb-delete"}}); err != nil {
		t.Fatalf("Reconcile delete: %v", err)
	}

	if callsContain(mock.Calls(), "RemoveRoleBinding "+seeded.Key()) {
		t.Fatalf("shared binding co-owned by the live role binding must be retained, calls %v", mock.Calls())
	}
	var gone v1alpha1.KafkaRoleBinding
	if err := cl.Get(context.Background(), client.ObjectKey{Namespace: "ns1", Name: "rb-delete"}, &gone); err == nil {
		t.Fatalf("role binding CR still present after finalization: finalizers %v", gone.Finalizers)
	}

	select {
	case ev := <-rec.Events:
		if !strings.Contains(ev, "SharedRoleBindingsRetained") {
			t.Fatalf("expected a SharedRoleBindingsRetained event, got %q", ev)
		}
		if !strings.Contains(ev, "KafkaRoleBinding/ns1/rb-live") {
			t.Fatalf("event should name the co-owner KafkaRoleBinding/ns1/rb-live, got %q", ev)
		}
	default:
		t.Fatal("expected a SharedRoleBindingsRetained event")
	}
}
