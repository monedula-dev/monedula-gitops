package controller

import (
	"context"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/monedula-dev/monedula-gitops/internal/access"
	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	kafkamock "github.com/monedula-dev/monedula-gitops/internal/kafka/mock"
)

// compile-time assertion: the policy reconciler is a controller-runtime Reconciler.
var _ reconcile.Reconciler = (*KafkaAccessPolicyReconciler)(nil)

// newPolicyFakeClient builds a fake client with the policy + cluster status
// subresources enabled so Status().Update works, and finalizer-respecting
// deletion (the fake client keeps an object with a finalizer until removed).
func newPolicyFakeClient(t *testing.T, s *runtime.Scheme, objs ...client.Object) client.Client {
	t.Helper()
	return fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(objs...).
		WithStatusSubresource(&v1alpha1.KafkaAccessPolicy{}, &v1alpha1.KafkaCluster{}).
		Build()
}

func policyCluster() *v1alpha1.KafkaCluster {
	c := &v1alpha1.KafkaCluster{}
	c.Name = "prod"
	c.Namespace = "ns1"
	c.Generation = 1
	return c
}

func policyObj(mode string) *v1alpha1.KafkaAccessPolicy {
	pol := &v1alpha1.KafkaAccessPolicy{
		Spec: v1alpha1.KafkaAccessPolicySpec{
			ClusterRef: v1alpha1.ClusterRef{Name: "prod"},
			Rules: []v1alpha1.ACLRule{{
				Principal:  "User:svc-payments",
				Permission: "Allow",
				Host:       "*",
				Resource:   v1alpha1.ACLResource{Type: "topic", Name: "payments.orders", PatternType: "literal"},
				Operations: []string{"Read", "Describe"},
			}},
		},
	}
	pol.Name = "payments-access"
	pol.Namespace = "ns1"
	pol.Generation = 1
	if mode != "" {
		pol.Spec.Reconciliation = &v1alpha1.Reconciliation{Mode: mode}
	}
	return pol
}

func policyReq() ctrl.Request {
	return ctrl.Request{NamespacedName: client.ObjectKey{Namespace: "ns1", Name: "payments-access"}}
}

// seededPolicyACLs returns the ACL states the policyObj compiles to, so the mock
// has something to delete on the teardown path.
func seededPolicyACLs(t *testing.T) []kafka.ACLState {
	t.Helper()
	desired, errs := access.BuildDesiredSet(access.CompilePolicy(policyObj("")))
	if len(errs) > 0 {
		t.Fatalf("compiling policy ACLs: %v", errs)
	}
	states := make([]kafka.ACLState, 0, len(desired))
	for _, a := range desired {
		states = append(states, kafka.ACLState{
			Principal: a.Principal, Host: a.Host, ResourceType: a.ResourceType,
			ResourceName: a.ResourceName, PatternType: a.PatternType,
			Operation: a.Operation, Permission: a.Permission,
		})
	}
	return states
}

func TestPolicyReconcile_NotFound(t *testing.T) {
	s := topicScheme(t)
	cl := newPolicyFakeClient(t, s)
	r := &KafkaAccessPolicyReconciler{Client: cl, Scheme: s, Clients: stubFactory{}}

	res, err := r.Reconcile(context.Background(), policyReq())
	if err != nil {
		t.Fatalf("Reconcile NotFound returned err: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Fatalf("NotFound should not requeue, got %v", res.RequeueAfter)
	}
}

func TestPolicyReconcile_CreatesAndAddsFinalizer(t *testing.T) {
	s := topicScheme(t)
	pol := policyObj("Enforce")
	c := policyCluster()
	cl := newPolicyFakeClient(t, s, pol, c)
	k := kafkamock.New(nil, nil)
	rec := events.NewFakeRecorder(8)
	r := &KafkaAccessPolicyReconciler{Client: cl, Scheme: s, Recorder: rec, Clients: stubFactory{k: k}}

	res, err := r.Reconcile(context.Background(), policyReq())
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if res.RequeueAfter != policyRequeueAfter {
		t.Fatalf("RequeueAfter = %v want %v", res.RequeueAfter, policyRequeueAfter)
	}

	var got v1alpha1.KafkaAccessPolicy
	if err := cl.Get(context.Background(), client.ObjectKeyFromObject(pol), &got); err != nil {
		t.Fatalf("get back: %v", err)
	}
	if !controllerutil.ContainsFinalizer(&got, FinalizerName) {
		t.Fatalf("finalizer not added: %v", got.Finalizers)
	}
	if got.Status == nil || got.Status.Phase != v1alpha1.PhaseReady {
		t.Fatalf("status phase = %+v want Ready", got.Status)
	}
	if cond := findCond(got.Status.Conditions, v1alpha1.CondAccessPolicySynced); cond == nil || cond.Status != metav1.ConditionTrue {
		t.Fatalf("AccessPolicySynced = %v want True", cond)
	}
	if !callsContain(k.Calls(), "CreateACLs") {
		t.Fatalf("expected CreateACLs call, got %v", k.Calls())
	}
	select {
	case ev := <-rec.Events:
		if !strings.Contains(ev, "Normal") {
			t.Fatalf("expected Normal event, got %q", ev)
		}
	default:
		t.Fatal("no event emitted on success")
	}
}

func TestPolicyReconcile_ClusterNotFound(t *testing.T) {
	s := topicScheme(t)
	pol := policyObj("Enforce")
	// No cluster object created.
	cl := newPolicyFakeClient(t, s, pol)
	rec := events.NewFakeRecorder(8)
	r := &KafkaAccessPolicyReconciler{Client: cl, Scheme: s, Recorder: rec, Clients: stubFactory{k: kafkamock.New(nil, nil)}}

	res, err := r.Reconcile(context.Background(), policyReq())
	if err == nil && res.RequeueAfter == 0 {
		t.Fatal("expected requeue (err or RequeueAfter) when cluster not found")
	}

	var got v1alpha1.KafkaAccessPolicy
	if err := cl.Get(context.Background(), client.ObjectKeyFromObject(pol), &got); err != nil {
		t.Fatalf("get back: %v", err)
	}
	if got.Status == nil || got.Status.Phase != v1alpha1.PhaseError {
		t.Fatalf("status phase = %+v want Error", got.Status)
	}
	if cond := findCond(got.Status.Conditions, v1alpha1.CondReady); cond == nil ||
		cond.Status != metav1.ConditionFalse || cond.Reason != "ClusterNotFound" {
		t.Fatalf("Ready cond = %v want False/ClusterNotFound", cond)
	}
	select {
	case ev := <-rec.Events:
		if !strings.Contains(ev, "Warning") || !strings.Contains(ev, "ClusterNotFound") {
			t.Fatalf("event = %q want Warning/ClusterNotFound", ev)
		}
	default:
		t.Fatal("no event emitted on cluster-not-found")
	}
}

func TestPolicyReconcile_ClientsBuildError(t *testing.T) {
	s := topicScheme(t)
	pol := policyObj("Enforce")
	c := policyCluster()
	cl := newPolicyFakeClient(t, s, pol, c)
	r := &KafkaAccessPolicyReconciler{Client: cl, Scheme: s, Clients: stubFactory{err: errReason("boom")}}

	_, err := r.Reconcile(context.Background(), policyReq())
	if err == nil {
		t.Fatal("expected error from clients-build failure (requeue signal)")
	}
	var got v1alpha1.KafkaAccessPolicy
	if err := cl.Get(context.Background(), client.ObjectKeyFromObject(pol), &got); err != nil {
		t.Fatalf("get back: %v", err)
	}
	if got.Status == nil || got.Status.Phase != v1alpha1.PhaseError {
		t.Fatalf("status phase = %+v want Error", got.Status)
	}
	if cond := findCond(got.Status.Conditions, v1alpha1.CondReady); cond == nil ||
		cond.Status != metav1.ConditionFalse || cond.Reason != "ClientsBuildFailed" {
		t.Fatalf("Ready cond = %v want False/ClientsBuildFailed", cond)
	}
}

// policyDeleteSetup creates a policy whose ACLs already exist in Kafka, with the
// finalizer set, then issues a Delete so the fake client sets DeletionTimestamp
// and keeps the object alive (finalizer present).
func policyDeleteSetup(t *testing.T, deletionPolicy string, annotations map[string]string) (client.Client, *runtime.Scheme) {
	t.Helper()
	s := topicScheme(t)
	pol := policyObj("Enforce")
	pol.Spec.DeletionPolicy = deletionPolicy
	pol.Finalizers = []string{FinalizerName}
	if annotations != nil {
		pol.Annotations = annotations
	}
	c := policyCluster()
	cl := newPolicyFakeClient(t, s, pol, c)

	if err := cl.Delete(context.Background(), pol); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	return cl, s
}

func TestPolicyReconcile_DeletePolicyDelete_WithApproval(t *testing.T) {
	k := kafkamock.New(nil, seededPolicyACLs(t))
	cl, s := policyDeleteSetup(t, "Delete", map[string]string{"gitops.monedula.dev/allow-delete": "true"})
	r := &KafkaAccessPolicyReconciler{Client: cl, Scheme: s, Clients: stubFactory{k: k}}

	if _, err := r.Reconcile(context.Background(), policyReq()); err != nil {
		t.Fatalf("Reconcile delete: %v", err)
	}

	if !callsContain(k.Calls(), "DeleteACLs") {
		t.Fatalf("expected DeleteACLs call, got %v", k.Calls())
	}
	// Finalizer removed -> object gone.
	var got v1alpha1.KafkaAccessPolicy
	if err := cl.Get(context.Background(), policyReq().NamespacedName, &got); err == nil {
		t.Fatalf("policy still present after deletion: finalizers %v", got.Finalizers)
	}
}

func TestPolicyReconcile_DeletePolicyDelete_NoApproval(t *testing.T) {
	k := kafkamock.New(nil, seededPolicyACLs(t))
	cl, s := policyDeleteSetup(t, "Delete", nil)
	rec := events.NewFakeRecorder(8)
	r := &KafkaAccessPolicyReconciler{Client: cl, Scheme: s, Recorder: rec, Clients: stubFactory{k: k}}

	if _, err := r.Reconcile(context.Background(), policyReq()); err != nil {
		t.Fatalf("Reconcile delete: %v", err)
	}

	if callsContain(k.Calls(), "DeleteACLs") {
		t.Fatalf("ACLs must NOT be deleted without approval, got %v", k.Calls())
	}
	// Finalizer retained -> object still present.
	var got v1alpha1.KafkaAccessPolicy
	if err := cl.Get(context.Background(), policyReq().NamespacedName, &got); err != nil {
		t.Fatalf("policy should still exist (finalizer retained): %v", err)
	}
	if !controllerutil.ContainsFinalizer(&got, FinalizerName) {
		t.Fatal("finalizer should be retained without approval")
	}
	select {
	case ev := <-rec.Events:
		if !strings.Contains(ev, "Warning") || !strings.Contains(ev, "DeleteNotApproved") {
			t.Fatalf("event = %q want Warning/DeleteNotApproved", ev)
		}
	default:
		t.Fatal("no Warning event emitted when delete not approved")
	}
}

func TestPolicyReconcile_DeletePolicyOrphan(t *testing.T) {
	k := kafkamock.New(nil, seededPolicyACLs(t))
	cl, s := policyDeleteSetup(t, "Orphan", nil)
	r := &KafkaAccessPolicyReconciler{Client: cl, Scheme: s, Clients: stubFactory{k: k}}

	if _, err := r.Reconcile(context.Background(), policyReq()); err != nil {
		t.Fatalf("Reconcile delete: %v", err)
	}

	if callsContain(k.Calls(), "DeleteACLs") {
		t.Fatalf("Orphan must NOT delete the managed ACLs, got %v", k.Calls())
	}
	var got v1alpha1.KafkaAccessPolicy
	if err := cl.Get(context.Background(), policyReq().NamespacedName, &got); err == nil {
		t.Fatalf("policy should be gone (finalizer removed), finalizers %v", got.Finalizers)
	}
}

func TestPolicyReconcile_DeleteUnreachable_ForceRemoval(t *testing.T) {
	cl, s := policyDeleteSetup(t, "Delete",
		map[string]string{"gitops.monedula.dev/force-finalizer-removal": "true"})
	rec := events.NewFakeRecorder(8)
	// Clients fail to build (cluster unreachable).
	r := &KafkaAccessPolicyReconciler{Client: cl, Scheme: s, Recorder: rec, Clients: stubFactory{err: errReason("unreachable")}}

	if _, err := r.Reconcile(context.Background(), policyReq()); err != nil {
		t.Fatalf("Reconcile delete with force-removal: %v", err)
	}
	var got v1alpha1.KafkaAccessPolicy
	if err := cl.Get(context.Background(), policyReq().NamespacedName, &got); err == nil {
		t.Fatalf("policy should be gone (force-removal), finalizers %v", got.Finalizers)
	}
}

func TestPolicyReconcile_DetectOnly_NoMutation(t *testing.T) {
	s := topicScheme(t)
	pol := policyObj("DetectOnly")
	c := policyCluster()
	cl := newPolicyFakeClient(t, s, pol, c)
	k := kafkamock.New(nil, nil) // ACLs absent -> drift
	r := &KafkaAccessPolicyReconciler{Client: cl, Scheme: s, Clients: stubFactory{k: k}}

	if _, err := r.Reconcile(context.Background(), policyReq()); err != nil {
		t.Fatalf("Reconcile DetectOnly: %v", err)
	}
	if callsContain(k.Calls(), "CreateACLs") {
		t.Fatalf("DetectOnly must not mutate, got %v", k.Calls())
	}
	var got v1alpha1.KafkaAccessPolicy
	if err := cl.Get(context.Background(), policyReq().NamespacedName, &got); err != nil {
		t.Fatalf("get back: %v", err)
	}
	if got.Status == nil || got.Status.Phase != v1alpha1.PhaseDrifted {
		t.Fatalf("status phase = %+v want Drifted", got.Status)
	}
}
