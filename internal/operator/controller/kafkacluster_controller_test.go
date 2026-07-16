package controller

import (
	"context"
	"errors"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	kafkamock "github.com/monedula-dev/monedula-gitops/internal/kafka/mock"
	"github.com/monedula-dev/monedula-gitops/internal/mds"
	"github.com/monedula-dev/monedula-gitops/internal/schemaregistry"
)

// compile-time assertions: the reconciler and factories satisfy the interfaces.
var (
	_ reconcile.Reconciler = (*KafkaClusterReconciler)(nil)
	_ ClientFactory        = (*DefaultClientFactory)(nil)
	_ ClientFactory        = stubFactory{}
)

type stubFactory struct {
	k      kafka.AdminClient
	sr     schemaregistry.Client
	mds    mds.Client
	err    error
	mdsErr error
}

func (f stubFactory) For(context.Context, *v1alpha1.KafkaCluster) (kafka.AdminClient, schemaregistry.Client, func(), error) {
	return f.k, f.sr, func() {}, f.err
}

func (f stubFactory) MDSFor(context.Context, *v1alpha1.KafkaCluster) (mds.Client, error) {
	return f.mds, f.mdsErr
}

func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}
	return s
}

func clusterObj() *v1alpha1.KafkaCluster {
	c := &v1alpha1.KafkaCluster{}
	c.Name = "prod"
	c.Namespace = "ns1"
	c.Generation = 1
	return c
}

func newFakeClient(t *testing.T, s *runtime.Scheme, objs ...client.Object) client.Client {
	t.Helper()
	return fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(objs...).
		WithStatusSubresource(&v1alpha1.KafkaCluster{}).
		Build()
}

func TestReconcile_NotFound(t *testing.T) {
	s := newScheme(t)
	cl := newFakeClient(t, s)
	r := &KafkaClusterReconciler{Client: cl, Scheme: s, Clients: stubFactory{}}

	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: client.ObjectKey{Namespace: "ns1", Name: "missing"},
	})
	if err != nil {
		t.Fatalf("Reconcile NotFound returned err: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Fatalf("NotFound should not requeue, got %v", res.RequeueAfter)
	}
}

func TestReconcile_ReadyWritesStatus(t *testing.T) {
	s := newScheme(t)
	c := clusterObj()
	cl := newFakeClient(t, s, c)
	r := &KafkaClusterReconciler{
		Client:  cl,
		Scheme:  s,
		Clients: stubFactory{k: kafkamock.New(nil, nil)},
	}

	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: client.ObjectKey{Namespace: "ns1", Name: "prod"},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if res.RequeueAfter != clusterRequeueAfter {
		t.Fatalf("RequeueAfter = %v want %v", res.RequeueAfter, clusterRequeueAfter)
	}

	var got v1alpha1.KafkaCluster
	if err := cl.Get(context.Background(), client.ObjectKeyFromObject(c), &got); err != nil {
		t.Fatalf("get back: %v", err)
	}
	if got.Status == nil || got.Status.Phase != v1alpha1.PhaseReady {
		t.Fatalf("status phase = %+v want Ready", got.Status)
	}
}

func TestReconcile_ClientsBuildError(t *testing.T) {
	s := newScheme(t)
	c := clusterObj()
	cl := newFakeClient(t, s, c)
	r := &KafkaClusterReconciler{
		Client:  cl,
		Scheme:  s,
		Clients: stubFactory{err: errors.New("secret not found")},
	}

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: client.ObjectKey{Namespace: "ns1", Name: "prod"},
	})
	if err == nil {
		t.Fatal("expected error from clients-build failure (requeue signal)")
	}

	var got v1alpha1.KafkaCluster
	if err := cl.Get(context.Background(), client.ObjectKeyFromObject(c), &got); err != nil {
		t.Fatalf("get back: %v", err)
	}
	if got.Status == nil || got.Status.Phase != v1alpha1.PhaseError {
		t.Fatalf("status phase = %+v want Error", got.Status)
	}
	if cond := findCond(got.Status.Conditions, v1alpha1.CondClusterReachable); cond == nil || cond.Status != metav1.ConditionFalse {
		t.Fatalf("ClusterReachable = %v want False", cond)
	}
}

// TestReconcile_EmitsEvents verifies one Event per reconcile outcome: a Normal
// Ready event on success and a Warning ClientsBuildFailed event on a build error.
func TestReconcile_EmitsEvents(t *testing.T) {
	s := newScheme(t)

	// Success -> Normal "Ready".
	c := clusterObj()
	cl := newFakeClient(t, s, c)
	rec := events.NewFakeRecorder(8)
	r := &KafkaClusterReconciler{Client: cl, Scheme: s, Recorder: rec, Clients: stubFactory{k: kafkamock.New(nil, nil)}}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKey{Namespace: "ns1", Name: "prod"}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	select {
	case ev := <-rec.Events:
		if !strings.Contains(ev, "Normal") || !strings.Contains(ev, "Ready") {
			t.Fatalf("success event = %q, want Normal/Ready", ev)
		}
	default:
		t.Fatal("no event emitted on success")
	}

	// Build error -> Warning "ClientsBuildFailed".
	c2 := clusterObj()
	cl2 := newFakeClient(t, s, c2)
	rec2 := events.NewFakeRecorder(8)
	r2 := &KafkaClusterReconciler{Client: cl2, Scheme: s, Recorder: rec2, Clients: stubFactory{err: errors.New("secret not found")}}
	if _, err := r2.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKey{Namespace: "ns1", Name: "prod"}}); err == nil {
		t.Fatal("expected build error")
	}
	select {
	case ev := <-rec2.Events:
		if !strings.Contains(ev, "Warning") || !strings.Contains(ev, "ClientsBuildFailed") {
			t.Fatalf("build-error event = %q, want Warning/ClientsBuildFailed", ev)
		}
	default:
		t.Fatal("no event emitted on build error")
	}
}

func findCond(conds []metav1.Condition, typ string) *metav1.Condition {
	for i := range conds {
		if conds[i].Type == typ {
			return &conds[i]
		}
	}
	return nil
}
