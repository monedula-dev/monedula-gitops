//go:build envtest

package controller

import (
	"context"
	"strings"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
)

func envMirror(name, clusterRef, sourceTopic string) *v1alpha1.KafkaMirrorTopic {
	return &v1alpha1.KafkaMirrorTopic{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
		Spec: v1alpha1.KafkaMirrorTopicSpec{
			ClusterRef:      v1alpha1.ClusterRef{Name: clusterRef},
			LinkName:        mirrorCtrlLink,
			SourceTopicName: sourceTopic,
		},
	}
}

// TestEnvtestMirrorLosesToOlderTopic: a KafkaTopic created first owns the
// destination topic identity, so a later KafkaMirrorTopic for the same
// (cluster, topic) goes terminal ValidationFailed/DuplicateIdentity naming the
// topic, without building any client or calling REST.
func TestEnvtestMirrorLosesToOlderTopic(t *testing.T) {
	env := startEnv(t)
	defer env.stop()
	ctx := context.Background()

	if err := env.cl.Create(ctx, newCluster(testNamespace, "mx-cluster")); err != nil {
		t.Fatalf("create cluster: %v", err)
	}
	// "mx-a" < "mx-b" so the topic also wins an equal-second tiebreak.
	topic := &v1alpha1.KafkaTopic{
		ObjectMeta: metav1.ObjectMeta{Name: "mx-a", Namespace: testNamespace},
		Spec: v1alpha1.KafkaTopicSpec{
			ClusterRef: v1alpha1.ClusterRef{Name: "mx-cluster"},
			TopicName:  mirrorCtrlTopic,
			Partitions: 3,
		},
	}
	if err := env.cl.Create(ctx, topic); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if err := env.cl.Create(ctx, envMirror("mx-b", "mx-cluster", mirrorCtrlTopic)); err != nil {
		t.Fatalf("create mirror: %v", err)
	}

	apiReader, err := client.New(env.cfg, client.Options{Scheme: env.scheme})
	if err != nil {
		t.Fatalf("building quorum reader: %v", err)
	}
	k, rest := mirrorCtrlFixture("")
	f := &countingFactory{stubFactory: stubFactory{k: k, rest: rest}}
	r := &KafkaMirrorTopicReconciler{Client: env.cl, Scheme: env.scheme, Clients: f, APIReader: apiReader}
	reconcileFor(t, r, testNamespace, "mx-b")

	if n := f.calls.Load(); n != 0 {
		t.Fatalf("the losing mirror built %d clients; it must not contact the cluster", n)
	}
	if len(rest.Created()) != 0 {
		t.Fatalf("the losing mirror must not create a mirror")
	}
	var got v1alpha1.KafkaMirrorTopic
	if err := env.cl.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: "mx-b"}, &got); err != nil {
		t.Fatalf("re-get mirror: %v", err)
	}
	if got.Status == nil || got.Status.Phase != v1alpha1.PhaseError {
		t.Fatalf("mirror status = %+v, want phase Error", got.Status)
	}
	vf := dupCond(got.Status.Conditions, v1alpha1.CondValidationFailed)
	if vf == nil || vf.Status != metav1.ConditionTrue || vf.Reason != reasonDuplicateIdentity {
		t.Fatalf("ValidationFailed = %+v, want True/DuplicateIdentity", vf)
	}
	if !strings.Contains(vf.Message, "KafkaTopic "+testNamespace+"/mx-a") {
		t.Fatalf("loser message must name the topic winner: %q", vf.Message)
	}
}

// TestEnvtestMirrorDeletionWithClusterGone: with the KafkaCluster deleted, a
// mirror CR (carrying a finalizer left by an older build) is deleted and its
// reconcile completes without error and without building a client; the CR is
// gone afterwards.
func TestEnvtestMirrorDeletionWithClusterGone(t *testing.T) {
	env := startEnv(t)
	defer env.stop()
	ctx := context.Background()

	cluster := newCluster(testNamespace, "md-cluster")
	if err := env.cl.Create(ctx, cluster); err != nil {
		t.Fatalf("create cluster: %v", err)
	}
	m := envMirror("md-mirror", "md-cluster", mirrorCtrlTopic)
	m.Finalizers = []string{FinalizerName}
	if err := env.cl.Create(ctx, m); err != nil {
		t.Fatalf("create mirror: %v", err)
	}

	k, rest := mirrorCtrlFixture("")
	f := &countingFactory{stubFactory: stubFactory{k: k, rest: rest}}
	r := &KafkaMirrorTopicReconciler{Client: env.cl, Scheme: env.scheme, Clients: f}
	reconcileFor(t, r, testNamespace, "md-mirror")
	if len(rest.Created()) != 1 {
		t.Fatalf("first reconcile should create the mirror, created = %d", len(rest.Created()))
	}

	if err := env.cl.Delete(ctx, cluster); err != nil {
		t.Fatalf("delete cluster: %v", err)
	}
	if err := env.cl.Delete(ctx, m); err != nil {
		t.Fatalf("delete mirror: %v", err)
	}
	before := f.calls.Load()

	res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNamespace, Name: "md-mirror"}})
	if err != nil {
		t.Fatalf("deletion reconcile returned error: %v", err)
	}
	if res != (ctrl.Result{}) {
		t.Fatalf("deletion reconcile result = %+v, want empty", res)
	}
	if f.calls.Load() != before {
		t.Fatalf("deletion built a client; it must never contact the cluster")
	}
	var gone v1alpha1.KafkaMirrorTopic
	if err := env.cl.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: "md-mirror"}, &gone); !apierrors.IsNotFound(err) {
		t.Fatalf("mirror should be gone; get err = %v, finalizers = %v", err, gone.Finalizers)
	}
	// A further reconcile of the gone CR is a clean no-op.
	reconcileFor(t, r, testNamespace, "md-mirror")
}
