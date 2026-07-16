//go:build envtest

package controller

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	kafkamock "github.com/monedula-dev/monedula-gitops/internal/kafka/mock"
	"github.com/monedula-dev/monedula-gitops/internal/mds"
	"github.com/monedula-dev/monedula-gitops/internal/schemaregistry"
	schemamock "github.com/monedula-dev/monedula-gitops/internal/schemaregistry/mock"
)

// delayingFactory wraps a ClientFactory, adding a fixed delay to every For
// call to simulate real Kafka connection/round-trip latency (For is invoked
// once per reconcile). The delay matters for the regression value of this
// test: with instant mocks the pre-fix hot loop SELF-QUENCHES, because
// metav1.Time has second granularity — consecutive sub-second reconciles
// write byte-identical statuses that the apiserver no-ops (no resourceVersion
// bump, no watch event, loop stops). A delay above one second guarantees each
// pre-fix write carries a fresh timestamp, so the write->event->reconcile
// loop sustains and the settle assertions below catch it deterministically.
type delayingFactory struct {
	inner ClientFactory
	delay time.Duration
}

func (f delayingFactory) For(ctx context.Context, c *v1alpha1.KafkaCluster) (kafka.AdminClient, schemaregistry.Client, func(), error) {
	time.Sleep(f.delay)
	return f.inner.For(ctx, c)
}

func (f delayingFactory) MDSFor(ctx context.Context, c *v1alpha1.KafkaCluster) (mds.Client, error) {
	return f.inner.MDSFor(ctx, c)
}

// countingReconciler wraps a reconciler and counts invocations, so the
// settling test can assert that a manager-driven controller does NOT keep
// re-reconciling after reaching steady state.
type countingReconciler struct {
	inner reconcile.Reconciler
	n     atomic.Int64
}

func (c *countingReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	c.n.Add(1)
	return c.inner.Reconcile(ctx, req)
}

// TestEnvtestManagerSettles is the manager-driven hot-loop regression test for
// review C3: it starts a REAL manager (watches + workqueue, not direct
// Reconcile calls) with the cluster + topic reconcilers behind the same
// watchEventFilter that SetupWithManager installs, creates a KafkaCluster and
// a KafkaTopic, waits for both to go Ready, and then asserts the system
// SETTLES: over a multi-second observation window there are no further status
// writes (resourceVersion stays put) and at most one trailing reconcile per
// object. Against the pre-fix code (unconditional status writes + unfiltered
// watch) each status write re-enqueued its own reconcile and this test fails
// with hundreds of reconciles in the window.
func TestEnvtestManagerSettles(t *testing.T) {
	env := startEnv(t)
	defer env.stop()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mgr, err := ctrl.NewManager(env.cfg, ctrl.Options{
		Scheme:  env.scheme,
		Metrics: metricsserver.Options{BindAddress: "0"}, // no metrics listener in tests
	})
	if err != nil {
		t.Fatalf("building manager: %v", err)
	}

	factory := delayingFactory{
		inner: stubFactory{k: kafkamock.New(nil, nil), sr: schemamock.New()},
		delay: 1200 * time.Millisecond,
	}

	// Register the controllers through the manager exactly as SetupWithManager
	// does (same For + watchEventFilter wiring), with a counting wrapper around
	// each reconciler so the test can observe reconcile activity.
	clusterCount := &countingReconciler{inner: &KafkaClusterReconciler{
		Client: mgr.GetClient(), Scheme: env.scheme, Clients: factory,
		Recorder: mgr.GetEventRecorder("kafkacluster-controller"),
	}}
	// SkipNameValidation lets `go test -count=N` re-register the controllers
	// (names are otherwise process-global for metrics uniqueness).
	if err := ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.KafkaCluster{}).
		WithEventFilter(watchEventFilter()).
		WithOptions(controller.Options{SkipNameValidation: ptr.To(true)}).
		Complete(clusterCount); err != nil {
		t.Fatalf("building cluster controller: %v", err)
	}
	topicCount := &countingReconciler{inner: &KafkaTopicReconciler{
		Client: mgr.GetClient(), Scheme: env.scheme, Clients: factory,
		Recorder: mgr.GetEventRecorder("kafkatopic-controller"),
	}}
	if err := ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.KafkaTopic{}).
		WithEventFilter(watchEventFilter()).
		WithOptions(controller.Options{SkipNameValidation: ptr.To(true)}).
		Complete(topicCount); err != nil {
		t.Fatalf("building topic controller: %v", err)
	}

	mgrDone := make(chan error, 1)
	go func() { mgrDone <- mgr.Start(ctx) }()
	defer func() {
		cancel()
		if err := <-mgrDone; err != nil {
			t.Errorf("manager exited with error: %v", err)
		}
	}()

	if err := env.cl.Create(ctx, newCluster(testNamespace, "settle-cluster")); err != nil {
		t.Fatalf("create cluster: %v", err)
	}
	topic := &v1alpha1.KafkaTopic{
		ObjectMeta: metav1.ObjectMeta{Name: "settle-topic", Namespace: testNamespace},
		Spec: v1alpha1.KafkaTopicSpec{
			ClusterRef: v1alpha1.ClusterRef{Name: "settle-cluster"},
			TopicName:  "settle.orders",
			Partitions: 3,
		},
	}
	if err := env.cl.Create(ctx, topic); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	clusterKey := types.NamespacedName{Namespace: testNamespace, Name: "settle-cluster"}
	topicKey := types.NamespacedName{Namespace: testNamespace, Name: "settle-topic"}

	// Wait for both objects to reach Ready via the manager-driven reconciles.
	waitFor(t, ctx, 30*time.Second, "cluster+topic Ready", func() error {
		var c v1alpha1.KafkaCluster
		if err := env.cl.Get(ctx, clusterKey, &c); err != nil {
			return err
		}
		if c.Status == nil || c.Status.Phase != v1alpha1.PhaseReady {
			return fmt.Errorf("cluster phase = %v", phaseOf(c.Status == nil, func() string { return c.Status.Phase }))
		}
		var tp v1alpha1.KafkaTopic
		if err := env.cl.Get(ctx, topicKey, &tp); err != nil {
			return err
		}
		if tp.Status == nil || tp.Status.Phase != v1alpha1.PhaseReady {
			return fmt.Errorf("topic phase = %v", phaseOf(tp.Status == nil, func() string { return tp.Status.Phase }))
		}
		return nil
	})

	// Grace period: let any in-flight re-enqueue (e.g. from the finalizer-add
	// update, which legitimately passes the filter) drain before snapshotting.
	time.Sleep(2 * time.Second)

	var clusterBefore v1alpha1.KafkaCluster
	var topicBefore v1alpha1.KafkaTopic
	if err := env.cl.Get(ctx, clusterKey, &clusterBefore); err != nil {
		t.Fatalf("get cluster before window: %v", err)
	}
	if err := env.cl.Get(ctx, topicKey, &topicBefore); err != nil {
		t.Fatalf("get topic before window: %v", err)
	}
	clusterReconciles := clusterCount.n.Load()
	topicReconciles := topicCount.n.Load()

	// Observation window. Both periodic RequeueAfters are 5 minutes, so within
	// this window a settled controller performs (at most one trailing) reconcile
	// and zero writes. A hot loop performs hundreds.
	time.Sleep(5 * time.Second)

	var clusterAfter v1alpha1.KafkaCluster
	var topicAfter v1alpha1.KafkaTopic
	if err := env.cl.Get(ctx, clusterKey, &clusterAfter); err != nil {
		t.Fatalf("get cluster after window: %v", err)
	}
	if err := env.cl.Get(ctx, topicKey, &topicAfter); err != nil {
		t.Fatalf("get topic after window: %v", err)
	}

	t.Logf("settle window: cluster reconciles +%d (rv %s -> %s), topic reconciles +%d (rv %s -> %s); totals cluster=%d topic=%d",
		clusterCount.n.Load()-clusterReconciles, clusterBefore.ResourceVersion, clusterAfter.ResourceVersion,
		topicCount.n.Load()-topicReconciles, topicBefore.ResourceVersion, topicAfter.ResourceVersion,
		clusterCount.n.Load(), topicCount.n.Load())

	if clusterBefore.ResourceVersion != clusterAfter.ResourceVersion {
		t.Errorf("cluster did not settle: resourceVersion %s -> %s",
			clusterBefore.ResourceVersion, clusterAfter.ResourceVersion)
	}
	if topicBefore.ResourceVersion != topicAfter.ResourceVersion {
		t.Errorf("topic did not settle: resourceVersion %s -> %s",
			topicBefore.ResourceVersion, topicAfter.ResourceVersion)
	}
	if d := clusterCount.n.Load() - clusterReconciles; d > 1 {
		t.Errorf("cluster reconciled %d times during the settle window, want <= 1", d)
	}
	if d := topicCount.n.Load() - topicReconciles; d > 1 {
		t.Errorf("topic reconciled %d times during the settle window, want <= 1", d)
	}
}

// waitFor polls cond every 100ms until it returns nil or the timeout elapses.
func waitFor(t *testing.T, ctx context.Context, timeout time.Duration, what string, cond func() error) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last error
	for time.Now().Before(deadline) {
		if last = cond(); last == nil {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("context done waiting for %s: %v (last: %v)", what, ctx.Err(), last)
		case <-time.After(100 * time.Millisecond):
		}
	}
	t.Fatalf("timed out waiting for %s (last: %v)", what, last)
}

// phaseOf renders a status phase for error messages without nil-dereferencing.
func phaseOf(isNil bool, phase func() string) string {
	if isNil {
		return "<no status>"
	}
	return phase()
}
