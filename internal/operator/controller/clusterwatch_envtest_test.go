//go:build envtest

// Package controller — envtest tests for the KafkaCluster watch on the
// data-plane controllers (review I2).
//
// These prove, with a REAL manager (watches + workqueue, not direct Reconcile
// calls):
//
//  1. A KafkaTopic created BEFORE its KafkaCluster recovers PROMPTLY (within a
//     window strictly smaller than the pending error-backoff gap) once the
//     cluster CR is created — the watch, not backoff or the 5-min resync,
//     drives the recovery.
//  2. A KafkaRoleBinding on a cluster WITHOUT authorization.mds lands in
//     MDSNotConfigured, and reconciles to Ready promptly after the cluster
//     spec gains an MDS config. This is the strongest proof available: the
//     MDSNotConfigured reconcile returns nil error (no backoff retries are
//     pending) and its RequeueAfter is 5 minutes, so ANY reconcile inside the
//     10s assertion window can only come from the KafkaCluster watch. Before
//     the I2 fix the binding was permanently wedged (no requeue at all).
//  3. A status-only KafkaCluster update does NOT re-reconcile dependents
//     (clusterWatchPredicate is generation-gated for updates).
//
// COUPLING: the For + Watches chains MIRROR the KafkaCluster-watch arms of the
// controllers' SetupWithManager (primary predicate watchEventFilter, cluster
// watch behind clusterWatchPredicate, map funcs mapClusterToTopics /
// mapClusterToRoleBindings) and the index registration mirrors manager.Run's
// operatorwebhook.RegisterIndexes call. The orthogonal Secret/ConfigMap watch
// arms are deliberately omitted (covered by their own envtest files). If
// production changes the cluster-watch wiring, update this harness to match.
package controller

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	kafkamock "github.com/monedula-dev/monedula-gitops/internal/kafka/mock"
	"github.com/monedula-dev/monedula-gitops/internal/mds"
	mdsmock "github.com/monedula-dev/monedula-gitops/internal/mds/mock"
	operatorwebhook "github.com/monedula-dev/monedula-gitops/internal/operator/webhook"
	"github.com/monedula-dev/monedula-gitops/internal/schemaregistry"
	schemamock "github.com/monedula-dev/monedula-gitops/internal/schemaregistry/mock"
)

// mdsGatedFactory mirrors DefaultClientFactory's MDSFor contract: (nil, nil)
// when the cluster has no authorization.mds, the configured mock otherwise.
// stubFactory cannot express this (it returns its mds field unconditionally),
// and the MDSNotConfigured test hinges exactly on this gate flipping when the
// cluster spec gains an MDS config.
type mdsGatedFactory struct {
	k   kafka.AdminClient
	sr  schemaregistry.Client
	mds mds.Client
}

func (f mdsGatedFactory) For(context.Context, *v1alpha1.KafkaCluster) (kafka.AdminClient, schemaregistry.Client, func(), error) {
	return f.k, f.sr, func() {}, nil
}

func (f mdsGatedFactory) MDSFor(_ context.Context, c *v1alpha1.KafkaCluster) (mds.Client, error) {
	if c.Spec.Authorization == nil || c.Spec.Authorization.MDS == nil {
		return nil, nil
	}
	return f.mds, nil
}

// clusterWatchEnv holds the env, a client, and per-kind reconcile counters.
type clusterWatchEnv struct {
	cl         *testEnv
	topicCount *countingReconciler
	rbCount    *countingReconciler
	cancel     func()
}

// startClusterWatchEnv starts a manager with the KafkaTopic and
// KafkaRoleBinding controllers wired exactly like SetupWithManager's primary +
// KafkaCluster-watch arms, wrapped in counting reconcilers.
func startClusterWatchEnv(t *testing.T) *clusterWatchEnv {
	t.Helper()
	env := startEnv(t)

	mgr, err := ctrl.NewManager(env.cfg, ctrl.Options{
		Scheme:  env.scheme,
		Metrics: metricsserver.Options{BindAddress: "0"},
	})
	if err != nil {
		env.stop()
		t.Fatalf("building manager: %v", err)
	}

	ctx, cancelFn := context.WithCancel(context.Background())

	// Same index registration as manager.Run: the cluster map funcs List with
	// MatchingFields on these per-type indexes.
	if err := operatorwebhook.RegisterIndexes(ctx, mgr); err != nil {
		cancelFn()
		env.stop()
		t.Fatalf("RegisterIndexes: %v", err)
	}

	topicInner := &KafkaTopicReconciler{
		Client:   mgr.GetClient(),
		Scheme:   env.scheme,
		Clients:  stubFactory{k: kafkamock.New(nil, nil), sr: schemamock.New()},
		Recorder: mgr.GetEventRecorder("kafkatopic-controller"),
	}
	topicCount := &countingReconciler{inner: topicInner}
	if err := ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.KafkaTopic{}, builder.WithPredicates(watchEventFilter())).
		Watches(&v1alpha1.KafkaCluster{}, handler.EnqueueRequestsFromMapFunc(topicInner.mapClusterToTopics),
			builder.WithPredicates(clusterWatchPredicate())).
		WithOptions(controller.Options{SkipNameValidation: ptr.To(true)}).
		Complete(topicCount); err != nil {
		cancelFn()
		env.stop()
		t.Fatalf("building topic controller: %v", err)
	}

	rbInner := &KafkaRoleBindingReconciler{
		Client:   mgr.GetClient(),
		Scheme:   env.scheme,
		Clients:  mdsGatedFactory{k: kafkamock.New(nil, nil), sr: schemamock.New(), mds: mdsmock.New()},
		Recorder: mgr.GetEventRecorder("kafkarolebinding-controller"),
	}
	rbCount := &countingReconciler{inner: rbInner}
	if err := ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.KafkaRoleBinding{}, builder.WithPredicates(watchEventFilter())).
		Watches(&v1alpha1.KafkaCluster{}, handler.EnqueueRequestsFromMapFunc(rbInner.mapClusterToRoleBindings),
			builder.WithPredicates(clusterWatchPredicate())).
		WithOptions(controller.Options{SkipNameValidation: ptr.To(true)}).
		Complete(rbCount); err != nil {
		cancelFn()
		env.stop()
		t.Fatalf("building rolebinding controller: %v", err)
	}

	mgrDone := make(chan error, 1)
	go func() { mgrDone <- mgr.Start(ctx) }()

	cancelAll := func() {
		cancelFn()
		if err := <-mgrDone; err != nil {
			t.Errorf("manager exited with error: %v", err)
		}
		env.stop()
	}
	return &clusterWatchEnv{cl: env, topicCount: topicCount, rbCount: rbCount, cancel: cancelAll}
}

// waitQuiet polls counter every 100ms and returns once it has not advanced for
// quiet (or fails at deadline). Used to anchor to the workqueue's actual
// error-backoff schedule: after a quiet stretch of length q, the current
// backoff gap G must exceed q, and since gaps are 5ms*2^k the smallest gap
// exceeding 11s is ~20.5s — so at return, the NEXT backoff retry is at least
// (20.5s - 11s - poll granularity) ≈ 9.4s away.
// The 5ms-base/doubling schedule comes from controller-runtime's default rate
// limiter (workqueue.DefaultControllerRateLimiter); re-derive the 11s/8s
// pairing below if that default or our controller Options ever change.
func waitQuiet(t *testing.T, counter *countingReconciler, quiet, deadline time.Duration) {
	t.Helper()
	last := counter.n.Load()
	lastChange := time.Now()
	end := time.Now().Add(deadline)
	for time.Since(lastChange) < quiet {
		if time.Now().After(end) {
			t.Fatalf("reconciles never went quiet for %v within %v", quiet, deadline)
		}
		time.Sleep(100 * time.Millisecond)
		if n := counter.n.Load(); n != last {
			last, lastChange = n, time.Now()
		}
	}
}

// TestEnvtestTopicRecoversOnClusterCreate: a KafkaTopic created BEFORE its
// KafkaCluster errors with ClusterNotFound; once its error-backoff gap has
// grown past ~20s (anchored via waitQuiet), creating the cluster CR must make
// the topic Ready within 8s — strictly inside the pending backoff gap, so only
// the KafkaCluster watch can have driven the reconcile. It also asserts a
// status-only cluster update does NOT re-reconcile the topic (predicate test).
func TestEnvtestTopicRecoversOnClusterCreate(t *testing.T) {
	senv := startClusterWatchEnv(t)
	defer senv.cancel()
	ctx := context.Background()

	topic := &v1alpha1.KafkaTopic{
		ObjectMeta: metav1.ObjectMeta{Name: "early-topic", Namespace: testNamespace},
		Spec: v1alpha1.KafkaTopicSpec{
			ClusterRef: v1alpha1.ClusterRef{Name: "late-cluster"},
			TopicName:  "early.orders",
			Partitions: 1,
		},
	}
	if err := senv.cl.cl.Create(ctx, topic); err != nil {
		t.Fatalf("create topic: %v", err)
	}
	topicKey := types.NamespacedName{Namespace: testNamespace, Name: "early-topic"}

	// The topic must be reconciled and NOT Ready (ClusterNotFound).
	waitFor(t, ctx, 30*time.Second, "topic in ClusterNotFound error state", func() error {
		var got v1alpha1.KafkaTopic
		if err := senv.cl.cl.Get(ctx, topicKey, &got); err != nil {
			return err
		}
		if got.Status == nil {
			return errReason("status not written yet")
		}
		if got.Status.Phase != v1alpha1.PhaseError {
			return errReason("phase " + got.Status.Phase + ", want Error")
		}
		cond := findCond(got.Status.Conditions, v1alpha1.CondReady)
		if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != "ClusterNotFound" {
			return errReason("Ready condition not False/ClusterNotFound yet")
		}
		return nil
	})

	// Anchor to the backoff schedule: once reconciles have been quiet for 11s,
	// the current backoff gap is >= ~20.5s and the next backoff retry is >= ~9.4s
	// away — beyond the 8s recovery window asserted below.
	waitQuiet(t, senv.topicCount, 11*time.Second, 90*time.Second)

	if err := senv.cl.cl.Create(ctx, newCluster(testNamespace, "late-cluster")); err != nil {
		t.Fatalf("create cluster: %v", err)
	}
	created := time.Now()

	waitFor(t, ctx, 8*time.Second, "topic Ready promptly after cluster create (watch-driven)", func() error {
		var got v1alpha1.KafkaTopic
		if err := senv.cl.cl.Get(ctx, topicKey, &got); err != nil {
			return err
		}
		if got.Status == nil || got.Status.Phase != v1alpha1.PhaseReady {
			return errReason("topic not Ready yet")
		}
		return nil
	})
	t.Logf("topic Ready %v after cluster create (backoff gap was >= ~9.4s)", time.Since(created))

	// Predicate check: a status-only cluster update must NOT fan out. Let any
	// trailing enqueue (finalizer add) drain first.
	time.Sleep(2 * time.Second)
	before := senv.topicCount.n.Load()
	var cluster v1alpha1.KafkaCluster
	if err := senv.cl.cl.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: "late-cluster"}, &cluster); err != nil {
		t.Fatalf("get cluster: %v", err)
	}
	now := metav1.Now()
	cluster.Status = &v1alpha1.KafkaClusterStatus{Phase: v1alpha1.PhaseReady, LastCheckedTime: &now}
	if err := senv.cl.cl.Status().Update(ctx, &cluster); err != nil {
		t.Fatalf("status-update cluster: %v", err)
	}
	time.Sleep(3 * time.Second)
	if after := senv.topicCount.n.Load(); after != before {
		t.Fatalf("status-only cluster update re-reconciled dependents: count %d -> %d", before, after)
	}
}

// TestEnvtestRoleBindingUnwedgesOnMDSConfigured: a KafkaRoleBinding whose
// cluster lacks authorization.mds lands in MDSNotConfigured (a nil-error
// outcome: no backoff retry is ever pending, and the requeue fallback is 5
// minutes). Adding authorization.mds to the cluster spec must reconcile the
// binding to Ready within 10s — only the KafkaCluster watch can deliver that.
func TestEnvtestRoleBindingUnwedgesOnMDSConfigured(t *testing.T) {
	senv := startClusterWatchEnv(t)
	defer senv.cancel()
	ctx := context.Background()

	// Cluster WITHOUT authorization.mds.
	if err := senv.cl.cl.Create(ctx, newCluster(testNamespace, "mds-later-cluster")); err != nil {
		t.Fatalf("create cluster: %v", err)
	}
	rb := newRoleBinding(testNamespace, "wedged-rb", "mds-later-cluster")
	if err := senv.cl.cl.Create(ctx, rb); err != nil {
		t.Fatalf("create role binding: %v", err)
	}
	rbKey := types.NamespacedName{Namespace: testNamespace, Name: "wedged-rb"}

	waitFor(t, ctx, 30*time.Second, "binding in MDSNotConfigured state", func() error {
		var got v1alpha1.KafkaRoleBinding
		if err := senv.cl.cl.Get(ctx, rbKey, &got); err != nil {
			return err
		}
		if got.Status == nil {
			return errReason("status not written yet")
		}
		cond := findCond(got.Status.Conditions, v1alpha1.CondReady)
		if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != "MDSNotConfigured" {
			return errReason("Ready condition not False/MDSNotConfigured yet")
		}
		return nil
	})

	// Let the workqueue drain fully (the MDSNotConfigured outcome returns nil
	// error, so nothing is pending except the 5-minute resync).
	time.Sleep(2 * time.Second)

	// Add authorization.mds — a spec change (generation bump) the cluster watch
	// must fan out to the binding.
	var cluster v1alpha1.KafkaCluster
	if err := senv.cl.cl.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: "mds-later-cluster"}, &cluster); err != nil {
		t.Fatalf("get cluster: %v", err)
	}
	cluster.Spec.Authorization = &v1alpha1.AuthorizationConfig{
		MDS: &v1alpha1.MDSConfig{
			Endpoint: "http://mds:8090",
			Clusters: v1alpha1.MDSClusters{KafkaCluster: "lkc-test"},
		},
	}
	if err := senv.cl.cl.Update(ctx, &cluster); err != nil {
		t.Fatalf("update cluster with mds config: %v", err)
	}
	updated := time.Now()

	waitFor(t, ctx, 10*time.Second, "binding Ready promptly after mds config added (watch-driven)", func() error {
		var got v1alpha1.KafkaRoleBinding
		if err := senv.cl.cl.Get(ctx, rbKey, &got); err != nil {
			return err
		}
		if got.Status == nil || got.Status.Phase != v1alpha1.PhaseReady {
			return errReason("binding not Ready yet")
		}
		if s := condStatus(got.Status.Conditions, v1alpha1.CondRoleBindingSynced); s != metav1.ConditionTrue {
			return errReason("RoleBindingSynced not True yet")
		}
		return nil
	})
	t.Logf("binding Ready %v after authorization.mds added (fallback requeue is 5m)", time.Since(updated))
}

// TestEnvtestEventsWrittenViaEventsAPI pins the v0.38 recorder migration: the
// manager-wired recorder (mgr.GetEventRecorder) must persist controller events
// through the events.k8s.io/v1 API group. A KafkaTopic referencing a missing
// cluster reconciles into ClusterNotFound, whose Warning event must be
// listable via events.k8s.io/v1 with the topic as the regarding object, the
// controller name as reportingController, and the reconcile-path action.
func TestEnvtestEventsWrittenViaEventsAPI(t *testing.T) {
	senv := startClusterWatchEnv(t)
	defer senv.cancel()
	ctx := context.Background()

	topic := &v1alpha1.KafkaTopic{
		ObjectMeta: metav1.ObjectMeta{Name: "eventful-topic", Namespace: testNamespace},
		Spec: v1alpha1.KafkaTopicSpec{
			ClusterRef: v1alpha1.ClusterRef{Name: "no-such-cluster"},
			TopicName:  "eventful.orders",
			Partitions: 1,
		},
	}
	if err := senv.cl.cl.Create(ctx, topic); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	// The broadcaster delivers to the sink asynchronously; poll the
	// events.k8s.io/v1 list until the topic's ClusterNotFound event lands.
	waitFor(t, ctx, 30*time.Second, "ClusterNotFound event via events.k8s.io/v1", func() error {
		var evs eventsv1.EventList
		if err := senv.cl.cl.List(ctx, &evs, client.InNamespace(testNamespace)); err != nil {
			return err
		}
		for _, ev := range evs.Items {
			if ev.Reason != "ClusterNotFound" ||
				ev.Regarding.Kind != "KafkaTopic" || ev.Regarding.Name != "eventful-topic" {
				continue
			}
			if ev.Type != corev1.EventTypeWarning {
				return errReason("event found but type " + ev.Type + ", want Warning")
			}
			if ev.ReportingController != "kafkatopic-controller" {
				return errReason("event found but reportingController " +
					ev.ReportingController + ", want kafkatopic-controller")
			}
			if ev.Action != actionReconcile {
				return errReason("event found but action " + ev.Action + ", want " + actionReconcile)
			}
			return nil
		}
		return errReason("no ClusterNotFound event for KafkaTopic eventful-topic yet")
	})
}
