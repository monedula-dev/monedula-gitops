//go:build envtest

// Package controller — envtest tests for the ConfigMap watch feature (spec §11.3).
//
// These tests prove that:
//  1. A LABELLED schema ConfigMap edit (Data change) promptly reconciles the
//     referencing KafkaTopic through the watch (not only at the 5-min resync).
//  2. A topic referencing an UNLABELLED ConfigMap gets
//     CondSchemaSourceUnwatched=True/ConfigMapNotLabeled on reconcile.
//  3. Adding the label clears the condition to False/AllWatchedOrNone on the
//     next reconcile (the label-add is a create-into-scope event for the
//     label-scoped cache, which re-enqueues the topic).
//
// Manager wiring: the test builds a manager inline mirroring manager.Run, with
// the identical cache/client/index/watch options:
//   - cache.Options.ByObject: label-scoped ConfigMap informer (only CMs with
//     gitops.monedula.dev/schema-source=true enter the cache / trigger watches)
//   - client.Options.CacheOptions.DisableFor[ConfigMap]: uncached reads so the
//     schema resolver can read ANY referenced CM body directly from the API
//   - RegisterSchemaConfigMapIndex: field index so mapConfigMapToTopics can
//     List topics by ConfigMap name
//   - ctrl.NewControllerManagedBy with .Watches(&corev1.ConfigMap{}, ...) using
//     the same mapConfigMapToTopics handler as SetupWithManager
//
// Observable signal for "reconciled again": a countingReconciler wrapper
// (atomic int64 counter) records every Reconcile invocation. The "prompt
// re-reconcile" assertion uses a 15-second timeout — far shorter than the
// 5-minute topicRequeueAfter — so only the ConfigMap watch can satisfy it.
package controller

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	kafkamock "github.com/monedula-dev/monedula-gitops/internal/kafka/mock"
	schemamock "github.com/monedula-dev/monedula-gitops/internal/schemaregistry/mock"
)

// watchEnv holds the envtest control-plane, the manager client, the reconcile
// counter, and a cancel function that stops both the manager and the env.
type watchEnv struct {
	cl      client.Client
	counter *countingReconciler
	cancel  func()
}

// startWatchManagerEnv starts an envtest control plane and a manager wired with
// the production ConfigMap watch options (§11.3). The manager runs in a
// background goroutine; call env.cancel() to stop it.
//
// COUPLING: the cache/client options, index registration, and For+Watches chain
// below MIRROR manager.Run + KafkaTopic SetupWithManager. If production adds a
// new ByObject entry, watch arm, or field index, this helper must be updated to
// match — otherwise the test silently diverges from production behaviour.
func startWatchManagerEnv(t *testing.T) *watchEnv {
	t.Helper()
	env := startEnv(t)

	// Replicate the cache + client options from manager.Run (spec §11.3):
	//   ByObject   — only label-bearing ConfigMaps enter the cache / informer.
	//   DisableFor — all ConfigMap reads bypass the cache so schema resolution
	//                can read unlabelled CMs directly from the API server.
	mgrOpts := ctrl.Options{
		Scheme:  env.scheme,
		Metrics: metricsserver.Options{BindAddress: "0"},
		Cache: cache.Options{
			ByObject: map[client.Object]cache.ByObject{
				&corev1.ConfigMap{}: {
					Label: labels.SelectorFromSet(labels.Set{
						SchemaSourceLabel: SchemaSourceLabelValue,
					}),
				},
			},
		},
		Client: client.Options{
			Cache: &client.CacheOptions{
				DisableFor: []client.Object{&corev1.ConfigMap{}},
			},
		},
	}

	mgr, err := ctrl.NewManager(env.cfg, mgrOpts)
	if err != nil {
		env.stop()
		t.Fatalf("building manager: %v", err)
	}

	ctx, cancelFn := context.WithCancel(context.Background())

	// Register the schema-configmap field index before mgr.Start (mirrors
	// the RegisterSchemaConfigMapIndex call in manager.Run).
	if err := RegisterSchemaConfigMapIndex(ctx, mgr); err != nil {
		cancelFn()
		env.stop()
		t.Fatalf("RegisterSchemaConfigMapIndex: %v", err)
	}

	inner := &KafkaTopicReconciler{
		Client:   mgr.GetClient(),
		Scheme:   env.scheme,
		Clients:  stubFactory{k: kafkamock.New(nil, nil), sr: schemamock.New()},
		Recorder: mgr.GetEventRecorder("kafkatopic-controller"),
	}
	counter := &countingReconciler{inner: inner}

	// Register the KafkaTopic controller manually, mirroring SetupWithManager:
	//   For KafkaTopic with watchEventFilter (GenerationChanged/Annotation/lifecycle)
	//   Watches ConfigMap with mapConfigMapToTopics (no generation predicate, so
	//     Data edits — which never bump generation — are delivered).
	// SkipNameValidation lets multiple test-manager instances coexist in the
	// same process (the controller name is process-global for metrics uniqueness).
	if err := ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.KafkaTopic{}, builder.WithPredicates(watchEventFilter())).
		Watches(&corev1.ConfigMap{}, handler.EnqueueRequestsFromMapFunc(inner.mapConfigMapToTopics)).
		WithOptions(controller.Options{SkipNameValidation: ptr.To(true)}).
		Complete(counter); err != nil {
		cancelFn()
		env.stop()
		t.Fatalf("building topic controller: %v", err)
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

	return &watchEnv{cl: env.cl, counter: counter, cancel: cancelAll}
}

// TestEnvtestConfigMapWatchPromptsReconcile proves that a Data edit on a
// LABELLED schema ConfigMap enqueues the referencing topic promptly (via the
// ConfigMap watch), well before the 5-minute resync cadence.
//
// Observable signal: reconcile counter. We record the count after the initial
// reconcile settles, edit the ConfigMap's Data, and assert the counter
// advances within 15 seconds (the resync is 5 minutes — only the watch fires).
func TestEnvtestConfigMapWatchPromptsReconcile(t *testing.T) {
	wenv := startWatchManagerEnv(t)
	defer wenv.cancel()
	ctx := context.Background()

	// Create a KafkaCluster (needed so the topic can resolve its clusterRef).
	if err := wenv.cl.Create(ctx, newCluster(testNamespace, "watch-cluster")); err != nil {
		t.Fatalf("create cluster: %v", err)
	}

	// Create a LABELLED schema ConfigMap.
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "watch-schema",
			Namespace: testNamespace,
			Labels:    map[string]string{SchemaSourceLabel: SchemaSourceLabelValue},
		},
		Data: map[string]string{"v.avsc": `{"type":"record","name":"V1","fields":[]}`},
	}
	if err := wenv.cl.Create(ctx, cm); err != nil {
		t.Fatalf("create ConfigMap: %v", err)
	}

	// Create a KafkaTopic referencing the ConfigMap via valueSchema.configMapKeyRef.
	topic := &v1alpha1.KafkaTopic{
		ObjectMeta: metav1.ObjectMeta{Name: "watch-topic", Namespace: testNamespace},
		Spec: v1alpha1.KafkaTopicSpec{
			ClusterRef: v1alpha1.ClusterRef{Name: "watch-cluster"},
			TopicName:  "watch.orders",
			Partitions: 3,
			Schema: &v1alpha1.TopicSchema{
				Format: "AVRO",
				ValueSchema: &v1alpha1.ValueFrom{
					ValueFrom: v1alpha1.ValueSource{
						ConfigMapKeyRef: &v1alpha1.SecretKeyRef{Name: "watch-schema", Key: "v.avsc"},
					},
				},
			},
		},
	}
	if err := wenv.cl.Create(ctx, topic); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	// Wait for the topic to reconcile at least once.
	waitFor(t, ctx, 30*time.Second, "initial topic reconcile", func() error {
		if wenv.counter.n.Load() < 1 {
			return errReason("no reconcile yet")
		}
		return nil
	})

	// Grace: let any trailing enqueue (e.g. finalizer-add update) drain.
	time.Sleep(2 * time.Second)
	countBefore := wenv.counter.n.Load()
	t.Logf("count before ConfigMap edit: %d", countBefore)

	// Edit the ConfigMap's Data — the ConfigMap watch should deliver this to
	// mapConfigMapToTopics which enqueues the topic.
	var latestCM corev1.ConfigMap
	if err := wenv.cl.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: "watch-schema"}, &latestCM); err != nil {
		t.Fatalf("get ConfigMap before edit: %v", err)
	}
	latestCM.Data["v.avsc"] = `{"type":"record","name":"V2","fields":[{"name":"id","type":"long"}]}`
	if err := wenv.cl.Update(ctx, &latestCM); err != nil {
		t.Fatalf("update ConfigMap: %v", err)
	}

	// Assert the reconcile counter advances within 15 seconds.
	// The periodic resync is 5 minutes — only the ConfigMap watch can fire this fast.
	waitFor(t, ctx, 15*time.Second, "topic re-reconciled after ConfigMap edit (watch fired)", func() error {
		if wenv.counter.n.Load() <= countBefore {
			return errReason("reconcile count has not advanced since ConfigMap edit")
		}
		return nil
	})
	t.Logf("count after ConfigMap edit: %d (delta: %d)", wenv.counter.n.Load(), wenv.counter.n.Load()-countBefore)
}

// TestEnvtestUnlabelledConfigMapSetsCondition proves that a topic referencing
// an UNLABELLED ConfigMap gets CondSchemaSourceUnwatched=True/ConfigMapNotLabeled.
func TestEnvtestUnlabelledConfigMapSetsCondition(t *testing.T) {
	wenv := startWatchManagerEnv(t)
	defer wenv.cancel()
	ctx := context.Background()

	if err := wenv.cl.Create(ctx, newCluster(testNamespace, "unwatched-cluster")); err != nil {
		t.Fatalf("create cluster: %v", err)
	}

	// ConfigMap WITHOUT the schema-source label.
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "unlabelled-schema",
			Namespace: testNamespace,
		},
		Data: map[string]string{"v.avsc": `{"type":"record","name":"U","fields":[]}`},
	}
	if err := wenv.cl.Create(ctx, cm); err != nil {
		t.Fatalf("create unlabelled ConfigMap: %v", err)
	}

	topic := &v1alpha1.KafkaTopic{
		ObjectMeta: metav1.ObjectMeta{Name: "unwatched-topic", Namespace: testNamespace},
		Spec: v1alpha1.KafkaTopicSpec{
			ClusterRef: v1alpha1.ClusterRef{Name: "unwatched-cluster"},
			TopicName:  "unwatched.orders",
			Partitions: 3,
			Schema: &v1alpha1.TopicSchema{
				Format: "AVRO",
				ValueSchema: &v1alpha1.ValueFrom{
					ValueFrom: v1alpha1.ValueSource{
						ConfigMapKeyRef: &v1alpha1.SecretKeyRef{Name: "unlabelled-schema", Key: "v.avsc"},
					},
				},
			},
		},
	}
	if err := wenv.cl.Create(ctx, topic); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	topicKey := types.NamespacedName{Namespace: testNamespace, Name: "unwatched-topic"}

	// Wait for CondSchemaSourceUnwatched=True/ConfigMapNotLabeled.
	waitFor(t, ctx, 30*time.Second, "CondSchemaSourceUnwatched=True/ConfigMapNotLabeled", func() error {
		var got v1alpha1.KafkaTopic
		if err := wenv.cl.Get(ctx, topicKey, &got); err != nil {
			return err
		}
		if got.Status == nil {
			return errReason("status not written yet")
		}
		cond := findCond(got.Status.Conditions, v1alpha1.CondSchemaSourceUnwatched)
		if cond == nil {
			return errReason("CondSchemaSourceUnwatched not set yet")
		}
		if cond.Status != metav1.ConditionTrue {
			return errReason("CondSchemaSourceUnwatched status = " + string(cond.Status) + ", want True")
		}
		if cond.Reason != "ConfigMapNotLabeled" {
			return errReason("reason = " + cond.Reason + ", want ConfigMapNotLabeled")
		}
		return nil
	})
}

// TestEnvtestAddLabelClearsCondition proves that adding the schema-source label
// to a previously-unlabelled ConfigMap clears CondSchemaSourceUnwatched to
// False/AllWatchedOrNone. The label-add is a create-into-scope event for the
// label-scoped cache, which triggers mapConfigMapToTopics → topic enqueue.
func TestEnvtestAddLabelClearsCondition(t *testing.T) {
	wenv := startWatchManagerEnv(t)
	defer wenv.cancel()
	ctx := context.Background()

	if err := wenv.cl.Create(ctx, newCluster(testNamespace, "label-add-cluster")); err != nil {
		t.Fatalf("create cluster: %v", err)
	}

	// Start unlabelled.
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "label-add-schema",
			Namespace: testNamespace,
		},
		Data: map[string]string{"v.avsc": `{"type":"record","name":"LA","fields":[]}`},
	}
	if err := wenv.cl.Create(ctx, cm); err != nil {
		t.Fatalf("create unlabelled ConfigMap: %v", err)
	}

	topic := &v1alpha1.KafkaTopic{
		ObjectMeta: metav1.ObjectMeta{Name: "label-add-topic", Namespace: testNamespace},
		Spec: v1alpha1.KafkaTopicSpec{
			ClusterRef: v1alpha1.ClusterRef{Name: "label-add-cluster"},
			TopicName:  "label.add.orders",
			Partitions: 3,
			Schema: &v1alpha1.TopicSchema{
				Format: "AVRO",
				ValueSchema: &v1alpha1.ValueFrom{
					ValueFrom: v1alpha1.ValueSource{
						ConfigMapKeyRef: &v1alpha1.SecretKeyRef{Name: "label-add-schema", Key: "v.avsc"},
					},
				},
			},
		},
	}
	if err := wenv.cl.Create(ctx, topic); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	topicKey := types.NamespacedName{Namespace: testNamespace, Name: "label-add-topic"}

	// Wait for initial reconcile to set condition True (unlabelled CM).
	waitFor(t, ctx, 30*time.Second, "initial CondSchemaSourceUnwatched=True", func() error {
		var got v1alpha1.KafkaTopic
		if err := wenv.cl.Get(ctx, topicKey, &got); err != nil {
			return err
		}
		if got.Status == nil {
			return errReason("status not written yet")
		}
		cond := findCond(got.Status.Conditions, v1alpha1.CondSchemaSourceUnwatched)
		if cond == nil {
			return errReason("condition not set yet")
		}
		if cond.Status != metav1.ConditionTrue {
			return errReason("want True, got " + string(cond.Status))
		}
		return nil
	})

	// Grace + snapshot counter.
	time.Sleep(500 * time.Millisecond)
	countBefore := wenv.counter.n.Load()
	t.Logf("count before label-add: %d", countBefore)

	// Add the schema-source label. This is a create-into-scope event for the
	// label-scoped informer, so the cache delivers it as a Create event to the
	// ConfigMap watch → mapConfigMapToTopics → topic enqueued.
	var latestCM corev1.ConfigMap
	if err := wenv.cl.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: "label-add-schema"}, &latestCM); err != nil {
		t.Fatalf("get CM before label add: %v", err)
	}
	if latestCM.Labels == nil {
		latestCM.Labels = map[string]string{}
	}
	latestCM.Labels[SchemaSourceLabel] = SchemaSourceLabelValue
	if err := wenv.cl.Update(ctx, &latestCM); err != nil {
		t.Fatalf("add label to ConfigMap: %v", err)
	}

	// Wait for a new reconcile AND CondSchemaSourceUnwatched=False.
	// 15 s — only the watch (label-add → create-into-scope event) can satisfy
	// this within that window, not the 5-minute resync.
	waitFor(t, ctx, 15*time.Second, "CondSchemaSourceUnwatched=False/AllWatchedOrNone after label-add", func() error {
		if wenv.counter.n.Load() <= countBefore {
			return errReason("no new reconcile since label-add")
		}
		var got v1alpha1.KafkaTopic
		if err := wenv.cl.Get(ctx, topicKey, &got); err != nil {
			return err
		}
		if got.Status == nil {
			return errReason("status nil")
		}
		cond := findCond(got.Status.Conditions, v1alpha1.CondSchemaSourceUnwatched)
		if cond == nil {
			return errReason("condition not set")
		}
		if cond.Status != metav1.ConditionFalse {
			return errReason("want False, got " + string(cond.Status))
		}
		if cond.Reason != "AllWatchedOrNone" {
			return errReason("want AllWatchedOrNone, got " + cond.Reason)
		}
		return nil
	})
	t.Logf("count after label-add: %d (delta: %d)", wenv.counter.n.Load(), wenv.counter.n.Load()-countBefore)
}
