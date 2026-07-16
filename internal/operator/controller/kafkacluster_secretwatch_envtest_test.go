//go:build envtest

// Package controller — envtest tests for the Secret watch feature (spec §11.4).
//
// These prove:
//  1. A LABELLED credential Secret change promptly reconciles BOTH the
//     referencing KafkaCluster AND a KafkaTopic on that cluster (the 2-hop
//     fan-out), well before the 5-min resync.
//  2. A cluster referencing an UNLABELLED Secret gets
//     CredentialSourceUnwatched=True/SecretNotLabeled and STILL reconciles
//     successfully (uncached read regression: an unlabelled Secret is resolvable
//     because the manager client reads Secrets via DisableFor, not the cache).
//  3. Adding the label clears the condition to False/AllWatchedOrNone.
//
// COUPLING: the cache/client options, index registration, and For+Watches chains
// MIRROR the SECRET-WATCH paths of manager.Run + the controllers'
// SetupWithManager. The harness is intentionally scoped to the Secret watch and
// deliberately OMITS the orthogonal ConfigMap-watch wiring (the ConfigMap
// ByObject entry + DisableFor[ConfigMap], RegisterSchemaConfigMapIndex, and the
// topic controller's ConfigMap Watches arm — all covered by
// kafkatopic_configmapwatch_envtest_test.go). If production changes the Secret
// cache scoping, the cluster secret-refs index, or either controller's Secret
// Watches arm, update this helper to match.
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

// secretWatchEnv holds the env, manager client, and per-kind reconcile counters.
type secretWatchEnv struct {
	cl           client.Client
	clusterCount *countingReconciler
	topicCount   *countingReconciler
	cancel       func()
}

func startSecretWatchEnv(t *testing.T) *secretWatchEnv {
	t.Helper()
	env := startEnv(t)

	mgrOpts := ctrl.Options{
		Scheme:  env.scheme,
		Metrics: metricsserver.Options{BindAddress: "0"},
		Cache: cache.Options{
			ByObject: map[client.Object]cache.ByObject{
				&corev1.Secret{}: {
					Label: labels.SelectorFromSet(labels.Set{
						CredentialSourceLabel: CredentialSourceLabelValue,
					}),
				},
			},
		},
		Client: client.Options{
			Cache: &client.CacheOptions{
				DisableFor: []client.Object{&corev1.Secret{}},
			},
		},
	}

	mgr, err := ctrl.NewManager(env.cfg, mgrOpts)
	if err != nil {
		env.stop()
		t.Fatalf("building manager: %v", err)
	}

	ctx, cancelFn := context.WithCancel(context.Background())

	if err := RegisterClusterSecretNamesIndex(ctx, mgr); err != nil {
		cancelFn()
		env.stop()
		t.Fatalf("RegisterClusterSecretNamesIndex: %v", err)
	}

	clusterInner := &KafkaClusterReconciler{
		Client:   mgr.GetClient(),
		Scheme:   env.scheme,
		Clients:  stubFactory{k: kafkamock.New(nil, nil), sr: schemamock.New()},
		Recorder: mgr.GetEventRecorder("kafkacluster-controller"),
	}
	clusterCount := &countingReconciler{inner: clusterInner}
	if err := ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.KafkaCluster{}, builder.WithPredicates(watchEventFilter())).
		Watches(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(clusterInner.mapSecretToClusters)).
		WithOptions(controller.Options{SkipNameValidation: ptr.To(true)}).
		Complete(clusterCount); err != nil {
		cancelFn()
		env.stop()
		t.Fatalf("building cluster controller: %v", err)
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
		Watches(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(topicInner.mapSecretToTopics)).
		WithOptions(controller.Options{SkipNameValidation: ptr.To(true)}).
		Complete(topicCount); err != nil {
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
	return &secretWatchEnv{cl: env.cl, clusterCount: clusterCount, topicCount: topicCount, cancel: cancelAll}
}

// credentialSecret builds an Opaque Secret with username/password data; labelled
// opts it into the credential-source watch.
func credentialSecret(ns, name string, labelled bool) *corev1.Secret {
	s := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Data:       map[string][]byte{"username": []byte("u"), "password": []byte("p")},
	}
	if labelled {
		s.Labels = map[string]string{CredentialSourceLabel: CredentialSourceLabelValue}
	}
	return s
}

func TestEnvtestSecretWatchFansOut(t *testing.T) {
	senv := startSecretWatchEnv(t)
	defer senv.cancel()
	ctx := context.Background()

	if err := senv.cl.Create(ctx, credentialSecret(testNamespace, "kafka-creds", true)); err != nil {
		t.Fatalf("create secret: %v", err)
	}
	if err := senv.cl.Create(ctx, clusterWithSecret(testNamespace, "fanout-cluster", "kafka-creds")); err != nil {
		t.Fatalf("create cluster: %v", err)
	}
	topic := &v1alpha1.KafkaTopic{
		ObjectMeta: metav1.ObjectMeta{Name: "fanout-topic", Namespace: testNamespace},
		Spec: v1alpha1.KafkaTopicSpec{
			ClusterRef: v1alpha1.ClusterRef{Name: "fanout-cluster"},
			TopicName:  "fanout.orders",
			Partitions: 1,
		},
	}
	if err := senv.cl.Create(ctx, topic); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	waitFor(t, ctx, 30*time.Second, "initial reconciles", func() error {
		if senv.clusterCount.n.Load() < 1 || senv.topicCount.n.Load() < 1 {
			return errReason("not reconciled yet")
		}
		return nil
	})
	// Grace: let any trailing enqueue (e.g. finalizer-add update) drain before
	// snapshotting the counters, so the post-rotation delta is attributable only
	// to the Secret watch.
	time.Sleep(2 * time.Second)
	clusterBefore := senv.clusterCount.n.Load()
	topicBefore := senv.topicCount.n.Load()
	t.Logf("counts before Secret rotation: cluster=%d topic=%d", clusterBefore, topicBefore)

	var s corev1.Secret
	if err := senv.cl.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: "kafka-creds"}, &s); err != nil {
		t.Fatalf("get secret: %v", err)
	}
	s.Data["password"] = []byte("rotated")
	if err := senv.cl.Update(ctx, &s); err != nil {
		t.Fatalf("update secret: %v", err)
	}

	waitFor(t, ctx, 15*time.Second, "cluster + topic re-reconciled after Secret rotation", func() error {
		if senv.clusterCount.n.Load() <= clusterBefore {
			return errReason("cluster not re-reconciled")
		}
		if senv.topicCount.n.Load() <= topicBefore {
			return errReason("topic not re-reconciled")
		}
		return nil
	})
	t.Logf("counts after Secret rotation: cluster=%d topic=%d", senv.clusterCount.n.Load(), senv.topicCount.n.Load())
}

func TestEnvtestUnlabelledSecretSetsConditionAndResolves(t *testing.T) {
	senv := startSecretWatchEnv(t)
	defer senv.cancel()
	ctx := context.Background()

	if err := senv.cl.Create(ctx, credentialSecret(testNamespace, "unlabelled-creds", false)); err != nil {
		t.Fatalf("create secret: %v", err)
	}
	if err := senv.cl.Create(ctx, clusterWithSecret(testNamespace, "unwatched-cluster", "unlabelled-creds")); err != nil {
		t.Fatalf("create cluster: %v", err)
	}
	clusterKey := types.NamespacedName{Namespace: testNamespace, Name: "unwatched-cluster"}

	waitFor(t, ctx, 30*time.Second, "CredentialSourceUnwatched=True + cluster reconciled (uncached read)", func() error {
		var got v1alpha1.KafkaCluster
		if err := senv.cl.Get(ctx, clusterKey, &got); err != nil {
			return err
		}
		if got.Status == nil {
			return errReason("status not written yet")
		}
		cond := findCond(got.Status.Conditions, v1alpha1.CondCredentialSourceUnwatched)
		if cond == nil {
			return errReason("condition not set yet")
		}
		if cond.Status != metav1.ConditionTrue || cond.Reason != "SecretNotLabeled" {
			return errReason("want True/SecretNotLabeled, got " + string(cond.Status) + "/" + cond.Reason)
		}
		if got.Status.Phase == v1alpha1.PhaseError {
			return errReason("cluster in Error phase — unlabelled Secret failed to resolve")
		}
		return nil
	})
}

func TestEnvtestAddLabelClearsClusterCondition(t *testing.T) {
	senv := startSecretWatchEnv(t)
	defer senv.cancel()
	ctx := context.Background()

	if err := senv.cl.Create(ctx, credentialSecret(testNamespace, "label-add-creds", false)); err != nil {
		t.Fatalf("create secret: %v", err)
	}
	if err := senv.cl.Create(ctx, clusterWithSecret(testNamespace, "label-add-cluster", "label-add-creds")); err != nil {
		t.Fatalf("create cluster: %v", err)
	}
	clusterKey := types.NamespacedName{Namespace: testNamespace, Name: "label-add-cluster"}

	waitFor(t, ctx, 30*time.Second, "initial CredentialSourceUnwatched=True", func() error {
		var got v1alpha1.KafkaCluster
		if err := senv.cl.Get(ctx, clusterKey, &got); err != nil {
			return err
		}
		if got.Status == nil {
			return errReason("status nil")
		}
		cond := findCond(got.Status.Conditions, v1alpha1.CondCredentialSourceUnwatched)
		if cond == nil || cond.Status != metav1.ConditionTrue {
			return errReason("condition not True yet")
		}
		return nil
	})

	// Snapshot the cluster reconcile count before the label-add so the assertion
	// below proves the label-add (a create-into-scope event for the label-scoped
	// cache) actually re-enqueued the cluster within the 15s window — not the
	// 5-min resync, and not a stale trailing enqueue.
	time.Sleep(time.Second)
	clusterBefore := senv.clusterCount.n.Load()
	t.Logf("cluster count before label-add: %d", clusterBefore)

	var s corev1.Secret
	if err := senv.cl.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: "label-add-creds"}, &s); err != nil {
		t.Fatalf("get secret: %v", err)
	}
	if s.Labels == nil {
		s.Labels = map[string]string{}
	}
	s.Labels[CredentialSourceLabel] = CredentialSourceLabelValue
	if err := senv.cl.Update(ctx, &s); err != nil {
		t.Fatalf("add label: %v", err)
	}

	waitFor(t, ctx, 15*time.Second, "CredentialSourceUnwatched=False/AllWatchedOrNone after label-add (watch fired)", func() error {
		if senv.clusterCount.n.Load() <= clusterBefore {
			return errReason("cluster not re-reconciled since label-add")
		}
		var got v1alpha1.KafkaCluster
		if err := senv.cl.Get(ctx, clusterKey, &got); err != nil {
			return err
		}
		if got.Status == nil {
			return errReason("status nil")
		}
		cond := findCond(got.Status.Conditions, v1alpha1.CondCredentialSourceUnwatched)
		if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != "AllWatchedOrNone" {
			return errReason("not cleared yet")
		}
		return nil
	})
}
