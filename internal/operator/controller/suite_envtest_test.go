//go:build envtest

// Package controller's envtest suite (Task 9) exercises the three reconcilers
// against a REAL Kubernetes apiserver + etcd (started via controller-runtime's
// envtest), with the CRDs from config/crd installed and the status subresource +
// finalizers persisted by the real apiserver. The Kafka and Schema-Registry
// clients are NOT real: they are the existing in-memory mocks
// (internal/kafka/mock, internal/schemaregistry/mock), injected through a stub
// ClientFactory. This proves the controllers' apiserver interactions (status
// writes through the /status subresource, finalizer add/remove and the
// apiserver-driven garbage collection that follows) without needing a broker.
//
// These tests are EXCLUDED from the default `go test ./...` run by the
// //go:build envtest tag, so the default suite stays hermetic. Run them with:
//
//	setup-envtest use            # one-time: fetch apiserver/etcd binaries
//	export KUBEBUILDER_ASSETS="$(setup-envtest use -p path)"
//	go test -tags envtest ./internal/operator/controller/ -v
//
// They SKIP cleanly (t.Skip) when the envtest control-plane binaries are
// unavailable (KUBEBUILDER_ASSETS unset / Environment.Start fails to find the
// apiserver), mirroring the Docker-skip pattern used by the kafka/franz
// integration test, so a binary-less environment never sees a failure.
//
// Reconciliation is driven DIRECTLY (reconciler.Reconcile(ctx, req) against the
// envtest client) rather than via a running manager: creating the CR with the
// client, invoking Reconcile, then re-Getting and asserting is deterministic and
// avoids manager start/watch timing flakiness while still exercising the real
// apiserver status subresource + finalizer persistence.
package controller

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	kafkamock "github.com/monedula-dev/monedula-gitops/internal/kafka/mock"
	schemamock "github.com/monedula-dev/monedula-gitops/internal/schemaregistry/mock"
)

// crdDir is the path (relative to this package directory) to the CRD manifests
// that envtest installs into the test apiserver.
const crdDir = "../../../config/crd"

// The envtest tests reuse the package's existing stubFactory (defined in
// kafkacluster_controller_test.go): a ClientFactory whose For returns the
// configured mock Kafka + Schema-Registry clients for every cluster, so a test
// can inspect the mocks' Calls()/state after driving Reconcile. We pass the
// SHARED mock instances so the assertions see the recorded mutations.

// testEnv holds the started control plane and a client for a single test.
// cfg is the rest config for tests that need to build their own manager
// (the settling test).
type testEnv struct {
	cl     client.Client
	cfg    *rest.Config
	scheme *runtime.Scheme
	stop   func()
}

// init wires a no-op logger so controller-runtime does not emit its noisy
// "log.SetLogger(...) was never called" warning + stack trace during the
// envtest control-plane bootstrap. The tests assert on returned values, not
// logs, so discarding output is fine.
func init() {
	ctrl.SetLogger(logr.Discard())
}

// startEnv starts an envtest control plane with the v1alpha1 CRDs installed and
// returns a client wired to the v1alpha1 scheme. It SKIPS the test cleanly when
// the control-plane binaries are unavailable (so a binary-less CI never fails).
func startEnv(t *testing.T) *testEnv {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("adding v1alpha1 to scheme: %v", err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("adding corev1 to scheme: %v", err)
	}
	// events.k8s.io/v1: the recorder-migration test lists the Events the
	// manager-wired events.EventRecorder writes via the new API group.
	if err := eventsv1.AddToScheme(scheme); err != nil {
		t.Fatalf("adding eventsv1 to scheme: %v", err)
	}

	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{crdDir},
		ErrorIfCRDPathMissing: true,
		Scheme:                scheme,
	}

	cfg, err := env.Start()
	if err != nil {
		if isAssetsUnavailable(err) {
			t.Skip("envtest assets unavailable; run: setup-envtest use & export KUBEBUILDER_ASSETS")
		}
		t.Fatalf("starting envtest: %v", err)
	}

	cl, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		_ = env.Stop()
		t.Fatalf("building client: %v", err)
	}

	return &testEnv{
		cl:     cl,
		cfg:    cfg,
		scheme: scheme,
		stop:   func() { _ = env.Stop() },
	}
}

// isAssetsUnavailable reports whether err from Environment.Start indicates the
// control-plane binaries (apiserver/etcd) could not be found — i.e. envtest was
// never set up in this environment. We match on the well-known phrases the
// control-plane bootstrap emits plus the unset KUBEBUILDER_ASSETS env var, so a
// binary-less run skips rather than fails.
func isAssetsUnavailable(err error) bool {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		return true
	}
	msg := err.Error()
	for _, needle := range []string{
		"unable to find",
		"no such file",
		"executable file not found",
		"KUBEBUILDER_ASSETS",
		"failed to start the controlplane",
	} {
		if strings.Contains(msg, needle) {
			return true
		}
	}
	return false
}

// reconcileFor invokes r.Reconcile for the named object and fails on error.
func reconcileFor(t *testing.T, r interface {
	Reconcile(context.Context, ctrl.Request) (ctrl.Result, error)
}, ns, name string) {
	t.Helper()
	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: ns, Name: name},
	}); err != nil {
		t.Fatalf("reconcile %s/%s: %v", ns, name, err)
	}
}

// condStatus returns the status of the named condition, or "" if absent.
func condStatus(conds []metav1.Condition, typ string) metav1.ConditionStatus {
	for _, c := range conds {
		if c.Type == typ {
			return c.Status
		}
	}
	return ""
}

// newCluster builds a minimal KafkaCluster CR (no TLS/Auth/SR) named name in ns.
func newCluster(ns, name string) *v1alpha1.KafkaCluster {
	return &v1alpha1.KafkaCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec:       v1alpha1.KafkaClusterSpec{BootstrapServers: "localhost:9092"},
	}
}

const testNamespace = "default"

// --- KafkaCluster ---

func TestEnvtestClusterReady(t *testing.T) {
	env := startEnv(t)
	defer env.stop()
	ctx := context.Background()

	c := newCluster(testNamespace, "cluster-ready")
	if err := env.cl.Create(ctx, c); err != nil {
		t.Fatalf("create cluster: %v", err)
	}

	r := &KafkaClusterReconciler{
		Client:  env.cl,
		Scheme:  env.scheme,
		Clients: stubFactory{k: kafkamock.New(nil, nil), sr: nil},
	}
	reconcileFor(t, r, testNamespace, "cluster-ready")

	var got v1alpha1.KafkaCluster
	if err := env.cl.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: "cluster-ready"}, &got); err != nil {
		t.Fatalf("re-get cluster: %v", err)
	}
	if got.Status == nil {
		t.Fatal("status not persisted via /status subresource")
	}
	if got.Status.Phase != v1alpha1.PhaseReady {
		t.Fatalf("phase = %q, want Ready", got.Status.Phase)
	}
	if s := condStatus(got.Status.Conditions, v1alpha1.CondClusterReachable); s != metav1.ConditionTrue {
		t.Fatalf("ClusterReachable = %q, want True", s)
	}
}

// --- KafkaTopic create ---

func TestEnvtestTopicCreate(t *testing.T) {
	env := startEnv(t)
	defer env.stop()
	ctx := context.Background()

	if err := env.cl.Create(ctx, newCluster(testNamespace, "topic-create-cluster")); err != nil {
		t.Fatalf("create cluster: %v", err)
	}

	topic := &v1alpha1.KafkaTopic{
		ObjectMeta: metav1.ObjectMeta{Name: "topic-create", Namespace: testNamespace},
		Spec: v1alpha1.KafkaTopicSpec{
			ClusterRef: v1alpha1.ClusterRef{Name: "topic-create-cluster"},
			TopicName:  "create.orders",
			Partitions: 3,
		},
	}
	if err := env.cl.Create(ctx, topic); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	mk := kafkamock.New(nil, nil)
	r := &KafkaTopicReconciler{
		Client:  env.cl,
		Scheme:  env.scheme,
		Clients: stubFactory{k: mk, sr: schemamock.New()},
	}
	reconcileFor(t, r, testNamespace, "topic-create")

	if calls := mk.Calls(); !containsCall(calls, "CreateTopic create.orders") {
		t.Fatalf("kafka calls = %v, want CreateTopic create.orders", calls)
	}

	var got v1alpha1.KafkaTopic
	if err := env.cl.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: "topic-create"}, &got); err != nil {
		t.Fatalf("re-get topic: %v", err)
	}
	if !controllerutil.ContainsFinalizer(&got, FinalizerName) {
		t.Fatal("finalizer not persisted on topic")
	}
	if got.Status == nil || got.Status.Phase != v1alpha1.PhaseReady {
		t.Fatalf("status = %+v, want phase Ready", got.Status)
	}
	if s := condStatus(got.Status.Conditions, v1alpha1.CondTopicSynced); s != metav1.ConditionTrue {
		t.Fatalf("TopicSynced = %q, want True", s)
	}
}

// --- KafkaTopic delete (Delete policy + allow-delete annotation) ---

func TestEnvtestTopicDelete(t *testing.T) {
	env := startEnv(t)
	defer env.stop()
	ctx := context.Background()

	if err := env.cl.Create(ctx, newCluster(testNamespace, "topic-delete-cluster")); err != nil {
		t.Fatalf("create cluster: %v", err)
	}

	topic := &v1alpha1.KafkaTopic{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "topic-delete",
			Namespace:   testNamespace,
			Annotations: map[string]string{"gitops.monedula.dev/allow-delete": "true"},
		},
		Spec: v1alpha1.KafkaTopicSpec{
			ClusterRef:     v1alpha1.ClusterRef{Name: "topic-delete-cluster"},
			TopicName:      "delete.orders",
			Partitions:     3,
			DeletionPolicy: "Delete",
		},
	}
	if err := env.cl.Create(ctx, topic); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	// Seed the topic so DeleteTopic operates on a live object.
	mk := kafkamock.New([]kafka.TopicState{{Name: "delete.orders", Partitions: 3}}, nil)
	r := &KafkaTopicReconciler{
		Client:  env.cl,
		Scheme:  env.scheme,
		Clients: stubFactory{k: mk, sr: schemamock.New()},
	}

	// First reconcile: adds the finalizer (and reconciles the topic).
	reconcileFor(t, r, testNamespace, "topic-delete")
	var afterCreate v1alpha1.KafkaTopic
	if err := env.cl.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: "topic-delete"}, &afterCreate); err != nil {
		t.Fatalf("re-get after create: %v", err)
	}
	if !controllerutil.ContainsFinalizer(&afterCreate, FinalizerName) {
		t.Fatal("finalizer not added before deletion")
	}

	// Delete: the apiserver sets deletionTimestamp but retains the object (the
	// finalizer blocks GC).
	if err := env.cl.Delete(ctx, &afterCreate); err != nil {
		t.Fatalf("delete topic: %v", err)
	}
	var afterDeleteMark v1alpha1.KafkaTopic
	if err := env.cl.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: "topic-delete"}, &afterDeleteMark); err != nil {
		t.Fatalf("re-get after delete mark: %v", err)
	}
	if afterDeleteMark.DeletionTimestamp.IsZero() {
		t.Fatal("deletionTimestamp not set; finalizer should retain the object")
	}

	// Reconcile the deletion: cluster-side cleanup runs (DeleteTopic), then the
	// finalizer is removed and the apiserver garbage-collects the object.
	reconcileFor(t, r, testNamespace, "topic-delete")
	if calls := mk.Calls(); !containsCall(calls, "DeleteTopic delete.orders") {
		t.Fatalf("kafka calls = %v, want DeleteTopic delete.orders", calls)
	}

	var gone v1alpha1.KafkaTopic
	err := env.cl.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: "topic-delete"}, &gone)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("topic still present after finalizer removal; get err = %v", err)
	}
}

// --- KafkaTopic DetectOnly (drift, no mutation) ---

func TestEnvtestTopicDetectOnlyDrift(t *testing.T) {
	env := startEnv(t)
	defer env.stop()
	ctx := context.Background()

	if err := env.cl.Create(ctx, newCluster(testNamespace, "topic-drift-cluster")); err != nil {
		t.Fatalf("create cluster: %v", err)
	}

	topic := &v1alpha1.KafkaTopic{
		ObjectMeta: metav1.ObjectMeta{Name: "topic-drift", Namespace: testNamespace},
		Spec: v1alpha1.KafkaTopicSpec{
			ClusterRef:     v1alpha1.ClusterRef{Name: "topic-drift-cluster"},
			TopicName:      "drift.orders",
			Partitions:     3,
			Config:         map[string]string{"retention.ms": "604800000"},
			Reconciliation: &v1alpha1.Reconciliation{Mode: "DetectOnly"},
		},
	}
	if err := env.cl.Create(ctx, topic); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	// Live state differs from spec (config drift) so DetectOnly reports Drifted
	// without mutating.
	live := []kafka.TopicState{{
		Name:       "drift.orders",
		Partitions: 3,
		Config:     map[string]string{"retention.ms": "1000"},
	}}
	mk := kafkamock.New(live, nil)
	r := &KafkaTopicReconciler{
		Client:  env.cl,
		Scheme:  env.scheme,
		Clients: stubFactory{k: mk, sr: schemamock.New()},
	}
	reconcileFor(t, r, testNamespace, "topic-drift")

	if calls := mk.Calls(); len(calls) != 0 {
		t.Fatalf("DetectOnly mutated kafka: calls = %v", calls)
	}

	var got v1alpha1.KafkaTopic
	if err := env.cl.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: "topic-drift"}, &got); err != nil {
		t.Fatalf("re-get topic: %v", err)
	}
	if got.Status == nil || got.Status.Phase != v1alpha1.PhaseDrifted {
		t.Fatalf("status = %+v, want phase Drifted", got.Status)
	}
	if got.Status.Drift == nil || !got.Status.Drift.Detected {
		t.Fatalf("drift = %+v, want detected", got.Status.Drift)
	}
}

// --- KafkaAccessPolicy create + delete ---

func TestEnvtestAccessPolicyCreate(t *testing.T) {
	env := startEnv(t)
	defer env.stop()
	ctx := context.Background()

	if err := env.cl.Create(ctx, newCluster(testNamespace, "policy-create-cluster")); err != nil {
		t.Fatalf("create cluster: %v", err)
	}

	pol := &v1alpha1.KafkaAccessPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "policy-create", Namespace: testNamespace},
		Spec: v1alpha1.KafkaAccessPolicySpec{
			ClusterRef: v1alpha1.ClusterRef{Name: "policy-create-cluster"},
			Rules: []v1alpha1.ACLRule{{
				Principal:  "User:svc",
				Operations: []string{"Read"},
				Resource:   v1alpha1.ACLResource{Type: "topic", Name: "policy.orders"},
			}},
		},
	}
	if err := env.cl.Create(ctx, pol); err != nil {
		t.Fatalf("create policy: %v", err)
	}

	mk := kafkamock.New(nil, nil)
	r := &KafkaAccessPolicyReconciler{
		Client:  env.cl,
		Scheme:  env.scheme,
		Clients: stubFactory{k: mk, sr: schemamock.New()},
	}
	reconcileFor(t, r, testNamespace, "policy-create")

	if calls := mk.Calls(); !hasCallPrefix(calls, "CreateACLs ") {
		t.Fatalf("kafka calls = %v, want a CreateACLs", calls)
	}

	var got v1alpha1.KafkaAccessPolicy
	if err := env.cl.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: "policy-create"}, &got); err != nil {
		t.Fatalf("re-get policy: %v", err)
	}
	if !controllerutil.ContainsFinalizer(&got, FinalizerName) {
		t.Fatal("finalizer not persisted on policy")
	}
	if got.Status == nil || got.Status.Phase != v1alpha1.PhaseReady {
		t.Fatalf("status = %+v, want phase Ready", got.Status)
	}
	if s := condStatus(got.Status.Conditions, v1alpha1.CondAccessPolicySynced); s != metav1.ConditionTrue {
		t.Fatalf("AccessPolicySynced = %q, want True", s)
	}
}

func TestEnvtestAccessPolicyDelete(t *testing.T) {
	env := startEnv(t)
	defer env.stop()
	ctx := context.Background()

	if err := env.cl.Create(ctx, newCluster(testNamespace, "policy-delete-cluster")); err != nil {
		t.Fatalf("create cluster: %v", err)
	}

	// DeletionPolicy defaults to Delete for policies; gate it with allow-delete.
	pol := &v1alpha1.KafkaAccessPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "policy-delete",
			Namespace:   testNamespace,
			Annotations: map[string]string{"gitops.monedula.dev/allow-delete": "true"},
		},
		Spec: v1alpha1.KafkaAccessPolicySpec{
			ClusterRef: v1alpha1.ClusterRef{Name: "policy-delete-cluster"},
			Rules: []v1alpha1.ACLRule{{
				Principal:  "User:svc",
				Operations: []string{"Read"},
				Resource:   v1alpha1.ACLResource{Type: "topic", Name: "policy.del.orders"},
			}},
		},
	}
	if err := env.cl.Create(ctx, pol); err != nil {
		t.Fatalf("create policy: %v", err)
	}

	mk := kafkamock.New(nil, nil)
	r := &KafkaAccessPolicyReconciler{
		Client:  env.cl,
		Scheme:  env.scheme,
		Clients: stubFactory{k: mk, sr: schemamock.New()},
	}

	// First reconcile: adds the finalizer + creates ACLs.
	reconcileFor(t, r, testNamespace, "policy-delete")
	var afterCreate v1alpha1.KafkaAccessPolicy
	if err := env.cl.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: "policy-delete"}, &afterCreate); err != nil {
		t.Fatalf("re-get after create: %v", err)
	}
	if !controllerutil.ContainsFinalizer(&afterCreate, FinalizerName) {
		t.Fatal("finalizer not added before deletion")
	}

	if err := env.cl.Delete(ctx, &afterCreate); err != nil {
		t.Fatalf("delete policy: %v", err)
	}

	// Reconcile deletion: DeleteACLs runs, finalizer removed, object GC'd.
	reconcileFor(t, r, testNamespace, "policy-delete")
	if calls := mk.Calls(); !hasCallPrefix(calls, "DeleteACLs ") {
		t.Fatalf("kafka calls = %v, want a DeleteACLs", calls)
	}

	var gone v1alpha1.KafkaAccessPolicy
	err := env.cl.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: "policy-delete"}, &gone)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("policy still present after finalizer removal; get err = %v", err)
	}
}

// containsCall reports whether calls contains the exact entry want.
func containsCall(calls []string, want string) bool {
	for _, c := range calls {
		if c == want {
			return true
		}
	}
	return false
}

// hasCallPrefix reports whether any entry in calls starts with prefix (used for
// CreateACLs/DeleteACLs whose recorded form ends with a count).
func hasCallPrefix(calls []string, prefix string) bool {
	for _, c := range calls {
		if strings.HasPrefix(c, prefix) {
			return true
		}
	}
	return false
}
