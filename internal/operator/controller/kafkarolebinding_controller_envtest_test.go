//go:build envtest

package controller

import (
	"context"
	"errors"
	"strings"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	kafkamock "github.com/monedula-dev/monedula-gitops/internal/kafka/mock"
	"github.com/monedula-dev/monedula-gitops/internal/mds"
	mdsmock "github.com/monedula-dev/monedula-gitops/internal/mds/mock"
	schemamock "github.com/monedula-dev/monedula-gitops/internal/schemaregistry/mock"
)

// errFakeMDS is a sentinel error used to simulate MDSFor failures in tests.
var errFakeMDS = errors.New("mds: simulated connection failure")

// newMDSCluster builds a KafkaCluster with authorization.mds configured (the
// kafkaCluster id is "lkc-test" for all envtest role-binding tests).
func newMDSCluster(ns, name string) *v1alpha1.KafkaCluster {
	return &v1alpha1.KafkaCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: v1alpha1.KafkaClusterSpec{
			BootstrapServers: "localhost:9092",
			Authorization: &v1alpha1.AuthorizationConfig{
				MDS: &v1alpha1.MDSConfig{
					Endpoint: "http://mds:8090",
					Clusters: v1alpha1.MDSClusters{
						KafkaCluster: "lkc-test",
					},
				},
			},
		},
	}
}

// newRoleBinding builds a minimal KafkaRoleBinding CR referencing clusterName
// in ns. Uses "SystemAdmin" (a cluster-scoped role that requires no spec.resources)
// so the binding passes ValidateRoleBindingShape without additional configuration.
func newRoleBinding(ns, name, clusterName string) *v1alpha1.KafkaRoleBinding {
	return &v1alpha1.KafkaRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: v1alpha1.KafkaRoleBindingSpec{
			ClusterRef: v1alpha1.ClusterRef{Name: clusterName},
			Principal:  "User:alice",
			Role:       "SystemAdmin",
			Scope:      v1alpha1.RoleBindingScope{Type: "kafka"},
		},
	}
}

// expectedMDSRoleBinding is the mds.RoleBinding the controller should add when
// it reconciles newRoleBinding against newMDSCluster.
var expectedMDSRoleBinding = mds.RoleBinding{
	Principal: "User:alice",
	Role:      "SystemAdmin",
	Scope: mds.Scope{
		Type:         "kafka",
		KafkaCluster: "lkc-test",
	},
}

// roleBindingReconciler builds a KafkaRoleBindingReconciler wired to the
// envtest client and the given MDS mock.
func roleBindingReconciler(env *testEnv, mock *mdsmock.Mock) *KafkaRoleBindingReconciler {
	return &KafkaRoleBindingReconciler{
		Client:  env.cl,
		Scheme:  env.scheme,
		Clients: stubFactory{mds: mock},
	}
}

// --- TestEnvtestRoleBindingCreate ---

// TestEnvtestRoleBindingCreate verifies that reconciling a new KafkaRoleBinding
// (+ a KafkaCluster with authorization.mds) produces:
//   - a finalizer added to the CR
//   - the MDS role binding added to the mock (AddRoleBinding called)
//   - status Phase Ready, RoleBindingSynced=True, MDSReachable=True
func TestEnvtestRoleBindingCreate(t *testing.T) {
	env := startEnv(t)
	defer env.stop()
	ctx := context.Background()

	if err := env.cl.Create(ctx, newMDSCluster(testNamespace, "rb-create-cluster")); err != nil {
		t.Fatalf("create cluster: %v", err)
	}

	rb := newRoleBinding(testNamespace, "rb-create", "rb-create-cluster")
	if err := env.cl.Create(ctx, rb); err != nil {
		t.Fatalf("create role binding: %v", err)
	}

	mock := mdsmock.New()
	r := roleBindingReconciler(env, mock)
	reconcileFor(t, r, testNamespace, "rb-create")

	// Finalizer must be present.
	var got v1alpha1.KafkaRoleBinding
	if err := env.cl.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: "rb-create"}, &got); err != nil {
		t.Fatalf("re-get role binding: %v", err)
	}
	if !controllerutil.ContainsFinalizer(&got, FinalizerName) {
		t.Fatal("finalizer not added to role binding")
	}

	// Status must be Ready.
	if got.Status == nil {
		t.Fatal("status not written")
	}
	if got.Status.Phase != v1alpha1.PhaseReady {
		t.Fatalf("phase = %q, want Ready", got.Status.Phase)
	}
	if s := condStatus(got.Status.Conditions, v1alpha1.CondRoleBindingSynced); s != metav1.ConditionTrue {
		t.Fatalf("RoleBindingSynced = %q, want True", s)
	}
	if s := condStatus(got.Status.Conditions, v1alpha1.CondMDSReachable); s != metav1.ConditionTrue {
		t.Fatalf("MDSReachable = %q, want True", s)
	}

	// MDS mock must have received AddRoleBinding.
	calls := mock.Calls()
	if !containsCall(calls, "AddRoleBinding "+expectedMDSRoleBinding.Key()) {
		t.Fatalf("MDS calls = %v, want AddRoleBinding for %s", calls, expectedMDSRoleBinding.Key())
	}
}

// --- TestEnvtestRoleBindingDrift ---

// TestEnvtestRoleBindingDrift verifies that if the MDS binding is removed
// out-of-band (drift), the next reconcile re-adds it.
func TestEnvtestRoleBindingDrift(t *testing.T) {
	env := startEnv(t)
	defer env.stop()
	ctx := context.Background()

	if err := env.cl.Create(ctx, newMDSCluster(testNamespace, "rb-drift-cluster")); err != nil {
		t.Fatalf("create cluster: %v", err)
	}

	rb := newRoleBinding(testNamespace, "rb-drift", "rb-drift-cluster")
	if err := env.cl.Create(ctx, rb); err != nil {
		t.Fatalf("create role binding: %v", err)
	}

	// First reconcile: binding is added.
	mock := mdsmock.New()
	r := roleBindingReconciler(env, mock)
	reconcileFor(t, r, testNamespace, "rb-drift")

	calls1 := mock.Calls()
	if !containsCall(calls1, "AddRoleBinding "+expectedMDSRoleBinding.Key()) {
		t.Fatalf("initial reconcile: MDS calls = %v, want AddRoleBinding", calls1)
	}

	// Simulate out-of-band removal: remove binding from the mock WITHOUT going
	// through the reconciler.
	if err := mock.RemoveRoleBinding(ctx, expectedMDSRoleBinding); err != nil {
		t.Fatalf("simulating drift removal: %v", err)
	}

	// Second reconcile: binding must be re-added.
	reconcileFor(t, r, testNamespace, "rb-drift")
	calls2 := mock.Calls()
	// calls2 includes the RemoveRoleBinding from our manual call AND the new
	// AddRoleBinding from the second reconcile.
	added := 0
	for _, c := range calls2 {
		if c == "AddRoleBinding "+expectedMDSRoleBinding.Key() {
			added++
		}
	}
	if added < 2 { // initial + drift re-add
		t.Fatalf("drift not corrected: AddRoleBinding seen %d times in calls %v", added, calls2)
	}
}

// --- TestEnvtestRoleBindingDeleteOrphan ---

// TestEnvtestRoleBindingDeleteOrphan verifies that an explicit
// deletionPolicy: Orphan leaves the MDS binding in place after the CR is
// deleted.
func TestEnvtestRoleBindingDeleteOrphan(t *testing.T) {
	env := startEnv(t)
	defer env.stop()
	ctx := context.Background()

	if err := env.cl.Create(ctx, newMDSCluster(testNamespace, "rb-orphan-cluster")); err != nil {
		t.Fatalf("create cluster: %v", err)
	}

	rb := newRoleBinding(testNamespace, "rb-orphan", "rb-orphan-cluster")
	// Orphan is no longer the default (see TestEnvtestRoleBindingDeleteDefault);
	// set it explicitly to exercise the non-default path.
	rb.Spec.DeletionPolicy = "Orphan"
	if err := env.cl.Create(ctx, rb); err != nil {
		t.Fatalf("create role binding: %v", err)
	}

	// Seed the mock with the binding so it is "live".
	mock := mdsmock.New(expectedMDSRoleBinding)
	r := roleBindingReconciler(env, mock)

	// First reconcile: adds finalizer (binding already present in mock).
	reconcileFor(t, r, testNamespace, "rb-orphan")
	var afterCreate v1alpha1.KafkaRoleBinding
	if err := env.cl.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: "rb-orphan"}, &afterCreate); err != nil {
		t.Fatalf("re-get after create: %v", err)
	}
	if !controllerutil.ContainsFinalizer(&afterCreate, FinalizerName) {
		t.Fatal("finalizer not added before deletion")
	}

	// Delete the CR.
	if err := env.cl.Delete(ctx, &afterCreate); err != nil {
		t.Fatalf("delete role binding: %v", err)
	}

	// Reconcile deletion: Orphan → DO NOT remove MDS binding; remove finalizer.
	reconcileFor(t, r, testNamespace, "rb-orphan")

	// MDS binding must still be present in the mock.
	live, err := mock.ListRoleBindings(ctx, mds.Scope{Type: "kafka", KafkaCluster: "lkc-test"})
	if err != nil {
		t.Fatalf("listing MDS bindings: %v", err)
	}
	found := false
	for _, b := range live {
		if b.Key() == expectedMDSRoleBinding.Key() {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("Orphan policy: MDS binding was incorrectly removed; live = %v (calls = %v)", live, mock.Calls())
	}

	// CR must be gone.
	var gone v1alpha1.KafkaRoleBinding
	if err := env.cl.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: "rb-orphan"}, &gone); !apierrors.IsNotFound(err) {
		t.Fatalf("role binding still present after finalizer removal; get err = %v", err)
	}
}

// --- TestEnvtestRoleBindingDeleteDelete ---

// TestEnvtestRoleBindingDeleteDelete verifies that deletionPolicy: Delete
// removes the MDS binding and then the CR.
func TestEnvtestRoleBindingDeleteDelete(t *testing.T) {
	env := startEnv(t)
	defer env.stop()
	ctx := context.Background()

	if err := env.cl.Create(ctx, newMDSCluster(testNamespace, "rb-delete-cluster")); err != nil {
		t.Fatalf("create cluster: %v", err)
	}

	rb := newRoleBinding(testNamespace, "rb-delete", "rb-delete-cluster")
	rb.Spec.DeletionPolicy = "Delete"
	if err := env.cl.Create(ctx, rb); err != nil {
		t.Fatalf("create role binding: %v", err)
	}

	// Seed the mock with the binding so there is something to remove.
	mock := mdsmock.New(expectedMDSRoleBinding)
	r := roleBindingReconciler(env, mock)

	// First reconcile: adds finalizer.
	reconcileFor(t, r, testNamespace, "rb-delete")
	var afterCreate v1alpha1.KafkaRoleBinding
	if err := env.cl.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: "rb-delete"}, &afterCreate); err != nil {
		t.Fatalf("re-get after create: %v", err)
	}
	if !controllerutil.ContainsFinalizer(&afterCreate, FinalizerName) {
		t.Fatal("finalizer not added before deletion")
	}

	// Delete the CR.
	if err := env.cl.Delete(ctx, &afterCreate); err != nil {
		t.Fatalf("delete role binding: %v", err)
	}

	// Reconcile deletion: Delete → remove MDS binding, then remove finalizer.
	reconcileFor(t, r, testNamespace, "rb-delete")

	// MDS binding must be gone from the mock.
	if !containsCall(mock.Calls(), "RemoveRoleBinding "+expectedMDSRoleBinding.Key()) {
		t.Fatalf("Delete policy: RemoveRoleBinding not called; calls = %v", mock.Calls())
	}
	live, err := mock.ListRoleBindings(ctx, mds.Scope{Type: "kafka", KafkaCluster: "lkc-test"})
	if err != nil {
		t.Fatalf("listing MDS bindings: %v", err)
	}
	for _, b := range live {
		if b.Key() == expectedMDSRoleBinding.Key() {
			t.Fatalf("Delete policy: MDS binding still present after deletion; live = %v", live)
		}
	}

	// CR must be gone.
	var gone v1alpha1.KafkaRoleBinding
	if err := env.cl.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: "rb-delete"}, &gone); !apierrors.IsNotFound(err) {
		t.Fatalf("role binding still present after finalizer removal; get err = %v", err)
	}
}

// --- TestEnvtestRoleBindingDeleteDefault ---

// TestEnvtestRoleBindingDeleteDefault verifies that OMITTING deletionPolicy
// (the default, resolved by defaulting.RoleBinding) behaves like an explicit
// Delete: the MDS binding is removed and then the CR. This pins the D2
// default flip (Orphan → Delete), aligning KafkaRoleBinding with
// KafkaAccessPolicy and KafkaUser — the MDS bindings are this CR's reason to
// exist.
func TestEnvtestRoleBindingDeleteDefault(t *testing.T) {
	env := startEnv(t)
	defer env.stop()
	ctx := context.Background()

	if err := env.cl.Create(ctx, newMDSCluster(testNamespace, "rb-default-cluster")); err != nil {
		t.Fatalf("create cluster: %v", err)
	}

	// newRoleBinding leaves DeletionPolicy unset — this exercises the default.
	rb := newRoleBinding(testNamespace, "rb-default", "rb-default-cluster")
	if err := env.cl.Create(ctx, rb); err != nil {
		t.Fatalf("create role binding: %v", err)
	}

	// Seed the mock with the binding so there is something to remove.
	mock := mdsmock.New(expectedMDSRoleBinding)
	r := roleBindingReconciler(env, mock)

	// First reconcile: adds finalizer.
	reconcileFor(t, r, testNamespace, "rb-default")
	var afterCreate v1alpha1.KafkaRoleBinding
	if err := env.cl.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: "rb-default"}, &afterCreate); err != nil {
		t.Fatalf("re-get after create: %v", err)
	}
	if !controllerutil.ContainsFinalizer(&afterCreate, FinalizerName) {
		t.Fatal("finalizer not added before deletion")
	}

	// Delete the CR.
	if err := env.cl.Delete(ctx, &afterCreate); err != nil {
		t.Fatalf("delete role binding: %v", err)
	}

	// Reconcile deletion: default (Delete) → remove MDS binding, then remove finalizer.
	reconcileFor(t, r, testNamespace, "rb-default")

	// MDS binding must be gone from the mock.
	if !containsCall(mock.Calls(), "RemoveRoleBinding "+expectedMDSRoleBinding.Key()) {
		t.Fatalf("default policy: RemoveRoleBinding not called; calls = %v", mock.Calls())
	}
	live, err := mock.ListRoleBindings(ctx, mds.Scope{Type: "kafka", KafkaCluster: "lkc-test"})
	if err != nil {
		t.Fatalf("listing MDS bindings: %v", err)
	}
	for _, b := range live {
		if b.Key() == expectedMDSRoleBinding.Key() {
			t.Fatalf("default policy: MDS binding still present after deletion; live = %v", live)
		}
	}

	// CR must be gone.
	var gone v1alpha1.KafkaRoleBinding
	if err := env.cl.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: "rb-default"}, &gone); !apierrors.IsNotFound(err) {
		t.Fatalf("role binding still present after finalizer removal; get err = %v", err)
	}
}

// --- TestEnvtestRoleBindingForceRemoval ---

// TestEnvtestRoleBindingForceRemoval verifies that when MDSFor returns an error
// (cluster unreachable) and the force-removal annotation is set, the finalizer
// is removed without cluster-side cleanup.
func TestEnvtestRoleBindingForceRemoval(t *testing.T) {
	env := startEnv(t)
	defer env.stop()
	ctx := context.Background()

	if err := env.cl.Create(ctx, newMDSCluster(testNamespace, "rb-force-cluster")); err != nil {
		t.Fatalf("create cluster: %v", err)
	}

	rb := newRoleBinding(testNamespace, "rb-force", "rb-force-cluster")
	if err := env.cl.Create(ctx, rb); err != nil {
		t.Fatalf("create role binding: %v", err)
	}

	// First reconcile with a working factory: adds finalizer.
	workingMock := mdsmock.New()
	r := roleBindingReconciler(env, workingMock)
	reconcileFor(t, r, testNamespace, "rb-force")
	var afterCreate v1alpha1.KafkaRoleBinding
	if err := env.cl.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: "rb-force"}, &afterCreate); err != nil {
		t.Fatalf("re-get after create: %v", err)
	}
	if !controllerutil.ContainsFinalizer(&afterCreate, FinalizerName) {
		t.Fatal("finalizer not added")
	}

	// Set the force-removal annotation.
	afterCreate.Annotations = map[string]string{AnnotationForceFinalizerRemoval: "true"}
	if err := env.cl.Update(ctx, &afterCreate); err != nil {
		t.Fatalf("setting force annotation: %v", err)
	}

	// Delete the CR.
	if err := env.cl.Delete(ctx, &afterCreate); err != nil {
		t.Fatalf("delete role binding: %v", err)
	}

	// Now switch to a factory that returns an MDS error (cluster unreachable).
	rBroken := &KafkaRoleBindingReconciler{
		Client:  env.cl,
		Scheme:  env.scheme,
		Clients: stubFactory{mdsErr: errFakeMDS},
	}

	// Reconcile: force-removal annotation must bypass the block and remove the finalizer.
	reconcileFor(t, rBroken, testNamespace, "rb-force")

	// CR must be gone.
	var gone v1alpha1.KafkaRoleBinding
	if err := env.cl.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: "rb-force"}, &gone); !apierrors.IsNotFound(err) {
		t.Fatalf("role binding still present after force removal; get err = %v", err)
	}
}

// --- TestEnvtestRoleBindingTenancyDeniedClusterScoped (spec §20.2) ---

// TestEnvtestRoleBindingTenancyDeniedClusterScoped verifies that a
// cluster-scoped KafkaRoleBinding (SystemAdmin, empty spec.resources) created
// from a prefix-restricted namespace is terminally rejected: Phase Error,
// ValidationFailed=True with reason TenancyDenied, and NO MDS mutation. This
// pins the fix for the SystemAdmin-escalation path.
func TestEnvtestRoleBindingTenancyDeniedClusterScoped(t *testing.T) {
	env := startEnv(t)
	defer env.stop()
	ctx := context.Background()

	cluster := newMDSCluster(testNamespace, "rb-tenancy-cluster")
	cluster.Spec.Tenancy = &v1alpha1.TenancyConfig{
		AllowedNamespaces: []string{"*"}, // allow-list passes...
		TopicPrefixes: []v1alpha1.TopicPrefixRule{
			// ...but testNamespace "default" is prefix-restricted.
			{Namespaces: []string{testNamespace}, Prefixes: []string{"payments."}},
		},
	}
	if err := env.cl.Create(ctx, cluster); err != nil {
		t.Fatalf("create cluster: %v", err)
	}

	// newRoleBinding is SystemAdmin with no spec.resources → cluster-scoped.
	rb := newRoleBinding(testNamespace, "rb-tenancy", "rb-tenancy-cluster")
	if err := env.cl.Create(ctx, rb); err != nil {
		t.Fatalf("create role binding: %v", err)
	}

	mock := mdsmock.New()
	r := roleBindingReconciler(env, mock)
	reconcileFor(t, r, testNamespace, "rb-tenancy")

	// No MDS mutation may have happened.
	if calls := mock.Calls(); len(calls) != 0 {
		t.Fatalf("tenancy denial must not touch MDS: calls = %v", calls)
	}

	var got v1alpha1.KafkaRoleBinding
	if err := env.cl.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: "rb-tenancy"}, &got); err != nil {
		t.Fatalf("re-get role binding: %v", err)
	}
	if got.Status == nil || got.Status.Phase != v1alpha1.PhaseError {
		t.Fatalf("status = %+v, want phase Error", got.Status)
	}
	if s := condStatus(got.Status.Conditions, v1alpha1.CondValidationFailed); s != metav1.ConditionTrue {
		t.Fatalf("ValidationFailed = %q, want True", s)
	}
	if reason := condReason(got.Status.Conditions, v1alpha1.CondValidationFailed); reason != "TenancyDenied" {
		t.Fatalf("ValidationFailed reason = %q, want TenancyDenied", reason)
	}
	if s := condStatus(got.Status.Conditions, v1alpha1.CondReady); s != metav1.ConditionFalse {
		t.Fatalf("Ready = %q, want False", s)
	}
}

// --- TestEnvtestTopicDualEmitRoleBinding (v0.15 accessBackends) ---

// TestEnvtestTopicDualEmitRoleBinding verifies that the KafkaTopicReconciler,
// when reconciling a topic whose cluster has accessBackends: [acl, rbac] and an
// MDS config, both:
//
//	(a) calls AddRoleBinding on the MDS mock for the producer's DeveloperWrite
//	    grant, and
//	(b) sets the RoleBindingSynced=True condition on the topic status.
//
// This exercises the full operator dual-emit path (spec §40, shipped v0.15) end
// to end through the real envtest apiserver: the topic controller calls
// MDSFor, builds the ClusterRoleBindingView, and dispatches role-binding ops via
// the executor — verified via the MDS mock's recorded call list.
func TestEnvtestTopicDualEmitRoleBinding(t *testing.T) {
	env := startEnv(t)
	defer env.stop()
	ctx := context.Background()

	// Cluster with both acl and rbac backends + MDS configured.
	cluster := &v1alpha1.KafkaCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "dual-cluster", Namespace: testNamespace},
		Spec: v1alpha1.KafkaClusterSpec{
			BootstrapServers: "localhost:9092",
			Authorization: &v1alpha1.AuthorizationConfig{
				AccessBackends: []string{"acl", "rbac"},
				MDS: &v1alpha1.MDSConfig{
					Endpoint: "http://mds:8090",
					Clusters: v1alpha1.MDSClusters{
						KafkaCluster: "lkc-test-dual",
					},
				},
			},
		},
	}
	if err := env.cl.Create(ctx, cluster); err != nil {
		t.Fatalf("create cluster: %v", err)
	}

	// Topic with a single producer access entry.
	topic := &v1alpha1.KafkaTopic{
		ObjectMeta: metav1.ObjectMeta{Name: "dual-topic", Namespace: testNamespace},
		Spec: v1alpha1.KafkaTopicSpec{
			ClusterRef: v1alpha1.ClusterRef{Name: "dual-cluster"},
			TopicName:  "dual.orders",
			Partitions: 3,
			Access: v1alpha1.TopicAccess{
				Producers: []v1alpha1.ProducerAccess{
					{Principal: "User:svc-checkout"},
				},
			},
		},
	}
	if err := env.cl.Create(ctx, topic); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	mk := kafkamock.New(nil, nil)
	mdsMock := mdsmock.New() // empty live state → AddRoleBinding expected
	r := &KafkaTopicReconciler{
		Client:  env.cl,
		Scheme:  env.scheme,
		Clients: stubFactory{k: mk, sr: schemamock.New(), mds: mdsMock},
	}
	reconcileFor(t, r, testNamespace, "dual-topic")

	// (a) MDS mock must have received AddRoleBinding for DeveloperWrite on
	// dual.orders for the producer User:svc-checkout.
	mdsCalls := mdsMock.Calls()
	foundAdd := false
	for _, c := range mdsCalls {
		if strings.HasPrefix(c, "AddRoleBinding") && strings.Contains(c, "User:svc-checkout") {
			foundAdd = true
			break
		}
	}
	if !foundAdd {
		t.Fatalf("expected AddRoleBinding for User:svc-checkout in MDS calls; got: %v", mdsCalls)
	}

	// (b) Topic status must have RoleBindingSynced=True.
	var got v1alpha1.KafkaTopic
	if err := env.cl.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: "dual-topic"}, &got); err != nil {
		t.Fatalf("re-get topic: %v", err)
	}
	if got.Status == nil {
		t.Fatal("topic status not written")
	}
	if s := condStatus(got.Status.Conditions, v1alpha1.CondRoleBindingSynced); s != metav1.ConditionTrue {
		t.Fatalf("RoleBindingSynced = %q, want True; conditions: %v", s, got.Status.Conditions)
	}
	// Topic itself must also be synced and ready.
	if got.Status.Phase != v1alpha1.PhaseReady {
		t.Fatalf("phase = %q, want Ready", got.Status.Phase)
	}
	if !controllerutil.ContainsFinalizer(&got, FinalizerName) {
		t.Fatal("finalizer not added to topic")
	}
}
