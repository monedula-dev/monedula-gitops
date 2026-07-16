//go:build envtest

// KafkaUser envtest lifecycles (v0.35 T5), following the suite pattern in
// suite_envtest_test.go: a real apiserver+etcd via envtest (status subresource,
// finalizers, owner references, resourceVersions), the in-memory Kafka mock
// through stubFactory, and reconciles driven directly (no manager) for
// determinism. Covered here — the interactions unit tests cannot prove:
//
//   - referenced-password lifecycle: appliedPasswordRef tracks the REAL
//     Secret resourceVersion; a data update bumps rV and drives a rotation;
//   - generate lifecycle: the credentials Secret is created with a controller
//     owner reference + keys; deleting it regenerates (rotation-by-deletion);
//   - finalizer: deletionPolicy Delete removes the credential, Orphan keeps it;
//   - foreign-Secret collision: terminal, never adopted, no upsert.
package controller

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	kafkamock "github.com/monedula-dev/monedula-gitops/internal/kafka/mock"
	schemamock "github.com/monedula-dev/monedula-gitops/internal/schemaregistry/mock"
)

// newUserReconciler builds a KafkaUserReconciler over the envtest client with
// the shared mock Kafka client, so tests can assert broker-side state.
func newUserReconciler(env *testEnv, mk *kafkamock.Client) *KafkaUserReconciler {
	return &KafkaUserReconciler{
		Client:  env.cl,
		Scheme:  env.scheme,
		Clients: stubFactory{k: mk, sr: schemamock.New()},
	}
}

// envUser builds a KafkaUser CR named name (username defaults from it) with
// the given password block and optional deletionPolicy.
func envUser(name string, pw *v1alpha1.UserPassword, deletionPolicy string) *v1alpha1.KafkaUser {
	return &v1alpha1.KafkaUser{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
		Spec: v1alpha1.KafkaUserSpec{
			ClusterRef:     v1alpha1.ClusterRef{Name: name + "-cluster"},
			Password:       pw,
			DeletionPolicy: deletionPolicy,
		},
	}
}

func secretKeyRefPassword(secretName string) *v1alpha1.UserPassword {
	return &v1alpha1.UserPassword{ValueFrom: &v1alpha1.ValueSource{
		SecretKeyRef: &v1alpha1.SecretKeyRef{Name: secretName, Key: "password"},
	}}
}

func getUser(t *testing.T, env *testEnv, name string) *v1alpha1.KafkaUser {
	t.Helper()
	var got v1alpha1.KafkaUser
	if err := env.cl.Get(context.Background(), types.NamespacedName{Namespace: testNamespace, Name: name}, &got); err != nil {
		t.Fatalf("re-get user %s: %v", name, err)
	}
	return &got
}

// --- referenced-password lifecycle ---

func TestEnvtestUserReferencedPasswordLifecycle(t *testing.T) {
	env := startEnv(t)
	defer env.stop()
	ctx := context.Background()

	if err := env.cl.Create(ctx, newCluster(testNamespace, "user-ref-cluster")); err != nil {
		t.Fatalf("create cluster: %v", err)
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "user-ref-pw", Namespace: testNamespace},
		Data:       map[string][]byte{"password": []byte("initial")},
	}
	if err := env.cl.Create(ctx, secret); err != nil {
		t.Fatalf("create secret: %v", err)
	}
	if err := env.cl.Create(ctx, envUser("user-ref", secretKeyRefPassword("user-ref-pw"), "")); err != nil {
		t.Fatalf("create user: %v", err)
	}

	mk := kafkamock.New(nil, nil)
	r := newUserReconciler(env, mk)
	reconcileFor(t, r, testNamespace, "user-ref")

	got := getUser(t, env, "user-ref")
	if !controllerutil.ContainsFinalizer(got, FinalizerName) {
		t.Fatal("finalizer not persisted on user")
	}
	if got.Status == nil || got.Status.Phase != v1alpha1.PhaseReady {
		t.Fatalf("status = %+v, want phase Ready", got.Status)
	}
	if s := condStatus(got.Status.Conditions, v1alpha1.CondUserSynced); s != metav1.ConditionTrue {
		t.Fatalf("UserSynced = %q, want True", s)
	}
	if n := mk.ScramUpsertCount("user-ref", "SCRAM-SHA-512"); n != 1 {
		t.Fatalf("upsert count = %d, want 1", n)
	}
	creds, _ := mk.ListScramCredentials(ctx, "user-ref")
	if len(creds) != 1 || creds[0].Mechanism != "SCRAM-SHA-512" {
		t.Fatalf("live credentials = %+v, want one SCRAM-SHA-512 for user-ref", creds)
	}
	// appliedPasswordRef records the REAL apiserver resourceVersion of the
	// source Secret at apply time.
	var liveSecret corev1.Secret
	if err := env.cl.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: "user-ref-pw"}, &liveSecret); err != nil {
		t.Fatalf("re-get secret: %v", err)
	}
	ref := got.Status.AppliedPasswordRef
	if ref == nil || ref.SecretName != "user-ref-pw" || ref.ResourceVersion != liveSecret.ResourceVersion {
		t.Fatalf("appliedPasswordRef = %+v, want {user-ref-pw %s}", ref, liveSecret.ResourceVersion)
	}

	// Steady state: a second reconcile performs NO upsert (rV unchanged).
	reconcileFor(t, r, testNamespace, "user-ref")
	if n := mk.ScramUpsertCount("user-ref", "SCRAM-SHA-512"); n != 1 {
		t.Fatalf("steady-state upsert count = %d, want 1 (no rotation)", n)
	}

	// Update the Secret's data: rV moves, the next reconcile rotates and the
	// watermark advances.
	liveSecret.Data["password"] = []byte("rotated")
	if err := env.cl.Update(ctx, &liveSecret); err != nil {
		t.Fatalf("update secret: %v", err)
	}
	reconcileFor(t, r, testNamespace, "user-ref")
	if n := mk.ScramUpsertCount("user-ref", "SCRAM-SHA-512"); n != 2 {
		t.Fatalf("post-rotation upsert count = %d, want 2", n)
	}
	var rotatedSecret corev1.Secret
	if err := env.cl.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: "user-ref-pw"}, &rotatedSecret); err != nil {
		t.Fatalf("re-get rotated secret: %v", err)
	}
	got = getUser(t, env, "user-ref")
	if ref := got.Status.AppliedPasswordRef; ref == nil || ref.ResourceVersion != rotatedSecret.ResourceVersion {
		t.Fatalf("appliedPasswordRef = %+v, want rV %s after rotation", ref, rotatedSecret.ResourceVersion)
	}
}

// --- generate lifecycle ---

func TestEnvtestUserGenerateLifecycle(t *testing.T) {
	env := startEnv(t)
	defer env.stop()
	ctx := context.Background()

	if err := env.cl.Create(ctx, newCluster(testNamespace, "user-gen-cluster")); err != nil {
		t.Fatalf("create cluster: %v", err)
	}
	if err := env.cl.Create(ctx, envUser("user-gen", &v1alpha1.UserPassword{Generate: &v1alpha1.GeneratePassword{}}, "")); err != nil {
		t.Fatalf("create user: %v", err)
	}

	mk := kafkamock.New(nil, nil)
	r := newUserReconciler(env, mk)
	reconcileFor(t, r, testNamespace, "user-gen")

	got := getUser(t, env, "user-gen")
	if got.Status == nil || got.Status.Phase != v1alpha1.PhaseReady {
		t.Fatalf("status = %+v, want phase Ready", got.Status)
	}
	const secretName = "user-gen-kafka-credentials"
	if got.Status.GeneratedSecretName != secretName {
		t.Fatalf("generatedSecretName = %q, want %q", got.Status.GeneratedSecretName, secretName)
	}
	var sec corev1.Secret
	if err := env.cl.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: secretName}, &sec); err != nil {
		t.Fatalf("generated secret not created: %v", err)
	}
	if !metav1.IsControlledBy(&sec, got) {
		t.Fatalf("generated secret ownerReferences = %+v, want controller ref to the KafkaUser", sec.OwnerReferences)
	}
	if string(sec.Data["username"]) != "user-gen" || string(sec.Data["mechanism"]) != "SCRAM-SHA-512" || len(sec.Data["password"]) == 0 {
		t.Fatalf("generated secret data keys = %v, want username/password/mechanism", sec.Data)
	}
	firstPassword := string(sec.Data["password"])
	if n := mk.ScramUpsertCount("user-gen", "SCRAM-SHA-512"); n != 1 {
		t.Fatalf("upsert count = %d, want 1", n)
	}

	// Steady state: no re-upsert, no secret rewrite.
	reconcileFor(t, r, testNamespace, "user-gen")
	if n := mk.ScramUpsertCount("user-gen", "SCRAM-SHA-512"); n != 1 {
		t.Fatalf("steady-state upsert count = %d, want 1", n)
	}

	// Deleting the generated Secret is the explicit rotation request: a new
	// password is generated, the Secret recreated, and the credential rotated.
	if err := env.cl.Delete(ctx, &sec); err != nil {
		t.Fatalf("delete generated secret: %v", err)
	}
	reconcileFor(t, r, testNamespace, "user-gen")
	var recreated corev1.Secret
	if err := env.cl.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: secretName}, &recreated); err != nil {
		t.Fatalf("generated secret not recreated: %v", err)
	}
	if string(recreated.Data["password"]) == firstPassword {
		t.Fatal("recreated secret carries the OLD password; want a fresh one")
	}
	if !metav1.IsControlledBy(&recreated, got) {
		t.Fatal("recreated secret lacks the controller owner reference")
	}
	if n := mk.ScramUpsertCount("user-gen", "SCRAM-SHA-512"); n != 2 {
		t.Fatalf("post-regeneration upsert count = %d, want 2", n)
	}
}

// --- finalizer: deletionPolicy Delete vs Orphan ---

func TestEnvtestUserFinalizerDelete(t *testing.T) {
	env := startEnv(t)
	defer env.stop()
	ctx := context.Background()

	if err := env.cl.Create(ctx, newCluster(testNamespace, "user-del-cluster")); err != nil {
		t.Fatalf("create cluster: %v", err)
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "user-del-pw", Namespace: testNamespace},
		Data:       map[string][]byte{"password": []byte("pw")},
	}
	if err := env.cl.Create(ctx, secret); err != nil {
		t.Fatalf("create secret: %v", err)
	}
	if err := env.cl.Create(ctx, envUser("user-del", secretKeyRefPassword("user-del-pw"), "Delete")); err != nil {
		t.Fatalf("create user: %v", err)
	}

	mk := kafkamock.New(nil, nil)
	r := newUserReconciler(env, mk)
	reconcileFor(t, r, testNamespace, "user-del")

	got := getUser(t, env, "user-del")
	if !controllerutil.ContainsFinalizer(got, FinalizerName) {
		t.Fatal("finalizer not added before deletion")
	}
	if creds, _ := mk.ListScramCredentials(ctx, "user-del"); len(creds) != 1 {
		t.Fatalf("live credentials = %+v, want 1 before deletion", creds)
	}

	if err := env.cl.Delete(ctx, got); err != nil {
		t.Fatalf("delete user: %v", err)
	}
	// The finalizer retains the object until the deletion reconcile runs.
	marked := getUser(t, env, "user-del")
	if marked.DeletionTimestamp.IsZero() {
		t.Fatal("deletionTimestamp not set; finalizer should retain the object")
	}
	reconcileFor(t, r, testNamespace, "user-del")

	if creds, _ := mk.ListScramCredentials(ctx, "user-del"); len(creds) != 0 {
		t.Fatalf("live credentials = %+v, want removed under deletionPolicy Delete", creds)
	}
	var gone v1alpha1.KafkaUser
	err := env.cl.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: "user-del"}, &gone)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("user still present after finalizer removal; get err = %v", err)
	}
}

func TestEnvtestUserFinalizerOrphan(t *testing.T) {
	env := startEnv(t)
	defer env.stop()
	ctx := context.Background()

	if err := env.cl.Create(ctx, newCluster(testNamespace, "user-orphan-cluster")); err != nil {
		t.Fatalf("create cluster: %v", err)
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "user-orphan-pw", Namespace: testNamespace},
		Data:       map[string][]byte{"password": []byte("pw")},
	}
	if err := env.cl.Create(ctx, secret); err != nil {
		t.Fatalf("create secret: %v", err)
	}
	if err := env.cl.Create(ctx, envUser("user-orphan", secretKeyRefPassword("user-orphan-pw"), "Orphan")); err != nil {
		t.Fatalf("create user: %v", err)
	}

	mk := kafkamock.New(nil, nil)
	r := newUserReconciler(env, mk)
	reconcileFor(t, r, testNamespace, "user-orphan")

	got := getUser(t, env, "user-orphan")
	if err := env.cl.Delete(ctx, got); err != nil {
		t.Fatalf("delete user: %v", err)
	}
	reconcileFor(t, r, testNamespace, "user-orphan")

	if creds, _ := mk.ListScramCredentials(ctx, "user-orphan"); len(creds) != 1 {
		t.Fatalf("live credentials = %+v, want RETAINED under deletionPolicy Orphan", creds)
	}
	if hasCallPrefix(mk.Calls(), "DeleteScramCredential ") {
		t.Fatalf("Orphan must not delete the credential: calls = %v", mk.Calls())
	}
	var gone v1alpha1.KafkaUser
	err := env.cl.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: "user-orphan"}, &gone)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("user still present after finalizer removal; get err = %v", err)
	}
}

// --- foreign generated-Secret collision ---

func TestEnvtestUserForeignGeneratedSecret(t *testing.T) {
	env := startEnv(t)
	defer env.stop()
	ctx := context.Background()

	if err := env.cl.Create(ctx, newCluster(testNamespace, "user-foreign-cluster")); err != nil {
		t.Fatalf("create cluster: %v", err)
	}
	// Pre-create an UN-OWNED Secret with the name generate mode would use.
	foreign := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "user-foreign-kafka-credentials", Namespace: testNamespace},
		Data:       map[string][]byte{"password": []byte("someone-elses")},
	}
	if err := env.cl.Create(ctx, foreign); err != nil {
		t.Fatalf("create foreign secret: %v", err)
	}
	if err := env.cl.Create(ctx, envUser("user-foreign", &v1alpha1.UserPassword{Generate: &v1alpha1.GeneratePassword{}}, "")); err != nil {
		t.Fatalf("create user: %v", err)
	}

	mk := kafkamock.New(nil, nil)
	r := newUserReconciler(env, mk)
	// Terminal outcome: the reconcile returns NO error (no requeue storm).
	reconcileFor(t, r, testNamespace, "user-foreign")

	got := getUser(t, env, "user-foreign")
	if got.Status == nil || got.Status.Phase != v1alpha1.PhaseError {
		t.Fatalf("status = %+v, want phase Error", got.Status)
	}
	if s := condStatus(got.Status.Conditions, v1alpha1.CondValidationFailed); s != metav1.ConditionTrue {
		t.Fatalf("ValidationFailed = %q, want True (ForeignSecret)", s)
	}
	if n := mk.ScramUpsertCount("user-foreign", "SCRAM-SHA-512"); n != 0 {
		t.Fatalf("upsert count = %d, want 0 (no credential for a foreign secret)", n)
	}
	// The foreign Secret is untouched: no adoption (no owner refs), same data.
	var after corev1.Secret
	if err := env.cl.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: "user-foreign-kafka-credentials"}, &after); err != nil {
		t.Fatalf("re-get foreign secret: %v", err)
	}
	if len(after.OwnerReferences) != 0 {
		t.Fatalf("foreign secret was adopted: ownerReferences = %+v", after.OwnerReferences)
	}
	if string(after.Data["password"]) != "someone-elses" {
		t.Fatalf("foreign secret data changed: %v", after.Data)
	}
}
