package reconcile

import (
	"context"
	"errors"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	kafkamock "github.com/monedula-dev/monedula-gitops/internal/kafka/mock"
)

// fakeSecretStore is an in-memory PasswordSecretStore. created/updated record
// writes so tests can assert generate-mode provisioning (and its ordering
// relative to the broker upsert, by injecting createErr).
type fakeSecretStore struct {
	secrets   map[string]*PasswordSecret
	created   map[string]map[string][]byte
	updated   map[string]map[string][]byte
	getErr    error
	createErr error
	updateErr error
}

func (f *fakeSecretStore) GetSecret(_ context.Context, name string) (*PasswordSecret, error) {
	if f.getErr != nil {
		return nil, f.getErr
	}
	return f.secrets[name], nil
}

func (f *fakeSecretStore) CreateOwnedSecret(_ context.Context, name string, data map[string][]byte) error {
	if f.createErr != nil {
		return f.createErr
	}
	if f.created == nil {
		f.created = map[string]map[string][]byte{}
	}
	f.created[name] = data
	if f.secrets == nil {
		f.secrets = map[string]*PasswordSecret{}
	}
	f.secrets[name] = &PasswordSecret{Data: data, ResourceVersion: "1", Owned: true}
	return nil
}

func (f *fakeSecretStore) UpdateOwnedSecret(_ context.Context, name string, data map[string][]byte) error {
	if f.updateErr != nil {
		return f.updateErr
	}
	if f.updated == nil {
		f.updated = map[string]map[string][]byte{}
	}
	f.updated[name] = data
	f.secrets[name].Data = data
	return nil
}

// listScramFailClient wraps the mock and forces ListScramCredentials to error
// (the mock's list is read-only and never fails via FailOn).
type listScramFailClient struct {
	*kafkamock.Client
	err error
}

func (c *listScramFailClient) ListScramCredentials(context.Context, ...string) ([]kafka.ScramCredential, error) {
	return nil, c.err
}

// mkUser builds a KafkaUser named/username "svc-orders" in team-a with the
// given password block.
func mkUser(pw *v1alpha1.UserPassword) *v1alpha1.KafkaUser {
	u := &v1alpha1.KafkaUser{
		Spec: v1alpha1.KafkaUserSpec{
			ClusterRef: v1alpha1.ClusterRef{Name: "c"},
			Password:   pw,
		},
	}
	u.Name = "svc-orders"
	u.Namespace = "team-a"
	u.Generation = 3
	return u
}

func secretRefPassword(name, key string) *v1alpha1.UserPassword {
	return &v1alpha1.UserPassword{ValueFrom: &v1alpha1.ValueSource{
		SecretKeyRef: &v1alpha1.SecretKeyRef{Name: name, Key: key},
	}}
}

func generatePasswordSpec() *v1alpha1.UserPassword {
	return &v1alpha1.UserPassword{Generate: &v1alpha1.GeneratePassword{}}
}

// refStore returns a store holding Secret "creds" with data[password]=pw at rv.
func refStore(pw, rv string) *fakeSecretStore {
	return &fakeSecretStore{secrets: map[string]*PasswordSecret{
		"creds": {Data: map[string][]byte{"password": []byte(pw)}, ResourceVersion: rv},
	}}
}

// seededMock returns a mock with an in-sync svc-orders SCRAM-SHA-512 credential.
func seededMock() *kafkamock.Client {
	return kafkamock.NewWithScramCredentials(nil, nil, []kafka.ScramCredential{
		{User: "svc-orders", Mechanism: "SCRAM-SHA-512", Iterations: 4096},
	})
}

func TestReconcileUserReferencedCreatesCredential(t *testing.T) {
	k := kafkamock.New(nil, nil)
	u := mkUser(secretRefPassword("creds", "password"))

	st, err := ReconcileUser(context.Background(), u, cluster(), k, refStore("s3cr3t", "5"))
	if err != nil {
		t.Fatalf("want nil error, got: %v", err)
	}
	if st.Phase != v1alpha1.PhaseReady {
		t.Fatalf("phase = %q, want Ready", st.Phase)
	}
	if s, _, _ := condStatus(st.Conditions, v1alpha1.CondUserSynced); s != metav1.ConditionTrue {
		t.Fatalf("UserSynced = %v, want True", s)
	}
	if s, _, _ := condStatus(st.Conditions, v1alpha1.CondClusterReachable); s != metav1.ConditionTrue {
		t.Fatalf("ClusterReachable = %v, want True", s)
	}
	if got := k.ScramUpsertCount("svc-orders", "SCRAM-SHA-512"); got != 1 {
		t.Fatalf("upsert count = %d, want 1", got)
	}
	if st.AppliedPasswordRef == nil || st.AppliedPasswordRef.SecretName != "creds" || st.AppliedPasswordRef.ResourceVersion != "5" {
		t.Fatalf("appliedPasswordRef = %+v, want {creds 5}", st.AppliedPasswordRef)
	}
	if st.GeneratedSecretName != "" {
		t.Fatalf("generatedSecretName = %q, want empty in valueFrom mode", st.GeneratedSecretName)
	}
}

// TestReconcileUserReferencedRotationOnResourceVersionChange pins the
// event-driven rotation contract: with an in-sync credential and an
// appliedPasswordRef matching the Secret's resourceVersion, NO upsert happens;
// when the Secret's rV moves, a rotate re-upserts and the ref is updated.
func TestReconcileUserReferencedRotationOnResourceVersionChange(t *testing.T) {
	k := seededMock()
	u := mkUser(secretRefPassword("creds", "password"))
	u.Status = &v1alpha1.KafkaUserStatus{
		AppliedPasswordRef: &v1alpha1.AppliedPasswordRef{SecretName: "creds", ResourceVersion: "5"},
	}

	// Unchanged Secret: steady state, no upsert.
	st, err := ReconcileUser(context.Background(), u, cluster(), k, refStore("s3cr3t", "5"))
	if err != nil {
		t.Fatalf("steady state: want nil error, got: %v", err)
	}
	if got := k.ScramUpsertCount("svc-orders", "SCRAM-SHA-512"); got != 0 {
		t.Fatalf("steady state upserted: count = %d, want 0", got)
	}
	if st.Phase != v1alpha1.PhaseReady {
		t.Fatalf("phase = %q, want Ready", st.Phase)
	}
	if st.AppliedPasswordRef == nil || st.AppliedPasswordRef.ResourceVersion != "5" {
		t.Fatalf("appliedPasswordRef = %+v, want rv 5 preserved", st.AppliedPasswordRef)
	}

	// Secret updated (rV 5 -> 9): rotation.
	u.Status = &st
	st2, err2 := ReconcileUser(context.Background(), u, cluster(), k, refStore("n3w", "9"))
	if err2 != nil {
		t.Fatalf("rotation: want nil error, got: %v", err2)
	}
	if got := k.ScramUpsertCount("svc-orders", "SCRAM-SHA-512"); got != 1 {
		t.Fatalf("rotation upsert count = %d, want 1", got)
	}
	if st2.AppliedPasswordRef == nil || st2.AppliedPasswordRef.ResourceVersion != "9" {
		t.Fatalf("appliedPasswordRef = %+v, want rv 9", st2.AppliedPasswordRef)
	}
}

// TestReconcileUserReferencedEmptyAppliedRefRotates: an in-sync credential with
// NO recorded appliedPasswordRef (first reconcile over a pre-existing
// credential) rotates so the declared source becomes the actual password.
func TestReconcileUserReferencedEmptyAppliedRefRotates(t *testing.T) {
	k := seededMock()
	u := mkUser(secretRefPassword("creds", "password"))

	st, err := ReconcileUser(context.Background(), u, cluster(), k, refStore("s3cr3t", "5"))
	if err != nil {
		t.Fatalf("want nil error, got: %v", err)
	}
	if got := k.ScramUpsertCount("svc-orders", "SCRAM-SHA-512"); got != 1 {
		t.Fatalf("upsert count = %d, want 1 (rotate)", got)
	}
	if st.AppliedPasswordRef == nil || st.AppliedPasswordRef.ResourceVersion != "5" {
		t.Fatalf("appliedPasswordRef = %+v, want {creds 5}", st.AppliedPasswordRef)
	}
}

// TestReconcileUserReferencedRotationFailurePreservesWatermark pins the
// watermark-commit invariant on the failure path: when a rotation is due
// (Secret rV moved past the last applied one) but the broker upsert fails,
// the reconcile must return a transient error AND leave
// status.appliedPasswordRef exactly as it was — advancing it here would
// permanently mask the fact that the new password was never actually
// applied, since the next reconcile would see rV "9" == applied "9" and
// treat the credential as in sync.
func TestReconcileUserReferencedRotationFailurePreservesWatermark(t *testing.T) {
	k := seededMock() // in-sync svc-orders SCRAM-SHA-512 credential
	k.FailOn("UpsertScramCredential", "svc-orders\x00SCRAM-SHA-512", errors.New("broker unavailable"))

	u := mkUser(secretRefPassword("creds", "password"))
	u.Status = &v1alpha1.KafkaUserStatus{
		AppliedPasswordRef: &v1alpha1.AppliedPasswordRef{SecretName: "creds", ResourceVersion: "5"},
	}

	// Secret has rotated (rV 5 -> 9): rotation is due, but the upsert fails.
	st, err := ReconcileUser(context.Background(), u, cluster(), k, refStore("n3w", "9"))
	if err == nil {
		t.Fatal("want transient error when the rotation upsert fails, got nil")
	}
	if st.Phase != v1alpha1.PhaseError {
		t.Fatalf("phase = %q, want Error", st.Phase)
	}
	if st.AppliedPasswordRef == nil || st.AppliedPasswordRef.SecretName != "creds" || st.AppliedPasswordRef.ResourceVersion != "5" {
		t.Fatalf("appliedPasswordRef = %+v, want watermark preserved at {creds 5} (not advanced to 9)", st.AppliedPasswordRef)
	}
}

func TestReconcileUserValidationFailedIsTerminal(t *testing.T) {
	k := kafkamock.New(nil, nil)
	u := mkUser(nil) // no password block: shape-invalid

	st, err := ReconcileUser(context.Background(), u, cluster(), k, &fakeSecretStore{})
	if err != nil {
		t.Fatalf("validation failure is terminal, want nil error, got: %v", err)
	}
	if st.Phase != v1alpha1.PhaseError {
		t.Fatalf("phase = %q, want Error", st.Phase)
	}
	if s, reason, ok := condStatus(st.Conditions, v1alpha1.CondValidationFailed); !ok || s != metav1.ConditionTrue || reason != reasonValidationFailed {
		t.Fatalf("ValidationFailed = %v/%s ok=%v, want True/%s", s, reason, ok, reasonValidationFailed)
	}
	if got := k.Calls(); len(got) != 0 {
		t.Fatalf("validation failure must not mutate: calls = %v", got)
	}
}

// TestReconcileUserTenancyDeniedNamespace: the namespace allow-list applies to
// users (any namespace could otherwise take over another team's principal).
func TestReconcileUserTenancyDeniedNamespace(t *testing.T) {
	k := kafkamock.New(nil, nil)
	u := mkUser(secretRefPassword("creds", "password"))
	u.Namespace = "team-b" // not in allow-list

	cl := tenancyCluster(&v1alpha1.TenancyConfig{AllowedNamespaces: []string{"team-a"}})

	st, err := ReconcileUser(context.Background(), u, cl, k, refStore("s3cr3t", "5"))
	if err != nil {
		t.Fatalf("tenancy denial is terminal, want nil error, got: %v", err)
	}
	if got := k.Calls(); len(got) != 0 {
		t.Fatalf("tenancy denial must not mutate: calls = %v", got)
	}
	if st.Phase != v1alpha1.PhaseError {
		t.Fatalf("phase = %q, want Error", st.Phase)
	}
	if s, reason, ok := condStatus(st.Conditions, v1alpha1.CondValidationFailed); !ok || s != metav1.ConditionTrue || reason != reasonTenancyDenied {
		t.Fatalf("ValidationFailed = %v/%s ok=%v, want True/%s", s, reason, ok, reasonTenancyDenied)
	}
}

// TestReconcileUserUnsupportedSourceIsTerminal: env/file are CLI-only and a
// ConfigMap is not a secret store — operator mode requires secretKeyRef.
func TestReconcileUserUnsupportedSourceIsTerminal(t *testing.T) {
	k := kafkamock.New(nil, nil)
	u := mkUser(&v1alpha1.UserPassword{ValueFrom: &v1alpha1.ValueSource{Env: "KAFKA_PASSWORD"}})

	st, err := ReconcileUser(context.Background(), u, cluster(), k, &fakeSecretStore{})
	if err != nil {
		t.Fatalf("unsupported source is terminal, want nil error, got: %v", err)
	}
	if st.Phase != v1alpha1.PhaseError {
		t.Fatalf("phase = %q, want Error", st.Phase)
	}
	s, _, _ := condStatus(st.Conditions, v1alpha1.CondValidationFailed)
	if s != metav1.ConditionTrue {
		t.Fatalf("ValidationFailed = %v, want True", s)
	}
	if got := k.ScramUpsertCount("svc-orders", "SCRAM-SHA-512"); got != 0 {
		t.Fatalf("upsert count = %d, want 0", got)
	}
}

// TestReconcileUserMissingSecretIsTransient: a missing referenced Secret is a
// retryable error (the Secret may simply not exist YET), not a terminal one.
func TestReconcileUserMissingSecretIsTransient(t *testing.T) {
	k := kafkamock.New(nil, nil)
	u := mkUser(secretRefPassword("creds", "password"))

	st, err := ReconcileUser(context.Background(), u, cluster(), k, &fakeSecretStore{})
	if err == nil {
		t.Fatal("want transient error for missing password secret, got nil")
	}
	if st.Phase != v1alpha1.PhaseError {
		t.Fatalf("phase = %q, want Error", st.Phase)
	}
	if s, reason, ok := condStatus(st.Conditions, v1alpha1.CondReady); !ok || s != metav1.ConditionFalse || reason != reasonPasswordUnresolved {
		t.Fatalf("Ready = %v/%s ok=%v, want False/%s", s, reason, ok, reasonPasswordUnresolved)
	}
	if got := k.ScramUpsertCount("svc-orders", "SCRAM-SHA-512"); got != 0 {
		t.Fatalf("upsert count = %d, want 0", got)
	}
}

func TestReconcileUserLiveStateErrorIsTransient(t *testing.T) {
	k := &listScramFailClient{Client: kafkamock.New(nil, nil), err: errors.New("broker down")}
	u := mkUser(secretRefPassword("creds", "password"))

	st, err := ReconcileUser(context.Background(), u, cluster(), k, refStore("s3cr3t", "5"))
	if err == nil {
		t.Fatal("want transient error for live-state read failure, got nil")
	}
	if st.Phase != v1alpha1.PhaseError {
		t.Fatalf("phase = %q, want Error", st.Phase)
	}
	if s, _, _ := condStatus(st.Conditions, v1alpha1.CondClusterReachable); s != metav1.ConditionFalse {
		t.Fatalf("ClusterReachable = %v, want False", s)
	}
}

// TestReconcileUserDriftIterationsUpdate: a pinned iteration count differing
// from live is identity drift and converges via UpdateScramCredential.
func TestReconcileUserDriftIterationsUpdate(t *testing.T) {
	k := seededMock() // live iterations 4096
	iters := int32(8192)
	u := mkUser(secretRefPassword("creds", "password"))
	u.Spec.Iterations = &iters
	u.Status = &v1alpha1.KafkaUserStatus{
		AppliedPasswordRef: &v1alpha1.AppliedPasswordRef{SecretName: "creds", ResourceVersion: "5"},
	}

	st, err := ReconcileUser(context.Background(), u, cluster(), k, refStore("s3cr3t", "5"))
	if err != nil {
		t.Fatalf("want nil error, got: %v", err)
	}
	if got := k.ScramUpsertCount("svc-orders", "SCRAM-SHA-512"); got != 1 {
		t.Fatalf("upsert count = %d, want 1 (iterations update)", got)
	}
	if st.Phase != v1alpha1.PhaseReady {
		t.Fatalf("phase = %q, want Ready", st.Phase)
	}
	if s, _, _ := condStatus(st.Conditions, v1alpha1.CondDriftDetected); s != metav1.ConditionFalse {
		t.Fatalf("DriftDetected = %v, want False after converging apply", s)
	}
}

func TestReconcileUserGenerateProvisionsSecretAndCredential(t *testing.T) {
	k := kafkamock.New(nil, nil)
	store := &fakeSecretStore{}
	u := mkUser(generatePasswordSpec())

	st, err := ReconcileUser(context.Background(), u, cluster(), k, store)
	if err != nil {
		t.Fatalf("want nil error, got: %v", err)
	}
	if st.Phase != v1alpha1.PhaseReady {
		t.Fatalf("phase = %q, want Ready", st.Phase)
	}
	const wantName = "svc-orders-kafka-credentials"
	data, ok := store.created[wantName]
	if !ok {
		t.Fatalf("generated secret %q not created; created = %v", wantName, store.created)
	}
	if string(data["username"]) != "svc-orders" || string(data["mechanism"]) != "SCRAM-SHA-512" || len(data["password"]) == 0 {
		t.Fatalf("generated secret data = %v, want username/mechanism/password keys populated", data)
	}
	if got := k.ScramUpsertCount("svc-orders", "SCRAM-SHA-512"); got != 1 {
		t.Fatalf("upsert count = %d, want 1", got)
	}
	if st.GeneratedSecretName != wantName {
		t.Fatalf("generatedSecretName = %q, want %q", st.GeneratedSecretName, wantName)
	}
	if st.AppliedPasswordRef != nil {
		t.Fatalf("appliedPasswordRef = %+v, want nil in generate mode", st.AppliedPasswordRef)
	}
}

// TestReconcileUserGenerateSecretCreateFailureBlocksUpsert pins the
// create-before-upsert ordering: if the Secret cannot be persisted, NO broker
// upsert may happen (otherwise a crash would strand an unreadable password).
func TestReconcileUserGenerateSecretCreateFailureBlocksUpsert(t *testing.T) {
	k := kafkamock.New(nil, nil)
	store := &fakeSecretStore{createErr: errors.New("apiserver unavailable")}
	u := mkUser(generatePasswordSpec())

	st, err := ReconcileUser(context.Background(), u, cluster(), k, store)
	if err == nil {
		t.Fatal("want transient error when the secret create fails, got nil")
	}
	if got := k.ScramUpsertCount("svc-orders", "SCRAM-SHA-512"); got != 0 {
		t.Fatalf("upsert count = %d, want 0 (secret-first ordering)", got)
	}
	if s, reason, _ := condStatus(st.Conditions, v1alpha1.CondReady); s != metav1.ConditionFalse || reason != reasonPasswordUnresolved {
		t.Fatalf("Ready = %v/%s, want False/%s", s, reason, reasonPasswordUnresolved)
	}
}

// TestReconcileUserGenerateSteadyStateNoOp: owned Secret present + credential
// in sync -> zero upserts, no writes.
func TestReconcileUserGenerateSteadyStateNoOp(t *testing.T) {
	k := seededMock()
	store := &fakeSecretStore{secrets: map[string]*PasswordSecret{
		"svc-orders-kafka-credentials": {
			Data: map[string][]byte{
				"username":  []byte("svc-orders"),
				"password":  []byte("existing"),
				"mechanism": []byte("SCRAM-SHA-512"),
			},
			ResourceVersion: "7",
			Owned:           true,
		},
	}}
	u := mkUser(generatePasswordSpec())
	u.Status = &v1alpha1.KafkaUserStatus{GeneratedSecretName: "svc-orders-kafka-credentials"}

	st, err := ReconcileUser(context.Background(), u, cluster(), k, store)
	if err != nil {
		t.Fatalf("want nil error, got: %v", err)
	}
	if got := k.ScramUpsertCount("svc-orders", "SCRAM-SHA-512"); got != 0 {
		t.Fatalf("upsert count = %d, want 0 in steady state", got)
	}
	if len(store.created) != 0 || len(store.updated) != 0 {
		t.Fatalf("steady state wrote secrets: created=%v updated=%v", store.created, store.updated)
	}
	if st.Phase != v1alpha1.PhaseReady {
		t.Fatalf("phase = %q, want Ready", st.Phase)
	}
}

// TestReconcileUserGenerateReupsertsMissingCredential: the owned Secret exists
// but the broker credential was deleted out-of-band -> re-upsert using the
// EXISTING Secret's password (no regeneration, no secret writes).
func TestReconcileUserGenerateReupsertsMissingCredential(t *testing.T) {
	k := kafkamock.New(nil, nil) // no live credential
	store := &fakeSecretStore{secrets: map[string]*PasswordSecret{
		"svc-orders-kafka-credentials": {
			Data: map[string][]byte{
				"username":  []byte("svc-orders"),
				"password":  []byte("existing"),
				"mechanism": []byte("SCRAM-SHA-512"),
			},
			ResourceVersion: "7",
			Owned:           true,
		},
	}}
	u := mkUser(generatePasswordSpec())
	u.Status = &v1alpha1.KafkaUserStatus{GeneratedSecretName: "svc-orders-kafka-credentials"}

	st, err := ReconcileUser(context.Background(), u, cluster(), k, store)
	if err != nil {
		t.Fatalf("want nil error, got: %v", err)
	}
	if got := k.ScramUpsertCount("svc-orders", "SCRAM-SHA-512"); got != 1 {
		t.Fatalf("upsert count = %d, want 1 (re-upsert of deleted credential)", got)
	}
	if len(store.created) != 0 || len(store.updated) != 0 {
		t.Fatalf("re-upsert must reuse the existing secret: created=%v updated=%v", store.created, store.updated)
	}
	if st.Phase != v1alpha1.PhaseReady {
		t.Fatalf("phase = %q, want Ready", st.Phase)
	}
}

// TestReconcileUserGenerateForeignSecretIsTerminal: an un-owned Secret with the
// generated name is never adopted, never overwritten, and blocks the upsert.
func TestReconcileUserGenerateForeignSecretIsTerminal(t *testing.T) {
	k := kafkamock.New(nil, nil)
	store := &fakeSecretStore{secrets: map[string]*PasswordSecret{
		"svc-orders-kafka-credentials": {
			Data:            map[string][]byte{"password": []byte("someone-elses")},
			ResourceVersion: "2",
			Owned:           false,
		},
	}}
	u := mkUser(generatePasswordSpec())

	st, err := ReconcileUser(context.Background(), u, cluster(), k, store)
	if err != nil {
		t.Fatalf("foreign secret is terminal, want nil error, got: %v", err)
	}
	if st.Phase != v1alpha1.PhaseError {
		t.Fatalf("phase = %q, want Error", st.Phase)
	}
	s, reason, ok := condStatus(st.Conditions, v1alpha1.CondValidationFailed)
	if !ok || s != metav1.ConditionTrue || reason != reasonForeignSecret {
		t.Fatalf("ValidationFailed = %v/%s ok=%v, want True/%s", s, reason, ok, reasonForeignSecret)
	}
	if got := k.ScramUpsertCount("svc-orders", "SCRAM-SHA-512"); got != 0 {
		t.Fatalf("upsert count = %d, want 0", got)
	}
	if len(store.created) != 0 || len(store.updated) != 0 {
		t.Fatalf("foreign secret must not be touched: created=%v updated=%v", store.created, store.updated)
	}
}

// TestReconcileUserGenerateMechanismChangeRefreshesSecret: a mechanism change
// re-stamps the owned Secret's metadata keys (password preserved) and the diff
// migrates the credential (upsert new mechanism + delete old).
func TestReconcileUserGenerateMechanismChangeRefreshesSecret(t *testing.T) {
	k := seededMock() // live SCRAM-SHA-512
	store := &fakeSecretStore{secrets: map[string]*PasswordSecret{
		"svc-orders-kafka-credentials": {
			Data: map[string][]byte{
				"username":  []byte("svc-orders"),
				"password":  []byte("existing"),
				"mechanism": []byte("SCRAM-SHA-512"),
			},
			ResourceVersion: "7",
			Owned:           true,
		},
	}}
	u := mkUser(generatePasswordSpec())
	u.Spec.Mechanism = "SCRAM-SHA-256"
	u.Status = &v1alpha1.KafkaUserStatus{GeneratedSecretName: "svc-orders-kafka-credentials"}

	st, err := ReconcileUser(context.Background(), u, cluster(), k, store)
	if err != nil {
		t.Fatalf("want nil error, got: %v", err)
	}
	upd, ok := store.updated["svc-orders-kafka-credentials"]
	if !ok {
		t.Fatalf("secret metadata not refreshed; updated = %v", store.updated)
	}
	if string(upd["mechanism"]) != "SCRAM-SHA-256" || string(upd["password"]) != "existing" {
		t.Fatalf("updated data = %v, want mechanism refreshed and password preserved", upd)
	}
	if got := k.ScramUpsertCount("svc-orders", "SCRAM-SHA-256"); got != 1 {
		t.Fatalf("new-mechanism upsert count = %d, want 1", got)
	}
	if !containsCall(k.Calls(), "DeleteScramCredential svc-orders\x00SCRAM-SHA-512") {
		t.Fatalf("old mechanism SCRAM-SHA-512 not deleted: calls = %v", k.Calls())
	}
	if st.Phase != v1alpha1.PhaseReady {
		t.Fatalf("phase = %q, want Ready", st.Phase)
	}
}

func containsCall(calls []string, want string) bool {
	for _, c := range calls {
		if c == want {
			return true
		}
	}
	return false
}
