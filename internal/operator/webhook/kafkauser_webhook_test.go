package webhook

import (
	"context"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
)

// newUserFakeClient builds a fake client seeded with objs and with the
// UserClusterRefNameIndex field index registered (mirroring RegisterIndexes)
// so the validator's MatchingFields List works hermetically.
func newUserFakeClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	return fake.NewClientBuilder().
		WithScheme(newScheme(t)).
		WithIndex(&v1alpha1.KafkaUser{}, UserClusterRefNameIndex, func(obj client.Object) []string {
			u, ok := obj.(*v1alpha1.KafkaUser)
			if !ok {
				return nil
			}
			return []string{u.Spec.ClusterRef.Name}
		}).
		WithObjects(objs...).
		Build()
}

// userGenerate builds a valid password block using generate mode, so shape
// checks pass by default.
func userGenerate() *v1alpha1.UserPassword {
	return &v1alpha1.UserPassword{Generate: &v1alpha1.GeneratePassword{}}
}

// userCR builds a minimal, shape-valid KafkaUser with a distinct UID.
// username may be "" to exercise the metadata.name default.
func userCR(uid, ns, name, clusterRef, username string) *v1alpha1.KafkaUser {
	return &v1alpha1.KafkaUser{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, UID: types.UID(uid)},
		Spec: v1alpha1.KafkaUserSpec{
			ClusterRef: v1alpha1.ClusterRef{Name: clusterRef},
			Username:   username,
			Password:   userGenerate(),
		},
	}
}

// ---- shape tests ----

func TestUserValidateCreate_InvalidShape_Denied(t *testing.T) {
	v := &KafkaUserValidator{Reader: newUserFakeClient(t)}
	// Password unset -> shape violation (required, exactly one of valueFrom/generate).
	bad := userCR("uid-bad", "team-a", "u-bad", "prod", "alice")
	bad.Spec.Password = nil

	_, err := v.ValidateCreate(context.Background(), bad)
	if err == nil {
		t.Fatal("expected denial for missing password")
	}
	if !strings.Contains(err.Error(), "spec.password is required") {
		t.Fatalf("expected a shape error mentioning password: %v", err)
	}
}

func TestUserValidateCreate_PasswordBothSources_Denied(t *testing.T) {
	v := &KafkaUserValidator{Reader: newUserFakeClient(t)}
	bad := userCR("uid-bad", "team-a", "u-bad", "prod", "alice")
	bad.Spec.Password = &v1alpha1.UserPassword{
		Generate:  &v1alpha1.GeneratePassword{},
		ValueFrom: &v1alpha1.ValueSource{SecretKeyRef: &v1alpha1.SecretKeyRef{Name: "s", Key: "k"}},
	}

	_, err := v.ValidateCreate(context.Background(), bad)
	if err == nil {
		t.Fatal("expected denial for both password sources set")
	}
	if !strings.Contains(err.Error(), "exactly one of valueFrom or generate") {
		t.Fatalf("expected a mutually-exclusive shape error: %v", err)
	}
}

func TestUserValidateCreate_PasswordInline_Denied(t *testing.T) {
	v := &KafkaUserValidator{Reader: newUserFakeClient(t)}
	bad := userCR("uid-bad", "team-a", "u-bad", "prod", "alice")
	bad.Spec.Password = &v1alpha1.UserPassword{
		ValueFrom: &v1alpha1.ValueSource{Inline: "plaintext-secret"},
	}

	_, err := v.ValidateCreate(context.Background(), bad)
	if err == nil {
		t.Fatal("expected denial for inline password value")
	}
	if !strings.Contains(err.Error(), "KafkaUser") {
		t.Fatalf("expected a shape error: %v", err)
	}
}

func TestUserValidateCreate_PasswordConfigMapKeyRef_Denied(t *testing.T) {
	v := &KafkaUserValidator{Reader: newUserFakeClient(t)}
	bad := userCR("uid-bad", "team-a", "u-bad", "prod", "alice")
	bad.Spec.Password = &v1alpha1.UserPassword{
		ValueFrom: &v1alpha1.ValueSource{ConfigMapKeyRef: &v1alpha1.SecretKeyRef{Name: "cm", Key: "k"}},
	}

	_, err := v.ValidateCreate(context.Background(), bad)
	if err == nil {
		t.Fatal("expected denial for configMapKeyRef password source")
	}
	if !strings.Contains(err.Error(), "KafkaUser") {
		t.Fatalf("expected a shape error: %v", err)
	}
}

func TestUserValidateCreate_InvalidMechanism_Denied(t *testing.T) {
	v := &KafkaUserValidator{Reader: newUserFakeClient(t)}
	bad := userCR("uid-bad", "team-a", "u-bad", "prod", "alice")
	bad.Spec.Mechanism = "MD5"

	_, err := v.ValidateCreate(context.Background(), bad)
	if err == nil {
		t.Fatal("expected denial for invalid mechanism")
	}
	if !strings.Contains(err.Error(), "mechanism") {
		t.Fatalf("expected a shape error mentioning mechanism: %v", err)
	}
}

func TestUserValidateCreate_InvalidIterations_Denied(t *testing.T) {
	v := &KafkaUserValidator{Reader: newUserFakeClient(t)}
	bad := userCR("uid-bad", "team-a", "u-bad", "prod", "alice")
	it := int32(100)
	bad.Spec.Iterations = &it

	_, err := v.ValidateCreate(context.Background(), bad)
	if err == nil {
		t.Fatal("expected denial for out-of-range iterations")
	}
	if !strings.Contains(err.Error(), "iterations") {
		t.Fatalf("expected a shape error mentioning iterations: %v", err)
	}
}

func TestUserValidateCreate_ValidShape_Allowed(t *testing.T) {
	v := &KafkaUserValidator{Reader: newUserFakeClient(t)}
	good := userCR("uid-a", "team-a", "u-alice", "prod", "alice")
	if _, err := v.ValidateCreate(context.Background(), good); err != nil {
		t.Fatalf("expected allow for valid user: %v", err)
	}
}

// ---- identity uniqueness tests ----

func TestUserValidateCreate_DuplicateSameNamespace_Denied(t *testing.T) {
	existing := userCR("uid-a", "team-a", "u-alice", "prod", "alice")
	v := &KafkaUserValidator{Reader: newUserFakeClient(t, existing)}

	dup := userCR("uid-b", "team-a", "u-alice-dup", "prod", "alice")
	_, err := v.ValidateCreate(context.Background(), dup)
	if err == nil {
		t.Fatal("expected denial for duplicate identity in same namespace")
	}
	if !strings.Contains(err.Error(), "team-a/u-alice") ||
		!strings.Contains(err.Error(), "team-a/u-alice-dup") ||
		!strings.Contains(err.Error(), "alice") {
		t.Fatalf("error should name both CRs and the contested username: %v", err)
	}
}

func TestUserValidateCreate_SameUsernameDifferentCluster_Allowed(t *testing.T) {
	existing := userCR("uid-a", "team-a", "u-alice", "prod", "alice")
	v := &KafkaUserValidator{Reader: newUserFakeClient(t, existing)}

	other := userCR("uid-b", "team-a", "u-alice-staging", "staging", "alice")
	if _, err := v.ValidateCreate(context.Background(), other); err != nil {
		t.Fatalf("expected allow for same username on a different cluster: %v", err)
	}
}

func TestUserValidateCreate_SameIdentityDifferentNamespace_NoClusterNamespace_Allowed(t *testing.T) {
	existing := userCR("uid-a", "team-a", "u-alice", "prod", "alice")
	// No ClusterNamespace: clusterRef "prod" is namespace-local, so team-b/prod
	// is a DIFFERENT cluster object than team-a/prod -> no collision.
	v := &KafkaUserValidator{Reader: newUserFakeClient(t, existing)}

	other := userCR("uid-b", "team-b", "u-alice", "prod", "alice")
	if _, err := v.ValidateCreate(context.Background(), other); err != nil {
		t.Fatalf("expected allow for same identity in different namespace without cluster-namespace: %v", err)
	}
}

func TestUserValidateCreate_SameIdentityDifferentNamespace_WithClusterNamespace_Denied(t *testing.T) {
	existing := userCR("uid-a", "team-a", "u-alice", "prod", "alice")
	v := &KafkaUserValidator{Reader: newUserFakeClient(t, existing), ClusterNamespace: "kafka"}

	other := userCR("uid-b", "team-b", "u-alice", "prod", "alice")
	_, err := v.ValidateCreate(context.Background(), other)
	if err == nil {
		t.Fatal("expected denial for cluster-wide duplicate with cluster-namespace set")
	}
	if !strings.Contains(err.Error(), "kafka") {
		t.Fatalf("error should name the shared cluster namespace: %v", err)
	}
}

func TestUserValidateCreate_DeletingDuplicate_StillBlocks(t *testing.T) {
	deleting := userCR("uid-a", "team-a", "u-alice", "prod", "alice")
	now := metav1.Now()
	deleting.DeletionTimestamp = &now
	deleting.Finalizers = []string{"gitops.monedula.dev/finalizer"}
	v := &KafkaUserValidator{Reader: newUserFakeClient(t, deleting)}

	dup := userCR("uid-b", "team-a", "u-alice-dup", "prod", "alice")
	if _, err := v.ValidateCreate(context.Background(), dup); err == nil {
		t.Fatal("expected denial: a deleting CR still occupies the identity")
	}
}

// ---- update / immutability tests ----

func TestUserValidateUpdate_Rename_Denied(t *testing.T) {
	v := &KafkaUserValidator{Reader: newUserFakeClient(t)}
	old := userCR("uid-a", "team-a", "u-alice", "prod", "alice")
	updated := userCR("uid-a", "team-a", "u-alice", "prod", "alice2")

	_, err := v.ValidateUpdate(context.Background(), old, updated)
	if err == nil {
		t.Fatal("expected denial for username rename")
	}
	if !strings.Contains(err.Error(), "immutable") ||
		!strings.Contains(err.Error(), "alice") || !strings.Contains(err.Error(), "alice2") {
		t.Fatalf("error should show old and new usernames: %v", err)
	}
}

func TestUserValidateUpdate_UnsetToSetSameAsName_Allowed(t *testing.T) {
	// old has username unset (resolves to metadata.name "u-alice"); new sets it
	// explicitly to the SAME resolved value -> no-op re-spelling, allowed.
	v := &KafkaUserValidator{Reader: newUserFakeClient(t)}
	old := userCR("uid-a", "team-a", "u-alice", "prod", "")
	updated := userCR("uid-a", "team-a", "u-alice", "prod", "u-alice")

	if _, err := v.ValidateUpdate(context.Background(), old, updated); err != nil {
		t.Fatalf("expected allow for unset -> explicit-same-as-name: %v", err)
	}
}

func TestUserValidateUpdate_UnsetToSetDifferent_Denied(t *testing.T) {
	// old has username unset (resolves to metadata.name "u-bob"); new sets it
	// to a DIFFERENT value. The CEL rule on KafkaUserSpec cannot catch this
	// (it only compares oldSelf.username vs self.username, and oldSelf.username
	// is empty), so this is the webhook-only richer check.
	v := &KafkaUserValidator{Reader: newUserFakeClient(t)}
	old := userCR("uid-a", "team-a", "u-bob", "prod", "")
	updated := userCR("uid-a", "team-a", "u-bob", "prod", "alice")

	_, err := v.ValidateUpdate(context.Background(), old, updated)
	if err == nil {
		t.Fatal("expected denial for unset -> set-different resolved username")
	}
	if !strings.Contains(err.Error(), "immutable") ||
		!strings.Contains(err.Error(), "u-bob") || !strings.Contains(err.Error(), "alice") {
		t.Fatalf("error should show the resolved old (metadata.name) and new username: %v", err)
	}
}

func TestUserValidateUpdate_ClusterRefChange_Denied(t *testing.T) {
	// Repointing the clusterRef orphans the credential on the previous cluster;
	// the webhook mirrors the always-on CEL rule on KafkaUserSpec.
	v := &KafkaUserValidator{Reader: newUserFakeClient(t)}
	old := userCR("uid-a", "team-a", "u-alice", "prod", "alice")
	updated := userCR("uid-a", "team-a", "u-alice", "staging", "alice")

	_, err := v.ValidateUpdate(context.Background(), old, updated)
	if err == nil {
		t.Fatal("expected denial for clusterRef change")
	}
	if !strings.Contains(err.Error(), "spec.clusterRef.name is immutable") ||
		!strings.Contains(err.Error(), "prod") || !strings.Contains(err.Error(), "staging") {
		t.Fatalf("error should name the field and both cluster refs: %v", err)
	}
}

func TestUserValidateUpdate_SelfUpdate_Allowed(t *testing.T) {
	existing := userCR("uid-a", "team-a", "u-alice", "prod", "alice")
	v := &KafkaUserValidator{Reader: newUserFakeClient(t, existing)}

	updated := userCR("uid-a", "team-a", "u-alice", "prod", "alice")
	it := int32(8192)
	updated.Spec.Iterations = &it // unrelated mutation
	if _, err := v.ValidateUpdate(context.Background(), existing, updated); err != nil {
		t.Fatalf("expected allow for self-update: %v", err)
	}
}

// ---- delete tests ----

func TestUserValidateDelete_AlwaysAllowed(t *testing.T) {
	v := &KafkaUserValidator{Reader: newUserFakeClient(t)}
	u := userCR("uid-a", "team-a", "u-alice", "prod", "alice")
	if _, err := v.ValidateDelete(context.Background(), u); err != nil {
		t.Fatalf("delete must always be allowed: %v", err)
	}
}
