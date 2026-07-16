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

func fl(v float64) *float64 { return &v }

// newQuotaFakeClient builds a fake client seeded with objs and with the
// QuotaClusterRefNameIndex field index registered (mirroring RegisterIndexes) so
// the validator's MatchingFields List works hermetically.
func newQuotaFakeClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	return fake.NewClientBuilder().
		WithScheme(newScheme(t)).
		WithIndex(&v1alpha1.KafkaQuota{}, QuotaClusterRefNameIndex, func(obj client.Object) []string {
			q, ok := obj.(*v1alpha1.KafkaQuota)
			if !ok {
				return nil
			}
			return []string{q.Spec.ClusterRef.Name}
		}).
		WithObjects(objs...).
		Build()
}

// quotaCR builds a KafkaQuota with a distinct UID and a single producer-byte-rate
// limit, targeting the given user entity.
func quotaCR(uid, ns, name, clusterRef, user string) *v1alpha1.KafkaQuota {
	return &v1alpha1.KafkaQuota{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, UID: types.UID(uid)},
		Spec: v1alpha1.KafkaQuotaSpec{
			ClusterRef: v1alpha1.ClusterRef{Name: clusterRef},
			Entity:     v1alpha1.QuotaEntity{User: user},
			Limits:     v1alpha1.QuotaLimits{ProducerByteRate: fl(1024)},
		},
	}
}

func TestQuotaValidateCreate_DuplicateSameNamespace_Denied(t *testing.T) {
	existing := quotaCR("uid-a", "team-a", "q-alice", "prod", "User:alice")
	v := &KafkaQuotaValidator{Reader: newQuotaFakeClient(t, existing)}

	dup := quotaCR("uid-b", "team-a", "q-alice-dup", "prod", "User:alice")
	_, err := v.ValidateCreate(context.Background(), dup)
	if err == nil {
		t.Fatal("expected denial for duplicate identity in same namespace")
	}
	if !strings.Contains(err.Error(), "team-a/q-alice") ||
		!strings.Contains(err.Error(), "team-a/q-alice-dup") ||
		!strings.Contains(err.Error(), "user=alice") {
		t.Fatalf("error should name both CRs and the contested entity: %v", err)
	}
}

func TestQuotaValidateCreate_SameIdentityDifferentNamespace_NoClusterNamespace_Allowed(t *testing.T) {
	existing := quotaCR("uid-a", "team-a", "q-alice", "prod", "User:alice")
	// No ClusterNamespace: clusterRef "prod" is namespace-local, so team-b/prod
	// is a DIFFERENT cluster object than team-a/prod -> no collision.
	v := &KafkaQuotaValidator{Reader: newQuotaFakeClient(t, existing)}

	other := quotaCR("uid-b", "team-b", "q-alice", "prod", "User:alice")
	if _, err := v.ValidateCreate(context.Background(), other); err != nil {
		t.Fatalf("expected allow for same identity in different namespace without cluster-namespace: %v", err)
	}
}

func TestQuotaValidateCreate_SameIdentityDifferentNamespace_WithClusterNamespace_Denied(t *testing.T) {
	existing := quotaCR("uid-a", "team-a", "q-alice", "prod", "User:alice")
	v := &KafkaQuotaValidator{Reader: newQuotaFakeClient(t, existing), ClusterNamespace: "kafka"}

	other := quotaCR("uid-b", "team-b", "q-alice", "prod", "User:alice")
	_, err := v.ValidateCreate(context.Background(), other)
	if err == nil {
		t.Fatal("expected denial for cluster-wide duplicate with cluster-namespace set")
	}
	if !strings.Contains(err.Error(), "kafka") {
		t.Fatalf("error should name the shared cluster namespace: %v", err)
	}
}

func TestQuotaValidateUpdate_EntityChange_Denied(t *testing.T) {
	v := &KafkaQuotaValidator{Reader: newQuotaFakeClient(t)}
	old := quotaCR("uid-a", "team-a", "q-alice", "prod", "User:alice")
	updated := quotaCR("uid-a", "team-a", "q-alice", "prod", "User:bob")

	_, err := v.ValidateUpdate(context.Background(), old, updated)
	if err == nil {
		t.Fatal("expected denial for entity change")
	}
	if !strings.Contains(err.Error(), "immutable") ||
		!strings.Contains(err.Error(), "user=alice") ||
		!strings.Contains(err.Error(), "user=bob") {
		t.Fatalf("error should show old and new entity keys: %v", err)
	}
}

func TestQuotaValidateUpdate_ClusterRefChange_Denied(t *testing.T) {
	// Repointing the clusterRef orphans the quota applied on the previous
	// cluster; the webhook mirrors the always-on CEL rule on KafkaQuotaSpec.
	v := &KafkaQuotaValidator{Reader: newQuotaFakeClient(t)}
	old := quotaCR("uid-a", "team-a", "q-alice", "prod", "User:alice")
	updated := quotaCR("uid-a", "team-a", "q-alice", "staging", "User:alice")

	_, err := v.ValidateUpdate(context.Background(), old, updated)
	if err == nil {
		t.Fatal("expected denial for clusterRef change")
	}
	if !strings.Contains(err.Error(), "spec.clusterRef.name is immutable") ||
		!strings.Contains(err.Error(), "prod") || !strings.Contains(err.Error(), "staging") {
		t.Fatalf("error should name the field and both cluster refs: %v", err)
	}
}

func TestQuotaValidateUpdate_SelfUpdate_Allowed(t *testing.T) {
	existing := quotaCR("uid-a", "team-a", "q-alice", "prod", "User:alice")
	v := &KafkaQuotaValidator{Reader: newQuotaFakeClient(t, existing)}

	updated := quotaCR("uid-a", "team-a", "q-alice", "prod", "User:alice")
	updated.Spec.Limits.ProducerByteRate = fl(2048) // unrelated mutation
	if _, err := v.ValidateUpdate(context.Background(), existing, updated); err != nil {
		t.Fatalf("expected allow for self-update: %v", err)
	}
}

func TestQuotaValidateCreate_InvalidShape_Denied(t *testing.T) {
	v := &KafkaQuotaValidator{Reader: newQuotaFakeClient(t)}
	// user AND userDefault set -> mutually exclusive shape violation.
	bad := quotaCR("uid-b", "team-a", "q-bad", "prod", "User:alice")
	bad.Spec.Entity.UserDefault = true

	_, err := v.ValidateCreate(context.Background(), bad)
	if err == nil {
		t.Fatal("expected denial for invalid shape (user + userDefault)")
	}
	if !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("expected a shape error: %v", err)
	}
}

func TestQuotaValidateCreate_ValidShape_Allowed(t *testing.T) {
	v := &KafkaQuotaValidator{Reader: newQuotaFakeClient(t)}
	good := quotaCR("uid-b", "team-a", "q-good", "prod", "User:alice")
	if _, err := v.ValidateCreate(context.Background(), good); err != nil {
		t.Fatalf("expected allow for valid quota: %v", err)
	}
}

func TestQuotaValidateCreate_DeletingDuplicate_StillBlocks(t *testing.T) {
	deleting := quotaCR("uid-a", "team-a", "q-alice", "prod", "User:alice")
	now := metav1.Now()
	deleting.DeletionTimestamp = &now
	deleting.Finalizers = []string{"gitops.monedula.dev/finalizer"}
	v := &KafkaQuotaValidator{Reader: newQuotaFakeClient(t, deleting)}

	dup := quotaCR("uid-b", "team-a", "q-alice-dup", "prod", "User:alice")
	if _, err := v.ValidateCreate(context.Background(), dup); err == nil {
		t.Fatal("expected denial: a deleting CR still occupies the identity")
	}
}

func TestQuotaValidateDelete_AlwaysAllowed(t *testing.T) {
	v := &KafkaQuotaValidator{Reader: newQuotaFakeClient(t)}
	q := quotaCR("uid-a", "team-a", "q-alice", "prod", "User:alice")
	if _, err := v.ValidateDelete(context.Background(), q); err != nil {
		t.Fatalf("delete must always be allowed: %v", err)
	}
}
