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

// newRBFakeClient builds a fake client seeded with objs and with the
// RoleBindingClusterRefNameIndex field index registered (mirroring
// RegisterIndexes) so the validator's MatchingFields List works hermetically.
func newRBFakeClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	return fake.NewClientBuilder().
		WithScheme(newScheme(t)).
		WithIndex(&v1alpha1.KafkaRoleBinding{}, RoleBindingClusterRefNameIndex, func(obj client.Object) []string {
			rb, ok := obj.(*v1alpha1.KafkaRoleBinding)
			if !ok {
				return nil
			}
			return []string{rb.Spec.ClusterRef.Name}
		}).
		WithObjects(objs...).
		Build()
}

// rbCR builds a KafkaRoleBinding with a distinct UID targeting the given
// cluster/principal/role/scope.type. Uses "ClusterAdmin" (cluster-scoped) by
// default so no resources are required to satisfy the shape check.
func rbCR(uid, ns, name, clusterRef, principal, role, scopeType string) *v1alpha1.KafkaRoleBinding {
	return &v1alpha1.KafkaRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, UID: types.UID(uid)},
		Spec: v1alpha1.KafkaRoleBindingSpec{
			ClusterRef: v1alpha1.ClusterRef{Name: clusterRef},
			Principal:  principal,
			Role:       role,
			Scope:      v1alpha1.RoleBindingScope{Type: scopeType},
		},
	}
}

// clusterWithMDS builds a KafkaCluster with the given MDS kafka cluster ID.
func clusterWithMDS(ns, name, kafkaClusterID string) *v1alpha1.KafkaCluster {
	return &v1alpha1.KafkaCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: v1alpha1.KafkaClusterSpec{
			BootstrapServers: "localhost:9092",
			Authorization: &v1alpha1.AuthorizationConfig{
				MDS: &v1alpha1.MDSConfig{
					Endpoint: "https://mds:8090",
					Clusters: v1alpha1.MDSClusters{
						KafkaCluster: kafkaClusterID,
					},
				},
			},
		},
	}
}

// TestRBValidateCreate_ShapeRejection ensures that a role binding with an
// invalid scope.type is rejected at create time.
func TestRBValidateCreate_ShapeRejection(t *testing.T) {
	v := &KafkaRoleBindingValidator{Reader: newRBFakeClient(t)}
	// "ClusterAdmin" is cluster-scoped (no resources needed) but the scope.type is invalid.
	bad := rbCR("uid-a", "team-a", "rb-bad", "prod", "User:alice", "ClusterAdmin", "INVALID_SCOPE")

	_, err := v.ValidateCreate(context.Background(), bad)
	if err == nil {
		t.Fatal("expected shape rejection for invalid scope.type")
	}
	if !strings.Contains(err.Error(), "scope.type") {
		t.Fatalf("error should mention scope.type: %v", err)
	}
}

// TestRBValidateCreate_IdentityCollision_Denied ensures that two role bindings
// resolving to the same MDS binding identity are rejected.
func TestRBValidateCreate_IdentityCollision_Denied(t *testing.T) {
	cluster := clusterWithMDS("team-a", "prod", "kafka-cluster-1")
	existing := rbCR("uid-a", "team-a", "rb-alice", "prod", "User:alice", "ClusterAdmin", "kafka")
	cl := newRBFakeClient(t, cluster, existing)
	v := &KafkaRoleBindingValidator{Reader: cl}

	// Same cluster-scoped binding (same principal/role/scope/no-resources) = same MDS identity.
	dup := rbCR("uid-b", "team-a", "rb-alice-dup", "prod", "User:alice", "ClusterAdmin", "kafka")
	_, err := v.ValidateCreate(context.Background(), dup)
	if err == nil {
		t.Fatal("expected denial for duplicate identity in same namespace")
	}
	if !strings.Contains(err.Error(), "team-a/rb-alice") ||
		!strings.Contains(err.Error(), "team-a/rb-alice-dup") {
		t.Fatalf("error should name both CRs: %v", err)
	}
}

// TestRBValidateCreate_DifferentIdentities_Allowed ensures that two role
// bindings with different MDS identities (different principals) are both allowed.
func TestRBValidateCreate_DifferentIdentities_Allowed(t *testing.T) {
	cluster := clusterWithMDS("team-a", "prod", "kafka-cluster-1")
	existing := rbCR("uid-a", "team-a", "rb-alice", "prod", "User:alice", "ClusterAdmin", "kafka")
	cl := newRBFakeClient(t, cluster, existing)
	v := &KafkaRoleBindingValidator{Reader: cl}

	// Different principal → different MDS identity → no conflict.
	other := rbCR("uid-b", "team-a", "rb-bob", "prod", "User:bob", "ClusterAdmin", "kafka")
	if _, err := v.ValidateCreate(context.Background(), other); err != nil {
		t.Fatalf("expected allow for different principal: %v", err)
	}
}

// TestRBValidateCreate_ClusterNotFound_Allow ensures that when the referenced
// cluster does not exist, the webhook allows the binding (defer to reconcile).
func TestRBValidateCreate_ClusterNotFound_Allow(t *testing.T) {
	// No cluster in the fake client.
	v := &KafkaRoleBindingValidator{Reader: newRBFakeClient(t)}
	rb := rbCR("uid-a", "team-a", "rb-alice", "prod", "User:alice", "ClusterAdmin", "kafka")
	if _, err := v.ValidateCreate(context.Background(), rb); err != nil {
		t.Fatalf("cluster-not-found should allow: %v", err)
	}
}

// TestRBValidateCreate_MDSNotConfigured_Allow ensures that when the referenced
// cluster exists but has no MDS configuration, the webhook allows (defer to reconcile).
func TestRBValidateCreate_MDSNotConfigured_Allow(t *testing.T) {
	// Cluster without MDS configuration.
	cluster := &v1alpha1.KafkaCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "prod", Namespace: "team-a"},
		Spec:       v1alpha1.KafkaClusterSpec{BootstrapServers: "localhost:9092"},
	}
	cl := newRBFakeClient(t, cluster)
	v := &KafkaRoleBindingValidator{Reader: cl}

	rb := rbCR("uid-a", "team-a", "rb-alice", "prod", "User:alice", "ClusterAdmin", "kafka")
	if _, err := v.ValidateCreate(context.Background(), rb); err != nil {
		t.Fatalf("MDS-not-configured should allow: %v", err)
	}
}

// TestRBValidateUpdate_Immutability_ClusterRef ensures that changing
// spec.clusterRef.name on update is rejected.
func TestRBValidateUpdate_Immutability_ClusterRef(t *testing.T) {
	v := &KafkaRoleBindingValidator{Reader: newRBFakeClient(t)}
	old := rbCR("uid-a", "team-a", "rb-a", "prod", "User:alice", "ClusterAdmin", "kafka")
	upd := rbCR("uid-a", "team-a", "rb-a", "staging", "User:alice", "ClusterAdmin", "kafka")

	_, err := v.ValidateUpdate(context.Background(), old, upd)
	if err == nil {
		t.Fatal("expected rejection for clusterRef.name change")
	}
	if !strings.Contains(err.Error(), "immutable") || !strings.Contains(err.Error(), "clusterRef.name") {
		t.Fatalf("error should say clusterRef.name is immutable: %v", err)
	}
}

// TestRBValidateUpdate_Immutability_Principal ensures that changing spec.principal
// on update is rejected.
func TestRBValidateUpdate_Immutability_Principal(t *testing.T) {
	v := &KafkaRoleBindingValidator{Reader: newRBFakeClient(t)}
	old := rbCR("uid-a", "team-a", "rb-a", "prod", "User:alice", "ClusterAdmin", "kafka")
	upd := rbCR("uid-a", "team-a", "rb-a", "prod", "User:bob", "ClusterAdmin", "kafka")

	_, err := v.ValidateUpdate(context.Background(), old, upd)
	if err == nil {
		t.Fatal("expected rejection for principal change")
	}
	if !strings.Contains(err.Error(), "immutable") || !strings.Contains(err.Error(), "principal") {
		t.Fatalf("error should say principal is immutable: %v", err)
	}
}

// TestRBValidateUpdate_Immutability_Role ensures that changing spec.role on
// update is rejected.
func TestRBValidateUpdate_Immutability_Role(t *testing.T) {
	v := &KafkaRoleBindingValidator{Reader: newRBFakeClient(t)}
	old := rbCR("uid-a", "team-a", "rb-a", "prod", "User:alice", "ClusterAdmin", "kafka")
	upd := rbCR("uid-a", "team-a", "rb-a", "prod", "User:alice", "Operator", "kafka")

	_, err := v.ValidateUpdate(context.Background(), old, upd)
	if err == nil {
		t.Fatal("expected rejection for role change")
	}
	if !strings.Contains(err.Error(), "immutable") || !strings.Contains(err.Error(), "role") {
		t.Fatalf("error should say role is immutable: %v", err)
	}
}

// TestRBValidateUpdate_Immutability_ScopeType ensures that changing
// spec.scope.type on update is rejected.
func TestRBValidateUpdate_Immutability_ScopeType(t *testing.T) {
	v := &KafkaRoleBindingValidator{Reader: newRBFakeClient(t)}
	old := rbCR("uid-a", "team-a", "rb-a", "prod", "User:alice", "ClusterAdmin", "kafka")
	upd := rbCR("uid-a", "team-a", "rb-a", "prod", "User:alice", "ClusterAdmin", "schema-registry")

	_, err := v.ValidateUpdate(context.Background(), old, upd)
	if err == nil {
		t.Fatal("expected rejection for scope.type change")
	}
	if !strings.Contains(err.Error(), "immutable") || !strings.Contains(err.Error(), "scope.type") {
		t.Fatalf("error should say scope.type is immutable: %v", err)
	}
}

// TestRBValidateUpdate_ResourcesChange_Allowed ensures that changing only
// non-identity fields (reconciliation mode) on update is allowed.
func TestRBValidateUpdate_ResourcesChange_Allowed(t *testing.T) {
	cluster := clusterWithMDS("team-a", "prod", "kafka-cluster-1")
	old := rbCR("uid-a", "team-a", "rb-a", "prod", "User:alice", "ClusterAdmin", "kafka")
	cl := newRBFakeClient(t, cluster, old)
	v := &KafkaRoleBindingValidator{Reader: cl}

	// Same principal/role/scope — only reconciliation mode changed (non-identity, non-immutable field).
	upd := rbCR("uid-a", "team-a", "rb-a", "prod", "User:alice", "ClusterAdmin", "kafka")
	upd.Spec.Reconciliation = &v1alpha1.Reconciliation{Mode: "DetectOnly"}
	if _, err := v.ValidateUpdate(context.Background(), old, upd); err != nil {
		t.Fatalf("non-immutable-field update should be allowed: %v", err)
	}
}

// TestRBValidateDelete_AlwaysAllowed ensures that ValidateDelete always returns nil.
func TestRBValidateDelete_AlwaysAllowed(t *testing.T) {
	v := &KafkaRoleBindingValidator{Reader: newRBFakeClient(t)}
	rb := rbCR("uid-a", "team-a", "rb-a", "prod", "User:alice", "ClusterAdmin", "kafka")
	if _, err := v.ValidateDelete(context.Background(), rb); err != nil {
		t.Fatalf("delete must always be allowed: %v", err)
	}
}

// TestRBValidateCreate_SelfUpdate_Allowed ensures that when a CR exists and the
// same namespace+name is re-submitted, it is not rejected as a duplicate of itself.
func TestRBValidateCreate_SelfUpdate_Allowed(t *testing.T) {
	cluster := clusterWithMDS("team-a", "prod", "kafka-cluster-1")
	existing := rbCR("uid-a", "team-a", "rb-alice", "prod", "User:alice", "ClusterAdmin", "kafka")
	cl := newRBFakeClient(t, cluster, existing)
	v := &KafkaRoleBindingValidator{Reader: cl}

	// Same namespace+name as the existing CR → skipped in identity scan.
	self := rbCR("uid-a", "team-a", "rb-alice", "prod", "User:alice", "ClusterAdmin", "kafka")
	if _, err := v.ValidateCreate(context.Background(), self); err != nil {
		t.Fatalf("self-update should be allowed: %v", err)
	}
}

// TestRBValidateCreate_SameIdentityDifferentNamespace_NoClusterNamespace_Allowed
// verifies that two KafkaRoleBindings with the same identity (principal, role,
// scope, resource) but in DIFFERENT namespaces are both allowed when
// ClusterNamespace is unset. Without the override, clusterRef is
// namespace-local: team-a/prod and team-b/prod are distinct cluster objects,
// so no MDS identity collision exists.
func TestRBValidateCreate_SameIdentityDifferentNamespace_NoClusterNamespace_Allowed(t *testing.T) {
	// Seed only the cluster in team-a. The incoming RB is in team-b, so its
	// cluster lookup (team-b/prod) returns NotFound → allow (defer to reconcile).
	cluster := clusterWithMDS("team-a", "prod", "kafka-cluster-1")
	existing := rbCR("uid-a", "team-a", "rb-alice", "prod", "User:alice", "ClusterAdmin", "kafka")
	cl := newRBFakeClient(t, cluster, existing)

	// No ClusterNamespace: clusterRef is namespace-local.
	v := &KafkaRoleBindingValidator{Reader: cl}

	// Incoming RB in a different namespace with the same identity.
	incoming := rbCR("uid-b", "team-b", "rb-alice", "prod", "User:alice", "ClusterAdmin", "kafka")
	if _, err := v.ValidateCreate(context.Background(), incoming); err != nil {
		t.Fatalf("expected allow for same identity in different namespace without cluster-namespace: %v", err)
	}
}

// TestRBValidateCreate_SameIdentityDifferentNamespace_WithClusterNamespace_Denied
// verifies that the same two KafkaRoleBindings (different namespaces, same
// identity) are DENIED when ClusterNamespace is set. The override causes all
// namespaces to share the single KafkaCluster in ClusterNamespace ("kafka"),
// so both CRs resolve to the same effective cluster and their MDS identities
// collide.
func TestRBValidateCreate_SameIdentityDifferentNamespace_WithClusterNamespace_Denied(t *testing.T) {
	// Seed the cluster in the shared ClusterNamespace "kafka".
	cluster := clusterWithMDS("kafka", "prod", "kafka-cluster-1")
	existing := rbCR("uid-a", "team-a", "rb-alice", "prod", "User:alice", "ClusterAdmin", "kafka")
	cl := newRBFakeClient(t, cluster, existing)

	// ClusterNamespace set: all namespaces share kafka/prod.
	v := &KafkaRoleBindingValidator{Reader: cl, ClusterNamespace: "kafka"}

	incoming := rbCR("uid-b", "team-b", "rb-alice", "prod", "User:alice", "ClusterAdmin", "kafka")
	_, err := v.ValidateCreate(context.Background(), incoming)
	if err == nil {
		t.Fatal("expected denial for cluster-wide duplicate with cluster-namespace set")
	}
	if !strings.Contains(err.Error(), "kafka") {
		t.Fatalf("error should name the shared cluster namespace: %v", err)
	}
	if !strings.Contains(err.Error(), "team-a/rb-alice") || !strings.Contains(err.Error(), "team-b/rb-alice") {
		t.Fatalf("error should name both conflicting CRs: %v", err)
	}
}

// TestRBValidateCreate_DeletingDuplicate_StillBlocks verifies that an existing
// KafkaRoleBinding with a non-zero DeletionTimestamp still blocks an incoming
// create with the same MDS identity. The webhook does not exempt terminating
// objects — the identity slot remains occupied until the finalizer releases it.
func TestRBValidateCreate_DeletingDuplicate_StillBlocks(t *testing.T) {
	cluster := clusterWithMDS("team-a", "prod", "kafka-cluster-1")
	deleting := rbCR("uid-a", "team-a", "rb-alice", "prod", "User:alice", "ClusterAdmin", "kafka")
	now := metav1.Now()
	deleting.DeletionTimestamp = &now
	deleting.Finalizers = []string{"gitops.monedula.dev/finalizer"}

	cl := newRBFakeClient(t, cluster, deleting)
	v := &KafkaRoleBindingValidator{Reader: cl}

	dup := rbCR("uid-b", "team-a", "rb-alice-dup", "prod", "User:alice", "ClusterAdmin", "kafka")
	if _, err := v.ValidateCreate(context.Background(), dup); err == nil {
		t.Fatal("expected denial: a deleting KafkaRoleBinding still occupies the MDS identity")
	}
}
