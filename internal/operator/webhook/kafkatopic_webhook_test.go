package webhook

import (
	"context"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
)

// newScheme returns a scheme with the v1alpha1 types registered.
func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("adding v1alpha1 scheme: %v", err)
	}
	return s
}

// newFakeClient builds a fake client seeded with objs and with the
// ClusterRefNameIndex field index registered (mirroring RegisterIndexes) so the
// validator's MatchingFields List works hermetically.
func newFakeClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	return fake.NewClientBuilder().
		WithScheme(newScheme(t)).
		WithIndex(&v1alpha1.KafkaTopic{}, ClusterRefNameIndex, func(obj client.Object) []string {
			tp, ok := obj.(*v1alpha1.KafkaTopic)
			if !ok {
				return nil
			}
			return []string{tp.Spec.ClusterRef.Name}
		}).
		WithObjects(objs...).
		Build()
}

// topic builds a KafkaTopic with a distinct UID (so it is not mistaken for a
// self-update during identity scans). topicName "" exercises the metadata-name
// default.
func topic(uid, ns, name, clusterRef, topicName string) *v1alpha1.KafkaTopic {
	return &v1alpha1.KafkaTopic{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			UID:       types.UID(uid),
		},
		Spec: v1alpha1.KafkaTopicSpec{
			ClusterRef: v1alpha1.ClusterRef{Name: clusterRef},
			TopicName:  topicName,
			Partitions: 1,
		},
	}
}

func cluster(ns, name string, tenancy *v1alpha1.TenancyConfig) *v1alpha1.KafkaCluster {
	return &v1alpha1.KafkaCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec:       v1alpha1.KafkaClusterSpec{BootstrapServers: "localhost:9092", Tenancy: tenancy},
	}
}

func TestValidateCreate_DuplicateSameNamespace_Denied(t *testing.T) {
	existing := topic("uid-a", "team-a", "orders", "prod", "orders.events")
	v := &KafkaTopicValidator{Reader: newFakeClient(t, existing)}

	// A different CR in the same namespace resolving to the same identity.
	dup := topic("uid-b", "team-a", "orders-dup", "prod", "orders.events")
	_, err := v.ValidateCreate(context.Background(), dup)
	if err == nil {
		t.Fatal("expected denial for duplicate identity in same namespace")
	}
	if !strings.Contains(err.Error(), "orders.events") ||
		!strings.Contains(err.Error(), "team-a/orders") ||
		!strings.Contains(err.Error(), "team-a/orders-dup") {
		t.Fatalf("error should name both CRs and the contested identity: %v", err)
	}
}

func TestValidateCreate_SameIdentityDifferentNamespace_NoClusterNamespace_Allowed(t *testing.T) {
	existing := topic("uid-a", "team-a", "orders", "prod", "orders.events")
	// No ClusterNamespace: clusterRef "prod" is namespace-local, so team-b/prod
	// is a DIFFERENT cluster object than team-a/prod -> no collision.
	v := &KafkaTopicValidator{Reader: newFakeClient(t, existing)}

	other := topic("uid-b", "team-b", "orders", "prod", "orders.events")
	if _, err := v.ValidateCreate(context.Background(), other); err != nil {
		t.Fatalf("expected allow for same identity in different namespace without cluster-namespace: %v", err)
	}
}

func TestValidateCreate_SameIdentityDifferentNamespace_WithClusterNamespace_Denied(t *testing.T) {
	existing := topic("uid-a", "team-a", "orders", "prod", "orders.events")
	// ClusterNamespace set: all namespaces share the one cluster object, so the
	// collision is cluster-wide across namespaces.
	v := &KafkaTopicValidator{Reader: newFakeClient(t, existing), ClusterNamespace: "kafka"}

	other := topic("uid-b", "team-b", "orders", "prod", "orders.events")
	_, err := v.ValidateCreate(context.Background(), other)
	if err == nil {
		t.Fatal("expected denial for cluster-wide duplicate with cluster-namespace set")
	}
	if !strings.Contains(err.Error(), "kafka") {
		t.Fatalf("error should name the shared cluster namespace: %v", err)
	}
}

func TestValidateUpdate_TopicNameChange_Denied(t *testing.T) {
	v := &KafkaTopicValidator{Reader: newFakeClient(t)}
	old := topic("uid-a", "team-a", "orders", "prod", "orders.events")
	updated := topic("uid-a", "team-a", "orders", "prod", "orders.renamed")

	_, err := v.ValidateUpdate(context.Background(), old, updated)
	if err == nil {
		t.Fatal("expected denial for topicName change")
	}
	if !strings.Contains(err.Error(), "orders.events") || !strings.Contains(err.Error(), "orders.renamed") {
		t.Fatalf("error should show old and new names: %v", err)
	}
}

func TestValidateUpdate_ClusterRefChange_Denied(t *testing.T) {
	// Repointing the clusterRef orphans state on the previous cluster; the
	// webhook now mirrors the always-on CEL rule on KafkaTopicSpec.
	v := &KafkaTopicValidator{Reader: newFakeClient(t)}
	old := topic("uid-a", "team-a", "orders", "prod", "orders.events")
	updated := topic("uid-a", "team-a", "orders", "staging", "orders.events")

	_, err := v.ValidateUpdate(context.Background(), old, updated)
	if err == nil {
		t.Fatal("expected denial for clusterRef change")
	}
	if !strings.Contains(err.Error(), "spec.clusterRef.name is immutable") ||
		!strings.Contains(err.Error(), "prod") || !strings.Contains(err.Error(), "staging") {
		t.Fatalf("error should name the field and both cluster refs: %v", err)
	}
}

func TestValidateUpdate_ResolvedNameUnchanged_EmptyToExplicitSame_Allowed(t *testing.T) {
	v := &KafkaTopicValidator{Reader: newFakeClient(t)}
	// Old leaves topicName empty (resolves to metadata.name "orders").
	old := topic("uid-a", "team-a", "orders", "prod", "")
	// New sets it explicitly to the same value as metadata.name.
	updated := topic("uid-a", "team-a", "orders", "prod", "orders")

	if _, err := v.ValidateUpdate(context.Background(), old, updated); err != nil {
		t.Fatalf("expected allow: empty->explicit-same is not a rename: %v", err)
	}
}

func TestValidateUpdate_SelfUpdate_Allowed(t *testing.T) {
	// The object being updated is already in the store with the same UID; the
	// identity scan must skip it.
	existing := topic("uid-a", "team-a", "orders", "prod", "orders.events")
	v := &KafkaTopicValidator{Reader: newFakeClient(t, existing)}

	updated := topic("uid-a", "team-a", "orders", "prod", "orders.events")
	updated.Spec.Partitions = 6 // an unrelated mutation
	if _, err := v.ValidateUpdate(context.Background(), existing, updated); err != nil {
		t.Fatalf("expected allow for self-update: %v", err)
	}
}

func TestValidateCreate_TenancyDeny_Denied(t *testing.T) {
	tn := &v1alpha1.TenancyConfig{
		TopicPrefixes: []v1alpha1.TopicPrefixRule{{
			Namespaces: []string{"team-a"},
			Prefixes:   []string{"team-a."},
		}},
	}
	cl := cluster("team-a", "prod", tn)
	v := &KafkaTopicValidator{Reader: newFakeClient(t, cl)}

	// topicName "orders.events" does not start with the allowed "team-a." prefix.
	bad := topic("uid-b", "team-a", "orders", "prod", "orders.events")
	_, err := v.ValidateCreate(context.Background(), bad)
	if err == nil {
		t.Fatal("expected tenancy denial")
	}
	if !strings.Contains(err.Error(), "tenancy") {
		t.Fatalf("expected a tenancy error: %v", err)
	}
}

func TestValidateCreate_TenancyCompliant_Allowed(t *testing.T) {
	tn := &v1alpha1.TenancyConfig{
		TopicPrefixes: []v1alpha1.TopicPrefixRule{{
			Namespaces: []string{"team-a"},
			Prefixes:   []string{"team-a."},
		}},
	}
	cl := cluster("team-a", "prod", tn)
	v := &KafkaTopicValidator{Reader: newFakeClient(t, cl)}

	good := topic("uid-b", "team-a", "orders", "prod", "team-a.orders")
	if _, err := v.ValidateCreate(context.Background(), good); err != nil {
		t.Fatalf("expected allow for tenancy-compliant topic: %v", err)
	}
}

func TestValidateUpdate_TenancyViolation_Denied(t *testing.T) {
	// A normal (non-deleting) update that violates tenancy must still be
	// rejected: only deletion in progress exempts the tenancy check.
	tn := &v1alpha1.TenancyConfig{
		TopicPrefixes: []v1alpha1.TopicPrefixRule{{
			Namespaces: []string{"team-a"},
			Prefixes:   []string{"team-a."},
		}},
	}
	cl := cluster("team-a", "prod", tn)
	v := &KafkaTopicValidator{Reader: newFakeClient(t, cl)}

	old := topic("uid-a", "team-a", "orders", "prod", "orders.events")
	updated := topic("uid-a", "team-a", "orders", "prod", "orders.events")
	updated.Spec.Partitions = 6 // an unrelated mutation; tenancy still violated

	_, err := v.ValidateUpdate(context.Background(), old, updated)
	if err == nil {
		t.Fatal("expected tenancy denial on non-deleting update")
	}
	if !strings.Contains(err.Error(), "tenancy") {
		t.Fatalf("expected a tenancy error: %v", err)
	}
}

func TestValidateUpdate_DeletionInProgress_TenancyViolation_Allowed(t *testing.T) {
	// Tenancy was tightened AFTER the topic was created (namespace no longer
	// satisfies the prefix rule). An update on an object being deleted — e.g.
	// the controller's finalizer-removal Update, or a user's allow-delete
	// annotation patch — must NOT be blocked by tenancy, or the topic could
	// never be deleted.
	tn := &v1alpha1.TenancyConfig{
		TopicPrefixes: []v1alpha1.TopicPrefixRule{{
			Namespaces: []string{"team-a"},
			Prefixes:   []string{"team-a."},
		}},
	}
	cl := cluster("team-a", "prod", tn)
	v := &KafkaTopicValidator{Reader: newFakeClient(t, cl)}

	old := topic("uid-a", "team-a", "orders", "prod", "orders.events")
	now := metav1.Now()
	updated := topic("uid-a", "team-a", "orders", "prod", "orders.events")
	updated.DeletionTimestamp = &now
	updated.Finalizers = []string{"gitops.monedula.dev/finalizer"} // e.g. removed by the next update

	if _, err := v.ValidateUpdate(context.Background(), old, updated); err != nil {
		t.Fatalf("expected allow: deletion must not be wedged by tenancy: %v", err)
	}
}

func TestValidateUpdate_DeletionInProgress_ImmutabilityStillEnforced(t *testing.T) {
	// Deletion exempts tenancy, but immutability checks stay active: they are
	// cheap and cannot wedge a deletion (finalizer removal / delete-annotation
	// updates don't touch these fields), so there is no reason to relax them.
	v := &KafkaTopicValidator{Reader: newFakeClient(t)}

	old := topic("uid-a", "team-a", "orders", "prod", "orders.events")
	now := metav1.Now()
	updated := topic("uid-a", "team-a", "orders", "staging", "orders.events") // clusterRef changed
	updated.DeletionTimestamp = &now
	updated.Finalizers = []string{"gitops.monedula.dev/finalizer"}

	_, err := v.ValidateUpdate(context.Background(), old, updated)
	if err == nil {
		t.Fatal("expected denial: clusterRef immutability must still be enforced during deletion")
	}
	if !strings.Contains(err.Error(), "spec.clusterRef.name is immutable") {
		t.Fatalf("expected an immutability error: %v", err)
	}
}

func TestValidateCreate_ClusterMissing_Allowed(t *testing.T) {
	// No cluster object present: admission must not block on a lagging cluster.
	v := &KafkaTopicValidator{Reader: newFakeClient(t)}
	tp := topic("uid-b", "team-a", "orders", "prod", "orders.events")
	if _, err := v.ValidateCreate(context.Background(), tp); err != nil {
		t.Fatalf("expected allow when cluster is missing: %v", err)
	}
}

func TestValidateCreate_DeletingDuplicate_StillBlocks(t *testing.T) {
	// An existing CR with the same identity that is being deleted still occupies
	// the identity (its finalizer may still be cleaning up).
	deleting := topic("uid-a", "team-a", "orders", "prod", "orders.events")
	now := metav1.Now()
	deleting.DeletionTimestamp = &now
	deleting.Finalizers = []string{"gitops.monedula.dev/finalizer"} // required for fake client to accept a deletionTimestamp
	v := &KafkaTopicValidator{Reader: newFakeClient(t, deleting)}

	dup := topic("uid-b", "team-a", "orders-dup", "prod", "orders.events")
	if _, err := v.ValidateCreate(context.Background(), dup); err == nil {
		t.Fatal("expected denial: a deleting CR still occupies the identity")
	}
}

func TestValidateDelete_AlwaysAllowed(t *testing.T) {
	v := &KafkaTopicValidator{Reader: newFakeClient(t)}
	tp := topic("uid-a", "team-a", "orders", "prod", "orders.events")
	if _, err := v.ValidateDelete(context.Background(), tp); err != nil {
		t.Fatalf("delete must always be allowed: %v", err)
	}
}
