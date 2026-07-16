//go:build envtest

package controller

// Envtest coverage for the finalizer-deletion co-ownership shield: deleting a
// CR with deletionPolicy: Delete must not remove ACL / MDS role-binding tuples
// another LIVE CR still desires. Driven end to end through the real apiserver
// (finalizer + deletionTimestamp semantics) with the in-memory Kafka/MDS mocks.

import (
	"context"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	kafkamock "github.com/monedula-dev/monedula-gitops/internal/kafka/mock"
	"github.com/monedula-dev/monedula-gitops/internal/mds"
	mdsmock "github.com/monedula-dev/monedula-gitops/internal/mds/mock"
	schemamock "github.com/monedula-dev/monedula-gitops/internal/schemaregistry/mock"
)

// liveACLOps returns the operations currently live in the mock for the given
// (principal, resourceType, resourceName) tuple.
func liveACLOps(t *testing.T, mk *kafkamock.Client, principal, resType, resName string) map[string]bool {
	t.Helper()
	acls, err := mk.ListACLs(context.Background())
	if err != nil {
		t.Fatalf("ListACLs: %v", err)
	}
	out := map[string]bool{}
	for _, a := range acls {
		if a.Principal == principal && a.ResourceType == resType && a.ResourceName == resName {
			out[a.Operation] = true
		}
	}
	return out
}

// TestEnvtestTopicDeleteSharedACLCoOwnership: topic A (Delete + allow-delete)
// and policy B both desire (User:svc-shared, topic coown.orders, Read/Describe).
// Deleting A removes the broker topic and A's own group ACL but RETAINS the
// shared tuples (B protects them). Deleting B afterwards removes them.
func TestEnvtestTopicDeleteSharedACLCoOwnership(t *testing.T) {
	env := startEnv(t)
	defer env.stop()
	ctx := context.Background()

	if err := env.cl.Create(ctx, newCluster(testNamespace, "coown-cluster")); err != nil {
		t.Fatalf("create cluster: %v", err)
	}

	topic := &v1alpha1.KafkaTopic{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "coown-topic",
			Namespace:   testNamespace,
			Annotations: map[string]string{"gitops.monedula.dev/allow-delete": "true"},
		},
		Spec: v1alpha1.KafkaTopicSpec{
			ClusterRef:     v1alpha1.ClusterRef{Name: "coown-cluster"},
			TopicName:      "coown.orders",
			Partitions:     3,
			DeletionPolicy: "Delete",
			Access: v1alpha1.TopicAccess{Consumers: []v1alpha1.ConsumerAccess{
				// Default ops: topic Read+Describe (shared with the policy) and
				// group Read (the topic's own, non-shared tuple).
				{Principal: "User:svc-shared", Group: "coown-g1"},
			}},
		},
	}
	if err := env.cl.Create(ctx, topic); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	pol := &v1alpha1.KafkaAccessPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "coown-policy",
			Namespace:   testNamespace,
			Annotations: map[string]string{"gitops.monedula.dev/allow-delete": "true"},
		},
		Spec: v1alpha1.KafkaAccessPolicySpec{
			ClusterRef: v1alpha1.ClusterRef{Name: "coown-cluster"},
			Rules: []v1alpha1.ACLRule{{
				Principal:  "User:svc-shared",
				Operations: []string{"Read", "Describe"},
				Resource:   v1alpha1.ACLResource{Type: "topic", Name: "coown.orders"},
			}},
		},
	}
	if err := env.cl.Create(ctx, pol); err != nil {
		t.Fatalf("create policy: %v", err)
	}

	mk := kafkamock.New(nil, nil)
	tr := &KafkaTopicReconciler{Client: env.cl, Scheme: env.scheme, Clients: stubFactory{k: mk, sr: schemamock.New()}}
	pr := &KafkaAccessPolicyReconciler{Client: env.cl, Scheme: env.scheme, Clients: stubFactory{k: mk, sr: schemamock.New()}}

	// Converge both resources: topic + all three ACL tuples exist.
	reconcileFor(t, tr, testNamespace, "coown-topic")
	reconcileFor(t, pr, testNamespace, "coown-policy")
	if ops := liveACLOps(t, mk, "User:svc-shared", "topic", "coown.orders"); !ops["Read"] || !ops["Describe"] {
		t.Fatalf("setup: shared tuples not live, ops = %v (calls %v)", ops, mk.Calls())
	}

	// Delete topic A and finalize it.
	var tp v1alpha1.KafkaTopic
	if err := env.cl.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: "coown-topic"}, &tp); err != nil {
		t.Fatalf("re-get topic: %v", err)
	}
	if err := env.cl.Delete(ctx, &tp); err != nil {
		t.Fatalf("delete topic: %v", err)
	}
	reconcileFor(t, tr, testNamespace, "coown-topic")

	// Broker topic gone (not co-owned), shared tuples retained, group tuple gone.
	if !containsCall(mk.Calls(), "DeleteTopic coown.orders") {
		t.Fatalf("kafka calls = %v, want DeleteTopic coown.orders", mk.Calls())
	}
	if ops := liveACLOps(t, mk, "User:svc-shared", "topic", "coown.orders"); !ops["Read"] || !ops["Describe"] {
		t.Fatalf("shared tuples were deleted despite the live policy co-owner, ops = %v (calls %v)", ops, mk.Calls())
	}
	if ops := liveACLOps(t, mk, "User:svc-shared", "group", "coown-g1"); len(ops) != 0 {
		t.Fatalf("the topic's own group tuple must be deleted, ops = %v", ops)
	}
	var goneTopic v1alpha1.KafkaTopic
	if err := env.cl.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: "coown-topic"}, &goneTopic); !apierrors.IsNotFound(err) {
		t.Fatalf("topic CR still present after finalization; get err = %v", err)
	}

	// Delete policy B: now the last owner, so the shared tuples go too.
	var p v1alpha1.KafkaAccessPolicy
	if err := env.cl.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: "coown-policy"}, &p); err != nil {
		t.Fatalf("re-get policy: %v", err)
	}
	if err := env.cl.Delete(ctx, &p); err != nil {
		t.Fatalf("delete policy: %v", err)
	}
	reconcileFor(t, pr, testNamespace, "coown-policy")

	if ops := liveACLOps(t, mk, "User:svc-shared", "topic", "coown.orders"); len(ops) != 0 {
		t.Fatalf("last owner deleted; shared tuples must be gone, ops = %v (calls %v)", ops, mk.Calls())
	}
	var gonePol v1alpha1.KafkaAccessPolicy
	if err := env.cl.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: "coown-policy"}, &gonePol); !apierrors.IsNotFound(err) {
		t.Fatalf("policy CR still present after finalization; get err = %v", err)
	}
}

// TestEnvtestRoleBindingDeleteSharedBindingCoOwnership: an explicit
// KafkaRoleBinding (Delete) shares one binding tuple with a live KafkaTopic's
// access block (topic-derived DeveloperWrite on an rbac cluster) — identity
// uniqueness forbids two explicit CRs sharing a tuple, so the real overlap is
// explicit + topic-derived, exactly as BuildDesiredSet's cross-source dedupe
// allows. Finalizing the CR must remove only its non-shared binding and retain
// the co-owned one.
func TestEnvtestRoleBindingDeleteSharedBindingCoOwnership(t *testing.T) {
	env := startEnv(t)
	defer env.stop()
	ctx := context.Background()

	cluster := &v1alpha1.KafkaCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "coown-dual-cluster", Namespace: testNamespace},
		Spec: v1alpha1.KafkaClusterSpec{
			BootstrapServers: "localhost:9092",
			Authorization: &v1alpha1.AuthorizationConfig{
				AccessBackends: []string{"acl", "rbac"},
				MDS: &v1alpha1.MDSConfig{
					Endpoint: "http://mds:8090",
					Clusters: v1alpha1.MDSClusters{KafkaCluster: "lkc-coown"},
				},
			},
		},
	}
	if err := env.cl.Create(ctx, cluster); err != nil {
		t.Fatalf("create cluster: %v", err)
	}

	// Live topic whose producer derives DeveloperWrite on Topic coown.dual.orders.
	topic := &v1alpha1.KafkaTopic{
		ObjectMeta: metav1.ObjectMeta{Name: "coown-dual-topic", Namespace: testNamespace},
		Spec: v1alpha1.KafkaTopicSpec{
			ClusterRef: v1alpha1.ClusterRef{Name: "coown-dual-cluster"},
			TopicName:  "coown.dual.orders",
			Partitions: 3,
			Access: v1alpha1.TopicAccess{Producers: []v1alpha1.ProducerAccess{
				{Principal: "User:svc-checkout"},
			}},
		},
	}
	if err := env.cl.Create(ctx, topic); err != nil {
		t.Fatalf("create topic: %v", err)
	}

	// Explicit role binding: the SAME derived tuple plus one of its own.
	rb := &v1alpha1.KafkaRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "coown-rb", Namespace: testNamespace},
		Spec: v1alpha1.KafkaRoleBindingSpec{
			ClusterRef:     v1alpha1.ClusterRef{Name: "coown-dual-cluster"},
			Principal:      "User:svc-checkout",
			Role:           "DeveloperWrite",
			Scope:          v1alpha1.RoleBindingScope{Type: "kafka"},
			DeletionPolicy: "Delete",
			Resources: []v1alpha1.RoleResource{
				{Type: "Topic", Name: "coown.dual.orders", PatternType: "literal"}, // shared
				{Type: "Topic", Name: "coown.other", PatternType: "literal"},       // own
			},
		},
	}
	if err := env.cl.Create(ctx, rb); err != nil {
		t.Fatalf("create role binding: %v", err)
	}

	scope := mds.Scope{Type: "kafka", KafkaCluster: "lkc-coown"}
	sharedBinding := mds.RoleBinding{
		Principal: "User:svc-checkout", Role: "DeveloperWrite", Scope: scope,
		Resource: &mds.ResourcePattern{Type: "Topic", Name: "coown.dual.orders", PatternType: "literal"},
	}
	ownBinding := mds.RoleBinding{
		Principal: "User:svc-checkout", Role: "DeveloperWrite", Scope: scope,
		Resource: &mds.ResourcePattern{Type: "Topic", Name: "coown.other", PatternType: "literal"},
	}
	mock := mdsmock.New(sharedBinding, ownBinding) // both live in MDS
	r := roleBindingReconciler(env, mock)

	// First reconcile: adds the finalizer (both bindings already live: no adds).
	reconcileFor(t, r, testNamespace, "coown-rb")

	// Delete the CR and finalize.
	var afterCreate v1alpha1.KafkaRoleBinding
	if err := env.cl.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: "coown-rb"}, &afterCreate); err != nil {
		t.Fatalf("re-get role binding: %v", err)
	}
	if err := env.cl.Delete(ctx, &afterCreate); err != nil {
		t.Fatalf("delete role binding: %v", err)
	}
	reconcileFor(t, r, testNamespace, "coown-rb")

	// Own binding removed; shared binding retained (the live topic co-owns it).
	if !containsCall(mock.Calls(), "RemoveRoleBinding "+ownBinding.Key()) {
		t.Fatalf("the CR's own binding must be removed; calls = %v", mock.Calls())
	}
	if containsCall(mock.Calls(), "RemoveRoleBinding "+sharedBinding.Key()) {
		t.Fatalf("the shared binding co-owned by the live topic was removed; calls = %v", mock.Calls())
	}
	live, err := mock.ListRoleBindings(ctx, scope)
	if err != nil {
		t.Fatalf("listing MDS bindings: %v", err)
	}
	foundShared := false
	for _, b := range live {
		if b.Key() == sharedBinding.Key() {
			foundShared = true
		}
		if b.Key() == ownBinding.Key() {
			t.Fatalf("own binding still live after deletion; live = %v", live)
		}
	}
	if !foundShared {
		t.Fatalf("shared binding missing from MDS after deletion; live = %v", live)
	}

	// CR must be gone.
	var gone v1alpha1.KafkaRoleBinding
	if err := env.cl.Get(ctx, types.NamespacedName{Namespace: testNamespace, Name: "coown-rb"}, &gone); !apierrors.IsNotFound(err) {
		t.Fatalf("role binding still present after finalizer removal; get err = %v", err)
	}
}
