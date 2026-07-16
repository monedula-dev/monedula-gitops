package controller

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	kafkamock "github.com/monedula-dev/monedula-gitops/internal/kafka/mock"
)

// newViewFakeClient builds a fake client with status subresources enabled for
// ALL managed types — the aclview tests reconcile topics and policies against
// the same client.
func newViewFakeClient(t *testing.T, s *runtime.Scheme, objs ...client.Object) client.Client {
	t.Helper()
	return fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(objs...).
		WithStatusSubresource(&v1alpha1.KafkaTopic{}, &v1alpha1.KafkaAccessPolicy{}, &v1alpha1.KafkaCluster{}).
		Build()
}

// overlapTopic is topicObj plus access for User:svc-payments (Write) on
// payments.orders — the same principal + resource pattern the policyObj rules
// target with DIFFERENT operations (Read, Describe). Spec §10.4 overlap.
func overlapTopic(prune bool) *v1alpha1.KafkaTopic {
	tp := topicObj("Enforce")
	tp.Spec.Access = v1alpha1.TopicAccess{Producers: []v1alpha1.ProducerAccess{
		{Principal: "User:svc-payments", Operations: []string{"Write"}},
	}}
	tp.Spec.Prune = prune
	return tp
}

// overlapLiveACLs is the converged live state: the topic's Write ACL plus the
// policy's Read+Describe ACLs, all on the shared (principal, pattern) pair.
func overlapLiveACLs(extra ...kafka.ACLState) []kafka.ACLState {
	live := []kafka.ACLState{
		{Principal: "User:svc-payments", Host: "*", ResourceType: "topic", ResourceName: "payments.orders", PatternType: "literal", Operation: "Write", Permission: "Allow"},
	}
	live = append(live, seededPolicyACLsState()...)
	return append(live, extra...)
}

// seededPolicyACLsState is seededPolicyACLs without the *testing.T plumbing.
func seededPolicyACLsState() []kafka.ACLState {
	return []kafka.ACLState{
		{Principal: "User:svc-payments", Host: "*", ResourceType: "topic", ResourceName: "payments.orders", PatternType: "literal", Operation: "Read", Permission: "Allow"},
		{Principal: "User:svc-payments", Host: "*", ResourceType: "topic", ResourceName: "payments.orders", PatternType: "literal", Operation: "Describe", Permission: "Allow"},
	}
}

// liveOpsFor returns the operations the mock currently stores for the shared
// (User:svc-payments, topic payments.orders) pair.
func liveOpsFor(t *testing.T, k *kafkamock.Client) map[string]bool {
	t.Helper()
	acls, err := k.ListACLs(context.Background())
	if err != nil {
		t.Fatalf("ListACLs: %v", err)
	}
	out := map[string]bool{}
	for _, a := range acls {
		if a.Principal == "User:svc-payments" && a.ResourceType == "topic" && a.ResourceName == "payments.orders" {
			out[a.Operation] = true
		}
	}
	return out
}

// TestTopicReconcile_AggregatesPolicyACLsBeforePruning is the controller-level
// §10.4 fix: a KafkaTopic and a KafkaAccessPolicy on the same cluster overlap
// on (principal, pattern) with different operations; reconciling the TOPIC with
// prune enabled must NOT delete the policy's live ACLs.
func TestTopicReconcile_AggregatesPolicyACLsBeforePruning(t *testing.T) {
	s := topicScheme(t)
	tp := overlapTopic(true)
	pol := policyObj("Enforce")
	pol.Spec.Prune = true
	c := topicCluster()
	cl := newViewFakeClient(t, s, tp, pol, c)
	k := kafkamock.New(seededTopics("payments.orders"), overlapLiveACLs())
	r := &KafkaTopicReconciler{Client: cl, Scheme: s, Clients: stubFactory{k: k}}

	if _, err := r.Reconcile(context.Background(), topicReq()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got := liveOpsFor(t, k)
	if !got["Read"] || !got["Describe"] {
		t.Fatalf("the policy's ACLs were pruned by the topic reconcile (§10.4 flap), live ops = %v (calls %v)", got, k.Calls())
	}
	if !got["Write"] {
		t.Fatalf("the topic's own Write ACL went missing, live ops = %v", got)
	}
}

// TestTopicReconcile_AggregatedPruneStillDeletesUndesired: a live ACL desired
// by NEITHER resource is still pruned when both covering resources consent.
func TestTopicReconcile_AggregatedPruneStillDeletesUndesired(t *testing.T) {
	s := topicScheme(t)
	tp := overlapTopic(true)
	pol := policyObj("Enforce")
	pol.Spec.Prune = true
	c := topicCluster()
	cl := newViewFakeClient(t, s, tp, pol, c)
	k := kafkamock.New(seededTopics("payments.orders"), overlapLiveACLs(kafka.ACLState{
		Principal: "User:svc-payments", Host: "*", ResourceType: "topic", ResourceName: "payments.orders",
		PatternType: "literal", Operation: "Alter", Permission: "Allow",
	}))
	r := &KafkaTopicReconciler{Client: cl, Scheme: s, Clients: stubFactory{k: k}}

	if _, err := r.Reconcile(context.Background(), topicReq()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got := liveOpsFor(t, k)
	if got["Alter"] {
		t.Fatalf("genuinely undesired Alter ACL survived an all-consenting prune, live ops = %v (calls %v)", got, k.Calls())
	}
	if !got["Read"] || !got["Describe"] || !got["Write"] {
		t.Fatalf("a desired ACL was pruned alongside the undesired one, live ops = %v", got)
	}
}

// TestTopicReconcile_AggregatedPruneVetoedByPolicy: prune consent is AND-merged
// across ALL aggregated owners (spec §10.3) — the policy's prune=false vetoes
// even a genuinely undesired candidate when the topic reconciles with
// prune=true.
func TestTopicReconcile_AggregatedPruneVetoedByPolicy(t *testing.T) {
	s := topicScheme(t)
	tp := overlapTopic(true)
	pol := policyObj("Enforce") // prune defaults to false: the veto
	c := topicCluster()
	cl := newViewFakeClient(t, s, tp, pol, c)
	k := kafkamock.New(seededTopics("payments.orders"), overlapLiveACLs(kafka.ACLState{
		Principal: "User:svc-payments", Host: "*", ResourceType: "topic", ResourceName: "payments.orders",
		PatternType: "literal", Operation: "Alter", Permission: "Allow",
	}))
	r := &KafkaTopicReconciler{Client: cl, Scheme: s, Clients: stubFactory{k: k}}

	if _, err := r.Reconcile(context.Background(), topicReq()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if callsContain(k.Calls(), "DeleteACLs") {
		t.Fatalf("policy vetoed pruning (prune=false) but DeleteACLs was called: %v", k.Calls())
	}
}

// TestTopicReconcile_AggregationIsNamespaceLocal: with no --cluster-namespace
// override, clusterRef is namespace-local, so a policy in ANOTHER namespace
// referencing a same-named cluster belongs to a different cluster and must NOT
// be aggregated — its prune=false cannot veto this cluster's prune.
func TestTopicReconcile_AggregationIsNamespaceLocal(t *testing.T) {
	s := topicScheme(t)
	tp := overlapTopic(true)
	otherNS := policyObj("Enforce") // prune=false; would veto if wrongly included
	otherNS.Namespace = "ns2"
	c := topicCluster()
	cl := newViewFakeClient(t, s, tp, otherNS, c)
	k := kafkamock.New(seededTopics("payments.orders"), []kafka.ACLState{
		{Principal: "User:svc-payments", Host: "*", ResourceType: "topic", ResourceName: "payments.orders", PatternType: "literal", Operation: "Write", Permission: "Allow"},
		{Principal: "User:svc-payments", Host: "*", ResourceType: "topic", ResourceName: "payments.orders", PatternType: "literal", Operation: "Alter", Permission: "Allow"},
	})
	r := &KafkaTopicReconciler{Client: cl, Scheme: s, Clients: stubFactory{k: k}}

	if _, err := r.Reconcile(context.Background(), topicReq()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got := liveOpsFor(t, k)
	if got["Alter"] {
		t.Fatalf("the ns2 policy must not be aggregated into ns1's cluster view; its veto blocked the prune: live ops = %v (calls %v)", got, k.Calls())
	}
}

// TestTopicReconcile_AggregationSpansNamespacesWithOverride: with a
// --cluster-namespace override every clusterRef resolves to the same namespace,
// so resources in DIFFERENT namespaces referencing the same cluster name share
// one cluster — the ns2 policy joins the view and its ACLs are protected.
func TestTopicReconcile_AggregationSpansNamespacesWithOverride(t *testing.T) {
	s := topicScheme(t)
	tp := overlapTopic(true)
	otherNS := policyObj("Enforce")
	otherNS.Namespace = "ns2"
	otherNS.Spec.Prune = true
	c := topicCluster()
	cl := newViewFakeClient(t, s, tp, otherNS, c)
	k := kafkamock.New(seededTopics("payments.orders"), overlapLiveACLs())
	r := &KafkaTopicReconciler{Client: cl, Scheme: s, Clients: stubFactory{k: k}, ClusterNamespace: "ns1"}

	if _, err := r.Reconcile(context.Background(), topicReq()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got := liveOpsFor(t, k)
	if !got["Read"] || !got["Describe"] {
		t.Fatalf("the ns2 policy's ACLs must be protected under the namespace override, live ops = %v (calls %v)", got, k.Calls())
	}
}

// TestPolicyReconcile_AggregatesTopicACLsBeforePruning mirrors the fix from the
// policy controller's side: reconciling the POLICY with prune enabled must not
// delete the topic's live Write ACL.
func TestPolicyReconcile_AggregatesTopicACLsBeforePruning(t *testing.T) {
	s := topicScheme(t)
	tp := overlapTopic(true)
	pol := policyObj("Enforce")
	pol.Spec.Prune = true
	c := policyCluster()
	cl := newViewFakeClient(t, s, tp, pol, c)
	k := kafkamock.New(seededTopics("payments.orders"), overlapLiveACLs())
	r := &KafkaAccessPolicyReconciler{Client: cl, Scheme: s, Clients: stubFactory{k: k}}

	if _, err := r.Reconcile(context.Background(), policyReq()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got := liveOpsFor(t, k)
	if !got["Write"] {
		t.Fatalf("the topic's Write ACL was pruned by the policy reconcile (§10.4 flap), live ops = %v (calls %v)", got, k.Calls())
	}
	if !got["Read"] || !got["Describe"] {
		t.Fatalf("the policy's own ACLs went missing, live ops = %v", got)
	}
}

// TestTopicReconcile_AggregationSkipsDeletedResources: a being-deleted policy
// (non-zero DeletionTimestamp) is excluded from the view — its ACLs are the
// finalizer's responsibility, so the topic's consenting prune deletes them.
func TestTopicReconcile_AggregationSkipsDeletedResources(t *testing.T) {
	s := topicScheme(t)
	tp := overlapTopic(true)
	pol := policyObj("Enforce")
	pol.Spec.Prune = true
	pol.Finalizers = []string{FinalizerName}
	c := topicCluster()
	cl := newViewFakeClient(t, s, tp, pol, c)
	// Delete the policy so it carries a DeletionTimestamp but stays listable
	// (the finalizer keeps it around).
	if err := cl.Delete(context.Background(), pol); err != nil {
		t.Fatalf("delete policy: %v", err)
	}
	k := kafkamock.New(seededTopics("payments.orders"), overlapLiveACLs())
	r := &KafkaTopicReconciler{Client: cl, Scheme: s, Clients: stubFactory{k: k}}

	if _, err := r.Reconcile(context.Background(), topicReq()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got := liveOpsFor(t, k)
	if got["Read"] || got["Describe"] {
		t.Fatalf("a being-deleted policy must not protect its ACLs via the view, live ops = %v (calls %v)", got, k.Calls())
	}
}
