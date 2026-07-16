package reconcile

import (
	"context"
	"testing"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	kafkamock "github.com/monedula-dev/monedula-gitops/internal/kafka/mock"
)

// overlapFixture models the spec §10.4 flapping scenario: a KafkaTopic and a
// KafkaAccessPolicy share the (User:svc, topic payments.orders literal) scope
// key but desire DIFFERENT operations. The mock live state is seeded with BOTH
// resources' ACLs, so reconciling either one with only its own per-CR scope
// sees the other's ACLs as in-scope-but-undesired prune candidates.
//
//	topic  -> User:svc Write payments.orders (prune per topicPrune)
//	policy -> User:svc Read  payments.orders (prune per policyPrune)
func overlapFixture(topicPrune, policyPrune bool, extraLive ...kafka.ACLState) (*v1alpha1.KafkaTopic, *v1alpha1.KafkaAccessPolicy, *kafkamock.Client) {
	tp := baseTopic("Enforce")
	tp.Spec.Access = v1alpha1.TopicAccess{Producers: []v1alpha1.ProducerAccess{
		{Principal: "User:svc", Operations: []string{"Write"}},
	}}
	tp.Spec.Prune = topicPrune

	pol := basePolicy("Enforce") // User:svc Read on topic payments.orders
	pol.Spec.Prune = policyPrune

	liveTopics := []kafka.TopicState{{Name: "payments.orders", Partitions: 3, Config: map[string]string{"retention.ms": "604800000"}}}
	liveACLs := append([]kafka.ACLState{
		// The topic's desired ACL.
		{Principal: "User:svc", Host: "*", ResourceType: "topic", ResourceName: "payments.orders", PatternType: "literal", Operation: "Write", Permission: "Allow"},
		// The policy's desired ACL: in the TOPIC's per-CR scope but not in its
		// desired set — the prune candidate that flaps without aggregation.
		{Principal: "User:svc", Host: "*", ResourceType: "topic", ResourceName: "payments.orders", PatternType: "literal", Operation: "Read", Permission: "Allow"},
	}, extraLive...)
	return tp, pol, kafkamock.New(liveTopics, liveACLs)
}

// liveOps returns the operations the mock currently stores for the fixture's
// shared (principal, resource pattern) pair.
func liveOps(t *testing.T, k *kafkamock.Client) map[string]bool {
	t.Helper()
	acls, err := k.ListACLs(context.Background())
	if err != nil {
		t.Fatalf("ListACLs: %v", err)
	}
	out := map[string]bool{}
	for _, a := range acls {
		if a.Principal == "User:svc" && a.ResourceType == "topic" && a.ResourceName == "payments.orders" {
			out[a.Operation] = true
		}
	}
	return out
}

// overlapView aggregates the fixture's two resources into the cluster-wide
// desired ACL view, the way the controllers do (spec §20.1).
func overlapView(t *testing.T, tp *v1alpha1.KafkaTopic, pol *v1alpha1.KafkaAccessPolicy) *ClusterACLView {
	t.Helper()
	return BuildClusterACLView([]*v1alpha1.KafkaTopic{tp}, []*v1alpha1.KafkaAccessPolicy{pol}, nil, nil)
}

// TestReconcileTopicPerCRScopePrunesOverlappingOwner documents the BUG the
// aggregated view fixes (spec §10.4): WITHOUT a view (nil — the per-CR path),
// reconciling the topic prunes the policy's live ACL because the topic's own
// scope covers it. This pins the per-CR semantics the CLI relies on; the
// operator must avoid it by passing a view.
func TestReconcileTopicPerCRScopePrunesOverlappingOwner(t *testing.T) {
	tp, _, k := overlapFixture(true, true)

	if _, err := ReconcileTopic(context.Background(), tp, cluster(), k, nil, stubResolver{}, nil, nil, nil); err != nil {
		t.Fatalf("ReconcileTopic: %v", err)
	}
	if got := liveOps(t, k); got["Read"] {
		t.Fatalf("per-CR path: expected the policy's Read ACL to be pruned (documenting the §10.4 flap), live ops = %v", got)
	}
}

// TestReconcileTopicAggregatedViewKeepsOtherOwnersACLs is the §10.4 fix: with a
// view aggregated across both resources, the policy's ACL is desired
// cluster-wide and must NOT be pruned when the topic reconciles.
func TestReconcileTopicAggregatedViewKeepsOtherOwnersACLs(t *testing.T) {
	tp, pol, k := overlapFixture(true, true)
	view := overlapView(t, tp.DeepCopy(), pol.DeepCopy())

	st, err := ReconcileTopic(context.Background(), tp, cluster(), k, nil, stubResolver{}, view, nil, nil)
	if err != nil {
		t.Fatalf("ReconcileTopic: %v", err)
	}
	got := liveOps(t, k)
	if !got["Read"] {
		t.Fatalf("aggregated view: the policy's Read ACL was pruned, live ops = %v (calls %v)", got, k.Calls())
	}
	if !got["Write"] {
		t.Fatalf("the topic's own Write ACL went missing, live ops = %v", got)
	}
	if st.Phase != v1alpha1.PhaseReady {
		t.Fatalf("phase = %q, want Ready", st.Phase)
	}
	if st.Drift != nil && st.Drift.Detected {
		t.Fatalf("drift detected on a converged overlap: %+v", st.Drift)
	}
}

// TestReconcileTopicAggregatedViewStillPrunesUndesired: an in-scope live ACL
// desired by NEITHER resource is still pruned when every covering resource
// consents.
func TestReconcileTopicAggregatedViewStillPrunesUndesired(t *testing.T) {
	tp, pol, k := overlapFixture(true, true, kafka.ACLState{
		Principal: "User:svc", Host: "*", ResourceType: "topic", ResourceName: "payments.orders",
		PatternType: "literal", Operation: "Alter", Permission: "Allow",
	})
	view := overlapView(t, tp.DeepCopy(), pol.DeepCopy())

	if _, err := ReconcileTopic(context.Background(), tp, cluster(), k, nil, stubResolver{}, view, nil, nil); err != nil {
		t.Fatalf("ReconcileTopic: %v", err)
	}
	got := liveOps(t, k)
	if got["Alter"] {
		t.Fatalf("genuinely undesired Alter ACL survived an all-consenting prune, live ops = %v (calls %v)", got, k.Calls())
	}
	if !got["Read"] || !got["Write"] {
		t.Fatalf("a desired ACL was pruned alongside the undesired one, live ops = %v", got)
	}
}

// TestReconcileTopicAggregatedViewVeto: prune consent is AND-merged across ALL
// aggregated owners (spec §10.3) — the policy's prune=false vetoes deletion of
// the genuinely undesired candidate even though the reconciled topic consents.
func TestReconcileTopicAggregatedViewVeto(t *testing.T) {
	tp, pol, k := overlapFixture(true, false, kafka.ACLState{
		Principal: "User:svc", Host: "*", ResourceType: "topic", ResourceName: "payments.orders",
		PatternType: "literal", Operation: "Alter", Permission: "Allow",
	})
	view := overlapView(t, tp.DeepCopy(), pol.DeepCopy())

	st, err := ReconcileTopic(context.Background(), tp, cluster(), k, nil, stubResolver{}, view, nil, nil)
	if err != nil {
		t.Fatalf("vetoed prune must not be a retryable failure: %v", err)
	}
	if callsContain(k.Calls(), "DeleteACLs") {
		t.Fatalf("policy vetoed pruning (prune=false) but DeleteACLs was called: %v", k.Calls())
	}
	if st.Drift == nil || !st.Drift.Detected {
		t.Fatalf("drift = %+v, want detected (the unpruned candidate is divergence)", st.Drift)
	}
	if st.Phase != v1alpha1.PhaseReady {
		t.Fatalf("phase = %q, want Ready (PruneDisabled is not a failure)", st.Phase)
	}
}

// TestReconcilePolicyAggregatedViewKeepsOtherOwnersACLs mirrors the topic test
// from the policy side: reconciling the POLICY with the aggregated view must
// not prune the topic's Write ACL.
func TestReconcilePolicyAggregatedViewKeepsOtherOwnersACLs(t *testing.T) {
	tp, pol, k := overlapFixture(true, true)
	view := overlapView(t, tp.DeepCopy(), pol.DeepCopy())

	if _, err := ReconcilePolicy(context.Background(), pol, cluster(), k, view); err != nil {
		t.Fatalf("ReconcilePolicy: %v", err)
	}
	got := liveOps(t, k)
	if !got["Write"] {
		t.Fatalf("aggregated view: the topic's Write ACL was pruned, live ops = %v (calls %v)", got, k.Calls())
	}
	if !got["Read"] {
		t.Fatalf("the policy's own Read ACL went missing, live ops = %v", got)
	}
}

// TestReconcileTopicNilViewMatchesPerCRBehavior pins that a nil view leaves the
// pre-§20.1 per-resource semantics fully intact (CLI single-resource path).
func TestReconcileTopicNilViewMatchesPerCRBehavior(t *testing.T) {
	tp, k := pruneTopicFixture(true)

	if _, err := ReconcileTopic(context.Background(), tp, cluster(), k, nil, stubResolver{}, nil, nil, nil); err != nil {
		t.Fatalf("ReconcileTopic: %v", err)
	}
	if !callsContain(k.Calls(), "DeleteACLs") {
		t.Fatalf("nil view must keep per-CR prune semantics: %v", k.Calls())
	}
}
