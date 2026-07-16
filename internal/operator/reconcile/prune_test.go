package reconcile

import (
	"context"
	"strings"
	"testing"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	kafkamock "github.com/monedula-dev/monedula-gitops/internal/kafka/mock"
)

// pruneTopicFixture builds an Enforce KafkaTopic whose live state matches the
// spec exactly except for ONE extra in-scope live ACL (a prune candidate,
// spec §10.3), plus the mock seeded with that state.
func pruneTopicFixture(prune bool) (*v1alpha1.KafkaTopic, *kafkamock.Client) {
	tp := baseTopic("Enforce")
	tp.Spec.Access = v1alpha1.TopicAccess{Producers: []v1alpha1.ProducerAccess{
		{Principal: "User:svc", Operations: []string{"Write"}},
	}}
	tp.Spec.Prune = prune

	liveTopics := []kafka.TopicState{{Name: "payments.orders", Partitions: 3, Config: map[string]string{"retention.ms": "604800000"}}}
	liveACLs := []kafka.ACLState{
		// Desired by the manifest.
		{Principal: "User:svc", Host: "*", ResourceType: "topic", ResourceName: "payments.orders", PatternType: "literal", Operation: "Write", Permission: "Allow"},
		// In scope (same principal + resource pattern) but no longer desired.
		{Principal: "User:svc", Host: "*", ResourceType: "topic", ResourceName: "payments.orders", PatternType: "literal", Operation: "Read", Permission: "Allow"},
	}
	return tp, kafkamock.New(liveTopics, liveACLs)
}

func callsContain(calls []string, prefix string) bool {
	for _, c := range calls {
		if strings.HasPrefix(c, prefix) {
			return true
		}
	}
	return false
}

func TestReconcileTopicPruneDisabledByDefault(t *testing.T) {
	tp, k := pruneTopicFixture(false)

	st, err := ReconcileTopic(context.Background(), tp, cluster(), k, nil, stubResolver{}, nil, nil, nil)
	if err != nil {
		t.Fatalf("unconsented prune must not be a retryable failure: %v", err)
	}
	if callsContain(k.Calls(), "DeleteACLs") {
		t.Fatalf("spec.prune=false but the live ACL was deleted: calls = %v", k.Calls())
	}
	if st.Phase != v1alpha1.PhaseReady {
		t.Fatalf("phase = %q, want Ready (PruneDisabled is not a failure)", st.Phase)
	}
	if st.Drift == nil || !st.Drift.Detected {
		t.Fatalf("drift = %+v, want detected (the prune candidate is divergence)", st.Drift)
	}
}

func TestReconcileTopicPruneEnabledDeletes(t *testing.T) {
	tp, k := pruneTopicFixture(true)

	st, err := ReconcileTopic(context.Background(), tp, cluster(), k, nil, stubResolver{}, nil, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error on consenting prune: %v", err)
	}
	if !callsContain(k.Calls(), "DeleteACLs") {
		t.Fatalf("spec.prune=true but no DeleteACLs call: calls = %v", k.Calls())
	}
	if st.Phase != v1alpha1.PhaseReady {
		t.Fatalf("phase = %q, want Ready", st.Phase)
	}
	if st.Drift != nil && st.Drift.Detected {
		t.Fatalf("drift detected after a clean prune: %+v", st.Drift)
	}
}

func prunePolicyFixture(prune bool) (*v1alpha1.KafkaAccessPolicy, *kafkamock.Client) {
	pol := basePolicy("Enforce")
	pol.Spec.Prune = prune
	liveACLs := []kafka.ACLState{
		// Desired by the policy (basePolicy rules: User:svc Read payments.orders).
		{Principal: "User:svc", Host: "*", ResourceType: "topic", ResourceName: "payments.orders", PatternType: "literal", Operation: "Read", Permission: "Allow"},
		// In scope but no longer desired.
		{Principal: "User:svc", Host: "*", ResourceType: "topic", ResourceName: "payments.orders", PatternType: "literal", Operation: "Write", Permission: "Allow"},
	}
	return pol, kafkamock.New(nil, liveACLs)
}

func TestReconcilePolicyPruneDisabledByDefault(t *testing.T) {
	pol, k := prunePolicyFixture(false)

	st, err := ReconcilePolicy(context.Background(), pol, cluster(), k, nil)
	if err != nil {
		t.Fatalf("unconsented prune must not be a retryable failure: %v", err)
	}
	if callsContain(k.Calls(), "DeleteACLs") {
		t.Fatalf("spec.prune=false but the live ACL was deleted: calls = %v", k.Calls())
	}
	if st.Drift == nil || !st.Drift.Detected {
		t.Fatalf("drift = %+v, want detected (the prune candidate is divergence)", st.Drift)
	}
}

func TestReconcilePolicyPruneEnabledDeletes(t *testing.T) {
	pol, k := prunePolicyFixture(true)

	st, err := ReconcilePolicy(context.Background(), pol, cluster(), k, nil)
	if err != nil {
		t.Fatalf("unexpected error on consenting prune: %v", err)
	}
	if !callsContain(k.Calls(), "DeleteACLs") {
		t.Fatalf("spec.prune=true but no DeleteACLs call: calls = %v", k.Calls())
	}
	if st.Phase != v1alpha1.PhaseReady {
		t.Fatalf("phase = %q, want Ready", st.Phase)
	}
}
