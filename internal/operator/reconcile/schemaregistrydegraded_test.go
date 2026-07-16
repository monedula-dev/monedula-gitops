package reconcile

import (
	"context"
	"errors"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	kafkamock "github.com/monedula-dev/monedula-gitops/internal/kafka/mock"
	schemamock "github.com/monedula-dev/monedula-gitops/internal/schemaregistry/mock"
)

// globalCompatFailClient wraps a schemamock.Client and forces
// GetGlobalCompatibility to error, simulating an older Schema Registry without
// GET /config (the mock's FailOn only injects failures on mutating,
// subject-keyed calls, so this unkeyed read needs its own wrapper — mirrors
// getTopicFailClient's approach in reconcile_test.go).
type globalCompatFailClient struct {
	*schemamock.Client
	err error
}

func (c *globalCompatFailClient) GetGlobalCompatibility(_ context.Context) (string, error) {
	return "", c.err
}

// schemaTopic returns an Enforce-mode topic with a governance-mode schema
// declared (no body), so a subject is managed and observeTopicLive fetches
// the global compatibility level.
func schemaTopic() *v1alpha1.KafkaTopic {
	tp := baseTopic("Enforce")
	tp.Spec.Schema = &v1alpha1.TopicSchema{
		Format:        "AVRO",
		Compatibility: "NONE",
	}
	return tp
}

// TestReconcileTopicSchemaRegistryDegradedOnFetchFailure pins the review gap:
// a failed GLOBAL compatibility fetch must surface as an informational
// SchemaRegistryDegraded=True condition (never fail the reconcile), and
// classification must still fall back to the legacy any-initial-set-is-a-Raise
// rule (the silent-swallow path the review noted had no test coverage).
func TestReconcileTopicSchemaRegistryDegradedOnFetchFailure(t *testing.T) {
	k := kafkamock.New(nil, nil)
	sr := &globalCompatFailClient{Client: schemamock.New(), err: errors.New("registry unreachable")}
	tp := schemaTopic()

	st, err := ReconcileTopic(context.Background(), tp, srConfiguredCluster(), k, sr, stubResolver{}, nil, nil, nil)
	if err != nil {
		t.Fatalf("a degraded SR fetch must not fail the reconcile, got: %v", err)
	}

	s, reason, ok := condStatus(st.Conditions, v1alpha1.CondSchemaRegistryDegraded)
	if !ok || s != metav1.ConditionTrue {
		t.Fatalf("SchemaRegistryDegraded = %v ok=%v, want True", s, ok)
	}
	if reason != reasonSchemaRegistryFetchFailed {
		t.Fatalf("SchemaRegistryDegraded reason = %q, want %q", reason, reasonSchemaRegistryFetchFailed)
	}

	// Classification falls back to legacy (any initial set is an ungated
	// Raise): the governance-mode NONE compatibility set applies WITHOUT
	// needing the allow-destructive annotation, and without error.
	if got := sr.Calls(); len(got) != 1 || got[0] != "SetCompatibility payments.orders-value NONE" {
		t.Fatalf("SR calls = %v, want [SetCompatibility payments.orders-value NONE] (legacy fallback: ungated raise)", got)
	}
	if st.Phase != v1alpha1.PhaseReady {
		t.Fatalf("phase = %q, want Ready", st.Phase)
	}
}

// TestReconcileTopicSchemaRegistryDegradedClearedOnSuccess pins the other
// half of the house style: a successful fetch sets the condition False (not
// absent), following the CondACLConflict/CondRBACCoarsened convention of
// always reporting a definite state while the feature is in play.
func TestReconcileTopicSchemaRegistryDegradedClearedOnSuccess(t *testing.T) {
	k := kafkamock.New(nil, nil)
	sr := schemamock.New()
	sr.SetGlobalCompatibility("BACKWARD")
	tp := schemaTopic()
	tp.Spec.Schema.Compatibility = "BACKWARD" // matches global; no gated Lower involved

	st, err := ReconcileTopic(context.Background(), tp, srConfiguredCluster(), k, sr, stubResolver{}, nil, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	s, reason, ok := condStatus(st.Conditions, v1alpha1.CondSchemaRegistryDegraded)
	if !ok || s != metav1.ConditionFalse {
		t.Fatalf("SchemaRegistryDegraded = %v ok=%v, want False", s, ok)
	}
	if reason != reasonSchemaRegistryOK {
		t.Fatalf("SchemaRegistryDegraded reason = %q, want %q", reason, reasonSchemaRegistryOK)
	}
}

// TestReconcileTopicSchemaRegistryDegradedAbsentWithoutSchema pins that the
// condition is cleared (not left stale True/False from a prior pass) when no
// schema is declared, mirroring the CondSchemaSynced removal in ReconcileTopic.
func TestReconcileTopicSchemaRegistryDegradedAbsentWithoutSchema(t *testing.T) {
	k := kafkamock.New(nil, nil)
	tp := baseTopic("Enforce") // no spec.Schema

	st, err := ReconcileTopic(context.Background(), tp, cluster(), k, nil, stubResolver{}, nil, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if _, _, ok := condStatus(st.Conditions, v1alpha1.CondSchemaRegistryDegraded); ok {
		t.Fatalf("SchemaRegistryDegraded should be absent when no schema is managed")
	}
}

// TestReconcileTopicSchemaRegistryDegradedAbsentOnResolveFailure pins a review
// finding: schemaDeclared alone is NOT sufficient to gate the condition. When
// spec.schema is declared but the body fails to RESOLVE (schemaResolveErr !=
// ""), desiredSchemas stays empty, so observeTopicLive's
// len(desiredSchemas) > 0 guard means GetGlobalCompatibility is never called
// at all — the fetch was never attempted, not successfully read. The
// condition must be absent (mirrors CondSchemaSynced's own
// schemaResolveErr == "" && schemaDeclared guard in applyEnforceResult), never
// a false "read successfully".
func TestReconcileTopicSchemaRegistryDegradedAbsentOnResolveFailure(t *testing.T) {
	k := kafkamock.New(nil, nil)
	sr := schemamock.New()
	tp := baseTopic("Enforce")
	tp.Spec.Schema = &v1alpha1.TopicSchema{
		Format:      "AVRO",
		ValueSchema: &v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{File: "schema.avsc"}},
	}

	st, err := ReconcileTopic(context.Background(), tp, srConfiguredCluster(), k, sr,
		stubResolver{err: errors.New("file refs unsupported in operator mode")}, nil, nil, nil)
	if err != nil {
		t.Fatalf("schema-resolve failure is terminal, want nil error, got: %v", err)
	}
	if got := sr.Calls(); len(got) != 0 {
		t.Fatalf("no SR calls expected when the schema fails to resolve: %v", got)
	}

	if _, _, ok := condStatus(st.Conditions, v1alpha1.CondSchemaRegistryDegraded); ok {
		t.Fatalf("SchemaRegistryDegraded should be absent when the fetch was never attempted (schema resolve failed)")
	}
}

// preThenSucceedCompatClient fails GetGlobalCompatibility on its FIRST call
// then succeeds on every subsequent call — simulating a transient blip that
// clears between the pre-apply observation and the Enforce post-apply
// re-observe.
type preThenSucceedCompatClient struct {
	*schemamock.Client
	calls int
	err   error
}

func (c *preThenSucceedCompatClient) GetGlobalCompatibility(ctx context.Context) (string, error) {
	c.calls++
	if c.calls == 1 {
		return "", c.err
	}
	return c.Client.GetGlobalCompatibility(ctx)
}

// TestReconcileTopicSchemaRegistryDegradedReflectsPreApplyFetch pins a review
// finding: the condition must reflect the PRE-APPLY observeTopicLive call —
// the one whose live.GlobalCompatibility fed diff.Compute's classification —
// not a later best-effort post-apply re-observe (Enforce mode reassigns
// `live` after a successful re-observe). If the pre-apply fetch fails
// (triggering the legacy ungated-Raise classification that gets APPLIED) but
// a transient blip clears before the post-apply re-observe, the condition
// must still report True: this reconcile's actual classification used the
// degraded fallback, regardless of what the LATER read returned.
func TestReconcileTopicSchemaRegistryDegradedReflectsPreApplyFetch(t *testing.T) {
	k := kafkamock.New(nil, nil)
	sr := &preThenSucceedCompatClient{Client: schemamock.New(), err: errors.New("transient blip")}
	sr.SetGlobalCompatibility("BACKWARD") // what the (successful) re-observe would read
	tp := schemaTopic()                   // Enforce, governance-mode NONE compatibility (first-time set)

	st, err := ReconcileTopic(context.Background(), tp, srConfiguredCluster(), k, sr, stubResolver{}, nil, nil, nil)
	if err != nil {
		t.Fatalf("a degraded SR fetch must not fail the reconcile, got: %v", err)
	}
	if sr.calls < 2 {
		t.Fatalf("expected at least 2 GetGlobalCompatibility calls (pre-apply + post-apply re-observe), got %d", sr.calls)
	}

	// Classification used the legacy fallback (pre-apply fetch failed): the
	// first-time NONE set applied UNGATED, proving this reconcile actually
	// degraded — so the condition asserted below must be True.
	if got := sr.Calls(); len(got) != 1 || got[0] != "SetCompatibility payments.orders-value NONE" {
		t.Fatalf("SR calls = %v, want [SetCompatibility payments.orders-value NONE] (legacy fallback used for classification)", got)
	}

	s, reason, ok := condStatus(st.Conditions, v1alpha1.CondSchemaRegistryDegraded)
	if !ok || s != metav1.ConditionTrue {
		t.Fatalf("SchemaRegistryDegraded = %v ok=%v, want True (pre-apply fetch failed and drove classification)", s, ok)
	}
	if reason != reasonSchemaRegistryFetchFailed {
		t.Fatalf("SchemaRegistryDegraded reason = %q, want %q", reason, reasonSchemaRegistryFetchFailed)
	}
}
