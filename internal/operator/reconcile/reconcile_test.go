package reconcile

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	k8smeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/monedula-dev/monedula-gitops/internal/access"
	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	kafkamock "github.com/monedula-dev/monedula-gitops/internal/kafka/mock"
	"github.com/monedula-dev/monedula-gitops/internal/operator"
	"github.com/monedula-dev/monedula-gitops/internal/schemaregistry"
	schemamock "github.com/monedula-dev/monedula-gitops/internal/schemaregistry/mock"
	"github.com/monedula-dev/monedula-gitops/internal/schemaregistry/recordname"
)

// stubResolver is a secrets.Resolver whose Resolve returns a fixed body (or a
// fixed error). It lets us exercise the schema path without files or k8s.
type stubResolver struct {
	body string
	err  error
}

func (s stubResolver) Resolve(_ v1alpha1.ValueFrom) (string, error) {
	if s.err != nil {
		return "", s.err
	}
	return s.body, nil
}

// getTopicFailClient wraps a kafka.AdminClient and forces GetTopic to error,
// simulating a live-state read failure (the in-memory mock only injects
// failures on mutating calls).
type getTopicFailClient struct {
	*kafkamock.Client
	err error
}

func (c *getTopicFailClient) GetTopic(ctx context.Context, name string) (*kafka.TopicState, error) {
	return nil, c.err
}

func cluster() *v1alpha1.KafkaCluster {
	return &v1alpha1.KafkaCluster{Spec: v1alpha1.KafkaClusterSpec{BootstrapServers: "localhost:9092"}}
}

// srConfiguredCluster is cluster() plus a schemaRegistry endpoint, needed since
// validation requires the cluster to configure a registry when a topic declares
// spec.schema.
func srConfiguredCluster() *v1alpha1.KafkaCluster {
	cl := cluster()
	cl.Spec.SchemaRegistry = &v1alpha1.SchemaRegistryConf{Endpoint: "http://sr"}
	return cl
}

func baseTopic(mode string) *v1alpha1.KafkaTopic {
	tp := &v1alpha1.KafkaTopic{
		Spec: v1alpha1.KafkaTopicSpec{
			ClusterRef: v1alpha1.ClusterRef{Name: "c"},
			TopicName:  "payments.orders",
			Partitions: 3,
			Config:     map[string]string{"retention.ms": "604800000"},
		},
	}
	tp.Name = "orders"
	tp.Generation = 7
	if mode != "" {
		tp.Spec.Reconciliation = &v1alpha1.Reconciliation{Mode: mode}
	}
	return tp
}

func condStatus(conds []metav1.Condition, typ string) (metav1.ConditionStatus, string, bool) {
	for _, c := range conds {
		if c.Type == typ {
			return c.Status, c.Reason, true
		}
	}
	return "", "", false
}

func TestReconcileTopicEnforceCreates(t *testing.T) {
	k := kafkamock.New(nil, nil)
	tp := baseTopic("Enforce")

	st, err := ReconcileTopic(context.Background(), tp, cluster(), k, nil, stubResolver{}, nil, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error on clean apply: %v", err)
	}

	if st.Phase != v1alpha1.PhaseReady {
		t.Fatalf("phase = %q, want Ready", st.Phase)
	}
	if got := k.Calls(); len(got) != 1 || got[0] != "CreateTopic payments.orders" {
		t.Fatalf("calls = %v, want [CreateTopic payments.orders]", got)
	}
	if s, _, ok := condStatus(st.Conditions, v1alpha1.CondTopicSynced); !ok || s != metav1.ConditionTrue {
		t.Fatalf("TopicSynced = %v ok=%v, want True", s, ok)
	}
	if s, _, _ := condStatus(st.Conditions, v1alpha1.CondDriftDetected); s != metav1.ConditionFalse {
		t.Fatalf("DriftDetected = %v, want False", s)
	}
	if st.Drift != nil && st.Drift.Detected {
		t.Fatalf("drift detected after clean apply")
	}
	if st.ObservedGeneration != 7 {
		t.Fatalf("observedGeneration = %d, want 7", st.ObservedGeneration)
	}
	if st.ObservedTopic == nil || st.ObservedTopic.TopicName != "payments.orders" || st.ObservedTopic.Partitions != 3 {
		t.Fatalf("observedTopic = %+v", st.ObservedTopic)
	}
	if st.LastAppliedTime == nil {
		t.Fatalf("LastAppliedTime not set after enforce apply")
	}
}

func TestReconcileTopicDetectOnlyReportsDrift(t *testing.T) {
	// Live topic exists with a different config value -> UpdateTopicConfig drift.
	live := []kafka.TopicState{{Name: "payments.orders", Partitions: 3, Config: map[string]string{"retention.ms": "1000"}}}
	k := kafkamock.New(live, nil)
	tp := baseTopic("DetectOnly")

	st, err := ReconcileTopic(context.Background(), tp, cluster(), k, nil, stubResolver{}, nil, nil, nil)
	if err != nil {
		t.Fatalf("DetectOnly drift is terminal, want nil error, got: %v", err)
	}

	if got := k.Calls(); len(got) != 0 {
		t.Fatalf("DetectOnly mutated cluster: calls = %v", got)
	}
	if st.Phase != v1alpha1.PhaseDrifted {
		t.Fatalf("phase = %q, want Drifted", st.Phase)
	}
	if st.Drift == nil || !st.Drift.Detected || len(st.Drift.Fields) == 0 {
		t.Fatalf("drift = %+v, want detected with fields", st.Drift)
	}
	if s, _, _ := condStatus(st.Conditions, v1alpha1.CondDriftDetected); s != metav1.ConditionTrue {
		t.Fatalf("DriftDetected = %v, want True", s)
	}
}

func TestReconcileTopicObserveOnly(t *testing.T) {
	live := []kafka.TopicState{{Name: "payments.orders", Partitions: 3, Config: map[string]string{"retention.ms": "1000"}}}
	k := kafkamock.New(live, nil)
	tp := baseTopic("ObserveOnly")

	st, err := ReconcileTopic(context.Background(), tp, cluster(), k, nil, stubResolver{}, nil, nil, nil)
	if err != nil {
		t.Fatalf("ObserveOnly is never a failure, want nil error, got: %v", err)
	}

	if got := k.Calls(); len(got) != 0 {
		t.Fatalf("ObserveOnly mutated cluster: calls = %v", got)
	}
	if st.Phase != v1alpha1.PhaseReady {
		t.Fatalf("phase = %q, want Ready (observe is not a failure)", st.Phase)
	}
	if st.Drift == nil || !st.Drift.Detected {
		t.Fatalf("drift = %+v, want detected", st.Drift)
	}
	if s, _, _ := condStatus(st.Conditions, v1alpha1.CondReady); s != metav1.ConditionTrue {
		t.Fatalf("Ready = %v, want True in observe-only", s)
	}
}

func TestReconcileTopicDestructiveBlockedWithoutAnnotation(t *testing.T) {
	// Live has fewer partitions -> IncreasePartitions, which is GateDestructive.
	live := []kafka.TopicState{{Name: "payments.orders", Partitions: 1, Config: map[string]string{"retention.ms": "604800000"}}}

	// Without annotation: blocked.
	k := kafkamock.New(live, nil)
	tp := baseTopic("Enforce")
	st, err := ReconcileTopic(context.Background(), tp, cluster(), k, nil, stubResolver{}, nil, nil, nil)
	// Blocked ops are TERMINAL (need a human approval annotation): nil error.
	if err != nil {
		t.Fatalf("blocked op is terminal, want nil error, got: %v", err)
	}
	if got := k.Calls(); len(got) != 0 {
		t.Fatalf("blocked op should not mutate: calls = %v", got)
	}
	if st.Phase != v1alpha1.PhaseError {
		t.Fatalf("phase = %q, want Error when op blocked", st.Phase)
	}
	if st.Drift == nil || !st.Drift.Detected {
		t.Fatalf("drift should remain detected when blocked: %+v", st.Drift)
	}

	// With annotation: applied.
	k2 := kafkamock.New(live, nil)
	tp2 := baseTopic("Enforce")
	tp2.Annotations = map[string]string{"gitops.monedula.dev/allow-destructive": "true"}
	st2, err2 := ReconcileTopic(context.Background(), tp2, cluster(), k2, nil, stubResolver{}, nil, nil, nil)
	if err2 != nil {
		t.Fatalf("approved apply succeeded, want nil error, got: %v", err2)
	}
	if got := k2.Calls(); len(got) != 1 || got[0] != "CreatePartitions payments.orders 3" {
		t.Fatalf("calls = %v, want [CreatePartitions payments.orders 3]", got)
	}
	if st2.Phase != v1alpha1.PhaseReady {
		t.Fatalf("phase = %q, want Ready after approved apply", st2.Phase)
	}
}

func TestReconcileTopicSchemaResolveFailureSkips(t *testing.T) {
	k := kafkamock.New(nil, nil)
	sr := schemamock.New()
	tp := baseTopic("Enforce")
	tp.Spec.Schema = &v1alpha1.TopicSchema{
		Format:      "AVRO",
		ValueSchema: &v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{File: "schema.avsc"}},
	}

	st, err := ReconcileTopic(context.Background(), tp, srConfiguredCluster(), k, sr,
		stubResolver{err: errors.New("file refs unsupported in operator mode")}, nil, nil, nil)
	// A schema-resolve failure is a config issue (TERMINAL): nil error.
	if err != nil {
		t.Fatalf("schema-resolve failure is terminal, want nil error, got: %v", err)
	}

	// Topic still reconciled.
	if got := k.Calls(); len(got) != 1 || got[0] != "CreateTopic payments.orders" {
		t.Fatalf("topic should still be created: calls = %v", got)
	}
	// No schema registered.
	if got := sr.Calls(); len(got) != 0 {
		t.Fatalf("schema should be skipped: SR calls = %v", got)
	}
	// SchemaSynced False with SchemaUnresolved reason.
	s, reason, ok := condStatus(st.Conditions, v1alpha1.CondSchemaSynced)
	if !ok || s != metav1.ConditionFalse || reason != "SchemaUnresolved" {
		t.Fatalf("SchemaSynced = %v reason=%q ok=%v, want False/SchemaUnresolved", s, reason, ok)
	}
	// Topic itself succeeded -> phase Ready (schema-resolve failure does not fail the topic).
	if st.Phase != v1alpha1.PhaseReady {
		t.Fatalf("phase = %q, want Ready (topic reconciled; schema skipped)", st.Phase)
	}
}

func TestReconcileTopicSchemaEnforce(t *testing.T) {
	k := kafkamock.New(nil, nil)
	sr := schemamock.New()
	tp := baseTopic("Enforce")
	tp.Spec.Schema = &v1alpha1.TopicSchema{
		Format:      "AVRO",
		ValueSchema: &v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{File: "schema.avsc"}},
	}
	body := `{"type":"record","name":"Order","fields":[]}`

	st, err := ReconcileTopic(context.Background(), tp, srConfiguredCluster(), k, sr, stubResolver{body: body}, nil, nil, nil)
	if err != nil {
		t.Fatalf("schema enforce succeeded, want nil error, got: %v", err)
	}

	if got := sr.Calls(); len(got) != 1 || got[0] != "RegisterSchema payments.orders-value" {
		t.Fatalf("SR calls = %v, want [RegisterSchema payments.orders-value]", got)
	}
	if s, _, ok := condStatus(st.Conditions, v1alpha1.CondSchemaSynced); !ok || s != metav1.ConditionTrue {
		t.Fatalf("SchemaSynced = %v ok=%v, want True", s, ok)
	}
	if st.Schema == nil || st.Schema.ValueSubject != "payments.orders-value" || st.Schema.ValueSchemaID == 0 {
		t.Fatalf("observed schema = %+v", st.Schema)
	}
	if st.Phase != v1alpha1.PhaseReady {
		t.Fatalf("phase = %q, want Ready", st.Phase)
	}
}

// TestReconcileTopicSchemaGovernanceMode pins governance mode (spec §12.2): the
// topic declares spec.schema with only format + compatibility (no body). The
// producer's pipeline has registered v1..v3 out-of-band (seeded). monedula must
// converge ONLY the subject compatibility level and NEVER call RegisterSchema —
// a producer-registered version is not drift. SchemaSynced reports True after
// the compatibility op applies, status.schema.compatibility is set to the
// managed level, and status.schema.valueSchemaId reflects the producer's latest.
func TestReconcileTopicSchemaGovernanceMode(t *testing.T) {
	k := kafkamock.New(nil, nil)
	sr := schemamock.New()
	// Producer registered v1..v3 out-of-band; current subject compatibility is
	// BACKWARD. The manifest desires FULL.
	sr.Seed("payments.orders-value", "AVRO", "BACKWARD",
		`{"type":"record","name":"Order","fields":[]}`,
		`{"type":"record","name":"Order","fields":[{"name":"id","type":"string"}]}`,
		`{"type":"record","name":"Order","fields":[{"name":"id","type":"string"},{"name":"amount","type":"long"}]}`,
	)

	tp := baseTopic("Enforce")
	tp.Spec.Schema = &v1alpha1.TopicSchema{
		Format:        "AVRO",
		Compatibility: "FULL",
		// No valueSchema/keySchema body -> governance mode.
	}

	st, err := ReconcileTopic(context.Background(), tp, srConfiguredCluster(), k, sr, stubResolver{}, nil, nil, nil)
	if err != nil {
		t.Fatalf("governance reconcile, want nil error, got: %v", err)
	}

	// ZERO RegisterSchema calls: the only mutating SR call is the compatibility raise.
	for _, c := range sr.Calls() {
		if strings.HasPrefix(c, "RegisterSchema") {
			t.Fatalf("unexpected RegisterSchema in governance mode: SR calls = %v", sr.Calls())
		}
	}
	if got := sr.Calls(); len(got) != 1 || got[0] != "SetCompatibility payments.orders-value FULL" {
		t.Fatalf("SR calls = %v, want [SetCompatibility payments.orders-value FULL]", got)
	}

	if s, _, ok := condStatus(st.Conditions, v1alpha1.CondSchemaSynced); !ok || s != metav1.ConditionTrue {
		t.Fatalf("SchemaSynced = %v ok=%v, want True", s, ok)
	}
	if st.Schema == nil || st.Schema.ValueSubject != "payments.orders-value" {
		t.Fatalf("observed schema = %+v", st.Schema)
	}
	if st.Schema.Compatibility != "FULL" {
		t.Fatalf("observed compatibility = %q, want FULL", st.Schema.Compatibility)
	}
	// Producer's latest (v3) id is surfaced; Seed assigned ids 1..3.
	if st.Schema.ValueSchemaID != 3 {
		t.Fatalf("observed valueSchemaID = %d, want 3 (producer's latest)", st.Schema.ValueSchemaID)
	}
}

// TestReconcileTopicSchemaFirstSetBelowGlobalGated pins the operator side of
// the first-time compatibility risk fix (spec §17.1): a fresh subject with NO
// subject-level override effectively runs at the registry's GLOBAL default, so
// declaring a level BELOW it (NONE under a global BACKWARD) is a gated Lower —
// Blocked without the allow-destructive annotation, applied with it.
func TestReconcileTopicSchemaFirstSetBelowGlobalGated(t *testing.T) {
	mkSR := func() *schemamock.Client {
		sr := schemamock.New()
		sr.SetGlobalCompatibility("BACKWARD")
		return sr
	}
	mkTopic := func() *v1alpha1.KafkaTopic {
		tp := baseTopic("Enforce")
		tp.Spec.Schema = &v1alpha1.TopicSchema{
			Format:        "AVRO",
			Compatibility: "NONE",
			// No valueSchema/keySchema body -> governance mode; the subject is
			// entirely absent, so this is a FIRST-TIME subject-level set.
		}
		return tp
	}

	// Without annotation: the LowerSchemaCompatibility op is Blocked; the
	// registry is never touched.
	sr := mkSR()
	st, err := ReconcileTopic(context.Background(), mkTopic(), srConfiguredCluster(), kafkamock.New(nil, nil), sr, stubResolver{}, nil, nil, nil)
	if err != nil {
		t.Fatalf("blocked op is terminal, want nil error, got: %v", err)
	}
	if got := sr.Calls(); len(got) != 0 {
		t.Fatalf("blocked first-time lowering must not mutate the registry: SR calls = %v", got)
	}
	if st.Phase != v1alpha1.PhaseError {
		t.Fatalf("phase = %q, want Error when op blocked", st.Phase)
	}

	// With the allow-destructive annotation: applied.
	sr2 := mkSR()
	tp2 := mkTopic()
	tp2.Annotations = map[string]string{"gitops.monedula.dev/allow-destructive": "true"}
	st2, err2 := ReconcileTopic(context.Background(), tp2, srConfiguredCluster(), kafkamock.New(nil, nil), sr2, stubResolver{}, nil, nil, nil)
	if err2 != nil {
		t.Fatalf("approved apply, want nil error, got: %v", err2)
	}
	if got := sr2.Calls(); len(got) != 1 || got[0] != "SetCompatibility payments.orders-value NONE" {
		t.Fatalf("SR calls = %v, want [SetCompatibility payments.orders-value NONE]", got)
	}
	if st2.Phase != v1alpha1.PhaseReady {
		t.Fatalf("phase = %q, want Ready after approved apply", st2.Phase)
	}
}

// TestReconcileTopicSchemaCustomStrategy pins a non-TopicName strategy (spec
// §11) end-to-end in the operator: a Custom content-mode topic registers under
// the explicit valueSubject (NOT <topic>-value) and the observed status reports
// that same custom subject.
func TestReconcileTopicSchemaCustomStrategy(t *testing.T) {
	k := kafkamock.New(nil, nil)
	sr := schemamock.New()
	tp := baseTopic("Enforce")
	tp.Spec.Schema = &v1alpha1.TopicSchema{
		Format:          "AVRO",
		SubjectStrategy: "Custom",
		ValueSubject:    "custom.value.subject",
		ValueSchema:     &v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{File: "schema.avsc"}},
	}
	body := `{"type":"record","name":"Order","fields":[]}`

	st, err := ReconcileTopic(context.Background(), tp, srConfiguredCluster(), k, sr, stubResolver{body: body}, nil, nil, nil)
	if err != nil {
		t.Fatalf("custom-strategy reconcile, want nil error, got: %v", err)
	}

	if got := sr.Calls(); len(got) != 1 || got[0] != "RegisterSchema custom.value.subject" {
		t.Fatalf("SR calls = %v, want [RegisterSchema custom.value.subject]", got)
	}
	if st.Schema == nil || st.Schema.ValueSubject != "custom.value.subject" || st.Schema.ValueSchemaID == 0 {
		t.Fatalf("observed schema = %+v, want valueSubject=custom.value.subject with an id", st.Schema)
	}
	if s, _, ok := condStatus(st.Conditions, v1alpha1.CondSchemaSynced); !ok || s != metav1.ConditionTrue {
		t.Fatalf("SchemaSynced = %v ok=%v, want True", s, ok)
	}
}

// TestReconcileTopicInvalidModeIsValidationFailure pins the CRITICAL case: a
// case-variant reconciliation mode ("detectOnly") must be a terminal validation
// failure — it must NEVER fall through the mode switch into the Enforce default
// and mutate the cluster.
func TestReconcileTopicInvalidModeIsValidationFailure(t *testing.T) {
	k := kafkamock.New(nil, nil)
	tp := baseTopic("detectOnly") // lowercase: invalid

	st, err := ReconcileTopic(context.Background(), tp, cluster(), k, nil, stubResolver{}, nil, nil, nil)
	if err != nil {
		t.Fatalf("validation failure is terminal, want nil error, got: %v", err)
	}
	if got := k.Calls(); len(got) != 0 {
		t.Fatalf("invalid mode must NOT mutate the cluster (fell through to Enforce?): calls = %v", got)
	}
	if st.Phase != v1alpha1.PhaseError {
		t.Fatalf("phase = %q, want Error on invalid mode", st.Phase)
	}
	s, _, ok := condStatus(st.Conditions, v1alpha1.CondValidationFailed)
	if !ok || s != metav1.ConditionTrue {
		t.Fatalf("ValidationFailed = %v ok=%v, want True", s, ok)
	}
	if s, _, _ := condStatus(st.Conditions, v1alpha1.CondReady); s != metav1.ConditionFalse {
		t.Fatalf("Ready = %v, want False", s)
	}
}

// TestReconcileTopicInvalidAccessOpIsValidationFailure: a case-variant access
// operation (WRITE) is a validation failure, not an eternally-reapplied ACL.
func TestReconcileTopicInvalidAccessOpIsValidationFailure(t *testing.T) {
	k := kafkamock.New(nil, nil)
	tp := baseTopic("Enforce")
	tp.Spec.Access = v1alpha1.TopicAccess{
		Producers: []v1alpha1.ProducerAccess{{Principal: "User:svc", Operations: []string{"WRITE"}}},
	}

	st, err := ReconcileTopic(context.Background(), tp, cluster(), k, nil, stubResolver{}, nil, nil, nil)
	if err != nil {
		t.Fatalf("validation failure is terminal, want nil error, got: %v", err)
	}
	if got := k.Calls(); len(got) != 0 {
		t.Fatalf("invalid spec must not mutate the cluster: calls = %v", got)
	}
	if st.Phase != v1alpha1.PhaseError {
		t.Fatalf("phase = %q, want Error", st.Phase)
	}
	if s, _, ok := condStatus(st.Conditions, v1alpha1.CondValidationFailed); !ok || s != metav1.ConditionTrue {
		t.Fatalf("ValidationFailed = %v ok=%v, want True", s, ok)
	}
}

// TestReconcilePolicyInvalidModeIsValidationFailure is the policy analogue of
// the invalid-mode test: lowercase "enforce" must not enforce.
func TestReconcilePolicyInvalidModeIsValidationFailure(t *testing.T) {
	k := kafkamock.New(nil, nil)
	pol := basePolicy("enforce") // lowercase: invalid

	st, err := ReconcilePolicy(context.Background(), pol, cluster(), k, nil)
	if err != nil {
		t.Fatalf("validation failure is terminal, want nil error, got: %v", err)
	}
	if got := k.Calls(); len(got) != 0 {
		t.Fatalf("invalid mode must NOT mutate the cluster (fell through to Enforce?): calls = %v", got)
	}
	if st.Phase != v1alpha1.PhaseError {
		t.Fatalf("phase = %q, want Error on invalid mode", st.Phase)
	}
	if s, _, ok := condStatus(st.Conditions, v1alpha1.CondValidationFailed); !ok || s != metav1.ConditionTrue {
		t.Fatalf("ValidationFailed = %v ok=%v, want True", s, ok)
	}
}

// TestReconcilePolicyInvalidRuleIsValidationFailure: an invalid rule (bad
// resource type) is terminal and mutation-free.
func TestReconcilePolicyInvalidRuleIsValidationFailure(t *testing.T) {
	k := kafkamock.New(nil, nil)
	pol := basePolicy("Enforce")
	pol.Spec.Rules[0].Resource.Type = "Topic" // non-canonical: invalid

	st, err := ReconcilePolicy(context.Background(), pol, cluster(), k, nil)
	if err != nil {
		t.Fatalf("validation failure is terminal, want nil error, got: %v", err)
	}
	if got := k.Calls(); len(got) != 0 {
		t.Fatalf("invalid rule must not mutate the cluster: calls = %v", got)
	}
	if st.Phase != v1alpha1.PhaseError {
		t.Fatalf("phase = %q, want Error", st.Phase)
	}
	if s, _, ok := condStatus(st.Conditions, v1alpha1.CondValidationFailed); !ok || s != metav1.ConditionTrue {
		t.Fatalf("ValidationFailed = %v ok=%v, want True", s, ok)
	}
}

// TestReconcilePolicyValidationConflict exercises the ACL Allow/Deny conflict
// path (only reachable via a policy, since topic access compiles all-Allow).
func TestReconcilePolicyValidationConflict(t *testing.T) {
	k := kafkamock.New(nil, nil)
	pol := basePolicy("Enforce")
	pol.Spec.Rules = []v1alpha1.ACLRule{
		{Principal: "User:x", Permission: "Allow", Operations: []string{"Read"},
			Resource: v1alpha1.ACLResource{Type: "topic", Name: "t"}},
		{Principal: "User:x", Permission: "Deny", Operations: []string{"Read"},
			Resource: v1alpha1.ACLResource{Type: "topic", Name: "t"}},
	}

	st, err := ReconcilePolicy(context.Background(), pol, cluster(), k, nil)
	// A validation conflict is TERMINAL (needs a spec change): nil error.
	if err != nil {
		t.Fatalf("validation conflict is terminal, want nil error, got: %v", err)
	}

	if got := k.Calls(); len(got) != 0 {
		t.Fatalf("conflict should not mutate: calls = %v", got)
	}
	if st.Phase != v1alpha1.PhaseError {
		t.Fatalf("phase = %q, want Error on ACL conflict", st.Phase)
	}
	if s, _, ok := condStatus(st.Conditions, v1alpha1.CondValidationFailed); !ok || s != metav1.ConditionTrue {
		t.Fatalf("ValidationFailed = %v ok=%v, want True", s, ok)
	}
}

// ---- Policy tests ----

func basePolicy(mode string) *v1alpha1.KafkaAccessPolicy {
	pol := &v1alpha1.KafkaAccessPolicy{
		Spec: v1alpha1.KafkaAccessPolicySpec{
			ClusterRef: v1alpha1.ClusterRef{Name: "c"},
			Rules: []v1alpha1.ACLRule{
				{
					Principal:  "User:svc",
					Operations: []string{"Read"},
					Resource:   v1alpha1.ACLResource{Type: "topic", Name: "payments.orders"},
				},
			},
		},
	}
	pol.Name = "p"
	pol.Generation = 4
	if mode != "" {
		pol.Spec.Reconciliation = &v1alpha1.Reconciliation{Mode: mode}
	}
	return pol
}

func TestReconcilePolicyEnforceCreatesAcls(t *testing.T) {
	k := kafkamock.New(nil, nil)
	pol := basePolicy("Enforce")

	st, err := ReconcilePolicy(context.Background(), pol, cluster(), k, nil)
	if err != nil {
		t.Fatalf("policy enforce succeeded, want nil error, got: %v", err)
	}

	if got := k.Calls(); len(got) != 1 || !strings.HasPrefix(got[0], "CreateACLs ") {
		t.Fatalf("calls = %v, want one CreateACLs", got)
	}
	if st.Phase != v1alpha1.PhaseReady {
		t.Fatalf("phase = %q, want Ready", st.Phase)
	}
	if s, _, ok := condStatus(st.Conditions, v1alpha1.CondAccessPolicySynced); !ok || s != metav1.ConditionTrue {
		t.Fatalf("AccessPolicySynced = %v ok=%v, want True", s, ok)
	}
	if len(st.ObservedRules) == 0 {
		t.Fatalf("observedRules empty")
	}
	if st.ObservedGeneration != 4 {
		t.Fatalf("observedGeneration = %d, want 4", st.ObservedGeneration)
	}
}

func TestReconcilePolicyDetectOnly(t *testing.T) {
	k := kafkamock.New(nil, nil)
	pol := basePolicy("DetectOnly")

	st, err := ReconcilePolicy(context.Background(), pol, cluster(), k, nil)
	if err != nil {
		t.Fatalf("DetectOnly drift is terminal, want nil error, got: %v", err)
	}

	if got := k.Calls(); len(got) != 0 {
		t.Fatalf("DetectOnly mutated cluster: calls = %v", got)
	}
	if st.Phase != v1alpha1.PhaseDrifted {
		t.Fatalf("phase = %q, want Drifted", st.Phase)
	}
	if st.Drift == nil || !st.Drift.Detected || len(st.Drift.Fields) == 0 {
		t.Fatalf("drift = %+v, want detected with fields", st.Drift)
	}
}

// TestReconcileTopicLiveStateError verifies that a live-state read failure is
// TRANSIENT: ReconcileTopic returns a non-nil (retryable) error, sets Phase
// Error / Ready False with reason LiveStateError, and still returns the status.
func TestReconcileTopicLiveStateError(t *testing.T) {
	// The in-memory mock only injects failures on mutating calls, so wrap it to
	// fail the GetTopic read (a live-state read failure).
	k := &getTopicFailClient{Client: kafkamock.New(nil, nil), err: errors.New("broker unreachable")}
	tp := baseTopic("Enforce")

	st, err := ReconcileTopic(context.Background(), tp, cluster(), k, nil, stubResolver{}, nil, nil, nil)

	if err == nil {
		t.Fatalf("live-state read failure is transient, want non-nil error")
	}
	if st.Phase != v1alpha1.PhaseError {
		t.Fatalf("phase = %q, want Error on live-state failure", st.Phase)
	}
	s, reason, ok := condStatus(st.Conditions, v1alpha1.CondReady)
	if !ok || s != metav1.ConditionFalse || reason != reasonLiveStateError {
		t.Fatalf("Ready = %v reason=%q ok=%v, want False/%s", s, reason, ok, reasonLiveStateError)
	}
	// Status is still returned (observed generation populated).
	if st.ObservedGeneration != 7 {
		t.Fatalf("status not returned: observedGeneration = %d, want 7", st.ObservedGeneration)
	}
}

// TestReconcileTopicPartialApply verifies a partial Enforce: the topic is
// created (Succeeded) but an ACL op Fails. The per-area conditions split
// (TopicSynced True, TopicAccessSynced False) and the Failed op is TRANSIENT,
// so a non-nil (retryable) error is returned and Phase is Error.
func TestReconcileTopicPartialApply(t *testing.T) {
	k := kafkamock.New(nil, nil)
	// One ACL op (the topic's compiled access) -> CreateACLs 1; make it fail.
	k.FailOn("CreateACLs", "1", errors.New("acl backend transient"))
	tp := baseTopic("Enforce")
	// Topic-local access so an ACL op is generated.
	tp.Spec.Access = v1alpha1.TopicAccess{
		Producers: []v1alpha1.ProducerAccess{
			{Principal: "User:svc", Operations: []string{"Write"}},
		},
	}

	st, err := ReconcileTopic(context.Background(), tp, cluster(), k, nil, stubResolver{}, nil, nil, nil)

	if err == nil {
		t.Fatalf("a Failed apply op is retryable, want non-nil error")
	}
	if s, _, ok := condStatus(st.Conditions, v1alpha1.CondTopicSynced); !ok || s != metav1.ConditionTrue {
		t.Fatalf("TopicSynced = %v ok=%v, want True (topic created)", s, ok)
	}
	if s, _, ok := condStatus(st.Conditions, v1alpha1.CondTopicAccessSynced); !ok || s != metav1.ConditionFalse {
		t.Fatalf("TopicAccessSynced = %v ok=%v, want False (acl failed)", s, ok)
	}
	if st.Phase != v1alpha1.PhaseError {
		t.Fatalf("phase = %q, want Error on partial apply", st.Phase)
	}
}

// TestReconcileTopicDriftFieldsContent pins the deterministic Drift.Fields slice
// produced by a DetectOnly run: a partition increase + a config update.
func TestReconcileTopicDriftFieldsContent(t *testing.T) {
	live := []kafka.TopicState{{Name: "payments.orders", Partitions: 1, Config: map[string]string{"retention.ms": "1000"}}}
	k := kafkamock.New(live, nil)
	tp := baseTopic("DetectOnly")

	st, err := ReconcileTopic(context.Background(), tp, cluster(), k, nil, stubResolver{}, nil, nil, nil)
	if err != nil {
		t.Fatalf("DetectOnly drift is terminal, want nil error, got: %v", err)
	}
	if st.Drift == nil {
		t.Fatalf("drift = nil, want detected with fields")
	}
	want := []string{
		"IncreasePartitions payments.orders",
		"UpdateTopicConfig payments.orders",
	}
	if !reflect.DeepEqual(st.Drift.Fields, want) {
		t.Fatalf("Drift.Fields = %#v, want %#v", st.Drift.Fields, want)
	}
}

// TestReconcileTopicPreservesTransitionTime verifies that seeding the new
// status's Conditions from the resource's existing status lets
// meta.SetStatusCondition preserve LastTransitionTime across reconciles when
// the condition's Type+Status are unchanged (the periodic-requeue case), and
// that it DOES change when the condition's Status flips.
func TestReconcileTopicPreservesTransitionTime(t *testing.T) {
	readyTime := func(conds []metav1.Condition) metav1.Time {
		for _, c := range conds {
			if c.Type == v1alpha1.CondReady {
				return c.LastTransitionTime
			}
		}
		t.Fatalf("no Ready condition in %v", conds)
		return metav1.Time{}
	}

	// First reconcile: clean apply -> Ready True.
	k1 := kafkamock.New(nil, nil)
	tp := baseTopic("Enforce")
	st1, err := ReconcileTopic(context.Background(), tp, cluster(), k1, nil, stubResolver{}, nil, nil, nil)
	if err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	t1 := readyTime(st1.Conditions)
	if t1.IsZero() {
		t.Fatalf("first reconcile Ready LastTransitionTime is zero")
	}

	// Second reconcile with the SAME live/desired state, feeding st1 back as the
	// resource's .Status. Live now has the topic (created above) so it's an
	// in-sync no-op -> Ready stays True. LastTransitionTime must be IDENTICAL.
	live := []kafka.TopicState{{Name: "payments.orders", Partitions: 3, Config: map[string]string{"retention.ms": "604800000"}}}
	k2 := kafkamock.New(live, nil)
	tp.Status = &st1
	st2, err := ReconcileTopic(context.Background(), tp, cluster(), k2, nil, stubResolver{}, nil, nil, nil)
	if err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	t2 := readyTime(st2.Conditions)
	if !t2.Equal(&t1) {
		t.Fatalf("Ready LastTransitionTime bumped on unchanged reconcile: %v -> %v", t1, t2)
	}
	if s, _, _ := condStatus(st2.Conditions, v1alpha1.CondReady); s != metav1.ConditionTrue {
		t.Fatalf("Ready = %v, want True (still in sync)", s)
	}

	// Third reconcile where the condition flips to False (live-state read error)
	// MUST update LastTransitionTime.
	k3 := &getTopicFailClient{Client: kafkamock.New(nil, nil), err: errors.New("broker unreachable")}
	tp.Status = &st2
	st3, _ := ReconcileTopic(context.Background(), tp, cluster(), k3, nil, stubResolver{}, nil, nil, nil)
	t3 := readyTime(st3.Conditions)
	if s, _, _ := condStatus(st3.Conditions, v1alpha1.CondReady); s != metav1.ConditionFalse {
		t.Fatalf("Ready = %v, want False (live-state error)", s)
	}
	if t3.Equal(&t2) {
		t.Fatalf("Ready LastTransitionTime NOT updated when status flipped: still %v", t3)
	}
}

// TestReconcileTopicReplicationFactorBlockedWithoutAnnotation pins review
// I10(a) through the engine: an RF change without the allow-destructive
// annotation is Blocked at the §17.1 gate — a TERMINAL outcome (nil error, no
// requeue), not a Failed/retried one.
func TestReconcileTopicReplicationFactorBlockedWithoutAnnotation(t *testing.T) {
	live := []kafka.TopicState{{Name: "payments.orders", Partitions: 3, ReplicationFactor: 3,
		Config: map[string]string{"retention.ms": "604800000"}}}
	k := kafkamock.New(live, nil)
	tp := baseTopic("Enforce")
	rf := 5
	tp.Spec.ReplicationFactor = &rf

	st, err := ReconcileTopic(context.Background(), tp, cluster(), k, nil, stubResolver{}, nil, nil, nil)
	if err != nil {
		t.Fatalf("blocked RF change is terminal, want nil error, got: %v", err)
	}
	if got := k.Calls(); len(got) != 0 {
		t.Fatalf("blocked RF change must not mutate: calls = %v", got)
	}
	if st.Phase != v1alpha1.PhaseError {
		t.Fatalf("phase = %q, want Error when RF change blocked", st.Phase)
	}
	if st.Drift == nil || !st.Drift.Detected {
		t.Fatalf("drift should remain detected when blocked: %+v", st.Drift)
	}
}

// TestReconcileTopicReplicationFactorUnsupportedTerminal pins review I10(b):
// an APPROVED RF change is Unsupported — a TERMINAL divergence. ReconcileTopic
// must return a nil error (no infinite backoff; only a spec change resolves
// it) while still reporting Error phase + detected drift.
func TestReconcileTopicReplicationFactorUnsupportedTerminal(t *testing.T) {
	live := []kafka.TopicState{{Name: "payments.orders", Partitions: 3, ReplicationFactor: 3,
		Config: map[string]string{"retention.ms": "604800000"}}}
	k := kafkamock.New(live, nil)
	tp := baseTopic("Enforce")
	rf := 5
	tp.Spec.ReplicationFactor = &rf
	tp.Annotations = map[string]string{"gitops.monedula.dev/allow-destructive": "true"}

	st, err := ReconcileTopic(context.Background(), tp, cluster(), k, nil, stubResolver{}, nil, nil, nil)
	if err != nil {
		t.Fatalf("Unsupported op is terminal (needs a spec change), want nil error, got: %v", err)
	}
	if got := k.Calls(); len(got) != 0 {
		t.Fatalf("unsupported RF change must not mutate: calls = %v", got)
	}
	if st.Phase != v1alpha1.PhaseError {
		t.Fatalf("phase = %q, want Error on unsupported op", st.Phase)
	}
	if st.Drift == nil || !st.Drift.Detected {
		t.Fatalf("drift should be detected for an unsupported op: %+v", st.Drift)
	}
}

// TestReconcileTopicValidationFailedClears pins review I11: a topic whose spec
// failed validation (ValidationFailed=True) and is then FIXED must see
// ValidationFailed flip to False on the next reconcile — conditions are seeded
// from the prior status, so without an explicit clear it would stay True
// forever.
func TestReconcileTopicValidationFailedClears(t *testing.T) {
	// First pass: invalid access operation -> ValidationFailed True.
	k1 := kafkamock.New(nil, nil)
	tp1 := baseTopic("Enforce")
	tp1.Spec.Access = v1alpha1.TopicAccess{
		Producers: []v1alpha1.ProducerAccess{{Principal: "User:svc", Operations: []string{"WRITE"}}},
	}
	st1, err := ReconcileTopic(context.Background(), tp1, cluster(), k1, nil, stubResolver{}, nil, nil, nil)
	if err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	if s, _, ok := condStatus(st1.Conditions, v1alpha1.CondValidationFailed); !ok || s != metav1.ConditionTrue {
		t.Fatalf("ValidationFailed = %v ok=%v, want True after invalid spec", s, ok)
	}

	// Second pass: spec fixed, prior status fed back (the controller seeds
	// conditions from it). ValidationFailed must clear.
	k2 := kafkamock.New(nil, nil)
	tp2 := baseTopic("Enforce")
	tp2.Status = &st1
	st2, err := ReconcileTopic(context.Background(), tp2, cluster(), k2, nil, stubResolver{}, nil, nil, nil)
	if err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	s, _, ok := condStatus(st2.Conditions, v1alpha1.CondValidationFailed)
	if !ok || s != metav1.ConditionFalse {
		t.Fatalf("ValidationFailed = %v ok=%v, want False after the spec is fixed (stale condition not cleared)", s, ok)
	}
	if st2.Phase != v1alpha1.PhaseReady {
		t.Fatalf("phase = %q, want Ready after the spec is fixed", st2.Phase)
	}
}

// TestReconcilePolicyValidationFailedClears is the policy analogue of the
// topic stale-ValidationFailed test, exercised via the ACL-conflict path.
func TestReconcilePolicyValidationFailedClears(t *testing.T) {
	// First pass: Allow/Deny conflict -> ValidationFailed True (ACLConflict).
	k1 := kafkamock.New(nil, nil)
	pol1 := basePolicy("Enforce")
	pol1.Spec.Rules = []v1alpha1.ACLRule{
		{Principal: "User:x", Permission: "Allow", Operations: []string{"Read"},
			Resource: v1alpha1.ACLResource{Type: "topic", Name: "t"}},
		{Principal: "User:x", Permission: "Deny", Operations: []string{"Read"},
			Resource: v1alpha1.ACLResource{Type: "topic", Name: "t"}},
	}
	st1, err := ReconcilePolicy(context.Background(), pol1, cluster(), k1, nil)
	if err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	if s, _, ok := condStatus(st1.Conditions, v1alpha1.CondValidationFailed); !ok || s != metav1.ConditionTrue {
		t.Fatalf("ValidationFailed = %v ok=%v, want True after conflict", s, ok)
	}

	// Second pass: conflict removed, prior status fed back.
	k2 := kafkamock.New(nil, nil)
	pol2 := basePolicy("Enforce")
	pol2.Status = &st1
	st2, err := ReconcilePolicy(context.Background(), pol2, cluster(), k2, nil)
	if err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	s, _, ok := condStatus(st2.Conditions, v1alpha1.CondValidationFailed)
	if !ok || s != metav1.ConditionFalse {
		t.Fatalf("ValidationFailed = %v ok=%v, want False after the conflict is fixed (stale condition not cleared)", s, ok)
	}
}

// TestReconcileTopicSchemaSyncedRemovedWhenSchemaRemoved pins review I11 for
// SchemaSynced: once the topic no longer declares spec.schema, the stale
// SchemaSynced condition (seeded from the prior status) must be REMOVED, not
// left at its old value.
func TestReconcileTopicSchemaSyncedRemovedWhenSchemaRemoved(t *testing.T) {
	// First pass: schema declared -> SchemaSynced set.
	k1 := kafkamock.New(nil, nil)
	sr := schemamock.New()
	tp1 := baseTopic("Enforce")
	tp1.Spec.Schema = &v1alpha1.TopicSchema{
		Format:      "AVRO",
		ValueSchema: &v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{File: "schema.avsc"}},
	}
	body := `{"type":"record","name":"Order","fields":[]}`
	st1, err := ReconcileTopic(context.Background(), tp1, srConfiguredCluster(), k1, sr, stubResolver{body: body}, nil, nil, nil)
	if err != nil {
		t.Fatalf("first reconcile: %v", err)
	}
	if _, _, ok := condStatus(st1.Conditions, v1alpha1.CondSchemaSynced); !ok {
		t.Fatal("SchemaSynced should be set while the schema is declared")
	}

	// Second pass: schema block removed, prior status fed back.
	k2 := kafkamock.New([]kafka.TopicState{{Name: "payments.orders", Partitions: 3,
		Config: map[string]string{"retention.ms": "604800000"}}}, nil)
	tp2 := baseTopic("Enforce")
	tp2.Status = &st1
	st2, err := ReconcileTopic(context.Background(), tp2, cluster(), k2, nil, stubResolver{}, nil, nil, nil)
	if err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	if s, _, ok := condStatus(st2.Conditions, v1alpha1.CondSchemaSynced); ok {
		t.Fatalf("SchemaSynced = %v, want condition REMOVED once spec.schema is gone (stale condition)", s)
	}
}

// TestSubjectName pins the TopicName-strategy subject names the reconciler emits
// (spec §11) via recordname.Subjects, the shared computation. Strategy-specific
// extraction is covered exhaustively in the recordname package tests.
func TestSubjectName(t *testing.T) {
	sc := &v1alpha1.TopicSchema{Format: "AVRO"}
	body := `{"type":"record","name":"Order","fields":[]}`
	vs, ks, err := recordname.Subjects("TopicName", "foo", sc, body, body)
	if err != nil {
		t.Fatalf("Subjects: %v", err)
	}
	if vs != "foo-value" {
		t.Fatalf("valueSubject = %q", vs)
	}
	if ks != "foo-key" {
		t.Fatalf("keySubject = %q", ks)
	}
}

var _ schemaregistry.Client = (*schemamock.Client)(nil)

// TestReconcileTopicSchemaSupersededTerminal pins spec §12.1 in the operator:
// the manifest schema is registered as v1 while the registry latest is v2.
// The reconcile must NOT re-register (dedupe loop), must set SchemaSynced
// False with reason SchemaSuperseded, and must return a NIL retry error —
// supersession is terminal, only a manifest or registry change resolves it.
func TestReconcileTopicSchemaSupersededTerminal(t *testing.T) {
	v1 := `{"type":"record","name":"Order","fields":[]}`
	v2 := `{"type":"record","name":"Order","fields":[{"name":"id","type":"string"}]}`

	k := kafkamock.New([]kafka.TopicState{{Name: "payments.orders", Partitions: 3, Config: map[string]string{"retention.ms": "604800000"}}}, nil)
	sr := schemamock.New()
	for _, def := range []string{v1, v2} {
		if _, err := sr.RegisterSchema(context.Background(), "payments.orders-value",
			schemaregistry.Schema{Type: schemaregistry.AVRO, Definition: def}); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	seeded := len(sr.Calls())

	tp := baseTopic("Enforce")
	tp.Spec.Schema = &v1alpha1.TopicSchema{
		Format:      "AVRO",
		ValueSchema: &v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{File: "schema.avsc"}},
	}

	st, err := ReconcileTopic(context.Background(), tp, srConfiguredCluster(), k, sr, stubResolver{body: v1}, nil, nil, nil)
	if err != nil {
		t.Fatalf("superseded is terminal: retry error must be nil, got %v", err)
	}

	if got := sr.Calls(); len(got) != seeded {
		t.Fatalf("registry mutated: calls = %v, want only the %d seeding calls", got, seeded)
	}
	s, reason, ok := condStatus(st.Conditions, v1alpha1.CondSchemaSynced)
	if !ok || s != metav1.ConditionFalse || reason != "SchemaSuperseded" {
		t.Fatalf("SchemaSynced = %v reason=%q ok=%v, want False/SchemaSuperseded", s, reason, ok)
	}
	if st.Phase != v1alpha1.PhaseError {
		t.Fatalf("phase = %q, want Error (the divergence stands unresolved)", st.Phase)
	}
}

// ---- ManagedSubjects unit tests (spec §12, fix 1 + fix 2) ----

// mapResolver is a per-ValueFrom resolver used in ManagedSubjects tests so we
// can seed specific bodies per source reference (and leave others missing).
type mapResolver struct {
	bodies map[string]string // key = ValueFrom.ValueFrom.Inline (used as a stable key in tests)
	err    error             // if non-nil, returned for ALL calls (simulates deleted Secret)
}

func (m mapResolver) Resolve(vf v1alpha1.ValueFrom) (string, error) {
	if m.err != nil {
		return "", m.err
	}
	if body, ok := m.bodies[vf.ValueFrom.Inline]; ok {
		return body, nil
	}
	// Unknown source — return the inline body verbatim (tests use inline refs
	// so the body IS the key).
	return vf.ValueFrom.Inline, nil
}

// avroValueDef is a minimal Avro record used for RecordName/TopicRecordName tests.
const avroValueDef = `{"type":"record","name":"OrderEvent","namespace":"com.example","fields":[]}`

// topicForManagedSubjects builds a minimal KafkaTopic for ManagedSubjects tests.
// strategy="" → TopicName default; topicNameField is spec.topicName.
func topicForManagedSubjects(strategy, topicNameField string, value, key *v1alpha1.ValueFrom) *v1alpha1.KafkaTopic {
	sc := &v1alpha1.TopicSchema{
		Format:          "AVRO",
		SubjectStrategy: strategy,
		ValueSchema:     value,
		KeySchema:       key,
	}
	if strategy == "Custom" {
		// Custom strategy: set explicit subject names based on which schemas are present.
		if value != nil {
			sc.ValueSubject = topicNameField + "-value-custom"
		}
		if key != nil {
			sc.KeySubject = topicNameField + "-key-custom"
		}
	}
	tp := &v1alpha1.KafkaTopic{
		Spec: v1alpha1.KafkaTopicSpec{
			TopicName: topicNameField,
			Schema:    sc,
		},
	}
	tp.Name = "orders"
	return tp
}

// inline returns a *ValueFrom wrapping an inline body.
func inline(body string) *v1alpha1.ValueFrom {
	return &v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{Inline: body}}
}

// errResolver always fails; it simulates a Secret deleted before finalization.
func errResolver(msg string) mapResolver {
	return mapResolver{err: errors.New(msg)}
}

// okResolver resolves inline bodies verbatim.
var okResolver = mapResolver{}

// TestManagedSubjects covers all four subject strategies, value-only/key-only/both,
// governance (nil, nil) → nil result, and RecordName with unresolvable body → error.
// This is the direct unit table for fix 1 + fix 2 (spec §12).
func TestManagedSubjects(t *testing.T) {
	const topic = "payments.orders"

	cases := []struct {
		name      string
		strategy  string
		value     *v1alpha1.ValueFrom
		key       *v1alpha1.ValueFrom
		resolver  mapResolver
		wantSubjs []string
		wantErr   bool
	}{
		// TopicName / "" (default) — bodyless, deterministic ---------
		{
			name:      "TopicName value-only",
			strategy:  "TopicName",
			value:     inline(avroValueDef),
			wantSubjs: []string{topic + "-value"},
		},
		{
			name:      "TopicName key-only",
			strategy:  "TopicName",
			key:       inline(avroValueDef),
			wantSubjs: []string{topic + "-key"},
		},
		{
			name:      "TopicName both value+key",
			strategy:  "TopicName",
			value:     inline(avroValueDef),
			key:       inline(avroValueDef),
			wantSubjs: []string{topic + "-value", topic + "-key"},
		},
		{
			name:     "TopicName (default strategy) value-only, Secret deleted — no error",
			strategy: "",
			value:    inline(avroValueDef), // ValueSchema is set...
			resolver: errResolver("secret deleted"),
			// Body resolution must NOT be attempted for TopicName — subject is deterministic.
			wantSubjs: []string{topic + "-value"},
		},
		// Custom — bodyless, deterministic ---------------------------
		{
			name:      "Custom value-only",
			strategy:  "Custom",
			value:     inline(avroValueDef),
			wantSubjs: []string{topic + "-value-custom"},
		},
		{
			name:      "Custom key-only",
			strategy:  "Custom",
			key:       inline(avroValueDef),
			wantSubjs: []string{topic + "-key-custom"},
		},
		{
			name:      "Custom both value+key",
			strategy:  "Custom",
			value:     inline(avroValueDef),
			key:       inline(avroValueDef),
			wantSubjs: []string{topic + "-value-custom", topic + "-key-custom"},
		},
		{
			name:     "Custom value-only, Secret deleted — no error",
			strategy: "Custom",
			value:    inline(avroValueDef),
			resolver: errResolver("secret deleted"),
			// Custom subjects are explicit strings; body resolution must NOT be attempted.
			wantSubjs: []string{topic + "-value-custom"},
		},
		// Governance — no valueSchema, no keySchema ------------------
		{
			name:      "governance mode (no schemas) → nil, nil",
			strategy:  "TopicName",
			value:     nil,
			key:       nil,
			wantSubjs: nil,
		},
		// RecordName — requires body resolution -----------------------
		{
			name:      "RecordName value-only",
			strategy:  "RecordName",
			value:     inline(avroValueDef),
			wantSubjs: []string{"com.example.OrderEvent"},
		},
		{
			name:      "RecordName both value+key",
			strategy:  "RecordName",
			value:     inline(avroValueDef),
			key:       inline(`{"type":"record","name":"OrderKey","namespace":"com.example","fields":[]}`),
			wantSubjs: []string{"com.example.OrderEvent", "com.example.OrderKey"},
		},
		{
			name:     "RecordName value-only, body unresolvable → error",
			strategy: "RecordName",
			value:    inline(avroValueDef),
			resolver: errResolver("secret deleted"),
			wantErr:  true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tp := topicForManagedSubjects(tc.strategy, topic, tc.value, tc.key)

			// Use tc.resolver when explicitly set; fall back to okResolver so that
			// cases that supply a body map but no explicit resolver still resolve.
			// Previously the code checked tc.resolver.err != nil, which ignored a
			// case-supplied body map — this form prefers tc.resolver whenever it is
			// non-zero (has a body map or an err).
			r := okResolver
			if tc.resolver.err != nil || len(tc.resolver.bodies) > 0 {
				r = tc.resolver
			}

			got, err := ManagedSubjects(tp, r)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got subjects %v", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, tc.wantSubjs) {
				t.Fatalf("ManagedSubjects = %v, want %v", got, tc.wantSubjs)
			}
		})
	}
}

// TestReconcileTopicHonorsDriftIgnoreFields pins spec §16 in the operator: a
// live config value diverging ONLY on an ignored key is not drift — the topic
// reconciles clean with no mutation.
func TestReconcileTopicHonorsDriftIgnoreFields(t *testing.T) {
	live := []kafka.TopicState{{Name: "payments.orders", Partitions: 3,
		Config: map[string]string{"retention.ms": "1000"}}} // desired: 604800000
	k := kafkamock.New(live, nil)
	tp := baseTopic("Enforce")
	tp.Spec.Drift = &v1alpha1.DriftConfig{IgnoreFields: []string{"config.retention.ms"}}

	st, err := ReconcileTopic(context.Background(), tp, cluster(), k, nil, stubResolver{}, nil, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(k.Calls()) != 0 {
		t.Fatalf("ignored drift must not mutate, got calls %v", k.Calls())
	}
	if st.Phase != v1alpha1.PhaseReady {
		t.Fatalf("phase = %q, want Ready", st.Phase)
	}
	if st.Drift == nil || st.Drift.Detected {
		t.Fatalf("drift = %+v, want detected=false", st.Drift)
	}
}

// ---- Tenancy enforcement tests (spec §20.2) ----

// tenancyCluster wraps cluster() and attaches a TenancyConfig.
func tenancyCluster(t *v1alpha1.TenancyConfig) *v1alpha1.KafkaCluster {
	cl := cluster()
	cl.Spec.Tenancy = t
	return cl
}

// TestReconcileTopicTenancyDeniedNamespace: namespace not in AllowedNamespaces
// → TenancyDenied terminal failure, zero mutating calls.
func TestReconcileTopicTenancyDeniedNamespace(t *testing.T) {
	k := kafkamock.New(nil, nil)
	tp := baseTopic("Enforce")
	tp.Namespace = "team-b" // not in allow-list

	cl := tenancyCluster(&v1alpha1.TenancyConfig{
		AllowedNamespaces: []string{"team-a"},
	})

	st, err := ReconcileTopic(context.Background(), tp, cl, k, nil, stubResolver{}, nil, nil, nil)
	if err != nil {
		t.Fatalf("tenancy denial is terminal, want nil error, got: %v", err)
	}
	if got := k.Calls(); len(got) != 0 {
		t.Fatalf("tenancy denial must not mutate: calls = %v", got)
	}
	if st.Phase != v1alpha1.PhaseError {
		t.Fatalf("phase = %q, want Error on tenancy denial", st.Phase)
	}
	s, reason, ok := condStatus(st.Conditions, v1alpha1.CondValidationFailed)
	if !ok || s != metav1.ConditionTrue {
		t.Fatalf("ValidationFailed = %v ok=%v, want True", s, ok)
	}
	if reason != reasonTenancyDenied {
		t.Fatalf("reason = %q, want %q", reason, reasonTenancyDenied)
	}
	if s, _, _ := condStatus(st.Conditions, v1alpha1.CondReady); s != metav1.ConditionFalse {
		t.Fatalf("Ready = %v, want False", s)
	}
}

// TestReconcileTopicTenancyDeniedNamespaceIncrementsTerminalCounter reuses the
// TenancyDeniedNamespace fixture above to pin v0.36 Task 7's
// monedula_reconcile_terminal_total{kind="kafkatopic", reason="TenancyDenied"}
// counter: a terminal reconcile must bump it, and a second terminal reconcile
// must accumulate rather than overwrite.
func TestReconcileTopicTenancyDeniedNamespaceIncrementsTerminalCounter(t *testing.T) {
	k := kafkamock.New(nil, nil)
	tp := baseTopic("Enforce")
	tp.Namespace = "team-b" // not in allow-list
	cl := tenancyCluster(&v1alpha1.TenancyConfig{AllowedNamespaces: []string{"team-a"}})

	before := operator.ReconcileTerminalCount("kafkatopic", reasonTenancyDenied)

	if _, err := ReconcileTopic(context.Background(), tp, cl, k, nil, stubResolver{}, nil, nil, nil); err != nil {
		t.Fatalf("tenancy denial is terminal, want nil error, got: %v", err)
	}
	after := operator.ReconcileTerminalCount("kafkatopic", reasonTenancyDenied)
	if after != before+1 {
		t.Fatalf("terminal counter = %v, want %v (before %v + 1)", after, before+1, before)
	}

	// A second terminal reconcile of the same kind+reason accumulates.
	if _, err := ReconcileTopic(context.Background(), tp, cl, k, nil, stubResolver{}, nil, nil, nil); err != nil {
		t.Fatalf("tenancy denial is terminal, want nil error, got: %v", err)
	}
	if got := operator.ReconcileTerminalCount("kafkatopic", reasonTenancyDenied); got != before+2 {
		t.Fatalf("terminal counter after second denial = %v, want %v", got, before+2)
	}
}

// TestReconcileTopicTenancyDeniedPrefix: namespace is allowed but topic name
// doesn't start with the required prefix → TenancyDenied, zero mutating calls.
func TestReconcileTopicTenancyDeniedPrefix(t *testing.T) {
	k := kafkamock.New(nil, nil)
	tp := baseTopic("Enforce")
	tp.Namespace = "team-a"
	// baseTopic uses topicName "payments.orders"; rule requires "finance." prefix.

	cl := tenancyCluster(&v1alpha1.TenancyConfig{
		AllowedNamespaces: []string{"team-a"},
		TopicPrefixes: []v1alpha1.TopicPrefixRule{
			{Namespaces: []string{"team-a"}, Prefixes: []string{"finance."}},
		},
	})

	st, err := ReconcileTopic(context.Background(), tp, cl, k, nil, stubResolver{}, nil, nil, nil)
	if err != nil {
		t.Fatalf("tenancy denial is terminal, want nil error, got: %v", err)
	}
	if got := k.Calls(); len(got) != 0 {
		t.Fatalf("tenancy denial must not mutate: calls = %v", got)
	}
	if st.Phase != v1alpha1.PhaseError {
		t.Fatalf("phase = %q, want Error on prefix denial", st.Phase)
	}
	s, reason, ok := condStatus(st.Conditions, v1alpha1.CondValidationFailed)
	if !ok || s != metav1.ConditionTrue {
		t.Fatalf("ValidationFailed = %v ok=%v, want True", s, ok)
	}
	if reason != reasonTenancyDenied {
		t.Fatalf("reason = %q, want %q", reason, reasonTenancyDenied)
	}
	if s, _, _ := condStatus(st.Conditions, v1alpha1.CondReady); s != metav1.ConditionFalse {
		t.Fatalf("Ready = %v, want False", s)
	}
}

// TestReconcileTopicTenancyAllowed: namespace is in AllowedNamespaces and
// topic name satisfies the prefix rule → reconcile proceeds and Kafka is called.
func TestReconcileTopicTenancyAllowed(t *testing.T) {
	k := kafkamock.New(nil, nil)
	tp := baseTopic("Enforce")
	tp.Namespace = "team-a"
	// topicName "payments.orders" satisfies the "payments." prefix.

	cl := tenancyCluster(&v1alpha1.TenancyConfig{
		AllowedNamespaces: []string{"team-a"},
		TopicPrefixes: []v1alpha1.TopicPrefixRule{
			{Namespaces: []string{"team-a"}, Prefixes: []string{"payments."}},
		},
	})

	st, err := ReconcileTopic(context.Background(), tp, cl, k, nil, stubResolver{}, nil, nil, nil)
	if err != nil {
		t.Fatalf("tenancy allowed: want nil error, got: %v", err)
	}
	if st.Phase != v1alpha1.PhaseReady {
		t.Fatalf("phase = %q, want Ready", st.Phase)
	}
	if got := k.Calls(); len(got) == 0 {
		t.Fatalf("tenancy allowed: expected Kafka calls, got none")
	}
}

// TestReconcilePolicyTenancyDeniedPrefixedTopicResource: policy has a prefixed
// topic resource whose name is outside the allowed prefix → TenancyDenied, zero
// mutating calls.
func TestReconcilePolicyTenancyDeniedPrefixedTopicResource(t *testing.T) {
	k := kafkamock.New(nil, nil)
	pol := basePolicy("Enforce")
	pol.Namespace = "team-a"
	// Rule resource: topic "infra." (prefixed), outside allowed "payments." prefix.
	pol.Spec.Rules = []v1alpha1.ACLRule{
		{
			Principal:  "User:svc",
			Permission: "Allow",
			Operations: []string{"Read"},
			Resource:   v1alpha1.ACLResource{Type: "topic", Name: "infra.", PatternType: "prefixed"},
		},
	}

	cl := tenancyCluster(&v1alpha1.TenancyConfig{
		AllowedNamespaces: []string{"team-*"},
		TopicPrefixes: []v1alpha1.TopicPrefixRule{
			{Namespaces: []string{"team-a"}, Prefixes: []string{"payments."}},
		},
	})

	st, err := ReconcilePolicy(context.Background(), pol, cl, k, nil)
	if err != nil {
		t.Fatalf("tenancy denial is terminal, want nil error, got: %v", err)
	}
	if got := k.Calls(); len(got) != 0 {
		t.Fatalf("tenancy denial must not mutate: calls = %v", got)
	}
	if st.Phase != v1alpha1.PhaseError {
		t.Fatalf("phase = %q, want Error", st.Phase)
	}
	s, reason, ok := condStatus(st.Conditions, v1alpha1.CondValidationFailed)
	if !ok || s != metav1.ConditionTrue {
		t.Fatalf("ValidationFailed = %v ok=%v, want True", s, ok)
	}
	if reason != reasonTenancyDenied {
		t.Fatalf("reason = %q, want %q", reason, reasonTenancyDenied)
	}
	if s, _, _ := condStatus(st.Conditions, v1alpha1.CondReady); s != metav1.ConditionFalse {
		t.Fatalf("Ready = %v, want False", s)
	}
}

// TestReconcilePolicyTenancyDeniedNamespace: policy in a namespace outside
// allowedNamespaces → terminal TenancyDenied (ValidationFailed=True reason
// TenancyDenied, Ready=False, Phase Error, zero mutating calls).
func TestReconcilePolicyTenancyDeniedNamespace(t *testing.T) {
	k := kafkamock.New(nil, nil)
	pol := basePolicy("Enforce")
	pol.Namespace = "team-b" // not in allow-list

	cl := tenancyCluster(&v1alpha1.TenancyConfig{
		AllowedNamespaces: []string{"team-a"},
	})

	st, err := ReconcilePolicy(context.Background(), pol, cl, k, nil)
	if err != nil {
		t.Fatalf("tenancy denial is terminal, want nil error, got: %v", err)
	}
	if got := k.Calls(); len(got) != 0 {
		t.Fatalf("tenancy denial must not mutate: calls = %v", got)
	}
	if st.Phase != v1alpha1.PhaseError {
		t.Fatalf("phase = %q, want Error on tenancy denial", st.Phase)
	}
	s, reason, ok := condStatus(st.Conditions, v1alpha1.CondValidationFailed)
	if !ok || s != metav1.ConditionTrue {
		t.Fatalf("ValidationFailed = %v ok=%v, want True", s, ok)
	}
	if reason != reasonTenancyDenied {
		t.Fatalf("reason = %q, want %q", reason, reasonTenancyDenied)
	}
	if s, _, _ := condStatus(st.Conditions, v1alpha1.CondReady); s != metav1.ConditionFalse {
		t.Fatalf("Ready = %v, want False", s)
	}
}

// TestReconcilePolicyTenancyDeniedGroupResource: groups reuse topic prefixes —
// a group rule whose name is outside the namespace's allowed prefixes is a
// terminal TenancyDenied (previously groups were unchecked; this closed a
// cross-team group-access hole).
func TestReconcilePolicyTenancyDeniedGroupResource(t *testing.T) {
	k := kafkamock.New(nil, nil)
	pol := basePolicy("Enforce")
	pol.Namespace = "team-a"
	pol.Spec.Rules = []v1alpha1.ACLRule{
		{
			Principal:  "User:svc",
			Permission: "Allow",
			Operations: []string{"Read"},
			Resource:   v1alpha1.ACLResource{Type: "group", Name: "infra.consumer-group"},
		},
	}

	cl := tenancyCluster(&v1alpha1.TenancyConfig{
		AllowedNamespaces: []string{"team-*"},
		TopicPrefixes: []v1alpha1.TopicPrefixRule{
			{Namespaces: []string{"team-a"}, Prefixes: []string{"payments."}},
		},
	})

	st, err := ReconcilePolicy(context.Background(), pol, cl, k, nil)
	if err != nil {
		t.Fatalf("tenancy denial is terminal, want nil error, got: %v", err)
	}
	if got := k.Calls(); len(got) != 0 {
		t.Fatalf("tenancy denial must not mutate: calls = %v", got)
	}
	if st.Phase != v1alpha1.PhaseError {
		t.Fatalf("phase = %q, want Error", st.Phase)
	}
	if _, reason, _ := condStatus(st.Conditions, v1alpha1.CondValidationFailed); reason != reasonTenancyDenied {
		t.Fatalf("reason = %q, want %q", reason, reasonTenancyDenied)
	}
}

// TestReconcilePolicyTenancyAllowedGroupResource: a group rule whose name is
// inside the namespace's allowed prefixes is allowed (groups reuse topic
// prefixes).
func TestReconcilePolicyTenancyAllowedGroupResource(t *testing.T) {
	k := kafkamock.New(nil, nil)
	pol := basePolicy("Enforce")
	pol.Namespace = "team-a"
	pol.Spec.Rules = []v1alpha1.ACLRule{
		{
			Principal:  "User:svc",
			Permission: "Allow",
			Operations: []string{"Read"},
			Resource:   v1alpha1.ACLResource{Type: "group", Name: "payments.consumer-group"},
		},
	}

	cl := tenancyCluster(&v1alpha1.TenancyConfig{
		AllowedNamespaces: []string{"team-*"},
		TopicPrefixes: []v1alpha1.TopicPrefixRule{
			{Namespaces: []string{"team-a"}, Prefixes: []string{"payments."}},
		},
	})

	st, err := ReconcilePolicy(context.Background(), pol, cl, k, nil)
	if err != nil {
		t.Fatalf("prefix-matching group resource: want nil error, got: %v", err)
	}
	if st.Phase != v1alpha1.PhaseReady {
		t.Fatalf("phase = %q, want Ready (group inside allowed prefix)", st.Phase)
	}
}

// TestReconcilePolicyTenancyDeniedClusterResource: a prefix-restricted
// namespace may not declare rules on unscopeable resource types — a cluster
// rule (e.g. Alter on the cluster = the power to create arbitrary ACLs) is a
// terminal TenancyDenied.
func TestReconcilePolicyTenancyDeniedClusterResource(t *testing.T) {
	k := kafkamock.New(nil, nil)
	pol := basePolicy("Enforce")
	pol.Namespace = "team-a"
	pol.Spec.Rules = []v1alpha1.ACLRule{
		{
			Principal:  "User:svc",
			Permission: "Allow",
			Operations: []string{"Alter"},
			Resource:   v1alpha1.ACLResource{Type: "cluster", Name: "kafka-cluster"},
		},
	}

	cl := tenancyCluster(&v1alpha1.TenancyConfig{
		AllowedNamespaces: []string{"team-*"},
		TopicPrefixes: []v1alpha1.TopicPrefixRule{
			{Namespaces: []string{"team-a"}, Prefixes: []string{"payments."}},
		},
	})

	st, err := ReconcilePolicy(context.Background(), pol, cl, k, nil)
	if err != nil {
		t.Fatalf("tenancy denial is terminal, want nil error, got: %v", err)
	}
	if got := k.Calls(); len(got) != 0 {
		t.Fatalf("tenancy denial must not mutate: calls = %v", got)
	}
	if st.Phase != v1alpha1.PhaseError {
		t.Fatalf("phase = %q, want Error", st.Phase)
	}
	if _, reason, _ := condStatus(st.Conditions, v1alpha1.CondValidationFailed); reason != reasonTenancyDenied {
		t.Fatalf("reason = %q, want %q", reason, reasonTenancyDenied)
	}
}

// TestReconcileTopicTenancyDeniedConsumerGroup: consumer group names in the
// access block are prefix-checked like topics (they produce group ACLs and/or
// group role bindings) → terminal TenancyDenied, zero mutating calls.
func TestReconcileTopicTenancyDeniedConsumerGroup(t *testing.T) {
	k := kafkamock.New(nil, nil)
	tp := baseTopic("Enforce")
	tp.Namespace = "team-a"
	// topicName "payments.orders" is fine; the GROUP is outside the prefix.
	tp.Spec.Access = v1alpha1.TopicAccess{
		Consumers: []v1alpha1.ConsumerAccess{
			{Principal: "User:svc", Group: "infra.cg"},
		},
	}

	cl := tenancyCluster(&v1alpha1.TenancyConfig{
		AllowedNamespaces: []string{"team-a"},
		TopicPrefixes: []v1alpha1.TopicPrefixRule{
			{Namespaces: []string{"team-a"}, Prefixes: []string{"payments."}},
		},
	})

	st, err := ReconcileTopic(context.Background(), tp, cl, k, nil, stubResolver{}, nil, nil, nil)
	if err != nil {
		t.Fatalf("tenancy denial is terminal, want nil error, got: %v", err)
	}
	if got := k.Calls(); len(got) != 0 {
		t.Fatalf("tenancy denial must not mutate: calls = %v", got)
	}
	if st.Phase != v1alpha1.PhaseError {
		t.Fatalf("phase = %q, want Error", st.Phase)
	}
	if _, reason, _ := condStatus(st.Conditions, v1alpha1.CondValidationFailed); reason != reasonTenancyDenied {
		t.Fatalf("reason = %q, want %q", reason, reasonTenancyDenied)
	}
}

// TestReconcileTopicTenancyAllowedConsumerGroup: a consumer group inside the
// namespace's allowed prefixes reconciles normally.
func TestReconcileTopicTenancyAllowedConsumerGroup(t *testing.T) {
	k := kafkamock.New(nil, nil)
	tp := baseTopic("Enforce")
	tp.Namespace = "team-a"
	tp.Spec.Access = v1alpha1.TopicAccess{
		Consumers: []v1alpha1.ConsumerAccess{
			{Principal: "User:svc", Group: "payments.cg"},
		},
	}

	cl := tenancyCluster(&v1alpha1.TenancyConfig{
		AllowedNamespaces: []string{"team-a"},
		TopicPrefixes: []v1alpha1.TopicPrefixRule{
			{Namespaces: []string{"team-a"}, Prefixes: []string{"payments."}},
		},
	})

	st, err := ReconcileTopic(context.Background(), tp, cl, k, nil, stubResolver{}, nil, nil, nil)
	if err != nil {
		t.Fatalf("tenancy allowed: want nil error, got: %v", err)
	}
	if st.Phase != v1alpha1.PhaseReady {
		t.Fatalf("phase = %q, want Ready", st.Phase)
	}
}

// TestReconcileQuotaTenancyDeniedNamespace: the namespace allow-list applies
// to quotas (previously unchecked — a cross-team quota-DoS hole): a KafkaQuota
// in a disallowed namespace is a terminal TenancyDenied with zero Kafka calls.
func TestReconcileQuotaTenancyDeniedNamespace(t *testing.T) {
	k := kafkamock.New(nil, nil)
	q := &v1alpha1.KafkaQuota{
		Spec: v1alpha1.KafkaQuotaSpec{
			ClusterRef: v1alpha1.ClusterRef{Name: "c"},
			Entity:     v1alpha1.QuotaEntity{User: "User:alice"},
			Limits:     v1alpha1.QuotaLimits{ProducerByteRate: floatPtr(1048576)},
		},
	}
	q.Name = "q"
	q.Namespace = "team-b" // not in allow-list
	q.Generation = 2

	cl := tenancyCluster(&v1alpha1.TenancyConfig{
		AllowedNamespaces: []string{"team-a"},
	})

	st, err := ReconcileQuota(context.Background(), q, cl, k)
	if err != nil {
		t.Fatalf("tenancy denial is terminal, want nil error, got: %v", err)
	}
	if got := k.Calls(); len(got) != 0 {
		t.Fatalf("tenancy denial must not mutate: calls = %v", got)
	}
	if st.Phase != v1alpha1.PhaseError {
		t.Fatalf("phase = %q, want Error", st.Phase)
	}
	s, reason, ok := condStatus(st.Conditions, v1alpha1.CondValidationFailed)
	if !ok || s != metav1.ConditionTrue {
		t.Fatalf("ValidationFailed = %v ok=%v, want True", s, ok)
	}
	if reason != reasonTenancyDenied {
		t.Fatalf("reason = %q, want %q", reason, reasonTenancyDenied)
	}
	if s, _, _ := condStatus(st.Conditions, v1alpha1.CondReady); s != metav1.ConditionFalse {
		t.Fatalf("Ready = %v, want False", s)
	}
}

// TestReconcileQuotaTenancyAllowedPrefixRestrictedNamespace: prefix rules do
// NOT block quotas (entities cannot be prefix-scoped — documented limitation);
// only the allow-list applies.
func TestReconcileQuotaTenancyAllowedPrefixRestrictedNamespace(t *testing.T) {
	k := kafkamock.New(nil, nil)
	q := &v1alpha1.KafkaQuota{
		Spec: v1alpha1.KafkaQuotaSpec{
			ClusterRef: v1alpha1.ClusterRef{Name: "c"},
			Entity:     v1alpha1.QuotaEntity{User: "User:alice"},
			Limits:     v1alpha1.QuotaLimits{ProducerByteRate: floatPtr(1048576)},
		},
	}
	q.Name = "q"
	q.Namespace = "team-a"
	q.Generation = 2

	cl := tenancyCluster(&v1alpha1.TenancyConfig{
		AllowedNamespaces: []string{"team-a"},
		TopicPrefixes: []v1alpha1.TopicPrefixRule{
			{Namespaces: []string{"team-a"}, Prefixes: []string{"payments."}},
		},
	})

	st, err := ReconcileQuota(context.Background(), q, cl, k)
	if err != nil {
		t.Fatalf("tenancy allowed: want nil error, got: %v", err)
	}
	if st.Phase != v1alpha1.PhaseReady {
		t.Fatalf("phase = %q, want Ready", st.Phase)
	}
}

// TestReconcileQuotaRemoveGatedByAnnotation pins the RemoveQuota gate operator-
// side (spec §17.1): removing a live limit key (the entity carries a key absent
// from spec.limits) is GateDestructive. Without the allow-destructive
// annotation the op is Blocked (no mutation, terminal Error phase); with it the
// key is deleted.
func TestReconcileQuotaRemoveGatedByAnnotation(t *testing.T) {
	strPtr := func(s string) *string { return &s }
	liveQuotas := func() []kafka.QuotaState {
		return []kafka.QuotaState{{
			Entity: []kafka.QuotaEntityComponent{{Type: "user", Name: strPtr("alice")}},
			Limits: map[string]float64{"producer_byte_rate": 1048576, "consumer_byte_rate": 2048},
		}}
	}
	mkQuota := func() *v1alpha1.KafkaQuota {
		q := &v1alpha1.KafkaQuota{
			Spec: v1alpha1.KafkaQuotaSpec{
				ClusterRef: v1alpha1.ClusterRef{Name: "c"},
				Entity:     v1alpha1.QuotaEntity{User: "User:alice"},
				// consumer_byte_rate is live but NOT desired -> RemoveQuota.
				Limits: v1alpha1.QuotaLimits{ProducerByteRate: floatPtr(1048576)},
			},
		}
		q.Name = "q"
		q.Namespace = "team-a"
		q.Generation = 2
		return q
	}

	// Without annotation: Blocked, live limit untouched.
	k := kafkamock.NewWithQuotas(nil, nil, liveQuotas())
	st, err := ReconcileQuota(context.Background(), mkQuota(), cluster(), k)
	if err != nil {
		t.Fatalf("blocked op is terminal, want nil error, got: %v", err)
	}
	for _, c := range k.Calls() {
		if strings.HasPrefix(c, "DeleteQuota") {
			t.Fatalf("blocked RemoveQuota must not mutate: calls = %v", k.Calls())
		}
	}
	if st.Phase != v1alpha1.PhaseError {
		t.Fatalf("phase = %q, want Error when op blocked", st.Phase)
	}
	if st.ObservedLimits == nil || st.ObservedLimits.ConsumerByteRate == nil {
		t.Fatalf("observedLimits = %+v, want live consumer_byte_rate still present", st.ObservedLimits)
	}

	// With the allow-destructive annotation: the key is removed.
	k2 := kafkamock.NewWithQuotas(nil, nil, liveQuotas())
	q2 := mkQuota()
	q2.Annotations = map[string]string{"gitops.monedula.dev/allow-destructive": "true"}
	st2, err2 := ReconcileQuota(context.Background(), q2, cluster(), k2)
	if err2 != nil {
		t.Fatalf("approved apply, want nil error, got: %v", err2)
	}
	if got := k2.Calls(); len(got) != 1 || got[0] != "DeleteQuota user=alice" {
		t.Fatalf("calls = %v, want [DeleteQuota user=alice]", got)
	}
	if st2.Phase != v1alpha1.PhaseReady {
		t.Fatalf("phase = %q, want Ready after approved apply", st2.Phase)
	}
	if st2.ObservedLimits == nil || st2.ObservedLimits.ConsumerByteRate != nil {
		t.Fatalf("observedLimits = %+v, want consumer_byte_rate removed", st2.ObservedLimits)
	}
}

// ---- ClusterACLView conflict-surfacing tests (spec §9) ----

// topicWithAllowWrite builds a KafkaTopic granting Allow Write on topicName for
// the given principal, with the given metadata name and namespace.
func topicWithAllowWrite(metaName, ns, topicName, principal string) *v1alpha1.KafkaTopic {
	tp := &v1alpha1.KafkaTopic{
		Spec: v1alpha1.KafkaTopicSpec{
			ClusterRef: v1alpha1.ClusterRef{Name: "c"},
			TopicName:  topicName,
			Partitions: 1,
			Access: v1alpha1.TopicAccess{
				Producers: []v1alpha1.ProducerAccess{
					{Principal: principal, Operations: []string{"Write"}},
				},
			},
		},
	}
	tp.Name = metaName
	tp.Namespace = ns
	// Explicit reconciliation mode (required by validation/defaulting).
	tp.Spec.Reconciliation = &v1alpha1.Reconciliation{Mode: "Enforce"}
	return tp
}

// policyWithDenyWrite builds a KafkaAccessPolicy granting Deny Write on topicName
// for the given principal, with the given metadata name and namespace.
func policyWithDenyWrite(metaName, ns, topicName, principal string) *v1alpha1.KafkaAccessPolicy {
	pol := &v1alpha1.KafkaAccessPolicy{
		Spec: v1alpha1.KafkaAccessPolicySpec{
			ClusterRef: v1alpha1.ClusterRef{Name: "c"},
			Rules: []v1alpha1.ACLRule{
				{
					Principal:  principal,
					Permission: "Deny",
					Operations: []string{"Write"},
					Resource:   v1alpha1.ACLResource{Type: "topic", Name: topicName},
				},
			},
		},
	}
	pol.Name = metaName
	pol.Namespace = ns
	pol.Spec.Reconciliation = &v1alpha1.Reconciliation{Mode: "Enforce"}
	return pol
}

// TestBuildClusterACLViewConflictSurfaced verifies that BuildClusterACLView
// surfaces a cross-resource Allow/Deny conflict when a KafkaTopic grants Allow
// Write and a KafkaAccessPolicy grants Deny Write on the same tuple.
func TestBuildClusterACLViewConflictSurfaced(t *testing.T) {
	tp := topicWithAllowWrite("topic-t1", "ns1", "t", "User:a")
	pol := policyWithDenyWrite("policy-p1", "ns1", "t", "User:a")

	view := BuildClusterACLView([]*v1alpha1.KafkaTopic{tp}, []*v1alpha1.KafkaAccessPolicy{pol}, nil, nil)

	if len(view.Conflicts) != 1 {
		t.Fatalf("Conflicts = %d, want exactly 1; got %+v", len(view.Conflicts), view.Conflicts)
	}

	c := view.Conflicts[0]
	// The two SourceNames across A and B must be the topic and policy names.
	names := map[string]bool{c.A.SourceName: true, c.B.SourceName: true}
	if !names["topic-t1"] {
		t.Errorf("expected SourceName %q in conflict parties, got A=%q B=%q", "topic-t1", c.A.SourceName, c.B.SourceName)
	}
	if !names["policy-p1"] {
		t.Errorf("expected SourceName %q in conflict parties, got A=%q B=%q", "policy-p1", c.A.SourceName, c.B.SourceName)
	}
	// Verify SourceKinds are attributed correctly.
	kinds := map[string]bool{c.A.SourceKind: true, c.B.SourceKind: true}
	if !kinds["KafkaTopic"] {
		t.Errorf("expected SourceKind %q in conflict parties", "KafkaTopic")
	}
	if !kinds["KafkaAccessPolicy"] {
		t.Errorf("expected SourceKind %q in conflict parties", "KafkaAccessPolicy")
	}
}

// TestBuildClusterACLViewNoConflictWhenAgree verifies that no conflict is
// reported when two resources agree on the same permission (both Allow).
func TestBuildClusterACLViewNoConflictWhenAgree(t *testing.T) {
	tp := topicWithAllowWrite("topic-t1", "ns1", "t", "User:a")
	// Policy also grants Allow Write on the same tuple.
	pol := &v1alpha1.KafkaAccessPolicy{
		Spec: v1alpha1.KafkaAccessPolicySpec{
			ClusterRef: v1alpha1.ClusterRef{Name: "c"},
			Rules: []v1alpha1.ACLRule{
				{
					Principal:  "User:a",
					Permission: "Allow",
					Operations: []string{"Write"},
					Resource:   v1alpha1.ACLResource{Type: "topic", Name: "t"},
				},
			},
		},
	}
	pol.Name = "policy-p1"
	pol.Namespace = "ns1"
	pol.Spec.Reconciliation = &v1alpha1.Reconciliation{Mode: "Enforce"}

	view := BuildClusterACLView([]*v1alpha1.KafkaTopic{tp}, []*v1alpha1.KafkaAccessPolicy{pol}, nil, nil)

	if len(view.Conflicts) != 0 {
		t.Fatalf("Conflicts = %d, want 0 when permissions agree; got %+v", len(view.Conflicts), view.Conflicts)
	}
}

// TestBuildClusterACLViewConflictOrderIndependent verifies that the sort inside
// BuildClusterACLView makes the Conflicts slice IDENTICAL regardless of the
// order in which topics and policies are passed — i.e. it is the SORT (not
// merely trivial equality) that makes the output converge.
//
// Two conflicting pairs are used so that reversing the input slices genuinely
// changes the order of ACLs before the sort runs, ensuring a sort-free
// implementation would produce a different output and the test would catch it.
func TestBuildClusterACLViewConflictOrderIndependent(t *testing.T) {
	// conflictTuple is the comparable form used in assertions.
	type conflictTuple struct {
		AKind, AName, BKind, BName, Subject string
	}
	toTuples := func(cs []access.Conflict) []conflictTuple {
		out := make([]conflictTuple, len(cs))
		for i, c := range cs {
			out[i] = conflictTuple{
				AKind: c.A.SourceKind, AName: c.A.SourceName,
				BKind: c.B.SourceKind, BName: c.B.SourceName,
				Subject: c.Subject,
			}
		}
		return out
	}

	// Call 1: topics and policies in FORWARD order [aaa, zzz] / [deny-aaa, deny-zzz].
	// BuildClusterACLView mutates inputs via defaulting, so construct fresh objects
	// for each call.
	view1 := BuildClusterACLView(
		[]*v1alpha1.KafkaTopic{
			topicWithAllowWrite("topic-aaa", "ns1", "aaa", "User:alice"),
			topicWithAllowWrite("topic-zzz", "ns1", "zzz", "User:alice"),
		},
		[]*v1alpha1.KafkaAccessPolicy{
			policyWithDenyWrite("deny-aaa", "ns1", "aaa", "User:alice"),
			policyWithDenyWrite("deny-zzz", "ns1", "zzz", "User:alice"),
		},
		nil,
		nil,
	)

	// Call 2: REVERSED within-slice order [zzz, aaa] / [deny-zzz, deny-aaa].
	view2 := BuildClusterACLView(
		[]*v1alpha1.KafkaTopic{
			topicWithAllowWrite("topic-zzz", "ns1", "zzz", "User:alice"),
			topicWithAllowWrite("topic-aaa", "ns1", "aaa", "User:alice"),
		},
		[]*v1alpha1.KafkaAccessPolicy{
			policyWithDenyWrite("deny-zzz", "ns1", "zzz", "User:alice"),
			policyWithDenyWrite("deny-aaa", "ns1", "aaa", "User:alice"),
		},
		nil,
		nil,
	)

	tuples1 := toTuples(view1.Conflicts)
	tuples2 := toTuples(view2.Conflicts)

	if len(tuples1) != 2 {
		t.Fatalf("view1: want 2 conflicts (one per pair), got %d: %+v", len(tuples1), tuples1)
	}
	if !reflect.DeepEqual(tuples1, tuples2) {
		t.Fatalf("Conflicts differ between forward and reversed input order:\n  forward:  %+v\n  reversed: %+v", tuples1, tuples2)
	}
}

// TestBuildClusterACLViewConflictDeterminism verifies that two identical calls
// to BuildClusterACLView produce the same Conflicts slice (same length, same A/B
// SourceNames in the same order).
func TestBuildClusterACLViewConflictDeterminism(t *testing.T) {
	tp := topicWithAllowWrite("topic-t1", "ns1", "t", "User:a")
	pol := policyWithDenyWrite("policy-p1", "ns1", "t", "User:a")

	view1 := BuildClusterACLView([]*v1alpha1.KafkaTopic{tp}, []*v1alpha1.KafkaAccessPolicy{pol}, nil, nil)
	// Re-build inputs identically (BuildClusterACLView calls defaulting in place;
	// reconstruct fresh objects so each call starts from the same spec).
	tp2 := topicWithAllowWrite("topic-t1", "ns1", "t", "User:a")
	pol2 := policyWithDenyWrite("policy-p1", "ns1", "t", "User:a")
	view2 := BuildClusterACLView([]*v1alpha1.KafkaTopic{tp2}, []*v1alpha1.KafkaAccessPolicy{pol2}, nil, nil)

	if len(view1.Conflicts) != len(view2.Conflicts) {
		t.Fatalf("Conflicts length differs: %d vs %d", len(view1.Conflicts), len(view2.Conflicts))
	}
	for i := range view1.Conflicts {
		c1, c2 := view1.Conflicts[i], view2.Conflicts[i]
		if c1.A.SourceName != c2.A.SourceName || c1.B.SourceName != c2.B.SourceName {
			t.Fatalf("Conflicts[%d] not deterministic: first call A=%q B=%q, second A=%q B=%q",
				i, c1.A.SourceName, c1.B.SourceName, c2.A.SourceName, c2.B.SourceName)
		}
	}
}

// ---- ACLConflict condition tests (spec §21) ----

// TestSetACLConflictConditionNilView verifies that a nil view causes the
// ACLConflict condition to be REMOVED (single-resource fallback path).
func TestSetACLConflictConditionNilView(t *testing.T) {
	// Seed a stale ACLConflict=True from a prior reconcile.
	conds := []metav1.Condition{
		{Type: v1alpha1.CondACLConflict, Status: metav1.ConditionTrue, Reason: "CrossResourceConflict", Message: "stale"},
	}
	setACLConflictCondition(&conds, nil, "KafkaTopic", "ns1", "topic-t1", 1)

	if c := k8smeta.FindStatusCondition(conds, v1alpha1.CondACLConflict); c != nil {
		t.Fatalf("ACLConflict condition should be removed when view is nil, got %+v", c)
	}
}

// TestSetACLConflictConditionNoConflict verifies that a view with no conflicts
// (or where this resource is not a party) yields ACLConflict=False/NoConflict.
func TestSetACLConflictConditionNoConflict(t *testing.T) {
	// Empty view — no conflicts.
	view := &ClusterACLView{}
	var conds []metav1.Condition
	setACLConflictCondition(&conds, view, "KafkaTopic", "ns1", "topic-t1", 5)

	c := k8smeta.FindStatusCondition(conds, v1alpha1.CondACLConflict)
	if c == nil {
		t.Fatal("ACLConflict condition should be present when view is non-nil")
	}
	if c.Status != metav1.ConditionFalse {
		t.Fatalf("ACLConflict status = %v, want False", c.Status)
	}
	if c.Reason != "NoConflict" {
		t.Fatalf("ACLConflict reason = %q, want NoConflict", c.Reason)
	}
}

// TestSetACLConflictConditionNotParty verifies that when the view has conflicts
// but the resource is not a party to any of them, ACLConflict=False is set.
func TestSetACLConflictConditionNotParty(t *testing.T) {
	// Build a view where other resources conflict but NOT this resource.
	aclA := access.ACL{SourceKind: "KafkaTopic", SourceNamespace: "ns1", SourceName: "other-topic", Permission: "Allow"}
	aclB := access.ACL{SourceKind: "KafkaAccessPolicy", SourceNamespace: "ns1", SourceName: "other-policy", Permission: "Deny"}
	view := &ClusterACLView{
		Conflicts: []access.Conflict{{Subject: "some-subject", A: aclA, B: aclB}},
	}
	var conds []metav1.Condition
	setACLConflictCondition(&conds, view, "KafkaTopic", "ns1", "topic-t1", 3)

	c := k8smeta.FindStatusCondition(conds, v1alpha1.CondACLConflict)
	if c == nil {
		t.Fatal("ACLConflict condition should be present")
	}
	if c.Status != metav1.ConditionFalse {
		t.Fatalf("ACLConflict status = %v, want False (not a party)", c.Status)
	}
	if c.Reason != "NoConflict" {
		t.Fatalf("ACLConflict reason = %q, want NoConflict", c.Reason)
	}
}

// TestSetACLConflictConditionIsPartyAsA verifies that when the resource is party
// A in a conflict, ACLConflict=True/CrossResourceConflict is set and the message
// names the other party (B).
func TestSetACLConflictConditionIsPartyAsA(t *testing.T) {
	aclA := access.ACL{SourceKind: "KafkaTopic", SourceNamespace: "ns1", SourceName: "topic-t1", Permission: "Allow"}
	aclB := access.ACL{SourceKind: "KafkaAccessPolicy", SourceNamespace: "ns1", SourceName: "policy-p1", Permission: "Deny"}
	view := &ClusterACLView{
		Conflicts: []access.Conflict{{Subject: "the-subject", A: aclA, B: aclB}},
	}
	var conds []metav1.Condition
	setACLConflictCondition(&conds, view, "KafkaTopic", "ns1", "topic-t1", 7)

	c := k8smeta.FindStatusCondition(conds, v1alpha1.CondACLConflict)
	if c == nil {
		t.Fatal("ACLConflict condition should be present")
	}
	if c.Status != metav1.ConditionTrue {
		t.Fatalf("ACLConflict status = %v, want True (is a party)", c.Status)
	}
	if c.Reason != "CrossResourceConflict" {
		t.Fatalf("ACLConflict reason = %q, want CrossResourceConflict", c.Reason)
	}
	if !strings.Contains(c.Message, "ns1/policy-p1") {
		t.Fatalf("ACLConflict message = %q, want to contain other party namespace/name %q", c.Message, "ns1/policy-p1")
	}
	if !strings.Contains(c.Message, "the-subject") {
		t.Fatalf("ACLConflict message = %q, want to contain subject %q", c.Message, "the-subject")
	}
}

// TestSetACLConflictConditionIsPartyAsB verifies that when the resource is party
// B in a conflict, ACLConflict=True is set naming party A as the other party.
func TestSetACLConflictConditionIsPartyAsB(t *testing.T) {
	aclA := access.ACL{SourceKind: "KafkaTopic", SourceNamespace: "ns1", SourceName: "topic-t1", Permission: "Allow"}
	aclB := access.ACL{SourceKind: "KafkaAccessPolicy", SourceNamespace: "ns1", SourceName: "policy-p1", Permission: "Deny"}
	view := &ClusterACLView{
		Conflicts: []access.Conflict{{Subject: "the-subject", A: aclA, B: aclB}},
	}
	var conds []metav1.Condition
	// This resource is policy-p1 (party B).
	setACLConflictCondition(&conds, view, "KafkaAccessPolicy", "ns1", "policy-p1", 2)

	c := k8smeta.FindStatusCondition(conds, v1alpha1.CondACLConflict)
	if c == nil {
		t.Fatal("ACLConflict condition should be present")
	}
	if c.Status != metav1.ConditionTrue {
		t.Fatalf("ACLConflict status = %v, want True (is party B)", c.Status)
	}
	if c.Reason != "CrossResourceConflict" {
		t.Fatalf("ACLConflict reason = %q, want CrossResourceConflict", c.Reason)
	}
	if !strings.Contains(c.Message, "ns1/topic-t1") {
		t.Fatalf("ACLConflict message = %q, want to contain other party namespace/name %q", c.Message, "ns1/topic-t1")
	}
}

// TestReconcileTopicACLConflictConditionTrue verifies that ReconcileTopic sets
// ACLConflict=True/CrossResourceConflict when the topic is a party to a
// cross-resource conflict in the view, while Phase and Ready remain unaffected.
func TestReconcileTopicACLConflictConditionTrue(t *testing.T) {
	k := kafkamock.New(nil, nil)
	// Build a real view: the topic "orders" (ns="") grants Allow Write; a policy
	// grants Deny Write on the same tuple.
	tp := topicWithAllowWrite("orders", "", "payments.orders", "User:svc")
	pol := policyWithDenyWrite("deny-policy", "", "payments.orders", "User:svc")
	view := BuildClusterACLView([]*v1alpha1.KafkaTopic{tp}, []*v1alpha1.KafkaAccessPolicy{pol}, nil, nil)

	// The reconciled topic mirrors baseTopic but with access that conflicts.
	reconcileTp := baseTopic("Enforce")
	reconcileTp.Spec.Access = v1alpha1.TopicAccess{
		Producers: []v1alpha1.ProducerAccess{
			{Principal: "User:svc", Operations: []string{"Write"}},
		},
	}

	st, err := ReconcileTopic(context.Background(), reconcileTp, cluster(), k, nil, stubResolver{}, view, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Phase and Ready are unaffected — the condition is informational only.
	if st.Phase != v1alpha1.PhaseReady {
		t.Fatalf("phase = %q, want Ready (ACLConflict is non-terminal)", st.Phase)
	}

	c := k8smeta.FindStatusCondition(st.Conditions, v1alpha1.CondACLConflict)
	if c == nil {
		t.Fatal("ACLConflict condition missing after reconcile with conflict view")
	}
	if c.Status != metav1.ConditionTrue {
		t.Fatalf("ACLConflict status = %v, want True", c.Status)
	}
	if c.Reason != "CrossResourceConflict" {
		t.Fatalf("ACLConflict reason = %q, want CrossResourceConflict", c.Reason)
	}
	if !strings.Contains(c.Message, "KafkaAccessPolicy//deny-policy") {
		t.Fatalf("ACLConflict message = %q, want to contain other party %q", c.Message, "KafkaAccessPolicy//deny-policy")
	}
}

// TestReconcileTopicACLConflictConditionFalse verifies that ReconcileTopic sets
// ACLConflict=False/NoConflict when a non-nil view has no conflicts for this topic.
func TestReconcileTopicACLConflictConditionFalse(t *testing.T) {
	k := kafkamock.New(nil, nil)
	// View with no conflicts.
	view := &ClusterACLView{}

	tp := baseTopic("Enforce")
	st, err := ReconcileTopic(context.Background(), tp, cluster(), k, nil, stubResolver{}, view, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	c := k8smeta.FindStatusCondition(st.Conditions, v1alpha1.CondACLConflict)
	if c == nil {
		t.Fatal("ACLConflict condition missing with non-nil view")
	}
	if c.Status != metav1.ConditionFalse {
		t.Fatalf("ACLConflict status = %v, want False", c.Status)
	}
	if c.Reason != "NoConflict" {
		t.Fatalf("ACLConflict reason = %q, want NoConflict", c.Reason)
	}
	if st.Phase != v1alpha1.PhaseReady {
		t.Fatalf("phase = %q, want Ready", st.Phase)
	}
}

// TestReconcileTopicACLConflictConditionNilView verifies that when view==nil,
// the ACLConflict condition is absent from the topic's status.
func TestReconcileTopicACLConflictConditionNilView(t *testing.T) {
	k := kafkamock.New(nil, nil)
	tp := baseTopic("Enforce")
	// Seed a stale ACLConflict so we verify it is removed, not just absent.
	tp.Status = &v1alpha1.KafkaTopicStatus{
		Conditions: []metav1.Condition{
			{Type: v1alpha1.CondACLConflict, Status: metav1.ConditionTrue, Reason: "CrossResourceConflict", Message: "stale"},
		},
	}

	st, err := ReconcileTopic(context.Background(), tp, cluster(), k, nil, stubResolver{}, nil, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if c := k8smeta.FindStatusCondition(st.Conditions, v1alpha1.CondACLConflict); c != nil {
		t.Fatalf("ACLConflict condition should be absent when view==nil, got %+v", c)
	}
}

// TestReconcilePolicyACLConflictConditionTrue verifies that ReconcilePolicy sets
// ACLConflict=True/CrossResourceConflict when the policy is a conflict party.
func TestReconcilePolicyACLConflictConditionTrue(t *testing.T) {
	k := kafkamock.New(nil, nil)
	// Build a real view: topic grants Allow Write; this policy grants Deny Write.
	tp := topicWithAllowWrite("topic-t1", "", "payments.orders", "User:svc")
	pol := policyWithDenyWrite("p", "", "payments.orders", "User:svc")
	view := BuildClusterACLView([]*v1alpha1.KafkaTopic{tp}, []*v1alpha1.KafkaAccessPolicy{pol}, nil, nil)

	// The policy under reconcile is basePolicy with namespace="" name="p".
	reconcilePol := basePolicy("Enforce")
	reconcilePol.Name = "p"
	reconcilePol.Namespace = ""
	reconcilePol.Spec.Rules = []v1alpha1.ACLRule{
		{
			Principal:  "User:svc",
			Permission: "Deny",
			Operations: []string{"Write"},
			Resource:   v1alpha1.ACLResource{Type: "topic", Name: "payments.orders"},
		},
	}

	st, err := ReconcilePolicy(context.Background(), reconcilePol, cluster(), k, view)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Phase/Ready must be unaffected.
	if st.Phase != v1alpha1.PhaseReady {
		t.Fatalf("phase = %q, want Ready (ACLConflict is non-terminal)", st.Phase)
	}

	c := k8smeta.FindStatusCondition(st.Conditions, v1alpha1.CondACLConflict)
	if c == nil {
		t.Fatal("ACLConflict condition missing after reconcile with conflict view")
	}
	if c.Status != metav1.ConditionTrue {
		t.Fatalf("ACLConflict status = %v, want True", c.Status)
	}
	if c.Reason != "CrossResourceConflict" {
		t.Fatalf("ACLConflict reason = %q, want CrossResourceConflict", c.Reason)
	}
	if !strings.Contains(c.Message, "KafkaTopic//topic-t1") {
		t.Fatalf("ACLConflict message = %q, want to contain other party %q", c.Message, "KafkaTopic//topic-t1")
	}
}

// TestReconcilePolicyACLConflictConditionFalse verifies that ReconcilePolicy sets
// ACLConflict=False/NoConflict when a non-nil view has no conflicts for this policy.
func TestReconcilePolicyACLConflictConditionFalse(t *testing.T) {
	k := kafkamock.New(nil, nil)
	view := &ClusterACLView{}

	pol := basePolicy("Enforce")
	st, err := ReconcilePolicy(context.Background(), pol, cluster(), k, view)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	c := k8smeta.FindStatusCondition(st.Conditions, v1alpha1.CondACLConflict)
	if c == nil {
		t.Fatal("ACLConflict condition missing with non-nil view")
	}
	if c.Status != metav1.ConditionFalse {
		t.Fatalf("ACLConflict status = %v, want False", c.Status)
	}
	if c.Reason != "NoConflict" {
		t.Fatalf("ACLConflict reason = %q, want NoConflict", c.Reason)
	}
	if st.Phase != v1alpha1.PhaseReady {
		t.Fatalf("phase = %q, want Ready", st.Phase)
	}
}

// TestBuildClusterACLViewExcludesRBACOnlyTopics verifies that a topic on an
// rbac-only cluster contributes NO ACLs to the cluster ACL view (spec §40).
// Policies are ACL-only regardless of backends and must still contribute.
func TestBuildClusterACLViewExcludesRBACOnlyTopics(t *testing.T) {
	rbacTopic := topicWithAllowWrite("orders", "team-a", "payments.orders", "User:svc")
	cl := rbacOnlyCluster() // cluster with accessBackends: [rbac] only

	view := BuildClusterACLView([]*v1alpha1.KafkaTopic{rbacTopic}, nil, nil, cl)
	if len(view.DesiredACLs) != 0 {
		t.Fatalf("rbac-only topic must not contribute ACLs; got %d ACLs: %+v", len(view.DesiredACLs), view.DesiredACLs)
	}
}

// TestBuildClusterACLViewIncludesACLTopics verifies that a topic on an
// acl-backed cluster still contributes ACLs to the view (spec §40).
func TestBuildClusterACLViewIncludesACLTopics(t *testing.T) {
	aclTopic := topicWithAllowWrite("orders", "team-a", "payments.orders", "User:svc")
	cl := cluster() // no accessBackends → defaults to "acl"

	view := BuildClusterACLView([]*v1alpha1.KafkaTopic{aclTopic}, nil, nil, cl)
	if len(view.DesiredACLs) == 0 {
		t.Fatalf("acl-backed topic must contribute ACLs to the view; got empty DesiredACLs")
	}
}

// TestBuildClusterACLViewPoliciesAlwaysContribute verifies that KafkaAccessPolicy
// ACLs are always included in the view, even when the cluster is rbac-only.
// Policies are ACL-only regardless of backends (spec §40).
func TestBuildClusterACLViewPoliciesAlwaysContribute(t *testing.T) {
	rbacTopic := topicWithAllowWrite("orders", "team-a", "payments.orders", "User:svc")
	pol := policyWithDenyWrite("policy-p1", "team-a", "payments.orders", "User:svc")
	cl := rbacOnlyCluster()

	view := BuildClusterACLView([]*v1alpha1.KafkaTopic{rbacTopic}, []*v1alpha1.KafkaAccessPolicy{pol}, nil, cl)
	// The topic contributes nothing, but the policy's Deny Write must appear.
	if len(view.DesiredACLs) == 0 {
		t.Fatalf("policy ACLs must contribute even on rbac-only cluster; got empty DesiredACLs")
	}
}

// TestReconcilePolicyACLConflictConditionNilView verifies that when view==nil,
// the ACLConflict condition is absent from the policy's status.
func TestReconcilePolicyACLConflictConditionNilView(t *testing.T) {
	k := kafkamock.New(nil, nil)
	pol := basePolicy("Enforce")
	// Seed a stale ACLConflict so we verify it is removed, not just absent.
	pol.Status = &v1alpha1.KafkaAccessPolicyStatus{
		Conditions: []metav1.Condition{
			{Type: v1alpha1.CondACLConflict, Status: metav1.ConditionTrue, Reason: "CrossResourceConflict", Message: "stale"},
		},
	}

	st, err := ReconcilePolicy(context.Background(), pol, cluster(), k, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if c := k8smeta.FindStatusCondition(st.Conditions, v1alpha1.CondACLConflict); c != nil {
		t.Fatalf("ACLConflict condition should be absent when view==nil, got %+v", c)
	}
}
