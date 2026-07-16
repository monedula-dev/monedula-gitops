package reconcile

import (
	"context"
	"errors"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/mds"
	mdsmock "github.com/monedula-dev/monedula-gitops/internal/mds/mock"
	"github.com/monedula-dev/monedula-gitops/internal/rbac"
)

// mdsCluster returns a KafkaCluster with authorization.mds configured —
// the minimum required for Compile to succeed in Enforce mode.
func mdsCluster() *v1alpha1.KafkaCluster {
	return &v1alpha1.KafkaCluster{
		Spec: v1alpha1.KafkaClusterSpec{
			BootstrapServers: "localhost:9092",
			Authorization: &v1alpha1.AuthorizationConfig{
				MDS: &v1alpha1.MDSConfig{
					Endpoint: "http://mds:8090",
					Clusters: v1alpha1.MDSClusters{
						KafkaCluster: "lkc-abc123",
					},
				},
			},
		},
	}
}

// baseRoleBinding builds a minimal valid KafkaRoleBinding for tests. mode may
// be "" to leave Reconciliation nil (defaults to Enforce).
func baseRoleBinding(mode string) *v1alpha1.KafkaRoleBinding {
	rb := &v1alpha1.KafkaRoleBinding{
		Spec: v1alpha1.KafkaRoleBindingSpec{
			ClusterRef: v1alpha1.ClusterRef{Name: "c"},
			Principal:  "User:alice",
			Role:       "DeveloperRead",
			Scope:      v1alpha1.RoleBindingScope{Type: "kafka"},
			Resources: []v1alpha1.RoleResource{
				{Type: "Topic", Name: "payments.orders", PatternType: "literal"},
			},
		},
	}
	rb.Name = "rb"
	rb.Namespace = "default"
	rb.Generation = 3
	// APIVersion is required by ValidateRoleBindingShape.
	rb.APIVersion = v1alpha1.APIVersion
	if mode != "" {
		rb.Spec.Reconciliation = &v1alpha1.Reconciliation{Mode: mode}
	}
	return rb
}

// expectedKey builds the mds.RoleBinding key for a resource-scoped binding
// based on baseRoleBinding's field values.
func expectedKey() string {
	rb := mds.RoleBinding{
		Principal: "User:alice",
		Role:      "DeveloperRead",
		Scope:     mds.Scope{Type: "kafka", KafkaCluster: "lkc-abc123"},
		Resource:  &mds.ResourcePattern{Type: "Topic", Name: "payments.orders", PatternType: "literal"},
	}
	return rb.Key()
}

// ---- create (Enforce) ----

// TestReconcileRoleBindingEnforceCreates verifies that a new binding (not
// present live) triggers an AddRoleBinding call and results in Ready +
// RoleBindingSynced=True.
func TestReconcileRoleBindingEnforceCreates(t *testing.T) {
	mock := mdsmock.New() // empty live state
	rb := baseRoleBinding("Enforce")

	st, err := ReconcileRoleBinding(context.Background(), rb, mdsCluster(), mock, nil)
	if err != nil {
		t.Fatalf("unexpected error on clean apply: %v", err)
	}

	calls := mock.Calls()
	if len(calls) != 1 || calls[0] != "AddRoleBinding "+expectedKey() {
		t.Fatalf("calls = %v, want one AddRoleBinding", calls)
	}
	if st.Phase != v1alpha1.PhaseReady {
		t.Fatalf("phase = %q, want Ready", st.Phase)
	}
	if s, _, ok := condStatus(st.Conditions, v1alpha1.CondRoleBindingSynced); !ok || s != metav1.ConditionTrue {
		t.Fatalf("RoleBindingSynced = %v ok=%v, want True", s, ok)
	}
	if s, _, ok := condStatus(st.Conditions, v1alpha1.CondMDSReachable); !ok || s != metav1.ConditionTrue {
		t.Fatalf("MDSReachable = %v ok=%v, want True", s, ok)
	}
	if s, _, ok := condStatus(st.Conditions, v1alpha1.CondReady); !ok || s != metav1.ConditionTrue {
		t.Fatalf("Ready = %v ok=%v, want True", s, ok)
	}
	if st.ObservedGeneration != 3 {
		t.Fatalf("observedGeneration = %d, want 3", st.ObservedGeneration)
	}
	if len(st.ObservedResources) == 0 {
		t.Fatalf("ObservedResources empty, want the declared resources")
	}
	if st.LastAppliedTime == nil {
		t.Fatalf("LastAppliedTime not set after enforce apply")
	}
}

// ---- converge (no-op) ----

// TestReconcileRoleBindingEnforceConverge verifies that when the desired
// binding is already live, no MDS mutations are issued and the result is Ready.
func TestReconcileRoleBindingEnforceConverge(t *testing.T) {
	// Pre-seed the mock with the exact binding we desire.
	live := mds.RoleBinding{
		Principal: "User:alice",
		Role:      "DeveloperRead",
		Scope:     mds.Scope{Type: "kafka", KafkaCluster: "lkc-abc123"},
		Resource:  &mds.ResourcePattern{Type: "Topic", Name: "payments.orders", PatternType: "literal"},
	}
	mock := mdsmock.New(live)
	rb := baseRoleBinding("Enforce")

	st, err := ReconcileRoleBinding(context.Background(), rb, mdsCluster(), mock, nil)
	if err != nil {
		t.Fatalf("converge should not fail: %v", err)
	}

	if got := mock.Calls(); len(got) != 0 {
		t.Fatalf("no-op converge mutated MDS: calls = %v", got)
	}
	if st.Phase != v1alpha1.PhaseReady {
		t.Fatalf("phase = %q, want Ready", st.Phase)
	}
	if s, _, _ := condStatus(st.Conditions, v1alpha1.CondRoleBindingSynced); s != metav1.ConditionTrue {
		t.Fatalf("RoleBindingSynced = %v, want True", s)
	}
}

// ---- drift (re-add) ----

// TestReconcileRoleBindingEnforceDrift verifies that a binding absent from live
// state (removed out-of-band) is re-added on the next reconcile.
func TestReconcileRoleBindingEnforceDrift(t *testing.T) {
	// Seed the mock with no bindings — the desired binding is already absent,
	// simulating an out-of-band removal. This makes the re-add unambiguous:
	// the only call recorded is the AddRoleBinding from the reconcile itself.
	mock := mdsmock.New()
	rb := baseRoleBinding("Enforce")

	st, err := ReconcileRoleBinding(context.Background(), rb, mdsCluster(), mock, nil)
	if err != nil {
		t.Fatalf("drift re-add should not fail: %v", err)
	}

	calls := mock.Calls()
	var addCalls int
	for _, c := range calls {
		if len(c) > 14 && c[:14] == "AddRoleBinding" {
			addCalls++
		}
	}
	if addCalls == 0 {
		t.Fatalf("drift: AddRoleBinding not called; calls = %v", calls)
	}
	if st.Phase != v1alpha1.PhaseReady {
		t.Fatalf("phase = %q, want Ready after drift re-add", st.Phase)
	}
}

// ---- prune ----

// TestReconcileRoleBindingPruneWithConsent verifies that a live binding in the
// managed scope but NOT in the desired set is removed when spec.prune=true.
func TestReconcileRoleBindingPruneWithConsent(t *testing.T) {
	// Desired: binding A (alice/DeveloperRead/kafka/Topic payments.orders)
	// Live:    binding A + binding B (alice/DeveloperRead/kafka/Topic extra.topic)
	extra := mds.RoleBinding{
		Principal: "User:alice",
		Role:      "DeveloperRead",
		Scope:     mds.Scope{Type: "kafka", KafkaCluster: "lkc-abc123"},
		Resource:  &mds.ResourcePattern{Type: "Topic", Name: "extra.topic", PatternType: "literal"},
	}
	desired := mds.RoleBinding{
		Principal: "User:alice",
		Role:      "DeveloperRead",
		Scope:     mds.Scope{Type: "kafka", KafkaCluster: "lkc-abc123"},
		Resource:  &mds.ResourcePattern{Type: "Topic", Name: "payments.orders", PatternType: "literal"},
	}
	mock := mdsmock.New(extra, desired)
	rb := baseRoleBinding("Enforce")
	rb.Spec.Prune = true // prune enabled

	st, err := ReconcileRoleBinding(context.Background(), rb, mdsCluster(), mock, nil)
	if err != nil {
		t.Fatalf("prune should not return error: %v", err)
	}

	calls := mock.Calls()
	var removeCalls int
	for _, c := range calls {
		if len(c) > 17 && c[:17] == "RemoveRoleBinding" {
			removeCalls++
		}
	}
	if removeCalls == 0 {
		t.Fatalf("prune=true: RemoveRoleBinding not called; calls = %v", calls)
	}
	if st.Phase != v1alpha1.PhaseReady {
		t.Fatalf("phase = %q, want Ready after prune", st.Phase)
	}
}

// TestReconcileRoleBindingPruneWithoutConsent verifies that a live binding in
// the managed scope but NOT desired is NOT removed when spec.prune=false — it
// is reported as drift (PruneDisabled) but the run stays Ready.
func TestReconcileRoleBindingPruneWithoutConsent(t *testing.T) {
	extra := mds.RoleBinding{
		Principal: "User:alice",
		Role:      "DeveloperRead",
		Scope:     mds.Scope{Type: "kafka", KafkaCluster: "lkc-abc123"},
		Resource:  &mds.ResourcePattern{Type: "Topic", Name: "extra.topic", PatternType: "literal"},
	}
	desired := mds.RoleBinding{
		Principal: "User:alice",
		Role:      "DeveloperRead",
		Scope:     mds.Scope{Type: "kafka", KafkaCluster: "lkc-abc123"},
		Resource:  &mds.ResourcePattern{Type: "Topic", Name: "payments.orders", PatternType: "literal"},
	}
	mock := mdsmock.New(extra, desired)
	rb := baseRoleBinding("Enforce")
	rb.Spec.Prune = false // no prune consent

	st, err := ReconcileRoleBinding(context.Background(), rb, mdsCluster(), mock, nil)
	if err != nil {
		t.Fatalf("prune=false should not return error: %v", err)
	}

	for _, c := range mock.Calls() {
		if len(c) > 17 && c[:17] == "RemoveRoleBinding" {
			t.Fatalf("prune=false: RemoveRoleBinding called without consent; calls = %v", mock.Calls())
		}
	}
	// PruneDisabled is not a failure: phase is Ready and result.OK() is true.
	if st.Phase != v1alpha1.PhaseReady {
		t.Fatalf("phase = %q, want Ready (PruneDisabled is not a failure)", st.Phase)
	}
}

// ---- validation-first ----

// TestReconcileRoleBindingValidationFailedMDSNotCalled verifies that an invalid
// shape (missing principal) results in a terminal ValidationFailed with no MDS
// calls.
func TestReconcileRoleBindingValidationFailedMDSNotCalled(t *testing.T) {
	mock := mdsmock.New()
	rb := baseRoleBinding("Enforce")
	rb.Spec.Principal = "" // invalid: empty principal

	st, err := ReconcileRoleBinding(context.Background(), rb, mdsCluster(), mock, nil)
	if err != nil {
		t.Fatalf("validation failure is terminal, want nil error, got: %v", err)
	}

	if got := mock.Calls(); len(got) != 0 {
		t.Fatalf("MDS called despite validation failure: calls = %v", got)
	}
	if st.Phase != v1alpha1.PhaseError {
		t.Fatalf("phase = %q, want Error", st.Phase)
	}
	s, _, ok := condStatus(st.Conditions, v1alpha1.CondValidationFailed)
	if !ok || s != metav1.ConditionTrue {
		t.Fatalf("ValidationFailed = %v ok=%v, want True", s, ok)
	}
	if s, _, _ := condStatus(st.Conditions, v1alpha1.CondReady); s != metav1.ConditionFalse {
		t.Fatalf("Ready = %v, want False", s)
	}
}

// ---- compile error ----

// TestReconcileRoleBindingCompileErrorTerminal verifies that a Compile failure
// (unresolvable scope: missing sub-cluster ID for non-kafka scope) is terminal.
func TestReconcileRoleBindingCompileErrorTerminal(t *testing.T) {
	mock := mdsmock.New()
	rb := baseRoleBinding("Enforce")
	rb.Spec.Scope = v1alpha1.RoleBindingScope{Type: "schema-registry"} // requires MDSClusters.SchemaRegistryCluster

	// mdsCluster has no schemaRegistryCluster set, so Compile will fail.
	st, err := ReconcileRoleBinding(context.Background(), rb, mdsCluster(), mock, nil)
	if err != nil {
		t.Fatalf("compile error is terminal, want nil error, got: %v", err)
	}

	if got := mock.Calls(); len(got) != 0 {
		t.Fatalf("MDS called despite compile error: calls = %v", got)
	}
	if st.Phase != v1alpha1.PhaseError {
		t.Fatalf("phase = %q, want Error", st.Phase)
	}
	if s, _, ok := condStatus(st.Conditions, v1alpha1.CondValidationFailed); !ok || s != metav1.ConditionTrue {
		t.Fatalf("ValidationFailed = %v ok=%v, want True", s, ok)
	}
}

// ---- MDS list error (transient) ----

// TestReconcileRoleBindingMDSListErrorTransient verifies that a
// ListRoleBindings failure is TRANSIENT: returns a non-nil error,
// MDSReachable=False, Ready=False, and still returns the status.
func TestReconcileRoleBindingMDSListErrorTransient(t *testing.T) {
	// Inject a list failure via a wrapper that always errors ListRoleBindings.
	rb := baseRoleBinding("Enforce")

	listErr := errors.New("MDS connection refused")
	failMock := &failListMDSClient{err: listErr}

	st, err := ReconcileRoleBinding(context.Background(), rb, mdsCluster(), failMock, nil)
	if err == nil {
		t.Fatalf("MDS list failure is transient, want non-nil error")
	}
	if !errors.Is(err, listErr) {
		t.Fatalf("error = %v, want listErr", err)
	}
	if st.Phase != v1alpha1.PhaseError {
		t.Fatalf("phase = %q, want Error", st.Phase)
	}
	s, _, ok := condStatus(st.Conditions, v1alpha1.CondMDSReachable)
	if !ok || s != metav1.ConditionFalse {
		t.Fatalf("MDSReachable = %v ok=%v, want False", s, ok)
	}
	if s, _, _ := condStatus(st.Conditions, v1alpha1.CondReady); s != metav1.ConditionFalse {
		t.Fatalf("Ready = %v, want False", s)
	}
	// ObservedGeneration is always stamped.
	if st.ObservedGeneration != 3 {
		t.Fatalf("observedGeneration = %d, want 3", st.ObservedGeneration)
	}
}

// failListMDSClient is a mds.Client that always fails ListRoleBindings.
type failListMDSClient struct {
	err error
}

func (f *failListMDSClient) ListRoleBindings(_ context.Context, _ mds.Scope) ([]mds.RoleBinding, error) {
	return nil, f.err
}
func (f *failListMDSClient) AddRoleBinding(_ context.Context, _ mds.RoleBinding) error {
	return errors.New("not expected")
}
func (f *failListMDSClient) RemoveRoleBinding(_ context.Context, _ mds.RoleBinding) error {
	return errors.New("not expected")
}

// ---- DetectOnly / ObserveOnly — no apply ----

// TestReconcileRoleBindingDetectOnly verifies that DetectOnly reports drift but
// never mutates MDS.
func TestReconcileRoleBindingDetectOnly(t *testing.T) {
	mock := mdsmock.New() // empty live: the desired binding is missing → drift
	rb := baseRoleBinding("DetectOnly")

	st, err := ReconcileRoleBinding(context.Background(), rb, mdsCluster(), mock, nil)
	if err != nil {
		t.Fatalf("DetectOnly drift is terminal, want nil error, got: %v", err)
	}

	if got := mock.Calls(); len(got) != 0 {
		t.Fatalf("DetectOnly mutated MDS: calls = %v", got)
	}
	if st.Phase != v1alpha1.PhaseDrifted {
		t.Fatalf("phase = %q, want Drifted", st.Phase)
	}
	if s, _, _ := condStatus(st.Conditions, v1alpha1.CondRoleBindingSynced); s != metav1.ConditionFalse {
		t.Fatalf("RoleBindingSynced = %v, want False (drift pending)", s)
	}
}

// TestReconcileRoleBindingObserveOnly verifies that ObserveOnly reports drift
// informally (phase stays Ready) and never mutates MDS.
func TestReconcileRoleBindingObserveOnly(t *testing.T) {
	mock := mdsmock.New() // empty live: drift
	rb := baseRoleBinding("ObserveOnly")

	st, err := ReconcileRoleBinding(context.Background(), rb, mdsCluster(), mock, nil)
	if err != nil {
		t.Fatalf("ObserveOnly is never a failure, want nil error, got: %v", err)
	}

	if got := mock.Calls(); len(got) != 0 {
		t.Fatalf("ObserveOnly mutated MDS: calls = %v", got)
	}
	if st.Phase != v1alpha1.PhaseReady {
		t.Fatalf("phase = %q, want Ready (observe is not a failure)", st.Phase)
	}
	if s, _, _ := condStatus(st.Conditions, v1alpha1.CondReady); s != metav1.ConditionTrue {
		t.Fatalf("Ready = %v, want True in observe-only", s)
	}
}

// ---- ValidationFailed clears on fix ----

// TestReconcileRoleBindingValidationFailedClears pins review I11: a stale
// ValidationFailed=True condition must be cleared when the spec is fixed.
func TestReconcileRoleBindingValidationFailedClears(t *testing.T) {
	// First pass: invalid spec (empty principal).
	mock1 := mdsmock.New()
	rb1 := baseRoleBinding("Enforce")
	rb1.Spec.Principal = ""
	st1, err := ReconcileRoleBinding(context.Background(), rb1, mdsCluster(), mock1, nil)
	if err != nil {
		t.Fatalf("first pass: %v", err)
	}
	if s, _, ok := condStatus(st1.Conditions, v1alpha1.CondValidationFailed); !ok || s != metav1.ConditionTrue {
		t.Fatalf("ValidationFailed = %v ok=%v, want True after invalid spec", s, ok)
	}

	// Second pass: spec fixed, prior status fed back.
	mock2 := mdsmock.New()
	rb2 := baseRoleBinding("Enforce")
	rb2.Status = &st1
	st2, err := ReconcileRoleBinding(context.Background(), rb2, mdsCluster(), mock2, nil)
	if err != nil {
		t.Fatalf("second pass: %v", err)
	}
	s, _, ok := condStatus(st2.Conditions, v1alpha1.CondValidationFailed)
	if !ok || s != metav1.ConditionFalse {
		t.Fatalf("ValidationFailed = %v ok=%v, want False after spec is fixed (stale condition not cleared)", s, ok)
	}
	if st2.Phase != v1alpha1.PhaseReady {
		t.Fatalf("phase = %q, want Ready after spec is fixed", st2.Phase)
	}
}

// ---- cluster without MDS ----

// TestReconcileRoleBindingNoMDSConfigTerminal verifies that a cluster without
// authorization.mds is a terminal validation failure.
func TestReconcileRoleBindingNoMDSConfigTerminal(t *testing.T) {
	mock := mdsmock.New()
	rb := baseRoleBinding("Enforce")
	noMDSCluster := &v1alpha1.KafkaCluster{
		Spec: v1alpha1.KafkaClusterSpec{BootstrapServers: "localhost:9092"},
	}

	st, err := ReconcileRoleBinding(context.Background(), rb, noMDSCluster, mock, nil)
	if err != nil {
		t.Fatalf("no-MDS cluster is terminal, want nil error, got: %v", err)
	}

	if got := mock.Calls(); len(got) != 0 {
		t.Fatalf("MDS called despite no MDS config: calls = %v", got)
	}
	if st.Phase != v1alpha1.PhaseError {
		t.Fatalf("phase = %q, want Error", st.Phase)
	}
	if s, _, ok := condStatus(st.Conditions, v1alpha1.CondValidationFailed); !ok || s != metav1.ConditionTrue {
		t.Fatalf("ValidationFailed = %v ok=%v, want True", s, ok)
	}
}

// ---- ClusterRoleBindingView prune scope ----

// TestReconcileRoleBindingDoesNotPruneOtherOwnersBinding verifies that
// DesiredBindings (the cluster-wide keep-set) protects a live binding that
// belongs to another owner sharing rb's scope key. When another owner's binding
// is present in view.DesiredBindings, it must NOT be pruned even though it is
// absent from this resource's own desired set.
func TestReconcileRoleBindingDoesNotPruneOtherOwnersBinding(t *testing.T) {
	// rb: User:alice DeveloperRead kafka Topic:payments.orders — with prune consent.
	rb := baseRoleBinding("Enforce")
	rb.Spec.Prune = true

	cl := mdsCluster()

	// Compile this resource's own desired bindings.
	mine, err := rbac.Compile(rb, cl.Spec.Authorization.MDS)
	if err != nil {
		t.Fatalf("compile rb: %v", err)
	}
	rbac.StampPrune(mine, true)

	// Build an "other owner" binding: same principal/role/scope, different topic.
	// SourceName differs so it is attributed to a different KafkaRoleBinding.
	other := rbac.RoleBinding{
		Principal: mine[0].Principal,
		Role:      mine[0].Role,
		Scope:     mine[0].Scope,
		Resource: &rbac.ResourcePattern{
			Type:        "Topic",
			Name:        "payments.events",
			PatternType: "literal",
		},
		Prune:           true,
		Mode:            "Enforce",
		SourceKind:      "KafkaRoleBinding",
		SourceNamespace: "default",
		SourceName:      "rb-payments-events",
	}

	// The cluster-wide desired set includes both mine and other.
	all := append(append([]rbac.RoleBinding(nil), mine...), other)
	view := &ClusterRoleBindingView{
		DesiredBindings: all,
		DesiredScope:    rbac.BuildScope(all),
	}

	// Seed the mock with both bindings live so there is no create op — only
	// a potential spurious prune of "other" is the risk.
	mineLive := mds.RoleBinding{
		Principal: "User:alice",
		Role:      "DeveloperRead",
		Scope:     mds.Scope{Type: "kafka", KafkaCluster: "lkc-abc123"},
		Resource:  &mds.ResourcePattern{Type: "Topic", Name: "payments.orders", PatternType: "literal"},
	}
	otherLive := mds.RoleBinding{
		Principal: "User:alice",
		Role:      "DeveloperRead",
		Scope:     mds.Scope{Type: "kafka", KafkaCluster: "lkc-abc123"},
		Resource:  &mds.ResourcePattern{Type: "Topic", Name: "payments.events", PatternType: "literal"},
	}
	mock := mdsmock.New(mineLive, otherLive)

	st, reconcileErr := ReconcileRoleBinding(context.Background(), rb, cl, mock, view)
	if reconcileErr != nil {
		t.Fatalf("unexpected error: %v", reconcileErr)
	}

	// Assert: ZERO RemoveRoleBinding calls — other's binding is in the keep-set.
	for _, c := range mock.Calls() {
		if len(c) >= 17 && c[:17] == "RemoveRoleBinding" {
			t.Fatalf("other owner's binding was pruned despite being in DesiredBindings keep-set: call = %q; all calls = %v", c, mock.Calls())
		}
	}

	if st.Phase != v1alpha1.PhaseReady {
		t.Fatalf("phase = %q, want Ready", st.Phase)
	}
}

// TestReconcileRoleBindingViewScopeVetoesPrune verifies that the
// ClusterRoleBindingView's DesiredScope governs prune decisions: a live binding
// whose (Principal, Role, Scope) tuple is NOT covered by the view's scope is
// NOT pruned — even when it is absent from this resource's desired set and
// prune=true is set on this resource.
//
// This tests the §20.1 aggregation guarantee: only live bindings within the
// cluster-wide aggregated scope are prune candidates.
func TestReconcileRoleBindingViewScopeVetoesPrune(t *testing.T) {
	// rb: User:a DeveloperWrite kafka Topic:orders — with prune consent.
	rb := &v1alpha1.KafkaRoleBinding{
		Spec: v1alpha1.KafkaRoleBindingSpec{
			ClusterRef: v1alpha1.ClusterRef{Name: "c"},
			Principal:  "User:a",
			Role:       "DeveloperWrite",
			Scope:      v1alpha1.RoleBindingScope{Type: "kafka"},
			Resources: []v1alpha1.RoleResource{
				{Type: "Topic", Name: "orders", PatternType: "literal"},
			},
			Prune: true,
		},
	}
	rb.Name = "rb"
	rb.Namespace = "default"
	rb.Generation = 1
	rb.APIVersion = v1alpha1.APIVersion

	// Live state: the desired binding PLUS an extra binding for a completely
	// different (principal, role) pair. The extra binding's ScopeKey is
	// (User:b, DeveloperRead, kafka, lkc-abc123, "") which is NOT in the view
	// scope built from rb alone — so it must NOT be pruned.
	desiredLive := mds.RoleBinding{
		Principal: "User:a",
		Role:      "DeveloperWrite",
		Scope:     mds.Scope{Type: "kafka", KafkaCluster: "lkc-abc123"},
		Resource:  &mds.ResourcePattern{Type: "Topic", Name: "orders", PatternType: "literal"},
	}
	extra := mds.RoleBinding{
		Principal: "User:b",
		Role:      "DeveloperRead",
		Scope:     mds.Scope{Type: "kafka", KafkaCluster: "lkc-abc123"},
		Resource:  &mds.ResourcePattern{Type: "Topic", Name: "other", PatternType: "literal"},
	}
	mock := mdsmock.New(desiredLive, extra)

	// Build the view's DesiredScope using the same aggregation the controller
	// uses: compile rb, stamp prune, BuildScope. This produces a scope that
	// covers only (User:a, DeveloperWrite, kafka, lkc-abc123, "") — NOT extra's
	// (User:b, DeveloperRead, ...) tuple.
	compiled, err := rbac.Compile(rb, mdsCluster().Spec.Authorization.MDS)
	if err != nil {
		t.Fatalf("compile rb for view: %v", err)
	}
	rbac.StampPrune(compiled, true)
	// DesiredBindings must be populated alongside DesiredScope, mirroring how
	// buildClusterRoleBindingView always builds both from the same aggregate
	// (internal/operator/controller/rolebindingview.go): the view's keep-set
	// (DesiredBindings) is what actually spares rb's own binding from prune,
	// not just the scope.
	view := &ClusterRoleBindingView{DesiredBindings: compiled, DesiredScope: rbac.BuildScope(compiled)}

	st, reconcileErr := ReconcileRoleBinding(context.Background(), rb, mdsCluster(), mock, view)
	if reconcileErr != nil {
		t.Fatalf("unexpected error: %v", reconcileErr)
	}

	// Assert: extra's key must NOT appear in any RemoveRoleBinding call.
	// extra's key: User:b|DeveloperRead|kafka|lkc-abc123||Topic|other|literal
	extraKey := extra.Key()
	for _, c := range mock.Calls() {
		if len(c) > 17 && c[:17] == "RemoveRoleBinding" && c == "RemoveRoleBinding "+extraKey {
			t.Fatalf("extra binding (out of view scope) was pruned: call = %q; all calls = %v", c, mock.Calls())
		}
	}

	// The desired binding should converge (no spurious removes of the desired one).
	desiredKey := desiredLive.Key()
	for _, c := range mock.Calls() {
		if c == "RemoveRoleBinding "+desiredKey {
			t.Fatalf("desired binding was removed: call = %q; all calls = %v", c, mock.Calls())
		}
	}

	if st.Phase != v1alpha1.PhaseReady {
		t.Fatalf("phase = %q, want Ready", st.Phase)
	}
}

// ---- Tenancy enforcement tests (spec §20.2) ----

// tenancyMDSCluster wraps mdsCluster() and attaches a TenancyConfig.
func tenancyMDSCluster(t *v1alpha1.TenancyConfig) *v1alpha1.KafkaCluster {
	cl := mdsCluster()
	cl.Spec.Tenancy = t
	return cl
}

// restrictedTenancy is the standard fixture for the role-binding tenancy
// tests: team-* namespaces pass the allow-list, "default" (baseRoleBinding's
// namespace) is prefix-restricted to "payments.".
func restrictedTenancy() *v1alpha1.TenancyConfig {
	return &v1alpha1.TenancyConfig{
		AllowedNamespaces: []string{"default", "team-*"},
		TopicPrefixes: []v1alpha1.TopicPrefixRule{
			{Namespaces: []string{"default"}, Prefixes: []string{"payments."}},
		},
	}
}

// assertRoleBindingTenancyDenied asserts the shared terminal-denial contract:
// nil error, zero MDS calls, Phase Error, ValidationFailed=True with reason
// TenancyDenied, Ready=False.
func assertRoleBindingTenancyDenied(t *testing.T, st v1alpha1.KafkaRoleBindingStatus, err error, mock *mdsmock.Mock) {
	t.Helper()
	if err != nil {
		t.Fatalf("tenancy denial is terminal, want nil error, got: %v", err)
	}
	if got := mock.Calls(); len(got) != 0 {
		t.Fatalf("tenancy denial must not touch MDS: calls = %v", got)
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

// TestReconcileRoleBindingTenancyDeniedNamespace: the namespace allow-list
// applies to role bindings (previously unchecked): a binding in a disallowed
// namespace is a terminal TenancyDenied with zero MDS calls.
func TestReconcileRoleBindingTenancyDeniedNamespace(t *testing.T) {
	mock := mdsmock.New()
	rb := baseRoleBinding("Enforce")
	rb.Namespace = "platform" // not in allow-list

	st, err := ReconcileRoleBinding(context.Background(), rb,
		tenancyMDSCluster(restrictedTenancy()), mock, nil)
	assertRoleBindingTenancyDenied(t, st, err, mock)
}

// TestReconcileRoleBindingTenancyDeniedClusterScoped: a prefix-restricted
// namespace may not create cluster-scoped bindings (e.g. SystemAdmin) — they
// cannot be scoped by a name prefix. Closes the SystemAdmin escalation path.
func TestReconcileRoleBindingTenancyDeniedClusterScoped(t *testing.T) {
	mock := mdsmock.New()
	rb := baseRoleBinding("Enforce")
	rb.Spec.Role = "SystemAdmin"
	rb.Spec.Resources = nil // cluster-scoped

	st, err := ReconcileRoleBinding(context.Background(), rb,
		tenancyMDSCluster(restrictedTenancy()), mock, nil)
	assertRoleBindingTenancyDenied(t, st, err, mock)
}

// TestReconcileRoleBindingTenancyDeniedTopicOutsidePrefix: a Topic resource
// whose name is outside the namespace's allowed prefixes is denied.
func TestReconcileRoleBindingTenancyDeniedTopicOutsidePrefix(t *testing.T) {
	mock := mdsmock.New()
	rb := baseRoleBinding("Enforce")
	rb.Spec.Resources = []v1alpha1.RoleResource{
		{Type: "Topic", Name: "infra.logs", PatternType: "literal"},
	}

	st, err := ReconcileRoleBinding(context.Background(), rb,
		tenancyMDSCluster(restrictedTenancy()), mock, nil)
	assertRoleBindingTenancyDenied(t, st, err, mock)
}

// TestReconcileRoleBindingTenancyDeniedClusterResource: a resource of an
// unscopeable type (Cluster) is denied for a prefix-restricted namespace.
func TestReconcileRoleBindingTenancyDeniedClusterResource(t *testing.T) {
	mock := mdsmock.New()
	rb := baseRoleBinding("Enforce")
	rb.Spec.Resources = []v1alpha1.RoleResource{
		{Type: "Cluster", Name: "kafka-cluster", PatternType: "literal"},
	}

	st, err := ReconcileRoleBinding(context.Background(), rb,
		tenancyMDSCluster(restrictedTenancy()), mock, nil)
	assertRoleBindingTenancyDenied(t, st, err, mock)
}

// TestReconcileRoleBindingTenancyAllowedTopicAndGroup: Topic + Group resources
// inside the namespace's allowed prefixes reconcile normally (AddRoleBinding
// reaches MDS, Ready).
func TestReconcileRoleBindingTenancyAllowedTopicAndGroup(t *testing.T) {
	mock := mdsmock.New()
	rb := baseRoleBinding("Enforce")
	rb.Spec.Resources = []v1alpha1.RoleResource{
		{Type: "Topic", Name: "payments.orders", PatternType: "literal"},
		{Type: "Group", Name: "payments.cg", PatternType: "prefixed"},
	}

	st, err := ReconcileRoleBinding(context.Background(), rb,
		tenancyMDSCluster(restrictedTenancy()), mock, nil)
	if err != nil {
		t.Fatalf("tenancy allowed: want nil error, got: %v", err)
	}
	if st.Phase != v1alpha1.PhaseReady {
		t.Fatalf("phase = %q, want Ready", st.Phase)
	}
	if got := mock.Calls(); len(got) != 2 {
		t.Fatalf("want two AddRoleBinding calls, got: %v", got)
	}
}

// TestReconcileRoleBindingTenancyNonRestrictedNamespaceClusterScoped: a
// namespace that passes the allow-list but matches no prefix rule may still
// create cluster-scoped bindings (allow-list only, like before).
func TestReconcileRoleBindingTenancyNonRestrictedNamespaceClusterScoped(t *testing.T) {
	mock := mdsmock.New()
	rb := baseRoleBinding("Enforce")
	rb.Namespace = "team-platform" // matches team-*, no prefix rule
	rb.Spec.Role = "SystemAdmin"
	rb.Spec.Resources = nil // cluster-scoped

	st, err := ReconcileRoleBinding(context.Background(), rb,
		tenancyMDSCluster(restrictedTenancy()), mock, nil)
	if err != nil {
		t.Fatalf("tenancy allowed: want nil error, got: %v", err)
	}
	if st.Phase != v1alpha1.PhaseReady {
		t.Fatalf("phase = %q, want Ready", st.Phase)
	}
	if got := mock.Calls(); len(got) != 1 {
		t.Fatalf("want one AddRoleBinding call, got: %v", got)
	}
}
