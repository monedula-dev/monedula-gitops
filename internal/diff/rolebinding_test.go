package diff

import (
	"testing"

	"github.com/monedula-dev/monedula-gitops/internal/operations"
	"github.com/monedula-dev/monedula-gitops/internal/rbac"
	"github.com/stretchr/testify/require"
)

// makeRB is a test helper: build a RoleBinding with a resource pattern (kafka scope).
func makeRB(principal, role string, prune bool) rbac.RoleBinding {
	return rbac.RoleBinding{
		Principal: principal,
		Role:      role,
		Scope:     rbac.Scope{Type: "kafka", KafkaCluster: "lkc-abc"},
		Resource: &rbac.ResourcePattern{
			Type:        "Topic",
			Name:        "payments.",
			PatternType: "prefixed",
		},
		SourceKind:      "KafkaRoleBinding",
		SourceNamespace: "billing",
		SourceName:      "rb-payments",
		Prune:           prune,
	}
}

// makeClusterRB is a test helper: build a cluster-scoped (nil Resource) RoleBinding.
func makeClusterRB(principal, role string, prune bool) rbac.RoleBinding {
	return rbac.RoleBinding{
		Principal:       principal,
		Role:            role,
		Scope:           rbac.Scope{Type: "kafka", KafkaCluster: "lkc-abc"},
		Resource:        nil,
		SourceKind:      "KafkaRoleBinding",
		SourceNamespace: "ops",
		SourceName:      "rb-cluster",
		Prune:           prune,
	}
}

// rbOps filters the operation list to AddRoleBinding / RemoveRoleBinding only.
func rbOps(res []operations.Operation) []operations.Operation {
	var out []operations.Operation
	for _, op := range res {
		switch op.Action {
		case operations.AddRoleBinding, operations.RemoveRoleBinding:
			out = append(out, op)
		}
	}
	return out
}

// ---- AddRoleBinding ----

func TestRoleBindingAddWhenAbsentFromLive(t *testing.T) {
	b := makeRB("User:svc-checkout", "DeveloperRead", false)
	desired := Desired{
		RoleBindings:     []rbac.RoleBinding{b},
		RoleBindingScope: rbac.BuildScope([]rbac.RoleBinding{b}),
	}
	ops := rbOps(Compute(desired, Live{}))
	require.Len(t, ops, 1)
	op := ops[0]
	require.Equal(t, operations.AddRoleBinding, op.Action)
	require.Equal(t, operations.RiskLow, op.Risk)
	require.False(t, op.RequiresApproval)
	require.NotNil(t, op.RoleBinding)
	require.Equal(t, "User:svc-checkout", op.RoleBinding.Principal)
	require.Equal(t, "DeveloperRead", op.RoleBinding.Role)
	require.Equal(t, "KafkaRoleBinding", op.Kind)
	require.Equal(t, "billing", op.Namespace)
	require.Equal(t, "rb-payments", op.Name)
}

// ---- RemoveRoleBinding (prune) ----

// rbPruneFixture builds desired/live for a prune test.
// desiredPrune controls whether the managing resource consented to pruning.
// live has the desired binding PLUS one extra in-scope binding (the prune candidate).
// The extra binding shares the same ScopeKey (Principal+Role+ScopeType+Cluster) but
// has a different Resource pattern — so it has a different FullKey (not in desired)
// but IS covered by the managed scope (same ScopeKey), making it a prune candidate.
func rbPruneFixture(prune bool) (Desired, Live) {
	managed := makeRB("User:svc-checkout", "DeveloperRead", prune)
	// extra: same Principal+Role+Scope (same ScopeKey → in scope) but different Resource → different FullKey.
	extra := rbac.RoleBinding{
		Principal: "User:svc-checkout",
		Role:      "DeveloperRead",
		Scope:     rbac.Scope{Type: "kafka", KafkaCluster: "lkc-abc"},
		Resource: &rbac.ResourcePattern{
			Type:        "Topic",
			Name:        "orders.", // different resource name → different FullKey
			PatternType: "prefixed",
		},
		// No SourceKind so the scope lookup falls back to the managing resource's info.
	}

	desired := Desired{
		RoleBindings:     []rbac.RoleBinding{managed},
		RoleBindingScope: rbac.BuildScope([]rbac.RoleBinding{managed}),
	}
	live := Live{
		RoleBindings: []rbac.RoleBinding{managed, extra},
	}
	return desired, live
}

func TestRoleBindingPruneWithConsentSetsAllowed(t *testing.T) {
	desired, live := rbPruneFixture(true)
	ops := rbOps(Compute(desired, live))
	require.Len(t, ops, 1)
	op := ops[0]
	require.Equal(t, operations.RemoveRoleBinding, op.Action)
	require.Equal(t, operations.RiskMedium, op.Risk)
	require.True(t, op.RequiresApproval)
	require.True(t, op.PruneAllowed, "RemoveRoleBinding must inherit scope's prune consent when true")
	require.NotNil(t, op.RoleBinding)
}

func TestRoleBindingPruneWithoutConsentStaysDisallowed(t *testing.T) {
	desired, live := rbPruneFixture(false)
	ops := rbOps(Compute(desired, live))
	require.Len(t, ops, 1)
	op := ops[0]
	require.Equal(t, operations.RemoveRoleBinding, op.Action)
	require.NotNil(t, op, "prune candidates always render, consent or not")
	require.False(t, op.PruneAllowed)
}

// ---- No-op ----

func TestRoleBindingNoOpWhenPresentInBoth(t *testing.T) {
	b := makeRB("User:svc-checkout", "DeveloperRead", false)
	desired := Desired{
		RoleBindings:     []rbac.RoleBinding{b},
		RoleBindingScope: rbac.BuildScope([]rbac.RoleBinding{b}),
	}
	live := Live{RoleBindings: []rbac.RoleBinding{b}}
	ops := rbOps(Compute(desired, live))
	require.Empty(t, ops)
}

// ---- Out-of-scope live binding ignored ----

func TestRoleBindingOutOfScopeIgnored(t *testing.T) {
	managed := makeRB("User:svc-checkout", "DeveloperRead", true)
	// different principal — out of scope
	outOfScope := makeRB("User:other", "CloudClusterAdmin", false)
	outOfScope.SourceKind, outOfScope.SourceNamespace, outOfScope.SourceName = "", "", ""

	desired := Desired{
		RoleBindings:     []rbac.RoleBinding{managed},
		RoleBindingScope: rbac.BuildScope([]rbac.RoleBinding{managed}),
	}
	live := Live{RoleBindings: []rbac.RoleBinding{managed, outOfScope}}
	ops := rbOps(Compute(desired, live))
	for _, op := range ops {
		require.NotEqual(t, operations.RemoveRoleBinding, op.Action, "must not prune out-of-scope binding")
	}
}

// ---- Cluster-scoped (nil Resource) ----

func TestRoleBindingClusterScopedAddWorks(t *testing.T) {
	b := makeClusterRB("User:admin", "SystemAdmin", false)
	desired := Desired{
		RoleBindings:     []rbac.RoleBinding{b},
		RoleBindingScope: rbac.BuildScope([]rbac.RoleBinding{b}),
	}
	ops := rbOps(Compute(desired, Live{}))
	require.Len(t, ops, 1)
	require.Equal(t, operations.AddRoleBinding, ops[0].Action)
	require.Nil(t, ops[0].RoleBinding.Resource, "cluster-scoped binding must have nil Resource")
}

// TestRoleBindingClusterScopedRemoveWorks checks that a live cluster-scoped binding
// (nil Resource, same ScopeKey as a desired resource-scoped binding) is treated as
// a prune candidate. The desired binding is resource-scoped; the live extra is
// cluster-scoped — they share a ScopeKey but have different FullKeys.
func TestRoleBindingClusterScopedRemoveWorks(t *testing.T) {
	// managed: resource-scoped (Topic, "orders.", prefixed) → establishes scope key.
	managed := rbac.RoleBinding{
		Principal:  "User:admin",
		Role:       "DeveloperRead",
		Scope:      rbac.Scope{Type: "kafka", KafkaCluster: "lkc-abc"},
		Resource:   &rbac.ResourcePattern{Type: "Topic", Name: "orders.", PatternType: "prefixed"},
		SourceKind: "KafkaRoleBinding", SourceNamespace: "ops", SourceName: "rb-orders",
		Prune: true,
	}
	// extra: cluster-scoped (nil Resource), same ScopeKey, different FullKey → prune candidate.
	extra := rbac.RoleBinding{
		Principal: "User:admin",
		Role:      "DeveloperRead",
		Scope:     rbac.Scope{Type: "kafka", KafkaCluster: "lkc-abc"},
		Resource:  nil,
	}
	desired := Desired{
		RoleBindings:     []rbac.RoleBinding{managed},
		RoleBindingScope: rbac.BuildScope([]rbac.RoleBinding{managed}),
	}
	live := Live{RoleBindings: []rbac.RoleBinding{managed, extra}}
	ops := rbOps(Compute(desired, live))
	require.Len(t, ops, 1)
	op := ops[0]
	require.Equal(t, operations.RemoveRoleBinding, op.Action)
	require.True(t, op.PruneAllowed)
	require.Nil(t, op.RoleBinding.Resource, "cluster-scoped live binding must have nil Resource in the op payload")
}

// ---- Deterministic ordering ----

func TestRoleBindingDeterministicOrdering(t *testing.T) {
	b1 := makeRB("User:svc-checkout", "DeveloperRead", false)
	b2 := makeRB("User:svc-payments", "DeveloperRead", false)
	desired := Desired{
		RoleBindings:     []rbac.RoleBinding{b2, b1}, // deliberately reversed
		RoleBindingScope: rbac.BuildScope([]rbac.RoleBinding{b2, b1}),
	}
	a := rbOps(Compute(desired, Live{}))
	b := rbOps(Compute(desired, Live{}))
	require.Equal(t, a, b)
	// Should be sorted by full key (principal sorts a < b lexically: svc-checkout < svc-payments).
	require.Len(t, a, 2)
	require.True(t, a[0].Target <= a[1].Target, "ops must be in deterministic order")
}

// ---- Mode threading (spec §16 / decision 4) ----

// TestAddRoleBindingCarriesMode verifies that an AddRoleBinding op inherits the
// Mode from the desired binding — mirroring CreateAcl's op.Mode = a.Mode.
func TestAddRoleBindingCarriesMode(t *testing.T) {
	b := makeRB("User:svc-checkout", "DeveloperRead", false)
	b.Mode = operations.ModeDetectOnly
	desired := Desired{
		RoleBindings:     []rbac.RoleBinding{b},
		RoleBindingScope: rbac.BuildScope([]rbac.RoleBinding{b}),
	}
	ops := rbOps(Compute(desired, Live{}))
	require.Len(t, ops, 1)
	op := ops[0]
	require.Equal(t, operations.AddRoleBinding, op.Action)
	require.Equal(t, operations.ModeDetectOnly, op.Mode,
		"AddRoleBinding must carry the owning binding's reconciliation mode")
}

// TestRemoveRoleBindingCarriesModeFromScope verifies that a RemoveRoleBinding
// op inherits Mode from the covering scope entry — mirroring DeleteAcl's
// op.Mode = info.Mode wiring.
func TestRemoveRoleBindingCarriesModeFromScope(t *testing.T) {
	// The managing desired binding is ObserveOnly; the live extra shares the same
	// ScopeKey but has a different FullKey (different resource) — prune candidate.
	managed := makeRB("User:svc-checkout", "DeveloperRead", true)
	managed.Mode = operations.ModeObserveOnly
	extra := rbac.RoleBinding{
		Principal: "User:svc-checkout",
		Role:      "DeveloperRead",
		Scope:     rbac.Scope{Type: "kafka", KafkaCluster: "lkc-abc"},
		Resource: &rbac.ResourcePattern{
			Type:        "Topic",
			Name:        "orders.",
			PatternType: "prefixed",
		},
	}
	desired := Desired{
		RoleBindings:     []rbac.RoleBinding{managed},
		RoleBindingScope: rbac.BuildScope([]rbac.RoleBinding{managed}),
	}
	live := Live{RoleBindings: []rbac.RoleBinding{managed, extra}}

	ops := rbOps(Compute(desired, live))
	require.Len(t, ops, 1)
	op := ops[0]
	require.Equal(t, operations.RemoveRoleBinding, op.Action)
	require.Equal(t, operations.ModeObserveOnly, op.Mode,
		"RemoveRoleBinding must carry the covering scope entry's reconciliation mode")
}

// TestAddRoleBindingDefaultModeIsEmpty verifies that an unattributed binding
// (no Reconciliation set) produces op.Mode == "" — the executor treats "" as
// Enforce (spec §16), consistent with CreateAcl behaviour.
func TestAddRoleBindingDefaultModeIsEmpty(t *testing.T) {
	b := makeRB("User:svc-checkout", "DeveloperRead", false)
	// Mode is "" (zero value) — no Reconciliation set.
	desired := Desired{
		RoleBindings:     []rbac.RoleBinding{b},
		RoleBindingScope: rbac.BuildScope([]rbac.RoleBinding{b}),
	}
	ops := rbOps(Compute(desired, Live{}))
	require.Len(t, ops, 1)
	require.Empty(t, ops[0].Mode,
		"unattributed binding must produce empty Mode (executor treats '' as Enforce)")
}

// ---- RoleBindingPruneDesired keep-set (spec §40 cross-resource anti-flap) ----

// TestRoleBindingPruneKeepSetSparesOtherOwners verifies that with a cluster-wide
// prune keep-set, a live binding desired by ANOTHER resource (present in
// RoleBindingPruneDesired but not in this resource's RoleBindings) is NOT pruned
// — the cross-resource anti-flap guarantee (spec §40).
func TestRoleBindingPruneKeepSetSparesOtherOwners(t *testing.T) {
	mine := makeRB("User:svc", "DeveloperRead", true)
	other := mine
	other.Resource = &rbac.ResourcePattern{Type: "Topic", Name: "orders.", PatternType: "prefixed"}

	d := Desired{
		RoleBindings:     []rbac.RoleBinding{mine}, // this resource's own desired (creates)
		RoleBindingScope: rbac.BuildScope([]rbac.RoleBinding{mine, other}),
	}
	d.SetRoleBindingPruneSet([]rbac.RoleBinding{mine, other}) // cluster-wide keep-set
	live := Live{RoleBindings: []rbac.RoleBinding{mine, other}}
	ops := rbOps(Compute(d, live))
	for _, op := range ops {
		if op.Action == operations.RemoveRoleBinding {
			t.Fatalf("other owner's live binding must not be pruned, got remove of %+v", op.RoleBinding)
		}
	}
}

// TestRoleBindingPruneRemovesUnowned verifies that a live binding within scope
// and desired by nobody is still pruned.
func TestRoleBindingPruneRemovesUnowned(t *testing.T) {
	mine := makeRB("User:svc", "DeveloperRead", true)
	stray := mine
	stray.Resource = &rbac.ResourcePattern{Type: "Topic", Name: "stray.", PatternType: "prefixed"}

	d := Desired{
		RoleBindings:     []rbac.RoleBinding{mine},
		RoleBindingScope: rbac.BuildScope([]rbac.RoleBinding{mine}),
	}
	d.SetRoleBindingPruneSet([]rbac.RoleBinding{mine})
	live := Live{RoleBindings: []rbac.RoleBinding{mine, stray}}
	ops := rbOps(Compute(d, live))
	var removed bool
	for _, op := range ops {
		if op.Action == operations.RemoveRoleBinding && op.RoleBinding.Resource != nil && op.RoleBinding.Resource.Name == "stray." {
			removed = true
		}
	}
	if !removed {
		t.Fatal("unowned in-scope live binding must be pruned")
	}
}

// TestRoleBindingPruneNilKeepSetUsesRoleBindings verifies back-compat: when
// RoleBindingPruneAggregateSet is left false (the zero value — no aggregate
// supplied), RoleBindings is the keep-set (CLI single-aggregate-set
// semantics) — existing behaviour preserved.
func TestRoleBindingPruneNilKeepSetUsesRoleBindings(t *testing.T) {
	mine := makeRB("User:svc", "DeveloperRead", true)
	stray := mine
	stray.Resource = &rbac.ResourcePattern{Type: "Topic", Name: "stray.", PatternType: "prefixed"}

	d := Desired{
		RoleBindings:     []rbac.RoleBinding{mine},
		RoleBindingScope: rbac.BuildScope([]rbac.RoleBinding{mine}),
		// RoleBindingPruneAggregateSet left false (zero value) → keep-set falls
		// back to RoleBindings.
	}
	live := Live{RoleBindings: []rbac.RoleBinding{mine, stray}}
	ops := rbOps(Compute(d, live))
	var removed bool
	for _, op := range ops {
		if op.Action == operations.RemoveRoleBinding && op.RoleBinding.Resource != nil && op.RoleBinding.Resource.Name == "stray." {
			removed = true
		}
	}
	if !removed {
		t.Fatal("unset aggregate must fall back to RoleBindings; stray (in scope, not desired) must prune")
	}
}

// TestRoleBindingPruneAccidentallyEmptySliceNoLongerMassPrunes pins the M6
// fix: assigning RoleBindingPruneDesired directly to a non-nil-but-empty
// slice — the old footgun ("callers must pass nil, not an empty slice") — no
// longer activates the keep-set, because the switch is the explicit
// RoleBindingPruneAggregateSet boolean, not RoleBindingPruneDesired's
// nilness. Without RoleBindingPruneAggregateSet, an accidentally-empty
// RoleBindingPruneDesired is simply ignored and RoleBindings (which contains
// "mine") is the keep-set — so "mine" is correctly spared, not mass-pruned.
func TestRoleBindingPruneAccidentallyEmptySliceNoLongerMassPrunes(t *testing.T) {
	mine := makeRB("User:svc", "DeveloperRead", true)

	d := Desired{
		RoleBindings:            []rbac.RoleBinding{mine},
		RoleBindingPruneDesired: []rbac.RoleBinding{}, // non-nil empty — the old footgun trigger
		RoleBindingScope:        rbac.BuildScope([]rbac.RoleBinding{mine}),
		// RoleBindingPruneAggregateSet NOT set (zero value: false).
	}
	live := Live{RoleBindings: []rbac.RoleBinding{mine}}
	ops := rbOps(Compute(d, live))
	for _, op := range ops {
		if op.Action == operations.RemoveRoleBinding {
			t.Fatalf("an accidentally-empty (but not aggregate-set) keep-set must not mass-prune, got remove of %+v", op.RoleBinding)
		}
	}
}

// TestRoleBindingPruneExplicitEmptyAggregatePrunesEverything verifies the
// converse, legitimate case: when a caller explicitly supplies an empty
// cluster-wide aggregate via SetRoleBindingPruneSet (e.g. a cluster-wide view
// with zero contributing resources), the keep-set really is empty and every
// in-scope live binding is a prune candidate — this is correct behaviour, not
// the footgun, because it was reached through the explicit setter rather than
// an accidental empty-slice assignment.
func TestRoleBindingPruneExplicitEmptyAggregatePrunesEverything(t *testing.T) {
	mine := makeRB("User:svc", "DeveloperRead", true)

	d := Desired{
		RoleBindings:     []rbac.RoleBinding{mine},
		RoleBindingScope: rbac.BuildScope([]rbac.RoleBinding{mine}),
	}
	d.SetRoleBindingPruneSet(nil) // explicit: cluster-wide aggregate has zero contributors
	live := Live{RoleBindings: []rbac.RoleBinding{mine}}
	ops := rbOps(Compute(d, live))
	var removed bool
	for _, op := range ops {
		if op.Action == operations.RemoveRoleBinding && op.RoleBinding.Principal == mine.Principal {
			removed = true
		}
	}
	if !removed {
		t.Fatal("explicit empty aggregate must prune everything in scope, including this resource's own binding")
	}
}
