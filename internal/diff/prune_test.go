package diff

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/monedula-dev/monedula-gitops/internal/access"
	"github.com/monedula-dev/monedula-gitops/internal/operations"
)

// pruneFixture returns a desired set with one ACL (consenting to pruning or
// not) and a live set containing that ACL plus one in-scope undesired ACL.
func pruneFixture(prune bool) (Desired, Live) {
	desiredACL := access.ACL{
		Principal: "User:x", Host: "*", ResourceType: "topic", ResourceName: "t",
		PatternType: "literal", Operation: "Read", Permission: "Allow",
		Mode: operations.ModeEnforce, Prune: prune,
	}
	liveExtra := desiredACL
	liveExtra.Operation = "Write"
	liveExtra.Mode, liveExtra.Prune = "", false
	desired := Desired{ACLs: []access.ACL{desiredACL}, Scope: access.BuildScope([]access.ACL{desiredACL})}
	live := Live{ACLs: []access.ACL{desiredACL, liveExtra}}
	return desired, live
}

func TestPruneAclCarriesConsentFromScope(t *testing.T) {
	desired, live := pruneFixture(true)
	ops := Compute(desired, live)
	op := findOp(ops, operations.DeleteAcl)
	require.NotNil(t, op)
	require.True(t, op.PruneAllowed, "DeleteAcl must inherit the covering scope's prune consent")
}

func TestPruneAclWithoutConsentStaysDisallowed(t *testing.T) {
	desired, live := pruneFixture(false)
	ops := Compute(desired, live)
	op := findOp(ops, operations.DeleteAcl)
	require.NotNil(t, op, "prune candidates always render, consent or not")
	require.False(t, op.PruneAllowed)
}

// ---- PruneDesired/PruneScope cluster-wide aggregate (M6: explicit sentinel) ----

// TestPruneAclAggregateSparesOtherOwners verifies that with a cluster-wide
// prune aggregate (SetPruneAggregate), a live ACL desired by ANOTHER resource
// (present in the aggregate but not in this resource's own ACLs) is NOT
// pruned — the ACL analogue of the role-binding cross-resource anti-flap
// guarantee (spec §10.4/§20.1).
func TestPruneAclAggregateSparesOtherOwners(t *testing.T) {
	mine := access.ACL{
		Principal: "User:x", Host: "*", ResourceType: "topic", ResourceName: "t",
		PatternType: "literal", Operation: "Read", Permission: "Allow",
	}
	other := mine
	other.Operation = "Write"

	d := Desired{ACLs: []access.ACL{mine}} // this resource's own desired (creates)
	d.SetPruneAggregate([]access.ACL{mine, other}, access.BuildScope([]access.ACL{mine, other}))
	live := Live{ACLs: []access.ACL{mine, other}}
	ops := Compute(d, live)
	for _, op := range ops {
		if op.Action == operations.DeleteAcl {
			t.Fatalf("other owner's live ACL must not be pruned, got delete of %+v", op.ACL)
		}
	}
}

// TestPruneAclAccidentallyEmptySliceNoLongerMassPrunes pins the M6 fix on the
// ACL side: assigning PruneDesired/PruneScope directly (the old footgun —
// "callers must pass nil, not an empty slice") no longer activates the
// aggregate, because the switch is the explicit PruneAggregateSet boolean,
// not PruneScope's nilness. Without PruneAggregateSet, the per-resource
// ACLs/Scope govern pruning as before, so "mine" (in both ACLs and live) is
// correctly spared.
func TestPruneAclAccidentallyEmptySliceNoLongerMassPrunes(t *testing.T) {
	mine := access.ACL{
		Principal: "User:x", Host: "*", ResourceType: "topic", ResourceName: "t",
		PatternType: "literal", Operation: "Read", Permission: "Allow",
	}

	d := Desired{
		ACLs:  []access.ACL{mine},
		Scope: access.BuildScope([]access.ACL{mine}),
		// PruneDesired/PruneScope assigned directly to non-nil-empty values —
		// the old footgun trigger — but PruneAggregateSet is left false.
		PruneDesired: []access.ACL{},
		PruneScope:   access.ManagedScope{},
	}
	live := Live{ACLs: []access.ACL{mine}}
	ops := Compute(d, live)
	for _, op := range ops {
		if op.Action == operations.DeleteAcl {
			t.Fatalf("an accidentally-empty (but not aggregate-set) prune aggregate must not mass-prune, got delete of %+v", op.ACL)
		}
	}
}

// TestPruneAclExplicitEmptyAggregatePrunesEverything verifies the converse,
// legitimate case: when a caller explicitly supplies an empty cluster-wide
// aggregate via SetPruneAggregate (e.g. a cluster-wide view with zero
// contributing resources), every in-scope live ACL is a genuine prune
// candidate.
func TestPruneAclExplicitEmptyAggregatePrunesEverything(t *testing.T) {
	mine := access.ACL{
		Principal: "User:x", Host: "*", ResourceType: "topic", ResourceName: "t",
		PatternType: "literal", Operation: "Read", Permission: "Allow", Prune: true,
	}

	d := Desired{
		ACLs:  []access.ACL{mine},
		Scope: access.BuildScope([]access.ACL{mine}),
	}
	d.SetPruneAggregate(nil, access.BuildScope([]access.ACL{mine})) // explicit: zero contributors in the aggregate
	live := Live{ACLs: []access.ACL{mine}}
	ops := Compute(d, live)
	op := findOp(ops, operations.DeleteAcl)
	require.NotNil(t, op, "explicit empty aggregate must prune this resource's own ACL too")
}
