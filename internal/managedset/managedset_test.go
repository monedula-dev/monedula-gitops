package managedset

import (
	"errors"
	"fmt"
	"testing"
)

// item is a minimal local element type: the generic core is tested in
// isolation, deliberately NOT through internal/access or internal/rbac (their
// suites pin the instantiations; these tests pin the generic's own contract).
type item struct {
	Key    string
	Mode   string
	Prune  bool
	Source string
}

var itemAttr = Attribution[item]{
	Get: func(it item) (string, bool) { return it.Mode, it.Prune },
	Set: func(it *item, mode string, prune bool) { it.Mode, it.Prune = mode, prune },
}

func itemKey(it item) string { return it.Key }

func TestBuildDesiredSetPreservesInputOrder(t *testing.T) {
	// Deliberately unsorted keys: output must be first-seen input order, not
	// key order (access relies on this; a future internal sort or map-range
	// iteration would silently break its documented ordering contract).
	in := []item{{Key: "z"}, {Key: "a"}, {Key: "m"}, {Key: "a"}, {Key: "b"}}
	out, errs := BuildDesiredSet(in, itemKey, itemAttr, nil, nil)
	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}
	want := []string{"z", "a", "m", "b"}
	if len(out) != len(want) {
		t.Fatalf("got %d items, want %d", len(out), len(want))
	}
	for i, k := range want {
		if out[i].Key != k {
			t.Errorf("out[%d].Key = %q, want %q (input order must be preserved)", i, out[i].Key, k)
		}
	}
}

func TestBuildDesiredSetMergeCommutativeAttributionFirstWins(t *testing.T) {
	a := item{Key: "k", Mode: "ObserveOnly", Prune: true, Source: "A"}
	b := item{Key: "k", Mode: "Enforce", Prune: false, Source: "B"}
	for _, tc := range []struct {
		name       string
		in         []item
		wantSource string
	}{
		{"a-then-b", []item{a, b}, "A"},
		{"b-then-a", []item{b, a}, "B"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, errs := BuildDesiredSet(tc.in, itemKey, itemAttr, nil, nil)
			if len(errs) != 0 || len(out) != 1 {
				t.Fatalf("got %d items, %v errors; want 1 item, none", len(out), errs)
			}
			// Mode/Prune merge is commutative: most-enforcing mode, AND-ed prune.
			if out[0].Mode != "Enforce" || out[0].Prune != false {
				t.Errorf("merged Mode=%q Prune=%v, want Enforce/false regardless of order", out[0].Mode, out[0].Prune)
			}
			// Attribution is order-dependent BY DESIGN: first contributor keeps it.
			if out[0].Source != tc.wantSource {
				t.Errorf("merged Source = %q, want first contributor %q", out[0].Source, tc.wantSource)
			}
		})
	}
}

func TestBuildDesiredSetConflictHookDropsIncomingAndSuppressesMerge(t *testing.T) {
	in := []item{
		{Key: "k", Mode: "ObserveOnly", Prune: true, Source: "A"},
		{Key: "k", Mode: "Enforce", Prune: false, Source: "B"},
	}
	conflictErr := errors.New("collision")
	conflict := func(existing, incoming item) error {
		// The hook must see (existing, incoming) in that order.
		if existing.Source != "A" || incoming.Source != "B" {
			t.Errorf("conflict(existing=%q, incoming=%q), want (A, B)", existing.Source, incoming.Source)
		}
		return conflictErr
	}
	out, errs := BuildDesiredSet(in, itemKey, itemAttr, nil, conflict)
	if len(errs) != 1 || !errors.Is(errs[0], conflictErr) {
		t.Fatalf("errs = %v, want exactly the conflict error", errs)
	}
	if len(out) != 1 {
		t.Fatalf("got %d items, want 1 (incoming dropped)", len(out))
	}
	// The merge must be suppressed: existing keeps its own Mode/Prune.
	if out[0].Mode != "ObserveOnly" || out[0].Prune != true || out[0].Source != "A" {
		t.Errorf("existing mutated to Mode=%q Prune=%v Source=%q; conflict must suppress the merge", out[0].Mode, out[0].Prune, out[0].Source)
	}
}

func TestBuildDesiredSetRejectHookSeesEveryItemBeforeIdentity(t *testing.T) {
	in := []item{{Key: "k", Source: "1"}, {Key: "k", Source: "2"}, {Key: "x", Source: "3"}}
	var seen []string
	reject := func(it item) error {
		seen = append(seen, it.Source)
		if it.Source == "2" || it.Source == "3" {
			return fmt.Errorf("rejected %s", it.Source)
		}
		return nil
	}
	out, errs := BuildDesiredSet(in, itemKey, itemAttr, reject, nil)
	// Consulted for EVERY item — including the exact-key duplicate — before
	// the identity lookup.
	if fmt.Sprint(seen) != "[1 2 3]" {
		t.Errorf("reject saw %v, want every item in input order", seen)
	}
	// Errors accumulate in input order.
	if len(errs) != 2 || errs[0].Error() != "rejected 2" || errs[1].Error() != "rejected 3" {
		t.Errorf("errs = %v, want [rejected 2, rejected 3]", errs)
	}
	if len(out) != 1 || out[0].Source != "1" {
		t.Errorf("out = %v, want only the accepted first item", out)
	}
}

func TestBuildScopeModeRankAndPruneVeto(t *testing.T) {
	info := func(it item) ScopeInfo {
		return ScopeInfo{Mode: it.Mode, Prune: it.Prune, SourceName: it.Source}
	}
	// Unattributed "" ranks below ObserveOnly: the entry upgrades.
	s := BuildScope([]item{
		{Key: "k", Mode: "", Prune: true, Source: "A"},
		{Key: "k", Mode: "ObserveOnly", Prune: true, Source: "B"},
	}, itemKey, info)
	if got := s["k"]; got.Mode != "ObserveOnly" || got.SourceName != "A" {
		t.Errorf("entry = %+v, want Mode=ObserveOnly (\"\" loses) with first contributor A", got)
	}
	// Prune AND-merge: one dissenter among consenters vetoes.
	s = BuildScope([]item{
		{Key: "k", Prune: true},
		{Key: "k", Prune: false},
		{Key: "k", Prune: true},
	}, itemKey, info)
	if s["k"].Prune {
		t.Error("Prune = true after a non-consenting contributor; AND-merge must veto")
	}
}
