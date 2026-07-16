// Package managedset is the single implementation of the managed-set
// machinery that internal/access (ACLs) and internal/rbac (MDS role bindings)
// instantiate: identity-keyed desired-set building with dedupe and conflict
// detection, managed-scope derivation, and the attribution-merge semantics
// both share.
//
// The merge semantics are the spec's, in one place:
//
//   - Mode merges toward acting — the MOST ENFORCING reconciliation mode among
//     the contributors wins (Enforce > DetectOnly > ObserveOnly > unattributed,
//     see ModeRank) — because enforcement is the declared intent of at least
//     one owner (spec §16).
//   - Prune consent merges the OPPOSITE way (spec §10.3): an entry consents to
//     pruning only if EVERY contributor opted in (AND-merge), because a prune
//     executes destructive deletion on a tuple several resources may share —
//     one non-consenting owner must be able to veto it.
//   - Owner attribution (Source*) stays with the FIRST contributor, which is
//     deterministic given deterministically-ordered input.
//
// Deliberate per-kind divergences are NOT unified here; they enter as hooks:
// access detects Allow/Deny conflicts across two DIFFERENT identity keys (a
// stateful Reject hook), rbac detects explicit-vs-explicit identity collisions
// against the already-accepted element (a Conflict hook), and rbac sorts its
// deduped output by identity while access preserves input order (the caller
// sorts, or doesn't, after BuildDesiredSet returns).
package managedset

// ModeRank orders reconciliation modes by how enforcing they are. It backs the
// "most enforcing wins" rule used by BuildDesiredSet and BuildScope when
// several resources contribute the same tuple/scope entry:
// Enforce > DetectOnly > ObserveOnly > unattributed ("").
func ModeRank(mode string) int {
	switch mode {
	case "Enforce":
		return 3
	case "DetectOnly":
		return 2
	case "ObserveOnly":
		return 1
	default:
		return 0
	}
}

// ScopeInfo is the attribution carried by a managed-scope entry: the
// reconciliation mode, prune consent, and owning resource that govern live
// tuples covered by the entry (e.g. prune candidates). Mode is the MOST
// ENFORCING mode among the resources contributing to the entry; Source* is
// the FIRST contributor (deterministic given ordered desired input); Prune is
// AND-merged consent (see the package doc for why the two merge in opposite
// directions). internal/access and internal/rbac alias this type, so their
// public ScopeInfo shapes stay what they always were.
type ScopeInfo struct {
	Mode            string
	Prune           bool
	SourceKind      string
	SourceNamespace string
	SourceName      string
}

// MergeAttribution folds an incoming contributor's mode and prune consent into
// an existing entry's: the most-enforcing mode wins and prune consent
// AND-merges. Owner attribution is deliberately NOT part of this merge — the
// first contributor keeps it.
func MergeAttribution(mode *string, prune *bool, incomingMode string, incomingPrune bool) {
	if ModeRank(incomingMode) > ModeRank(*mode) {
		*mode = incomingMode
	}
	*prune = *prune && incomingPrune
}

// merge applies MergeAttribution to a ScopeInfo entry.
func (i *ScopeInfo) merge(incoming ScopeInfo) {
	MergeAttribution(&i.Mode, &i.Prune, incoming.Mode, incoming.Prune)
}

// Attribution exposes an element type's Mode and Prune attribution fields to
// the generic merge, without reflection. Get reads both; Set writes both. The
// per-kind packages supply trivial field plumbing; the merge SEMANTICS stay
// here (MergeAttribution).
type Attribution[T any] struct {
	Get func(T) (mode string, prune bool)
	Set func(*T, string, bool)
}

// BuildDesiredSet dedupes items by their identity key while merging
// attribution: when the same identity is contributed more than once, the
// survivor takes the most-enforcing mode, keeps Prune=true only if EVERY
// contributor opted in, and retains every other field — including Source*
// owner attribution — from the FIRST contributor. Output preserves first-seen
// input order (callers wanting key order sort afterwards).
//
// Two optional conflict hooks capture the per-kind rejection rules; a non-nil
// error from either drops the incoming item from the output and is reported:
//
//   - reject, when non-nil, is consulted for EVERY incoming item BEFORE the
//     identity lookup. It may carry its own state across the walk — access
//     uses it for the Allow/Deny subject conflict, which spans two DIFFERENT
//     identity keys (the permissions differ, so the full keys differ).
//   - conflict, when non-nil, is consulted only when an incoming item collides
//     with an already-accepted item of the SAME identity; the error suppresses
//     the merge — rbac uses it for the explicit-vs-explicit identity-uniqueness
//     collision (spec §40), which needs the existing element's attribution.
func BuildDesiredSet[T any](
	items []T,
	key func(T) string,
	attr Attribution[T],
	reject func(T) error,
	conflict func(existing, incoming T) error,
) ([]T, []error) {
	index := map[string]int{} // identity key -> index into out
	var out []T
	var errs []error
	for _, it := range items {
		if reject != nil {
			if err := reject(it); err != nil {
				errs = append(errs, err)
				continue
			}
		}
		k := key(it)
		if i, ok := index[k]; ok {
			if conflict != nil {
				if err := conflict(out[i], it); err != nil {
					errs = append(errs, err)
					continue
				}
			}
			mode, prune := attr.Get(out[i])
			inMode, inPrune := attr.Get(it)
			MergeAttribution(&mode, &prune, inMode, inPrune)
			attr.Set(&out[i], mode, prune)
			continue
		}
		index[k] = len(out)
		out = append(out, it)
	}
	return out, errs
}

// BuildScope derives the managed scope from desired items: the set of scope
// keys the items reference, each carrying the merged attribution of its
// contributors (most-enforcing mode wins, prune consent AND-merges, the first
// contributor keeps owner attribution — mirroring BuildDesiredSet's rule).
// The per-kind packages wrap the returned map in their own ManagedScope types,
// whose Contains/Info stay plain lookups.
func BuildScope[T any, K comparable](items []T, key func(T) K, info func(T) ScopeInfo) map[K]ScopeInfo {
	s := map[K]ScopeInfo{}
	for _, it := range items {
		k := key(it)
		existing, ok := s[k]
		if !ok {
			s[k] = info(it)
			continue
		}
		existing.merge(info(it))
		s[k] = existing
	}
	return s
}
