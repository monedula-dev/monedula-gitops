package access

import "github.com/monedula-dev/monedula-gitops/internal/managedset"

// ScopeKey identifies a (resource pattern, principal) pair in the managed scope.
type ScopeKey struct {
	Principal    string
	ResourceType string
	ResourceName string
	PatternType  string
}

func scopeKeyOf(a ACL) ScopeKey {
	return ScopeKey{Principal: a.Principal, ResourceType: a.ResourceType, ResourceName: a.ResourceName, PatternType: a.PatternType}
}

// ScopeInfo is the attribution carried by a managed-scope entry: the
// reconciliation mode, prune consent, and owning resource that govern live
// ACLs covered by the entry (e.g. prune candidates). Mode is the MOST
// ENFORCING mode among the resources contributing to the entry; Source* is
// the FIRST contributor (deterministic given ordered desired ACLs).
//
// Prune is the opposite merge (spec §10.3): an entry consents to pruning only
// if EVERY contributor opted in (AND-merge). Mode merges toward acting
// (most-enforcing wins) because enforcement is the declared intent of at
// least one owner; Prune merges toward NOT deleting because a prune executes
// destructive deletion on a tuple that several resources may share — one
// non-consenting owner must be able to veto it. The type (and the merge) is
// managedset.ScopeInfo — the single shared implementation.
type ScopeInfo = managedset.ScopeInfo

// scopeInfoOf extracts an ACL's attribution as a scope entry.
func scopeInfoOf(a ACL) ScopeInfo {
	return ScopeInfo{Mode: a.Mode, Prune: a.Prune, SourceKind: a.SourceKind, SourceNamespace: a.SourceNamespace, SourceName: a.SourceName}
}

// ManagedScope is the set of (resource pattern, principal) pairs the loaded
// manifests reference, each carrying the attribution of its contributors.
type ManagedScope map[ScopeKey]ScopeInfo

// BuildScope derives the managed scope from the desired ACLs. When several
// ACLs share a scope key, the most-enforcing mode wins, prune consent is
// AND-merged (every contributor must opt in — see ScopeInfo), and the first
// contributor keeps owner attribution (mirroring BuildDesiredSet's rule).
func BuildScope(desired []ACL) ManagedScope {
	return ManagedScope(managedset.BuildScope(desired, scopeKeyOf, scopeInfoOf))
}

// Contains reports whether a live ACL falls within the managed scope.
func (s ManagedScope) Contains(a ACL) bool {
	_, ok := s[scopeKeyOf(a)]
	return ok
}

// Info returns the attribution of the scope entry covering a live ACL, and
// whether such an entry exists.
func (s ManagedScope) Info(a ACL) (ScopeInfo, bool) {
	info, ok := s[scopeKeyOf(a)]
	return info, ok
}
