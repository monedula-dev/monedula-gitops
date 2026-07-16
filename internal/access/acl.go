package access

import (
	"fmt"

	"github.com/monedula-dev/monedula-gitops/internal/managedset"
)

// ACL is the canonical Kafka ACL tuple — the full ownership identity.
type ACL struct {
	Principal    string
	Host         string
	ResourceType string
	ResourceName string
	PatternType  string
	Operation    string
	Permission   string

	// Mode and Source* are ATTRIBUTION, not identity: they record the
	// reconciliation mode (spec §16) and owning resource of the manifest that
	// compiled this tuple. They are deliberately EXCLUDED from subjectKey and
	// FullKey, so two resources desiring the same tuple still dedupe (and
	// Allow/Deny-conflict) as one ACL regardless of mode/owner. The CLI
	// pipeline stamps them after compilation; the operator path leaves them
	// empty (it decides modes per resource before calling the executor).
	Mode            string // Enforce | DetectOnly | ObserveOnly | "" (unattributed)
	SourceKind      string // KafkaTopic | KafkaAccessPolicy
	SourceNamespace string
	SourceName      string

	// Prune is the owning resource's opt-in prune consent (spec §10.3,
	// spec.prune). Attribution like Mode/Source*: excluded from identity. The
	// OPERATOR stamps it from the resource's spec so the scope can carry
	// per-resource consent; the CLI pipeline deliberately leaves it false —
	// in CLI mode `apply --prune` is THE prune switch (run-wide consent via
	// executor Approvals.Prune), spec.prune is not consulted.
	Prune bool
}

// StampPrune sets the owning resource's spec.prune consent on every desired
// ACL so BuildScope carries it into the managed scope (and the diff onto each
// DeleteAcl's PruneAllowed). Operator-only: both the per-resource reconcile
// core and the controllers' cluster-wide desired-set aggregation (spec §20.1)
// stamp consent with it; BuildScope's AND-merge then guarantees a prune
// executes only when EVERY covering resource opted in (spec §10.3). The CLI
// pipeline never calls it — there `apply --prune` is the run-wide consent.
func StampPrune(acls []ACL, prune bool) {
	for i := range acls {
		acls[i].Prune = prune
	}
}

// StampSource sets the owning-resource attribution on every ACL (excluded from
// identity; used to name conflict parties and scope owners).
func StampSource(acls []ACL, kind, namespace, name string) {
	for i := range acls {
		acls[i].SourceKind = kind
		acls[i].SourceNamespace = namespace
		acls[i].SourceName = name
	}
}

// aclAttribution plumbs an ACL's Mode/Prune attribution fields to the shared
// managed-set merge (most-enforcing mode wins per managedset.ModeRank; prune
// consent AND-merges). The merge SEMANTICS live in managedset; this is field
// access only.
var aclAttribution = managedset.Attribution[ACL]{
	Get: func(a ACL) (string, bool) { return a.Mode, a.Prune },
	Set: func(a *ACL, mode string, prune bool) { a.Mode, a.Prune = mode, prune },
}

// subjectKey is the ACL identity WITHOUT permission — used to detect Allow/Deny conflicts.
// NUL (\x00) separators keep field boundaries unambiguous: a principal (or
// other field) containing the codebase's old "|" join character can no
// longer alias two distinct tuples (mirrors the aclKey/user.Key convention
// used elsewhere, e.g. internal/importer, internal/user).
func (a ACL) subjectKey() string {
	return fmt.Sprintf("%s\x00%s\x00%s\x00%s\x00%s\x00%s", a.Principal, a.Host, a.ResourceType, a.ResourceName, a.PatternType, a.Operation)
}

// FullKey returns the full 7-field ownership identity (subjectKey plus
// Permission), the canonical string used to match and order ACLs. It is the
// single source of truth — callers outside this package (e.g. internal/diff)
// must route through it rather than rebuilding the key locally. Fields are
// NUL-separated (see subjectKey); this is an in-memory identity key only —
// never persisted, serialized, or parsed back.
func (a ACL) FullKey() string { return a.subjectKey() + "\x00" + a.Permission }

func (a ACL) fullKey() string { return a.FullKey() }
