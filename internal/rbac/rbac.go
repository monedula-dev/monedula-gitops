// Package rbac compiles KafkaRoleBinding resources into MDS role-binding tuples,
// deduplicates them by identity, and builds the managed scope used for prune
// decisions (spec §40).
package rbac

import (
	"fmt"
	"sort"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/managedset"
)

// Scope is a resolved MDS scope: the kafka-cluster id (always present) plus,
// for non-kafka scope types, the matching sub-cluster id.
type Scope struct {
	Type         string // kafka | schema-registry | connect | ksql
	KafkaCluster string
	SubCluster   string // SR/connect/ksql cluster id; "" for kafka scope
}

// ResourcePattern is one MDS resource pattern (nil on a cluster-scoped binding).
type ResourcePattern struct {
	Type        string
	Name        string
	PatternType string // literal | prefixed
}

// RoleBinding is one desired MDS role binding (one resource pattern, or none for
// cluster-scoped). Source* carry owning-resource attribution (excluded from identity).
type RoleBinding struct {
	Principal string
	Role      string
	Scope     Scope
	Resource  *ResourcePattern

	// Mode and Source* are ATTRIBUTION, not identity: they record the
	// reconciliation mode (spec §16) and owning resource of the manifest that
	// compiled this tuple. They are deliberately EXCLUDED from FullKey, so two
	// resources desiring the same binding still dedupe as one regardless of
	// mode/owner. Mirrors access.ACL's Mode/Source* semantics exactly.
	Mode            string // Enforce | DetectOnly | ObserveOnly | "" (unattributed)
	SourceKind      string
	SourceNamespace string
	SourceName      string

	// Prune is the owning resource's opt-in prune consent (spec §10.3,
	// spec.prune). Attribution like Source*: excluded from identity. Stamped by
	// StampPrune from the resource's spec so the scope carries per-resource
	// consent; AND-merge in BuildDesiredSet guarantees a prune executes only when
	// EVERY covering resource opted in.
	Prune bool
}

// bindingAttribution plumbs a RoleBinding's Mode/Prune attribution fields to
// the shared managed-set merge (most-enforcing mode wins per
// managedset.ModeRank; prune consent AND-merges). The merge SEMANTICS live in
// managedset; this is field access only.
var bindingAttribution = managedset.Attribution[RoleBinding]{
	Get: func(b RoleBinding) (string, bool) { return b.Mode, b.Prune },
	Set: func(b *RoleBinding, mode string, prune bool) { b.Mode, b.Prune = mode, prune },
}

// resourceKey returns the stable string for the resource pattern (or "" for
// cluster-scoped nil resources).
func resourceKey(r *ResourcePattern) string {
	if r == nil {
		return ""
	}
	return fmt.Sprintf("%s\x00%s\x00%s", r.Type, r.Name, r.PatternType)
}

// FullKey is the stable identity of a binding (Principal, Role, Scope, Resource);
// Source* and Prune are excluded. Fields are NUL (\x00) separated so a
// principal (or other field) containing "|" cannot alias two distinct
// bindings (mirrors the aclKey/user.Key convention used elsewhere in this
// codebase). Layout MUST stay byte-identical to mds.RoleBinding.Key —
// TestRoleBindingKeyLayoutInvariant in internal/importer pins this.
// This is an in-memory identity key only — never persisted, serialized, or
// parsed back.
func (b RoleBinding) FullKey() string {
	rk := resourceKey(b.Resource)
	return fmt.Sprintf("%s\x00%s\x00%s\x00%s\x00%s\x00%s",
		b.Principal,
		b.Role,
		b.Scope.Type,
		b.Scope.KafkaCluster,
		b.Scope.SubCluster,
		rk,
	)
}

// resolveScope builds the MDS Scope from the spec's scope type and the MDS
// cluster configuration. Returns an error if a required cluster id is missing.
func resolveScope(scopeType string, clusters v1alpha1.MDSClusters) (Scope, error) {
	if clusters.KafkaCluster == "" {
		return Scope{}, fmt.Errorf("rbac: MDSClusters.KafkaCluster is required but empty")
	}

	s := Scope{
		Type:         scopeType,
		KafkaCluster: clusters.KafkaCluster,
	}

	switch scopeType {
	case "kafka":
		// SubCluster stays "".
	case "schema-registry":
		if clusters.SchemaRegistryCluster == "" {
			return Scope{}, fmt.Errorf("rbac: MDSClusters.SchemaRegistryCluster is required for scope type %q but empty", scopeType)
		}
		s.SubCluster = clusters.SchemaRegistryCluster
	case "connect":
		if clusters.ConnectCluster == "" {
			return Scope{}, fmt.Errorf("rbac: MDSClusters.ConnectCluster is required for scope type %q but empty", scopeType)
		}
		s.SubCluster = clusters.ConnectCluster
	case "ksql":
		if clusters.KsqlCluster == "" {
			return Scope{}, fmt.Errorf("rbac: MDSClusters.KsqlCluster is required for scope type %q but empty", scopeType)
		}
		s.SubCluster = clusters.KsqlCluster
	default:
		// Unknown scope type — pass through; validation (internal/validation) enforces this.
		// Compile is structural only; we can still build the scope here.
	}

	return s, nil
}

// Compile compiles a KafkaRoleBinding into one or more RoleBindings. It is
// purely structural: it resolves scope IDs and expands resource patterns, but
// does NOT validate role classification or resource-presence (validation (internal/validation) enforces this; Compile is structural only).
// It returns an error only when the MDS scope cannot be resolved (missing
// cluster IDs), since no binding can be built without them.
func Compile(rb *v1alpha1.KafkaRoleBinding, mds *v1alpha1.MDSConfig) ([]RoleBinding, error) {
	scope, err := resolveScope(rb.Spec.Scope.Type, mds.Clusters)
	if err != nil {
		return nil, err
	}

	// Extract reconciliation.mode: nil Reconciliation → "" (unattributed).
	// Mirrors how quota.Compile and the pipeline derive mode from spec —
	// no implicit default here; the executor treats "" as Enforce (spec §16).
	mode := ""
	if rb.Spec.Reconciliation != nil {
		mode = rb.Spec.Reconciliation.Mode
	}

	var out []RoleBinding

	if len(rb.Spec.Resources) == 0 {
		// Cluster-scoped binding: one binding with nil Resource.
		out = append(out, RoleBinding{
			Principal:       rb.Spec.Principal,
			Role:            rb.Spec.Role,
			Scope:           scope,
			Resource:        nil,
			Mode:            mode,
			SourceKind:      "KafkaRoleBinding",
			SourceNamespace: rb.Namespace,
			SourceName:      rb.Name,
		})
	} else {
		// Resource-scoped bindings: one binding per resource entry.
		for _, res := range rb.Spec.Resources {
			pt := res.PatternType
			if pt == "" {
				pt = "literal"
			}
			out = append(out, RoleBinding{
				Principal: rb.Spec.Principal,
				Role:      rb.Spec.Role,
				Scope:     scope,
				Resource: &ResourcePattern{
					Type:        res.Type,
					Name:        res.Name,
					PatternType: pt,
				},
				Mode:            mode,
				SourceKind:      "KafkaRoleBinding",
				SourceNamespace: rb.Namespace,
				SourceName:      rb.Name,
			})
		}
	}

	return out, nil
}

// BuildDesiredSet dedupes identical bindings by FullKey and reports identity
// collisions. Two distinct explicit KafkaRoleBindings sharing an identity
// collide (identity uniqueness, spec §40, v0.14 behaviour preserved); any
// overlap involving a topic-access-derived binding (SourceKind !=
// "KafkaRoleBinding") dedups silently — most-enforcing mode wins, Prune is
// AND-merged (spec §40 decision 4). Same-source duplicates always dedupe
// silently. Returns the deduped set sorted by FullKey for determinism.
//
// The walk is managedset.BuildDesiredSet; the rbac-specific part is the
// explicit-vs-explicit collision rule, which needs the already-accepted
// element's Source* and therefore enters as the same-identity conflict hook
// (access's Allow/Deny rejection is the other, stateful hook — see
// managedset.BuildDesiredSet). The FullKey sort is also deliberately rbac-only:
// access.BuildDesiredSet preserves input order.
func BuildDesiredSet(bindings []RoleBinding) ([]RoleBinding, []error) {
	explicitCollision := func(existing, b RoleBinding) error {
		bothExplicit := existing.SourceKind == "KafkaRoleBinding" && b.SourceKind == "KafkaRoleBinding"
		sameSource := existing.SourceKind == b.SourceKind &&
			existing.SourceNamespace == b.SourceNamespace &&
			existing.SourceName == b.SourceName
		if bothExplicit && !sameSource {
			// Two distinct explicit KafkaRoleBindings desiring the same
			// identity is an identity-uniqueness collision (spec §40).
			return fmt.Errorf(
				"role binding conflict: %s desired by both %s/%s and %s/%s",
				b.FullKey(),
				existing.SourceNamespace, existing.SourceName,
				b.SourceNamespace, b.SourceName,
			)
		}
		// Otherwise dedupe (same source, or an overlap involving a
		// topic-access-derived binding — expected, benign, since RBAC is
		// additive). Most-enforcing mode wins; prune consent AND-merges.
		return nil
	}
	out, errs := managedset.BuildDesiredSet(bindings, RoleBinding.FullKey, bindingAttribution, nil, explicitCollision)

	// Sort by FullKey for determinism.
	sort.Slice(out, func(i, j int) bool {
		return out[i].FullKey() < out[j].FullKey()
	})

	return out, errs
}

// ScopeKey identifies a (Principal, Role, Scope) tuple in the managed scope —
// the MDS "subject" without a specific resource pattern. Used to determine
// which live bindings are prune candidates.
type ScopeKey struct {
	Principal    string
	Role         string
	ScopeType    string
	KafkaCluster string
	SubCluster   string
}

func scopeKeyOf(b RoleBinding) ScopeKey {
	return ScopeKey{
		Principal:    b.Principal,
		Role:         b.Role,
		ScopeType:    b.Scope.Type,
		KafkaCluster: b.Scope.KafkaCluster,
		SubCluster:   b.Scope.SubCluster,
	}
}

// ScopeInfo carries the attribution of a managed-scope entry: the
// reconciliation mode, prune consent, and owning resource. Mode is the MOST
// ENFORCING mode among the resources contributing to the entry (Enforce >
// DetectOnly > ObserveOnly > unattributed); Source* is the FIRST contributor
// (deterministic given ordered desired bindings). Prune is AND-merged (one
// non-consenting owner vetoes deletion). The type (and the merge) is
// managedset.ScopeInfo — the single shared implementation, also aliased by
// access.ScopeInfo.
type ScopeInfo = managedset.ScopeInfo

// scopeInfoOf extracts a binding's attribution as a scope entry.
func scopeInfoOf(b RoleBinding) ScopeInfo {
	return ScopeInfo{
		Mode:            b.Mode,
		Prune:           b.Prune,
		SourceKind:      b.SourceKind,
		SourceNamespace: b.SourceNamespace,
		SourceName:      b.SourceName,
	}
}

// ManagedScope is the set of (Principal, Role, Scope) tuples the loaded
// manifests reference, each carrying the attribution of its contributors.
type ManagedScope map[ScopeKey]ScopeInfo

// BuildScope derives the managed scope from the desired bindings. When several
// bindings share a scope key, the most-enforcing mode wins, prune consent is
// AND-merged (every contributor must opt in), and the first contributor keeps
// owner attribution (mirroring access.BuildScope's rule exactly — both are
// managedset.BuildScope).
func BuildScope(bindings []RoleBinding) ManagedScope {
	return ManagedScope(managedset.BuildScope(bindings, scopeKeyOf, scopeInfoOf))
}

// Contains reports whether a live binding falls within the managed scope.
func (s ManagedScope) Contains(b RoleBinding) bool {
	_, ok := s[scopeKeyOf(b)]
	return ok
}

// Info returns the attribution of the scope entry covering a live binding, and
// whether such an entry exists.
func (s ManagedScope) Info(b RoleBinding) (ScopeInfo, bool) {
	info, ok := s[scopeKeyOf(b)]
	return info, ok
}

// StampPrune sets the owning resource's spec.prune consent on every desired
// RoleBinding so BuildScope carries it into the managed scope. Mirrors
// access.StampPrune — operator-only; AND-merge in BuildDesiredSet/BuildScope
// guarantees a prune executes only when EVERY covering resource opted in
// (spec §10.3).
func StampPrune(bindings []RoleBinding, prune bool) {
	for i := range bindings {
		bindings[i].Prune = prune
	}
}
