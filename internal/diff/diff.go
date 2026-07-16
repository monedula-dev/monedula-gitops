// Package diff is the shared core that compares desired vs. live state
// (topics + ACLs within the managed scope) and emits a typed, deterministic
// list of operations with risk/approval fixed from the spec risk taxonomy.
//
// verify reports drift (any op present), diff renders the list for humans, and
// apply --dry-run renders the same list — all from Compute.
package diff

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/monedula-dev/monedula-gitops/internal/access"
	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	"github.com/monedula-dev/monedula-gitops/internal/managedset"
	"github.com/monedula-dev/monedula-gitops/internal/operations"
	"github.com/monedula-dev/monedula-gitops/internal/quota"
	"github.com/monedula-dev/monedula-gitops/internal/rbac"
	"github.com/monedula-dev/monedula-gitops/internal/schemaregistry"
	"github.com/monedula-dev/monedula-gitops/internal/user"
)

// DesiredTopic is a topic the manifests declare.
type DesiredTopic struct {
	Kind, Namespace, Name string // Kind/Namespace used for Operation metadata; Name is resolved topicName
	Partitions            int
	ReplicationFactor     int
	Config                map[string]string
	Mode                  string // reconciliation mode (spec §16); stamped onto every emitted op

	// IgnoreFields is spec.drift.ignoreFields (spec §16): field paths excluded
	// from DRIFT calculation on an EXISTING topic (creation always sends the
	// full spec). Supported syntax (validated upstream, see
	// internal/validation):
	//   partitions          - skip partition reconciliation (increase AND the
	//                         decrease rejection);
	//   replicationFactor   - skip replication-factor drift;
	//   config.<key>        - skip drift on one config key. Config keys contain
	//                         dots themselves (config.retention.ms ignores key
	//                         "retention.ms"), so matching strips the literal
	//                         "config." prefix rather than splitting on dots.
	IgnoreFields []string
}

// ignoresField reports whether field (one of "partitions",
// "replicationFactor", or "config.<key>") is listed in IgnoreFields.
func (dt DesiredTopic) ignoresField(field string) bool {
	for _, f := range dt.IgnoreFields {
		if f == field {
			return true
		}
	}
	return false
}

// DesiredSchema is a schema subject the manifests declare.
type DesiredSchema struct {
	Subject       string
	Topic         string // owning topic name (for executor prerequisite-skip)
	Type          string // AVRO|JSON|PROTOBUF
	Definition    string
	Compatibility string // "" = not managed
	Mode          string // owning topic's reconciliation mode (spec §16)
}

// Desired is the full set of declared state plus the managed scope derived from it.
type Desired struct {
	Topics  []DesiredTopic
	ACLs    []access.ACL
	Schemas []DesiredSchema
	Quotas  []quota.Desired // compiled KafkaQuota entities + limit values (spec §39)
	Users   []user.Desired  // compiled KafkaUser credentials + password refs (v0.35)
	Scope   access.ManagedScope

	// RotatePasswords, when set (apply --rotate-passwords), makes computeUserOps
	// emit a RotateScramCredential for every declared user whose observable
	// identity is otherwise IN SYNC — an event-driven password re-upsert from
	// the configured source. Users that are NOT in sync need no rotate op: the
	// Create/Update they already get upserts the new password anyway. Plain
	// diff/verify never set this — rotation is an apply-time action, not drift.
	RotatePasswords bool

	// RoleBindings and RoleBindingScope carry the desired RBAC role bindings and
	// their managed scope (spec §40). RoleBindingScope gates prune decisions for
	// RemoveRoleBinding, mirroring how Scope gates DeleteAcl.
	RoleBindings     []rbac.RoleBinding
	RoleBindingScope rbac.ManagedScope

	// RoleBindingPruneDesired, when RoleBindingPruneAggregateSet is true,
	// REPLACES RoleBindings as the prune keep-set: a live binding in scope but
	// absent from RoleBindingPruneDesired is a prune candidate. Creates still
	// derive from RoleBindings. Operators supply the cluster-wide union here so
	// a single CR/topic reconcile never prunes a binding owned by another
	// resource sharing a scope key (spec §40, the role-binding analogue of
	// PruneDesired for ACLs). When RoleBindingPruneAggregateSet is false,
	// RoleBindings is used for both (CLI single-aggregate-set semantics).
	//
	// RoleBindingPruneAggregateSet is the explicit switch — NOT the nilness of
	// RoleBindingPruneDesired — so a legitimately empty aggregate (e.g. a
	// cluster-wide view with zero contributing resources) still activates the
	// keep-set instead of being silently mistaken for "not supplied" and
	// falling back to RoleBindings (which would make every live in-scope
	// binding a prune candidate). Set both together via SetRoleBindingPruneSet.
	RoleBindingPruneDesired      []rbac.RoleBinding
	RoleBindingPruneAggregateSet bool

	// PruneDesired and PruneScope, when PruneAggregateSet is true, REPLACE ACLs
	// and Scope for the DeleteAcl (prune) computation ONLY. CreateAcl ops are
	// still emitted from ACLs — the caller's own desired set — so creates keep
	// per-resource attribution and are not duplicated across reconciles.
	//
	// This is the operator's spec §20.1 / §10.4 seam: the reconcile core passes
	// the cluster-wide desired ACL union (aggregated across every resource
	// referencing the cluster) so a live ACL desired by ANOTHER resource on the
	// same (principal, resource pattern) is not seen as in-scope-but-undesired
	// and pruned — the flapping bug §10.4 fixes. PruneScope carries the
	// AND-merged prune consent of ALL covering resources, so one
	// non-consenting owner vetoes deletion.
	//
	// When PruneAggregateSet is false (the CLI and any single-resource caller),
	// both fields are ignored and prune candidates derive from ACLs + Scope
	// exactly as before.
	//
	// PruneAggregateSet is the explicit switch — NOT the nilness of PruneScope
	// — so a legitimately empty aggregate cannot be mistaken for "not
	// supplied" (the same footgun RoleBindingPruneAggregateSet closes for role
	// bindings). Set both together via SetPruneAggregate.
	PruneDesired      []access.ACL
	PruneScope        access.ManagedScope
	PruneAggregateSet bool
}

// SetPruneAggregate sets the cluster-wide ACL prune aggregate (PruneDesired,
// PruneScope) and marks it supplied, regardless of whether desired/scope are
// empty. Operator callers building a ClusterACLView MUST use this rather than
// assigning the fields directly, so an aggregate with zero contributing
// resources still replaces (not falls back to) the per-resource ACLs/Scope.
func (d *Desired) SetPruneAggregate(desired []access.ACL, scope access.ManagedScope) {
	d.PruneDesired = desired
	d.PruneScope = scope
	d.PruneAggregateSet = true
}

// pruneInputs returns the desired set and scope governing DeleteAcl (prune)
// computation: the aggregated PruneDesired/PruneScope when PruneAggregateSet
// is true, else the per-resource ACLs/Scope.
func (d Desired) pruneInputs() ([]access.ACL, access.ManagedScope) {
	if d.PruneAggregateSet {
		return d.PruneDesired, d.PruneScope
	}
	return d.ACLs, d.Scope
}

// SetRoleBindingPruneSet sets the cluster-wide role-binding prune keep-set
// (RoleBindingPruneDesired) and marks it supplied, regardless of whether keep
// is empty. Operator callers building a cluster-wide role-binding view MUST
// use this rather than assigning the field directly, so an aggregate with
// zero contributing resources still replaces (not falls back to)
// RoleBindings.
func (d *Desired) SetRoleBindingPruneSet(keep []rbac.RoleBinding) {
	d.RoleBindingPruneDesired = keep
	d.RoleBindingPruneAggregateSet = true
}

// roleBindingPruneKeep returns the prune keep-set for role bindings: the
// cluster-wide RoleBindingPruneDesired when RoleBindingPruneAggregateSet is
// true, else RoleBindings.
func (d Desired) roleBindingPruneKeep() []rbac.RoleBinding {
	if d.RoleBindingPruneAggregateSet {
		return d.RoleBindingPruneDesired
	}
	return d.RoleBindings
}

// Live is the observed cluster state.
type Live struct {
	Topics       []kafka.TopicState
	ACLs         []access.ACL
	Schemas      []schemaregistry.SubjectState
	Quotas       []kafka.QuotaState // observed client quotas (spec §39)
	RoleBindings []rbac.RoleBinding // observed MDS role bindings (spec §40)

	// ScramCredentials is the observed SCRAM credential identity set (v0.35):
	// exactly the (user, mechanism, iterations) triples Kafka's
	// DescribeUserSCRAMCredentials exposes — never passwords, which Kafka does
	// not return. Live-state readers may bound the read to the declared
	// usernames; entries for undeclared users are ignored by the diff either way.
	ScramCredentials []kafka.ScramCredential

	// SupersededSchemas carries SchemaSuperseded detection (spec §12.1):
	// subject -> the OLDER version under which the DESIRED schema is already
	// registered. The diff engine has no registry client, so the live-state
	// readers (CLI computeOps, operator observeTopicLive) populate it — for
	// each desired subject whose definition differs from the latest version
	// they call LookupSchema and record a hit here. computeSchemaOps then
	// emits a terminal SchemaSuperseded op instead of a RegisterSchema that
	// would dedupe to the old version and loop forever. May be nil.
	SupersededSchemas map[string]int

	// GlobalCompatibility is the Schema Registry's GLOBAL compatibility level
	// (GET /config), fetched once per run by the live-state readers alongside
	// the subject reads. It is the EFFECTIVE level of any subject without a
	// subject-level override, so compatibilityOp uses it as the baseline when
	// classifying a FIRST-TIME subject-level set (spec §17.1): an initial set
	// below the global default is a gated Lower, not an ungated Raise. "" means
	// unknown (older SR, or GET /config failed — readers must NOT fail the run
	// on it); compatibilityOp then falls back to the legacy any-initial-set-is-
	// a-Raise classification.
	GlobalCompatibility string

	// GlobalCompatibilityErr, when non-empty, is the error message from a
	// failed GET /config fetch during THIS observation (the reader still leaves
	// GlobalCompatibility as "" and continues rather than failing the run). The
	// operator surfaces it as the informational SchemaRegistryDegraded
	// condition (spec §17.1); the CLI logs it as a warning. Empty when the
	// fetch was skipped (no subjects managed) or it succeeded.
	GlobalCompatibilityErr string
}

// aclTarget is a human-readable identifier for an ACL operation.
// When the host is non-default (anything other than "*"), it is appended as
// "(host=<value>)" so operators can verify host-scoped ACLs in dry-run / diff
// output (spec §8.4). The overwhelmingly common wildcard host "*" is omitted
// to keep existing output backward-compatible.
func aclTarget(a access.ACL) string {
	base := fmt.Sprintf("%s %s %s:%s %s", a.Principal, a.Operation, a.ResourceType, a.ResourceName, a.Permission)
	if a.Host != "*" {
		base += fmt.Sprintf(" (host=%s)", a.Host)
	}
	return base
}

// Compute compares desired vs. live state and returns the deterministic
// operation list needed to reconcile live toward desired, within scope.
func Compute(desired Desired, live Live) []operations.Operation {
	var ops []operations.Operation

	ops = append(ops, computeTopicOps(desired, live)...)
	ops = append(ops, computeACLOps(desired, live)...)
	ops = append(ops, computeSchemaOps(desired, live)...)
	ops = append(ops, computeQuotaOps(desired, live)...)
	ops = append(ops, computeRoleBindingOps(desired, live)...)
	ops = append(ops, computeUserOps(desired, live)...)

	// Deterministic final ordering: by Action, then Target, then Field, then Subject.
	// This is a STABLE refinement of already-sorted inputs (topic ops are emitted
	// in name order; ACL ops in FullKey order; schema ops in Subject order; quota
	// ops in entity-key order, with Target set to the entity string so the
	// Action+Target keys order them deterministically), not a standalone total
	// order — it does not tie-break on every field. SliceStable
	// must be retained so that ops equal under the compared keys keep their
	// pre-sorted input order. Subject is included so two schema ops on different
	// subjects (which share an empty Target/Field tie under the topic/ACL keys when
	// Target==Subject) order stably regardless of input order.
	sort.SliceStable(ops, func(i, j int) bool {
		if ops[i].Action != ops[j].Action {
			return ops[i].Action < ops[j].Action
		}
		if ops[i].Target != ops[j].Target {
			return ops[i].Target < ops[j].Target
		}
		if ops[i].Field != ops[j].Field {
			return ops[i].Field < ops[j].Field
		}
		return ops[i].Subject < ops[j].Subject
	})
	return ops
}

func computeTopicOps(desired Desired, live Live) []operations.Operation {
	liveByName := make(map[string]kafka.TopicState, len(live.Topics))
	for _, t := range live.Topics {
		liveByName[t.Name] = t
	}

	// Iterate desired topics sorted by name for determinism.
	topics := append([]DesiredTopic(nil), desired.Topics...)
	sort.Slice(topics, func(i, j int) bool { return topics[i].Name < topics[j].Name })

	var ops []operations.Operation
	for _, dt := range topics {
		lt, present := liveByName[dt.Name]
		if !present {
			op := operations.New(operations.CreateTopic)
			op.Kind, op.Namespace, op.Name, op.Target = dt.Kind, dt.Namespace, dt.Name, dt.Name
			op.Mode = dt.Mode
			op.Partitions = dt.Partitions
			op.ReplicationFactor = dt.ReplicationFactor
			op.Config = dt.Config
			ops = append(ops, op)
			continue
		}

		// Partition reconciliation (skipped entirely when "partitions" is
		// ignored, spec §16 — both the increase and the decrease rejection).
		switch {
		case dt.ignoresField("partitions"):
			// excluded from drift calculation
		case dt.Partitions < lt.Partitions:
			op := operations.New(operations.Rejected)
			op.Kind, op.Namespace, op.Name, op.Target = dt.Kind, dt.Namespace, dt.Name, dt.Name
			op.Mode = dt.Mode
			op.From = strconv.Itoa(lt.Partitions)
			op.To = strconv.Itoa(dt.Partitions)
			op.Message = fmt.Sprintf("partition decrease rejected: %d -> %d is not supported by Kafka", lt.Partitions, dt.Partitions)
			ops = append(ops, op)
		case dt.Partitions > lt.Partitions:
			op := operations.New(operations.IncreasePartitions)
			op.Kind, op.Namespace, op.Name, op.Target = dt.Kind, dt.Namespace, dt.Name, dt.Name
			op.Mode = dt.Mode
			op.From = strconv.Itoa(lt.Partitions)
			op.To = strconv.Itoa(dt.Partitions)
			op.Partitions = dt.Partitions
			ops = append(ops, op)
		}

		// Config drift: only declared (desired) keys are managed. Sorted keys.
		keys := make([]string, 0, len(dt.Config))
		for k := range dt.Config {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			if dt.ignoresField("config." + k) {
				continue // excluded from drift calculation (spec §16)
			}
			want := dt.Config[k]
			got, ok := lt.Config[k]
			if ok && got == want {
				continue
			}
			op := operations.New(operations.UpdateTopicConfig)
			op.Kind, op.Namespace, op.Name, op.Target = dt.Kind, dt.Namespace, dt.Name, dt.Name
			op.Mode = dt.Mode
			op.Field = k
			// From=="" conflates "key absent in live" with "key present but empty".
			// We do not distinguish the two: for v0.1 rendering both display as an
			// empty current value, which is acceptable.
			op.From = got
			op.To = want
			ops = append(ops, op)
		}

		// Replication factor: 0 means unspecified on either side; skip then.
		// Also skipped when "replicationFactor" is ignored (spec §16).
		if dt.ReplicationFactor != 0 && lt.ReplicationFactor != 0 && dt.ReplicationFactor != lt.ReplicationFactor &&
			!dt.ignoresField("replicationFactor") {
			op := operations.New(operations.UpdateReplicationFactor)
			op.Kind, op.Namespace, op.Name, op.Target = dt.Kind, dt.Namespace, dt.Name, dt.Name
			op.Mode = dt.Mode
			op.From = strconv.Itoa(lt.ReplicationFactor)
			op.To = strconv.Itoa(dt.ReplicationFactor)
			ops = append(ops, op)
		}
	}
	return ops
}

// addPruneKind bundles the per-kind hooks of the shared add/prune diff walk
// (addPruneOps). The walk itself — key matching, sorted iteration, the
// scope-gated prune candidacy, and the attribution stamping — is one
// implementation; what varies per kind is the identity key, the scope lookups,
// the op actions, and the Target/payload emission.
type addPruneKind[T any] struct {
	key         func(T) string                                  // identity key (FullKey)
	defaultKind string                                          // op Kind for unattributed input
	attr        func(T) managedset.ScopeInfo                    // Mode + Source* of a DESIRED item (add path; Prune unused)
	contains    func(T) bool                                    // managed-scope gate for prune candidacy
	info        func(T) (managedset.ScopeInfo, bool)            // covering scope entry for a prune candidate
	addAction   operations.Action                               // emitted for desired-not-live
	pruneAction operations.Action                               // emitted for live-in-scope-not-desired
	emit        func(T, operations.Action) operations.Operation // action + Target + payload
}

// addPruneOps is the shared diff walk behind computeACLOps and
// computeRoleBindingOps:
//
//   - desired not in live → k.addAction, carrying the desired item's owner
//     attribution (spec §17.5): the op takes the kind/namespace/name of the
//     resource that compiled the tuple, falling back to k.defaultKind for
//     unattributed input (operator path, direct callers), plus its mode.
//   - live in scope but not in keep → k.pruneAction. A prune candidate is
//     governed by the resources whose managed scope covers the tuple: it takes
//     the scope entry's mode (most enforcing among contributors), prune
//     consent (spec §10.3: true only if EVERY contributor opted in — the
//     executor still needs this OR the run-wide --prune approval before
//     deleting), and owner. The !ok branch is unreachable — the contains gate
//     guarantees an entry — but is kept defensive: an uncovered prune is
//     conservatively reported (PruneAllowed stays false, mode ModeDetectOnly),
//     never deleted.
//   - live NOT in scope → ignored (invisible, never pruned).
//   - present in both → no-op.
//
// desired feeds the add computation; keep is the prune keep-set (the
// cluster-wide aggregate when the operator supplies one, else the same set as
// desired). Matching is by k.key; both passes iterate sorted by it for
// deterministic output.
func addPruneOps[T any](k addPruneKind[T], desired, keep, live []T) []operations.Operation {
	keepKeys := make(map[string]bool, len(keep))
	for _, it := range keep {
		keepKeys[k.key(it)] = true
	}
	liveKeys := make(map[string]bool, len(live))
	for _, it := range live {
		liveKeys[k.key(it)] = true
	}

	var ops []operations.Operation

	// Add: each desired item not present in live, iterated sorted by full key.
	desiredItems := append([]T(nil), desired...)
	sort.Slice(desiredItems, func(i, j int) bool { return k.key(desiredItems[i]) < k.key(desiredItems[j]) })
	for _, it := range desiredItems {
		if liveKeys[k.key(it)] {
			continue
		}
		op := k.emit(it, k.addAction)
		op.Kind = k.defaultKind
		a := k.attr(it)
		if a.SourceKind != "" {
			op.Kind, op.Namespace, op.Name = a.SourceKind, a.SourceNamespace, a.SourceName
		}
		op.Mode = a.Mode
		ops = append(ops, op)
	}

	// Prune: each in-scope live item not in the keep-set, iterated sorted.
	liveItems := append([]T(nil), live...)
	sort.Slice(liveItems, func(i, j int) bool { return k.key(liveItems[i]) < k.key(liveItems[j]) })
	for _, it := range liveItems {
		if !k.contains(it) {
			continue // out-of-scope live items are invisible
		}
		if keepKeys[k.key(it)] {
			continue // already desired (by this resource or, with an aggregated
			// keep-set, by ANY resource referencing the cluster)
		}
		op := k.emit(it, k.pruneAction)
		op.Kind = k.defaultKind
		if info, ok := k.info(it); ok {
			op.Mode = info.Mode
			op.PruneAllowed = info.Prune
			if info.SourceKind != "" {
				op.Kind, op.Namespace, op.Name = info.SourceKind, info.SourceNamespace, info.SourceName
			}
		} else {
			op.Mode = operations.ModeDetectOnly
		}
		ops = append(ops, op)
	}

	return ops
}

// computeACLOps instantiates the shared add/prune walk for ACLs. Prune
// candidacy is governed by pruneDesired/pruneScope (the cluster-wide aggregate
// when the operator supplies one — see Desired.PruneDesired); creates always
// derive from desired.ACLs.
func computeACLOps(desired Desired, live Live) []operations.Operation {
	pruneDesired, pruneScope := desired.pruneInputs()
	return addPruneOps(addPruneKind[access.ACL]{
		key:         access.ACL.FullKey,
		defaultKind: "ACL",
		attr: func(a access.ACL) managedset.ScopeInfo {
			return managedset.ScopeInfo{Mode: a.Mode, SourceKind: a.SourceKind, SourceNamespace: a.SourceNamespace, SourceName: a.SourceName}
		},
		contains:    pruneScope.Contains,
		info:        pruneScope.Info,
		addAction:   operations.CreateAcl,
		pruneAction: operations.DeleteAcl,
		emit: func(a access.ACL, action operations.Action) operations.Operation {
			op := operations.New(action)
			op.Target = aclTarget(a)
			op.ACL = &kafka.ACLState{Principal: a.Principal, Host: a.Host, ResourceType: a.ResourceType, ResourceName: a.ResourceName, PatternType: a.PatternType, Operation: a.Operation, Permission: a.Permission}
			return op
		},
	}, desired.ACLs, pruneDesired, live.ACLs)
}

// computeSchemaOps reconciles declared schema subjects toward live registry
// state. The schema diff is ADDITIVE in v0.4: it emits RegisterSchema and
// compatibility changes only. It never emits DeleteSubject — a subject removed
// from desired state is orphaned, not deleted (DeleteSubject remains in the
// taxonomy/executor for completeness, but is not produced here).
func computeSchemaOps(desired Desired, live Live) []operations.Operation {
	liveBySubject := make(map[string]schemaregistry.SubjectState, len(live.Schemas))
	for _, s := range live.Schemas {
		liveBySubject[s.Subject] = s
	}

	schemas := append([]DesiredSchema(nil), desired.Schemas...)
	sort.Slice(schemas, func(i, j int) bool { return schemas[i].Subject < schemas[j].Subject })

	var ops []operations.Operation
	for _, ds := range schemas {
		ls, present := liveBySubject[ds.Subject]

		// Governance mode (spec §12.2): an empty Definition means monedula
		// manages ONLY the subject compatibility level — the producer's
		// pipeline registers versions out-of-band, which is NOT drift. Never
		// emit RegisterSchema or SchemaSuperseded, and never consult
		// SupersededSchemas; emit only the compatibility op when the desired
		// level differs from live. A live subject absent from live.Schemas has
		// level "" (treated the same as an absent subject), so the
		// compatibility-diff below handles both an absent subject and a
		// producer-registered one uniformly.
		if ds.Definition == "" {
			if ds.Compatibility != "" && ds.Compatibility != ls.Compatibility {
				ops = append(ops, compatibilityOp(ds, ls.Compatibility, live.GlobalCompatibility))
			}
			continue
		}

		if !present {
			// New subject: register the schema.
			reg := operations.New(operations.RegisterSchema)
			reg.Kind = "Schema"
			reg.Target = ds.Subject
			reg.Subject = ds.Subject
			reg.SchemaType = ds.Type
			reg.SchemaDef = ds.Definition
			reg.Topic = ds.Topic
			reg.Mode = ds.Mode
			ops = append(ops, reg)

			// Setting a subject-level compatibility on a brand-new subject is a
			// FIRST-TIME set: the subject inherited the global default, so
			// compatibilityOp classifies against that baseline (a level below
			// the known global default is an effective Lower, gated; at or
			// above it is a Raise, ungated; unknown global falls back to Raise).
			if ds.Compatibility != "" {
				ops = append(ops, compatibilityOp(ds, "", live.GlobalCompatibility))
			}
			continue
		}

		// Existing subject: register a new version if the definition differs —
		// unless the desired schema is already registered as an OLDER version
		// (spec §12.1): re-registering would dedupe to that version and never
		// become latest, so the divergence is surfaced as the terminal
		// SchemaSuperseded instead of looping on RegisterSchema forever.
		if !schemaEqual(ds.Type, ds.Definition, ls.Schema.Definition) {
			if oldV, superseded := live.SupersededSchemas[ds.Subject]; superseded {
				sup := operations.New(operations.SchemaSuperseded)
				sup.Kind = "Schema"
				sup.Target = ds.Subject
				sup.Subject = ds.Subject
				sup.Topic = ds.Topic
				sup.Mode = ds.Mode
				sup.Message = fmt.Sprintf(
					"manifest schema is an older version of subject %s (registered as v%d; latest is v%d); update the manifest or roll the registry forward",
					ds.Subject, oldV, ls.Version)
				ops = append(ops, sup)
			} else {
				reg := operations.New(operations.RegisterSchema)
				reg.Kind = "Schema"
				reg.Target = ds.Subject
				reg.Subject = ds.Subject
				reg.SchemaType = ds.Type
				reg.SchemaDef = ds.Definition
				reg.Topic = ds.Topic
				reg.Mode = ds.Mode
				ops = append(ops, reg)
			}
		}

		// Compatibility change (only when managed and actually different).
		if ds.Compatibility != "" && ds.Compatibility != ls.Compatibility {
			ops = append(ops, compatibilityOp(ds, ls.Compatibility, live.GlobalCompatibility))
		}
	}
	return ops
}

// computeQuotaOps reconciles declared client quotas toward live cluster state.
// The managed scope is the DESIRED set only (spec §39.3): a live quota entity
// absent from desired is left untouched — never pruned. For each desired entity:
//
//   - absent from live  -> one SetQuota op carrying the full desired limit set;
//   - present, but a desired limit value differs or a desired key is missing
//     from live -> one UpdateQuota op carrying the FULL desired limit set
//     (re-setting all desired keys is authoritative and deterministic);
//   - present, with live limit keys NOT in desired -> one RemoveQuota op
//     carrying those extra keys (authoritative removal of unmanaged values).
//
// An entity may need BOTH an UpdateQuota and a RemoveQuota (a changed/added key
// alongside an extra live key); the two are emitted independently. SetQuota and
// UpdateQuota both map to client.SetQuota in the executor — the distinction is
// for human-readable output and risk clarity (entirely new entity vs. drift on
// an existing one). Entities are keyed by quota.Entity.Key() on both sides; the
// live entity is converted to a quota.Entity to reuse the exact same canonical
// key, guaranteeing consistency with the desired side.
func computeQuotaOps(desired Desired, live Live) []operations.Operation {
	liveByKey := make(map[string]kafka.QuotaState, len(live.Quotas))
	for _, qs := range live.Quotas {
		liveByKey[liveQuotaEntity(qs.Entity).Key()] = qs
	}

	quotas := append([]quota.Desired(nil), desired.Quotas...)
	sort.Slice(quotas, func(i, j int) bool { return quotas[i].Entity.Key() < quotas[j].Entity.Key() })

	var ops []operations.Operation
	for _, dq := range quotas {
		entity := quotaEntityComponents(dq.Entity)
		target := quotaTarget(dq.Entity)

		lq, present := liveByKey[dq.Entity.Key()]
		if !present {
			op := operations.New(operations.SetQuota)
			op.Kind = "KafkaQuota"
			op.Target = target
			op.Mode = dq.Mode
			op.QuotaEntity = entity
			op.QuotaLimits = copyLimits(dq.Limits)
			ops = append(ops, op)
			continue
		}

		// needsUpdate: true if any desired key is absent from live or has a different value.
		needsUpdate := false
		for k, want := range dq.Limits {
			if got, ok := lq.Limits[k]; !ok || got != want {
				needsUpdate = true
				break
			}
		}
		if needsUpdate {
			op := operations.New(operations.UpdateQuota)
			op.Kind = "KafkaQuota"
			op.Target = target
			op.Mode = dq.Mode
			op.QuotaEntity = entity
			// Authoritative + deterministic: re-set the FULL desired set.
			op.QuotaLimits = copyLimits(dq.Limits)
			ops = append(ops, op)
		}

		// toRemove: live keys not present in desired.
		remove := map[string]float64{}
		for k := range lq.Limits {
			if _, ok := dq.Limits[k]; !ok {
				remove[k] = 0 // value is irrelevant for removal; only the key is sent to DeleteQuota
			}
		}
		if len(remove) > 0 {
			op := operations.New(operations.RemoveQuota)
			op.Kind = "KafkaQuota"
			op.Target = target
			op.Mode = dq.Mode
			op.QuotaEntity = entity
			op.QuotaLimits = remove
			ops = append(ops, op)
		}
	}
	return ops
}

// roleBindingTarget returns a human-readable identifier for a role-binding operation.
// Format: "Principal Role scope-type[/sub-cluster] [Resource]".
func roleBindingTarget(b rbac.RoleBinding) string {
	scope := b.Scope.Type
	if b.Scope.SubCluster != "" {
		scope += "/" + b.Scope.SubCluster
	}
	target := fmt.Sprintf("%s %s %s", b.Principal, b.Role, scope)
	if b.Resource != nil {
		target += fmt.Sprintf(" %s:%s(%s)", b.Resource.Type, b.Resource.Name, b.Resource.PatternType)
	}
	return target
}

// computeRoleBindingOps instantiates the shared add/prune walk for MDS role
// bindings (spec §40) — the same walk as computeACLOps, by construction:
//
//   - desired not in live → AddRoleBinding (RiskLow, GateNone).
//   - live in scope but not desired → RemoveRoleBinding (RiskMedium, GatePrune);
//     PruneAllowed is stamped from the covering scope entry's Prune consent,
//     mirroring DeleteAcl's PruneAllowed wiring via ManagedScope.Info.
//   - live NOT in scope → ignored (never pruned).
//   - present in both → no-op.
//
// Matching is by FullKey(). Ordering is deterministic (sorted by FullKey).
// Unlike the ACL side, only the KEEP-SET is aggregate-swappable
// (roleBindingPruneKeep); the scope is always desired.RoleBindingScope — role
// bindings have no PruneScope analogue.
func computeRoleBindingOps(desired Desired, live Live) []operations.Operation {
	return addPruneOps(addPruneKind[rbac.RoleBinding]{
		key:         rbac.RoleBinding.FullKey,
		defaultKind: "KafkaRoleBinding",
		attr: func(b rbac.RoleBinding) managedset.ScopeInfo {
			return managedset.ScopeInfo{Mode: b.Mode, SourceKind: b.SourceKind, SourceNamespace: b.SourceNamespace, SourceName: b.SourceName}
		},
		contains:    desired.RoleBindingScope.Contains,
		info:        desired.RoleBindingScope.Info,
		addAction:   operations.AddRoleBinding,
		pruneAction: operations.RemoveRoleBinding,
		emit: func(b rbac.RoleBinding, action operations.Action) operations.Operation {
			op := operations.New(action)
			op.Target = roleBindingTarget(b)
			bc := b
			op.RoleBinding = &bc
			return op
		},
	}, desired.RoleBindings, desired.roleBindingPruneKeep(), live.RoleBindings)
}

// userTarget is the human-readable identifier for a SCRAM credential
// operation: "<username> (<mechanism>)". Distinct per declared user (at most
// one user op is emitted per username), so the Action+Target sort keys in
// Compute order user ops deterministically.
func userTarget(username, mechanism string) string {
	return username + " (" + mechanism + ")"
}

// computeUserOps reconciles declared KafkaUser credentials toward live SCRAM
// state (v0.35 spec §2-§4). The drift surface is ONLY the observable identity
// (username, mechanism, iterations) — Kafka never exposes passwords, so
// password changes are event-driven (--rotate-passwords), never drift.
//
// Scope is declared-mechanism-only (the user analogue of quotas' §39.3
// desired-set scope, per the §10 scope philosophy):
//
//   - declared (username, mechanism) absent live, user has NO other mechanism
//     -> CreateScramCredential (Low, ungated);
//   - declared mechanism present but iterations mismatch — compared only when
//     the spec SETS iterations (Iterations 0 = unset = never compared)
//     -> UpdateScramCredential (Medium, ungated);
//   - declared mechanism absent but the user has only the OTHER live mechanism
//     (a mechanism change) -> ONE UpdateScramCredential that upserts the
//     declared mechanism and carries ScramDeleteMechanism so the executor
//     drops the old credential after the new one is in place;
//   - declared + in sync -> nothing, unless Desired.RotatePasswords is set,
//     in which case a RotateScramCredential re-upserts the password from its
//     configured source (not-in-sync users need no rotate op — their
//     Create/Update already writes the new password);
//   - an ENTIRELY undeclared live user is out of scope: never drift, never
//     pruned (users are not derived from any ACL-like managed scope);
//   - an EXTRA live mechanism on a user whose declared mechanism is present
//     is invisible: never drift, never pruned. NOTE this is also the residue
//     state left by a PARTIALLY-applied mechanism change (upsert of the new
//     mechanism succeeded, delete of the old one failed): the next diff sees
//     the declared mechanism present+in-sync and the old one as this same
//     EXTRA case, so it is never re-surfaced as an op. That residue is
//     invisible to diff/apply by this scope design and cleanup is manual
//     (or via the operator) — the executor's error on that delete failure
//     says so explicitly rather than implying re-apply will retry it.
//
// Consequently the CLI diff NEVER emits a standalone DeleteScramCredential:
// the only credential deletion it ever schedules is the mechanism-change one
// folded into UpdateScramCredential above. DeleteScramCredential remains in
// the taxonomy (RiskMedium, GateDestructive) for the executor and the
// operator's finalizer path (T5), so no destructive-gated user op is reachable
// from a pure CLI diff.
func computeUserOps(desired Desired, live Live) []operations.Operation {
	// username -> mechanism -> live iterations.
	liveByUser := make(map[string]map[string]int32)
	for _, c := range live.ScramCredentials {
		m := liveByUser[c.User]
		if m == nil {
			m = make(map[string]int32)
			liveByUser[c.User] = m
		}
		m[c.Mechanism] = c.Iterations
	}

	// Iterate desired users sorted by username for determinism (validation
	// guarantees usernames are unique per cluster).
	users := append([]user.Desired(nil), desired.Users...)
	sort.Slice(users, func(i, j int) bool { return users[i].Credential.Username < users[j].Credential.Username })

	var ops []operations.Operation
	for _, du := range users {
		cred := du.Credential
		liveMechs := liveByUser[cred.Username]
		liveIters, declaredPresent := liveMechs[cred.Mechanism]

		// Local constructor closure, unlike computeTopicOps/computeACLOps/etc.:
		// every branch below (in-sync rotate, iterations update, mechanism
		// change, create) stamps the SAME five fields off the SAME cred/du, so
		// closing over them here beats repeating a five-field literal per branch.
		newOp := func(a operations.Action) operations.Operation {
			op := operations.New(a)
			op.Kind = "KafkaUser"
			op.Target = userTarget(cred.Username, cred.Mechanism)
			op.ScramUser = cred.Username
			op.ScramMechanism = cred.Mechanism
			op.ScramIterations = cred.Iterations
			op.PasswordRef = du.PasswordRef
			return op
		}

		switch {
		case declaredPresent && (cred.Iterations == 0 || cred.Iterations == liveIters):
			// In sync. Iterations 0 means the CR did not pin a count, so ANY
			// live value is accepted (the broker default is not drift).
			if desired.RotatePasswords {
				// No Message: the action name is self-descriptive and the human
				// renderer adds an explicit "(--rotate-passwords)" annotation.
				ops = append(ops, newOp(operations.RotateScramCredential))
			}

		case declaredPresent:
			// Declared mechanism exists but the pinned iteration count differs.
			op := newOp(operations.UpdateScramCredential)
			op.Field = "iterations"
			op.From = strconv.Itoa(int(liveIters))
			op.To = strconv.Itoa(int(cred.Iterations))
			ops = append(ops, op)

		case len(liveMechs) > 0:
			// Mechanism change: the user exists live but only under the other
			// mechanism. One op: upsert declared, then delete the old. With
			// exactly two SCRAM mechanisms in existence there is exactly one
			// other; the sort is a determinism guard should that ever grow.
			others := make([]string, 0, len(liveMechs))
			for m := range liveMechs {
				others = append(others, m)
			}
			sort.Strings(others)
			op := newOp(operations.UpdateScramCredential)
			op.Field = "mechanism"
			op.From = others[0]
			op.To = cred.Mechanism
			op.ScramDeleteMechanism = others[0]
			ops = append(ops, op)

		default:
			// No credential at all for this username: create.
			ops = append(ops, newOp(operations.CreateScramCredential))
		}
	}
	return ops
}

// liveQuotaEntity converts a live entity ([]kafka.QuotaEntityComponent) to a
// quota.Entity so its canonical .Key() can be reused for matching against
// desired quotas — the two structs share an identical shape (Type, Name *string).
func liveQuotaEntity(comps []kafka.QuotaEntityComponent) quota.Entity {
	e := make(quota.Entity, 0, len(comps))
	for _, c := range comps {
		e = append(e, quota.Component{Type: c.Type, Name: c.Name})
	}
	return e
}

// quotaEntityComponents converts a desired quota.Entity to the op payload type
// ([]kafka.QuotaEntityComponent) — the inverse trivial map of liveQuotaEntity.
func quotaEntityComponents(e quota.Entity) []kafka.QuotaEntityComponent {
	comps := make([]kafka.QuotaEntityComponent, 0, len(e))
	for _, c := range e {
		comps = append(comps, kafka.QuotaEntityComponent{Type: c.Type, Name: c.Name})
	}
	return comps
}

// quotaTarget renders a human-readable entity identifier: components sorted by
// type, a nil name shown as <default>, joined with ",". Mirrors quota.Entity.Key
// but uses a comma separator for diff/dry-run readability (e.g.
// "client-id=batch,user=svc-checkout"; a default user as "user=<default>").
func quotaTarget(e quota.Entity) string {
	cp := append(quota.Entity(nil), e...)
	sort.Slice(cp, func(i, j int) bool { return cp[i].Type < cp[j].Type })
	parts := make([]string, 0, len(cp))
	for _, c := range cp {
		name := "<default>"
		if c.Name != nil {
			name = *c.Name
		}
		parts = append(parts, c.Type+"="+name)
	}
	return strings.Join(parts, ",")
}

// copyLimits returns a shallow copy of a limit map so the op payload does not
// alias the caller's desired map.
func copyLimits(m map[string]float64) map[string]float64 {
	out := make(map[string]float64, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// compatibilityOp builds the Raise/LowerSchemaCompatibility op moving a subject
// from liveLevel to ds.Compatibility. Risk/gating follow the spec §17.1
// taxonomy: a strictly stronger constraint is a Raise (Low, ungated); a
// lowering OR a sideways move (equal rank, different value) is a Lower (High,
// destructive-gated). Callers gate on ds.Compatibility != liveLevel.
//
// liveLevel is "" for a subject with NO subject-level override (absent subject,
// or present but never configured) — such a subject's EFFECTIVE level is the
// registry's GLOBAL default, so a FIRST-TIME set is classified against
// globalLevel as the baseline: declaring a level below the global default (e.g.
// NONE under a global BACKWARD) is an effective lowering and is gated exactly
// like an explicit Lower. A first-time set that PINS the global level verbatim
// changes nothing effective and stays an ungated Raise.
//
// globalLevel == "" means the global default is UNKNOWN (older SR without
// GET /config, or the read failed — live-state readers deliberately do not fail
// the run on it). The explicit fallback is then the legacy behavior: "" ranks
// -1, so ANY initial set classifies as a Raise (ungated).
//
// Truth table (baseline = liveLevel if set, else globalLevel):
//
//	baseline source      | desired vs baseline          | action
//	explicit live level  | below / sideways             | Lower (gated)
//	explicit live level  | above                         | Raise
//	known global (1st)   | below / sideways             | Lower (gated)
//	known global (1st)   | equal value (exact pin)      | Raise (writes the override)
//	known global (1st)   | above                         | Raise
//	unknown global (1st) | n/a (baseline "")             | Raise (legacy fallback)
func compatibilityOp(ds DesiredSchema, liveLevel, globalLevel string) operations.Operation {
	// Baseline = explicit subject level if set, else the global level if
	// known, else "" (legacy rank -1 fallback).
	baseline := liveLevel
	if baseline == "" {
		baseline = globalLevel
	}
	var action operations.Action
	// Unknown compatibility levels rank -1; combined with the caller's
	// ds.Compatibility != liveLevel guard, two DISTINCT unknown levels (equal
	// rank, different value) fall through to the default branch and are
	// conservatively classified as Lower (gated), mirroring the sideways
	// rationale. Upstream validation restricts compatibility to the 7 known
	// values, so this is a defensive fallback.
	switch {
	case liveLevel == "" && baseline != "" && ds.Compatibility == baseline:
		// First-time set pinning exactly the effective (global) level: nothing
		// is weakened, so it is a Raise (Low, ungated) — without this case the
		// equal-rank-same-value pair would fall into the sideways Lower branch.
		action = operations.RaiseSchemaCompatibility
	case compatRank(ds.Compatibility) > compatRank(baseline):
		// Strictly stronger constraint => raise (Low).
		action = operations.RaiseSchemaCompatibility
	default:
		// Lower, or sideways (equal rank, different value) => Lower (High, gated).
		action = operations.LowerSchemaCompatibility
	}
	op := operations.New(action)
	op.Kind = "Schema"
	op.Target = ds.Subject
	op.Subject = ds.Subject
	op.Compatibility = ds.Compatibility
	op.Topic = ds.Topic
	op.Mode = ds.Mode
	// Surface the target level in rendered output. From shows the EFFECTIVE
	// current level: the explicit subject level, or (first-time set) the known
	// global default the subject inherits — so a gated first-time Lower renders
	// as e.g. "BACKWARD -> NONE" rather than a baffling "-> NONE".
	op.Field = "compatibility"
	op.From = baseline
	op.To = ds.Compatibility
	// First-time set: the baseline is the INHERITED global default, not an
	// explicit subject-level value. From alone doesn't convey that provenance
	// (it looks like an explicit live level), so annotate it via Message —
	// From itself stays machine-consumed and unchanged.
	if liveLevel == "" && baseline != "" {
		op.Message = fmt.Sprintf(
			"compatibility baseline %s inherited from the registry global default", baseline)
	}
	return op
}

// SchemaEqual reports whether a desired schema definition is canonically equal
// to a live one (spec §12.1 "in sync" comparison). Exported for the live-state
// readers (CLI computeOps, operator observeTopicLive), which use it to decide
// whether a desired subject diverges from the latest version and therefore
// needs a LookupSchema supersession probe.
func SchemaEqual(typ, desired, live string) bool { return schemaEqual(typ, desired, live) }

// schemaEqual reports whether two schema definitions are semantically equal for
// drift purposes. For AVRO and JSON it canonicalizes each by round-tripping
// through encoding/json (Go marshals map keys in sorted order, yielding a
// canonical form that ignores whitespace and object key ordering, while
// preserving number literals verbatim — see canonicalJSON); if either side
// fails to parse as JSON it falls back to a trimmed-string compare. For
// PROTOBUF (and any other type) it compares trimmed strings verbatim.
func schemaEqual(typ, a, b string) bool {
	switch typ {
	case "AVRO", "JSON":
		ca, ok1 := canonicalJSON(a)
		cb, ok2 := canonicalJSON(b)
		if ok1 && ok2 {
			return ca == cb
		}
		return strings.TrimSpace(a) == strings.TrimSpace(b)
	default:
		return strings.TrimSpace(a) == strings.TrimSpace(b)
	}
}

// canonicalJSON decodes s into a generic value and re-marshals it; the result
// is canonical with respect to object key order (json.Marshal sorts map keys)
// and whitespace (normalized away). NUMBER LITERALS ARE PRESERVED VERBATIM:
// decoding uses json.Number (not float64), so a literal keeps its original
// text and is never rounded — this avoids corrupting integers beyond float64's
// exact range (e.g. 9223372036854775807, common in Avro `default` values) and
// avoids false drift. A consequence is that distinct literals are NOT
// considered equal: "10" and "10.0" canonicalize to different forms. The bool
// is false if s is not valid JSON (callers then fall back to a trimmed-string
// compare).
func canonicalJSON(s string) (string, bool) {
	dec := json.NewDecoder(bytes.NewReader([]byte(s)))
	dec.UseNumber()
	var v interface{}
	if err := dec.Decode(&v); err != nil {
		return "", false
	}
	b, err := json.Marshal(v)
	if err != nil {
		return "", false
	}
	return string(b), true
}

// compatRank assigns a total-order rank to schema-registry compatibility levels
// so that raise vs. lower is decidable. Higher rank == stronger constraint:
//
//	NONE                = 0
//	BACKWARD, FORWARD   = 1
//	BACKWARD_TRANSITIVE, FORWARD_TRANSITIVE = 2
//	FULL                = 3
//	FULL_TRANSITIVE     = 4
//	unknown             = -1
//
// BACKWARD and FORWARD share a rank (as do their transitive variants): they are
// incomparable directions of the same strength. A change between two levels of
// EQUAL rank but DIFFERENT value (e.g. BACKWARD -> FORWARD) is "sideways"; it is
// conservatively treated as a Lower (gated, High risk) rather than a Raise,
// because it relaxes the previously-guaranteed direction.
func compatRank(level string) int {
	switch level {
	case "NONE":
		return 0
	case "BACKWARD", "FORWARD":
		return 1
	case "BACKWARD_TRANSITIVE", "FORWARD_TRANSITIVE":
		return 2
	case "FULL":
		return 3
	case "FULL_TRANSITIVE":
		return 4
	default:
		return -1
	}
}
