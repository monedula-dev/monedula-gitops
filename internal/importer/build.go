package importer

import (
	"fmt"
	"sort"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/monedula-dev/monedula-gitops/internal/access"
	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	"github.com/monedula-dev/monedula-gitops/internal/mds"
	"github.com/monedula-dev/monedula-gitops/internal/rbac"
	"github.com/monedula-dev/monedula-gitops/internal/schemaregistry"
	"github.com/monedula-dev/monedula-gitops/internal/schemaregistry/recordname"
)

const importedFromAnnotation = "gitops.monedula.dev/imported-from-cluster"

// placementConstraintsKey is the Confluent topic config that drives replica
// placement. It mirrors the consts of the same name in internal/validation and
// internal/defaulting: validation rejects manifests that set both
// spec.replicationFactor and this config, so the importer must omit the RF
// whenever the live config carries the constraint.
const placementConstraintsKey = "confluent.placement.constraints"

// Result is the output of Build: reconstructed topic and access-policy manifests
// plus any warnings about lossy reconstruction decisions.
type Result struct {
	Topics      []*v1alpha1.KafkaTopic
	Policies    []*v1alpha1.KafkaAccessPolicy
	Quotas      []*v1alpha1.KafkaQuota
	SchemaFiles []SchemaFile
	Warnings    []string
	// RoleBindings holds explicit KafkaRoleBinding manifests for role bindings
	// that did not fold into topic access (spec §40 import).
	RoleBindings []*v1alpha1.KafkaRoleBinding
	// Users holds reconstructed KafkaUser manifests, one per live SCRAM
	// principal (minus the connecting principal, unless
	// --include-connecting-user was requested). See buildUsers.
	Users []*v1alpha1.KafkaUser
	// RecordNameSubjects holds the RecordName-strategy subjects recognized during
	// snapshot gathering. They cannot be attributed to a topic from cluster state
	// alone (no topic in the subject name; schema may be shared), so they are
	// surfaced in the import report for manual attribution rather than written into
	// manifests (spec §24.1).
	RecordNameSubjects []RecordNameSubject
}

// SchemaFile is a schema body to be written verbatim alongside the manifests.
// Namespace is stamped by AssignNamespaces; the file is written to
// <dir>/<Namespace>/schemas/<BaseName>.<Ext>. MetaName is the owning topic's
// Metadata.Name, giving AssignNamespaces an explicit link to the topic (rather
// than recovering it by string-stripping BaseName).
type SchemaFile struct {
	Namespace string
	MetaName  string
	BaseName  string
	Ext       string
	Content   string
}

// schemaExt maps a Schema Registry type to the schema file extension. The second
// return is false when the type is unrecognized (extension defaults to "txt"),
// letting the caller emit a deterministic warning.
func schemaExt(t schemaregistry.SchemaType) (string, bool) {
	switch t {
	case schemaregistry.AVRO:
		return "avsc", true
	case schemaregistry.JSON:
		return "json", true
	case schemaregistry.PROTOBUF:
		return "proto", true
	default:
		return "txt", false
	}
}

// schemaFileForSlot builds the on-disk schema file for one subject slot
// (value, key, or an explicit/report-only slot). fileName is both the file's
// BaseName and the identifier used in the unknown-schema-type warning, so
// callers control whether it reads as the strategy-derived slot name
// ("<metaName>-value") or the live subject name (explicit fallback files).
//
// It warns AT MOST ONCE per call about an unknown schema type; callers must
// not re-warn for the same slot. In particular, verifySchemaSubjects's
// fallback re-emits the SAME subjects applySchemas already warned about via
// the strategy-derived slot names, so it calls this helper with warn=false to
// avoid a second, differently-worded warning for the same root cause.
func schemaFileForSlot(fileName string, metaName string, state *schemaregistry.SubjectState, warn bool, warnings *[]string) SchemaFile {
	ext, known := schemaExt(state.Schema.Type)
	if !known && warn {
		*warnings = append(*warnings, fmt.Sprintf(
			"subject %q has unknown schema type %q; writing as .txt",
			fileName, state.Schema.Type))
	}
	return SchemaFile{
		MetaName: metaName,
		BaseName: fileName,
		Ext:      ext,
		Content:  state.Schema.Definition,
	}
}

// stateToACL converts a kafka.ACLState into an access.ACL (identical fields).
func stateToACL(s kafka.ACLState) access.ACL {
	return access.ACL{
		Principal:    s.Principal,
		Host:         s.Host,
		ResourceType: s.ResourceType,
		ResourceName: s.ResourceName,
		PatternType:  s.PatternType,
		Operation:    s.Operation,
		Permission:   s.Permission,
	}
}

// BuildOptions carries CLI-only inputs to Build that most callers do not need
// (a trailing variadic keeps every existing 4-arg call site compiling
// unchanged; only the CLI's `import cluster` command needs to pass one).
type BuildOptions struct {
	// ConnectingUser is the bare username (no "User:" prefix) the importer
	// authenticated to the cluster as, or "" when it could not be resolved
	// (mTLS/OAuth clusters, or an unresolvable secret reference). When
	// non-empty, that user's SCRAM credential is skipped from Users unless
	// IncludeConnectingUser is set — importing your own credential risks
	// self-lockout (spec §30.3-style guard).
	ConnectingUser string
	// IncludeConnectingUser forces the connecting principal's credential to be
	// captured anyway (CLI flag --include-connecting-user).
	IncludeConnectingUser bool
}

// Build converts a snapshot into manifests stamped with clusterName as
// clusterRef. It reconstructs producer/consumer access where possible and emits
// everything else as raw KafkaAccessPolicy rules and/or KafkaRoleBinding
// manifests. A two-sided recompile-verify safety net guarantees the round-trip
// invariant on every active backend: compiling the Result reproduces the
// snapshot ACL set (ACL backend) and the snapshot role-binding set (RBAC
// backend). When either side fails, a fallbackAllExplicit discards all folded
// access and re-emits every live grant as its backend-specific resource for
// fidelity. Schema reconstruction has the analogous safety net
// (verifySchemaSubjects): every emitted spec.schema block is verified to
// recompute the live subjects on apply, and topics that fail fall back to
// explicit, report-only schema files. SCRAM credentials become KafkaUser
// manifests (see buildUsers). Namespaces are assigned in a later step.
func Build(snap Snapshot, clusterName string, accessBackends []string, mdsCfg *v1alpha1.MDSConfig, opts ...BuildOptions) Result {
	var warnings []string
	var opt BuildOptions
	if len(opts) > 0 {
		opt = opts[0]
	}

	aclActive := backendActive(accessBackends, "acl")
	rbacActive := backendActive(accessBackends, "rbac")

	// --- 1. Topics ---
	topics := make([]*v1alpha1.KafkaTopic, 0, len(snap.Topics))
	topicByName := make(map[string]*v1alpha1.KafkaTopic, len(snap.Topics))
	// topicNameOwner tracks already-assigned metadata.names so distinct topics
	// whose Kafka names slug to the same base (e.g. "Orders_V2" and "orders-v2"
	// both -> "orders-v2") get deterministic "-2", "-3", ... suffixes instead of
	// silently colliding (mirrors buildPolicies/role-binding naming). Topics are
	// visited in snapshot order, which snapshot gathering sorts by name, so the
	// disambiguation is stable across Build calls.
	topicNameOwner := map[string]bool{}
	for _, ts := range snap.Topics {
		base := topicMetaName(ts.Name)
		metaName := disambiguateName(base, topicNameOwner)
		if metaName != base {
			warnings = append(warnings, fmt.Sprintf(
				"topic name collision: topic %q slugs to a metadata.name already used by another topic; renamed to %q for uniqueness", ts.Name, metaName))
		}
		tp := &v1alpha1.KafkaTopic{
			TypeMeta: metav1.TypeMeta{APIVersion: v1alpha1.APIVersion, Kind: "KafkaTopic"},
			ObjectMeta: metav1.ObjectMeta{
				Name:        metaName,
				Annotations: map[string]string{importedFromAnnotation: clusterName},
			},
			Spec: v1alpha1.KafkaTopicSpec{
				ClusterRef: v1alpha1.ClusterRef{Name: clusterName},
				// TopicName is ALWAYS set explicitly (even when metaName == ts.Name):
				// defaulting.SetDefaults falls back to ObjectMeta.Name when TopicName
				// is empty, which would resolve the real Kafka topic name from the
				// SLUGGED metadata.name instead of the live name whenever the two
				// diverge. Setting it unconditionally here means slugging never
				// changes the topic the manifest targets.
				TopicName:      ts.Name,
				Partitions:     ts.Partitions,
				DeletionPolicy: "Orphan",
			},
		}
		// Omit the replication factor when the topic carries a replica-placement
		// constraint: validation treats the two as mutually exclusive (the
		// constraint determines replication), so emitting both would generate a
		// manifest the tool itself rejects.
		_, hasPlacement := ts.Config[placementConstraintsKey]
		if ts.ReplicationFactor > 0 && !hasPlacement {
			rf := ts.ReplicationFactor
			tp.Spec.ReplicationFactor = &rf
		}
		if len(ts.Config) > 0 {
			tp.Spec.Config = ts.Config
		}
		topics = append(topics, tp)
		topicByName[ts.Name] = tp
	}

	// --- 2. Schemas: set spec.schema and record verbatim schema files ---
	// Schema warnings accumulate in their own slice so the fallbackAllExplicit
	// path (which rebuilds the Result from scratch) can re-attach them: schema
	// diagnostics must not vanish exactly when access reconstruction fails.
	var schemaWarnings []string
	schemaFiles := applySchemas(snap, topicByName, &schemaWarnings)

	// --- 2b. Schema round-trip verify safety net ---
	// Mirrors step 5 for access: never emit a spec.schema block whose
	// recomputed subjects differ from the live subjects attributed to the topic.
	schemaFiles = verifySchemaSubjects(snap, topicByName, schemaFiles, &schemaWarnings)
	warnings = append(warnings, schemaWarnings...)

	// --- 3. Classify ACLs (when ACL backend active) ---
	var raw []access.ACL
	if aclActive {
		raw = classifyAndFold(snap, topicByName, &warnings)
	}

	// --- 4. Raw ACLs -> policies ---
	policies := buildPolicies(raw, clusterName, &warnings)

	// --- 4b. RBAC fold (when RBAC backend active AND live bindings exist) ---
	var roleBindings []*v1alpha1.KafkaRoleBinding
	if rbacActive && len(snap.RoleBindings) > 0 {
		leftover := foldRoleBindings(snap.RoleBindings, topicByName)
		nameOwner := map[string]bool{}
		for _, rb := range leftover {
			m := roleBindingManifest(rb, clusterName)
			base := m.Name
			m.Name = disambiguateName(base, nameOwner)
			if m.Name != base {
				warnings = append(warnings, fmt.Sprintf("role binding name collision: %q renamed to %q for uniqueness", base, m.Name))
			}
			roleBindings = append(roleBindings, m)
		}
		sort.Slice(roleBindings, func(i, j int) bool {
			return roleBindings[i].Name < roleBindings[j].Name
		})
	}

	// --- 4c. Quotas ---
	quotas := buildQuotas(snap, clusterName, &warnings)

	// --- 4d. Users (SCRAM credentials) ---
	users := buildUsers(snap, clusterName, opt, &warnings)

	sort.Strings(warnings)
	r := Result{Topics: topics, Policies: policies, RoleBindings: roleBindings, Quotas: quotas, Users: users, SchemaFiles: schemaFiles, Warnings: warnings, RecordNameSubjects: snap.RecordNameSubjects}

	// --- 5. Two-sided recompile-verify safety net ---
	aclOK := !aclActive || roundTripsClean(r, snap)
	rbacOK := !rbacActive || roleBindingsRoundTripClean(r, snap, mdsCfg)
	if !aclOK || !rbacOK {
		r = fallbackAllExplicit(snap, topics, clusterName, aclActive, rbacActive)
		// spec.schema lives on the topic pointers (untouched by the fallback,
		// which only clears Access); re-attach the recorded schema files, the
		// accumulated schema warnings, and the RecordName report so they
		// survive the fallback path. Quotas and Users are independent of
		// ACL/RBAC reconstruction entirely (fallbackAllExplicit only rebuilds
		// Topics/Policies/RoleBindings), so they must be re-attached too or
		// they would silently vanish whenever the fallback triggers.
		r.SchemaFiles = schemaFiles
		r.RecordNameSubjects = snap.RecordNameSubjects
		r.Quotas = quotas
		r.Users = users
		r.Warnings = append(r.Warnings, schemaWarnings...)
		r.Warnings = append(r.Warnings,
			"access reconstruction could not reproduce live state on all active backends; emitted all ACLs as KafkaAccessPolicy and all role bindings as KafkaRoleBinding for fidelity")
		// Re-verify the ACL side after the fallback. The fallback emits every
		// live ACL as a raw rule, so the only way it can still fail is an
		// internal Allow/Deny conflict on the same subject in the live set,
		// which BuildDesiredSet drops. Surface that honestly rather than
		// assuming the guarantee. No loop: this is the final attempt.
		if aclActive && !roundTripsClean(r, snap) {
			r.Warnings = append(r.Warnings,
				"WARNING: imported ACLs could not be represented faithfully (conflicting Allow/Deny on the same subject)")
		}
		sort.Strings(r.Warnings)
	}

	return r
}

// applySchemas sets spec.schema on each topic with a matched value subject and
// returns the verbatim schema files to write. Subjects that did not map to an
// imported topic (snap.UnmatchedSubjects) are reported as warnings. The returned
// files and emitted warnings are deterministic (topics are visited in snapshot
// order, which is sorted by name).
//
// A spec.schema block carries a SINGLE subjectStrategy for both slots, so when
// the value and key slots were attributed via DIFFERENT strategies no block can
// reproduce both live subjects — applying one would compute a subject that does
// not exist live and create it (mutating the registry). Such topics fall back
// to explicit, report-only schema files (see explicitSchemaFiles) with a
// warning. Likewise, the block carries a single compatibility level: a key
// subject whose explicit level diverges from the value subject's is warned
// about (the manifest carries the value subject's level).
func applySchemas(snap Snapshot, topicByName map[string]*v1alpha1.KafkaTopic, warnings *[]string) []SchemaFile {
	var files []SchemaFile

	for _, ts := range snap.Topics {
		s, ok := snap.Schemas[ts.Name]
		if !ok {
			continue
		}
		tp := topicByName[ts.Name]
		metaName := tp.Name

		if s.Value == nil {
			// A value subject is required to set spec.schema. A key-only subject
			// would silently vanish, so surface it.
			if s.Key != nil {
				*warnings = append(*warnings, fmt.Sprintf(
					"subject %q imported without a value subject; skipped (value schema required)",
					metaName+"-key"))
			}
			continue
		}

		strategy := effectiveSlotStrategy(s.ValueStrategy)
		keyStrategy := effectiveSlotStrategy(s.KeyStrategy)

		// Mixed per-slot strategies: no representable spec.schema block. Fall
		// back to explicit schema files so apply never mutates the registry.
		// (see also step 2b for schemas — the round-trip verify below is the
		// same kind of safety net as step 5's recompile-verify for access)
		if s.Key != nil && keyStrategy != strategy {
			*warnings = append(*warnings, fmt.Sprintf(
				"topic %q: value subject %q (strategy %s) and key subject %q (strategy %s) use different subject strategies; spec.schema cannot represent both, so schema bodies are emitted as files only — attribute them manually",
				ts.Name, s.Value.Subject, strategy, s.Key.Subject, keyStrategy))
			files = append(files, explicitSchemaFiles(metaName, s, true, warnings)...)
			continue
		}

		// Key-subject compatibility divergence: spec.schema.compatibility is
		// stamped onto BOTH subjects on apply, so a diverging explicit key level
		// would be silently rewritten. Warn instead of hiding the mutation.
		if s.Key != nil && s.Key.Compatibility != "" && s.Key.Compatibility != s.Value.Compatibility {
			*warnings = append(*warnings, fmt.Sprintf(
				"topic %q: key subject %q compatibility %q differs from value subject %q compatibility %q; the manifest carries the value subject's level — align the key subject manually or manage it explicitly",
				ts.Name, s.Key.Subject, s.Key.Compatibility, s.Value.Subject, s.Value.Compatibility))
		}

		valueFile := schemaFileForSlot(metaName+"-value", metaName, s.Value, true, warnings)
		sc := &v1alpha1.TopicSchema{
			Format:          string(s.Value.Schema.Type),
			SubjectStrategy: strategy,
			ValueSchema: &v1alpha1.ValueFrom{
				ValueFrom: v1alpha1.ValueSource{File: "../schemas/" + valueFile.BaseName + "." + valueFile.Ext},
			},
		}
		if s.Value.Compatibility != "" {
			sc.Compatibility = s.Value.Compatibility
		}
		files = append(files, valueFile)

		if s.Key != nil {
			keyFile := schemaFileForSlot(metaName+"-key", metaName, s.Key, true, warnings)
			sc.KeySchema = &v1alpha1.ValueFrom{
				ValueFrom: v1alpha1.ValueSource{File: "../schemas/" + keyFile.BaseName + "." + keyFile.Ext},
			}
			files = append(files, keyFile)
		}

		tp.Spec.Schema = sc
	}

	for _, subject := range snap.UnmatchedSubjects {
		*warnings = append(*warnings, fmt.Sprintf(
			"subject %q not imported (does not map to an imported topic via TopicName/TopicRecordName)", subject))
	}
	*warnings = append(*warnings, snap.SchemaAmbiguities...)
	sort.Strings(*warnings)

	return files
}

// effectiveSlotStrategy maps a recorded per-slot strategy to the strategy a
// manifest would carry on apply: the zero value "" behaves as "TopicName"
// (defaulting does not stamp a strategy; mirrors recordname.Subjects).
func effectiveSlotStrategy(s string) string {
	if s == "" {
		return "TopicName"
	}
	return s
}

// explicitSchemaFiles returns report-only schema files for a topic whose live
// subjects cannot be represented by a spec.schema block (mixed per-slot
// strategies, or a failed schema round-trip verify). The files are named after
// the LIVE subject — not the strategy-derived "<metaName>-value" base name — so
// the operator can attribute them manually, and no manifest references them:
// applying the import output never touches these subjects. This mirrors the
// report-only handling of RecordName subjects (spec §24.1).
//
// warn controls whether an unknown-schema-type warning is emitted for these
// slots. Callers reaching this function via the applySchemas mixed-strategy
// path (see step 2b's comment block) pass true: applySchemas returned before
// ever building a SchemaFile for this topic, so this is the FIRST and ONLY
// opportunity to warn. Callers reaching it via verifySchemaSubjects's fallback
// pass false: applySchemas already built (and warned about, if unknown) the
// strategy-derived value/key files for this topic before the verify step
// discarded them, so warning again here would duplicate that warning under a
// different (live-subject) name for the same root cause.
func explicitSchemaFiles(metaName string, s TopicSchemas, warn bool, warnings *[]string) []SchemaFile {
	var files []SchemaFile
	for _, state := range []*schemaregistry.SubjectState{s.Value, s.Key} {
		if state == nil {
			continue
		}
		files = append(files, schemaFileForSlot(state.Subject, metaName, state, warn, warnings))
	}
	return files
}

// verifySchemaSubjects is the schema side of the recompile-verify safety net,
// mirroring roundTripsClean / roleBindingsRoundTripClean for ACLs/RBAC: for
// every topic whose manifest carries a spec.schema block, recompute the
// subjects that block would produce on apply — via recordname.Subjects, the
// same computation the CLI pipeline and the operator use — and compare them
// against the live subjects attributed to the topic during snapshot gathering.
//
// A mismatch (or a recompute error) means applying the manifest would MUTATE
// the Schema Registry: the pipeline would target a subject that differs from
// the live one and create or overwrite it instead of converging. Such topics
// fall back to explicit schema files: spec.schema is cleared, the
// strategy-derived files are replaced with live-subject-named files, and a
// warning is recorded. Returns the (possibly rewritten) schema file list.
// Deterministic: topics are visited in snapshot order (sorted by name).
func verifySchemaSubjects(snap Snapshot, topicByName map[string]*v1alpha1.KafkaTopic, files []SchemaFile, warnings *[]string) []SchemaFile {
	for _, ts := range snap.Topics {
		s, ok := snap.Schemas[ts.Name]
		if !ok || s.Value == nil {
			continue
		}
		tp := topicByName[ts.Name]
		sc := tp.Spec.Schema
		if sc == nil {
			continue // already report-only (mixed strategies or key-only subject)
		}

		var keyDef, wantKey string
		if s.Key != nil {
			keyDef = s.Key.Schema.Definition
			wantKey = s.Key.Subject
		}
		gotValue, gotKey, err := recordname.Subjects(sc.SubjectStrategy, tp.Spec.TopicName, sc, s.Value.Schema.Definition, keyDef)
		if err == nil && gotValue == s.Value.Subject && gotKey == wantKey {
			continue // round-trips clean: applying this block converges on live state
		}

		if err != nil {
			*warnings = append(*warnings, fmt.Sprintf(
				"topic %q: schema round-trip verify failed (%v); schema bodies are emitted as files only — attribute them manually",
				ts.Name, err))
		} else {
			*warnings = append(*warnings, fmt.Sprintf(
				"topic %q: schema round-trip verify failed: applying the manifest would target subjects (value %q, key %q) but the live subjects are (value %q, key %q); schema bodies are emitted as files only — attribute them manually",
				ts.Name, gotValue, gotKey, s.Value.Subject, wantKey))
		}

		// Fall back: drop the schema block and its strategy-derived files, then
		// re-emit the live bodies as explicit (report-only) files.
		tp.Spec.Schema = nil
		metaName := tp.Name
		kept := make([]SchemaFile, 0, len(files))
		for _, f := range files {
			if f.MetaName != metaName {
				kept = append(kept, f)
			}
		}
		// warn=false: applySchemas already built and warned about (if unknown)
		// the strategy-derived value/key files for this topic before this verify
		// step discarded them; re-warning here under the live-subject name would
		// duplicate that warning for the same root cause.
		files = append(kept, explicitSchemaFiles(metaName, s, false, warnings)...)
	}
	// Sort for symmetry with applySchemas, which ends in sort.Strings(*warnings):
	// Build appends schemaWarnings (accumulated across both applySchemas and this
	// function) into the overall warnings slice, which is itself sorted again, but
	// callers of verifySchemaSubjects directly (as tests do) must see a
	// deterministic order without relying on that later sort.
	sort.Strings(*warnings)
	return files
}

// foldEligible reports whether an ACL can participate in producer/consumer
// folding: it must be a plain Allow grant with a literal pattern. Host-scoped
// grants fold too (spec §8.4) — they are keyed by (principal, host) so a given
// host's grants only combine with one another.
func foldEligible(a access.ACL) bool {
	return a.Permission == "Allow" && a.PatternType == "literal"
}

// opSet builds a set of operations from a slice of ACLs.
func opSet(acls []access.ACL) map[string]bool {
	s := make(map[string]bool, len(acls))
	for _, a := range acls {
		s[a.Operation] = true
	}
	return s
}

func setEquals(s map[string]bool, want ...string) bool {
	if len(s) != len(want) {
		return false
	}
	for _, w := range want {
		if !s[w] {
			return false
		}
	}
	return true
}

// classifyAndFold folds eligible ACLs into topic producer/consumer access and
// returns the remaining (raw) ACLs that must become policy rules.
func classifyAndFold(snap Snapshot, topicByName map[string]*v1alpha1.KafkaTopic, warnings *[]string) []access.ACL {
	var raw []access.ACL

	// Partition into eligible and ineligible; group eligible by (principal, host).
	// Host-scoped grants fold too (spec §8.4), but only with other grants from the
	// same host: a producer reachable from two hosts yields two ProducerAccess
	// entries, and a consumer's topic-Read and group-Read must share a host to pair.
	type principalHost struct{ principal, host string }
	byPH := map[principalHost][]access.ACL{}
	var keys []principalHost
	for _, s := range snap.ACLs {
		a := stateToACL(s)
		if !foldEligible(a) {
			raw = append(raw, a)
			continue
		}
		k := principalHost{a.Principal, a.Host}
		if _, ok := byPH[k]; !ok {
			keys = append(keys, k)
		}
		byPH[k] = append(byPH[k], a)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].principal != keys[j].principal {
			return keys[i].principal < keys[j].principal
		}
		return keys[i].host < keys[j].host
	})

	for _, key := range keys {
		p := key.principal
		// host is the bucket's source host; stamp it on emitted access only when
		// it is not the wildcard "*", so common manifests stay clean via omitempty.
		var host string
		if key.host != "*" {
			host = key.host
		}
		acls := byPH[key]

		// Bucket this principal's eligible ACLs.
		topicACLs := map[string][]access.ACL{} // topic -> acls (imported topics only)
		groupACLs := map[string][]access.ACL{} // group -> acls
		for _, a := range acls {
			switch a.ResourceType {
			case "topic":
				if _, ok := topicByName[a.ResourceName]; ok {
					topicACLs[a.ResourceName] = append(topicACLs[a.ResourceName], a)
				} else {
					raw = append(raw, a) // not an imported topic
				}
			case "group":
				groupACLs[a.ResourceName] = append(groupACLs[a.ResourceName], a)
			default:
				raw = append(raw, a)
			}
		}

		// Classify topic buckets into producers / consumer-topics; others raw.
		var producerTopics []string
		var consumerTopics []string
		for tName, tACLs := range topicACLs {
			ops := opSet(tACLs)
			switch {
			case setEquals(ops, "Write", "Describe"):
				producerTopics = append(producerTopics, tName)
			case setEquals(ops, "Read", "Describe"):
				consumerTopics = append(consumerTopics, tName)
			default:
				raw = append(raw, tACLs...)
			}
		}

		// Classify group buckets into groupRead; others raw.
		var groupReads []string
		for gName, gACLs := range groupACLs {
			if setEquals(opSet(gACLs), "Read") {
				groupReads = append(groupReads, gName)
			} else {
				raw = append(raw, gACLs...)
			}
		}
		sort.Strings(producerTopics)
		sort.Strings(consumerTopics)
		sort.Strings(groupReads)

		// Producers fold directly.
		for _, tName := range producerTopics {
			tp := topicByName[tName]
			tp.Spec.Access.Producers = append(tp.Spec.Access.Producers, v1alpha1.ProducerAccess{Principal: p, Host: host})
		}

		// Consumers require exactly one groupRead.
		switch {
		case len(consumerTopics) == 0:
			// No consumer topics: any groupReads are unpaired -> raw.
			for _, g := range groupReads {
				raw = append(raw, groupACLs[g]...)
			}
		case len(groupReads) == 1:
			g := groupReads[0]
			for _, tName := range consumerTopics {
				tp := topicByName[tName]
				tp.Spec.Access.Consumers = append(tp.Spec.Access.Consumers, v1alpha1.ConsumerAccess{Principal: p, Host: host, Group: g})
			}
		case len(groupReads) == 0:
			// Consumer topics present but no group-Read in this (principal,host)
			// bucket. This happens when the topic-Read and group-Read ACLs carry
			// different hosts and therefore land in different buckets after the
			// (principal,host) regroup (spec §8.4). Emit the consumer-topic ACLs
			// as raw rules and surface a host-specific warning so operators can
			// identify the mismatch.
			for _, tName := range consumerTopics {
				raw = append(raw, topicACLs[tName]...)
			}
			*warnings = append(*warnings, fmt.Sprintf(
				"principal %q has consumer topics %v with no matching group-Read in host %q; emitted as KafkaAccessPolicy",
				p, consumerTopics, key.host))
		default:
			// >1 groupReads with consumer topics: genuinely ambiguous — cannot
			// pick a single group for folding, so emit both the consumer-topic
			// ACLs and the group ACLs as raw rules.
			for _, tName := range consumerTopics {
				raw = append(raw, topicACLs[tName]...)
			}
			for _, g := range groupReads {
				raw = append(raw, groupACLs[g]...)
			}
			// consumerTopics is already sorted above; report the demoted topics
			// deterministically so operators can triage.
			*warnings = append(*warnings, fmt.Sprintf(
				"principal %q has ambiguous consumer group mapping (%d groups); topics %v emitted as KafkaAccessPolicy",
				p, len(groupReads), consumerTopics))
		}
	}

	sort.Strings(*warnings)
	return raw
}

// buildPolicies groups raw ACLs into one KafkaAccessPolicy per principal with
// deterministically ordered rules.
//
// Policy names are "imported-<slug(principal)>". Distinct principals can slug to
// the same base name (e.g. "User:a-b", "User:a:b" both -> "user-a-b"; all-non-
// ASCII principals collapse to bare "user"). To avoid two objects sharing a
// metadata.name (which would clobber files on write and overwrite each other on
// kubectl apply), names are disambiguated deterministically: principals are
// processed in sorted order, and when a base name is already taken by a
// DIFFERENT principal, the next free "-2", "-3", ... suffix is appended. Because
// the iteration order is stable, the same principal always maps to the same name
// within a Build. Each disambiguation records a warning.
func buildPolicies(raw []access.ACL, clusterName string, warnings *[]string) []*v1alpha1.KafkaAccessPolicy {
	if len(raw) == 0 {
		return nil
	}

	// Group by principal, then by (permission, host, resource).
	type ruleKey struct {
		permission, host, resType, resName, patternType string
	}
	byPrincipal := map[string]map[ruleKey]map[string]bool{} // principal -> ruleKey -> ops set
	var principals []string
	for _, a := range raw {
		if _, ok := byPrincipal[a.Principal]; !ok {
			byPrincipal[a.Principal] = map[ruleKey]map[string]bool{}
			principals = append(principals, a.Principal)
		}
		k := ruleKey{a.Permission, a.Host, a.ResourceType, a.ResourceName, a.PatternType}
		if byPrincipal[a.Principal][k] == nil {
			byPrincipal[a.Principal][k] = map[string]bool{}
		}
		byPrincipal[a.Principal][k][a.Operation] = true
	}
	sort.Strings(principals)

	// nameByBase maps an already-assigned base slug to the principal that owns
	// it, so a colliding (different) principal can be disambiguated.
	nameOwner := map[string]string{} // assigned metadata.name -> principal

	policies := make([]*v1alpha1.KafkaAccessPolicy, 0, len(principals))
	for _, p := range principals {
		ruleMap := byPrincipal[p]

		base := "imported-" + slug(p)
		name := base
		if owner, taken := nameOwner[name]; taken {
			// Collision with a different principal (sorted order guarantees the
			// owner is deterministic). Find the next free "-N" suffix.
			n := 2
			for {
				cand := fmt.Sprintf("%s-%d", base, n)
				if _, used := nameOwner[cand]; !used {
					name = cand
					break
				}
				n++
			}
			*warnings = append(*warnings, fmt.Sprintf(
				"policy name collision: principals %q and %q both slug to %q; renamed the latter to %q",
				owner, p, base, name))
		}
		nameOwner[name] = p

		rules := make([]v1alpha1.ACLRule, 0, len(ruleMap))
		for k, ops := range ruleMap {
			opList := make([]string, 0, len(ops))
			for op := range ops {
				opList = append(opList, op)
			}
			sort.Strings(opList)
			rules = append(rules, v1alpha1.ACLRule{
				Principal:  p,
				Permission: k.permission,
				Host:       k.host,
				Resource: v1alpha1.ACLResource{
					Type:        k.resType,
					Name:        k.resName,
					PatternType: k.patternType,
				},
				Operations: opList,
			})
		}
		sortRules(rules)
		policies = append(policies, &v1alpha1.KafkaAccessPolicy{
			TypeMeta: metav1.TypeMeta{APIVersion: v1alpha1.APIVersion, Kind: "KafkaAccessPolicy"},
			ObjectMeta: metav1.ObjectMeta{
				Name:        name,
				Annotations: map[string]string{importedFromAnnotation: clusterName},
			},
			Spec: v1alpha1.KafkaAccessPolicySpec{
				ClusterRef:     v1alpha1.ClusterRef{Name: clusterName},
				Rules:          rules,
				DeletionPolicy: "Delete",
			},
		})
	}
	sort.Slice(policies, func(i, j int) bool {
		return policies[i].Name < policies[j].Name
	})
	return policies
}

// sortRules orders rules by resource type, name, patternType, then permission.
func sortRules(rules []v1alpha1.ACLRule) {
	sort.Slice(rules, func(i, j int) bool {
		a, b := rules[i], rules[j]
		if a.Resource.Type != b.Resource.Type {
			return a.Resource.Type < b.Resource.Type
		}
		if a.Resource.Name != b.Resource.Name {
			return a.Resource.Name < b.Resource.Name
		}
		if a.Resource.PatternType != b.Resource.PatternType {
			return a.Resource.PatternType < b.Resource.PatternType
		}
		return a.Permission < b.Permission
	})
}

// maxNameLen is the Kubernetes metadata.name length limit (DNS-1123 subdomain).
const maxNameLen = 253

// nameSuffixReserve is the room left at the end of a truncated base name for a
// disambiguateName suffix ("-2", "-3", ...). Collisions are rare enough in
// practice that a handful of reserved characters comfortably covers them; if
// disambiguateName ever needs more than fits, the truncated base itself still
// disambiguates correctly since disambiguateName re-checks the full candidate
// against the taken set regardless of length.
const nameSuffixReserve = 6

// topicMetaName derives a DNS-1123-safe metadata.name from a live Kafka topic
// name, so names with underscores, uppercase, or other characters invalid as
// Kubernetes object names (e.g. "Orders_V2") become valid ones ("orders-v2").
// The live name is never lost: spec.topicName always carries it verbatim.
//
// Unlike slug() (used for ACL principals, which have no notion of hierarchy),
// this preserves '.' as a label separator: "." is a common, meaningful
// delimiter in Kafka topic names (e.g. "payments.orders") AND is itself valid
// in a DNS-1123 subdomain, so a dotted name that is ALREADY fully valid must
// come out byte-identical — collapsing it into slug()'s single-alphabet
// output would needlessly rename every already-valid dotted topic. Each
// '.'-delimited label is otherwise sanitized exactly like slug(): lowercased,
// non-alphanumeric runs collapsed to a single '-', leading/trailing '-'
// trimmed. Empty labels (from "..", a leading/trailing '.', or a label that
// sanitizes to nothing) are dropped, since DNS-1123 labels cannot be empty.
//
// Truncated to leave room for a disambiguateName suffix so a collision on a
// long name can still be resolved without exceeding maxNameLen.
func topicMetaName(topicName string) string {
	labels := strings.Split(topicName, ".")
	out := make([]string, 0, len(labels))
	for _, l := range labels {
		if s := slug(l); s != "" {
			out = append(out, s)
		}
	}
	s := strings.Join(out, ".")
	if s == "" {
		// Wholly non-alphanumeric topic name (e.g. all-punctuation, or "..."):
		// every label sanitized to nothing, which is not a usable metadata.name.
		// Fall back to a fixed placeholder; disambiguateName still guarantees
		// uniqueness across multiple such topics.
		s = "topic"
	}
	limit := maxNameLen - nameSuffixReserve
	if len(s) > limit {
		s = strings.Trim(s[:limit], ".-")
	}
	return s
}

// slug lowercases principal, replacing ':' and non-alphanumeric chars with '-',
// collapsing repeats and trimming leading/trailing '-'.
func slug(s string) string {
	var b strings.Builder
	prevDash := false
	for _, r := range strings.ToLower(s) {
		isAlnum := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')
		if isAlnum {
			b.WriteRune(r)
			prevDash = false
		} else {
			if !prevDash {
				b.WriteByte('-')
				prevDash = true
			}
		}
	}
	return strings.Trim(b.String(), "-")
}

// buildQuotas converts each kafka.QuotaState in the snapshot to a *v1alpha1.KafkaQuota.
// Results are sorted by metadata.name for determinism.
//
// Names are run through disambiguateName (mirrors buildPolicies and role
// binding naming): quotaStateToEntity's key derivation resists the common
// collision shapes by construction, but joining components with "-" can still
// alias across component boundaries (e.g. entity {clientId: "b-user-a"} vs.
// {user: "a", clientId: "b"}) — without this pass, WriteToDir/RenderManifests
// would silently drop one of two distinct quotas onto the same path/name.
func buildQuotas(snap Snapshot, clusterName string, warnings *[]string) []*v1alpha1.KafkaQuota {
	if len(snap.Quotas) == 0 {
		return nil
	}
	quotas := make([]*v1alpha1.KafkaQuota, 0, len(snap.Quotas))
	nameOwner := map[string]bool{}
	for _, qs := range snap.Quotas {
		entity, entityKey := quotaStateToEntity(qs)
		limits := quotaStateLimits(qs)
		base := quotaMetaName(entityKey)
		name := disambiguateName(base, nameOwner)
		if name != base {
			*warnings = append(*warnings, fmt.Sprintf(
				"quota name collision: entity key %q renamed to %q for uniqueness", entityKey, name))
		}
		quotas = append(quotas, &v1alpha1.KafkaQuota{
			TypeMeta: metav1.TypeMeta{APIVersion: v1alpha1.APIVersion, Kind: "KafkaQuota"},
			ObjectMeta: metav1.ObjectMeta{
				Name:        name,
				Annotations: map[string]string{importedFromAnnotation: clusterName},
			},
			Spec: v1alpha1.KafkaQuotaSpec{
				ClusterRef: v1alpha1.ClusterRef{Name: clusterName},
				Entity:     entity,
				Limits:     limits,
			},
		})
	}
	sort.Slice(quotas, func(i, j int) bool {
		return quotas[i].Name < quotas[j].Name
	})
	return quotas
}

// quotaDefaultSentinel marks a per-type default component in the entity key
// (see quotaStateToEntity). quotaMetaName runs the joined key through slug(),
// which maps every non-alphanumeric character to "-" and collapses repeats —
// so a punctuation-only marker (e.g. a NUL byte) would slug away to nothing
// and CAN'T distinguish the sentinel from a literal name that happens to spell
// "default". The marker must therefore survive slugging as distinct
// alphanumeric content: "0dflt0" (digit-led, so it cannot itself collide with
// a bare word a real entity name would plausibly contain, and the leading/
// trailing digits make it visually distinct from "default" in the rendered
// name).
const quotaDefaultSentinel = "0dflt0"

// quotaStateToEntity converts a QuotaState's entity components into a QuotaEntity
// and returns a stable entity key for name derivation.
//
// A literal name contributes "<type>-<name>" (unescaped, so the common case —
// a single named user or client-id — stays human-readable, e.g.
// "user-alice" -> metadata.name "quota-user-alice"). The per-type default
// contributes "<type>-<quotaDefaultSentinel>" instead of the old
// "<type>-default": that old scheme made {userDefault: true} and
// {user: "default"} both produce the literal string "user-default", which is
// exactly the collision this fixes — the sentinel token cannot be produced by
// any literal name (it isn't the word "default").
//
// Joining multiple components with "-" can still alias across DIFFERENT
// component boundaries (e.g. a client-id literally containing "-user-a" can
// produce the same joined string as a separate user component) — that
// residual ambiguity is caught by buildQuotas running every derived name
// through disambiguateName, the same safety net buildPolicies/role-binding
// naming use.
func quotaStateToEntity(qs kafka.QuotaState) (v1alpha1.QuotaEntity, string) {
	var entity v1alpha1.QuotaEntity
	var keyParts []string
	part := func(typ string, name *string) string {
		if name == nil {
			return typ + "-" + quotaDefaultSentinel
		}
		return typ + "-" + *name
	}
	for _, c := range qs.Entity {
		switch c.Type {
		case "user":
			if c.Name != nil {
				entity.User = "User:" + *c.Name
			} else {
				entity.UserDefault = true
			}
			keyParts = append(keyParts, part("user", c.Name))
		case "client-id":
			if c.Name != nil {
				entity.ClientId = *c.Name
			} else {
				entity.ClientIdDefault = true
			}
			keyParts = append(keyParts, part("client-id", c.Name))
		case "ip":
			if c.Name != nil {
				entity.Ip = *c.Name
			} else {
				entity.IpDefault = true
			}
			keyParts = append(keyParts, part("ip", c.Name))
		}
	}
	sort.Strings(keyParts)
	return entity, strings.Join(keyParts, "-")
}

// quotaStateLimits maps the kafka limit key map to QuotaLimits fields.
func quotaStateLimits(qs kafka.QuotaState) v1alpha1.QuotaLimits {
	var l v1alpha1.QuotaLimits
	for k, v := range qs.Limits {
		vCopy := v
		switch k {
		case "producer_byte_rate":
			l.ProducerByteRate = &vCopy
		case "consumer_byte_rate":
			l.ConsumerByteRate = &vCopy
		case "request_percentage":
			l.RequestPercentage = &vCopy
		case "controller_mutation_rate":
			l.ControllerMutationRate = &vCopy
		case "connection_creation_rate":
			l.ConnectionCreationRate = &vCopy
		}
	}
	return l
}

// quotaMetaName derives a DNS-1123-safe metadata.name from the entity key.
// It prepends "quota-" and sanitizes non-alnum chars to "-", collapsing repeats
// and trimming trailing/leading dashes. Truncated to leave room for a
// disambiguateName suffix (mirrors topicMetaName), so a collision on a long
// key can still be resolved without exceeding maxNameLen.
func quotaMetaName(entityKey string) string {
	const prefix = "quota-"
	sanitized := slug(entityKey)
	name := prefix + sanitized
	limit := maxNameLen - nameSuffixReserve
	if len(name) > limit {
		name = name[:limit]
	}
	return strings.Trim(name, "-")
}

// defaultScramIterations mirrors kafka/franz.defaultScramIterations and
// kafka/mock.defaultScramIterations: Kafka's own broker default (and the
// lower bound of the valid range). buildUsers uses this to decide whether to
// emit spec.iterations at all: emitting the default on every manifest would
// be pure noise (every credential that never customized iterations reports
// this value), so it is only emitted when the live value differs. Either
// choice keeps verify clean: an unset spec.iterations is not drift-compared
// (accepts any live value), and an explicitly-set one that matches live also
// compares clean — so this is a fidelity/noise tradeoff, not a correctness one.
const defaultScramIterations = 4096

// scramMechanismRank orders SCRAM mechanisms by import preference:
// SCRAM-SHA-512 is captured over SCRAM-SHA-256 when a user has both live
// (stronger mechanism, and matches the KafkaUserSpec default in
// internal/defaulting). Higher rank wins.
var scramMechanismRank = map[string]int{
	"SCRAM-SHA-512": 2,
	"SCRAM-SHA-256": 1,
}

// buildUsers converts each live SCRAM credential in the snapshot into a
// *v1alpha1.KafkaUser manifest.
//
// One manifest per USER (not per credential): a principal can carry both
// SCRAM-SHA-256 and SCRAM-SHA-512 live simultaneously (e.g. mid-rotation), but
// a KafkaUserSpec has exactly one spec.mechanism, so only one can be
// represented. The preferred mechanism (SCRAM-SHA-512) is captured, and a
// warning names the other so operators know to manage it manually or add a
// second manifest under a different metadata.name.
//
// spec.iterations is emitted only when it differs from defaultScramIterations
// (see that const's doc) — round-trip-clean either way, so this is purely a
// noise-reduction choice.
//
// spec.password is always a placeholder env-var reference
// (KAFKA_USER_<SANITIZED_USERNAME>_PASSWORD): Kafka's DescribeUserSCRAMCredentials
// API never returns the password (or salted password), so there is nothing to
// recover it from. A loud warning is emitted once per user (and a single
// aggregate reminder) so operators set the env var (or switch to
// secretKeyRef/generate) before applying — otherwise apply would fail to
// resolve a missing credential. verify/diff stay clean regardless: the
// password is not part of the observable identity (user.Credential carries
// only username/mechanism/iterations).
//
// The connecting principal (opt.ConnectingUser, resolved by the CLI from
// auth.scram.username) is skipped by default: importing your own credential
// risks self-lockout if it is later deleted/rotated incorrectly. Pass
// opt.IncludeConnectingUser to capture it anyway.
//
// Results are sorted by metadata.name for determinism (mirrors buildQuotas).
func buildUsers(snap Snapshot, clusterName string, opt BuildOptions, warnings *[]string) []*v1alpha1.KafkaUser {
	if len(snap.ScramCredentials) == 0 {
		return nil
	}

	// Group live credentials by username, preserving the snapshot's sorted
	// (User, Mechanism) order so mechanism selection within a user is
	// deterministic regardless of map iteration.
	type userCreds struct {
		user  string
		creds []kafka.ScramCredential
	}
	byUser := map[string]*userCreds{}
	var order []string
	for _, c := range snap.ScramCredentials {
		uc, ok := byUser[c.User]
		if !ok {
			uc = &userCreds{user: c.User}
			byUser[c.User] = uc
			order = append(order, c.User)
		}
		uc.creds = append(uc.creds, c)
	}

	var anyEmitted bool
	users := make([]*v1alpha1.KafkaUser, 0, len(order))
	nameOwner := map[string]bool{}
	envOwner := map[string]bool{}
	for _, uname := range order {
		uc := byUser[uname]

		if uname == opt.ConnectingUser && opt.ConnectingUser != "" && !opt.IncludeConnectingUser {
			*warnings = append(*warnings, fmt.Sprintf(
				"skipped connecting principal %q (managing your own credential risks self-lockout); use --include-connecting-user to include it",
				uname))
			continue
		}

		// Pick the preferred mechanism when more than one is live for this user.
		best := uc.creds[0]
		for _, c := range uc.creds[1:] {
			if scramMechanismRank[c.Mechanism] > scramMechanismRank[best.Mechanism] {
				best = c
			}
		}
		if len(uc.creds) > 1 {
			for _, c := range uc.creds {
				if c.Mechanism != best.Mechanism {
					*warnings = append(*warnings, fmt.Sprintf(
						"user %q also has a %s credential; only the %s one is captured — manage the other manually or add a second manifest with a different metadata.name",
						uname, c.Mechanism, best.Mechanism))
				}
			}
		}

		base := topicMetaName(uname) // dots are meaningful in usernames too (e.g. service.checkout)
		metaName := disambiguateName(base, nameOwner)
		if metaName != base {
			*warnings = append(*warnings, fmt.Sprintf(
				"user name collision: user %q slugs to a metadata.name already used by another user; renamed to %q for uniqueness", uname, metaName))
		}

		envBase := userEnvVarName(uname)
		envName := disambiguateEnvVar(envBase, envOwner)
		if envName != envBase {
			*warnings = append(*warnings, fmt.Sprintf(
				"user %q: password env var %q collides with another user's; renamed to %q for uniqueness", uname, envBase, envName))
		}

		u := &v1alpha1.KafkaUser{
			TypeMeta: metav1.TypeMeta{APIVersion: v1alpha1.APIVersion, Kind: "KafkaUser"},
			ObjectMeta: metav1.ObjectMeta{
				Name:        metaName,
				Annotations: map[string]string{importedFromAnnotation: clusterName},
			},
			Spec: v1alpha1.KafkaUserSpec{
				ClusterRef: v1alpha1.ClusterRef{Name: clusterName},
				// Username is ALWAYS set explicitly, mirroring topicMetaName's
				// TopicName handling: defaulting.User falls back to
				// ObjectMeta.Name when Username is empty, which would resolve the
				// wrong live principal whenever slugging changed the name.
				Username:  uname,
				Mechanism: best.Mechanism,
				Password: &v1alpha1.UserPassword{
					ValueFrom: &v1alpha1.ValueSource{Env: envName},
				},
				DeletionPolicy: "Orphan",
			},
		}
		if best.Iterations != 0 && best.Iterations != defaultScramIterations {
			it := best.Iterations
			u.Spec.Iterations = &it
		}
		users = append(users, u)
		anyEmitted = true
	}

	if anyEmitted {
		*warnings = append(*warnings,
			"imported KafkaUser passwords are placeholders (env var references) — Kafka never exposes SCRAM passwords, so these are NOT recoverable from the cluster; set the referenced env vars (or switch to secretKeyRef/generate) before applying, or apply will fail to resolve a missing credential")
	}

	sort.Slice(users, func(i, j int) bool {
		return users[i].Name < users[j].Name
	})
	return users
}

// userEnvVarName derives the placeholder password env var name for a
// username: KAFKA_USER_<SANITIZED>_PASSWORD, where SANITIZED uppercases the
// username and replaces every non-alphanumeric run with a single "_" (env var
// names cannot contain '.', '-', ':', etc., all of which are valid in Kafka
// principals).
func userEnvVarName(username string) string {
	var b strings.Builder
	prevUnderscore := false
	for _, r := range strings.ToUpper(username) {
		isAlnum := (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')
		if isAlnum {
			b.WriteRune(r)
			prevUnderscore = false
		} else if !prevUnderscore {
			b.WriteByte('_')
			prevUnderscore = true
		}
	}
	sanitized := strings.Trim(b.String(), "_")
	if sanitized == "" {
		sanitized = "USER"
	}
	return "KAFKA_USER_" + sanitized + "_PASSWORD"
}

// disambiguateEnvVar ensures an env var name is unique within the set of
// already-assigned names, appending _2, _3, ... on collision (mirrors
// disambiguateName; a distinct suffix style since underscore, not dash, is the
// idiomatic env-var word separator).
func disambiguateEnvVar(base string, taken map[string]bool) string {
	if !taken[base] {
		taken[base] = true
		return base
	}
	for n := 2; ; n++ {
		cand := fmt.Sprintf("%s_%d", base, n)
		if !taken[cand] {
			taken[cand] = true
			return cand
		}
	}
}

// roundTripsClean reports whether compiling r reproduces snap's ACL set exactly.
func roundTripsClean(r Result, snap Snapshot) bool {
	var all []access.ACL
	for _, tp := range r.Topics {
		all = append(all, access.CompileTopic(tp)...)
	}
	for _, pol := range r.Policies {
		all = append(all, access.CompilePolicy(pol)...)
	}
	desired, errs := access.BuildDesiredSet(all)
	if len(errs) > 0 {
		return false
	}
	got := map[string]bool{}
	for _, a := range desired {
		got[a.FullKey()] = true
	}
	want := map[string]bool{}
	for _, s := range snap.ACLs {
		want[stateToACL(s).FullKey()] = true
	}
	if len(got) != len(want) {
		return false
	}
	for k := range want {
		if !got[k] {
			return false
		}
	}
	return true
}

// roleBindingsRoundTripClean reports whether the generated manifests' RBAC
// desired set reproduces snap.RoleBindings exactly. Recompiles topic access
// (CompileTopicAccess) + explicit bindings (Compile) through BuildDesiredSet,
// re-injecting scope IDs from mdsCfg, and compares by FullKey.
func roleBindingsRoundTripClean(r Result, snap Snapshot, mdsCfg *v1alpha1.MDSConfig) bool {
	if mdsCfg == nil {
		return false
	}
	var all []rbac.RoleBinding
	for _, tp := range r.Topics {
		compiled, _, err := rbac.CompileTopicAccess(tp, mdsCfg)
		if err != nil {
			return false
		}
		all = append(all, compiled...)
	}
	for _, m := range r.RoleBindings {
		compiled, err := rbac.Compile(m, mdsCfg)
		if err != nil {
			return false
		}
		all = append(all, compiled...)
	}
	desired, errs := rbac.BuildDesiredSet(all)
	if len(errs) > 0 {
		return false
	}
	got := map[string]bool{}
	for _, b := range desired {
		got[b.FullKey()] = true
	}
	want := map[string]bool{}
	for _, lr := range snap.RoleBindings {
		want[liveRBFullKey(lr)] = true
	}
	if len(got) != len(want) {
		return false
	}
	for k := range want {
		if !got[k] {
			return false
		}
	}
	return true
}

// liveRBFullKey returns the identity key of a live mds.RoleBinding for comparison
// against compiled rbac.RoleBinding.FullKey()s. mds.RoleBinding.Key() and
// rbac.RoleBinding.FullKey() are defined to use the identical field layout, so
// Key() is the correct bridge; if that invariant ever changes, this must too.
func liveRBFullKey(rb mds.RoleBinding) string {
	return rb.Key()
}

// backendActive reports whether backend is in accessBackends, treating an empty
// list as ["acl"] (back-compat, mirrors v1alpha1.EffectiveAccessBackends).
func backendActive(backends []string, want string) bool {
	if len(backends) == 0 {
		return want == "acl"
	}
	for _, b := range backends {
		if b == want {
			return true
		}
	}
	return false
}

// disambiguateName ensures a metadata.name is unique within the set of already-
// assigned names, appending -2,-3,... on collision (mirrors buildPolicies). It
// records the chosen name in taken.
func disambiguateName(base string, taken map[string]bool) string {
	if !taken[base] {
		taken[base] = true
		return base
	}
	for n := 2; ; n++ {
		cand := fmt.Sprintf("%s-%d", base, n)
		if !taken[cand] {
			taken[cand] = true
			return cand
		}
	}
}

// fallbackAllExplicit discards all folded access and re-emits every live grant as
// its backend-specific resource: ACLs → KafkaAccessPolicy (when acl active),
// role bindings → KafkaRoleBinding (when rbac active). Guarantees fidelity.
func fallbackAllExplicit(snap Snapshot, topics []*v1alpha1.KafkaTopic, clusterName string, aclActive, rbacActive bool) Result {
	for _, tp := range topics {
		tp.Spec.Access = v1alpha1.TopicAccess{}
	}
	var warnings []string
	r := Result{Topics: topics}
	if aclActive {
		all := make([]access.ACL, 0, len(snap.ACLs))
		for _, s := range snap.ACLs {
			all = append(all, stateToACL(s))
		}
		r.Policies = buildPolicies(all, clusterName, &warnings)
	}
	if rbacActive {
		nameOwner := map[string]bool{}
		rbs := make([]*v1alpha1.KafkaRoleBinding, 0, len(snap.RoleBindings))
		for _, lr := range snap.RoleBindings {
			m := roleBindingManifest(lr, clusterName)
			base := m.Name
			m.Name = disambiguateName(base, nameOwner)
			if m.Name != base {
				warnings = append(warnings, fmt.Sprintf("role binding name collision: %q renamed to %q for uniqueness", base, m.Name))
			}
			rbs = append(rbs, m)
		}
		sort.Slice(rbs, func(i, j int) bool { return rbs[i].Name < rbs[j].Name })
		r.RoleBindings = rbs
	}
	r.Warnings = warnings
	return r
}
