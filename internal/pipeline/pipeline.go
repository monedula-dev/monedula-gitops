// Package pipeline is the shared assembly layer between the domain packages and
// the CLI. Build performs the full load -> split -> default -> validate ->
// compile sequence that every command (validate/diff/verify/apply) relies on,
// so the commands stay thin and behave identically.
package pipeline

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/monedula-dev/monedula-gitops/internal/access"
	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/defaulting"
	"github.com/monedula-dev/monedula-gitops/internal/diff"
	"github.com/monedula-dev/monedula-gitops/internal/loader"
	"github.com/monedula-dev/monedula-gitops/internal/quota"
	"github.com/monedula-dev/monedula-gitops/internal/rbac"
	"github.com/monedula-dev/monedula-gitops/internal/schemaregistry/recordname"
	"github.com/monedula-dev/monedula-gitops/internal/secrets"
	"github.com/monedula-dev/monedula-gitops/internal/user"
	"github.com/monedula-dev/monedula-gitops/internal/validation"
)

// Options configures a Build. It mirrors the flags every CLI command exposes.
type Options struct {
	Filenames []string
	Recursive bool
	// Stdin supplies MANIFEST input when a filename is "-" (i.e. -f -). It is NOT
	// used for cluster-config files; the cluster-config load passes Stdin: nil to
	// avoid draining the reader.
	Stdin              io.Reader
	ClusterConfigFiles []string // --cluster-config-file (file or dir); may be empty
	Cluster            string   // --cluster selector/filter; may be empty
	RequireClusters    bool     // verify/diff/apply set true; validate sets false
}

// Plan is the fully assembled desired state derived from the manifests.
type Plan struct {
	Clusters        map[string]*v1alpha1.KafkaCluster
	SelectedCluster string                 // resolved single cluster name, or "" if none/ambiguous
	Topics          []*v1alpha1.KafkaTopic // selected + defaulted
	Policies        []*v1alpha1.KafkaAccessPolicy
	Quotas          []*v1alpha1.KafkaQuota // validated quota resources (all clusters)
	RoleBindings    []*v1alpha1.KafkaRoleBinding
	// Users holds validated+defaulted KafkaUser resources (all clusters).
	Users               []*v1alpha1.KafkaUser
	DesiredTopics       []diff.DesiredTopic
	DesiredSchemas      []diff.DesiredSchema
	DesiredACLs         []access.ACL
	DesiredQuotas       []quota.Desired    // flat list for the CLI single-cluster diff path
	DesiredRoleBindings []rbac.RoleBinding // flat list for the CLI single-cluster diff path
	// DesiredUsers is the compiled credential state (observable identity +
	// password ref — never a password value) for the CLI single-cluster diff
	// path, mirroring DesiredQuotas (v0.35 §T4).
	DesiredUsers     []user.Desired
	Scope            access.ManagedScope
	RoleBindingScope rbac.ManagedScope
	// RBACWarnings holds human-readable coarsening notices from topic-access →
	// RBAC auto-map (non-"*" host / custom operation lists on rbac-backed
	// clusters, spec §40). Informational; surfaced by the CLI as warnings.
	// Surfaced by the live-cluster commands (diff/verify/apply); the validate
	// command does not read them, matching the unknown-role warning's behavior.
	RBACWarnings []string
}

// Build runs the shared pipeline and returns a Plan or an aggregated error.
func Build(opts Options) (*Plan, error) {
	// 1. Load manifests and split by kind.
	objs, err := loader.Load(loader.Options{Filenames: opts.Filenames, Recursive: opts.Recursive, Stdin: opts.Stdin})
	if err != nil {
		return nil, err
	}

	var topics []*v1alpha1.KafkaTopic
	var policies []*v1alpha1.KafkaAccessPolicy
	var quotas []*v1alpha1.KafkaQuota
	var roleBindings []*v1alpha1.KafkaRoleBinding
	var users []*v1alpha1.KafkaUser
	clusters := map[string]*v1alpha1.KafkaCluster{}
	// topicSource maps each loaded topic to the file path (or "stdin") it came
	// from, so schema-file refs can be resolved relative to the manifest.
	topicSource := map[*v1alpha1.KafkaTopic]string{}
	for _, o := range objs {
		switch {
		case o.Topic != nil:
			topics = append(topics, o.Topic)
			topicSource[o.Topic] = o.Source
		case o.Policy != nil:
			policies = append(policies, o.Policy)
		case o.Cluster != nil:
			// A KafkaCluster in -f input is unusual but, if present, is treated
			// like cluster config too.
			clusters[o.Cluster.Name] = o.Cluster
		case o.Quota != nil:
			quotas = append(quotas, o.Quota)
		case o.RoleBinding != nil:
			roleBindings = append(roleBindings, o.RoleBinding)
		case o.User != nil:
			users = append(users, o.User)
		}
	}

	// 2. Load cluster configs.
	if len(opts.ClusterConfigFiles) > 0 {
		// Stdin is intentionally nil here: it carries manifest input (-f -) only,
		// and reusing it would drain the reader before the manifest load reads it.
		cfgObjs, err := loader.Load(loader.Options{Filenames: opts.ClusterConfigFiles, Recursive: opts.Recursive, Stdin: nil})
		if err != nil {
			return nil, err
		}
		for _, o := range cfgObjs {
			if o.Cluster != nil {
				clusters[o.Cluster.Name] = o.Cluster
			}
		}
	}
	clustersLoaded := len(clusters) > 0
	if opts.RequireClusters && !clustersLoaded {
		return nil, errors.New("no cluster configuration provided (use --cluster-config-file)")
	}

	// 3. Resolve the selected cluster.
	selected := ""
	if opts.Cluster != "" {
		selected = opts.Cluster
		if clustersLoaded {
			if _, ok := clusters[opts.Cluster]; !ok {
				return nil, fmt.Errorf("selected cluster %q not found in cluster config", opts.Cluster)
			}
		}
	} else if len(clusters) == 1 {
		for name := range clusters {
			selected = name
		}
	}

	// 4. Filter by --cluster (documented filter behavior).
	if opts.Cluster != "" {
		var ft []*v1alpha1.KafkaTopic
		for _, tp := range topics {
			if tp.Spec.ClusterRef.Name == opts.Cluster {
				ft = append(ft, tp)
			}
		}
		topics = ft
		var fp []*v1alpha1.KafkaAccessPolicy
		for _, pol := range policies {
			if pol.Spec.ClusterRef.Name == opts.Cluster {
				fp = append(fp, pol)
			}
		}
		policies = fp
		var fq []*v1alpha1.KafkaQuota
		for _, q := range quotas {
			if q.Spec.ClusterRef.Name == opts.Cluster {
				fq = append(fq, q)
			}
		}
		quotas = fq
		var frb []*v1alpha1.KafkaRoleBinding
		for _, rb := range roleBindings {
			if rb.Spec.ClusterRef.Name == opts.Cluster {
				frb = append(frb, rb)
			}
		}
		roleBindings = frb
		var fu []*v1alpha1.KafkaUser
		for _, u := range users {
			if u.Spec.ClusterRef.Name == opts.Cluster {
				fu = append(fu, u)
			}
		}
		users = fu
	}

	// 5. Apply defaults.
	for _, tp := range topics {
		var cd *v1alpha1.ClusterDefaults
		if cl, ok := clusters[tp.Spec.ClusterRef.Name]; ok {
			cd = cl.Spec.Defaults
		}
		defaulting.Topic(tp, cd)
	}
	for _, pol := range policies {
		defaulting.Policy(pol)
	}
	for _, cl := range clusters {
		defaulting.Cluster(cl)
	}
	for _, u := range users {
		defaulting.User(u)
	}

	// 6. Validate.
	// validationClusters stays nil (not an empty map) when no cluster config was
	// loaded. Passing nil deliberately signals validation to SKIP cluster-ref and
	// identity checks in validate-mode; an empty map would instead make every
	// clusterRef look unresolved.
	var validationClusters map[string]*v1alpha1.KafkaCluster
	if clustersLoaded {
		validationClusters = clusters
	}
	if verrs := validation.Validate(validation.Input{Topics: topics, Policies: policies, Quotas: quotas, Clusters: validationClusters, RoleBindings: roleBindings, Users: users}); len(verrs) > 0 {
		return nil, aggregate(verrs)
	}

	// 6b. CLI-vs-operator password-source check (v0.35 §T2). Core validation
	// (ValidateUserShape) is mode-agnostic and accepts spec.password.generate
	// as shape-valid, because the CRD/webhook/reconcile path must accept it —
	// the operator is the only thing that can actually create the generated
	// Secret. The CLI has no such capability, so this pipeline (the CLI's
	// sole assembly seam — see the package doc) is the earliest CLI-side point
	// that can reject it. Rejecting here rather than silently ignoring
	// generate is required: without this check the CLI would validate/diff/
	// apply a manifest that requests a password source it can never honor.
	var genErrs []error
	for _, u := range users {
		if u.Spec.Password != nil && u.Spec.Password.Generate != nil {
			genErrs = append(genErrs, fmt.Errorf("KafkaUser %s: spec.password.generate is operator-only; the CLI cannot generate or store credentials — use spec.password.valueFrom with env/file (or secretKeyRef when applied via the operator) instead", u.Name))
		}
	}
	if len(genErrs) > 0 {
		return nil, aggregate(genErrs)
	}

	// 7. Compile ACLs per cluster (deterministic: topics in load order, then
	// policies). ACL identity is scoped to a cluster: an identical tuple on two
	// different clusters must neither dedupe nor Allow/Deny-conflict, so the
	// dedup/conflict pass (BuildDesiredSet) runs per clusterRef group.
	aclsByCluster := map[string][]access.ACL{}
	var clusterOrder []string // first-seen order, for deterministic error output
	addACLs := func(clusterName string, acls []access.ACL) {
		if _, seen := aclsByCluster[clusterName]; !seen {
			clusterOrder = append(clusterOrder, clusterName)
		}
		aclsByCluster[clusterName] = append(aclsByCluster[clusterName], acls...)
	}
	// Attribution (spec §16, §17.5): every compiled ACL is stamped with its
	// source resource's reconciliation mode and identity, so the diff can emit
	// owner-attributed, mode-aware ACL ops. When BuildDesiredSet later dedupes
	// identical tuples desired by multiple resources, the most-enforcing mode
	// wins and the first contributor keeps owner attribution.
	//
	// Prune consent is deliberately NOT stamped here (ACL.Prune stays false):
	// in CLI mode `apply --prune` is THE prune switch (spec §10.3), supplied
	// run-wide via executor Approvals.Prune. Resource-level spec.prune is the
	// operator's consent mechanism and is not consulted by the CLI.
	for _, tp := range topics {
		cl := clusters[tp.Spec.ClusterRef.Name] // nil in validate-only mode
		if cl == nil || v1alpha1.HasAccessBackend(cl, "acl") {
			addACLs(tp.Spec.ClusterRef.Name, stampACLs(access.CompileTopic(tp),
				"KafkaTopic", tp.Namespace, tp.Name, tp.Spec.Reconciliation.Mode))
		}
	}
	for _, pol := range policies {
		addACLs(pol.Spec.ClusterRef.Name, stampACLs(access.CompilePolicy(pol),
			"KafkaAccessPolicy", pol.Namespace, pol.Name, pol.Spec.Reconciliation.Mode))
	}

	desiredByCluster := map[string][]access.ACL{}
	var aerrs []error
	for _, name := range clusterOrder {
		ds, errs := access.BuildDesiredSet(aclsByCluster[name])
		aerrs = append(aerrs, errs...)
		desiredByCluster[name] = ds
	}
	if len(aerrs) > 0 {
		return nil, aggregate(aerrs)
	}

	// The Plan's flat DesiredACLs/Scope feed the CLI's single-cluster live diff,
	// so they carry the SELECTED cluster's set. Without an explicit selection,
	// a single cluster group (the common case) is used; with multiple groups
	// and no selection they stay empty (diff/verify/apply require a selection
	// before reaching live state anyway).
	var desiredACLs []access.ACL
	switch {
	case selected != "":
		desiredACLs = desiredByCluster[selected]
	case len(clusterOrder) == 1:
		desiredACLs = desiredByCluster[clusterOrder[0]]
	}
	scope := access.BuildScope(desiredACLs)

	// 8. Assemble desired topic state.
	var desiredTopics []diff.DesiredTopic
	for _, tp := range topics {
		topicName := tp.ResolvedTopicName()
		rf := 0
		if tp.Spec.ReplicationFactor != nil {
			rf = *tp.Spec.ReplicationFactor
		}
		dt := diff.DesiredTopic{
			Kind:              "KafkaTopic",
			Namespace:         tp.Namespace,
			Name:              topicName,
			Partitions:        tp.Spec.Partitions,
			ReplicationFactor: rf,
			Config:            tp.Spec.Config,
			Mode:              tp.Spec.Reconciliation.Mode,
		}
		if tp.Spec.Drift != nil {
			dt.IgnoreFields = tp.Spec.Drift.IgnoreFields // spec §16
		}
		desiredTopics = append(desiredTopics, dt)
	}

	// 9. Assemble desired schema state. Subject names follow spec.schema.
	// subjectStrategy (spec §11) via recordname.Subjects — the same computation
	// the operator uses. Value subject before key subject per topic; topics
	// already in load order. Schema-file refs resolve relative to the directory of
	// the topic's source manifest.
	var desiredSchemas []diff.DesiredSchema
	var schemaErrs []error
	for _, tp := range topics {
		if tp.Spec.Schema == nil {
			continue
		}
		topicName := tp.ResolvedTopicName()
		name := tp.Name

		// Resolve schema-file refs relative to the manifest's directory. For
		// stdin input there is no directory, so paths resolve against cwd.
		baseDir := "."
		if src := topicSource[tp]; src != "" && src != "stdin" {
			baseDir = filepath.Dir(src)
		}

		// Resolve the value/key schema bodies FIRST (subject computation for the
		// RecordName/TopicRecordName strategies needs the body to extract the
		// record name). Bodies are empty in governance mode (no valueSchema/
		// keySchema) — that is legal for TopicName and Custom.
		sc := tp.Spec.Schema
		valueDef, vErr := resolveSchemaBody("value", name, baseDir, sc.ValueSchema, sc.Format)
		keyDef, kErr := resolveSchemaBody("key", name, baseDir, sc.KeySchema, sc.Format)
		if vErr != nil {
			schemaErrs = append(schemaErrs, vErr)
		}
		if kErr != nil {
			schemaErrs = append(schemaErrs, kErr)
		}
		if vErr != nil || kErr != nil {
			continue
		}

		// Single source of truth for subject names (spec §11), shared with the
		// operator reconciler.
		valueSubject, keySubject, err := recordname.Subjects(sc.SubjectStrategy, topicName, sc, valueDef, keyDef)
		if err != nil {
			schemaErrs = append(schemaErrs, fmt.Errorf("KafkaTopic %s: %v", name, err))
			continue
		}

		// Governance mode (spec §12.2): spec.schema with NO valueSchema and NO
		// keySchema body manages only the subject compatibility level — the
		// producer's pipeline registers versions out-of-band, which is NOT drift.
		// Emit exactly one DesiredSchema for the VALUE subject with an empty
		// Definition (the diff never emits RegisterSchema for it). Validation has
		// already required Compatibility in this mode. The value subject is
		// <topic>-value (TopicName) or the explicit ValueSubject (Custom);
		// RecordName/TopicRecordName are illegal in governance mode (no body to
		// extract from) and rejected by validation.
		// Mirror: operator buildDesiredSchemas (internal/operator/reconcile/reconcile.go).
		if valueDef == "" && keyDef == "" {
			desiredSchemas = append(desiredSchemas, diff.DesiredSchema{
				Subject:       valueSubject,
				Topic:         topicName,
				Type:          sc.Format,
				Definition:    "",
				Compatibility: sc.Compatibility,
				Mode:          tp.Spec.Reconciliation.Mode,
			})
			continue
		}

		// Content mode: valueSchema and/or keySchema bodies are registered. Value
		// subject is emitted before key per topic.
		if valueDef != "" {
			desiredSchemas = append(desiredSchemas, diff.DesiredSchema{
				Subject:       valueSubject,
				Topic:         topicName,
				Type:          sc.Format,
				Definition:    valueDef,
				Compatibility: sc.Compatibility,
				Mode:          tp.Spec.Reconciliation.Mode,
			})
		}
		if keyDef != "" {
			desiredSchemas = append(desiredSchemas, diff.DesiredSchema{
				Subject:       keySubject,
				Topic:         topicName,
				Type:          sc.Format,
				Definition:    keyDef,
				Compatibility: sc.Compatibility,
				Mode:          tp.Spec.Reconciliation.Mode,
			})
		}
	}
	if len(schemaErrs) > 0 {
		return nil, aggregate(schemaErrs)
	}

	// 10. Compile quota state. quota.Compile is cluster-agnostic (no per-cluster
	// dedup/conflict pass unlike ACLs), so no intermediate grouping map is needed.
	// Step 4 already narrowed `quotas` to the selected cluster when --cluster was
	// given, so in the selected != "" branch all remaining quotas belong to that
	// cluster and compiling them all is correct. The single-distinct-cluster branch
	// mirrors the ACL/schema flat-select pattern.
	// Mode "" (no spec.reconciliation set) executes as Enforce in the executor
	// (only DetectOnly/ObserveOnly are report-only); no explicit defaulting needed.
	clusterSet := map[string]struct{}{}
	for _, q := range quotas {
		clusterSet[q.Spec.ClusterRef.Name] = struct{}{}
	}
	var desiredQuotas []quota.Desired
	switch {
	case selected != "":
		for _, q := range quotas {
			desiredQuotas = append(desiredQuotas, quota.Compile(q))
		}
	case len(clusterSet) == 1:
		for _, q := range quotas {
			desiredQuotas = append(desiredQuotas, quota.Compile(q))
		}
	}

	// 11. Compile role-binding state. Mirrors quota assembly: cluster-aware
	// (role bindings are scoped to a cluster via clusterRef + MDS config),
	// single-cluster flat output for the CLI diff path.
	//
	// Per-cluster Compile requires the cluster's MDS config. A binding whose
	// cluster has no authorization.mds is a compile error (Compile errors on
	// missing KafkaCluster id); surface gracefully as an aggregated error
	// (mirrors the ACL/quota compile-error surfacing pattern). BuildDesiredSet
	// deduplicates and surfaces collision errors likewise.
	rbClusterSet := map[string]struct{}{}
	for _, rb := range roleBindings {
		rbClusterSet[rb.Spec.ClusterRef.Name] = struct{}{}
	}
	// A cluster may have topic-access-derived role bindings without any explicit
	// KafkaRoleBinding; include it so single-cluster flat selection still
	// populates its DesiredRoleBindings.
	for _, tp := range topics {
		if cl, ok := clusters[tp.Spec.ClusterRef.Name]; ok && v1alpha1.HasAccessBackend(cl, "rbac") {
			rbClusterSet[tp.Spec.ClusterRef.Name] = struct{}{}
		}
	}
	var allCompiledRBs []rbac.RoleBinding
	var rbacWarnings []string
	// Role-binding compilation requires MDS config from the cluster spec, so it is
	// only attempted when cluster config was loaded. When no cluster config is
	// present (validate-only mode, spec §40), validation has already checked shape;
	// skipping compile here mirrors how ACL/quota compilation produces an empty
	// desired set without clusters (those also ultimately derive from cluster data,
	// but ACL compilation is cluster-agnostic at the structure level). The CLI
	// commands that need live state (diff/verify/apply) always set RequireClusters,
	// so clusters will be present by the time live-state queries are issued.
	var rbErrs []error
	if clustersLoaded {
		for _, rb := range roleBindings {
			cl, ok := clusters[rb.Spec.ClusterRef.Name]
			if !ok || cl.Spec.Authorization == nil || cl.Spec.Authorization.MDS == nil {
				// No MDS config for this cluster. Compile would panic on nil MDSConfig;
				// surface a clean error instead. Validation (internal/validation) surfaces
				// this as an error; the pipeline also guards it at compile time.
				rbErrs = append(rbErrs, fmt.Errorf(
					"KafkaRoleBinding %s/%s references cluster %q which has no authorization.mds configured",
					rb.Namespace, rb.Name, rb.Spec.ClusterRef.Name,
				))
				continue
			}
			compiled, err := rbac.Compile(rb, cl.Spec.Authorization.MDS)
			if err != nil {
				rbErrs = append(rbErrs, fmt.Errorf("KafkaRoleBinding %s/%s: %w", rb.Namespace, rb.Name, err))
				continue
			}
			allCompiledRBs = append(allCompiledRBs, compiled...)
		}
		// Topic-access role bindings: for clusters with the "rbac" backend, compile
		// topic.spec.access entries into RBAC role bindings (spec §40). This runs
		// after the explicit-roleBinding loop so both sets accumulate in
		// allCompiledRBs before BuildDesiredSet deduplicates them.
		for _, tp := range topics {
			cl, ok := clusters[tp.Spec.ClusterRef.Name]
			if !ok || !v1alpha1.HasAccessBackend(cl, "rbac") {
				continue
			}
			if cl.Spec.Authorization == nil || cl.Spec.Authorization.MDS == nil {
				// validation rejects rbac-without-mds; guard defensively.
				rbErrs = append(rbErrs, fmt.Errorf(
					"KafkaTopic %s/%s references cluster %q with accessBackends rbac but no authorization.mds",
					tp.Namespace, tp.Name, tp.Spec.ClusterRef.Name))
				continue
			}
			compiled, warns, err := rbac.CompileTopicAccess(tp, cl.Spec.Authorization.MDS)
			if err != nil {
				rbErrs = append(rbErrs, fmt.Errorf("KafkaTopic %s/%s: %w", tp.Namespace, tp.Name, err))
				continue
			}
			allCompiledRBs = append(allCompiledRBs, compiled...)
			rbacWarnings = append(rbacWarnings, warns...)
		}
	}
	if len(rbErrs) > 0 {
		return nil, aggregate(rbErrs)
	}
	desiredRBs, collisionErrs := rbac.BuildDesiredSet(allCompiledRBs)
	if len(collisionErrs) > 0 {
		return nil, aggregate(collisionErrs)
	}

	// The Plan's flat DesiredRoleBindings/RoleBindingScope feed the CLI's
	// single-cluster live diff. Selection mirrors the ACL/quota pattern.
	var desiredRoleBindings []rbac.RoleBinding
	switch {
	case selected != "":
		// Step 4 narrowed BOTH roleBindings and topics to the selected cluster,
		// so all compiled bindings — explicit and topic-access-derived — belong
		// to that cluster.
		desiredRoleBindings = desiredRBs
	case len(rbClusterSet) == 1:
		desiredRoleBindings = desiredRBs
	}
	roleBindingScope := rbac.BuildScope(desiredRoleBindings)

	// 12. Compile user credential state (v0.35 §T4). Mirrors the quota
	// assembly exactly: user.CompileDesired is cluster-agnostic and per-user
	// dedup/collision is already handled by validation ((clusterRef, username)
	// identity uniqueness), so only the single-cluster flat selection applies.
	// The compiled Desired carries the password REFERENCE only — resolution
	// happens in the executor at execute time, never here. Generate-mode
	// passwords were rejected in step 6b, so every compiled user has a
	// valueFrom ref. KafkaUser has no spec.reconciliation: its ops carry no
	// mode and execute as Enforce.
	userClusterSet := map[string]struct{}{}
	for _, u := range users {
		userClusterSet[u.Spec.ClusterRef.Name] = struct{}{}
	}
	var desiredUsers []user.Desired
	switch {
	case selected != "":
		// Step 4 narrowed `users` to the selected cluster when --cluster was
		// given; otherwise selection implies a single loaded cluster config.
		for _, u := range users {
			desiredUsers = append(desiredUsers, user.CompileDesired(u))
		}
	case len(userClusterSet) == 1:
		for _, u := range users {
			desiredUsers = append(desiredUsers, user.CompileDesired(u))
		}
	}

	return &Plan{
		Clusters:            clusters,
		SelectedCluster:     selected,
		Topics:              topics,
		Policies:            policies,
		Quotas:              quotas,
		RoleBindings:        roleBindings,
		Users:               users,
		DesiredTopics:       desiredTopics,
		DesiredSchemas:      desiredSchemas,
		DesiredACLs:         desiredACLs,
		DesiredQuotas:       desiredQuotas,
		DesiredRoleBindings: desiredRoleBindings,
		DesiredUsers:        desiredUsers,
		Scope:               scope,
		RoleBindingScope:    roleBindingScope,
		RBACWarnings:        rbac.SortedWarnings(rbacWarnings),
	}, nil
}

// resolveSchemaBody resolves a single schema-body ref (value or key) to its
// verbatim text. A nil ref yields "" with no error (the schema is absent —
// governance mode or a value-only topic). role ("value"/"key") and the topic
// name are used only for diagnostics. AVRO/JSON bodies are checked for JSON
// validity here so subject computation never sees a malformed body.
func resolveSchemaBody(role, topicResName, baseDir string, vf *v1alpha1.ValueFrom, format string) (string, error) {
	if vf == nil {
		return "", nil
	}
	src := vf.ValueFrom

	var definition string
	switch {
	case src.Inline != "":
		// Inline schema body: use verbatim.
		definition = src.Inline

	case src.ConfigMapKeyRef != nil:
		// configMapKeyRef is operator-only. Surface a clean error rather than
		// panic so users get actionable output (use file or inline in CLI mode).
		return "", fmt.Errorf("KafkaTopic %s: configMapKeyRef is not supported in CLI mode", topicResName)

	default:
		// File-based reference (the original/default CLI path). Resolve relative
		// refs against the manifest dir (absolute paths are read as-is) and confine
		// the result. Schema layouts intentionally use a single "../schemas/" hop to
		// a sibling directory (the importer emits exactly this), so confinement is
		// against the manifest's PARENT directory: deeper "../.." traversal that
		// escapes that parent is rejected as path traversal.
		path, err := secrets.SafeJoinUnder(baseDir, filepath.Dir(baseDir), src.File)
		if err != nil {
			return "", fmt.Errorf("KafkaTopic %s: %v", topicResName, err)
		}
		content, err := os.ReadFile(path)
		if err != nil {
			return "", fmt.Errorf("KafkaTopic %s: reading schema file %q: %v", topicResName, path, err)
		}
		if len(content) == 0 {
			return "", fmt.Errorf("KafkaTopic %s: schema file %q is empty", topicResName, path)
		}
		definition = string(content)
	}

	if (format == "AVRO" || format == "JSON") && !json.Valid([]byte(definition)) {
		return "", fmt.Errorf("KafkaTopic %s: %s schema body is not valid JSON", topicResName, role)
	}
	return definition, nil
}

// aggregate joins multiple errors into one. errors.Join preserves the individual
// errors for errors.As/errors.Is inspection while still producing a newline-joined
// Error() string.
func aggregate(errs []error) error {
	return errors.Join(errs...)
}

// stampACLs sets the attribution fields (reconciliation mode + owning resource)
// on every compiled ACL. Mutates and returns acls for call-site brevity.
func stampACLs(acls []access.ACL, kind, namespace, name, mode string) []access.ACL {
	for i := range acls {
		acls[i].Mode = mode
		acls[i].SourceKind = kind
		acls[i].SourceNamespace = namespace
		acls[i].SourceName = name
	}
	return acls
}
