// Package reconcile is the mode-aware reconcile core of the operator. It is the
// controller-runtime-free engine that the KafkaTopic and KafkaAccessPolicy
// controllers call: it builds desired state from a manifest, observes live
// state via the kafka/schema-registry seams, and REUSES the same diff and
// executor packages as the CLI (it never reimplements diff or apply). The
// result is a fully-populated Status the controller writes back.
//
// # Status vs. retryable error
//
// ReconcileTopic and ReconcilePolicy return (Status, error). The Status is
// ALWAYS returned (even alongside a non-nil error) so the controller can write
// it. The error is the requeue signal: the controller should requeue-with-
// backoff if and only if error != nil.
//
//	A non-nil error means a TRANSIENT / retryable failure that is worth
//	retrying without any human or spec change:
//	  - a live-state read failure (GetTopic / ListACLs / GetSubject /
//	    GetCompatibility errored), and
//	  - an Enforce apply that produced Failed ops (executor Failed status —
//	    infrastructure errors).
//
//	A nil error means a TERMINAL outcome that retrying alone cannot resolve;
//	these still set an Error/Drifted phase plus conditions, but signal the
//	controller to wait for a human or a spec change rather than requeue:
//	  - a validation / ACL conflict (ValidationFailed),
//	  - Blocked ops needing a risk-gate approval annotation,
//	  - Rejected ops (e.g. a partition decrease), and
//	  - a schema-resolve failure (a configuration issue).
//
// sr may be nil (no Schema Registry configured for the cluster).
//
// Reconciliation honors spec §16 modes:
//
//	Enforce     - compute the diff and apply it via the executor.
//	DetectOnly  - compute the diff, report drift, but never mutate the cluster.
//	ObserveOnly - compute the diff, report drift as informational (never an
//	              Error phase), and never mutate the cluster.
//
// The §17.1 risk gates are honored through annotations on the managed object
// rather than CLI flags:
//
//	gitops.monedula.dev/allow-delete       -> executor Approvals.Delete
//	gitops.monedula.dev/allow-destructive  -> executor Approvals.Destructive
//
// ACL pruning (spec §10.3) is NOT annotation-gated: consent is the declarative
// spec.prune field, stamped onto the resource's desired ACLs so the managed
// scope (and from it each DeleteAcl's PruneAllowed) carries it. Approvals.Prune
// is never set in operator mode; an unconsented prune candidate is recorded as
// PruneDisabled — reported as drift, never deleted, never a failure.
//
// Schema bodies are resolved through the supplied secrets.Resolver. In operator
// mode the resolver is Kubernetes-Secret backed and rejects `file:` references;
// a resolve failure does NOT fail the whole reconcile — the schema is skipped
// (SchemaSynced=False, reason SchemaUnresolved) while topic + access reconcile
// continue. File-based schema refs are therefore unsupported in operator mode.
package reconcile

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/monedula-dev/monedula-gitops/internal/access"
	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/defaulting"
	"github.com/monedula-dev/monedula-gitops/internal/diff"
	"github.com/monedula-dev/monedula-gitops/internal/executor"
	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	"github.com/monedula-dev/monedula-gitops/internal/mds"
	"github.com/monedula-dev/monedula-gitops/internal/operations"
	"github.com/monedula-dev/monedula-gitops/internal/operator"
	"github.com/monedula-dev/monedula-gitops/internal/schemaregistry"
	"github.com/monedula-dev/monedula-gitops/internal/schemaregistry/recordname"
	"github.com/monedula-dev/monedula-gitops/internal/secrets"
	"github.com/monedula-dev/monedula-gitops/internal/tenancy"
	"github.com/monedula-dev/monedula-gitops/internal/validation"
)

// Reconciliation mode constants (spec §16).
const (
	ModeEnforce     = "Enforce"
	ModeDetectOnly  = "DetectOnly"
	ModeObserveOnly = "ObserveOnly"
)

// Risk-gate annotations (spec §17.1). The controller-side equivalent of the
// CLI's --allow-delete / --allow-destructive flags.
const (
	AnnotationAllowDelete      = "gitops.monedula.dev/allow-delete"
	AnnotationAllowDestructive = "gitops.monedula.dev/allow-destructive"
)

// Condition Reason strings used across this package's status conditions. These
// are the internal status reasons (distinct from the api condition-TYPE consts
// such as v1alpha1.CondReady). Centralizing them as consts avoids silent typos.
const (
	reasonInSync                = "InSync"
	reasonReconciled            = "Reconciled"
	reasonObserved              = "Observed"
	reasonDrifted               = "Drifted"
	reasonDriftDetected         = "DriftDetected"
	reasonDriftPending          = "DriftPending"
	reasonApplyIncomplete       = "ApplyIncomplete"
	reasonSchemaUnresolved      = "SchemaUnresolved"
	reasonSchemaSuperseded      = "SchemaSuperseded"
	reasonLiveStateError        = "LiveStateError"
	reasonValidationFailed      = "ValidationFailed"
	reasonACLConflict           = "ACLConflict"
	reasonCrossResourceConflict = "CrossResourceConflict"
	reasonNoConflict            = "NoConflict"
	// reasonTenancyDenied is set on ValidationFailed=True conditions when the
	// tenancy policy (spec §20.2) rejects a namespace or topic-name prefix.
	// The operator enforces tenancy; the CLI does not (it runs as cluster admin).
	reasonTenancyDenied = "TenancyDenied"
	reasonValid         = "Valid"
	// reasonRBACCoarsened is set on CondRBACCoarsened when a topic's access
	// block contains host or operation refinements that RBAC roles cannot express
	// exactly (spec §40). The binding is still applied; the coarsening is
	// informational.
	reasonRBACCoarsened = "RBACCoarsened"
	// reasonSchemaRegistryFetchFailed is set on CondSchemaRegistryDegraded=True
	// when the Schema Registry's GLOBAL compatibility fetch failed this
	// reconcile (message carries the underlying error).
	reasonSchemaRegistryFetchFailed = "GlobalCompatibilityFetchFailed"
	// reasonSchemaRegistryOK is set on CondSchemaRegistryDegraded=False when the
	// fetch succeeded (schemas are in play and the global level was read).
	reasonSchemaRegistryOK = "GlobalCompatibilityRead"
)

// Kind label values for monedula_reconcile_terminal_total (v0.36 Task 7).
// These MUST match the "controller" label values internal/operator/controller/
// status.go's recordReconcile uses (controllerKafkaTopic etc.) so the two
// metrics families stay joinable on the same dimension, even though this
// package cannot import the controller package (see the package doc: engine
// stays controller-runtime-free / avoids the import cycle controller ->
// reconcile already has the other way).
const (
	kindKafkaTopic        = "kafkatopic"
	kindKafkaAccessPolicy = "kafkaaccesspolicy"
	kindKafkaQuota        = "kafkaquota"
	kindKafkaRoleBinding  = "kafkarolebinding"
	kindKafkaUser         = "kafkauser"
)

// Subject names follow spec.schema.subjectStrategy (spec §11) and are computed
// by recordname.Subjects — the single computation site for the RECONCILE path.
// buildDesiredSchemas returns the computed value subject so the
// status-observation path (observedSchema) reports the SAME subject a non-
// TopicName strategy produced, rather than re-deriving <topic>-value.
// ManagedSubjects' TopicName/Custom arms take a body-independent shortcut that
// duplicates the suffix convention (see ManagedSubjects doc).

// ClusterACLView is the cluster-wide desired ACL union + managed scope,
// aggregated across EVERY resource referencing the cluster (spec §20.1). The
// controllers build it (from the cached resource lists) and pass it to
// ReconcileTopic / ReconcilePolicy, where it governs PRUNE computation only:
// without it, two resources sharing a (principal, resource pattern) pair with
// different operations each see the other's live ACLs as in-scope-but-
// undesired and — with prune enabled — delete them in an infinite flap
// (spec §10.4). Creates stay per-CR.
//
// Scope carries the AND-merged prune consent of all contributors (spec §10.3),
// so a candidate prunes only if every covering resource opted in; the
// aggregation is what makes that veto actually see all owners.
//
// When nil, the core falls back to the per-resource desired set + scope (the
// CLI single-resource semantics).
type ClusterACLView struct {
	DesiredACLs []access.ACL // union, deduped (most-enforcing mode, AND-merged prune)
	Scope       access.ManagedScope
	// Conflicts holds cross-resource Allow/Deny disagreements found in the union.
	// Informational: the conflicting tuple is still dropped from DesiredACLs
	// (BuildDesiredSet drops it) — these are surfaced so reconcilers can report them.
	Conflicts []access.Conflict
}

// BuildClusterACLView aggregates the desired ACL set + scope across the given
// resources — all KafkaTopics and KafkaAccessPolicies referencing one cluster.
// Each resource is defaulted (in place: pass copies if the originals must stay
// pristine), compiled, and stamped with ITS OWN spec.prune consent before the
// union is deduped (BuildDesiredSet) and the scope derived (BuildScope), so
// the AND-merge sees every owner's consent. clusterDefaults may be nil.
//
// Cross-resource Allow/Deny conflicts (which no single-resource validation can
// see) are resolved first-seen-wins, exactly as BuildDesiredSet documents: the
// conflicting tuple is dropped from the union. That is deliberate — the view
// must stay buildable so reconciles keep running; the conflict surfaces as
// drift on the losing resource.
func BuildClusterACLView(topics []*v1alpha1.KafkaTopic, policies []*v1alpha1.KafkaAccessPolicy,
	clusterDefaults *v1alpha1.ClusterDefaults, cluster *v1alpha1.KafkaCluster) *ClusterACLView {

	var all []access.ACL
	for _, tp := range topics {
		defaulting.Topic(tp, clusterDefaults)
		// Gate: on an rbac-only cluster, topics emit no ACLs (spec §40). A nil
		// cluster is treated as acl-backed (HasAccessBackend is nil-safe → true),
		// preserving legacy behaviour for callers that do not resolve the cluster
		// (test helpers and any caller where cluster lookup is elided).
		if !v1alpha1.HasAccessBackend(cluster, "acl") {
			continue
		}
		acls := access.CompileTopic(tp)
		access.StampSource(acls, "KafkaTopic", tp.Namespace, tp.Name)
		access.StampPrune(acls, tp.Spec.Prune)
		all = append(all, acls...)
	}
	for _, pol := range policies {
		defaulting.Policy(pol)
		acls := access.CompilePolicy(pol)
		access.StampSource(acls, "KafkaAccessPolicy", pol.Namespace, pol.Name)
		access.StampPrune(acls, pol.Spec.Prune)
		all = append(all, acls...)
	}

	// Sort all by a stable composite key so that BuildDesiredSet and
	// access.Conflicts both produce deterministic output regardless of the
	// iteration order of the topics/policies slices (spec §9).
	// FullKey() is the canonical 7-field identity (single source of truth);
	// the three source-attribution fields break ties across resources that
	// share the same identity tuple.
	sort.Slice(all, func(i, j int) bool {
		ki := all[i].FullKey() + "|" + all[i].SourceKind + "|" + all[i].SourceNamespace + "|" + all[i].SourceName
		kj := all[j].FullKey() + "|" + all[j].SourceKind + "|" + all[j].SourceNamespace + "|" + all[j].SourceName
		return ki < kj
	})

	union, _ := access.BuildDesiredSet(all) // conflicts: first-seen wins, see doc
	conflicts := access.Conflicts(all)      // cross-resource Allow/Deny disagreements
	return &ClusterACLView{DesiredACLs: union, Scope: access.BuildScope(union), Conflicts: conflicts}
}

// ReconcileTopic builds the desired state for a KafkaTopic, observes live state,
// reuses diff.Compute + executor.Apply per the topic's reconciliation mode, and
// returns the status the controller should write plus a retryable-error signal.
//
// The returned status is ALWAYS populated. The returned error is non-nil ONLY
// for TRANSIENT failures the controller should requeue-with-backoff on: a
// live-state read failure, or an Enforce apply with Failed ops. Terminal
// outcomes (ValidationFailed, Blocked, Rejected, schema-resolve failure) set the
// Error/Drifted phase + conditions but return a nil error. See the package doc.
//
// view, when non-nil, is the cluster-wide aggregated desired ACL set + scope
// (spec §20.1); it governs prune computation only — see ClusterACLView. A nil
// view keeps the per-resource (CLI single-resource) prune semantics.
func ReconcileTopic(ctx context.Context, topic *v1alpha1.KafkaTopic, cluster *v1alpha1.KafkaCluster,
	k kafka.AdminClient, sr schemaregistry.Client, r secrets.Resolver, view *ClusterACLView,
	mdsClient mds.Client, rbView *ClusterRoleBindingView) (v1alpha1.KafkaTopicStatus, error) {

	now := metav1.Now()
	st := v1alpha1.KafkaTopicStatus{ObservedGeneration: topic.Generation, LastCheckedTime: &now}
	// Seed conditions from the existing status so meta.SetStatusCondition can
	// preserve LastTransitionTime when a condition's Type+Status are unchanged
	// (otherwise every periodic requeue would re-stamp the transition time).
	if topic.Status != nil {
		st.Conditions = append([]metav1.Condition(nil), topic.Status.Conditions...)
	}

	var clusterDefaults *v1alpha1.ClusterDefaults
	if cluster != nil {
		clusterDefaults = cluster.Spec.Defaults
	}
	defaulting.Topic(topic, clusterDefaults)
	topicName := topic.Spec.TopicName

	// Validate the (defaulted) spec BEFORE touching any live state. Objects
	// fetched through the typed client have an empty TypeMeta (stripped by the
	// API machinery), so fill the known apiVersion first. A validation failure
	// is terminal: Phase Error, ValidationFailed=True, no mutation, nil error.
	if topic.APIVersion == "" {
		topic.APIVersion = v1alpha1.APIVersion
	}
	if verrs := validation.Validate(validation.Input{
		Topics:   []*v1alpha1.KafkaTopic{topic},
		Clusters: validationClusters(topic.Spec.ClusterRef.Name, cluster),
	}); len(verrs) > 0 {
		msg := joinErrMsgs(verrs)
		st.Phase = v1alpha1.PhaseError
		setTerminalValidationFailed(kindKafkaTopic, &st.Conditions, reasonValidationFailed, msg, topic.Generation)
		return st, nil // terminal: needs a spec change
	}

	// Tenancy enforcement (spec §20.2): the operator checks the cluster owner's
	// namespace allow-list and topic-prefix rules AFTER validation + defaulting
	// (so topicName is already resolved) but BEFORE any live-state read or
	// mutation. A denial is terminal: Phase Error, ValidationFailed=True with
	// reason TenancyDenied, no mutation, nil error.
	var clusterTenancy *v1alpha1.TenancyConfig
	if cluster != nil {
		clusterTenancy = cluster.Spec.Tenancy
	}
	if err := tenancy.Check(clusterTenancy, topic.Namespace, topicName); err != nil {
		msg := err.Error()
		st.Phase = v1alpha1.PhaseError
		setTerminalValidationFailed(kindKafkaTopic, &st.Conditions, reasonTenancyDenied, msg, topic.Generation)
		return st, nil // terminal: needs a tenancy config or namespace change
	}
	// Consumer GROUP names in the access block are tenancy-checked like topics
	// (groups reuse topic prefixes, spec §20.2): both the acl backend (group
	// Read ACLs via access.CompileTopic) and the rbac backend (DeveloperRead on
	// the group via rbac.CompileTopicAccess) derive live grants from these
	// names, so an unchecked group name would let a prefix-restricted namespace
	// grant access to another team's consumer group. Checking once here covers
	// both backends; reconcileTopicRoleBindings does not re-check.
	for _, c := range topic.Spec.Access.Consumers {
		if err := tenancy.CheckResource(clusterTenancy, topic.Namespace, "group", c.Group, "literal"); err != nil {
			msg := err.Error()
			st.Phase = v1alpha1.PhaseError
			setTerminalValidationFailed(kindKafkaTopic, &st.Conditions, reasonTenancyDenied, msg, topic.Generation)
			return st, nil // terminal: needs a tenancy config or namespace change
		}
	}

	// Desired ACLs (topic-local access). A conflict is a validation failure.
	// ACLs are only emitted when the cluster uses the "acl" backend (spec §40).
	// With an empty topicACLs slice, BuildDesiredSet produces an empty set, which
	// makes the ACL diff a no-op — topics on rbac-only clusters do not manage ACLs.
	var topicACLs []access.ACL
	if v1alpha1.HasAccessBackend(cluster, "acl") {
		topicACLs = access.CompileTopic(topic)
	}
	desiredACLs, errs := access.BuildDesiredSet(topicACLs)
	if len(errs) > 0 {
		msg := joinErrMsgs(errs)
		st.Phase = v1alpha1.PhaseError
		setTerminalValidationFailedReasons(kindKafkaTopic, &st.Conditions, reasonACLConflict, reasonValidationFailed, msg, topic.Generation)
		return st, nil // terminal: needs a spec change
	}
	access.StampPrune(desiredACLs, topic.Spec.Prune)
	scope := access.BuildScope(desiredACLs)

	// Both validation gates passed: clear a stale ValidationFailed left by a
	// prior pass (review I11) — conditions are seeded from the existing status,
	// so a fixed spec would otherwise report ValidationFailed=True forever.
	setCond(&st.Conditions, v1alpha1.CondValidationFailed, metav1.ConditionFalse, reasonValid, "spec validated", topic.Generation)

	// Desired schema (only when declared AND a registry is wired). A resolve
	// failure is non-fatal: skip the schema, continue topic + access reconcile.
	var desiredSchemas []diff.DesiredSchema
	schemaValueSubject := ""
	schemaResolveErr := ""
	schemaDeclared := topic.Spec.Schema != nil && sr != nil
	if schemaDeclared {
		ds, vs, err := buildDesiredSchemas(topic, topicName, r)
		if err != nil {
			schemaResolveErr = err.Error()
		} else {
			desiredSchemas = ds
			schemaValueSubject = vs
		}
	} else {
		// No schema declared (or no registry configured): REMOVE a stale
		// SchemaSynced seeded from the prior status (review I11). The condition
		// is only meaningful while a schema is managed; leaving the old value
		// would misreport a topic whose schema block was deleted.
		meta.RemoveStatusCondition(&st.Conditions, v1alpha1.CondSchemaSynced)
	}

	// Live state.
	rf := 0
	if topic.Spec.ReplicationFactor != nil {
		rf = *topic.Spec.ReplicationFactor
	}
	desiredTopic := diff.DesiredTopic{
		Kind: "KafkaTopic", Namespace: topic.Namespace, Name: topicName,
		Partitions: topic.Spec.Partitions, ReplicationFactor: rf, Config: topic.Spec.Config,
	}
	if topic.Spec.Drift != nil {
		desiredTopic.IgnoreFields = topic.Spec.Drift.IgnoreFields // spec §16
	}

	live, err := observeTopicLive(ctx, k, sr, topicName, desiredSchemas)
	if err != nil {
		st.Phase = v1alpha1.PhaseError
		setCond(&st.Conditions, v1alpha1.CondReady, metav1.ConditionFalse, reasonLiveStateError, err.Error(), topic.Generation)
		return st, err // transient: requeue with backoff
	}
	// Captured from THIS (pre-apply) observation only: diff.Compute below
	// classifies using this observation's live.GlobalCompatibility, so the
	// SchemaRegistryDegraded condition must reflect whether THIS fetch failed —
	// not the best-effort post-apply re-observe (Enforce mode reassigns live
	// further down), which would misattribute which attempt actually degraded
	// classification.
	globalCompatibilityErr := live.GlobalCompatibilityErr

	d := diff.Desired{
		Topics:  []diff.DesiredTopic{desiredTopic},
		ACLs:    desiredACLs,
		Scope:   scope,
		Schemas: desiredSchemas,
	}
	if view != nil {
		// Spec §20.1: prune candidates are computed against the cluster-wide
		// union so other resources' ACLs on a shared (principal, pattern) pair
		// are not pruned (§10.4); creates stay per-CR (ACLs/Scope above).
		d.SetPruneAggregate(view.DesiredACLs, view.Scope)
	}
	ops := diff.Compute(d, live)

	mode := topic.Spec.Reconciliation.Mode

	// SchemaSynced condition reflecting the resolve outcome up-front; the apply
	// path may keep it True (success) or the resolve-failure path forces False.
	if schemaResolveErr != "" {
		setCond(&st.Conditions, v1alpha1.CondSchemaSynced, metav1.ConditionFalse, reasonSchemaUnresolved,
			"schema not resolved (file-based refs are unsupported in operator mode): "+schemaResolveErr, topic.Generation)
	}

	var retryErr error
	switch mode {
	case ModeDetectOnly:
		applyDetectOnly(&st, ops, topic.Generation, false)
		topicConditionsFromOps(&st, ops, schemaDeclared, schemaResolveErr, topic.Generation, false)
	case ModeObserveOnly:
		applyDetectOnly(&st, ops, topic.Generation, true)
		topicConditionsFromOps(&st, ops, schemaDeclared, schemaResolveErr, topic.Generation, true)
	case ModeEnforce:
		ap := approvalsFromAnnotations(topic.Annotations)
		res := executor.Apply(ctx, executor.Clients{Kafka: k, Schema: sr}, ops, ap)
		st.LastAppliedTime = &now
		applyEnforceResult(&st, res, schemaDeclared, schemaResolveErr, topic.Generation)
		retryErr = applyRetryError(res)
		// Re-observe so the status reflects post-apply live state (e.g. a topic
		// or subject created this pass). Best-effort: a re-read failure leaves the
		// pre-apply observation in place rather than overriding the apply outcome.
		if relive, rerr := observeTopicLive(ctx, k, sr, topicName, desiredSchemas); rerr == nil {
			live = relive
		}
	default:
		// Unreachable: the up-front validation rejects unknown modes. Kept as
		// defense in depth so an unknown mode can NEVER fall through into the
		// mutating Enforce path.
		setInvalidMode(kindKafkaTopic, topicTarget(&st), mode, topic.Generation)
		return st, nil
	}

	// SchemaSuperseded (spec §12.1) overrides the generic SchemaSynced reason
	// set above: the divergence is terminal (the Enforce executor records the
	// op as Unsupported, which applyRetryError deliberately does NOT retry), so
	// the condition carries the specific reason + remediation message. A human
	// must update the manifest or roll the registry forward.
	if msg := schemaSupersededMessage(ops); msg != "" {
		setCond(&st.Conditions, v1alpha1.CondSchemaSynced, metav1.ConditionFalse, reasonSchemaSuperseded, msg, topic.Generation)
	}

	// Observed status from (post-apply, for Enforce) live state.
	if lt := liveTopicByName(live.Topics, topicName); lt != nil {
		st.ObservedTopic = &v1alpha1.ObservedTopic{
			TopicName: lt.Name, Partitions: lt.Partitions, ReplicationFactor: lt.ReplicationFactor, Config: lt.Config,
		}
	}
	st.Schema = observedSchema(live.Schemas, schemaValueSubject)

	// SchemaRegistryDegraded condition: informational surface for a failed
	// GLOBAL compatibility fetch (see observeTopicLive) — never fails the
	// reconcile; classification already fell back to legacy above. Gated the
	// same way as CondSchemaSynced in applyEnforceResult: the fetch only ever
	// runs when a schema is declared AND resolved (schemaResolveErr == "") —
	// desiredSchemas stays empty on a resolve failure, so observeTopicLive
	// never attempts the fetch and an empty error here would otherwise be
	// misread as "read successfully" instead of "never attempted".
	setSchemaRegistryDegradedCondition(&st.Conditions, schemaDeclared && schemaResolveErr == "", globalCompatibilityErr, topic.Generation)

	// Cross-resource ACLConflict condition (spec §21): non-terminal, informational.
	// Set on BOTH the True (conflict party) and False (no conflict) paths so the
	// condition is always present on a healthy resource. Placed after all mode
	// logic so it runs for every non-terminal outcome (Ready, Drifted, etc.).
	setACLConflictCondition(&st.Conditions, view, "KafkaTopic", topic.Namespace, topic.Name, topic.Generation)

	// Topic-access → RBAC role bindings (spec §40), only on rbac-backed clusters.
	if v1alpha1.HasAccessBackend(cluster, "rbac") {
		if cluster.Spec.Authorization == nil || cluster.Spec.Authorization.MDS == nil {
			msg := "accessBackends includes \"rbac\" but cluster has no authorization.mds configured"
			setTerminalValidationFailed(kindKafkaTopic, &st.Conditions, reasonValidationFailed, msg, topic.Generation)
			st.Phase = v1alpha1.PhaseError
			return st, nil // terminal: needs cluster config
		}
		if mdsClient == nil {
			msg := "rbac backend active but no MDS client available"
			setCond(&st.Conditions, v1alpha1.CondMDSReachable, metav1.ConditionFalse, reasonLiveStateError, msg, topic.Generation)
			setCond(&st.Conditions, v1alpha1.CondReady, metav1.ConditionFalse, reasonLiveStateError, msg, topic.Generation)
			return st, errors.New(msg) // transient
		}
		if rberr := reconcileTopicRoleBindings(ctx, &st, topic, cluster, mdsClient, rbView, now); rberr != nil {
			setCond(&st.Conditions, v1alpha1.CondReady, metav1.ConditionFalse, reasonApplyIncomplete,
				"role binding reconcile failed: "+rberr.Error(), topic.Generation)
			if retryErr == nil {
				retryErr = rberr
			}
		}
	} else {
		// No rbac backend: clear any RBAC conditions left by a prior pass (the
		// cluster may have dropped the rbac backend). Mirrors the SchemaSynced
		// removal when no schema is managed.
		meta.RemoveStatusCondition(&st.Conditions, v1alpha1.CondRBACCoarsened)
		meta.RemoveStatusCondition(&st.Conditions, v1alpha1.CondRoleBindingSynced)
		meta.RemoveStatusCondition(&st.Conditions, v1alpha1.CondMDSReachable)
	}

	return st, retryErr
}

// ReconcilePolicy is the ACL-only analogue of ReconcileTopic for a
// KafkaAccessPolicy. The returned status is always populated; the returned
// error is non-nil only for TRANSIENT failures (a live-state read failure or an
// Enforce apply with Failed ops). Terminal outcomes (ValidationFailed, Blocked,
// Rejected) return a nil error. See the package doc.
//
// view, when non-nil, is the cluster-wide aggregated desired ACL set + scope
// (spec §20.1); it governs prune computation only — see ClusterACLView. A nil
// view keeps the per-resource (CLI single-resource) prune semantics.
func ReconcilePolicy(ctx context.Context, pol *v1alpha1.KafkaAccessPolicy, cluster *v1alpha1.KafkaCluster,
	k kafka.AdminClient, view *ClusterACLView) (v1alpha1.KafkaAccessPolicyStatus, error) {

	_ = cluster // reserved (cluster defaults do not apply to policies in v0.5)
	now := metav1.Now()
	st := v1alpha1.KafkaAccessPolicyStatus{ObservedGeneration: pol.Generation, LastCheckedTime: &now}
	// Seed conditions from the existing status so meta.SetStatusCondition can
	// preserve LastTransitionTime when a condition's Type+Status are unchanged.
	if pol.Status != nil {
		st.Conditions = append([]metav1.Condition(nil), pol.Status.Conditions...)
	}

	defaulting.Policy(pol)

	// Validate the (defaulted) spec BEFORE touching any live state; see
	// ReconcileTopic. A failure is terminal: Phase Error, no mutation, nil error.
	if pol.APIVersion == "" {
		pol.APIVersion = v1alpha1.APIVersion
	}
	if verrs := validation.Validate(validation.Input{
		Policies: []*v1alpha1.KafkaAccessPolicy{pol},
		Clusters: validationClusters(pol.Spec.ClusterRef.Name, cluster),
	}); len(verrs) > 0 {
		msg := joinErrMsgs(verrs)
		st.Phase = v1alpha1.PhaseError
		setTerminalValidationFailed(kindKafkaAccessPolicy, &st.Conditions, reasonValidationFailed, msg, pol.Generation)
		return st, nil // terminal: needs a spec change
	}

	// Tenancy enforcement (spec §20.2): per-rule check — namespace allow-list +
	// prefix rules for topic AND group resources (groups reuse topic prefixes);
	// prefix-restricted namespaces are denied rules on unscopeable resource
	// types (cluster, transactionalId, delegationToken). First denial is
	// terminal.
	var clusterTenancy *v1alpha1.TenancyConfig
	if cluster != nil {
		clusterTenancy = cluster.Spec.Tenancy
	}
	for _, rule := range pol.Spec.Rules {
		if err := tenancy.CheckResource(clusterTenancy, pol.Namespace,
			rule.Resource.Type, rule.Resource.Name, rule.Resource.PatternType); err != nil {
			msg := err.Error()
			st.Phase = v1alpha1.PhaseError
			setTerminalValidationFailed(kindKafkaAccessPolicy, &st.Conditions, reasonTenancyDenied, msg, pol.Generation)
			return st, nil // terminal: needs a tenancy config or namespace change
		}
	}

	desiredACLs, errs := access.BuildDesiredSet(access.CompilePolicy(pol))
	if len(errs) > 0 {
		msg := joinErrMsgs(errs)
		st.Phase = v1alpha1.PhaseError
		setTerminalValidationFailedReasons(kindKafkaAccessPolicy, &st.Conditions, reasonACLConflict, reasonValidationFailed, msg, pol.Generation)
		return st, nil // terminal: needs a spec change
	}
	access.StampPrune(desiredACLs, pol.Spec.Prune)
	scope := access.BuildScope(desiredACLs)

	// Both validation gates passed: clear a stale ValidationFailed left by a
	// prior pass (review I11); see ReconcileTopic.
	setCond(&st.Conditions, v1alpha1.CondValidationFailed, metav1.ConditionFalse, reasonValid, "spec validated", pol.Generation)

	liveACLStates, err := k.ListACLs(ctx)
	if err != nil {
		st.Phase = v1alpha1.PhaseError
		setCond(&st.Conditions, v1alpha1.CondReady, metav1.ConditionFalse, reasonLiveStateError, err.Error(), pol.Generation)
		return st, err // transient: requeue with backoff
	}

	st.ObservedRules = pol.Spec.Rules

	d := diff.Desired{ACLs: desiredACLs, Scope: scope}
	if view != nil {
		// Spec §20.1: aggregated prune inputs; creates stay per-CR. See
		// ReconcileTopic.
		d.SetPruneAggregate(view.DesiredACLs, view.Scope)
	}
	ops := diff.Compute(d, diff.Live{ACLs: toAccessACLs(liveACLStates)})

	var retryErr error
	switch pol.Spec.Reconciliation.Mode {
	case ModeDetectOnly:
		applyPolicyDetectOnly(&st, ops, pol.Generation, false)
	case ModeObserveOnly:
		applyPolicyDetectOnly(&st, ops, pol.Generation, true)
	case ModeEnforce:
		ap := approvalsFromAnnotations(pol.Annotations)
		res := executor.Apply(ctx, executor.Clients{Kafka: k}, ops, ap)
		st.LastAppliedTime = &now
		applyPolicyEnforceResult(&st, res, pol.Generation)
		retryErr = applyRetryError(res)
	default:
		// Unreachable: the up-front validation rejects unknown modes. Kept as
		// defense in depth so an unknown mode can NEVER fall through into the
		// mutating Enforce path.
		setInvalidMode(kindKafkaAccessPolicy, policyTarget(&st), pol.Spec.Reconciliation.Mode, pol.Generation)
		return st, nil
	}

	// Cross-resource ACLConflict condition (spec §21): non-terminal, informational.
	// Set on BOTH the True (conflict party) and False (no conflict) paths so the
	// condition is always present on a healthy resource. Placed after all mode
	// logic so it runs for every non-terminal outcome (Ready, Drifted, etc.).
	setACLConflictCondition(&st.Conditions, view, "KafkaAccessPolicy", pol.Namespace, pol.Name, pol.Generation)

	return st, retryErr
}

// applyRetryError returns a non-nil retryable error iff the apply produced any
// Failed ops. Failed is the executor's status for an infrastructure error that
// is worth retrying. Blocked / Rejected / Skipped / Unsupported are terminal
// and do not requeue (they need a human approval or a spec change — retrying
// an Unsupported op in particular can never succeed), so they yield nil.
func applyRetryError(res executor.Result) error {
	if n := res.Counts()[executor.Failed]; n > 0 {
		return fmt.Errorf("apply incomplete: %d failed operation(s): %s", n, applyFailureMsg(res))
	}
	return nil
}

// ---- helpers ----

// validationClusters builds the single-entry cluster map handed to
// validation.Validate so the cluster-dependent checks (cluster-ref resolution,
// schema-requires-schemaRegistry) run against the resource's own cluster. A nil
// cluster returns nil, which tells validation to SKIP those checks (degrade
// gracefully) rather than flag the ref as unresolved. API-server-fetched
// clusters have an empty TypeMeta, so the known apiVersion is filled on a copy.
func validationClusters(refName string, cluster *v1alpha1.KafkaCluster) map[string]*v1alpha1.KafkaCluster {
	if cluster == nil {
		return nil
	}
	cl := *cluster
	if cl.APIVersion == "" {
		cl.APIVersion = v1alpha1.APIVersion
	}
	if cl.Name == "" {
		cl.Name = refName
	}
	return map[string]*v1alpha1.KafkaCluster{refName: &cl}
}

// joinErrMsgs renders a deterministic single-line summary of validation errors.
func joinErrMsgs(errs []error) string {
	msgs := make([]string, 0, len(errs))
	for _, e := range errs {
		msgs = append(msgs, e.Error())
	}
	return strings.Join(msgs, "; ")
}

// setInvalidMode marks an unknown reconciliation mode as a terminal validation
// failure. It backs the defensive default arm of the mode switches.
// setInvalidMode is unreachable in production (every caller's up-front
// validation already rejects an unknown reconciliation.mode before reaching
// its Enforce/DetectOnly/ObserveOnly switch) but is kept as defense in depth.
// kind still records the terminal-outcome metric on the (untested-in-practice)
// off chance validation and this defense drift apart.
func setInvalidMode(kind string, t driftTarget, mode string, gen int64) {
	msg := fmt.Sprintf("invalid reconciliation.mode %q", mode)
	t.setPhase(v1alpha1.PhaseError)
	setTerminalValidationFailed(kind, t.conds, reasonValidationFailed, msg, gen)
}

func approvalsFromAnnotations(ann map[string]string) executor.Approvals {
	// Prune is deliberately absent (spec §10.3): operator-side prune consent is
	// the declarative spec.prune (carried per-op via PruneAllowed through the
	// managed scope — see access.StampPrune), not a run-wide annotation approval.
	return executor.Approvals{
		Delete:      ann[AnnotationAllowDelete] == "true",
		Destructive: ann[AnnotationAllowDestructive] == "true",
	}
}

// buildDesiredSchemas resolves the value (and optional key) schema bodies and
// builds DesiredSchema entries, naming subjects per spec.schema.subjectStrategy
// (spec §11) via recordname.Subjects. It also returns the computed VALUE subject
// so the caller can report it in status without re-deriving a TopicName name. A
// resolve failure on either subject (or a record-name extraction failure for the
// RecordName/TopicRecordName strategies) returns an error; the caller skips
// schema reconcile.
func buildDesiredSchemas(topic *v1alpha1.KafkaTopic, topicName string, r secrets.Resolver) ([]diff.DesiredSchema, string, error) {
	sc := topic.Spec.Schema

	// Resolve bodies FIRST: the RecordName/TopicRecordName strategies extract the
	// record name from the body, so subject computation depends on it. Bodies are
	// empty in governance mode (no valueSchema/keySchema) — legal for TopicName
	// and Custom.
	var valueDef, keyDef string
	if sc.ValueSchema != nil {
		body, err := r.Resolve(*sc.ValueSchema)
		if err != nil {
			return nil, "", err
		}
		valueDef = body
	}
	if sc.KeySchema != nil {
		body, err := r.Resolve(*sc.KeySchema)
		if err != nil {
			return nil, "", err
		}
		keyDef = body
	}

	valueSubject, keySubject, err := recordname.Subjects(sc.SubjectStrategy, topicName, sc, valueDef, keyDef)
	if err != nil {
		return nil, "", err
	}

	// Governance mode (spec §12.2): spec.schema with NO valueSchema and NO
	// keySchema body manages only the subject compatibility level. The
	// producer's pipeline registers versions out-of-band (NOT drift). Emit
	// exactly one DesiredSchema for the VALUE subject with an empty Definition
	// (the diff never emits RegisterSchema for it). The value subject is
	// <topic>-value (TopicName) or the explicit ValueSubject (Custom);
	// RecordName/TopicRecordName are illegal in governance mode (no body) and
	// rejected by validation.
	// Mirror: CLI pipeline schema-assembly path (internal/pipeline/pipeline.go).
	if valueDef == "" && keyDef == "" {
		return []diff.DesiredSchema{{
			Subject: valueSubject, Topic: topicName,
			Type: sc.Format, Definition: "", Compatibility: sc.Compatibility,
			Mode: topic.Spec.Reconciliation.Mode,
		}}, valueSubject, nil
	}

	var out []diff.DesiredSchema
	if valueDef != "" {
		out = append(out, diff.DesiredSchema{
			Subject: valueSubject, Topic: topicName,
			Type: sc.Format, Definition: valueDef, Compatibility: sc.Compatibility,
		})
	}
	if keyDef != "" {
		out = append(out, diff.DesiredSchema{
			Subject: keySubject, Topic: topicName,
			Type: sc.Format, Definition: keyDef, Compatibility: sc.Compatibility,
		})
	}
	return out, valueSubject, nil
}

// ManagedSubjects computes the list of Schema Registry subjects that the topic
// OWNS — subjects that monedula registered and manages content for. It is used
// at finalization time (spec §12 topic-deletion-driven subject deletion) to know
// which subjects to soft-delete along with the topic.
//
// Only CONTENT-mode topics are included (spec.schema with valueSchema or
// keySchema present). Governance-mode topics (spec.schema without either body)
// only manage a subject's compatibility level — the schema content belongs to
// the producer, not monedula (spec §12.2). The producer's subjects are NOT
// deleted when the topic is removed.
//
// The topic MUST already be defaulted (spec.topicName populated) before calling
// ManagedSubjects. r is the secrets.Resolver used to read schema bodies for
// RecordName/TopicRecordName strategies; for TopicName/Custom the subjects are
// deterministic and never require body resolution, so the resolver is never
// called and deletion succeeds even when Secrets have been removed before
// finalization. For RecordName/TopicRecordName, body resolution may fail and an
// error is returned.
//
// Returns nil, nil when the topic declares no schema block.
func ManagedSubjects(topic *v1alpha1.KafkaTopic, r secrets.Resolver) ([]string, error) {
	sc := topic.Spec.Schema
	if sc == nil {
		return nil, nil // no schema block: nothing to delete
	}

	topicName := topic.ResolvedTopicName() // fallback to metadata.name (should already be defaulted)

	// Governance mode: no valueSchema and no keySchema body — the operator only
	// manages the subject compatibility level, not the schema content. The content
	// was registered by the producer's pipeline out-of-band. Do NOT delete the
	// subject: that would remove versions that belong to the producer (spec §12.2).
	if sc.ValueSchema == nil && sc.KeySchema == nil {
		return nil, nil
	}

	// Dispatch on strategy FIRST.
	//
	// TopicName / "" (default) and Custom: subjects are DETERMINISTIC without any
	// schema body. We must NOT call the resolver here — the Secret may already be
	// deleted by the time a topic is finalized (the common cascading-delete
	// scenario), and these strategies do not need the body to name the subject.
	//
	// RecordName / TopicRecordName: the subject name is derived from the record
	// full-name embedded in the schema body, so body resolution is required.
	switch sc.SubjectStrategy {
	case "", "TopicName":
		// Subject = <topic>-value iff ValueSchema is declared,
		//           <topic>-key  iff KeySchema  is declared.
		// This mirrors exactly what buildDesiredSchemas would register (fix 2).
		// Suffix convention must stay in sync with recordname.Subjects.
		var subjects []string
		if sc.ValueSchema != nil {
			subjects = append(subjects, topicName+"-value")
		}
		if sc.KeySchema != nil {
			subjects = append(subjects, topicName+"-key")
		}
		return subjects, nil

	case "Custom":
		// Subject names are verbatim from spec.schema.valueSubject /
		// spec.schema.keySubject; no body is needed. Include only the subjects
		// that have a corresponding schema declaration (fix 2).
		var subjects []string
		if sc.ValueSchema != nil && sc.ValueSubject != "" {
			subjects = append(subjects, sc.ValueSubject)
		}
		if sc.KeySchema != nil && sc.KeySubject != "" {
			subjects = append(subjects, sc.KeySubject)
		}
		return subjects, nil

	default:
		// RecordName / TopicRecordName: bodies are required to extract the record
		// full name, so resolve them. Resolution may fail if the Secret/ConfigMap
		// was already deleted before finalization.
		var valueDef, keyDef string
		if sc.ValueSchema != nil {
			body, err := r.Resolve(*sc.ValueSchema)
			if err != nil {
				return nil, fmt.Errorf("resolve value schema body for subject computation: %w", err)
			}
			valueDef = body
		}
		if sc.KeySchema != nil {
			body, err := r.Resolve(*sc.KeySchema)
			if err != nil {
				return nil, fmt.Errorf("resolve key schema body for subject computation: %w", err)
			}
			keyDef = body
		}

		valueSubject, keySubject, err := recordname.Subjects(sc.SubjectStrategy, topicName, sc, valueDef, keyDef)
		if err != nil {
			return nil, err
		}

		var subjects []string
		if valueSubject != "" {
			subjects = append(subjects, valueSubject)
		}
		if keySubject != "" {
			subjects = append(subjects, keySubject)
		}
		return subjects, nil
	}
}

// observeTopicLive reads live topic, ACL, and (for each desired subject) schema
// state into a diff.Live. Subject queries are bounded to desired subjects.
func observeTopicLive(ctx context.Context, k kafka.AdminClient, sr schemaregistry.Client,
	topicName string, desiredSchemas []diff.DesiredSchema) (diff.Live, error) {

	var live diff.Live

	lt, err := k.GetTopic(ctx, topicName)
	if err != nil {
		return live, fmt.Errorf("get topic %q: %w", topicName, err)
	}
	if lt != nil {
		live.Topics = []kafka.TopicState{*lt}
	}

	acls, err := k.ListACLs(ctx)
	if err != nil {
		return live, fmt.Errorf("list ACLs: %w", err)
	}
	live.ACLs = toAccessACLs(acls)

	// Global compatibility level, fetched ONCE per reconcile when subjects are
	// managed: an unset subject's effective level is this global default, so
	// the diff uses it as the baseline when classifying a first-time
	// subject-level set (spec §17.1 — a level below the default is a gated
	// Lower). A failure (older SR without GET /config) deliberately does NOT
	// fail the reconcile: the level stays "" (unknown) and the diff falls back
	// to the legacy any-initial-set-is-a-Raise classification. The error is
	// recorded on live.GlobalCompatibilityErr so the caller can surface it as
	// the informational SchemaRegistryDegraded condition instead of silently
	// degrading (review: operator was silent where the CLI warns).
	// Mirror: CLI computeOps (internal/cli/diff.go).
	if len(desiredSchemas) > 0 {
		if level, gerr := sr.GetGlobalCompatibility(ctx); gerr == nil {
			live.GlobalCompatibility = level
		} else {
			live.GlobalCompatibilityErr = gerr.Error()
		}
	}

	for _, ds := range desiredSchemas {
		stt, err := sr.GetSubject(ctx, ds.Subject)
		if err != nil {
			return live, fmt.Errorf("get subject %q: %w", ds.Subject, err)
		}
		if stt == nil {
			// Subject has no registered versions. In GOVERNANCE mode
			// (spec §12.2, empty Definition) monedula still manages the subject
			// compatibility level, which Confluent permits to exist before any
			// version. Synthesize a live entry from GetCompatibility so the diff
			// and status see the current subject-level config (absent config
			// reads back as ""). In content mode an absent subject is left out
			// -> RegisterSchema drift, as before.
			// Mirror: CLI computeOps synthesis (internal/cli/diff.go).
			if ds.Definition == "" {
				level, err := sr.GetCompatibility(ctx, ds.Subject)
				if err != nil {
					return live, fmt.Errorf("get compatibility %q: %w", ds.Subject, err)
				}
				live.Schemas = append(live.Schemas, schemaregistry.SubjectState{
					Subject:       ds.Subject,
					Compatibility: level,
				})
			}
			continue
		}
		level, err := sr.GetCompatibility(ctx, ds.Subject)
		if err != nil {
			return live, fmt.Errorf("get compatibility %q: %w", ds.Subject, err)
		}
		stt.Compatibility = level
		live.Schemas = append(live.Schemas, *stt)

		// Governance mode (empty Definition) never registers versions, so skip
		// the supersession probe — a producer-registered version is not drift.
		if ds.Definition == "" {
			continue
		}

		// SchemaSuperseded probe (spec §12.1): supersession is detected where
		// live state is read (the diff engine has no registry client). When
		// the desired schema diverges from the latest version but is already
		// registered as an older one, the diff emits the terminal
		// SchemaSuperseded instead of a never-converging RegisterSchema.
		if !diff.SchemaEqual(ds.Type, ds.Definition, stt.Schema.Definition) {
			v, err := sr.LookupSchema(ctx, ds.Subject, schemaregistry.Schema{
				Type:       schemaregistry.SchemaType(ds.Type),
				Definition: ds.Definition,
			})
			if err != nil {
				return live, fmt.Errorf("lookup schema %q: %w", ds.Subject, err)
			}
			if v > 0 {
				if live.SupersededSchemas == nil {
					live.SupersededSchemas = map[string]int{}
				}
				live.SupersededSchemas[ds.Subject] = v
			}
		}
	}
	return live, nil
}

// observedSchema builds the ObservedSchema from the live value subject, if any.
// valueSubject is the subject computed by buildDesiredSchemas (spec §11 strategy-
// aware), threaded through so the status reports the actual subject — e.g. a
// RecordName topic's status carries the record full name, not <topic>-value. An
// empty valueSubject (no schema declared) matches nothing and yields nil.
func observedSchema(live []schemaregistry.SubjectState, valueSubject string) *v1alpha1.ObservedSchema {
	if valueSubject == "" {
		return nil
	}
	for _, s := range live {
		if s.Subject == valueSubject {
			return &v1alpha1.ObservedSchema{
				ValueSubject:  s.Subject,
				ValueSchemaID: s.ID,
				Compatibility: s.Compatibility,
			}
		}
	}
	return nil
}

func liveTopicByName(topics []kafka.TopicState, name string) *kafka.TopicState {
	for i := range topics {
		if topics[i].Name == name {
			return &topics[i]
		}
	}
	return nil
}

func toAccessACLs(states []kafka.ACLState) []access.ACL {
	out := make([]access.ACL, 0, len(states))
	for _, s := range states {
		out = append(out, access.ACL{
			Principal: s.Principal, Host: s.Host, ResourceType: s.ResourceType,
			ResourceName: s.ResourceName, PatternType: s.PatternType,
			Operation: s.Operation, Permission: s.Permission,
		})
	}
	return out
}

// driftFields renders a stable, human-readable summary of pending ops.
func driftFields(ops []operations.Operation) []string {
	out := make([]string, 0, len(ops))
	for _, op := range ops {
		if op.Action == operations.NoOp {
			continue
		}
		out = append(out, fmt.Sprintf("%s %s", op.Action, op.Target))
	}
	sort.Strings(out)
	return out
}

func pendingOps(ops []operations.Operation) []operations.Operation {
	out := make([]operations.Operation, 0, len(ops))
	for _, op := range ops {
		if op.Action != operations.NoOp {
			out = append(out, op)
		}
	}
	return out
}

// driftTarget bundles the parts of a Status the shared drift/Ready skeleton
// mutates: the conditions slice, the Drift field, and a phase setter. Both
// resource statuses adapt to it so the DriftDetected/Ready/phase decision lives
// in exactly one place (finishDetectOnly / finishEnforce); only the per-resource
// AREA conditions stay wired per type.
type driftTarget struct {
	conds    *[]metav1.Condition
	drift    **v1alpha1.DriftStatus
	setPhase func(string)
}

func topicTarget(st *v1alpha1.KafkaTopicStatus) driftTarget {
	return driftTarget{conds: &st.Conditions, drift: &st.Drift, setPhase: func(p string) { st.Phase = p }}
}

func policyTarget(st *v1alpha1.KafkaAccessPolicyStatus) driftTarget {
	return driftTarget{conds: &st.Conditions, drift: &st.Drift, setPhase: func(p string) { st.Phase = p }}
}

// finishDetectOnly sets the drift status + DriftDetected condition + Ready/phase
// for the non-applying modes (DetectOnly/ObserveOnly). When observe is true,
// drift is informational and the phase stays Ready. Shared by topic and policy.
func finishDetectOnly(t driftTarget, ops []operations.Operation, gen int64, observe bool) {
	pending := pendingOps(ops)
	drifted := len(pending) > 0
	fields := driftFields(pending)
	*t.drift = &v1alpha1.DriftStatus{Detected: drifted, Fields: fields}

	if drifted {
		setCond(t.conds, v1alpha1.CondDriftDetected, metav1.ConditionTrue, reasonDriftDetected, strings.Join(fields, "; "), gen)
	} else {
		setCond(t.conds, v1alpha1.CondDriftDetected, metav1.ConditionFalse, reasonInSync, "no drift detected", gen)
	}

	switch {
	case observe:
		t.setPhase(v1alpha1.PhaseReady)
		setCond(t.conds, v1alpha1.CondReady, metav1.ConditionTrue, reasonObserved, "observe-only: drift reported, not enforced", gen)
	case drifted:
		t.setPhase(v1alpha1.PhaseDrifted)
		setCond(t.conds, v1alpha1.CondReady, metav1.ConditionFalse, reasonDrifted, "detect-only: drift present, not enforced", gen)
	default:
		t.setPhase(v1alpha1.PhaseReady)
		setCond(t.conds, v1alpha1.CondReady, metav1.ConditionTrue, reasonInSync, "no drift detected", gen)
	}
}

// finishEnforce sets the drift status + DriftDetected condition + Ready/phase
// from an executor.Result for the applying mode. Shared by topic and policy;
// the per-area conditions are set by the caller before invoking this.
func finishEnforce(t driftTarget, res executor.Result, gen int64) {
	counts := res.Counts()
	// PruneDisabled counts as drift (the in-scope live ACL diverges from the
	// manifest) but not as failure: res.OK() below still yields Ready, so an
	// unconsented prune candidate reports Ready + DriftDetected (spec §10.3).
	unresolved := counts[executor.Blocked] + counts[executor.Failed] +
		counts[executor.Rejected] + counts[executor.Skipped] +
		counts[executor.PruneDisabled] + counts[executor.Unsupported]

	drifted := unresolved > 0
	*t.drift = &v1alpha1.DriftStatus{Detected: drifted, Fields: unresolvedFields(res)}
	if drifted {
		setCond(t.conds, v1alpha1.CondDriftDetected, metav1.ConditionTrue, reasonApplyIncomplete, strings.Join(unresolvedFields(res), "; "), gen)
	} else {
		setCond(t.conds, v1alpha1.CondDriftDetected, metav1.ConditionFalse, reasonInSync, "reconciled clean", gen)
	}

	if res.OK() {
		t.setPhase(v1alpha1.PhaseReady)
		setCond(t.conds, v1alpha1.CondReady, metav1.ConditionTrue, reasonReconciled, "all operations succeeded", gen)
		return
	}
	t.setPhase(v1alpha1.PhaseError)
	setCond(t.conds, v1alpha1.CondReady, metav1.ConditionFalse, reasonApplyIncomplete, applyFailureMsg(res), gen)
}

// applyDetectOnly sets the topic's per-area conditions then the shared
// drift/Ready skeleton for the non-applying modes.
func applyDetectOnly(st *v1alpha1.KafkaTopicStatus, ops []operations.Operation, gen int64, observe bool) {
	finishDetectOnly(topicTarget(st), ops, gen, observe)
}

// topicConditionsFromOps sets per-area conditions (TopicSynced/TopicAccessSynced
// /SchemaSynced) for the non-applying modes based on whether any pending op
// touches each area. In observe/detect mode "synced" means no pending op.
func topicConditionsFromOps(st *v1alpha1.KafkaTopicStatus, ops []operations.Operation,
	schemaDeclared bool, schemaResolveErr string, gen int64, observe bool) {

	var topicPending, aclPending, schemaPending bool
	for _, op := range pendingOps(ops) {
		switch {
		case isACLAction(op.Action):
			aclPending = true
		case isSchemaAction(op.Action):
			schemaPending = true
		default:
			topicPending = true
		}
	}

	okReason := reasonInSync
	pendingReason := reasonDriftPending
	if observe {
		pendingReason = reasonObserved
	}

	setSyncCond(st, v1alpha1.CondTopicSynced, !topicPending, okReason, pendingReason, gen)
	setSyncCond(st, v1alpha1.CondTopicAccessSynced, !aclPending, okReason, pendingReason, gen)

	if schemaResolveErr != "" {
		return // SchemaSynced already set to False/SchemaUnresolved by caller.
	}
	if schemaDeclared {
		setSyncCond(st, v1alpha1.CondSchemaSynced, !schemaPending, okReason, pendingReason, gen)
	}
}

func setSyncCond(st *v1alpha1.KafkaTopicStatus, typ string, synced bool, okReason, pendingReason string, gen int64) {
	if synced {
		setCond(&st.Conditions, typ, metav1.ConditionTrue, okReason, "in sync", gen)
	} else {
		setCond(&st.Conditions, typ, metav1.ConditionFalse, pendingReason, "out of sync", gen)
	}
}

// applyEnforceResult sets the topic's per-area conditions from an
// executor.Result, then delegates the drift/Ready/phase decision to the shared
// finishEnforce skeleton.
func applyEnforceResult(st *v1alpha1.KafkaTopicStatus, res executor.Result,
	schemaDeclared bool, schemaResolveErr string, gen int64) {

	// Per-area outcomes.
	topicOK, topicMsg := areaOutcome(res, func(a operations.Action) bool {
		return !isACLAction(a) && !isSchemaAction(a)
	})
	aclOK, aclMsg := areaOutcome(res, isACLAction)
	schemaOK, schemaMsg := areaOutcome(res, isSchemaAction)

	setSyncedCondFromOutcome(st, v1alpha1.CondTopicSynced, topicOK, topicMsg, gen)
	setSyncedCondFromOutcome(st, v1alpha1.CondTopicAccessSynced, aclOK, aclMsg, gen)

	if schemaResolveErr == "" && schemaDeclared {
		setSyncedCondFromOutcome(st, v1alpha1.CondSchemaSynced, schemaOK, schemaMsg, gen)
	}

	finishEnforce(topicTarget(st), res, gen)
}

func setSyncedCondFromOutcome(st *v1alpha1.KafkaTopicStatus, typ string, ok bool, msg string, gen int64) {
	if ok {
		setCond(&st.Conditions, typ, metav1.ConditionTrue, reasonReconciled, "in sync", gen)
	} else {
		setCond(&st.Conditions, typ, metav1.ConditionFalse, reasonApplyIncomplete, msg, gen)
	}
}

// areaOutcome reports whether every op matched by pred succeeded (vacuously true
// if none), plus a message summarizing the first non-success.
func areaOutcome(res executor.Result, pred func(operations.Action) bool) (bool, string) {
	ok := true
	msg := ""
	for _, r := range res.Results {
		if !pred(r.Op.Action) {
			continue
		}
		if r.Status != executor.Succeeded {
			ok = false
			if msg == "" {
				msg = fmt.Sprintf("%s %s: %s", r.Op.Action, r.Op.Target, statusDetail(r))
			}
		}
	}
	return ok, msg
}

func statusDetail(r executor.OpResult) string {
	if r.Err != "" {
		return string(r.Status) + " (" + r.Err + ")"
	}
	return string(r.Status)
}

func unresolvedFields(res executor.Result) []string {
	var out []string
	for _, r := range res.Results {
		if r.Status == executor.Succeeded {
			continue
		}
		out = append(out, fmt.Sprintf("%s %s [%s]", r.Op.Action, r.Op.Target, r.Status))
	}
	sort.Strings(out)
	return out
}

func applyFailureMsg(res executor.Result) string {
	var parts []string
	for _, r := range res.Results {
		if r.Status == executor.Succeeded {
			continue
		}
		parts = append(parts, fmt.Sprintf("%s %s: %s", r.Op.Action, r.Op.Target, statusDetail(r)))
	}
	sort.Strings(parts)
	return strings.Join(parts, "; ")
}

// ---- policy mode handlers ----

func applyPolicyDetectOnly(st *v1alpha1.KafkaAccessPolicyStatus, ops []operations.Operation, gen int64, observe bool) {
	// Per-area AccessPolicySynced condition; the shared skeleton owns drift/Ready.
	drifted := len(pendingOps(ops)) > 0
	if drifted {
		setCond(&st.Conditions, v1alpha1.CondAccessPolicySynced, metav1.ConditionFalse, reasonDriftPending, "out of sync", gen)
	} else {
		setCond(&st.Conditions, v1alpha1.CondAccessPolicySynced, metav1.ConditionTrue, reasonInSync, "in sync", gen)
	}

	finishDetectOnly(policyTarget(st), ops, gen, observe)
}

func applyPolicyEnforceResult(st *v1alpha1.KafkaAccessPolicyStatus, res executor.Result, gen int64) {
	// Per-area AccessPolicySynced condition; the shared skeleton owns drift/Ready.
	if res.OK() {
		setCond(&st.Conditions, v1alpha1.CondAccessPolicySynced, metav1.ConditionTrue, reasonReconciled, "all ACL operations succeeded", gen)
	} else {
		setCond(&st.Conditions, v1alpha1.CondAccessPolicySynced, metav1.ConditionFalse, reasonApplyIncomplete, applyFailureMsg(res), gen)
	}

	finishEnforce(policyTarget(st), res, gen)
}

// ---- shared small helpers ----

func isACLAction(a operations.Action) bool {
	return a == operations.CreateAcl || a == operations.DeleteAcl
}

func isSchemaAction(a operations.Action) bool {
	return a == operations.RegisterSchema ||
		a == operations.RaiseSchemaCompatibility ||
		a == operations.LowerSchemaCompatibility ||
		a == operations.DeleteSubject ||
		a == operations.SchemaSuperseded
}

// schemaSupersededMessage returns the first SchemaSuperseded op's message, or
// "" when no subject is superseded.
func schemaSupersededMessage(ops []operations.Operation) string {
	for _, op := range ops {
		if op.Action == operations.SchemaSuperseded {
			return op.Message
		}
	}
	return ""
}

// setCond sets a status condition with ObservedGeneration and LastTransitionTime
// managed by apimachinery's meta.SetStatusCondition.
func setCond(conds *[]metav1.Condition, typ string, status metav1.ConditionStatus, reason, msg string, gen int64) {
	meta.SetStatusCondition(conds, metav1.Condition{
		Type:               typ,
		Status:             status,
		Reason:             reason,
		Message:            msg,
		ObservedGeneration: gen,
	})
}

// setTerminalValidationFailed sets the ValidationFailed=True / Ready=False
// condition pair every TERMINAL outcome in this package uses (spec §16: nil
// error, needs a human/spec change — validation, tenancy, ACL-conflict-as-
// ValidationFailed, an unreachable rbac backend, ...) and bumps
// monedula_reconcile_terminal_total{kind, reason} (v0.36 Task 7). kind is the
// same controller label RecordReconcile uses (e.g. "kafkatopic"); reason is
// the condition Reason (reasonValidationFailed, reasonTenancyDenied, ...).
//
// This is the single choke point for every terminal path INSIDE the engine
// (ReconcileTopic/ReconcilePolicy/ReconcileQuota/ReconcileRoleBinding/
// ReconcileUser and their helpers). It intentionally does NOT cover the
// controller-package terminal paths that live outside the engine — the
// duplicate-identity gate (internal/operator/controller/duplicate.go) and the
// KafkaRoleBinding MDSNotConfigured branch — which call
// operator.RecordReconcileTerminal directly at their own call sites instead.
func setTerminalValidationFailed(kind string, conds *[]metav1.Condition, reason, msg string, gen int64) {
	setTerminalValidationFailedReasons(kind, conds, reason, reason, msg, gen)
}

// setTerminalValidationFailedReasons is setTerminalValidationFailed with
// independently-specified reasons for the ValidationFailed and Ready
// conditions — needed by the ACL-conflict terminal paths, which set
// ValidationFailed's reason to the more specific reasonACLConflict while Ready
// keeps the generic reasonValidationFailed (pre-existing behavior, preserved
// here rather than changed). The metric records validationFailedReason (the
// more specific of the two), matching what an operator reading the
// ValidationFailed condition would see.
func setTerminalValidationFailedReasons(kind string, conds *[]metav1.Condition, validationFailedReason, readyReason, msg string, gen int64) {
	setCond(conds, v1alpha1.CondValidationFailed, metav1.ConditionTrue, validationFailedReason, msg, gen)
	setCond(conds, v1alpha1.CondReady, metav1.ConditionFalse, readyReason, msg, gen)
	operator.RecordReconcileTerminal(kind, validationFailedReason)
}

// setACLConflictCondition sets CondACLConflict on conds from the cluster view:
// True (reason CrossResourceConflict) when this resource (kind/namespace/name)
// is a party to any conflict in the view, naming the other party + subject;
// else False (reason NoConflict). NON-terminal — informational only; the
// conflicting tuple is already dropped from the applied union (no flapping).
// When view is nil (single-resource fallback), the condition is cleared.
func setACLConflictCondition(conds *[]metav1.Condition, view *ClusterACLView, kind, namespace, name string, gen int64) {
	if view == nil {
		meta.RemoveStatusCondition(conds, v1alpha1.CondACLConflict)
		return
	}
	isParty := func(c access.ACL) bool {
		return c.SourceKind == kind && c.SourceNamespace == namespace && c.SourceName == name
	}
	for _, cf := range view.Conflicts {
		var other access.ACL
		switch {
		case isParty(cf.A):
			other = cf.B
		case isParty(cf.B):
			other = cf.A
		default:
			continue
		}
		msg := fmt.Sprintf("ACL %s conflicts Allow/Deny with %s/%s/%s on the same tuple; the tuple is dropped from the applied set",
			cf.Subject, other.SourceKind, other.SourceNamespace, other.SourceName)
		setCond(conds, v1alpha1.CondACLConflict, metav1.ConditionTrue, reasonCrossResourceConflict, msg, gen)
		return
	}
	setCond(conds, v1alpha1.CondACLConflict, metav1.ConditionFalse, reasonNoConflict, "no cross-resource ACL conflict", gen)
}

// setSchemaRegistryDegradedCondition sets CondSchemaRegistryDegraded on conds
// from this reconcile's PRE-APPLY live observation: True (reason
// GlobalCompatibilityFetchFailed, message carrying the underlying error) when
// the GLOBAL compatibility fetch failed, so a first-time subject-level
// compatibility set silently classified using the legacy any-initial-set-is-
// a-Raise rule instead of the true global default (spec §17.1) — the gap the
// CLI already warns about but the operator used to swallow. False (reason
// GlobalCompatibilityRead) when the fetch succeeded. NON-terminal —
// informational only; NEVER fails the reconcile.
//
// fetchAttempted must be schemaDeclared && schemaResolveErr == "" — mirroring
// applyEnforceResult's CondSchemaSynced guard exactly: observeTopicLive only
// calls GetGlobalCompatibility when len(desiredSchemas) > 0, and
// desiredSchemas stays empty when the schema failed to resolve (even though
// schemaDeclared is still true), so schemaDeclared alone would misreport an
// unattempted fetch as "read successfully". When fetchAttempted is false (no
// schema managed, no registry configured, or the schema failed to resolve),
// the fetch never ran and the condition is cleared rather than left stale
// from a prior pass — mirrors the CondSchemaSynced removal above.
//
// globalCompatibilityErr must come from the reconcile's PRE-APPLY
// observeTopicLive call specifically (the one whose live.GlobalCompatibility
// fed diff.Compute's classification) — NOT a later best-effort post-apply
// re-observe (Enforce mode), which can independently succeed or fail and
// would misattribute which attempt actually degraded classification.
func setSchemaRegistryDegradedCondition(conds *[]metav1.Condition, fetchAttempted bool, globalCompatibilityErr string, gen int64) {
	if !fetchAttempted {
		meta.RemoveStatusCondition(conds, v1alpha1.CondSchemaRegistryDegraded)
		return
	}
	if globalCompatibilityErr != "" {
		msg := "could not read Schema Registry global compatibility; first-time compatibility sets classify as Raise: " + globalCompatibilityErr
		setCond(conds, v1alpha1.CondSchemaRegistryDegraded, metav1.ConditionTrue, reasonSchemaRegistryFetchFailed, msg, gen)
		return
	}
	setCond(conds, v1alpha1.CondSchemaRegistryDegraded, metav1.ConditionFalse, reasonSchemaRegistryOK, "global compatibility level read successfully", gen)
}
