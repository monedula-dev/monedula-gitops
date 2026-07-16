package reconcile

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/diff"
	"github.com/monedula-dev/monedula-gitops/internal/executor"
	"github.com/monedula-dev/monedula-gitops/internal/mds"
	"github.com/monedula-dev/monedula-gitops/internal/operations"
	"github.com/monedula-dev/monedula-gitops/internal/rbac"
	"github.com/monedula-dev/monedula-gitops/internal/tenancy"
	"github.com/monedula-dev/monedula-gitops/internal/validation"
)

// ClusterRoleBindingView governs the prune scope for MDS role-binding
// reconciliation, aggregated across every KafkaRoleBinding referencing the same
// cluster (spec §40). It is the RBAC analogue of ClusterACLView: the
// DesiredScope carries the AND-merged prune consent of all contributing
// resources, ensuring a live binding is only pruned when EVERY owner opts in.
//
// When nil, ReconcileRoleBinding falls back to per-resource (single-CR) scope
// semantics — the same as the CLI.
type ClusterRoleBindingView struct {
	// DesiredBindings is the cluster-wide deduped desired role-binding set,
	// aggregated across every KafkaRoleBinding AND every topic-access-derived
	// binding referencing the cluster. It is the prune KEEP-SET: a live binding
	// in scope but absent here is a prune candidate. Mirrors ClusterACLView.DesiredACLs.
	DesiredBindings []rbac.RoleBinding
	// DesiredScope is the cluster-wide managed scope (prune candidacy +
	// AND-merged consent), built from DesiredBindings.
	DesiredScope rbac.ManagedScope
}

// ReconcileRoleBinding is the MDS role-binding reconcile core (spec §40). It
// mirrors ReconcileQuota for structure — validation-first, compile, live-read,
// diff+apply — but drives MDS instead of the Kafka admin client.
//
// The returned status is ALWAYS populated. The returned error is non-nil ONLY
// for TRANSIENT failures (MDSReachable=False: ListRoleBindings error; or an
// Enforce apply with Failed ops). Terminal outcomes (ValidationFailed, Blocked,
// Rejected) return a nil error. See the package doc.
//
// view, when non-nil, is the cluster-wide aggregated desired role-binding scope
// (spec §40); it governs prune computation only. A nil view keeps the
// per-resource (CLI single-resource) prune semantics.
func ReconcileRoleBinding(ctx context.Context, rb *v1alpha1.KafkaRoleBinding,
	cluster *v1alpha1.KafkaCluster, mdsClient mds.Client,
	view *ClusterRoleBindingView) (v1alpha1.KafkaRoleBindingStatus, error) {

	now := metav1.Now()
	st := v1alpha1.KafkaRoleBindingStatus{ObservedGeneration: rb.Generation, LastCheckedTime: &now}
	// Seed conditions from the existing status so meta.SetStatusCondition can
	// preserve LastTransitionTime when a condition's Type+Status are unchanged
	// (otherwise every periodic requeue would re-stamp the transition time).
	if rb.Status != nil {
		st.Conditions = append([]metav1.Condition(nil), rb.Status.Conditions...)
	}

	// Validate the spec BEFORE touching any live state. Objects fetched through
	// the typed client have an empty TypeMeta (stripped by the API machinery),
	// so fill the known apiVersion first — mirrors ReconcileTopic/ReconcileAccessPolicy.
	if rb.APIVersion == "" {
		rb.APIVersion = v1alpha1.APIVersion
	}
	// A validation failure is terminal: Phase Error, ValidationFailed=True, no mutation, nil error.
	if verrs := validation.ValidateRoleBindingShape(rb); len(verrs) > 0 {
		msg := joinErrMsgs(verrs)
		st.Phase = v1alpha1.PhaseError
		setTerminalValidationFailed(kindKafkaRoleBinding, &st.Conditions, reasonValidationFailed, msg, rb.Generation)
		return st, nil // terminal: needs a spec change
	}

	// Tenancy enforcement (spec §20.2): the namespace allow-list applies to
	// role bindings like every other data-plane kind, and prefix-restricted
	// namespaces are limited to Topic/Group resources matching their allowed
	// prefixes — a cluster-scoped binding (e.g. SystemAdmin) or a
	// Cluster/TransactionalId resource would let a tenant escalate past its
	// prefix. Runs AFTER shape validation but BEFORE any live-state read or
	// mutation, mirroring ReconcileTopic/ReconcilePolicy. A denial is terminal:
	// Phase Error, ValidationFailed=True with reason TenancyDenied, no
	// mutation, nil error.
	var clusterTenancy *v1alpha1.TenancyConfig
	if cluster != nil {
		clusterTenancy = cluster.Spec.Tenancy
	}
	if terr := tenancy.CheckRoleBinding(clusterTenancy, rb.Namespace, rb.Spec.Resources); terr != nil {
		msg := terr.Error()
		st.Phase = v1alpha1.PhaseError
		setTerminalValidationFailed(kindKafkaRoleBinding, &st.Conditions, reasonTenancyDenied, msg, rb.Generation)
		return st, nil // terminal: needs a tenancy config or namespace change
	}

	// Validation passed: clear a stale ValidationFailed left by a prior pass
	// (review I11) — conditions are seeded from the existing status, so a fixed
	// spec would otherwise report ValidationFailed=True forever.
	setCond(&st.Conditions, v1alpha1.CondValidationFailed, metav1.ConditionFalse, reasonValid, "spec validated", rb.Generation)

	// Guard: cluster must have authorization.mds configured. This is caught by
	// the admission webhook and cross-resource validation, but guard defensively
	// so a misconfigured cluster never panics the reconcile path.
	if cluster == nil || cluster.Spec.Authorization == nil || cluster.Spec.Authorization.MDS == nil {
		msg := "cluster has no authorization.mds configured"
		st.Phase = v1alpha1.PhaseError
		setTerminalValidationFailed(kindKafkaRoleBinding, &st.Conditions, reasonValidationFailed, msg, rb.Generation)
		return st, nil // terminal: needs a cluster configuration change
	}

	// Compile the desired role-binding set from the spec + cluster MDS config.
	// A compile error (unresolvable scope: missing cluster IDs) is terminal.
	desired, err := rbac.Compile(rb, cluster.Spec.Authorization.MDS)
	if err != nil {
		msg := fmt.Sprintf("compile role bindings: %s", err.Error())
		st.Phase = v1alpha1.PhaseError
		setTerminalValidationFailed(kindKafkaRoleBinding, &st.Conditions, reasonValidationFailed, msg, rb.Generation)
		return st, nil // terminal: needs a cluster configuration change
	}

	// Stamp the owning resource's prune consent so the managed scope carries it
	// into the diff engine (spec §10.3). Must happen BEFORE BuildScope so the
	// scope inherits the AND-merged consent across all bindings.
	rbac.StampPrune(desired, rb.Spec.Prune)

	// Gather the distinct scopes from the desired set and list live bindings
	// from MDS for each scope. A list error is TRANSIENT.
	seenScopes := make(map[string]mds.Scope)
	for _, b := range desired {
		scopeID := fmt.Sprintf("%s|%s|%s", b.Scope.Type, b.Scope.KafkaCluster, b.Scope.SubCluster)
		if _, seen := seenScopes[scopeID]; !seen {
			seenScopes[scopeID] = mds.Scope{
				Type:         b.Scope.Type,
				KafkaCluster: b.Scope.KafkaCluster,
				SubCluster:   b.Scope.SubCluster,
			}
		}
	}

	var liveBindings []rbac.RoleBinding
	for _, scope := range seenScopes {
		listed, lerr := mdsClient.ListRoleBindings(ctx, scope)
		if lerr != nil {
			st.Phase = v1alpha1.PhaseError
			setCond(&st.Conditions, v1alpha1.CondMDSReachable, metav1.ConditionFalse, reasonLiveStateError, lerr.Error(), rb.Generation)
			setCond(&st.Conditions, v1alpha1.CondReady, metav1.ConditionFalse, reasonLiveStateError, lerr.Error(), rb.Generation)
			return st, lerr // transient: requeue with backoff
		}
		liveBindings = append(liveBindings, fromMDSRoleBindings(listed)...)
	}

	// Avoid duplicate entries (same key from two scopes, though MDS returns per-scope).
	liveBindings = dedupRoleBindings(liveBindings)

	// MDS reachable.
	setCond(&st.Conditions, v1alpha1.CondMDSReachable, metav1.ConditionTrue, reasonObserved, "listed live role bindings", rb.Generation)

	// Build desired scope: either the cluster-wide aggregated scope (when a view
	// is supplied) or the per-resource scope derived from this resource's desired
	// set (CLI single-resource semantics).
	desiredScope := rbac.BuildScope(desired)
	pruneScope := desiredScope
	if view != nil {
		pruneScope = view.DesiredScope
	}

	// Diff desired vs. live and compute ops.
	d := diff.Desired{
		RoleBindings:     desired,
		RoleBindingScope: pruneScope,
	}
	if view != nil {
		d.SetRoleBindingPruneSet(view.DesiredBindings)
	}
	ops := diff.Compute(d, diff.Live{RoleBindings: liveBindings})

	// Resolve the effective mode. A nil or empty spec.reconciliation defaults to
	// Enforce (spec §16 default), mirroring ReconcileQuota.
	mode := ModeEnforce
	if rb.Spec.Reconciliation != nil && rb.Spec.Reconciliation.Mode != "" {
		mode = rb.Spec.Reconciliation.Mode
	}

	var retryErr error
	switch mode {
	case ModeDetectOnly:
		applyRoleBindingDetectOnly(&st, ops, rb.Generation, false)
	case ModeObserveOnly:
		applyRoleBindingDetectOnly(&st, ops, rb.Generation, true)
	case ModeEnforce:
		// Role-binding ops are GateNone for AddRoleBinding; RemoveRoleBinding is
		// GatePrune (spec §40). Per-op PruneAllowed (stamped by desiredScope from
		// spec.prune) governs prune execution; Approvals.Prune is false in operator
		// mode (spec §10.3: operator consent is declarative spec.prune, not a
		// run-wide flag). Mirrors ReconcileQuota's executor.Apply call.
		res := executor.Apply(ctx, executor.Clients{MDS: mdsClient}, ops, executor.Approvals{})
		st.LastAppliedTime = &now
		applyRoleBindingEnforceResult(&st, res, rb.Generation)
		retryErr = applyRetryError(res)
	default:
		// Unreachable: the up-front validation rejects unknown modes. Kept as
		// defense in depth so an unknown mode can NEVER fall through into the
		// mutating Enforce path.
		setInvalidMode(kindKafkaRoleBinding, roleBindingTarget(&st), mode, rb.Generation)
		return st, nil
	}

	// ObservedResources from spec: MDS does not return a resource-detail list,
	// so we report the declared set on success.
	st.ObservedResources = rb.Spec.Resources

	return st, retryErr
}

// ---- role-binding mode handlers ----

// roleBindingTarget adapts a KafkaRoleBindingStatus to the shared
// drift/Ready skeleton.
func roleBindingTarget(st *v1alpha1.KafkaRoleBindingStatus) driftTarget {
	return driftTarget{
		conds: &st.Conditions,
		// drift is an intentional discard sink: KafkaRoleBindingStatus omits the
		// Drift field by design (MDS bindings are managed as an authoritative set;
		// per-binding drift detail is not surfaced — see types.go). The driftTarget
		// contract requires a non-nil pointer, so we allocate a throwaway value.
		drift:    new(*v1alpha1.DriftStatus),
		setPhase: func(p string) { st.Phase = p },
	}
}

// applyRoleBindingDetectOnly sets the per-area RoleBindingSynced condition
// then the shared drift/Ready skeleton for the non-applying modes.
func applyRoleBindingDetectOnly(st *v1alpha1.KafkaRoleBindingStatus, ops []operations.Operation, gen int64, observe bool) {
	if len(pendingOps(ops)) > 0 {
		setCond(&st.Conditions, v1alpha1.CondRoleBindingSynced, metav1.ConditionFalse, reasonDriftPending, "out of sync", gen)
	} else {
		setCond(&st.Conditions, v1alpha1.CondRoleBindingSynced, metav1.ConditionTrue, reasonInSync, "in sync", gen)
	}
	finishDetectOnly(roleBindingTarget(st), ops, gen, observe)
}

// applyRoleBindingEnforceResult sets the per-area RoleBindingSynced condition
// from an executor.Result, then delegates the drift/Ready/phase decision to
// the shared finishEnforce skeleton.
func applyRoleBindingEnforceResult(st *v1alpha1.KafkaRoleBindingStatus, res executor.Result, gen int64) {
	if res.OK() {
		setCond(&st.Conditions, v1alpha1.CondRoleBindingSynced, metav1.ConditionTrue, reasonReconciled, "all role-binding operations succeeded", gen)
	} else {
		setCond(&st.Conditions, v1alpha1.CondRoleBindingSynced, metav1.ConditionFalse, reasonApplyIncomplete, applyFailureMsg(res), gen)
	}
	finishEnforce(roleBindingTarget(st), res, gen)
}

// fromMDSRoleBindings converts mds.RoleBinding (wire type) to rbac.RoleBinding
// (engine type) for feeding diff.Live.RoleBindings. Attribution fields
// (Mode, Source*) are left zero — live bindings carry no owner attribution.
// This mirrors the unexported cli.fromMDSRoleBindings, kept in-package here
// to avoid importing cli from reconcile and to preserve mds's decoupling from
// rbac per the package doc's wire-type strategy.
func fromMDSRoleBindings(in []mds.RoleBinding) []rbac.RoleBinding {
	out := make([]rbac.RoleBinding, 0, len(in))
	for _, rb := range in {
		b := rbac.RoleBinding{
			Principal: rb.Principal,
			Role:      rb.Role,
			Scope: rbac.Scope{
				Type:         rb.Scope.Type,
				KafkaCluster: rb.Scope.KafkaCluster,
				SubCluster:   rb.Scope.SubCluster,
			},
		}
		if rb.Resource != nil {
			b.Resource = &rbac.ResourcePattern{
				Type:        rb.Resource.Type,
				Name:        rb.Resource.Name,
				PatternType: rb.Resource.PatternType,
			}
		}
		out = append(out, b)
	}
	return out
}

// dedupRoleBindings removes duplicate live bindings by FullKey and returns the
// deduped slice. Used to merge results from multiple ListRoleBindings calls
// (one per distinct scope).
func dedupRoleBindings(bindings []rbac.RoleBinding) []rbac.RoleBinding {
	seen := make(map[string]struct{}, len(bindings))
	out := make([]rbac.RoleBinding, 0, len(bindings))
	for _, b := range bindings {
		key := b.FullKey()
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, b)
	}
	return out
}
