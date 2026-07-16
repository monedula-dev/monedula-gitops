package reconcile

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/diff"
	"github.com/monedula-dev/monedula-gitops/internal/executor"
	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	"github.com/monedula-dev/monedula-gitops/internal/operations"
	"github.com/monedula-dev/monedula-gitops/internal/quota"
	"github.com/monedula-dev/monedula-gitops/internal/tenancy"
	"github.com/monedula-dev/monedula-gitops/internal/validation"
)

// ReconcileQuota is the client-quota analogue of ReconcilePolicy for a
// KafkaQuota (spec §39.6). It validates the (per-resource) shape, compiles the
// entity + limit set, observes live quotas, reuses diff.Compute + executor.Apply
// per the quota's reconciliation mode, and returns the status the controller
// should write plus a retryable-error signal.
//
// The returned status is ALWAYS populated. The returned error is non-nil ONLY
// for TRANSIENT failures the controller should requeue-with-backoff on: a
// live-state read failure (ListQuotas errored) or an Enforce apply with Failed
// ops. Terminal outcomes (ValidationFailed) set the Error phase + conditions but
// return a nil error. See the package doc.
//
// Tenancy enforcement (spec §20.2): the cluster's namespace ALLOW-LIST applies
// to quotas like every other data-plane kind — otherwise any namespace could
// zero-out another team's quota (a denial-of-service path). Entity-level
// scoping is NOT enforced: a quota targets a principal/client-id, which the
// topic-prefix rules cannot scope, so prefix-restricted namespaces get no
// additional entity check (documented limitation; see docs/operator.md).
func ReconcileQuota(ctx context.Context, q *v1alpha1.KafkaQuota, cluster *v1alpha1.KafkaCluster,
	k kafka.AdminClient) (v1alpha1.KafkaQuotaStatus, error) {

	now := metav1.Now()
	st := v1alpha1.KafkaQuotaStatus{ObservedGeneration: q.Generation, LastCheckedTime: &now}
	// Seed conditions from the existing status so meta.SetStatusCondition can
	// preserve LastTransitionTime when a condition's Type+Status are unchanged.
	if q.Status != nil {
		st.Conditions = append([]metav1.Condition(nil), q.Status.Conditions...)
	}

	// Validate the spec BEFORE touching any live state; see ReconcilePolicy. A
	// failure is terminal: Phase Error, ValidationFailed=True, no mutation, nil
	// error. ValidateQuotaShape is the single-resource entry the webhook reuses;
	// clusterRef resolution + identity uniqueness are cross-resource concerns not
	// checked here.
	if verrs := validation.ValidateQuotaShape(q); len(verrs) > 0 {
		msg := joinErrMsgs(verrs)
		st.Phase = v1alpha1.PhaseError
		setTerminalValidationFailed(kindKafkaQuota, &st.Conditions, reasonValidationFailed, msg, q.Generation)
		return st, nil // terminal: needs a spec change
	}

	// Tenancy enforcement (spec §20.2): namespace allow-list only (see the
	// function doc for why entities are not prefix-scoped). Runs AFTER shape
	// validation but BEFORE any live-state read or mutation, mirroring
	// ReconcileTopic/ReconcilePolicy. A denial is terminal: Phase Error,
	// ValidationFailed=True with reason TenancyDenied, no mutation, nil error.
	var clusterTenancy *v1alpha1.TenancyConfig
	if cluster != nil {
		clusterTenancy = cluster.Spec.Tenancy
	}
	if err := tenancy.CheckNamespace(clusterTenancy, q.Namespace); err != nil {
		msg := err.Error()
		st.Phase = v1alpha1.PhaseError
		setTerminalValidationFailed(kindKafkaQuota, &st.Conditions, reasonTenancyDenied, msg, q.Generation)
		return st, nil // terminal: needs a tenancy config or namespace change
	}

	// Validation passed: clear a stale ValidationFailed left by a prior pass
	// (review I11); see ReconcileTopic.
	setCond(&st.Conditions, v1alpha1.CondValidationFailed, metav1.ConditionFalse, reasonValid, "spec validated", q.Generation)

	compiled := quota.Compile(q)

	live, err := k.ListQuotas(ctx)
	if err != nil {
		st.Phase = v1alpha1.PhaseError
		setCond(&st.Conditions, v1alpha1.CondClusterReachable, metav1.ConditionFalse, reasonLiveStateError, err.Error(), q.Generation)
		setCond(&st.Conditions, v1alpha1.CondReady, metav1.ConditionFalse, reasonLiveStateError, err.Error(), q.Generation)
		return st, err // transient: requeue with backoff
	}
	// Reached the cluster successfully.
	setCond(&st.Conditions, v1alpha1.CondClusterReachable, metav1.ConditionTrue, reasonObserved, "listed live quotas", q.Generation)

	// Diff THIS entity only: computeQuotaOps scopes to the desired set, so a
	// single-element Desired.Quotas yields ops for exactly this quota (spec §39.3
	// — never prunes unmanaged entities).
	ops := diff.Compute(diff.Desired{Quotas: []quota.Desired{compiled}}, diff.Live{Quotas: live})

	// Resolve the effective mode. KafkaQuota has no defaulting webhook/function
	// (unlike topics/policies), so a nil or empty spec.reconciliation defaults to
	// Enforce here — the spec §16 default.
	mode := ModeEnforce
	if q.Spec.Reconciliation != nil && q.Spec.Reconciliation.Mode != "" {
		mode = q.Spec.Reconciliation.Mode
	}

	var retryErr error
	switch mode {
	case ModeDetectOnly:
		applyQuotaDetectOnly(&st, ops, q.Generation, false)
	case ModeObserveOnly:
		applyQuotaDetectOnly(&st, ops, q.Generation, true)
	case ModeEnforce:
		// Set/UpdateQuota are GateNone (reversible, spec §39) and execute
		// directly, but RemoveQuota authoritatively DELETES a live limit and is
		// GateDestructive (spec §17.1) — approvals therefore come from the CR's
		// risk-gate annotations exactly like the topic path: removing a live
		// limit key requires gitops.monedula.dev/allow-destructive: "true", or
		// the op is Blocked (reported, never executed). The ops carry their
		// owning resource's Mode, so the executor honors §16 even here.
		res := executor.Apply(ctx, executor.Clients{Kafka: k}, ops, approvalsFromAnnotations(q.Annotations))
		st.LastAppliedTime = &now
		applyQuotaEnforceResult(&st, res, q.Generation)
		retryErr = applyRetryError(res)
		// Re-observe so ObservedLimits reflects post-apply live state (e.g. the
		// entity created this pass). Best-effort: a re-read failure leaves the
		// pre-apply observation in place rather than overriding the apply outcome.
		if relive, rerr := k.ListQuotas(ctx); rerr == nil {
			live = relive
		}
	default:
		// Unreachable in practice (the spec defaults the mode to Enforce). Kept as
		// defense in depth so an unknown mode can NEVER fall through into the
		// mutating Enforce path.
		setInvalidMode(kindKafkaQuota, quotaTarget(&st), mode, q.Generation)
		return st, nil
	}

	// ObservedLimits from the (post-apply, for Enforce) live state for this entity.
	st.ObservedLimits = observedQuotaLimits(live, compiled.Entity)

	return st, retryErr
}

// ---- quota mode handlers ----

// quotaTarget adapts a KafkaQuotaStatus to the shared drift/Ready skeleton.
func quotaTarget(st *v1alpha1.KafkaQuotaStatus) driftTarget {
	return driftTarget{conds: &st.Conditions, drift: &st.Drift, setPhase: func(p string) { st.Phase = p }}
}

// applyQuotaDetectOnly sets the per-area QuotaSynced condition then the shared
// drift/Ready skeleton for the non-applying modes (DetectOnly/ObserveOnly).
func applyQuotaDetectOnly(st *v1alpha1.KafkaQuotaStatus, ops []operations.Operation, gen int64, observe bool) {
	if len(pendingOps(ops)) > 0 {
		setCond(&st.Conditions, v1alpha1.CondQuotaSynced, metav1.ConditionFalse, reasonDriftPending, "out of sync", gen)
	} else {
		setCond(&st.Conditions, v1alpha1.CondQuotaSynced, metav1.ConditionTrue, reasonInSync, "in sync", gen)
	}
	finishDetectOnly(quotaTarget(st), ops, gen, observe)
}

// applyQuotaEnforceResult sets the per-area QuotaSynced condition from an
// executor.Result, then delegates the drift/Ready/phase decision to the shared
// finishEnforce skeleton.
func applyQuotaEnforceResult(st *v1alpha1.KafkaQuotaStatus, res executor.Result, gen int64) {
	if res.OK() {
		setCond(&st.Conditions, v1alpha1.CondQuotaSynced, metav1.ConditionTrue, reasonReconciled, "all quota operations succeeded", gen)
	} else {
		setCond(&st.Conditions, v1alpha1.CondQuotaSynced, metav1.ConditionFalse, reasonApplyIncomplete, applyFailureMsg(res), gen)
	}
	finishEnforce(quotaTarget(st), res, gen)
}

// observedQuotaLimits finds the live quota for entity and renders it as a
// QuotaLimits status pointer. Entities are matched by quota.Entity.Key() so the
// live []kafka.QuotaEntityComponent and the desired quota.Entity compare on the
// same canonical key. Returns nil when the entity has no live quota.
func observedQuotaLimits(live []kafka.QuotaState, entity quota.Entity) *v1alpha1.QuotaLimits {
	want := entity.Key()
	for _, qs := range live {
		le := make(quota.Entity, 0, len(qs.Entity))
		for _, c := range qs.Entity {
			le = append(le, quota.Component{Type: c.Type, Name: c.Name})
		}
		if le.Key() != want {
			continue
		}
		var out v1alpha1.QuotaLimits
		if v, ok := qs.Limits["producer_byte_rate"]; ok {
			out.ProducerByteRate = floatPtr(v)
		}
		if v, ok := qs.Limits["consumer_byte_rate"]; ok {
			out.ConsumerByteRate = floatPtr(v)
		}
		if v, ok := qs.Limits["request_percentage"]; ok {
			out.RequestPercentage = floatPtr(v)
		}
		if v, ok := qs.Limits["controller_mutation_rate"]; ok {
			out.ControllerMutationRate = floatPtr(v)
		}
		return &out
	}
	return nil
}

func floatPtr(v float64) *float64 { return &v }
