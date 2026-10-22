package reconcile

import (
	"context"
	"errors"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/defaulting"
	"github.com/monedula-dev/monedula-gitops/internal/diff"
	"github.com/monedula-dev/monedula-gitops/internal/executor"
	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	"github.com/monedula-dev/monedula-gitops/internal/kafkarest"
	"github.com/monedula-dev/monedula-gitops/internal/mirror"
	"github.com/monedula-dev/monedula-gitops/internal/operations"
	"github.com/monedula-dev/monedula-gitops/internal/tenancy"
	"github.com/monedula-dev/monedula-gitops/internal/validation"
)

// ReconcileMirrorTopic reconciles one KafkaMirrorTopic against the destination
// cluster. It reuses the CLI's mirror planning end to end: mirror.ReadSnapshot
// for live state, diff.Compute with a single DesiredMirror (the same field
// mapping as the CLI pipeline), and executor.Apply with mirror.Writer, which
// re-verifies every write on a fresh snapshot.
//
// The returned status is always populated. The error is non-nil only for
// transient failures worth a backoff requeue: any StateReadError finding (in
// every mode) and an Enforce apply with Failed ops. Validation, tenancy,
// RESTNotConfigured, Rejected and mirror-health findings are terminal (nil
// error) and are re-evaluated at the next resync.
//
// Stall detection: status.lastObservedStatusChangeTime is stamped with now
// whenever the observed mirror_status differs from the previously persisted
// one, so the clock survives controller restarts. A MirrorTopicProgressing
// finding whose status has been unchanged for longer than stallAfter becomes
// MirrorTopicStalled (drift). PENDING_STOPPED is planned as
// MirrorTopicStopping and is drift immediately regardless of duration.
//
// rest is nil when the cluster has no spec.rest; that is a terminal
// ValidationFailed condition with reason RESTNotConfigured.
func ReconcileMirrorTopic(ctx context.Context, m *v1alpha1.KafkaMirrorTopic, cluster *v1alpha1.KafkaCluster,
	k kafka.AdminClient, rest kafkarest.Client, now time.Time, stallAfter time.Duration) (v1alpha1.KafkaMirrorTopicStatus, error) {

	nowT := metav1.NewTime(now)
	st := v1alpha1.KafkaMirrorTopicStatus{ObservedGeneration: m.Generation, LastCheckedTime: &nowT}
	// Seed conditions (so LastTransitionTime is preserved) and the observation
	// tracking fields, so terminal early returns and unreadable passes never
	// reset the stall clock or forget the last write.
	if m.Status != nil {
		st.Conditions = append([]metav1.Condition(nil), m.Status.Conditions...)
		st.ObservedMirrorStatus = m.Status.ObservedMirrorStatus
		st.LastObservedStatusChangeTime = m.Status.LastObservedStatusChangeTime.DeepCopy()
		st.LastAppliedTime = m.Status.LastAppliedTime.DeepCopy()
	}

	defaulting.MirrorTopic(m)
	if m.APIVersion == "" {
		m.APIVersion = v1alpha1.APIVersion
	}
	st.LinkName = m.Spec.LinkName
	st.SourceTopicName = m.Spec.SourceTopicName

	if verrs := validation.ValidateMirrorTopicShape(m); len(verrs) > 0 {
		st.Phase = v1alpha1.PhaseError
		setTerminalValidationFailed(kindKafkaMirrorTopic, &st.Conditions, reasonValidationFailed, joinErrMsgs(verrs), m.Generation)
		return st, nil // terminal: needs a spec change
	}

	// Tenancy checks the effective destination name; the source topic lives on
	// another cluster and is not tenancy-scoped here.
	topicName := m.Spec.EffectiveTopicName()
	var clusterTenancy *v1alpha1.TenancyConfig
	if cluster != nil {
		clusterTenancy = cluster.Spec.Tenancy
	}
	if err := tenancy.Check(clusterTenancy, m.Namespace, topicName); err != nil {
		st.Phase = v1alpha1.PhaseError
		setTerminalValidationFailed(kindKafkaMirrorTopic, &st.Conditions, reasonTenancyDenied, err.Error(), m.Generation)
		return st, nil // terminal: needs a tenancy config or namespace change
	}

	if rest == nil {
		st.Phase = v1alpha1.PhaseError
		setTerminalValidationFailed(kindKafkaMirrorTopic, &st.Conditions, reasonRESTNotConfigured,
			"the referenced KafkaCluster has no spec.rest; KafkaMirrorTopic needs a Kafka REST endpoint", m.Generation)
		return st, nil // terminal: needs spec.rest on the cluster
	}

	setCond(&st.Conditions, v1alpha1.CondValidationFailed, metav1.ConditionFalse, reasonValid, "spec validated", m.Generation)

	snap := mirror.ReadSnapshot(ctx, k, rest, []string{m.Spec.LinkName})
	if snap.Complete() {
		setCond(&st.Conditions, v1alpha1.CondClusterReachable, metav1.ConditionTrue, reasonObserved, "read live topic and mirror state", m.Generation)
		observed := ""
		if lm, ok := snap.Mirrors[topicName]; ok {
			observed = lm.Status
		}
		if observed != st.ObservedMirrorStatus || (st.LastObservedStatusChangeTime == nil && observed != "") {
			st.LastObservedStatusChangeTime = &nowT
		}
		st.ObservedMirrorStatus = observed
	} else {
		// Nothing was observed this pass: keep the previous observation so a
		// transient read failure does not restart the stall clock.
		setCond(&st.Conditions, v1alpha1.CondClusterReachable, metav1.ConditionFalse, reasonLiveStateError, snapErrMsg(snap), m.Generation)
	}

	rf := 0
	if m.Spec.ReplicationFactor != nil {
		rf = *m.Spec.ReplicationFactor
	}
	mode := m.Spec.Reconciliation.Mode
	ops := diff.Compute(diff.Desired{Mirrors: []diff.DesiredMirror{{
		Kind:              "KafkaMirrorTopic",
		Namespace:         m.Namespace,
		Name:              m.Name,
		LinkName:          m.Spec.LinkName,
		SourceTopicName:   m.Spec.SourceTopicName,
		TopicName:         topicName,
		TopicNameExplicit: m.Spec.TopicName != "",
		ReplicationFactor: rf,
		Config:            copyStringMap(m.Spec.Config),
		Mode:              mode,
	}}}, diff.Live{Mirrors: &snap})

	ops = substituteStalled(ops, st.ObservedMirrorStatus, st.LastObservedStatusChangeTime, now, stallAfter)

	// A state read error fails closed in every mode: never Ready, no writes,
	// backoff requeue. ObserveOnly does not hide operational errors.
	if msg := operationalErrorMsg(ops); msg != "" {
		st.Phase = v1alpha1.PhaseError
		setCond(&st.Conditions, v1alpha1.CondReady, metav1.ConditionFalse, reasonLiveStateError, msg, m.Generation)
		return st, errors.New(msg)
	}

	// Progressing is informational: it is neither drift nor executable.
	ops = withoutInformational(ops)

	t := mirrorTarget(&st)
	switch mode {
	case ModeDetectOnly:
		finishDetectOnly(t, ops, m.Generation, false)
		return st, nil
	case ModeObserveOnly:
		finishDetectOnly(t, ops, m.Generation, true)
		return st, nil
	case ModeEnforce:
		res := executor.Apply(ctx, executor.Clients{Kafka: k, Mirrors: mirror.Writer{Kafka: k, REST: rest}},
			ops, executor.Approvals{})
		if executedWrite(res) {
			st.LastAppliedTime = &nowT
		}
		finishMirrorEnforce(&st, res, m.Generation)
		return st, applyRetryError(res)
	default:
		// Unreachable: ValidateMirrorTopicShape rejects unknown modes.
		setInvalidMode(kindKafkaMirrorTopic, t, mode, m.Generation)
		return st, nil
	}
}

// substituteStalled replaces a MirrorTopicProgressing finding with
// MirrorTopicStalled once the observed status has been unchanged for longer
// than stallAfter. since is the persisted lastObservedStatusChangeTime.
func substituteStalled(ops []operations.Operation, status string, since *metav1.Time, now time.Time,
	stallAfter time.Duration) []operations.Operation {

	if since == nil {
		return ops
	}
	d := now.Sub(since.Time)
	if d <= stallAfter {
		return ops
	}
	for i := range ops {
		if ops[i].Action != operations.MirrorTopicProgressing {
			continue
		}
		stalled := ops[i]
		stalled.Action = operations.MirrorTopicStalled
		stalled.Message = fmt.Sprintf("mirror has been in status %s for %s (stall threshold %s)",
			status, d.Truncate(time.Second), stallAfter)
		ops[i] = stalled
	}
	return ops
}

// operationalErrorMsg joins the messages of every operational-error finding
// (StateReadError), or returns "" when there are none.
func operationalErrorMsg(ops []operations.Operation) string {
	msg := ""
	for _, op := range ops {
		if !operations.OperationalError(op.Action) {
			continue
		}
		if msg != "" {
			msg += "; "
		}
		msg += fmt.Sprintf("%s %s: %s", op.Action, op.Target, op.Message)
	}
	return msg
}

func withoutInformational(ops []operations.Operation) []operations.Operation {
	out := make([]operations.Operation, 0, len(ops))
	for _, op := range ops {
		if !operations.Informational(op.Action) {
			out = append(out, op)
		}
	}
	return out
}

// executedWrite reports whether any mirror write succeeded this pass.
func executedWrite(res executor.Result) bool {
	for _, r := range res.Results {
		if r.Status != executor.Succeeded {
			continue
		}
		switch r.Op.Action {
		case operations.CreateMirrorTopic, operations.UpdateMirrorTopicConfig:
			return true
		}
	}
	return false
}

// mirrorHealthFinding reports whether a is a non-executable mirror finding
// that represents drift (spec: observed, never executed automatically) rather
// than a rejected or failed operation.
func mirrorHealthFinding(a operations.Action) bool {
	switch a {
	case operations.MirrorTopicUnhealthy, operations.MirrorTopicStopping, operations.MirrorTopicPromoted,
		operations.MirrorTopicStalled, operations.UnsupportedReplicationFactorChange:
		return true
	}
	return false
}

// finishMirrorEnforce applies the shared Enforce skeleton, then reports the
// phase as Drifted (not Error) when the only unresolved results are mirror
// health findings: the reconcile itself succeeded, the live mirror diverges.
// Rejected, Blocked and Failed results keep the Error phase.
func finishMirrorEnforce(st *v1alpha1.KafkaMirrorTopicStatus, res executor.Result, gen int64) {
	finishEnforce(mirrorTarget(st), res, gen)
	if res.OK() {
		return
	}
	for _, r := range res.Results {
		switch r.Status {
		case executor.Succeeded, executor.ReportOnly, executor.PruneDisabled:
			continue
		case executor.Unsupported:
			if mirrorHealthFinding(r.Op.Action) {
				continue
			}
		}
		return
	}
	st.Phase = v1alpha1.PhaseDrifted
	setCond(&st.Conditions, v1alpha1.CondReady, metav1.ConditionFalse, reasonDrifted, applyFailureMsg(res), gen)
}

func snapErrMsg(s mirror.Snapshot) string {
	if s.Err != nil {
		return s.Err.Error()
	}
	return "mirror state not read"
}

func copyStringMap(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
