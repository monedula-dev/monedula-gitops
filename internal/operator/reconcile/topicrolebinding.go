package reconcile

import (
	"context"
	"fmt"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/diff"
	"github.com/monedula-dev/monedula-gitops/internal/executor"
	"github.com/monedula-dev/monedula-gitops/internal/mds"
	"github.com/monedula-dev/monedula-gitops/internal/rbac"
)

// reconcileTopicRoleBindings reconciles a KafkaTopic's access-derived MDS role
// bindings (spec §40), used only when the cluster's accessBackends includes
// "rbac". It mutates st in place: sets RoleBindingSynced + RBACCoarsened
// conditions, and returns a non-nil error ONLY for transient failures (MDS list
// error, or an Enforce apply with failed ops) so the controller requeues.
//
// view is the cluster-wide role-binding view (prune keep-set + scope). mdsClient
// must be non-nil. cluster.Spec.Authorization.MDS must be non-nil (caller guards).
// The topic is already defaulted + validated + TENANCY-CHECKED by
// ReconcileTopic: the resolved topicName and every consumer group name in the
// access block passed the tenancy gate, and the derived bindings reference
// exactly those names (Topic <topicName>, Group <consumer.group>) — so no
// tenancy re-check happens here.
func reconcileTopicRoleBindings(ctx context.Context, st *v1alpha1.KafkaTopicStatus,
	topic *v1alpha1.KafkaTopic, cluster *v1alpha1.KafkaCluster, mdsClient mds.Client,
	view *ClusterRoleBindingView, now metav1.Time) error {

	mdsCfg := cluster.Spec.Authorization.MDS

	desired, warns, err := rbac.CompileTopicAccess(topic, mdsCfg)
	if err != nil {
		msg := fmt.Sprintf("compile topic access role bindings: %s", err.Error())
		setTerminalValidationFailed(kindKafkaTopic, &st.Conditions, reasonValidationFailed, msg, topic.Generation)
		st.Phase = v1alpha1.PhaseError
		return nil // terminal
	}

	if len(warns) > 0 {
		setCond(&st.Conditions, v1alpha1.CondRBACCoarsened, metav1.ConditionTrue, reasonRBACCoarsened,
			strings.Join(rbac.SortedWarnings(warns), "; "), topic.Generation)
	} else {
		setCond(&st.Conditions, v1alpha1.CondRBACCoarsened, metav1.ConditionFalse, reasonRBACCoarsened,
			"no coarsening", topic.Generation)
	}

	rbac.StampPrune(desired, topic.Spec.Prune)

	seen := map[string]mds.Scope{}
	for _, b := range desired {
		id := fmt.Sprintf("%s|%s|%s", b.Scope.Type, b.Scope.KafkaCluster, b.Scope.SubCluster)
		if _, ok := seen[id]; !ok {
			seen[id] = mds.Scope{Type: b.Scope.Type, KafkaCluster: b.Scope.KafkaCluster, SubCluster: b.Scope.SubCluster}
		}
	}
	var live []rbac.RoleBinding
	for _, scope := range seen {
		listed, lerr := mdsClient.ListRoleBindings(ctx, scope)
		if lerr != nil {
			setCond(&st.Conditions, v1alpha1.CondMDSReachable, metav1.ConditionFalse, reasonLiveStateError, lerr.Error(), topic.Generation)
			setCond(&st.Conditions, v1alpha1.CondRoleBindingSynced, metav1.ConditionFalse, reasonLiveStateError, lerr.Error(), topic.Generation)
			return lerr // transient
		}
		live = append(live, fromMDSRoleBindings(listed)...)
	}
	live = dedupRoleBindings(live)
	setCond(&st.Conditions, v1alpha1.CondMDSReachable, metav1.ConditionTrue, reasonObserved, "listed live role bindings", topic.Generation)

	d := diff.Desired{RoleBindings: desired, RoleBindingScope: rbac.BuildScope(desired)}
	if view != nil {
		d.RoleBindingScope = view.DesiredScope
		d.SetRoleBindingPruneSet(view.DesiredBindings)
	}
	ops := diff.Compute(d, diff.Live{RoleBindings: live})

	// Resolve the effective mode. A nil or empty spec.reconciliation defaults to
	// Enforce (spec §16 default), mirroring ReconcileRoleBinding.
	mode := ModeEnforce
	if topic.Spec.Reconciliation != nil && topic.Spec.Reconciliation.Mode != "" {
		mode = topic.Spec.Reconciliation.Mode
	}

	switch mode {
	case ModeDetectOnly, ModeObserveOnly:
		if len(pendingOps(ops)) > 0 {
			setCond(&st.Conditions, v1alpha1.CondRoleBindingSynced, metav1.ConditionFalse, reasonDriftPending, "out of sync", topic.Generation)
		} else {
			setCond(&st.Conditions, v1alpha1.CondRoleBindingSynced, metav1.ConditionTrue, reasonInSync, "in sync", topic.Generation)
		}
		return nil
	default: // ModeEnforce ("" → Enforce; ReconcileTopic already validated the mode)
		res := executor.Apply(ctx, executor.Clients{MDS: mdsClient}, ops, executor.Approvals{})
		st.LastAppliedTime = &now
		if res.OK() {
			setCond(&st.Conditions, v1alpha1.CondRoleBindingSynced, metav1.ConditionTrue, reasonReconciled, "all role-binding operations succeeded", topic.Generation)
		} else {
			setCond(&st.Conditions, v1alpha1.CondRoleBindingSynced, metav1.ConditionFalse, reasonApplyIncomplete, applyFailureMsg(res), topic.Generation)
		}
		return applyRetryError(res)
	}
}
