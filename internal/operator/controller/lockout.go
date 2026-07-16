package controller

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/monedula-dev/monedula-gitops/internal/access"
	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/operator"
	"github.com/monedula-dev/monedula-gitops/internal/operator/reconcile"
)

// lockoutEventReason is the Event reason for the spec §30.3 self-lockout
// guard: the resource's desired ACL set does not list the operator's own
// connecting principal for some resource it covers.
const lockoutEventReason = "SelfLockoutRisk"

// connectingPrincipal resolves the SASL principal the operator connects to the
// cluster as, in Kafka's "User:<username>" form. The username is the
// auth.scram.username secret reference, resolved from the cluster's own
// namespace via the K8sResolver — the same source DefaultClientFactory builds
// the live client from. It returns "" (no lockout check possible) when no SASL
// auth is configured (mechanism None) or the reference cannot be resolved: the
// guard is a best-effort heuristic and must never fail a reconcile (a real
// client build would surface the same resolution error anyway).
func connectingPrincipal(ctx context.Context, c client.Client, cl *v1alpha1.KafkaCluster) string {
	if cl == nil || cl.Spec.Auth == nil || cl.Spec.Auth.SCRAM == nil {
		return ""
	}
	switch cl.Spec.Auth.Mechanism {
	case "", "None":
		return ""
	}
	res := &operator.K8sResolver{Client: c, Namespace: cl.Namespace, Ctx: ctx}
	user, err := res.Resolve(cl.Spec.Auth.SCRAM.Username)
	if err != nil || user == "" {
		return ""
	}
	return "User:" + user
}

// warnSelfLockout emits one Warning event per resource whose desired ACLs omit
// the connecting principal (spec §30.3). It is invoked on Enforce reconciles
// only — the modes that actually create ACLs; for simplicity it re-warns on
// every such reconcile while the desired set stays vulnerable (the event
// recorder aggregates repeats), rather than tracking whether this pass created
// the ACLs. Super-users bypass ACLs but cannot be detected client-side, so the
// warning is advisory (see access.LockoutWarnings).
func warnSelfLockout(ctx context.Context, c client.Client, recorder eventEmitter,
	obj runtime.Object, cl *v1alpha1.KafkaCluster, mode string, desiredACLs []access.ACL) {

	if recorder == nil || mode != reconcile.ModeEnforce {
		return
	}
	principal := connectingPrincipal(ctx, c, cl)
	if principal == "" {
		return
	}
	for _, w := range access.LockoutWarnings(desiredACLs, principal) {
		recorder.Eventf(obj, nil, corev1.EventTypeWarning, lockoutEventReason, actionReconcile, "%s", w)
	}
}

// eventEmitter is the slice of events.EventRecorder warnSelfLockout needs;
// both reconcilers' Recorder fields satisfy it (and nil disables events, as
// everywhere else in this package).
type eventEmitter interface {
	Eventf(regarding runtime.Object, related runtime.Object, eventtype, reason, action, note string, args ...interface{})
}
