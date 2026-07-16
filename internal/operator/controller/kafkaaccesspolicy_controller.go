package controller

import (
	"context"
	"errors"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	ctrlreconcile "sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/monedula-dev/monedula-gitops/internal/access"
	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	"github.com/monedula-dev/monedula-gitops/internal/operator"
	"github.com/monedula-dev/monedula-gitops/internal/operator/locking"
	"github.com/monedula-dev/monedula-gitops/internal/operator/reconcile"
)

// policyRequeueAfter is the DEFAULT periodic resync interval, used when a
// reconciler's ResyncInterval field is zero. A healthy policy is re-reconciled
// on this cadence so drift introduced out-of-band (a manual ACL change on the
// broker) is re-detected even without a spec change. See resync.go.
const policyRequeueAfter = DefaultResyncInterval

// KafkaAccessPolicyReconciler reconciles a KafkaAccessPolicy. It is the ACL-only
// analogue of the KafkaTopic controller: it owns external Kafka ACL state, so it
// manages a finalizer and honors the policy's deletionPolicy on removal. A policy
// needs only the Kafka admin client (no Schema Registry).
type KafkaAccessPolicyReconciler struct {
	client.Client
	// Scheme is held for manager and event-recorder wiring.
	Scheme *runtime.Scheme
	// Clients builds the live Kafka/Schema-Registry clients for a cluster. Only
	// the Kafka admin client is used here; the SR client is ignored.
	Clients ClientFactory
	// Recorder emits one Kubernetes Event (events.k8s.io API) per reconcile
	// outcome. Set by SetupWithManager; may be nil in unit tests (events are
	// then skipped).
	Recorder events.EventRecorder
	// ClusterNamespace is where KafkaCluster CRs are looked up. When empty, the
	// policy's own namespace is used (clusterRef is namespace-local by default).
	ClusterNamespace string
	// ResyncInterval overrides the periodic resync cadence (--resync-interval).
	// Zero uses DefaultResyncInterval (5m); see resync.go.
	ResyncInterval time.Duration
	// MaxConcurrentReconciles is passed to controller.Options in
	// SetupWithManager. Zero uses DefaultMaxConcurrentReconciles (1); see
	// resync.go and --max-concurrent-reconciles.
	MaxConcurrentReconciles int
	// Locks is the process-wide keyed lock registry serializing substrate
	// writers per (KafkaCluster, substrate) — see locks.go. manager.Run always
	// injects it; nil (unit tests constructing the struct literal) acquires no
	// locks.
	Locks *locking.Registry
}

// +kubebuilder:rbac:groups=gitops.monedula.dev,resources=kafkaaccesspolicies,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=gitops.monedula.dev,resources=kafkaaccesspolicies/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=gitops.monedula.dev,resources=kafkaaccesspolicies/finalizers,verbs=update
// +kubebuilder:rbac:groups=gitops.monedula.dev,resources=kafkaclusters,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

// Reconcile drives a KafkaAccessPolicy toward its spec: it resolves the
// referenced cluster, builds the live Kafka client, manages the finalizer
// (running deletionPolicy cleanup on removal), then delegates the in-sync
// reconcile to the engine and writes the resulting status. It requeues with
// backoff on a transient error and on the periodic resync cadence otherwise.
func (r *KafkaAccessPolicyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (res ctrl.Result, retErr error) {
	logger := log.FromContext(ctx)
	defer recordReconcile(controllerKafkaAccessPolicy, time.Now(), &retErr)

	var pol v1alpha1.KafkaAccessPolicy
	if err := r.Get(ctx, req.NamespacedName, &pol); err != nil {
		// NotFound: the object is fully gone (finalizer already removed). The
		// Delete watch event always passes the event filter (predicates only
		// constrain Updates), so this branch reliably fires after a deletion and
		// is the safety net for the finalizer-path series cleanup (review I12).
		if client.IgnoreNotFound(err) == nil {
			operator.DeletePolicyDrift(req.Namespace, req.Name)
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Resolve the referenced cluster. clusterRef is namespace-local unless a
	// ClusterNamespace override is configured.
	clusterNS := r.ClusterNamespace
	if clusterNS == "" {
		clusterNS = pol.Namespace
	}
	var cluster v1alpha1.KafkaCluster
	cerr := r.Get(ctx, types.NamespacedName{Namespace: clusterNS, Name: pol.Spec.ClusterRef.Name}, &cluster)
	if cerr != nil {
		// Cluster not found (or unreadable): report Error + Ready False and
		// requeue. We requeue so the policy recovers once the cluster CR appears.
		// NOTE: if the policy is being deleted, fall through to deletion handling
		// so orphaned ACLs can still be finalized.
		if !pol.DeletionTimestamp.IsZero() {
			return r.handleUnreachableDeletion(ctx, &pol)
		}
		logger.Error(cerr, "resolving clusterRef", "cluster", pol.Spec.ClusterRef.Name, "namespace", clusterNS)
		r.event(&pol, corev1.EventTypeWarning, "ClusterNotFound", cerr.Error())
		if uerr := updateStatus(ctx, r.Client, req.NamespacedName, &pol, func() {
			st := policyErrorStatus(&pol, "ClusterNotFound", cerr.Error())
			pol.Status = &st
		}); uerr != nil {
			return ctrl.Result{}, errors.Join(cerr, uerr)
		}
		return ctrl.Result{}, cerr
	}

	// Build the live clients. On the deletion path a build failure is handled
	// specially (finalizer block / force-removal); on the normal path it is a
	// transient error. A policy uses only the Kafka admin client.
	k, _, cleanup, berr := r.Clients.For(ctx, &cluster)
	if berr != nil {
		if !pol.DeletionTimestamp.IsZero() {
			return r.handleUnreachableDeletion(ctx, &pol)
		}
		logger.Error(berr, "building cluster clients")
		r.event(&pol, corev1.EventTypeWarning, "ClientsBuildFailed", berr.Error())
		if uerr := updateStatus(ctx, r.Client, req.NamespacedName, &pol, func() {
			st := policyErrorStatus(&pol, "ClientsBuildFailed", berr.Error())
			pol.Status = &st
		}); uerr != nil {
			return ctrl.Result{}, errors.Join(berr, uerr)
		}
		return ctrl.Result{}, berr
	}
	defer cleanup()

	// Deletion path: clients built, so cleanup can run per deletionPolicy.
	if !pol.DeletionTimestamp.IsZero() {
		return r.handleDeletionWithClient(ctx, &pol, k, &cluster, clusterNS)
	}

	// Not being deleted: ensure the finalizer is present before mutating Kafka,
	// so an interrupted reconcile still leaves cleanup possible.
	if !controllerutil.ContainsFinalizer(&pol, FinalizerName) {
		controllerutil.AddFinalizer(&pol, FinalizerName)
		if err := r.Update(ctx, &pol); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Serialize this reconcile's ACL read-compute-write span against every
	// other ACL writer on the same cluster (locks.go): the sibling-CR view
	// build below, the engine's live ListACLs, and the executor apply
	// (including prunes) must be one atomic critical section — a lock taken
	// after the view is built would guard a stale union. A policy writes only
	// ACLs, so only the ACL substrate is held. Released explicitly right
	// after the engine returns, BEFORE the status write and its conflict
	// retries; the deferred call covers the error return in between and is a
	// no-op the second time.
	unlockACL := lockSubstrate(r.Locks, &cluster, locking.SubstrateACL)
	defer unlockACL()

	// Aggregate the cluster-wide desired ACL set + scope across every resource
	// referencing this cluster (spec §20.1) so the engine computes prune
	// candidates against the union, not just this policy's own ACLs — the
	// §10.4 overlapping-owners flapping fix. A list failure is transient.
	view, verr := buildClusterACLView(ctx, r.Client, pol.Spec.ClusterRef.Name,
		clusterNS, r.ClusterNamespace == "", cluster.Spec.Defaults, &cluster)
	if verr != nil {
		// No substrate mutation has happened: release the lock BEFORE the
		// status write below so its conflict retries (RetryOnConflict) cannot
		// hold up other ACL writers on this cluster — this branch fires
		// exactly when the API server is already struggling.
		unlockACL()
		logger.Error(verr, "building cluster ACL view")
		r.event(&pol, corev1.EventTypeWarning, "ACLViewFailed", verr.Error())
		if uerr := updateStatus(ctx, r.Client, req.NamespacedName, &pol, func() {
			st := policyErrorStatus(&pol, "ACLViewFailed", verr.Error())
			pol.Status = &st
		}); uerr != nil {
			return ctrl.Result{}, errors.Join(verr, uerr)
		}
		return ctrl.Result{}, verr
	}

	// In-sync reconcile via the engine. The reconcile — including its live-state
	// reads and Kafka mutations — runs exactly ONCE, before the status write: a
	// 409 Conflict on the write must retry only the write, never re-mutate Kafka
	// (review I9). Condition seeding (the LastTransitionTime preservation) reads
	// the status of the object fetched at the top of this Reconcile, as it
	// always has.
	st, rerr := reconcile.ReconcilePolicy(ctx, &pol, &cluster, k, view)
	// Substrate mutations are done: release the lock before the status write
	// below (which may retry on conflict) so other ACL writers on this
	// cluster are not held up by API-server latency.
	unlockACL()
	// Self-lockout guard (spec §30.3): on Enforce reconciles, warn when the
	// policy's desired ACLs omit the operator's own connecting principal. The
	// engine has already defaulted the policy in place.
	if pol.Spec.Reconciliation != nil {
		desiredACLs, _ := access.BuildDesiredSet(access.CompilePolicy(&pol))
		warnSelfLockout(ctx, r.Client, r.Recorder, &pol, &cluster, pol.Spec.Reconciliation.Mode, desiredACLs)
	}
	// Drift gauge semantic (review I12): set it from the freshly ENGINE-computed
	// status whenever one exists — on success, terminal outcomes AND transient
	// errors (the engine always populates st, drift included) — so a policy that
	// flips to an error does not keep its previous drift value. Only pre-engine
	// failures (cluster lookup / client build / ACL view above), where no drift
	// was computed, leave the gauge unchanged.
	operator.SetPolicyDrift(pol.Namespace, pol.Name, st.Drift != nil && st.Drift.Detected)
	if uerr := updateStatus(ctx, r.Client, req.NamespacedName, &pol, func() {
		pol.Status = &st
	}); uerr != nil {
		return ctrl.Result{}, uerr
	}

	if rerr != nil {
		r.event(&pol, corev1.EventTypeWarning, "ReconcileError", rerr.Error())
		return ctrl.Result{}, rerr // transient: requeue with backoff
	}
	r.event(&pol, corev1.EventTypeNormal, "Reconciled", "access policy reconciled")
	return ctrl.Result{RequeueAfter: resyncInterval(r.ResyncInterval)}, nil
}

// handleUnreachableDeletion is the deletion path taken when no live Kafka client
// is available (the cluster CR is missing or the clients failed to build). It
// blocks finalizer removal unless force-removal is requested, in which case it
// removes the finalizer and lets Kubernetes garbage-collect the object.
func (r *KafkaAccessPolicyReconciler) handleUnreachableDeletion(ctx context.Context, pol *v1alpha1.KafkaAccessPolicy) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(pol, FinalizerName) {
		return ctrl.Result{}, nil // already finalized; nothing to do
	}

	if pol.Annotations[AnnotationForceFinalizerRemoval] == "true" {
		r.finalizeEvent(pol, corev1.EventTypeWarning, "ForceFinalizerRemoval",
			"removing finalizer without cluster-side cleanup (cluster unreachable)")
		return r.removeFinalizer(ctx, pol)
	}

	// Cannot clean up and not forced: retain the finalizer and requeue so the
	// policy finalizes once the cluster becomes reachable again.
	msg := "cluster unreachable; cannot run deletion cleanup. Make the cluster reachable, " +
		"or set annotation " + AnnotationForceFinalizerRemoval + "=true to force removal"
	r.finalizeEvent(pol, corev1.EventTypeWarning, "DeletionBlocked", msg)
	return ctrl.Result{}, errors.New(msg) // requeue with backoff
}

// handleDeletionWithClient runs cluster-side cleanup per the policy's
// deletionPolicy using the live Kafka client, then removes the finalizer.
//
//	Orphan: leave the policy's managed ACLs in place.
//	Delete (the default for policies, spec §14.9): delete the policy's managed
//	        ACLs, but ONLY when approved via the gitops.monedula.dev/allow-delete
//	        =true annotation. Without approval the finalizer is retained (a Warning
//	        is emitted) so the deletion is gated behind an explicit operator
//	        acknowledgement — consistent with the topic teardown gate, preventing
//	        accidental ACL deletion on CR removal.
//
// cluster is the resolved KafkaCluster and clusterNS the namespace its
// clusterRef resolved in; both scope the cluster ACL view used by the
// co-ownership shield on the Delete path (see deletePolicyACLs).
func (r *KafkaAccessPolicyReconciler) handleDeletionWithClient(ctx context.Context, pol *v1alpha1.KafkaAccessPolicy, k kafka.AdminClient, cluster *v1alpha1.KafkaCluster, clusterNS string) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(pol, FinalizerName) {
		return ctrl.Result{}, nil
	}

	// "" defaults to Delete for policies (spec §14.9 / defaulting.Policy).
	policy := pol.Spec.DeletionPolicy
	if policy == "" {
		policy = deletionPolicyDelete
	}
	if policy == deletionPolicyDelete {
		if pol.Annotations[reconcile.AnnotationAllowDelete] != "true" {
			// Destructive deletion is gated: retain the finalizer until approved.
			msg := "deletionPolicy=Delete requires approval; set annotation " +
				reconcile.AnnotationAllowDelete + "=true to delete the policy's managed ACLs"
			r.finalizeEvent(pol, corev1.EventTypeWarning, "DeleteNotApproved", msg)
			return ctrl.Result{RequeueAfter: resyncInterval(r.ResyncInterval)}, nil
		}
		if err := r.deletePolicyACLs(ctx, pol, k, cluster, clusterNS); err != nil {
			r.finalizeEvent(pol, corev1.EventTypeWarning, "DeleteFailed", err.Error())
			return ctrl.Result{}, err // transient: requeue, finalizer retained
		}
		r.finalizeEvent(pol, corev1.EventTypeNormal, "Deleted", "managed ACLs deleted")
	}

	return r.removeFinalizer(ctx, pol)
}

// deletePolicyACLs deletes the policy's managed ACLs (the ACLs it compiles to),
// EXCEPT any tuple still desired by another live CR.
//
// Co-ownership shield: the cluster-wide desired ACL union is built across the
// REMAINING live CRs (buildClusterACLView skips resources with a non-zero
// DeletionTimestamp, which excludes this policy — and any other CR mid-
// deletion — by construction) and any tuple another live KafkaTopic /
// KafkaAccessPolicy still desires is subtracted from the to-delete set. This
// mirrors the prune path's §10.4 aggregation: deleting one co-owner must not
// revoke access a surviving co-owner depends on. If the view cannot be built
// (a List failure), the deletion attempt FAILS and is retried with the
// finalizer retained — never fall back to deleting the full set on error,
// since that could over-delete a co-owned tuple.
func (r *KafkaAccessPolicyReconciler) deletePolicyACLs(ctx context.Context, pol *v1alpha1.KafkaAccessPolicy, k kafka.AdminClient, cluster *v1alpha1.KafkaCluster, clusterNS string) error {
	// Serialize the finalizer's ACL cleanup against every other ACL writer on
	// this cluster: the shield's remaining-CR view build and the DeleteACLs it
	// gates must be one atomic critical section, or a concurrent reconcile
	// could re-create / co-claim tuples between the view read and the delete.
	// The function ends at DeleteACLs, so the defer IS the narrow span.
	defer lockSubstrate(r.Locks, cluster, locking.SubstrateACL)()
	desiredACLs, _ := access.BuildDesiredSet(access.CompilePolicy(pol))
	desiredACLs, err := shieldACLDeletion(ctx, r.Client, pol.Spec.ClusterRef.Name,
		clusterNS, r.ClusterNamespace == "", cluster, desiredACLs,
		func(reason, msg string) { r.finalizeEvent(pol, corev1.EventTypeNormal, reason, msg) })
	if err != nil {
		return err
	}
	if len(desiredACLs) == 0 {
		return nil
	}
	states := make([]kafka.ACLState, 0, len(desiredACLs))
	for _, a := range desiredACLs {
		states = append(states, kafka.ACLState{
			Principal: a.Principal, Host: a.Host, ResourceType: a.ResourceType,
			ResourceName: a.ResourceName, PatternType: a.PatternType,
			Operation: a.Operation, Permission: a.Permission,
		})
	}
	return k.DeleteACLs(ctx, states)
}

// removeFinalizer drops the finalizer and persists it, letting Kubernetes
// garbage-collect the object. No requeue. This is the single deletion-success
// exit (both the Orphan and Delete paths, plus force-removal, end here), so the
// per-CR drift-series cleanup lives here (review I12).
func (r *KafkaAccessPolicyReconciler) removeFinalizer(ctx context.Context, pol *v1alpha1.KafkaAccessPolicy) (ctrl.Result, error) {
	controllerutil.RemoveFinalizer(pol, FinalizerName)
	if err := r.Update(ctx, pol); err != nil {
		return ctrl.Result{}, err
	}
	operator.DeletePolicyDrift(pol.Namespace, pol.Name)
	return ctrl.Result{}, nil
}

// event emits a Kubernetes Event for obj on the reconcile path, a no-op when
// no Recorder is wired. See events.go for the action convention.
func (r *KafkaAccessPolicyReconciler) event(obj *v1alpha1.KafkaAccessPolicy, eventType, reason, msg string) {
	if r.Recorder == nil {
		return
	}
	r.Recorder.Eventf(obj, nil, eventType, reason, actionReconcile, "%s", msg)
}

// finalizeEvent is event for the deletion/finalizer path (actionFinalize).
func (r *KafkaAccessPolicyReconciler) finalizeEvent(obj *v1alpha1.KafkaAccessPolicy, eventType, reason, msg string) {
	if r.Recorder == nil {
		return
	}
	r.Recorder.Eventf(obj, nil, eventType, reason, actionFinalize, "%s", msg)
}

// mapSecretToPolicies enqueues the KafkaAccessPolicies on any cluster (in the
// Secret's namespace) that references the changed credential/TLS Secret (§11.4).
// 2nd hop of the fan-out; list scope mirrors buildClusterACLView.
func (r *KafkaAccessPolicyReconciler) mapSecretToPolicies(ctx context.Context, obj client.Object) []ctrlreconcile.Request {
	if _, ok := obj.(*corev1.Secret); !ok {
		return nil
	}
	clusterNames := clustersReferencingSecret(ctx, r.Client, obj)
	if len(clusterNames) == 0 {
		return nil
	}
	want := make(map[string]struct{}, len(clusterNames))
	for _, n := range clusterNames {
		want[n] = struct{}{}
	}
	var listOpts []client.ListOption
	if r.ClusterNamespace == "" {
		listOpts = append(listOpts, client.InNamespace(obj.GetNamespace()))
	}
	var list v1alpha1.KafkaAccessPolicyList
	if err := r.List(ctx, &list, listOpts...); err != nil {
		return nil
	}
	var out []ctrlreconcile.Request
	for i := range list.Items {
		if _, ok := want[list.Items[i].Spec.ClusterRef.Name]; ok {
			out = append(out, ctrlreconcile.Request{NamespacedName: types.NamespacedName{
				Namespace: list.Items[i].Namespace,
				Name:      list.Items[i].Name,
			}})
		}
	}
	return out
}

// SetupWithManager registers the reconciler with the manager, watching
// KafkaAccessPolicy objects. watchEventFilter drops status-only updates
// (notably this controller's own status writes) so a reconcile does not
// re-enqueue itself; spec/annotation/lifecycle changes and the periodic
// RequeueAfter still trigger.
//
// Predicate scoped to the primary kind (not global WithEventFilter) so the
// Secret watch's generation-less data-change events are not dropped (§11.4).
// The KafkaCluster watch (review I2) recovers policies promptly when their
// cluster CR appears or its spec is fixed, instead of waiting out error backoff.
func (r *KafkaAccessPolicyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Recorder == nil {
		r.Recorder = mgr.GetEventRecorder("kafkaaccesspolicy-controller")
	}
	// Insurance against silently running unlocked: nil Locks means every
	// substrate span in this controller is unserialized (locks.go). Expected
	// only in tests — manager.Run always injects the registry.
	if r.Locks == nil {
		mgr.GetLogger().Info("substrate locking disabled (nil Locks registry); expected only in tests",
			"controller", "kafkaaccesspolicy")
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.KafkaAccessPolicy{}, builder.WithPredicates(watchEventFilter())).
		Watches(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(r.mapSecretToPolicies)).
		Watches(&v1alpha1.KafkaCluster{}, handler.EnqueueRequestsFromMapFunc(r.mapClusterToPolicies),
			builder.WithPredicates(clusterWatchPredicate())).
		WithOptions(reconcilerOptions(r.MaxConcurrentReconciles)).
		Complete(r)
}

// policyErrorStatus builds an Error status with Ready False (reason) for a
// pre-engine failure (cluster lookup or client build). It seeds conditions from
// the existing status so LastTransitionTime is preserved across requeues.
func policyErrorStatus(pol *v1alpha1.KafkaAccessPolicy, reason, msg string) v1alpha1.KafkaAccessPolicyStatus {
	now := metav1.Now()
	st := v1alpha1.KafkaAccessPolicyStatus{
		ObservedGeneration: pol.Generation,
		Phase:              v1alpha1.PhaseError,
		LastCheckedTime:    &now,
	}
	if pol.Status != nil {
		st.Conditions = append([]metav1.Condition(nil), pol.Status.Conditions...)
	}
	meta.SetStatusCondition(&st.Conditions, metav1.Condition{
		Type:               v1alpha1.CondReady,
		Status:             metav1.ConditionFalse,
		Reason:             reason,
		Message:            msg,
		ObservedGeneration: pol.Generation,
	})
	return st
}
