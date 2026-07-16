package controller

import (
	"context"
	"errors"
	"fmt"
	"sync"
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

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/defaulting"
	"github.com/monedula-dev/monedula-gitops/internal/mds"
	"github.com/monedula-dev/monedula-gitops/internal/operator"
	"github.com/monedula-dev/monedula-gitops/internal/operator/locking"
	"github.com/monedula-dev/monedula-gitops/internal/operator/reconcile"
	"github.com/monedula-dev/monedula-gitops/internal/rbac"
)

// roleBindingRequeueAfter is the DEFAULT periodic resync interval, used when a
// reconciler's ResyncInterval field is zero. A healthy role binding is
// re-reconciled on this cadence so drift introduced out-of-band (a manual MDS
// change) is re-detected even without a spec change. See resync.go.
const roleBindingRequeueAfter = DefaultResyncInterval

// KafkaRoleBindingReconciler reconciles a KafkaRoleBinding. It is the MDS RBAC
// analogue of the KafkaQuotaReconciler: it owns external MDS role-binding state,
// so it manages a finalizer and honours the resource's deletionPolicy on removal.
// Unlike quota teardown (always-delete), role-binding teardown is GATED on
// spec.deletionPolicy (Orphan / Delete), mirroring the KafkaTopic pattern.
type KafkaRoleBindingReconciler struct {
	client.Client
	// Scheme is held for manager and event-recorder wiring.
	Scheme *runtime.Scheme
	// Clients builds the live MDS client for a cluster via MDSFor.
	Clients ClientFactory
	// Recorder emits one Kubernetes Event (events.k8s.io API) per reconcile
	// outcome. Set by SetupWithManager; may be nil in unit tests (events are
	// then skipped).
	Recorder events.EventRecorder
	// ClusterNamespace is where KafkaCluster CRs are looked up. When empty,
	// the role binding's own namespace is used (clusterRef is namespace-local).
	ClusterNamespace string
	// ResyncInterval overrides the periodic resync cadence (--resync-interval).
	// Zero uses DefaultResyncInterval (5m); see resync.go.
	ResyncInterval time.Duration
	// MaxConcurrentReconciles is passed to controller.Options in
	// SetupWithManager. Zero uses DefaultMaxConcurrentReconciles (1); see
	// resync.go and --max-concurrent-reconciles.
	MaxConcurrentReconciles int
	// Locks is the process-wide keyed lock registry serializing substrate
	// writers per (KafkaCluster, substrate) and identity claimants per
	// (KafkaCluster, kind, identity) — see locks.go. manager.Run always
	// injects it; nil (unit tests constructing the struct literal) acquires no
	// locks.
	Locks *locking.Registry
	// APIReader is the manager's uncached quorum reader (mgr.GetAPIReader()),
	// used ONLY for the duplicate-identity gate's contested-path recheck (see
	// duplicate.go). manager.Run always injects it; nil (unit tests, the
	// plain-client envtest harness) skips the rechecks.
	APIReader client.Reader
}

// +kubebuilder:rbac:groups=gitops.monedula.dev,resources=kafkarolebindings,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=gitops.monedula.dev,resources=kafkarolebindings/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=gitops.monedula.dev,resources=kafkarolebindings/finalizers,verbs=update
// +kubebuilder:rbac:groups=gitops.monedula.dev,resources=kafkaclusters,verbs=get;list;watch

// Reconcile drives a KafkaRoleBinding toward its spec: it resolves the
// referenced cluster, builds the live MDS client, manages the finalizer
// (running deletionPolicy cleanup on removal), then delegates the in-sync
// reconcile to the engine and writes the resulting status. It requeues with
// backoff on a transient error and on the periodic resync cadence otherwise.
func (r *KafkaRoleBindingReconciler) Reconcile(ctx context.Context, req ctrl.Request) (res ctrl.Result, retErr error) {
	logger := log.FromContext(ctx)
	defer recordReconcile(controllerKafkaRoleBinding, time.Now(), &retErr)

	var rb v1alpha1.KafkaRoleBinding
	if err := r.Get(ctx, req.NamespacedName, &rb); err != nil {
		// NotFound: the object is fully gone (finalizer already removed). The
		// Delete watch event always passes the event filter, so this branch
		// reliably fires after a deletion and is the safety net for the
		// finalizer-path metrics cleanup (review I12) — e.g. after an operator
		// restart between finalize and delete. Mirrors KafkaTopicReconciler.
		if client.IgnoreNotFound(err) == nil {
			forgetRoleBindingMetrics(req.Namespace, req.Name)
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	r.observeManaged(&rb)

	// Resolve the referenced cluster. clusterRef is namespace-local unless a
	// ClusterNamespace override is configured.
	clusterNS := r.ClusterNamespace
	if clusterNS == "" {
		clusterNS = rb.Namespace
	}
	clusterRefName := rb.Spec.ClusterRef.Name

	var cluster v1alpha1.KafkaCluster
	cerr := r.Get(ctx, types.NamespacedName{Namespace: clusterNS, Name: clusterRefName}, &cluster)
	if cerr != nil {
		if !rb.DeletionTimestamp.IsZero() {
			return r.handleUnreachableDeletion(ctx, &rb)
		}
		logger.Error(cerr, "resolving clusterRef", "cluster", clusterRefName, "namespace", clusterNS)
		r.event(&rb, corev1.EventTypeWarning, "ClusterNotFound", cerr.Error())
		if uerr := updateStatus(ctx, r.Client, req.NamespacedName, &rb, func() {
			st := roleBindingErrorStatus(&rb, "ClusterNotFound", cerr.Error())
			rb.Status = &st
		}); uerr != nil {
			return ctrl.Result{}, errors.Join(cerr, uerr)
		}
		return ctrl.Result{}, cerr
	}

	// Duplicate-identity gate (§40; the webhook-off backstop, see duplicate.go):
	// if an OLDER live KafkaRoleBinding's compiled MDS bindings overlap this
	// CR's, this CR goes terminal (ValidationFailed/DuplicateIdentity) instead
	// of flapping last-writer-wins against the winner. Guarded to the
	// non-deletion path — a deleting loser must still reach its finalizer
	// below — and placed BEFORE the MDS client build, so a loser never even
	// connects to MDS. A cluster without authorization.mds skips the gate (the
	// MDSNotConfigured handling below owns that case).
	//
	// The gate → quorum-recheck → engine-mutation span runs under this
	// binding's per-identity lock (locks.go; key roleBindingLockIdentity),
	// taken BEFORE the gate — and before the RBAC substrate lock below, per
	// the identity → acl → rbac global order — so two same-identity claimants
	// can never interleave their gate checks and MDS mutations. Released
	// explicitly right after the engine returns (with the substrate lock); the
	// deferred call backstops every error return in between. The deletion path
	// takes NO identity lock: its MDS cleanup is substrate-shared (the §40
	// co-ownership shield) and already serialized against every MDS writer —
	// same-identity rivals included — by the RBAC substrate lock in
	// handleDeletionWithClient.
	unlockIdentity := func() {}
	if rb.DeletionTimestamp.IsZero() {
		unlockIdentity = lockIdentity(r.Locks, &cluster, "KafkaRoleBinding", roleBindingLockIdentity(&rb))
		defer unlockIdentity()
		var mdsCfg *v1alpha1.MDSConfig
		if cluster.Spec.Authorization != nil {
			mdsCfg = cluster.Spec.Authorization.MDS
		}
		if res, done, err := r.duplicateIdentityGate(ctx, &rb, mdsCfg); done {
			return res, err
		}
	}

	// Build the MDS client. On the deletion path a build failure is handled
	// specially (finalizer block / force-removal); on the normal path it is a
	// transient error.
	mdsClient, berr := r.Clients.MDSFor(ctx, &cluster)
	if berr != nil {
		if !rb.DeletionTimestamp.IsZero() {
			return r.handleUnreachableDeletion(ctx, &rb)
		}
		logger.Error(berr, "building MDS client")
		r.event(&rb, corev1.EventTypeWarning, "MDSClientFailed", berr.Error())
		if uerr := updateStatus(ctx, r.Client, req.NamespacedName, &rb, func() {
			st := roleBindingErrorStatus(&rb, "MDSClientFailed", berr.Error())
			meta.SetStatusCondition(&st.Conditions, metav1.Condition{
				Type:               v1alpha1.CondMDSReachable,
				Status:             metav1.ConditionFalse,
				Reason:             "MDSClientFailed",
				Message:            berr.Error(),
				ObservedGeneration: rb.Generation,
			})
			rb.Status = &st
		}); uerr != nil {
			return ctrl.Result{}, errors.Join(berr, uerr)
		}
		return ctrl.Result{}, berr
	}

	// nil mdsClient means the cluster has no authorization.mds configured. On
	// the deletion path this is handled like an unreachable client (cannot
	// clean up MDS bindings). On the normal path it is a terminal misconfiguration.
	if mdsClient == nil {
		if !rb.DeletionTimestamp.IsZero() {
			// No MDS to clean up; just drop the finalizer so the object can be GC'd.
			return r.removeFinalizer(ctx, &rb)
		}
		msg := "cluster has no authorization.mds configured"
		r.event(&rb, corev1.EventTypeWarning, "MDSNotConfigured", msg)
		operator.RecordReconcileTerminal(controllerKafkaRoleBinding, "MDSNotConfigured")
		if uerr := updateStatus(ctx, r.Client, req.NamespacedName, &rb, func() {
			st := roleBindingErrorStatus(&rb, "MDSNotConfigured", msg)
			rb.Status = &st
		}); uerr != nil {
			return ctrl.Result{}, uerr
		}
		// Needs a cluster configuration change to proceed. The KafkaCluster
		// watch re-reconciles this binding the moment authorization.mds is
		// added (a spec change); the resync RequeueAfter is the safety net so
		// the binding can never be PERMANENTLY wedged (review I2) — e.g. after
		// a missed watch event or operator restart race. (Primary recovery is
		// the KafkaCluster watch — see SetupWithManager; this is the fallback.)
		return ctrl.Result{RequeueAfter: resyncInterval(r.ResyncInterval)}, nil
	}

	// Deletion path: MDS client available, so cluster-side cleanup can run.
	if !rb.DeletionTimestamp.IsZero() {
		return r.handleDeletionWithClient(ctx, &rb, &cluster, mdsClient)
	}

	// Not being deleted: ensure the finalizer is present before mutating MDS,
	// so an interrupted reconcile still leaves cleanup possible.
	if !controllerutil.ContainsFinalizer(&rb, FinalizerName) {
		controllerutil.AddFinalizer(&rb, FinalizerName)
		if err := r.Update(ctx, &rb); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Serialize this reconcile's RBAC read-compute-write span against every
	// other MDS writer on the same cluster (locks.go): the sibling-CR view
	// build below, the engine's per-scope ListRoleBindings, and the executor
	// apply (including prunes) must be one atomic critical section — a lock
	// taken after the view is built would guard a stale union. A role binding
	// writes only MDS, so only the RBAC substrate is held (never while
	// holding ACL — the acl→rbac global order is owned by LockACLThenRBAC).
	// Released explicitly right after the engine returns, BEFORE the status
	// write and its conflict retries; the deferred call covers the error
	// return in between and is a no-op the second time.
	unlockRBAC := lockSubstrate(r.Locks, &cluster, locking.SubstrateRBAC)
	defer unlockRBAC()

	// Build the cluster-wide role-binding view for prune-scope computation
	// (spec §40 — the anti-flap fix mirroring the ACL view for topics).
	//
	// Defensive nil check: mdsClient != nil implies Authorization.MDS != nil per
	// BuildMDSClient's invariant, so mdsCfg will be non-nil on the production path.
	// This guard protects against a future MDSFor stub returning a non-nil client
	// for a cluster with nil Authorization; buildClusterRoleBindingView already
	// handles a nil mds argument gracefully (it skips compilation).
	var mdsCfg *v1alpha1.MDSConfig
	if cluster.Spec.Authorization != nil {
		mdsCfg = cluster.Spec.Authorization.MDS
	}
	view, verr := buildClusterRoleBindingView(ctx, r.Client, clusterRefName, clusterNS,
		r.ClusterNamespace == "", mdsCfg, &cluster)
	if verr != nil {
		// No substrate mutation has happened: release the lock BEFORE the
		// status write below so its conflict retries (RetryOnConflict) cannot
		// hold up other MDS writers on this cluster — this branch fires
		// exactly when the API server is already struggling.
		unlockRBAC()
		logger.Error(verr, "building cluster role-binding view")
		r.event(&rb, corev1.EventTypeWarning, "RoleBindingViewFailed", verr.Error())
		if uerr := updateStatus(ctx, r.Client, req.NamespacedName, &rb, func() {
			st := roleBindingErrorStatus(&rb, "RoleBindingViewFailed", verr.Error())
			rb.Status = &st
		}); uerr != nil {
			return ctrl.Result{}, errors.Join(verr, uerr)
		}
		return ctrl.Result{}, verr
	}

	// In-sync reconcile via the engine. The reconcile — including its live-state
	// reads and MDS mutations — runs exactly ONCE, before the status write: a 409
	// Conflict on the write must retry only the write, never re-mutate MDS
	// (review I9).
	st, rerr := reconcile.ReconcileRoleBinding(ctx, &rb, &cluster, mdsClient, view)
	// Substrate mutations are done: release the lock before the status write
	// below (which may retry on conflict) so other MDS writers on this
	// cluster are not held up by API-server latency. Reverse acquisition
	// order: the substrate lock first, then the identity lock.
	unlockRBAC()
	unlockIdentity()
	// Drift gauge semantic (review I12, mirrors KafkaTopicReconciler): set it
	// from the freshly ENGINE-computed status whenever one exists, including
	// transient-error outcomes. KafkaRoleBindingStatus deliberately omits a
	// Drift struct (see roleBindingTarget), so this is keyed off Phase ==
	// Drifted — the same signal, sourced from what this kind's status exposes.
	operator.SetRoleBindingDrift(rb.Namespace, rb.Name, st.Phase == v1alpha1.PhaseDrifted)
	if uerr := updateStatus(ctx, r.Client, req.NamespacedName, &rb, func() {
		rb.Status = &st
	}); uerr != nil {
		return ctrl.Result{}, uerr
	}

	if rerr != nil {
		r.event(&rb, corev1.EventTypeWarning, "ReconcileError", rerr.Error())
		return ctrl.Result{}, rerr // transient: requeue with backoff
	}
	r.event(&rb, corev1.EventTypeNormal, "Reconciled", "role bindings reconciled")
	return ctrl.Result{RequeueAfter: resyncInterval(r.ResyncInterval)}, nil
}

// handleUnreachableDeletion is the deletion path taken when no live MDS client
// is available (the cluster CR is missing, the client failed to build, or the
// cluster has no MDS configured). It blocks finalizer removal unless
// force-removal is requested, in which case it removes the finalizer and lets
// Kubernetes garbage-collect the object.
func (r *KafkaRoleBindingReconciler) handleUnreachableDeletion(ctx context.Context, rb *v1alpha1.KafkaRoleBinding) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(rb, FinalizerName) {
		return ctrl.Result{}, nil // already finalized; nothing to do
	}

	if rb.Annotations[AnnotationForceFinalizerRemoval] == "true" {
		r.finalizeEvent(rb, corev1.EventTypeWarning, "ForceFinalizerRemoval",
			"removing finalizer without cluster-side cleanup (cluster unreachable)")
		return r.removeFinalizer(ctx, rb)
	}

	// Cannot clean up and not forced: retain the finalizer and requeue so the
	// binding finalizes once the cluster becomes reachable again.
	msg := "cluster unreachable; cannot run deletion cleanup. Make the cluster reachable, " +
		"or set annotation " + AnnotationForceFinalizerRemoval + "=true to force removal"
	r.finalizeEvent(rb, corev1.EventTypeWarning, "DeletionBlocked", msg)
	return ctrl.Result{}, errors.New(msg) // requeue with backoff
}

// handleDeletionWithClient runs cluster-side cleanup per the role binding's
// deletionPolicy using the live MDS client, then removes the finalizer.
//
//	Delete (default per defaulting.RoleBinding — the compiled MDS role bindings
//	        are this CR's entire reason to exist, mirroring KafkaAccessPolicy and
//	        KafkaUser): remove each compiled MDS role binding via
//	        mdsClient.RemoveRoleBinding. A removal error requeues with backoff
//	        (the finalizer is retained) until all bindings are removed — unlike
//	        quota's best-effort approach, an incomplete MDS cleanup leaves
//	        orphaned bindings that the next operator start would re-add, so we
//	        gate finalizer removal on success.
//	Orphan: leave the MDS role bindings in place.
//
// Co-ownership shield: before removing, the cluster-wide desired role-binding
// union is built across the REMAINING live CRs (buildClusterRoleBindingView
// skips resources with a non-zero DeletionTimestamp, which excludes this CR —
// and any other CR mid-deletion — by construction) and any binding still
// desired by a live KafkaRoleBinding or a live topic's access block (on rbac
// clusters) is subtracted from the to-remove set. Deleting one co-owner must
// not revoke a binding a surviving co-owner depends on — the delete-path
// analogue of the prune path's §40 aggregation. If the view cannot be built
// (a List failure), the deletion attempt FAILS and is retried with the
// finalizer retained — never fall back to removing the full set on error.
func (r *KafkaRoleBindingReconciler) handleDeletionWithClient(ctx context.Context, rb *v1alpha1.KafkaRoleBinding, cluster *v1alpha1.KafkaCluster, mdsClient mds.Client) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(rb, FinalizerName) {
		return ctrl.Result{}, nil
	}

	// Resolve the declared deletionPolicy exactly as the reconcile engine does
	// (Delete is the default — see defaulting.RoleBinding) — in-memory only;
	// nothing is written back to the spec.
	defaulting.RoleBinding(rb)

	if rb.Spec.DeletionPolicy == deletionPolicyDelete {
		if cluster.Spec.Authorization == nil || cluster.Spec.Authorization.MDS == nil {
			// Cannot compile without MDS config: treat like Orphan (nothing to remove).
			r.finalizeEvent(rb, corev1.EventTypeWarning, "DeletionSkipped",
				"deletionPolicy=Delete but cluster has no authorization.mds; skipping MDS cleanup")
		} else {
			compiled, err := rbac.Compile(rb, cluster.Spec.Authorization.MDS)
			if err != nil {
				// Compile error: cannot determine what to remove. Log, emit an
				// event, and retain the finalizer + requeue with backoff — this
				// does NOT orphan the CR; deletion retries once the compile
				// error is resolved (e.g. the cluster's MDS config is fixed).
				msg := "failed to compile role bindings for deletion: " + err.Error()
				log.FromContext(ctx).Error(err, "role binding compile failed during deletion")
				r.finalizeEvent(rb, corev1.EventTypeWarning, "DeletionFailed", msg)
				return ctrl.Result{}, errors.New(msg) // requeue: retain finalizer
			}
			// Co-ownership shield (see doc comment): subtract bindings still
			// desired by other live CRs; only the remainder is removed.
			clusterNS := r.ClusterNamespace
			if clusterNS == "" {
				clusterNS = rb.Namespace
			}
			// Serialize the finalizer's MDS cleanup against every other MDS
			// writer on this cluster: the shield's remaining-CR view build and
			// the RemoveRoleBinding loop it gates must be one atomic critical
			// section, or a concurrent reconcile could re-add / co-claim
			// bindings between the view read and the removal. Released
			// explicitly after the removal loop, before the finalizer-removal
			// Update; the deferred call covers the error returns in between.
			// No identity lock on this path (v0.37): every MDS mutation in
			// this controller runs under this same substrate lock, which
			// therefore already serializes same-identity rivals; adding an
			// identity lock here would also invert the identity → substrate
			// global order (locks.go).
			unlockRBAC := lockSubstrate(r.Locks, cluster, locking.SubstrateRBAC)
			defer unlockRBAC()
			// No len(compiled) > 0 guard here (unlike the ACL delete paths):
			// rbac.Compile provably never returns an empty set for a valid CR —
			// an empty spec.resources compiles to exactly one cluster-scoped
			// binding — so the view is always needed.
			view, verr := buildClusterRoleBindingView(ctx, r.Client, rb.Spec.ClusterRef.Name,
				clusterNS, r.ClusterNamespace == "", cluster.Spec.Authorization.MDS, cluster)
			if verr != nil {
				msg := "building cluster role-binding view for deletion (retry; not removing bindings blindly): " + verr.Error()
				log.FromContext(ctx).Error(verr, "cluster role-binding view build failed during deletion")
				r.finalizeEvent(rb, corev1.EventTypeWarning, "DeletionFailed", msg)
				return ctrl.Result{}, errors.New(msg) // requeue: retain finalizer
			}
			remaining := subtractProtectedRoleBindings(compiled, view.DesiredBindings)
			if shared := len(compiled) - len(remaining); shared > 0 {
				retained := retainedRoleBindings(compiled, view.DesiredBindings)
				r.finalizeEvent(rb, corev1.EventTypeNormal, "SharedRoleBindingsRetained",
					fmt.Sprintf("%d MDS role binding(s) still desired by other live resources were retained (%s)",
						shared, roleBindingCoOwnerSummary(retained, coOwnerNamesLimit)))
			}
			for _, b := range remaining {
				rb2 := mds.RoleBinding{
					Principal: b.Principal,
					Role:      b.Role,
					Scope: mds.Scope{
						Type:         b.Scope.Type,
						KafkaCluster: b.Scope.KafkaCluster,
						SubCluster:   b.Scope.SubCluster,
					},
				}
				if b.Resource != nil {
					rb2.Resource = &mds.ResourcePattern{
						Type:        b.Resource.Type,
						Name:        b.Resource.Name,
						PatternType: b.Resource.PatternType,
					}
				}
				if err := mdsClient.RemoveRoleBinding(ctx, rb2); err != nil {
					msg := "failed to remove MDS role binding: " + err.Error()
					log.FromContext(ctx).Error(err, "MDS role binding removal failed", "binding", b.FullKey())
					r.finalizeEvent(rb, corev1.EventTypeWarning, "DeletionFailed", msg)
					return ctrl.Result{}, errors.New(msg) // requeue: retain finalizer
				}
			}
			unlockRBAC() // substrate cleanup done; release before the finalizer-removal Update
			r.finalizeEvent(rb, corev1.EventTypeNormal, "Deleted", "managed MDS role bindings removed")
		}
	}
	// Orphan: leave MDS role bindings in place.

	return r.removeFinalizer(ctx, rb)
}

// removeFinalizer drops the finalizer and persists it, letting Kubernetes
// garbage-collect the object. No requeue. This is the single deletion-success
// exit (both the Orphan and Delete paths, plus force-removal, end here).
func (r *KafkaRoleBindingReconciler) removeFinalizer(ctx context.Context, rb *v1alpha1.KafkaRoleBinding) (ctrl.Result, error) {
	controllerutil.RemoveFinalizer(rb, FinalizerName)
	if err := r.Update(ctx, rb); err != nil {
		return ctrl.Result{}, err
	}
	forgetRoleBindingMetrics(rb.Namespace, rb.Name)
	return ctrl.Result{}, nil
}

// managedRoleBindingKeys tracks which KafkaRoleBindings have already been
// counted into the monedula_managed_rolebindings gauge so a re-reconcile of
// the same binding does not double-count. Package-scoped because the gauge is
// process-global (mirrors managedTopicKeys in kafkatopic_controller.go;
// review I12).
var managedRoleBindingKeys sync.Map

// observeManaged bumps the managed-rolebindings gauge the first time a given
// role binding is observed by this process. forgetRoleBindingMetrics undoes it
// on finalization so the gauge tracks CURRENTLY managed role bindings.
func (r *KafkaRoleBindingReconciler) observeManaged(rb *v1alpha1.KafkaRoleBinding) {
	key := rb.Namespace + "/" + rb.Name
	if _, loaded := managedRoleBindingKeys.LoadOrStore(key, struct{}{}); !loaded {
		operator.IncManagedRoleBindings(1)
	}
}

// forgetRoleBindingMetrics is the single per-CR metrics cleanup hook for a
// KafkaRoleBinding that is gone: it un-counts it from the managed-rolebindings
// gauge and drops its drift series so deleted CRs do not leak stale series.
// Called on the successful-finalization path (removeFinalizer) and, as a
// safety net, on the post-delete NotFound reconcile. LoadAndDelete makes the
// decrement idempotent.
func forgetRoleBindingMetrics(namespace, name string) {
	if _, loaded := managedRoleBindingKeys.LoadAndDelete(namespace + "/" + name); loaded {
		operator.IncManagedRoleBindings(-1)
	}
	operator.DeleteRoleBindingDrift(namespace, name)
}

// event emits a Kubernetes Event for obj on the reconcile path, a no-op when
// no Recorder is wired. See events.go for the action convention.
func (r *KafkaRoleBindingReconciler) event(obj *v1alpha1.KafkaRoleBinding, eventType, reason, msg string) {
	if r.Recorder == nil {
		return
	}
	r.Recorder.Eventf(obj, nil, eventType, reason, actionReconcile, "%s", msg)
}

// finalizeEvent is event for the deletion/finalizer path (actionFinalize).
func (r *KafkaRoleBindingReconciler) finalizeEvent(obj *v1alpha1.KafkaRoleBinding, eventType, reason, msg string) {
	if r.Recorder == nil {
		return
	}
	r.Recorder.Eventf(obj, nil, eventType, reason, actionFinalize, "%s", msg)
}

// mapSecretToRoleBindings enqueues the KafkaRoleBindings on any cluster (in the
// Secret's namespace) that references the changed credential/TLS Secret (§11.4).
// 2nd hop of the fan-out; list scope mirrors buildClusterACLView.
func (r *KafkaRoleBindingReconciler) mapSecretToRoleBindings(ctx context.Context, obj client.Object) []ctrlreconcile.Request {
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
	var list v1alpha1.KafkaRoleBindingList
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
// KafkaRoleBinding objects. watchEventFilter drops status-only updates so a
// reconcile does not re-enqueue itself; spec/annotation/lifecycle changes and
// the periodic RequeueAfter still trigger.
//
// Predicate scoped to the primary kind (not global WithEventFilter) so the
// Secret watch's generation-less data-change events are not dropped (§11.4).
// The KafkaCluster watch (review I2) recovers bindings promptly when their
// cluster CR appears or its spec changes — in particular, adding
// authorization.mds to the cluster immediately un-wedges bindings in
// MDSNotConfigured instead of leaving them to the resync fallback.
func (r *KafkaRoleBindingReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Recorder == nil {
		r.Recorder = mgr.GetEventRecorder("kafkarolebinding-controller")
	}
	// Insurance against silently running unlocked: nil Locks means every
	// substrate span in this controller is unserialized (locks.go). Expected
	// only in tests — manager.Run always injects the registry.
	if r.Locks == nil {
		mgr.GetLogger().Info("substrate locking disabled (nil Locks registry); expected only in tests",
			"controller", "kafkarolebinding")
	}
	// Same insurance for the duplicate-gate quorum recheck (duplicate.go): nil
	// APIReader silently degrades to cached-scan-only behavior.
	if r.APIReader == nil {
		mgr.GetLogger().Info("duplicate-gate quorum recheck disabled (nil APIReader); expected only in tests",
			"controller", "kafkarolebinding")
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.KafkaRoleBinding{}, builder.WithPredicates(watchEventFilter())).
		Watches(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(r.mapSecretToRoleBindings)).
		Watches(&v1alpha1.KafkaCluster{}, handler.EnqueueRequestsFromMapFunc(r.mapClusterToRoleBindings),
			builder.WithPredicates(clusterWatchPredicate())).
		WithOptions(reconcilerOptions(r.MaxConcurrentReconciles)).
		Complete(r)
}

// roleBindingErrorStatus builds an Error status with Ready False (reason) for a
// pre-engine failure (cluster lookup, client build, or view build). It seeds
// conditions from the existing status so LastTransitionTime is preserved across
// requeues.
func roleBindingErrorStatus(rb *v1alpha1.KafkaRoleBinding, reason, msg string) v1alpha1.KafkaRoleBindingStatus {
	now := metav1.Now()
	st := v1alpha1.KafkaRoleBindingStatus{
		ObservedGeneration: rb.Generation,
		Phase:              v1alpha1.PhaseError,
		LastCheckedTime:    &now,
	}
	if rb.Status != nil {
		st.Conditions = append([]metav1.Condition(nil), rb.Status.Conditions...)
	}
	meta.SetStatusCondition(&st.Conditions, metav1.Condition{
		Type:               v1alpha1.CondReady,
		Status:             metav1.ConditionFalse,
		Reason:             reason,
		Message:            msg,
		ObservedGeneration: rb.Generation,
	})
	return st
}
