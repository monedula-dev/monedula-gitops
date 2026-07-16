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
	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	"github.com/monedula-dev/monedula-gitops/internal/operator"
	"github.com/monedula-dev/monedula-gitops/internal/operator/locking"
	"github.com/monedula-dev/monedula-gitops/internal/operator/reconcile"
	operatorwebhook "github.com/monedula-dev/monedula-gitops/internal/operator/webhook"
	"github.com/monedula-dev/monedula-gitops/internal/quota"
)

// quotaRequeueAfter is the DEFAULT periodic resync interval, used when a
// reconciler's ResyncInterval field is zero. A healthy quota is re-reconciled
// on this cadence so drift introduced out-of-band (a manual quota change on the
// broker) is re-detected even without a spec change. See resync.go.
const quotaRequeueAfter = DefaultResyncInterval

// allQuotaKeys are the four Kafka client-quota value keys a KafkaQuota manages.
// On deletion the entity's quota is removed by deleting ALL four keys (spec
// §39.4) regardless of which the spec declared, so no managed value is orphaned.
var allQuotaKeys = []string{
	"producer_byte_rate",
	"consumer_byte_rate",
	"request_percentage",
	"controller_mutation_rate",
}

// KafkaQuotaReconciler reconciles a KafkaQuota. It is the client-quota analogue
// of the KafkaAccessPolicy controller: it owns external Kafka quota state, so it
// manages a finalizer. Unlike topics/policies, quota teardown is UNGATED (spec
// §39.4): there is no deletionPolicy and no allow-delete approval — the entity's
// managed quota is removed on CR deletion. A quota needs only the Kafka admin
// client (no Schema Registry).
type KafkaQuotaReconciler struct {
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
	// quota's own namespace is used (clusterRef is namespace-local by default).
	ClusterNamespace string
	// ResyncInterval overrides the periodic resync cadence (--resync-interval).
	// Zero uses DefaultResyncInterval (5m); see resync.go.
	ResyncInterval time.Duration
	// MaxConcurrentReconciles is passed to controller.Options in
	// SetupWithManager. Zero uses DefaultMaxConcurrentReconciles (1); see
	// resync.go and --max-concurrent-reconciles.
	MaxConcurrentReconciles int
	// Locks is the process-wide keyed lock registry — see locks.go. A quota
	// writes no cluster-wide substrate, so it takes only its per-identity
	// (KafkaCluster, "KafkaQuota", entity-key) lock, serializing the
	// gate → recheck → engine span and the deletion co-claimant-scan → cleanup
	// span against same-identity rivals. manager.Run always injects it; nil
	// (unit tests constructing the struct literal) acquires no locks.
	Locks *locking.Registry
	// APIReader is the manager's uncached quorum reader (mgr.GetAPIReader()),
	// used ONLY for the duplicate-identity gate's and deletion co-claimant
	// scan's contested-path rechecks (see duplicate.go). manager.Run always
	// injects it; nil (unit tests, the plain-client envtest harness) skips the
	// rechecks.
	APIReader client.Reader
}

// +kubebuilder:rbac:groups=gitops.monedula.dev,resources=kafkaquotas,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=gitops.monedula.dev,resources=kafkaquotas/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=gitops.monedula.dev,resources=kafkaquotas/finalizers,verbs=update
// +kubebuilder:rbac:groups=gitops.monedula.dev,resources=kafkaclusters,verbs=get;list;watch

// Reconcile drives a KafkaQuota toward its spec: it resolves the referenced
// cluster, builds the live Kafka client, manages the finalizer (removing the
// entity's managed quota on deletion), then delegates the in-sync reconcile to
// the engine and writes the resulting status. It requeues with backoff on a
// transient error and on the periodic resync cadence otherwise.
func (r *KafkaQuotaReconciler) Reconcile(ctx context.Context, req ctrl.Request) (res ctrl.Result, retErr error) {
	logger := log.FromContext(ctx)
	defer recordReconcile(controllerKafkaQuota, time.Now(), &retErr)

	var q v1alpha1.KafkaQuota
	if err := r.Get(ctx, req.NamespacedName, &q); err != nil {
		// NotFound: the object is fully gone (finalizer already removed). The
		// Delete watch event always passes the event filter, so this branch
		// reliably fires after a deletion and is the safety net for the
		// finalizer-path metrics cleanup (review I12) — e.g. after an operator
		// restart between finalize and delete. Mirrors KafkaTopicReconciler.
		if client.IgnoreNotFound(err) == nil {
			forgetQuotaMetrics(req.Namespace, req.Name)
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	r.observeManaged(&q)

	// Resolve the referenced cluster. clusterRef is namespace-local unless a
	// ClusterNamespace override is configured.
	clusterNS := r.ClusterNamespace
	if clusterNS == "" {
		clusterNS = q.Namespace
	}
	var cluster v1alpha1.KafkaCluster
	cerr := r.Get(ctx, types.NamespacedName{Namespace: clusterNS, Name: q.Spec.ClusterRef.Name}, &cluster)
	if cerr != nil {
		// Cluster not found (or unreadable): report Error + Ready False and
		// requeue. If the quota is being deleted, fall through to deletion handling
		// so an orphaned quota can still be finalized.
		if !q.DeletionTimestamp.IsZero() {
			return r.handleUnreachableDeletion(ctx, &q)
		}
		logger.Error(cerr, "resolving clusterRef", "cluster", q.Spec.ClusterRef.Name, "namespace", clusterNS)
		r.event(&q, corev1.EventTypeWarning, "ClusterNotFound", cerr.Error())
		if uerr := updateStatus(ctx, r.Client, req.NamespacedName, &q, func() {
			st := quotaErrorStatus(&q, "ClusterNotFound", cerr.Error())
			q.Status = &st
		}); uerr != nil {
			return ctrl.Result{}, errors.Join(cerr, uerr)
		}
		return ctrl.Result{}, cerr
	}

	// Per-identity lock (locks.go): everything below — the duplicate gate and
	// its quorum recheck, the engine's read-modify-write of the entity's
	// quota, and the deletion path's co-claimant scan → broker cleanup →
	// finalizer removal — must be atomic against same-identity rivals, so the
	// lock is taken here, right after cluster resolution (the entity key
	// resolves from the spec alone), and spans BOTH paths. A quota holds no
	// substrate lock, so the identity → acl → rbac global order is trivially
	// respected. Released explicitly right after the engine returns on the
	// normal path; the deferred call backstops the error returns and the
	// deletion path (where it releases only after finalizer removal).
	unlockIdentity := lockIdentity(r.Locks, &cluster, "KafkaQuota", operatorwebhook.ResolvedEntityKey(&q))
	defer unlockIdentity()

	// Duplicate-identity gate (§39.5; the webhook-off backstop, see
	// duplicate.go): if an OLDER live KafkaQuota claims the same (cluster,
	// entity) identity, this CR goes terminal (ValidationFailed/
	// DuplicateIdentity) instead of flapping last-writer-wins against the
	// winner. Guarded to the non-deletion path — a deleting loser must still
	// reach its finalizer below — and placed BEFORE the client build, so a
	// loser never even connects to the broker.
	if q.DeletionTimestamp.IsZero() {
		if res, done, err := r.duplicateIdentityGate(ctx, &q); done {
			return res, err
		}
	}

	// Build the live clients. On the deletion path a build failure is handled
	// specially (finalizer block / force-removal); on the normal path it is a
	// transient error. A quota uses only the Kafka admin client.
	k, _, cleanup, berr := r.Clients.For(ctx, &cluster)
	if berr != nil {
		if !q.DeletionTimestamp.IsZero() {
			return r.handleUnreachableDeletion(ctx, &q)
		}
		logger.Error(berr, "building cluster clients")
		r.event(&q, corev1.EventTypeWarning, "ClientsBuildFailed", berr.Error())
		if uerr := updateStatus(ctx, r.Client, req.NamespacedName, &q, func() {
			st := quotaErrorStatus(&q, "ClientsBuildFailed", berr.Error())
			q.Status = &st
		}); uerr != nil {
			return ctrl.Result{}, errors.Join(berr, uerr)
		}
		return ctrl.Result{}, berr
	}
	defer cleanup()

	// Deletion path: clients built, so cleanup can run.
	if !q.DeletionTimestamp.IsZero() {
		return r.handleDeletionWithClient(ctx, &q, k)
	}

	// Not being deleted: ensure the finalizer is present before mutating Kafka,
	// so an interrupted reconcile still leaves cleanup possible.
	if !controllerutil.ContainsFinalizer(&q, FinalizerName) {
		controllerutil.AddFinalizer(&q, FinalizerName)
		if err := r.Update(ctx, &q); err != nil {
			return ctrl.Result{}, err
		}
	}

	// In-sync reconcile via the engine. The reconcile — including its live-state
	// reads and Kafka mutations — runs exactly ONCE, before the status write: a
	// 409 Conflict on the write must retry only the write, never re-mutate Kafka
	// (review I9).
	st, rerr := reconcile.ReconcileQuota(ctx, &q, &cluster, k)
	// Broker mutations are done: release the identity lock before the status
	// write below (which may retry on conflict) so a same-identity rival is
	// not held up by API-server latency.
	unlockIdentity()
	// Drift gauge semantic (review I12, mirrors KafkaTopicReconciler): set it
	// from the freshly ENGINE-computed status whenever one exists, including
	// transient-error outcomes, so a quota that flips to an error does not
	// keep its previous drift value.
	operator.SetQuotaDrift(q.Namespace, q.Name, quotaDriftDetected(&st))
	if uerr := updateStatus(ctx, r.Client, req.NamespacedName, &q, func() {
		q.Status = &st
	}); uerr != nil {
		return ctrl.Result{}, uerr
	}

	if rerr != nil {
		r.event(&q, corev1.EventTypeWarning, "ReconcileError", rerr.Error())
		return ctrl.Result{}, rerr // transient: requeue with backoff
	}
	r.event(&q, corev1.EventTypeNormal, "Reconciled", "quota reconciled")
	return ctrl.Result{RequeueAfter: resyncInterval(r.ResyncInterval)}, nil
}

// handleUnreachableDeletion is the deletion path taken when no live Kafka client
// is available (the cluster CR is missing or the clients failed to build). It
// blocks finalizer removal unless force-removal is requested, in which case it
// removes the finalizer and lets Kubernetes garbage-collect the object.
func (r *KafkaQuotaReconciler) handleUnreachableDeletion(ctx context.Context, q *v1alpha1.KafkaQuota) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(q, FinalizerName) {
		return ctrl.Result{}, nil // already finalized; nothing to do
	}

	if q.Annotations[AnnotationForceFinalizerRemoval] == "true" {
		r.finalizeEvent(q, corev1.EventTypeWarning, "ForceFinalizerRemoval",
			"removing finalizer without cluster-side cleanup (cluster unreachable)")
		return r.removeFinalizer(ctx, q)
	}

	// Cannot clean up and not forced: retain the finalizer and requeue so the
	// quota finalizes once the cluster becomes reachable again.
	msg := "cluster unreachable; cannot run deletion cleanup. Make the cluster reachable, " +
		"or set annotation " + AnnotationForceFinalizerRemoval + "=true to force removal"
	r.finalizeEvent(q, corev1.EventTypeWarning, "DeletionBlocked", msg)
	return ctrl.Result{}, errors.New(msg) // requeue with backoff
}

// handleDeletionWithClient removes the entity's managed quota using the live
// Kafka client, then removes the finalizer. Quota teardown is UNGATED (spec
// §39.4): there is no deletionPolicy and no allow-delete approval. Mirroring the
// topic subject-deletion approach, a removal error emits a Warning event but
// does NOT block finalizer removal — a wedged namespace is worse than a possibly
// orphaned quota value, and the next reconcile of a re-created CR would re-set it.
//
// EXCEPT when another live KafkaQuota still claims the same (cluster, entity)
// identity (a duplicate-identity pair, either side being deleted): then the
// broker cleanup is skipped and only the finalizer is removed, orphaning the
// quota to the surviving claimant — otherwise deleting the losing duplicate
// (the natural remediation) would strip the entity's quota out from under the
// winner until its next resync. See findLiveQuotaCoClaimant and duplicate.go's
// Deletion path doc.
func (r *KafkaQuotaReconciler) handleDeletionWithClient(ctx context.Context, q *v1alpha1.KafkaQuota, k kafka.AdminClient) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(q, FinalizerName) {
		return ctrl.Result{}, nil
	}

	co, cerr := findLiveQuotaCoClaimant(ctx, r.Client, q, r.ClusterNamespace == "")
	// Quorum recheck (duplicate.go): only when the cached scan found NO live
	// co-claimant — the branch about to DESTROY broker state — re-scan through
	// the uncached APIReader; a co-claimant the cache missed flips the outcome
	// to the fail-safe skip (leak, never destroy a survivor's state). Finding
	// a co-claimant needs no recheck: skipping cleanup is already fail-safe.
	if cerr == nil && co == nil && r.APIReader != nil {
		co, cerr = findLiveQuotaCoClaimant(ctx, r.APIReader, q, r.ClusterNamespace == "")
	}
	if cerr != nil {
		// Transient: requeue with backoff. Guessing here could strip the quota
		// out from under a surviving claimant, so unlike the broker deletion
		// below the scan is NOT best-effort.
		r.finalizeEvent(q, corev1.EventTypeWarning, reasonDuplicateCheckFailed, cerr.Error())
		return ctrl.Result{}, cerr
	}
	if co != nil {
		key := quota.Compile(q).Entity.Key()
		msg := fmt.Sprintf("managed quota for entity %q left in place: live KafkaQuota %s/%s still claims it and keeps managing it",
			key, co.Namespace, co.Name)
		log.FromContext(ctx).Info("skipping quota deletion: identity has a surviving claimant",
			"entity", key, "claimant", co.Namespace+"/"+co.Name)
		r.finalizeEvent(q, corev1.EventTypeNormal, reasonOrphanedToCoClaimant, msg)
		return r.removeFinalizer(ctx, q)
	}

	// Compile the entity and delete ALL four quota keys for it (spec §39.4).
	compiled := quota.Compile(q)
	entity := make([]kafka.QuotaEntityComponent, 0, len(compiled.Entity))
	for _, c := range compiled.Entity {
		entity = append(entity, kafka.QuotaEntityComponent{Type: c.Type, Name: c.Name})
	}
	if len(entity) > 0 {
		if err := k.DeleteQuota(ctx, entity, allQuotaKeys); err != nil {
			// Best-effort: warn but still remove the finalizer (mirror topic
			// subject-deletion).
			msg := "failed to delete managed quota: " + err.Error()
			log.FromContext(ctx).Error(err, "quota deletion failed", "entity", quota.Entity(compiled.Entity).Key())
			r.finalizeEvent(q, corev1.EventTypeWarning, "QuotaDeletionFailed", msg)
		} else {
			r.finalizeEvent(q, corev1.EventTypeNormal, "Deleted", "managed quota deleted")
		}
	}

	return r.removeFinalizer(ctx, q)
}

// removeFinalizer drops the finalizer and persists it, letting Kubernetes
// garbage-collect the object. No requeue. This is the single deletion-success
// exit (the cleanup path plus force-removal end here).
func (r *KafkaQuotaReconciler) removeFinalizer(ctx context.Context, q *v1alpha1.KafkaQuota) (ctrl.Result, error) {
	controllerutil.RemoveFinalizer(q, FinalizerName)
	if err := r.Update(ctx, q); err != nil {
		return ctrl.Result{}, err
	}
	forgetQuotaMetrics(q.Namespace, q.Name)
	return ctrl.Result{}, nil
}

// managedQuotaKeys tracks which KafkaQuotas have already been counted into the
// monedula_managed_quotas gauge so a re-reconcile of the same quota does not
// double-count. Package-scoped because the gauge is process-global (mirrors
// managedTopicKeys in kafkatopic_controller.go; review I12).
var managedQuotaKeys sync.Map

// observeManaged bumps the managed-quotas gauge the first time a given quota
// is observed by this process. forgetQuotaMetrics undoes it on finalization so
// the gauge tracks CURRENTLY managed quotas.
func (r *KafkaQuotaReconciler) observeManaged(q *v1alpha1.KafkaQuota) {
	key := q.Namespace + "/" + q.Name
	if _, loaded := managedQuotaKeys.LoadOrStore(key, struct{}{}); !loaded {
		operator.IncManagedQuotas(1)
	}
}

// forgetQuotaMetrics is the single per-CR metrics cleanup hook for a KafkaQuota
// that is gone: it un-counts the quota from the managed-quotas gauge and drops
// its drift series so deleted CRs do not leak stale series. Called on the
// successful-finalization path (removeFinalizer) and, as a safety net, on the
// post-delete NotFound reconcile. LoadAndDelete makes the decrement idempotent.
func forgetQuotaMetrics(namespace, name string) {
	if _, loaded := managedQuotaKeys.LoadAndDelete(namespace + "/" + name); loaded {
		operator.IncManagedQuotas(-1)
	}
	operator.DeleteQuotaDrift(namespace, name)
}

// quotaDriftDetected reports whether a quota status records detected drift.
func quotaDriftDetected(st *v1alpha1.KafkaQuotaStatus) bool {
	return st != nil && st.Drift != nil && st.Drift.Detected
}

// event emits a Kubernetes Event for obj on the reconcile path, a no-op when
// no Recorder is wired. See events.go for the action convention.
func (r *KafkaQuotaReconciler) event(obj *v1alpha1.KafkaQuota, eventType, reason, msg string) {
	if r.Recorder == nil {
		return
	}
	r.Recorder.Eventf(obj, nil, eventType, reason, actionReconcile, "%s", msg)
}

// finalizeEvent is event for the deletion/finalizer path (actionFinalize).
func (r *KafkaQuotaReconciler) finalizeEvent(obj *v1alpha1.KafkaQuota, eventType, reason, msg string) {
	if r.Recorder == nil {
		return
	}
	r.Recorder.Eventf(obj, nil, eventType, reason, actionFinalize, "%s", msg)
}

// mapSecretToQuotas enqueues the KafkaQuotas on any cluster (in the Secret's
// namespace) that references the changed credential/TLS Secret (§11.4).
// 2nd hop of the fan-out; list scope mirrors buildClusterACLView.
func (r *KafkaQuotaReconciler) mapSecretToQuotas(ctx context.Context, obj client.Object) []ctrlreconcile.Request {
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
	var list v1alpha1.KafkaQuotaList
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
// KafkaQuota objects. watchEventFilter drops status-only updates so a reconcile
// does not re-enqueue itself; spec/annotation/lifecycle changes and the periodic
// RequeueAfter still trigger.
//
// Predicate scoped to the primary kind (not global WithEventFilter) so the
// Secret watch's generation-less data-change events are not dropped (§11.4).
// The KafkaCluster watch (review I2) recovers quotas promptly when their
// cluster CR appears or its spec is fixed, instead of waiting out error backoff.
func (r *KafkaQuotaReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Recorder == nil {
		r.Recorder = mgr.GetEventRecorder("kafkaquota-controller")
	}
	// Insurance against silently running unlocked / unrechecked: nil Locks
	// skips the per-identity serialization, nil APIReader the duplicate-gate
	// quorum recheck (locks.go, duplicate.go). Expected only in tests —
	// manager.Run always injects both.
	if r.Locks == nil {
		mgr.GetLogger().Info("identity locking disabled (nil Locks registry); expected only in tests",
			"controller", "kafkaquota")
	}
	if r.APIReader == nil {
		mgr.GetLogger().Info("duplicate-gate quorum recheck disabled (nil APIReader); expected only in tests",
			"controller", "kafkaquota")
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.KafkaQuota{}, builder.WithPredicates(watchEventFilter())).
		Watches(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(r.mapSecretToQuotas)).
		Watches(&v1alpha1.KafkaCluster{}, handler.EnqueueRequestsFromMapFunc(r.mapClusterToQuotas),
			builder.WithPredicates(clusterWatchPredicate())).
		WithOptions(reconcilerOptions(r.MaxConcurrentReconciles)).
		Complete(r)
}

// quotaErrorStatus builds an Error status with Ready False (reason) for a
// pre-engine failure (cluster lookup or client build). It seeds conditions from
// the existing status so LastTransitionTime is preserved across requeues.
func quotaErrorStatus(q *v1alpha1.KafkaQuota, reason, msg string) v1alpha1.KafkaQuotaStatus {
	now := metav1.Now()
	st := v1alpha1.KafkaQuotaStatus{
		ObservedGeneration: q.Generation,
		Phase:              v1alpha1.PhaseError,
		LastCheckedTime:    &now,
	}
	if q.Status != nil {
		st.Conditions = append([]metav1.Condition(nil), q.Status.Conditions...)
	}
	meta.SetStatusCondition(&st.Conditions, metav1.Condition{
		Type:               v1alpha1.CondReady,
		Status:             metav1.ConditionFalse,
		Reason:             reason,
		Message:            msg,
		ObservedGeneration: q.Generation,
	})
	return st
}
