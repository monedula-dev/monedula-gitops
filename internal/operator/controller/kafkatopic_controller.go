package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
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

	"github.com/monedula-dev/monedula-gitops/internal/access"
	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/defaulting"
	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	"github.com/monedula-dev/monedula-gitops/internal/mds"
	"github.com/monedula-dev/monedula-gitops/internal/operator"
	"github.com/monedula-dev/monedula-gitops/internal/operator/locking"
	"github.com/monedula-dev/monedula-gitops/internal/operator/reconcile"
	operatorwebhook "github.com/monedula-dev/monedula-gitops/internal/operator/webhook"
	"github.com/monedula-dev/monedula-gitops/internal/schemaregistry"
	"github.com/monedula-dev/monedula-gitops/internal/secrets"
)

// topicRequeueAfter is the DEFAULT periodic resync interval, used when a
// reconciler's ResyncInterval field is zero. A healthy topic is re-reconciled
// on this cadence so drift introduced out-of-band (a manual change on the
// broker) is re-detected even without a spec change. See resync.go.
const topicRequeueAfter = DefaultResyncInterval

// FinalizerName is the finalizer placed on managed resources so the controller
// can run cluster-side cleanup (per the resource's deletionPolicy) before
// Kubernetes garbage-collects the object (spec §22).
const FinalizerName = "gitops.monedula.dev/finalizer"

// AnnotationForceFinalizerRemoval, when set to "true", removes the finalizer on
// a being-deleted resource EVEN IF cluster-side cleanup could not run (the
// cluster is unreachable). It is the operator escape hatch for deleting a
// KafkaTopic whose backing cluster is gone; otherwise the finalizer blocks
// deletion until the cluster is reachable. A Warning event is emitted when used.
const AnnotationForceFinalizerRemoval = "gitops.monedula.dev/force-finalizer-removal"

// deletionPolicyDelete is the spec value that opts a topic into cluster-side
// deletion of its managed topic + ACLs on resource removal. The default
// ("Orphan") leaves the Kafka objects in place.
const deletionPolicyDelete = "Delete"

// KafkaTopicReconciler reconciles a KafkaTopic. Unlike the cluster reconciler it
// owns external Kafka state, so it manages a finalizer and honors the topic's
// deletionPolicy on removal.
type KafkaTopicReconciler struct {
	client.Client
	// Scheme is held for manager and event-recorder wiring.
	Scheme *runtime.Scheme
	// Clients builds the live Kafka/Schema-Registry clients for a cluster.
	Clients ClientFactory
	// Recorder emits one Kubernetes Event (events.k8s.io API) per reconcile
	// outcome. Set by SetupWithManager; may be nil in unit tests (events are
	// then skipped).
	Recorder events.EventRecorder
	// ClusterNamespace is where KafkaCluster CRs are looked up. When empty, the
	// topic's own namespace is used (clusterRef is namespace-local by default).
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

// +kubebuilder:rbac:groups=gitops.monedula.dev,resources=kafkatopics,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=gitops.monedula.dev,resources=kafkatopics/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=gitops.monedula.dev,resources=kafkatopics/finalizers,verbs=update
// +kubebuilder:rbac:groups=gitops.monedula.dev,resources=kafkaclusters,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch

// Reconcile drives a KafkaTopic toward its spec: it resolves the referenced
// cluster, builds the live clients, manages the finalizer (running deletionPolicy
// cleanup on removal), then delegates the in-sync reconcile to the engine and
// writes the resulting status. It requeues with backoff on a transient error and
// on the periodic resync cadence otherwise.
func (r *KafkaTopicReconciler) Reconcile(ctx context.Context, req ctrl.Request) (res ctrl.Result, retErr error) {
	logger := log.FromContext(ctx)
	defer recordReconcile(controllerKafkaTopic, time.Now(), &retErr)

	var topic v1alpha1.KafkaTopic
	if err := r.Get(ctx, req.NamespacedName, &topic); err != nil {
		// NotFound: the object is fully gone (finalizer already removed). The
		// Delete watch event always passes the event filter (predicates only
		// constrain Updates), so this branch reliably fires after a deletion and
		// is the safety net for the finalizer-path metrics cleanup (review I12) —
		// e.g. after an operator restart between finalize and delete.
		if client.IgnoreNotFound(err) == nil {
			forgetTopicMetrics(req.Namespace, req.Name)
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	r.observeManaged(&topic)

	// Resolve the referenced cluster. clusterRef is namespace-local unless a
	// ClusterNamespace override is configured.
	clusterNS := r.ClusterNamespace
	if clusterNS == "" {
		clusterNS = topic.Namespace
	}
	var cluster v1alpha1.KafkaCluster
	cerr := r.Get(ctx, types.NamespacedName{Namespace: clusterNS, Name: topic.Spec.ClusterRef.Name}, &cluster)
	if cerr != nil {
		// Cluster not found (or unreadable): report Error + Ready False and
		// requeue. We requeue so the topic recovers once the cluster CR appears.
		// NOTE: if the topic is being deleted, fall through to deletion handling
		// so an orphaned topic can still be finalized.
		if !topic.DeletionTimestamp.IsZero() {
			return r.handleUnreachableDeletion(ctx, &topic)
		}
		logger.Error(cerr, "resolving clusterRef", "cluster", topic.Spec.ClusterRef.Name, "namespace", clusterNS)
		r.event(&topic, corev1.EventTypeWarning, "ClusterNotFound", cerr.Error())
		if uerr := updateStatus(ctx, r.Client, req.NamespacedName, &topic, func() {
			st := topicErrorStatus(&topic, "ClusterNotFound", cerr.Error())
			topic.Status = &st
		}); uerr != nil {
			return ctrl.Result{}, errors.Join(cerr, uerr)
		}
		return ctrl.Result{}, cerr
	}

	// Duplicate-identity gate (§5.3; the webhook-off backstop, see duplicate.go):
	// if an OLDER live KafkaTopic claims the same (cluster, topicName) identity,
	// this CR goes terminal (ValidationFailed/DuplicateIdentity) instead of
	// flapping last-writer-wins against the winner. Guarded to the non-deletion
	// path — a deleting loser must still reach its finalizer below — and placed
	// BEFORE the client build, so a loser never even connects to the broker.
	//
	// The gate → quorum-recheck → engine-mutation span runs under this topic's
	// per-identity lock (locks.go), taken BEFORE the gate — and before the
	// substrate locks below, per the identity → acl → rbac global order — so
	// two same-identity claimants can never interleave their gate checks and
	// broker mutations. Released explicitly right after the engine returns
	// (with the substrate locks); the deferred call backstops every error
	// return in between. The deletion path takes NO identity lock: its only
	// contested broker state (the ACL cleanup) is substrate-shared and already
	// serialized by the ACL substrate lock in deleteTopicState.
	unlockIdentity := func() {}
	if topic.DeletionTimestamp.IsZero() {
		unlockIdentity = lockIdentity(r.Locks, &cluster, "KafkaTopic", operatorwebhook.ResolvedTopicName(&topic))
		defer unlockIdentity()
		if res, done, err := r.duplicateIdentityGate(ctx, &topic); done {
			return res, err
		}
	}

	// Build the live clients. On the deletion path a build failure is handled
	// specially (finalizer block / force-removal); on the normal path it is a
	// transient error.
	k, sr, cleanup, berr := r.Clients.For(ctx, &cluster)
	if berr != nil {
		if !topic.DeletionTimestamp.IsZero() {
			return r.handleUnreachableDeletion(ctx, &topic)
		}
		logger.Error(berr, "building cluster clients")
		r.event(&topic, corev1.EventTypeWarning, "ClientsBuildFailed", berr.Error())
		if uerr := updateStatus(ctx, r.Client, req.NamespacedName, &topic, func() {
			st := topicErrorStatus(&topic, "ClientsBuildFailed", berr.Error())
			topic.Status = &st
		}); uerr != nil {
			return ctrl.Result{}, errors.Join(berr, uerr)
		}
		return ctrl.Result{}, berr
	}
	defer cleanup()

	// Deletion path: clients built, so cleanup can run per deletionPolicy.
	if !topic.DeletionTimestamp.IsZero() {
		resolver := &operator.K8sResolver{Client: r.Client, Namespace: topic.Namespace, Ctx: ctx}
		return r.handleDeletionWithClient(ctx, &topic, k, sr, resolver, &cluster, clusterNS)
	}

	// Not being deleted: ensure the finalizer is present before mutating Kafka,
	// so an interrupted reconcile still leaves cleanup possible.
	if !controllerutil.ContainsFinalizer(&topic, FinalizerName) {
		controllerutil.AddFinalizer(&topic, FinalizerName)
		if err := r.Update(ctx, &topic); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Serialize this reconcile's substrate read-compute-write span against
	// every other writer on the same cluster's substrates (locks.go): the
	// sibling-CR view builds below, the engine's live broker/MDS reads, and
	// the executor apply (including prunes) must be one atomic critical
	// section — a lock taken after the views are built would guard a stale
	// union. Which substrates are held follows the cluster's accessBackends
	// (both, in acl→rbac order, when rbac is enabled — see
	// lockTopicSubstrates). Released explicitly right after the engine
	// returns, BEFORE the status write and its conflict retries (status
	// writes are per-object safe); the deferred call covers the error
	// returns in between and is a no-op the second time.
	unlockSubstrates := lockTopicSubstrates(r.Locks, &cluster)
	defer unlockSubstrates()

	// Aggregate the cluster-wide desired ACL set + scope across every resource
	// referencing this cluster (spec §20.1) so the engine computes prune
	// candidates against the union, not just this topic's own ACLs — the
	// §10.4 overlapping-owners flapping fix. A list failure is transient.
	view, verr := buildClusterACLView(ctx, r.Client, topic.Spec.ClusterRef.Name,
		clusterNS, r.ClusterNamespace == "", cluster.Spec.Defaults, &cluster)
	if verr != nil {
		// No substrate mutation has happened: release the locks BEFORE the
		// status write below so its conflict retries (RetryOnConflict) cannot
		// hold up other writers on this cluster — this branch fires exactly
		// when the API server is already struggling.
		unlockSubstrates()
		logger.Error(verr, "building cluster ACL view")
		r.event(&topic, corev1.EventTypeWarning, "ACLViewFailed", verr.Error())
		if uerr := updateStatus(ctx, r.Client, req.NamespacedName, &topic, func() {
			st := topicErrorStatus(&topic, "ACLViewFailed", verr.Error())
			topic.Status = &st
		}); uerr != nil {
			return ctrl.Result{}, errors.Join(verr, uerr)
		}
		return ctrl.Result{}, verr
	}

	// Build MDS client + cluster role-binding view for topics on rbac-backed
	// clusters (spec §40). Both are nil for non-rbac clusters and passed to the
	// engine unchanged — the engine is a no-op on the rbac path when nil.
	var mdsClient mds.Client
	var rbView *reconcile.ClusterRoleBindingView
	if v1alpha1.HasAccessBackend(&cluster, "rbac") {
		mc, merr := r.Clients.MDSFor(ctx, &cluster)
		if merr != nil {
			// No substrate mutation yet: release before the status write (see
			// the ACL-view failure branch above).
			unlockSubstrates()
			logger.Error(merr, "building MDS client for topic role bindings")
			r.event(&topic, corev1.EventTypeWarning, "MDSClientFailed", merr.Error())
			if uerr := updateStatus(ctx, r.Client, req.NamespacedName, &topic, func() {
				st := topicErrorStatus(&topic, "MDSClientFailed", merr.Error())
				topic.Status = &st
			}); uerr != nil {
				return ctrl.Result{}, errors.Join(merr, uerr)
			}
			return ctrl.Result{}, merr
		}
		mdsClient = mc
		var mdsCfg *v1alpha1.MDSConfig
		if cluster.Spec.Authorization != nil {
			mdsCfg = cluster.Spec.Authorization.MDS
		}
		rbv, rverr := buildClusterRoleBindingView(ctx, r.Client, topic.Spec.ClusterRef.Name,
			clusterNS, r.ClusterNamespace == "", mdsCfg, &cluster)
		if rverr != nil {
			// No substrate mutation yet: release before the status write (see
			// the ACL-view failure branch above).
			unlockSubstrates()
			logger.Error(rverr, "building cluster role-binding view")
			r.event(&topic, corev1.EventTypeWarning, "RoleBindingViewFailed", rverr.Error())
			if uerr := updateStatus(ctx, r.Client, req.NamespacedName, &topic, func() {
				st := topicErrorStatus(&topic, "RoleBindingViewFailed", rverr.Error())
				topic.Status = &st
			}); uerr != nil {
				return ctrl.Result{}, errors.Join(rverr, uerr)
			}
			return ctrl.Result{}, rverr
		}
		rbView = rbv
	}

	// In-sync reconcile via the engine. The schema resolver is scoped to the
	// topic's namespace (secret refs are namespace-local). The reconcile —
	// including its live-state reads and Kafka mutations — runs exactly ONCE,
	// before the status write: a 409 Conflict on the write must retry only the
	// write, never re-mutate Kafka (review I9). Condition seeding (the
	// LastTransitionTime preservation) reads the status of the object fetched at
	// the top of this Reconcile, as it always has.
	resolver := &operator.K8sResolver{Client: r.Client, Namespace: topic.Namespace, Ctx: ctx}
	st, rerr := reconcile.ReconcileTopic(ctx, &topic, &cluster, k, sr, resolver, view, mdsClient, rbView)
	// Substrate mutations are done: release the locks before the status write
	// below (which may retry on conflict) so other writers on this cluster
	// are not held up by API-server latency. Reverse acquisition order:
	// substrates first, then the identity lock.
	unlockSubstrates()
	unlockIdentity()
	// Self-lockout guard (spec §30.3): on Enforce reconciles, warn when the
	// topic's desired ACLs omit the operator's own connecting principal. The
	// engine has already defaulted the topic in place, so the compiled set and
	// mode are the effective ones.
	if topic.Spec.Reconciliation != nil {
		desiredACLs, _ := access.BuildDesiredSet(access.CompileTopic(&topic))
		warnSelfLockout(ctx, r.Client, r.Recorder, &topic, &cluster, topic.Spec.Reconciliation.Mode, desiredACLs)
	}
	// Drift gauge semantic (review I12): set it from the freshly ENGINE-computed
	// status whenever one exists — on success, terminal outcomes AND transient
	// errors (the engine always populates st, drift included) — so a topic that
	// flips to an error does not keep its previous drift value. Only pre-engine
	// failures (cluster lookup / client build / ACL view above), where no drift
	// was computed, leave the gauge unchanged.
	operator.SetTopicDrift(topic.Namespace, topic.Name, driftDetected(&st))
	r.setSchemaSourceUnwatchedCondition(ctx, &topic, &st)
	if uerr := updateStatus(ctx, r.Client, req.NamespacedName, &topic, func() {
		topic.Status = &st
	}); uerr != nil {
		return ctrl.Result{}, uerr
	}

	if rerr != nil {
		r.event(&topic, corev1.EventTypeWarning, "ReconcileError", rerr.Error())
		return ctrl.Result{}, rerr // transient: requeue with backoff
	}
	r.event(&topic, corev1.EventTypeNormal, "Reconciled", "topic reconciled")
	return ctrl.Result{RequeueAfter: resyncInterval(r.ResyncInterval)}, nil
}

// managedTopicKeys tracks which KafkaTopics have already been counted into the
// monedula_managed_topics gauge so a re-reconcile of the same topic does not
// double-count. It is package-scoped because the gauge is process-global.
var managedTopicKeys sync.Map

// observeManaged bumps the managed-topics gauge the first time a given topic is
// observed by this process. forgetTopicMetrics undoes it on finalization so the
// gauge tracks CURRENTLY managed topics (review I12).
func (r *KafkaTopicReconciler) observeManaged(topic *v1alpha1.KafkaTopic) {
	key := topic.Namespace + "/" + topic.Name
	if _, loaded := managedTopicKeys.LoadOrStore(key, struct{}{}); !loaded {
		operator.IncManagedTopics(1)
	}
}

// forgetTopicMetrics is the single per-CR metrics cleanup hook for a KafkaTopic
// that is gone: it un-counts the topic from the managed-topics gauge and drops
// its drift series so deleted CRs do not leak stale series (review I12). It is
// called on the successful-finalization path (removeFinalizer) and, as a safety
// net, on the post-delete NotFound reconcile. LoadAndDelete makes the decrement
// idempotent, so the two call sites can never double-decrement.
func forgetTopicMetrics(namespace, name string) {
	if _, loaded := managedTopicKeys.LoadAndDelete(namespace + "/" + name); loaded {
		operator.IncManagedTopics(-1)
	}
	operator.DeleteTopicDrift(namespace, name)
}

// driftDetected reports whether a topic status records detected drift.
func driftDetected(st *v1alpha1.KafkaTopicStatus) bool {
	return st != nil && st.Drift != nil && st.Drift.Detected
}

// handleUnreachableDeletion is the deletion path taken when no live Kafka client
// is available (the cluster CR is missing or the clients failed to build). It
// blocks finalizer removal unless force-removal is requested, in which case it
// removes the finalizer and lets Kubernetes garbage-collect the object.
func (r *KafkaTopicReconciler) handleUnreachableDeletion(ctx context.Context, topic *v1alpha1.KafkaTopic) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(topic, FinalizerName) {
		return ctrl.Result{}, nil // already finalized; nothing to do
	}

	if topic.Annotations[AnnotationForceFinalizerRemoval] == "true" {
		r.finalizeEvent(topic, corev1.EventTypeWarning, "ForceFinalizerRemoval",
			"removing finalizer without cluster-side cleanup (cluster unreachable)")
		return r.removeFinalizer(ctx, topic)
	}

	// Cannot clean up and not forced: retain the finalizer and requeue so the
	// topic finalizes once the cluster becomes reachable again.
	msg := "cluster unreachable; cannot run deletion cleanup. Make the cluster reachable, " +
		"or set annotation " + AnnotationForceFinalizerRemoval + "=true to force removal"
	r.finalizeEvent(topic, corev1.EventTypeWarning, "DeletionBlocked", msg)
	return ctrl.Result{}, errors.New(msg) // requeue with backoff
}

// effectiveDeletionPolicy resolves the deletion policy for a topic at
// finalization time, applying the same precedence as [defaulting.Topic]:
//
//  1. explicit spec.deletionPolicy wins
//  2. cluster-level defaults.topicDeletionPolicy is the fallback
//  3. "Orphan" (non-destructive) is the final default
//
// Note: this does NOT mutate the topic — the cluster default must not be
// persisted back to the API server (spec §4.7).
func effectiveDeletionPolicy(topic *v1alpha1.KafkaTopic, clusterDefaults *v1alpha1.ClusterDefaults) string {
	if topic.Spec.DeletionPolicy != "" {
		return topic.Spec.DeletionPolicy
	}
	if clusterDefaults != nil && clusterDefaults.TopicDeletionPolicy != "" {
		return clusterDefaults.TopicDeletionPolicy
	}
	return "Orphan"
}

// handleDeletionWithClient runs cluster-side cleanup per the topic's
// deletionPolicy using the live Kafka client, then removes the finalizer.
// cluster is the resolved KafkaCluster (its spec.defaults resolve the effective
// policy when the topic's own spec.deletionPolicy is empty, spec §4.7);
// clusterNS is the namespace the clusterRef resolved in (needed to scope the
// cluster ACL view on the Delete path).
//
//	Orphan (default): leave the Kafka topic + ACLs in place.
//	Delete: delete the topic's managed ACLs, the topic itself, and the topic's
//	        managed Schema Registry subjects (spec §12), but ONLY when approved
//	        via the gitops.monedula.dev/allow-delete=true annotation.
//	        Without approval the finalizer is retained (a Warning is emitted) so
//	        the deletion is gated behind an explicit operator acknowledgement.
//
// sr may be nil (no Schema Registry configured for the cluster); in that case
// subject deletion is skipped without error. r is the secrets.Resolver used to
// read schema bodies for subject-name computation.
func (r *KafkaTopicReconciler) handleDeletionWithClient(ctx context.Context, topic *v1alpha1.KafkaTopic, k kafka.AdminClient, sr schemaregistry.Client, resolver secrets.Resolver, cluster *v1alpha1.KafkaCluster, clusterNS string) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(topic, FinalizerName) {
		return ctrl.Result{}, nil
	}

	policy := effectiveDeletionPolicy(topic, cluster.Spec.Defaults)
	if policy == deletionPolicyDelete {
		if topic.Annotations[reconcile.AnnotationAllowDelete] != "true" {
			// Destructive deletion is gated: retain the finalizer until approved.
			msg := "deletionPolicy=Delete requires approval; set annotation " +
				reconcile.AnnotationAllowDelete + "=true to delete the Kafka topic + ACLs"
			r.finalizeEvent(topic, corev1.EventTypeWarning, "DeleteNotApproved", msg)
			return ctrl.Result{RequeueAfter: resyncInterval(r.ResyncInterval)}, nil
		}
		if err := r.deleteTopicState(ctx, topic, k, sr, resolver, cluster, clusterNS); err != nil {
			r.finalizeEvent(topic, corev1.EventTypeWarning, "DeleteFailed", err.Error())
			return ctrl.Result{}, err // transient: requeue, finalizer retained
		}
		r.finalizeEvent(topic, corev1.EventTypeNormal, "Deleted", "topic, managed ACLs, and managed schema subjects deleted")
	}

	return r.removeFinalizer(ctx, topic)
}

// deleteTopicState deletes the topic's managed ACLs (its access scope), the
// topic itself, and — when the topic declares schema CONTENT (spec §12) — the
// managed Schema Registry subjects. ACLs are deleted first so a partial failure
// leaves the topic present (and thus re-discoverable) rather than orphaning
// ACLs. Subject deletion runs AFTER DeleteTopic succeeds.
//
// The spec is defaulted first (review I6): a manifest that relies on the
// metadata-name default leaves spec.topicName empty, and the normal reconcile
// path only fills it via defaulting inside the engine — without defaulting
// here the finalizer would call DeleteTopic("") and never remove the real
// topic. (CompileTopic has its own fallback, so the ACL cleanup was already
// safe.) Cluster defaults are irrelevant to deletion, so nil is passed.
//
// Co-ownership shield: before deleting, the cluster-wide desired ACL union is
// built across the REMAINING live CRs (buildClusterACLView skips resources
// with a non-zero DeletionTimestamp, which excludes this topic — and any other
// CR mid-deletion — by construction) and any tuple another live KafkaTopic /
// KafkaAccessPolicy still desires is subtracted from the to-delete set. This
// mirrors the prune path's §10.4 aggregation: deleting one co-owner must not
// revoke access a surviving co-owner depends on. The broker topic itself needs
// no such shield (identity uniqueness guarantees one CR per cluster+topicName).
// If the view cannot be built (a List failure), the deletion attempt FAILS and
// is retried with the finalizer retained — never fall back to deleting the
// full set on error, since that could over-delete a co-owned tuple.
//
// sr may be nil (no Schema Registry configured for the cluster), in which case
// subject deletion is skipped without error. Subject-deletion failures emit a
// SubjectDeletionFailed Warning event and are logged, but do NOT block
// finalizer removal: the Kafka topic is already gone; an orphaned subject is
// preferable to a wedged namespace (spec §12).
func (r *KafkaTopicReconciler) deleteTopicState(ctx context.Context, topic *v1alpha1.KafkaTopic, k kafka.AdminClient, sr schemaregistry.Client, resolver secrets.Resolver, cluster *v1alpha1.KafkaCluster, clusterNS string) error {
	logger := log.FromContext(ctx)
	// Serialize the finalizer's ACL cleanup against every other ACL writer on
	// this cluster: the shield's remaining-CR view build and the DeleteACLs it
	// gates must be one atomic critical section, or a concurrent reconcile
	// could re-create / co-claim tuples between the view read and the delete.
	// Held (deferred) across the whole function rather than released after
	// DeleteACLs: the remaining work (DeleteTopic, subject deletion) is brief
	// and not a contended substrate (identity uniqueness guarantees one CR per
	// (cluster, topicName)), and a narrow release would complicate the error
	// paths for no correctness gain. No RBAC lock: this path never touches MDS
	// (rbac auto-mapped bindings are not removed on topic deletion).
	// No IDENTITY lock either (v0.37): the contested broker state here — the
	// ACL cleanup — is substrate-shared, and this substrate lock already
	// serializes it against every ACL writer, same-identity rivals included.
	// DeleteTopic itself is identity-exclusive, but a same-name rival taking
	// over from a deleting CR is an inter-reconcile handover (documented in
	// duplicate.go), not an intra-window race a lock could close; and taking
	// an identity lock here would invert the identity → substrate order.
	defer lockSubstrate(r.Locks, cluster, locking.SubstrateACL)()
	// nil ClusterDefaults is safe here only because no current default affects
	// ACL identity fields (principal/resource/operation). If a ClusterDefaults
	// field ever influences compiled ACL tuples, this must switch to
	// cluster.Spec.Defaults so the deleting CR's set matches how the
	// co-ownership view compiles every other CR.
	defaulting.Topic(topic, nil)
	desiredACLs, _ := access.BuildDesiredSet(access.CompileTopic(topic))
	desiredACLs, err := shieldACLDeletion(ctx, r.Client, topic.Spec.ClusterRef.Name,
		clusterNS, r.ClusterNamespace == "", cluster, desiredACLs,
		func(reason, msg string) { r.finalizeEvent(topic, corev1.EventTypeNormal, reason, msg) })
	if err != nil {
		return err
	}
	if len(desiredACLs) > 0 {
		states := make([]kafka.ACLState, 0, len(desiredACLs))
		for _, a := range desiredACLs {
			states = append(states, kafka.ACLState{
				Principal: a.Principal, Host: a.Host, ResourceType: a.ResourceType,
				ResourceName: a.ResourceName, PatternType: a.PatternType,
				Operation: a.Operation, Permission: a.Permission,
			})
		}
		if err := k.DeleteACLs(ctx, states); err != nil {
			return err
		}
	}
	if err := k.DeleteTopic(ctx, topic.Spec.TopicName); err != nil {
		return err
	}

	// Subject deletion (spec §12): only when a Schema Registry client is
	// available and the topic declares schema CONTENT (valueSchema or keySchema
	// present). Governance-mode topics (spec.schema without content bodies) only
	// manage a subject's compatibility level — only the compatibility level was
	// managed by monedula, not the content (spec §12.2). Deleting those subjects
	// would remove versions registered out-of-band by the producer's pipeline.
	if sr == nil || topic.Spec.Schema == nil {
		return nil // no schema registry or no schema block: nothing to do
	}
	// sr == nil is the only guard ManagedSubjects does not own; schema-nil and governance-mode are handled inside it.

	subjects, err := reconcile.ManagedSubjects(topic, resolver)
	if err != nil {
		// Body resolution failed (e.g. Secret already deleted). This only happens
		// for RecordName/TopicRecordName strategies — ManagedSubjects never calls
		// the resolver for TopicName/Custom. Those strategies need the schema body
		// to derive the subject name, so we cannot proceed; emit a warning and
		// continue (do not block finalization).
		msg := "could not compute managed subjects for deletion (schema body unresolvable): " + err.Error()
		logger.Error(err, "subject deletion skipped", "topic", topic.Spec.TopicName)
		r.finalizeEvent(topic, corev1.EventTypeWarning, "SubjectDeletionFailed", msg)
		return nil // do not block finalization
	}
	for _, subj := range subjects {
		if derr := sr.DeleteSubject(ctx, subj); derr != nil {
			msg := "failed to delete schema registry subject " + subj + ": " + derr.Error()
			logger.Error(derr, "subject deletion failed", "subject", subj, "topic", topic.Spec.TopicName)
			r.finalizeEvent(topic, corev1.EventTypeWarning, "SubjectDeletionFailed", msg)
			// Continue: do not block finalization even if one subject fails.
		}
	}
	return nil
}

// removeFinalizer drops the finalizer and persists it, letting Kubernetes
// garbage-collect the object. No requeue. This is the single deletion-success
// exit (both the Orphan and Delete paths, plus force-removal, end here), so the
// per-CR metrics cleanup lives here.
func (r *KafkaTopicReconciler) removeFinalizer(ctx context.Context, topic *v1alpha1.KafkaTopic) (ctrl.Result, error) {
	controllerutil.RemoveFinalizer(topic, FinalizerName)
	if err := r.Update(ctx, topic); err != nil {
		return ctrl.Result{}, err
	}
	forgetTopicMetrics(topic.Namespace, topic.Name)
	return ctrl.Result{}, nil
}

// event emits a Kubernetes Event for obj on the reconcile path, a no-op when
// no Recorder is wired. See events.go for the action convention.
func (r *KafkaTopicReconciler) event(obj *v1alpha1.KafkaTopic, eventType, reason, msg string) {
	if r.Recorder == nil {
		return
	}
	r.Recorder.Eventf(obj, nil, eventType, reason, actionReconcile, "%s", msg)
}

// finalizeEvent is event for the deletion/finalizer path (actionFinalize).
func (r *KafkaTopicReconciler) finalizeEvent(obj *v1alpha1.KafkaTopic, eventType, reason, msg string) {
	if r.Recorder == nil {
		return
	}
	r.Recorder.Eventf(obj, nil, eventType, reason, actionFinalize, "%s", msg)
}

// mapSecretToTopics enqueues the KafkaTopics on any cluster (in the Secret's
// namespace) that references the changed credential/TLS Secret (§11.4). The
// Secret is referenced by the cluster, not the topic, so this is the 2nd hop:
// Secret -> referencing cluster(s) -> topics on those clusters. List scope
// mirrors buildClusterACLView: namespace-local when ClusterNamespace == "";
// cluster-wide when a --cluster-namespace override is set.
func (r *KafkaTopicReconciler) mapSecretToTopics(ctx context.Context, obj client.Object) []ctrlreconcile.Request {
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
	var list v1alpha1.KafkaTopicList
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

// mapConfigMapToTopics enqueues the KafkaTopics in the ConfigMap's namespace
// that reference it for a schema body (via SchemaConfigMapIndex). Only labelled
// ConfigMaps reach here (the cache is label-scoped, §11.3); a change to one
// reconciles its referencing topics promptly instead of at the resync.
func (r *KafkaTopicReconciler) mapConfigMapToTopics(ctx context.Context, obj client.Object) []ctrlreconcile.Request {
	cm, ok := obj.(*corev1.ConfigMap)
	if !ok {
		return nil
	}
	var topics v1alpha1.KafkaTopicList
	if err := r.List(ctx, &topics,
		client.InNamespace(cm.GetNamespace()),
		client.MatchingFields{SchemaConfigMapIndex: cm.GetName()}); err != nil {
		return nil
	}
	out := make([]ctrlreconcile.Request, 0, len(topics.Items))
	for i := range topics.Items {
		out = append(out, ctrlreconcile.Request{NamespacedName: types.NamespacedName{
			Namespace: topics.Items[i].Namespace,
			Name:      topics.Items[i].Name,
		}})
	}
	return out
}

// SetupWithManager registers the reconciler with the manager, watching
// KafkaTopic objects, labelled schema ConfigMaps (§11.3), labelled
// credential/TLS Secrets (§11.4), and KafkaClusters (review I2: a topic created
// before its cluster, or one erroring on a broken cluster spec, recovers the
// moment the cluster CR appears / is fixed instead of waiting out error
// backoff).
//
// watchEventFilter is scoped to the KafkaTopic source (via builder.WithPredicates)
// rather than applied globally (WithEventFilter). This is intentional:
// watchEventFilter includes GenerationChangedPredicate, but ConfigMap/Secret
// metadata.generation is NOT bumped when Data changes — a global predicate
// would silently drop the very updates we want to deliver. Scoping the
// predicate to the KafkaTopic source preserves the status-only self-update
// filtering for KafkaTopic reconciles while allowing all ConfigMap/Secret events
// (Create/Update/Delete) through to the map functions.
//
// Both caches are label-scoped, so only ConfigMaps carrying
// gitops.monedula.dev/schema-source=true reach mapConfigMapToTopics, and only
// Secrets carrying gitops.monedula.dev/credential-source=true reach
// mapSecretToTopics. The watches use informers from the manager cache; the
// DisableFor entries in manager.go's CacheOptions affect only direct client
// reads (the schema resolver / credential lookups), not these informers.
func (r *KafkaTopicReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Recorder == nil {
		r.Recorder = mgr.GetEventRecorder("kafkatopic-controller")
	}
	// Insurance against silently running unlocked: nil Locks means every
	// substrate span in this controller is unserialized (locks.go). Expected
	// only in tests — manager.Run always injects the registry.
	if r.Locks == nil {
		mgr.GetLogger().Info("substrate locking disabled (nil Locks registry); expected only in tests",
			"controller", "kafkatopic")
	}
	// Same insurance for the duplicate-gate quorum recheck (duplicate.go): nil
	// APIReader silently degrades to cached-scan-only behavior.
	if r.APIReader == nil {
		mgr.GetLogger().Info("duplicate-gate quorum recheck disabled (nil APIReader); expected only in tests",
			"controller", "kafkatopic")
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.KafkaTopic{}, builder.WithPredicates(watchEventFilter())).
		Watches(&corev1.ConfigMap{}, handler.EnqueueRequestsFromMapFunc(r.mapConfigMapToTopics)).
		Watches(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(r.mapSecretToTopics)).
		Watches(&v1alpha1.KafkaCluster{}, handler.EnqueueRequestsFromMapFunc(r.mapClusterToTopics),
			builder.WithPredicates(clusterWatchPredicate())).
		WithOptions(reconcilerOptions(r.MaxConcurrentReconciles)).
		Complete(r)
}

// setSchemaSourceUnwatchedCondition sets CondSchemaSourceUnwatched on st: True
// (reason ConfigMapNotLabeled) when a referenced schema ConfigMap lacks the
// SchemaSourceLabel — its edits reconcile only at the resync, not promptly
// (§11.3); else False (reason AllWatchedOrNone). Non-terminal: never fails the
// reconcile. Reads ConfigMaps via the (uncached) client; a read error is treated
// as "cannot confirm" and skips that ConfigMap (no false positive).
func (r *KafkaTopicReconciler) setSchemaSourceUnwatchedCondition(ctx context.Context, topic *v1alpha1.KafkaTopic, st *v1alpha1.KafkaTopicStatus) {
	names := schemaConfigMapNames(topic)
	var unlabelled []string
	for _, n := range names {
		var cm corev1.ConfigMap
		if err := r.Get(ctx, types.NamespacedName{Namespace: topic.Namespace, Name: n}, &cm); err != nil {
			continue // cannot confirm; do not flag
		}
		if cm.Labels[SchemaSourceLabel] != SchemaSourceLabelValue {
			unlabelled = append(unlabelled, n)
		}
	}
	if len(unlabelled) == 0 {
		meta.SetStatusCondition(&st.Conditions, metav1.Condition{
			Type:               v1alpha1.CondSchemaSourceUnwatched,
			Status:             metav1.ConditionFalse,
			Reason:             "AllWatchedOrNone",
			Message:            "all referenced schema ConfigMaps are watched (or none referenced)",
			ObservedGeneration: topic.Generation,
		})
		return
	}
	meta.SetStatusCondition(&st.Conditions, metav1.Condition{
		Type:               v1alpha1.CondSchemaSourceUnwatched,
		Status:             metav1.ConditionTrue,
		Reason:             "ConfigMapNotLabeled",
		Message:            fmt.Sprintf("schema ConfigMap(s) %s lack the %s=%q label; edits reconcile only at the periodic resync, not promptly", strings.Join(unlabelled, ", "), SchemaSourceLabel, SchemaSourceLabelValue),
		ObservedGeneration: topic.Generation,
	})
}

// topicErrorStatus builds an Error status with Ready False (reason) for a
// pre-engine failure (cluster lookup or client build). It seeds conditions from
// the existing status so LastTransitionTime is preserved across requeues.
func topicErrorStatus(topic *v1alpha1.KafkaTopic, reason, msg string) v1alpha1.KafkaTopicStatus {
	now := metav1.Now()
	st := v1alpha1.KafkaTopicStatus{
		ObservedGeneration: topic.Generation,
		Phase:              v1alpha1.PhaseError,
		LastCheckedTime:    &now,
	}
	if topic.Status != nil {
		st.Conditions = append([]metav1.Condition(nil), topic.Status.Conditions...)
	}
	meta.SetStatusCondition(&st.Conditions, metav1.Condition{
		Type:               v1alpha1.CondReady,
		Status:             metav1.ConditionFalse,
		Reason:             reason,
		Message:            msg,
		ObservedGeneration: topic.Generation,
	})
	return st
}
