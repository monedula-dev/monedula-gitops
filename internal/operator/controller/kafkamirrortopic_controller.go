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
	"github.com/monedula-dev/monedula-gitops/internal/kafkarest"
	"github.com/monedula-dev/monedula-gitops/internal/operator"
	"github.com/monedula-dev/monedula-gitops/internal/operator/index"
	"github.com/monedula-dev/monedula-gitops/internal/operator/locking"
	"github.com/monedula-dev/monedula-gitops/internal/operator/reconcile"
)

// DefaultMirrorStallThreshold is how long a mirror may report the same
// transitional status before it is reported as stalled drift, when
// StallThreshold is zero.
const DefaultMirrorStallThreshold = 30 * time.Minute

// DefaultFinalizerVerificationTimeout is the total deadline for the KafkaTopic
// finalizer's final mirror verification when
// KafkaTopicReconciler.FinalizerVerificationTimeout is zero.
const DefaultFinalizerVerificationTimeout = 5 * time.Second

// KafkaMirrorTopicReconciler reconciles a KafkaMirrorTopic: a destination
// topic created and kept healthy as a cluster-link mirror through the Kafka
// REST API. Unlike the other topic-owning kinds it manages NO finalizer:
// deleting the CR always orphans the mirror topic, so there is nothing to
// clean up and deletion can never block on Kafka. Deletion is handled right
// after the Get, before the cluster is resolved or any client is built.
type KafkaMirrorTopicReconciler struct {
	client.Client
	// Scheme is held for manager wiring.
	Scheme *runtime.Scheme
	// Clients builds the live Kafka admin and Kafka REST clients for a cluster.
	Clients ClientFactory
	// Recorder emits one Kubernetes Event per reconcile outcome. Set by
	// SetupWithManager; may be nil in unit tests (events are then skipped).
	Recorder events.EventRecorder
	// ClusterNamespace is where KafkaCluster CRs are looked up. When empty, the
	// mirror's own namespace is used (clusterRef is namespace-local by default).
	ClusterNamespace string
	// ResyncInterval overrides the periodic resync cadence. Zero uses
	// DefaultResyncInterval; see resync.go.
	ResyncInterval time.Duration
	// StallThreshold is how long an unchanged transitional mirror status is
	// tolerated before it is reported as stalled. Zero uses
	// DefaultMirrorStallThreshold (30m).
	StallThreshold time.Duration
	// MaxConcurrentReconciles is passed to controller.Options in
	// SetupWithManager. Zero uses DefaultMaxConcurrentReconciles.
	MaxConcurrentReconciles int
	// Locks is the process-wide keyed lock registry (locks.go). A mirror takes
	// the per-identity lock shared with KafkaTopic (topicIdentityKind), so two
	// claimants of one destination topic serialize the gate, recheck and
	// engine span. nil (unit tests) acquires no locks.
	Locks *locking.Registry
	// APIReader is the manager's uncached quorum reader, used only for the
	// duplicate-identity gate's contested-path recheck (duplicate.go). nil
	// skips the recheck.
	APIReader client.Reader
	// Now is the clock used for status timestamps and stall detection. nil
	// uses time.Now.
	Now func() time.Time
}

// +kubebuilder:rbac:groups=gitops.monedula.dev,resources=kafkamirrortopics,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=gitops.monedula.dev,resources=kafkamirrortopics/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=gitops.monedula.dev,resources=kafkamirrortopics/finalizers,verbs=update
// +kubebuilder:rbac:groups=gitops.monedula.dev,resources=kafkaclusters,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

// Reconcile drives a KafkaMirrorTopic toward its spec. A deleting CR returns
// immediately (removing a finalizer left by an older build, if any) without
// resolving the cluster or contacting Kafka. Otherwise it resolves the
// cluster, takes the shared topic identity lock, runs the cross-kind
// duplicate-identity gate, builds the Kafka and REST clients, delegates to
// reconcile.ReconcileMirrorTopic and writes the resulting status.
func (r *KafkaMirrorTopicReconciler) Reconcile(ctx context.Context, req ctrl.Request) (res ctrl.Result, retErr error) {
	logger := log.FromContext(ctx)
	defer recordReconcile(controllerKafkaMirrorTopic, time.Now(), &retErr)

	var m v1alpha1.KafkaMirrorTopic
	if err := r.Get(ctx, req.NamespacedName, &m); err != nil {
		if client.IgnoreNotFound(err) == nil {
			forgetMirrorTopicMetrics(req.Namespace, req.Name)
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Deletion: always orphan, never contact the cluster. There is no
	// finalizer to run; one left by an older build is simply dropped.
	if !m.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&m, FinalizerName) {
			controllerutil.RemoveFinalizer(&m, FinalizerName)
			if err := r.Update(ctx, &m); err != nil {
				return ctrl.Result{}, client.IgnoreNotFound(err)
			}
		}
		forgetMirrorTopicMetrics(m.Namespace, m.Name)
		return ctrl.Result{}, nil
	}
	r.observeManaged(&m)

	clusterNS := r.ClusterNamespace
	if clusterNS == "" {
		clusterNS = m.Namespace
	}
	var cluster v1alpha1.KafkaCluster
	if cerr := r.Get(ctx, types.NamespacedName{Namespace: clusterNS, Name: m.Spec.ClusterRef.Name}, &cluster); cerr != nil {
		logger.Error(cerr, "resolving clusterRef", "cluster", m.Spec.ClusterRef.Name, "namespace", clusterNS)
		r.event(&m, corev1.EventTypeWarning, "ClusterNotFound", cerr.Error())
		if uerr := updateStatus(ctx, r.Client, req.NamespacedName, &m, func() {
			st := mirrorErrorStatus(&m, "ClusterNotFound", cerr.Error(), r.now())
			m.Status = &st
		}); uerr != nil {
			return ctrl.Result{}, errors.Join(cerr, uerr)
		}
		return ctrl.Result{}, cerr
	}

	// Shared per-identity lock with KafkaTopic (topicclaim.go): the gate, its
	// quorum recheck and the engine's REST writes are atomic against any
	// other claimant of the same destination topic. Released explicitly after
	// the engine; the deferred call backstops the early returns.
	unlockIdentity := lockIdentity(r.Locks, &cluster, topicIdentityKind, m.Spec.EffectiveTopicName())
	defer unlockIdentity()

	if res, done, err := r.duplicateIdentityGate(ctx, &m); done {
		return res, err
	}

	// Build the Kafka admin and REST clients. rest is nil when the cluster has
	// no spec.rest; the engine reports that as terminal RESTNotConfigured.
	var rest kafkarest.Client
	k, _, cleanup, berr := r.Clients.For(ctx, &cluster)
	if berr == nil {
		defer cleanup()
		rest, berr = r.Clients.RESTFor(ctx, &cluster)
	}
	if berr != nil {
		logger.Error(berr, "building cluster clients")
		r.event(&m, corev1.EventTypeWarning, "ClientsBuildFailed", berr.Error())
		if uerr := updateStatus(ctx, r.Client, req.NamespacedName, &m, func() {
			st := mirrorErrorStatus(&m, "ClientsBuildFailed", berr.Error(), r.now())
			m.Status = &st
		}); uerr != nil {
			return ctrl.Result{}, errors.Join(berr, uerr)
		}
		return ctrl.Result{}, berr
	}

	// The engine runs exactly once, before the status write: a conflict on
	// the write retries only the write, never the REST mutations.
	st, rerr := reconcile.ReconcileMirrorTopic(ctx, &m, &cluster, k, rest, r.now(), r.stallThreshold())
	unlockIdentity()
	operator.SetMirrorTopicDrift(m.Namespace, m.Name, st.Drift != nil && st.Drift.Detected)
	if uerr := updateStatus(ctx, r.Client, req.NamespacedName, &m, func() {
		m.Status = &st
	}); uerr != nil {
		return ctrl.Result{}, uerr
	}

	if rerr != nil {
		r.event(&m, corev1.EventTypeWarning, "ReconcileError", rerr.Error())
		return ctrl.Result{}, rerr // transient: requeue with backoff
	}
	r.event(&m, corev1.EventTypeNormal, "Reconciled", "mirror topic reconciled")
	return ctrl.Result{RequeueAfter: resyncInterval(r.ResyncInterval)}, nil
}

// duplicateIdentityGate is the KafkaMirrorTopic call site of the cross-kind
// topic claim check, modelled exactly on the KafkaTopic gate: an older live
// KafkaTopic or KafkaMirrorTopic claiming the same (cluster, destination
// topic) makes this CR terminal (ValidationFailed/DuplicateIdentity) before
// any client is built. done=false means proceed with the normal reconcile.
func (r *KafkaMirrorTopicReconciler) duplicateIdentityGate(ctx context.Context, m *v1alpha1.KafkaMirrorTopic) (ctrl.Result, bool, error) {
	winner, derr := findOlderTopicClaimant(ctx, r.Client, m, r.ClusterNamespace == "")
	if derr == nil && r.APIReader != nil &&
		(winner != nil || m.Status == nil || !identityMaterialized(m.Status.Conditions)) {
		winner, derr = findOlderTopicClaimant(ctx, r.APIReader, m, r.ClusterNamespace == "")
	}
	if derr != nil {
		r.event(m, corev1.EventTypeWarning, reasonDuplicateCheckFailed, derr.Error())
		if uerr := updateStatus(ctx, r.Client, client.ObjectKeyFromObject(m), m, func() {
			st := mirrorErrorStatus(m, reasonDuplicateCheckFailed, derr.Error(), r.now())
			m.Status = &st
		}); uerr != nil {
			return ctrl.Result{}, true, errors.Join(derr, uerr)
		}
		return ctrl.Result{}, true, derr // transient: requeue with backoff
	}
	if winner == nil {
		return ctrl.Result{}, false, nil
	}
	msg := duplicateIdentityMessage(
		fmt.Sprintf("(cluster %q, topic %q)", m.Spec.ClusterRef.Name, m.Spec.EffectiveTopicName()),
		topicClaimKind(winner), winner.GetNamespace(), winner.GetName())
	r.event(m, corev1.EventTypeWarning, reasonDuplicateIdentity, msg)
	operator.RecordReconcileTerminal(controllerKafkaMirrorTopic, reasonDuplicateIdentity)
	// The engine never runs for a loser: clear the drift gauge rather than let
	// a value from a previous winning reconcile linger.
	operator.SetMirrorTopicDrift(m.Namespace, m.Name, false)
	if uerr := updateStatus(ctx, r.Client, client.ObjectKeyFromObject(m), m, func() {
		st := mirrorErrorStatus(m, reasonDuplicateIdentity, msg, r.now())
		meta.SetStatusCondition(&st.Conditions, metav1.Condition{
			Type:               v1alpha1.CondValidationFailed,
			Status:             metav1.ConditionTrue,
			Reason:             reasonDuplicateIdentity,
			Message:            msg,
			ObservedGeneration: m.Generation,
		})
		m.Status = &st
	}); uerr != nil {
		return ctrl.Result{}, true, uerr
	}
	return ctrl.Result{RequeueAfter: resyncInterval(r.ResyncInterval)}, true, nil
}

// now returns the reconciler clock.
func (r *KafkaMirrorTopicReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// stallThreshold returns the effective stall threshold.
func (r *KafkaMirrorTopicReconciler) stallThreshold() time.Duration {
	if r.StallThreshold == 0 {
		return DefaultMirrorStallThreshold
	}
	return r.StallThreshold
}

// managedMirrorTopicKeys tracks which KafkaMirrorTopics are counted into the
// managed-mirror-topics gauge, so a re-reconcile does not double-count.
var managedMirrorTopicKeys sync.Map

// observeManaged bumps the managed-mirror-topics gauge the first time a given
// mirror is observed by this process.
func (r *KafkaMirrorTopicReconciler) observeManaged(m *v1alpha1.KafkaMirrorTopic) {
	if _, loaded := managedMirrorTopicKeys.LoadOrStore(m.Namespace+"/"+m.Name, struct{}{}); !loaded {
		operator.IncManagedMirrorTopics(1)
	}
}

// forgetMirrorTopicMetrics un-counts a gone (or deleting) mirror from the
// managed gauge and drops its drift series. Idempotent.
func forgetMirrorTopicMetrics(namespace, name string) {
	if _, loaded := managedMirrorTopicKeys.LoadAndDelete(namespace + "/" + name); loaded {
		operator.IncManagedMirrorTopics(-1)
	}
	operator.DeleteMirrorTopicDrift(namespace, name)
}

// event emits a Kubernetes Event for obj, a no-op when no Recorder is wired.
func (r *KafkaMirrorTopicReconciler) event(obj *v1alpha1.KafkaMirrorTopic, eventType, reason, msg string) {
	if r.Recorder == nil {
		return
	}
	r.Recorder.Eventf(obj, nil, eventType, reason, actionReconcile, "%s", msg)
}

// mapSecretToMirrorTopics enqueues the KafkaMirrorTopics on any cluster that
// references the changed credential/TLS Secret (Secret -> referencing
// cluster(s) -> mirrors on those clusters). Scoped like mapSecretToTopics.
func (r *KafkaMirrorTopicReconciler) mapSecretToMirrorTopics(ctx context.Context, obj client.Object) []ctrlreconcile.Request {
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
	var list v1alpha1.KafkaMirrorTopicList
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

// mapClusterToMirrorTopics enqueues the KafkaMirrorTopics whose clusterRef
// resolves to the changed KafkaCluster — including when spec.rest is added,
// which un-wedges a mirror reporting RESTNotConfigured.
func (r *KafkaMirrorTopicReconciler) mapClusterToMirrorTopics(ctx context.Context, obj client.Object) []ctrlreconcile.Request {
	return mapClusterToDependents(ctx, r.Client, r.ClusterNamespace, obj,
		&v1alpha1.KafkaMirrorTopicList{}, index.MirrorTopicClusterRefNameIndex)
}

// SetupWithManager registers the reconciler with the manager: KafkaMirrorTopic
// events (status-only updates filtered), cluster credential Secrets, and
// KafkaCluster changes.
func (r *KafkaMirrorTopicReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Recorder == nil {
		r.Recorder = mgr.GetEventRecorder("kafkamirrortopic-controller")
	}
	if r.Locks == nil {
		mgr.GetLogger().Info("identity locking disabled (nil Locks registry); expected only in tests",
			"controller", "kafkamirrortopic")
	}
	if r.APIReader == nil {
		mgr.GetLogger().Info("duplicate-gate quorum recheck disabled (nil APIReader); expected only in tests",
			"controller", "kafkamirrortopic")
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.KafkaMirrorTopic{}, builder.WithPredicates(watchEventFilter())).
		Watches(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(r.mapSecretToMirrorTopics)).
		Watches(&v1alpha1.KafkaCluster{}, handler.EnqueueRequestsFromMapFunc(r.mapClusterToMirrorTopics),
			builder.WithPredicates(clusterWatchPredicate())).
		WithOptions(reconcilerOptions(r.MaxConcurrentReconciles)).
		Complete(r)
}

// mirrorErrorStatus builds an Error status with Ready False (reason) for a
// pre-engine failure. It seeds conditions and the observation tracking fields
// from the existing status, so LastTransitionTime and the stall clock survive
// error passes.
func mirrorErrorStatus(m *v1alpha1.KafkaMirrorTopic, reason, msg string, now time.Time) v1alpha1.KafkaMirrorTopicStatus {
	nowT := metav1.NewTime(now)
	st := v1alpha1.KafkaMirrorTopicStatus{
		ObservedGeneration: m.Generation,
		Phase:              v1alpha1.PhaseError,
		LinkName:           m.Spec.LinkName,
		SourceTopicName:    m.Spec.SourceTopicName,
		LastCheckedTime:    &nowT,
	}
	if m.Status != nil {
		st.Conditions = append([]metav1.Condition(nil), m.Status.Conditions...)
		st.ObservedMirrorStatus = m.Status.ObservedMirrorStatus
		st.LastObservedStatusChangeTime = m.Status.LastObservedStatusChangeTime.DeepCopy()
		st.LastAppliedTime = m.Status.LastAppliedTime.DeepCopy()
	}
	meta.SetStatusCondition(&st.Conditions, metav1.Condition{
		Type:               v1alpha1.CondReady,
		Status:             metav1.ConditionFalse,
		Reason:             reason,
		Message:            msg,
		ObservedGeneration: m.Generation,
	})
	return st
}
