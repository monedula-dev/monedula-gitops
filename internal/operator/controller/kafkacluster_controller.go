// Package controller holds the controller-runtime reconcilers that drive the
// Monedula GitOps operator. Each reconciler is thin glue: it fetches the managed
// object, delegates to the controller-runtime-free reconcile core, and writes
// the resulting status back. The Kafka/Schema-Registry clients are obtained
// through an injected ClientFactory so the reconcile logic stays testable
// (Task 9 envtest stubs it with mock clients).
package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
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
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	ctrlreconcile "sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/cluster"
	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	"github.com/monedula-dev/monedula-gitops/internal/mds"
	"github.com/monedula-dev/monedula-gitops/internal/operator"
	"github.com/monedula-dev/monedula-gitops/internal/operator/reconcile"
	"github.com/monedula-dev/monedula-gitops/internal/schemaregistry"
)

// clusterRequeueAfter is the DEFAULT periodic re-check interval for a healthy
// cluster, used when a reconciler's ResyncInterval field is zero. Readiness has
// no watchable trigger, so we poll. See resync.go.
const clusterRequeueAfter = DefaultResyncInterval

// ClientFactory builds the live Kafka and Schema-Registry clients for a
// KafkaCluster. It is injected into the reconciler so tests (and Task 9's
// envtest) can supply mock clients without a real broker, while production wires
// DefaultClientFactory.
type ClientFactory interface {
	// For builds the clients for c. sr is nil when the cluster has no Schema
	// Registry configured. cleanup must always be non-nil and is deferred by the
	// caller (it closes the Kafka client).
	For(ctx context.Context, c *v1alpha1.KafkaCluster) (k kafka.AdminClient, sr schemaregistry.Client, cleanup func(), err error)
	// MDSFor builds the MDS client for c. Returns (nil, nil) when the cluster
	// has no authorization.mds configured — callers must nil-guard the result.
	MDSFor(ctx context.Context, c *v1alpha1.KafkaCluster) (mds.Client, error)
}

// KafkaClusterReconciler reconciles a KafkaCluster. It performs readiness/status
// only: a KafkaCluster owns no external state, so there is no mutation and no
// finalizer.
type KafkaClusterReconciler struct {
	client.Client
	// Scheme is held for the manager and event-recorder wiring.
	Scheme *runtime.Scheme
	// Clients builds the live Kafka/Schema-Registry clients for a cluster.
	Clients ClientFactory
	// Recorder emits one Kubernetes Event (events.k8s.io API) per reconcile
	// outcome. Set by SetupWithManager; may be nil in unit tests (events are
	// then skipped).
	Recorder events.EventRecorder
	// ResyncInterval overrides the periodic resync cadence (--resync-interval).
	// Zero uses DefaultResyncInterval (5m); see resync.go.
	ResyncInterval time.Duration
	// MaxConcurrentReconciles is passed to controller.Options in
	// SetupWithManager. Zero uses DefaultMaxConcurrentReconciles (1); see
	// resync.go and --max-concurrent-reconciles.
	MaxConcurrentReconciles int
}

// Secrets are read UNCACHED via DefaultClientFactory (the manager's client has
// Secrets in DisableFor, §11.4), so the get verb covers credential reads. The
// list+watch verbs are needed by the label-scoped Secret informer that powers
// the credential-source prompt watch (mapSecretToClusters). events.k8s.io
// create/patch is required for Recorder.Eventf against CRs in any namespace
// (the events.k8s.io recorder creates new Events and patches event series).
// +kubebuilder:rbac:groups=gitops.monedula.dev,resources=kafkaclusters,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=gitops.monedula.dev,resources=kafkaclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch

// Reconcile probes the cluster's readiness and writes the resulting status. It
// requeues periodically so readiness is re-checked even without a spec change.
func (r *KafkaClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (res ctrl.Result, retErr error) {
	logger := log.FromContext(ctx)
	defer recordReconcile(controllerKafkaCluster, time.Now(), &retErr)

	var c v1alpha1.KafkaCluster
	if err := r.Get(ctx, req.NamespacedName, &c); err != nil {
		// NotFound: the object was deleted; no finalizer, so this is the only
		// deletion hook. The Delete watch event always passes the event filter
		// (predicates only constrain Updates), so this branch reliably fires after
		// a deletion and removes the cluster's reachability series (review I12).
		if client.IgnoreNotFound(err) == nil {
			operator.DeleteClusterReachable(req.Namespace, req.Name)
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	k, sr, cleanup, err := r.Clients.For(ctx, &c)
	if err != nil {
		// Could not build clients (e.g. a secret ref failed to resolve). Report
		// Error + ClusterReachable False and requeue to retry.
		logger.Error(err, "building cluster clients")
		r.event(&c, corev1.EventTypeWarning, "ClientsBuildFailed", err.Error())
		// Write the build-error status conflict-safely. If the status write also
		// fails, join both errors so the build error (root cause) is not lost.
		if uerr := updateStatus(ctx, r.Client, req.NamespacedName, &c, func() {
			st := clientsBuildErrorStatus(&c, err)
			c.Status = &st
		}); uerr != nil {
			return ctrl.Result{}, errors.Join(err, uerr)
		}
		// A cluster whose clients cannot even be built is not reachable: keep the
		// gauge fresh on this error path too (review I12) instead of leaving the
		// last probed value.
		operator.SetClusterReachable(c.Namespace, c.Name, false)
		return ctrl.Result{}, err
	}
	defer cleanup()

	// Probe readiness exactly ONCE, then write the result conflict-safely: a 409
	// Conflict on the status write retries only the write, never the probe
	// (review I9 — same retry hygiene as the mutating controllers). Condition
	// seeding (LastTransitionTime preservation) reads the status of the object
	// fetched at the top of this Reconcile.
	st := reconcile.ReconcileCluster(ctx, &c, k, sr)
	r.setCredentialSourceUnwatchedCondition(ctx, &c, &st)
	if uerr := updateStatus(ctx, r.Client, req.NamespacedName, &c, func() {
		c.Status = &st
	}); uerr != nil {
		return ctrl.Result{}, uerr
	}
	reachable := c.Status != nil && c.Status.Phase == v1alpha1.PhaseReady
	operator.SetClusterReachable(c.Namespace, c.Name, reachable)
	if reachable {
		r.event(&c, corev1.EventTypeNormal, "Ready", "cluster reachable")
	} else {
		r.event(&c, corev1.EventTypeWarning, "Unreachable", "cluster not ready")
	}

	return ctrl.Result{RequeueAfter: resyncInterval(r.ResyncInterval)}, nil
}

// event emits a Kubernetes Event for obj, no-op when no Recorder is wired (unit
// tests). reason is a short CamelCase token; msg is human-readable. The action
// is actionReconcile — this controller has no deletion path (see events.go for
// the action convention).
func (r *KafkaClusterReconciler) event(obj *v1alpha1.KafkaCluster, eventType, reason, msg string) {
	if r.Recorder == nil {
		return
	}
	r.Recorder.Eventf(obj, nil, eventType, reason, actionReconcile, "%s", msg)
}

// SetupWithManager registers the reconciler with the manager, watching
// KafkaCluster objects. watchEventFilter drops status-only updates (notably
// this controller's own status writes) so a reconcile does not re-enqueue
// itself; periodic readiness re-checks come from RequeueAfter instead.
func (r *KafkaClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Recorder == nil {
		r.Recorder = mgr.GetEventRecorder("kafkacluster-controller")
	}
	// watchEventFilter is scoped to the KafkaCluster source (builder.WithPredicates)
	// rather than applied globally (WithEventFilter): it includes
	// GenerationChangedPredicate, but a Secret data change does NOT bump
	// metadata.generation, so a global predicate would silently drop the very
	// rotation events the Secret watch exists to deliver (§11.4). The cache is
	// label-scoped, so only Secrets carrying gitops.monedula.dev/credential-source=true
	// ever reach mapSecretToClusters.
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.KafkaCluster{}, builder.WithPredicates(watchEventFilter())).
		Watches(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(r.mapSecretToClusters)).
		WithOptions(reconcilerOptions(r.MaxConcurrentReconciles)).
		Complete(r)
}

// clientsBuildErrorStatus produces an Error status with ClusterReachable False
// for the case where clients could not even be constructed (so no probe ran).
func clientsBuildErrorStatus(c *v1alpha1.KafkaCluster, err error) v1alpha1.KafkaClusterStatus {
	now := metav1.Now()
	st := v1alpha1.KafkaClusterStatus{
		ObservedGeneration: c.Generation,
		Phase:              v1alpha1.PhaseError,
		LastCheckedTime:    &now,
	}
	reconcile.SetClusterClientsError(&st, c.Generation, err)
	return st
}

// DefaultClientFactory builds clients from Kubernetes Secrets via a per-cluster
// operator.K8sResolver, reusing cluster.BuildKafkaClient/BuildSchemaClient (the
// same seam the CLI uses). It is the production ClientFactory; tests inject a
// stub instead.
type DefaultClientFactory struct {
	// Client reads Secrets referenced by the cluster spec.
	Client client.Client
}

var _ ClientFactory = (*DefaultClientFactory)(nil)

// For resolves the cluster's secrets from its own namespace and builds the live
// clients.
func (f *DefaultClientFactory) For(ctx context.Context, c *v1alpha1.KafkaCluster) (kafka.AdminClient, schemaregistry.Client, func(), error) {
	res := &operator.K8sResolver{Client: f.Client, Namespace: c.Namespace, Ctx: ctx}

	k, cleanup, err := cluster.BuildKafkaClient(c, res)
	if err != nil {
		return nil, nil, func() {}, err
	}
	sr, err := cluster.BuildSchemaClient(c, res)
	if err != nil {
		cleanup()
		return nil, nil, func() {}, err
	}
	return k, sr, cleanup, nil
}

// MDSFor resolves the cluster's secrets from its own namespace and builds the
// MDS client. Returns (nil, nil) when the cluster has no authorization.mds
// configured.
func (f *DefaultClientFactory) MDSFor(ctx context.Context, c *v1alpha1.KafkaCluster) (mds.Client, error) {
	res := &operator.K8sResolver{Client: f.Client, Namespace: c.Namespace, Ctx: ctx}
	return cluster.BuildMDSClient(c, res)
}

// mapSecretToClusters enqueues the KafkaCluster(s) in the Secret's namespace
// that reference it for credentials/TLS (via ClusterSecretNamesIndex). Only
// labelled Secrets reach here (the cache is label-scoped, §11.4); a rotation
// reconciles its referencing cluster(s) promptly instead of at the resync.
func (r *KafkaClusterReconciler) mapSecretToClusters(ctx context.Context, obj client.Object) []ctrlreconcile.Request {
	if _, ok := obj.(*corev1.Secret); !ok {
		return nil
	}
	names := clustersReferencingSecret(ctx, r.Client, obj)
	out := make([]ctrlreconcile.Request, 0, len(names))
	for _, n := range names {
		out = append(out, ctrlreconcile.Request{NamespacedName: types.NamespacedName{
			Namespace: obj.GetNamespace(),
			Name:      n,
		}})
	}
	return out
}

// setCredentialSourceUnwatchedCondition sets CondCredentialSourceUnwatched on st:
// True (reason SecretNotLabeled) when a referenced credential/TLS Secret lacks
// the CredentialSourceLabel — a rotation reconciles only at the resync, not
// promptly (§11.4); else False (reason AllWatchedOrNone). Non-terminal: never
// fails the reconcile. Reads Secrets via the (uncached) client; a read error is
// treated as "cannot confirm" and skips that Secret (no false positive).
func (r *KafkaClusterReconciler) setCredentialSourceUnwatchedCondition(ctx context.Context, c *v1alpha1.KafkaCluster, st *v1alpha1.KafkaClusterStatus) {
	names := clusterSecretNames(c)
	var unlabelled []string
	for _, n := range names {
		var s corev1.Secret
		if err := r.Get(ctx, types.NamespacedName{Namespace: c.Namespace, Name: n}, &s); err != nil {
			continue // cannot confirm; do not flag
		}
		if s.Labels[CredentialSourceLabel] != CredentialSourceLabelValue {
			unlabelled = append(unlabelled, n)
		}
	}
	if len(unlabelled) == 0 {
		meta.SetStatusCondition(&st.Conditions, metav1.Condition{
			Type:               v1alpha1.CondCredentialSourceUnwatched,
			Status:             metav1.ConditionFalse,
			Reason:             "AllWatchedOrNone",
			Message:            "all referenced credential/TLS Secrets are watched (or none referenced)",
			ObservedGeneration: c.Generation,
		})
		return
	}
	meta.SetStatusCondition(&st.Conditions, metav1.Condition{
		Type:               v1alpha1.CondCredentialSourceUnwatched,
		Status:             metav1.ConditionTrue,
		Reason:             "SecretNotLabeled",
		Message:            fmt.Sprintf("credential Secret(s) %s lack the %s=%q label; a rotation reconciles only at the periodic resync, not promptly", strings.Join(unlabelled, ", "), CredentialSourceLabel, CredentialSourceLabelValue),
		ObservedGeneration: c.Generation,
	})
}
