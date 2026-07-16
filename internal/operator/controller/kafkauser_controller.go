package controller

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	"github.com/monedula-dev/monedula-gitops/internal/operator"
	"github.com/monedula-dev/monedula-gitops/internal/operator/locking"
	"github.com/monedula-dev/monedula-gitops/internal/operator/reconcile"
	operatorwebhook "github.com/monedula-dev/monedula-gitops/internal/operator/webhook"
)

// KafkaUserReconciler reconciles a KafkaUser. It is the SCRAM-credential
// analogue of the KafkaQuotaReconciler: it owns external Kafka credential
// state, so it manages a finalizer. Teardown honours spec.deletionPolicy
// (Delete, the default per defaulting.User / Orphan) and — mirroring quota —
// is UNGATED by risk-gate annotations: deleting the CR is the explicit action.
// A user needs only the Kafka admin client (no Schema Registry), plus the
// Kubernetes client for password Secrets.
type KafkaUserReconciler struct {
	client.Client
	// Scheme is held for manager wiring and generated-Secret owner references.
	Scheme *runtime.Scheme
	// Clients builds the live Kafka/Schema-Registry clients for a cluster. Only
	// the Kafka admin client is used here; the SR client is ignored.
	Clients ClientFactory
	// Recorder emits one Kubernetes Event (events.k8s.io API) per reconcile
	// outcome. Set by SetupWithManager; may be nil in unit tests (events are
	// then skipped).
	Recorder events.EventRecorder
	// ClusterNamespace is where KafkaCluster CRs are looked up. When empty, the
	// user's own namespace is used (clusterRef is namespace-local by default).
	ClusterNamespace string
	// ResyncInterval overrides the periodic resync cadence (--resync-interval).
	// Zero uses DefaultResyncInterval (5m); see resync.go.
	ResyncInterval time.Duration
	// MaxConcurrentReconciles is passed to controller.Options in
	// SetupWithManager. Zero uses DefaultMaxConcurrentReconciles (1); see
	// resync.go and --max-concurrent-reconciles.
	MaxConcurrentReconciles int
	// Locks is the process-wide keyed lock registry — see locks.go. A user
	// writes no cluster-wide substrate, so it takes only its per-identity
	// (KafkaCluster, "KafkaUser", username) lock, serializing the
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

// Secrets need create+update in addition to the read verbs the other
// controllers require: generate mode provisions (and metadata-refreshes) the
// owner-referenced "<cr-name>-kafka-credentials" Secret. delete is deliberately
// NOT requested — the generated Secret is garbage-collected via its owner
// reference when the CR goes away, so the operator never deletes it itself.
// +kubebuilder:rbac:groups=gitops.monedula.dev,resources=kafkausers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=gitops.monedula.dev,resources=kafkausers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=gitops.monedula.dev,resources=kafkausers/finalizers,verbs=update
// +kubebuilder:rbac:groups=gitops.monedula.dev,resources=kafkaclusters,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update

// Reconcile drives a KafkaUser toward its spec: it resolves the referenced
// cluster, builds the live Kafka client, manages the finalizer (removing the
// declared mechanism's credential on deletion under deletionPolicy Delete),
// then delegates the in-sync reconcile to the engine and writes the resulting
// status. It requeues with backoff on a transient error and on the periodic
// resync cadence otherwise.
func (r *KafkaUserReconciler) Reconcile(ctx context.Context, req ctrl.Request) (res ctrl.Result, retErr error) {
	logger := log.FromContext(ctx)
	defer recordReconcile(controllerKafkaUser, time.Now(), &retErr)

	var u v1alpha1.KafkaUser
	if err := r.Get(ctx, req.NamespacedName, &u); err != nil {
		// NotFound: the object is fully gone (finalizer already removed). The
		// Delete watch event always passes the event filter, so this branch
		// reliably fires after a deletion and is the safety net for the
		// finalizer-path metrics cleanup (review I12) — e.g. after an operator
		// restart between finalize and delete. Mirrors KafkaQuotaReconciler.
		if client.IgnoreNotFound(err) == nil {
			forgetUserMetrics(req.Namespace, req.Name)
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	r.observeManaged(&u)

	// Resolve the referenced cluster. clusterRef is namespace-local unless a
	// ClusterNamespace override is configured.
	clusterNS := r.ClusterNamespace
	if clusterNS == "" {
		clusterNS = u.Namespace
	}
	var cluster v1alpha1.KafkaCluster
	cerr := r.Get(ctx, types.NamespacedName{Namespace: clusterNS, Name: u.Spec.ClusterRef.Name}, &cluster)
	if cerr != nil {
		// Cluster not found (or unreadable): report Error + Ready False and
		// requeue. If the user is being deleted, fall through to deletion
		// handling so an orphaned user can still be finalized.
		if !u.DeletionTimestamp.IsZero() {
			return r.handleUnreachableDeletion(ctx, &u)
		}
		logger.Error(cerr, "resolving clusterRef", "cluster", u.Spec.ClusterRef.Name, "namespace", clusterNS)
		r.event(&u, corev1.EventTypeWarning, "ClusterNotFound", cerr.Error())
		if uerr := updateStatus(ctx, r.Client, req.NamespacedName, &u, func() {
			st := userErrorStatus(&u, "ClusterNotFound", cerr.Error())
			u.Status = &st
		}); uerr != nil {
			return ctrl.Result{}, errors.Join(cerr, uerr)
		}
		return ctrl.Result{}, cerr
	}

	// Per-identity lock (locks.go): everything below — the duplicate gate and
	// its quorum recheck, the engine's read-modify-write of the SCRAM
	// credential, and the deletion path's co-claimant scan → broker cleanup →
	// finalizer removal — must be atomic against same-identity rivals, so the
	// lock is taken here, right after cluster resolution (ResolvedUsername
	// resolves from the spec alone), and spans BOTH paths. A user holds no
	// substrate lock, so the identity → acl → rbac global order is trivially
	// respected. Released explicitly right after the engine returns on the
	// normal path; the deferred call backstops the error returns and the
	// deletion path (where it releases only after finalizer removal).
	unlockIdentity := lockIdentity(r.Locks, &cluster, "KafkaUser", operatorwebhook.ResolvedUsername(&u))
	defer unlockIdentity()

	// Duplicate-identity gate (the webhook-off backstop, see duplicate.go): if
	// an OLDER live KafkaUser claims the same (cluster, username) identity,
	// this CR goes terminal (ValidationFailed/DuplicateIdentity) instead of
	// flapping last-writer-wins on the underlying credential. Guarded to the
	// non-deletion path — a deleting loser must still reach its finalizer
	// below — and placed BEFORE the client build, so a loser never even
	// connects to the broker.
	if u.DeletionTimestamp.IsZero() {
		if res, done, err := r.duplicateIdentityGate(ctx, &u); done {
			return res, err
		}
	}

	// Build the live clients. On the deletion path a build failure is handled
	// specially (finalizer block / force-removal); on the normal path it is a
	// transient error. A user uses only the Kafka admin client.
	k, _, cleanup, berr := r.Clients.For(ctx, &cluster)
	if berr != nil {
		if !u.DeletionTimestamp.IsZero() {
			return r.handleUnreachableDeletion(ctx, &u)
		}
		logger.Error(berr, "building cluster clients")
		r.event(&u, corev1.EventTypeWarning, "ClientsBuildFailed", berr.Error())
		if uerr := updateStatus(ctx, r.Client, req.NamespacedName, &u, func() {
			st := userErrorStatus(&u, "ClientsBuildFailed", berr.Error())
			u.Status = &st
		}); uerr != nil {
			return ctrl.Result{}, errors.Join(berr, uerr)
		}
		return ctrl.Result{}, berr
	}
	defer cleanup()

	// Deletion path: clients built, so cleanup can run.
	if !u.DeletionTimestamp.IsZero() {
		return r.handleDeletionWithClient(ctx, &u, k)
	}

	// Not being deleted: ensure the finalizer is present before mutating Kafka,
	// so an interrupted reconcile still leaves cleanup possible.
	if !controllerutil.ContainsFinalizer(&u, FinalizerName) {
		controllerutil.AddFinalizer(&u, FinalizerName)
		if err := r.Update(ctx, &u); err != nil {
			return ctrl.Result{}, err
		}
	}

	// In-sync reconcile via the engine. The reconcile — including its live-state
	// reads, Secret writes, and Kafka mutations — runs exactly ONCE, before the
	// status write: a 409 Conflict on the write must retry only the write, never
	// re-mutate Kafka (review I9).
	st, rerr := reconcile.ReconcileUser(ctx, &u, &cluster, k, r.secretStore(&u))
	// Broker mutations are done: release the identity lock before the status
	// write below (which may retry on conflict) so a same-identity rival is
	// not held up by API-server latency.
	unlockIdentity()
	// Drift gauge semantic (review I12, mirrors KafkaQuotaReconciler): set it
	// from the freshly ENGINE-computed status whenever one exists, including
	// transient-error outcomes. KafkaUserStatus deliberately omits a Drift
	// struct (see reconcile.userStatusTarget), so this is keyed off the
	// DriftDetected condition — the same signal, sourced from what this kind's
	// status exposes (users are always-Enforce, so Phase == Drifted never
	// occurs, unlike role bindings).
	operator.SetUserDrift(u.Namespace, u.Name, userDriftDetected(&st))
	if uerr := updateStatus(ctx, r.Client, req.NamespacedName, &u, func() {
		u.Status = &st
	}); uerr != nil {
		return ctrl.Result{}, uerr
	}

	if rerr != nil {
		r.event(&u, corev1.EventTypeWarning, "ReconcileError", rerr.Error())
		return ctrl.Result{}, rerr // transient: requeue with backoff
	}
	r.event(&u, corev1.EventTypeNormal, "Reconciled", "user reconciled")
	return ctrl.Result{RequeueAfter: resyncInterval(r.ResyncInterval)}, nil
}

// handleUnreachableDeletion is the deletion path taken when no live Kafka
// client is available (the cluster CR is missing or the clients failed to
// build). It blocks finalizer removal unless force-removal is requested, in
// which case it removes the finalizer and lets Kubernetes garbage-collect the
// object (and, via its owner reference, the generated Secret).
func (r *KafkaUserReconciler) handleUnreachableDeletion(ctx context.Context, u *v1alpha1.KafkaUser) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(u, FinalizerName) {
		return ctrl.Result{}, nil // already finalized; nothing to do
	}

	if u.Annotations[AnnotationForceFinalizerRemoval] == "true" {
		r.finalizeEvent(u, corev1.EventTypeWarning, "ForceFinalizerRemoval",
			"removing finalizer without cluster-side cleanup (cluster unreachable)")
		return r.removeFinalizer(ctx, u)
	}

	// Cannot clean up and not forced: retain the finalizer and requeue so the
	// user finalizes once the cluster becomes reachable again.
	msg := "cluster unreachable; cannot run deletion cleanup. Make the cluster reachable, " +
		"or set annotation " + AnnotationForceFinalizerRemoval + "=true to force removal"
	r.finalizeEvent(u, corev1.EventTypeWarning, "DeletionBlocked", msg)
	return ctrl.Result{}, errors.New(msg) // requeue with backoff
}

// handleDeletionWithClient runs cluster-side cleanup per the user's
// deletionPolicy using the live Kafka client, then removes the finalizer.
//
//	Delete (default per defaulting.User — the credential is this CR's entire
//	reason to exist): remove the DECLARED mechanism's credential. Mirroring
//	quota exactly, the removal is UNGATED (no allow-destructive annotation:
//	deleting the CR is the explicit action) and BEST-EFFORT — a removal error
//	emits a Warning event but does NOT block finalizer removal; a wedged
//	namespace is worse than a possibly orphaned credential, and a re-created
//	CR's next reconcile would re-set it.
//	Orphan: leave the credential in place.
//
// EXCEPT when another live KafkaUser still claims the same (cluster,
// username, mechanism) credential (a duplicate-identity pair, either side
// being deleted): then the Delete path skips the broker cleanup and only
// removes the finalizer, orphaning the credential to the surviving claimant —
// otherwise deleting the losing duplicate (the natural remediation) would
// break the shared principal's authentication until the survivor's next
// resync. See findLiveUserCoClaimant and duplicate.go's Deletion path doc.
//
// The generated Secret (if any) needs no cleanup on either path: it carries a
// controller owner reference to this CR, so Kubernetes garbage-collects it
// once the finalizer is removed.
func (r *KafkaUserReconciler) handleDeletionWithClient(ctx context.Context, u *v1alpha1.KafkaUser, k kafka.AdminClient) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(u, FinalizerName) {
		return ctrl.Result{}, nil
	}

	// Resolve the declared identity exactly as the reconcile engine does
	// (username from metadata.name, mechanism SCRAM-SHA-512, deletionPolicy
	// Delete) — in-memory only; nothing is written back to the spec.
	defaulting.User(u)

	if u.Spec.DeletionPolicy == deletionPolicyDelete {
		co, cerr := findLiveUserCoClaimant(ctx, r.Client, u, r.ClusterNamespace == "")
		// Quorum recheck (duplicate.go): only when the cached scan found NO
		// live co-claimant — the branch about to DESTROY broker state —
		// re-scan through the uncached APIReader; a co-claimant the cache
		// missed flips the outcome to the fail-safe skip (leak, never destroy
		// a survivor's state). Finding a co-claimant needs no recheck:
		// skipping cleanup is already fail-safe.
		if cerr == nil && co == nil && r.APIReader != nil {
			co, cerr = findLiveUserCoClaimant(ctx, r.APIReader, u, r.ClusterNamespace == "")
		}
		if cerr != nil {
			// Transient: requeue with backoff. Guessing here could destroy a
			// surviving claimant's credential, so unlike the broker deletion
			// below the scan is NOT best-effort.
			r.finalizeEvent(u, corev1.EventTypeWarning, reasonDuplicateCheckFailed, cerr.Error())
			return ctrl.Result{}, cerr
		}
		if co != nil {
			msg := fmt.Sprintf("SCRAM credential %q (%s) left in place: live KafkaUser %s/%s still claims it and keeps managing it",
				u.Spec.Username, u.Spec.Mechanism, co.Namespace, co.Name)
			log.FromContext(ctx).Info("skipping SCRAM credential deletion: identity has a surviving claimant",
				"username", u.Spec.Username, "mechanism", u.Spec.Mechanism, "claimant", co.Namespace+"/"+co.Name)
			r.finalizeEvent(u, corev1.EventTypeNormal, reasonOrphanedToCoClaimant, msg)
			return r.removeFinalizer(ctx, u)
		}
		if err := k.DeleteScramCredential(ctx, u.Spec.Username, u.Spec.Mechanism); err != nil {
			// Best-effort: warn but still remove the finalizer (mirror quota).
			msg := "failed to delete SCRAM credential: " + err.Error()
			log.FromContext(ctx).Error(err, "SCRAM credential deletion failed",
				"username", u.Spec.Username, "mechanism", u.Spec.Mechanism)
			r.finalizeEvent(u, corev1.EventTypeWarning, "CredentialDeletionFailed", msg)
		} else {
			r.finalizeEvent(u, corev1.EventTypeNormal, "Deleted", "SCRAM credential deleted")
		}
	}
	// Orphan: leave the credential in place.

	return r.removeFinalizer(ctx, u)
}

// removeFinalizer drops the finalizer and persists it, letting Kubernetes
// garbage-collect the object (and its owner-referenced generated Secret). No
// requeue. This is the single deletion-success exit (the Delete and Orphan
// paths plus force-removal end here).
func (r *KafkaUserReconciler) removeFinalizer(ctx context.Context, u *v1alpha1.KafkaUser) (ctrl.Result, error) {
	controllerutil.RemoveFinalizer(u, FinalizerName)
	if err := r.Update(ctx, u); err != nil {
		return ctrl.Result{}, err
	}
	forgetUserMetrics(u.Namespace, u.Name)
	return ctrl.Result{}, nil
}

// managedUserKeys tracks which KafkaUsers have already been counted into the
// monedula_managed_users gauge so a re-reconcile of the same user does not
// double-count. Package-scoped because the gauge is process-global (mirrors
// managedQuotaKeys; review I12).
var managedUserKeys sync.Map

// observeManaged bumps the managed-users gauge the first time a given user is
// observed by this process. forgetUserMetrics undoes it on finalization so the
// gauge tracks CURRENTLY managed users.
func (r *KafkaUserReconciler) observeManaged(u *v1alpha1.KafkaUser) {
	key := u.Namespace + "/" + u.Name
	if _, loaded := managedUserKeys.LoadOrStore(key, struct{}{}); !loaded {
		operator.IncManagedUsers(1)
	}
}

// forgetUserMetrics is the single per-CR metrics cleanup hook for a KafkaUser
// that is gone: it un-counts the user from the managed-users gauge and drops
// its drift series so deleted CRs do not leak stale series. Called on the
// successful-finalization path (removeFinalizer) and, as a safety net, on the
// post-delete NotFound reconcile. LoadAndDelete makes the decrement idempotent.
func forgetUserMetrics(namespace, name string) {
	if _, loaded := managedUserKeys.LoadAndDelete(namespace + "/" + name); loaded {
		operator.IncManagedUsers(-1)
	}
	operator.DeleteUserDrift(namespace, name)
}

// userDriftDetected reports whether a user status records detected drift.
// KafkaUserStatus has no Drift struct, so the DriftDetected condition is the
// kind's drift signal (see the gauge comment in Reconcile).
func userDriftDetected(st *v1alpha1.KafkaUserStatus) bool {
	return st != nil && meta.IsStatusConditionTrue(st.Conditions, v1alpha1.CondDriftDetected)
}

// event emits a Kubernetes Event for obj on the reconcile path, a no-op when
// no Recorder is wired. See events.go for the action convention.
func (r *KafkaUserReconciler) event(obj *v1alpha1.KafkaUser, eventType, reason, msg string) {
	if r.Recorder == nil {
		return
	}
	r.Recorder.Eventf(obj, nil, eventType, reason, actionReconcile, "%s", msg)
}

// finalizeEvent is event for the deletion/finalizer path (actionFinalize).
func (r *KafkaUserReconciler) finalizeEvent(obj *v1alpha1.KafkaUser, eventType, reason, msg string) {
	if r.Recorder == nil {
		return
	}
	r.Recorder.Eventf(obj, nil, eventType, reason, actionFinalize, "%s", msg)
}

// secretStore builds the per-reconcile PasswordSecretStore for u — the
// reconcile engine's seam to password Secrets in u's namespace.
func (r *KafkaUserReconciler) secretStore(u *v1alpha1.KafkaUser) *userSecretStore {
	return &userSecretStore{client: r.Client, scheme: r.Scheme, owner: u}
}

// userSecretStore implements reconcile.PasswordSecretStore with the manager
// client. Reads go through the manager client, which reads Secrets UNCACHED
// (client.CacheOptions.DisableFor in manager.Run, §11.4) — the same read path
// operator.K8sResolver uses — so the resourceVersion driving referenced-mode
// rotation detection is the live apiserver value, taken from the SAME read as
// the password bytes.
type userSecretStore struct {
	client client.Client
	scheme *runtime.Scheme
	owner  *v1alpha1.KafkaUser
}

var _ reconcile.PasswordSecretStore = (*userSecretStore)(nil)

// GetSecret returns the named Secret in the owner's namespace, or (nil, nil)
// when absent. Owned reflects a CONTROLLER owner reference to the owning
// KafkaUser (UID-matched), the generate-mode adoption guard.
func (s *userSecretStore) GetSecret(ctx context.Context, name string) (*reconcile.PasswordSecret, error) {
	var sec corev1.Secret
	err := s.client.Get(ctx, types.NamespacedName{Namespace: s.owner.Namespace, Name: name}, &sec)
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &reconcile.PasswordSecret{
		Data:            sec.Data,
		ResourceVersion: sec.ResourceVersion,
		Owned:           metav1.IsControlledBy(&sec, s.owner),
	}, nil
}

// CreateOwnedSecret creates the named Secret with a controller owner reference
// to the KafkaUser (Kubernetes garbage-collects it with the CR) and the
// credential-source watch label: the manager's Secret informer is scoped to
// that label (§11.4), so labelling the generated Secret is what makes the
// Owns() watch actually see its events (a user deleting it triggers prompt
// regeneration instead of waiting out the resync).
func (s *userSecretStore) CreateOwnedSecret(ctx context.Context, name string, data map[string][]byte) error {
	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: s.owner.Namespace,
			Labels:    map[string]string{CredentialSourceLabel: CredentialSourceLabelValue},
		},
		Data: data,
	}
	if err := controllerutil.SetControllerReference(s.owner, sec, s.scheme); err != nil {
		return err
	}
	return s.client.Create(ctx, sec)
}

// UpdateOwnedSecret replaces the named Secret's data, refusing to touch a
// Secret not controller-owned by the KafkaUser (defense in depth: the engine
// only calls this for Secrets it already observed as owned, but a concurrent
// replacement must not be clobbered).
func (s *userSecretStore) UpdateOwnedSecret(ctx context.Context, name string, data map[string][]byte) error {
	var sec corev1.Secret
	if err := s.client.Get(ctx, types.NamespacedName{Namespace: s.owner.Namespace, Name: name}, &sec); err != nil {
		return err
	}
	if !metav1.IsControlledBy(&sec, s.owner) {
		return fmt.Errorf("secret %q is not owned by KafkaUser %s/%s; refusing to update",
			name, s.owner.Namespace, s.owner.Name)
	}
	sec.Data = data
	if sec.Labels == nil {
		sec.Labels = map[string]string{}
	}
	sec.Labels[CredentialSourceLabel] = CredentialSourceLabelValue
	return s.client.Update(ctx, &sec)
}

// mapSecretToUsers enqueues the KafkaUsers affected by a changed Secret via
// TWO fan-outs (§11.4):
//
//  1. users whose spec.password.valueFrom.secretKeyRef names the Secret
//     (via UserPasswordSecretNamesIndex) — the event-driven password-rotation
//     trigger. Password refs are namespace-local, so the list is always
//     scoped to the Secret's namespace.
//  2. users on any cluster (in the Secret's namespace) that references the
//     changed credential/TLS Secret — the same 2nd-hop fan-out as
//     mapSecretToQuotas.
//
// Generated Secrets are NOT mapped here: they carry a controller owner
// reference, so the Owns() watch enqueues their KafkaUser directly.
func (r *KafkaUserReconciler) mapSecretToUsers(ctx context.Context, obj client.Object) []ctrlreconcile.Request {
	if _, ok := obj.(*corev1.Secret); !ok {
		return nil
	}

	seen := map[types.NamespacedName]struct{}{}
	var out []ctrlreconcile.Request
	add := func(ns, name string) {
		key := types.NamespacedName{Namespace: ns, Name: name}
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		out = append(out, ctrlreconcile.Request{NamespacedName: key})
	}

	// 1st fan-out: direct password references (index lookup).
	var refs v1alpha1.KafkaUserList
	if err := r.List(ctx, &refs,
		client.InNamespace(obj.GetNamespace()),
		client.MatchingFields{UserPasswordSecretNamesIndex: obj.GetName()}); err == nil {
		for i := range refs.Items {
			add(refs.Items[i].Namespace, refs.Items[i].Name)
		}
	}

	// 2nd fan-out: cluster credential/TLS Secrets → users on those clusters.
	clusterNames := clustersReferencingSecret(ctx, r.Client, obj)
	if len(clusterNames) == 0 {
		return out
	}
	want := make(map[string]struct{}, len(clusterNames))
	for _, n := range clusterNames {
		want[n] = struct{}{}
	}
	var listOpts []client.ListOption
	if r.ClusterNamespace == "" {
		listOpts = append(listOpts, client.InNamespace(obj.GetNamespace()))
	}
	var list v1alpha1.KafkaUserList
	if err := r.List(ctx, &list, listOpts...); err != nil {
		return out
	}
	for i := range list.Items {
		if _, ok := want[list.Items[i].Spec.ClusterRef.Name]; ok {
			add(list.Items[i].Namespace, list.Items[i].Name)
		}
	}
	return out
}

// SetupWithManager registers the reconciler with the manager, watching
// KafkaUser objects. watchEventFilter drops status-only updates so a reconcile
// does not re-enqueue itself; spec/annotation/lifecycle changes and the
// periodic RequeueAfter still trigger.
//
// Owns(&corev1.Secret{}) enqueues the owning KafkaUser for events on its
// GENERATED credentials Secret (owner-reference based — the idiomatic path for
// operator-owned children); deleting that Secret is the explicit rotation
// request and regenerates promptly. The unowned-Secret Watches map-func covers
// REFERENCED password Secrets and cluster credential Secrets. Note the
// manager's Secret informer is label-scoped to credential-source=true (§11.4):
// the store labels every generated Secret, and referenced password Secrets
// rotate promptly only when labelled — unlabelled ones are picked up by the
// periodic resync (same trade-off as cluster credential Secrets).
// The KafkaCluster watch (review I2) recovers users promptly when their
// cluster CR appears or its spec is fixed, instead of waiting out error backoff.
func (r *KafkaUserReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Recorder == nil {
		r.Recorder = mgr.GetEventRecorder("kafkauser-controller")
	}
	// Insurance against silently running unlocked / unrechecked: nil Locks
	// skips the per-identity serialization, nil APIReader the duplicate-gate
	// quorum recheck (locks.go, duplicate.go). Expected only in tests —
	// manager.Run always injects both.
	if r.Locks == nil {
		mgr.GetLogger().Info("identity locking disabled (nil Locks registry); expected only in tests",
			"controller", "kafkauser")
	}
	if r.APIReader == nil {
		mgr.GetLogger().Info("duplicate-gate quorum recheck disabled (nil APIReader); expected only in tests",
			"controller", "kafkauser")
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.KafkaUser{}, builder.WithPredicates(watchEventFilter())).
		Owns(&corev1.Secret{}).
		Watches(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(r.mapSecretToUsers)).
		Watches(&v1alpha1.KafkaCluster{}, handler.EnqueueRequestsFromMapFunc(r.mapClusterToUsers),
			builder.WithPredicates(clusterWatchPredicate())).
		WithOptions(reconcilerOptions(r.MaxConcurrentReconciles)).
		Complete(r)
}

// userErrorStatus builds an Error status with Ready False (reason) for a
// pre-engine failure (cluster lookup or client build). It seeds conditions —
// and the password-tracking fields — from the existing status so
// LastTransitionTime and rotation state are preserved across requeues.
func userErrorStatus(u *v1alpha1.KafkaUser, reason, msg string) v1alpha1.KafkaUserStatus {
	now := metav1.Now()
	st := v1alpha1.KafkaUserStatus{
		ObservedGeneration: u.Generation,
		Phase:              v1alpha1.PhaseError,
		LastCheckedTime:    &now,
	}
	if u.Status != nil {
		st.Conditions = append([]metav1.Condition(nil), u.Status.Conditions...)
		st.AppliedPasswordRef = u.Status.AppliedPasswordRef.DeepCopy()
		st.GeneratedSecretName = u.Status.GeneratedSecretName
	}
	meta.SetStatusCondition(&st.Conditions, metav1.Condition{
		Type:               v1alpha1.CondReady,
		Status:             metav1.ConditionFalse,
		Reason:             reason,
		Message:            msg,
		ObservedGeneration: u.Generation,
	})
	return st
}
