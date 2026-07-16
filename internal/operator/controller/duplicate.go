// Duplicate-identity gate (v0.36): the reconciler-side, eventually-consistent
// backstop for broker-identity uniqueness across the four kinds that own a
// single broker identity — KafkaTopic (cluster, topicName), KafkaQuota
// (cluster, entity), KafkaUser (cluster, username), and KafkaRoleBinding (the
// compiled MDS binding identity set). KafkaAccessPolicy is deliberately
// excluded: a policy has no single broker identity (its ACL tuples are
// legitimately co-owned, handled by the cluster-wide view + ACLConflict).
//
// The admission webhook (when enabled) rejects duplicates before they are
// persisted. In the DEFAULT install (webhooks off) — and in the webhook-on
// install's TOCTOU cache-lag window — two CRs claiming the same identity used
// to flap last-writer-wins forever: each reconcile overwrote the other's
// broker state every resync. This gate closes that gap:
//
//   - Before any live-state read or broker mutation, each reconciler lists the
//     same-kind CRs on the same cluster (the same in-process filter scope as
//     buildClusterACLView; see the scan-scope note below), resolves their
//     identities with the SAME exported webhook helpers the validators use,
//     and checks whether another live (non-deleting) CR holds the same
//     identity and is OLDER — earlier creationTimestamp; tiebreak:
//     lexicographically smaller namespace/name.
//   - If so, THIS CR is the loser and goes terminal, mirroring the engine's
//     tenancy-denied pattern exactly: Phase Error, ValidationFailed=True with
//     reason DuplicateIdentity (message naming the winner), Ready=False, a
//     Warning event, NO broker mutation, and a nil error (no requeue backoff;
//     the periodic resync re-evaluates).
//   - The OLDER CR never checks "am I duplicated by someone newer" — only the
//     newer claimant loses, so the winner's reconcile path is entirely
//     unaffected by the conflict.
//
// # Deletion path
//
// The gate NEVER runs on the deletion path: every call site is guarded by
// DeletionTimestamp.IsZero(), so a deleting loser still reaches its finalizer
// (cleanup must never be blocked by a duplicate). Conversely, a rival with a
// non-zero DeletionTimestamp never blocks others — it is skipped as a
// claimant, so the surviving CR takes over the moment the winner is marked
// for deletion (note the asymmetry with the webhook, which treats a deleting
// CR as still occupying the identity: at admission time re-claiming early is
// the user's race to lose, while at reconcile time the finalizer's own
// cleanup ordering already serializes the handover).
//
// Reaching the finalizer must not mean DESTROYING the shared identity,
// though: for the two kinds whose finalizer default-deletes broker state
// (KafkaUser credentials, KafkaQuota entity quotas), the deletion handlers
// first run the co-claimant scan below (findLiveUserCoClaimant /
// findLiveQuotaCoClaimant) and, when ANY other live (non-deleting) CR still
// claims the identity, remove the finalizer WITHOUT the broker cleanup —
// otherwise deleting the loser of a pre-existing duplicate pair (the natural
// remediation) would delete the credential/quota out from under the winner.
// Unlike the gate, the scan is deliberately age-blind: deleting the WINNER
// while a loser waits must also orphan the broker state to the surviving
// claimant (the loser takes over at its next resync and must not find the
// identity destroyed). KafkaRoleBinding is already protected by the cross-CR
// co-ownership shield, and KafkaTopic by allow-delete + the Orphan default.
//
// # Loser recovery latency
//
// When the winning CR is deleted, nothing re-triggers the loser promptly:
// there is no same-kind cross-CR watch (each controller watches only its own
// primary objects plus Secrets/ConfigMaps/KafkaClusters), so the loser
// recovers at its next periodic resync (RequeueAfter, the configured
// --resync-interval — default 5 minutes) at worst. This is an accepted
// trade-off — a duplicate identity is a configuration error, not a hot path —
// and is documented in docs/operator.md.
//
// # Quorum recheck (D1, v0.37)
//
// The scans above run against the manager's informer cache; cache lag can hide
// a rival that already exists at quorum (two young CRs racing each other's
// creation) or keep showing one that was just deleted. On the CONTESTED paths
// only — never on the steady-state hot path — the gate re-runs the SAME scan
// through the reconciler's APIReader (mgr.GetAPIReader(), an uncached quorum
// read) and takes the recheck result as authoritative:
//
//   - loser path: the cached scan found an older rival. Rechecking avoids going
//     terminal on a rival that is already gone at quorum (and picks up an even
//     older one the cache missed).
//   - never-materialized path: the cached scan found NO rival, but this CR has
//     never proven a successful reconcile (identityMaterialized) — exactly the
//     young-CR-races-its-rival window where cache lag is dangerous. Once a CR
//     has been Ready, its identity is established on the broker and the loser
//     path recheck of any future rival covers the pair, so established CRs
//     never pay an apiserver round-trip.
//   - deletion co-claimant scans: rechecked only when the cached scan found NO
//     live co-claimant — i.e. when the finalizer is about to DESTROY broker
//     state; a quorum-visible co-claimant the cache missed then flips the
//     outcome to the fail-safe skip. Finding a co-claimant needs no recheck:
//     skipping cleanup is already the leak-never-destroy direction.
//
// The recheck runs while the reconcile HOLDS its per-identity lock (locks.go),
// so gate + recheck + mutation are atomic against same-identity rivals
// in-process; the quorum read closes the cross-cache-lag window. A nil
// APIReader (unit tests, the plain-client envtest harness) skips every
// recheck — the cached answer stands, which is exactly the pre-D1 behavior.
//
// # Scan scope (why no field-indexed List)
//
// The scan filters in-process on spec.clusterRef.name (namespace-scoped when
// clusterRef is namespace-local) instead of using the manager cache's field
// indexes (webhook.ClusterRefNameIndex et al.). This deliberately mirrors
// buildClusterACLView / buildClusterRoleBindingView. The binding constraint is
// the envtest suite (suite_envtest_test.go), which drives the reconcilers with
// a plain client.New client — no manager, no cache, no field indexes
// registered — so client.MatchingFields would fail there; fake unit-test
// clients are not the issue (fake.NewClientBuilder can register indexes via
// WithIndex, as several other tests in this package do). Under the real
// manager the List is served from the cache either way, so the in-process
// filter scans exactly the same candidate set an index would return.
package controller

import (
	"context"
	"errors"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/defaulting"
	"github.com/monedula-dev/monedula-gitops/internal/operator"
	operatorwebhook "github.com/monedula-dev/monedula-gitops/internal/operator/webhook"
	"github.com/monedula-dev/monedula-gitops/internal/rbac"
)

// reasonDuplicateIdentity is the ValidationFailed / Ready reason (and Warning
// event reason) set on the NEWER of two CRs claiming the same broker identity.
const reasonDuplicateIdentity = "DuplicateIdentity"

// reasonDuplicateCheckFailed is the Ready reason for a transient failure of
// the duplicate scan itself (a List error). Requeued with backoff like every
// other pre-engine failure.
const reasonDuplicateCheckFailed = "DuplicateCheckFailed"

// olderIdentityClaim reports whether a's claim on a contested identity
// precedes b's: earlier creationTimestamp wins; on equal timestamps (the
// apiserver stores them with 1s granularity) the lexicographically smaller
// namespace/name wins. This is a strict total order over distinct objects, so
// exactly one CR of any duplicate set is the winner.
func olderIdentityClaim(a, b client.Object) bool {
	at, bt := a.GetCreationTimestamp(), b.GetCreationTimestamp()
	if !at.Equal(&bt) {
		return at.Before(&bt)
	}
	return a.GetNamespace()+"/"+a.GetName() < b.GetNamespace()+"/"+b.GetName()
}

// oldestRival returns the rival that beats obj in the identity contest — the
// OLDEST (per olderIdentityClaim) among the rivals that precede obj — or nil
// when obj itself precedes every rival (obj is the winner and reconciles
// normally). rivals must already be filtered to live (non-deleting),
// same-identity, same-cluster CRs excluding obj itself.
func oldestRival(obj client.Object, rivals []client.Object) client.Object {
	var winner client.Object
	for _, r := range rivals {
		if !olderIdentityClaim(r, obj) {
			continue // r is newer than obj: r loses to obj, not vice versa
		}
		if winner == nil || olderIdentityClaim(r, winner) {
			winner = r
		}
	}
	return winner
}

// duplicateIdentityMessage renders the terminal-condition message for a losing
// CR. identity is the rendered identity tuple, e.g. `(cluster "prod", topic
// "payments.orders")`.
func duplicateIdentityMessage(identity, winnerKind, winnerNS, winnerName string) string {
	return fmt.Sprintf(
		"identity %s is already managed by %s %s/%s (older); this resource is ignored until the conflict is resolved",
		identity, winnerKind, winnerNS, winnerName)
}

// identityMaterialized reports whether a CR's status proves it has completed a
// successful reconcile at least once: the Ready condition is currently True.
// It is the skip predicate for the never-materialized quorum recheck (see the
// package doc): Ready=True is only ever written after the engine converged the
// broker state, at which point the identity is certainly claimed on the broker
// and the young-CR cache-lag window is closed. The predicate deliberately errs
// toward rechecking — a CR whose Ready later flipped False (transient error,
// terminal outcome, never-yet-reconciled) cannot PROVE materialization from
// its current status, so it pays one quorum List per reconcile; that is never
// the steady-state hot path, and the fail-safe direction (an extra read, never
// a missed rival) is the right one.
func identityMaterialized(conds []metav1.Condition) bool {
	return meta.IsStatusConditionTrue(conds, v1alpha1.CondReady)
}

// --- KafkaTopic ---

// findOlderTopicClaim returns the OLDER live KafkaTopic claiming the same
// (cluster, topicName) identity as topic, or nil when topic is the oldest
// claimant (or unique). namespaceLocal follows the operator convention: with
// no --cluster-namespace override, clusterRef is namespace-local, so only
// same-namespace CRs can contest the identity (the same scope the webhook's
// clusterNamespaceFor comparison implements). c is a plain client.Reader (like
// every finder in this file) so the same scan runs against either the cached
// client or the quorum APIReader; the in-process clusterRef filtering (see the
// scan-scope note) needs no field indexes, which the APIReader lacks anyway.
func findOlderTopicClaim(ctx context.Context, c client.Reader, topic *v1alpha1.KafkaTopic, namespaceLocal bool) (*v1alpha1.KafkaTopic, error) {
	var listOpts []client.ListOption
	if namespaceLocal {
		listOpts = append(listOpts, client.InNamespace(topic.Namespace))
	}
	var list v1alpha1.KafkaTopicList
	if err := c.List(ctx, &list, listOpts...); err != nil {
		return nil, fmt.Errorf("listing KafkaTopics for duplicate-identity check: %w", err)
	}
	want := operatorwebhook.ResolvedTopicName(topic)
	var rivals []client.Object
	for i := range list.Items {
		other := &list.Items[i]
		if other.UID == topic.UID || !other.DeletionTimestamp.IsZero() {
			continue // self, or a deleting CR (never blocks others)
		}
		if other.Spec.ClusterRef.Name != topic.Spec.ClusterRef.Name {
			continue
		}
		if operatorwebhook.ResolvedTopicName(other) != want {
			continue
		}
		rivals = append(rivals, other)
	}
	if w := oldestRival(topic, rivals); w != nil {
		return w.(*v1alpha1.KafkaTopic), nil
	}
	return nil, nil
}

// duplicateIdentityGate is the KafkaTopic call site of the duplicate-identity
// check. It returns done=true when the reconcile is finished here — either
// terminally (an older CR owns the identity; res carries the resync
// RequeueAfter) or transiently (the scan failed; err requeues with backoff).
// done=false means no older claimant: proceed with the normal reconcile.
// MUST only be called on the non-deletion path (see the package doc).
func (r *KafkaTopicReconciler) duplicateIdentityGate(ctx context.Context, topic *v1alpha1.KafkaTopic) (ctrl.Result, bool, error) {
	winner, derr := findOlderTopicClaim(ctx, r.Client, topic, r.ClusterNamespace == "")
	// Quorum recheck on the contested paths only (see the package doc): a
	// cached rival (loser path) or a CR that cannot prove a past successful
	// reconcile (never-materialized path) re-runs the same scan uncached and
	// takes that result as authoritative. Established CRs with no cached
	// rival — the steady-state hot path — never reach the APIReader.
	if derr == nil && r.APIReader != nil &&
		(winner != nil || topic.Status == nil || !identityMaterialized(topic.Status.Conditions)) {
		winner, derr = findOlderTopicClaim(ctx, r.APIReader, topic, r.ClusterNamespace == "")
	}
	if derr != nil {
		r.event(topic, corev1.EventTypeWarning, reasonDuplicateCheckFailed, derr.Error())
		if uerr := updateStatus(ctx, r.Client, client.ObjectKeyFromObject(topic), topic, func() {
			st := topicErrorStatus(topic, reasonDuplicateCheckFailed, derr.Error())
			topic.Status = &st
		}); uerr != nil {
			return ctrl.Result{}, true, errors.Join(derr, uerr)
		}
		return ctrl.Result{}, true, derr // transient: requeue with backoff
	}
	if winner == nil {
		return ctrl.Result{}, false, nil
	}
	msg := duplicateIdentityMessage(
		fmt.Sprintf("(cluster %q, topic %q)", topic.Spec.ClusterRef.Name, operatorwebhook.ResolvedTopicName(topic)),
		"KafkaTopic", winner.Namespace, winner.Name)
	r.event(topic, corev1.EventTypeWarning, reasonDuplicateIdentity, msg)
	operator.RecordReconcileTerminal(controllerKafkaTopic, reasonDuplicateIdentity)
	// The loser's drift is not evaluated (the engine never runs), so clear the
	// gauge rather than let a value from a previous winning reconcile linger —
	// the same value the engine's own terminal outcomes yield (st.Drift nil).
	operator.SetTopicDrift(topic.Namespace, topic.Name, false)
	if uerr := updateStatus(ctx, r.Client, client.ObjectKeyFromObject(topic), topic, func() {
		st := topicErrorStatus(topic, reasonDuplicateIdentity, msg)
		meta.SetStatusCondition(&st.Conditions, metav1.Condition{
			Type:               v1alpha1.CondValidationFailed,
			Status:             metav1.ConditionTrue,
			Reason:             reasonDuplicateIdentity,
			Message:            msg,
			ObservedGeneration: topic.Generation,
		})
		topic.Status = &st
	}); uerr != nil {
		return ctrl.Result{}, true, uerr
	}
	// Terminal: nil error, resync-cadence requeue only. The loser recovers at
	// the next resync (or KafkaCluster watch event) after the winner goes away.
	return ctrl.Result{RequeueAfter: resyncInterval(r.ResyncInterval)}, true, nil
}

// --- KafkaQuota ---

// findOlderQuotaClaim returns the OLDER live KafkaQuota claiming the same
// (cluster, entity) identity as q, or nil when q is the oldest claimant.
func findOlderQuotaClaim(ctx context.Context, c client.Reader, q *v1alpha1.KafkaQuota, namespaceLocal bool) (*v1alpha1.KafkaQuota, error) {
	var listOpts []client.ListOption
	if namespaceLocal {
		listOpts = append(listOpts, client.InNamespace(q.Namespace))
	}
	var list v1alpha1.KafkaQuotaList
	if err := c.List(ctx, &list, listOpts...); err != nil {
		return nil, fmt.Errorf("listing KafkaQuotas for duplicate-identity check: %w", err)
	}
	want := operatorwebhook.ResolvedEntityKey(q)
	var rivals []client.Object
	for i := range list.Items {
		other := &list.Items[i]
		if other.UID == q.UID || !other.DeletionTimestamp.IsZero() {
			continue
		}
		if other.Spec.ClusterRef.Name != q.Spec.ClusterRef.Name {
			continue
		}
		if operatorwebhook.ResolvedEntityKey(other) != want {
			continue
		}
		rivals = append(rivals, other)
	}
	if w := oldestRival(q, rivals); w != nil {
		return w.(*v1alpha1.KafkaQuota), nil
	}
	return nil, nil
}

// duplicateIdentityGate is the KafkaQuota call site of the duplicate-identity
// check; semantics identical to the KafkaTopic gate.
func (r *KafkaQuotaReconciler) duplicateIdentityGate(ctx context.Context, q *v1alpha1.KafkaQuota) (ctrl.Result, bool, error) {
	winner, derr := findOlderQuotaClaim(ctx, r.Client, q, r.ClusterNamespace == "")
	// Quorum recheck on the contested paths only; see the KafkaTopic gate.
	if derr == nil && r.APIReader != nil &&
		(winner != nil || q.Status == nil || !identityMaterialized(q.Status.Conditions)) {
		winner, derr = findOlderQuotaClaim(ctx, r.APIReader, q, r.ClusterNamespace == "")
	}
	if derr != nil {
		r.event(q, corev1.EventTypeWarning, reasonDuplicateCheckFailed, derr.Error())
		if uerr := updateStatus(ctx, r.Client, client.ObjectKeyFromObject(q), q, func() {
			st := quotaErrorStatus(q, reasonDuplicateCheckFailed, derr.Error())
			q.Status = &st
		}); uerr != nil {
			return ctrl.Result{}, true, errors.Join(derr, uerr)
		}
		return ctrl.Result{}, true, derr
	}
	if winner == nil {
		return ctrl.Result{}, false, nil
	}
	msg := duplicateIdentityMessage(
		fmt.Sprintf("(cluster %q, entity %q)", q.Spec.ClusterRef.Name, operatorwebhook.ResolvedEntityKey(q)),
		"KafkaQuota", winner.Namespace, winner.Name)
	r.event(q, corev1.EventTypeWarning, reasonDuplicateIdentity, msg)
	operator.RecordReconcileTerminal(controllerKafkaQuota, reasonDuplicateIdentity)
	operator.SetQuotaDrift(q.Namespace, q.Name, false) // see the topic gate's gauge note
	if uerr := updateStatus(ctx, r.Client, client.ObjectKeyFromObject(q), q, func() {
		st := quotaErrorStatus(q, reasonDuplicateIdentity, msg)
		meta.SetStatusCondition(&st.Conditions, metav1.Condition{
			Type:               v1alpha1.CondValidationFailed,
			Status:             metav1.ConditionTrue,
			Reason:             reasonDuplicateIdentity,
			Message:            msg,
			ObservedGeneration: q.Generation,
		})
		q.Status = &st
	}); uerr != nil {
		return ctrl.Result{}, true, uerr
	}
	return ctrl.Result{RequeueAfter: resyncInterval(r.ResyncInterval)}, true, nil
}

// --- KafkaUser ---

// findOlderUserClaim returns the OLDER live KafkaUser claiming the same
// (cluster, username) identity as u, or nil when u is the oldest claimant.
func findOlderUserClaim(ctx context.Context, c client.Reader, u *v1alpha1.KafkaUser, namespaceLocal bool) (*v1alpha1.KafkaUser, error) {
	var listOpts []client.ListOption
	if namespaceLocal {
		listOpts = append(listOpts, client.InNamespace(u.Namespace))
	}
	var list v1alpha1.KafkaUserList
	if err := c.List(ctx, &list, listOpts...); err != nil {
		return nil, fmt.Errorf("listing KafkaUsers for duplicate-identity check: %w", err)
	}
	want := operatorwebhook.ResolvedUsername(u)
	var rivals []client.Object
	for i := range list.Items {
		other := &list.Items[i]
		if other.UID == u.UID || !other.DeletionTimestamp.IsZero() {
			continue
		}
		if other.Spec.ClusterRef.Name != u.Spec.ClusterRef.Name {
			continue
		}
		if operatorwebhook.ResolvedUsername(other) != want {
			continue
		}
		rivals = append(rivals, other)
	}
	if w := oldestRival(u, rivals); w != nil {
		return w.(*v1alpha1.KafkaUser), nil
	}
	return nil, nil
}

// duplicateIdentityGate is the KafkaUser call site of the duplicate-identity
// check; semantics identical to the KafkaTopic gate.
func (r *KafkaUserReconciler) duplicateIdentityGate(ctx context.Context, u *v1alpha1.KafkaUser) (ctrl.Result, bool, error) {
	winner, derr := findOlderUserClaim(ctx, r.Client, u, r.ClusterNamespace == "")
	// Quorum recheck on the contested paths only; see the KafkaTopic gate.
	if derr == nil && r.APIReader != nil &&
		(winner != nil || u.Status == nil || !identityMaterialized(u.Status.Conditions)) {
		winner, derr = findOlderUserClaim(ctx, r.APIReader, u, r.ClusterNamespace == "")
	}
	if derr != nil {
		r.event(u, corev1.EventTypeWarning, reasonDuplicateCheckFailed, derr.Error())
		if uerr := updateStatus(ctx, r.Client, client.ObjectKeyFromObject(u), u, func() {
			st := userErrorStatus(u, reasonDuplicateCheckFailed, derr.Error())
			u.Status = &st
		}); uerr != nil {
			return ctrl.Result{}, true, errors.Join(derr, uerr)
		}
		return ctrl.Result{}, true, derr
	}
	if winner == nil {
		return ctrl.Result{}, false, nil
	}
	msg := duplicateIdentityMessage(
		fmt.Sprintf("(cluster %q, username %q)", u.Spec.ClusterRef.Name, operatorwebhook.ResolvedUsername(u)),
		"KafkaUser", winner.Namespace, winner.Name)
	r.event(u, corev1.EventTypeWarning, reasonDuplicateIdentity, msg)
	operator.RecordReconcileTerminal(controllerKafkaUser, reasonDuplicateIdentity)
	operator.SetUserDrift(u.Namespace, u.Name, false) // see the topic gate's gauge note
	if uerr := updateStatus(ctx, r.Client, client.ObjectKeyFromObject(u), u, func() {
		st := userErrorStatus(u, reasonDuplicateIdentity, msg)
		meta.SetStatusCondition(&st.Conditions, metav1.Condition{
			Type:               v1alpha1.CondValidationFailed,
			Status:             metav1.ConditionTrue,
			Reason:             reasonDuplicateIdentity,
			Message:            msg,
			ObservedGeneration: u.Generation,
		})
		// KafkaUserStatus has no Drift struct (see userDriftDetected): the
		// DriftDetected condition IS this kind's drift signal, so — mirroring
		// the gauge zeroing above — it must be explicitly cleared here too. A
		// loser that carries a pre-existing DriftDetected=True from before it
		// lost the identity contest would otherwise leave that condition
		// dangling: userErrorStatus only forward-copies prior conditions, it
		// never clears this one, and the engine that normally maintains it
		// never runs on the loser path.
		meta.SetStatusCondition(&st.Conditions, metav1.Condition{
			Type:               v1alpha1.CondDriftDetected,
			Status:             metav1.ConditionFalse,
			Reason:             reasonDuplicateIdentity,
			Message:            "drift not evaluated: resource is a duplicate-identity loser",
			ObservedGeneration: u.Generation,
		})
		u.Status = &st
	}); uerr != nil {
		return ctrl.Result{}, true, uerr
	}
	return ctrl.Result{RequeueAfter: resyncInterval(r.ResyncInterval)}, true, nil
}

// --- KafkaRoleBinding ---

// findOlderRoleBindingClaim returns the OLDER live KafkaRoleBinding whose
// compiled MDS bindings overlap (by FullKey) with rb's — the same identity
// tuple set the webhook's uniqueness scan compares — plus the first
// overlapping FullKey (for the terminal message), or nil when rb is the
// oldest claimant. mdsCfg is the resolved cluster's authorization.mds; a nil
// config or a compile error on rb SKIPS the check (no gate): the reconcile
// engine surfaces those as its own terminal outcomes (MDSNotConfigured /
// ValidationFailed), and without a compiled identity there is nothing to
// contest. A rival that fails to compile is skipped for the same reason.
func findOlderRoleBindingClaim(ctx context.Context, c client.Reader, rb *v1alpha1.KafkaRoleBinding, namespaceLocal bool, mdsCfg *v1alpha1.MDSConfig) (*v1alpha1.KafkaRoleBinding, string, error) {
	if mdsCfg == nil {
		return nil, "", nil
	}
	mine, cerr := rbac.Compile(rb, mdsCfg)
	if cerr != nil {
		return nil, "", nil // defer to the engine's terminal ValidationFailed
	}
	myKeys := make(map[string]struct{}, len(mine))
	for _, b := range mine {
		myKeys[b.FullKey()] = struct{}{}
	}

	var listOpts []client.ListOption
	if namespaceLocal {
		listOpts = append(listOpts, client.InNamespace(rb.Namespace))
	}
	var list v1alpha1.KafkaRoleBindingList
	if err := c.List(ctx, &list, listOpts...); err != nil {
		return nil, "", fmt.Errorf("listing KafkaRoleBindings for duplicate-identity check: %w", err)
	}

	var rivals []client.Object
	overlapKey := make(map[*v1alpha1.KafkaRoleBinding]string)
	for i := range list.Items {
		other := &list.Items[i]
		if other.UID == rb.UID || !other.DeletionTimestamp.IsZero() {
			continue
		}
		if other.Spec.ClusterRef.Name != rb.Spec.ClusterRef.Name {
			continue
		}
		otherBindings, oerr := rbac.Compile(other, mdsCfg)
		if oerr != nil {
			continue // uncompilable rival claims nothing
		}
		for _, ob := range otherBindings {
			if _, ok := myKeys[ob.FullKey()]; ok {
				rivals = append(rivals, other)
				overlapKey[other] = ob.FullKey()
				break
			}
		}
	}
	if w := oldestRival(rb, rivals); w != nil {
		won := w.(*v1alpha1.KafkaRoleBinding)
		return won, overlapKey[won], nil
	}
	return nil, "", nil
}

// roleBindingLockIdentity is the per-identity LOCK key (locks.go) for a
// KafkaRoleBinding. The kind's contested identity is a compiled binding SET,
// but every compiled FullKey embeds the CR-level (principal, role, scope.type)
// triple — the MDS cluster ids are shared by all CRs on one KafkaCluster and
// only the resource pattern varies — so any FullKey overlap between two CRs
// implies an equal triple. Locking on the triple is therefore a sound (merely
// coarser) over-approximation of the compiled identity: all potential rivals
// share the lock, it is derivable from the spec alone (no MDS config, no
// compile), and a reconcile still takes exactly ONE identity lock.
func roleBindingLockIdentity(rb *v1alpha1.KafkaRoleBinding) string {
	return rb.Spec.Principal + "\x00" + rb.Spec.Role + "\x00" + rb.Spec.Scope.Type
}

// duplicateIdentityGate is the KafkaRoleBinding call site of the
// duplicate-identity check; semantics identical to the KafkaTopic gate.
// mdsCfg may be nil (no authorization.mds on the cluster), in which case the
// gate is a no-op and the reconcile proceeds to its MDSNotConfigured handling.
func (r *KafkaRoleBindingReconciler) duplicateIdentityGate(ctx context.Context, rb *v1alpha1.KafkaRoleBinding, mdsCfg *v1alpha1.MDSConfig) (ctrl.Result, bool, error) {
	winner, key, derr := findOlderRoleBindingClaim(ctx, r.Client, rb, r.ClusterNamespace == "", mdsCfg)
	// Quorum recheck on the contested paths only; see the KafkaTopic gate. A
	// nil mdsCfg or an uncompilable rb makes the finder a no-op either way, so
	// the never-materialized trigger costs nothing on unconfigured clusters.
	if derr == nil && r.APIReader != nil &&
		(winner != nil || rb.Status == nil || !identityMaterialized(rb.Status.Conditions)) {
		winner, key, derr = findOlderRoleBindingClaim(ctx, r.APIReader, rb, r.ClusterNamespace == "", mdsCfg)
	}
	if derr != nil {
		r.event(rb, corev1.EventTypeWarning, reasonDuplicateCheckFailed, derr.Error())
		if uerr := updateStatus(ctx, r.Client, client.ObjectKeyFromObject(rb), rb, func() {
			st := roleBindingErrorStatus(rb, reasonDuplicateCheckFailed, derr.Error())
			rb.Status = &st
		}); uerr != nil {
			return ctrl.Result{}, true, errors.Join(derr, uerr)
		}
		return ctrl.Result{}, true, derr
	}
	if winner == nil {
		return ctrl.Result{}, false, nil
	}
	msg := duplicateIdentityMessage(
		fmt.Sprintf("(cluster %q, binding %q)", rb.Spec.ClusterRef.Name, key),
		"KafkaRoleBinding", winner.Namespace, winner.Name)
	r.event(rb, corev1.EventTypeWarning, reasonDuplicateIdentity, msg)
	operator.RecordReconcileTerminal(controllerKafkaRoleBinding, reasonDuplicateIdentity)
	operator.SetRoleBindingDrift(rb.Namespace, rb.Name, false) // see the topic gate's gauge note
	if uerr := updateStatus(ctx, r.Client, client.ObjectKeyFromObject(rb), rb, func() {
		st := roleBindingErrorStatus(rb, reasonDuplicateIdentity, msg)
		meta.SetStatusCondition(&st.Conditions, metav1.Condition{
			Type:               v1alpha1.CondValidationFailed,
			Status:             metav1.ConditionTrue,
			Reason:             reasonDuplicateIdentity,
			Message:            msg,
			ObservedGeneration: rb.Generation,
		})
		rb.Status = &st
	}); uerr != nil {
		return ctrl.Result{}, true, uerr
	}
	return ctrl.Result{RequeueAfter: resyncInterval(r.ResyncInterval)}, true, nil
}

// --- Deletion path: co-claimant scan (see the package doc's Deletion path section) ---

// reasonOrphanedToCoClaimant is the event reason emitted when a deleting
// KafkaUser or KafkaQuota skips its broker-side cleanup because another live
// (non-deleting) same-kind CR still claims the same broker identity: the
// finalizer is removed, but the credential/quota is orphaned to the surviving
// claimant, which keeps managing it.
const reasonOrphanedToCoClaimant = "OrphanedToCoClaimant"

// findLiveUserCoClaimant returns another live (non-deleting) KafkaUser
// claiming the same (cluster, username, mechanism) credential as u, or nil
// when u is the identity's only live claimant. Unlike findOlderUserClaim this
// is age-blind — ANY other live claimant counts, older or newer — because on
// the deletion path the question is not "who wins the identity" but "does
// anyone still need the broker state" (deleting the winner while a loser
// waits must skip cleanup too). u MUST already be defaulted (defaulting.User;
// the deletion handler does this) so its username/mechanism are resolved;
// each candidate is defaulted in-memory the same way before comparing (List
// items are copies, nothing is written back). The mechanism is part of the
// comparison, unlike in the gate: a claimant on a different mechanism manages
// a DIFFERENT broker credential, which deleting ours cannot break. List
// semantics mirror findOlderUserClaim (in-process clusterRef filtering; see
// the package doc's scan-scope note).
func findLiveUserCoClaimant(ctx context.Context, c client.Reader, u *v1alpha1.KafkaUser, namespaceLocal bool) (*v1alpha1.KafkaUser, error) {
	var listOpts []client.ListOption
	if namespaceLocal {
		listOpts = append(listOpts, client.InNamespace(u.Namespace))
	}
	var list v1alpha1.KafkaUserList
	if err := c.List(ctx, &list, listOpts...); err != nil {
		return nil, fmt.Errorf("listing KafkaUsers for deletion co-claimant check: %w", err)
	}
	want := operatorwebhook.ResolvedUsername(u)
	for i := range list.Items {
		other := &list.Items[i]
		if other.UID == u.UID || !other.DeletionTimestamp.IsZero() {
			continue // self, or a fellow deleter (its own finalizer runs this same scan)
		}
		if other.Spec.ClusterRef.Name != u.Spec.ClusterRef.Name {
			continue
		}
		if operatorwebhook.ResolvedUsername(other) != want {
			continue
		}
		defaulting.User(other) // resolve the candidate's mechanism (in-memory only)
		if other.Spec.Mechanism != u.Spec.Mechanism {
			continue // a different mechanism is a different credential
		}
		return other, nil
	}
	return nil, nil
}

// findLiveQuotaCoClaimant returns another live (non-deleting) KafkaQuota
// claiming the same (cluster, entity) identity as q, or nil when q is the
// identity's only live claimant. Age-blind for the same reason as
// findLiveUserCoClaimant; List semantics mirror findOlderQuotaClaim.
func findLiveQuotaCoClaimant(ctx context.Context, c client.Reader, q *v1alpha1.KafkaQuota, namespaceLocal bool) (*v1alpha1.KafkaQuota, error) {
	var listOpts []client.ListOption
	if namespaceLocal {
		listOpts = append(listOpts, client.InNamespace(q.Namespace))
	}
	var list v1alpha1.KafkaQuotaList
	if err := c.List(ctx, &list, listOpts...); err != nil {
		return nil, fmt.Errorf("listing KafkaQuotas for deletion co-claimant check: %w", err)
	}
	want := operatorwebhook.ResolvedEntityKey(q)
	for i := range list.Items {
		other := &list.Items[i]
		if other.UID == q.UID || !other.DeletionTimestamp.IsZero() {
			continue // self, or a fellow deleter (its own finalizer runs this same scan)
		}
		if other.Spec.ClusterRef.Name != q.Spec.ClusterRef.Name {
			continue
		}
		if operatorwebhook.ResolvedEntityKey(other) != want {
			continue
		}
		return other, nil
	}
	return nil, nil
}
