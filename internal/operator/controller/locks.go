package controller

import (
	"sync"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/operator/locking"
)

// This file is the controllers' single seam onto the keyed lock registry
// (internal/operator/locking): every substrate writer acquires its
// (KafkaCluster, substrate) span, and every gated kind its per-broker-identity
// span, through these helpers so the key derivation and nil handling live in
// exactly one place.
//
// GLOBAL LOCK ORDER: identity → acl → rbac. A reconcile acquires at most ONE
// identity lock (lockIdentity), always BEFORE any substrate lock, and never
// acquires an identity lock while holding a substrate lock. Substrate order is
// owned by locking.LockACLThenRBAC (acl before rbac). Current acquisition
// sequences, all conforming:
//
//   - KafkaTopic reconcile:        identity → {acl | acl→rbac}
//   - KafkaTopic finalizer:        acl only (no identity; see deleteTopicState)
//   - KafkaAccessPolicy (both):    acl only (ungated kind, no identity lock)
//   - KafkaRoleBinding reconcile:  identity → rbac
//   - KafkaRoleBinding finalizer:  rbac only (no identity; see its handler)
//   - KafkaQuota / KafkaUser:      identity only (both paths; no substrate)
//
// Nil tolerance: reg == nil acquires nothing and returns a no-op release.
// Production wiring always injects the process-wide registry (manager.Run);
// nil only occurs in unit tests that construct a reconciler struct literal
// without the Locks field — a single-threaded context where serialization is
// moot. This keeps the locking package's methods free of nil-receiver
// special-casing.
//
// Every returned release func is idempotent (sync.OnceFunc), so callers can
// BOTH defer it (covering the error returns inside the span) and invoke it
// explicitly at the narrow release point (right after the executor/engine
// returns, before the per-object status writes and their conflict retries).

// clusterLockKey derives the one consistent lock key for a resolved
// KafkaCluster: the namespace the clusterRef resolved in plus the CR name —
// the (namespace, name) the reconcile's own Get used, which uniquely
// identifies the KafkaCluster object every substrate view is scoped to.
// cluster must be the fetched CR (metadata populated).
func clusterLockKey(cluster *v1alpha1.KafkaCluster) string {
	return cluster.Namespace + "/" + cluster.Name
}

// lockSubstrate acquires cluster's substrate mutex (locking.SubstrateACL or
// locking.SubstrateRBAC) and returns its idempotent release func. A nil reg
// returns a no-op (see the file comment).
func lockSubstrate(reg *locking.Registry, cluster *v1alpha1.KafkaCluster, substrate string) func() {
	if reg == nil {
		return func() {}
	}
	return sync.OnceFunc(reg.LockSubstrate(clusterLockKey(cluster), substrate))
}

// lockTopicSubstrates acquires the substrate set a KafkaTopic reconcile
// writes through, decided from the CLUSTER's effective accessBackends (they
// are cluster-level config — v1alpha1.EffectiveAccessBackends — so the
// decision needs no topic defaulting and is available right after the cluster
// resolves, BEFORE any view build):
//
//   - rbac present: BOTH substrates via LockACLThenRBAC (the sanctioned
//     acl→rbac order). Even an rbac-ONLY cluster takes the ACL lock: the
//     engine still reads live ACLs (observeTopicLive) and — because the
//     §20.1 prune aggregate carries every KafkaAccessPolicy's scope
//     regardless of backend — a topic reconcile can PRUNE ACLs inside that
//     aggregated scope, making it a genuine ACL-substrate writer.
//   - otherwise (acl-only, the default): the ACL lock alone.
//
// Returns an idempotent release func; nil reg returns a no-op.
func lockTopicSubstrates(reg *locking.Registry, cluster *v1alpha1.KafkaCluster) func() {
	if reg == nil {
		return func() {}
	}
	if v1alpha1.HasAccessBackend(cluster, "rbac") {
		return sync.OnceFunc(reg.LockACLThenRBAC(clusterLockKey(cluster)))
	}
	return sync.OnceFunc(reg.LockSubstrate(clusterLockKey(cluster), locking.SubstrateACL))
}

// lockIdentity acquires the per-broker-identity mutex for (cluster, kind,
// identity) and returns its idempotent release func; nil reg returns a no-op
// (see the file comment). kind is the CR kind string ("KafkaTopic",
// "KafkaQuota", "KafkaUser", "KafkaRoleBinding"); identity is the resolved
// broker identity the duplicate-identity gate compares (duplicate.go) —
// derivable from the spec alone, so the lock can be taken right after cluster
// resolution, BEFORE the gate's scan and before any substrate lock (the
// identity → acl → rbac global order above). Holding it across
// gate → quorum recheck → engine mutation makes the identity claim atomic
// against same-identity rivals in-process; on the KafkaUser/KafkaQuota
// deletion paths it likewise spans co-claimant scan → broker cleanup →
// finalizer removal.
func lockIdentity(reg *locking.Registry, cluster *v1alpha1.KafkaCluster, kind, identity string) func() {
	if reg == nil {
		return func() {}
	}
	return sync.OnceFunc(reg.LockIdentity(clusterLockKey(cluster), kind, identity))
}
