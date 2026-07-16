// Package locking provides the operator's in-process keyed-mutex registry,
// the serialization backbone that lets --max-concurrent-reconciles exceed 1.
//
// The consistency unit the operator must protect is (KafkaCluster, substrate)
// — NOT the CR kind: the substrate writers build cluster-wide ACL and
// role-binding views, so two reconciles of DIFFERENT kinds (a KafkaTopic and
// a KafkaAccessPolicy on the ACL substrate; a KafkaTopic rbac auto-map and a
// KafkaRoleBinding on the MDS substrate) race each other just as surely as
// two topics do. Serializing per cluster+substrate keeps
// unrelated clusters (and the two substrates of one cluster) fully
// concurrent while making each substrate's read-modify-write critical
// section atomic. Per-broker-identity locks additionally serialize the
// duplicate-identity gate and the deletion co-claimant paths, which key on a
// resolved identity rather than a substrate.
//
// # Global lock order
//
// identity → acl → rbac. A reconcile acquires at most ONE identity lock, and
// always BEFORE any substrate lock; a goroutine holding a substrate lock must
// never acquire an identity lock, and one holding SubstrateRBAC must never
// acquire SubstrateACL (LockACLThenRBAC is the only sanctioned way to hold
// both substrates). Any acquisition sequence that respects this order is
// deadlock-free.
//
// The locks are in-process only. They are sufficient because exactly one
// operator replica is ever active: single-active-replica is enforced
// separately by the leader-election guard, not by this package.
//
// Like internal/operator/index, this is deliberately a leaf package — it
// imports nothing from the rest of the module — so both
// internal/operator/controller and internal/operator/reconcile can consume
// it without layering inversions.
package locking

import "sync"

// Substrate names for LockSubstrate. These are the only two substrates the
// operator writes through cluster-wide views.
const (
	// SubstrateACL is the Kafka ACL substrate.
	SubstrateACL = "acl"
	// SubstrateRBAC is the Confluent MDS role-binding substrate.
	SubstrateRBAC = "rbac"
)

// keySep separates key components. NUL keeps field boundaries unambiguous —
// no legal Kubernetes name, kind, cluster key, or resolved identity contains
// a NUL byte, so distinct component tuples can never alias one key. This is
// the same convention as the v0.36 identity keys (access.ACL.FullKey,
// rbac.RoleBinding.FullKey); keep them aligned.
const keySep = "\x00"

// Registry is a set of lazily-created named mutexes. The zero value is ready
// to use. One process-wide instance is shared by all reconcilers (wired in
// via manager field injection); a Registry must not be copied after first
// use.
//
// Entries are never removed, and that is fine: substrate entries are bounded
// by (number of KafkaClusters) x 2, and identity entries by the set of
// distinct broker identities ever reconciled, which is bounded by CR count
// over the process lifetime — a few small strings per CR, not a leak in
// practice. Do not add eviction; removing a mutex that a goroutine is about
// to lock reintroduces the very races this package exists to prevent.
type Registry struct {
	mus sync.Map // key string -> *sync.Mutex
}

// lock acquires the mutex named key, creating it on first use, and returns
// the function that releases it.
func (r *Registry) lock(key string) (unlock func()) {
	v, _ := r.mus.LoadOrStore(key, new(sync.Mutex))
	m := v.(*sync.Mutex)
	m.Lock()
	return m.Unlock
}

// LockSubstrate acquires the mutex for one cluster's substrate (SubstrateACL
// or SubstrateRBAC) and returns its release function:
//
//	unlock := reg.LockSubstrate(clusterKey, locking.SubstrateACL)
//	defer unlock()
//
// Global order invariant: never acquire SubstrateACL while holding
// SubstrateRBAC for any cluster. Callers needing BOTH substrates of a
// cluster must use LockACLThenRBAC — the one sanctioned way to hold both —
// rather than composing two LockSubstrate calls.
func (r *Registry) LockSubstrate(clusterKey, substrate string) (unlock func()) {
	return r.lock("substrate" + keySep + clusterKey + keySep + substrate)
}

// LockIdentity acquires the mutex for one resolved broker identity on one
// cluster and returns its release function. kind (the CR kind, e.g.
// "KafkaTopic") disambiguates the identity namespace so a topic named "x"
// and a username "x" never share a lock. The identity string comes from the
// resolved-identity helpers; the registry treats it as an opaque key.
//
// Global order invariant: an identity lock is always acquired BEFORE any
// substrate lock (see the package doc), and each reconcile acquires at most
// one identity lock, so identity locks can never deadlock among themselves.
func (r *Registry) LockIdentity(clusterKey, kind, identity string) (unlock func()) {
	return r.lock("identity" + keySep + clusterKey + keySep + kind + keySep + identity)
}

// LockACLThenRBAC acquires BOTH of one cluster's substrate mutexes — ACL
// first, then RBAC — and returns a single release function that unlocks them
// in reverse (RBAC, then ACL). It exists for reconciles whose accessBackends
// span both substrates (e.g. a topic with [acl, rbac]) and is the only
// exported way to hold both: the fixed acquisition order is the global order
// invariant that makes cross-substrate deadlock impossible, so callers must
// not build their own two-lock sequence out of LockSubstrate.
func (r *Registry) LockACLThenRBAC(clusterKey string) (unlock func()) {
	unlockACL := r.LockSubstrate(clusterKey, SubstrateACL)
	unlockRBAC := r.LockSubstrate(clusterKey, SubstrateRBAC)
	return func() {
		unlockRBAC()
		unlockACL()
	}
}
