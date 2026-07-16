package locking

import (
	"runtime"
	"sync"
	"testing"
	"time"
)

// acquireTimeout bounds waits for locks the tests expect to be free. It is
// only ever slept through on failure, so it can be generous without making
// the passing path slow.
const acquireTimeout = 5 * time.Second

// mustAcquire asserts that lock() succeeds promptly (i.e. nobody holds the
// key), then releases it. Run in a goroutine with a bounded wait so a broken
// implementation fails the test instead of hanging the whole package.
func mustAcquire(t *testing.T, name string, lock func() func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		unlock := lock()
		unlock()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(acquireTimeout):
		t.Fatalf("%s: expected lock to be immediately acquirable, still blocked after %v", name, acquireTimeout)
	}
}

// assertExclusive runs workers goroutines that repeatedly enter a critical
// section guarded by lock() and fails if two are ever inside at once. The
// occupancy channel is a deterministic overlap detector (buffer of one: a
// failed non-blocking send means another goroutine is in the section), and
// the unsynchronized counter gives the race detector something to bite on if
// the lock does not actually exclude.
func assertExclusive(t *testing.T, lock func() func()) {
	t.Helper()
	const workers, iters = 8, 200
	occupied := make(chan struct{}, 1)
	counter := 0
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range iters {
				unlock := lock()
				select {
				case occupied <- struct{}{}:
				default:
					t.Error("two goroutines inside the critical section at once")
					unlock()
					return
				}
				counter++
				runtime.Gosched() // widen the interleaving window
				<-occupied
				unlock()
			}
		}()
	}
	wg.Wait()
	if want := workers * iters; counter != want {
		t.Fatalf("critical-section counter = %d, want %d (lost updates imply broken exclusion)", counter, want)
	}
}

func TestLockSubstrateSameKeyExclusion(t *testing.T) {
	var reg Registry
	assertExclusive(t, func() func() { return reg.LockSubstrate("kafka-prod", SubstrateACL) })
}

func TestLockIdentitySameKeyExclusion(t *testing.T) {
	var reg Registry
	assertExclusive(t, func() func() { return reg.LockIdentity("kafka-prod", "KafkaTopic", "orders") })
}

// TestLockSubstrateDifferentKeysIndependent pins that holding (clusterA, acl)
// blocks neither (clusterA, rbac) nor (clusterB, acl).
func TestLockSubstrateDifferentKeysIndependent(t *testing.T) {
	var reg Registry
	unlock := reg.LockSubstrate("cluster-a", SubstrateACL)
	defer unlock()

	mustAcquire(t, "(cluster-a, rbac)", func() func() { return reg.LockSubstrate("cluster-a", SubstrateRBAC) })
	mustAcquire(t, "(cluster-b, acl)", func() func() { return reg.LockSubstrate("cluster-b", SubstrateACL) })
}

// TestLockIdentityKindNamespacing pins that the kind component keeps a topic
// named "x" and a user named "x" on independent locks, and that identity
// locks are independent of the substrate locks of the same cluster.
func TestLockIdentityKindNamespacing(t *testing.T) {
	var reg Registry
	unlock := reg.LockIdentity("cluster-a", "KafkaTopic", "x")
	defer unlock()

	mustAcquire(t, `(cluster-a, KafkaUser, "x")`, func() func() { return reg.LockIdentity("cluster-a", "KafkaUser", "x") })
	mustAcquire(t, `(cluster-b, KafkaTopic, "x")`, func() func() { return reg.LockIdentity("cluster-b", "KafkaTopic", "x") })
	mustAcquire(t, "(cluster-a, acl) substrate", func() func() { return reg.LockSubstrate("cluster-a", SubstrateACL) })
}

// TestLockACLThenRBACReleasesBoth pins that the combined unlock releases both
// substrate keys: after it returns, each is immediately acquirable.
func TestLockACLThenRBACReleasesBoth(t *testing.T) {
	var reg Registry
	unlock := reg.LockACLThenRBAC("cluster-a")
	unlock()

	mustAcquire(t, "(cluster-a, acl) after combined unlock", func() func() { return reg.LockSubstrate("cluster-a", SubstrateACL) })
	mustAcquire(t, "(cluster-a, rbac) after combined unlock", func() func() { return reg.LockSubstrate("cluster-a", SubstrateRBAC) })
}

// TestLockACLThenRBACExclusion pins that concurrent LockACLThenRBAC callers
// on the same cluster exclude each other (and, run under -race, that the
// fixed acquisition order never deadlocks them).
func TestLockACLThenRBACExclusion(t *testing.T) {
	var reg Registry
	assertExclusive(t, func() func() { return reg.LockACLThenRBAC("cluster-a") })
}

// TestLockACLThenRBACIndependentClusters pins that holding both substrates of
// one cluster leaves another cluster's combined lock free.
func TestLockACLThenRBACIndependentClusters(t *testing.T) {
	var reg Registry
	unlock := reg.LockACLThenRBAC("cluster-a")
	defer unlock()

	mustAcquire(t, "LockACLThenRBAC(cluster-b)", func() func() { return reg.LockACLThenRBAC("cluster-b") })
}
