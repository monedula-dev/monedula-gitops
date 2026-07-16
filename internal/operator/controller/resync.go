package controller

import (
	"time"

	"sigs.k8s.io/controller-runtime/pkg/controller"
)

// DefaultResyncInterval is the periodic resync cadence used when a reconciler's
// ResyncInterval field is left zero (the CLI's --resync-interval defaults to
// this too; see internal/cli/operator.go). Every kind's healthy-object
// RequeueAfter, plus the duplicate-identity gate's loser-recovery RequeueAfter,
// derives from this single value so operators can tune the whole operator's
// resync cadence with one flag instead of per-kind constants.
const DefaultResyncInterval = 5 * time.Minute

// resyncInterval returns configured when it is set (non-zero), else
// DefaultResyncInterval. Reconcilers call this instead of reading a hardcoded
// per-kind constant, so a zero-value ResyncInterval field (e.g. in unit tests
// that construct a reconciler struct literal without setting it) preserves the
// pre-v0.36 5-minute behavior.
func resyncInterval(configured time.Duration) time.Duration {
	if configured > 0 {
		return configured
	}
	return DefaultResyncInterval
}

// DefaultMaxConcurrentReconciles is controller-runtime's own default (1) and
// the value a zero/negative MaxConcurrentReconciles field normalizes to.
// Values >1 pass through unchanged (v0.37): the per-(cluster, substrate) and
// per-(cluster, kind, identity) locks in locks.go / internal/operator/locking
// serialize the view-build→apply spans and identity claims, so same-kind
// reconciles may run concurrently — within a single ACTIVE replica only. The
// locking is in-process, which is why the CLI refuses >1 without
// --leader-elect (internal/cli/operator.go).
const DefaultMaxConcurrentReconciles = 1

// reconcilerOptions builds the controller.Options passed to WithOptions in each
// kind's SetupWithManager, passing MaxConcurrentReconciles through unchanged.
// Zero/negative values (e.g. a reconciler struct literal in a unit test that
// never sets the field) normalize to DefaultMaxConcurrentReconciles.
func reconcilerOptions(maxConcurrentReconciles int) controller.Options {
	if maxConcurrentReconciles <= 0 {
		maxConcurrentReconciles = DefaultMaxConcurrentReconciles
	}
	return controller.Options{MaxConcurrentReconciles: maxConcurrentReconciles}
}
