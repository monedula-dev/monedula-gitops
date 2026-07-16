package controller

// Event "action" convention for the events.k8s.io recorder API.
//
// The new events API (k8s.io/client-go/tools/events) requires an action per
// emission: per the Kubernetes API conventions it is "what action was taken or
// failed regarding the regarding object" — machine-readable, UpperCamelCase.
// The one thing this operator does to a CR is reconcile it, in two phases, so
// the honest minimal mapping is the reconcile phase the emission happens in:
//
//   - actionReconcile — the normal reconcile path (spec applied to the broker,
//     including gates like duplicate-identity checks and self-lockout warnings
//     that run as part of a reconcile);
//   - actionFinalize — the deletion/finalizer path (broker state torn down or
//     deliberately retained before the finalizer is removed).
//
// Reasons stay the fine-grained per-outcome identifiers they always were; the
// action answers the coarser "during what" and is intentionally a two-value
// enum. Each reconciler has an event (actionReconcile) and a finalizeEvent
// (actionFinalize) helper; both are no-ops when no Recorder is wired, which
// unit tests that construct bare reconcilers rely on.
const (
	// actionReconcile marks events emitted on the reconcile path.
	actionReconcile = "Reconcile"
	// actionFinalize marks events emitted on the deletion/finalizer path.
	actionFinalize = "Finalize"
)
