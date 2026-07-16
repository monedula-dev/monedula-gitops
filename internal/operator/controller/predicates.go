package controller

import (
	"reflect"

	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

// watchEventFilter is the event filter shared by all six controllers'
// SetupWithManager. Without it, the controller's own status write bumps the
// object's resourceVersion, the unfiltered watch re-enqueues the object, and
// the reconciler spins in a hot loop chasing its own writes (review C3).
//
// The filter passes only the Update events that can change the reconcile
// OUTCOME, and is a predicate.Or of:
//
//   - GenerationChangedPredicate: spec changes. The apiserver bumps
//     metadata.generation only for spec writes on CRs with a status
//     subresource (a /status write never bumps it). For CRs with finalizers
//     the apiserver ALSO bumps generation when it sets deletionTimestamp, so
//     deletion is normally caught here too.
//   - AnnotationChangedPredicate: the risk gates are annotations
//     (allow-delete, allow-destructive, force-finalizer-removal), so an
//     annotation edit must re-trigger reconcile.
//   - lifecycleChangedPredicate: explicit deletionTimestamp / finalizer
//     change pass-through, so deletion handling does not depend on the
//     generation-bump-on-delete apiserver behavior above.
//
// Create / Delete / Generic events ALWAYS pass: each member predicate only
// implements Update (their embedded Funcs default the other event kinds to
// true, and predicate.Or passes when any member passes).
//
// A status-only update changes none of these signals and is filtered out;
// freshness re-checks are driven by the reconcilers' periodic RequeueAfter
// instead of by watch self-triggering.
func watchEventFilter() predicate.Predicate {
	return predicate.Or(
		predicate.GenerationChangedPredicate{},
		predicate.AnnotationChangedPredicate{},
		lifecycleChangedPredicate{},
	)
}

// lifecycleChangedPredicate passes Update events where the object's deletion
// state changed (deletionTimestamp set/cleared) or its finalizers changed.
// All other event kinds pass via the embedded Funcs defaults.
type lifecycleChangedPredicate struct {
	predicate.Funcs
}

// Update implements predicate.Predicate.
func (lifecycleChangedPredicate) Update(e event.UpdateEvent) bool {
	if e.ObjectOld == nil || e.ObjectNew == nil {
		return false
	}
	if e.ObjectOld.GetDeletionTimestamp().IsZero() != e.ObjectNew.GetDeletionTimestamp().IsZero() {
		return true
	}
	return !reflect.DeepEqual(e.ObjectOld.GetFinalizers(), e.ObjectNew.GetFinalizers())
}
