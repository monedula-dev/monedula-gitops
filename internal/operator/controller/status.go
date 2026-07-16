package controller

import (
	"context"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/operator"
)

// Controller name labels for metrics (spec §28.2). Kept stable so the
// monedula_reconcile_* series have a consistent "controller" dimension.
const (
	controllerKafkaCluster      = "kafkacluster"
	controllerKafkaTopic        = "kafkatopic"
	controllerKafkaAccessPolicy = "kafkaaccesspolicy"
	controllerKafkaQuota        = "kafkaquota"
	controllerKafkaRoleBinding  = "kafkarolebinding"
	controllerKafkaUser         = "kafkauser"
)

// recordReconcile is the deferred metrics tail every reconciler runs: it
// observes the reconcile duration (since start) and bumps the reconcile/result
// counters based on whether the reconcile returned an error. retErr is the
// reconcile's named error return, read at defer time.
func recordReconcile(controller string, start time.Time, retErr *error) {
	operator.ObserveReconcileDuration(controller, time.Since(start).Seconds())
	result := operator.ResultSuccess
	if retErr != nil && *retErr != nil {
		result = operator.ResultError
	}
	operator.RecordReconcile(controller, result)
}

// updateStatus writes obj's status conflict-safely. It re-Gets obj inside a
// RetryOnConflict loop, applies mutate (which assigns the PRECOMPUTED status
// onto the freshly-fetched object), and writes the status subresource.
// Re-getting on each attempt picks up the latest resourceVersion so a
// concurrent write (e.g. from the periodic requeue) does not cause a permanent
// 409 Conflict.
//
// mutate must set obj.Status and MUST be side-effect-free: it is re-invoked on
// every conflict retry against the freshly-fetched obj, so it must only assign
// a status computed before updateStatus was called — never run the reconcile
// core (live-state reads, Kafka mutations) itself, or a 409 on the status write
// would re-mutate Kafka (review I9). obj is the typed pointer the caller
// already holds — it is overwritten in place by each Get.
//
// This helper is shared by all resource controllers (KafkaCluster / KafkaTopic /
// KafkaAccessPolicy) so the conflict-retry behavior lives in exactly one place.
//
// The write is SKIPPED when the mutated status is semantically equal to the
// stored one ignoring the volatile timestamps (LastCheckedTime /
// LastAppliedTime / conditions' LastTransitionTime): refreshing only a
// timestamp is not worth a write — every status write bumps resourceVersion
// and, before the watch filters were added, re-enqueued the controller's own
// reconcile in a hot loop (review C3). Skipping never changes the reconcile
// result the caller returns (the periodic RequeueAfter still re-checks).
func updateStatus(ctx context.Context, c client.Client, key client.ObjectKey, obj client.Object, mutate func()) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if err := c.Get(ctx, key, obj); err != nil {
			return err
		}
		prev := obj.DeepCopyObject().(client.Object)
		mutate()
		if statusEqualIgnoringTimestamps(prev, obj) {
			return nil // semantically unchanged: skip the write entirely
		}
		return c.Status().Update(ctx, obj)
	})
}

// statusEqualIgnoringTimestamps reports whether a's and b's statuses are
// semantically equal, ignoring the volatile timestamp fields that change on
// every reconcile pass (LastCheckedTime, LastAppliedTime, and the conditions'
// LastTransitionTime). Unknown or mismatched object types report NOT equal so
// the caller falls back to writing (an unnecessary write is benign; a missed
// write is not).
func statusEqualIgnoringTimestamps(a, b client.Object) bool {
	switch oldObj := a.(type) {
	case *v1alpha1.KafkaCluster:
		newObj, ok := b.(*v1alpha1.KafkaCluster)
		return ok && equalIgnoringTimestamps(oldObj.Status, newObj.Status, func(st *v1alpha1.KafkaClusterStatus) {
			if st == nil {
				return
			}
			st.LastCheckedTime = nil
			zeroConditionTransitionTimes(st.Conditions)
		})
	case *v1alpha1.KafkaTopic:
		newObj, ok := b.(*v1alpha1.KafkaTopic)
		return ok && equalIgnoringTimestamps(oldObj.Status, newObj.Status, func(st *v1alpha1.KafkaTopicStatus) {
			if st == nil {
				return
			}
			st.LastCheckedTime = nil
			st.LastAppliedTime = nil
			zeroConditionTransitionTimes(st.Conditions)
		})
	case *v1alpha1.KafkaAccessPolicy:
		newObj, ok := b.(*v1alpha1.KafkaAccessPolicy)
		return ok && equalIgnoringTimestamps(oldObj.Status, newObj.Status, func(st *v1alpha1.KafkaAccessPolicyStatus) {
			if st == nil {
				return
			}
			st.LastCheckedTime = nil
			st.LastAppliedTime = nil
			zeroConditionTransitionTimes(st.Conditions)
		})
	case *v1alpha1.KafkaQuota:
		newObj, ok := b.(*v1alpha1.KafkaQuota)
		return ok && equalIgnoringTimestamps(oldObj.Status, newObj.Status, func(st *v1alpha1.KafkaQuotaStatus) {
			if st == nil {
				return
			}
			st.LastCheckedTime = nil
			st.LastAppliedTime = nil
			zeroConditionTransitionTimes(st.Conditions)
		})
	case *v1alpha1.KafkaRoleBinding:
		newObj, ok := b.(*v1alpha1.KafkaRoleBinding)
		return ok && equalIgnoringTimestamps(oldObj.Status, newObj.Status, func(st *v1alpha1.KafkaRoleBindingStatus) {
			if st == nil {
				return
			}
			st.LastCheckedTime = nil
			st.LastAppliedTime = nil
			zeroConditionTransitionTimes(st.Conditions)
		})
	case *v1alpha1.KafkaUser:
		newObj, ok := b.(*v1alpha1.KafkaUser)
		return ok && equalIgnoringTimestamps(oldObj.Status, newObj.Status, func(st *v1alpha1.KafkaUserStatus) {
			if st == nil {
				return
			}
			st.LastCheckedTime = nil
			st.LastAppliedTime = nil
			zeroConditionTransitionTimes(st.Conditions)
		})
	default:
		return false
	}
}

// equalIgnoringTimestamps deep-copies both statuses (either may be nil — the
// generated DeepCopy handles a nil receiver), zeroes the volatile timestamp
// fields via zeroTimes, and compares the rest semantically.
func equalIgnoringTimestamps[T interface{ DeepCopy() T }](a, b T, zeroTimes func(T)) bool {
	ca, cb := a.DeepCopy(), b.DeepCopy()
	zeroTimes(ca)
	zeroTimes(cb)
	return equality.Semantic.DeepEqual(ca, cb)
}

// zeroConditionTransitionTimes clears LastTransitionTime on every condition.
// The reconcile core already preserves LastTransitionTime by seeding new
// conditions from the existing status, so in steady state these are equal
// anyway — zeroing keeps the comparison robust if that seeding is ever missed.
// nil-safe via the callers' nil-tolerant zeroTimes closures (a nil status's
// nil slice is a no-op here).
func zeroConditionTransitionTimes(conds []metav1.Condition) {
	for i := range conds {
		conds[i].LastTransitionTime = metav1.Time{}
	}
}
