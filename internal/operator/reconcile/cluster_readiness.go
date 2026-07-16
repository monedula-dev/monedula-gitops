package reconcile

import (
	"context"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	"github.com/monedula-dev/monedula-gitops/internal/schemaregistry"
)

// Cluster readiness condition reasons.
const (
	reasonReachable     = "Reachable"
	reasonUnreachable   = "Unreachable"
	reasonAuthenticated = "Authenticated"
	reasonAllReady      = "AllReady"
	reasonNotReady      = "NotReady"
)

// ReconcileCluster probes a KafkaCluster's reachability and (when configured)
// its Schema Registry, returning the status the controller should write. It is
// controller-runtime-free and performs NO cluster mutation: a KafkaCluster owns
// no external state, so reconcile is readiness/status only.
//
// Reachability is probed with a cheap admin call (ListTopics). A successful
// admin call implies the connection AND credentials are valid (the broker would
// reject an unauthenticated/unauthorized Metadata request), so Authenticated is
// reported True iff that probe succeeds; auth is not probed separately.
//
//	CondClusterReachable        True  iff the admin probe succeeded.
//	CondAuthenticated           mirrors CondClusterReachable (auth is implicit).
//	CondSchemaRegistryReachable True/False when sr != nil; OMITTED when sr == nil
//	                            (no Schema Registry configured for the cluster).
//	CondReady                   True iff reachable AND (SR reachable when configured).
//
// Phase is Ready when CondReady is True, else Error. ObservedGeneration is set
// to c.Generation and LastCheckedTime to now.
func ReconcileCluster(ctx context.Context, c *v1alpha1.KafkaCluster, k kafka.AdminClient, sr schemaregistry.Client) v1alpha1.KafkaClusterStatus {
	now := metav1.Now()
	gen := c.Generation
	st := v1alpha1.KafkaClusterStatus{ObservedGeneration: gen, LastCheckedTime: &now}
	// Seed conditions from the existing status so meta.SetStatusCondition can
	// preserve LastTransitionTime when a condition's Type+Status are unchanged
	// (otherwise every periodic requeue would re-stamp the transition time).
	if c.Status != nil {
		st.Conditions = append([]metav1.Condition(nil), c.Status.Conditions...)
	}

	// Kafka reachability (a successful admin call also proves authentication).
	reachable := true
	if _, err := k.ListTopics(ctx); err != nil {
		reachable = false
		setCond(&st.Conditions, v1alpha1.CondClusterReachable, metav1.ConditionFalse, reasonUnreachable, err.Error(), gen)
		setCond(&st.Conditions, v1alpha1.CondAuthenticated, metav1.ConditionFalse, reasonUnreachable, "cluster not reachable", gen)
	} else {
		setCond(&st.Conditions, v1alpha1.CondClusterReachable, metav1.ConditionTrue, reasonReachable, "admin API reachable", gen)
		setCond(&st.Conditions, v1alpha1.CondAuthenticated, metav1.ConditionTrue, reasonAuthenticated, "admin API call authenticated", gen)
	}

	// Schema Registry reachability (only when configured).
	srOK := true
	if sr != nil {
		if _, err := sr.ListSubjects(ctx); err != nil {
			srOK = false
			setCond(&st.Conditions, v1alpha1.CondSchemaRegistryReachable, metav1.ConditionFalse, reasonUnreachable, err.Error(), gen)
		} else {
			setCond(&st.Conditions, v1alpha1.CondSchemaRegistryReachable, metav1.ConditionTrue, reasonReachable, "schema registry reachable", gen)
		}
	} else {
		// No registry configured: REMOVE a stale SchemaRegistryReachable seeded
		// from the prior status (review I11) — e.g. the spec dropped its
		// schemaRegistry block — so "omitted when unconfigured" holds across
		// spec changes, not just on first reconcile.
		meta.RemoveStatusCondition(&st.Conditions, v1alpha1.CondSchemaRegistryReachable)
	}

	finalizeClusterReady(&st, reachable && srOK, gen)
	return st
}

// SetClusterClientsError populates conds/phase for the case where the live
// clients could not be constructed (e.g. a secret ref failed to resolve), so no
// reachability probe ran. ClusterReachable is reported False and the cluster is
// not Ready; the caller has already set Phase/ObservedGeneration/LastCheckedTime.
func SetClusterClientsError(st *v1alpha1.KafkaClusterStatus, gen int64, err error) {
	setCond(&st.Conditions, v1alpha1.CondClusterReachable, metav1.ConditionFalse, reasonUnreachable, err.Error(), gen)
	setCond(&st.Conditions, v1alpha1.CondReady, metav1.ConditionFalse, reasonNotReady, "could not build cluster clients", gen)
}

// finalizeClusterReady sets the Ready condition and Phase from the combined
// reachability result.
func finalizeClusterReady(st *v1alpha1.KafkaClusterStatus, ready bool, gen int64) {
	if ready {
		st.Phase = v1alpha1.PhaseReady
		setCond(&st.Conditions, v1alpha1.CondReady, metav1.ConditionTrue, reasonAllReady, "cluster reachable", gen)
	} else {
		st.Phase = v1alpha1.PhaseError
		setCond(&st.Conditions, v1alpha1.CondReady, metav1.ConditionFalse, reasonNotReady, "cluster not ready", gen)
	}
}
