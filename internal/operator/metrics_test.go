package operator

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

// TestMetricsRegistered confirms the collectors registered on the global
// controller-runtime registry without panicking (the init ran) and that the
// helpers move the series.
func TestMetricsRegistered(t *testing.T) {
	// init() already ran MustRegister; a second register of the same collector
	// would panic, so reaching here means registration succeeded.

	RecordReconcile("kafkatopic", ResultSuccess)
	RecordReconcile("kafkatopic", ResultError)
	require.InDelta(t, 1, testutil.ToFloat64(reconcileTotal.WithLabelValues("kafkatopic", ResultSuccess)), 0)
	require.InDelta(t, 1, testutil.ToFloat64(reconcileTotal.WithLabelValues("kafkatopic", ResultError)), 0)
	require.InDelta(t, 1, testutil.ToFloat64(reconcileErrors.WithLabelValues("kafkatopic")), 0)

	SetClusterReachable("ns", "prod", true)
	require.InDelta(t, 1, testutil.ToFloat64(clusterReachable.WithLabelValues("ns", "prod")), 0)
	SetClusterReachable("ns", "prod", false)
	require.InDelta(t, 0, testutil.ToFloat64(clusterReachable.WithLabelValues("ns", "prod")), 0)

	SetTopicDrift("ns", "t1", true)
	require.InDelta(t, 1, testutil.ToFloat64(topicDrift.WithLabelValues("ns", "t1")), 0)

	SetPolicyDrift("ns", "p1", true)
	require.InDelta(t, 1, testutil.ToFloat64(policyDrift.WithLabelValues("ns", "p1")), 0)

	IncManagedTopics(2)
	require.InDelta(t, 2, testutil.ToFloat64(managedTopics), 0)
	IncManagedTopics(-2) // restore so other tests' deltas stay meaningful

	SetQuotaDrift("ns", "q1", true)
	require.InDelta(t, 1, testutil.ToFloat64(quotaDrift.WithLabelValues("ns", "q1")), 0)

	IncManagedQuotas(3)
	require.InDelta(t, 3, testutil.ToFloat64(managedQuotas), 0)
	IncManagedQuotas(-3)

	SetRoleBindingDrift("ns", "rb1", true)
	require.InDelta(t, 1, testutil.ToFloat64(roleBindingDrift.WithLabelValues("ns", "rb1")), 0)

	IncManagedRoleBindings(4)
	require.InDelta(t, 4, testutil.ToFloat64(managedRoleBindings), 0)
	IncManagedRoleBindings(-4)

	ObserveReconcileDuration("kafkatopic", 0.1) // must not panic
}

// TestClusterReachableNamespaceKeyed pins review I12: two same-named clusters
// in different namespaces must produce two independent series.
func TestClusterReachableNamespaceKeyed(t *testing.T) {
	SetClusterReachable("ns-keyed-a", "same", true)
	SetClusterReachable("ns-keyed-b", "same", false)
	require.InDelta(t, 1, testutil.ToFloat64(clusterReachable.WithLabelValues("ns-keyed-a", "same")), 0)
	require.InDelta(t, 0, testutil.ToFloat64(clusterReachable.WithLabelValues("ns-keyed-b", "same")), 0)
}

// TestDeleteSeriesHelpers pins review I12: the Delete* helpers must remove the
// per-CR series so deleted CRs do not leak stale series. Unique label values
// keep this independent of other tests sharing the global collectors.
func TestDeleteSeriesHelpers(t *testing.T) {
	SetTopicDrift("ns-del", "t-del", true)
	n := testutil.CollectAndCount(topicDrift, "monedula_kafka_topic_drift_detected")
	DeleteTopicDrift("ns-del", "t-del")
	require.Equal(t, n-1, testutil.CollectAndCount(topicDrift, "monedula_kafka_topic_drift_detected"))

	SetPolicyDrift("ns-del", "p-del", true)
	n = testutil.CollectAndCount(policyDrift, "monedula_access_policy_drift_detected")
	DeletePolicyDrift("ns-del", "p-del")
	require.Equal(t, n-1, testutil.CollectAndCount(policyDrift, "monedula_access_policy_drift_detected"))

	SetClusterReachable("ns-del", "c-del", true)
	n = testutil.CollectAndCount(clusterReachable, "monedula_kafka_cluster_reachable")
	DeleteClusterReachable("ns-del", "c-del")
	require.Equal(t, n-1, testutil.CollectAndCount(clusterReachable, "monedula_kafka_cluster_reachable"))

	SetQuotaDrift("ns-del", "q-del", true)
	n = testutil.CollectAndCount(quotaDrift, "monedula_kafka_quota_drift_detected")
	DeleteQuotaDrift("ns-del", "q-del")
	require.Equal(t, n-1, testutil.CollectAndCount(quotaDrift, "monedula_kafka_quota_drift_detected"))

	SetRoleBindingDrift("ns-del", "rb-del", true)
	n = testutil.CollectAndCount(roleBindingDrift, "monedula_kafka_rolebinding_drift_detected")
	DeleteRoleBindingDrift("ns-del", "rb-del")
	require.Equal(t, n-1, testutil.CollectAndCount(roleBindingDrift, "monedula_kafka_rolebinding_drift_detected"))
}

// TestCollectorsOnGlobalRegistry asserts the collectors are visible on the
// controller-runtime registry that the metrics server serves.
func TestCollectorsOnGlobalRegistry(t *testing.T) {
	RecordReconcile("kafkacluster", ResultSuccess)
	count, err := testutil.GatherAndCount(crmetrics.Registry, "monedula_reconcile_total")
	require.NoError(t, err)
	require.GreaterOrEqual(t, count, 1)
}

// TestRecordReconcileTerminal pins v0.36 Task 7's terminal-outcome counter:
// it increments per kind+reason and accumulates across repeated terminal
// reconciles of the same kind+reason (a counter, not a gauge — a resource
// flapping in and out of a terminal state should accumulate).
func TestRecordReconcileTerminal(t *testing.T) {
	RecordReconcileTerminal("kafkatopic", "ValidationFailed")
	require.InDelta(t, 1, testutil.ToFloat64(reconcileTerminalTotal.WithLabelValues("kafkatopic", "ValidationFailed")), 0)

	RecordReconcileTerminal("kafkatopic", "ValidationFailed")
	require.InDelta(t, 2, testutil.ToFloat64(reconcileTerminalTotal.WithLabelValues("kafkatopic", "ValidationFailed")), 0)

	// A different reason on the same kind is an independent series.
	RecordReconcileTerminal("kafkatopic", "TenancyDenied")
	require.InDelta(t, 1, testutil.ToFloat64(reconcileTerminalTotal.WithLabelValues("kafkatopic", "TenancyDenied")), 0)
	require.InDelta(t, 2, testutil.ToFloat64(reconcileTerminalTotal.WithLabelValues("kafkatopic", "ValidationFailed")), 0)

	// A different kind with the same reason is also an independent series.
	RecordReconcileTerminal("kafkauser", "ValidationFailed")
	require.InDelta(t, 1, testutil.ToFloat64(reconcileTerminalTotal.WithLabelValues("kafkauser", "ValidationFailed")), 0)
}

// TestReconcileTerminalTotalOnGlobalRegistry mirrors
// TestCollectorsOnGlobalRegistry for the new counter: it must be scraped from
// the same registry the metrics server serves.
func TestReconcileTerminalTotalOnGlobalRegistry(t *testing.T) {
	RecordReconcileTerminal("kafkacluster", "SomeReason")
	count, err := testutil.GatherAndCount(crmetrics.Registry, "monedula_reconcile_terminal_total")
	require.NoError(t, err)
	require.GreaterOrEqual(t, count, 1)
}
