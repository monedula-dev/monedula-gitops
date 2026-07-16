package operator

import (
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

// Prometheus collectors for the operator (spec §28.2). They are registered on
// controller-runtime's global registry so they are scraped from the same
// /metrics endpoint the manager already serves. The exported Record*/Set*
// helpers below are the only intended way to mutate them; the reconcilers call
// them so the series actually move.
var (
	// reconcileTotal counts reconcile invocations partitioned by controller and
	// outcome ("success" | "error").
	reconcileTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "monedula_reconcile_total",
		Help: "Total number of reconcile invocations, by controller and result.",
	}, []string{"controller", "result"})

	// reconcileErrors counts reconcile invocations that returned an error,
	// partitioned by controller. (Redundant with reconcile_total{result=error}
	// but kept as a first-class series per the spec.)
	reconcileErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "monedula_reconcile_errors_total",
		Help: "Total number of reconcile invocations that returned an error, by controller.",
	}, []string{"controller"})

	// reconcileDuration observes wall-clock reconcile duration, by controller.
	reconcileDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "monedula_reconcile_duration_seconds",
		Help:    "Reconcile duration in seconds, by controller.",
		Buckets: prometheus.DefBuckets,
	}, []string{"controller"})

	// clusterReachable is 1 when a KafkaCluster is reachable, 0 otherwise.
	// Keyed by namespace+name like every per-CR series (review I12): two
	// same-named clusters in different namespaces must not share a series.
	clusterReachable = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "monedula_kafka_cluster_reachable",
		Help: "Whether a Kafka cluster is reachable (1) or not (0), by namespace and name.",
	}, []string{"namespace", "name"})

	// topicDrift is 1 when a KafkaTopic has detected drift, 0 otherwise.
	topicDrift = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "monedula_kafka_topic_drift_detected",
		Help: "Whether drift was detected for a KafkaTopic (1) or not (0), by namespace and name.",
	}, []string{"namespace", "name"})

	// managedTopics is the number of KafkaTopics currently observed by the
	// operator.
	managedTopics = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "monedula_managed_topics",
		Help: "Number of KafkaTopics currently managed by the operator.",
	})

	// policyDrift is 1 when a KafkaAccessPolicy has detected drift, 0 otherwise.
	policyDrift = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "monedula_access_policy_drift_detected",
		Help: "Whether drift was detected for a KafkaAccessPolicy (1) or not (0), by namespace and name.",
	}, []string{"namespace", "name"})

	// quotaDrift is 1 when a KafkaQuota has detected drift, 0 otherwise.
	quotaDrift = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "monedula_kafka_quota_drift_detected",
		Help: "Whether drift was detected for a KafkaQuota (1) or not (0), by namespace and name.",
	}, []string{"namespace", "name"})

	// managedQuotas is the number of KafkaQuotas currently observed by the
	// operator.
	managedQuotas = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "monedula_managed_quotas",
		Help: "Number of KafkaQuotas currently managed by the operator.",
	})

	// roleBindingDrift is 1 when a KafkaRoleBinding has detected drift, 0
	// otherwise. KafkaRoleBindingStatus deliberately omits a detailed Drift
	// struct (MDS bindings are managed as an authoritative set; see
	// roleBindingTarget), so this is keyed off Phase == Drifted rather than a
	// Drift.Detected field — the same "reconciled but out of sync" signal the
	// topic/policy gauges expose, sourced from wherever the status makes it
	// available for this kind.
	roleBindingDrift = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "monedula_kafka_rolebinding_drift_detected",
		Help: "Whether drift was detected for a KafkaRoleBinding (1) or not (0), by namespace and name.",
	}, []string{"namespace", "name"})

	// managedRoleBindings is the number of KafkaRoleBindings currently observed
	// by the operator.
	managedRoleBindings = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "monedula_managed_rolebindings",
		Help: "Number of KafkaRoleBindings currently managed by the operator.",
	})

	// userDrift is 1 when a KafkaUser has detected drift, 0 otherwise.
	// KafkaUserStatus deliberately omits a Drift struct (the drift surface is
	// the credential identity triple; see reconcile.userStatusTarget), so this
	// is keyed off the DriftDetected condition rather than a Drift.Detected
	// field — the same "reconciled but out of sync" signal the topic/policy
	// gauges expose, sourced from where this kind's status makes it available.
	userDrift = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "monedula_kafka_user_drift_detected",
		Help: "Whether drift was detected for a KafkaUser (1) or not (0), by namespace and name.",
	}, []string{"namespace", "name"})

	// managedUsers is the number of KafkaUsers currently observed by the
	// operator.
	managedUsers = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "monedula_managed_users",
		Help: "Number of KafkaUsers currently managed by the operator.",
	})

	// reconcileTerminalTotal counts TERMINAL reconcile outcomes (spec §16's
	// "nil error, needs a human/spec change" class: ValidationFailed,
	// TenancyDenied, ACLConflict-as-ValidationFailed, DuplicateIdentity,
	// MDSNotConfigured, ...), partitioned by kind and reason. Deliberately has
	// NO per-CR labels (no namespace/name) — unlike the drift gauges above,
	// there is nothing to delete on CR removal, and kind+reason stays bounded
	// regardless of CR churn (v0.36 Task 7). It is a counter, not a gauge: a
	// resource flapping in and out of a terminal state should accumulate, not
	// overwrite, so an alert on rate() actually reflects churn.
	reconcileTerminalTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "monedula_reconcile_terminal_total",
		Help: "Total number of terminal reconcile outcomes (no retry without a human/spec change), by kind and reason.",
	}, []string{"kind", "reason"})
)

func init() {
	crmetrics.Registry.MustRegister(
		reconcileTotal,
		reconcileErrors,
		reconcileDuration,
		clusterReachable,
		topicDrift,
		managedTopics,
		policyDrift,
		quotaDrift,
		managedQuotas,
		roleBindingDrift,
		managedRoleBindings,
		userDrift,
		managedUsers,
		reconcileTerminalTotal,
	)
}

// Reconcile result label values.
const (
	ResultSuccess = "success"
	ResultError   = "error"
)

// RecordReconcile bumps the reconcile counters for a controller. result should
// be ResultSuccess or ResultError; when ResultError, the per-controller error
// counter is also incremented.
func RecordReconcile(controller, result string) {
	reconcileTotal.WithLabelValues(controller, result).Inc()
	if result == ResultError {
		reconcileErrors.WithLabelValues(controller).Inc()
	}
}

// ObserveReconcileDuration records a reconcile's duration (in seconds) for a
// controller.
func ObserveReconcileDuration(controller string, seconds float64) {
	reconcileDuration.WithLabelValues(controller).Observe(seconds)
}

// SetClusterReachable sets the reachability gauge for a cluster.
func SetClusterReachable(namespace, name string, reachable bool) {
	clusterReachable.WithLabelValues(namespace, name).Set(boolToFloat(reachable))
}

// DeleteClusterReachable removes a deleted KafkaCluster's reachability series
// so CR churn does not accumulate stale series (review I12).
func DeleteClusterReachable(namespace, name string) {
	clusterReachable.DeleteLabelValues(namespace, name)
}

// SetTopicDrift sets the drift gauge for a KafkaTopic.
func SetTopicDrift(namespace, name string, drifted bool) {
	topicDrift.WithLabelValues(namespace, name).Set(boolToFloat(drifted))
}

// DeleteTopicDrift removes a deleted KafkaTopic's drift series so CR churn
// does not accumulate stale series (review I12).
func DeleteTopicDrift(namespace, name string) {
	topicDrift.DeleteLabelValues(namespace, name)
}

// SetPolicyDrift sets the drift gauge for a KafkaAccessPolicy.
func SetPolicyDrift(namespace, name string, drifted bool) {
	policyDrift.WithLabelValues(namespace, name).Set(boolToFloat(drifted))
}

// DeletePolicyDrift removes a deleted KafkaAccessPolicy's drift series so CR
// churn does not accumulate stale series (review I12).
func DeletePolicyDrift(namespace, name string) {
	policyDrift.DeleteLabelValues(namespace, name)
}

// IncManagedTopics adjusts the managed-topics gauge by delta. The topic
// reconciler bumps it by +1 the first time it observes a given topic and by -1
// when the topic is finalized/gone, so the gauge tracks the live count — not
// "topics ever seen since process start" (review I12).
func IncManagedTopics(delta float64) {
	managedTopics.Add(delta)
}

// SetQuotaDrift sets the drift gauge for a KafkaQuota.
func SetQuotaDrift(namespace, name string, drifted bool) {
	quotaDrift.WithLabelValues(namespace, name).Set(boolToFloat(drifted))
}

// DeleteQuotaDrift removes a deleted KafkaQuota's drift series so CR churn
// does not accumulate stale series (review I12).
func DeleteQuotaDrift(namespace, name string) {
	quotaDrift.DeleteLabelValues(namespace, name)
}

// IncManagedQuotas adjusts the managed-quotas gauge by delta, mirroring
// IncManagedTopics: +1 the first time a given quota is observed, -1 when it is
// finalized/gone, so the gauge tracks the live count.
func IncManagedQuotas(delta float64) {
	managedQuotas.Add(delta)
}

// SetRoleBindingDrift sets the drift gauge for a KafkaRoleBinding.
func SetRoleBindingDrift(namespace, name string, drifted bool) {
	roleBindingDrift.WithLabelValues(namespace, name).Set(boolToFloat(drifted))
}

// DeleteRoleBindingDrift removes a deleted KafkaRoleBinding's drift series so
// CR churn does not accumulate stale series (review I12).
func DeleteRoleBindingDrift(namespace, name string) {
	roleBindingDrift.DeleteLabelValues(namespace, name)
}

// IncManagedRoleBindings adjusts the managed-rolebindings gauge by delta,
// mirroring IncManagedTopics: +1 the first time a given role binding is
// observed, -1 when it is finalized/gone, so the gauge tracks the live count.
func IncManagedRoleBindings(delta float64) {
	managedRoleBindings.Add(delta)
}

// SetUserDrift sets the drift gauge for a KafkaUser.
func SetUserDrift(namespace, name string, drifted bool) {
	userDrift.WithLabelValues(namespace, name).Set(boolToFloat(drifted))
}

// DeleteUserDrift removes a deleted KafkaUser's drift series so CR churn does
// not accumulate stale series (review I12).
func DeleteUserDrift(namespace, name string) {
	userDrift.DeleteLabelValues(namespace, name)
}

// IncManagedUsers adjusts the managed-users gauge by delta, mirroring
// IncManagedTopics: +1 the first time a given user is observed, -1 when it is
// finalized/gone, so the gauge tracks the live count.
func IncManagedUsers(delta float64) {
	managedUsers.Add(delta)
}

// RecordReconcileTerminal bumps the terminal-outcome counter for kind+reason
// (v0.36 Task 7). kind should match the RecordReconcile controller label
// values (e.g. "kafkatopic"); reason is the condition Reason string the
// terminal outcome carries (e.g. "ValidationFailed", "TenancyDenied",
// "DuplicateIdentity", "MDSNotConfigured").
func RecordReconcileTerminal(kind, reason string) {
	reconcileTerminalTotal.WithLabelValues(kind, reason).Inc()
}

// ReconcileTerminalCount returns the current value of the
// monedula_reconcile_terminal_total{kind, reason} series (0 if never
// incremented). Exported for tests in other packages (internal/operator/
// reconcile, internal/operator/controller) that trigger a terminal outcome via
// their own fixtures and assert the counter moved, without depending on this
// package's unexported collector or pulling prometheus/client_golang's test
// helper package into non-test code.
func ReconcileTerminalCount(kind, reason string) float64 {
	var m dto.Metric
	if err := reconcileTerminalTotal.WithLabelValues(kind, reason).Write(&m); err != nil {
		return 0
	}
	if m.Counter == nil {
		return 0
	}
	return m.Counter.GetValue()
}

func boolToFloat(b bool) float64 {
	if b {
		return 1
	}
	return 0
}
