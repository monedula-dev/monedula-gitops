package pipeline

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// buildPlanForBackends builds a plan for a KafkaTopic on a cluster that has the
// given accessBackends (nil → omit the field entirely, defaulting to ["acl"]).
// The topic has a single producer with host "*" (no coarsening warning).
func buildPlanForBackends(t *testing.T, backends []string) *Plan {
	t.Helper()
	return buildPlanForBackendsWithHost(t, backends, "")
}

// buildPlanForBackendsWithHost is like buildPlanForBackends but allows
// specifying the producer host (non-"*" triggers a coarsening warning on rbac
// clusters).
func buildPlanForBackendsWithHost(t *testing.T, backends []string, host string) *Plan {
	t.Helper()
	clusterFile := "testdata/cluster-mds.yaml" // acl-only (no accessBackends field) with MDS
	switch {
	case len(backends) == 1 && backends[0] == "rbac":
		clusterFile = "testdata/cluster-rbac-only.yaml"
	case len(backends) == 2:
		clusterFile = "testdata/cluster-acl-rbac-dual.yaml"
	case len(backends) == 0 || backends == nil:
		clusterFile = "testdata/cluster-prod.yaml" // plain acl-only, no MDS needed
	}

	topicFile := "testdata/topic-orders-producer.yaml"
	if host != "" && host != "*" {
		topicFile = "testdata/topic-orders-producer-host.yaml"
	}

	plan, err := Build(Options{
		Filenames:          []string{topicFile},
		ClusterConfigFiles: []string{clusterFile},
		RequireClusters:    true,
	})
	require.NoError(t, err)
	return plan
}

func TestBuildHappyPath(t *testing.T) {
	plan, err := Build(Options{
		Filenames:          []string{"testdata/manifests-happy.yaml"},
		ClusterConfigFiles: []string{"testdata/cluster-prod.yaml"},
		RequireClusters:    true,
	})
	require.NoError(t, err)
	require.Len(t, plan.Topics, 1)
	require.Len(t, plan.Policies, 1)
	require.Equal(t, "prod", plan.SelectedCluster)
	require.NotEmpty(t, plan.DesiredACLs)
	require.Len(t, plan.DesiredTopics, 1)
	require.Equal(t, "payments.orders", plan.DesiredTopics[0].Name)
	require.Equal(t, "KafkaTopic", plan.DesiredTopics[0].Kind)
	require.Equal(t, "payments", plan.DesiredTopics[0].Namespace)
	require.Equal(t, 6, plan.DesiredTopics[0].Partitions)
	// replicationFactor defaulted from cluster defaults (3).
	require.Equal(t, 3, plan.DesiredTopics[0].ReplicationFactor)
}

func TestBuildClusterFilterExcludesOthers(t *testing.T) {
	plan, err := Build(Options{
		Filenames:          []string{"testdata/manifests-ab.yaml"},
		ClusterConfigFiles: []string{"testdata/clusters-ab.yaml"},
		Cluster:            "a",
		RequireClusters:    true,
	})
	require.NoError(t, err)
	require.Equal(t, "a", plan.SelectedCluster)
	require.Len(t, plan.Topics, 1)
	require.Equal(t, "topic-a", plan.Topics[0].Name)
}

func TestBuildUnknownClusterRefErrors(t *testing.T) {
	_, err := Build(Options{
		Filenames:          []string{"testdata/topic-missing-ref.yaml"},
		ClusterConfigFiles: []string{"testdata/cluster-prod.yaml"},
		RequireClusters:    true,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing")
}

func TestBuildValidationErrorsSurface(t *testing.T) {
	_, err := Build(Options{
		Filenames:          []string{"testdata/topic-bad-partitions.yaml"},
		ClusterConfigFiles: []string{"testdata/cluster-prod.yaml"},
		RequireClusters:    true,
	})
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "partitions")
}

func TestBuildRequireClustersErrorsWhenNone(t *testing.T) {
	_, err := Build(Options{
		Filenames:       []string{"testdata/manifests-happy.yaml"},
		RequireClusters: true,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "no cluster configuration provided")
}

func TestBuildValidateModeNoClusters(t *testing.T) {
	plan, err := Build(Options{
		Filenames:       []string{"testdata/topic-valid-noref.yaml"},
		RequireClusters: false,
	})
	require.NoError(t, err)
	require.Len(t, plan.Topics, 1)
	require.Equal(t, "standalone", plan.Topics[0].Name)
}

func TestBuildACLConflictErrors(t *testing.T) {
	_, err := Build(Options{
		Filenames:          []string{"testdata/policy-acl-conflict.yaml"},
		ClusterConfigFiles: []string{"testdata/cluster-prod.yaml"},
		RequireClusters:    true,
	})
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "conflict")
}

// TestBuildCrossClusterSameTupleNoConflict pins the per-cluster desired-set
// assembly: an identical ACL tuple requested Allow on cluster "a" and Deny on
// cluster "b" is NOT a conflict (the tuples live on different clusters).
func TestBuildCrossClusterSameTupleNoConflict(t *testing.T) {
	plan, err := Build(Options{
		Filenames:       []string{"testdata/policies-cross-cluster.yaml"},
		RequireClusters: false, // validate-mode: no cluster config loaded
	})
	require.NoError(t, err, "cross-cluster identical tuples must not false-conflict")
	require.Len(t, plan.Policies, 2)
}

// TestBuildSameClusterConflictWithoutClusterConfig verifies the Allow/Deny
// conflict is still detected on a single cluster even when no cluster config is
// loaded (the validate -f PR-lint path).
func TestBuildSameClusterConflictWithoutClusterConfig(t *testing.T) {
	_, err := Build(Options{
		Filenames:       []string{"testdata/policy-acl-conflict.yaml"},
		RequireClusters: false,
	})
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "conflict")
}

// TestBuildCrossClusterSelectedDesiredACLs verifies that with --cluster the
// Plan's DesiredACLs/Scope carry ONLY the selected cluster's tuples.
func TestBuildCrossClusterSelectedDesiredACLs(t *testing.T) {
	plan, err := Build(Options{
		Filenames:          []string{"testdata/policies-cross-cluster.yaml"},
		ClusterConfigFiles: []string{"testdata/clusters-ab.yaml"},
		Cluster:            "a",
		RequireClusters:    true,
	})
	require.NoError(t, err)
	require.Len(t, plan.DesiredACLs, 1)
	require.Equal(t, "Allow", plan.DesiredACLs[0].Permission)
}

func TestBuildMultiClusterNoSelectorAmbiguous(t *testing.T) {
	plan, err := Build(Options{
		Filenames:          []string{"testdata/manifests-ab.yaml"},
		ClusterConfigFiles: []string{"testdata/clusters-ab.yaml"},
		RequireClusters:    true,
	})
	require.NoError(t, err)
	require.Equal(t, "", plan.SelectedCluster)
	require.Len(t, plan.Topics, 2)
}

func TestBuildDesiredSchemas(t *testing.T) {
	plan, err := Build(Options{
		Filenames:          []string{"testdata/topic-schema.yaml"},
		ClusterConfigFiles: []string{"testdata/cluster-schema.yaml"},
		RequireClusters:    true,
	})
	require.NoError(t, err)
	require.Len(t, plan.DesiredSchemas, 1)
	ds := plan.DesiredSchemas[0]
	require.Equal(t, "payments.orders-value", ds.Subject)
	require.Equal(t, "payments.orders", ds.Topic)
	require.Equal(t, "AVRO", ds.Type)
	require.Equal(t, "BACKWARD", ds.Compatibility)
	require.Contains(t, ds.Definition, "\"name\": \"Order\"")
}

func TestBuildDesiredSchemasMissingFileErrors(t *testing.T) {
	_, err := Build(Options{
		Filenames:          []string{"testdata/topic-schema-badfile.yaml"},
		ClusterConfigFiles: []string{"testdata/cluster-schema.yaml"},
		RequireClusters:    true,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "reading schema file")
}

func TestBuildDesiredSchemasNonJSONErrors(t *testing.T) {
	_, err := Build(Options{
		Filenames:          []string{"testdata/topic-schema-notjson.yaml"},
		ClusterConfigFiles: []string{"testdata/cluster-schema.yaml"},
		RequireClusters:    true,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "is not valid JSON")
}

// TestBuildDesiredSchemasSiblingDirAllowed confirms a single "../schemas/" hop
// to a sibling directory (the importer's layout) is allowed.
func TestBuildDesiredSchemasSiblingDirAllowed(t *testing.T) {
	plan, err := Build(Options{
		Filenames:          []string{"testdata/ns/topics/sibling.yaml"},
		ClusterConfigFiles: []string{"testdata/cluster-schema.yaml"},
		RequireClusters:    true,
	})
	require.NoError(t, err)
	require.Len(t, plan.DesiredSchemas, 1)
	require.Equal(t, "payments.orders-value", plan.DesiredSchemas[0].Subject)
}

// TestBuildDesiredSchemasTraversalRejected confirms deep "../.." traversal that
// escapes the manifest's parent directory is rejected as path traversal.
func TestBuildDesiredSchemasTraversalRejected(t *testing.T) {
	_, err := Build(Options{
		Filenames:          []string{"testdata/ns/topics/traversal.yaml"},
		ClusterConfigFiles: []string{"testdata/cluster-schema.yaml"},
		RequireClusters:    true,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "escapes the base directory")
}

// TestBuildDesiredSchemasInline verifies that an inline schema body is accepted
// by the pipeline and produces a DesiredSchema with the verbatim body (spec §11).
func TestBuildDesiredSchemasInline(t *testing.T) {
	plan, err := Build(Options{
		Filenames:          []string{"testdata/topic-schema-inline.yaml"},
		ClusterConfigFiles: []string{"testdata/cluster-schema.yaml"},
		RequireClusters:    true,
	})
	require.NoError(t, err)
	require.Len(t, plan.DesiredSchemas, 1)
	ds := plan.DesiredSchemas[0]
	require.Equal(t, "payments.orders-value", ds.Subject)
	require.Equal(t, "AVRO", ds.Type)
	require.Contains(t, ds.Definition, `"name":"Order"`)
}

// TestBuildDesiredSchemasConfigMapKeyRefErrors verifies that a configMapKeyRef
// schema source in the CLI pipeline produces a clean error (not a panic), since
// configMapKeyRef is operator-only (spec §11).
func TestBuildDesiredSchemasConfigMapKeyRefErrors(t *testing.T) {
	_, err := Build(Options{
		Filenames:          []string{"testdata/topic-schema-configmapkeyref.yaml"},
		ClusterConfigFiles: []string{"testdata/cluster-schema.yaml"},
		RequireClusters:    true,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "configMapKeyRef is not supported in CLI mode")
}

func TestBuildNoSchemaEmpty(t *testing.T) {
	plan, err := Build(Options{
		Filenames:          []string{"testdata/manifests-happy.yaml"},
		ClusterConfigFiles: []string{"testdata/cluster-prod.yaml"},
		RequireClusters:    true,
	})
	require.NoError(t, err)
	require.Empty(t, plan.DesiredSchemas)
}

func TestBuildManifestProvidedClusterMerges(t *testing.T) {
	plan, err := Build(Options{
		Filenames:       []string{"testdata/manifest-with-cluster.yaml"},
		RequireClusters: true,
	})
	require.NoError(t, err)
	require.Contains(t, plan.Clusters, "inline")
	require.Len(t, plan.Topics, 1)
	require.Equal(t, "inline-topic", plan.Topics[0].Name)
	require.Equal(t, "inline", plan.SelectedCluster)
}

// TestBuildSubjectCollisionErrors verifies that when a topic's value and key
// schemas resolve to the same subject under RecordName strategy, the pipeline
// Build returns an error (the collision is detected at subject-name computation
// time, which blocks the plan from being built).
func TestBuildSubjectCollisionErrors(t *testing.T) {
	_, err := Build(Options{
		Filenames:          []string{"testdata/topic-schema-collision.yaml"},
		ClusterConfigFiles: []string{"testdata/cluster-schema.yaml"},
		RequireClusters:    true,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "same subject")
}

// TestBuildThreadsDriftIgnoreFields: spec.drift.ignoreFields reaches the
// diff's DesiredTopic so the engine can exclude the fields from drift (§16).
func TestBuildThreadsDriftIgnoreFields(t *testing.T) {
	plan, err := Build(Options{
		Filenames:          []string{"testdata/topic-ignorefields.yaml"},
		ClusterConfigFiles: []string{"testdata/cluster-prod.yaml"},
		RequireClusters:    true,
	})
	require.NoError(t, err)
	require.Len(t, plan.DesiredTopics, 1)
	require.Equal(t, []string{"config.retention.ms", "partitions"}, plan.DesiredTopics[0].IgnoreFields)
}

// TestBuildDesiredQuotas verifies that a KafkaQuota manifest is compiled into
// DesiredQuotas with the correct entity and limits, and appears under the right
// cluster (spec §39).
func TestBuildDesiredQuotas(t *testing.T) {
	plan, err := Build(Options{
		Filenames:          []string{"testdata/quota-prod.yaml"},
		ClusterConfigFiles: []string{"testdata/cluster-prod.yaml"},
		RequireClusters:    true,
	})
	require.NoError(t, err)
	require.Len(t, plan.DesiredQuotas, 1)
	dq := plan.DesiredQuotas[0]
	// Entity: user component; "User:" prefix stripped by quota.Compile.
	require.Len(t, dq.Entity, 1)
	require.Equal(t, "user", dq.Entity[0].Type)
	require.NotNil(t, dq.Entity[0].Name)
	require.Equal(t, "svc-checkout", *dq.Entity[0].Name)
	// Limits
	require.InDelta(t, 1048576.0, dq.Limits["producer_byte_rate"], 0.001)
	// Mode "" (no spec.reconciliation) is not explicitly defaulted; the executor
	// treats "" identically to Enforce (only DetectOnly/ObserveOnly are report-only).
	require.Equal(t, "", dq.Mode)
}

// TestBuildDesiredQuotasClusterFilter verifies that --cluster filters KafkaQuota
// objects to only the selected cluster's quotas.
func TestBuildDesiredQuotasClusterFilter(t *testing.T) {
	// The manifests-ab.yaml multi-cluster fixture has no quotas; use the
	// single-cluster quota fixture and request the matching cluster explicitly.
	plan, err := Build(Options{
		Filenames:          []string{"testdata/quota-prod.yaml"},
		ClusterConfigFiles: []string{"testdata/cluster-prod.yaml"},
		Cluster:            "prod",
		RequireClusters:    true,
	})
	require.NoError(t, err)
	require.Len(t, plan.DesiredQuotas, 1)

	// Selecting a different cluster yields no quotas.
	plan2, err := Build(Options{
		Filenames:          []string{"testdata/quota-prod.yaml"},
		ClusterConfigFiles: []string{"testdata/clusters-ab.yaml"},
		Cluster:            "a",
		RequireClusters:    true,
	})
	require.NoError(t, err)
	require.Empty(t, plan2.DesiredQuotas)
}

// TestBuildDesiredRoleBindings verifies that a KafkaRoleBinding manifest for a
// cluster with authorization.mds configured produces DesiredRoleBindings and a
// RoleBindingScope (spec §40).
func TestBuildDesiredRoleBindings(t *testing.T) {
	plan, err := Build(Options{
		Filenames:          []string{"testdata/rolebinding-prod.yaml"},
		ClusterConfigFiles: []string{"testdata/cluster-mds.yaml"},
		RequireClusters:    true,
	})
	require.NoError(t, err)
	require.Len(t, plan.RoleBindings, 1)
	require.Len(t, plan.DesiredRoleBindings, 1)

	drb := plan.DesiredRoleBindings[0]
	require.Equal(t, "User:alice", drb.Principal)
	require.Equal(t, "DeveloperRead", drb.Role)
	require.Equal(t, "kafka", drb.Scope.Type)
	require.Equal(t, "lkc-abc123", drb.Scope.KafkaCluster)
	require.NotNil(t, drb.Resource)
	require.Equal(t, "Topic", drb.Resource.Type)
	require.Equal(t, "payments.orders", drb.Resource.Name)
	require.Equal(t, "literal", drb.Resource.PatternType)

	// RoleBindingScope carries the (Principal, Role, Scope) entry.
	require.NotEmpty(t, plan.RoleBindingScope)
}

// TestBuildRoleBindingNoMDSErrors verifies that a KafkaRoleBinding referencing
// a cluster without authorization.mds produces a clean compile error (not a
// panic). Task 7 adds a formal validator; the pipeline surfaces this gracefully.
func TestBuildRoleBindingNoMDSErrors(t *testing.T) {
	_, err := Build(Options{
		Filenames:          []string{"testdata/rolebinding-no-mds.yaml"},
		ClusterConfigFiles: []string{"testdata/cluster-prod.yaml"},
		RequireClusters:    true,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "authorization.mds")
}

// TestPipelineRBACOnlyEmitsRoleBindingsNotACLs: a topic on an [rbac]-only
// cluster must produce role bindings and NO ACLs (spec §40).
func TestPipelineRBACOnlyEmitsRoleBindingsNotACLs(t *testing.T) {
	plan := buildPlanForBackends(t, []string{"rbac"})
	require.Empty(t, plan.DesiredACLs, "rbac-only cluster must not emit topic ACLs")
	require.NotEmpty(t, plan.DesiredRoleBindings, "rbac-only cluster must emit role bindings")
}

// TestPipelineACLOnlyEmitsACLsNotRoleBindings: a topic on an [acl]-only cluster
// (default when accessBackends is unset) must produce ACLs and NO role bindings.
func TestPipelineACLOnlyEmitsACLsNotRoleBindings(t *testing.T) {
	plan := buildPlanForBackends(t, nil) // unset → ["acl"]
	require.NotEmpty(t, plan.DesiredACLs)
	require.Empty(t, plan.DesiredRoleBindings)
}

// TestPipelineDualEmit: a topic on an [acl,rbac] cluster must produce BOTH
// ACLs and role bindings (dual-emit, spec §40).
func TestPipelineDualEmit(t *testing.T) {
	plan := buildPlanForBackends(t, []string{"acl", "rbac"})
	require.NotEmpty(t, plan.DesiredACLs)
	require.NotEmpty(t, plan.DesiredRoleBindings)
}

// TestPipelineRecordsCoarseningWarnings: a producer entry with a non-"*" host
// on an rbac-backed cluster triggers a coarsening warning recorded on the plan.
func TestPipelineRecordsCoarseningWarnings(t *testing.T) {
	plan := buildPlanForBackendsWithHost(t, []string{"rbac"}, "10.0.0.1")
	require.NotEmpty(t, plan.RBACWarnings)
}

// ---- KafkaUser pipeline tests (v0.35 §T2) ----

// TestPipelineUserValueFromPasses: a KafkaUser with an env-sourced password
// flows through load -> default -> validate and lands on the Plan.
func TestPipelineUserValueFromPasses(t *testing.T) {
	t.Setenv("SVC_CHECKOUT_PASSWORD", "hunter2")
	plan, err := Build(Options{
		Filenames:          []string{"testdata/user-valid.yaml"},
		ClusterConfigFiles: []string{"testdata/cluster-prod.yaml"},
		RequireClusters:    true,
	})
	require.NoError(t, err)
	require.Len(t, plan.Users, 1)
	require.Equal(t, "svc-checkout", plan.Users[0].Spec.Username, "defaulting.User resolves username from metadata.name")
	require.Equal(t, "Delete", plan.Users[0].Spec.DeletionPolicy, "defaulting.User applies the default deletionPolicy")
}

// TestPipelineUserGenerateRejectedByCLI: spec.password.generate is shape-valid
// (core validation accepts it, since the operator/webhook path must), but the
// CLI pipeline is the earliest CLI-side seam that can reject it — the CLI has
// no way to generate/store a credential, so silently ignoring generate would
// let validate/diff/apply proceed against a manifest they can never satisfy.
func TestPipelineUserGenerateRejectedByCLI(t *testing.T) {
	_, err := Build(Options{
		Filenames:          []string{"testdata/user-generate.yaml"},
		ClusterConfigFiles: []string{"testdata/cluster-prod.yaml"},
		RequireClusters:    true,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "operator-only")
}

// TestPipelineUserValidateModeNoClusters: the validate command runs without
// --cluster-config-file, so shape/identity checks must still run without
// requiring cluster resolution.
func TestPipelineUserValidateModeNoClusters(t *testing.T) {
	t.Setenv("SVC_CHECKOUT_PASSWORD", "hunter2")
	plan, err := Build(Options{
		Filenames:       []string{"testdata/user-valid.yaml"},
		RequireClusters: false,
	})
	require.NoError(t, err)
	require.Len(t, plan.Users, 1)
}
