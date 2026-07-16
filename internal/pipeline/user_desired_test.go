package pipeline

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// ---- KafkaUser desired-state compile tests (v0.35 §T4) ----

// TestPipelineUserCompilesDesired: a valid KafkaUser compiles into
// Plan.DesiredUsers carrying the observable identity (defaulted username +
// mechanism) and the password REFERENCE — never a resolved value. The env var
// is deliberately NOT set: the pipeline must not resolve passwords (that is
// the executor's execute-time job).
func TestPipelineUserCompilesDesired(t *testing.T) {
	plan, err := Build(Options{
		Filenames:          []string{"testdata/user-valid.yaml"},
		ClusterConfigFiles: []string{"testdata/cluster-prod.yaml"},
		RequireClusters:    true,
	})
	require.NoError(t, err, "building the plan must not resolve the (unset) password env var")
	require.Len(t, plan.DesiredUsers, 1)
	du := plan.DesiredUsers[0]
	require.Equal(t, "svc-checkout", du.Credential.Username, "username defaulted from metadata.name")
	require.Equal(t, "SCRAM-SHA-512", du.Credential.Mechanism)
	require.Equal(t, int32(0), du.Credential.Iterations, "unset iterations compile to 0 (never compared)")
	require.NotNil(t, du.PasswordRef)
	require.Equal(t, "SVC_CHECKOUT_PASSWORD", du.PasswordRef.ValueFrom.Env)
}

// TestPipelineUserDesiredEmptyWithoutSingleCluster: with users spanning two
// clusters and no --cluster selection, the flat DesiredUsers stays empty
// (mirroring the quota/ACL single-cluster flat-select pattern); a --cluster
// selection narrows and compiles only that cluster's users.
func TestPipelineUserDesiredEmptyWithoutSingleCluster(t *testing.T) {
	plan, err := Build(Options{
		Filenames:          []string{"testdata/users-ab.yaml"},
		ClusterConfigFiles: []string{"testdata/clusters-ab.yaml"},
		RequireClusters:    true,
	})
	require.NoError(t, err)
	require.Empty(t, plan.SelectedCluster)
	require.Empty(t, plan.DesiredUsers)
	require.Len(t, plan.Users, 2, "the raw resources still ride on the plan")

	selected, err := Build(Options{
		Filenames:          []string{"testdata/users-ab.yaml"},
		ClusterConfigFiles: []string{"testdata/clusters-ab.yaml"},
		Cluster:            "a",
		RequireClusters:    true,
	})
	require.NoError(t, err)
	require.Len(t, selected.DesiredUsers, 1)
	require.Equal(t, "user-a", selected.DesiredUsers[0].Credential.Username)
}
