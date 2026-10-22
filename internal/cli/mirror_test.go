package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// KafkaMirrorTopic end to end through the CLI. Every fixture cluster is named
// "dr" and carries both the Kafka mock (mock-state-file) and the Kafka REST
// mock (mock-rest-file); the link prod-to-dr is ACTIVE, has no prefix and
// syncs only cleanup.policy, so the declared retention.ms is settable.

func TestDiffMirrorCreate(t *testing.T) {
	out, err := run(t, "diff", "-f", "testdata/mirror/create",
		"--cluster-config-file", "testdata/clusters/mirror-cluster.yaml")
	require.NoError(t, err, out)
	require.Contains(t, out, "CreateMirrorTopic")
	require.Contains(t, out, "payments.orders")
	require.NotContains(t, out, "StateReadError")
}

func TestApplyMirrorCreate(t *testing.T) {
	out, err := run(t, "apply", "-f", "testdata/mirror/create",
		"--cluster-config-file", "testdata/clusters/mirror-cluster.yaml")
	require.NoError(t, err, out)
	require.Contains(t, out, "Succeeded CreateMirrorTopic KafkaMirrorTopic/orders-dr payments.orders")
}

func TestVerifyMirrorUnhealthyExit1(t *testing.T) {
	out, err := run(t, "verify", "-f", "testdata/mirror/create",
		"--cluster-config-file", "testdata/clusters/mirror-unhealthy-cluster.yaml")
	requireExitCode(t, err, 1)
	require.Contains(t, out, "MirrorTopicUnhealthy")
}

func TestVerifyMirrorProgressingExit0(t *testing.T) {
	out, err := run(t, "verify", "-f", "testdata/mirror/create",
		"--cluster-config-file", "testdata/clusters/mirror-progressing-cluster.yaml")
	require.NoError(t, err, out)
	require.Contains(t, out, "MirrorTopicProgressing") // still rendered
}

func TestVerifyObserveOnlyRestDownExit2(t *testing.T) {
	// An unreadable live state is an operational error, not ObserveOnly drift:
	// verify must not report success it cannot prove.
	out, err := run(t, "verify", "-f", "testdata/mirror/observe",
		"--cluster-config-file", "testdata/clusters/mirror-rest-down-cluster.yaml")
	requireExitCode(t, err, 2)
	require.Contains(t, err.Error(), "live state could not be read; see StateReadError operations")
	require.Contains(t, out, "StateReadError")
}

func TestDiffTopicOnMirrorRejected(t *testing.T) {
	out, err := run(t, "diff", "-f", "testdata/mirror/topic-on-mirror",
		"--cluster-config-file", "testdata/clusters/mirror-unhealthy-cluster.yaml")
	require.NoError(t, err, out)
	require.Contains(t, out, "Rejected")
	require.Contains(t, out, "is a mirror topic")
}

func TestPolicyOnlyRunDoesNotContactREST(t *testing.T) {
	// The REST mock fails every method: any contact would surface as a
	// StateReadError or an error exit.
	out, err := run(t, "diff", "-f", "testdata/mirror/policy",
		"--cluster-config-file", "testdata/clusters/mirror-policy-cluster.yaml")
	require.NoError(t, err, out)
	require.NotContains(t, out, "StateReadError")

	// Stronger: with the REST mock file missing, even building the client
	// would fail the run with exit 2. A policy-only run must not build it.
	dir := t.TempDir()
	cluster, err := os.ReadFile("testdata/clusters/mirror-policy-cluster.yaml")
	require.NoError(t, err)
	cluster = []byte(strings.Replace(string(cluster), "mirror-rest-allfail.yaml", "does-not-exist.yaml", 1))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "cluster.yaml"), cluster, 0o644))
	state, err := os.ReadFile("testdata/clusters/mirror-state.yaml")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "mirror-state.yaml"), state, 0o644))

	out, err = run(t, "diff", "-f", "testdata/mirror/policy",
		"--cluster-config-file", filepath.Join(dir, "cluster.yaml"))
	require.NoError(t, err, out)
}

func TestClusterIDMismatchShowsStateReadError(t *testing.T) {
	out, err := run(t, "diff", "-f", "testdata/mirror/create",
		"--cluster-config-file", "testdata/clusters/mirror-idmismatch-cluster.yaml")
	require.NoError(t, err, out)
	require.Contains(t, out, "StateReadError")
	require.Contains(t, out, "ClusterIdentityMismatch")
	require.NotContains(t, out, "CreateMirrorTopic")
}

func TestMissingRESTMockFileExit2(t *testing.T) {
	// A REST client build error (here: unreadable mock file) is exit 2, not a
	// StateReadError op.
	dir := t.TempDir()
	cluster, err := os.ReadFile("testdata/clusters/mirror-cluster.yaml")
	require.NoError(t, err)
	cluster = []byte(strings.Replace(string(cluster), "mirror-rest.yaml", "does-not-exist.yaml", 1))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "cluster.yaml"), cluster, 0o644))
	state, err := os.ReadFile("testdata/clusters/mirror-state.yaml")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "mirror-state.yaml"), state, 0o644))

	_, err = run(t, "diff", "-f", "testdata/mirror/create",
		"--cluster-config-file", filepath.Join(dir, "cluster.yaml"))
	requireExitCode(t, err, 2)
}

// A plain KafkaTopic manifest on a cluster with spec.rest: the mirror guard
// reads Kafka REST, and an unreadable mirror state fails closed.
func TestVerifyTopicOnlyRestDownExit2(t *testing.T) {
	out, err := run(t, "verify", "-f", "testdata/mirror/topic-only",
		"--cluster-config-file", "testdata/clusters/mirror-rest-down-cluster.yaml")
	requireExitCode(t, err, 2)
	require.Contains(t, out, "StateReadError")
	require.Contains(t, err.Error(), "live state could not be read; see StateReadError operations")
	require.Contains(t, out, "StateReadError KafkaTopic/payments.events")
	require.Contains(t, out, `cannot read mirror state for topic "payments.events"`)
	require.NotContains(t, out, "CreateTopic")
}

func TestDiffTopicOnlyRestHealthy(t *testing.T) {
	out, err := run(t, "diff", "-f", "testdata/mirror/topic-only",
		"--cluster-config-file", "testdata/clusters/mirror-cluster.yaml")
	require.NoError(t, err, out)
	require.Contains(t, out, "CreateTopic")
	require.Contains(t, out, "payments.events")
	require.NotContains(t, out, "StateReadError")
}
