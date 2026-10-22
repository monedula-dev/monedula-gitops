package cli

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// mirrorCheckFixture writes a scenario dir (mirror-only expect.yaml) and a
// cluster manifest into a temp dir and returns (scenarioDir, clusterFile).
// restFile "" produces a cluster without spec.rest.
func mirrorCheckFixture(t *testing.T, expect, restFile string) (string, string) {
	t.Helper()
	dir := t.TempDir()
	scn := filepath.Join(dir, "scn")
	require.NoError(t, os.MkdirAll(scn, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(scn, "expect.yaml"), []byte(expect), 0o644))
	cluster := "apiVersion: gitops.monedula.dev/v1alpha1\nkind: KafkaCluster\nmetadata:\n  name: dr\n"
	if restFile != "" {
		cluster += "  annotations:\n    gitops.monedula.dev/mock-rest-file: rest.yaml\n"
		require.NoError(t, os.WriteFile(filepath.Join(dir, "rest.yaml"), []byte(restFile), 0o644))
	}
	cluster += "spec:\n  bootstrapServers: localhost:9092\n"
	if restFile != "" {
		cluster += "  rest:\n    endpoint: https://broker-0.dr:8090/kafka\n"
	}
	cf := filepath.Join(dir, "cluster.yaml")
	require.NoError(t, os.WriteFile(cf, []byte(cluster), 0o644))
	return scn, cf
}

const mirrorExpect = "liveState:\n  mirrors:\n    - name: orders\n      linkName: src-to-dest\n      status: ACTIVE\n"

func TestE2ECheckMirrorsReadErrorFails(t *testing.T) {
	scn, cf := mirrorCheckFixture(t, mirrorExpect,
		"clusterId: c\nfailures:\n  - method: ListMirrors\n    error: rest exploded\n")
	out, err := run(t, "e2e", "check", "--scenario", scn, "--mode", "cli", "--cluster-config", cf)
	requireExitCode(t, err, 1)
	require.Contains(t, out, "rest exploded")
}

func TestE2ECheckMirrorsNoRESTFails(t *testing.T) {
	scn, cf := mirrorCheckFixture(t, mirrorExpect, "")
	out, err := run(t, "e2e", "check", "--scenario", scn, "--mode", "cli", "--cluster-config", cf)
	requireExitCode(t, err, 1)
	require.Contains(t, out, "cluster has no spec.rest")
}

func TestE2ECheckMirrorsAbsentOnlyNoRESTFails(t *testing.T) {
	scn, cf := mirrorCheckFixture(t, "liveState:\n  absent:\n    mirrors:\n      - ledger\n", "")
	out, err := run(t, "e2e", "check", "--scenario", scn, "--mode", "cli", "--cluster-config", cf)
	requireExitCode(t, err, 1)
	require.Contains(t, out, "cluster has no spec.rest")
}

func TestE2ECheckMirrorsMatchPasses(t *testing.T) {
	scn, cf := mirrorCheckFixture(t, mirrorExpect,
		"clusterId: c\nmirrors:\n  - linkName: src-to-dest\n    mirrorTopicName: orders\n    sourceTopicName: orders\n    status: ACTIVE\n")
	out, err := run(t, "e2e", "check", "--scenario", scn, "--mode", "cli", "--cluster-config", cf)
	require.NoError(t, err, out)
	require.Contains(t, out, "[PASS] mirror orders")
}
