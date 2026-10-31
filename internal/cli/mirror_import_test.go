package cli

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestImportSummaryPartialJSON(t *testing.T) {
	stdout, stderr, err := runSplit(t, "import", "cluster", "-o", "json",
		"-c", "testdata/clusters/mirror-import-cluster.yaml")
	require.NoError(t, err, stderr)
	require.Contains(t, stdout, "kind: KafkaMirrorTopic")
	require.Contains(t, stdout, "sourceTopicName: payments.orders")
	require.NotContains(t, stdout, "payments.pending")

	var out struct {
		Summary struct {
			Topics  int  `json:"topics"`
			Mirrors int  `json:"mirrors"`
			Partial bool `json:"partial"`
		} `json:"summary"`
		Warnings []string `json:"warnings"`
	}
	require.NoError(t, json.Unmarshal([]byte(stderr), &out), stderr)
	require.True(t, out.Summary.Partial)
	require.Equal(t, 1, out.Summary.Mirrors)
	require.Equal(t, 1, out.Summary.Topics)
	require.Contains(t, stderr, `"partial": true`)
	require.Contains(t, out.Warnings,
		`MirrorImportDeferred: mirror topic "payments.pending" is being promoted (PENDING_STOPPED); re-run import after it finishes`)
}

func TestImportMirrorRESTDownExit2(t *testing.T) {
	_, _, err := runSplit(t, "import", "cluster",
		"-c", "testdata/clusters/mirror-rest-down-cluster.yaml")
	requireExitCode(t, err, 2)
}
