package cli

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// The modes fixture has three topics — Enforce, DetectOnly, ObserveOnly — each
// drifting from the mock state by one UpdateTopicConfig op (spec §16).

func TestApplyMutatesOnlyEnforceResources(t *testing.T) {
	out, err := run(t, "apply", "-f", "testdata/modes",
		"--cluster-config-file", "testdata/clusters/modes-cluster.yaml")
	require.NoError(t, err) // ReportOnly entries are not failures -> exit 0

	// Only the Enforce topic's op executes.
	require.Contains(t, out, "Succeeded UpdateTopicConfig KafkaTopic/modes.enforce")
	// DetectOnly/ObserveOnly drift is rendered as report-only, never executed.
	require.Contains(t, out, "ReportOnly UpdateTopicConfig KafkaTopic/modes.detect")
	require.Contains(t, out, "ReportOnly UpdateTopicConfig KafkaTopic/modes.observe")
	require.NotContains(t, out, "Succeeded UpdateTopicConfig KafkaTopic/modes.detect")
	require.NotContains(t, out, "Succeeded UpdateTopicConfig KafkaTopic/modes.observe")
}

func TestApplyDryRunShowsModeMarkers(t *testing.T) {
	out, err := run(t, "apply", "-f", "testdata/modes",
		"--cluster-config-file", "testdata/clusters/modes-cluster.yaml", "--dry-run")
	require.NoError(t, err)
	require.Contains(t, out, "modes.enforce")
	require.Contains(t, out, "(mode=DetectOnly, report-only)")
	require.Contains(t, out, "(mode=ObserveOnly, report-only)")
}

func TestVerifyFailsOnDetectOnlyDrift(t *testing.T) {
	// Enforce + DetectOnly drift counts toward verify's exit-1 contract.
	_, err := run(t, "verify", "-f", "testdata/modes",
		"--cluster-config-file", "testdata/clusters/modes-cluster.yaml")
	requireExitCode(t, err, 1)
}

func TestVerifyIgnoresObserveOnlyDrift(t *testing.T) {
	// Drift exclusively on ObserveOnly resources renders but does not fail.
	out, err := run(t, "verify", "-f", "testdata/modes-observe",
		"--cluster-config-file", "testdata/clusters/modes-observe-cluster.yaml")
	require.NoError(t, err)
	require.Contains(t, out, "UpdateTopicConfig") // drift is still reported
	require.Contains(t, out, "(mode=ObserveOnly, report-only)")
}
