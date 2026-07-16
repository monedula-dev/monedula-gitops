package cli

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// The prune fixture diverges from mock state by exactly one in-scope live ACL
// that is no longer desired — a prune candidate (spec §10.3).

func TestApplyReportsPruneCandidateWithoutFlag(t *testing.T) {
	// Without --prune the candidate is reported but never deleted, and the run
	// does not fail because of it.
	out, err := run(t, "apply", "-f", "testdata/prune",
		"--cluster-config-file", "testdata/clusters/prune-cluster.yaml")
	require.NoError(t, err) // PruneDisabled is not a failure -> exit 0
	require.Contains(t, out, "PruneDisabled DeleteAcl")
	require.NotContains(t, out, "Succeeded DeleteAcl")
}

func TestApplyPruneFlagDeletes(t *testing.T) {
	out, err := run(t, "apply", "-f", "testdata/prune",
		"--cluster-config-file", "testdata/clusters/prune-cluster.yaml", "--prune")
	require.NoError(t, err)
	require.Contains(t, out, "Succeeded DeleteAcl")
}

func TestApplyDestructiveFlagDoesNotPrune(t *testing.T) {
	// DeleteAcl moved out of the destructive gate (spec §10.3): only --prune
	// enables it.
	out, err := run(t, "apply", "-f", "testdata/prune",
		"--cluster-config-file", "testdata/clusters/prune-cluster.yaml", "--allow-destructive")
	require.NoError(t, err)
	require.Contains(t, out, "PruneDisabled DeleteAcl")
	require.NotContains(t, out, "Succeeded DeleteAcl")
}

func TestDiffMarksPruneCandidate(t *testing.T) {
	out, err := run(t, "diff", "-f", "testdata/prune",
		"--cluster-config-file", "testdata/clusters/prune-cluster.yaml")
	require.NoError(t, err)
	require.Contains(t, out, "DeleteAcl")
	require.Contains(t, out, "(prune candidate; enable with --prune)")
}

func TestVerifyPruneCandidateIsDrift(t *testing.T) {
	// Prune candidates ARE divergence: verify keeps the exit-1 contract even
	// though apply would not delete them without --prune.
	_, err := run(t, "verify", "-f", "testdata/prune",
		"--cluster-config-file", "testdata/clusters/prune-cluster.yaml")
	requireExitCode(t, err, 1)
}
