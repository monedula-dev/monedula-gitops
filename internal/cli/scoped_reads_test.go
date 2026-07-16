package cli

// Scoped live reads (least-privilege credentials): computeOps reads live state
// ONLY for the kinds the manifests declare, so a credential that can e.g. only
// manage topics (Confluent Cloud API keys cannot DescribeClientQuotas; scoped
// on-prem principals may lack Describe on Cluster) can still run a topic-only
// pipeline. The fixtures inject read failures at the mock seam
// (testdata/clusters/scoped-state.yaml denies ListACLs+ListQuotas;
// scoped-topics-denied-state.yaml denies ListTopics): a command succeeding
// despite an injected failure proves the read never happened, and a command
// surfacing it proves the gate still fires when the kind IS declared.

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestTopicOnlyCommandsSkipACLAndQuotaReads: a topic-only manifest set must
// never call ListACLs or ListQuotas — the fixture would fail both with
// CLUSTER_AUTHORIZATION_FAILED (the exact Cloud symptom this pins).
func TestTopicOnlyCommandsSkipACLAndQuotaReads(t *testing.T) {
	for _, cmd := range []string{"diff", "verify", "apply"} {
		out, err := run(t, cmd, "-f", "testdata/scoped-topic",
			"--cluster-config-file", "testdata/clusters/scoped.yaml")
		require.NoError(t, err,
			"%s of a topic-only manifest set must not read ACLs/quotas:\n%s", cmd, out)
	}
}

// TestQuotaOnlyCommandsSkipTopicRead: the inverse — quota-only manifests must
// not call ListTopics.
func TestQuotaOnlyCommandsSkipTopicRead(t *testing.T) {
	out, err := run(t, "diff", "-f", "testdata/scoped-quota",
		"--cluster-config-file", "testdata/clusters/scoped-topics-denied.yaml")
	require.NoError(t, err, "diff of a quota-only manifest set must not read topics:\n%s", out)
	require.Contains(t, out, "SetQuota")

	out, err = run(t, "apply", "-f", "testdata/scoped-quota",
		"--cluster-config-file", "testdata/clusters/scoped-topics-denied.yaml")
	require.NoError(t, err, "apply of a quota-only manifest set must not read topics:\n%s", out)
	require.Contains(t, out, "Succeeded SetQuota")
}

// TestTopicWithAccessStillReadsACLs: an access block compiles DesiredACLs, so
// the ACL read must still happen — and the simulated authorization failure
// must surface as exit 2 with the broker's error.
func TestTopicWithAccessStillReadsACLs(t *testing.T) {
	_, err := run(t, "diff", "-f", "testdata/scoped-topic-access",
		"--cluster-config-file", "testdata/clusters/scoped.yaml")
	var ee *ExitError
	require.ErrorAs(t, err, &ee)
	require.Equal(t, 2, ee.Code)
	require.Contains(t, ee.Msg, "CLUSTER_AUTHORIZATION_FAILED")
}
