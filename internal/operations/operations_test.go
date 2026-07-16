package operations

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewDeleteTopic(t *testing.T) {
	op := New(DeleteTopic)
	require.Equal(t, RiskCritical, op.Risk)
	require.True(t, op.RequiresApproval)
}

func TestNewCreateTopic(t *testing.T) {
	op := New(CreateTopic)
	require.Equal(t, RiskLow, op.Risk)
	require.False(t, op.RequiresApproval)
}

func TestNewIncreasePartitions(t *testing.T) {
	op := New(IncreasePartitions)
	require.Equal(t, RiskMedium, op.Risk)
	require.True(t, op.RequiresApproval)
}

func TestNewDeleteAclUsesPruneGate(t *testing.T) {
	// Spec §10.3: DeleteAcl is gated by explicit prune consent (--prune /
	// spec.prune), no longer by --allow-destructive.
	op := New(DeleteAcl)
	require.Equal(t, RiskMedium, op.Risk)
	require.True(t, op.RequiresApproval)
	require.Equal(t, GatePrune, GateOf(DeleteAcl))
}

func TestQuotaTaxonomy(t *testing.T) {
	// Set/UpdateQuota are reversible (no data loss) and stay ungated; RemoveQuota
	// authoritatively DELETES a live limit (unthrottles a client) and is gated by
	// --allow-destructive / the allow-destructive annotation (spec §17.1) — it
	// must not be the only authoritative deletion without a gate.
	require.Equal(t, RiskLow, RiskOf(SetQuota))
	require.Equal(t, GateNone, GateOf(SetQuota))
	require.Equal(t, RiskLow, RiskOf(UpdateQuota))
	require.Equal(t, GateNone, GateOf(UpdateQuota))

	op := New(RemoveQuota)
	require.Equal(t, RiskMedium, op.Risk)
	require.True(t, op.RequiresApproval)
	require.Equal(t, GateDestructive, GateOf(RemoveQuota))
}

func TestNewRejected(t *testing.T) {
	op := New(Rejected)
	require.Equal(t, RiskNone, op.Risk)
	require.Equal(t, Risk(""), op.Risk)
	require.False(t, op.RequiresApproval)
}
