package operations

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestUserTaxonomy pins the v0.35 SCRAM credential op rows (spec §2-§4).
// Create establishes a brand-new credential and Rotate is the explicit
// --rotate-passwords re-upsert — both Low, ungated. Update re-writes a live
// credential (Medium) but stays ungated: it converges the DECLARED credential,
// like UpdateQuota. Delete removes a principal's ability to authenticate and
// follows the RemoveQuota precedent: Medium + destructive-gated.
func TestUserTaxonomy(t *testing.T) {
	require.Equal(t, RiskLow, RiskOf(CreateScramCredential))
	require.Equal(t, GateNone, GateOf(CreateScramCredential))

	require.Equal(t, RiskMedium, RiskOf(UpdateScramCredential))
	require.Equal(t, GateNone, GateOf(UpdateScramCredential))

	require.Equal(t, RiskLow, RiskOf(RotateScramCredential))
	require.Equal(t, GateNone, GateOf(RotateScramCredential))

	require.Equal(t, RiskMedium, RiskOf(DeleteScramCredential))
	require.Equal(t, GateDestructive, GateOf(DeleteScramCredential))

	del := New(DeleteScramCredential)
	require.True(t, del.RequiresApproval, "the gated delete must require approval")
	upd := New(UpdateScramCredential)
	require.False(t, upd.RequiresApproval, "ungated update must not require approval")
}
