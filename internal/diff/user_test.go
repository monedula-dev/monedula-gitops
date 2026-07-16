package diff

import (
	"testing"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	"github.com/monedula-dev/monedula-gitops/internal/operations"
	"github.com/monedula-dev/monedula-gitops/internal/user"
	"github.com/stretchr/testify/require"
)

// userOps filters the SCRAM credential ops out of a Compute result.
func userOps(res []operations.Operation) []operations.Operation {
	var out []operations.Operation
	for _, op := range res {
		switch op.Action {
		case operations.CreateScramCredential, operations.UpdateScramCredential,
			operations.RotateScramCredential, operations.DeleteScramCredential:
			out = append(out, op)
		}
	}
	return out
}

func envRef(name string) *v1alpha1.ValueFrom {
	return &v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{Env: name}}
}

func desiredUser(username, mechanism string, iterations int32) user.Desired {
	return user.Desired{
		Credential:  user.Credential{Username: username, Mechanism: mechanism, Iterations: iterations},
		PasswordRef: envRef("PW_" + username),
	}
}

// Absent (username, mechanism) with no other live mechanism -> Create.
func TestUserAbsentCreates(t *testing.T) {
	desired := Desired{Users: []user.Desired{desiredUser("svc-payments", "SCRAM-SHA-512", 8192)}}
	res := userOps(Compute(desired, Live{}))
	require.Len(t, res, 1)
	require.Equal(t, operations.CreateScramCredential, res[0].Action)
	require.Equal(t, operations.RiskLow, res[0].Risk)
	require.False(t, res[0].RequiresApproval)
	require.Equal(t, "KafkaUser", res[0].Kind)
	require.Equal(t, "svc-payments (SCRAM-SHA-512)", res[0].Target)
	// Executable payload: identity + password *reference* (never a value).
	require.Equal(t, "svc-payments", res[0].ScramUser)
	require.Equal(t, "SCRAM-SHA-512", res[0].ScramMechanism)
	require.Equal(t, int32(8192), res[0].ScramIterations)
	require.Empty(t, res[0].ScramDeleteMechanism)
	require.NotNil(t, res[0].PasswordRef)
	require.Equal(t, "PW_svc-payments", res[0].PasswordRef.ValueFrom.Env)
}

// Declared mechanism present, pinned iterations differ -> Update (iterations).
func TestUserIterationsMismatchUpdates(t *testing.T) {
	desired := Desired{Users: []user.Desired{desiredUser("svc-payments", "SCRAM-SHA-512", 8192)}}
	live := Live{ScramCredentials: []kafka.ScramCredential{
		{User: "svc-payments", Mechanism: "SCRAM-SHA-512", Iterations: 4096},
	}}
	res := userOps(Compute(desired, live))
	require.Len(t, res, 1)
	require.Equal(t, operations.UpdateScramCredential, res[0].Action)
	require.Equal(t, operations.RiskMedium, res[0].Risk)
	require.False(t, res[0].RequiresApproval)
	require.Equal(t, "iterations", res[0].Field)
	require.Equal(t, "4096", res[0].From)
	require.Equal(t, "8192", res[0].To)
	require.Empty(t, res[0].ScramDeleteMechanism, "iterations drift must not delete anything")
	require.NotNil(t, res[0].PasswordRef)
}

// Declared mechanism absent, live has only the OTHER mechanism -> ONE Update
// carrying ScramDeleteMechanism (upsert declared + delete other at apply).
func TestUserMechanismSwapUpdatesWithDelete(t *testing.T) {
	desired := Desired{Users: []user.Desired{desiredUser("svc-payments", "SCRAM-SHA-512", 0)}}
	live := Live{ScramCredentials: []kafka.ScramCredential{
		{User: "svc-payments", Mechanism: "SCRAM-SHA-256", Iterations: 4096},
	}}
	res := userOps(Compute(desired, live))
	require.Len(t, res, 1, "a mechanism change must be ONE op, not an update+delete pair")
	require.Equal(t, operations.UpdateScramCredential, res[0].Action)
	require.Equal(t, "mechanism", res[0].Field)
	require.Equal(t, "SCRAM-SHA-256", res[0].From)
	require.Equal(t, "SCRAM-SHA-512", res[0].To)
	require.Equal(t, "SCRAM-SHA-512", res[0].ScramMechanism)
	require.Equal(t, "SCRAM-SHA-256", res[0].ScramDeleteMechanism)
	require.False(t, res[0].RequiresApproval,
		"the folded delete is part of an ungated Update; no standalone gated delete is emitted")
}

// Declared + identical live identity -> nothing.
func TestUserInSyncNoOp(t *testing.T) {
	desired := Desired{Users: []user.Desired{desiredUser("svc-payments", "SCRAM-SHA-512", 8192)}}
	live := Live{ScramCredentials: []kafka.ScramCredential{
		{User: "svc-payments", Mechanism: "SCRAM-SHA-512", Iterations: 8192},
	}}
	require.Empty(t, userOps(Compute(desired, live)))
}

// Iterations unset (0) in the spec -> NOT compared: any live count is in sync.
func TestUserIterationsUnsetAcceptsAnyLive(t *testing.T) {
	desired := Desired{Users: []user.Desired{desiredUser("svc-payments", "SCRAM-SHA-512", 0)}}
	live := Live{ScramCredentials: []kafka.ScramCredential{
		{User: "svc-payments", Mechanism: "SCRAM-SHA-512", Iterations: 12288},
	}}
	require.Empty(t, userOps(Compute(desired, live)))
}

// In-sync + RotatePasswords -> exactly one Rotate op (Low, ungated).
func TestUserInSyncRotateEmitsRotate(t *testing.T) {
	desired := Desired{
		Users:           []user.Desired{desiredUser("svc-payments", "SCRAM-SHA-512", 8192)},
		RotatePasswords: true,
	}
	live := Live{ScramCredentials: []kafka.ScramCredential{
		{User: "svc-payments", Mechanism: "SCRAM-SHA-512", Iterations: 8192},
	}}
	res := userOps(Compute(desired, live))
	require.Len(t, res, 1)
	require.Equal(t, operations.RotateScramCredential, res[0].Action)
	require.Equal(t, operations.RiskLow, res[0].Risk)
	require.False(t, res[0].RequiresApproval)
	require.Equal(t, "svc-payments", res[0].ScramUser)
	require.Equal(t, "SCRAM-SHA-512", res[0].ScramMechanism)
	require.Equal(t, int32(8192), res[0].ScramIterations)
	require.NotNil(t, res[0].PasswordRef, "rotate must carry the password reference for the executor")
}

// RotatePasswords with a NOT-in-sync user -> only the Create/Update (which
// upserts the new password anyway); no additional Rotate op.
func TestUserRotateNotInSyncOnlyConvergenceOp(t *testing.T) {
	desired := Desired{
		Users: []user.Desired{
			desiredUser("svc-absent", "SCRAM-SHA-512", 0),   // absent -> Create
			desiredUser("svc-drift", "SCRAM-SHA-512", 8192), // iterations drift -> Update
		},
		RotatePasswords: true,
	}
	live := Live{ScramCredentials: []kafka.ScramCredential{
		{User: "svc-drift", Mechanism: "SCRAM-SHA-512", Iterations: 4096},
	}}
	res := userOps(Compute(desired, live))
	require.Len(t, res, 2)
	require.Nil(t, findOp(res, operations.RotateScramCredential),
		"not-in-sync users are covered by Create/Update, which set the new password anyway")
	require.NotNil(t, findOp(res, operations.CreateScramCredential))
	require.NotNil(t, findOp(res, operations.UpdateScramCredential))
}

// Plain diff (no RotatePasswords) with in-sync users -> no Rotate ever.
func TestUserNoRotateWithoutFlag(t *testing.T) {
	desired := Desired{Users: []user.Desired{desiredUser("svc-payments", "SCRAM-SHA-512", 0)}}
	live := Live{ScramCredentials: []kafka.ScramCredential{
		{User: "svc-payments", Mechanism: "SCRAM-SHA-512", Iterations: 4096},
	}}
	require.Empty(t, userOps(Compute(desired, live)))
}

// An entirely undeclared live user is OUT OF SCOPE: no drift, no prune, and in
// particular no DeleteScramCredential — the CLI diff never emits it standalone.
func TestUserUndeclaredLiveUserIgnored(t *testing.T) {
	desired := Desired{Users: []user.Desired{desiredUser("svc-payments", "SCRAM-SHA-512", 0)}}
	live := Live{ScramCredentials: []kafka.ScramCredential{
		{User: "svc-payments", Mechanism: "SCRAM-SHA-512", Iterations: 4096},
		{User: "legacy-analytics", Mechanism: "SCRAM-SHA-256", Iterations: 4096},
	}}
	require.Empty(t, userOps(Compute(desired, live)))
}

// An extra live mechanism on a user whose DECLARED mechanism is present and in
// sync is invisible (declared-mechanism-only scope): no drift, never pruned.
func TestUserExtraLiveMechanismInvisible(t *testing.T) {
	desired := Desired{Users: []user.Desired{desiredUser("svc-payments", "SCRAM-SHA-512", 0)}}
	live := Live{ScramCredentials: []kafka.ScramCredential{
		{User: "svc-payments", Mechanism: "SCRAM-SHA-512", Iterations: 4096},
		{User: "svc-payments", Mechanism: "SCRAM-SHA-256", Iterations: 4096},
	}}
	require.Empty(t, userOps(Compute(desired, live)))
}

// Extra live mechanism + iterations drift on the declared one: only the
// iterations Update fires; the extra mechanism stays invisible.
func TestUserExtraMechanismWithIterationsDrift(t *testing.T) {
	desired := Desired{Users: []user.Desired{desiredUser("svc-payments", "SCRAM-SHA-512", 8192)}}
	live := Live{ScramCredentials: []kafka.ScramCredential{
		{User: "svc-payments", Mechanism: "SCRAM-SHA-512", Iterations: 4096},
		{User: "svc-payments", Mechanism: "SCRAM-SHA-256", Iterations: 4096},
	}}
	res := userOps(Compute(desired, live))
	require.Len(t, res, 1)
	require.Equal(t, operations.UpdateScramCredential, res[0].Action)
	require.Equal(t, "iterations", res[0].Field)
	require.Empty(t, res[0].ScramDeleteMechanism)
}

// The gate rule end-to-end at the diff layer: NO destructive-gated op is
// reachable from a pure CLI diff across every user branch. DeleteScramCredential
// stays taxonomy-only (executor + operator finalizer).
func TestUserDiffNeverEmitsGatedDelete(t *testing.T) {
	desired := Desired{
		Users: []user.Desired{
			desiredUser("a-absent", "SCRAM-SHA-512", 0),
			desiredUser("b-drift", "SCRAM-SHA-512", 8192),
			desiredUser("c-swap", "SCRAM-SHA-512", 0),
			desiredUser("d-insync", "SCRAM-SHA-256", 0),
		},
		RotatePasswords: true,
	}
	live := Live{ScramCredentials: []kafka.ScramCredential{
		{User: "b-drift", Mechanism: "SCRAM-SHA-512", Iterations: 4096},
		{User: "c-swap", Mechanism: "SCRAM-SHA-256", Iterations: 4096},
		{User: "d-insync", Mechanism: "SCRAM-SHA-256", Iterations: 4096},
		{User: "z-undeclared", Mechanism: "SCRAM-SHA-512", Iterations: 4096},
	}}
	res := userOps(Compute(desired, live))
	require.Len(t, res, 4)
	for _, op := range res {
		require.NotEqual(t, operations.DeleteScramCredential, op.Action)
		require.False(t, op.RequiresApproval, "no user op from the CLI diff may be gated: %s", op.Action)
	}
}

// Deterministic ordering: user ops sort by Action then Target (username).
func TestUserOpsDeterministicOrder(t *testing.T) {
	desired := Desired{Users: []user.Desired{
		desiredUser("zeta", "SCRAM-SHA-512", 0),
		desiredUser("alpha", "SCRAM-SHA-512", 0),
	}}
	res1 := userOps(Compute(desired, Live{}))
	// Reversed input order must not change the output order.
	desired.Users[0], desired.Users[1] = desired.Users[1], desired.Users[0]
	res2 := userOps(Compute(desired, Live{}))
	require.Len(t, res1, 2)
	require.Equal(t, "alpha (SCRAM-SHA-512)", res1[0].Target)
	require.Equal(t, "zeta (SCRAM-SHA-512)", res1[1].Target)
	require.Equal(t, res1, res2)
}
