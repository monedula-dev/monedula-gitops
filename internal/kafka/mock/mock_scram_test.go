package mock_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	"github.com/monedula-dev/monedula-gitops/internal/kafka/mock"
)

// TestUpsertThenListReturnsIdentityNeverPassword: basic round-trip. The
// returned kafka.ScramCredential has no password field by construction, but
// this also confirms the mock's List path only ever surfaces the observable
// triple.
func TestUpsertThenListReturnsIdentityNeverPassword(t *testing.T) {
	ctx := context.Background()
	c := mock.New(nil, nil)

	require.NoError(t, c.UpsertScramCredential(ctx, kafka.ScramUpsert{
		User:       "svc-checkout",
		Mechanism:  "SCRAM-SHA-512",
		Iterations: 8192,
		Password:   "hunter2",
	}))

	got, err := c.ListScramCredentials(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, "svc-checkout", got[0].User)
	require.Equal(t, "SCRAM-SHA-512", got[0].Mechanism)
	require.EqualValues(t, 8192, got[0].Iterations)
}

// TestUpsertZeroIterationsDefaults: an upsert with Iterations: 0 stores the
// mock's default rather than a literal 0, mirroring the franz adapter.
func TestUpsertZeroIterationsDefaults(t *testing.T) {
	ctx := context.Background()
	c := mock.New(nil, nil)

	require.NoError(t, c.UpsertScramCredential(ctx, kafka.ScramUpsert{
		User:      "svc-checkout",
		Mechanism: "SCRAM-SHA-512",
		Password:  "hunter2",
	}))

	got, err := c.ListScramCredentials(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.NotZero(t, got[0].Iterations, "Iterations: 0 on upsert must resolve to a real default, not 0")
}

// TestUpsertRotationIncrementsCounterNeverStoresPassword: repeated upserts for
// the same (user, mechanism) bump ScramUpsertCount -- the primitive tests use
// to assert a rotation happened without ever inspecting a secret.
func TestUpsertRotationIncrementsCounterNeverStoresPassword(t *testing.T) {
	ctx := context.Background()
	c := mock.New(nil, nil)

	up := kafka.ScramUpsert{User: "svc-checkout", Mechanism: "SCRAM-SHA-512", Iterations: 4096}

	up.Password = "first-password"
	require.NoError(t, c.UpsertScramCredential(ctx, up))
	require.Equal(t, 1, c.ScramUpsertCount("svc-checkout", "SCRAM-SHA-512"))

	up.Password = "rotated-password"
	require.NoError(t, c.UpsertScramCredential(ctx, up))
	require.Equal(t, 2, c.ScramUpsertCount("svc-checkout", "SCRAM-SHA-512"))

	// Only one credential entry exists (upsert, not append).
	got, err := c.ListScramCredentials(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
}

// TestUpsertOverwritesIterations: a second upsert for the same (user,
// mechanism) with different Iterations updates the stored value.
func TestUpsertOverwritesIterations(t *testing.T) {
	ctx := context.Background()
	c := mock.New(nil, nil)

	require.NoError(t, c.UpsertScramCredential(ctx, kafka.ScramUpsert{
		User: "svc-checkout", Mechanism: "SCRAM-SHA-512", Iterations: 4096, Password: "p1",
	}))
	require.NoError(t, c.UpsertScramCredential(ctx, kafka.ScramUpsert{
		User: "svc-checkout", Mechanism: "SCRAM-SHA-512", Iterations: 8192, Password: "p2",
	}))

	got, err := c.ListScramCredentials(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.EqualValues(t, 8192, got[0].Iterations)
}

// TestDeleteRemovesOnlyThatMechanism: DeleteScramCredential removes exactly
// the (user, mechanism) pair, leaving any other mechanism for the same user
// untouched.
func TestDeleteRemovesOnlyThatMechanism(t *testing.T) {
	ctx := context.Background()
	c := mock.New(nil, nil)

	require.NoError(t, c.UpsertScramCredential(ctx, kafka.ScramUpsert{
		User: "svc-checkout", Mechanism: "SCRAM-SHA-256", Iterations: 4096, Password: "p1",
	}))
	require.NoError(t, c.UpsertScramCredential(ctx, kafka.ScramUpsert{
		User: "svc-checkout", Mechanism: "SCRAM-SHA-512", Iterations: 4096, Password: "p2",
	}))

	require.NoError(t, c.DeleteScramCredential(ctx, "svc-checkout", "SCRAM-SHA-256"))

	got, err := c.ListScramCredentials(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, "SCRAM-SHA-512", got[0].Mechanism)
}

// TestDeleteAbsentCredentialIsNoOp: deleting an unknown (user, mechanism) pair
// returns no error (idempotent, mirroring DeleteTopic/DeleteQuota).
func TestDeleteAbsentCredentialIsNoOp(t *testing.T) {
	ctx := context.Background()
	c := mock.New(nil, nil)
	require.NoError(t, c.DeleteScramCredential(ctx, "nobody", "SCRAM-SHA-512"))
}

// TestListScramCredentialsFiltersByUsername: passing usernames restricts the
// result to those users; a requested-but-absent user is simply missing (not
// an error), matching the franz adapter's per-user semantics.
func TestListScramCredentialsFiltersByUsername(t *testing.T) {
	ctx := context.Background()
	c := mock.New(nil, nil)

	require.NoError(t, c.UpsertScramCredential(ctx, kafka.ScramUpsert{
		User: "alice", Mechanism: "SCRAM-SHA-512", Iterations: 4096, Password: "p1",
	}))
	require.NoError(t, c.UpsertScramCredential(ctx, kafka.ScramUpsert{
		User: "bob", Mechanism: "SCRAM-SHA-512", Iterations: 4096, Password: "p2",
	}))

	got, err := c.ListScramCredentials(ctx, "alice", "nobody")
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, "alice", got[0].User)
}

// TestListScramCredentialsSortedDeterministically: multiple users/mechanisms
// always come back in the same (user, then mechanism) order.
func TestListScramCredentialsSortedDeterministically(t *testing.T) {
	ctx := context.Background()
	c := mock.New(nil, nil)

	require.NoError(t, c.UpsertScramCredential(ctx, kafka.ScramUpsert{
		User: "z-svc", Mechanism: "SCRAM-SHA-512", Iterations: 4096, Password: "p1",
	}))
	require.NoError(t, c.UpsertScramCredential(ctx, kafka.ScramUpsert{
		User: "a-svc", Mechanism: "SCRAM-SHA-512", Iterations: 4096, Password: "p2",
	}))
	require.NoError(t, c.UpsertScramCredential(ctx, kafka.ScramUpsert{
		User: "a-svc", Mechanism: "SCRAM-SHA-256", Iterations: 4096, Password: "p3",
	}))

	got, err := c.ListScramCredentials(ctx)
	require.NoError(t, err)
	require.Len(t, got, 3)
	require.Equal(t, "a-svc", got[0].User)
	require.Equal(t, "SCRAM-SHA-256", got[0].Mechanism)
	require.Equal(t, "a-svc", got[1].User)
	require.Equal(t, "SCRAM-SHA-512", got[1].Mechanism)
	require.Equal(t, "z-svc", got[2].User)
}

// TestStateFileSeedingScram: SCRAM credentials declared in the YAML state
// file are loaded and returned by ListScramCredentials, with a zero
// UpsertCount (seeding is not a rotation).
func TestStateFileSeedingScram(t *testing.T) {
	ctx := context.Background()
	c, err := mock.FromFile("testdata/state.yaml")
	require.NoError(t, err)

	got, err := c.ListScramCredentials(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1, "state.yaml seeds exactly one SCRAM credential")
	require.Equal(t, "svc-checkout", got[0].User)
	require.Equal(t, "SCRAM-SHA-512", got[0].Mechanism)
	require.EqualValues(t, 4096, got[0].Iterations)

	// Seeding is not an upsert: the rotation counter must start at zero.
	require.Equal(t, 0, c.ScramUpsertCount("svc-checkout", "SCRAM-SHA-512"))
}

// TestScramCallsRecorded: Upsert/Delete are recorded in Calls().
func TestScramCallsRecorded(t *testing.T) {
	ctx := context.Background()
	c := mock.New(nil, nil)

	require.NoError(t, c.UpsertScramCredential(ctx, kafka.ScramUpsert{
		User: "svc-checkout", Mechanism: "SCRAM-SHA-512", Iterations: 4096, Password: "p1",
	}))
	require.NoError(t, c.DeleteScramCredential(ctx, "svc-checkout", "SCRAM-SHA-512"))

	got := c.Calls()
	require.Len(t, got, 2)
	require.Equal(t, "UpsertScramCredential svc-checkout\x00SCRAM-SHA-512", got[0])
	require.Equal(t, "DeleteScramCredential svc-checkout\x00SCRAM-SHA-512", got[1])
}

// TestScramFailOn: FailOn injects failures and prevents state mutation
// (including the upsert counter, mirroring the quota FailOn contract).
func TestScramFailOn(t *testing.T) {
	boom := errors.New("boom")
	c := mock.New(nil, nil)
	key := "svc-checkout\x00SCRAM-SHA-512"

	c.FailOn("UpsertScramCredential", key, boom)
	err := c.UpsertScramCredential(context.Background(), kafka.ScramUpsert{
		User: "svc-checkout", Mechanism: "SCRAM-SHA-512", Iterations: 4096, Password: "p1",
	})
	require.ErrorIs(t, err, boom)

	// Must NOT mutate state.
	got, _ := c.ListScramCredentials(context.Background())
	require.Empty(t, got)
	require.Equal(t, 0, c.ScramUpsertCount("svc-checkout", "SCRAM-SHA-512"))

	// The failed call is still recorded.
	calls := c.Calls()
	require.Len(t, calls, 1)
	require.Equal(t, "UpsertScramCredential "+key, calls[0])

	// DeleteScramCredential FailOn also works.
	c2 := mock.New(nil, nil)
	require.NoError(t, c2.UpsertScramCredential(context.Background(), kafka.ScramUpsert{
		User: "svc-checkout", Mechanism: "SCRAM-SHA-512", Iterations: 4096, Password: "p1",
	}))

	c2.FailOn("DeleteScramCredential", key, boom)
	err = c2.DeleteScramCredential(context.Background(), "svc-checkout", "SCRAM-SHA-512")
	require.ErrorIs(t, err, boom)

	got, _ = c2.ListScramCredentials(context.Background())
	require.Len(t, got, 1, "failed delete must not mutate state")
}

// TestNewWithScramCredentials: the programmatic constructor seeds identity
// state readable via ListScramCredentials.
func TestNewWithScramCredentials(t *testing.T) {
	ctx := context.Background()
	c := mock.NewWithScramCredentials(nil, nil, []kafka.ScramCredential{
		{User: "svc-checkout", Mechanism: "SCRAM-SHA-512", Iterations: 4096},
	})

	got, err := c.ListScramCredentials(ctx)
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, "svc-checkout", got[0].User)
}
