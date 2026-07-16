package executor

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	"github.com/monedula-dev/monedula-gitops/internal/kafka/mock"
	"github.com/monedula-dev/monedula-gitops/internal/operations"
	"github.com/stretchr/testify/require"
)

const testPassword = "s3cr3t-hunter2" // never allowed to surface in results

// staticResolver resolves every reference to the same fixed password.
type staticResolver struct{ password string }

func (r staticResolver) Resolve(v1alpha1.ValueFrom) (string, error) { return r.password, nil }

// failingResolver mimics the secrets contract on failure: the error names the
// SOURCE (env var), never a value.
type failingResolver struct{}

func (failingResolver) Resolve(vf v1alpha1.ValueFrom) (string, error) {
	return "", fmt.Errorf("env var %q not set", vf.ValueFrom.Env)
}

func scramOp(a operations.Action, username, mechanism string, iterations int32) operations.Operation {
	op := operations.New(a)
	op.Kind = "KafkaUser"
	op.Target = username + " (" + mechanism + ")"
	op.ScramUser = username
	op.ScramMechanism = mechanism
	op.ScramIterations = iterations
	op.PasswordRef = &v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{Env: "PW_" + username}}
	return op
}

// TestApplyCreateScramCredentialUpserts verifies the execute-time password
// resolution seam: the op carries only a reference; Clients.Passwords resolves
// it and the upsert lands with the declared identity.
func TestApplyCreateScramCredentialUpserts(t *testing.T) {
	client := mock.New(nil, nil)
	op := scramOp(operations.CreateScramCredential, "svc-payments", "SCRAM-SHA-512", 8192)

	res := Apply(context.Background(), Clients{Kafka: client, Passwords: staticResolver{testPassword}},
		[]operations.Operation{op}, Approvals{})

	require.Len(t, res.Results, 1)
	require.Equal(t, Succeeded, res.Results[0].Status)
	require.Equal(t, 1, client.ScramUpsertCount("svc-payments", "SCRAM-SHA-512"))

	creds, err := client.ListScramCredentials(context.Background())
	require.NoError(t, err)
	require.Equal(t, []kafka.ScramCredential{{User: "svc-payments", Mechanism: "SCRAM-SHA-512", Iterations: 8192}}, creds)
}

// TestApplyUpdateScramMechanismChange verifies the one-op mechanism-change
// mechanics: upsert the declared mechanism FIRST, then delete the old one.
func TestApplyUpdateScramMechanismChange(t *testing.T) {
	client := mock.NewWithScramCredentials(nil, nil, []kafka.ScramCredential{
		{User: "svc-payments", Mechanism: "SCRAM-SHA-256", Iterations: 4096},
	})
	op := scramOp(operations.UpdateScramCredential, "svc-payments", "SCRAM-SHA-512", 0)
	op.ScramDeleteMechanism = "SCRAM-SHA-256"

	res := Apply(context.Background(), Clients{Kafka: client, Passwords: staticResolver{testPassword}},
		[]operations.Operation{op}, Approvals{})

	require.Len(t, res.Results, 1)
	require.Equal(t, Succeeded, res.Results[0].Status, "ungated Medium op needs no approval: %s", res.Results[0].Err)
	// Old mechanism gone, new one present.
	creds, err := client.ListScramCredentials(context.Background())
	require.NoError(t, err)
	require.Len(t, creds, 1)
	require.Equal(t, "SCRAM-SHA-512", creds[0].Mechanism)
	// Ordering: the upsert is recorded before the delete.
	calls := client.Calls()
	require.Equal(t, []string{
		"UpsertScramCredential svc-payments\x00SCRAM-SHA-512",
		"DeleteScramCredential svc-payments\x00SCRAM-SHA-256",
	}, calls)
}

// TestApplyMechanismChangeUpsertOKDeleteFails: when the upsert of the new
// mechanism succeeds but the delete of the old one fails, the op must record
// Failed with an honest error — the principal now has BOTH mechanisms live,
// and the error must say so rather than implying a plain re-apply will clean
// up the old credential (it won't: see computeUserOps, the old mechanism
// becomes an invisible EXTRA once the declared one is present+in-sync).
func TestApplyMechanismChangeUpsertOKDeleteFails(t *testing.T) {
	client := mock.NewWithScramCredentials(nil, nil, []kafka.ScramCredential{
		{User: "svc-payments", Mechanism: "SCRAM-SHA-256", Iterations: 4096},
	})
	client.FailOn("DeleteScramCredential", "svc-payments\x00SCRAM-SHA-256", errors.New("broker unavailable"))
	op := scramOp(operations.UpdateScramCredential, "svc-payments", "SCRAM-SHA-512", 0)
	op.ScramDeleteMechanism = "SCRAM-SHA-256"

	res := Apply(context.Background(), Clients{Kafka: client, Passwords: staticResolver{testPassword}},
		[]operations.Operation{op}, Approvals{})

	require.Equal(t, Failed, res.Results[0].Status)
	require.Contains(t, res.Results[0].Err, "BOTH", "error must warn both mechanisms are now live")
	require.NotContains(t, res.Results[0].Err, testPassword)

	// Both credentials present: the new one is live, the old one still there.
	creds, err := client.ListScramCredentials(context.Background())
	require.NoError(t, err)
	require.Len(t, creds, 2)
	byMech := map[string]kafka.ScramCredential{}
	for _, c := range creds {
		byMech[c.Mechanism] = c
	}
	require.Contains(t, byMech, "SCRAM-SHA-512", "new credential must be live despite the failed delete")
	require.Contains(t, byMech, "SCRAM-SHA-256", "old credential must survive a failed delete")
}

// TestApplyMechanismChangeFailedUpsertKeepsOld: when the upsert fails, the old
// mechanism must NOT be deleted (the principal stays authenticable).
func TestApplyMechanismChangeFailedUpsertKeepsOld(t *testing.T) {
	client := mock.NewWithScramCredentials(nil, nil, []kafka.ScramCredential{
		{User: "svc-payments", Mechanism: "SCRAM-SHA-256", Iterations: 4096},
	})
	client.FailOn("UpsertScramCredential", "svc-payments\x00SCRAM-SHA-512", errors.New("broker unavailable"))
	op := scramOp(operations.UpdateScramCredential, "svc-payments", "SCRAM-SHA-512", 0)
	op.ScramDeleteMechanism = "SCRAM-SHA-256"

	res := Apply(context.Background(), Clients{Kafka: client, Passwords: staticResolver{testPassword}},
		[]operations.Operation{op}, Approvals{})

	require.Equal(t, Failed, res.Results[0].Status)
	creds, err := client.ListScramCredentials(context.Background())
	require.NoError(t, err)
	require.Len(t, creds, 1)
	require.Equal(t, "SCRAM-SHA-256", creds[0].Mechanism, "old credential must survive a failed upsert")
	require.NotContains(t, client.Calls(), "DeleteScramCredential svc-payments\x00SCRAM-SHA-256")
}

// TestApplyRotateScramCredentialReUpserts: rotation is just an upsert of the
// same identity; the mock's UpsertCount is the rotation-assertion primitive.
func TestApplyRotateScramCredentialReUpserts(t *testing.T) {
	client := mock.NewWithScramCredentials(nil, nil, []kafka.ScramCredential{
		{User: "svc-payments", Mechanism: "SCRAM-SHA-512", Iterations: 8192},
	})
	op := scramOp(operations.RotateScramCredential, "svc-payments", "SCRAM-SHA-512", 8192)

	res := Apply(context.Background(), Clients{Kafka: client, Passwords: staticResolver{testPassword}},
		[]operations.Operation{op}, Approvals{})

	require.Equal(t, Succeeded, res.Results[0].Status)
	require.Equal(t, 1, client.ScramUpsertCount("svc-payments", "SCRAM-SHA-512"))
	// Identity unchanged by rotation.
	creds, err := client.ListScramCredentials(context.Background())
	require.NoError(t, err)
	require.Equal(t, []kafka.ScramCredential{{User: "svc-payments", Mechanism: "SCRAM-SHA-512", Iterations: 8192}}, creds)
}

// TestApplyScramResolverErrorFieldNameOnly: a failed resolution records Failed
// with an error naming the SOURCE only — never any password-like value.
func TestApplyScramResolverErrorFieldNameOnly(t *testing.T) {
	client := mock.New(nil, nil)
	op := scramOp(operations.CreateScramCredential, "svc-payments", "SCRAM-SHA-512", 0)

	res := Apply(context.Background(), Clients{Kafka: client, Passwords: failingResolver{}},
		[]operations.Operation{op}, Approvals{})

	require.Equal(t, Failed, res.Results[0].Status)
	require.Contains(t, res.Results[0].Err, "PW_svc-payments", "error must name the source")
	require.Contains(t, res.Results[0].Err, "svc-payments")
	require.Equal(t, 0, client.ScramUpsertCount("svc-payments", "SCRAM-SHA-512"), "no upsert without a password")
}

// TestApplyScramPasswordNeverInResults: across success AND failure paths, no
// OpResult may carry the resolved password anywhere.
func TestApplyScramPasswordNeverInResults(t *testing.T) {
	client := mock.New(nil, nil)
	client.FailOn("UpsertScramCredential", "fail-user\x00SCRAM-SHA-512", errors.New("simulated broker error"))
	ops := []operations.Operation{
		scramOp(operations.CreateScramCredential, "ok-user", "SCRAM-SHA-512", 0),
		scramOp(operations.CreateScramCredential, "fail-user", "SCRAM-SHA-512", 0),
	}

	res := Apply(context.Background(), Clients{Kafka: client, Passwords: staticResolver{testPassword}},
		ops, Approvals{})

	for _, r := range res.Results {
		require.NotContains(t, r.Err, testPassword)
		require.NotContains(t, r.Op.Target, testPassword)
		require.NotContains(t, r.Op.Message, testPassword)
	}
}

// TestApplyScramNilResolverFails: a user op with no resolver configured is a
// clean Failed (field-name-only error), mirroring the nil Schema/MDS handling.
func TestApplyScramNilResolverFails(t *testing.T) {
	client := mock.New(nil, nil)
	op := scramOp(operations.CreateScramCredential, "svc-payments", "SCRAM-SHA-512", 0)

	res := Apply(context.Background(), Clients{Kafka: client}, []operations.Operation{op}, Approvals{})

	require.Equal(t, Failed, res.Results[0].Status)
	require.Contains(t, res.Results[0].Err, "no password resolver configured")
}

// TestApplyScramNilPasswordRefFails: an op without a password reference must
// fail rather than upsert a blank password.
func TestApplyScramNilPasswordRefFails(t *testing.T) {
	client := mock.New(nil, nil)
	op := scramOp(operations.CreateScramCredential, "svc-payments", "SCRAM-SHA-512", 0)
	op.PasswordRef = nil

	res := Apply(context.Background(), Clients{Kafka: client, Passwords: staticResolver{testPassword}},
		[]operations.Operation{op}, Approvals{})

	require.Equal(t, Failed, res.Results[0].Status)
	require.Contains(t, res.Results[0].Err, "has no password reference")
	require.Equal(t, 0, client.ScramUpsertCount("svc-payments", "SCRAM-SHA-512"))
}

// TestApplyDeleteScramCredentialGated: the standalone delete (operator
// finalizer territory) honors the destructive gate exactly like RemoveQuota —
// Blocked without --allow-destructive, executed with it.
func TestApplyDeleteScramCredentialGated(t *testing.T) {
	client := mock.NewWithScramCredentials(nil, nil, []kafka.ScramCredential{
		{User: "svc-payments", Mechanism: "SCRAM-SHA-512", Iterations: 8192},
	})
	op := operations.New(operations.DeleteScramCredential)
	op.Kind = "KafkaUser"
	op.Target = "svc-payments (SCRAM-SHA-512)"
	op.ScramUser = "svc-payments"
	op.ScramMechanism = "SCRAM-SHA-512"

	blocked := Apply(context.Background(), Clients{Kafka: client}, []operations.Operation{op}, Approvals{})
	require.Equal(t, Blocked, blocked.Results[0].Status)
	require.Empty(t, client.Calls(), "a blocked delete must not touch the client")

	approved := Apply(context.Background(), Clients{Kafka: client}, []operations.Operation{op}, Approvals{Destructive: true})
	require.Equal(t, Succeeded, approved.Results[0].Status)
	creds, err := client.ListScramCredentials(context.Background())
	require.NoError(t, err)
	require.Empty(t, creds)
}
