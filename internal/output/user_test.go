package output

import (
	"encoding/json"
	"testing"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/executor"
	"github.com/monedula-dev/monedula-gitops/internal/operations"
	"github.com/stretchr/testify/require"
)

func rotateOp() operations.Operation {
	op := operations.New(operations.RotateScramCredential)
	op.Kind = "KafkaUser"
	op.Target = "svc-payments (SCRAM-SHA-512)"
	op.ScramUser = "svc-payments"
	op.ScramMechanism = "SCRAM-SHA-512"
	op.PasswordRef = &v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{Env: "SVC_PAYMENTS_PASSWORD"}}
	return op
}

// TestRenderHumanRotateAnnotation: the human plan render marks a rotate op
// with an explicit "(--rotate-passwords)" so a reader knows it is
// flag-triggered, not drift.
func TestRenderHumanRotateAnnotation(t *testing.T) {
	out, err := Render([]operations.Operation{rotateOp()}, "human", "dev", KindApplyDryRun)
	require.NoError(t, err)
	require.Contains(t, string(out), "RotateScramCredential KafkaUser/ svc-payments (SCRAM-SHA-512)")
	require.Contains(t, string(out), "(--rotate-passwords)")
}

// TestRenderHumanUserUpdateGeneric: the generic op line handles the SCRAM
// update shape (field/from/to) with no special casing.
func TestRenderHumanUserUpdateGeneric(t *testing.T) {
	op := operations.New(operations.UpdateScramCredential)
	op.Kind = "KafkaUser"
	op.Target = "svc-payments (SCRAM-SHA-512)"
	op.Field = "mechanism"
	op.From = "SCRAM-SHA-256"
	op.To = "SCRAM-SHA-512"
	out, err := Render([]operations.Operation{op}, "human", "dev", KindDiff)
	require.NoError(t, err)
	require.Contains(t, string(out), "UpdateScramCredential KafkaUser/ svc-payments (SCRAM-SHA-512) [field=mechanism SCRAM-SHA-256 -> SCRAM-SHA-512] (risk=Medium approval=false)")
	require.NotContains(t, string(out), "(--rotate-passwords)", "only rotate ops carry the annotation")
}

// TestRenderJSONUserOpOmitsPayload: the json document renders identity fields
// only; the executable payload (including the password reference) never
// serializes — mirroring how quota payloads stay internal.
func TestRenderJSONUserOpOmitsPayload(t *testing.T) {
	out, err := Render([]operations.Operation{rotateOp()}, "json", "dev", KindApplyDryRun)
	require.NoError(t, err)
	var doc map[string]interface{}
	require.NoError(t, json.Unmarshal(out, &doc))
	require.Equal(t, "ApplyDryRunOutput", doc["kind"])
	changes := doc["changes"].([]interface{})
	require.Len(t, changes, 1)
	ch := changes[0].(map[string]interface{})
	require.Equal(t, "RotateScramCredential", ch["action"])
	require.Equal(t, "KafkaUser", ch["kind"])
	require.Equal(t, "svc-payments (SCRAM-SHA-512)", ch["target"])
	require.NotContains(t, string(out), "SVC_PAYMENTS_PASSWORD", "the password ref must not serialize")
	require.NotContains(t, string(out), "PasswordRef")
}

// TestRenderApplyResultRotateAnnotation: the human apply-result line carries
// the same flag annotation.
func TestRenderApplyResultRotateAnnotation(t *testing.T) {
	res := executor.Result{Results: []executor.OpResult{{Op: rotateOp(), Status: executor.Succeeded}}}
	out, err := RenderApplyResult(res, "human", "dev")
	require.NoError(t, err)
	require.Contains(t, string(out), "Succeeded RotateScramCredential KafkaUser/ svc-payments (SCRAM-SHA-512)")
	require.Contains(t, string(out), "(--rotate-passwords)")
}
