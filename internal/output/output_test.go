package output

import (
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/monedula-dev/monedula-gitops/internal/executor"
	"github.com/monedula-dev/monedula-gitops/internal/operations"
)

var update = flag.Bool("update", false, "update golden files")

func sampleOps() []operations.Operation {
	o1 := operations.New(operations.UpdateTopicConfig)
	o1.Kind, o1.Namespace, o1.Name, o1.Target = "KafkaTopic", "payments", "orders", "payments.orders"
	o1.Field, o1.From, o1.To = "config.retention.ms", "86400000", "604800000"

	o2 := operations.New(operations.CreateAcl)
	o2.Kind, o2.Name, o2.Target = "KafkaTopic", "orders", "User:svc-checkout Write topic:payments.orders Allow"

	// Non-Enforce op: serialized output carries mode, human output gains the
	// report-only marker. Enforce/empty modes stay invisible (the default).
	o3 := operations.New(operations.UpdateTopicConfig)
	o3.Kind, o3.Namespace, o3.Name, o3.Target = "KafkaTopic", "payments", "refunds", "payments.refunds"
	o3.Field, o3.From, o3.To = "config.retention.ms", "86400000", "604800000"
	o3.Mode = operations.ModeDetectOnly

	return []operations.Operation{o1, o2, o3}
}

func assertGolden(t *testing.T, name string, got []byte) {
	t.Helper()
	p := filepath.Join("testdata", name)
	if *update {
		require.NoError(t, os.WriteFile(p, got, 0o644))
	}
	want, err := os.ReadFile(p)
	require.NoError(t, err)
	require.Equal(t, string(want), string(got))
}

func TestRenderJSONDeterministic(t *testing.T) {
	got, err := Render(sampleOps(), "json", "prod-eu", KindApplyDryRun)
	require.NoError(t, err)
	assertGolden(t, "apply_dryrun.json", got)
	// determinism: render again, must be identical
	got2, err := Render(sampleOps(), "json", "prod-eu", KindApplyDryRun)
	require.NoError(t, err)
	require.Equal(t, string(got), string(got2))
}

func TestRenderYAMLDeterministic(t *testing.T) {
	got, err := Render(sampleOps(), "yaml", "prod-eu", KindApplyDryRun)
	require.NoError(t, err)
	assertGolden(t, "apply_dryrun.yaml", got)
}

func TestRenderHumanDeterministic(t *testing.T) {
	got, err := Render(sampleOps(), "human", "prod-eu", KindApplyDryRun)
	require.NoError(t, err)
	assertGolden(t, "apply_dryrun.human", got)
}

func TestRenderHumanMarksPruneCandidate(t *testing.T) {
	// A DeleteAcl without prune consent renders with an actionable marker
	// (spec §10.3); a consented one (operator path, scope opted in) does not.
	op := operations.New(operations.DeleteAcl)
	op.Kind, op.Name, op.Target = "KafkaTopic", "orders", "User:x Read topic:t Allow"

	got, err := Render([]operations.Operation{op}, "human", "dev", KindApplyDryRun)
	require.NoError(t, err)
	require.Contains(t, string(got), "(prune candidate; enable with --prune)")

	op.PruneAllowed = true
	got, err = Render([]operations.Operation{op}, "human", "dev", KindApplyDryRun)
	require.NoError(t, err)
	require.NotContains(t, string(got), "prune candidate")
}

func TestRenderStampsRequestedKind(t *testing.T) {
	// The document kind names the producing command (diff/verify get their own
	// kinds; apply --dry-run keeps the spec §17.5 ApplyDryRunOutput).
	for _, kind := range []string{KindDiff, KindVerify, KindApplyDryRun} {
		got, err := Render(sampleOps(), "yaml", "prod-eu", kind)
		require.NoError(t, err)
		require.Contains(t, string(got), "kind: "+kind)
	}
}

func TestRenderEmptyOps(t *testing.T) {
	for _, format := range []string{"human", "yaml", "json"} {
		got, err := Render(nil, format, "prod-eu", KindApplyDryRun)
		require.NoError(t, err)
		require.NotEmpty(t, got) // still prints a header / "no changes"
	}
}

func TestRenderUnknownFormatErrors(t *testing.T) {
	_, err := Render(sampleOps(), "xml", "prod-eu", KindApplyDryRun)
	require.Error(t, err)
}

func sampleResult() executor.Result {
	o1 := operations.New(operations.UpdateTopicConfig)
	o1.Kind, o1.Namespace, o1.Name, o1.Target = "KafkaTopic", "payments", "orders", "payments.orders"
	o1.Field, o1.From, o1.To = "config.retention.ms", "86400000", "604800000"

	o2 := operations.New(operations.IncreasePartitions)
	o2.Kind, o2.Name, o2.Target = "KafkaTopic", "orders", "payments.orders"
	o2.Field, o2.From, o2.To = "partitions", "3", "6"

	o3 := operations.New(operations.UpdateTopicConfig)
	o3.Kind, o3.Name, o3.Target = "KafkaTopic", "refunds", "payments.refunds"
	o3.Field, o3.From, o3.To = "config.retention.ms", "86400000", "604800000"
	o3.Mode = operations.ModeDetectOnly

	return executor.Result{Results: []executor.OpResult{
		{Op: o1, Status: executor.Succeeded},
		{Op: o2, Status: executor.Blocked},
		{Op: o3, Status: executor.ReportOnly},
	}}
}

func TestRenderApplyResultJSONDeterministic(t *testing.T) {
	got, err := RenderApplyResult(sampleResult(), "json", "prod-eu")
	require.NoError(t, err)
	assertGolden(t, "apply_result.json", got)
	got2, err := RenderApplyResult(sampleResult(), "json", "prod-eu")
	require.NoError(t, err)
	require.Equal(t, string(got), string(got2))
}

func TestRenderApplyResultYAMLDeterministic(t *testing.T) {
	got, err := RenderApplyResult(sampleResult(), "yaml", "prod-eu")
	require.NoError(t, err)
	assertGolden(t, "apply_result.yaml", got)
	got2, err := RenderApplyResult(sampleResult(), "yaml", "prod-eu")
	require.NoError(t, err)
	require.Equal(t, string(got), string(got2))
}

func TestRenderApplyResultHumanDeterministic(t *testing.T) {
	got, err := RenderApplyResult(sampleResult(), "human", "prod-eu")
	require.NoError(t, err)
	assertGolden(t, "apply_result.human", got)
	got2, err := RenderApplyResult(sampleResult(), "human", "prod-eu")
	require.NoError(t, err)
	require.Equal(t, string(got), string(got2))
}

func TestRenderApplyResultUnknownFormatErrors(t *testing.T) {
	_, err := RenderApplyResult(sampleResult(), "xml", "prod-eu")
	require.Error(t, err)
}

// TestRenderApplyResultFailedOpErrRendersVerbatim pins the executor/output
// redaction contract (spec §30.2, internal/executor's OpResult.Err doc): the
// output package applies NO further redaction to OpResult.Err — it renders
// exactly the string the executor produced, in every format. The executor
// side of the contract (that Err never contains a secret value — only field
// names/sources, proven for the SCRAM password path by
// internal/executor.TestApplyScramPasswordNeverInResults, which exercises a
// genuinely resolved password against a genuinely FAILING op) is what makes
// this verbatim pass-through safe. This test uses a field-name-only fixture
// error (never a real secret) to confirm the render path itself does not
// mangle, truncate, or (just as importantly) silently swallow Err.
func TestRenderApplyResultFailedOpErrRendersVerbatim(t *testing.T) {
	op := operations.New(operations.CreateScramCredential)
	op.Kind, op.Namespace, op.Name, op.Target = "KafkaUser", "payments", "svc-payments", "svc-payments (SCRAM-SHA-512)"
	// No quotes in the fixture error (survives JSON/YAML escaping unmodified)
	// and short enough not to be YAML line-wrapped, so a plain substring
	// Contains is a valid verbatim check in every format.
	const fieldNameOnlyErr = `env var MONEDULA_TEST_PW not set`
	res := executor.Result{Results: []executor.OpResult{
		{Op: op, Status: executor.Failed, Err: fieldNameOnlyErr},
	}}

	human, err := RenderApplyResult(res, "human", "prod-eu")
	require.NoError(t, err)
	require.Contains(t, string(human), fieldNameOnlyErr, "human output must carry Err verbatim")

	jsonOut, err := RenderApplyResult(res, "json", "prod-eu")
	require.NoError(t, err)
	require.Contains(t, string(jsonOut), fieldNameOnlyErr, "JSON output must carry Err verbatim")

	yamlOut, err := RenderApplyResult(res, "yaml", "prod-eu")
	require.NoError(t, err)
	require.Contains(t, string(yamlOut), fieldNameOnlyErr, "YAML output must carry Err verbatim")
}

// TestRenderRemoveRoleBindingPruneCandidate mirrors TestRenderHumanMarksPruneCandidate
// for RemoveRoleBinding: without prune consent the human output carries the
// "(prune candidate; enable with --prune)" marker; with PruneAllowed it does not.
func TestRenderRemoveRoleBindingPruneCandidate(t *testing.T) {
	op := operations.New(operations.RemoveRoleBinding)
	op.Kind = "KafkaRoleBinding"
	op.Name = "checkout-writer"
	op.Target = "User:svc-checkout DeveloperWrite kafka Topic:orders(literal)"

	got, err := Render([]operations.Operation{op}, "human", "dev", KindApplyDryRun)
	require.NoError(t, err)
	require.Contains(t, string(got), "(prune candidate; enable with --prune)")

	op.PruneAllowed = true
	got, err = Render([]operations.Operation{op}, "human", "dev", KindApplyDryRun)
	require.NoError(t, err)
	require.NotContains(t, string(got), "prune candidate")
}

// TestRenderAddRoleBindingOp confirms that an AddRoleBinding operation renders
// a human-readable line and is preserved in YAML/JSON with the target string.
// The generic output path handles role-binding ops identically to quota/ACL ops —
// this is a smoke-test that the wiring is correct end-to-end (spec §40).
func TestRenderAddRoleBindingOp(t *testing.T) {
	op := operations.New(operations.AddRoleBinding)
	op.Kind = "KafkaRoleBinding"
	op.Name = "checkout-writer"
	op.Target = "User:svc-checkout DeveloperWrite kafka Topic:orders(literal)"

	humanOut, err := Render([]operations.Operation{op}, "human", "prod-eu", KindApplyDryRun)
	require.NoError(t, err)
	require.Contains(t, string(humanOut), "AddRoleBinding")
	require.Contains(t, string(humanOut), "KafkaRoleBinding")
	require.Contains(t, string(humanOut), "User:svc-checkout DeveloperWrite kafka Topic:orders(literal)")

	jsonOut, err := Render([]operations.Operation{op}, "json", "prod-eu", KindApplyDryRun)
	require.NoError(t, err)
	require.Contains(t, string(jsonOut), `"action": "AddRoleBinding"`)
	require.Contains(t, string(jsonOut), `"kind": "KafkaRoleBinding"`)
	require.Contains(t, string(jsonOut), "User:svc-checkout DeveloperWrite kafka Topic:orders(literal)")
	// Payload field (op.RoleBinding pointer) must not appear in output — it is json:"-".
	require.NotContains(t, string(jsonOut), `"roleBinding"`)
	require.NotContains(t, string(jsonOut), `"RoleBinding":`)
	require.NotContains(t, string(jsonOut), `"principal":`)

	yamlOut, err := Render([]operations.Operation{op}, "yaml", "prod-eu", KindApplyDryRun)
	require.NoError(t, err)
	require.Contains(t, string(yamlOut), "action: AddRoleBinding")
	require.Contains(t, string(yamlOut), "kind: KafkaRoleBinding")
}

// TestRenderSetQuotaOp confirms that a SetQuota operation renders a readable
// human line (Action Kind/ Target) and is preserved in YAML/JSON without the
// json:"-" payload fields (QuotaEntity/QuotaLimits). The generic output path
// handles quota ops identically to topic/ACL ops — this is a smoke-test that
// the wiring is correct end-to-end (spec §39, diff §39.6).
func TestRenderSetQuotaOp(t *testing.T) {
	op := operations.New(operations.SetQuota)
	op.Kind = "KafkaQuota"
	op.Target = "client-id=batch,user=svc-checkout"

	humanOut, err := Render([]operations.Operation{op}, "human", "prod-eu", KindApplyDryRun)
	require.NoError(t, err)
	require.Contains(t, string(humanOut), "SetQuota")
	require.Contains(t, string(humanOut), "KafkaQuota")
	require.Contains(t, string(humanOut), "client-id=batch,user=svc-checkout")

	jsonOut, err := Render([]operations.Operation{op}, "json", "prod-eu", KindApplyDryRun)
	require.NoError(t, err)
	require.Contains(t, string(jsonOut), `"action": "SetQuota"`)
	require.Contains(t, string(jsonOut), `"kind": "KafkaQuota"`)
	require.Contains(t, string(jsonOut), `"target": "client-id=batch,user=svc-checkout"`)
	// Payload fields (QuotaEntity/QuotaLimits) must not appear in output.
	require.NotContains(t, string(jsonOut), "QuotaEntity")
	require.NotContains(t, string(jsonOut), "QuotaLimits")

	yamlOut, err := Render([]operations.Operation{op}, "yaml", "prod-eu", KindApplyDryRun)
	require.NoError(t, err)
	require.Contains(t, string(yamlOut), "action: SetQuota")
	require.Contains(t, string(yamlOut), "kind: KafkaQuota")
	require.Contains(t, string(yamlOut), "target: client-id=batch,user=svc-checkout")
}
