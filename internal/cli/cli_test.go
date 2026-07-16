package cli

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/monedula-dev/monedula-gitops/internal/executor"
)

func run(t *testing.T, args ...string) (string, error) {
	t.Helper()
	cmd := NewRootCmd()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), err
}

func TestValidateValidSucceeds(t *testing.T) {
	_, err := run(t, "validate", "-f", "testdata/valid")
	require.NoError(t, err)
}

func requireExitCode(t *testing.T, err error, code int) {
	t.Helper()
	var ee *ExitError
	require.ErrorAs(t, err, &ee)
	require.Equal(t, code, ee.Code)
}

func TestValidateInvalidFails(t *testing.T) {
	_, err := run(t, "validate", "-f", "testdata/invalid")
	requireExitCode(t, err, 2)
}

// TestClusterConfigFileShorthand pins the -c shorthand for
// --cluster-config-file across the commands that register it (the shared flag
// set used by diff/verify/apply/validate, plus doctor and import which
// register it separately since they don't take -f manifests).
func TestClusterConfigFileShorthand(t *testing.T) {
	_, err := run(t, "diff", "-f", "testdata/valid", "-c", "testdata/clusters/dev.yaml")
	require.NoError(t, err)

	out, err := run(t, "doctor", "-c", "testdata/clusters/dev.yaml")
	require.NoError(t, err)
	require.Contains(t, out, "healthy")

	_, _, err = runSplit(t, "import", "cluster", "-c", "testdata/clusters/importable.yaml")
	require.NoError(t, err)
}

func TestFilenameWithCommaNotSplit(t *testing.T) {
	// -f must be repeatable WITHOUT comma-splitting (StringArray, not
	// StringSlice): a path containing a comma has to reach the loader verbatim.
	dir := filepath.Join(t.TempDir(), "team,a")
	require.NoError(t, os.MkdirAll(dir, 0o755))
	manifest := `apiVersion: gitops.monedula.dev/v1alpha1
kind: KafkaTopic
metadata: { name: t1 }
spec: { clusterRef: { name: c }, partitions: 1 }`
	require.NoError(t, os.WriteFile(filepath.Join(dir, "topic.yaml"), []byte(manifest), 0o644))

	out, err := run(t, "validate", "-f", dir)
	require.NoError(t, err, "comma in -f path must not be split: %s", out)
	require.Contains(t, out, "1 resource(s) valid")
}

func TestClusterConfigFileWithCommaNotSplit(t *testing.T) {
	// Same contract for --cluster-config-file.
	dir := filepath.Join(t.TempDir(), "env,dev")
	require.NoError(t, os.MkdirAll(dir, 0o755))
	src, err := os.ReadFile("testdata/clusters/dev.yaml")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, "dev.yaml"), src, 0o644))

	_, err = run(t, "validate", "-f", "testdata/valid", "--cluster-config-file", dir)
	require.NoError(t, err)
}

func TestApplyDryRunHuman(t *testing.T) {
	out, err := run(t, "apply", "-f", "testdata/valid",
		"--cluster-config-file", "testdata/clusters/dev.yaml", "--dry-run")
	require.NoError(t, err)
	require.Contains(t, out, "UpdateTopicConfig")
}

func TestApplyExecutesAndReports(t *testing.T) {
	// dev mock state differs in config -> one UpdateTopicConfig (ungated, Low).
	// File-seeded mocks do not persist across runs, so we only assert the apply
	// result here (not a follow-up clean verify).
	out, err := run(t, "apply", "-f", "testdata/valid",
		"--cluster-config-file", "testdata/clusters/dev.yaml")
	require.NoError(t, err)
	require.Contains(t, out, "Succeeded")
}

func TestApplyDryRunStillRendersPlan(t *testing.T) {
	// --dry-run renders the plan and does NOT execute, so the output contains the
	// planned action but no apply-result status.
	out, err := run(t, "apply", "-f", "testdata/valid",
		"--cluster-config-file", "testdata/clusters/dev.yaml", "--dry-run")
	require.NoError(t, err)
	require.Contains(t, out, "UpdateTopicConfig")
	require.NotContains(t, out, "Succeeded")
}

func TestApplyBlockedWithoutGate(t *testing.T) {
	// gated fixture drifts only by an IncreasePartitions op (destructive gate).
	// Without --allow-destructive it is Blocked -> exit 3 (spec §15: blocked-only
	// means "needs human approval", distinct from exit-2 failures).
	out, err := run(t, "apply", "-f", "testdata/gated",
		"--cluster-config-file", "testdata/clusters/gated-cluster.yaml")
	requireExitCode(t, err, 3)
	require.Contains(t, out, "Blocked")

	// With the gate it executes and succeeds -> exit 0.
	out, err = run(t, "apply", "-f", "testdata/gated",
		"--cluster-config-file", "testdata/clusters/gated-cluster.yaml", "--allow-destructive")
	require.NoError(t, err)
	require.Contains(t, out, "Succeeded")
}

func TestApplyExitCodeClassification(t *testing.T) {
	// applyExitCode is only consulted for non-OK results: 3 iff every non-OK
	// status is Blocked; any Failed/Skipped/Rejected/Unsupported keeps 2.
	mkRes := func(statuses ...executor.Status) executor.Result {
		var r executor.Result
		for _, s := range statuses {
			r.Results = append(r.Results, executor.OpResult{Status: s})
		}
		return r
	}
	require.Equal(t, 3, applyExitCode(mkRes(executor.Blocked)))
	require.Equal(t, 3, applyExitCode(mkRes(executor.Succeeded, executor.Blocked, executor.ReportOnly)))
	require.Equal(t, 2, applyExitCode(mkRes(executor.Failed)))
	require.Equal(t, 2, applyExitCode(mkRes(executor.Blocked, executor.Failed)))
	require.Equal(t, 2, applyExitCode(mkRes(executor.Skipped)))
	require.Equal(t, 2, applyExitCode(mkRes(executor.Rejected)))
	require.Equal(t, 2, applyExitCode(mkRes(executor.Unsupported)))
}

func TestVerifyDriftExitCode(t *testing.T) {
	_, err := run(t, "verify", "-f", "testdata/valid",
		"--cluster-config-file", "testdata/clusters/dev.yaml")
	requireExitCode(t, err, 1) // drift present vs mock state
}

func TestVerifyNoDriftSucceeds(t *testing.T) {
	_, err := run(t, "verify", "-f", "testdata/nodrift",
		"--cluster-config-file", "testdata/clusters/nodrift-cluster.yaml")
	require.NoError(t, err) // live state matches manifests exactly -> exit 0
}

func TestDocumentKindReflectsCommand(t *testing.T) {
	// The yaml/json document kind names the command that produced it: diff ->
	// DiffOutput, verify -> VerifyOutput; only apply --dry-run keeps the
	// spec §17.5 ApplyDryRunOutput.
	out, err := run(t, "diff", "-f", "testdata/valid",
		"--cluster-config-file", "testdata/clusters/dev.yaml", "-o", "yaml")
	require.NoError(t, err)
	require.Contains(t, out, "kind: DiffOutput")

	out, err = run(t, "verify", "-f", "testdata/nodrift",
		"--cluster-config-file", "testdata/clusters/nodrift-cluster.yaml", "-o", "yaml")
	require.NoError(t, err)
	require.Contains(t, out, "kind: VerifyOutput")

	out, err = run(t, "apply", "-f", "testdata/valid",
		"--cluster-config-file", "testdata/clusters/dev.yaml", "--dry-run", "-o", "yaml")
	require.NoError(t, err)
	require.Contains(t, out, "kind: ApplyDryRunOutput")
}

func TestDiffNoErrorOnDrift(t *testing.T) {
	_, err := run(t, "diff", "-f", "testdata/valid",
		"--cluster-config-file", "testdata/clusters/dev.yaml")
	require.NoError(t, err)
}

func TestApplyDryRunShowsRegisterSchema(t *testing.T) {
	out, err := run(t, "apply", "-f", "testdata/schema", "-R",
		"--cluster-config-file", "testdata/clusters/schema-cluster.yaml", "--dry-run")
	require.NoError(t, err)
	require.Contains(t, out, "RegisterSchema")
	require.Contains(t, out, "payments.orders-value")
}

func TestVerifySchemaDrift(t *testing.T) {
	_, err := run(t, "verify", "-f", "testdata/schema", "-R",
		"--cluster-config-file", "testdata/clusters/schema-cluster.yaml")
	requireExitCode(t, err, 1) // value subject absent in mock SR -> drift
}

func TestApplyRegistersSchema(t *testing.T) {
	out, err := run(t, "apply", "-f", "testdata/schema", "-R",
		"--cluster-config-file", "testdata/clusters/schema-cluster.yaml")
	require.NoError(t, err)
	require.Contains(t, out, "Succeeded")
	require.Contains(t, out, "RegisterSchema")
}

func TestApplySchemaLowerBlockedWithoutGate(t *testing.T) {
	// schema-lower fixture: kafka state matches the manifest (no kafka drift) and
	// the SR mock seeds subject payments.orders-value with the SAME definition as
	// the manifest's .avsc (no RegisterSchema drift) but compatibility FULL. The
	// manifest desires BACKWARD, which is strictly lower -> exactly one
	// LowerSchemaCompatibility op. Without --allow-destructive it is Blocked
	// (exit 3: blocked-only, spec §15) and nothing executes.
	out, err := run(t, "apply", "-f", "testdata/schema-lower", "-R",
		"--cluster-config-file", "testdata/clusters/schema-lower-cluster.yaml")
	requireExitCode(t, err, 3)
	require.Contains(t, out, "Blocked")
	require.Contains(t, out, "LowerSchemaCompatibility")
	require.NotContains(t, out, "Succeeded")
}

func TestApplySchemaLowerSucceedsWithGate(t *testing.T) {
	// Same fixture as above, but --allow-destructive opens the gate so the single
	// LowerSchemaCompatibility op executes -> exit 0.
	out, err := run(t, "apply", "-f", "testdata/schema-lower", "-R",
		"--cluster-config-file", "testdata/clusters/schema-lower-cluster.yaml", "--allow-destructive")
	require.NoError(t, err)
	require.Contains(t, out, "Succeeded")
}

// Governance mode (spec §12.2): the manifest declares spec.schema with only
// format + compatibility (no body), and the SR mock has the producer's v1..v3
// already registered. diff/apply must show ONLY compatibility convergence and
// NEVER RegisterSchema — the producer-registered versions are not drift.
func TestGovernanceModeDiffShowsOnlyCompatibility(t *testing.T) {
	out, err := run(t, "diff", "-f", "testdata/governance", "-R",
		"--cluster-config-file", "testdata/clusters/governance-cluster.yaml")
	require.NoError(t, err)
	require.Contains(t, out, "RaiseSchemaCompatibility")
	require.Contains(t, out, "payments.orders-value")
	require.NotContains(t, out, "RegisterSchema")
	require.NotContains(t, out, "SchemaSuperseded")
}

func TestGovernanceModeApplyConvergesCompatibilityNeverRegisters(t *testing.T) {
	out, err := run(t, "apply", "-f", "testdata/governance", "-R",
		"--cluster-config-file", "testdata/clusters/governance-cluster.yaml")
	require.NoError(t, err)
	require.Contains(t, out, "Succeeded")
	require.Contains(t, out, "RaiseSchemaCompatibility")
	require.NotContains(t, out, "RegisterSchema")
}

// RecordName strategy (spec §11): the value subject is the schema's record full
// name ("payments.Order"), not "<topic>-value". diff must emit a RegisterSchema
// op targeting that subject.
func TestRecordNameStrategyRegistersUnderRecordFullName(t *testing.T) {
	out, err := run(t, "diff", "-f", "testdata/recordname", "-R",
		"--cluster-config-file", "testdata/clusters/recordname-cluster.yaml")
	require.NoError(t, err)
	require.Contains(t, out, "RegisterSchema")
	require.Contains(t, out, "payments.Order")
	require.NotContains(t, out, "payments.orders-value")
}

// Custom strategy + governance mode (spec §11, §12.2): monedula governs the
// compatibility of an ARBITRARY subject named verbatim via valueSubject. diff
// must emit only a compatibility op on that subject and NEVER RegisterSchema.
func TestCustomGovernanceConvergesCompatibilityOnExplicitSubject(t *testing.T) {
	out, err := run(t, "diff", "-f", "testdata/custom-governance", "-R",
		"--cluster-config-file", "testdata/clusters/custom-governance-cluster.yaml")
	require.NoError(t, err)
	require.Contains(t, out, "RaiseSchemaCompatibility")
	require.Contains(t, out, "my.custom.subject")
	require.NotContains(t, out, "RegisterSchema")
	require.NotContains(t, out, "payments.orders-value")
}

// runSplit runs a command capturing stdout and stderr separately, so import
// tests can assert determinism of the manifest stream (stdout) independent of
// the summary (stderr).
func runSplit(t *testing.T, args ...string) (stdout, stderr string, err error) {
	t.Helper()
	cmd := NewRootCmd()
	var outBuf, errBuf bytes.Buffer
	cmd.SetOut(&outBuf)
	cmd.SetErr(&errBuf)
	cmd.SetArgs(args)
	err = cmd.Execute()
	return outBuf.String(), errBuf.String(), err
}

func TestLogLevelInvalidExitsTwo(t *testing.T) {
	_, err := run(t, "diff", "--log-level", "verbose",
		"-f", "testdata/valid", "--cluster-config-file", "testdata/clusters/dev.yaml")
	requireExitCode(t, err, 2)
}

func TestLogLevelDebugEmitsToStderrOnly(t *testing.T) {
	// Logs go to STDERR so stdout stays a clean, pipeable command output.
	stdout, stderr, err := runSplit(t, "diff", "--log-level", "debug",
		"-f", "testdata/valid", "--cluster-config-file", "testdata/clusters/dev.yaml")
	require.NoError(t, err)
	require.Contains(t, stderr, "computed operations")
	require.Contains(t, stderr, "connecting to cluster")
	require.Contains(t, stderr, "reading live state")
	require.NotContains(t, stdout, "level=")
	require.Contains(t, stdout, "UpdateTopicConfig") // command output intact
}

func TestLogLevelDefaultIsQuiet(t *testing.T) {
	// Default level is warn: a run with nothing to warn about must still add
	// nothing to stderr (this fixture has no unknown RBAC roles/coarsening).
	_, stderr, err := runSplit(t, "diff",
		"-f", "testdata/valid", "--cluster-config-file", "testdata/clusters/dev.yaml")
	require.NoError(t, err)
	require.Empty(t, stderr)
}

// TestLogLevelDefaultShowsUnknownRoleWarning pins the review fix: at the
// default log level (now warn, was error) a role-name typo is no longer
// silent. testdata/unknown-role declares a KafkaRoleBinding with an
// unrecognised role name against a cluster with MDS configured (validation
// accepts unknown roles per spec §40; the CLI only warns).
func TestLogLevelDefaultShowsUnknownRoleWarning(t *testing.T) {
	_, stderr, err := runSplit(t, "diff", "-f", "testdata/unknown-role",
		"--cluster-config-file", "testdata/clusters/import-rbac-cluster.yaml")
	require.NoError(t, err)
	require.Contains(t, stderr, "unknown RBAC role")

	// --log-level error suppresses it again.
	_, stderr, err = runSplit(t, "diff", "-f", "testdata/unknown-role",
		"--log-level", "error",
		"--cluster-config-file", "testdata/clusters/import-rbac-cluster.yaml")
	require.NoError(t, err)
	require.NotContains(t, stderr, "unknown RBAC role")
}

func TestLogLevelInfoEmitsStartAndCounts(t *testing.T) {
	_, stderr, err := runSplit(t, "diff", "--log-level", "info",
		"-f", "testdata/valid", "--cluster-config-file", "testdata/clusters/dev.yaml")
	require.NoError(t, err)
	require.Contains(t, stderr, "command start")
	require.Contains(t, stderr, "computed operations")
	require.NotContains(t, stderr, "connecting to cluster") // debug-only
}

func TestImportClusterStdout(t *testing.T) {
	stdout, stderr, err := runSplit(t, "import", "cluster",
		"--cluster-config-file", "testdata/clusters/importable.yaml")
	require.NoError(t, err)
	require.Contains(t, stdout, "kind: KafkaTopic")
	require.Contains(t, stdout, "User:svc-checkout") // producer principal folded
	require.Contains(t, stdout, "kind: KafkaAccessPolicy")

	// stdout/stderr split contract: manifests go to stdout, the human summary
	// goes to stderr so the manifest stream stays a clean pipe target. The
	// human RenderSummary output begins with a stable "Cluster:" line.
	const summaryMarker = "Cluster:"
	require.Contains(t, stderr, summaryMarker)
	require.NotContains(t, stdout, summaryMarker)

	// Determinism: a second run produces byte-identical manifests.
	stdout2, _, err := runSplit(t, "import", "cluster",
		"--cluster-config-file", "testdata/clusters/importable.yaml")
	require.NoError(t, err)
	require.Equal(t, stdout, stdout2)
}

// TestImportClusterExcludesInternalTopicsByDefault: Kafka/Confluent
// housekeeping topics present in the live cluster (see
// testdata/clusters/importable-state.yaml) are excluded from the generated
// manifests by default — no "__consumer_offsets", "_schemas", or
// "_confluent-monitoring" KafkaTopic is emitted.
func TestImportClusterExcludesInternalTopicsByDefault(t *testing.T) {
	stdout, _, err := runSplit(t, "import", "cluster",
		"--cluster-config-file", "testdata/clusters/importable.yaml")
	require.NoError(t, err)
	require.NotContains(t, stdout, "__consumer_offsets")
	require.NotContains(t, stdout, "_schemas")
	require.NotContains(t, stdout, "_confluent-monitoring")
}

// TestImportClusterIncludeInternal: --include-internal disables the default
// filter, importing the housekeeping topics as ordinary KafkaTopic manifests.
func TestImportClusterIncludeInternal(t *testing.T) {
	stdout, _, err := runSplit(t, "import", "cluster",
		"--cluster-config-file", "testdata/clusters/importable.yaml",
		"--include-internal")
	require.NoError(t, err)
	require.Contains(t, stdout, "__consumer_offsets")
	require.Contains(t, stdout, "_schemas")
	require.Contains(t, stdout, "_confluent-monitoring")
}

func TestImportClusterToDir(t *testing.T) {
	dir := t.TempDir()
	out, err := run(t, "import", "cluster",
		"--cluster-config-file", "testdata/clusters/importable.yaml",
		"--output-dir", dir)
	require.NoError(t, err)

	// Build sets Metadata.Name = topic name ("payments.orders"); default
	// namespace strategy "single" -> "default". WriteToDir path is
	// <dir>/<ns>/topics/<name>.yaml.
	ordersPath := filepath.Join(dir, "default", "topics", "payments.orders.yaml")
	require.FileExists(t, ordersPath)
	require.Contains(t, out, "Wrote")

	// Re-run with default overwrite "never": existing files are skipped, exit 0.
	out2, err := run(t, "import", "cluster",
		"--cluster-config-file", "testdata/clusters/importable.yaml",
		"--output-dir", dir)
	require.NoError(t, err)
	require.Contains(t, out2, "skipped")
}

// ---- SCRAM user import (spec §5) ----

// TestImportClusterSkipsConnectingUserByDefault: the mock cluster connects as
// "admin" (IMPORT_USERS_TEST_USER), which is also a seeded SCRAM credential.
// By default it must be skipped (self-lockout guard) while other users are
// captured, and a warning naming it must appear on stderr.
func TestImportClusterSkipsConnectingUserByDefault(t *testing.T) {
	t.Setenv("IMPORT_USERS_TEST_USER", "admin")
	t.Setenv("IMPORT_USERS_TEST_PASS", "pw")

	stdout, stderr, err := runSplit(t, "import", "cluster",
		"--cluster-config-file", "testdata/clusters/import-users-cluster.yaml")
	require.NoError(t, err)

	require.Contains(t, stdout, "kind: KafkaUser")
	require.Contains(t, stdout, "svc-checkout")
	require.NotContains(t, stdout, "username: admin")
	require.Contains(t, stderr, `skipped connecting principal "admin"`)
	require.Contains(t, stderr, "--include-connecting-user")

	// Both-mechanisms warning: only SCRAM-SHA-512 captured for svc-reporting.
	require.Contains(t, stderr, `user "svc-reporting" also has a SCRAM-SHA-256 credential`)

	// Password-unrecoverability warning.
	require.Contains(t, stderr, "NOT recoverable from the cluster")
}

// TestImportClusterIncludeConnectingUser: --include-connecting-user captures
// the connecting principal's own credential too, with no skip warning.
func TestImportClusterIncludeConnectingUser(t *testing.T) {
	t.Setenv("IMPORT_USERS_TEST_USER", "admin")
	t.Setenv("IMPORT_USERS_TEST_PASS", "pw")

	stdout, stderr, err := runSplit(t, "import", "cluster",
		"--cluster-config-file", "testdata/clusters/import-users-cluster.yaml",
		"--include-connecting-user")
	require.NoError(t, err)

	require.Contains(t, stdout, "username: admin")
	require.NotContains(t, stderr, "skipped connecting principal")
}

// TestImportClusterSkipUsers: --skip-users omits SCRAM credential
// reconstruction entirely and the summary reports it as skipped rather than a
// zero count (mirrors --skip-schemas).
func TestImportClusterSkipUsers(t *testing.T) {
	t.Setenv("IMPORT_USERS_TEST_USER", "admin")
	t.Setenv("IMPORT_USERS_TEST_PASS", "pw")

	stdout, stderr, err := runSplit(t, "import", "cluster",
		"--cluster-config-file", "testdata/clusters/import-users-cluster.yaml",
		"--skip-users")
	require.NoError(t, err)

	require.NotContains(t, stdout, "kind: KafkaUser")
	require.Contains(t, stderr, "Users: skipped (--skip-users)")
}

// TestImportClusterSkipQuotas: --skip-quotas omits quota reconstruction
// entirely (no ListQuotas/DescribeClientQuotas call — the Confluent Cloud
// escape hatch, where quota describes are rejected outright) and the summary
// reports it as skipped rather than a zero count (mirrors --skip-users).
func TestImportClusterSkipQuotas(t *testing.T) {
	// Sanity: without the flag the seeded quota IS reconstructed, so the skip
	// below is proven to actually omit something.
	stdout, stderr, err := runSplit(t, "import", "cluster",
		"--cluster-config-file", "testdata/clusters/importable.yaml")
	require.NoError(t, err)
	require.Contains(t, stdout, "kind: KafkaQuota")
	require.Contains(t, stderr, "Quotas: 1")

	stdout, stderr, err = runSplit(t, "import", "cluster",
		"--cluster-config-file", "testdata/clusters/importable.yaml",
		"--skip-quotas")
	require.NoError(t, err)
	require.NotContains(t, stdout, "kind: KafkaQuota")
	require.Contains(t, stderr, "Quotas: skipped (--skip-quotas)")
}

// TestImportClusterUsersToDir: users are written under <ns>/users/<name>.yaml,
// mirroring quotas/rolebindings.
func TestImportClusterUsersToDir(t *testing.T) {
	t.Setenv("IMPORT_USERS_TEST_USER", "admin")
	t.Setenv("IMPORT_USERS_TEST_PASS", "pw")

	dir := t.TempDir()
	out, err := run(t, "import", "cluster",
		"--cluster-config-file", "testdata/clusters/import-users-cluster.yaml",
		"--output-dir", dir)
	require.NoError(t, err)
	require.Contains(t, out, "Wrote")

	checkoutPath := filepath.Join(dir, "default", "users", "svc-checkout.yaml")
	require.FileExists(t, checkoutPath)
	data, err := os.ReadFile(checkoutPath)
	require.NoError(t, err)
	require.Contains(t, string(data), "kind: KafkaUser")
	require.Contains(t, string(data), "username: svc-checkout")
	require.Contains(t, string(data), "KAFKA_USER_SVC_CHECKOUT_PASSWORD")

	// The connecting principal must not have been written.
	adminPath := filepath.Join(dir, "default", "users", "admin.yaml")
	require.NoFileExists(t, adminPath)
}

func TestImportNamespacePrefix(t *testing.T) {
	dir := t.TempDir()
	_, err := run(t, "import", "cluster",
		"--cluster-config-file", "testdata/clusters/importable.yaml",
		"--output-dir", dir,
		"--namespace-strategy", "prefix",
		"--prefix-separator", ".")
	require.NoError(t, err)

	// "payments.orders" -> first segment "payments".
	ordersPath := filepath.Join(dir, "payments", "topics", "payments.orders.yaml")
	require.FileExists(t, ordersPath)
}

func TestImportClusterWithSchemasToDir(t *testing.T) {
	dir := t.TempDir()
	out, err := run(t, "import", "cluster",
		"--cluster-config-file", "testdata/clusters/import-schema-cluster.yaml",
		"--output-dir", dir)
	require.NoError(t, err)
	require.Contains(t, out, "Wrote")

	// Topic manifest references the schema file via a relative path.
	topicPath := filepath.Join(dir, "default", "topics", "payments.orders.yaml")
	require.FileExists(t, topicPath)
	topicYAML, err := os.ReadFile(topicPath)
	require.NoError(t, err)
	require.Contains(t, string(topicYAML), "../schemas/payments.orders-value.avsc")

	// The schema file is written verbatim under <ns>/schemas.
	schemaPath := filepath.Join(dir, "default", "schemas", "payments.orders-value.avsc")
	require.FileExists(t, schemaPath)
	schemaBody, err := os.ReadFile(schemaPath)
	require.NoError(t, err)
	require.Contains(t, string(schemaBody), `"name": "Order"`)
}

func TestImportClusterStdoutRefusesSchemas(t *testing.T) {
	// I13: stdout mode streams only manifests, so schema files referenced via
	// spec.schema.*.valueFrom.file would never be written. Refuse with exit 2
	// and point the user at --output-dir instead of emitting broken manifests.
	_, _, err := runSplit(t, "import", "cluster",
		"--cluster-config-file", "testdata/clusters/import-schema-cluster.yaml")
	requireExitCode(t, err, 2)
	var ee *ExitError
	require.ErrorAs(t, err, &ee)
	require.Contains(t, ee.Msg, "--output-dir")
	require.Contains(t, ee.Msg, "schema")
}

func TestImportMultiClusterRequiresSelector(t *testing.T) {
	_, err := run(t, "import", "cluster",
		"--cluster-config-file", "testdata/clusters-multi/a.yaml")
	requireExitCode(t, err, 2)
}

// TestImportSkipSchemasToDir: when --skip-schemas is set on a cluster that
// HAS a Schema Registry with a seeded subject (import-schema-cluster.yaml),
// no spec.schema block must appear in the emitted topic manifest, and the
// summary must report "Schemas: skipped (--skip-schemas)".
func TestImportSkipSchemasToDir(t *testing.T) {
	dir := t.TempDir()
	out, err := run(t, "import", "cluster",
		"--cluster-config-file", "testdata/clusters/import-schema-cluster.yaml",
		"--output-dir", dir,
		"--skip-schemas")
	require.NoError(t, err)

	// Topic manifest must NOT carry a spec.schema block.
	topicPath := filepath.Join(dir, "default", "topics", "payments.orders.yaml")
	require.FileExists(t, topicPath)
	topicYAML, err := os.ReadFile(topicPath)
	require.NoError(t, err)
	require.NotContains(t, string(topicYAML), "schema:")

	// Schema file must NOT be written at all.
	schemaPath := filepath.Join(dir, "default", "schemas", "payments.orders-value.avsc")
	require.NoFileExists(t, schemaPath)

	// Summary (stdout for --output-dir) must carry the skip note.
	require.Contains(t, out, "Schemas: skipped (--skip-schemas)")
	require.NotContains(t, out, "schemasSkipped") // human format only, not raw field
}

// TestImportSkipSchemasSummaryJSON: --skip-schemas with -o json must set
// schemasSkipped:true in the JSON output so machine consumers can distinguish
// "intentionally skipped" from "cluster has no Schema Registry".
func TestImportSkipSchemasSummaryJSON(t *testing.T) {
	dir := t.TempDir()
	out, err := run(t, "import", "cluster",
		"--cluster-config-file", "testdata/clusters/import-schema-cluster.yaml",
		"--output-dir", dir,
		"--skip-schemas",
		"-o", "json")
	require.NoError(t, err)
	require.Contains(t, out, `"schemasSkipped": true`)
}

// TestImportSkipSchemasStdoutSucceeds: --skip-schemas unblocks stdout mode on a
// cluster that HAS a Schema Registry with seeded subjects.  Normally, stdout
// import of such a cluster fails with exit 2 ("schema files cannot be emitted
// to stdout — use --output-dir"); with --skip-schemas no schema files are
// produced, so the guard never fires and the command exits 0.
//
// Control: TestImportClusterStdoutRefusesSchemas covers the exit-2 path with
// the same fixture but without --skip-schemas.
func TestImportSkipSchemasStdoutSucceeds(t *testing.T) {
	stdout, stderr, err := runSplit(t, "import", "cluster",
		"--cluster-config-file", "testdata/clusters/import-schema-cluster.yaml",
		"--skip-schemas")
	require.NoError(t, err)

	// Manifest stream: topics must be emitted, no schema block present.
	require.Contains(t, stdout, "kind: KafkaTopic")
	require.NotContains(t, stdout, "schema:")

	// Summary (stderr in stdout mode): must carry the skip note.
	require.Contains(t, stderr, "Schemas: skipped (--skip-schemas)")
}

// TestImportNoSkipSchemasHasSchemas: without --skip-schemas the same cluster
// fixture must produce a schema block in the topic manifest (control case).
func TestImportNoSkipSchemasHasSchemas(t *testing.T) {
	dir := t.TempDir()
	_, err := run(t, "import", "cluster",
		"--cluster-config-file", "testdata/clusters/import-schema-cluster.yaml",
		"--output-dir", dir)
	require.NoError(t, err)

	topicPath := filepath.Join(dir, "default", "topics", "payments.orders.yaml")
	topicYAML, err := os.ReadFile(topicPath)
	require.NoError(t, err)
	require.Contains(t, string(topicYAML), "schema:")

	schemaPath := filepath.Join(dir, "default", "schemas", "payments.orders-value.avsc")
	require.FileExists(t, schemaPath)
}

// TestImportRBACClusterEmitsRoleBindings: a cluster with authorization.mds +
// accessBackends:[rbac] and the mock-mds-file annotation must emit
// KafkaRoleBinding manifests — one per live role binding that could not be
// folded into a topic's spec.access block. The SystemAdmin cluster-scoped
// binding has no resource pattern and is always emitted as an explicit
// KafkaRoleBinding. The DeveloperWrite topic binding may be folded or emitted
// explicitly; either way the output must contain "KafkaRoleBinding".
func TestImportRBACClusterEmitsRoleBindings(t *testing.T) {
	stdout, _, err := runSplit(t, "import", "cluster",
		"--cluster-config-file", "testdata/clusters/import-rbac-cluster.yaml",
		"--skip-schemas")
	require.NoError(t, err)
	require.Contains(t, stdout, "kind: KafkaRoleBinding")
	require.Contains(t, stdout, "User:admin")
}

// TestImportNoMDSClusterUnchanged: a cluster without authorization.mds still
// imports topics and ACLs successfully and emits no KafkaRoleBinding manifests.
// This is the regression guard: wiring nil MDS must be a no-op.
func TestImportNoMDSClusterUnchanged(t *testing.T) {
	stdout, _, err := runSplit(t, "import", "cluster",
		"--cluster-config-file", "testdata/clusters/importable.yaml")
	require.NoError(t, err)
	require.Contains(t, stdout, "kind: KafkaTopic")
	require.NotContains(t, stdout, "kind: KafkaRoleBinding")
}

// TestApplySchemaSupersededExits2 pins spec §12.1 end to end: the manifest
// schema is registered as v1 while the registry's latest is v2. Apply must NOT
// re-register (it would dedupe and loop); it records the terminal Unsupported
// outcome with the explanatory message and exits 2.
func TestApplySchemaSupersededExits2(t *testing.T) {
	out, err := run(t, "apply", "-f", "testdata/superseded",
		"--cluster-config-file", "testdata/clusters/superseded-cluster.yaml")
	requireExitCode(t, err, 2)
	require.Contains(t, out, "Unsupported SchemaSuperseded")
	require.Contains(t, out, "older version of subject payments.sup-value")
	require.Contains(t, out, "latest is v2")
	require.NotContains(t, out, "RegisterSchema")
}

// TestDiffSchemaSupersededRendersDrift: superseded IS divergence — diff renders
// it with the explanatory message; verify exits 1 on it.
func TestDiffSchemaSupersededRendersDrift(t *testing.T) {
	out, err := run(t, "diff", "-f", "testdata/superseded",
		"--cluster-config-file", "testdata/clusters/superseded-cluster.yaml")
	require.NoError(t, err)
	require.Contains(t, out, "SchemaSuperseded")
	require.Contains(t, out, "older version of subject payments.sup-value")

	_, err = run(t, "verify", "-f", "testdata/superseded",
		"--cluster-config-file", "testdata/clusters/superseded-cluster.yaml")
	requireExitCode(t, err, 1)
}

// ---- self-lockout warning (spec §30.3) ----

// TestApplyLockoutWarningOnStderr: the desired ACL set lists only
// User:svc-checkout for the topic, while the CLI connects as User:admin -> a
// heuristic warning on STDERR (stdout stays pipeable) for apply and
// apply --dry-run alike.
func TestApplyLockoutWarningOnStderr(t *testing.T) {
	t.Setenv("LOCKOUT_TEST_USER", "admin")
	t.Setenv("LOCKOUT_TEST_PASS", "pw")

	stdout, stderr, err := runSplit(t, "apply", "-f", "testdata/lockout",
		"--cluster-config-file", "testdata/clusters/lockout-cluster.yaml")
	require.NoError(t, err)
	require.Contains(t, stderr, "warning:")
	require.Contains(t, stderr, `topic "payments.orders"`)
	require.Contains(t, stderr, "User:admin")
	require.NotContains(t, stdout, "warning:")

	_, stderr, err = runSplit(t, "apply", "--dry-run", "-f", "testdata/lockout",
		"--cluster-config-file", "testdata/clusters/lockout-cluster.yaml")
	require.NoError(t, err)
	require.Contains(t, stderr, `topic "payments.orders"`)
}

// TestApplyNoLockoutWarningWhenPrincipalListed: the connecting principal IS in
// the topic's desired principal set -> silence.
func TestApplyNoLockoutWarningWhenPrincipalListed(t *testing.T) {
	t.Setenv("LOCKOUT_TEST_USER", "svc-checkout")
	t.Setenv("LOCKOUT_TEST_PASS", "pw")

	_, stderr, err := runSplit(t, "apply", "-f", "testdata/lockout",
		"--cluster-config-file", "testdata/clusters/lockout-cluster.yaml")
	require.NoError(t, err)
	require.NotContains(t, stderr, "warning:")
}

// TestApplyNoLockoutWarningWithoutAuth: no auth configured (mechanism None) ->
// no connecting principal to check -> no warning. The dev cluster fixture has
// no auth block and its manifest set declares ACLs.
func TestApplyNoLockoutWarningWithoutAuth(t *testing.T) {
	_, stderr, err := runSplit(t, "apply", "-f", "testdata/valid",
		"--cluster-config-file", "testdata/clusters/dev.yaml")
	require.NoError(t, err)
	require.NotContains(t, stderr, "warning:")
}
