// Package acceptance holds the v0.1 black-box acceptance suite. It builds the
// real monedula-gitops binary and runs it as a subprocess against the
// repository-root testdata/ fixtures, asserting exit codes and output. This
// pins the spec §36 acceptance criteria, catching wiring/exit-code regressions
// that the in-process unit tests cannot.
package acceptance

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// buildBinary compiles the CLI once into a temp dir and returns the binary path
// plus the repository root (used as the working dir so ./testdata paths
// resolve).
func buildBinary(t *testing.T) (bin string, repoRoot string) {
	t.Helper()
	wd, err := os.Getwd()
	require.NoError(t, err)
	// This test file lives in internal/acceptance, so the repo root is two
	// levels up.
	repoRoot = filepath.Clean(filepath.Join(wd, "..", ".."))
	bin = filepath.Join(t.TempDir(), "monedula-gitops")
	cmd := exec.Command("go", "build", "-o", bin, "./cmd/monedula-gitops")
	cmd.Dir = repoRoot
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "build failed: %s", out)
	return bin, repoRoot
}

// runCLI runs the binary from repoRoot, returning combined output and the
// process exit code.
func runCLI(t *testing.T, bin, repoRoot string, args ...string) (string, int) {
	t.Helper()
	cmd := exec.Command(bin, args...)
	cmd.Dir = repoRoot
	out, err := cmd.CombinedOutput()
	code := 0
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			code = ee.ExitCode()
		} else {
			t.Fatalf("run failed: %v (%s)", err, out)
		}
	}
	return string(out), code
}

func TestAcceptanceValidateValid(t *testing.T) {
	bin, root := buildBinary(t)
	_, code := runCLI(t, bin, root, "validate", "-f", "./testdata/valid")
	require.Equal(t, 0, code)
}

func TestAcceptanceValidateInvalid(t *testing.T) {
	bin, root := buildBinary(t)
	out, code := runCLI(t, bin, root, "validate", "-f", "./testdata/invalid")
	require.Equal(t, 2, code)
	require.NotEmpty(t, out)
	require.Contains(t, out, "partitions")
}

// TestAcceptanceConfigSamplesValid keeps config/samples honest: every shipped
// sample manifest must pass full CLI validation (with the sample clusters as
// cluster config, so cluster-ref and identity checks run too). This covers the
// v0.7 surface as well — the OAUTHBEARER + tenancy secure cluster and the
// governance-mode topic (spec.schema with no body) must validate cleanly.
func TestAcceptanceConfigSamplesValid(t *testing.T) {
	bin, root := buildBinary(t)
	out, code := runCLI(t, bin, root, "validate",
		"-f", "./config/samples/gitops_v1alpha1_kafkatopic.yaml",
		"-f", "./config/samples/gitops_v1alpha1_kafkaaccesspolicy.yaml",
		"-f", "./config/samples/gitops_v1alpha1_kafkatopic_governance.yaml",
		"-f", "./config/samples/gitops_v1alpha1_kafkaquota.yaml",
		"-f", "./config/samples/gitops_v1alpha1_kafkarolebinding.yaml",
		"--cluster-config-file", "./config/samples/gitops_v1alpha1_kafkacluster.yaml",
		"--cluster-config-file", "./config/samples/gitops_v1alpha1_kafkacluster_secure.yaml")
	require.Equal(t, 0, code, "config/samples must validate cleanly:\n%s", out)
	require.Contains(t, out, "6 resource(s) valid")
}

func TestAcceptanceApplyDryRunHuman(t *testing.T) {
	bin, root := buildBinary(t)
	out, code := runCLI(t, bin, root, "apply",
		"-f", "./testdata/valid",
		"--cluster-config-file", "./testdata/clusters/dev.yaml",
		"--dry-run")
	require.Equal(t, 0, code)
	require.Contains(t, out, "UpdateTopicConfig")
}

func TestAcceptanceApplyDryRunYAMLDeterministic(t *testing.T) {
	bin, root := buildBinary(t)
	args := []string{"apply",
		"-f", "./testdata/valid",
		"--cluster-config-file", "./testdata/clusters/dev.yaml",
		"--dry-run", "-o", "yaml"}
	out1, code1 := runCLI(t, bin, root, args...)
	require.Equal(t, 0, code1)
	require.Contains(t, out1, "ApplyDryRunOutput")
	out2, code2 := runCLI(t, bin, root, args...)
	require.Equal(t, 0, code2)
	require.Equal(t, out1, out2, "yaml dry-run output must be byte-identical across runs")
}

func TestAcceptanceApplyDryRunJSONDeterministic(t *testing.T) {
	bin, root := buildBinary(t)
	args := []string{"apply",
		"-f", "./testdata/valid",
		"--cluster-config-file", "./testdata/clusters/dev.yaml",
		"--dry-run", "-o", "json"}
	out1, code1 := runCLI(t, bin, root, args...)
	require.Equal(t, 0, code1)
	require.Contains(t, out1, "ApplyDryRunOutput")
	out2, code2 := runCLI(t, bin, root, args...)
	require.Equal(t, 0, code2)
	require.Equal(t, out1, out2, "json dry-run output must be byte-identical across runs")
}

func TestAcceptanceVerifyDrift(t *testing.T) {
	bin, root := buildBinary(t)
	_, code := runCLI(t, bin, root, "verify",
		"-f", "./testdata/valid",
		"--cluster-config-file", "./testdata/clusters/dev.yaml")
	require.Equal(t, 1, code)
}

func TestAcceptanceUnknownClusterRef(t *testing.T) {
	bin, root := buildBinary(t)
	_, code := runCLI(t, bin, root, "validate",
		"-f", "./testdata/unknownref",
		"--cluster-config-file", "./testdata/clusters/dev.yaml")
	require.Equal(t, 2, code)
}

// --- v0.2: real apply (hermetic via the mock-state annotation seam) ---

// apply (no --dry-run) executes the plan against the (mock-seeded) cluster and
// reports per-op results. The dev fixture differs by one ungated config op.
func TestAcceptanceApplyExecutes(t *testing.T) {
	bin, root := buildBinary(t)
	out, code := runCLI(t, bin, root, "apply",
		"-f", "./testdata/valid",
		"--cluster-config-file", "./testdata/clusters/dev.yaml")
	require.Equal(t, 0, code)
	require.Contains(t, out, "Succeeded")
}

// apply --dry-run previews the plan and must NOT execute (no apply result).
func TestAcceptanceApplyDryRunNoExecute(t *testing.T) {
	bin, root := buildBinary(t)
	out, code := runCLI(t, bin, root, "apply",
		"-f", "./testdata/valid",
		"--cluster-config-file", "./testdata/clusters/dev.yaml",
		"--dry-run")
	require.Equal(t, 0, code)
	require.Contains(t, out, "UpdateTopicConfig")
	require.NotContains(t, out, "Succeeded")
}

// A destructive op (partition increase) without the gate is Blocked and apply
// exits non-zero without mutating. Since v0.6 a blocked-ONLY apply exits 3
// (spec §15: "needs human approval"), distinct from the exit-2 failure class.
func TestAcceptanceApplyBlockedWithoutGate(t *testing.T) {
	bin, root := buildBinary(t)
	out, code := runCLI(t, bin, root, "apply",
		"-f", "./testdata/gated",
		"--cluster-config-file", "./testdata/clusters/gated-cluster.yaml")
	require.Equal(t, 3, code)
	require.Contains(t, out, "Blocked")
}

// The same op succeeds once the gate flag is supplied.
func TestAcceptanceApplyGatedSucceeds(t *testing.T) {
	bin, root := buildBinary(t)
	out, code := runCLI(t, bin, root, "apply",
		"-f", "./testdata/gated",
		"--cluster-config-file", "./testdata/clusters/gated-cluster.yaml",
		"--allow-destructive")
	require.Equal(t, 0, code)
	require.Contains(t, out, "Succeeded")
}

// --- v0.3: import (hermetic via the mock-state annotation seam) ---

// import cluster (stdout mode) reads live state and emits manifests on stdout
// (with the summary on stderr). The combined output must include topic and
// access-policy manifests plus a folded producer principal, and be
// deterministic across runs.
func TestAcceptanceImportClusterStdout(t *testing.T) {
	bin, root := buildBinary(t)
	args := []string{"import", "cluster",
		"--cluster-config-file", "./testdata/clusters/importable.yaml"}
	out1, code1 := runCLI(t, bin, root, args...)
	require.Equal(t, 0, code1)
	require.Contains(t, out1, "kind: KafkaTopic")
	require.Contains(t, out1, "kind: KafkaAccessPolicy")
	require.Contains(t, out1, "User:svc-checkout")

	out2, code2 := runCLI(t, bin, root, args...)
	require.Equal(t, 0, code2)
	require.Equal(t, out1, out2, "import stdout output must be byte-identical across runs")
}

// TestAcceptanceImportRoundTrip is the end-to-end proof of the core invariant:
// importing live state into manifests and then verifying those manifests
// against the SAME cluster reports zero drift. Both import and verify are backed
// by the same mock state (via the mock-state-file annotation), so a drift here
// would be a real round-trip bug.
func TestAcceptanceImportRoundTrip(t *testing.T) {
	bin, root := buildBinary(t)
	dir := t.TempDir() // absolute path

	// 1. Import live state into <dir>.
	out, code := runCLI(t, bin, root, "import", "cluster",
		"--cluster-config-file", "./testdata/clusters/importable.yaml",
		"--output-dir", dir)
	require.Equal(t, 0, code, "import failed: %s", out)

	// 2. Verify the imported manifests against the same cluster. The loader is
	// non-recursive by default; -R is required because import nests files at
	// <ns>/topics/<name>.yaml.
	vout, vcode := runCLI(t, bin, root, "verify",
		"-f", dir, "-R",
		"--cluster-config-file", "./testdata/clusters/importable.yaml")
	require.Equal(t, 0, vcode,
		"round-trip drift: import->verify did not converge. verify output:\n%s", vout)
}

// import cluster --output-dir writes manifests to disk under
// <namespace>/topics/<metadata.name>.yaml. With the default single strategy the
// namespace is "default" and metadata.name is the full topic name.
func TestAcceptanceImportToDir(t *testing.T) {
	bin, root := buildBinary(t)
	dir := t.TempDir()

	out, code := runCLI(t, bin, root, "import", "cluster",
		"--cluster-config-file", "./testdata/clusters/importable.yaml",
		"--output-dir", dir)
	require.Equal(t, 0, code, "import failed: %s", out)

	topicFile := filepath.Join(dir, "default", "topics", "payments.orders.yaml")
	require.FileExists(t, topicFile)
}

// --- v0.4: Schema Registry (hermetic via the mock-schema-file annotation) ---

// apply --dry-run against a cluster whose Schema Registry has NO subjects
// registered previews a RegisterSchema op for the desired value subject
// (payments.orders-value). The kafka state matches the manifest so the only
// planned op is the schema registration.
func TestAcceptanceApplyDryRunRegistersSchema(t *testing.T) {
	bin, root := buildBinary(t)
	out, code := runCLI(t, bin, root, "apply",
		"-f", "./testdata/schema", "-R",
		"--cluster-config-file", "./testdata/clusters/schema-cluster.yaml",
		"--dry-run")
	require.Equal(t, 0, code, "apply --dry-run output:\n%s", out)
	require.Contains(t, out, "RegisterSchema")
	require.Contains(t, out, "payments.orders-value")
}

// TestAcceptanceSchemaImportRoundTrip is the end-to-end proof of the schema
// round-trip: importing a cluster whose Schema Registry is SEEDED with a value
// subject folds that schema into the generated KafkaTopic (writing the schema
// body verbatim under <ns>/schemas/), and verifying those manifests against the
// SAME cluster reports zero drift — including the schema. Both import and verify
// are backed by the same mock kafka state and mock SR state (via the
// mock-state-file / mock-schema-file annotations), so any drift here is a real
// round-trip bug.
func TestAcceptanceSchemaImportRoundTrip(t *testing.T) {
	bin, root := buildBinary(t)
	dir := t.TempDir() // absolute path

	// 1. Import live state (topics + schemas) into <dir>.
	out, code := runCLI(t, bin, root, "import", "cluster",
		"--cluster-config-file", "./testdata/clusters/schema-import-cluster.yaml",
		"--output-dir", dir)
	require.Equal(t, 0, code, "import failed: %s", out)

	// 2. Verify the imported manifests against the same cluster. -R is required
	// because import nests files at <ns>/topics/<name>.yaml and the schema body
	// at <ns>/schemas/<name>-value.avsc.
	vout, vcode := runCLI(t, bin, root, "verify",
		"-f", dir, "-R",
		"--cluster-config-file", "./testdata/clusters/schema-import-cluster.yaml")
	require.Equal(t, 0, vcode,
		"schema round-trip drift: import->verify did not converge. verify output:\n%s", vout)
}

// TestAcceptanceSchemaCompatLowerBlocked pins the destructive gate for schema
// compatibility lowering: the SR has the subject at FULL compatibility while the
// manifest desires the strictly-lower BACKWARD, with no other drift. Without
// --allow-destructive the LowerSchemaCompatibility op is Blocked and apply exits
// 3 without mutating (blocked-only exit code, spec §15).
func TestAcceptanceSchemaCompatLowerBlocked(t *testing.T) {
	bin, root := buildBinary(t)
	out, code := runCLI(t, bin, root, "apply",
		"-f", "./testdata/schema-lower", "-R",
		"--cluster-config-file", "./testdata/clusters/schema-lower-cluster.yaml")
	require.Equal(t, 3, code, "apply output:\n%s", out)
	require.Contains(t, out, "Blocked")
	require.Contains(t, out, "LowerSchemaCompatibility")
}

// TestAcceptanceFirstCompatBelowGlobalBlocked pins the first-time-set risk fix
// (spec §17.1) end-to-end through the CLI pipeline: the SR mock has NO subject
// config for payments.orders-value but a GLOBAL default of BACKWARD, and the
// manifest declares compatibility NONE (governance mode). The subject's
// EFFECTIVE level is the global default, so this first-time set is an effective
// lowering: a gated LowerSchemaCompatibility that apply Blocks (exit 3) without
// --allow-destructive — previously it sailed through as an ungated Raise.
func TestAcceptanceFirstCompatBelowGlobalBlocked(t *testing.T) {
	bin, root := buildBinary(t)
	out, code := runCLI(t, bin, root, "apply",
		"-f", "./testdata/schema-fresh-none", "-R",
		"--cluster-config-file", "./testdata/clusters/schema-fresh-none-cluster.yaml")
	require.Equal(t, 3, code, "apply output:\n%s", out)
	require.Contains(t, out, "Blocked")
	require.Contains(t, out, "LowerSchemaCompatibility")
	require.Contains(t, out, "payments.orders-value")
}

// TestAcceptanceFirstCompatBelowGlobalApprovedApplies is the approved
// counterpart: the same first-time NONE-below-global set executes once
// --allow-destructive consents to the lowering.
func TestAcceptanceFirstCompatBelowGlobalApprovedApplies(t *testing.T) {
	bin, root := buildBinary(t)
	out, code := runCLI(t, bin, root, "apply",
		"-f", "./testdata/schema-fresh-none", "-R",
		"--cluster-config-file", "./testdata/clusters/schema-fresh-none-cluster.yaml",
		"--allow-destructive")
	require.Equal(t, 0, code, "apply output:\n%s", out)
	require.Contains(t, out, "LowerSchemaCompatibility")
	require.Contains(t, out, "Succeeded")
	require.NotContains(t, out, "Blocked")
}

// --- v0.7: governance-mode schema (compatibility-only) ---

// TestAcceptanceGovernanceDiffOnlyCompatibility pins the compatibility-only
// governance mode (spec §11.2, §12.2) end-to-end through the CLI: the manifest
// declares spec.schema with format + compatibility but NO valueSchema body, and
// the mock Schema Registry already has the producer-registered v1..v3 of
// payments.orders-value at BACKWARD. diff must show ONLY a compatibility
// convergence op (RaiseSchemaCompatibility BACKWARD->FULL) and must NEVER emit
// RegisterSchema — the producer-owned versions are not monedula-managed drift.
func TestAcceptanceGovernanceDiffOnlyCompatibility(t *testing.T) {
	bin, root := buildBinary(t)
	out, code := runCLI(t, bin, root, "diff",
		"-f", "./internal/cli/testdata/governance", "-R",
		"--cluster-config-file", "./internal/cli/testdata/clusters/governance-cluster.yaml")
	require.Equal(t, 0, code, "diff output:\n%s", out)
	require.Contains(t, out, "RaiseSchemaCompatibility")
	require.Contains(t, out, "payments.orders-value")
	require.NotContains(t, out, "RegisterSchema")
	require.NotContains(t, out, "SchemaSuperseded")
}

// TestAcceptanceGovernanceApplyNeverRegisters proves the same invariant on the
// mutating path: apply converges the subject compatibility (Succeeded) without
// ever registering a schema version.
func TestAcceptanceGovernanceApplyNeverRegisters(t *testing.T) {
	bin, root := buildBinary(t)
	out, code := runCLI(t, bin, root, "apply",
		"-f", "./internal/cli/testdata/governance", "-R",
		"--cluster-config-file", "./internal/cli/testdata/clusters/governance-cluster.yaml")
	require.Equal(t, 0, code, "apply output:\n%s", out)
	require.Contains(t, out, "Succeeded")
	require.Contains(t, out, "RaiseSchemaCompatibility")
	require.NotContains(t, out, "RegisterSchema")
}

// --- v0.8: per-entry host scoping (spec §8.4) ---

// TestAcceptanceHostScopedProducerCreateAcl proves end-to-end that a
// KafkaTopic manifest with a host-scoped producer entry (host: 10.0.0.0/8)
// compiles through the full CLI pipeline and produces a CreateAcl operation
// when the live state has no ACL for that topic. The host is part of the ACL
// identity (diffed against live state), so a CreateAcl in the output means the
// host-scoped entry was correctly compiled, not collapsed to "*".
func TestAcceptanceHostScopedProducerCreateAcl(t *testing.T) {
	bin, root := buildBinary(t)
	out, code := runCLI(t, bin, root, "apply",
		"-f", "./testdata/host-scoped",
		"--cluster-config-file", "./testdata/clusters/host-scoped-cluster.yaml",
		"--dry-run")
	require.Equal(t, 0, code, "apply --dry-run output:\n%s", out)
	require.Contains(t, out, "CreateAcl")
	require.Contains(t, out, "User:svc-ingest")
	// Verify the host-scoped ACL is for the correct topic, not a wildcard collapse.
	require.Contains(t, out, "payments.ingest")
	// Verify the non-default host is visible in the rendered output (spec §8.4).
	require.Contains(t, out, "10.0.0.0/8")
}

// TestAcceptanceHostScopedConverges proves that once the live state holds the
// host-scoped ACL (host: 10.0.0.0/8), a subsequent apply --dry-run reports
// "No changes." — the host is part of the ACL identity so the desired and live
// ACLs match exactly and no CreateAcl is emitted.
func TestAcceptanceHostScopedConverges(t *testing.T) {
	bin, root := buildBinary(t)
	// The converged cluster config points to a mock state that already contains
	// the host-scoped ACLs for User:svc-ingest on host 10.0.0.0/8.
	out, code := runCLI(t, bin, root, "apply",
		"-f", "./testdata/host-scoped",
		"--cluster-config-file", "./testdata/clusters/host-scoped-converged-cluster.yaml",
		"--dry-run")
	require.Equal(t, 0, code, "apply --dry-run output:\n%s", out)
	require.Contains(t, out, "No changes.")
	require.NotContains(t, out, "CreateAcl")
}

// --- v0.9: KafkaQuota (spec §39) ---

// TestAcceptanceKafkaQuotaDryRunSetQuota proves end-to-end that a KafkaQuota
// manifest with a user+clientId entity produces a SetQuota operation in the
// apply --dry-run output when live state has no quota for that entity. The
// entity target string (client-id=batch,user=svc-checkout) must appear in the
// human output, confirming the generic output path handles quota ops correctly.
func TestAcceptanceKafkaQuotaDryRunSetQuota(t *testing.T) {
	bin, root := buildBinary(t)
	out, code := runCLI(t, bin, root, "apply",
		"-f", "./testdata/quota",
		"--cluster-config-file", "./testdata/clusters/quota-dev.yaml",
		"--dry-run")
	require.Equal(t, 0, code, "apply --dry-run output:\n%s", out)
	require.Contains(t, out, "SetQuota")
	require.Contains(t, out, "KafkaQuota")
	// Entity components sorted alphabetically: client-id before user.
	require.Contains(t, out, "client-id=batch,user=svc-checkout")
}

// TestAcceptanceKafkaQuotaConverges proves that once the live mock state holds
// the exact quota for (User:svc-checkout, batch), a subsequent apply --dry-run
// reports "No changes." — the entity key matches and all desired limit values
// are present and correct in the live state.
func TestAcceptanceKafkaQuotaConverges(t *testing.T) {
	bin, root := buildBinary(t)
	// The converged cluster config points to a mock state that already contains
	// the quota for (svc-checkout, batch) with the desired limits.
	out, code := runCLI(t, bin, root, "apply",
		"-f", "./testdata/quota",
		"--cluster-config-file", "./testdata/clusters/quota-converged.yaml",
		"--dry-run")
	require.Equal(t, 0, code, "apply --dry-run output:\n%s", out)
	require.Contains(t, out, "No changes.")
	require.NotContains(t, out, "SetQuota")
}

// TestAcceptanceKafkaQuotaIPDryRunSetQuota proves end-to-end that a KafkaQuota
// with an ip entity + connectionCreationRate produces a SetQuota op in apply
// --dry-run output when live state has no quota for that ip (spec §39, v0.17).
// The ip entity target (ip=10.0.0.1) must appear in the human output.
func TestAcceptanceKafkaQuotaIPDryRunSetQuota(t *testing.T) {
	bin, root := buildBinary(t)
	out, code := runCLI(t, bin, root, "apply",
		"-f", "./testdata/quota-ip",
		"--cluster-config-file", "./testdata/clusters/quota-dev.yaml",
		"--dry-run")
	require.Equal(t, 0, code, "apply --dry-run output:\n%s", out)
	require.Contains(t, out, "SetQuota")
	require.Contains(t, out, "KafkaQuota")
	require.Contains(t, out, "ip=10.0.0.1")
}

// TestAcceptanceKafkaQuotaIPImportRoundTrip proves `import cluster` reconstructs
// an ip quota from live mock state into a KafkaQuota manifest carrying
// entity.ip + connectionCreationRate (spec §39, v0.17).
func TestAcceptanceKafkaQuotaIPImportRoundTrip(t *testing.T) {
	bin, root := buildBinary(t)
	out, code := runCLI(t, bin, root, "import", "cluster",
		"--cluster-config-file", "./testdata/clusters/quota-ip-import.yaml")
	require.Equal(t, 0, code, "import output:\n%s", out)
	require.Contains(t, out, "kind: KafkaQuota", "import must emit a KafkaQuota:\n%s", out)
	require.Contains(t, out, "ip: 10.0.0.1", "import must reconstruct entity.ip:\n%s", out)
	require.Contains(t, out, "connectionCreationRate", "import must reconstruct the ip limit:\n%s", out)
}

// --- v0.10: cross-resource ACL conflict (spec §9, §20.3) ---

// TestAcceptanceACLConflictValidateExitsNonZero pins the spec §9 CLI behavior:
// when two manifests referencing the same cluster grant opposing Allow and Deny
// permissions on the same ACL tuple (same principal/host/resource/operation),
// validate must exit non-zero (exit 2) and name the conflict in its output.
// This guards the existing BuildDesiredSet conflict detection end-to-end through
// the real binary, ensuring pipeline wiring regressions are caught before
// operator-mode paths (reconciler + webhook) are exercised.
func TestAcceptanceACLConflictValidateExitsNonZero(t *testing.T) {
	bin, root := buildBinary(t)
	out, code := runCLI(t, bin, root, "validate",
		"-f", "./testdata/acl-conflict")
	require.Equal(t, 2, code, "validate with Allow/Deny conflict must exit 2; output:\n%s", out)
	require.Contains(t, out, "ACL conflict", "output must name the ACL conflict")
}

// --- v0.12: TopicRecordName import strategy (spec §24.1) ---

// TestAcceptanceImportTopicRecordName proves the end-to-end TopicRecordName
// classification path: a mock Schema Registry is seeded with three subject
// kinds (TopicRecordName, TopicName, RecordName). The importer must:
//   - emit subjectStrategy: TopicRecordName in the orders topic manifest,
//   - surface the RecordName subject in the import summary's
//     "RecordName subjects needing manual attribution" section,
//   - exit 0.
//
// Because schemas are present the importer cannot write to stdout; --output-dir
// is used and the summary (in human format) goes to stdout.
func TestAcceptanceImportTopicRecordName(t *testing.T) {
	bin, root := buildBinary(t)
	dir := t.TempDir()

	out, code := runCLI(t, bin, root, "import", "cluster",
		"--cluster-config-file", "./testdata/clusters/recordname-import-cluster.yaml",
		"--output-dir", dir,
		"-o", "human")
	require.Equal(t, 0, code, "import must exit 0; output:\n%s", out)

	// The summary must report the RecordName subject needing manual attribution.
	require.Contains(t, out, "RecordName subjects needing manual attribution",
		"summary must include RecordName section; output:\n%s", out)
	require.Contains(t, out, "com.acme.Shared",
		"summary must name the RecordName subject com.acme.Shared; output:\n%s", out)

	// The orders topic manifest must carry subjectStrategy: TopicRecordName.
	ordersManifest := filepath.Join(dir, "default", "topics", "orders.yaml")
	require.FileExists(t, ordersManifest,
		"orders topic manifest must exist at default/topics/orders.yaml")
	manifestBytes, err := os.ReadFile(ordersManifest)
	require.NoError(t, err)
	require.Contains(t, string(manifestBytes), "subjectStrategy: TopicRecordName",
		"orders manifest must carry subjectStrategy TopicRecordName; manifest:\n%s", string(manifestBytes))
}

// TestAcceptanceImportSkipSchemas proves that --skip-schemas produces no
// spec.schema block in any emitted manifest, the summary notes the skip, and
// the command exits 0.
//
// With --skip-schemas, no Schema Registry connection is attempted, so no schema
// files are emitted and the importer can write manifests to stdout. We still
// use --output-dir here for consistency (so both manifest content and summary
// are captured in the combined output).
func TestAcceptanceImportSkipSchemas(t *testing.T) {
	bin, root := buildBinary(t)
	dir := t.TempDir()

	out, code := runCLI(t, bin, root, "import", "cluster",
		"--cluster-config-file", "./testdata/clusters/recordname-import-cluster.yaml",
		"--output-dir", dir,
		"--skip-schemas",
		"-o", "human")
	require.Equal(t, 0, code, "import --skip-schemas must exit 0; output:\n%s", out)

	// The summary must note the skip.
	require.Contains(t, out, "skipped (--skip-schemas)",
		"summary must note schemas were skipped; output:\n%s", out)

	// No manifest should contain a schema block.
	ordersManifest := filepath.Join(dir, "default", "topics", "orders.yaml")
	require.FileExists(t, ordersManifest)
	manifestBytes, err := os.ReadFile(ordersManifest)
	require.NoError(t, err)
	require.NotContains(t, string(manifestBytes), "schema:",
		"orders manifest must not contain schema block with --skip-schemas; manifest:\n%s", string(manifestBytes))

	eventsManifest := filepath.Join(dir, "default", "topics", "events.yaml")
	require.FileExists(t, eventsManifest)
	eventsBytes, err := os.ReadFile(eventsManifest)
	require.NoError(t, err)
	require.NotContains(t, string(eventsBytes), "schema:",
		"events manifest must not contain schema block with --skip-schemas; manifest:\n%s", string(eventsBytes))
}

// --- v0.13: KafkaRoleBinding validate/diff/apply (spec §40) ---

// TestAcceptanceRoleBindingValidateValid pins that a well-formed KafkaRoleBinding
// (resource-scoped DeveloperWrite on a kafka topic) passes validate and the count
// includes the role binding.
func TestAcceptanceRoleBindingValidateValid(t *testing.T) {
	bin, root := buildBinary(t)
	out, code := runCLI(t, bin, root, "validate",
		"-f", "./testdata/rolebinding/binding.yaml")
	require.Equal(t, 0, code, "valid KafkaRoleBinding must pass validate:\n%s", out)
	require.Contains(t, out, "1 resource(s) valid")
}

// TestAcceptanceRoleBindingValidateInvalidScope pins that a KafkaRoleBinding with
// an invalid scope.type fails validate (exit 2) and the error names the bad scope.
func TestAcceptanceRoleBindingValidateInvalidScope(t *testing.T) {
	bin, root := buildBinary(t)
	out, code := runCLI(t, bin, root, "validate",
		"-f", "./testdata/rolebinding-invalid/binding.yaml")
	require.Equal(t, 2, code, "KafkaRoleBinding with invalid scope.type must exit 2:\n%s", out)
	require.Contains(t, out, "scope.type", "error must name the bad scope.type field:\n%s", out)
}

// TestAcceptanceRoleBindingDiffAddRoleBinding proves end-to-end that when the MDS
// live state is empty (seeded via the mock-mds-file annotation), diff emits an
// AddRoleBinding for the desired binding absent from live.
func TestAcceptanceRoleBindingDiffAddRoleBinding(t *testing.T) {
	bin, root := buildBinary(t)
	out, code := runCLI(t, bin, root, "diff",
		"-f", "./testdata/rolebinding/binding.yaml",
		"--cluster-config-file", "./testdata/clusters/rbac-dev.yaml")
	require.Equal(t, 0, code, "diff output:\n%s", out)
	require.Contains(t, out, "AddRoleBinding", "diff must show AddRoleBinding:\n%s", out)
	require.Contains(t, out, "KafkaRoleBinding", "diff must identify kind:\n%s", out)
	require.Contains(t, out, "User:svc-checkout", "diff must include principal in target:\n%s", out)
	require.Contains(t, out, "DeveloperWrite", "diff must include role in target:\n%s", out)
}

// TestAcceptanceRoleBindingPruneCandidateReported proves that a live binding in
// scope but absent from desired surfaces as a RemoveRoleBinding prune candidate
// in diff output — it is REPORTED but NOT applied without --prune.
//
// The seeded state has two DeveloperWrite bindings for User:svc-checkout:
// one for Topic:orders (desired) and one for Topic:payments.old (extra, in scope
// because same principal+role+scope type). The extra one is a prune candidate.
func TestAcceptanceRoleBindingPruneCandidateReported(t *testing.T) {
	bin, root := buildBinary(t)
	// rbac-seeded cluster: live has svc-checkout/orders (desired) + svc-checkout/payments.old (extra, in scope).
	out, code := runCLI(t, bin, root, "diff",
		"-f", "./testdata/rolebinding/binding.yaml",
		"--cluster-config-file", "./testdata/clusters/rbac-seeded.yaml")
	require.Equal(t, 0, code, "diff output:\n%s", out)
	require.Contains(t, out, "RemoveRoleBinding", "diff must show RemoveRoleBinding prune candidate:\n%s", out)
	require.Contains(t, out, "prune candidate", "prune candidate must be flagged:\n%s", out)
	require.Contains(t, out, "payments.old", "prune candidate must name the extra resource:\n%s", out)
}

// TestAcceptanceRoleBindingApplyPruneCandidateNotApplied proves that apply
// WITHOUT --prune records the prune candidate as PruneDisabled (not deleted) and
// exits 0 (PruneDisabled is OK-equivalent, spec §10.3).
func TestAcceptanceRoleBindingApplyPruneCandidateNotApplied(t *testing.T) {
	bin, root := buildBinary(t)
	out, code := runCLI(t, bin, root, "apply",
		"-f", "./testdata/rolebinding/binding.yaml",
		"--cluster-config-file", "./testdata/clusters/rbac-seeded.yaml")
	require.Equal(t, 0, code, "apply without --prune must exit 0:\n%s", out)
	require.Contains(t, out, "PruneDisabled", "extra live binding must be PruneDisabled:\n%s", out)
}

// TestAcceptanceRoleBindingApplyPruneCandidateApplied proves that apply WITH
// --prune removes the extra live binding (Succeeded) and exits 0.
func TestAcceptanceRoleBindingApplyPruneCandidateApplied(t *testing.T) {
	bin, root := buildBinary(t)
	out, code := runCLI(t, bin, root, "apply",
		"-f", "./testdata/rolebinding/binding.yaml",
		"--cluster-config-file", "./testdata/clusters/rbac-seeded.yaml",
		"--prune")
	require.Equal(t, 0, code, "apply --prune must exit 0:\n%s", out)
	require.Contains(t, out, "Succeeded", "apply --prune must succeed:\n%s", out)
}

// TestAcceptanceRoleBindingConvergesNoOp proves that when the live MDS state
// exactly matches the desired set, apply --dry-run reports "No changes."
func TestAcceptanceRoleBindingConvergesNoOp(t *testing.T) {
	bin, root := buildBinary(t)
	out, code := runCLI(t, bin, root, "apply",
		"-f", "./testdata/rolebinding/binding.yaml",
		"--cluster-config-file", "./testdata/clusters/rbac-converged.yaml",
		"--dry-run")
	require.Equal(t, 0, code, "apply --dry-run output:\n%s", out)
	require.Contains(t, out, "No changes.", "converged cluster must show no changes:\n%s", out)
	require.NotContains(t, out, "AddRoleBinding")
	require.NotContains(t, out, "RemoveRoleBinding")
}

// --- v0.15: accessBackends dual-emit (spec §40) ---

// TestAcceptanceAccessBackendsACLOnly proves that a topic on a cluster whose
// accessBackends defaults to [acl] (i.e., MDS configured but no accessBackends
// field, so acl is the effective backend) produces CreateAcl operations and NO
// AddRoleBinding — the RBAC path is inactive.
func TestAcceptanceAccessBackendsACLOnly(t *testing.T) {
	bin, root := buildBinary(t)
	out, code := runCLI(t, bin, root, "apply",
		"-f", "./testdata/accessbackends/topic.yaml",
		"--cluster-config-file", "./testdata/clusters/ab-acl-only.yaml",
		"--dry-run")
	require.Equal(t, 0, code, "apply --dry-run output:\n%s", out)
	require.Contains(t, out, "CreateAcl",
		"acl-only cluster must emit CreateAcl:\n%s", out)
	require.NotContains(t, out, "AddRoleBinding",
		"acl-only cluster must NOT emit AddRoleBinding:\n%s", out)
}

// TestAcceptanceAccessBackendsRBACOnly proves that a topic on a cluster with
// accessBackends: [rbac] produces AddRoleBinding operations and NO CreateAcl —
// the ACL path is inactive.
func TestAcceptanceAccessBackendsRBACOnly(t *testing.T) {
	bin, root := buildBinary(t)
	out, code := runCLI(t, bin, root, "apply",
		"-f", "./testdata/accessbackends/topic.yaml",
		"--cluster-config-file", "./testdata/clusters/ab-rbac-only.yaml",
		"--dry-run")
	require.Equal(t, 0, code, "apply --dry-run output:\n%s", out)
	require.Contains(t, out, "AddRoleBinding",
		"rbac-only cluster must emit AddRoleBinding:\n%s", out)
	require.NotContains(t, out, "CreateAcl",
		"rbac-only cluster must NOT emit CreateAcl:\n%s", out)
}

// TestAcceptanceAccessBackendsDualEmit proves end-to-end that a topic on a
// cluster with accessBackends: [acl, rbac] produces BOTH CreateAcl and
// AddRoleBinding operations from the same access block — the dual-emit path
// (spec §40, shipped v0.15). This is the core invariant: a single
// KafkaTopic.spec.access producer entry produces an ACL Write AND a
// DeveloperWrite role binding simultaneously.
func TestAcceptanceAccessBackendsDualEmit(t *testing.T) {
	bin, root := buildBinary(t)
	out, code := runCLI(t, bin, root, "apply",
		"-f", "./testdata/accessbackends/topic.yaml",
		"--cluster-config-file", "./testdata/clusters/ab-dual.yaml",
		"--dry-run")
	require.Equal(t, 0, code, "apply --dry-run output:\n%s", out)
	require.Contains(t, out, "CreateAcl",
		"dual cluster must emit CreateAcl:\n%s", out)
	require.Contains(t, out, "AddRoleBinding",
		"dual cluster must emit AddRoleBinding:\n%s", out)
	// Confirm both sides reference the correct principal.
	require.Contains(t, out, "User:svc-checkout",
		"output must include the producer principal:\n%s", out)
	// Confirm the consumer's role binding for the group is also present.
	require.Contains(t, out, "fulfillment",
		"output must include the consumer group in role bindings:\n%s", out)
}

// --- v0.16: RBAC import (spec §40) ---

// TestAcceptanceImportRBACFoldedAccess proves end-to-end that `import cluster`
// on an rbac-only cluster reads live MDS role bindings, folds unambiguous
// producer/consumer patterns back into KafkaTopic.spec.access, and emits an
// explicit KafkaRoleBinding for the cluster-scoped SystemAdmin that cannot fold.
// Both the folded producer access (User:svc-checkout) and the explicit binding
// (User:ops-admin/SystemAdmin) must appear in the combined stdout output.
func TestAcceptanceImportRBACFoldedAccess(t *testing.T) {
	bin, root := buildBinary(t)
	args := []string{"import", "cluster",
		"--cluster-config-file", "./testdata/clusters/rbac-import-cluster.yaml"}
	out, code := runCLI(t, bin, root, args...)
	require.Equal(t, 0, code, "import must exit 0; output:\n%s", out)

	// Folded producer entry: the orders topic must carry a producers access block.
	require.Contains(t, out, "User:svc-checkout",
		"output must include the folded producer principal:\n%s", out)

	// Explicit KafkaRoleBinding for the cluster-scoped SystemAdmin.
	require.Contains(t, out, "kind: KafkaRoleBinding",
		"output must include an explicit KafkaRoleBinding for the unfoldable binding:\n%s", out)
	require.Contains(t, out, "SystemAdmin",
		"output must include the SystemAdmin cluster-scoped role:\n%s", out)
}

// TestAcceptanceImportRBACRoundTrip is the end-to-end proof of the RBAC
// import round-trip: importing live MDS role bindings into manifests and then
// verifying those manifests against the SAME cluster reports zero drift.
// Both import and verify are backed by the same mock state (mock-state-file +
// mock-mds-file), so any drift here is a real round-trip bug.
func TestAcceptanceImportRBACRoundTrip(t *testing.T) {
	bin, root := buildBinary(t)
	dir := t.TempDir()

	// 1. Import live state (topics + role bindings) into <dir>.
	out, code := runCLI(t, bin, root, "import", "cluster",
		"--cluster-config-file", "./testdata/clusters/rbac-import-cluster.yaml",
		"--output-dir", dir)
	require.Equal(t, 0, code, "import failed: %s", out)

	// 2. Verify the imported manifests against the same cluster. -R is required
	// because import nests files at <ns>/topics/<name>.yaml and
	// <ns>/rolebindings/<name>.yaml.
	vout, vcode := runCLI(t, bin, root, "verify",
		"-f", dir, "-R",
		"--cluster-config-file", "./testdata/clusters/rbac-import-cluster.yaml")
	require.Equal(t, 0, vcode,
		"RBAC round-trip drift: import->verify did not converge. verify output:\n%s", vout)
}

// --- v0.35: KafkaUser SCRAM credentials (spec §2-§4) ---

// TestAcceptanceKafkaUserDryRunCreate proves end-to-end that a KafkaUser
// manifest produces a CreateScramCredential op in apply --dry-run output when
// the declared user has no live credential. The undeclared live user seeded in
// the mock state (legacy-analytics) must stay invisible: no op mentions it and
// no destructive-gated DeleteScramCredential is reachable from the diff.
// Dry-run needs no password: resolution happens only at execute time.
func TestAcceptanceKafkaUserDryRunCreate(t *testing.T) {
	bin, root := buildBinary(t)
	out, code := runCLI(t, bin, root, "apply",
		"-f", "./testdata/user",
		"--cluster-config-file", "./testdata/clusters/user-dev.yaml",
		"--dry-run")
	require.Equal(t, 0, code, "apply --dry-run output:\n%s", out)
	require.Contains(t, out, "CreateScramCredential")
	require.Contains(t, out, "KafkaUser")
	require.Contains(t, out, "svc-payments (SCRAM-SHA-512)")
	require.NotContains(t, out, "legacy-analytics",
		"an entirely undeclared live user is out of scope:\n%s", out)
	require.NotContains(t, out, "DeleteScramCredential",
		"no standalone credential delete may come from a CLI diff:\n%s", out)
	require.NotContains(t, out, "approval=true",
		"no destructive-gated user op is reachable from a pure CLI diff:\n%s", out)
}

// TestAcceptanceKafkaUserDryRunIterationsUpdate: the declared mechanism exists
// live but with a different pinned iteration count -> UpdateScramCredential
// with the iterations field transition, still ungated.
func TestAcceptanceKafkaUserDryRunIterationsUpdate(t *testing.T) {
	bin, root := buildBinary(t)
	out, code := runCLI(t, bin, root, "apply",
		"-f", "./testdata/user",
		"--cluster-config-file", "./testdata/clusters/user-iterations.yaml",
		"--dry-run")
	require.Equal(t, 0, code, "apply --dry-run output:\n%s", out)
	require.Contains(t, out, "UpdateScramCredential")
	require.Contains(t, out, "[field=iterations 4096 -> 8192]")
	require.NotContains(t, out, "CreateScramCredential")
	require.NotContains(t, out, "DeleteScramCredential")
	require.NotContains(t, out, "approval=true")
}

// TestAcceptanceKafkaUserDryRunMechanismSwap: live has ONLY the other
// mechanism for the declared user -> ONE UpdateScramCredential showing the
// mechanism transition (its apply upserts the declared mechanism and drops the
// old one); never a separate gated delete op.
func TestAcceptanceKafkaUserDryRunMechanismSwap(t *testing.T) {
	bin, root := buildBinary(t)
	out, code := runCLI(t, bin, root, "apply",
		"-f", "./testdata/user",
		"--cluster-config-file", "./testdata/clusters/user-mechswap.yaml",
		"--dry-run")
	require.Equal(t, 0, code, "apply --dry-run output:\n%s", out)
	require.Contains(t, out, "UpdateScramCredential")
	require.Contains(t, out, "[field=mechanism SCRAM-SHA-256 -> SCRAM-SHA-512]")
	require.NotContains(t, out, "DeleteScramCredential",
		"the mechanism-change delete is folded into the Update, not a standalone gated op:\n%s", out)
	require.NotContains(t, out, "approval=true")
}

// TestAcceptanceKafkaUserConvergesAndRotates: with the live identity exactly
// matching the manifest, plain dry-run reports "No changes." — passwords are
// never drift. Adding --rotate-passwords to the same dry-run shows exactly the
// flag-annotated RotateScramCredential for the declared in-sync user, still
// without touching the undeclared live user.
func TestAcceptanceKafkaUserConvergesAndRotates(t *testing.T) {
	bin, root := buildBinary(t)
	out, code := runCLI(t, bin, root, "apply",
		"-f", "./testdata/user",
		"--cluster-config-file", "./testdata/clusters/user-converged.yaml",
		"--dry-run")
	require.Equal(t, 0, code, "apply --dry-run output:\n%s", out)
	require.Contains(t, out, "No changes.")

	rout, rcode := runCLI(t, bin, root, "apply",
		"-f", "./testdata/user",
		"--cluster-config-file", "./testdata/clusters/user-converged.yaml",
		"--dry-run", "--rotate-passwords")
	require.Equal(t, 0, rcode, "apply --dry-run --rotate-passwords output:\n%s", rout)
	require.Contains(t, rout, "RotateScramCredential")
	require.Contains(t, rout, "(--rotate-passwords)")
	require.Contains(t, rout, "svc-payments (SCRAM-SHA-512)")
	require.NotContains(t, rout, "legacy-analytics",
		"rotation covers declared users only:\n%s", rout)
	require.NotContains(t, rout, "approval=true")
}

// TestAcceptanceKafkaUserRealApplyCreates runs a REAL apply (mock-backed) with
// the password supplied via the environment: the credential op must succeed,
// exit 0, and the password value must never appear in any output.
func TestAcceptanceKafkaUserRealApplyCreates(t *testing.T) {
	const password = "acceptance-s3cr3t"
	t.Setenv("MONEDULA_TEST_SVC_PAYMENTS_PASSWORD", password) // inherited by the subprocess
	bin, root := buildBinary(t)
	out, code := runCLI(t, bin, root, "apply",
		"-f", "./testdata/user",
		"--cluster-config-file", "./testdata/clusters/user-dev.yaml")
	require.Equal(t, 0, code, "apply output:\n%s", out)
	require.Contains(t, out, "Succeeded CreateScramCredential")
	require.NotContains(t, out, password, "the password must never surface in output")
}

// TestAcceptanceKafkaUserRealApplyMissingPassword: a real apply whose password
// env var is unset fails the op with a source-naming error (exit 2), and the
// error names the env var — never a value.
func TestAcceptanceKafkaUserRealApplyMissingPassword(t *testing.T) {
	bin, root := buildBinary(t)
	out, code := runCLI(t, bin, root, "apply",
		"-f", "./testdata/user",
		"--cluster-config-file", "./testdata/clusters/user-dev.yaml")
	require.Equal(t, 2, code, "apply with an unresolvable password must exit 2:\n%s", out)
	require.Contains(t, out, "Failed CreateScramCredential")
	require.Contains(t, out, "MONEDULA_TEST_SVC_PAYMENTS_PASSWORD",
		"the error must name the missing source:\n%s", out)
}
