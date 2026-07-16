//go:build cloud

// Package cloude2e is the opt-in Confluent Cloud validation harness.
//
// The docs support matrix (docs/connecting.md) marks Confluent Cloud as
// "untested". This suite is the credential-gated test a maintainer runs
// against a REAL Confluent Cloud cluster to turn that claim into evidence:
// it exercises what should work there (topics, ACLs, schemas, import) and
// proves that what Cloud does not expose (client quotas and SCRAM users via
// the Kafka Admin API) fails gracefully — non-zero exit, broker error text,
// no panic.
//
// It runs ONLY when explicitly invoked with `-tags cloud` AND configured via
// MONEDULA_CLOUD_* environment variables (see README.md in this directory).
// It never runs in CI and never in the normal `go test ./...` suite. With no
// MONEDULA_CLOUD_* variables set the suite SKIPS with guidance; with a
// half-configured environment (some but not all of the core trio) it FAILS,
// so an explicit run can never silently do nothing.
//
// Every resource the suite creates carries a unique run prefix
// (mgcc<unix-timestamp>), and cleanup — registered immediately after each
// resource is created — deletes only resources carrying that prefix.
package cloude2e

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// binPath holds the compiled monedula-gitops binary path (set by TestMain).
var binPath string

// repoRoot is the absolute path to the repository root, resolved from this
// file's location.
var repoRoot string

// Environment contract. All variables are prefixed MONEDULA_CLOUD_; the
// core trio is required, the SR trio and the service account are optional
// (their subtests skip with a note when unset).
const (
	envBootstrap      = "MONEDULA_CLOUD_BOOTSTRAP"          // host:9092
	envAPIKey         = "MONEDULA_CLOUD_API_KEY"            // Kafka cluster API key
	envAPISecret      = "MONEDULA_CLOUD_API_SECRET"         // Kafka cluster API secret
	envSRURL          = "MONEDULA_CLOUD_SR_URL"             // Schema Registry endpoint (https://...)
	envSRKey          = "MONEDULA_CLOUD_SR_KEY"             // Schema Registry API key
	envSRSecret       = "MONEDULA_CLOUD_SR_SECRET"          // Schema Registry API secret
	envServiceAccount = "MONEDULA_CLOUD_SERVICE_ACCOUNT_ID" // e.g. sa-abc123 (ACL subtest)

	cloudEnvPrefix = "MONEDULA_CLOUD_"
)

// userPasswordEnv is set BY THE HARNESS ITSELF (a harmless literal, not a
// secret) so the KafkaUser negative manifest has a CLI-legal password source
// (valueFrom.env). It deliberately does not carry the MONEDULA_CLOUD_ prefix
// so it can never influence env gating.
const (
	userPasswordEnv   = "MGCC_CLOUD_USER_PASSWORD"
	userPasswordValue = "mgcc-cloud-user-negative-test"
)

// cloudEnv carries the NON-SECRET connection parameters. Secrets (API
// secrets) are never copied out of the environment by the harness: the CLI
// subprocess and the in-process admin client resolve them by env var NAME.
type cloudEnv struct {
	bootstrap      string
	srURL          string // "" when SR is not configured
	serviceAccount string // "" when the ACL subtest should skip
	hasSR          bool
}

// anyCloudEnvSet reports whether any MONEDULA_CLOUD_* variable is set to a
// non-empty value.
func anyCloudEnvSet() bool {
	for _, kv := range os.Environ() {
		if strings.HasPrefix(kv, cloudEnvPrefix) {
			if i := strings.IndexByte(kv, '='); i >= 0 && kv[i+1:] != "" {
				return true
			}
		}
	}
	return false
}

// loadEnv gates the suite on the environment contract:
//   - NO MONEDULA_CLOUD_* variables at all -> Skip with guidance;
//   - some set but the core trio incomplete -> Fatal listing what is missing
//     (an explicit half-configured run must not silently pass);
//   - SR trio partially set -> Fatal (same reasoning);
//   - otherwise returns the resolved parameters.
func loadEnv(t *testing.T) cloudEnv {
	t.Helper()
	if !anyCloudEnvSet() {
		t.Skipf("Confluent Cloud validation harness skipped: no %s* environment variables are set.\n"+
			"Required: %s, %s, %s.\n"+
			"Optional: %s + %s + %s (Schema Registry subtests), %s (ACL subtest).\n"+
			"See test/e2e/cloud/README.md for setup instructions.",
			cloudEnvPrefix, envBootstrap, envAPIKey, envAPISecret,
			envSRURL, envSRKey, envSRSecret, envServiceAccount)
	}

	var missing []string
	for _, name := range []string{envBootstrap, envAPIKey, envAPISecret} {
		if os.Getenv(name) == "" {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		t.Fatalf("Confluent Cloud validation harness is half-configured: some %s* variables are set "+
			"but the core connection variables are incomplete.\nMissing: %s.\n"+
			"Set them (or unset all %s* variables to skip). See test/e2e/cloud/README.md.",
			cloudEnvPrefix, strings.Join(missing, ", "), cloudEnvPrefix)
	}

	srSet := 0
	var srMissing []string
	for _, name := range []string{envSRURL, envSRKey, envSRSecret} {
		if os.Getenv(name) != "" {
			srSet++
		} else {
			srMissing = append(srMissing, name)
		}
	}
	if srSet > 0 && srSet < 3 {
		t.Fatalf("Schema Registry env is half-configured: missing %s. "+
			"Set all of %s, %s, %s — or none of them to skip the SR subtests.",
			strings.Join(srMissing, ", "), envSRURL, envSRKey, envSRSecret)
	}

	return cloudEnv{
		bootstrap:      os.Getenv(envBootstrap),
		srURL:          os.Getenv(envSRURL),
		serviceAccount: os.Getenv(envServiceAccount),
		hasSR:          srSet == 3,
	}
}

func TestMain(m *testing.M) {
	// Resolve the repository root: this file lives at
	// test/e2e/cloud/cloud_test.go, so the root is three directories up.
	_, thisFile, _, _ := runtime.Caller(0)
	repoRoot = filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", "..", ".."))

	// Without any cloud env the only outcome is TestCloud's skip-with-guidance;
	// don't spend time building the binary for that.
	if !anyCloudEnvSet() {
		os.Exit(m.Run())
	}

	tmpDir, err := os.MkdirTemp("", "monedula-cloud-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create temp dir: %v\n", err)
		os.Exit(1)
	}
	binPath = filepath.Join(tmpDir, "monedula-gitops")
	cmd := exec.Command("go", "build", "-o", binPath, "./cmd/monedula-gitops")
	cmd.Dir = repoRoot
	if out, err := cmd.CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "build failed: %v\n%s\n", err, out)
		os.RemoveAll(tmpDir)
		os.Exit(1)
	}

	code := m.Run()
	os.RemoveAll(tmpDir)
	os.Exit(code)
}

// TestCloud is the single sequential validation run. Subtests share one run
// prefix and one cluster config; each logs a one-line verdict, and the final
// summary table (the artifact for the support-matrix update) is logged at the
// end. Later subtests proceed unless their dependency failed.
func TestCloud(t *testing.T) {
	env := loadEnv(t)

	runPrefix := fmt.Sprintf("mgcc%d", time.Now().Unix())
	topic := runPrefix + ".demo"
	subject := topic + "-value"
	t.Logf("run prefix: %s (all created resources carry it; cleanup deletes only %s*)", runPrefix, runPrefix)

	workDir := t.TempDir()
	cfgPath := writeClusterConfig(t, workDir, env)
	admin := newAdminClient(t, env)

	notes := map[string]string{}

	// Explicit outcome booleans — deliberately NOT t.Run's return value, which
	// reports "did not fail" and is therefore TRUE for a SKIPPED subtest.
	// Gating on t.Run's return would let dependents run after their dependency
	// was skipped (e.g. doctor fails -> 02 skips -> 06 would create the topic
	// with no cleanup registered). Each subtest instead sets its flag as its
	// final statement via !st.Failed(): a skip or an early Fatal leaves it
	// false. These booleans gate later subtests AND drive the verdict table.
	var doctorOK, topicOK, restrictionOK, driftOK, aclOK, schemaOK, quotaOK, userOK, importOK bool
	// skipped maps a subtest key to the REASON it was skipped ("" = it ran).
	// The verdict table prints this actual cause — a dependency failure must
	// never be reported as "env var not set".
	skipped := map[string]string{}

	// --- 1. doctor -------------------------------------------------------
	t.Run("01_doctor", func(st *testing.T) {
		out, code := runCLI(st, "doctor", "-c", cfgPath)
		noPanic(st, out)
		if code != 0 {
			st.Fatalf("verdict: doctor FAILED (exit %d) — broker connectivity not established", code)
		}
		if !strings.Contains(out, "PASS kafka-admin") || !strings.Contains(out, "Doctor: healthy") {
			st.Fatalf("verdict: doctor output missing PASS kafka-admin / Doctor: healthy")
		}
		st.Logf("verdict: doctor OK — broker connectivity and admin reads confirmed")
		doctorOK = !st.Failed()
	})

	// --- 2. topic apply + verify -----------------------------------------
	topicDir := writeManifestDir(t, workDir, "topic", map[string]string{
		"topic.yaml": topicYAML(topic, "86400000", false),
	})
	t.Run("02_topic_apply", func(st *testing.T) {
		if !doctorOK {
			skipped["topic"] = "dependency failed: doctor"
			st.Skip("skipped: doctor failed")
		}
		out, code := runCLI(st, "apply", "-f", topicDir, "-c", cfgPath)
		// Cleanup registered on the PARENT test immediately after the create
		// attempt (the topic may exist even when apply reports a failure), so
		// it runs at the very end of the run, after all dependent subtests.
		cleanupTopic(t, admin, runPrefix, topic)
		noPanic(st, out)
		if code != 0 {
			st.Fatalf("verdict: topic apply FAILED (exit %d)", code)
		}
		if !strings.Contains(out, "Succeeded CreateTopic") {
			st.Fatalf("verdict: topic apply FAILED — no 'Succeeded CreateTopic' in output")
		}
		vout, vcode := runCLI(st, "verify", "-f", topicDir, "-c", cfgPath)
		if vcode != 0 || !strings.Contains(vout, "No changes.") {
			st.Fatalf("verdict: topic verify FAILED (exit %d) — expected 'No changes.'", vcode)
		}
		st.Logf("verdict: topic create + verify OK (partitions=1, replicationFactor=3, retention.ms)")
		topicOK = !st.Failed()
	})

	// --- 3. Cloud-restricted topic config --------------------------------
	// Confluent Cloud enforces a broker-side topic policy: min.insync.replicas
	// may only be 1 or 2 (replication factor is fixed at 3 and Cloud rejects
	// min.insync.replicas values it considers unsafe/unsupported). Asking for
	// 3 must be rejected BY THE BROKER as a policy violation — the CLI's job
	// is to surface that as a per-operation failure with the broker's message,
	// a non-zero exit, and no panic. This is a "fails well" check, not a bug.
	restrictedDir := writeManifestDir(t, workDir, "restricted", map[string]string{
		"topic.yaml": topicYAMLWithMinISR(topic, "86400000", "3"),
	})
	t.Run("03_topic_config_restriction", func(st *testing.T) {
		if !topicOK {
			skipped["restriction"] = "dependency failed: topic apply"
			st.Skip("skipped: topic apply failed or skipped")
		}
		out, code := runCLI(st, "apply", "-f", restrictedDir, "-c", cfgPath)
		noPanic(st, out)
		if code == 0 {
			notes["restriction"] = "UNEXPECTED: Cloud accepted min.insync.replicas=3"
			st.Fatalf("verdict: expected Cloud to reject min.insync.replicas=3 with a policy error, but apply succeeded")
		}
		if !strings.Contains(out, "Failed") {
			st.Fatalf("verdict: expected a per-operation 'Failed' status in the apply output (exit was %d)", code)
		}
		if !strings.Contains(out, "min.insync.replicas") {
			st.Errorf("verdict: failed operation does not name min.insync.replicas")
		}
		notes["restriction"] = "restricted config (min.insync.replicas=3) rejected by broker policy: " +
			firstLineContaining(out, "Failed")
		st.Logf("verdict: restricted config change FAILS-CLEANLY — broker policy error surfaced, exit %d, no panic", code)
		restrictionOK = !st.Failed()
	})

	// --- 4. drift detect + reconcile --------------------------------------
	t.Run("04_drift", func(st *testing.T) {
		if !topicOK {
			skipped["drift"] = "dependency failed: topic apply"
			st.Skip("skipped: topic apply failed or skipped")
		}
		// Out-of-band change through the repo's own franz-go admin client
		// (same credentials, no CLI): lower retention.ms directly.
		ctx, cancel := adminCtx()
		defer cancel()
		if err := admin.UpdateTopicConfig(ctx, topic, map[string]string{"retention.ms": "43200000"}); err != nil {
			st.Fatalf("out-of-band retention.ms alter failed: %v", err)
		}
		out, code := runCLI(st, "verify", "-f", topicDir, "-c", cfgPath)
		if code != 1 || !strings.Contains(out, "retention.ms") {
			st.Fatalf("verdict: drift detection FAILED — want exit 1 mentioning retention.ms, got exit %d", code)
		}
		aout, acode := runCLI(st, "apply", "-f", topicDir, "-c", cfgPath)
		if acode != 0 || !strings.Contains(aout, "Succeeded UpdateTopicConfig") {
			st.Fatalf("verdict: drift reconciliation FAILED (exit %d)", acode)
		}
		vout, vcode := runCLI(st, "verify", "-f", topicDir, "-c", cfgPath)
		if vcode != 0 || !strings.Contains(vout, "No changes.") {
			st.Fatalf("verdict: post-reconcile verify FAILED (exit %d)", vcode)
		}
		st.Logf("verdict: drift detect + reconcile OK (retention.ms altered out-of-band, detected, reconciled)")
		driftOK = !st.Failed()
	})

	// --- 5. ACLs (needs a service account id) -----------------------------
	t.Run("05_acl", func(st *testing.T) {
		if !doctorOK {
			skipped["acl"] = "dependency failed: doctor"
			st.Skip("skipped: doctor failed")
		}
		if env.serviceAccount == "" {
			skipped["acl"] = envServiceAccount + " not set"
			st.Skipf("%s not set — ACL subtest skipped (create a service account and set it to enable)", envServiceAccount)
		}
		// Confluent Cloud stores Kafka-protocol ACLs under the service
		// account's NUMERIC id (e.g. 9207877), not the sa- resource id:
		// CreateAcls with a User:sa-... principal is acknowledged and then
		// silently dropped, and DescribeAcls returns the numeric form — so an
		// sa- id can neither persist nor round-trip. Validated 2026-07-09.
		if strings.HasPrefix(env.serviceAccount, "sa-") {
			st.Logf("WARNING: %s=%q is a resource id; Confluent Cloud silently drops protocol ACL creates for sa- principals — use the NUMERIC service-account id (see README)", envServiceAccount, env.serviceAccount)
		}
		principal := "User:" + env.serviceAccount
		aclDirA := writeManifestDir(st, workDir, "acl-a", map[string]string{
			"access.yaml": accessPolicyYAML(runPrefix, topic, principal, []string{"Read", "Describe"}),
		})
		out, code := runCLI(st, "apply", "-f", aclDirA, "-c", cfgPath)
		cleanupACLs(t, admin, runPrefix)
		noPanic(st, out)
		if code != 0 || !strings.Contains(out, "Succeeded CreateAcl") {
			st.Fatalf("verdict: ACL apply FAILED (exit %d)", code)
		}
		vout, vcode := runCLI(st, "verify", "-f", aclDirA, "-c", cfgPath)
		if vcode != 0 || !strings.Contains(vout, "No changes.") {
			st.Fatalf("verdict: ACL verify FAILED (exit %d) — expected 'No changes.'", vcode)
		}
		// Remove one operation and prune it: (topic, principal) stays in the
		// managed scope, so the now-undesired Describe ACL is a prune
		// candidate that --prune deletes.
		aclDirB := writeManifestDir(st, workDir, "acl-b", map[string]string{
			"access.yaml": accessPolicyYAML(runPrefix, topic, principal, []string{"Read"}),
		})
		pout, pcode := runCLI(st, "apply", "-f", aclDirB, "-c", cfgPath, "--prune")
		if pcode != 0 || !strings.Contains(pout, "Succeeded DeleteAcl") {
			st.Fatalf("verdict: ACL prune FAILED (exit %d) — expected 'Succeeded DeleteAcl'", pcode)
		}
		// Confirm removal against the raw admin API, not just CLI output.
		ctx, cancel := adminCtx()
		defer cancel()
		acls, err := admin.ListACLs(ctx)
		if err != nil {
			st.Fatalf("listing ACLs after prune: %v", err)
		}
		for _, a := range acls {
			if a.ResourceName == topic && a.Principal == principal && a.Operation == "Describe" {
				st.Fatalf("verdict: pruned Describe ACL still present on the broker")
			}
		}
		vout2, vcode2 := runCLI(st, "verify", "-f", aclDirB, "-c", cfgPath)
		if vcode2 != 0 {
			st.Fatalf("verdict: post-prune verify FAILED (exit %d)\n%s", vcode2, vout2)
		}
		st.Logf("verdict: ACL grant + no-diff + prune OK for %s", principal)
		aclOK = !st.Failed()
	})

	// --- 6. schema (needs SR env) ------------------------------------------
	t.Run("06_schema", func(st *testing.T) {
		if !topicOK {
			skipped["schema"] = "dependency failed: topic apply"
			st.Skip("skipped: topic apply failed or skipped")
		}
		if !env.hasSR {
			skipped["schema"] = envSRURL + " (+key/secret) not set"
			st.Skipf("%s/%s/%s not set — Schema Registry subtest skipped", envSRURL, envSRKey, envSRSecret)
		}
		schemaDir := writeManifestDir(st, workDir, "schema-v1", map[string]string{
			"topic.yaml": topicYAML(topic, "86400000", true),
		})
		out, code := runCLI(st, "apply", "-f", schemaDir, "-c", cfgPath)
		cleanupSubject(t, env, runPrefix, subject)
		noPanic(st, out)
		if code != 0 || !strings.Contains(out, "RegisterSchema") {
			st.Fatalf("verdict: schema register FAILED (exit %d)", code)
		}
		vout, vcode := runCLI(st, "verify", "-f", schemaDir, "-c", cfgPath)
		if vcode != 0 || !strings.Contains(vout, "No changes.") {
			st.Fatalf("verdict: schema verify FAILED (exit %d)", vcode)
		}
		// Compatible evolution: add an optional (defaulted) field -> version 2.
		schemaDirV2 := writeManifestDir(st, workDir, "schema-v2", map[string]string{
			"topic.yaml": topicYAMLSchemaV2(topic, "86400000"),
		})
		out2, code2 := runCLI(st, "apply", "-f", schemaDirV2, "-c", cfgPath)
		if code2 != 0 || !strings.Contains(out2, "RegisterSchema") {
			st.Fatalf("verdict: schema evolution FAILED (exit %d)", code2)
		}
		versions := srSubjectVersions(st, env, subject)
		if len(versions) != 2 || versions[len(versions)-1] != 2 {
			st.Fatalf("verdict: expected subject %s at versions [1 2], got %v", subject, versions)
		}
		st.Logf("verdict: schema register + compatible evolution OK (subject %s at version 2)", subject)
		schemaOK = !st.Failed()
	})

	// --- 7. quota negative --------------------------------------------------
	// Confluent Cloud does not expose client-quota management through the
	// Kafka Admin API (quotas are managed by Cloud itself); the point here is
	// that the failure is graceful and names the quota entity. With live reads
	// scoped to the declared kinds, a KafkaQuota manifest triggers the quota
	// READ (ListQuotas -> DescribeClientQuotas), which Cloud rejects with
	// CLUSTER_AUTHORIZATION_FAILED before any write is attempted — so the
	// apply may fail at read time or (should Cloud ever permit the describe)
	// at write time. Both shapes satisfy the assertions below: non-zero exit,
	// no panic, and an output line mentioning the quota.
	t.Run("07_quota_negative", func(st *testing.T) {
		if !doctorOK {
			skipped["quota"] = "dependency failed: doctor"
			st.Skip("skipped: doctor failed")
		}
		// A synthetic run-prefixed principal, NEVER the real service account:
		// this is a rejection probe, and if Cloud ever accepted the quota,
		// cleanup must not touch quota config on a principal the user owns.
		principal := "User:" + runPrefix + "-principal"
		quotaDir := writeManifestDir(st, workDir, "quota", map[string]string{
			"quota.yaml": quotaYAML(runPrefix, principal),
		})
		out, code := runCLI(st, "apply", "-f", quotaDir, "-c", cfgPath)
		noPanic(st, out)
		if code == 0 {
			// Unexpected acceptance: clean the quota up and fail the check.
			cleanupQuota(t, admin, runPrefix, principal)
			notes["quota"] = "UNEXPECTED: Cloud accepted a client quota via the Kafka Admin API"
			st.Fatalf("verdict: expected Cloud to reject the client quota, but apply succeeded")
		}
		if !strings.Contains(strings.ToLower(out), "quota") {
			st.Errorf("verdict: quota failure output does not mention the quota operation/entity")
		}
		errLine := firstLineContaining(out, "quota")
		if errLine == "" {
			errLine = "rejected with no quota-mentioning output line — see subtest logs"
		}
		notes["quota"] = "broker error: " + errLine
		st.Logf("verdict: quota apply FAILS-CLEANLY (exit %d, no panic).\nExact broker error (for docs): %s", code, errLine)
		quotaOK = !st.Failed()
	})

	// --- 8. user negative ----------------------------------------------------
	// Confluent Cloud has no SCRAM credential APIs (API keys replace SCRAM
	// users); creating a KafkaUser must fail gracefully.
	t.Run("08_user_negative", func(st *testing.T) {
		if !doctorOK {
			skipped["user"] = "dependency failed: doctor"
			st.Skip("skipped: doctor failed")
		}
		username := runPrefix + "-user"
		userDir := writeManifestDir(st, workDir, "user", map[string]string{
			"user.yaml": userYAML(username),
		})
		out, code := runCLI(st, "apply", "-f", userDir, "-c", cfgPath)
		noPanic(st, out)
		if code == 0 {
			cleanupScram(t, admin, username)
			notes["user"] = "UNEXPECTED: Cloud accepted a SCRAM credential upsert"
			st.Fatalf("verdict: expected Cloud to reject the SCRAM user, but apply succeeded")
		}
		errLine := firstLineContaining(out, "cram")
		if errLine == "" {
			errLine = firstLineContaining(out, "Error")
		}
		if errLine == "" {
			errLine = "rejected with no SCRAM/Error output line — see subtest logs"
		}
		notes["user"] = "broker error: " + errLine
		st.Logf("verdict: SCRAM user apply FAILS-CLEANLY (exit %d, no panic).\nExact broker error (for docs): %s", code, errLine)
		userOK = !st.Failed()
	})

	// --- 9. import -------------------------------------------------------------
	t.Run("09_import", func(st *testing.T) {
		if !topicOK {
			skipped["import"] = "dependency failed: topic apply"
			st.Skip("skipped: topic apply failed or skipped")
		}
		outDir := st.TempDir()
		args := []string{"import", "cluster", "-c", cfgPath, "--output-dir", outDir}

		// Escalating fallback ladder: Cloud is expected to reject SCRAM
		// credential listing AND (as 07 demonstrates) client-quota describes,
		// so import must remain usable by skipping those reads one at a time:
		// plain -> --skip-users -> --skip-users --skip-quotas. Each failed
		// rung's output is kept so importNote can attribute honestly — a
		// skipped listing is only blamed when the failure actually names it.
		modes := []struct {
			name  string
			flags []string
		}{
			{"no extra flags", nil},
			{"--skip-users", []string{"--skip-users"}},
			{"--skip-users --skip-quotas", []string{"--skip-users", "--skip-quotas"}},
		}
		usedMode := -1
		var failures []string
		var code int
		for i, m := range modes {
			var out string
			out, code = runCLI(st, append(append([]string(nil), args...), m.flags...)...)
			noPanic(st, out)
			if code == 0 {
				usedMode = i
				break
			}
			failures = append(failures, out)
			if i+1 < len(modes) {
				st.Logf("import with %s failed (exit %d) — retrying with %s", m.name, code, modes[i+1].name)
			}
		}
		if usedMode < 0 {
			st.Fatalf("verdict: import FAILED even with --skip-users --skip-quotas (exit %d)", code)
		}
		assertImportOutput(st, outDir, topic)
		notes["import"] = importNote(usedMode, failures)
		st.Logf("verdict: import OK — run topic present, internal topics absent, no secret values emitted (mode: %s)", modes[usedMode].name)
		importOK = !st.Failed()
	})

	// --- 10. summary -----------------------------------------------------------
	// Rows derive ONLY from the explicit outcome booleans and skip reasons —
	// never from t.Run return values — so a subtest that never ran can never
	// be reported OK.
	t.Log("\n" + summaryTable([]summaryRow{
		topicRow(skipped["topic"], topicOK, restrictionOK, driftOK, notes),
		verdictRow("ACLs", aclOK, skipped["acl"],
			"grant/no-diff/prune validated against a service-account principal"),
		verdictRow("Schemas", schemaOK, skipped["schema"],
			"register + compatible evolution to version 2 validated"),
		negativeRow("Quotas", quotaOK, skipped["quota"], notes["quota"]),
		negativeRow("Users", userOK, skipped["user"], notes["user"]),
		verdictRow("Import", importOK, skipped["import"], notes["import"]),
	}))
}

// importNote renders notes["import"] for the escalating 09_import ladder.
// usedMode is the index of the mode that succeeded (0 = no flags, 1 =
// --skip-users, 2 = --skip-users --skip-quotas); failures holds the raw output
// of each preceding failed attempt. Attribution honesty: a skipped listing is
// blamed only when the failure output that made us skip it actually mentions
// it (SCRAM/credential for --skip-users, quota for --skip-quotas); otherwise
// the real error line is recorded instead.
func importNote(usedMode int, failures []string) string {
	if usedMode == 0 {
		return "import cluster works without extra flags"
	}
	blame := func(out, listing string, keywords ...string) string {
		lower := strings.ToLower(out)
		for _, kw := range keywords {
			if strings.Contains(lower, kw) {
				return listing
			}
		}
		reason := firstLineContaining(out, "Error")
		if reason == "" {
			reason = "see subtest logs"
		}
		return "prior attempt failed with: " + reason
	}
	flags := "--skip-users"
	parts := []string{blame(failures[0], "Cloud rejects SCRAM credential listing", "scram", "credential")}
	if usedMode >= 2 {
		flags = "--skip-users --skip-quotas"
		parts = append(parts, blame(failures[1], "Cloud rejects client-quota describes", "quota"))
	}
	return "import cluster works with " + flags + " (" + strings.Join(parts, "; ") + ")"
}
