//go:build cloud

package cloude2e

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/monedula-dev/monedula-gitops/internal/api/v1alpha1"
	"github.com/monedula-dev/monedula-gitops/internal/kafka"
	"github.com/monedula-dev/monedula-gitops/internal/kafka/franz"
	"github.com/monedula-dev/monedula-gitops/internal/secrets"
)

// ---------------------------------------------------------------------------
// Cluster config + manifests
//
// SECRET HYGIENE: every generated file references credentials by env var NAME
// (valueFrom.env); no secret VALUE is ever written to disk or logged. The only
// non-secret connection parameters embedded in files are the bootstrap servers
// and the Schema Registry endpoint.
// ---------------------------------------------------------------------------

// writeClusterConfig writes the KafkaCluster cluster-config CR into dir and
// returns its path. TLS is enabled WITHOUT a caCert: Confluent Cloud presents
// a publicly-trusted certificate, and the franz wiring uses the system trust
// store when tls.caCert is nil (internal/kafka/franz/config.go).
func writeClusterConfig(t *testing.T, dir string, env cloudEnv) string {
	t.Helper()
	var b strings.Builder
	fmt.Fprintf(&b, `apiVersion: gitops.monedula.dev/v1alpha1
kind: KafkaCluster
metadata:
  name: cloud
spec:
  bootstrapServers: %s
  tls:
    enabled: true
  auth:
    mechanism: PLAIN
    scram:
      username:
        valueFrom:
          env: %s
      password:
        valueFrom:
          env: %s
`, env.bootstrap, envAPIKey, envAPISecret)
	if env.hasSR {
		fmt.Fprintf(&b, `  schemaRegistry:
    endpoint: %s
    auth:
      type: basic
      username:
        valueFrom:
          env: %s
      password:
        valueFrom:
          env: %s
`, env.srURL, envSRKey, envSRSecret)
	}
	path := filepath.Join(dir, "cluster.yaml")
	if err := os.WriteFile(path, []byte(b.String()), 0o600); err != nil {
		t.Fatalf("writing cluster config: %v", err)
	}
	return path
}

// writeManifestDir writes the given files into <root>/manifests/<stage>/ and
// returns that directory.
func writeManifestDir(t *testing.T, root, stage string, files map[string]string) string {
	t.Helper()
	dir := filepath.Join(root, "manifests", stage)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("creating manifest dir %s: %v", dir, err)
	}
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
			t.Fatalf("writing manifest %s: %v", name, err)
		}
	}
	return dir
}

// metaName converts a topic name into a manifest metadata.name (dots are not
// DNS-label safe).
func metaName(topic string) string {
	return strings.ReplaceAll(topic, ".", "-")
}

const (
	avroSchemaV1 = `{"type":"record","name":"MgccDemo","fields":[{"name":"id","type":"long"}]}`
	// v2 adds an optional (defaulted) field — a BACKWARD-compatible evolution.
	avroSchemaV2 = `{"type":"record","name":"MgccDemo","fields":[{"name":"id","type":"long"},{"name":"note","type":"string","default":""}]}`
)

// topicYAML renders the run topic manifest. Confluent Cloud fixes the
// replication factor at 3, so the manifest declares it explicitly. When
// withSchema is set, an AVRO value schema (v1) with BACKWARD compatibility is
// attached.
func topicYAML(topic, retentionMS string, withSchema bool) string {
	s := fmt.Sprintf(`apiVersion: gitops.monedula.dev/v1alpha1
kind: KafkaTopic
metadata:
  name: %s
spec:
  clusterRef:
    name: cloud
  topicName: %s
  partitions: 1
  replicationFactor: 3
  config:
    retention.ms: "%s"
`, metaName(topic), topic, retentionMS)
	if withSchema {
		s += fmt.Sprintf(`  schema:
    format: AVRO
    compatibility: BACKWARD
    valueSchema:
      valueFrom:
        inline: '%s'
`, avroSchemaV1)
	}
	return s
}

// topicYAMLSchemaV2 is topicYAML with the evolved (v2) value schema.
func topicYAMLSchemaV2(topic, retentionMS string) string {
	return strings.Replace(topicYAML(topic, retentionMS, true), avroSchemaV1, avroSchemaV2, 1)
}

// topicYAMLWithMinISR is topicYAML plus a min.insync.replicas config entry —
// the Cloud-restricted value used by the "fails well" subtest.
func topicYAMLWithMinISR(topic, retentionMS, minISR string) string {
	return topicYAML(topic, retentionMS, false) +
		fmt.Sprintf("    min.insync.replicas: \"%s\"\n", minISR)
}

// accessPolicyYAML renders a KafkaAccessPolicy granting the listed operations
// on the run topic to principal.
func accessPolicyYAML(runPrefix, topic, principal string, operations []string) string {
	var ops strings.Builder
	for _, op := range operations {
		fmt.Fprintf(&ops, "        - %s\n", op)
	}
	return fmt.Sprintf(`apiVersion: gitops.monedula.dev/v1alpha1
kind: KafkaAccessPolicy
metadata:
  name: %s-access
spec:
  clusterRef:
    name: cloud
  rules:
    - principal: "%s"
      permission: Allow
      host: "*"
      resource:
        type: topic
        name: %s
        patternType: literal
      operations:
%s`, runPrefix, principal, topic, ops.String())
}

// quotaYAML renders a KafkaQuota for the given principal (the quota-negative
// subtest expects Cloud to reject it).
func quotaYAML(runPrefix, principal string) string {
	return fmt.Sprintf(`apiVersion: gitops.monedula.dev/v1alpha1
kind: KafkaQuota
metadata:
  name: %s-quota
spec:
  clusterRef:
    name: cloud
  entity:
    user: "%s"
  limits:
    producerByteRate: 1048576
`, runPrefix, principal)
}

// userYAML renders a KafkaUser whose password comes from the harness-owned
// (non-secret) env var — Cloud has no SCRAM APIs, so the apply must fail
// before any credential material matters.
func userYAML(username string) string {
	return fmt.Sprintf(`apiVersion: gitops.monedula.dev/v1alpha1
kind: KafkaUser
metadata:
  name: %s
spec:
  clusterRef:
    name: cloud
  username: %s
  mechanism: SCRAM-SHA-512
  password:
    valueFrom:
      env: %s
`, username, username, userPasswordEnv)
}

// ---------------------------------------------------------------------------
// CLI runner
// ---------------------------------------------------------------------------

// runCLI executes the built monedula-gitops binary with the given args. The
// subprocess inherits the caller's environment (which carries the
// MONEDULA_CLOUD_* variables the cluster config references by name) plus the
// harness-owned KafkaUser password variable. Logged output is safe by
// construction: commands reference the config file, and the config file
// references env var names, never values.
func runCLI(t *testing.T, args ...string) (string, int) {
	t.Helper()
	// Per-command timeout: a hung CLI must fail THIS subtest, not trip go
	// test's -timeout, which panics the process WITHOUT running t.Cleanup —
	// leaking run-prefixed resources on the paid cluster.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, binPath, args...)
	cmd.Env = append(os.Environ(), userPasswordEnv+"="+userPasswordValue)
	out, err := cmd.CombinedOutput()
	code := 0
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			code = ee.ExitCode()
		} else {
			// Non-exit error (process could not start): treat as code 127.
			code = 127
		}
	}
	if ctx.Err() != nil {
		t.Errorf("command timed out after 5m: monedula-gitops %s", strings.Join(args, " "))
	}
	t.Logf("$ monedula-gitops %s (exit %d)\n%s", strings.Join(args, " "), code, out)
	return string(out), code
}

// noPanic asserts the CLI output carries no panic or goroutine stack trace —
// the "fails gracefully" floor for every negative subtest.
func noPanic(t *testing.T, out string) {
	t.Helper()
	if strings.Contains(out, "panic:") || strings.Contains(out, "goroutine ") {
		t.Errorf("CLI output contains a panic/stack trace — failures must be graceful")
	}
}

// firstLineContaining returns the first (trimmed) output line containing
// substr, case-insensitively. Used to capture broker error text for the docs.
func firstLineContaining(out, substr string) string {
	needle := strings.ToLower(substr)
	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(strings.ToLower(line), needle) {
			return strings.TrimSpace(line)
		}
	}
	return ""
}

// ---------------------------------------------------------------------------
// Direct admin access (out-of-band mutation + cleanup)
// ---------------------------------------------------------------------------

// newAdminClient builds the repo's own franz-go admin client from the same
// connection parameters the CLI uses (PLAIN over TLS with system roots,
// credentials resolved from env by name). Used for the out-of-band drift
// mutation and for prefix-scoped cleanup.
func newAdminClient(t *testing.T, env cloudEnv) *franz.Client {
	t.Helper()
	c := &v1alpha1.KafkaCluster{
		Spec: v1alpha1.KafkaClusterSpec{
			BootstrapServers: env.bootstrap,
			TLS:              &v1alpha1.TLSConfig{Enabled: true},
			Auth: &v1alpha1.AuthConfig{
				Mechanism: "PLAIN",
				SCRAM: &v1alpha1.SCRAMAuth{
					Username: v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{Env: envAPIKey}},
					Password: v1alpha1.ValueFrom{ValueFrom: v1alpha1.ValueSource{Env: envAPISecret}},
				},
			},
		},
	}
	cl, err := franz.New(c, secrets.FileEnvResolver{})
	if err != nil {
		t.Fatalf("building direct admin client: %v", err)
	}
	t.Cleanup(cl.Close)
	return cl
}

// adminCtx returns a bounded context for direct admin calls.
func adminCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 60*time.Second)
}

// cleanupTopic registers deletion of the run topic. Defense-in-depth: it
// refuses to delete anything not carrying the run prefix.
func cleanupTopic(t *testing.T, admin *franz.Client, runPrefix, topic string) {
	t.Cleanup(func() {
		if !strings.HasPrefix(topic, runPrefix) {
			t.Logf("cleanup: refusing to delete topic %q (missing run prefix %q)", topic, runPrefix)
			return
		}
		ctx, cancel := adminCtx()
		defer cancel()
		if err := admin.DeleteTopic(ctx, topic); err != nil {
			t.Logf("cleanup: deleting topic %s: %v (it may already be gone)", topic, err)
			return
		}
		t.Logf("cleanup: deleted topic %s", topic)
	})
}

// cleanupACLs registers deletion of every ACL whose resource name carries the
// run prefix — and only those.
func cleanupACLs(t *testing.T, admin *franz.Client, runPrefix string) {
	t.Cleanup(func() {
		ctx, cancel := adminCtx()
		defer cancel()
		acls, err := admin.ListACLs(ctx)
		if err != nil {
			t.Logf("cleanup: listing ACLs: %v", err)
			return
		}
		var mine []kafka.ACLState
		for _, a := range acls {
			if strings.HasPrefix(a.ResourceName, runPrefix) {
				mine = append(mine, a)
			}
		}
		if len(mine) == 0 {
			return
		}
		if err := admin.DeleteACLs(ctx, mine); err != nil {
			t.Logf("cleanup: deleting %d run-prefixed ACL(s): %v", len(mine), err)
			return
		}
		t.Logf("cleanup: deleted %d run-prefixed ACL(s)", len(mine))
	})
}

// cleanupQuota registers best-effort removal of the run quota. Only reachable
// when Cloud unexpectedly ACCEPTED a client quota (the subtest expects
// rejection). Defense-in-depth: it refuses to touch quota config on any
// entity not carrying the run prefix, so it can never strip a limit off a
// principal the user actually owns.
func cleanupQuota(t *testing.T, admin *franz.Client, runPrefix, principal string) {
	t.Cleanup(func() {
		user := strings.TrimPrefix(principal, "User:")
		if !strings.HasPrefix(user, runPrefix) {
			t.Logf("cleanup: refusing to remove quota for %q (missing run prefix %q)", principal, runPrefix)
			return
		}
		ctx, cancel := adminCtx()
		defer cancel()
		entity := []kafka.QuotaEntityComponent{{Type: "user", Name: &user}}
		if err := admin.DeleteQuota(ctx, entity, []string{"producer_byte_rate"}); err != nil {
			t.Logf("cleanup: removing quota for %s: %v", principal, err)
			return
		}
		t.Logf("cleanup: removed quota for %s", principal)
	})
}

// cleanupScram registers best-effort removal of the run SCRAM credential.
// Only reachable when Cloud unexpectedly ACCEPTED a SCRAM upsert.
func cleanupScram(t *testing.T, admin *franz.Client, username string) {
	t.Cleanup(func() {
		ctx, cancel := adminCtx()
		defer cancel()
		if err := admin.DeleteScramCredential(ctx, username, "SCRAM-SHA-512"); err != nil {
			t.Logf("cleanup: removing SCRAM credential %s: %v", username, err)
			return
		}
		t.Logf("cleanup: removed SCRAM credential %s", username)
	})
}

// ---------------------------------------------------------------------------
// Schema Registry REST (verification + subject cleanup)
// ---------------------------------------------------------------------------

// srRequest performs one Schema Registry REST call with basic auth resolved
// in-process from the environment. Credentials are never logged; only status
// and body (SR metadata, no secrets) are returned.
func srRequest(env cloudEnv, method, path string) (int, string, error) {
	req, err := http.NewRequest(method, strings.TrimRight(env.srURL, "/")+path, nil)
	if err != nil {
		return 0, "", err
	}
	req.SetBasicAuth(os.Getenv(envSRKey), os.Getenv(envSRSecret))
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, "", err
	}
	return resp.StatusCode, string(body), nil
}

// srSubjectVersions returns the version list of a subject.
func srSubjectVersions(t *testing.T, env cloudEnv, subject string) []int {
	t.Helper()
	status, body, err := srRequest(env, http.MethodGet, "/subjects/"+url.PathEscape(subject)+"/versions")
	if err != nil || status != http.StatusOK {
		t.Fatalf("listing versions of subject %s: HTTP %d, err=%v, body=%s", subject, status, err, body)
	}
	var versions []int
	if err := json.Unmarshal([]byte(body), &versions); err != nil {
		t.Fatalf("parsing versions of subject %s: %v (body=%s)", subject, err, body)
	}
	return versions
}

// cleanupSubject registers a soft delete of the run subject. Defense-in-depth:
// refuses to delete a subject not carrying the run prefix.
func cleanupSubject(t *testing.T, env cloudEnv, runPrefix, subject string) {
	t.Cleanup(func() {
		if !strings.HasPrefix(subject, runPrefix) {
			t.Logf("cleanup: refusing to delete subject %q (missing run prefix %q)", subject, runPrefix)
			return
		}
		status, _, err := srRequest(env, http.MethodDelete, "/subjects/"+url.PathEscape(subject))
		if err != nil {
			t.Logf("cleanup: soft-deleting subject %s: %v", subject, err)
			return
		}
		t.Logf("cleanup: soft-deleted subject %s (HTTP %d)", subject, status)
	})
}

// ---------------------------------------------------------------------------
// Import output assertions
// ---------------------------------------------------------------------------

// assertImportOutput checks the emitted manifest tree: the run topic must be
// present, internal/housekeeping topics must be absent (default import
// behavior), and no file may contain a secret value. Secret values are
// compared in-process and never logged.
func assertImportOutput(t *testing.T, outDir, topic string) {
	t.Helper()
	var secretValues []string
	for _, name := range []string{envAPIKey, envAPISecret, envSRKey, envSRSecret} {
		if v := os.Getenv(name); v != "" {
			secretValues = append(secretValues, v)
		}
	}
	foundTopic := false
	err := filepath.WalkDir(outDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		data, rerr := os.ReadFile(path)
		if rerr != nil {
			return rerr
		}
		content := string(data)
		if strings.Contains(content, "topicName: "+topic) {
			foundTopic = true
		}
		for _, marker := range []string{"topicName: __", "topicName: _confluent", "topicName: _schemas"} {
			if strings.Contains(content, marker) {
				t.Errorf("import emitted an internal topic manifest (%q in %s) — internal topics must be skipped by default", marker, path)
			}
		}
		for _, sv := range secretValues {
			if strings.Contains(content, sv) {
				t.Errorf("import output file %s contains a secret value (value redacted)", path)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking import output dir: %v", err)
	}
	if !foundTopic {
		t.Errorf("run topic %s not found in the import output under %s", topic, outDir)
	}
}

// ---------------------------------------------------------------------------
// Summary table
// ---------------------------------------------------------------------------

// summaryRow is one line of the final verdict table — the artifact a
// maintainer pastes into the support-matrix update.
type summaryRow struct {
	surface, verdict, note string
}

// summaryTable renders the fixed-width verdict table.
func summaryTable(rows []summaryRow) string {
	var b strings.Builder
	b.WriteString("==== Confluent Cloud validation summary ====\n")
	fmt.Fprintf(&b, "%-9s %-14s %s\n", "SURFACE", "VERDICT", "NOTE")
	for _, r := range rows {
		fmt.Fprintf(&b, "%-9s %-14s %s\n", r.surface, r.verdict, r.note)
	}
	return b.String()
}

// topicRow folds the three topic subtests (apply/verify, restricted config,
// drift) into the Topics verdict. topicSkip is the skip reason recorded when
// 02_topic_apply never ran ("" = it ran); 03/04 skip only as a consequence of
// 02 failing, so their outcomes matter only when the topic subtest ran.
func topicRow(topicSkip string, topicOK, restrictionOK, driftOK bool, notes map[string]string) summaryRow {
	if topicSkip != "" {
		return summaryRow{"Topics", "SKIPPED", topicSkip}
	}
	if !topicOK || !restrictionOK || !driftOK {
		return summaryRow{"Topics", "FAILED", "see subtest logs (02_topic_apply / 03_topic_config_restriction / 04_drift)"}
	}
	note := "create/verify/drift-reconcile validated"
	if n := notes["restriction"]; n != "" {
		note += "; " + n
	}
	return summaryRow{"Topics", "OK", note}
}

// verdictRow maps a positive subtest outcome to OK / SKIPPED / FAILED.
// skipReason is the ACTUAL cause recorded at the skip site ("" = the subtest
// ran); ok comes from the explicit outcome boolean, never t.Run's return.
func verdictRow(surface string, ok bool, skipReason, okNote string) summaryRow {
	switch {
	case skipReason != "":
		return summaryRow{surface, "SKIPPED", skipReason}
	case ok:
		return summaryRow{surface, "OK", okNote}
	default:
		return summaryRow{surface, "FAILED", "see subtest logs"}
	}
}

// negativeRow maps an expected-rejection subtest outcome to FAILS-CLEANLY /
// SKIPPED / FAILED. Same contract as verdictRow.
func negativeRow(surface string, ok bool, skipReason, note string) summaryRow {
	switch {
	case skipReason != "":
		return summaryRow{surface, "SKIPPED", skipReason}
	case ok:
		return summaryRow{surface, "FAILS-CLEANLY", note}
	default:
		return summaryRow{surface, "FAILED", strings.TrimSpace("expected graceful rejection — see subtest logs. " + note)}
	}
}
