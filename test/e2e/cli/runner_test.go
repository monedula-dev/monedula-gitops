//go:build e2e

// Package clie2e is the CLI scenario orchestrator for the Monedula GitOps
// scenarios/ suite. It brings up Docker Compose cluster-profile stacks,
// runs each cli-mode seed scenario's documented command against the real
// broker, asserts exit code and output (via internal/e2e), checks live state
// (via `monedula-gitops e2e check`), and tears down the stack afterwards.
//
// The suite FAILS when Docker is absent (TestMain exits non-zero), so an
// explicit `-tags e2e` run can't silently do nothing and still report `ok`;
// set MONEDULA_E2E_SKIP_WITHOUT_DOCKER=1 to skip cleanly instead.
// The two top-level tests run sequentially — both profiles bind host :9092
// so they must not overlap.
package clie2e

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/monedula-dev/monedula-gitops/internal/e2e"
)

// binPath holds the compiled monedula-gitops binary path. It is set by
// TestMain before any test runs.
var binPath string

// repoRoot is the absolute path to the repository root, resolved from this
// file's location.
var repoRoot string

// credEnv is the set of environment variable KEY=VALUE pairs required by both
// profiles (SCRAM + SR credentials exported from compose.yaml). It is global
// (not per-scenario) since runCLIBinary appends it to every invocation;
// ORDERS_APP_PASSWORD is scenario 25's KafkaUser password source
// (manifests-cli/user.yaml: password.valueFrom.env), harmless to export for
// every other scenario since nothing else reads that variable.
var credEnv = []string{
	"KAFKA_USERNAME=admin",
	"KAFKA_PASSWORD=admin-secret",
	"SR_USERNAME=sr",
	"SR_PASSWORD=sr-secret",
	"ORDERS_APP_PASSWORD=orders-app-secret",
}

// dockerUnavailableReason returns a non-empty human-readable reason when Docker
// cannot be used to run the e2e suite, or "" when Docker is ready.
func dockerUnavailableReason() string {
	if _, err := exec.LookPath("docker"); err != nil {
		return "docker not found in PATH"
	}
	if out, err := exec.Command("docker", "info").CombinedOutput(); err != nil {
		return fmt.Sprintf("docker info failed (%v): %s", err, strings.TrimSpace(string(out)))
	}
	return ""
}

// truthyEnv reports whether an environment-variable value should be treated as
// "on" (anything other than empty/0/false/no/off).
func truthyEnv(s string) bool {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "0", "false", "no", "off":
		return false
	default:
		return true
	}
}

func TestMain(m *testing.M) {
	// This suite stands up real Kafka (and Schema Registry / cp-server / OIDC /
	// LDAP) via Docker Compose, so Docker must be running. When it is not, the
	// suite FAILS by default: an explicit `go test -tags e2e ./test/e2e/cli/`
	// asks to run the scenarios, so reporting a silent `ok` (which `go test`
	// prints for any exit-0 package, hiding a skip) would be misleading. Set
	// MONEDULA_E2E_SKIP_WITHOUT_DOCKER=1 to skip cleanly instead, for environments
	// that intentionally run without Docker. Note: plain `go test ./...` never
	// reaches this code — the suite is behind the `e2e` build tag.
	if reason := dockerUnavailableReason(); reason != "" {
		if truthyEnv(os.Getenv("MONEDULA_E2E_SKIP_WITHOUT_DOCKER")) {
			fmt.Fprintf(os.Stderr,
				"\nSKIPPED: monedula-gitops CLI e2e suite did NOT run: %s.\n"+
					"(MONEDULA_E2E_SKIP_WITHOUT_DOCKER is set; the `ok` below is a skip, not a pass.)\n\n",
				reason)
			os.Exit(0)
		}
		fmt.Fprintf(os.Stderr,
			"\nFAIL: the monedula-gitops CLI e2e suite requires a running Docker daemon.\n"+
				"Reason: %s.\n"+
				"Start Docker and re-run, or set MONEDULA_E2E_SKIP_WITHOUT_DOCKER=1 to skip when Docker is unavailable.\n\n",
			reason)
		os.Exit(1)
	}

	// Resolve the repository root: this file lives at test/e2e/cli/runner_test.go,
	// so the repo root is three directories up from this file's directory.
	_, thisFile, _, _ := runtime.Caller(0)
	repoRoot = filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", "..", ".."))

	// Build the binary once into a temp dir.
	tmpDir, err := os.MkdirTemp("", "monedula-e2e-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create temp dir: %v\n", err)
		os.Exit(1)
	}
	defer os.RemoveAll(tmpDir)

	binPath = filepath.Join(tmpDir, "monedula-gitops")
	cmd := exec.Command("go", "build", "-o", binPath, "./cmd/monedula-gitops")
	cmd.Dir = repoRoot
	if out, err := cmd.CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "build failed: %v\n%s\n", err, out)
		os.Exit(1)
	}

	os.Exit(m.Run())
}

// composeUp starts a Docker Compose stack in detached mode, waiting for all
// services to become healthy (--wait) and for the one-shot kafka-init service
// (if the profile has one) to complete. On failure it dumps the compose logs
// and fatals the test. Teardown is registered via t.Cleanup BEFORE the stack
// starts, so a failure inside this function still tears the stack down —
// otherwise a leftover broker holds host :9092 and every later profile fails
// with "port is already allocated".
func composeUp(t *testing.T, profileDir, project string) {
	t.Helper()
	composeFile := filepath.Join(profileDir, "compose.yaml")
	t.Cleanup(func() { composeDown(profileDir, project) })
	args := []string{
		"compose",
		"-f", composeFile,
		"-p", project,
		"up", "-d", "--wait",
	}
	cmd := exec.Command("docker", args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		// Dump logs to help diagnose the failure.
		logsCmd := exec.Command("docker", "compose",
			"-f", composeFile, "-p", project, "logs")
		logsOut, _ := logsCmd.CombinedOutput()
		t.Logf("compose up output:\n%s", out)
		t.Logf("compose logs:\n%s", logsOut)
		t.Fatalf("docker compose up failed: %v", err)
	}

	// `up --wait` returns the moment the broker is healthy — which is exactly
	// when the one-shot kafka-init service (SCRAM user creation) STARTS, not
	// when it finishes. Running a scenario before kafka-init completes fails
	// with SASL_AUTHENTICATION_FAILED (the SCRAM user doesn't exist yet); the
	// race is usually won on an idle machine and reliably lost under load.
	// Poll `compose ps -a` (which, unlike `compose wait`, also reports
	// already-exited containers) until kafka-init has exited 0. Profiles
	// without a kafka-init service (e.g. auth-mtls) report "no such service".
	waitForInit(t, composeFile, project, "kafka-init")
	t.Logf("compose stack %q is up", project)
}

// waitForInit blocks until the named one-shot compose service has exited 0,
// the profile turns out not to define it, or a 90s deadline passes. A nonzero
// exit or a timeout fatals with the service's logs.
func waitForInit(t *testing.T, composeFile, project, service string) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for {
		psCmd := exec.Command("docker", "compose",
			"-f", composeFile, "-p", project, "ps", "-a", "--format", "json", service)
		psOut, psErr := psCmd.CombinedOutput()
		if psErr != nil && strings.Contains(string(psOut), "no such service") {
			return // profile has no such one-shot — nothing to wait for
		}
		if psErr == nil {
			// NDJSON: one object per container of the service (scale is 1).
			var st struct {
				State    string `json:"State"`
				ExitCode int    `json:"ExitCode"`
			}
			line := strings.TrimSpace(string(psOut))
			if line != "" && json.Unmarshal([]byte(strings.SplitN(line, "\n", 2)[0]), &st) == nil {
				if st.State == "exited" {
					if st.ExitCode != 0 {
						logsCmd := exec.Command("docker", "compose",
							"-f", composeFile, "-p", project, "logs", service)
						logsOut, _ := logsCmd.CombinedOutput()
						t.Fatalf("%s exited with code %d\nlogs:\n%s", service, st.ExitCode, logsOut)
					}
					return
				}
			}
		}
		if time.Now().After(deadline) {
			logsCmd := exec.Command("docker", "compose",
				"-f", composeFile, "-p", project, "logs", service)
			logsOut, _ := logsCmd.CombinedOutput()
			t.Fatalf("timed out waiting for %s to complete\nlast ps output: %s\nlogs:\n%s",
				service, psOut, logsOut)
		}
		time.Sleep(time.Second)
	}
}

// composeDown tears down a Docker Compose stack, removing volumes.
// It is called with best-effort semantics (logged but not fatal on error).
func composeDown(profileDir, project string) {
	composeFile := filepath.Join(profileDir, "compose.yaml")
	args := []string{
		"compose",
		"-f", composeFile,
		"-p", project,
		"down", "-v",
	}
	cmd := exec.Command("docker", args...)
	if out, err := cmd.CombinedOutput(); err != nil {
		// Best-effort: log but don't fatal — the test already ran.
		fmt.Fprintf(os.Stderr, "compose down %q warning: %v\n%s\n", project, err, out)
	}
}

// composeUpMDS starts the auth-mds stack in detached mode without --wait
// (which would exit 1 because rbac-bootstrap is a one-shot that exits 0).
// After starting, it polls the MDS /authenticate endpoint until it responds,
// confirming kafka + MDS + rbac-bootstrap all completed successfully.
func composeUpMDS(t *testing.T, profileDir, project string) {
	t.Helper()
	composeFile := filepath.Join(profileDir, "compose.yaml")
	// Teardown registered before start — see composeUp for why.
	t.Cleanup(func() { composeDown(profileDir, project) })

	// Start the stack detached; don't use --wait because compose v2 exits 1
	// whenever any service container exits, even restart:"no" one-shots that
	// completed successfully (like rbac-bootstrap).
	upArgs := []string{"compose", "-f", composeFile, "-p", project, "up", "-d"}
	cmd := exec.Command("docker", upArgs...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Logf("compose up output:\n%s", out)
		t.Fatalf("docker compose up (auth-mds) failed: %v", err)
	}
	t.Logf("compose stack %q started (detached)", project)

	// Poll MDS /authenticate (basic-auth with mds/mds-secret) until it returns
	// HTTP 200, signalling that kafka, MDS, and rbac-bootstrap are all ready.
	// MDS is the last component to become healthy (cp-server has a 90s start_period).
	const (
		pollInterval = 5 * time.Second
		deadline     = 600 * time.Second
	)
	client := &http.Client{Timeout: 5 * time.Second}
	req, _ := http.NewRequest(http.MethodGet, "http://localhost:8090/security/1.0/authenticate", nil)
	req.SetBasicAuth("mds", "mds-secret")

	start := time.Now()
	for {
		resp, err := client.Do(req)
		if err == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			t.Logf("MDS /authenticate healthy after %s", time.Since(start).Round(time.Second))
			return
		}
		if resp != nil {
			resp.Body.Close()
		}
		if time.Since(start) > deadline {
			// Dump logs to help diagnose.
			logsCmd := exec.Command("docker", "compose", "-f", composeFile, "-p", project, "logs")
			logsOut, _ := logsCmd.CombinedOutput()
			t.Logf("compose logs:\n%s", logsOut)
			t.Fatalf("auth-mds MDS not healthy after %s", deadline)
		}
		time.Sleep(pollInterval)
	}
}

// runCLIBinary executes binPath with the given args, injecting the cred env
// (plus the current os.Environ) into the subprocess. Returns combined output
// and the exit code.
func runCLIBinary(args ...string) (string, int) {
	cmd := exec.Command(binPath, args...)
	cmd.Env = append(os.Environ(), credEnv...)
	out, err := cmd.CombinedOutput()
	code := 0
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			code = ee.ExitCode()
		} else {
			// Non-exit error (e.g. process couldn't start); treat as code 127.
			code = 127
		}
	}
	return string(out), code
}

// scenarioManifestsDir resolves the manifests directory a mode should apply
// for scenarioDir. Almost every scenario uses one shared manifests/ directory
// for both cli and k8s modes; scenario 25-kafka-user is the sole exception
// (its KafkaUser's password source is mode-exclusive: CLI resolves
// valueFrom.env, the operator resolves valueFrom.secretKeyRef/generate — never
// both from the same manifest, see internal/validation.ValidateUserShape and
// internal/secrets.FileEnvResolver/internal/operator.K8sResolver). When a
// manifests-<mode>/ subdirectory exists it wins; otherwise the flat
// manifests/ directory is used, preserving every other scenario's layout
// unchanged.
func scenarioManifestsDir(scenarioDir, mode string) string {
	modeDir := filepath.Join(scenarioDir, "manifests-"+mode)
	if fi, err := os.Stat(modeDir); err == nil && fi.IsDir() {
		return modeDir
	}
	return filepath.Join(scenarioDir, "manifests")
}

// runCLIScenario loads a scenario's metadata and expect contract, runs the
// appropriate CLI command against the real broker, asserts the exit code and
// output, optionally checks live state, and defers cleanup.sh.
func runCLIScenario(t *testing.T, scenarioDir, profileDir string) {
	t.Helper()

	// Load scenario metadata.
	sc, err := e2e.LoadScenario(scenarioDir)
	if err != nil {
		t.Fatalf("loading scenario.yaml: %v", err)
	}
	if !sc.HasMode("cli") {
		t.Skipf("scenario %q does not declare cli mode", sc.Title)
	}

	// Load expect contract.
	exp, err := e2e.LoadExpect(filepath.Join(scenarioDir, "expect.yaml"))
	if err != nil {
		t.Fatalf("loading expect.yaml: %v", err)
	}

	clusterConfigFile := filepath.Join(profileDir, "cluster.yaml")

	// Multi-step scenarios: execute the ordered steps, then the liveState check.
	if len(exp.Steps) > 0 {
		// Defer cleanup before running any step so it always runs.
		cleanupScript := filepath.Join(scenarioDir, "cleanup.sh")
		t.Cleanup(func() {
			c := exec.Command("bash", cleanupScript, "cli")
			c.Env = append(os.Environ(), "MONEDULA_BIN="+binPath, "CLUSTER_CONFIG="+clusterConfigFile)
			c.Env = append(c.Env, credEnv...)
			if out, err := c.CombinedOutput(); err != nil {
				t.Logf("cleanup.sh warning: %v\n%s", err, out)
			}
		})
		runSteps(t, scenarioDir, clusterConfigFile, exp.Steps)
		checkLiveState(t, scenarioDir, clusterConfigFile, exp)
		return
	}

	// Determine which CLI command to run and the expected assertions.
	var (
		cliCmd    string
		cmdExpect *e2e.CommandExpect
	)
	if exp.CLI != nil && exp.CLI.Apply != nil {
		cliCmd = "apply"
		cmdExpect = exp.CLI.Apply
	} else if exp.CLI != nil && exp.CLI.Validate != nil {
		cliCmd = "validate"
		cmdExpect = exp.CLI.Validate
	} else {
		t.Skipf("scenario %q has no CLI apply or validate expectation", sc.Title)
	}

	// Defer cleanup.sh before running the command so cleanup always runs.
	cleanupScript := filepath.Join(scenarioDir, "cleanup.sh")
	t.Cleanup(func() {
		cmd := exec.Command("bash", cleanupScript, "cli")
		cmd.Env = append(os.Environ(),
			"MONEDULA_BIN="+binPath,
			"CLUSTER_CONFIG="+clusterConfigFile,
		)
		cmd.Env = append(cmd.Env, credEnv...)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Logf("cleanup.sh warning: %v\n%s", err, out)
		}
	})

	// Run the CLI command.
	manifestsDir := scenarioManifestsDir(scenarioDir, "cli")
	args := []string{
		cliCmd,
		"-f", manifestsDir,
		"--cluster-config-file", clusterConfigFile,
	}
	out, code := runCLIBinary(args...)
	t.Logf("=== %s %s output (exit %d) ===\n%s", cliCmd, sc.Title, code, out)

	// Assert exit code.
	if r := e2e.CheckExitCode(code, cmdExpect.ExitCode); !r.Pass {
		t.Errorf("exit code check: %s\n%s", r.Name, r.Detail)
	}

	// Assert output.
	if r := e2e.CheckOutput(out, cmdExpect.OutputContains, cmdExpect.OutputMatches); !r.Pass {
		t.Errorf("output check: %s\n%s", r.Name, r.Detail)
	}

	checkLiveState(t, scenarioDir, clusterConfigFile, exp)
}

// checkLiveState runs `e2e check --mode cli` when the scenario declares any
// live state to assert.
func checkLiveState(t *testing.T, scenarioDir, clusterConfigFile string, exp *e2e.Expect) {
	t.Helper()
	hasLiveState := len(exp.LiveState.Topics) > 0 || len(exp.LiveState.ACLs) > 0 ||
		len(exp.LiveState.Quotas) > 0 || len(exp.LiveState.Subjects) > 0 || len(exp.LiveState.Users) > 0
	if !hasLiveState {
		return
	}
	checkOut, checkCode := runCLIBinary("e2e", "check", "--scenario", scenarioDir, "--mode", "cli", "--cluster-config", clusterConfigFile)
	t.Logf("=== e2e check output (exit %d) ===\n%s", checkCode, checkOut)
	if checkCode != 0 {
		t.Errorf("e2e check failed (exit %d):\n%s", checkCode, checkOut)
	}
}

// runSteps executes a scenario's ordered steps against the (already-up) cluster.
func runSteps(t *testing.T, scenarioDir, clusterConfigFile string, steps []e2e.Step) {
	t.Helper()
	var importedDir string // set by an "import" step; referenced via the "@imported" sentinel
	for i, st := range steps {
		switch st.Run {
		case "apply", "verify", "diff":
			manifests := filepath.Join(scenarioDir, "manifests")
			switch {
			case st.Manifests == "@imported":
				if importedDir == "" {
					t.Fatalf("step %d: manifests %q but no prior import step produced an output dir", i, st.Manifests)
				}
				manifests = importedDir
			case st.Manifests != "":
				manifests = filepath.Join(scenarioDir, st.Manifests)
			}
			args := []string{st.Run, "-f", manifests, "--cluster-config-file", clusterConfigFile}
			args = append(args, st.Flags...)
			out, code := runCLIBinary(args...)
			t.Logf("=== step %d: %s (exit %d) ===\n%s", i, st.Run, code, out)
			if st.Expect != nil {
				if r := e2e.CheckExitCode(code, st.Expect.ExitCode); !r.Pass {
					t.Errorf("step %d exit: %s\n%s", i, r.Name, r.Detail)
				}
				if r := e2e.CheckOutput(out, st.Expect.OutputContains, st.Expect.OutputMatches); !r.Pass {
					t.Errorf("step %d output: %s\n%s", i, r.Name, r.Detail)
				}
			}
		case "import":
			importedDir = t.TempDir()
			args := []string{"import", "cluster", "--cluster-config-file", clusterConfigFile, "--output-dir", importedDir}
			args = append(args, st.Flags...)
			out, code := runCLIBinary(args...)
			t.Logf("=== step %d: import -> %s (exit %d) ===\n%s", i, importedDir, code, out)
			if st.Expect != nil {
				if r := e2e.CheckExitCode(code, st.Expect.ExitCode); !r.Pass {
					t.Errorf("step %d exit: %s\n%s", i, r.Name, r.Detail)
				}
				if r := e2e.CheckOutput(out, st.Expect.OutputContains, st.Expect.OutputMatches); !r.Pass {
					t.Errorf("step %d output: %s\n%s", i, r.Name, r.Detail)
				}
			} else if code != 0 {
				t.Fatalf("step %d: import failed (exit %d):\n%s", i, code, out)
			}
		case "mutate":
			if st.Mutate == nil || st.Mutate.TopicConfig == nil {
				t.Fatalf("step %d: mutate step missing topicConfig", i)
			}
			tc := st.Mutate.TopicConfig
			var pairs []string
			for k, v := range tc.Set {
				pairs = append(pairs, k+"="+v)
			}
			out, code := runCLIBinary("e2e", "mutate", "--cluster-config", clusterConfigFile,
				"--topic", tc.Topic, "--set", strings.Join(pairs, ","))
			t.Logf("=== step %d: mutate %s (exit %d) ===\n%s", i, tc.Topic, code, out)
			if code != 0 {
				t.Fatalf("step %d: mutate failed (exit %d):\n%s", i, code, out)
			}
		case "doctor":
			args := []string{"doctor", "--cluster-config-file", clusterConfigFile}
			args = append(args, st.Flags...)
			out, code := runCLIBinary(args...)
			t.Logf("=== step %d: doctor (exit %d) ===\n%s", i, code, out)
			if st.Expect != nil {
				if r := e2e.CheckExitCode(code, st.Expect.ExitCode); !r.Pass {
					t.Errorf("step %d exit: %s\n%s", i, r.Name, r.Detail)
				}
				if r := e2e.CheckOutput(out, st.Expect.OutputContains, st.Expect.OutputMatches); !r.Pass {
					t.Errorf("step %d output: %s\n%s", i, r.Name, r.Detail)
				}
			} else if code != 0 {
				t.Fatalf("step %d: doctor failed (exit %d):\n%s", i, code, out)
			}
		default:
			t.Fatalf("step %d: unknown run %q", i, st.Run)
		}
	}
}

// TestSharedSASLScenarios brings up the shared-sasl compose stack, runs
// scenario 01-create-topic (apply) and 02-invalid-manifest (validate), then
// tears down the stack.
func TestSharedSASLScenarios(t *testing.T) {
	profileDir := filepath.Join(repoRoot, "scenarios", "clusters", "shared-sasl")
	project := "mon-e2e-shared"

	composeUp(t, profileDir, project)

	scenarios := []string{
		filepath.Join(repoRoot, "scenarios", "01-create-topic"),
		filepath.Join(repoRoot, "scenarios", "02-invalid-manifest"),
		filepath.Join(repoRoot, "scenarios", "05-topic-with-access"),
		filepath.Join(repoRoot, "scenarios", "06-access-policy"),
		filepath.Join(repoRoot, "scenarios", "07-user-quota"),
		filepath.Join(repoRoot, "scenarios", "08-ip-quota"),
		filepath.Join(repoRoot, "scenarios", "09-register-schema"),
		filepath.Join(repoRoot, "scenarios", "10-drift-detect-reconcile"),
		filepath.Join(repoRoot, "scenarios", "11-reconciliation-modes"),
		filepath.Join(repoRoot, "scenarios", "12-opt-in-prune"),
		filepath.Join(repoRoot, "scenarios", "16-import-round-trip"),
		filepath.Join(repoRoot, "scenarios", "17-diff-dry-run"),
		filepath.Join(repoRoot, "scenarios", "18-schema-evolution"),
		filepath.Join(repoRoot, "scenarios", "19-drift-ignore-fields"),
		filepath.Join(repoRoot, "scenarios", "23-schema-import-round-trip"),
		filepath.Join(repoRoot, "scenarios", "24-doctor-preflight"),
		filepath.Join(repoRoot, "scenarios", "25-kafka-user"),
	}
	for _, sd := range scenarios {
		sd := sd // capture
		name := filepath.Base(sd)
		t.Run(name, func(t *testing.T) {
			runCLIScenario(t, sd, profileDir)
		})
	}
}

// TestAuthSASLSSLScenarios brings up the auth-sasl-ssl compose stack, runs
// scenario 04-sasl-ssl (apply), then tears down the stack.
func TestAuthSASLSSLScenarios(t *testing.T) {
	profileDir := filepath.Join(repoRoot, "scenarios", "clusters", "auth-sasl-ssl")
	project := "mon-e2e-tls"

	composeUp(t, profileDir, project)

	scenarios := []string{
		filepath.Join(repoRoot, "scenarios", "04-sasl-ssl"),
	}
	for _, sd := range scenarios {
		sd := sd // capture
		name := filepath.Base(sd)
		t.Run(name, func(t *testing.T) {
			runCLIScenario(t, sd, profileDir)
		})
	}
}

// TestAuthMTLSScenarios brings up the auth-mtls compose stack and runs the
// mTLS client-cert scenario.
func TestAuthMTLSScenarios(t *testing.T) {
	profileDir := filepath.Join(repoRoot, "scenarios", "clusters", "auth-mtls")
	project := "mon-e2e-mtls"

	composeUp(t, profileDir, project)

	scenarios := []string{
		filepath.Join(repoRoot, "scenarios", "20-mtls"),
	}
	for _, sd := range scenarios {
		sd := sd
		name := filepath.Base(sd)
		t.Run(name, func(t *testing.T) {
			runCLIScenario(t, sd, profileDir)
		})
	}
}

// TestAuthOAuthScenarios brings up the auth-oauth compose stack (broker + mock
// OIDC server) and runs the OAUTHBEARER scenario.
func TestAuthOAuthScenarios(t *testing.T) {
	profileDir := filepath.Join(repoRoot, "scenarios", "clusters", "auth-oauth")
	project := "mon-e2e-oauth"

	t.Setenv("OAUTH_CLIENT_ID", "monedula")
	t.Setenv("OAUTH_CLIENT_SECRET", "monedula-secret")

	composeUp(t, profileDir, project)

	scenarios := []string{
		filepath.Join(repoRoot, "scenarios", "21-oauthbearer"),
	}
	for _, sd := range scenarios {
		sd := sd
		name := filepath.Base(sd)
		t.Run(name, func(t *testing.T) {
			runCLIScenario(t, sd, profileDir)
		})
	}
}

// TestAuthMDSScenarios brings up the auth-mds compose stack (cp-server + LDAP +
// MDS RBAC) and runs the role-binding scenario.
// cp-server is a large image with a slow startup — a generous timeout is required
// (pass -timeout 1200s when invoking go test directly).
func TestAuthMDSScenarios(t *testing.T) {
	profileDir := filepath.Join(repoRoot, "scenarios", "clusters", "auth-mds")
	project := "mon-e2e-mds"

	// Swap credEnv for the duration of this test so that runCLIBinary (which
	// appends credEnv last, overriding os.Environ) carries the MDS/LDAP creds
	// rather than the shared-sasl creds. Tests run sequentially so this is safe.
	origCredEnv := credEnv
	credEnv = []string{
		// Kafka PLAIN credentials (LDAP-validated on the CLIENTHOST listener).
		"KAFKA_USERNAME=kafka-admin",
		"KAFKA_PASSWORD=kafka-admin-secret",
		// MDS basic-auth credentials (LDAP user that holds SystemAdmin after bootstrap).
		"MDS_USER=mds",
		"MDS_PASSWORD=mds-secret",
	}
	t.Cleanup(func() { credEnv = origCredEnv })

	composeUpMDS(t, profileDir, project)

	scenarios := []string{
		filepath.Join(repoRoot, "scenarios", "22-rolebinding"),
	}
	for _, sd := range scenarios {
		sd := sd
		name := filepath.Base(sd)
		t.Run(name, func(t *testing.T) {
			runCLIScenario(t, sd, profileDir)
		})
	}
}
