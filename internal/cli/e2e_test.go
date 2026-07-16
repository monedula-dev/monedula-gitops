package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

func findSub(root *cobra.Command, name string) *cobra.Command {
	for _, c := range root.Commands() {
		if c.Name() == name {
			return c
		}
	}
	return nil
}

func TestE2ECommandIsHidden(t *testing.T) {
	root := NewRootCmd()
	e2e := findSub(root, "e2e")
	if e2e == nil {
		t.Fatal("e2e command not registered")
	}
	if !e2e.Hidden {
		t.Errorf("e2e command should be Hidden (test tooling, not user-facing)")
	}
	check := findSub(e2e, "check")
	if check == nil {
		t.Fatal("e2e check subcommand not registered")
	}
	for _, f := range []string{"scenario", "mode"} {
		if check.Flags().Lookup(f) == nil {
			t.Errorf("e2e check missing --%s flag", f)
		}
	}
}

func TestE2ECheckRejectsBadMode(t *testing.T) {
	root := NewRootCmd()
	root.SetArgs([]string{"e2e", "check", "--scenario", ".", "--mode", "bogus"})
	root.SetOut(&strings.Builder{})
	root.SetErr(&strings.Builder{})
	if err := root.Execute(); err == nil {
		t.Error("expected error for invalid --mode")
	}
}

// writeE2EFixture lays out a scenario dir + a cluster manifest carrying the
// mock-state annotation + a mock state file, all under root, and returns the
// scenario dir and cluster-config path. cleanupPolicy is the topic's seeded
// cleanup.policy in the mock state.
func writeE2EFixture(t *testing.T, cleanupPolicy string) (scenarioDir, clusterConfig string) {
	t.Helper()
	root := t.TempDir()
	scenarioDir = filepath.Join(root, "scn")
	if err := os.MkdirAll(scenarioDir, 0o755); err != nil {
		t.Fatal(err)
	}
	expect := "liveState:\n  topics:\n    - name: payments.orders\n      config:\n        cleanup.policy: compact\n"
	if err := os.WriteFile(filepath.Join(scenarioDir, "expect.yaml"), []byte(expect), 0o644); err != nil {
		t.Fatal(err)
	}
	state := "topics:\n  - name: payments.orders\n    partitions: 3\n    config:\n      cleanup.policy: " + cleanupPolicy + "\nacls: []\n"
	if err := os.WriteFile(filepath.Join(root, "state.yaml"), []byte(state), 0o644); err != nil {
		t.Fatal(err)
	}
	clusterConfig = filepath.Join(root, "cluster.yaml")
	manifest := "apiVersion: gitops.monedula.dev/v1alpha1\nkind: KafkaCluster\nmetadata:\n  name: shared\n  annotations:\n    gitops.monedula.dev/mock-state-file: state.yaml\nspec:\n  bootstrapServers: localhost:9092\n"
	if err := os.WriteFile(clusterConfig, []byte(manifest), 0o644); err != nil {
		t.Fatal(err)
	}
	return scenarioDir, clusterConfig
}

// TestE2ECheckLiveStateViaMockSeam exercises the command's glue end-to-end
// hermetically: buildAdminClientForE2E loads the cluster manifest, the
// mock-state annotation seeds a mock admin client, and CheckLiveState asserts
// the topic config. No real broker.
func TestE2ECheckLiveStateViaMockSeam(t *testing.T) {
	t.Run("matching config passes", func(t *testing.T) {
		scn, cc := writeE2EFixture(t, "compact")
		root := NewRootCmd()
		root.SetArgs([]string{"e2e", "check", "--scenario", scn, "--mode", "cli", "--cluster-config", cc})
		root.SetOut(&strings.Builder{})
		root.SetErr(&strings.Builder{})
		if err := root.Execute(); err != nil {
			t.Errorf("expected pass, got error: %v", err)
		}
	})
	t.Run("mismatched config fails (exit 1)", func(t *testing.T) {
		scn, cc := writeE2EFixture(t, "delete") // seeded != expected "compact"
		root := NewRootCmd()
		root.SetArgs([]string{"e2e", "check", "--scenario", scn, "--mode", "cli", "--cluster-config", cc})
		root.SetOut(&strings.Builder{})
		root.SetErr(&strings.Builder{})
		err := root.Execute()
		ee, ok := err.(*ExitError)
		if !ok || ee.Code != 1 {
			t.Errorf("expected ExitError code 1 on assertion failure, got %v", err)
		}
	})
}

func TestE2ECheckSubjectsNoSR(t *testing.T) {
	root := t.TempDir()
	scn := filepath.Join(root, "scn")
	if err := os.MkdirAll(scn, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(scn, "expect.yaml"),
		[]byte("liveState:\n  subjects:\n    - name: x-value\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	cc := filepath.Join(root, "cluster.yaml")
	if err := os.WriteFile(cc,
		[]byte("apiVersion: gitops.monedula.dev/v1alpha1\nkind: KafkaCluster\nmetadata:\n  name: shared\nspec:\n  bootstrapServers: localhost:9092\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	r := NewRootCmd()
	r.SetArgs([]string{"e2e", "check", "--scenario", scn, "--mode", "cli", "--cluster-config", cc})
	r.SetOut(&strings.Builder{})
	r.SetErr(&strings.Builder{})
	err := r.Execute()
	ee, ok := err.(*ExitError)
	if !ok || ee.Code != 1 {
		t.Errorf("expected ExitError code 1 (subject check failed), got %v", err)
	}
}

func TestE2EMutateRegisteredAndValidates(t *testing.T) {
	root := NewRootCmd()
	e2e := findSub(root, "e2e")
	if e2e == nil {
		t.Fatal("e2e command missing")
	}
	if findSub(e2e, "mutate") == nil {
		t.Fatal("e2e mutate subcommand not registered")
	}
}

func TestE2EMutateMissingTopic(t *testing.T) {
	root := t.TempDir()
	cc := filepath.Join(root, "cluster.yaml")
	if err := os.WriteFile(cc, []byte("apiVersion: gitops.monedula.dev/v1alpha1\nkind: KafkaCluster\nmetadata:\n  name: shared\nspec:\n  bootstrapServers: localhost:9092\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	r := NewRootCmd()
	r.SetArgs([]string{"e2e", "mutate", "--cluster-config", cc, "--set", "cleanup.policy=delete"})
	r.SetOut(&strings.Builder{})
	r.SetErr(&strings.Builder{})
	if err := r.Execute(); err == nil {
		t.Error("expected error when --topic is missing")
	}
}

func TestE2EMutateBadSet(t *testing.T) {
	root := t.TempDir()
	cc := filepath.Join(root, "cluster.yaml")
	if err := os.WriteFile(cc, []byte("apiVersion: gitops.monedula.dev/v1alpha1\nkind: KafkaCluster\nmetadata:\n  name: shared\nspec:\n  bootstrapServers: localhost:9092\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	r := NewRootCmd()
	r.SetArgs([]string{"e2e", "mutate", "--cluster-config", cc, "--topic", "t", "--set", "noequalsign"})
	r.SetOut(&strings.Builder{})
	r.SetErr(&strings.Builder{})
	err := r.Execute()
	ee, ok := err.(*ExitError)
	if !ok || ee.Code != 2 {
		t.Errorf("expected ExitError code 2 for malformed --set, got %v", err)
	}
}

func TestE2EMutateViaMockSeam(t *testing.T) {
	root := t.TempDir()
	state := "topics:\n  - name: drift.demo\n    partitions: 1\n    config:\n      cleanup.policy: compact\n"
	if err := os.WriteFile(filepath.Join(root, "state.yaml"), []byte(state), 0o644); err != nil {
		t.Fatal(err)
	}
	cc := filepath.Join(root, "cluster.yaml")
	manifest := "apiVersion: gitops.monedula.dev/v1alpha1\nkind: KafkaCluster\nmetadata:\n  name: shared\n  annotations:\n    gitops.monedula.dev/mock-state-file: state.yaml\nspec:\n  bootstrapServers: localhost:9092\n"
	if err := os.WriteFile(cc, []byte(manifest), 0o644); err != nil {
		t.Fatal(err)
	}
	r := NewRootCmd()
	r.SetArgs([]string{"e2e", "mutate", "--cluster-config", cc, "--topic", "drift.demo", "--set", "cleanup.policy=delete,retention.ms=1000"})
	r.SetOut(&strings.Builder{})
	r.SetErr(&strings.Builder{})
	if err := r.Execute(); err != nil {
		t.Errorf("expected mutate to succeed against mock, got %v", err)
	}
}

func TestE2ECheckAbsentRequiresClusterConfigCLI(t *testing.T) {
	dir := t.TempDir()
	scenarioDir := filepath.Join(dir, "scn")
	if err := os.MkdirAll(scenarioDir, 0o755); err != nil {
		t.Fatal(err)
	}
	// A scenario whose expect.yaml declares ONLY liveState.absent.
	expect := "liveState:\n  absent:\n    topics:\n      - delete.demo\n"
	if err := os.WriteFile(filepath.Join(scenarioDir, "expect.yaml"), []byte(expect), 0o644); err != nil {
		t.Fatal(err)
	}
	r := NewRootCmd()
	r.SetArgs([]string{"e2e", "check", "--scenario", scenarioDir, "--mode", "cli"})
	r.SetOut(&strings.Builder{})
	r.SetErr(&strings.Builder{})
	err := r.Execute()
	// cli mode + no --cluster-config + an absent-only liveState must error,
	// proving the absent block is recognized as needing a broker probe.
	ee, ok := err.(*ExitError)
	if !ok || ee.Code != 2 {
		t.Errorf("expected ExitError code 2 (cluster-config required), got %v", err)
	}
}
