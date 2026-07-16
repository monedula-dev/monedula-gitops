package e2e

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadExpect(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "expect.yaml")
	const doc = `
cli:
  apply:
    exitCode: 0
    outputContains: ["CreateTopic", "payments.orders"]
  validate:
    exitCode: 1
    outputMatches: ["identity collision"]
k8s:
  conditions:
    - kind: KafkaTopic
      name: payments-orders
      type: Ready
      status: "True"
  admission:
    rejected: true
    messageMatches: "immutable"
liveState:
  topics:
    - name: payments.orders
      config:
        cleanup.policy: compact
`
	if err := os.WriteFile(path, []byte(doc), 0o644); err != nil {
		t.Fatal(err)
	}
	e, err := LoadExpect(path)
	if err != nil {
		t.Fatalf("LoadExpect: %v", err)
	}
	if e.CLI == nil || e.CLI.Apply == nil || e.CLI.Apply.ExitCode == nil || *e.CLI.Apply.ExitCode != 0 {
		t.Fatalf("apply exitCode not parsed: %+v", e.CLI)
	}
	if len(e.CLI.Apply.OutputContains) != 2 || e.CLI.Apply.OutputContains[0] != "CreateTopic" {
		t.Errorf("outputContains wrong: %+v", e.CLI.Apply.OutputContains)
	}
	if e.CLI.Validate == nil || e.CLI.Validate.ExitCode == nil || *e.CLI.Validate.ExitCode != 1 {
		t.Errorf("validate exitCode wrong")
	}
	if e.K8s == nil || len(e.K8s.Conditions) != 1 || e.K8s.Conditions[0].Type != "Ready" {
		t.Errorf("conditions wrong: %+v", e.K8s)
	}
	if e.K8s.Admission == nil || !e.K8s.Admission.Rejected || e.K8s.Admission.MessageMatches != "immutable" {
		t.Errorf("admission wrong: %+v", e.K8s.Admission)
	}
	if len(e.LiveState.Topics) != 1 || e.LiveState.Topics[0].Config["cleanup.policy"] != "compact" {
		t.Errorf("liveState wrong: %+v", e.LiveState)
	}
}

func TestLoadExpectUsers(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "expect.yaml")
	const doc = `
liveState:
  users:
    - username: svc-orders-app
      mechanism: SCRAM-SHA-512
      iterations: 8192
  absent:
    users:
      - username: svc-retired-app
        mechanism: SCRAM-SHA-256
`
	if err := os.WriteFile(path, []byte(doc), 0o644); err != nil {
		t.Fatal(err)
	}
	e, err := LoadExpect(path)
	if err != nil {
		t.Fatalf("LoadExpect: %v", err)
	}
	if len(e.LiveState.Users) != 1 {
		t.Fatalf("users not parsed: %+v", e.LiveState.Users)
	}
	u := e.LiveState.Users[0]
	if u.Username != "svc-orders-app" || u.Mechanism != "SCRAM-SHA-512" || u.Iterations != 8192 {
		t.Errorf("user entry wrong: %+v", u)
	}
	if e.LiveState.Absent == nil || len(e.LiveState.Absent.Users) != 1 {
		t.Fatalf("absent.users not parsed: %+v", e.LiveState.Absent)
	}
	au := e.LiveState.Absent.Users[0]
	if au.Username != "svc-retired-app" || au.Mechanism != "SCRAM-SHA-256" {
		t.Errorf("absent user entry wrong: %+v", au)
	}
}

func TestLoadExpectSteps(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "expect.yaml")
	const doc = `
steps:
  - run: apply
    expect: { exitCode: 0 }
  - run: mutate
    mutate:
      topicConfig: { topic: drift.demo, set: { cleanup.policy: delete } }
  - run: verify
    expect: { exitCode: 1, outputContains: ["cleanup.policy"] }
  - run: apply
    manifests: manifests-pruned
    flags: ["--prune"]
    expect: { exitCode: 0, outputContains: ["DeleteAcl"] }
`
	if err := os.WriteFile(path, []byte(doc), 0o644); err != nil {
		t.Fatal(err)
	}
	e, err := LoadExpect(path)
	if err != nil {
		t.Fatalf("LoadExpect: %v", err)
	}
	if len(e.Steps) != 4 {
		t.Fatalf("want 4 steps, got %d: %+v", len(e.Steps), e.Steps)
	}
	if e.Steps[0].Run != "apply" || e.Steps[0].Expect == nil || e.Steps[0].Expect.ExitCode == nil || *e.Steps[0].Expect.ExitCode != 0 {
		t.Errorf("step0 wrong: %+v", e.Steps[0])
	}
	if e.Steps[1].Run != "mutate" || e.Steps[1].Mutate == nil || e.Steps[1].Mutate.TopicConfig == nil ||
		e.Steps[1].Mutate.TopicConfig.Topic != "drift.demo" || e.Steps[1].Mutate.TopicConfig.Set["cleanup.policy"] != "delete" {
		t.Errorf("step1 mutate wrong: %+v", e.Steps[1])
	}
	if e.Steps[2].Expect == nil || e.Steps[2].Expect.ExitCode == nil || *e.Steps[2].Expect.ExitCode != 1 ||
		len(e.Steps[2].Expect.OutputContains) != 1 {
		t.Errorf("step2 wrong: %+v", e.Steps[2])
	}
	if e.Steps[3].Manifests != "manifests-pruned" || len(e.Steps[3].Flags) != 1 || e.Steps[3].Flags[0] != "--prune" {
		t.Errorf("step3 manifests/flags wrong: %+v", e.Steps[3])
	}
}

func TestLoadExpectNoStepsBackCompat(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "expect.yaml")
	if err := os.WriteFile(path, []byte("cli:\n  apply: { exitCode: 0 }\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	e, err := LoadExpect(path)
	if err != nil {
		t.Fatalf("LoadExpect: %v", err)
	}
	if len(e.Steps) != 0 {
		t.Errorf("expected no steps, got %+v", e.Steps)
	}
}

func TestLoadExpectMissingFile(t *testing.T) {
	if _, err := LoadExpect(filepath.Join(t.TempDir(), "nope.yaml")); err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestLoadExpectAbsent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "expect.yaml")
	const doc = `
liveState:
  absent:
    topics:
      - delete.demo
`
	if err := os.WriteFile(path, []byte(doc), 0o644); err != nil {
		t.Fatal(err)
	}
	e, err := LoadExpect(path)
	if err != nil {
		t.Fatalf("LoadExpect: %v", err)
	}
	if e.LiveState.Absent == nil || len(e.LiveState.Absent.Topics) != 1 || e.LiveState.Absent.Topics[0] != "delete.demo" {
		t.Fatalf("absent not parsed: %+v", e.LiveState.Absent)
	}
}

func TestLoadExpectNoAbsentBackCompat(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "expect.yaml")
	if err := os.WriteFile(path, []byte("liveState:\n  topics:\n    - name: x\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	e, err := LoadExpect(path)
	if err != nil {
		t.Fatalf("LoadExpect: %v", err)
	}
	if e.LiveState.Absent != nil {
		t.Errorf("expected nil Absent, got %+v", e.LiveState.Absent)
	}
}

func TestLoadExpectLiveStateSections(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "expect.yaml")
	const doc = `
liveState:
  acls:
    - principal: "User:svc-orders"
      operation: Write
      resourceType: topic
      resourceName: payments.orders
  quotas:
    - entity: { user: "svc-orders" }
      limits: { producer_byte_rate: 1048576 }
    - entity: { ip: "10.0.0.1" }
      limits: { connection_creation_rate: 100 }
    - entity: { clientId: "svc-app" }
      limits: { consumer_byte_rate: 2048 }
  subjects:
    - name: payments.orders-value
      compatibility: BACKWARD
`
	if err := os.WriteFile(path, []byte(doc), 0o644); err != nil {
		t.Fatal(err)
	}
	e, err := LoadExpect(path)
	if err != nil {
		t.Fatalf("LoadExpect: %v", err)
	}
	if len(e.LiveState.ACLs) != 1 || e.LiveState.ACLs[0].Principal != "User:svc-orders" ||
		e.LiveState.ACLs[0].Operation != "Write" || e.LiveState.ACLs[0].ResourceType != "topic" {
		t.Errorf("acls parsed wrong: %+v", e.LiveState.ACLs)
	}
	if len(e.LiveState.Quotas) != 3 || e.LiveState.Quotas[0].Entity.User != "svc-orders" ||
		e.LiveState.Quotas[0].Limits["producer_byte_rate"] != 1048576 {
		t.Errorf("user quota parsed wrong: %+v", e.LiveState.Quotas)
	}
	if e.LiveState.Quotas[1].Entity.IP != "10.0.0.1" || e.LiveState.Quotas[1].Limits["connection_creation_rate"] != 100 {
		t.Errorf("ip quota parsed wrong: %+v", e.LiveState.Quotas[1])
	}
	if e.LiveState.Quotas[2].Entity.ClientID != "svc-app" || e.LiveState.Quotas[2].Limits["consumer_byte_rate"] != 2048 {
		t.Errorf("clientId quota parsed wrong (camelCase json tag): %+v", e.LiveState.Quotas[2])
	}
	if len(e.LiveState.Subjects) != 1 || e.LiveState.Subjects[0].Name != "payments.orders-value" ||
		e.LiveState.Subjects[0].Compatibility != "BACKWARD" {
		t.Errorf("subjects parsed wrong: %+v", e.LiveState.Subjects)
	}
}
