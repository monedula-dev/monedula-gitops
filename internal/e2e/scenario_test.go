package e2e

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadScenario(t *testing.T) {
	dir := t.TempDir()
	const doc = `
title: Create a topic with config
modes: [cli, k8s]
cluster: shared-sasl
summary: Applies a single KafkaTopic and verifies it lands on the broker.
`
	if err := os.WriteFile(filepath.Join(dir, "scenario.yaml"), []byte(doc), 0o644); err != nil {
		t.Fatal(err)
	}
	s, err := LoadScenario(dir)
	if err != nil {
		t.Fatalf("LoadScenario: %v", err)
	}
	if s.Title != "Create a topic with config" || s.Cluster != "shared-sasl" {
		t.Errorf("metadata wrong: %+v", s)
	}
	if !s.HasMode("cli") || !s.HasMode("k8s") || s.HasMode("bogus") {
		t.Errorf("HasMode wrong: %+v", s.Modes)
	}
	if s.Dir != dir {
		t.Errorf("Dir not set: %q", s.Dir)
	}
}

func TestLoadScenarioMissingFile(t *testing.T) {
	if _, err := LoadScenario(t.TempDir()); err == nil {
		t.Fatal("expected error for missing scenario.yaml")
	}
}

func TestLoadScenarioMissingRequiredFields(t *testing.T) {
	cases := map[string]string{
		"missing title":   "modes: [cli]\ncluster: shared-sasl\n",
		"missing modes":   "title: T\ncluster: shared-sasl\n",
		"missing cluster": "title: T\nmodes: [cli]\n",
	}
	for name, doc := range cases {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, "scenario.yaml"), []byte(doc), 0o644); err != nil {
				t.Fatal(err)
			}
			if _, err := LoadScenario(dir); err == nil {
				t.Errorf("expected error for %s", name)
			}
		})
	}
}
