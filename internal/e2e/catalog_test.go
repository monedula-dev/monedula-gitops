package e2e

import (
	"os"
	"path/filepath"
	"testing"
)

// TestSeedCatalogParses ensures every committed scenario dir has a valid
// scenario.yaml + expect.yaml that the loaders accept. Keeps authored content
// honest without standing up infra.
func TestSeedCatalogParses(t *testing.T) {
	root := filepath.Join("..", "..", "scenarios")
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("read scenarios/: %v", err)
	}
	seen := 0
	for _, e := range entries {
		if !e.IsDir() || e.Name() == "clusters" {
			continue
		}
		dir := filepath.Join(root, e.Name())
		if _, err := os.Stat(filepath.Join(dir, "scenario.yaml")); err != nil {
			continue
		}
		seen++
		if _, err := LoadScenario(dir); err != nil {
			t.Errorf("%s: scenario.yaml: %v", e.Name(), err)
		}
		if _, err := LoadExpect(filepath.Join(dir, "expect.yaml")); err != nil {
			t.Errorf("%s: expect.yaml: %v", e.Name(), err)
		}
	}
	if seen < 4 {
		t.Errorf("expected at least 4 seed scenarios, found %d", seen)
	}
}
