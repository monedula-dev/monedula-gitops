package e2e

import (
	"fmt"
	"os"
	"path/filepath"

	"sigs.k8s.io/yaml"
)

// Scenario is a scenario directory's metadata (scenario.yaml) plus its resolved
// directory path.
type Scenario struct {
	Title   string   `json:"title"`
	Modes   []string `json:"modes"`
	Cluster string   `json:"cluster"`
	Summary string   `json:"summary,omitempty"`
	// Dir is the path to the scenario directory (set by LoadScenario, not parsed).
	Dir string `json:"-"`
}

// HasMode reports whether the scenario declares the given mode ("cli"/"k8s").
func (s *Scenario) HasMode(mode string) bool {
	for _, m := range s.Modes {
		if m == mode {
			return true
		}
	}
	return false
}

// LoadScenario reads dir/scenario.yaml and records dir on the result.
func LoadScenario(dir string) (*Scenario, error) {
	b, err := os.ReadFile(filepath.Join(dir, "scenario.yaml"))
	if err != nil {
		return nil, fmt.Errorf("reading scenario.yaml: %w", err)
	}
	var s Scenario
	if err := yaml.Unmarshal(b, &s); err != nil {
		return nil, fmt.Errorf("parsing scenario.yaml in %s: %w", dir, err)
	}
	if s.Title == "" || len(s.Modes) == 0 || s.Cluster == "" {
		return nil, fmt.Errorf("scenario.yaml in %s missing required title/modes/cluster", dir)
	}
	s.Dir = dir
	return &s, nil
}
