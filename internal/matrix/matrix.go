// Package matrix is the single source of truth for the Kafka and Confluent
// Platform versions this project validates against.
//
// The version list lives in versions.yaml next to this file (embedded, so it
// must be in the package directory — go:embed cannot reach outside it). Both
// container-backed tiers resolve their cell from MONEDULA_MATRIX_CELL and skip
// what their cell does not support, with a stated reason.
//
// This file deliberately has no testcontainers dependency: it is compiled into
// every build, and the default `go build ./...` must stay Docker-free. The
// container starter lives in broker.go behind a build tag.
package matrix

import (
	_ "embed"
	"fmt"
	"os"
	"slices"

	"gopkg.in/yaml.v3"
)

// Families a cell's broker image can belong to. The family selects the
// container start path (broker.go) and the compose overlay (test/e2e/cli).
const (
	FamilyApache    = "apache"
	FamilyConfluent = "confluent"
)

// Tiers a cell can participate in.
const (
	TierIntegration = "integration"
	TierE2E         = "e2e"
)

// The closed capability vocabulary. A cell declares what its broker stack can
// actually do; tests skip what is not declared.
const (
	CapTopics         = "topics"
	CapACLs           = "acls"
	CapQuotas         = "quotas"
	CapSCRAM          = "scram"
	CapSchemaRegistry = "schemaregistry"
	CapMDS            = "mds"
)

// EnvCell names the environment variable that selects a cell by id. Unset (or
// empty) selects the cell marked `default: true`.
const EnvCell = "MONEDULA_MATRIX_CELL"

//go:embed versions.yaml
var versionsYAML []byte

// Cell is one broker configuration the project is validated against.
type Cell struct {
	// ID is the stable identifier used as the MONEDULA_MATRIX_CELL value and
	// as the CI job name.
	ID string `yaml:"id"`
	// Family is FamilyApache or FamilyConfluent.
	Family string `yaml:"family"`
	// Image is the broker image reference.
	Image string `yaml:"image"`
	// SchemaRegistryImage is the Schema Registry image. Non-empty exactly when
	// the cell declares CapSchemaRegistry.
	SchemaRegistryImage string `yaml:"schemaRegistryImage"`
	// Profile, when non-empty, restricts the cell to that single compose
	// profile — and that profile runs under no other cell.
	Profile string `yaml:"profile"`
	// Tiers lists the suites that run this cell.
	Tiers []string `yaml:"tiers"`
	// Default marks the cell used when EnvCell is unset. Exactly one cell.
	Default bool `yaml:"default"`
	// Capabilities is the subset of the capability vocabulary this cell offers.
	Capabilities []string `yaml:"capabilities"`
	// Overlays names extra compose overlays this cell needs beyond the
	// family-based one. Each entry is applied on top of the profile's base
	// compose.yaml (and the apache overlay, when applicable) as
	// compose.<name>.yaml, when that file exists for the profile — same rule
	// as the family-based apache overlay in test/e2e/cli. Order matters: the
	// e2e runner applies these in declaration order.
	Overlays []string `yaml:"overlays"`
}

// Supports reports whether the cell declares the given capability.
func (c Cell) Supports(capability string) bool {
	return slices.Contains(c.Capabilities, capability)
}

// InTier reports whether the cell participates in the given tier.
func (c Cell) InTier(tier string) bool {
	return slices.Contains(c.Tiers, tier)
}

type registry struct {
	Cells []Cell `yaml:"cells"`
}

// loaded is parsed once at init. A malformed versions.yaml is a programming
// error, not a runtime condition — the unit tests in this package are what
// keep it well-formed, so failing loudly here is correct.
var loaded = mustLoad()

func mustLoad() []Cell {
	var r registry
	if err := yaml.Unmarshal(versionsYAML, &r); err != nil {
		panic(fmt.Sprintf("matrix: parsing versions.yaml: %v", err))
	}
	if len(r.Cells) == 0 {
		panic("matrix: versions.yaml declares no cells")
	}
	return r.Cells
}

// Cells returns every declared cell, in file order.
func Cells() []Cell {
	return slices.Clone(loaded)
}

// Get returns the cell with the given id.
func Get(id string) (Cell, error) {
	for _, c := range loaded {
		if c.ID == id {
			return c, nil
		}
	}
	return Cell{}, fmt.Errorf("matrix: no cell with id %q (known ids: %v)", id, ids())
}

// Default returns the cell marked `default: true`. It panics when the file does
// not declare exactly one — a condition the package's unit tests assert against.
func Default() Cell {
	var found []Cell
	for _, c := range loaded {
		if c.Default {
			found = append(found, c)
		}
	}
	if len(found) != 1 {
		panic(fmt.Sprintf("matrix: want exactly one default cell, got %d", len(found)))
	}
	return found[0]
}

// FromEnv resolves the cell named by EnvCell, falling back to Default when the
// variable is unset or empty.
func FromEnv() (Cell, error) {
	id := os.Getenv(EnvCell)
	if id == "" {
		return Default(), nil
	}
	return Get(id)
}

// ForTier returns the cells participating in the given tier, in file order.
func ForTier(tier string) []Cell {
	var out []Cell
	for _, c := range loaded {
		if c.InTier(tier) {
			out = append(out, c)
		}
	}
	return out
}

func ids() []string {
	out := make([]string, 0, len(loaded))
	for _, c := range loaded {
		out = append(out, c.ID)
	}
	return out
}
