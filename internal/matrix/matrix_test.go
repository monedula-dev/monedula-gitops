package matrix

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// overlayNamePattern is the safe vocabulary for an Overlays entry: it gets
// interpolated into a filename (compose.<name>.yaml) by the e2e runner, so it
// must not contain path separators, "..", or anything else that could escape
// the profile directory.
var overlayNamePattern = regexp.MustCompile(`^[a-z0-9]+$`)

func TestCellsLoad(t *testing.T) {
	cells := Cells()
	require.NotEmpty(t, cells, "versions.yaml produced no cells")

	seen := map[string]bool{}
	for _, c := range cells {
		assert.NotEmpty(t, c.ID, "cell with empty id")
		assert.False(t, seen[c.ID], "duplicate cell id %q", c.ID)
		seen[c.ID] = true

		assert.Contains(t, []string{FamilyApache, FamilyConfluent}, c.Family,
			"cell %q has unknown family %q", c.ID, c.Family)
		assert.NotEmpty(t, c.Image, "cell %q has no image", c.ID)
		assert.NotEmpty(t, c.Tiers, "cell %q declares no tiers", c.ID)

		for _, tier := range c.Tiers {
			assert.Contains(t, []string{TierIntegration, TierE2E}, tier,
				"cell %q has unknown tier %q", c.ID, tier)
		}
		for _, capability := range c.Capabilities {
			assert.Contains(t,
				[]string{CapTopics, CapACLs, CapQuotas, CapSCRAM, CapSchemaRegistry, CapMDS},
				capability, "cell %q has unknown capability %q", c.ID, capability)
		}
		for _, overlay := range c.Overlays {
			assert.NotEmpty(t, overlay, "cell %q declares an empty overlay name", c.ID)
			assert.Regexp(t, overlayNamePattern, overlay,
				"cell %q has overlay name %q outside the safe pattern (lowercase letters and digits only)",
				c.ID, overlay)
		}

		// The e2e runner's profile-gated scenario tests only know about "" and
		// "auth-mds" (test/e2e/cli/runner_test.go skips on Profile != "" or
		// Profile != "auth-mds"). Any other value isn't a typo that fails loudly:
		// every one of those tests would just skip, go test would exit 0, and the
		// cell would report a green PASS while running zero of its scenarios.
		assert.Contains(t, []string{"", "auth-mds"}, c.Profile,
			"cell %q has profile %q, which no e2e runner test recognises — it would silently skip every profile-gated scenario for this cell instead of failing", c.ID, c.Profile)
	}
}

func TestExactlyOneDefault(t *testing.T) {
	var defaults []string
	for _, c := range Cells() {
		if c.Default {
			defaults = append(defaults, c.ID)
		}
	}
	require.Len(t, defaults, 1, "want exactly one cell with default: true, got %v", defaults)
}

// A cell claiming the schemaregistry capability must say which image provides
// it, and an image without the claim would never be started.
func TestSchemaRegistryImageMatchesCapability(t *testing.T) {
	for _, c := range Cells() {
		assert.Equal(t, c.Supports(CapSchemaRegistry), c.SchemaRegistryImage != "",
			"cell %q: schemaregistry capability and schemaRegistryImage disagree", c.ID)
	}
}

func TestGet(t *testing.T) {
	c, err := Get("cp-8.3")
	require.NoError(t, err)
	assert.Equal(t, "confluentinc/cp-kafka:8.3.1", c.Image)

	_, err = Get("no-such-cell")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no-such-cell")
}

func TestFromEnvUnsetReturnsDefault(t *testing.T) {
	t.Setenv(EnvCell, "")
	c, err := FromEnv()
	require.NoError(t, err)
	assert.True(t, c.Default, "FromEnv with unset %s returned non-default cell %q", EnvCell, c.ID)
	assert.Equal(t, Default().ID, c.ID)
}

func TestFromEnvNamedCell(t *testing.T) {
	t.Setenv(EnvCell, "ak-4.0")
	c, err := FromEnv()
	require.NoError(t, err)
	assert.Equal(t, "ak-4.0", c.ID)
	assert.Equal(t, FamilyApache, c.Family)
}

func TestFromEnvUnknownCellErrors(t *testing.T) {
	t.Setenv(EnvCell, "ak-9.9")
	_, err := FromEnv()
	require.Error(t, err)
}

func TestSupports(t *testing.T) {
	c, err := Get("ak-3.9")
	require.NoError(t, err)
	assert.True(t, c.Supports(CapTopics))
	assert.False(t, c.Supports(CapSchemaRegistry))
	assert.False(t, c.Supports(CapMDS))
}

func TestForTier(t *testing.T) {
	integration := ForTier(TierIntegration)
	e2e := ForTier(TierE2E)

	require.NotEmpty(t, integration)
	require.NotEmpty(t, e2e)

	for _, c := range integration {
		assert.True(t, c.InTier(TierIntegration))
	}
	// Every tier must have at least one cell able to exercise Schema Registry,
	// or the SR code paths silently stop being covered.
	var srCells int
	for _, c := range e2e {
		if c.Supports(CapSchemaRegistry) {
			srCells++
		}
	}
	assert.Positive(t, srCells, "no e2e cell supports schemaregistry")
}

// The MDS profile is reachable only through a cell that names it. If no such
// cell exists in the e2e tier, scenario 22 silently stops running.
func TestMDSProfileHasACell(t *testing.T) {
	var found bool
	for _, c := range ForTier(TierE2E) {
		if c.Profile == "auth-mds" && c.Supports(CapMDS) {
			found = true
		}
	}
	assert.True(t, found, "no e2e cell provides the auth-mds profile")
}
