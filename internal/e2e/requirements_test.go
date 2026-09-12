package e2e

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/monedula-dev/monedula-gitops/internal/matrix"
)

// TestScenarioRequiresAreSatisfiable is pure metadata consistency: it needs no
// Docker and no containers, so it lives here (an ordinary untagged test that
// runs on every `go test ./...`) rather than behind the `e2e` build tag in
// test/e2e/cli, whose TestMain requires a running Docker daemon.
//
// Every capability a scenario requires must be spelled from the matrix's
// closed capability vocabulary and offered by at least one e2e-tier cell —
// otherwise the scenario silently stops running everywhere. The test also
// counts the requirements it actually parsed and fails if that count is zero,
// so a parsing regression (a renamed struct tag, a future strict unmarshaller,
// a scenario author writing `require:` instead of `requires:`) is caught
// loudly instead of leaving the assertion loop vacuously green.
func TestScenarioRequiresAreSatisfiable(t *testing.T) {
	root := filepath.Join("..", "..", "scenarios")
	entries, err := os.ReadDir(root)
	require.NoError(t, err)

	e2eCells := matrix.ForTier(matrix.TierE2E)
	require.NotEmpty(t, e2eCells)

	validCapabilities := []string{
		matrix.CapTopics,
		matrix.CapACLs,
		matrix.CapQuotas,
		matrix.CapSCRAM,
		matrix.CapSchemaRegistry,
		matrix.CapMDS,
	}

	totalRequirements := 0
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		dir := filepath.Join(root, e.Name())
		if _, err := os.Stat(filepath.Join(dir, "scenario.yaml")); err != nil {
			continue
		}
		sc, err := LoadScenario(dir)
		require.NoError(t, err, "loading %s", e.Name())

		for _, capability := range sc.Requires {
			totalRequirements++

			assert.Contains(t, validCapabilities, capability,
				"scenario %s requires %q, which is not in the matrix capability vocabulary", e.Name(), capability)

			var satisfied bool
			for _, c := range e2eCells {
				if c.Supports(capability) {
					satisfied = true
				}
			}
			assert.True(t, satisfied,
				"scenario %s requires %q, which no e2e cell offers", e.Name(), capability)
		}
	}

	// If Requires ever stops parsing (renamed struct tag, a switch to a strict
	// unmarshaller, a typo'd YAML key), this loop's body never executes and the
	// assertions above pass vacuously. Fail loudly instead: as of writing, four
	// scenarios declare requires:, so the true count is never zero. Don't
	// hard-code that number — just assert it's non-zero.
	assert.NotZero(t, totalRequirements,
		"no scenario.yaml requirements were parsed at all; Requires may have stopped binding")
}
