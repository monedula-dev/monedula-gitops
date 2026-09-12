# Version-Matrix Integration and E2E Testing — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run the container-backed test suites in CI against a declared set of Apache Kafka and Confluent Platform versions, driven by one source-of-truth file.

**Architecture:** A new `internal/matrix` package embeds `versions.yaml` — a list of broker "cells", each naming an image, a family (`apache` or `confluent`), the tiers it runs in, and the capabilities it supports. The integration tier (Testcontainers) and the e2e tier (Docker Compose scenarios) both resolve their cell from `MONEDULA_MATRIX_CELL` and skip capabilities the cell lacks, with a stated reason. A small Go tool renders the cell list as CI matrix JSON and as a README table, so GitHub Actions and the docs never restate the versions.

**Tech Stack:** Go 1.26, `gopkg.in/yaml.v3`, `testcontainers-go` v0.42, Docker Compose v2 file format, GitHub Actions.

## Global Constraints

- Module path: `github.com/monedula-dev/monedula-gitops`. Go 1.26.0.
- `go build ./...` and `go test ./...` (no build tags) MUST stay Docker-free and MUST NOT pull `testcontainers-go` into the non-test dependency graph. Any file importing testcontainers carries `//go:build integration || e2e`.
- Capability vocabulary is closed: `topics`, `acls`, `quotas`, `scram`, `schemaregistry`, `mds`.
- Family vocabulary is closed: `apache`, `confluent`. Tier vocabulary is closed: `integration`, `e2e`.
- Environment variable selecting the cell: `MONEDULA_MATRIX_CELL`. Unset means the cell with `default: true`.
- Compose image parameterisation always carries a literal default (`${VAR:-image:tag}`) so a bare `docker compose up` keeps working with no environment set.
- `gofmt` clean, `go vet ./...` clean, `golangci-lint` v2.12.2 clean. CI enforces `test -z "$(gofmt -l .)"` — but that command is **useless on a Windows checkout**: this repo has `core.autocrlf=true` and no `.gitattributes`, so every pre-existing `.go` file is CRLF in the working copy and `gofmt -l .` flags all ~200 of them. CI checks out LF on Linux and is clean. When working on Windows, scope the check to the directories you touched (`gofmt -l internal/matrix/`), and never "fix" a file you did not otherwise change.
- Existing behaviour preserved: container-backed tests skip cleanly when Docker is unavailable; the e2e suite FAILS without Docker unless `MONEDULA_E2E_SKIP_WITHOUT_DOCKER=1`.
- Commit trailer on every commit: `Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>`.

## Verified Facts

These were confirmed against live containers on 2026-09-12. Do not re-litigate them; do not "fix" code that depends on them.

1. `apache/kafka:4.3.1` mirrors the Confluent image layout at `/etc/kafka/docker/` (not `/etc/confluent/docker/`): `bash-config`, `configureDefaults`, `configure`, `launch`, `run`. Entrypoint is `/__cacert_entrypoint.sh`, cmd is `/etc/kafka/docker/run`. `KAFKA_*` environment variables translate to broker properties exactly as in the Confluent images.
2. `/opt/kafka/bin` is **NOT** on `PATH` in `apache/kafka:4.3.1`. `PATH` is `/opt/java/openjdk/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin`. Kafka scripts must be invoked by absolute path and carry a `.sh` suffix (`/opt/kafka/bin/kafka-configs.sh`).
3. `/etc/kafka/docker/launch` formats KRaft storage itself and `configureDefaults` supplies a fallback `CLUSTER_ID`, so an apache starter script needs no explicit `kafka-storage format` line (unlike the Confluent path, where the testcontainers module injects one).
4. `confluentinc/cp-schema-registry:8.3.1` has **no** `curl`, `wget`, or `nc`. It has `python3` and `bash`. `confluentinc/cp-schema-registry:8.0.0` has all of them. The existing curl-based SR healthcheck therefore breaks on the version bump and must be rewritten using `python3`, which is present across 7.6 through 8.3.
5. `apache/kafka:4.3.1` boots healthy on the unmodified `shared-sasl` broker configuration when only the image and the healthcheck command are overridden. SCRAM bootstrap via `/opt/kafka/bin/kafka-configs.sh` succeeds.
6. `confluentinc/cp-schema-registry:8.3.1` works against an `apache/kafka:4.3.1` broker. `monedula-gitops doctor` against that stack returns exit 0 with `config`, `kafka-connect`, `kafka-admin`, `acl-read` and `schema-registry` all PASS.
7. `testcontainers-go/modules/kafka` v0.42.0 cannot run `apache/kafka`: its starter script hard-codes `/etc/confluent/docker/*`. Confirmed at `kafka.go:22-30`.

---

### Task 1: The `internal/matrix` cell registry

**Files:**
- Create: `internal/matrix/versions.yaml`
- Create: `internal/matrix/matrix.go`
- Test: `internal/matrix/matrix_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces:
  ```go
  type Cell struct {
      ID, Family, Image, SchemaRegistryImage, Profile string
      Tiers, Capabilities []string
      Default bool
  }
  const (
      FamilyApache    = "apache"
      FamilyConfluent = "confluent"
      TierIntegration = "integration"
      TierE2E         = "e2e"
      CapTopics         = "topics"
      CapACLs           = "acls"
      CapQuotas         = "quotas"
      CapSCRAM          = "scram"
      CapSchemaRegistry = "schemaregistry"
      CapMDS            = "mds"
      EnvCell = "MONEDULA_MATRIX_CELL"
  )
  func Cells() []Cell
  func Get(id string) (Cell, error)
  func Default() Cell
  func FromEnv() (Cell, error)
  func ForTier(tier string) []Cell
  func (c Cell) Supports(capability string) bool
  func (c Cell) InTier(tier string) bool
  ```
  Note: the design doc sketched `Tier(string) bool`; the method is named `InTier` here because `Tier` reads as an accessor. Use `InTier` everywhere.

- [ ] **Step 1: Write `versions.yaml`**

Create `internal/matrix/versions.yaml`:

```yaml
# Single source of truth for the Kafka/Confluent versions this project validates
# against. Consumed by:
#   - internal/matrix (the Go loader, embedded via go:embed)
#   - hack/matrix-cells (CI matrix JSON + the README support table)
#   - the integration tier (internal/kafka/franz, internal/schemaregistry/confluent)
#   - the e2e tier (test/e2e/cli)
#
# Cell-to-profile rule: a cell WITHOUT `profile` runs every compose profile
# EXCEPT auth-mds. The auth-mds profile runs ONLY under a cell that names it.
# This keeps MDS on its pinned cp-server line without dropping it from the suite.
#
# Version rationale: cover the breaks that change behaviour, not every minor.
# Apache 3.9 -> 4.0 is the KRaft-only / ZooKeeper-removal boundary. CP 7.x -> 8.x
# removed the scope-wide POST /security/1.0/lookup/rolebindings MDS endpoint,
# which is why auth-mds is pinned to the 7.6 line.
# Support floor: Apache Kafka 3.9, Confluent Platform 7.6.
cells:
  - id: ak-3.9
    family: apache
    image: apache/kafka:3.9.2
    tiers: [integration]
    capabilities: [topics, acls, quotas, scram]

  - id: ak-4.0
    family: apache
    image: apache/kafka:4.0.2
    tiers: [integration]
    capabilities: [topics, acls, quotas, scram]

  # Deliberately pairs an Apache broker with a Confluent Schema Registry: a
  # common real-world combination that nothing else in the suite covers.
  - id: ak-4.3
    family: apache
    image: apache/kafka:4.3.1
    schemaRegistryImage: confluentinc/cp-schema-registry:8.3.1
    tiers: [integration, e2e]
    capabilities: [topics, acls, quotas, scram, schemaregistry]

  - id: cp-7.6
    family: confluent
    image: confluentinc/cp-kafka:7.6.13
    schemaRegistryImage: confluentinc/cp-schema-registry:7.6.13
    tiers: [integration]
    capabilities: [topics, acls, quotas, scram, schemaregistry]

  - id: cp-7.9
    family: confluent
    image: confluentinc/cp-kafka:7.9.9
    schemaRegistryImage: confluentinc/cp-schema-registry:7.9.9
    tiers: [integration, e2e]
    capabilities: [topics, acls, quotas, scram, schemaregistry]

  - id: cp-8.0
    family: confluent
    image: confluentinc/cp-kafka:8.0.7
    schemaRegistryImage: confluentinc/cp-schema-registry:8.0.7
    tiers: [integration]
    capabilities: [topics, acls, quotas, scram, schemaregistry]

  - id: cp-8.3
    family: confluent
    image: confluentinc/cp-kafka:8.3.1
    schemaRegistryImage: confluentinc/cp-schema-registry:8.3.1
    default: true
    tiers: [integration, e2e]
    capabilities: [topics, acls, quotas, scram, schemaregistry]

  # cp-server carries MDS. Pinned to 7.6: CP 8.x removed the scope-wide
  # lookup/rolebindings endpoint the product's MDS client uses.
  - id: cp-7.6-mds
    family: confluent
    image: confluentinc/cp-server:7.6.13
    profile: auth-mds
    tiers: [e2e]
    capabilities: [topics, acls, quotas, scram, mds]
```

- [ ] **Step 2: Write the failing tests**

Create `internal/matrix/matrix_test.go`:

```go
package matrix

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
```

- [ ] **Step 3: Run tests to verify they fail**

```bash
go test ./internal/matrix/ -v
```

Expected: FAIL — the package does not compile (`undefined: Cells`, `undefined: Cell`, etc.).

- [ ] **Step 4: Write the implementation**

Create `internal/matrix/matrix.go`:

```go
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
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
go test ./internal/matrix/ -v
```

Expected: PASS, all 10 tests.

- [ ] **Step 6: Verify the production build stays Docker-free**

```bash
go build ./... && go vet ./... && test -z "$(gofmt -l internal/matrix/)" && echo CLEAN
```

Expected: `CLEAN`.

- [ ] **Step 7: Commit**

```bash
git add internal/matrix/
git commit -m "feat(matrix): add version cell registry backed by versions.yaml" -m "Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 2: `hack/matrix-cells` — CI matrix JSON and the README table

**Files:**
- Create: `hack/matrix-cells/main.go`
- Test: `hack/matrix-cells/main_test.go`

**Interfaces:**
- Consumes: `matrix.ForTier`, `matrix.Cells`, `matrix.Cell` (Task 1).
- Produces: a CLI with two modes —
  - `go run ./hack/matrix-cells -tier=integration` prints a compact JSON array of cell ids, e.g. `["ak-3.9","ak-4.0",...]`, for `fromJSON` in GitHub Actions.
  - `go run ./hack/matrix-cells -readme=README.md` rewrites the block between the markers `<!-- BEGIN GENERATED: version-matrix -->` and `<!-- END GENERATED: version-matrix -->` in place.
  - Internal funcs later tasks do not use, but the tests do: `tierJSON(tier string) (string, error)`, `readmeTable() string`, `spliceBlock(doc, block string) (string, error)`.

A Go tool rather than `yq` in the workflow: it reuses the validated loader, so the CI matrix and the docs cannot disagree with the tests, and there is no dependency on which `yq` dialect a runner ships.

- [ ] **Step 1: Write the failing tests**

Create `hack/matrix-cells/main_test.go`:

```go
package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/monedula-dev/monedula-gitops/internal/matrix"
)

func TestTierJSONIsCompactArray(t *testing.T) {
	got, err := tierJSON(matrix.TierIntegration)
	require.NoError(t, err)

	assert.True(t, strings.HasPrefix(got, `["`), "want a JSON array, got %q", got)
	assert.True(t, strings.HasSuffix(got, `"]`), "want a JSON array, got %q", got)
	assert.NotContains(t, got, "\n", "GITHUB_OUTPUT values must be single-line")
	assert.Contains(t, got, `"cp-8.3"`)
}

func TestTierJSONUnknownTierErrors(t *testing.T) {
	_, err := tierJSON("nonsense")
	require.Error(t, err)
}

func TestReadmeTableListsEveryCell(t *testing.T) {
	table := readmeTable()
	for _, c := range matrix.Cells() {
		assert.Contains(t, table, c.Image, "cell %q image missing from the table", c.ID)
	}
	assert.True(t, strings.HasPrefix(table, "|"), "want a markdown table, got %q", table[:20])
}

func TestSpliceBlockReplacesBetweenMarkers(t *testing.T) {
	doc := "before\n" + beginMarker + "\nstale\n" + endMarker + "\nafter\n"
	got, err := spliceBlock(doc, "fresh")
	require.NoError(t, err)

	assert.Equal(t, "before\n"+beginMarker+"\nfresh\n"+endMarker+"\nafter\n", got)
	assert.NotContains(t, got, "stale")
}

func TestSpliceBlockMissingMarkersErrors(t *testing.T) {
	_, err := spliceBlock("no markers here", "fresh")
	require.Error(t, err)
	assert.Contains(t, err.Error(), beginMarker)
}

func TestSpliceBlockIsIdempotent(t *testing.T) {
	doc := "before\n" + beginMarker + "\nstale\n" + endMarker + "\nafter\n"
	once, err := spliceBlock(doc, "fresh")
	require.NoError(t, err)
	twice, err := spliceBlock(once, "fresh")
	require.NoError(t, err)
	assert.Equal(t, once, twice)
}
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
go test ./hack/matrix-cells/ -v
```

Expected: FAIL — `undefined: tierJSON`, `undefined: beginMarker`, etc.

- [ ] **Step 3: Write the implementation**

Create `hack/matrix-cells/main.go`:

```go
// Command matrix-cells renders internal/matrix/versions.yaml for consumers that
// cannot parse it themselves: the GitHub Actions job matrix (as a JSON array of
// cell ids) and the README support table (as a markdown block between markers).
//
// Using this tool rather than yq in the workflow means CI and the docs share the
// loader the unit tests cover, so they cannot disagree with each other.
//
// Usage:
//
//	go run ./hack/matrix-cells -tier=integration   # ["ak-3.9","ak-4.0",...]
//	go run ./hack/matrix-cells -readme=README.md   # rewrite the block in place
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/monedula-dev/monedula-gitops/internal/matrix"
)

const (
	beginMarker = "<!-- BEGIN GENERATED: version-matrix -->"
	endMarker   = "<!-- END GENERATED: version-matrix -->"
)

func main() {
	tier := flag.String("tier", "", "print the cell ids for this tier as a JSON array (integration|e2e)")
	readme := flag.String("readme", "", "rewrite the generated version-matrix block in this markdown file")
	flag.Parse()

	switch {
	case *tier != "":
		out, err := tierJSON(*tier)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Println(out)
	case *readme != "":
		if err := updateReadme(*readme); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	default:
		flag.Usage()
		os.Exit(2)
	}
}

// tierJSON renders the tier's cell ids as a single-line JSON array, the shape
// GitHub Actions' fromJSON expects in a job output.
func tierJSON(tier string) (string, error) {
	cells := matrix.ForTier(tier)
	if len(cells) == 0 {
		return "", fmt.Errorf("matrix-cells: no cells in tier %q", tier)
	}
	ids := make([]string, 0, len(cells))
	for _, c := range cells {
		ids = append(ids, c.ID)
	}
	b, err := json.Marshal(ids)
	if err != nil {
		return "", fmt.Errorf("matrix-cells: marshalling tier %q: %w", tier, err)
	}
	return string(b), nil
}

// readmeTable renders every cell as a markdown table. No trailing newline: the
// splice adds the line breaks around the block.
func readmeTable() string {
	var b strings.Builder
	b.WriteString("| Cell | Broker image | Schema Registry image | Tiers | Capabilities |\n")
	b.WriteString("|---|---|---|---|---|\n")
	for _, c := range matrix.Cells() {
		sr := c.SchemaRegistryImage
		if sr == "" {
			sr = "—"
		}
		fmt.Fprintf(&b, "| `%s` | `%s` | `%s` | %s | %s |\n",
			c.ID, c.Image, sr,
			strings.Join(c.Tiers, ", "),
			strings.Join(c.Capabilities, ", "))
	}
	return strings.TrimRight(b.String(), "\n")
}

// spliceBlock replaces the content between the markers, leaving the markers and
// everything around them untouched.
func spliceBlock(doc, block string) (string, error) {
	start := strings.Index(doc, beginMarker)
	if start < 0 {
		return "", fmt.Errorf("matrix-cells: marker %q not found", beginMarker)
	}
	end := strings.Index(doc, endMarker)
	if end < 0 {
		return "", fmt.Errorf("matrix-cells: marker %q not found", endMarker)
	}
	if end < start {
		return "", fmt.Errorf("matrix-cells: %q appears before %q", endMarker, beginMarker)
	}
	return doc[:start+len(beginMarker)] + "\n" + block + "\n" + doc[end:], nil
}

func updateReadme(path string) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("matrix-cells: reading %s: %w", path, err)
	}
	out, err := spliceBlock(string(raw), readmeTable())
	if err != nil {
		return err
	}
	if out == string(raw) {
		return nil // already current; don't touch the mtime
	}
	if err := os.WriteFile(path, []byte(out), 0o644); err != nil {
		return fmt.Errorf("matrix-cells: writing %s: %w", path, err)
	}
	return nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
go test ./hack/matrix-cells/ -v
```

Expected: PASS, all 6 tests.

- [ ] **Step 5: Verify the CLI by hand**

```bash
go run ./hack/matrix-cells -tier=integration
go run ./hack/matrix-cells -tier=e2e
```

Expected, exactly:
```
["ak-3.9","ak-4.0","ak-4.3","cp-7.6","cp-7.9","cp-8.0","cp-8.3"]
["ak-4.3","cp-7.9","cp-8.3","cp-7.6-mds"]
```

- [ ] **Step 6: Commit**

```bash
git add hack/matrix-cells/
git commit -m "feat(matrix): add matrix-cells tool for CI JSON and README table" -m "Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 3: `matrix.StartBroker` — family-dispatched container start

**Files:**
- Create: `internal/matrix/broker.go` (build tag `integration || e2e`)
- Test: `internal/matrix/broker_test.go` (build tag `integration`)

**Interfaces:**
- Consumes: `matrix.Cell`, `matrix.FamilyApache`, `matrix.FamilyConfluent` (Task 1).
- Produces:
  ```go
  type Broker struct {
      testcontainers.Container
      Bootstrap []string // host-reachable, e.g. ["127.0.0.1:54321"]
      Internal  string   // in-network "alias:9092"; empty unless WithNetwork was given
  }
  type Option func(*startConfig)
  func WithNetwork(networkName, alias string) Option
  func StartBroker(ctx context.Context, c Cell, opts ...Option) (*Broker, error)
  func SkipUnlessDocker(t *testing.T)
  func SkipWithoutCapability(t *testing.T, c Cell, capability string)
  ```

The Confluent path delegates to `testcontainers-go/modules/kafka`. The Apache path cannot (Verified Fact 7) and uses a generic container with the same starter-script technique, pointed at `/etc/kafka/docker/*` (Verified Facts 1 and 3).

- [ ] **Step 1: Write the failing test**

Create `internal/matrix/broker_test.go`:

```go
//go:build integration

package matrix

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// StartBroker must produce a broker that answers a real Kafka API call, for
// whichever cell the environment selects — that is the whole point of the
// family dispatch.
func TestStartBrokerServesMetadata(t *testing.T) {
	SkipUnlessDocker(t)

	cell, err := FromEnv()
	require.NoError(t, err)
	t.Logf("cell %s (%s, %s)", cell.ID, cell.Family, cell.Image)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	broker, err := StartBroker(ctx, cell)
	require.NoError(t, err, "starting broker for cell %s", cell.ID)
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(broker); err != nil {
			t.Logf("terminating broker: %v", err)
		}
	})

	require.NotEmpty(t, broker.Bootstrap)

	client, err := kgo.NewClient(kgo.SeedBrokers(broker.Bootstrap...))
	require.NoError(t, err)
	t.Cleanup(client.Close)

	admin := kadm.NewClient(client)
	md, err := admin.Metadata(ctx)
	require.NoError(t, err, "fetching metadata from cell %s", cell.ID)
	assert.NotEmpty(t, md.Brokers, "cell %s reported no brokers", cell.ID)
}
```

This is the only test in this file. `WithNetwork` and `Broker.Internal` are exercised for real by
Task 5's Schema Registry test — a Schema Registry container actually reaching the broker over the
shared network is a stronger test of that path than anything synthetic here, and duplicating it would
mean starting two extra containers per run for no added signal.

- [ ] **Step 2: Run the test to verify it fails**

```bash
go test -tags integration ./internal/matrix/ -run TestStartBrokerServesMetadata -v
```

Expected: FAIL — `undefined: SkipUnlessDocker`, `undefined: StartBroker`.

- [ ] **Step 3: Write the implementation**

Create `internal/matrix/broker.go`:

```go
//go:build integration || e2e

package matrix

// Container start paths for the matrix cells. Behind a build tag so the default
// `go build ./...` never pulls testcontainers into the production dependency
// graph.
//
// Two families, two paths:
//
//   - confluent: the testcontainers kafka module handles it.
//   - apache: the module CANNOT. Its starter script hard-codes the Confluent
//     image layout (/etc/confluent/docker/{bash-config,configure,launch}). The
//     apache/kafka image mirrors that layout under /etc/kafka/docker/ instead,
//     so this file replicates the module's technique with the apache paths.
//     Unlike the Confluent path, no explicit `kafka-storage format` is needed:
//     the apache image's own `launch` formats storage and `configureDefaults`
//     supplies a fallback CLUSTER_ID.

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/testcontainers/testcontainers-go/wait"
)

// apacheStarterScript is the apache/kafka image's own /etc/kafka/docker/run with
// one line inserted: the advertised listeners, which are only knowable after the
// container starts and Docker assigns the host port.
//
// First %s is the host-reachable PLAINTEXT endpoint, second is the container
// hostname used for the in-network BROKER listener.
const apacheStarterScript = `#!/usr/bin/env bash
. /etc/kafka/docker/bash-config
export KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://%s,BROKER://%s:9092
. /etc/kafka/docker/configureDefaults
. /etc/kafka/docker/configure
. /etc/kafka/docker/launch
`

const (
	apacheStarterPath = "/usr/sbin/monedula_start.sh"
	publicPort        = "9093/tcp"
)

// Broker is a started broker container plus the addresses to reach it.
type Broker struct {
	testcontainers.Container
	// Bootstrap is the host-reachable bootstrap server list.
	Bootstrap []string
	// Internal is the in-network "alias:9092" address, empty unless the caller
	// passed WithNetwork.
	Internal string
}

type startConfig struct {
	networkName string
	alias       string
}

// Option customises StartBroker.
type Option func(*startConfig)

// WithNetwork attaches the broker to an existing docker network under the given
// alias, and populates Broker.Internal so sibling containers can reach it.
func WithNetwork(networkName, alias string) Option {
	return func(sc *startConfig) {
		sc.networkName = networkName
		sc.alias = alias
	}
}

// StartBroker starts the broker for a cell. The caller terminates it via
// testcontainers.TerminateContainer.
func StartBroker(ctx context.Context, c Cell, opts ...Option) (*Broker, error) {
	var sc startConfig
	for _, o := range opts {
		o(&sc)
	}

	switch c.Family {
	case FamilyConfluent:
		return startConfluent(ctx, c, sc)
	case FamilyApache:
		return startApache(ctx, c, sc)
	default:
		return nil, fmt.Errorf("matrix: cell %q has unknown family %q", c.ID, c.Family)
	}
}

func startConfluent(ctx context.Context, c Cell, sc startConfig) (*Broker, error) {
	var opts []testcontainers.ContainerCustomizer
	if sc.networkName != "" {
		opts = append(opts,
			testcontainers.WithNetwork([]string{sc.alias}, nil),
			testcontainers.WithNetworkName(sc.networkName),
		)
	}

	ctr, err := tckafka.Run(ctx, c.Image, opts...)
	if err != nil {
		return nil, fmt.Errorf("matrix: starting confluent broker %s: %w", c.Image, err)
	}
	brokers, err := ctr.Brokers(ctx)
	if err != nil {
		return nil, fmt.Errorf("matrix: reading brokers from %s: %w", c.Image, err)
	}
	return &Broker{
		Container: ctr,
		Bootstrap: brokers,
		Internal:  internalAddr(sc.alias),
	}, nil
}

func startApache(ctx context.Context, c Cell, sc startConfig) (*Broker, error) {
	env := map[string]string{
		"KAFKA_LISTENERS":                        "PLAINTEXT://0.0.0.0:9093,BROKER://0.0.0.0:9092,CONTROLLER://0.0.0.0:9094",
		"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":   "BROKER:PLAINTEXT,PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT",
		"KAFKA_INTER_BROKER_LISTENER_NAME":       "BROKER",
		"KAFKA_NODE_ID":                          "1",
		"KAFKA_PROCESS_ROLES":                    "broker,controller",
		"KAFKA_CONTROLLER_LISTENER_NAMES":        "CONTROLLER",
		"KAFKA_CONTROLLER_QUORUM_VOTERS":         "1@localhost:9094",
		"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": "1",
		"KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS":     "1",
		"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
		"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":            "1",
		"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS":         "0",
		"CLUSTER_ID": "4L6g3nShT-eMCtK--X86sw",
	}

	opts := []testcontainers.ContainerCustomizer{
		testcontainers.WithExposedPorts(publicPort),
		testcontainers.WithEnv(env),
		// Hold the container at a shell until the starter script lands, exactly
		// as the testcontainers kafka module does: the advertised listener host
		// port is unknown until Docker has assigned it.
		testcontainers.WithEntrypoint("sh"),
		testcontainers.WithCmd("-c", "while [ ! -f "+apacheStarterPath+" ]; do sleep 0.1; done; bash "+apacheStarterPath),
		testcontainers.WithLifecycleHooks(testcontainers.ContainerLifecycleHooks{
			PostStarts: []testcontainers.ContainerHook{
				func(ctx context.Context, ctr testcontainers.Container) error {
					if err := copyApacheStarter(ctx, ctr, sc.alias); err != nil {
						return fmt.Errorf("copy starter script: %w", err)
					}
					return wait.ForLog("Kafka Server started").
						WaitUntilReady(ctx, ctr)
				},
			},
		}),
	}
	if sc.networkName != "" {
		opts = append(opts,
			testcontainers.WithNetwork([]string{sc.alias}, nil),
			testcontainers.WithNetworkName(sc.networkName),
		)
	}

	ctr, err := testcontainers.Run(ctx, c.Image, opts...)
	if err != nil {
		return nil, fmt.Errorf("matrix: starting apache broker %s: %w", c.Image, err)
	}

	endpoint, err := ctr.PortEndpoint(ctx, publicPort, "")
	if err != nil {
		return nil, fmt.Errorf("matrix: reading endpoint from %s: %w", c.Image, err)
	}
	return &Broker{
		Container: ctr,
		Bootstrap: []string{endpoint},
		Internal:  internalAddr(sc.alias),
	}, nil
}

func copyApacheStarter(ctx context.Context, ctr testcontainers.Container, alias string) error {
	if err := wait.ForMappedPort(publicPort).WaitUntilReady(ctx, ctr); err != nil {
		return fmt.Errorf("wait for mapped port: %w", err)
	}
	endpoint, err := ctr.PortEndpoint(ctx, publicPort, "")
	if err != nil {
		return fmt.Errorf("port endpoint: %w", err)
	}
	inspect, err := ctr.Inspect(ctx)
	if err != nil {
		return fmt.Errorf("inspect: %w", err)
	}
	hostname := inspect.Config.Hostname
	if alias != "" {
		hostname = alias
	}
	script := fmt.Sprintf(apacheStarterScript, endpoint, hostname)
	return ctr.CopyToContainer(ctx, []byte(script), apacheStarterPath, 0o755)
}

func internalAddr(alias string) string {
	if alias == "" {
		return ""
	}
	return alias + ":9092"
}

// SkipUnlessDocker skips the test when Docker is unavailable, matching the
// long-standing behaviour of this repository's integration tests: a Docker-less
// environment never sees a failure.
func SkipUnlessDocker(t *testing.T) {
	t.Helper()
	testcontainers.SkipIfProviderIsNotHealthy(t)
}

// SkipWithoutCapability skips the test with a stated reason when the cell does
// not offer the capability. Never skip silently: a reader must be able to tell
// a pass from a no-op.
func SkipWithoutCapability(t *testing.T, c Cell, capability string) {
	t.Helper()
	if !c.Supports(capability) {
		t.Skipf("cell %s (%s) has no %s capability", c.ID, c.Image, capability)
	}
}

// IsDockerUnavailable reports whether an error from a container start is a
// Docker-connectivity problem rather than a real failure.
func IsDockerUnavailable(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	for _, s := range []string{
		"Cannot connect to the Docker daemon",
		"error during connect",
		"docker daemon is not running",
		"failed to find a viable Docker environment",
	} {
		if strings.Contains(msg, s) {
			return true
		}
	}
	return false
}
```

- [ ] **Step 4: Run the test against the default (Confluent) cell**

```bash
go test -tags integration ./internal/matrix/ -run TestStartBrokerServesMetadata -v -timeout 10m
```

Expected: PASS, log line `cell cp-8.3 (confluent, confluentinc/cp-kafka:8.3.1)`.

- [ ] **Step 5: Run the same test against an Apache cell**

```bash
MONEDULA_MATRIX_CELL=ak-4.3 go test -tags integration ./internal/matrix/ -run TestStartBrokerServesMetadata -v -timeout 10m -count=1
```

Expected: PASS, log line `cell ak-4.3 (apache, apache/kafka:4.3.1)`. `-count=1` is required: the cell comes from an environment variable, which `go test`'s result cache does not track.

If the Apache broker fails to become ready, get the container logs before changing anything — the starter script is the apache image's own `run` with a single added `export`, so a failure is far more likely to be an env-var name than the technique.

- [ ] **Step 6: Run the oldest Apache cell too**

```bash
MONEDULA_MATRIX_CELL=ak-3.9 go test -tags integration ./internal/matrix/ -run TestStartBrokerServesMetadata -v -timeout 10m -count=1
```

Expected: PASS. This is the cell most likely to differ; 3.9 predates some of the 4.x env handling.

- [ ] **Step 7: Commit**

```bash
git add internal/matrix/broker.go internal/matrix/broker_test.go
git commit -m "feat(matrix): add family-dispatched broker starter" -m "Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 4: Rewire the franz integration test onto the matrix

**Files:**
- Modify: `internal/kafka/franz/integration_test.go` (the `kafkaImage` const at line 46 and the `startKafka` helper below it)

**Interfaces:**
- Consumes: `matrix.FromEnv`, `matrix.StartBroker`, `matrix.SkipUnlessDocker`, `matrix.SkipWithoutCapability`, `matrix.Broker` (Tasks 1 and 3).
- Produces: nothing other tasks consume.

- [ ] **Step 1: Read the current helper**

```bash
sed -n 40,120p internal/kafka/franz/integration_test.go
```

Note the existing signature: `startKafka(t *testing.T) *Client`. Keep it — every test in the file calls it, and changing the signature would balloon this task.

- [ ] **Step 2: Replace the image constant and the container start**

Delete the `kafkaImage` constant and its doc comment. In `startKafka`, replace the `tckafka.Run` call and the brokers lookup with:

```go
	// Primary Docker-availability gate.
	matrix.SkipUnlessDocker(t)

	cell, err := matrix.FromEnv()
	if err != nil {
		t.Fatalf("resolving matrix cell: %v", err)
	}
	t.Logf("matrix cell %s (%s)", cell.ID, cell.Image)

	ctx := context.Background()
	broker, err := matrix.StartBroker(ctx, cell)
	if err != nil {
		if matrix.IsDockerUnavailable(err) {
			t.Skipf("Docker not available, skipping integration test: %v", err)
		}
		t.Fatalf("starting kafka container for cell %s: %v", cell.ID, err)
	}
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(broker); err != nil {
			t.Logf("terminating kafka container: %v", err)
		}
	})

	brokers := broker.Bootstrap
	require.NotEmpty(t, brokers, "container returned no brokers")
```

Update the import block: drop `tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"`, add `"github.com/monedula-dev/monedula-gitops/internal/matrix"`. Keep `"github.com/testcontainers/testcontainers-go"` — `TerminateContainer` still comes from it.

- [ ] **Step 3: Gate the capability-specific tests**

Find the tests exercising quotas, SCRAM users, and ACLs:

```bash
grep -n "^func Test" internal/kafka/franz/integration_test.go
```

At the top of each test body whose subject is a capability, add the corresponding guard, resolving the cell first. For example, in a SCRAM-user test:

```go
	cell, err := matrix.FromEnv()
	require.NoError(t, err)
	matrix.SkipWithoutCapability(t, cell, matrix.CapSCRAM)
```

Use `matrix.CapACLs` for ACL tests and `matrix.CapQuotas` for quota tests. Topic tests need no guard: every cell declares `topics`.

- [ ] **Step 4: Run against the default cell**

```bash
go test -tags integration ./internal/kafka/franz/ -v -timeout 15m -count=1
```

Expected: PASS, with `matrix cell cp-8.3 (confluentinc/cp-kafka:8.3.1)` in the log.

- [ ] **Step 5: Run against an Apache cell**

```bash
MONEDULA_MATRIX_CELL=ak-4.3 go test -tags integration ./internal/kafka/franz/ -v -timeout 15m -count=1
```

Expected: PASS. If a test fails here rather than on the Confluent cell, that is a real finding about Apache-vs-Confluent behaviour — record it, do not paper over it with a skip.

- [ ] **Step 6: Verify the untagged build is unaffected**

```bash
go build ./... && go test ./internal/kafka/... && test -z "$(gofmt -l internal/kafka/)" && echo CLEAN
```

Expected: `CLEAN`.

- [ ] **Step 7: Commit**

```bash
git add internal/kafka/franz/integration_test.go
git commit -m "test(kafka): resolve broker image from the version matrix" -m "Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 5: Rewire the Schema Registry integration test onto the matrix

**Files:**
- Modify: `internal/schemaregistry/confluent/integration_test.go` (the `kafkaImage`/`srImage` consts at lines 45-47 and `startSchemaRegistry`)

**Interfaces:**
- Consumes: `matrix.FromEnv`, `matrix.StartBroker`, `matrix.WithNetwork`, `matrix.SkipWithoutCapability`, `matrix.Cell.SchemaRegistryImage` (Tasks 1 and 3).
- Produces: nothing other tasks consume.

This is the task that proves the `ak-4.3` cell: a Confluent Schema Registry against an Apache broker (Verified Fact 6).

- [ ] **Step 1: Replace the constants**

Delete the `kafkaImage` and `srImage` constants, keeping `kafkaAlias` and `srPort`:

```go
const (
	// kafkaAlias is the stable network alias the broker is reachable at from the
	// SR container on the shared network.
	kafkaAlias = "broker"
	// srPort is the SR REST API port inside the container.
	srPort = "8081/tcp"
)
```

- [ ] **Step 2: Rewrite the start helper's broker half**

In `startSchemaRegistry`, after the network is created, replace the kafka container start with:

```go
	cell, err := matrix.FromEnv()
	if err != nil {
		t.Fatalf("resolving matrix cell: %v", err)
	}
	matrix.SkipWithoutCapability(t, cell, matrix.CapSchemaRegistry)
	t.Logf("matrix cell %s: broker=%s sr=%s", cell.ID, cell.Image, cell.SchemaRegistryImage)

	broker, err := matrix.StartBroker(ctx, cell, matrix.WithNetwork(net.Name, kafkaAlias))
	if err != nil {
		if matrix.IsDockerUnavailable(err) {
			t.Skipf("Docker not available, skipping integration test: %v", err)
		}
		t.Fatalf("starting kafka container for cell %s: %v", cell.ID, err)
	}
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(broker); err != nil {
			t.Logf("terminating kafka container: %v", err)
		}
	})
```

- [ ] **Step 3: Point the SR container at the cell's image and the broker's internal address**

In the SR container request, replace the hard-coded `srImage` with `cell.SchemaRegistryImage`, and set the kafkastore bootstrap from `broker.Internal`:

```go
		"SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS": "PLAINTEXT://" + broker.Internal,
```

Update the import block: drop `tckafka`, add `"github.com/monedula-dev/monedula-gitops/internal/matrix"`.

- [ ] **Step 4: Run against the default cell**

```bash
go test -tags integration ./internal/schemaregistry/confluent/ -v -timeout 15m -count=1
```

Expected: PASS, log `matrix cell cp-8.3: broker=confluentinc/cp-kafka:8.3.1 sr=confluentinc/cp-schema-registry:8.3.1`.

- [ ] **Step 5: Run the Apache-broker/Confluent-SR cell**

```bash
MONEDULA_MATRIX_CELL=ak-4.3 go test -tags integration ./internal/schemaregistry/confluent/ -v -timeout 15m -count=1
```

Expected: PASS. This combination was verified by hand on 2026-09-12 (Verified Fact 6), so a failure here is a wiring bug in this task, not an incompatibility.

- [ ] **Step 6: Verify a cell without Schema Registry skips with a reason**

```bash
MONEDULA_MATRIX_CELL=ak-3.9 go test -tags integration ./internal/schemaregistry/confluent/ -v -count=1
```

Expected: every test SKIPs with `cell ak-3.9 (apache/kafka:3.9.2) has no schemaregistry capability`. Confirm the reason is printed — a silent skip is a plan failure.

- [ ] **Step 7: Commit**

```bash
git add internal/schemaregistry/confluent/integration_test.go
git commit -m "test(schemaregistry): resolve broker and SR images from the version matrix" -m "Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 6: Parameterise the compose profiles and fix the Schema Registry healthcheck

**Files:**
- Modify: `scenarios/clusters/shared-sasl/compose.yaml` (lines 24, 82, 101)
- Modify: `scenarios/clusters/auth-sasl-ssl/compose.yaml` (lines 32, 109)
- Modify: `scenarios/clusters/auth-mtls/compose.yaml` (line 23)
- Modify: `scenarios/clusters/auth-oauth/compose.yaml` (line 32)
- Modify: `scenarios/clusters/auth-mds/compose.yaml` (lines 43, 136 — version bump only)
- Create: `scenarios/clusters/shared-sasl/compose.apache.yaml`
- Create: `scenarios/clusters/auth-sasl-ssl/compose.apache.yaml`
- Create: `scenarios/clusters/auth-mtls/compose.apache.yaml`
- Create: `scenarios/clusters/auth-oauth/compose.apache.yaml`

**Interfaces:**
- Consumes: the environment variables `MONEDULA_KAFKA_IMAGE` and `MONEDULA_SR_IMAGE`, exported by Task 7's runner.
- Produces: the overlay filename convention `compose.apache.yaml`, which Task 7's runner selects on `cell.Family == matrix.FamilyApache`.

- [ ] **Step 1: Parameterise the four cp-kafka profiles**

In each of `shared-sasl`, `auth-sasl-ssl`, `auth-mtls`, `auth-oauth`, replace every `image: confluentinc/cp-kafka:8.0.0` with:

```yaml
    image: ${MONEDULA_KAFKA_IMAGE:-confluentinc/cp-kafka:8.3.1}
```

and in `shared-sasl` replace `image: confluentinc/cp-schema-registry:8.0.0` with:

```yaml
    image: ${MONEDULA_SR_IMAGE:-confluentinc/cp-schema-registry:8.3.1}
```

The literal default keeps a bare `docker compose up` working with no environment set.

- [ ] **Step 2: Fix the Schema Registry healthcheck**

This is required, not cosmetic: `cp-schema-registry:8.3.1` ships no `curl`, `wget` or `nc` (Verified Fact 4), so the current healthcheck fails forever on the bumped image. Replace the `schema-registry` healthcheck in `scenarios/clusters/shared-sasl/compose.yaml` with a `python3` probe — `python3` is present in 7.6 through 8.3:

```yaml
    healthcheck:
      # curl/wget/nc were removed from cp-schema-registry between 8.0 and 8.3;
      # python3 is the one HTTP-capable tool present across the whole 7.6-8.3
      # range the matrix covers. A 200 means ready, a 401 means SR is listening
      # but the credentials are wrong — both prove the port is serving, and the
      # scenario itself surfaces a real auth error with a better message.
      test:
        - CMD
        - python3
        - -c
        - |
          import base64, sys, urllib.error, urllib.request
          req = urllib.request.Request('http://localhost:8081/subjects')
          req.add_header('Authorization', 'Basic ' + base64.b64encode(b'sr:sr-secret').decode())
          try:
              sys.exit(0 if urllib.request.urlopen(req, timeout=5).status == 200 else 1)
          except urllib.error.HTTPError as e:
              sys.exit(0 if e.code == 401 else 1)
          except Exception:
              sys.exit(1)
      interval: 5s
      timeout: 10s
      retries: 18
      start_period: 15s
```

- [ ] **Step 3: Verify the bumped stack comes up**

```bash
docker compose -f scenarios/clusters/shared-sasl/compose.yaml -p mon-bump up -d --wait
```

Expected: exit 0, all services healthy. Then:

```bash
docker inspect monedula-quickstart-schema-registry --format '{{.State.Health.Status}}'
```

Expected: `healthy`. If it reports `unhealthy`, read the probe output before touching anything else:

```bash
docker inspect monedula-quickstart-schema-registry --format '{{range .State.Health.Log}}{{.ExitCode}}: {{.Output}}{{end}}' | tail -3
```

Tear down:

```bash
docker compose -f scenarios/clusters/shared-sasl/compose.yaml -p mon-bump down -v
```

- [ ] **Step 4: Write the Apache overlays**

Create `scenarios/clusters/shared-sasl/compose.apache.yaml`:

```yaml
# Overlay applied on top of compose.yaml for matrix cells whose family is
# `apache`, merged as:
#
#   docker compose -f compose.yaml -f compose.apache.yaml ...
#
# The delta is narrow by design. The apache/kafka image mirrors the Confluent
# image's configuration layout (/etc/kafka/docker/* instead of
# /etc/confluent/docker/*) and applies KAFKA_* environment variables to broker
# properties identically, so the whole security/listener configuration in
# compose.yaml carries over untouched. What differs is the tooling:
# /opt/kafka/bin is NOT on PATH, and the scripts carry a .sh suffix.
services:
  kafka:
    image: ${MONEDULA_KAFKA_IMAGE:-apache/kafka:4.3.1}
    healthcheck:
      test: ["CMD", "/opt/kafka/bin/kafka-broker-api-versions.sh", "--bootstrap-server", "kafka:9094"]
      interval: 10s
      timeout: 10s
      retries: 12
      start_period: 20s

  kafka-init:
    image: ${MONEDULA_KAFKA_IMAGE:-apache/kafka:4.3.1}
    command:
      - |
        set -euo pipefail
        /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka:9094 --alter \
          --add-config 'SCRAM-SHA-512=[password=admin-secret]' \
          --entity-type users --entity-name admin
        /opt/kafka/bin/kafka-configs.sh --bootstrap-server kafka:9094 --alter \
          --add-config 'SCRAM-SHA-512=[password=monedula-secret]' \
          --entity-type users --entity-name monedula
        echo "SCRAM users created"
```

For `auth-sasl-ssl`, `auth-mtls`, and `auth-oauth`, create the same file, keeping only the services each profile actually defines and rewriting any Kafka CLI invocation in a `command:` or `healthcheck:` to the `/opt/kafka/bin/*.sh` form. Read each compose file first:

```bash
grep -n "image:\|healthcheck:\|kafka-configs\|kafka-broker-api-versions\|kafka-acls" scenarios/clusters/auth-sasl-ssl/compose.yaml scenarios/clusters/auth-mtls/compose.yaml scenarios/clusters/auth-oauth/compose.yaml
```

Do not add a `schema-registry` override to the overlays: SR is a Confluent image in every cell, and its image already comes from `MONEDULA_SR_IMAGE`.

- [ ] **Step 5: Verify the Apache overlay stack comes up end to end**

```bash
MONEDULA_KAFKA_IMAGE=apache/kafka:4.3.1 MONEDULA_SR_IMAGE=confluentinc/cp-schema-registry:8.3.1 \
  docker compose -f scenarios/clusters/shared-sasl/compose.yaml \
                 -f scenarios/clusters/shared-sasl/compose.apache.yaml \
                 -p mon-ak up -d --wait
```

Expected: exit 0, all services healthy.

Then prove the product talks to it:

```bash
go build -o /tmp/mg ./cmd/monedula-gitops
KAFKA_USERNAME=admin KAFKA_PASSWORD=admin-secret SR_USERNAME=sr SR_PASSWORD=sr-secret \
  /tmp/mg doctor --cluster-config-file scenarios/clusters/shared-sasl/cluster.yaml
```

Expected: exit 0, ending in `Doctor: healthy`, with `kafka-admin`, `acl-read` and `schema-registry` all PASS. This exact invocation was verified by hand on 2026-09-12.

Tear down:

```bash
docker compose -f scenarios/clusters/shared-sasl/compose.yaml -f scenarios/clusters/shared-sasl/compose.apache.yaml -p mon-ak down -v
```

- [ ] **Step 6: Bump the MDS profile**

In `scenarios/clusters/auth-mds/compose.yaml`, change both `confluentinc/cp-server:7.6.1` to `confluentinc/cp-server:7.6.13`. Leave it unparameterised: the cell `cp-7.6-mds` pins this profile by design, and the comment at the top of the file already explains why 8.x is excluded. Update that comment's version reference from `7.6.1` to `7.6.13`, and the two occurrences in `scenarios/clusters/auth-mds/README.md`.

- [ ] **Step 7: Commit**

```bash
git add scenarios/clusters/
git commit -m "test(e2e): parameterise compose images and add apache overlays" -m "cp-schema-registry dropped curl/wget/nc between 8.0 and 8.3, so the SR healthcheck moves to python3, which is present across the whole 7.6-8.3 range." -m "Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 7: Scenario capability gating and cell-aware e2e runner

**Files:**
- Modify: `scenarios/22-rolebinding/scenario.yaml`
- Modify: `scenarios/09-register-schema/scenario.yaml`, `scenarios/18-schema-evolution/scenario.yaml`, `scenarios/23-schema-import-round-trip/scenario.yaml`
- Modify: `scenarios/README.md` (the "Scenario convention" section)
- Modify: `test/e2e/cli/runner_test.go` (`TestMain`, `composeUp`, `composeUpMDS`, and the five profile tests at lines 510-641)

**Interfaces:**
- Consumes: `matrix.FromEnv`, `matrix.Cell`, `matrix.FamilyApache`, `matrix.CapMDS`, `matrix.CapSchemaRegistry` (Tasks 1 and 3); the `compose.apache.yaml` convention (Task 6).
- Produces: the `requires:` key in `scenario.yaml`; `MONEDULA_KAFKA_IMAGE` / `MONEDULA_SR_IMAGE` exported to compose.

- [ ] **Step 1: Add `requires:` to the capability-dependent scenarios**

In `scenarios/22-rolebinding/scenario.yaml`, append:

```yaml
requires: [mds]
```

In each of `09-register-schema`, `18-schema-evolution`, `23-schema-import-round-trip`, append:

```yaml
requires: [schemaregistry]
```

- [ ] **Step 2: Document the key**

In `scenarios/README.md`, in the "Scenario convention" bullet list, change the `scenario.yaml` bullet to:

```markdown
- `scenario.yaml` — metadata: `title`, `modes`, `cluster`, `summary`, and optional
  `requires` (capabilities from the version matrix — `schemaregistry`, `mds` — that the
  broker cell must offer; the runner skips the scenario, with a stated reason, on cells
  that do not)
```

- [ ] **Step 3: Write the failing test for scenario gating**

Add to `test/e2e/cli/runner_test.go`:

```go
// Every capability a scenario requires must be spelled from the matrix
// vocabulary and offered by at least one e2e cell — otherwise the scenario
// silently stops running everywhere.
func TestScenarioRequiresAreSatisfiable(t *testing.T) {
	entries, err := os.ReadDir(filepath.Join(repoRoot, "scenarios"))
	require.NoError(t, err)

	e2eCells := matrix.ForTier(matrix.TierE2E)
	require.NotEmpty(t, e2eCells)

	for _, e := range entries {
		if !e.IsDir() || e.Name() == "clusters" {
			continue
		}
		sc, err := loadScenario(filepath.Join(repoRoot, "scenarios", e.Name()))
		require.NoError(t, err, "loading %s", e.Name())

		for _, capability := range sc.Requires {
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
}
```

- [ ] **Step 4: Run it to verify it fails**

```bash
go test -tags e2e ./test/e2e/cli/ -run TestScenarioRequiresAreSatisfiable -v
```

Expected: FAIL — the scenario struct has no `Requires` field (the exact message depends on how `loadScenario` is currently defined; find it with `grep -n "scenario.yaml\|type scenario" test/e2e/cli/runner_test.go`).

- [ ] **Step 5: Add the `Requires` field and the runtime gate**

Add `Requires []string \`yaml:"requires"\`` to the scenario metadata struct. Then in `runCLIScenario`, immediately after the scenario is loaded:

```go
	for _, capability := range sc.Requires {
		if !cell.Supports(capability) {
			t.Skipf("scenario %s requires %q; cell %s (%s) does not offer it",
				filepath.Base(scenarioDir), capability, cell.ID, cell.Image)
		}
	}
```

`cell` comes from a package-level variable set in `TestMain`:

```go
// cell is the matrix cell this run exercises, resolved once in TestMain.
var cell matrix.Cell
```

and in `TestMain`, after `repoRoot` is resolved:

```go
	var err error
	cell, err = matrix.FromEnv()
	if err != nil {
		fmt.Fprintf(os.Stderr, "resolving matrix cell: %v\n", err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "\nmonedula-gitops e2e: cell %s (%s)\n\n", cell.ID, cell.Image)
```

- [ ] **Step 6: Export the images to compose and select the overlay**

Add a helper next to `composeUp`:

```go
// composeFiles returns the -f arguments for a profile under the active cell:
// the base file, plus the apache overlay when the cell's broker is an Apache
// image. See scenarios/clusters/*/compose.apache.yaml for what the overlay
// changes and why the delta is as narrow as it is.
func composeFiles(profileDir string) []string {
	files := []string{"-f", filepath.Join(profileDir, "compose.yaml")}
	if cell.Family == matrix.FamilyApache {
		overlay := filepath.Join(profileDir, "compose.apache.yaml")
		if _, err := os.Stat(overlay); err == nil {
			files = append(files, "-f", overlay)
		}
	}
	return files
}

// composeEnv returns the environment compose needs to resolve the image
// placeholders in the profile files.
func composeEnv() []string {
	env := append(os.Environ(), "MONEDULA_KAFKA_IMAGE="+cell.Image)
	if cell.SchemaRegistryImage != "" {
		env = append(env, "MONEDULA_SR_IMAGE="+cell.SchemaRegistryImage)
	}
	return env
}
```

Then in `composeUp`, `composeDown`, `composeUpMDS` and `waitForInit`, replace the hard-coded `"-f", composeFile` argument pairs with `composeFiles(profileDir)...` and set `cmd.Env = composeEnv()` on every `exec.Command("docker", ...)` invocation. `composeDown` takes `profileDir` already, so its signature does not change.

- [ ] **Step 7: Gate the profile tests on the cell**

At the top of each of the five profile test functions, add the appropriate guard.

`TestAuthMDSScenarios` runs only under a cell that names the profile:

```go
	if cell.Profile != "auth-mds" {
		t.Skipf("auth-mds runs only under a cell that pins it; active cell is %s", cell.ID)
	}
```

The other four skip when the cell pins a different profile:

```go
	if cell.Profile != "" {
		t.Skipf("cell %s is pinned to profile %s", cell.ID, cell.Profile)
	}
```

- [ ] **Step 8: Run the gating test and the full suite on the default cell**

```bash
go test -tags e2e ./test/e2e/cli/ -run TestScenarioRequiresAreSatisfiable -v
```

Expected: PASS.

```bash
go test -tags e2e ./test/e2e/cli/ -v -timeout 60m -count=1
```

Expected: PASS. `TestAuthMDSScenarios` SKIPs with `auth-mds runs only under a cell that pins it; active cell is cp-8.3`.

- [ ] **Step 9: Run the MDS cell**

```bash
MONEDULA_MATRIX_CELL=cp-7.6-mds go test -tags e2e ./test/e2e/cli/ -run TestAuthMDSScenarios -v -timeout 30m -count=1
```

Expected: PASS. The other four profile tests SKIP with `cell cp-7.6-mds is pinned to profile auth-mds`.

- [ ] **Step 10: Run the Apache cell**

```bash
MONEDULA_MATRIX_CELL=ak-4.3 go test -tags e2e ./test/e2e/cli/ -v -timeout 60m -count=1
```

Expected: PASS. If a profile other than `shared-sasl` fails, its overlay from Task 6 needs a Kafka CLI path it is missing — check the failing container's logs before changing test code.

- [ ] **Step 11: Commit**

```bash
git add scenarios/ test/e2e/cli/runner_test.go
git commit -m "test(e2e): gate scenarios on matrix cell capabilities" -m "Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 8: The integration matrix in CI

**Files:**
- Modify: `.github/workflows/ci.yml`

**Interfaces:**
- Consumes: `go run ./hack/matrix-cells -tier=integration` (Task 2); `MONEDULA_MATRIX_CELL` (Task 1).
- Produces: the job output `needs.matrix.outputs.integration`, reused by Task 9's workflow pattern.

- [ ] **Step 1: Add the matrix-generation job**

Append to the `jobs:` block of `.github/workflows/ci.yml`:

```yaml
  # Renders internal/matrix/versions.yaml into job matrices. Using the project's
  # own loader (hack/matrix-cells) rather than yq keeps CI and the unit tests on
  # the same parser, so the two cannot disagree about what the file means.
  matrix:
    runs-on: ubuntu-latest
    outputs:
      integration: ${{ steps.cells.outputs.integration }}
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version: '1.26'
      - id: cells
        run: echo "integration=$(go run ./hack/matrix-cells -tier=integration)" >> "$GITHUB_OUTPUT"
```

- [ ] **Step 2: Add the integration matrix job**

```yaml
  # Container-backed adapter tests, once per broker version the project claims to
  # support. Until this existed, `go test -race ./...` skipped every one of them:
  # they sit behind the `integration` build tag, so no container had ever started
  # in CI. See internal/matrix/versions.yaml for why these versions.
  integration-matrix:
    needs: matrix
    runs-on: ubuntu-latest
    timeout-minutes: 30
    strategy:
      # One broken broker version must not hide the results of the others.
      fail-fast: false
      matrix:
        cell: ${{ fromJSON(needs.matrix.outputs.integration) }}
    name: integration (${{ matrix.cell }})
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version: '1.26'
      # Authenticated pulls when credentials exist: 7 cells x several images per
      # PR runs into Docker Hub's anonymous rate limit. The job still works
      # without the secrets — it just risks being throttled.
      - name: Log in to Docker Hub
        if: ${{ secrets.DOCKERHUB_USERNAME != '' }}
        uses: docker/login-action@v3
        with:
          username: ${{ secrets.DOCKERHUB_USERNAME }}
          password: ${{ secrets.DOCKERHUB_TOKEN }}
      # -count=1 is required: the cell comes from an environment variable, which
      # go test's result cache does not track — a cached "ok" from another cell
      # would otherwise be reported as a pass for this one.
      - name: Integration tests
        run: go test -tags integration ./... -timeout 25m -count=1
        env:
          MONEDULA_MATRIX_CELL: ${{ matrix.cell }}
```

- [ ] **Step 3: Validate the workflow parses**

```bash
python3 -c "import yaml,sys; yaml.safe_load(open('.github/workflows/ci.yml')); print('parses')"
```

Expected: `parses`.

- [ ] **Step 4: Verify the generated matrix matches the file**

```bash
go run ./hack/matrix-cells -tier=integration
```

Expected: `["ak-3.9","ak-4.0","ak-4.3","cp-7.6","cp-7.9","cp-8.0","cp-8.3"]` — seven jobs.

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: run integration tests across the broker version matrix" -m "Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 9: The nightly e2e workflow

**Files:**
- Create: `.github/workflows/e2e.yml`

**Interfaces:**
- Consumes: `go run ./hack/matrix-cells -tier=e2e` (Task 2); the cell-aware runner (Task 7).
- Produces: nothing later tasks consume.

- [ ] **Step 1: Write the workflow**

Create `.github/workflows/e2e.yml`:

```yaml
# The scenario suite (test/e2e/cli) across the broker versions tagged `e2e` in
# internal/matrix/versions.yaml. Separate from ci.yml and off the PR path: each
# cell brings up several Docker Compose stacks and takes 20-40 minutes, which is
# too slow to gate a pull request. Running on push to main as well as nightly
# means a regression is attached to a commit rather than to a date.
name: e2e

on:
  schedule:
    - cron: '0 3 * * *'
  push:
    branches: [main]
  workflow_dispatch:

permissions:
  contents: read

concurrency:
  group: ${{ github.workflow }}-${{ github.ref }}
  cancel-in-progress: ${{ github.ref != 'refs/heads/main' }}

jobs:
  matrix:
    runs-on: ubuntu-latest
    outputs:
      e2e: ${{ steps.cells.outputs.e2e }}
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version: '1.26'
      - id: cells
        run: echo "e2e=$(go run ./hack/matrix-cells -tier=e2e)" >> "$GITHUB_OUTPUT"

  scenarios:
    needs: matrix
    runs-on: ubuntu-latest
    timeout-minutes: 75
    strategy:
      fail-fast: false
      matrix:
        cell: ${{ fromJSON(needs.matrix.outputs.e2e) }}
    name: e2e (${{ matrix.cell }})
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version: '1.26'
      - name: Log in to Docker Hub
        if: ${{ secrets.DOCKERHUB_USERNAME != '' }}
        uses: docker/login-action@v3
        with:
          username: ${{ secrets.DOCKERHUB_USERNAME }}
          password: ${{ secrets.DOCKERHUB_TOKEN }}
      # cp-server (the auth-mds cell) has a 90s start_period and is a large
      # image, hence the generous timeout.
      - name: Scenario suite
        run: go test -tags e2e ./test/e2e/cli/ -v -timeout 60m -count=1
        env:
          MONEDULA_MATRIX_CELL: ${{ matrix.cell }}
      # Without these, diagnosing why a broker failed to start on one version is
      # guesswork: the failure surfaces as a compose timeout with no broker log.
      - name: Collect compose logs
        if: failure()
        run: |
          mkdir -p /tmp/compose-logs
          for c in $(docker ps -aq); do
            name=$(docker inspect --format '{{.Name}}' "$c" | tr -d '/')
            docker logs "$c" > "/tmp/compose-logs/${name}.log" 2>&1 || true
          done
      - name: Upload compose logs
        if: failure()
        uses: actions/upload-artifact@v4
        with:
          name: compose-logs-${{ matrix.cell }}
          path: /tmp/compose-logs
          retention-days: 7
          if-no-files-found: ignore
```

- [ ] **Step 2: Validate the workflow parses**

```bash
python3 -c "import yaml,sys; yaml.safe_load(open('.github/workflows/e2e.yml')); print('parses')"
```

Expected: `parses`.

- [ ] **Step 3: Verify the e2e cell list**

```bash
go run ./hack/matrix-cells -tier=e2e
```

Expected: `["ak-4.3","cp-7.9","cp-8.3","cp-7.6-mds"]` — four jobs.

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/e2e.yml
git commit -m "ci: add nightly e2e scenario matrix" -m "Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 10: Generated README table, Makefile targets, and remaining version bumps

**Files:**
- Modify: `README.md` (the support-matrix section around line 89, and the CI claim at line 108)
- Modify: `Makefile` (add `matrix-docs`, `integration`, `e2e-cli` targets)
- Modify: `.github/workflows/ci.yml` (add the docs drift check)
- Modify: `quickstart/cli/docker-compose.yml` (lines 24, 80, 99)
- Modify: `quickstart/k8s/kafka/kafka.yaml` (lines 84, 204)
- Modify: `quickstart/k8s/kafka/schema-registry.yaml` (lines 80, 107)
- Modify: `scenarios/clusters/shared-sasl/k8s/kafka.yaml`, `scenarios/clusters/shared-sasl/k8s/schema-registry.yaml`, `scenarios/clusters/auth-sasl-ssl/k8s/kafka.yaml`

**Interfaces:**
- Consumes: `go run ./hack/matrix-cells -readme=README.md` (Task 2).
- Produces: nothing later tasks consume. This is the last task.

- [ ] **Step 1: Insert the markers into README.md**

Immediately after the support-matrix table's footnote paragraph (the one ending "see [Connecting](docs/connecting.md#support-matrix) for details."), replace the bolded CI claim with:

```markdown
### Validated versions

CI runs the container-backed suites against every version below on each pull request
(adapter integration tests) and nightly (the full scenario suite). Confluent Cloud is
validated with the opt-in maintainer harness (`make e2e-cloud`), not in CI.

<!-- BEGIN GENERATED: version-matrix -->
<!-- END GENERATED: version-matrix -->

This table is generated from `internal/matrix/versions.yaml` by `make matrix-docs`; CI fails
if it is stale.
```

- [ ] **Step 2: Generate the table**

```bash
go run ./hack/matrix-cells -readme=README.md
git diff README.md
```

Expected: the block between the markers now holds an 8-row table, one row per cell.

- [ ] **Step 3: Add the Makefile targets**

Add to `.PHONY` and to the file:

```makefile
# Regenerate the version-matrix table in README.md from internal/matrix/versions.yaml.
# CI fails when this leaves the tree dirty, so the docs cannot drift from what runs.
matrix-docs:
	go run ./hack/matrix-cells -readme=README.md

# Adapter integration tests against one matrix cell (default: the cell marked
# `default: true`). Override with MONEDULA_MATRIX_CELL=ak-4.3 make integration.
# -count=1 is required: the cell comes from an env var, which go test's result
# cache does not track.
integration:
	go test -tags integration ./... -timeout 25m -count=1

# The CLI scenario suite against one matrix cell. Needs a running Docker daemon;
# fails (rather than silently passing) without one.
e2e-cli:
	go test -tags e2e ./test/e2e/cli/ -v -timeout 60m -count=1
```

Update the `.PHONY` line to include `matrix-docs integration e2e-cli`.

- [ ] **Step 4: Add the drift check to CI**

In `.github/workflows/ci.yml`, in the `build-test` job, after the `go.mod is tidy` step:

```yaml
      # The README version table is generated from internal/matrix/versions.yaml.
      # A stale table means the documented support claim no longer matches what
      # CI actually runs — which is exactly how the previous claim went wrong.
      - name: README version matrix is current
        run: |
          make matrix-docs
          git diff --exit-code README.md
```

- [ ] **Step 5: Bump the remaining hard-coded images**

Replace every remaining `confluentinc/cp-kafka:8.0.0` with `confluentinc/cp-kafka:8.3.1` and every `confluentinc/cp-schema-registry:8.0.0` with `confluentinc/cp-schema-registry:8.3.1` in the quickstart and k8s manifests:

```bash
grep -rln "cp-kafka:8.0.0\|cp-schema-registry:8.0.0" quickstart/ scenarios/
```

Fix each hit. These are demo and k8s-suite manifests, not matrix cells, so they take literal versions rather than placeholders.

- [ ] **Step 6: Verify nothing stale remains**

```bash
grep -rn "cp-kafka:8.0.0\|cp-schema-registry:8.0.0\|cp-server:7.6.1\b\|confluent-local" \
  --include=*.yaml --include=*.yml --include=*.go --include=*.md . | grep -v '^./docs/design/'
```

Expected: no output. The design and plan documents under `docs/design/` legitimately quote the old versions as the "before" state, hence the exclusion.

- [ ] **Step 7: Verify the k8s quickstart still renders**

```bash
helm lint charts/monedula-gitops
python3 -c "import yaml,sys; list(yaml.safe_load_all(open('quickstart/k8s/kafka/kafka.yaml'))); print('parses')"
```

Expected: `1 chart(s) linted, 0 chart(s) failed` and `parses`.

- [ ] **Step 8: Full clean verification**

```bash
go build ./... && go vet ./... && go test ./... && echo ALL CLEAN
```

Expected: `ALL CLEAN`.

- [ ] **Step 9: Commit**

```bash
git add README.md Makefile .github/workflows/ci.yml quickstart/ scenarios/
git commit -m "docs: generate the validated-versions table from the matrix" -m "README claimed CI validated continuously against Kafka and Confluent Platform while no container had ever started in CI. The table is now generated from internal/matrix/versions.yaml and diff-checked, so the claim tracks what runs." -m "Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

## Self-Review

**Spec coverage.** Every section of the design maps to a task: `versions.yaml` and the cell schema → Task 1; the `internal/matrix` API → Tasks 1 and 3; `StartBroker` family dispatch → Task 3; integration tier rewiring → Tasks 4 and 5; compose parameterisation and the Apache overlay → Task 6; scenario `requires:` gating → Task 7; the CI matrix job → Task 8; the nightly e2e workflow → Task 9; README generation and the incidental version bumps → Task 10. The spec's "Testing" list is covered by Task 1 Step 2, except the cross-check that every scenario `requires:` is satisfiable by some e2e cell, which lives in Task 7 Step 3 because it needs the scenario loader.

**Deviations from the spec, deliberate.**
- `Cell.Tier(string)` renamed to `InTier(string)` (flagged in Task 1's Interfaces block): `Tier` reads as an accessor.
- `func Cell(id string)` from the spec would collide with the type `Cell` and not compile; it is `Get(id string)`.
- The spec proposed `yq` for the CI matrix; Task 2 uses a Go tool instead, so CI and the unit tests share one parser and there is no dependency on a runner's `yq` dialect.
- The spec listed the Schema Registry healthcheck rewrite nowhere: it was discovered during verification (Verified Fact 4) and is required, not optional, so it is part of Task 6.

**Placeholder scan.** No TBD/TODO. Every code step carries the actual code. The one instruction that cannot be fully spelled out in advance is Task 6 Step 4 for the three non-`shared-sasl` overlays, because their compose files must be read first; the step gives the exact grep and the exact rewrite rule, and `shared-sasl` is written out in full as the worked example.

**Type consistency.** `Cell`, `Broker`, `Option`, `StartBroker`, `WithNetwork`, `Supports`, `InTier`, `ForTier`, `Get`, `Default`, `FromEnv`, `SkipUnlessDocker`, `SkipWithoutCapability`, `IsDockerUnavailable` are defined in Tasks 1 and 3 and used with those exact names and signatures in Tasks 4, 5, and 7. `beginMarker`/`endMarker`/`tierJSON`/`readmeTable`/`spliceBlock` are defined and used within Task 2. The capability constants are defined in Task 1 and referenced in Tasks 4, 5, and 7. `MONEDULA_KAFKA_IMAGE` / `MONEDULA_SR_IMAGE` are produced by Task 7's `composeEnv` and consumed by Task 6's compose files — the two tasks agree on both names.

**Ordering note.** Task 6 writes compose files that reference environment variables Task 7 exports. Task 6's own verification steps set those variables by hand, so it is independently testable; the tasks may be implemented in order without a broken intermediate state.
