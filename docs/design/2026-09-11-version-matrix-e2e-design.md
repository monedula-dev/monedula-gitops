# Design: version-matrix integration and e2e testing

Date: 2026-09-11
Status: implemented, on branch `feat/version-matrix-e2e`

## Problem

`README.md` claims:

> CI validates continuously against Apache Kafka (`cp-kafka` images) and Confluent Platform
> components; Confluent Cloud was validated with the opt-in maintainer harness (`make e2e-cloud`),
> not in CI.

This is not true today. `.github/workflows/ci.yml` runs `go test -race ./...`, which excludes every
container-backed test in the repository:

- `internal/kafka/franz/integration_test.go` and `internal/schemaregistry/confluent/integration_test.go`
  are behind `//go:build integration`.
- `test/e2e/cli/runner_test.go` (the 25-scenario Docker Compose suite) is behind `//go:build e2e`.
- `test/e2e/k8s/run.bats` needs `kind`/`kubectl`/`bats` and is only reachable via `make e2e-k8s`.

No container starts in CI. Separately, every broker version is hard-coded in eight places
(`confluentinc/cp-kafka:8.0.0` in four compose profiles plus the quickstarts,
`confluentinc/cp-server:7.6.1` in `auth-mds`, `confluentinc/cp-schema-registry:8.0.0`,
`confluentinc/confluent-local:7.6.1` in the integration tests), so there is no way to run the suite
against a different Kafka or Confluent Platform release, and no image of `apache/kafka` appears
anywhere — "Apache Kafka" support is currently exercised through Confluent's build of Kafka.

## Goals

1. Validate the product against a deliberate set of Apache Kafka and Confluent Platform versions,
   continuously, in CI.
2. Keep the version list in exactly one place, consumed by CI, by the Makefile, and by the tests.
3. Represent version-specific capability gaps declaratively, so unsupported combinations skip with a
   stated reason rather than failing or silently passing.
4. Make the README support claim generated from what actually runs, so it cannot drift again.

## Non-goals

- A Kubernetes-version axis. The `bats`/kind suite stays on a single k8s version; that is a separate
  dimension deserving its own spec.
- Confluent Cloud in CI. It stays an opt-in maintainer harness (`make e2e-cloud`).
- A Cartesian product of "Kafka version" x "Confluent version". Those axes are not orthogonal:
  CP 8.0 *is* Kafka 4.0. The matrix is a flat list of broker flavors.

## Architecture

### Single source of truth: `internal/matrix/versions.yaml`

The file lives inside the package rather than under `test/` because `go:embed` cannot reach outside
its own directory.

```yaml
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

  - id: cp-7.6-mds
    family: confluent
    image: confluentinc/cp-server:7.6.13
    profile: auth-mds
    tiers: [e2e]
    capabilities: [topics, acls, quotas, scram, mds]
```

Fields:

| Field | Meaning |
|---|---|
| `id` | Stable cell identifier; used as the `MONEDULA_MATRIX_CELL` value and the CI job name |
| `family` | `apache` or `confluent`; selects the broker start path and the compose overlay |
| `image` | Broker image reference |
| `schemaRegistryImage` | Schema Registry image; absent means the cell has no `schemaregistry` capability |
| `profile` | Restricts the cell to one compose profile (only `cp-7.6-mds`) |
| `tiers` | Which suites run this cell: `integration`, `e2e`, or both |
| `default` | Exactly one cell; used when `MONEDULA_MATRIX_CELL` is unset |
| `capabilities` | From the closed vocabulary: `topics`, `acls`, `quotas`, `scram`, `schemaregistry`, `mds` |
| `overlays` | Extra compose overlays this cell needs beyond the family-based one (e.g. `jetty9`), applied in declaration order as `compose.<name>.yaml` on top of the profile's base `compose.yaml` when that file exists for the profile |

Cell-to-profile rule: a cell **without** `profile` runs every compose profile *except* `auth-mds`. The
`auth-mds` profile runs **only** under a cell that names it. This keeps the MDS profile on its pinned
`cp-server` line without excluding it from the suite.

That yields, per tier: 7 integration cells (`ak-3.9`, `ak-4.0`, `ak-4.3`, `cp-7.6`, `cp-7.9`, `cp-8.0`,
`cp-8.3`) and 4 e2e cells — 3 general (`ak-4.3`, `cp-7.9`, `cp-8.3`) plus the profile-pinned
`cp-7.6-mds`.

Version selection rationale: the list covers each break that plausibly changes behaviour rather than
every minor. Apache 3.9 to 4.0 is the ZooKeeper removal / KRaft-only boundary. CP 7.x to 8.x removed
the scope-wide `POST /security/1.0/lookup/rolebindings` MDS endpoint, which is why `auth-mds` is
pinned to the 7.6 line. Support floor: Apache Kafka 3.9, Confluent Platform 7.6.

`ak-4.3` deliberately pairs an Apache broker with a Confluent Schema Registry. That combination is
common in real deployments and is untested today.

### `internal/matrix` package

Build-tag-free, so both tiers and the unit tests can import it.

```go
func Cells() []Cell                              // all cells, in file order
func Get(id string) (Cell, error)                // lookup by id (not `Cell`: that name is the type)
func Default() Cell                              // the cell with default: true
func FromEnv() (Cell, error)                     // MONEDULA_MATRIX_CELL, else Default()
func (c Cell) Supports(capability string) bool
func (c Cell) Tier(tier string) bool
```

Plus the one piece of behaviour that differs by family:

```go
func StartBroker(ctx context.Context, c Cell) (Broker, error)
```

- `family: confluent` delegates to the existing `testcontainers-go/modules/kafka` `Run`.
- `family: apache` cannot. The module's starter script is hard-wired to the Confluent image layout
  (`source /etc/confluent/docker/bash-config`, `/etc/confluent/docker/configure`,
  `/etc/confluent/docker/launch`). Apache cells therefore use a generic testcontainers container with
  `KAFKA_*` environment variables (which the `apache/kafka` image translates to broker properties
  from 3.7 onward) and a `wait.ForLog` readiness strategy.

`StartBroker` preserves the existing Docker-availability semantics: `SkipIfProviderIsNotHealthy`
first, with a fallback check on the container-start error.

### Integration tier changes

`internal/kafka/franz/integration_test.go` and `internal/schemaregistry/confluent/integration_test.go`
drop their `kafkaImage` / `srImage` constants and resolve the cell through `matrix.FromEnv()`.

A test whose cell lacks the required capability skips with an explicit reason
(`skip: cell ak-3.9 has no schemaregistry capability`), never silently.

### E2E tier changes

The four `cp-kafka` compose profiles (`shared-sasl`, `auth-sasl-ssl`, `auth-mtls`, `auth-oauth`)
parameterise their image:

```yaml
image: ${MONEDULA_KAFKA_IMAGE:-confluentinc/cp-kafka:8.3.1}
```

The `:-` default is deliberate: a bare `docker compose up` keeps working with no environment set,
exactly as today. `auth-mds` keeps its own pinned `cp-server` image; that profile is single-version
by nature and is represented by the `cp-7.6-mds` cell.

Apache cells need a per-profile `compose.apache.yaml` overlay, merged as
`-f compose.yaml -f compose.apache.yaml`. The known delta is binary naming:

| | cp-kafka | apache/kafka |
|---|---|---|
| SCRAM bootstrap in `kafka-init` | `kafka-configs` | `kafka-configs.sh` |
| broker healthcheck | `kafka-broker-api-versions` | `kafka-broker-api-versions.sh` |

Everything else (`KAFKA_*` to properties, `KAFKA_OPTS` carrying the JAAS config, KRaft settings) is
shared.

**Verified 2026-09-12 against live containers.** The overlay approach holds, with two corrections to
the assumptions above:

- `/opt/kafka/bin` is **not** on `PATH` in `apache/kafka:4.3.1`, so the overlay invokes Kafka scripts
  by absolute path (`/opt/kafka/bin/kafka-configs.sh`), not by name.
- `confluentinc/cp-schema-registry:8.3.1` ships **no `curl`, `wget` or `nc`** — Confluent removed them
  between 8.0 and 8.3. The existing curl-based SR healthcheck therefore breaks on the version bump
  and must be rewritten using `python3`, which is present across 7.6 through 8.3. This is a required
  change, not a nicety, and it is exactly the kind of breakage the matrix exists to catch.

With only the image and healthcheck overridden, `apache/kafka:4.3.1` boots healthy on the unmodified
`shared-sasl` configuration, SCRAM bootstrap succeeds, `cp-schema-registry:8.3.1` runs against it,
and `monedula-gitops doctor` returns exit 0 with every check passing. The fallback of a complete
per-profile `compose.apache.yaml` is not needed.

`test/e2e/cli/runner_test.go` resolves its cell from the environment, exports
`MONEDULA_KAFKA_IMAGE` / `MONEDULA_SR_IMAGE` to compose, and selects the overlay by family.

### Scenario gating

`scenario.yaml` gains an optional `requires:` list drawn from the same capability vocabulary:

```yaml
requires: [mds]
```

The runner consults `cell.Supports()` and skips non-applicable scenarios with a stated reason. Today
this knowledge is implicit in the profile-to-scenario mapping (scenario 22 simply lives in the
`auth-mds` profile); `requires:` makes it explicit and checkable.

### CI workflows

`ci.yml` gains two jobs. A small `matrix` job converts `versions.yaml` into JSON outputs (`yq`, then
`fromJSON` in the consumer) so the version list is never duplicated in Actions YAML:

```yaml
integration-matrix:
  needs: matrix
  strategy:
    fail-fast: false
    matrix:
      cell: ${{ fromJSON(needs.matrix.outputs.integration) }}
  steps:
    - run: go test -tags integration ./...
      env:
        MONEDULA_MATRIX_CELL: ${{ matrix.cell }}
```

`fail-fast: false` matters: one broken version must not hide the results of the others.

A new `.github/workflows/e2e.yml` runs the scenario suite on `schedule` (nightly), `push` to `main`,
and `workflow_dispatch`, over the cells tagged `tiers: [e2e]`, executing
`go test -tags e2e ./test/e2e/cli/`. It uploads compose logs on failure — diagnosing why a 4.3 broker
failed to start without them is guesswork.

Operational risk: Docker Hub rate limits. Seven cells each pulling images on every PR is a real
constraint on anonymous runners. Mitigation: `docker/login-action` when credentials are available,
plus layer caching. If limits still bite, the fallback is moving the Apache cells to nightly.

### README generation

`make matrix-docs` renders a version table from `versions.yaml` into `README.md` between markers, and
CI asserts `git diff --exit-code`. The support claim becomes generated from what actually runs, which
is the only way it stays true.

## Testing

Unit tests for `internal/matrix` (no Docker required):

- `versions.yaml` parses, and every capability is in the closed vocabulary.
- Exactly one cell carries `default: true`.
- Cell ids are unique.
- `FromEnv()` resolves a named cell, falls back to the default, and errors on an unknown id.
- Every cell with `schemaregistry` has a `schemaRegistryImage`, and vice versa.
- Guard: every capability named by a scenario's `requires:` is covered by at least one `e2e` cell, so
  a scenario cannot become permanently unreachable.

The container-backed tiers are themselves the test of `StartBroker`.

## Incidental changes

- `confluentinc/cp-kafka:8.0.0` to `8.3.1` in the quickstarts and `quickstart/k8s`.
- `confluentinc/cp-server:7.6.1` to `7.6.13` in `auth-mds`.
- `confluentinc/confluent-local:7.6.1` removed; the integration tests use matrix cells.

## Risks

| Risk | Mitigation |
|---|---|
| ~~`apache/kafka` compose delta wider than expected~~ | Retired 2026-09-12: verified against live containers, delta is image + absolute script paths only |
| Docker Hub rate limits on 7-cell PR runs | Authenticated pulls; fall back to nightly-only Apache cells |
| Nightly e2e goes red and is ignored | Failures on `main` push too, so a regression surfaces attached to a commit |
| Version list drifts from README | README table generated and diff-checked in CI |
