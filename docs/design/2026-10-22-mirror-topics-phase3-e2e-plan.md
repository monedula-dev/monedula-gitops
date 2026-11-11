# Mirror topics, Phase 3 (real-cluster e2e) Implementation Plan

**Goal:** Prove declarative mirror topics end to end against two real Confluent Server 8.3 clusters joined by a cluster link, fix the CLI behaviors that only a real cluster revealed, and extend the opt-in Confluent Cloud harness with a mirror smoke test.

**Architecture:** A new e2e cluster profile `cluster-link` starts `source` and `dest` cp-server brokers plus a one-shot service that creates source topics and the destination link `src-to-dest`. A new matrix cell pins that profile and the new `cluster-linking` capability. Scenario `26-mirror-topic` drives the CLI through create, replication, pause drift, collision and import, using a new `exec` step (run a command inside a compose service) and a new `liveState.mirrors` assertion read through Kafka REST. The CLI-only scope matches existing profile-pinned scenarios; the operator path stays covered by envtest.

**Tech Stack:** Go, Docker Compose, Confluent Server `confluentinc/cp-server:8.3.1`, the existing `internal/e2e` and `test/e2e/cli` runner, GitHub Actions `e2e.yml`.

**Spec:** `docs/design/2026-10-20-mirror-topics-design.md` (v7), sections "End-to-end profile" and "Version and cloud compatibility". Phase 1 and 2 plans in `docs/design/` give the vocabulary. Branch `feat/mirror-topics-e2e`, stacked on `feat/mirror-topics-operator` (#26).

## Facts established by a local spike (cp-server 8.3.1, two clusters, trial license)

These are verified, not assumptions; the plan builds on them.

- Cluster linking works with no license key (`confluent.cluster.link.enable` defaults to true). Single-node brokers need RF=1 overrides for Confluent internal topics: `KAFKA_CONFLUENT_LICENSE_TOPIC_REPLICATION_FACTOR`, `..._BALANCER_TOPIC_...`, `..._METADATA_TOPIC_...`, `..._CLUSTER_LINK_METADATA_TOPIC_...`, `..._DURABILITY_TOPIC_...`, `KAFKA_CONFLUENT_TIER_METADATA_REPLICATION_FACTOR`, all `"1"`.
- Embedded Kafka REST v3 is on by default at `:8090` under `/kafka/v3`, with no auth. `spec.rest.endpoint` is `http://localhost:8090/kafka`. `/v3/...` without `/kafka` is 404.
- The image has no `curl`/`wget`; healthchecks use `python3` (`urllib.request.urlopen('http://localhost:8090/kafka/v3/clusters')`) or Kafka CLI tools.
- A dest broker without an authorizer makes `doctor` fail (`acl-read: SECURITY_DISABLED`). The profile sets `KAFKA_AUTHORIZER_CLASS_NAME=org.apache.kafka.metadata.authorizer.StandardAuthorizer` and `KAFKA_SUPER_USERS=User:ANONYMOUS` on dest.
- Link creation: `kafka-cluster-links --bootstrap-server dest:9092 --create --link src-to-dest --config-file <file with bootstrap.servers=source:9092>`. Link is usable about 20s after `up` with the image cached.
- `GET .../links/{link}/configs`: `topic.config.sync.include` is non-empty by default (8 keys); `cluster.link.prefix` is JSON `null`; `metadata.next` is `null`.
- Mirror create returns 201 and is ACTIVE by the first poll (PENDING_* on create is not observable). STOPPED entries stay listed after promote/failover. `kafka-mirrors` uses `--pause`/`--unpause`.
- REST errors: duplicate create 400/40002; missing source topic 404/40403; synced config on create 400/40002.
- Mirror topics report the 8 synced configs as non-default (`DYNAMIC_TOPIC_CONFIG`); every cp-server topic reports `min.insync.replicas` as non-default (`DYNAMIC_DEFAULT_BROKER_CONFIG`).
- With that compose, `doctor`, `validate`, `diff`, `apply` and `verify` of a `KafkaMirrorTopic` all exit 0 and `verify` reports "No changes".

## Global Constraints

- Module `github.com/monedula-dev/monedula-gitops`. Unit tests `go test ./...`; e2e `MONEDULA_MATRIX_CELL=cp-8.3-link go test -tags e2e -count=1 -timeout 60m -run TestClusterLinkScenarios ./test/e2e/cli/` (needs Docker, which is available locally).
- Image `confluentinc/cp-server:8.3.1`, injected through the existing `MONEDULA_KAFKA_IMAGE` variable.
- Commit messages: conventional, no AI attribution or tool trailers; no tool references in code, comments or docs.
- Windows checkout with `core.autocrlf=true`: `internal/output` golden tests fail locally on CRLF only; check gofmt on committed content; commit shell scripts and YAML with LF endings (`.gitattributes` already forces this for `*.sh`; verify).
- README version table is generated: run `go run ./hack/matrix-cells -readme=README.md` after matrix changes (CI diffs it).

## Decisions this plan makes

- Scenario 26 is `modes: [cli]`, like the other profile-pinned scenarios; the operator path is covered by the Phase 2 envtest suites (now run in CI by #27). A kind-based k8s variant is out of scope.
- No separate `internal/kafkarest/confluent` integration test: the e2e scenario exercises every REST method the product uses against a real cluster (`ClusterID`, `ListLinks`, `LinkConfigs`, `ListMirrors`, `CreateMirror`, `AlterTopicConfigs`), and starting two linked brokers through testcontainers would duplicate the compose profile.
- Source topics and a dest-only normal topic are created by the profile's one-shot `link-setup` service, not by scenario steps.
- Replication and pause/unpause are driven by a generic `exec` step that runs a command in a compose service, instead of new purpose-built step kinds.
- The new matrix cell is e2e-tier only.
- `diff` keeps exiting 0 when the plan contains `Rejected` ops (existing diff contract); `verify` and `apply` are the gates.

## Review Focus

- A definite REST rejection on mirror create (4xx) must not be reported as "outcome unknown"; only transport errors, timeouts and 5xx are uncertain. Test owned by Task 1.
- Importing a healthy mirror must not report it as partial just because the link syncs configs. Test owned by Task 1.
- The `exec` step must fail the scenario on a non-zero exit unless the step's `expect.exitCode` says otherwise, and its output must be checkable. Test owned by Task 3.
- The cluster-link profile must not collide with other profiles' fixed host ports or container names when stacks are torn down and started in sequence. Test owned by Task 3 (distinct names/ports; runner `down -v` in cleanup).
- A `liveState.mirrors` expectation must fail, not pass, when REST is unreachable. Test owned by Task 4.

---

### Task 1: CLI fixes found against a real cluster

**Files:**
- Modify: `internal/kafkarest/client.go` (status error type), `internal/kafkarest/confluent/client.go` (return it), `internal/mirror/writer.go` (create error wrapping), `internal/output/output.go` (apply result line), `internal/importer/build.go` (mirror config warning)
- Test: `internal/kafkarest/confluent/client_write_test.go`, `internal/mirror/writer_test.go`, `internal/output/output_test.go` (or the existing apply-result test file), `internal/importer/mirror_test.go`

**Interfaces:**
- Produces:
  - `type StatusError struct { Code int; ErrorCode int; Message string }` in `kafkarest` with `Error() string`; the confluent client returns `*kafkarest.StatusError` for non-2xx responses (keep current message text in `Error()`; `ErrorCode` is Confluent's `error_code` when the body has one).
  - `mirror.Writer.CreateMirror`: when the POST error is a `*kafkarest.StatusError` with `400 <= Code < 500`, wrap as `"create mirror %s rejected by Kafka REST (%d): %w"`; otherwise keep `"create mirror %s: outcome unknown, re-run diff before retrying: %w"`.
  - Apply result rendering prints the status once (no "Rejected Rejected"); find the duplication in `internal/output` and fix it for every status/action pair where they are equal.
  - Import: `MirrorConfigNotImported` counts only explicit config keys that are not link-managed per `mirror.LinkManaged(link, key)` (managed=true, known=true → excluded); keys when sync is undeterminable still count. No warning when nothing remains.

- [ ] **Step 1: Write failing tests**

```go
func TestCreateMirror4xxIsStatusError(t *testing.T)        // httptest 404 {"error_code":40403,"message":"..."} -> errors.As *kafkarest.StatusError, Code 404, ErrorCode 40403
func TestWriterCreateDefiniteRejection(t *testing.T)        // REST mock CreateMirror returns &kafkarest.StatusError{Code: 404} -> error contains "rejected by Kafka REST (404)", not "outcome unknown"
func TestWriterCreateTransportErrorIsUnknown(t *testing.T)  // plain error -> still "outcome unknown"
func TestApplyResultNoDuplicatedStatus(t *testing.T)        // Rejected op -> rendered human line contains "Rejected" exactly once before the kind
func TestImportMirrorSyncedConfigsNotPartial(t *testing.T)  // mirror with explicit keys all in topic.config.sync.include -> no MirrorConfigNotImported, Partial false
func TestImportMirrorLocalConfigStillWarned(t *testing.T)   // one key outside sync.include -> warning names count 1
```

- [ ] **Step 2–4:** run `go test ./internal/kafkarest/... ./internal/mirror/... ./internal/output/... ./internal/importer/...` → FAIL → implement → PASS (CRLF golden failures in internal/output excepted; the new output test must not depend on golden files).
- [ ] **Step 5: Commit** `fix(cli): report definite mirror create rejections and synced-only import configs accurately`

---

### Task 2: Matrix capability and cell

**Files:**
- Modify: `internal/matrix/matrix.go` (`CapClusterLinking = "cluster-linking"`), `internal/matrix/versions.yaml` (cell + profile rule comment at `:8-10`), `internal/matrix/matrix_test.go` (allowed capabilities `:38`, allowed profiles `:48-54`, new profile-has-a-cell test), `internal/e2e/requirements_test.go` (`validCapabilities` `:349-356`), `README.md` (regenerated)

**Interfaces:**
- Produces: cell
  ```yaml
  - id: cp-8.3-link
    family: confluent
    image: confluentinc/cp-server:8.3.1
    profile: cluster-link
    tiers: [e2e]
    capabilities: [topics, acls, cluster-linking]
  ```
  (match the field names and family value the file already uses for `cp-7.6-mds`); `func TestClusterLinkProfileHasACell(t *testing.T)` modelled on `TestMDSProfileHasACell`.

- [ ] **Step 1:** add `TestClusterLinkProfileHasACell` and the new capability to both allowed lists → run `go test ./internal/matrix/ ./internal/e2e/` → FAIL.
- [ ] **Step 2:** add the constant and the cell; update the profile rule comment to say profile-pinned cells (`auth-mds`, `cluster-link`) run only their own profile's scenarios.
- [ ] **Step 3:** `go run ./hack/matrix-cells -readme=README.md`; `go run ./hack/matrix-cells -tier=e2e` lists `cp-8.3-link`.
- [ ] **Step 4:** `go test ./internal/matrix/ ./internal/e2e/` → PASS.
- [ ] **Step 5: Commit** `test(matrix): add a cluster-linking e2e cell`

---

### Task 3: `cluster-link` profile, runner support and the `exec` step

**Files:**
- Create: `scenarios/clusters/cluster-link/compose.yaml`, `cluster.yaml`, `README.md`
- Modify: `internal/e2e/expect.go` (`Step.Exec`), `internal/e2e/expect_test.go`, `test/e2e/cli/runner_test.go` (`TestClusterLinkScenarios`, compose-up helper, `exec` in `runSteps`), `scenarios/README.md` (profile list)

**Interfaces:**
- Compose (from the spike, adapted): services `source`, `dest`, `link-setup`; image `${MONEDULA_KAFKA_IMAGE:-confluentinc/cp-server:8.3.1}` on all three; container names `monedula-cl-source`, `monedula-cl-dest`, `monedula-cl-setup`; host ports source `19092`, dest `29092`, dest REST `8090`; distinct `CLUSTER_ID`s; dest authorizer + `User:ANONYMOUS` super user; python3 REST healthcheck on dest and `kafka-broker-api-versions` healthcheck on source. `link-setup` (bash, `set -euo pipefail`): create source topics `orders` (3 partitions) and `payments` (1 partition), create dest topic `ledger` (normal topic, for the collision case), create link `src-to-dest`, then `--list`.
- `cluster.yaml`: `KafkaCluster` `dest`, `bootstrapServers: localhost:29092`, `rest.endpoint: http://localhost:8090/kafka`, no auth.
- `type ExecSpec struct { Service string \`json:"service"\`; Command []string \`json:"command"\` }`; `Step.Exec *ExecSpec \`json:"exec,omitempty"\``; `Run: "exec"`. Load-time validation: `exec` requires `Exec` with non-empty `Service` and `Command`.
- Runner: `TestClusterLinkScenarios` skips unless `cell.Profile == "cluster-link"`; project `mon-e2e-link`; brings the stack up with `up -d` then `compose wait link-setup` (fail with logs on non-zero) instead of `up -d --wait`; runs the scenario list `{"26-mirror-topic"}`. `runSteps` gains access to the compose project and files (add parameters or a small struct) and runs `exec` as `docker compose -p <project> <-f files> exec -T <service> <command...>`; exit code and output go through the same `CheckExitCode`/`CheckOutput` as other steps; with no `expect`, a non-zero exit fails the test.

- [ ] **Step 1: Write failing tests**

```go
func TestLoadExpectExecStep(t *testing.T)          // YAML step {run: exec, exec: {service: dest, command: [echo, hi]}} parses
func TestLoadExpectExecStepRequiresService(t *testing.T) // missing service -> load error
```

- [ ] **Step 2–4:** FAIL → implement → PASS (`go test ./internal/e2e/`), and `go vet -tags e2e ./test/e2e/cli/`.
- [ ] **Step 5:** bring the profile up by hand (`docker compose -p mon-e2e-link -f scenarios/clusters/cluster-link/compose.yaml up -d` then `... wait link-setup`), run `doctor` against `cluster.yaml` with the locally built binary → exit 0; `down -v`.
- [ ] **Step 6: Commit** `test(e2e): add the cluster-link profile and exec steps`

---

### Task 4: `liveState.mirrors` assertions

**Files:**
- Create: `internal/e2e/mirrors.go`, `internal/e2e/mirrors_test.go`
- Modify: `internal/e2e/expect.go` (`LiveState.Mirrors`, `AbsentState.Mirrors`), `internal/cli/e2e.go` (build a REST client when mirror expectations exist), `test/e2e/cli/runner_test.go` (`checkLiveState` triggers on mirrors)

**Interfaces:**
- Produces:
  - `type MirrorExpect struct { Name, LinkName, SourceTopic, Status string }` (`json:"name"`, `"linkName"`, `"sourceTopic"`, `"status"`; empty field = not checked); `LiveState.Mirrors []MirrorExpect`; `AbsentState.Mirrors []string`.
  - `type MirrorProber interface { ListMirrors(ctx context.Context) ([]kafkarest.Mirror, error) }` (satisfied by `kafkarest.Client`); `func CheckMirrors(ctx context.Context, p MirrorProber, want []MirrorExpect, absent []string) Report` — a read error fails every expectation with the error text; a missing mirror fails; each non-empty field must match.
  - `e2e check`: when `len(LiveState.Mirrors) > 0 || len(Absent.Mirrors) > 0`, build the REST client with `cluster.BuildKafkaRESTClient`; a nil client (no `spec.rest`) is a failed check with "cluster has no spec.rest".

- [ ] **Step 1: Write failing tests**

```go
func TestCheckMirrorsMatch(t *testing.T)          // mock with ACTIVE orders on src-to-dest -> pass
func TestCheckMirrorsStatusMismatch(t *testing.T) // want PAUSED, live ACTIVE -> fail naming both
func TestCheckMirrorsMissing(t *testing.T)
func TestCheckMirrorsAbsent(t *testing.T)         // absent ["ledger"] with no mirror -> pass; mirror present -> fail
func TestCheckMirrorsReadErrorFails(t *testing.T) // mock Fail("ListMirrors") -> every expectation fails, detail has the error
```

- [ ] **Step 2–4:** FAIL → implement → PASS (`go test ./internal/e2e/ ./internal/cli/`).
- [ ] **Step 5: Commit** `test(e2e): assert mirror topics through Kafka REST`

---

### Task 5: Scenario `26-mirror-topic`

**Files:**
- Create: `scenarios/26-mirror-topic/{scenario.yaml,expect.yaml,README.md,cleanup.sh}`, `scenarios/26-mirror-topic/manifests/orders-mirror.yaml`, `scenarios/26-mirror-topic/manifests-collision/ledger-mirror.yaml`
- Modify: `scenarios/README.md` (index row), `CHANGELOG.md` (Unreleased: real-cluster e2e for mirror topics; the Task 1 fixes under Fixed)

**Interfaces:**
- `scenario.yaml`: `title`, `summary`, `modes: [cli]`, `cluster: cluster-link`, `requires: [cluster-linking]`.
- `manifests/orders-mirror.yaml`: `KafkaMirrorTopic` `orders-mirror`, `clusterRef: dest`, `linkName: src-to-dest`, `sourceTopicName: orders`.
- `manifests-collision/ledger-mirror.yaml`: `KafkaMirrorTopic` `ledger-mirror` on `src-to-dest` with `sourceTopicName: ledger` (no `topicName`; the link has no prefix, so the destination name is `ledger`, a live normal topic created by `link-setup`). Required outcome: `Rejected` with "topic exists and is not a mirror". The source topic `ledger` does not exist on `source`, which does not matter because the diff rejects before any write.
- `expect.yaml` steps, in order (each with `expect` exit code and the listed `outputContains`):
  1. `doctor` → 0, `link:src-to-dest`.
  2. `apply` → 0, `CreateMirrorTopic`.
  3. `verify` → 0.
  4. `exec` service `source`: produce three records `m1`,`m2`,`m3` to `orders` with `kafka-console-producer --bootstrap-server source:9092 --topic orders` (feed stdin via `bash -c "printf 'm1\nm2\nm3\n' | kafka-console-producer ..."`) → 0.
  5. `exec` service `dest`: `kafka-console-consumer --bootstrap-server dest:9092 --topic orders --from-beginning --max-messages 3 --timeout-ms 60000` → 0, `m1`,`m2`,`m3`.
  6. `exec` service `dest`: `kafka-mirrors --bootstrap-server dest:9092 --pause --topics orders` → 0.
  7. `verify` → 1, `MirrorTopicUnhealthy`, `PAUSED`.
  8. `exec` service `dest`: `kafka-mirrors --bootstrap-server dest:9092 --unpause --topics orders` → 0.
  9. `verify` → 0.
  10. `apply` with `manifests: manifests-collision` → 2, `topic exists and is not a mirror`.
  11. `import` → 0.
  12. `verify` with `manifests: "@imported"` → 0.
  - `liveState.mirrors`: `[{name: orders, linkName: src-to-dest, sourceTopic: orders, status: ACTIVE}]`; `liveState.absent.mirrors: [ledger]`.
- `cleanup.sh`: no-op in cli mode (the runner tears the stack down), same shape as other cli-only scenarios.

- [ ] **Step 1:** write the scenario files; `go test ./internal/e2e/` (catalog and requirements tests) → PASS.
- [ ] **Step 2:** run `MONEDULA_MATRIX_CELL=cp-8.3-link go test -tags e2e -count=1 -timeout 60m -run TestClusterLinkScenarios ./test/e2e/cli/ -v` → PASS; paste the step summary in the report. Fix scenario details (not product code) if a step's expectation was wrong; any product defect found here is reported, not silently worked around.
- [ ] **Step 3:** run `MONEDULA_MATRIX_CELL=cp-8.3 go test -tags e2e -count=1 -timeout 60m -run 'TestSharedSASLScenarios' ./test/e2e/cli/` → still PASS (the new profile does not disturb the default cell).
- [ ] **Step 4: Commit** `test(e2e): mirror topic scenario against two linked Confluent Server clusters`

---

### Task 6: Confluent Cloud mirror smoke test (opt-in)

**Files:**
- Modify: `test/e2e/cloud/cloud_test.go`, `test/e2e/cloud/helpers_test.go`, `test/e2e/cloud/README.md`

**Interfaces:**
- Optional env, all-or-none like the Schema Registry trio: `MONEDULA_CLOUD_REST_ENDPOINT`, `MONEDULA_CLOUD_REST_KEY`, `MONEDULA_CLOUD_REST_SECRET`, `MONEDULA_CLOUD_CLUSTER_ID` (`lkc-…`, required on Cloud), `MONEDULA_CLOUD_LINK_NAME`, `MONEDULA_CLOUD_MIRROR_SOURCE_TOPIC` (an existing source topic on the linked cluster). Partial set → test failure, none → subtest skipped.
- `writeClusterConfig` adds `rest: {endpoint, clusterId, auth: {type: basic, username/password: valueFrom.env}}` when the REST set is present.
- Subtest `10_mirror`: `doctor` (exit 0, link listed); `apply` a `KafkaMirrorTopic` `<prefix>-mirror` with `sourceTopicName` = `MONEDULA_CLOUD_MIRROR_SOURCE_TOPIC` and no `topicName` (the smoke test assumes a link without a prefix and a source topic that is not yet mirrored; document both); `verify` → 0. Cleanup deletes the destination mirror topic through the Kafka Admin API and records the outcome; a failed cleanup is reported in the summary, not hidden. The summary table gains a `Mirrors` row.
- README: env table, what the subtest does, that it needs a pre-existing link and a dedicated source topic, cost note.

- [ ] **Step 1:** implement; `go vet -tags cloud ./test/e2e/cloud/` clean; `go test -tags cloud ./test/e2e/cloud/ -run TestCloud` with no `MONEDULA_CLOUD_*` set → skips (paste output).
- [ ] **Step 2: Commit** `test(cloud): opt-in mirror topic smoke test`

## Out of scope

Kind-based operator e2e for mirrors; a multi-version cluster-linking matrix (only 8.3 is pinned); REST auth/TLS against cp-server (the profile's REST is anonymous, matching the spike); performance of `ListMirrors` on large link sets.
