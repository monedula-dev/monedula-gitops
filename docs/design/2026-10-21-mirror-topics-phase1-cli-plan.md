# Mirror topics, Phase 1 (CLI vertical slice) Implementation Plan

**Goal:** Declare Confluent mirror topics as `KafkaMirrorTopic` manifests and have the CLI (`validate`, `diff`, `apply`, `verify`, `import`, `doctor`) create, check and report them, with a fail-closed mirror guard on `KafkaTopic`.

**Architecture:** A new Kafka REST v3 client (`internal/kafkarest`) reads links and mirrors and creates mirrors. A new `internal/mirror` package binds it to the Kafka Admin client (cluster-ID check), builds one complete per-run snapshot and classifies every destination topic. `internal/diff` turns desired mirrors plus that snapshot into operations, including non-executable lifecycle/state operations, and guards `KafkaTopic`. The executor writes through a `mirror.Writer` that re-reads live state just before each write.

**Tech Stack:** Go, franz-go (`kadm`), `net/http` + `internal/httpretry`, controller-gen, cobra.

**Spec:** `docs/design/2026-10-20-mirror-topics-design.md` (v7). Phase 1 = "Phase 1 — CLI vertical slice" of the spec. Operator (Phase 2) and the two-cluster e2e profile (Phase 3) get their own plans.

## Global Constraints

- Module path: `github.com/monedula-dev/monedula-gitops`. Test command: `go test ./...`. Regenerate with `make generate` (deepcopy) and `make manifests` (CRDs, RBAC, webhook, Helm CRD sync).
- REST `endpoint` is a base URL **without** `/v3` (e.g. `https://broker-0.dest:8090/kafka`); the client appends `/v3/...`. Topic and link names are URL path-escaped.
- `clusterId` optional on Confluent Platform (discovered with `GET /v3/clusters`, exactly one result required), required on Confluent Cloud. Discovery failure or ambiguity is an error.
- Kafka Admin cluster ID and REST cluster ID must both be non-empty and equal before any REST data is used or any REST write happens; otherwise `ClusterIdentityMismatch`.
- All REST list endpoints follow `metadata.next` to completion. A failed, unauthorized, malformed or incomplete response is unknown state, never an empty list.
- GETs may retry through `internal/httpretry`; POSTs are never retried.
- Mirror create body: `{"source_topic_name", "mirror_topic_name", "replication_factor", "configs": [{"name","value"}]}`; omit `replication_factor` and `configs` when not declared.
- `DELETE /v3/clusters/{id}/topics/{topic}` is never called. The CLI never deletes a mirror.
- `KafkaMirrorTopic.spec.deletionPolicy`: only `Orphan` (or empty). `replicationFactor` is create-time only.
- Effective destination name = `spec.topicName` when non-empty, else `spec.sourceTopicName`.
- `KafkaTopic` and `KafkaMirrorTopic` share one identity space keyed on `(clusterRef.name, effective topic name)`; in the CLI a collision is a hard validation error.
- Mirror status classification is by exact value: `STOPPED` and `PENDING_STOPPED` are checked before any other `PENDING_*`; an unrecognized status is `Unknown`.

## Decisions this plan makes (spec leaves them to the builder)

- Findings are new `operations.Action` values that are never executed, following the existing `Rejected`/`SchemaSuperseded` precedent, rather than a parallel findings type.
- Explicit `topicName: ""` is treated exactly like an omitted `topicName` (the spec allows "equivalent to omission" if applied consistently; Phase 2 CEL uses the same rule).
- `UpdateMirrorTopicConfig` writes through REST `configs:alter`, as the spec's `apply` section says.
- A declared config key is link-managed when it matches an entry of the link's `topic.config.sync.include` (exact, or prefix when the entry ends in `*`). If the link does not report `topic.config.sync.include`, every declared key is `Rejected` ("cannot determine whether <key> is link-managed").
- `mirror_topic_name` is always sent (the effective name), prefixed link or not.
- Link "direction" check = the link appears in the destination cluster's `GET /links` list and its `link_state` is `ACTIVE`; a non-`ACTIVE` link rejects creation.
- `import` keeps exit code 0 for partial imports; the JSON/YAML summary gets `partial: true` and each diagnostic warning starts with a stable code (`MirrorConfigNotImported:`, `MirrorImportDeferred:`, `ExternallyManagedOrAmbiguousACLs:`, `MirrorUnhealthy:`).
- `doctor` gains a `warn` status (does not make the run unhealthy). A cluster without `rest` reports the mirror-guard limitation as `skip` with a message, not `warn`, to avoid noise for non-Confluent users.
- The CLI does not enforce tenancy for any kind today (`internal/tenancy` doc); `KafkaMirrorTopic` follows that. Tenancy for mirrors lands with the operator in Phase 2.
- The `KafkaMirrorTopic` CRD is generated in Phase 1 (one type definition from day one, keeps `make manifests` drift-free) but no controller is registered until Phase 2.

## Review Focus

- REST endpoint written with a trailing slash or with `/v3` already on it: the client must still hit `<base>/v3/...` exactly once (slash), or `validate` must reject the `/v3` suffix. Test owned by Task 3 and Task 7.
- Live mirrors that no manifest declares (other teams' mirrors, other links): they must produce no operations and no errors. Test owned by Task 8.
- A cluster with `rest` configured whose manifests declare no topics and no mirrors (only policies/quotas/users): the CLI must not contact REST at all, so a REST outage cannot fail an ACL-only run. Test owned by Task 11.
- `ObserveOnly` mirror while REST is down: `verify` must exit 2 (operational error), not 0. Test owned by Task 11.
- Link reports `cluster.link.prefix` as an empty string: treat it as "no prefix", not as a prefix that every name must start with. Test owned by Task 8.

---

### Task 1: API types for `KafkaCluster.spec.rest` and `KafkaMirrorTopic`

**Files:**
- Modify: `internal/api/v1alpha1/types.go` (add `REST` to `KafkaClusterSpec`; add the new kind after `KafkaUser`)
- Modify: `internal/api/v1alpha1/zz_generated.deepcopy.go` (generated), `internal/api/v1alpha1/groupversion_info.go` only if kinds are registered there explicitly
- Create (generated): `config/crd/gitops.monedula.dev_kafkamirrortopics.yaml`, `charts/monedula-gitops/templates/crds/gitops.monedula.dev_kafkamirrortopics.yaml`; modify `config/crd/kustomization.yaml`
- Test: `internal/api/v1alpha1/mirror_types_test.go`

**Interfaces:**
- Produces:
  - `type KafkaRESTConfig struct { Endpoint string \`json:"endpoint"\`; Auth *MDSAuth \`json:"auth,omitempty"\`; TLS *TLSConfig \`json:"tls,omitempty"\`; ClusterID string \`json:"clusterId,omitempty"\` }` and `KafkaClusterSpec.REST *KafkaRESTConfig \`json:"rest,omitempty"\`` (auth reuses `MDSAuth`: same shape as `authorization.mds.auth`, per spec).
  - `type KafkaMirrorTopic struct` (TypeMeta, ObjectMeta, `Spec KafkaMirrorTopicSpec`, `Status *KafkaMirrorTopicStatus`) and `KafkaMirrorTopicList`, with `+kubebuilder:object:root=true` and `+kubebuilder:subresource:status` like `KafkaUser`.
  - `type KafkaMirrorTopicSpec struct { ClusterRef ClusterRef; LinkName string; SourceTopicName string; TopicName string (omitempty); ReplicationFactor *int (Minimum=1, omitempty); Config map[string]string (omitempty); Reconciliation *Reconciliation (omitempty); DeletionPolicy string (Enum=Orphan, omitempty) }` with JSON names `clusterRef, linkName, sourceTopicName, topicName, replicationFactor, config, reconciliation, deletionPolicy`.
  - `type KafkaMirrorTopicStatus struct { ObservedGeneration int64; Phase string; Conditions []metav1.Condition; ObservedMirrorStatus string; LinkName string; SourceTopicName string; LastObservedStatusChangeTime *metav1.Time; Drift *DriftStatus; LastCheckedTime *metav1.Time; LastAppliedTime *metav1.Time }` (JSON camelCase, all omitempty except as in `KafkaTopicStatus`).
  - `func (s KafkaMirrorTopicSpec) EffectiveTopicName() string`.

- [ ] **Step 1: Write the failing tests**

```go
func TestMirrorEffectiveTopicName(t *testing.T) {
	s := KafkaMirrorTopicSpec{SourceTopicName: "payments.orders"}
	if got := s.EffectiveTopicName(); got != "payments.orders" { t.Fatalf("omitted: %q", got) }
	s.TopicName = ""
	if got := s.EffectiveTopicName(); got != "payments.orders" { t.Fatalf("empty explicit: %q", got) }
	s.TopicName = "dr.payments.orders"
	if got := s.EffectiveTopicName(); got != "dr.payments.orders" { t.Fatalf("explicit: %q", got) }
}

func TestMirrorDeepCopyRoundTrip(t *testing.T) {
	rf := 3
	in := &KafkaMirrorTopic{Spec: KafkaMirrorTopicSpec{ClusterRef: ClusterRef{Name: "dr"}, LinkName: "prod-to-dr",
		SourceTopicName: "a", ReplicationFactor: &rf, Config: map[string]string{"min.insync.replicas": "2"}}}
	out := in.DeepCopy()
	*out.Spec.ReplicationFactor = 1
	out.Spec.Config["min.insync.replicas"] = "1"
	if *in.Spec.ReplicationFactor != 3 || in.Spec.Config["min.insync.replicas"] != "2" { t.Fatal("DeepCopy aliases spec") }
}

func TestKafkaClusterRESTDeepCopy(t *testing.T) {
	in := &KafkaCluster{Spec: KafkaClusterSpec{REST: &KafkaRESTConfig{Endpoint: "https://x:8090/kafka", ClusterID: "lkc-1"}}}
	out := in.DeepCopy()
	out.Spec.REST.ClusterID = "other"
	if in.Spec.REST.ClusterID != "lkc-1" { t.Fatal("DeepCopy aliases rest") }
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `go test ./internal/api/v1alpha1/ -run 'Mirror|KafkaClusterREST'`
Expected: FAIL (undefined: `KafkaMirrorTopicSpec`, `KafkaRESTConfig`).

- [ ] **Step 3: Add the types and markers**

CEL rules on `KafkaMirrorTopicSpec` (spec "Identity, validation and prefix"; empty explicit name = omitted):

```go
// +kubebuilder:validation:XValidation:rule="self.clusterRef.name == oldSelf.clusterRef.name",message="spec.clusterRef.name is immutable"
// +kubebuilder:validation:XValidation:rule="self.linkName == oldSelf.linkName",message="spec.linkName is immutable"
// +kubebuilder:validation:XValidation:rule="self.sourceTopicName == oldSelf.sourceTopicName",message="spec.sourceTopicName is immutable"
// +kubebuilder:validation:XValidation:rule="((has(oldSelf.topicName) && oldSelf.topicName != '') ? oldSelf.topicName : oldSelf.sourceTopicName) == ((has(self.topicName) && self.topicName != '') ? self.topicName : self.sourceTopicName)",message="the effective destination topic name is immutable (a different name needs a new KafkaMirrorTopic)"
```

Doc comment on `DeletionPolicy`: present only so `Delete` is rejected (spec v3 note: the API server prunes unknown fields).

- [ ] **Step 4: Regenerate**

Run: `make generate && make manifests`
Expected: new CRD files exist; `git status` shows only the files listed above plus generated deepcopy.

- [ ] **Step 5: Run tests**

Run: `go test ./internal/api/... ./internal/operator/manager/...`
Expected: PASS (`TestHelmChartCRDsMatchConfig` included).

- [ ] **Step 6: Commit**

```bash
git add internal/api config/crd charts/monedula-gitops/templates/crds
git commit -m "feat(api): add KafkaMirrorTopic kind and KafkaCluster rest block"
```

---

### Task 2: `AdminClient.ClusterID`

**Files:**
- Modify: `internal/kafka/admin.go`, `internal/kafka/franz/client.go`, `internal/kafka/mock/mock.go`
- Test: `internal/kafka/mock/mock_clusterid_test.go`, `internal/kafka/franz/integration_test.go` (add one case, build tag `integration`)

**Interfaces:**
- Produces: `AdminClient.ClusterID(ctx context.Context) (string, error)`. franz: `kadm.Client.Metadata(ctx)` → `ClusterID`; empty ID is an error (`"kafka: broker reported no cluster id"`). Mock: `fileState.ClusterID string \`json:"clusterId"\``, field on `*mock.Client`, setter `func (c *Client) SetClusterID(id string)`; empty → error `"mock: cluster id not set"`; a `failures` entry with `method: ClusterID` returns its error.

- [ ] **Step 1: Write the failing tests**

```go
func TestMockClusterID(t *testing.T) {
	c := mock.New(nil, nil)
	if _, err := c.ClusterID(context.Background()); err == nil { t.Fatal("want error when unset") }
	c.SetClusterID("abc")
	if id, err := c.ClusterID(context.Background()); err != nil || id != "abc" { t.Fatalf("got %q %v", id, err) }
}

func TestMockClusterIDFromFile(t *testing.T) { /* state file with clusterId: xyz -> "xyz" */ }
func TestMockClusterIDInjectedFailure(t *testing.T) { /* failures: [{method: ClusterID, error: boom}] -> error contains "boom" */ }
```

Integration (tag `integration`): `ClusterID` against the test broker returns a non-empty string.

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/kafka/...`
Expected: FAIL to compile (no method `ClusterID`).

- [ ] **Step 3: Implement in `admin.go`, `franz/client.go`, `mock/mock.go`.** Every other `AdminClient` implementation embeds the interface (e.g. `trackedScramAdmin` in operator tests), so no further changes are needed; confirm with `go build ./... && go vet ./...`.

- [ ] **Step 4: Run tests**

Run: `go test ./internal/kafka/... && go build ./...`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/kafka
git commit -m "feat(kafka): expose the broker cluster id on AdminClient"
```

---

### Task 3: Kafka REST v3 client, read side

**Files:**
- Create: `internal/kafkarest/client.go` (domain types + interface), `internal/kafkarest/confluent/client.go`
- Test: `internal/kafkarest/confluent/client_test.go` (`httptest.Server`)

**Interfaces:**
- Produces (package `kafkarest`):
  - `type Link struct { Name, State, Error string }` (`link_name`, `link_state`, `link_error`)
  - `type Mirror struct { LinkName, MirrorTopicName, SourceTopicName, Status string; NumPartitions int; StateTimeMs int64 }` (`link_name`, `mirror_topic_name`, `source_topic_name`, `mirror_status`, `num_partitions`, `state_time_ms`)
  - `type CreateMirrorRequest struct { LinkName, SourceTopicName, MirrorTopicName string; ReplicationFactor int; Configs map[string]string }`
  - `var ErrNotFound = errors.New("kafkarest: not found")`
  - `type Client interface { ClusterID(ctx) (string, error); ListLinks(ctx) ([]Link, error); LinkConfigs(ctx, link string) (map[string]string, error); ListMirrors(ctx) ([]Mirror, error); CreateMirror(ctx, r CreateMirrorRequest) error; AlterTopicConfigs(ctx, topic string, set map[string]string) error }`
- Produces (package `confluent`): `type Auth` with `BasicAuth(user, pass)`, `BearerAuth(token)`, `MTLSAuth()` (same shape as `internal/mds/confluent`); `func New(endpoint, clusterID string, auth *Auth, httpClient *http.Client) (*Client, error)`.
- Write methods are stubs returning `errors.New("not implemented")` until Task 4.

Behavior pinned by tests:
- `New` trims one trailing `/` from `endpoint` and rejects an endpoint whose path ends in `/v3` (`"rest endpoint must not include /v3"`).
- `ClusterID`: returns the configured ID without any request; otherwise `GET /v3/clusters` must return exactly one `data[].cluster_id`; 0 or >1 → error naming the count. Cached on the `*Client` after success; not cached on error.
- `LinkConfigs`: `GET /v3/clusters/{id}/links/{link}/configs` → map `name → value` (include defaults); `404` → `ErrNotFound`.
- `ListMirrors`: `GET /v3/clusters/{id}/links/-/mirrors`, following `metadata.next`. `next` must have the endpoint's scheme and host and a path under the endpoint's path, else error `"untrusted next link"`; a repeated `next` → error `"pagination loop"`; more than 1000 pages → error. Missing `mirror_topic_name`/`link_name`/`mirror_status` → error. Two entries with the same `mirror_topic_name` → error. `ListLinks` follows the same pagination rules.
- Any non-2xx except the `404` cases above → error that includes the status code and never the auth header.

- [ ] **Step 1: Write the failing tests**

```go
func TestNewRejectsV3Suffix(t *testing.T)               // New("https://h:8090/kafka/v3", ...) -> error contains "/v3"
func TestEndpointTrailingSlashJoinsOnce(t *testing.T)   // server asserts r.URL.Path == "/kafka/v3/clusters/c1/links"
func TestClusterIDConfiguredMakesNoRequest(t *testing.T) // server fails the test on any request
func TestClusterIDDiscoveryRequiresExactlyOne(t *testing.T) // 0 and 2 clusters -> errors; 1 -> id
func TestListMirrorsFollowsNext(t *testing.T)           // 3 pages, 5 mirrors total, order preserved
func TestListMirrorsRejectsForeignNext(t *testing.T)    // next on another host -> "untrusted next link"
func TestListMirrorsRejectsLoop(t *testing.T)           // page 2 next == page 1 url -> "pagination loop"
func TestListMirrorsRejectsDuplicateName(t *testing.T)
func TestListMirrorsMissingFieldIsError(t *testing.T)
func TestListMirrors403IsError(t *testing.T)            // error contains "403", not an empty slice
func TestLinkConfigs404IsErrNotFound(t *testing.T)      // errors.Is(err, kafkarest.ErrNotFound)
func TestBasicAuthHeaderNotInErrors(t *testing.T)       // 500 response; err.Error() lacks the base64 credentials
func TestPathEscaping(t *testing.T)                     // link "a b" -> RawPath contains "a%20b"
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/kafkarest/...`
Expected: FAIL to compile.

- [ ] **Step 3: Implement.** Copy the request helper shape from `internal/mds/confluent/confluent.go` (`do`, `applyAuth`, `statusError`), GETs through `httpretry.Do` as there.

- [ ] **Step 4: Run tests**

Run: `go test ./internal/kafkarest/...`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/kafkarest
git commit -m "feat(kafkarest): add Kafka REST v3 client for links and mirrors"
```

---

### Task 4: Kafka REST v3 client, writes

**Files:**
- Modify: `internal/kafkarest/confluent/client.go`
- Test: `internal/kafkarest/confluent/client_write_test.go`

**Interfaces:**
- Consumes: Task 3 `Client`, `CreateMirrorRequest`.
- Produces: real `CreateMirror` (`POST /v3/clusters/{id}/links/{link}/mirrors`) and `AlterTopicConfigs` (`POST /v3/clusters/{id}/topics/{topic}/configs:alter`, body `{"data":[{"name","value"}]}`), both sent exactly once (no `httpretry`).

- [ ] **Step 1: Write the failing tests**

```go
func TestCreateMirrorBody(t *testing.T) {
	// server decodes JSON; assert:
	// source_topic_name=="payments.orders", mirror_topic_name=="dr.payments.orders",
	// replication_factor==3, configs==[{"name":"min.insync.replicas","value":"2"}]
}
func TestCreateMirrorOmitsOptionalFields(t *testing.T) // RF 0 and nil Configs -> no "replication_factor", no "configs" keys
func TestCreateMirrorConfigsSortedByName(t *testing.T)   // deterministic order
func TestCreateMirrorNotRetriedOn503(t *testing.T)        // server counts requests: exactly 1, error returned
func TestAlterTopicConfigsBody(t *testing.T)              // {"data":[{"name":"min.insync.replicas","value":"2"}]}
```

- [ ] **Step 2: Run to verify failure** — `go test ./internal/kafkarest/confluent/ -run 'Create|Alter'` → FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** — `go test ./internal/kafkarest/...` → PASS.
- [ ] **Step 5: Commit**

```bash
git add internal/kafkarest
git commit -m "feat(kafkarest): create mirror topics and alter topic configs"
```

---

### Task 5: REST mock and client factory

**Files:**
- Create: `internal/kafkarest/mock/mock.go`
- Modify: `internal/cluster/clients.go`
- Test: `internal/kafkarest/mock/mock_test.go`, `internal/cluster/clients_test.go`

**Interfaces:**
- Produces:
  - `mock.New(clusterID string) *mock.Client` with exported mutators `AddLink(l kafkarest.Link, configs map[string]string)`, `AddMirror(m kafkarest.Mirror)`, `Fail(method string, err error)`; `mock.FromFile(path string) (*mock.Client, error)` reading YAML/JSON `{clusterId, links: [{name, state, error, configs}], mirrors: [{linkName, mirrorTopicName, sourceTopicName, status, numPartitions}], failures: [{method, error}]}`. `Created() []kafkarest.CreateMirrorRequest` and `Altered() map[string]map[string]string` for assertions. `CreateMirror` adds an `ACTIVE` mirror; `LinkConfigs` on an unknown link → `kafkarest.ErrNotFound`.
  - `cluster.BuildKafkaRESTClient(c *v1alpha1.KafkaCluster, r secrets.Resolver) (kafkarest.Client, error)` → `(nil, nil)` when `c.Spec.REST == nil`; auth/TLS resolution mirrors `BuildMDSClient`.

- [ ] **Step 1: Write the failing tests**

```go
func TestMockFromFile(t *testing.T)            // file with 1 link + 2 mirrors -> ListMirrors len 2, LinkConfigs ok
func TestMockFailureInjection(t *testing.T)    // failures: [{method: ListMirrors, error: boom}] -> error
func TestMockCreateMirrorRecorded(t *testing.T)
func TestBuildKafkaRESTClientNilWithoutREST(t *testing.T)
func TestBuildKafkaRESTClientBasicAuth(t *testing.T) // env-resolved username/password; httptest server sees Basic header
```

- [ ] **Step 2–4:** run (`go test ./internal/kafkarest/... ./internal/cluster/...`) → FAIL, implement, → PASS.
- [ ] **Step 5: Commit**

```bash
git add internal/kafkarest/mock internal/cluster
git commit -m "feat(kafkarest): add mock client and cluster factory"
```

---

### Task 6: `internal/mirror`: snapshot, cluster binding, classification

**Files:**
- Create: `internal/mirror/status.go`, `internal/mirror/snapshot.go`
- Test: `internal/mirror/status_test.go`, `internal/mirror/snapshot_test.go`

**Interfaces:**
- Consumes: Task 2 `ClusterID`, Task 3/5 `kafkarest.Client` + mock.
- Produces:
  - `type StatusClass int` with `StatusHealthy, StatusUnhealthy, StatusStopping, StatusStopped, StatusProgressing, StatusUnknown`; `func ClassifyStatus(status string) StatusClass`. Table: `ACTIVE`→Healthy; `PAUSED, LINK_PAUSED, FAILED, LINK_FAILED, SOURCE_UNAVAILABLE`→Unhealthy; `PENDING_STOPPED`→Stopping; `STOPPED`→Stopped; `PENDING_MIRROR, PENDING_SYNCHRONIZE, PENDING_SETUP_FOR_RESTORE, PENDING_RESTORE, PENDING_RESTORE_MIRROR`→Progressing; anything else (including other `PENDING_*`)→Unknown.
  - `type Classification string` with `Absent, Normal, Mirror, StoppingMirror, StoppedFormerMirror, Unknown` (values `"Absent"`, … as in the spec table).
  - `type LinkInfo struct { Link kafkarest.Link; Configs map[string]string; ConfigErr error }`
  - `type Snapshot struct { AdminClusterID, RESTClusterID string; Topics map[string]kafka.TopicState; Mirrors map[string]kafkarest.Mirror; Links map[string]LinkInfo; Err error }`
  - `var ErrClusterIdentityMismatch = errors.New("ClusterIdentityMismatch")`
  - `func ReadSnapshot(ctx context.Context, k kafka.AdminClient, r kafkarest.Client, linkNames []string) Snapshot` — reads Admin ID, REST ID (mismatch or either error → `Err` wraps `ErrClusterIdentityMismatch` with both IDs and stops), `ListTopics`, `ListMirrors`, `ListLinks`, then `LinkConfigs` for each name in `linkNames` that exists (per-link failure → `LinkInfo.ConfigErr`, not `Err`). Any other read failure → `Err`. `Err != nil` means every `Classify` returns `Unknown`.
  - `func (s Snapshot) Classify(topic string) Classification` — spec "Classification order" table, with `StatusUnknown` → `Unknown`.
  - `func (s Snapshot) Complete() bool` → `s.Err == nil`.

- [ ] **Step 1: Write the failing tests**

```go
func TestClassifyStatusTable(t *testing.T) // every literal above, plus "PENDING_FOO" and "" -> StatusUnknown

func TestClassify(t *testing.T) {
	// one table; each row builds a Snapshot directly:
	// no topic, no mirror                          -> Absent
	// topic, no mirror                             -> Normal
	// mirror ACTIVE (topic present)                -> Mirror
	// mirror FAILED                                -> Mirror
	// mirror PENDING_STOPPED                       -> StoppingMirror
	// mirror STOPPED, topic present                -> StoppedFormerMirror
	// mirror STOPPED, topic absent                 -> Unknown
	// mirror "WEIRD"                               -> Unknown
	// Err set                                      -> Unknown (even for a name with no data)
}

func TestReadSnapshotClusterIDMismatch(t *testing.T) // admin "A", rest "B" -> errors.Is(s.Err, ErrClusterIdentityMismatch); message has both ids
func TestReadSnapshotAdminIDError(t *testing.T)      // -> ErrClusterIdentityMismatch
func TestReadSnapshotListMirrorsFailure(t *testing.T) // -> s.Err non-nil, Classify("x") == Unknown
func TestReadSnapshotLinkConfigFailureIsPerLink(t *testing.T) // s.Err nil; Links["l"].ConfigErr non-nil
```

- [ ] **Step 2–4:** `go test ./internal/mirror/...` → FAIL, implement, → PASS.
- [ ] **Step 5: Commit**

```bash
git add internal/mirror
git commit -m "feat(mirror): classify destination topics from Kafka and REST state"
```

---

### Task 7: Load, default, validate and compile `KafkaMirrorTopic`

**Files:**
- Modify: `internal/loader/loader.go` (`Object.MirrorTopic *v1alpha1.KafkaMirrorTopic`, case `"KafkaMirrorTopic"` with `UnmarshalStrict`)
- Modify: `internal/defaulting/defaulting.go` (`func MirrorTopic(m *v1alpha1.KafkaMirrorTopic)`)
- Modify: `internal/validation/validation.go` (`Input.MirrorTopics`, `ValidateMirrorTopicShape`, `ValidateClusterREST`, identity + cluster checks in `Validate`)
- Modify: `internal/diff/diff.go` (add `DesiredMirror` and `Desired.Mirrors` only; no logic yet)
- Modify: `internal/pipeline/pipeline.go` (collect, `--cluster` filter, default, validate, `Plan.MirrorTopics`, `Plan.DesiredMirrors`)
- Test: `internal/loader/loader_test.go`, `internal/validation/mirror_test.go`, `internal/pipeline/mirror_test.go`

**Interfaces:**
- Produces:
  - `defaulting.MirrorTopic`: `Reconciliation` nil → `&Reconciliation{}`, empty mode → `"Enforce"` (same as `defaulting.Topic`), `DeletionPolicy ""` → `"Orphan"`. Does **not** write `TopicName` and applies no cluster defaults (RF stays unset unless declared).
  - `validation.ValidateMirrorTopicShape(m *v1alpha1.KafkaMirrorTopic) []error`: apiVersion; `clusterRef.name`, `linkName`, `sourceTopicName` required; `replicationFactor >= 1` when set; config keys non-empty; mode valid; `deletionPolicy` not in `{"", "Orphan"}` → `KafkaMirrorTopic <name>: deletionPolicy "Delete" is not supported; mirror topics are always orphaned in this version`.
  - `validation.ValidateClusterREST(c *v1alpha1.KafkaCluster) []error`: endpoint required, `http`/`https` scheme, path not ending in `/v3` or `/v3/`; `auth.type` basic needs username+password, bearer needs token, mtls needs `tls.clientCert`+`clientKey`; TLS via `validateTLSShape(name, "rest.tls", ...)`.
  - In `Validate`: identity key `clusterRef.name + "\x00" + effectiveName` shared with the existing `KafkaTopic` `seen` map (message `KafkaMirrorTopic %s collides with %s: both resolve to topic %q on cluster %q`, same for a topic colliding with a mirror); when clusters are loaded, the cluster must exist and have `spec.rest` (`KafkaMirrorTopic %s: cluster %q has no spec.rest; mirror topics need Kafka REST v3`).
  - `diff.DesiredMirror struct { Kind, Namespace, Name string; LinkName, SourceTopicName, TopicName string; TopicNameExplicit bool; ReplicationFactor int; Config map[string]string; Mode string }` (`TopicName` = effective name; `ReplicationFactor` 0 = not declared).
  - `pipeline.Plan.MirrorTopics []*v1alpha1.KafkaMirrorTopic`, `pipeline.Plan.DesiredMirrors []diff.DesiredMirror`.

- [ ] **Step 1: Write the failing tests**

```go
func TestLoaderDecodesMirrorTopicStrict(t *testing.T) // unknown field "configs:" -> error
func TestValidateMirrorDeletionPolicyDelete(t *testing.T) // error mentions "not supported"
func TestValidateMirrorRequiresREST(t *testing.T)      // cluster without rest -> error mentions "spec.rest"
func TestValidateMirrorTopicCrossKindCollision(t *testing.T) // KafkaTopic orders + mirror effective "orders" same cluster -> error
func TestValidateMirrorNoCollisionAcrossClusters(t *testing.T)
func TestValidateMirrorEmptyTopicNameIsOmitted(t *testing.T) // topicName "" and another mirror on same source -> collision
func TestValidateClusterRESTRejectsV3(t *testing.T)    // endpoint ".../kafka/v3/" -> error
func TestValidateClusterRESTAuthShape(t *testing.T)    // basic without password -> error
func TestPipelineDesiredMirrors(t *testing.T) {
	// manifest dir with one KafkaMirrorTopic (no topicName, mode default) ->
	// plan.DesiredMirrors[0] == {Kind:"KafkaMirrorTopic", Name:"orders-dr", TopicName:"payments.orders",
	//   TopicNameExplicit:false, LinkName:"prod-to-dr", SourceTopicName:"payments.orders", Mode:"Enforce"}
}
func TestPipelineClusterFilterAppliesToMirrors(t *testing.T)
```

- [ ] **Step 2–4:** `go test ./internal/loader/... ./internal/validation/... ./internal/defaulting/... ./internal/pipeline/...` → FAIL, implement, → PASS.
- [ ] **Step 5: Commit**

```bash
git add internal/loader internal/defaulting internal/validation internal/diff internal/pipeline
git commit -m "feat(pipeline): load, validate and compile KafkaMirrorTopic manifests"
```

---

### Task 8: Mirror operations and diff

**Files:**
- Modify: `internal/operations/operations.go`
- Create: `internal/mirror/rules.go` (prefix and link-managed rules, shared with the Task 10 writer)
- Create: `internal/diff/mirror.go`
- Modify: `internal/diff/diff.go` (`Live.Mirrors *mirror.Snapshot`; call `computeMirrorOps` in `Compute`)
- Test: `internal/diff/mirror_test.go`, `internal/operations/operations_test.go`

**Interfaces:**
- Consumes: Task 6 `mirror.Snapshot`, `ClassifyStatus`; Task 7 `DesiredMirror`.
- Produces (operations):
  - Executable, in `taxonomy`: `CreateMirrorTopic {RiskLow, GateNone}`, `UpdateMirrorTopicConfig {RiskLow, GateNone}`.
  - Never executed, **not** in `taxonomy`: `MirrorTopicUnhealthy`, `MirrorTopicStopping`, `MirrorTopicPromoted`, `MirrorTopicProgressing`, `UnsupportedReplicationFactorChange`, `StateReadError`.
  - `func Informational(a Action) bool` (true only for `MirrorTopicProgressing`), `func OperationalError(a Action) bool` (true only for `StateReadError`).
  - `Operation` payload: `MirrorLink string \`json:"-"\``, `MirrorSource string \`json:"-"\``, `MirrorNameExplicit bool \`json:"-"\`` (from `DesiredMirror.TopicNameExplicit`, so the Task 10 writer can re-run `CheckPrefix`); reuse `ReplicationFactor`, `Config`, `Field/From/To`.
- Produces (mirror rules, used by diff now and by the writer in Task 10):
  - `func CheckPrefix(l LinkInfo, topicName, sourceTopicName string, explicit bool) error` (nil = ok; error text is the `Rejected` message below)
  - `func LinkManaged(l LinkInfo, key string) (managed bool, known bool)` (`known == false` when `topic.config.sync.include` is absent)
- Produces (diff): `func computeMirrorOps(desired Desired, live Live) []operations.Operation`. Every op has `Kind/Namespace/Name/Target(=effective name)/Mode` from the desired mirror; `Message` set on every non-create op.

Decision table (in order; first match wins), per desired mirror `dm`, with `s := live.Mirrors`:

| Condition | Op |
|---|---|
| `s == nil` | `StateReadError` "Kafka REST not configured" (unreachable after validation; defensive) |
| `!s.Complete()` | `StateReadError` with `s.Err` text |
| `s.Links[dm.LinkName]` missing | `Rejected` "link %q not found on the destination cluster" |
| `LinkInfo.ConfigErr != nil` | `StateReadError` "cannot read link %q configs: …" |
| prefix check fails (below) | `Rejected` with the reason |
| classification `Absent` and link state != `ACTIVE` | `Rejected` "link %q is %s; not creating mirror" |
| `Absent` and a declared config key is link-managed or undeterminable | `Rejected` naming the key |
| `Absent` | `CreateMirrorTopic` (`MirrorLink`, `MirrorSource`, `ReplicationFactor`, `Config`) |
| `Normal` or `StoppedFormerMirror` | `Rejected` "topic exists and is not a mirror" (plus `MirrorTopicPromoted` for `StoppedFormerMirror`, so verify shows the lifecycle reason) |
| `Unknown` | `StateReadError` |
| live mirror's link or source differs from desired | `Rejected` "mirror of %s on link %s; manifest declares %s on %s" |
| `StoppingMirror` | `MirrorTopicStopping` |
| status Unhealthy | `MirrorTopicUnhealthy` (message includes raw status) |
| status Progressing | `MirrorTopicProgressing` |
| then, for a healthy/progressing/unhealthy mirror: declared RF > 0 and != live RF | `UnsupportedReplicationFactorChange` (`From`/`To`) |
| then, per declared config key (sorted) whose live value differs | link-managed or undeterminable → `Rejected`; else `UpdateMirrorTopicConfig` (`Field`=key, `From`, `To`) |

Prefix check: `p := LinkInfo.Configs["cluster.link.prefix"]`. `p == ""` → effective name must equal `SourceTopicName` (`Rejected` "topicName %q differs from sourceTopicName %q and link %q has no prefix"). `p != ""` → `TopicNameExplicit` must be true and `TopicName == p+SourceTopicName` (`Rejected` "link %q has prefix %q; topicName must be %q").

Link-managed check: `topic.config.sync.include` absent from `Configs` → undeterminable; else split on `,`, trim; entry `e` matches key `k` when `e == k` or (`strings.HasSuffix(e, "*")` and `strings.HasPrefix(k, strings.TrimSuffix(e, "*"))`).

- [ ] **Step 1: Write the failing tests** (one table test per row of the decision table, plus:)

```go
func TestMirrorOpsIgnoreUndeclaredLiveMirrors(t *testing.T) // 3 live mirrors on 2 links, none declared -> zero ops
func TestMirrorPrefixEmptyStringIsNoPrefix(t *testing.T)   // configs {"cluster.link.prefix": ""} + omitted topicName -> CreateMirrorTopic
func TestMirrorPrefixedLinkRequiresExplicitName(t *testing.T) // prefix "dr." + omitted topicName -> Rejected mentioning "dr.payments.orders"
func TestMirrorSyncIncludeWildcard(t *testing.T)           // include "min.insync.*" -> min.insync.replicas Rejected
func TestMirrorConfigUndeterminable(t *testing.T)          // no topic.config.sync.include -> Rejected "cannot determine"
func TestMirrorOpsCarryMode(t *testing.T)                  // DetectOnly desired -> op.Mode == "DetectOnly"; ReportOnly() true
func TestNonExecutableActionsHaveNoRisk(t *testing.T)      // RiskOf(MirrorTopicUnhealthy) == RiskNone, etc.
```

- [ ] **Step 2–4:** `go test ./internal/operations/... ./internal/diff/...` → FAIL, implement, → PASS.
- [ ] **Step 5: Commit**

```bash
git add internal/operations internal/mirror internal/diff
git commit -m "feat(diff): plan mirror topic creation, config drift and lifecycle findings"
```

---

### Task 9: `KafkaTopic` mirror guard in the diff

**Files:**
- Modify: `internal/diff/diff.go` (`computeTopicOps`, `computeACLOps`, `computeSchemaOps` filtering via a guard set computed once in `Compute`)
- Test: `internal/diff/topic_guard_test.go`

**Interfaces:**
- Consumes: `Live.Mirrors`.
- Produces: `func guardedTopics(desired Desired, live Live) map[string]operations.Operation` (topic name → the single op that replaces that topic's ops). Only when `live.Mirrors != nil`:
  - classification `Mirror` / `StoppingMirror` → `Rejected` "topic %q is a mirror topic on link %q; manage it with KafkaMirrorTopic";
  - `Unknown` → `StateReadError`.
  - `Normal`, `Absent`, `StoppedFormerMirror` → not guarded.
- For a guarded name: drop that topic's `CreateTopic`/`UpdateTopicConfig`/`IncreasePartitions`/`UpdateReplicationFactor`/`Rejected` ops, drop schema ops whose `Topic` is the name, drop `CreateAcl` ops whose ACL is `ResourceType=="topic"`, `PatternType=="literal"`, `ResourceName==name`; emit the guard op once (with the `KafkaTopic`'s Kind/Namespace/Name/Mode).
- `live.Mirrors == nil` (cluster has no `rest`) → behavior identical to today.

- [ ] **Step 1: Write the failing tests**

```go
func TestTopicGuardRejectsLiveMirror(t *testing.T)       // desired topic "orders" with config + access; live mirror ACTIVE -> exactly one Rejected, no CreateAcl/UpdateTopicConfig for "orders"
func TestTopicGuardStateReadError(t *testing.T)          // snapshot Err -> StateReadError, no CreateTopic
func TestTopicGuardStoppedFormerMirrorManaged(t *testing.T) // STOPPED + topic present -> normal UpdateTopicConfig ops
func TestTopicGuardKeepsPolicyACLsForOtherTopics(t *testing.T)
func TestTopicGuardNilSnapshotUnchanged(t *testing.T)    // Live.Mirrors nil -> same ops as before the change (compare with a golden slice)
```

- [ ] **Step 2–4:** `go test ./internal/diff/...` → FAIL, implement, → PASS (all existing diff tests still pass).
- [ ] **Step 5: Commit**

```bash
git add internal/diff
git commit -m "feat(diff): guard KafkaTopic against live mirror topics"
```

---

### Task 10: `mirror.Writer` and executor dispatch

**Files:**
- Create: `internal/mirror/writer.go`
- Modify: `internal/executor/executor.go`
- Test: `internal/mirror/writer_test.go`, `internal/executor/mirror_test.go`

**Interfaces:**
- Consumes: Task 4 writes, Task 6 `ReadSnapshot`, Task 8 ops and `mirror.CheckPrefix` / `mirror.LinkManaged`.
- Produces:
  - `type Writer struct { Kafka kafka.AdminClient; REST kafkarest.Client }`
  - `func (w Writer) CreateMirror(ctx context.Context, op operations.Operation) error` — fresh `ReadSnapshot(ctx, w.Kafka, w.REST, []string{op.MirrorLink})`; requires `Complete()`, `Classify(op.Target) == Absent`, link present and `ACTIVE`, `CheckPrefix` ok, no declared key link-managed/undeterminable; otherwise returns an error starting `"pre-write check: "` and does not POST. Then `REST.CreateMirror`. A POST error is wrapped as `"create mirror %s: outcome unknown, re-run diff before retrying: %w"`.
  - `func (w Writer) AlterMirrorConfig(ctx context.Context, op operations.Operation) error` — fresh snapshot; requires classification `Mirror` on `op.MirrorLink` with source `op.MirrorSource` and key not link-managed; then `REST.AlterTopicConfigs(ctx, op.Target, {op.Field: op.To})`.
  - `executor.Clients.Mirrors interface { CreateMirror(ctx, operations.Operation) error; AlterMirrorConfig(ctx, operations.Operation) error }` (nil → op `Failed` "Kafka REST not configured").
  - `applyOne` statuses (after the existing mode gate): `MirrorTopicProgressing` → `ReportOnly`; `MirrorTopicUnhealthy`, `MirrorTopicStopping`, `MirrorTopicPromoted`, `UnsupportedReplicationFactorChange` → `Unsupported` with `Err = op.Message`; `StateReadError` → `Failed` with `Err = op.Message`. A failed `CreateMirrorTopic` marks `failedTopics[op.Target]`.

- [ ] **Step 1: Write the failing tests**

```go
func TestWriterCreateRechecksTopicAppeared(t *testing.T) // Kafka mock gains topic after planning -> error "pre-write check", REST mock Created() empty
func TestWriterCreateRechecksMirrorAppeared(t *testing.T)
func TestWriterCreateClusterIDMismatch(t *testing.T)     // -> error, no POST
func TestWriterCreatePostFailureSaysOutcomeUnknown(t *testing.T)
func TestWriterAlterRequiresSameLink(t *testing.T)
func TestExecutorMirrorStatuses(t *testing.T)            // table: each non-executable action -> expected Status; Progressing keeps Result.OK() true
func TestExecutorCreateMirrorDispatch(t *testing.T)      // fake Mirrors records one call
```

- [ ] **Step 2–4:** `go test ./internal/mirror/... ./internal/executor/... ./internal/diff/...` → FAIL, implement, → PASS.
- [ ] **Step 5: Commit**

```bash
git add internal/mirror internal/executor
git commit -m "feat(executor): create and alter mirrors with a pre-write recheck"
```

---

### Task 11: CLI wiring for `diff` / `apply` / `verify`

**Files:**
- Modify: `internal/cli/live.go` (`buildRESTClient`, mock annotation `gitops.monedula.dev/mock-rest-file`), `internal/cli/diff.go` (`computeOps`), `internal/cli/verify.go`
- Create: `internal/cli/testdata/clusters/mirror-cluster.yaml`, `mirror-state.yaml` (Kafka mock, with `clusterId`), `mirror-rest.yaml` (REST mock), `mirror-rest-down.yaml` (`failures: [{method: ListMirrors, ...}]`), `internal/cli/testdata/mirror/*.yaml` manifests
- Test: `internal/cli/mirror_test.go`

**Interfaces:**
- Consumes: Tasks 5–10.
- Produces:
  - `func buildRESTClient(plan *pipeline.Plan, clusterConfigFiles []string) (kafkarest.Client, error)` — mock annotation first, else `cluster.BuildKafkaRESTClient`; `(nil, nil)` without `rest`.
  - In `computeOps`: when `len(plan.DesiredTopics)+len(plan.DesiredMirrors) > 0` and the REST client is non-nil, `snap := mirror.ReadSnapshot(ctx, client, rest, linkNames(plan.DesiredMirrors))`, pass `Live.Mirrors = &snap`, `Desired.Mirrors = plan.DesiredMirrors`, set `clients.Mirrors = mirror.Writer{Kafka: client, REST: rest}`. A snapshot error does **not** abort the run: it flows into `StateReadError` ops. A REST client build error is `ExitError{Code: 2}`.
  - `verify`: first, any op with `operations.OperationalError(op.Action)` → `ExitError{Code: 2, Msg: "live state could not be read; see StateReadError operations"}` regardless of mode; then skip `operations.Informational` ops; then the existing ObserveOnly rule.

- [ ] **Step 1: Write the failing tests** (use `run(t, ...)` from `cli_test.go`)

```go
func TestDiffMirrorCreate(t *testing.T)            // output contains "CreateMirrorTopic" and target
func TestApplyMirrorCreate(t *testing.T)           // exit 0; rendered apply result shows CreateMirrorTopic "Succeeded"
func TestVerifyMirrorUnhealthyExit1(t *testing.T)  // REST mock mirror FAILED, Enforce -> ExitError code 1
func TestVerifyMirrorProgressingExit0(t *testing.T) // PENDING_SYNCHRONIZE -> nil error
func TestVerifyObserveOnlyRestDownExit2(t *testing.T) // ObserveOnly mirror + mirror-rest-down -> ExitError code 2
func TestDiffTopicOnMirrorRejected(t *testing.T)   // KafkaTopic targeting a live mirror -> "Rejected" and "is a mirror topic"
func TestPolicyOnlyRunDoesNotContactREST(t *testing.T) // cluster has rest + mock-rest-file pointing at a file with failures on every method; manifests only a KafkaAccessPolicy -> diff succeeds
func TestClusterIDMismatchShowsStateReadError(t *testing.T)
```

- [ ] **Step 2–4:** `go test ./internal/cli/...` → FAIL, implement, → PASS.
- [ ] **Step 5: Commit**

```bash
git add internal/cli
git commit -m "feat(cli): diff, apply and verify KafkaMirrorTopic"
```

---

### Task 12: `import` of mirror topics

**Files:**
- Modify: `internal/importer/snapshot.go` (`SnapshotOptions.REST kafkarest.Client`; `Snapshot.Mirrors []MirrorSnapshot`, `Snapshot.MirrorLinks map[string]MirrorLinkInfo`, `Snapshot.DeferredMirrors []string`)
- Modify: `internal/importer/build.go` (`Result.Mirrors`, `Result.Partial`, `buildMirrors`, ambiguous-ACL filter), `internal/importer/output.go` (render, summary counts, `WriteToDir`), `internal/importer/namespace.go` (`AssignNamespaces` covers mirrors by effective topic name)
- Modify: `internal/cli/import.go` (build REST client via `buildRESTClient`-equivalent for import)
- Test: `internal/importer/mirror_test.go`, `internal/cli/mirror_import_test.go`

**Interfaces:**
- Produces:
  - `type MirrorSnapshot struct { Name, LinkName, SourceTopicName, Status string; ReplicationFactor int; ExplicitConfigKeys []string }`; `type MirrorLinkInfo struct { ACLSync *bool }` (`acl.sync.enable` parsed; nil when absent or unreadable).
  - `ReadSnapshot` with `REST` set: build `mirror.ReadSnapshot`; `Err` → return error (import fails, exit 2, "Unknown"). Classification `Mirror` → move the topic from `Topics` to `Mirrors`; `StoppingMirror` → remove from `Topics`, append to `DeferredMirrors`; `StoppedFormerMirror` → stays in `Topics`. Without `REST`, behavior unchanged.
  - `buildMirrors(snap Snapshot, clusterName string, warnings *[]string) []*v1alpha1.KafkaMirrorTopic`: `TopicName` always explicit, `ReplicationFactor` = observed, no `Config`, no `DeletionPolicy`, `imported-from-cluster` annotation; metadata names from `topicMetaName` + `disambiguateName` sharing the topics' owner map. Warnings: `MirrorConfigNotImported: mirror topic %q has N explicitly set configs; declare reviewed local keys manually` (when `ExplicitConfigKeys` non-empty), `MirrorUnhealthy: mirror topic %q is %s`, `MirrorImportDeferred: mirror topic %q is being promoted (PENDING_STOPPED); re-run import after it finishes`.
  - ACL filter at the top of `Build`: for each mirror whose link `ACLSync` is nil or true, remove from a copy of `snap.ACLs` every ACL with `ResourceType=="topic"`, `PatternType=="literal"`, `ResourceName==mirror.Name`, and warn `ExternallyManagedOrAmbiguousACLs: N ACL(s) on mirror topic %q were not imported (ACL sync on link %q is enabled or unknown)`. ACLs on mirrors whose link has `ACLSync == false` fall through to the existing `buildPolicies` path (they are not folded because mirrors are not in `topicByName`).
  - `Result.Partial` = any warning with one of the codes above; `ImportSummary` gets `Mirrors int` and `Partial bool` (`partial` in JSON/YAML).

- [ ] **Step 1: Write the failing tests**

```go
func TestImportMirrorNotAlsoTopic(t *testing.T)          // one physical topic -> 1 KafkaMirrorTopic, 0 KafkaTopic
func TestImportStoppedBecomesTopic(t *testing.T)
func TestImportPendingStoppedDeferred(t *testing.T)      // neither kind; warning code; Partial true
func TestImportUnknownFails(t *testing.T)                // REST ListMirrors failure -> ReadSnapshot error
func TestImportMirrorConfigsOmitted(t *testing.T)        // spec.config nil; MirrorConfigNotImported warning
func TestImportACLSyncEnabledDropsACLs(t *testing.T)     // no policy rule for the mirror; round trip does NOT fall back to all-explicit
func TestImportACLSyncDisabledKeepsPolicy(t *testing.T)  // ACL appears in a KafkaAccessPolicy
func TestImportSummaryPartialJSON(t *testing.T)          // CLI: -o json summary has "partial": true
```

- [ ] **Step 2–4:** `go test ./internal/importer/... ./internal/cli/...` → FAIL, implement, → PASS.
- [ ] **Step 5: Commit**

```bash
git add internal/importer internal/cli
git commit -m "feat(import): import mirror topics as KafkaMirrorTopic"
```

---

### Task 13: `doctor` checks for Kafka REST and links, plus CHANGELOG

**Files:**
- Modify: `internal/cli/doctor.go`, `CHANGELOG.md` (Unreleased: "KafkaMirrorTopic (CLI)")
- Test: `internal/cli/doctor_test.go`

**Interfaces:**
- Produces: `doctorWarn = "warn"` (does not affect `Healthy`). Checks, in order, all reported even after failures:
  - `rest`: no `spec.rest` → `skip` "Kafka REST not configured; mirror topics unavailable and KafkaTopic cannot detect out-of-band mirrors". Otherwise REST `ClusterID` → pass with the ID / fail.
  - `rest-cluster-binding`: Admin vs REST ID → pass "kafka=<A> rest=<A>" / fail "ClusterIdentityMismatch: kafka=<A> rest=<B>".
  - `rest-mirrors`: `ListMirrors` complete → pass "N mirror topics (read-only checks; create/alter permissions not tested)" / fail.
  - one `link:<name>` check per `ListLinks` entry: pass "state=ACTIVE prefix=<p or none>"; `warn` when `auto.create.mirror.topics.enable=true` ("mirrors may be created outside Git"); fail when state != `ACTIVE` or configs unreadable.

- [ ] **Step 1: Write the failing tests**

```go
func TestDoctorRESTSkippedWithoutRest(t *testing.T)
func TestDoctorRESTClusterMismatchFails(t *testing.T) // exit 2; check message contains both ids
func TestDoctorLinkAutoCreateWarns(t *testing.T)     // status "warn"; healthy stays true when everything else passes
func TestDoctorLinkPausedFails(t *testing.T)
```

- [ ] **Step 2–4:** `go test ./internal/cli/... && go test ./...` → FAIL, implement, → PASS (full suite).
- [ ] **Step 5: Commit**

```bash
git add internal/cli CHANGELOG.md
git commit -m "feat(doctor): check Kafka REST binding and cluster links"
```

---

## Out of scope for this plan (spec phases 2 and 3)

Operator controller/webhook/status/metrics for `KafkaMirrorTopic`, cross-kind duplicate gate and shared identity lock, `MirrorTopicStalled` and its 30-minute threshold, the `KafkaTopic` `Delete` finalizer guard with its verification budget, Secret rotation for REST credentials, the two-cluster `cp-server` e2e profile and scenario `26-mirror-topic`, the integration test of the REST client against a real `cp-server`, and the Confluent Cloud smoke test.
