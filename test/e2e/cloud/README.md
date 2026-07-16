# Confluent Cloud validation harness

An **opt-in, maintainer-run** test suite that validates monedula-gitops against a
real Confluent Cloud cluster. The support matrix in
[docs/connecting.md](../../../docs/connecting.md) marks Confluent Cloud as
⚠️ untested; this harness is how a maintainer turns that ⚠️ into evidence.

It exercises what *should* work on Cloud (topics, ACLs, schemas, import) and
proves that what Cloud does *not* expose through the Kafka Admin API (client
quotas, SCRAM users) fails gracefully — non-zero exit, the broker's own error
text, no panic. The final log block is a fixed-width verdict table you can
paste into a support-matrix update.

It never runs in CI and never in the normal test suite: it is behind the
`cloud` build tag and gated on `MONEDULA_CLOUD_*` environment variables.

## Cost and safety

- **Use a dedicated test cluster. Never point this at production.** The
  harness creates and deletes topics, ACLs, subjects — and applies
  deliberately-rejected quota/user changes — against whatever cluster the env
  vars name.
- A Confluent Cloud **Basic** cluster has no base cost (you pay per use;
  new accounts come with promo credits). A full harness run creates one
  1-partition topic for a few minutes — expect **cents per run**, usually
  covered entirely by free credits.
- Every resource carries a unique run prefix `mgcc<unix-timestamp>` (topic
  `mgcc<ts>.demo`, subject `mgcc<ts>.demo-value`, policy/quota/user names
  `mgcc<ts>-*`). Cleanup runs via `t.Cleanup` and deletes **only** resources
  carrying the prefix — even when subtests fail.
- An **aborted** run (Ctrl-C, machine death, `-timeout` kill) can skip
  cleanup. Leftovers are easy to spot: anything named `mgcc*`. Remove them in
  the Cloud console or CLI, e.g.:

  ```sh
  confluent kafka topic list | grep '^ *mgcc'
  confluent kafka topic delete <topic>
  confluent kafka acl list          # look for mgcc* resource names
  curl -su "$MONEDULA_CLOUD_SR_KEY:$MONEDULA_CLOUD_SR_SECRET" \
    "$MONEDULA_CLOUD_SR_URL/subjects" | grep -o 'mgcc[^"]*'
  ```

## Setup

1. **Confluent Cloud account** — sign up at <https://confluent.cloud> (promo
   credits apply to new accounts).
2. **Basic cluster** — create a Basic cluster in any cloud/region. Note the
   bootstrap endpoint (`pkc-....confluent.cloud:9092`).
3. **Kafka API key** — create a cluster-scoped API key (cluster admin access,
   so it can create topics/ACLs and *attempt* quota/user changes).
4. **Schema Registry API key** *(optional — enables the schema subtests)* —
   enable Schema Registry (Essentials) for the environment, note its endpoint
   URL, and create an SR API key.
5. **Service account** *(optional — enables the ACL subtest)* — create a
   service account and note its id (`sa-...`). It needs no permissions; it is
   only the *principal* the test grants ACLs to.

Export the environment variables:

```sh
# Required (core trio):
export MONEDULA_CLOUD_BOOTSTRAP=pkc-xxxxx.eu-central-1.aws.confluent.cloud:9092
export MONEDULA_CLOUD_API_KEY=XXXXXXXXXXXXXXXX
export MONEDULA_CLOUD_API_SECRET=...

# Optional — Schema Registry subtests (set all three or none):
export MONEDULA_CLOUD_SR_URL=https://psrc-xxxxx.eu-central-1.aws.confluent.cloud
export MONEDULA_CLOUD_SR_KEY=XXXXXXXXXXXXXXXX
export MONEDULA_CLOUD_SR_SECRET=...

# Optional — ACL subtest: the NUMERIC service-account id, NOT the sa- resource id
export MONEDULA_CLOUD_SERVICE_ACCOUNT_ID=9207877
```

**The service-account id must be the NUMERIC id** (e.g. `9207877`), not the
`sa-xxxxxx` resource id. Confluent Cloud stores Kafka-protocol ACLs under the
numeric id: `CreateAcls` with a `User:sa-...` principal is acknowledged and
then **silently dropped** (never persisted), and `DescribeAcls` returns the
numeric form — so `sa-` principals can neither persist nor round-trip through
a diff (validated against a live Basic cluster, 2026-07-09). The Cloud CLI and
console translate between the two forms; the raw protocol does not. To find
the numeric id: create any ACL for the account with
`confluent kafka acl create ... --service-account sa-xxxxxx`, read it back over
the protocol (e.g. `monedula-gitops import cluster`), and note the
`User:<number>` principal; then delete the probe ACL.

Secret hygiene: the harness writes a cluster config that references these
variables **by name** (`valueFrom.env`); no secret value is ever written to a
file or a log line.

## Run

```sh
go test -tags cloud ./test/e2e/cloud/ -v -timeout 20m -count=1
# or:
make e2e-cloud
```

`-count=1` matters: the env-var gating reads `os.Environ()`, which go test's
result cache does not track — without it a cached result could mask a newly
configured (or newly fixed) environment.

Gating behavior:

- **No `MONEDULA_CLOUD_*` vars set** → the suite **skips** with guidance
  (safe to run anywhere; nothing is contacted).
- **Some vars set but the core trio incomplete** → the suite **fails**,
  listing the missing variables. An explicitly configured run must never
  silently pass while doing nothing.
- **SR trio partially set** → fail (same reasoning).

## What runs, what skips

| Subtest | Needs | Expectation |
|---|---|---|
| `01_doctor` | core trio | connectivity + admin reads healthy |
| `02_topic_apply` | core trio | create topic (RF=3), verify no drift |
| `03_topic_config_restriction` | core trio | `min.insync.replicas=3` **rejected** by broker policy, surfaced cleanly |
| `04_drift` | core trio | out-of-band `retention.ms` change detected and reconciled |
| `05_acl` | `MONEDULA_CLOUD_SERVICE_ACCOUNT_ID` (skips otherwise) | grant, no-diff, prune removal |
| `06_schema` | SR trio (skips otherwise) | AVRO register + compatible evolution to version 2 |
| `07_quota_negative` | core trio | quota apply **fails gracefully** (Cloud manages quotas itself) |
| `08_user_negative` | core trio | SCRAM user apply **fails gracefully** (Cloud has no SCRAM APIs) |
| `09_import` | core trio | `import cluster` emits the run topic, skips internal topics, leaks no secrets (falls back to `--skip-users` if Cloud rejects SCRAM listing) |
| summary | — | fixed-width verdict table (Topics / ACLs / Schemas / Quotas / Users / Import) |

A validated run does **not** by itself change the support matrix — update
[docs/connecting.md](../../../docs/connecting.md) with the summary table from
the log, citing the run date and cluster type.
