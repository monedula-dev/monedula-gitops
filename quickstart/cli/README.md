# Monedula GitOps - CLI Quickstart

This quickstart spins up a local single-node Kafka (KRaft) with SCRAM auth and a
Schema Registry via Docker Compose, then walks you through the full
`monedula-gitops` CLI workflow against it: `validate`, `apply`, `verify`, `diff`,
`import`, and a destructive change demo.

All commands below assume your current working directory is `quickstart/cli`
(this directory).

## What's here

```
quickstart/cli/
  docker-compose.yml          # Kafka (SCRAM) + kafka-init + Schema Registry
  .env.example                # credentials/endpoints (copy to .env)
  clusters/dev.yaml           # KafkaCluster "local" (env-based secret refs)
  manifests/
    payments-orders.yaml      # KafkaTopic payments.orders (+ access + AVRO schema)
    billing-access.yaml       # KafkaAccessPolicy billing-access
    schemas/orders-value.avsc # AVRO value schema (referenced by the topic)
```

## 1. Prerequisites

- Docker and the Docker Compose plugin (`docker compose version`).
- Go 1.22+ to build the CLI (`go version`). Alternatively use a prebuilt
  `monedula-gitops` binary on your `PATH` and skip the build step.

## 2. Configure credentials

```bash
cp .env.example .env
# edit .env if you want different credentials; defaults work out of the box
```

The defaults are:

| Variable             | Value                    | Used by                       |
| -------------------- | ------------------------ | ----------------------------- |
| `KAFKA_BOOTSTRAP`    | `localhost:9092`         | Kafka clients                 |
| `KAFKA_USERNAME`     | `admin`                  | SCRAM-SHA-512 super-user      |
| `KAFKA_PASSWORD`     | `admin-secret`           | SCRAM-SHA-512 super-user      |
| `SCHEMA_REGISTRY_URL`| `http://localhost:8081`  | Schema Registry endpoint      |
| `SR_USERNAME`        | `sr`                     | Schema Registry basic auth    |
| `SR_PASSWORD`        | `sr-secret`              | Schema Registry basic auth    |

`clusters/dev.yaml` references these via `valueFrom.env`, so the CLI reads the
credentials from your shell environment (step 4) rather than hardcoding them.

> **Why the super-user?** Monedula manages topics *and ACLs*, so it must bypass
> ACL enforcement. An app user (e.g. `monedula`) locks itself out the moment
> `apply` creates ACLs on a topic: with `allow.everyone.if.no.acl.found=true`, a
> resource stays open only while it has **no** ACLs — once the manifest's ACLs
> exist, any principal not listed loses access (the topic even disappears from
> that user's `--list`).

## 3. Start the stack

```bash
docker compose up -d
```

Wait for the services to become healthy:

```bash
docker compose ps
```

The `kafka-init` container creates the SCRAM users (including `monedula`) and
must finish before Kafka will accept the app credentials. Schema Registry starts
only after `kafka-init` succeeds and Kafka is healthy. Follow their logs until
things settle:

```bash
docker compose logs -f kafka-init schema-registry
```

`kafka-init` should exit 0 ("created"/"completed"); `schema-registry` should log
that it is listening on `0.0.0.0:8081`. Press Ctrl-C to stop following logs.

## 4. Export credentials for the CLI

The CLI's env resolver reads the values referenced by `clusters/dev.yaml` from
your shell environment. Load `.env` into the current shell:

```bash
set -a; source .env; set +a
```

(`set -a` exports every variable defined while sourcing, so `KAFKA_USERNAME`,
`KAFKA_PASSWORD`, `SR_USERNAME`, and `SR_PASSWORD` become available to the CLI.)

## 5. Build the CLI

From this directory (`quickstart/cli`):

```bash
go build -o ../../bin/monedula-gitops ../../cmd/monedula-gitops
```

Then either put it on your `PATH` or call it by path. The examples below assume
`monedula-gitops` is resolvable; substitute `../../bin/monedula-gitops` if not.
You can also run without building:

```bash
go run ../../cmd/monedula-gitops <args...>
```

## 6. The workflow

Validate the manifests against the cluster config. This parses the manifests,
resolves the referenced AVRO schema file, and checks cluster references. It does
**not** require a running Kafka:

```bash
monedula-gitops validate -f manifests --cluster-config-file clusters/dev.yaml
```

Preview the operations without changing anything:

```bash
monedula-gitops apply -f manifests --cluster-config-file clusters/dev.yaml --dry-run
```

Apply for real (creates the `payments.orders` topic, registers its AVRO value
schema, and provisions the ACLs):

```bash
monedula-gitops apply -f manifests --cluster-config-file clusters/dev.yaml
```

Verify the live cluster matches the manifests (exit 0 and "no drift" on success):

```bash
monedula-gitops verify -f manifests --cluster-config-file clusters/dev.yaml
```

Show the diff between live state and manifests (should be empty right after an
apply):

```bash
monedula-gitops diff -f manifests --cluster-config-file clusters/dev.yaml
```

Round-trip: import the live cluster state back into manifests and inspect them:

```bash
monedula-gitops import cluster --cluster-config-file clusters/dev.yaml --output-dir ./imported
ls -R ./imported
```

### Destructive change demo

Partition increases are destructive (they cannot be undone), so the CLI refuses
them unless you opt in. Bump the partition count and watch it get gated:

1. Edit `manifests/payments-orders.yaml` and change `partitions: 6` to
   `partitions: 12`.
2. A plain apply will refuse the destructive operation — it is reported as
   `Blocked` and the command exits 3 (approval pending, nothing mutated):

   ```bash
   monedula-gitops apply -f manifests --cluster-config-file clusters/dev.yaml
   ```

3. Re-run with the opt-in flag to actually grow the topic:

   ```bash
   monedula-gitops apply -f manifests --cluster-config-file clusters/dev.yaml --allow-destructive
   ```

(Revert the file back to `partitions: 6` when you're done experimenting; note
that partition counts can only ever increase.)

## Using native Kafka CLI tools

The broker image bundles the standard Kafka tools, and the stack mounts a
`config/client.properties` with the SASL/SCRAM settings, so you can poke at the
cluster directly — no host install required:

```bash
# List topics
docker compose exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --command-config /etc/kafka/secrets/client.properties --list

# Describe the topic created by `apply`
docker compose exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --command-config /etc/kafka/secrets/client.properties \
  --describe --topic payments.orders

# List ACLs
docker compose exec kafka kafka-acls \
  --bootstrap-server localhost:9092 \
  --command-config /etc/kafka/secrets/client.properties --list
```

`client.properties` authenticates as the super-user `admin`, so listings always
show every topic regardless of ACLs (edit it to use `monedula`/`monedula-secret`
to see the cluster as an ACL-restricted app user). If you have the Kafka CLI tools on
your host instead, point `--bootstrap-server localhost:9092` and
`--command-config quickstart/cli/config/client.properties` at them directly.

> Tip: for quick poking without auth you can also use the internal PLAINTEXT
> listener from inside the container: `--bootstrap-server localhost:9094` (no
> `--command-config` needed).

## 7. Teardown

```bash
docker compose down -v
```

The `-v` flag removes the volumes so the next `up` starts from a clean slate.

## Verification checklist

Success looks like:

- `docker compose ps` shows `kafka` and `schema-registry` healthy, and
  `kafka-init` exited 0.
- `validate` reports the resources as valid and exits 0.
- `apply --dry-run` lists create operations for the topic, schema, and ACLs.
- `apply` completes without errors.
- `verify` reports no drift and exits 0.
- `diff` shows no pending operations after a clean apply.
- `import cluster` writes `KafkaTopic` and `KafkaAccessPolicy` manifests under
  `./imported` that mirror what you applied.
- The destructive demo is refused without `--allow-destructive` and succeeds
  with it.

## Troubleshooting

- **A topic was applied but doesn't show up in `--list` / can't be read.** You
  are probably connecting as a non-super-user whose principal isn't in the
  topic's ACLs. Once `apply` creates the manifest's ACLs, the broker's
  `allow.everyone.if.no.acl.found=true` no longer applies to that topic, and any
  unlisted principal loses access (it even disappears from listings). Use the
  `admin` super-user (as `.env.example` and `client.properties` now do), or add
  your principal to the topic's `access` block.
- **`apply`/`verify` fail with authentication errors.** The `kafka-init`
  container must finish creating SCRAM users before the app user can log in.
  Check `docker compose logs kafka-init` and confirm it exited successfully.
  Also make sure `KAFKA_USERNAME`/`KAFKA_PASSWORD` in your shell (step 4) match
  the values in `.env`.
- **SCRAM auth errors (SaslAuthenticationException).** Your exported credentials
  don't match the ones `kafka-init` provisioned. Re-run
  `set -a; source .env; set +a` and verify the values with
  `echo $KAFKA_USERNAME`.
- **Schema Registry errors / it won't start.** Schema Registry needs its
  internal `_schemas` topic and a healthy Kafka. Wait for Kafka to be healthy
  and `kafka-init` to finish, then check `docker compose logs schema-registry`.
- **Schema Registry returns 401 on `apply`/`verify`/`import`.** The REST Basic-auth
  realm is rejecting the login. The JAAS `PropertyFileLoginModule` class path is
  Jetty-version-sensitive; the configs target the pinned `cp-schema-registry:8.0.0`
  (Jetty 12). If you change the image to a Jetty 9.4-11 build (cp 7.x) and start
  getting 401s, swap the class in `config/schema-registry-jaas.conf` back to
  `org.eclipse.jetty.jaas.spi.PropertyFileLoginModule`. To confirm which class the
  running image wants, check `docker compose logs schema-registry` for a
  `LoginException` / class-not-found naming the module.
  Confirm `SR_USERNAME`/`SR_PASSWORD` match `.env`.
- **`validate` reports "references cluster ... no KafkaCluster config".** Make
  sure you pass `--cluster-config-file clusters/dev.yaml`; the manifests use
  `clusterRef.name: local`, which is the name of the cluster in that file.
- **Schema file not found.** The topic's `valueSchema.valueFrom.file` path
  (`./schemas/orders-value.avsc`) is resolved relative to the manifest file, so
  run the commands from this directory with `-f manifests`.
