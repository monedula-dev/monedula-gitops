# Cluster Profile: shared-sasl

Single-node Kafka (KRaft) + Schema Registry with SASL_PLAINTEXT / SCRAM-SHA-512
client authentication. This is the **default profile** used by most scenarios.
It is a thin wrapper around the quickstart stack — config files are copied
verbatim from `quickstart/cli/`.

## Listener summary

| Listener       | Protocol        | Auth               | Port |
| -------------- | --------------- | ------------------ | ---- |
| CLIENT_HOST    | SASL_PLAINTEXT  | SCRAM-SHA-512      | 9092 |
| CLIENT_DOCKER  | SASL_PLAINTEXT  | SCRAM-SHA-512      | 9095 |
| INTERNAL       | PLAINTEXT       | none (inter-broker)| 9094 |
| CONTROLLER     | PLAINTEXT       | KRaft quorum       | 9093 |

Schema Registry REST API is on `http://localhost:8081` with HTTP Basic auth.

## Credentials (DEMO ONLY)

| Variable          | Value           | Role                          |
| ----------------- | --------------- | ----------------------------- |
| `KAFKA_USERNAME`  | `admin`         | SCRAM super-user              |
| `KAFKA_PASSWORD`  | `admin-secret`  | SCRAM super-user password     |
| `SR_USERNAME`     | `sr`            | Schema Registry Basic auth    |
| `SR_PASSWORD`     | `sr-secret`     | Schema Registry Basic auth    |

Export them before running the CLI:

```bash
export KAFKA_USERNAME=admin
export KAFKA_PASSWORD=admin-secret
export SR_USERNAME=sr
export SR_PASSWORD=sr-secret
```

## CLI mode (Docker Compose)

```bash
# From scenarios/clusters/shared-sasl/
docker compose -f compose.yaml up -d --wait

# Tear down
docker compose -f compose.yaml down -v
```

The `kafka-init` one-shot container creates the SCRAM users before
Schema Registry starts. The `--wait` flag blocks until all services are
healthy and `kafka-init` has exited successfully.

## k8s mode

Apply the manifests in order (namespace first):

```bash
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/kafka.yaml
kubectl apply -f k8s/schema-registry.yaml
```

The `kafka-scram-bootstrap` Job populates SCRAM credentials over the internal
PLAINTEXT listener; Schema Registry's init-container waits for SASL readiness
before the Deployment starts.

Tear down:

```bash
kubectl delete -f k8s/schema-registry.yaml
kubectl delete -f k8s/kafka.yaml
kubectl delete -f k8s/namespace.yaml
```

## Compose overlays

This directory also has `compose.apache.yaml` and `compose.jetty9.yaml`. The e2e
runner (`test/e2e/cli`) layers these on top of `compose.yaml` (`-f compose.yaml
-f compose.apache.yaml -f compose.jetty9.yaml`) depending on the active matrix
cell: `compose.apache.yaml` for an `apache`-family broker image, `compose.jetty9.yaml`
for a cell that declares the `jetty9` overlay (cp-schema-registry 7.x's older
Jetty line needs a different JAAS class path — see
`config/schema-registry-jaas-jetty9.conf`). Both are resolved via
`MONEDULA_KAFKA_IMAGE` / `MONEDULA_SR_IMAGE`, which the runner exports from the
cell. `compose.yaml` still works standalone with a plain `docker compose up`,
since every image reference carries a literal default. If you change the
broker or Schema Registry service in `compose.yaml`, check whether the same
change needs to be mirrored in these overlays.

## cluster.yaml reference

`cluster.yaml` (this directory) defines the `shared` KafkaCluster CR that
scenarios reference via `clusterRef.name: shared`. Credentials are resolved
from environment variables, so the same file works in both CLI mode (env vars
in the shell) and k8s mode (injected as pod env vars from Secrets/ConfigMaps
by the operator).
