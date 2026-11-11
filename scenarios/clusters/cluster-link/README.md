# cluster-link cluster profile

> DEV ONLY — two single-node cp-server clusters over PLAINTEXT, no authentication. Not for production.

Two single-node KRaft Confluent Server clusters (`confluentinc/cp-server:8.3.1`), a source
and a destination, joined by the destination-side cluster link `src-to-dest`. Used by the
`26-mirror-topic` e2e scenario to prove the product's Kafka REST client
(`internal/kafkarest/confluent`) can create and verify `KafkaMirrorTopic` resources against
real Confluent cluster linking.

The image reads `MONEDULA_KAFKA_IMAGE` like every other profile; in practice this profile
runs only under the `cp-8.3-link` cell, which pins the same `confluentinc/cp-server:8.3.1`
that the compose file carries as its literal default. Cluster linking needs no license key
(`confluent.cluster.link.enable` defaults to true).

## Services

| Container | Image | Role |
|---|---|---|
| `monedula-cl-source` | `confluentinc/cp-server:8.3.1` | Source cluster; mirror topics read from it |
| `monedula-cl-dest` | `confluentinc/cp-server:8.3.1` | Destination cluster the product manages; embedded REST v3 on :8090 |
| `monedula-cl-setup` | `confluentinc/cp-server:8.3.1` | One-shot: creates the topics below and the `src-to-dest` link |

`link-setup` waits for both brokers to be healthy, then creates:

- on the source: `orders` (3 partitions) and `payments` (1 partition);
- on the destination: `ledger`, a normal topic (for the mirror-name collision case);
- on the destination: the cluster link `src-to-dest` (`bootstrap.servers=source:9092`).

## Ports

| Port | Service |
|---|---|
| `19092` | Source Kafka, PLAINTEXT, host-facing |
| `29092` | Destination Kafka, PLAINTEXT, host-facing |
| `8090` | Destination embedded Kafka REST v3 (`http://localhost:8090/kafka/v3/...`) |

Host port 8090 is also used by the `auth-mds` profile, so the two stacks cannot run at the same time.

In the compose network the brokers are `source:9092` and `dest:9092`.

## Security model

No authentication anywhere: Kafka listeners are PLAINTEXT and the REST API is plain HTTP
with anonymous access. The destination still runs `StandardAuthorizer` with
`User:ANONYMOUS` as a super user, only so ACL reads (`doctor`'s `acl-read` check) do not
fail with `SECURITY_DISABLED`. The source has no authorizer.

## Credentials

None. `cluster.yaml` needs no environment variables.

## Files

```
cluster-link/
├── compose.yaml   # Docker Compose stack (source + dest + link-setup)
└── cluster.yaml   # KafkaCluster CR for the CLI: dest Kafka + rest.endpoint
```

## Cluster ids

Pinned through `CLUSTER_ID` in `compose.yaml`: source `c291cmNlLWNsdXN0ZXItMQ`,
destination `ZGVzdC1jbHVzdGVyLTAwMQ`. `cluster.yaml` does not pin `rest.clusterId`; the
client discovers the destination's id from the REST API.

## How to run (manual)

```bash
docker compose -p mon-e2e-link -f scenarios/clusters/cluster-link/compose.yaml up -d
docker compose -p mon-e2e-link -f scenarios/clusters/cluster-link/compose.yaml ps -a link-setup  # Exited (0)

bin/monedula-gitops doctor --cluster-config-file scenarios/clusters/cluster-link/cluster.yaml

docker compose -p mon-e2e-link -f scenarios/clusters/cluster-link/compose.yaml down -v
```

`up -d` returns once both brokers are healthy and `link-setup` has started; the link is
usable when `link-setup` has exited 0 (a few seconds later). The e2e runner
(`TestClusterLinkScenarios`) waits for that before running scenarios.
