# Scenario 26 — Mirror topic over a cluster link

**Modes:** cli  
**Cluster profile:** cluster-link

## What this teaches

A `KafkaMirrorTopic` declares a Confluent mirror topic on an existing cluster link. The CLI
manages it through the destination cluster's Kafka REST v3 API: `apply` creates the mirror,
`verify` reports its health, and `import` brings it back as a `KafkaMirrorTopic` manifest.

The `cluster-link` profile runs two single-node Confluent Server clusters. Its one-shot
`link-setup` service creates the source topics `orders` and `payments`, a normal topic `ledger`
on the destination, and the `src-to-dest` link on the destination (no prefix). The scenario then:

1. runs `doctor`: Kafka REST is reachable and bound to the destination, and the link is listed
   (`link:src-to-dest`);
2. applies `manifests/orders-mirror.yaml` (`CreateMirrorTopic`) and verifies it clean;
3. produces `m1`, `m2`, `m3` to `orders` on the source and consumes them from the mirror on the
   destination;
4. pauses the mirror with `kafka-mirrors --pause`: `verify` exits 1 with `MirrorTopicUnhealthy`
   (status `PAUSED`); after `--unpause`, `verify` is clean again;
5. applies `manifests-collision/ledger-mirror.yaml`, a mirror whose destination name `ledger` is
   already a normal topic: the plan rejects it (`topic exists and is not a mirror`) and `apply`
   exits 2 without writing anything;
6. imports the destination cluster and verifies the imported manifests are drift-free.

The live-state check confirms `orders` is an `ACTIVE` mirror of `orders` on `src-to-dest`, and
that no mirror named `ledger` exists.

## How to run

### CLI mode

```bash
docker compose -p mon-e2e-link -f ../clusters/cluster-link/compose.yaml up -d
docker compose -p mon-e2e-link -f ../clusters/cluster-link/compose.yaml ps -a link-setup  # wait until it shows Exited (0)

monedula-gitops doctor --cluster-config-file ../clusters/cluster-link/cluster.yaml
monedula-gitops apply  -f manifests --cluster-config-file ../clusters/cluster-link/cluster.yaml
monedula-gitops verify -f manifests --cluster-config-file ../clusters/cluster-link/cluster.yaml
```

The e2e runner drives the whole sequence from `expect.yaml`, running the Kafka tools inside the
compose services with `exec` steps.

## Expected outcome

`doctor`, `apply` and `verify` exit 0; records produced on the source arrive on the destination
mirror; `verify` exits 1 while the mirror is paused; the collision `apply` exits 2 with a
`Rejected` operation; the imported manifests verify clean.

## Cleanup

```bash
./cleanup.sh cli   # no-op — the runner tears the stack down (compose down -v)
./cleanup.sh k8s   # no-op — cli-only scenario
```
