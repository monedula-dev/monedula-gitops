# Monedula GitOps quickstarts

Two self-contained playgrounds for trying Monedula GitOps end to end against a
real Kafka cluster and Schema Registry. Pick whichever front-end you want to
exercise — the CLI or the in-cluster operator — and follow its README for
step-by-step instructions.

## The two playgrounds

| Playground | What it does | Prerequisites |
|------------|--------------|---------------|
| [`cli/`](cli/README.md) | Spins up Kafka + Schema Registry with Docker Compose, then drives the `monedula-gitops` CLI through the full workflow: `validate`, `apply`, `verify`, `diff`, `import`, and a destructive-change demo. | Docker (+ Compose plugin), Go |
| [`k8s/`](k8s/README.md) | Runs the operator on a local [k3d](https://k3d.io) cluster against an in-cluster Kafka, then watches sample `KafkaCluster` / `KafkaTopic` / `KafkaAccessPolicy` resources reconcile. | Docker, k3d, kubectl (Go + Docker are used to build the operator image) |

Both stacks authenticate with **SASL/SCRAM-SHA-512** to Kafka and a
**basic-auth Schema Registry**, so they exercise the same auth paths you would
use in a real deployment.

> **The credentials in these quickstarts are fake demo values.** They are
> committed only to make the playgrounds work out of the box — never reuse them
> in production. In a real setup, source credentials from your secrets manager
> (env/file for the CLI, a Kubernetes `Secret` for the operator).

## Next steps

- CLI walkthrough: [`cli/README.md`](cli/README.md)
- Operator walkthrough: [`k8s/README.md`](k8s/README.md)
- Full tool documentation: the [repo root README](../README.md)
