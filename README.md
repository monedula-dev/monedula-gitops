# Monedula GitOps

[![CI](https://github.com/monedula-dev/monedula-gitops/actions/workflows/ci.yml/badge.svg)](https://github.com/monedula-dev/monedula-gitops/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/monedula-dev/monedula-gitops)](https://github.com/monedula-dev/monedula-gitops/releases)
[![License: AGPL v3](https://img.shields.io/badge/license-AGPL--3.0-blue.svg)](LICENSE)
[![Go Report Card](https://goreportcard.com/badge/github.com/monedula-dev/monedula-gitops)](https://goreportcard.com/report/github.com/monedula-dev/monedula-gitops)

GitOps for Apache Kafka: topics, access, quotas, schemas, SCRAM users and Confluent RBAC, from a CLI or a Kubernetes operator.

**Documentation:** [monedula.dev/flock/docs/gitops](https://monedula.dev/flock/docs/gitops/)

- [Install monedula-gitops](https://monedula.dev/flock/docs/gitops/how-to/install/): the CLI and the operator's Helm chart
- [Get started with the CLI](https://monedula.dev/flock/docs/gitops/tutorials/getting-started-cli/) or [with the operator](https://monedula.dev/flock/docs/gitops/tutorials/getting-started-operator/)
- [CLI overview](https://monedula.dev/flock/docs/gitops/reference/cli/): every command, flag and exit code
- [Resource reference](https://monedula.dev/flock/docs/gitops/reference/resources/kafka-topic/): the fields of each resource kind, starting with `KafkaTopic`
- [Supported platforms and versions](https://monedula.dev/flock/docs/gitops/concepts/supported-platforms/): Apache Kafka, Confluent Platform and Confluent Cloud
- [How monedula-gitops compares](https://monedula.dev/flock/docs/gitops/concepts/comparisons/) with Jikkou, JulieOps, Strimzi, Confluent for Kubernetes, kafka-gitops and the Terraform provider

## Install

```bash
brew install monedula-dev/tap/monedula-gitops
```

The [install page](https://monedula.dev/flock/docs/gitops/how-to/install/) covers `go install`, Docker, the prebuilt binaries and the operator.

## Try it

Preview, apply and check a directory of manifests against the cluster that `cluster.yaml` describes:

```bash
monedula-gitops diff   -f manifests/ --cluster-config-file cluster.yaml
monedula-gitops apply  -f manifests/ --cluster-config-file cluster.yaml
monedula-gitops verify -f manifests/ --cluster-config-file cluster.yaml   # exits 1 on drift
```

[`quickstart/cli`](quickstart/cli/) has a local Kafka and Schema Registry and the manifests to run these commands against.

## Runnable examples

- [`quickstart/`](quickstart/) holds two local playgrounds: [`cli/`](quickstart/cli/) with Docker Compose and [`k8s/`](quickstart/k8s/) with the operator on k3d. [Get started with the CLI](https://monedula.dev/flock/docs/gitops/tutorials/getting-started-cli/) and [Get started with the operator](https://monedula.dev/flock/docs/gitops/tutorials/getting-started-operator/) walk through them step by step.
- [`scenarios/`](scenarios/) holds 25 end-to-end scenarios, each with a README and a test that CI runs. Each how-to guide in the docs links the scenario that runs it; for example, [Grant producers and consumers access](https://monedula.dev/flock/docs/gitops/how-to/grant-topic-access/) links [`scenarios/05-topic-with-access`](scenarios/05-topic-with-access/).

## Validated versions

CI runs the adapter integration tests against every cell below on each pull request, and the end-to-end scenario suite against the `e2e` cells nightly, on every push to `main` and on demand. Confluent Cloud is validated with the opt-in maintainer harness (`make e2e-cloud`), not in CI; [Supported platforms and versions](https://monedula.dev/flock/docs/gitops/concepts/supported-platforms/#confluent-cloud) lists what it supports.

<!-- BEGIN GENERATED: version-matrix -->
| Cell | Broker image | Schema Registry image | Tiers | Capabilities |
|---|---|---|---|---|
| `ak-3.9` | `apache/kafka:3.9.2` | `—` | integration | topics, acls, quotas, scram |
| `ak-4.0` | `apache/kafka:4.0.2` | `—` | integration | topics, acls, quotas, scram |
| `ak-4.3` | `apache/kafka:4.3.1` | `confluentinc/cp-schema-registry:8.3.1` | integration, e2e | topics, acls, quotas, scram, schemaregistry |
| `cp-7.6` | `confluentinc/cp-kafka:7.6.13` | `confluentinc/cp-schema-registry:7.6.13` | integration | topics, acls, quotas, scram, schemaregistry |
| `cp-7.9` | `confluentinc/cp-kafka:7.9.9` | `confluentinc/cp-schema-registry:7.9.9` | integration, e2e | topics, acls, quotas, scram, schemaregistry |
| `cp-8.0` | `confluentinc/cp-kafka:8.0.7` | `confluentinc/cp-schema-registry:8.0.7` | integration | topics, acls, quotas, scram, schemaregistry |
| `cp-8.3` | `confluentinc/cp-kafka:8.3.1` | `confluentinc/cp-schema-registry:8.3.1` | integration, e2e | topics, acls, quotas, scram, schemaregistry |
| `cp-7.6-mds` | `confluentinc/cp-server:7.6.13` | `—` | e2e | topics, acls, quotas, scram, mds |
<!-- END GENERATED: version-matrix -->

`make matrix-docs` generates this table from [`internal/matrix/versions.yaml`](internal/matrix/versions.yaml), and CI fails if it is stale.

## Building from source

With Go 1.26 or newer:

```bash
git clone https://github.com/monedula-dev/monedula-gitops.git
cd monedula-gitops
go build ./cmd/monedula-gitops
go test ./...
```

[CONTRIBUTING.md](CONTRIBUTING.md) lists the lint checks and the end-to-end suites for the CLI (Docker) and the operator (kind and bats).

## Contributing and security

Report bugs and request features in [GitHub issues](https://github.com/monedula-dev/monedula-gitops/issues). Read [CONTRIBUTING.md](CONTRIBUTING.md) before opening a pull request, and [SECURITY.md](SECURITY.md) before reporting a vulnerability. [CHANGELOG.md](CHANGELOG.md) lists the changes in each release.

## License

monedula-gitops is licensed under the AGPL-3.0; see [LICENSE](LICENSE) and [NOTICE](NOTICE). Using it on your own clusters, as a CLI or as an operator in your own infrastructure, does not oblige you to share your code or manifests: AGPL obligations apply when you distribute it, modified or not, or let others use a modified version over a network. For commercial licensing, [contact us](https://monedula.dev/contact/).

Apache Kafka, Kafka and the Kafka logo are trademarks of The Apache Software Foundation. monedula-gitops is an independent project that works with Kafka clusters; it is not affiliated with, endorsed by or sponsored by the ASF.
