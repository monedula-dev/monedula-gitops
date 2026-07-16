# Monedula GitOps

Declarative GitOps for Apache Kafka — topics, ACLs, quotas, schemas, and RBAC.

- **A topic and its access in one manifest** — declare the topic, its config, and its producers/consumers together, instead of maintaining topics and ACLs in separate places.
- **Works with Apache Kafka *and* Confluent Platform** — open-source Kafka plus Confluent extras (Schema Registry, MDS/RBAC).
- **Start from an existing cluster in minutes** — `import cluster` reverse-engineers manifests from live state, with a guaranteed drift-free round-trip.
- **One tool, two modes** — run it as a CLI (e.g. in CI/CD pipelines) or as a Kubernetes operator, driven by the same model and engine.

[![CI](https://github.com/monedula-dev/monedula-gitops/actions/workflows/ci.yml/badge.svg)](https://github.com/monedula-dev/monedula-gitops/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/monedula-dev/monedula-gitops)](https://github.com/monedula-dev/monedula-gitops/releases)
[![License: AGPL v3](https://img.shields.io/badge/license-AGPL--3.0-blue.svg)](LICENSE)
[![Go Report Card](https://goreportcard.com/badge/github.com/monedula-dev/monedula-gitops)](https://goreportcard.com/report/github.com/monedula-dev/monedula-gitops)

Monedula GitOps manages Apache Kafka resources — topics, ACLs, access policies, user and IP quotas, Schema Registry schemas, and RBAC role bindings — as declarative, Kubernetes-style YAML. It runs both as a CLI for CI/CD pipelines and as an in-cluster Kubernetes operator, driven by the same model and engine. In either mode it validates manifests, diffs them against live state, and applies (or reconciles) the operations needed to converge.

## Table of contents

- [Why Monedula GitOps](#why-monedula-gitops)
- [Features](#features)
- [Alternatives](#alternatives)
- [Installation](#installation)
- [Quickstart](#quickstart)
- [Documentation](#documentation)
- [Building from source](#building-from-source)
- [Contributing](#contributing)
- [License](#license)

## Why Monedula GitOps

Kafka topic, ACL, quota, schema, and RBAC configuration tends to drift. It is often managed ad hoc — through scripts, consoles, and tribal knowledge — with no single source of truth, so what runs in production rarely matches what anyone intended.

Monedula GitOps makes the desired state explicit as version-controlled YAML and converges the cluster to it. It is safe by default: `--dry-run` previews every change, `verify` detects drift for CI gating, and deletions/prunes are opt-in (`--prune`). The same model and engine work as a CLI in your pipelines and as an operator inside Kubernetes, so the gating and drift semantics match exactly. Already running a cluster? `import cluster` reverse-engineers manifests from live state to bootstrap you, with a guaranteed drift-free round-trip.

### A topic and its access in one place

The design goal was to be *simpler* than the alternatives for the case you hit every day. Most Kafka tooling keeps a topic in one place and its access in another: Strimzi splits them across `KafkaTopic` and `KafkaUser`, Confluent for Kubernetes and Jikkou put ACLs in separate principal/role resources, and Terraform models every ACL as its own resource. Even the tools that keep everything in one file (JulieOps, kafka-gitops) anchor access on the *application or project*, not the topic. Keeping two separate objects in sync is the tedious, drift-prone part.

Monedula GitOps attaches access to the **topic itself** — the common case (a topic plus the apps that produce to and consume from it) is a single manifest:

```yaml
apiVersion: gitops.monedula.dev/v1alpha1
kind: KafkaTopic
metadata:
  name: orders
spec:
  clusterRef: { name: prod }
  topicName: orders
  partitions: 6
  config:
    retention.ms: "604800000"
  access:
    producers:
      - principal: User:svc-checkout
    consumers:
      - principal: User:svc-fraud
        group: fraud-orders
```

That compiles to the right ACLs automatically (and, on RBAC-backed clusters, to Confluent MDS role bindings). When you do need cross-cutting or advanced rules — prefixed patterns, shared consumer groups, host restrictions — standalone [`KafkaAccessPolicy`](docs/manifest-reference.md) and [`KafkaRoleBinding`](docs/manifest-reference.md) resources are there. The simple case stays simple; the complex case stays possible.

The one piece still missing was the principal itself: `svc-checkout` has to actually exist as a SCRAM credential before its ACLs mean anything. [`KafkaUser`](docs/manifest-reference.md#kafkauser) declares that too, so the topic, its access, and the principal that uses it all live in Git together:

```yaml
apiVersion: gitops.monedula.dev/v1alpha1
kind: KafkaUser
metadata:
  name: svc-checkout
spec:
  clusterRef: { name: prod }
  password:
    valueFrom: { env: SVC_CHECKOUT_PASSWORD }
```

## Features

- **Topics** — partitions, configs, and lifecycle managed declaratively.
- **ACLs** — topic-local `spec.access` grants plus standalone `KafkaAccessPolicy` rules, with cross-resource Allow/Deny conflict detection.
- **Quotas** — per-`user` / per-`client-id` quotas (byte rate, request %, controller mutation rate) and per-IP connection-rate quotas.
- **Schemas** — Schema Registry registration and guarded compatibility evolution, plus compatibility-only governance mode.
- **RBAC role bindings** — Confluent MDS role bindings, coexisting with ACLs and auto-mappable from topic access.
- **Users** — Kafka SCRAM principals (username, mechanism, password) managed declaratively, with event-driven password rotation and operator-generated credentials.
- **Drift detection** — `verify` exits non-zero on drift for CI gating.
- **Import** — `import cluster` round-trips live topics, ACLs, quotas, schemas, and RBAC back to manifests.
- **Doctor** — `doctor` preflight checks connectivity and readiness without mutating anything.
- **Authentication** — SASL/SCRAM, SASL_SSL, mTLS, OAUTHBEARER, and MDS.
- **CLI + Kubernetes operator** — admission webhooks, reconciliation modes, finalizers, and multi-tenancy.

### Support matrix

| Capability | Apache Kafka | Confluent Platform | Confluent Cloud |
|---|:---:|:---:|:---:|
| Topics / ACLs | ✅ | ✅ | ✅ validated¹ |
| Quotas / SCRAM users | ✅ | ✅ | ❌ (not exposed by Cloud) |
| Schema Registry | ✅ | ✅ | ✅ validated¹ |
| MDS/RBAC role bindings | ❌ (no MDS) | ✅ | ❌ (different API, not implemented) |

¹ Validated 2026-07-09 against a live Confluent Cloud Basic cluster (topic create/alter/drift
reconcile, ACLs, schema registration + evolution, import). Cloud caveats: ACL principals must use
the service account's **numeric id** (e.g. `User:9207877`, never the `sa-...` resource id — Cloud
silently drops Kafka-protocol ACL creates for `sa-` principals); some topic configs are
policy-restricted; the quota and SCRAM APIs are not exposed (operations fail cleanly, and
`import cluster` needs `--skip-users`/`--skip-quotas`). Schema Registry support only requires API
compatibility, which any Confluent-compatible registry (including self-hosted ones) provides.
MDS/RBAC is a Confluent Platform-only component; Confluent Cloud authorizes RBAC through a
separate, cloud-specific API this tool does not implement — see
[Connecting](docs/connecting.md#support-matrix) for details.
**CI validates continuously against Apache Kafka (`cp-kafka` images) and Confluent Platform
components; Confluent Cloud was validated with the opt-in maintainer harness (`make e2e-cloud`),
not in CI.**

## Alternatives

| Tool | Topic+access together | Topics | ACLs | Principals (SCRAM) | Quotas | Schemas | RBAC (MDS) | CLI | Operator | Drift/verify | Import |
|------|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| **monedula-gitops** | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Strimzi | ❌ | ✅ | ✅ (KafkaUser) | ✅ (KafkaUser) | ✅ (KafkaUser) | ❌ | ❌ | ❌ | ✅ | partial | ❌ |
| Confluent for Kubernetes | ❌ | ✅ | ✅ | ⚠️ (Secret-based, PLAIN-focused) | ❌ | ✅ | ✅ | ❌ | ✅ | partial | ❌ |
| Jikkou | ❌ | ✅ | ✅ | ✅ (KafkaUser, SCRAM) | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ✅ |
| JulieOps | ⚠️ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ | ✅ | ❌ | partial | ❌ |
| kafka-gitops (devshawn) | ⚠️ | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ | ✅ | ❌ | ✅ | ❌ |
| terraform-provider-kafka | ❌ | ✅ | ✅ | ✅ (`kafka_user_scram_credential`) | ✅ | ❌ | ❌ | ✅ (TF) | ❌ | ✅ (TF state) | ✅ (TF import) |

> **Topic+access together**: ✅ = producer/consumer access is declared on the topic itself; ⚠️ = access lives in the same file but is anchored on an application/project rather than the topic; ❌ = topics and access are separate objects.
>
> **Principals (SCRAM)**: ✅ = the tool can create/rotate a broker-side SCRAM credential declaratively; ⚠️ = partial or adjacent support (e.g. Secret-backed credentials for a narrower mechanism); ❌ = the tool assumes principals already exist and manages only their authorization.
>
> Comparison reflects each tool's primary focus at the time of writing; capabilities evolve — see each project's documentation.

## Installation

### Homebrew
```bash
brew install monedula-dev/tap/monedula-gitops
```

### Prebuilt binary
Download the archive for your OS/arch from the [latest release](https://github.com/monedula-dev/monedula-gitops/releases/latest), extract, and put `monedula-gitops` on your `PATH`.

### Go
```bash
go install github.com/monedula-dev/monedula-gitops/cmd/monedula-gitops@latest
```

### Docker
```bash
docker run --rm ghcr.io/monedula-dev/monedula-gitops:latest --help
```

### Kubernetes operator (Helm)
```bash
helm install monedula-gitops oci://ghcr.io/monedula-dev/charts/monedula-gitops
```
See [docs/operator.md](docs/operator.md) for full operator install and configuration.

## Quickstart

Describe your desired state as YAML and converge it from the CLI:

```bash
# 1. Preview what would change against the live cluster
monedula-gitops diff -f ./manifests --cluster-config-file ./cluster.yaml

# 2. Apply the changes
monedula-gitops apply -f ./manifests --cluster-config-file ./cluster.yaml

# 3. Confirm there is no drift
monedula-gitops verify -f ./manifests --cluster-config-file ./cluster.yaml
```

For the in-cluster GitOps workflow, deploy the operator (see [Installation](#installation)) and apply the same `KafkaTopic` / `KafkaAccessPolicy` / `KafkaQuota` / `KafkaRoleBinding` / `KafkaUser` custom resources. Worked, end-to-end examples live in [`scenarios/`](scenarios/).

## Documentation

- [Manifest reference](docs/manifest-reference.md)
- [CLI reference](docs/cli.md)
- [Kubernetes operator](docs/operator.md)
- [Connecting to Kafka, Schema Registry, and MDS](docs/connecting.md)
- [Schema management](docs/schemas.md)
- Worked, end-to-end examples live in [`scenarios/`](scenarios/).

## Building from source

```bash
git clone https://github.com/monedula-dev/monedula-gitops.git
cd monedula-gitops
go build ./cmd/monedula-gitops
```
Run `go test ./...` for the test suite; see [CONTRIBUTING.md](CONTRIBUTING.md) for the end-to-end suites.

## Contributing

Contributions are welcome — see [CONTRIBUTING.md](CONTRIBUTING.md). Please file issues and report security concerns through the issue templates and [SECURITY.md](SECURITY.md).

## License

Licensed under the GNU AGPL-3.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).

Using monedula-gitops to manage your own Kafka clusters — as a CLI or as an operator inside your own infrastructure — does not require you to share any of your code or manifests; AGPL obligations arise only if you distribute a modified version or offer one to others as a network service. For commercial licensing (including embedding monedula-gitops in a product), open a discussion on GitHub.
