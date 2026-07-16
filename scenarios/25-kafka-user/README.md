# Scenario 25 — KafkaUser: SCRAM credential and the topic that trusts it

**Modes:** cli, k8s
**Cluster profile:** shared-sasl

## What this teaches

A `KafkaUser` declares a SCRAM-SHA-512 credential for the principal
`svc-orders-app`. A `KafkaTopic` (`users.orders`) declares an inline `access`
block granting that same principal `Write`. Together they tell the full
story: a service identity is provisioned as a Kafka credential, and a topic's
access policy references that identity by name — the credential and the ACL
are two halves of one onboarding flow, verified together against a real
broker in this one scenario.

## A note on the password source (per-mode manifests)

Every other scenario in this catalog applies the exact same `manifests/`
directory in both cli and k8s mode. This scenario cannot: a KafkaUser's
`spec.password` source is mode-exclusive by design (spec v0.35 §T2,
`internal/validation.ValidateUserShape`):

- **CLI mode** resolves `password.valueFrom.env` or `.file` (there is no
  Kubernetes API to read a Secret from, and `secretKeyRef`/`generate` are
  rejected — see `internal/secrets.FileEnvResolver` and
  `internal/pipeline.Build`).
- **k8s (operator) mode** resolves `password.valueFrom.secretKeyRef` or
  provisions a credential itself via `password.generate: {}` (the operator
  never reads the host environment or filesystem — see
  `internal/operator.K8sResolver`).

`password.valueFrom.inline` is rejected outright in both modes (a plaintext
password would be committed to git), so there is no single manifest shape
that resolves in both runtimes.

Rather than invent a synthetic Secret pre-creation step, this scenario takes
the showcase that best fits each mode:

- `manifests-cli/user.yaml` sets `password.valueFrom.env: ORDERS_APP_PASSWORD`
  — the CLI's documented credential-injection story (mirrors how
  `KAFKA_USERNAME`/`KAFKA_PASSWORD` are exported for the cluster profile
  itself).
- `manifests-k8s/user.yaml` sets `password.generate: {}` — the operator
  provisions and owns a Secret (`svc-orders-app-kafka-credentials`) holding a
  generated password, needing zero pre-created Secret material. This is the
  better k8s-native showcase: nothing to leak, nothing to rotate manually.

The e2e harness picks whichever directory matches the active mode
(`test/e2e/cli/runner_test.go`'s `scenarioManifestsDir` /
`test/e2e/k8s/lib.bash`'s `apply_scenario`), falling back to a flat
`manifests/` for every other scenario, which is unaffected by this.

`topic.yaml` is identical in both directories (duplicated, not templated) —
it does not touch the password field, so it isn't the source of the split.

## How to run

### CLI mode

```bash
export ORDERS_APP_PASSWORD=orders-app-secret
monedula-gitops apply -f manifests-cli/ \
  --cluster-config-file ../clusters/shared-sasl/cluster.yaml
```

### k8s mode

```bash
kubectl apply -f manifests-k8s/
```

Confirm reconciliation:

```bash
kubectl get kafkauser svc-orders-app -o jsonpath='{.status.conditions}'
kubectl get kafkatopic users-orders -o jsonpath='{.status.conditions}'
kubectl get secret svc-orders-app-kafka-credentials
```

## Expected outcome

A SCRAM-SHA-512 credential for `svc-orders-app` exists on the broker. The
topic `users.orders` is created with a `Write` ACL for `User:svc-orders-app`.
In k8s mode both CRs reach `Ready=True` (`KafkaUser` also reports
`UserSynced=True`; `KafkaTopic` also reports `TopicAccessSynced=True`), and
the operator-owned Secret `svc-orders-app-kafka-credentials` exists holding
the generated password. The CLI exits 0 and prints `svc-orders-app` and
`users.orders` in its output summary.

## Cleanup

```bash
./cleanup.sh cli   # for CLI mode
./cleanup.sh k8s   # for k8s mode
```
