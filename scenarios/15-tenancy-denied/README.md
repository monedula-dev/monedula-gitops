# Scenario 15 — Tenancy (admission rejects a disallowed topic prefix)

**Modes:** k8s
**Cluster profile:** shared-sasl

## What this teaches

`KafkaCluster.spec.tenancy` enforces multi-tenancy boundaries directly in the
operator's custom resource model:

- **`allowedNamespaces`** — lists which Kubernetes namespaces may create
  resources that reference this cluster. A `KafkaTopic` in any other namespace
  is rejected at admission.

- **`topicPrefixes`** — per-namespace prefix rules. Each entry maps a set of
  namespaces to a list of required topic-name prefixes. A topic whose
  `spec.topicName` does not start with one of the allowed prefixes for its
  namespace is rejected at admission.

In this scenario the `tenant` cluster requires that topics created in the
`monedula-e2e` namespace start with `payments.`. The topic manifest uses the
name `forbidden.topic`, which violates that rule, so `kubectl apply -f
manifests/topic.yaml` is rejected by the validating admission webhook before the
object is ever written to etcd.

Note: when the webhook is disabled the operator backstops by refusing to
reconcile and setting a `TenancyDenied` condition on the topic. The live-run
task (which follows) confirms the exact rejection message returned by the webhook.

## How to run

This scenario has **two manifests applied in order** — the cluster must exist
so the webhook can read its tenancy spec when evaluating the topic.

Apply the tenancy-configured cluster (succeeds):

```bash
kubectl apply -f manifests/cluster.yaml
```

Attempt the prefix-violating topic (rejected):

```bash
kubectl apply -f manifests/topic.yaml
```

The second command should fail with an admission error in stderr containing
the word "tenancy" (or the specific rejection message once the live run
pins it).

## Expected outcome

`kubectl apply -f manifests/topic.yaml` is **rejected** by the admission
webhook. The error message from the apiserver indicates the tenancy prefix
violation for `forbidden.topic` against the `payments.` prefix policy on the
`tenant` cluster.

## Cleanup

```bash
./cleanup.sh k8s
```
