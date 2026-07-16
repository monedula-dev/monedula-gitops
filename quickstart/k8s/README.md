# Monedula k8s quickstart

Stand up the Monedula GitOps operator on a local [k3d](https://k3d.io) cluster,
running against an in-cluster Kafka + Schema Registry, and watch it reconcile
sample `KafkaTopic` / `KafkaAccessPolicy` resources. The sample `KafkaCluster`
resolves all of its credentials from a Kubernetes `Secret` via `secretKeyRef` --
the v0.5 capability this quickstart demonstrates.

By default the quickstart also installs [cert-manager](https://cert-manager.io)
and runs the operator with the KafkaTopic identity **validating admission
webhook** enabled (v0.7). The webhook rejects duplicate topic identities and
immutable-field violations at admission time.

## Prerequisites

- [docker](https://docs.docker.com/get-docker/) (to build + run the cluster)
- [k3d](https://k3d.io/#installation) (local k3s-in-docker)
- [kubectl](https://kubernetes.io/docs/tasks/tools/)
- [helm](https://helm.sh/docs/intro/install/) v3.x (to install the operator chart)

## Run it

```sh
./setup.sh
```

`setup.sh` is idempotent on the cluster (it reuses an existing `monedula` k3d
cluster) and performs, in order:

1. Create the k3d cluster `monedula` (if missing).
2. `docker build` the operator image and `k3d image import` it into the cluster.
3. Install cert-manager **v1.16.3** (pinned) and wait for its three deployments
   to be `Available`.
4. Deploy Kafka + Schema Registry (`quickstart/k8s/kafka/`) and wait for the
   StatefulSet, the SCRAM bootstrap Job, and Schema Registry to be ready.
5. Install the operator via `helm upgrade --install monedula-gitops
   charts/monedula-gitops` — the chart installs CRDs, RBAC, and the Deployment
   in one shot. Webhook mode passes `--set webhook.enabled=true`; cert-free mode
   omits it (the chart default is `webhook.enabled=false`). `--wait --timeout
   180s` blocks until the Deployment is ready, replacing the separate rollout-wait
   step.
6. Apply the demo namespace, the `kafka-credentials` Secret, and the sample CRs
   (`quickstart/k8s/operator/`).

### No-webhook fallback (no cert-manager required)

To run the operator **without** the admission webhook — useful when testing on a
cluster that already has cert-manager elsewhere, or when you want the simplest
possible setup — pass `MONEDULA_WEBHOOKS=false`:

```sh
MONEDULA_WEBHOOKS=false ./setup.sh
```

In this mode `setup.sh` automatically:

- **Skips cert-manager** entirely — the 5-step sequence is: cluster → image →
  Kafka → helm install → sample CRs.
- **Omits `--set webhook.enabled=true`** from the `helm upgrade --install` call,
  so the chart deploys with `webhook.enabled=false` (the chart default). The
  operator starts without opening port 9443 and requires no TLS certs at all.

No manual patching or `grep -v` transforms are needed; the Helm chart handles
the webhook/no-webhook branching via `values.yaml`.

### Kustomize alternative

If you prefer Kustomize over Helm, apply the Kustomize overlays directly:

```sh
# webhook mode
kubectl apply -k ../../config/overlays/webhook
# cert-free mode
kubectl apply -k ../../config/default
```

## cert-manager dependency

cert-manager **v1.16.3** is pinned in `setup.sh` and installed from the
[official static manifest](https://github.com/cert-manager/cert-manager/releases/download/v1.16.3/cert-manager.yaml).

cert-manager manages the webhook's TLS serving certificate when
`webhook.enabled=true`. The Helm chart (`charts/monedula-gitops`) renders the
cert-manager `Issuer`, `Certificate`, and the CA-injection annotation on the
`ValidatingWebhookConfiguration` automatically when `webhook.certManager.enabled=true`
(the chart default when `webhook.enabled=true`). cert-manager creates the `Secret`
`<release>-webhook-serving-cert` and rotates it automatically; the CA injector
keeps the API server's CA bundle up-to-date without any operator involvement.

## What to observe

```sh
# The operator pod should be Ready:
kubectl -n monedula-system get pods

# The sample resources, all in the monedula-demo namespace:
kubectl -n monedula-demo get kafkaclusters,kafkatopics,kafkaaccesspolicies

# The topic should converge to Ready with TopicSynced=True:
kubectl -n monedula-demo describe kafkatopic orders
kubectl -n monedula-demo get kafkatopic orders -o jsonpath='{.status.phase}{"\n"}'
```

Expected outcome:

- The operator pod (`monedula-controller-manager`) is `Running`/`Ready`.
- `KafkaCluster/demo` reports a healthy connection (it could reach the broker and
  Schema Registry using credentials it read from the Secret).
- `KafkaTopic/orders` reaches `.status.phase: Ready` with a `TopicSynced=True`
  condition, and the Kafka topic `payments.orders` exists with 3 partitions.
- `KafkaAccessPolicy/billing-access` reconciles its prefixed Topic ACL.

### Webhook rejection demo

After the quickstart is running, try to apply a **duplicate** `KafkaTopic` that
maps to the same `(cluster, topicName)` identity as the existing `orders` CR:

```sh
kubectl -n monedula-demo apply -f - <<EOF
apiVersion: gitops.monedula.dev/v1alpha1
kind: KafkaTopic
metadata:
  name: orders-dup
  namespace: monedula-demo
spec:
  clusterRef:
    name: demo
  topicName: payments.orders
  partitions: 3
  deletionPolicy: Orphan
EOF
```

Expected output (admission webhook rejection):

```
Error from server (Forbidden): error when creating "STDIN":
  admission webhook "vkafkatopic.gitops.monedula.dev" denied the request:
  KafkaTopic monedula-demo/orders-dup conflicts with monedula-demo/orders:
  both resolve to topicName "payments.orders" on cluster "demo" ...
```

The webhook also rejects topicName renames on update:

```sh
kubectl -n monedula-demo patch kafkatopic orders --type merge \
  -p '{"spec":{"topicName":"payments.orders-renamed"}}'
# Error: spec.topicName is immutable: resolved topic name cannot change ...
```

### secretKeyRef demo (Secret resolution)

The sample `KafkaCluster` (`samples/kafkacluster.yaml`) carries **no plaintext**
credentials. Both the Kafka SASL/SCRAM username/password and the Schema Registry
basic-auth username/password come from `secretKeyRef`s into the
`kafka-credentials` Secret in the `monedula-demo` namespace. The operator's
in-cluster resolver reads Secrets from the **resource's own namespace**, which is
why the Secret and all CRs live together in `monedula-demo`. The topic reaching
`Ready` proves the operator successfully resolved and used those Secret values.

> The credentials in `kafka-credentials.secret.yaml` are **demo-only** and match
> the in-cluster Kafka/Schema Registry. In production, source the Secret from
> your secrets manager (SOPS, External Secrets, etc.) and never commit it.

> **Why the super-user?** The Secret carries the `admin` SCRAM super-user
> because the operator manages topics *and ACLs* and must bypass ACL
> enforcement. A non-super-user would lock itself out the moment it creates a
> topic's ACLs: with `allow.everyone.if.no.acl.found=true`, a resource is open
> only while it has **no** ACLs — once ACLs exist, any unlisted principal loses
> access.

### Deletion demo (deletionPolicy + allow-delete gate)

The sample topic uses `deletionPolicy: Orphan`, so deleting the CR leaves the
Kafka topic in place. To actually delete the topic from Kafka you must both flip
the policy to `Delete` **and** approve it with the allow-delete annotation:

```sh
kubectl -n monedula-demo patch kafkatopic orders --type merge \
  -p '{"spec":{"deletionPolicy":"Delete"}}'
kubectl -n monedula-demo annotate kafkatopic orders \
  gitops.monedula.dev/allow-delete=true --overwrite

kubectl -n monedula-demo delete kafkatopic orders
```

Without the `gitops.monedula.dev/allow-delete=true` annotation the operator
refuses to delete the managed topic + ACLs, even with `deletionPolicy: Delete`.

> Registering a topic schema via `secretKeyRef` is supported, but the sample
> topic is intentionally schema-free: a `file:` schema ref is unsupported in
> operator mode (it would set `SchemaSynced=False`).

## Tear down

```sh
./teardown.sh
```

`teardown.sh` first runs `helm uninstall monedula-gitops` to remove the operator
release, then deletes the entire k3d `monedula` cluster. Because the chart's CRDs
carry `helm.sh/resource-policy: keep`, Helm does **not** delete the CRDs (and
therefore the CRs) on uninstall — deleting the cluster achieves a full reset.

To remove CRDs without deleting the cluster (e.g. in a shared cluster):

```sh
kubectl delete crd kafkatopics.gitops.monedula.dev \
  kafkaclusters.gitops.monedula.dev \
  kafkaaccesspolicies.gitops.monedula.dev
```

> **Warning:** this deletes all `KafkaTopic`, `KafkaCluster`, and
> `KafkaAccessPolicy` CRs cluster-wide.

## Troubleshooting

- **Operator pod stuck in `ErrImagePull` / `ImagePullBackOff`** -- the image must
  be imported into the cluster: re-run `k3d image import
  monedula/monedula-gitops:latest -c monedula`. The manager manifest sets
  `imagePullPolicy: IfNotPresent` so the imported image is used.
- **Operator pod `CrashLoopBackOff` / "no such file or directory" for certs** --
  cert-manager has not yet created the serving-cert Secret. Check:
  `kubectl -n monedula-system get certificate monedula-gitops-webhook-cert`
  and `kubectl -n cert-manager get pods`. If cert-manager is unhealthy, re-run
  `kubectl apply -f https://...cert-manager.yaml` and wait for its pods.
- **ValidatingWebhookConfiguration not updated with CA bundle** -- the
  cert-manager CA injector is responsible. Check:
  `kubectl -n cert-manager logs deploy/cert-manager-cainjector`.
- **Schema Registry / topic never becomes Ready** -- check the Kafka bootstrap
  Job created the SCRAM users:
  `kubectl -n kafka logs job/kafka-scram-bootstrap`.
- **Diagnosing reconciliation** -- tail the operator logs:
  `kubectl -n monedula-system logs deploy/monedula-controller-manager -f`.
- **Inspect status conditions** -- `kubectl -n monedula-demo describe
  kafkatopic orders` (and `kafkacluster demo`) shows the most recent error.
- **Schema Registry returns 401** -- the REST Basic-auth realm is rejecting the
  login. The JAAS `PropertyFileLoginModule` class path is Jetty-version-sensitive;
  the `schema-registry-auth` ConfigMap (`kafka/schema-registry.yaml`) targets cp
  8.0.0 (Jetty 12). If you change to a Jetty 9.4-11 image and start getting 401s,
  swap the class back to `org.eclipse.jetty.jaas.spi.PropertyFileLoginModule`.
  Confirm via `kubectl -n kafka logs deploy/schema-registry` (look for a
  `LoginException` / class-not-found).

## Verification checklist

- [ ] `kubectl -n monedula-system get pods` shows the operator pod `Ready`.
- [ ] `kubectl -n monedula-demo get kafkatopic orders` shows phase `Ready`.
- [ ] `describe kafkatopic orders` shows condition `TopicSynced=True`.
- [ ] `KafkaCluster/demo` is healthy (Secret-sourced creds resolved).
- [ ] `KafkaAccessPolicy/billing-access` reconciled its prefixed ACL.
- [ ] Deletion demo: with `Delete` + the allow-delete annotation, deleting the CR
      removes `payments.orders` from Kafka.
- [ ] Webhook rejection demo: applying `orders-dup` (same topicName) is rejected
      by the admission webhook.
- [ ] Webhook immutability demo: renaming `spec.topicName` on `orders` is rejected.
