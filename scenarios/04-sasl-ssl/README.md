# Scenario 04 — Connect to a SASL_SSL (SCRAM-over-TLS) cluster

**Modes:** cli, k8s  
**Cluster profile:** auth-sasl-ssl

## What this teaches

Real Kafka clusters require encrypted transport (TLS) combined with SASL
authentication (SCRAM-SHA-256 or SCRAM-SHA-512). This scenario verifies that
the operator and CLI both negotiate SASL_SSL correctly using the `auth-sasl-ssl`
cluster profile, which provides a CA certificate and SCRAM credentials. A
minimal topic `tls.demo` is created end-to-end, confirming the full TLS
handshake and authentication exchange succeed.

The cluster profile lives in `scenarios/clusters/auth-sasl-ssl/cluster.yaml`
(added in the next task). It carries the bootstrap address, protocol
`SASL_SSL`, SCRAM mechanism, and the CA bundle path.

## How to run

### CLI mode

```bash
monedula-gitops apply -f manifests/ \
  --cluster-config-file ../clusters/auth-sasl-ssl/cluster.yaml
```

### k8s mode

```bash
kubectl apply -f manifests/
```

The operator picks up the `auth-sasl-ssl` cluster profile via the
`KafkaCluster` CR that the harness pre-provisions. The KafkaTopic CR
references `clusterRef.name: shared` which maps to that profile in the
`auth-sasl-ssl` harness namespace.

Every cluster profile's `KafkaCluster` CR is named `shared` on purpose, so
scenario manifests stay profile-agnostic (the same `clusterRef.name: shared`
works against any profile). The harness — not the manifest — selects which
broker `shared` points at by choosing the profile/namespace it provisions.

## Expected outcome

The topic `tls.demo` is created on the SASL_SSL broker. In k8s mode the
`KafkaTopic` resource `tls-demo` reaches `Ready=True`. The CLI exits 0.

## Cleanup

```bash
./cleanup.sh cli   # for CLI mode
./cleanup.sh k8s   # for k8s mode
```
