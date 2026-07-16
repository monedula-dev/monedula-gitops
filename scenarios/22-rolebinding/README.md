# 22-rolebinding — RBAC role binding via MDS

## What this tests

This scenario verifies that the Monedula CLI can manage Kafka RBAC role bindings
through the Confluent Metadata Service (MDS). It applies a `KafkaRoleBinding`
manifest that grants `User:svc-checkout` the `DeveloperRead` role on all topics
with the `payments.` prefix, then confirms the binding is in sync by running
`verify`.

## How it works

1. The `auth-mds` compose profile starts three services:
   - **ldap** — an OpenLDAP server seeded with users (`mds`, `kafka-admin`,
     `svc-checkout`) via `config/users.ldif`.
   - **kafka** — a single-node Confluent Server (cp-server 7.6.1) configured
     with SASL_PLAINTEXT/PLAIN (LDAP-validated) on `:9092` and the Confluent
     RBAC authorizer + Metadata Service on `:8090`.
   - **rbac-bootstrap** — a one-shot container that authenticates to MDS as
     `User:mds` and grants it `SystemAdmin` over the Kafka cluster scope,
     enabling the product's MDS client to manage role bindings.
2. The CLI reads `cluster.yaml` from the profile, which configures:
   - Kafka PLAIN auth using `KAFKA_USERNAME` / `KAFKA_PASSWORD` env vars.
   - MDS basic auth using `MDS_USER` / `MDS_PASSWORD` env vars.
3. On `apply`, the CLI authenticates to MDS as `mds/mds-secret`, obtains a
   bearer token, and calls `POST /security/1.0/principals/User:svc-checkout/roles/DeveloperRead`
   to create the role binding. Output contains `AddRoleBinding`.
4. On `verify`, the CLI fetches the current bindings from MDS and confirms
   they match the declared manifest. Output exits 0 with `No changes.` when
   the binding is already present.

## Required environment variables

| Variable           | Value                |
|--------------------|----------------------|
| `MDS_USER`         | `mds`                |
| `MDS_PASSWORD`     | `mds-secret`         |
| `KAFKA_USERNAME`   | `kafka-admin`        |
| `KAFKA_PASSWORD`   | `kafka-admin-secret` |

These are set automatically by `TestAuthMDSScenarios` in
`test/e2e/cli/runner_test.go`.

## Expected outcome

- `monedula-gitops apply` exits 0 and output contains `AddRoleBinding`.
- `monedula-gitops verify` exits 0 (binding is in sync, no drift).
