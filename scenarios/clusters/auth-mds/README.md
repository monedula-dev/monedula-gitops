# auth-mds cluster profile

> DEV ONLY — single-broker cp-server with embedded MDS and LDAP identity backend. Not for production.

Single-node Confluent Server (`confluentinc/cp-server:7.6.1`, Kafka 3.6) with the embedded
Metadata Service (MDS) RBAC engine and an LDAP identity backend. Used by the `22-rolebinding`
e2e scenario to prove the product's MDS REST client (`internal/mds/confluent`) can add and
verify a `KafkaRoleBinding` against a real Confluent MDS.

## Services

| Container | Image | Role |
|---|---|---|
| `monedula-mds-ldap` | `osixia/openldap:1.5.0` | LDAP identity provider, seeded from `config/users.ldif` |
| `monedula-mds-kafka` | `confluentinc/cp-server:7.6.1` | Kafka broker + MDS on :8090 |
| `monedula-mds-bootstrap` | `confluentinc/cp-server:7.6.1` | One-shot: grants `User:mds` SystemAdmin over the kafka scope |

## Ports

| Port | Service |
|---|---|
| `9092` | Kafka SASL_PLAINTEXT / PLAIN (LDAP-validated) — host-facing client listener |
| `8090` | MDS REST API (HTTP, no TLS for dev) |

## Security model

**Kafka client auth**: SASL_PLAINTEXT + PLAIN mechanism, credentials validated against LDAP
via `LdapAuthenticateCallbackHandler`. The JAAS config (`config/kafka-jaas.conf`) enables
the PLAIN login module; the actual validation is delegated to LDAP.

**MDS auth**: HTTP Basic → MDS issues a short-lived RS256 bearer token (signed with
`certs/mds-token.key`). The product's MDS client authenticates as `mds` / `mds-secret`,
which is an LDAP user with a bootstrapped SystemAdmin role.

**RBAC authorizer**: `ConfluentServerAuthorizer` with `confluent.authorizer.access.rule.providers=CONFLUENT`.

## Credentials (DEV ONLY)

| User | Password | Role |
|---|---|---|
| `mds` | `mds-secret` | LDAP user — MDS admin; product authenticates as this user |
| `kafka-admin` | `kafka-admin-secret` | LDAP user — Kafka client super-user |
| `svc-checkout` | `checkout-secret` | LDAP user — role-binding subject (target of `22-rolebinding`) |
| LDAP admin | `admin-secret` | `cn=admin,dc=monedula,dc=dev` — LDAP bind DN |

## Files

```
auth-mds/
├── compose.yaml              # Docker Compose stack (ldap + kafka + rbac-bootstrap)
├── cluster.yaml              # KafkaCluster CR for the CLI
├── certs/
│   ├── gen.sh                # Generate RSA keypair for MDS token signing
│   ├── mds-token.key         # RS256 private key (MDS signs JWT tokens with this)
│   └── mds-token.pub         # RS256 public key (MDS verifies tokens with this)
└── config/
    ├── users.ldif            # LDAP seed (mds, svc-checkout, kafka-admin)
    └── kafka-jaas.conf       # KafkaServer JAAS (enables PLAIN; validation via LDAP callback)
```

## Kafka cluster id

The cluster id is pinned in `cluster.yaml` at `authorization.mds.clusters.kafkaCluster`:
```
4L6g3nShT-eMCtK--X86sw
```
This matches the `CLUSTER_ID` env in `compose.yaml`. The bootstrap service verifies this at startup.

## Bootstrap sequence

1. LDAP starts and seeds users from `config/users.ldif`.
2. `kafka` (cp-server) starts: initialises KRaft, loads the RBAC authorizer, starts MDS on `:8090`.
   MDS validates HTTP Basic credentials against LDAP and issues RS256 JWTs.
   `User:mds` and `User:ANONYMOUS` are `KAFKA_SUPER_USERS` to allow bootstrap writes.
3. `rbac-bootstrap` (one-shot) waits for MDS health, then:
   - Fetches a bearer token for `mds`
   - Queries `GET /security/1.0/metadataClusterId` to confirm the cluster id
   - Grants `User:mds` SystemAdmin over the kafka scope via the MDS REST API
   This resolves the chicken-and-egg: from this point the product's MDS client
   (authenticating as `mds` with basic auth) can manage role bindings.

## How to run (manual spike / dev)

```bash
# Generate token-signing keys (already committed for dev; re-run only if rotating)
chmod +x certs/gen.sh && bash certs/gen.sh

# Start the full stack and wait for readiness
cd scenarios/clusters/auth-mds
docker compose -p mon-mds-spike up -d --wait

# Environment variables required by cluster.yaml
export MDS_USER=mds
export MDS_PASSWORD=mds-secret
export KAFKA_USERNAME=kafka-admin
export KAFKA_PASSWORD=kafka-admin-secret

# Apply a KafkaRoleBinding against the real MDS
cat > /tmp/rb.yaml <<'YAML'
apiVersion: gitops.monedula.dev/v1alpha1
kind: KafkaRoleBinding
metadata:
  name: checkout-read
spec:
  clusterRef:
    name: shared
  principal: User:svc-checkout
  role: DeveloperRead
  scope:
    type: kafka
  resources:
    - type: Topic
      name: payments.
      patternType: prefixed
  reconciliation:
    mode: Enforce
YAML

bin/monedula-gitops apply -f /tmp/rb.yaml --cluster-config-file scenarios/clusters/auth-mds/cluster.yaml
bin/monedula-gitops verify -f /tmp/rb.yaml --cluster-config-file scenarios/clusters/auth-mds/cluster.yaml

# Tear down
docker compose -p mon-mds-spike down -v
```

## MDS API compatibility note

`cp-server` (7.x and 8.x) does not expose `POST /security/1.0/lookup/rolebindings`
(scope-wide, all-principals listing). The product's `ListRoleBindings` implementation
uses the two-step alternative (`GET /roles` → `POST /lookup/role/{role}` →
`POST /lookup/rolebindings/principal/{p}`) which works on both versions.
