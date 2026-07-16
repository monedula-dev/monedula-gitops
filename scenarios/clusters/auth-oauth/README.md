# auth-oauth — SASL_PLAINTEXT / OAUTHBEARER cluster profile

DEV ONLY. Single-node Kafka (KRaft) with SASL_PLAINTEXT/OAUTHBEARER authentication
backed by a mock OIDC server ([navikt/mock-oauth2-server](https://github.com/navikt/mock-oauth2-server)).

Do NOT use this profile in production. The OIDC server is a local mock with no
persistent state, no access controls, and HTTP (not HTTPS) throughout.

---

## How it works

```
CLI (host) ──POST client_credentials──► mock-oauth2:8080/default/token
              ◄── JWT (sub=monedula, iss=http://localhost:8080/default, aud=kafka) ──

CLI ──SASL OAUTHBEARER (Bearer <JWT>)──► kafka:9092 (CLIENTHOST listener)
                                         │
                              broker fetches JWKS from
                              mock-oauth2:8080/default/jwks
                              validates: sig + iss + aud + exp
                                         │
                              aud=kafka ✓, iss matched ✓
                              ◄── SASL success (principal=User:monedula) ──
```

The CLI's `auth.oauth` block in `cluster.yaml` uses the `clientcredentials` flow:
it POSTs to `tokenEndpoint` with the client credentials and presents the returned
JWT via SASL/OAUTHBEARER. The broker validates the JWT signature against the JWKS
endpoint (fetched once and cached), checks the issuer and audience, then admits
the principal as `User:<sub>` = `User:monedula`.

### Issuer URL behavior

`navikt/mock-oauth2-server` sets the `iss` claim dynamically to reflect the
`Host` header of the incoming request:

| Who requests the token | Resulting `iss` in token |
|---|---|
| CLI on host (`localhost:8080`) | `http://localhost:8080/default` |
| Broker inside Docker (`mock-oauth2:8080`) | `http://mock-oauth2:8080/default` |

Because the CLI runs on the host, `KAFKA_SASL_OAUTHBEARER_EXPECTED_ISSUER` is
set to `http://localhost:8080/default` (not `mock-oauth2`). The JWKS URL
(`http://mock-oauth2:8080/default/jwks`) is the in-network address, which is
reachable from the broker container.

### cp-kafka 8.x caveats applied in this profile

| Problem | Fix |
|---|---|
| Listener name `CLIENT_HOST` breaks env→property conversion (underscore → dot instead of staying underscore) | Renamed listener to `CLIENTHOST` (no underscore) |
| `OAuthBearerValidatorCallbackHandler` moved from `.secured.` subpackage to top-level | Use `org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallbackHandler` |
| HTTP JWKS URLs blocked by new allowlist requirement (Kafka 3.7+) | Pass `-Dorg.apache.kafka.sasl.oauthbearer.allowed.urls=...` in `KAFKA_OPTS` |
| Per-listener JAAS config via `KAFKA_LISTENER_NAME_*_SASL_JAAS_CONFIG` broken in cp-kafka 8.x | Use `KAFKA_OPTS: -Djava.security.auth.login.config=...` + mounted `kafka-jaas.conf` |
| `navikt/mock-oauth2-server` is a distroless image — no shell for `CMD-SHELL` healthchecks | Mount `healthcheck.java` and run with `java --source 11` |

---

## Prerequisites

- Docker with Compose v2
- `bin/monedula-gitops` built (`go build -o bin/monedula-gitops ./cmd/monedula-gitops`)

## Usage

```bash
# Start the cluster
docker compose -f scenarios/clusters/auth-oauth/compose.yaml -p mon-oauth up -d --wait

# Apply resources (set credentials via env vars)
export OAUTH_CLIENT_ID=monedula
export OAUTH_CLIENT_SECRET=monedula-secret

bin/monedula-gitops apply -f <your-manifest.yaml> \
  --cluster-config-file scenarios/clusters/auth-oauth/cluster.yaml

# Tear down
docker compose -f scenarios/clusters/auth-oauth/compose.yaml -p mon-oauth down -v
```

## Validated token claims (spike)

| Claim | Value |
|---|---|
| `sub` | `monedula` |
| `iss` | `http://localhost:8080/default` |
| `aud` | `kafka` |

The broker principal is `User:monedula` (derived from `sub`). `KAFKA_SUPER_USERS`
includes this principal and `KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND=true` lets the
CLI's `ListACLs` call succeed without pre-provisioning ACL entries.
