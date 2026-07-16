# 21-oauthbearer — OAUTHBEARER token auth

## What this tests

This scenario verifies that the Monedula CLI can authenticate to a Kafka broker
configured with `SASL_PLAINTEXT / OAUTHBEARER` by exchanging OIDC
client-credentials for a bearer token and using it to apply a topic manifest.

## How it works

1. The `auth-oauth` compose profile starts two services:
   - **kafka** — a Kafka broker configured to require OAUTHBEARER tokens,
     expecting issuer `http://localhost:8080/default` and audience `kafka`.
   - **mock-oauth2-server** — [navikt/mock-oauth2-server](https://github.com/navikt/mock-oauth2-server)
     acting as a lightweight OIDC server on `localhost:8080`.
2. The CLI reads `cluster.yaml` from the profile, which specifies
   `auth.mechanism: OAUTHBEARER` and `oauth.tokenEndpoint`.  Client ID and
   secret are injected via `OAUTH_CLIENT_ID` / `OAUTH_CLIENT_SECRET` env vars.
3. On `apply`, the CLI performs an OAuth 2.0 client-credentials grant against
   `http://localhost:8080/default/token`, receives a signed JWT, and presents it
   as the SASL OAUTHBEARER token to Kafka.
4. Kafka validates the token (issuer, audience, expiry) and, if valid, allows
   the connection under principal `User:monedula` (the token `sub` claim).
5. The topic `oauth.demo` is created, and the `e2e check` sub-command confirms
   it exists on the broker.

## Required environment variables

| Variable             | Value            |
|----------------------|------------------|
| `OAUTH_CLIENT_ID`    | `monedula`       |
| `OAUTH_CLIENT_SECRET`| `monedula-secret`|

These are set automatically by `TestAuthOAuthScenarios` in
`test/e2e/cli/runner_test.go`.

## Expected outcome

- `monedula-gitops apply` exits 0.
- Output contains `oauth.demo`.
- Live-state check confirms the topic `oauth.demo` exists on the broker.
