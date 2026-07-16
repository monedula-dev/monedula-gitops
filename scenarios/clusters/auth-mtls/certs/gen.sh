#!/usr/bin/env bash
# Generate a dev-only CA + Kafka server cert + client cert (PEM) for the mTLS profile.
# Re-run to regenerate. The committed certs are DEV ONLY — never reuse in prod.
set -euo pipefail
cd "$(dirname "$0")"
openssl req -x509 -newkey rsa:2048 -nodes -keyout ca.key -out ca.crt -days 3650 -subj "/CN=monedula-e2e-ca"
openssl req -newkey rsa:2048 -nodes -keyout server.key -out server.csr -subj "/CN=kafka"
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out server.crt -days 3650 \
  -extfile <(printf "subjectAltName=DNS:localhost,DNS:kafka,DNS:host.docker.internal")
openssl req -newkey rsa:2048 -nodes -keyout client.key -out client.csr -subj "/CN=monedula"
openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out client.crt -days 3650
rm -f server.csr client.csr ca.srl
echo "generated ca.crt ca.key server.crt server.key client.crt client.key"
