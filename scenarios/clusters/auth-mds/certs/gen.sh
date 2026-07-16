#!/usr/bin/env bash
# Generate a dev-only RSA keypair for MDS token signing (PEM). DEV ONLY.
set -euo pipefail
cd "$(dirname "$0")"
openssl genrsa -out mds-token.key 2048
openssl rsa -in mds-token.key -pubout -out mds-token.pub
echo "generated mds-token.key mds-token.pub"
