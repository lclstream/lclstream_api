#!/usr/bin/env bash
# Mints an S3DF/Dex bearer token (device-code flow via `s3df login`) and
# writes it to .env.local as VITE_DEV_BEARER_TOKEN, so the frontend can call
# the real dev k8s API (see ../.env.example) with a valid Authorization
# header. Same token lclstream_api's own LCLSTREAM_IRI_S3DF_TOKEN_FILE uses
# (real Dex, issuer https://dex.slac.stanford.edu, aud=s3df) — mirrors
# `make token` in lclstream-api's apps/lclstream-api-v2/overlays/*/Makefile.
#
# `s3df login` needs a browser, so it's run on TOKEN_HOST over `ssh -t`
# (forwards the prompt to your terminal); the resulting token file is then
# scp'd back. Run from frontend/: ./scripts/dev-token.sh (or `bun run dev:token`).
set -euo pipefail

TOKEN_HOST="${TOKEN_HOST:-iana}"
REMOTE_TOKEN="${REMOTE_TOKEN:-.s3df-access-token}"

echo "Logging in on $TOKEN_HOST -- follow the browser prompt it prints"
ssh -t "$TOKEN_HOST" '/sdf/sw/s3df-cli/bin/s3df login'

echo "Copying the token back from $TOKEN_HOST"
tmp=$(mktemp .s3df-token.XXXXXX)
trap 'rm -f "$tmp"' EXIT
scp "$TOKEN_HOST:$REMOTE_TOKEN" "$tmp"

printf 'VITE_DEV_BEARER_TOKEN=%s\n' "$(cat "$tmp")" > .env.local
echo "Wrote .env.local with a bearer token from $TOKEN_HOST"
