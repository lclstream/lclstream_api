#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CLIENT_DIR="$REPO_ROOT/client"
GENERATOR_CONFIG="$CLIENT_DIR/openapi-generator-config.yaml"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

cd "$REPO_ROOT"
uv run python scripts/dump_openapi.py

GENERATOR_IMAGE="openapitools/openapi-generator-cli@sha256:da552b8d0add9fd3cd89ce836d55b433c8cf467b287dd50c62bbd4482c03f677"

docker run --rm \
  -v "$REPO_ROOT/openapi.json:/spec.json:ro" \
  -v "$GENERATOR_CONFIG:/config.yaml:ro" \
  -v "$TMP_DIR:/out" \
  "$GENERATOR_IMAGE" generate -i /spec.json -o /out -c /config.yaml

rm -rf "$CLIENT_DIR/src/lclstream_api_client/_generated"
cp -R "$TMP_DIR/lclstream_api_client/_generated" "$CLIENT_DIR/src/lclstream_api_client/_generated"

echo "Regenerated $CLIENT_DIR/src/lclstream_api_client/_generated"
