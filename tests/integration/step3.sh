#!/bin/bash

set -euo pipefail

API_URL=http://localhost:8000/v1

uv run uvicorn lclstream_api.server:app &
trap "kill $!" EXIT
sleep 1

# Check that lclstream_api is running
if curl $API_URL/openapi.json >/dev/null; then
  echo "API is not accessible."
  exit 1
fi
echo "OK: API Accessible"

# post a transfer with lclstream
uv run lclstream get --server http://127.0.0.1:8000 --ndial 1 tests/integration/generic_source.yaml | tar tf -

