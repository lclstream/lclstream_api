#!/bin/bash
# Start the lclstream-api container, binding in a local identity

VIRTUAL_ENV="${VIRTUAL_ENV:-./venv}"
CERTIFIED_CONFIG="${CERTIFIED_CONFIG:-$VIRTUAL_ENV/etc/certified}"

if ! [ -d "$CERTIFIED_CONFIG" ]; then
    echo "You must setup a venv and an identity via 'certified init',"
    echo "either in \$VIRTUAL_ENV/etc/certified (default) or by setting \$CERTIFIED_CONFIG"
    exit 1
fi

image="localhost/lclstream-api"

# to replace the config file:
#       -v "./config/lclstream_api.yaml:/etc/lclstream_api.yaml"
podman run --network host \
        -v "$CERTIFIED_CONFIG:/etc/certified" \
        -d "$image"
