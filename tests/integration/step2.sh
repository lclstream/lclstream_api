#!/bin/bash

set -euo pipefail

BASE=$HOME/src/lclstreamer
uv run lclstream pull -l tcp://127.0.0.1:12321 | tar tf - &
trap "kill $!" EXIT

pixi run --manifest-path $BASE lclstreamer --config $BASE/examples/lclstreamer-internal.yaml
#pixi run --manifest-path $BASE lclstreamer --config tests/integration/generic_source.yaml
