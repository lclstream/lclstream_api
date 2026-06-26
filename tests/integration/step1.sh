#!/bin/bash

set -euo pipefail

uv run lclstream push -n 1 --addr tcp://127.0.0.1:12322 /bin/z* &
trap "kill $!" EXIT

uv run lclstream pull -vv -l tcp://127.0.0.1:12322 | tar tf -
