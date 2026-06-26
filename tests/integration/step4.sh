#!/bin/bash

set -euo pipefail

run="uv run --no-sync"
# note: we use no-sync here since uv pip install
# was used to manually update the psik package during testing.
# The psik-api backend works as of v3.1.0, so this is no longer
# necessary.

BASE=$PWD/tests/integration
export PSIK_CONFIG=$BASE/psik.json

start_server() {
  export PSIK_API_CONFIG=$BASE/psik_api.json
  (cd $HOME/src/microservices/psik_api && $run uvicorn psik_api.main:app --log-level info --port 5555) &
  trap "kill $!" EXIT
}

# setup a job workdir and then start job (mimicks the API's actions)
jobid=$($run psik run --no-submit $BASE/lclstreamer_job.yaml | cut -d ' ' -f 2)
#workdir=$($run psik ls $jobid | sed -ne 's/ *work: //p'
workdir=/tmp/psik/$jobid/work
echo "'$jobid'"
echo "'$workdir'"
cp $BASE/lclstreamer.yaml $workdir
$run psik start $jobid # POST fires here

$run lclstream pull -d tcp://127.0.0.1:12321 | tar tf -

$run psik ls $jobid
$run psik cancel $jobid
