# Integration Tests

These come in a series of steps showing each component <-> component
connection that needs to be made.

All tests are executed from the source directory root, NOT
from the tests/ or tests/integrations dir.


## Step 1:

- lclstream cli push
- lclcstream cli pull (results as tar to stdout)

This demonstrates the client tester is working.


## Step 2:

- lclstreamer push
- lclstream cli pull

This demonstrates lclstreamer can generate data and send to
to the client.


## Step 3:

- `lclstream_api` server push
  * Spawns zmq PULL/PUSH message forwarder
  * Spawns lclstreamer producer

  Note: server's configuration is `test_config.yaml`

- lclstream cli get
  * POST to /v1/transfers
  * run pull (results as tar to stdout)

This demonstrates that the forwarder and producer can be
started by the API, and that the client receives these messages.

This also demonstrates that psik's local backend works
and sends progress updates to the API via /v1/callback/\*


## Step 4:

Like step 2, but launched with psik-api.

- psik run <(echo '{"backend":"s3df", "script":"lclstreamer ..."}')

- lclstream cli pull

This demonstrates the JobSpec needed to run lclstreamer via
an API.


## Step 5:

Launch both forwarder and producer without the API.

- psik run <(echo '{"backend":"sdfdtn", "script":"zmqbuf ..."}')

- psik run <(echo '{"backend":"s3df", "script":"lclstreamer ..."}')

- lclstream cli pull

This demonstrates the JobSpec needed to run lclstreamer via
an API.

## Step 6:

Now the `lclstreamer_api.yaml` can be updated with the two successful
JobSpec-s above and the API should succeed just as it did in step 4.

