# LCLStreamer-API

Detector data streaming application for LCLS.

This is a FastAPI server that pushes datasets
on command.  The steps run as follows,

1. Receive a REST-API POST request to initiate a dataset push.
   - API request should follow [LCLStreamer](https://slac-lcls.github.io/lclstreamer/) format.

2. A new transfer ID is created and a port allocated.

3. A separate LCLStreamer process is run via psik to send all images
   to the requested server.

4. The transfer ID / url combination is returned to the
   requestor.

5. The requestor should pull the data, e.g. using [lclstream pull](https://github.com/lcls-users/lclstream)

6. While the transfer is active, the user can GET the transfer-id
   to read transfer state and statistics.

7. The pipeline terminates naturally, or the user can DELETE
   the transfer-id to cancel the producer.

# Configuration

Create a configuration file in `/etc/lclstream_api.json` or
`$VIRTUAL_ENV/etc/lclstream_api.json` like:

```
{
  "cache_fmt": "/sdf/home/r/rogersdd/lclstream_cache/%s",
  "psik": {
    "prefix": "/sdf/home/r/rogersdd/psik",
    "backends": {
      "default": {
        "type": "local"
      },
      "slurm": {
        "type": "slurm",
        "project_name": "lcls:tmox42619",
        "queue_name": "milano"
      }
    }
  },
  "run_cache": "/sdf/home/r/rogersdd/src/nng_stream/nng_cache",
  "callback_url": "https://134.79.23.43:4433/v1/callback",
  "cache_ip": "134.79.23.43",
  "start_port": 30001,
  "end_port": 31000,
  "replay_job": {
    "name": "lclstream-push",
    "backend": "default",
    "resources": {
      "duration": 60,
      "node_count": 1,
      "processes_per_node": 1,
      "cpu_cores_per_process": 1
    },
    "script": "lclstream push --addr {url} --ndial 8 {pre}*.h5"
  },
  "lclstream_job": {
    "name": "lclstreamer",
    "backend": "slurm",
    "resources": {
      "duration": 60,
      "node_count": 1,
      "processes_per_node": 120,
      "cpu_cores_per_process": 1
    },
    "environment": {
      "CERTIFIED_CONFIG": "/sdf/home/r/rogersdd/venvs/lclstream_api/etc/certified"
    },
    "script": "pixi run --manifest-path /sdf/home/r/rogersdd/src/lclstreamer -e {psana_env} /sdf/group/lcls/ds/ana/sw/conda2/inst/envs/ps_20241122/bin/mpirun lclstreamer --config lclstreamer.json"
  }
}
```

Most of the important configuration parameters are system paths
at which various executables and data directories should live.

- `cache_fmt`: not used at present, it could be a server-side
  cache. This pairs with `replay_job`

- `replay_job`: not used at present, this is a jobspec
  to replay files cached server-side.

- `run_cache`: executable to run the nng\_cache program
  `lclstream_api/cache.py` runs this process on the same
  host where lclstream\_api is running.

- `callback_url`: The URL to which psik jobs should post their
  progress.  This has to be accessible from the host running
  the lclstreamer job.

- `cache_ip`: The IP address used to construct the cache URIs
  (`tcp://IP:port`)

- `start_port`/`end_port`: The range of ports that will be used to
  start nng\_cache servers locally.  Cache servers always allocate
  two ports at once (receive,send -- in that order).

The '{}' variables within the scripts are substituted with the
appropriate values by `lclstream_api` during job creation.
Also during job creation, the api ensures `lclstreamer.json`
(as requested by the user's API call) is present in
the job's run directory.  So that path name should be left as-is.


## Bootstrapping a New Installation

Installing this package brings with it the lclstream and
the psik packages.  The lclstream package is a client
for lclstreamer.  It can be used to run a test consumer.
The psik package is invoked by lclstream\_api to run the
producer job.  Before running transfers through the API,
these two should be tested.

Create a `$VIRTUAL_ENV/etc/psik.json` file with backend
configuration information,

```
{ "prefix": "/sdf/home/r/rogersdd/psik",
  "backends": {
    "default": {
        "type": "local"
    },
    "slurm": {
        "type": "slurm",
        "project_name": "lcls:tmox42619",
        "queue_name": "milano"
    }
}
```

Create the following yaml file specifying a psik job:

```
# slurm_test.yaml
name: lclstreamer_test
backend: slurm
resources:
  duration: 5
  node_count: 1
  processes_per_node: 1
  cpu_cores_per_process: 5
  gpu_cores_per_process: 0
  exclusive_node_use: false # not needed during testing
script: |
  cat >params.json <<.
  { "lclstreamer": {
    "source_identifier": "",
    "event_source": "InternalEventSource",
    "processing_pipeline": "BatchProcessingPipeline",
    "data_serializer": "Hdf5BinarySerializer",
    "data_handlers": [ "BinaryDataStreamingDataHandler" ],
    "skip_incomplete_events": false
  },
  "event_source": {
    "InternalEventSource": { "number_of_events_to_generate": 476 }
  },
  "data_sources": {
    "random": {
      "type": "GenericRandomNumpyArray",
      "array_shape": "20,2",
      "array_dtype": "float32"
    }
  },
  "data_serializer": {
    "Hdf5BinarySerializer": {
      "compression_level": 3,
      "fields": { "random": "/data/random" }
    }
  },
  "data_handlers": {
    "BinaryDataStreamingDataHandler": {
      "urls": [ "tcp://134.79.23.43:5001" ],
      "role": "client",
      "library": "nng",
      "socket_type": "push"
    }
  },
  "processing_pipeline": {
    "BatchProcessingPipeline": { "batch_size": 10 }
  }}
  .
  pixi run --manifest-path /sdf/home/r/rogersdd/src/lclstreamer -e psana1 mpiexec lclstreamer --config params.json
```

This creates an lclstreamer config, then executes lclstreamer
within mpiexec.  The producers will send their data to `tcp://134.79.23.43:5001`.

Before running this, execute a consumer on the host above
(the IP above is sdfdtn003 in this case):

    lclstream pull --listen tcp://134.79.23.43:5001 | tar tf -

Now put the job into the queue using

    psik run slurm_test.yaml

If psik's `slurm` backend has been configured to run this
job on the psana cluster, you should see data being received
from the consumer side.  To diagnose issues at this stage,
look at the job's output using:

    psik ls

and then reading the logs stored in this job's `log` directory.
If there are errors, you can re-start the same job using

    psik start <jobid>

rather than re-creating a copy of the same job.  However, you'll
need a new `psik run` command if you change the backend config.
or the job spec (`slurm_test.yaml`).

Once this is working, both of the two pieces of information above
(the psik config and the run-script line)
should be pasted into `$VIRTUAL_ENV/etc/lclstream_api.json`
described above.

## Comments on psik backend configuration

Note that [backend configuration](https://github.com/frobnitzem/psik/blob/main/psik/models.py) uses a template system, and that builds jobscripts using values from:

- `queue_name     : Optional[str] = None`
- `project_name   : Optional[str] = None`
- `reservation_id : Optional[str] = None`
- `attributes     : Dict[str,str] = {}`

At S3DF, we can use the SLURM job queue on psana by maintaining
a custom backend to psik that starts jobs with 'ssh psana sbatch'
instead of 'sbatch', etc.

This works because sdfdtn003 uses the same filesystem as psana,
so lclstream-api can see the psik job status, and jobs running
on psana can also run `psik` installed within lclstream-api's
python environment.

If lclstream-api is run containerized, however, then psik
should be setup to use psik-api as a backend, and psik-api
should run on the psana SLURM cluster.


# Development

Install the code using `poetry install` or `pip install -e .`.

Create a configuration file as explaned above, or in a local
directory.  If you set the `LCLSTREAM_API_CONFIG`
environment variable to point at your own json file, it
will override the one in `$VIRTUAL_ENV/etc`.

For testing, you can create a self-signed identity
using

    poetry run certified init --host 127.0.0.1 lclstream-api

Manually run the server code with:

    poetry run uvicorn lclstream_api.server:app --reload

or

    poetry run certified serve lclstream_api.server:app https://127.0.0.1:4433

It is helpful to create a minimal `$VIRTUAL_ENV/etc/psik.json`
setting 

    { "prefix": "/path/to/prefix"}

so you can use `psik ls`.

# Deployment Instructions

Install this package and its dependencies
using `pip install .` (for deployment)

If you do not yet have a server certificate,
create a server keypair using instructions from
[certified](https://certified.readthedocs.io/en/latest/).

Run the server with the `uvicorn` launch command
above, but specifying the key and certificate files
as explained there.

    certified serve lclstream_api.server:app https://0.0.0.0:4433

or

    uvicorn --ssl-keyfile server.key --ssl-certfile server.pem \
            --ssl-cert-reqs 1 --ssl-ca-certs ca_root.pem \
            lclstream_api.server:app

Sign user certificates using:

    certified introduce user.pem

or by installing and running [signer](https://gitlab.com/frobnitzem/signer).
