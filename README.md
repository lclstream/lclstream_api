# LCLStreamer-API

Image streaming application for LCLS.

This is a FastAPI server that pushes datasets
on command.  The steps run as follows,

1. Receive a REST-API POST request to initiate a dataset push.
   - API request should follow [LCLStreamer](https://slac-lcls.github.io/lclstreamer/) format.

2. A new transfer ID is created and a port allocated.

3. A separate process spawns to send all images
   to the requested server.

4. The transfer ID / url combination is returned to the
   requestor.

# Configuration

Create a configuration file in `/etc/lclstream_api.json` or
`$VIRTUAL_ENV/etc/lclstream_api.json` like:

    { "database_url": "sqlite+pysqlite:////sdf/home/r/rogersdd/lclstream_api.db",
      "psik": {
        "prefix": "/sdf/home/r/rogersdd/psik",
        "rc_path": "/sdf/home/r/rogersdd/bin/rc"
        "backends": {
          "default": {
            "type": "local"
          }
        }
      }
    }

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
