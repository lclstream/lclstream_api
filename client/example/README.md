# See [main](main.py) for example usage code

```sh
cp .env.example .env # and edit LCLSTREAM_EXAMPLE_SOURCE_IDENTIFIER for your data
./scripts/s3df-login
```

## CrystFEL at NERSC

`submit_crystfel_nersc.py` runs at SLAC and submits the job through NERSC's
IRI API as psdatmgr:

```sh
uv run submit_crystfel_nersc.py --resources                 # resource names
uv run submit_crystfel_nersc.py --exp mfx101592326 --run 12
uv run submit_crystfel_nersc.py --exp mfx101592326 --run 12 --status <job_id>
```

The bearer token is minted from psdatmgr's SFAPI key via `sfapi_client`
(same credential shape as `wflow_jid`'s `dal/sfapi.py`); IRI gets a token
provider, so it refreshes when the short-lived token expires.

`--exp` and `--run` become the `exp=...,run=...` source identifier, passed to
the job as an argument -- not through the JobSpec environment, which replaces
the inherited Slurm env. The API parses exp and run back out of that string
and charges the producer to `lcls:<exp>`. One caveat: if the data's experiment
is not the one you can charge, `LCLSTREAM_CRYSTFEL_JOB_SPEC_ACCOUNT` in the
`.env` still overrides it, and it is per-experiment.

### Where things live on NERSC

Everything runs as psdatmgr, so all four paths are psdatmgr's.

| What | Where | Why |
|---|---|---|
| this repo, and the image build | `~/repos/github/lclstream/lclstream_api/client/example` | `crystfel_nersc.sh` is read from the clone, so `git pull` is the only way to update it -- no staged copy to drift |
| `.env`, geometry, `.secrets/s3df_token.<exp>.<run>`, `crystfel.<exp>.<run>.log` | `/global/cfs/cdirs/lcls/crystfel-demo` | group-readable so the team can edit config and read logs, and CFS is the only filesystem IRI's `tail` can reach |
| `crystfel.<jobid>.stream` | `$PSCRATCH/crystfel-demo` | write bandwidth, auto-purged, and it leaves for SLAC immediately |

Stage only `.env` and the geometry in the job dir; the job writes the log and
creates the scratch dir. Submitting uploads the token for you, mode 600, and
the job deletes it on exit -- including the paths where it fails early.
lclstream_api rejects a token that expires before the producer job would, so
it needs ~75 minutes of life left *when the job starts* -- queue wait counts
against it.

Override either with `LCLSTREAM_NERSC_SCRIPT` or `LCLSTREAM_NERSC_DIRECTORY`.

### Two images, on purpose

`Dockerfile` builds **only** the lclstream_api client -- no CrystFEL in it.
The job body runs three containers:

| # | Image | Does |
|---|---|---|
| 1 | `lclstream-client` | `transfer_client.py create` -- starts the transfer, prints the ZMQ URI |
| 2 | upstream CrystFEL | `indexamajig --zmq-input=<uri> …` with whatever arguments you pass |
| 3 | `lclstream-client` | `transfer_client.py cancel <id>`, from a trap |

So indexamajig's arguments, its geometry, and CrystFEL's own version all
change without rebuilding anything of ours. The submit command sends one
argument string, and it *replaces* the script's defaults rather than adding
to them -- so a caller that sends anything owns the whole set, including
`--data-format=msgpack`, `--peaks=msgpack`, and the `--copy-header`s that
this data path needs:

```sh
uv run submit_crystfel_nersc.py --exp mfx101592326 --run 12 \
    --crystfel-args "--zmq-request=next --data-format=msgpack \
        --peaks=msgpack --indexing=mosflm -j 8"
# or after a bare --
uv run submit_crystfel_nersc.py --exp mfx101592326 --run 12 -- --indexing=none
```

Send nothing and the script's defaults apply, which is the configuration
above with `--indexing` left to CrystFEL. Three arguments stay the script's
either way, because only the job knows them: `--zmq-input` (the URI does not
exist until the transfer opens), `--temp-dir`, and `-o`. The log line
`[crystfel] args: …` records what actually reached indexamajig.

Geometry is a file in the job dir, selected with `CRYSTFEL_GEOM` in `.env`;
it is not baked into any image, and it is deliberately not part of the
argument string -- the caller would have to know the remote path. Do not put
a `-g` in the string: the script's lands after it and wins.

Build ours once on a login node, as psdatmgr:

```sh
cd ~/repos/github/lclstream/lclstream_api/client/example
podman-hpc build -t lclstream-client:latest -f Dockerfile
podman-hpc migrate lclstream-client:latest     # again after every rebuild
podman-hpc build -t crystfel:latest -f Dockerfile.crystfel
podman-hpc migrate crystfel:latest
```

`migrate` is what makes an image visible to compute nodes, and the image
store is per-user -- build as psdatmgr, the account the job runs as.

The client container mounts the job dir at `/cfg` (read-only) and the token
at `/run/s3df_token`; the CrystFEL container mounts `/cfg` read-only for the
geometry and the scratch dir at `/out` for temp files and the stream.

## Debugging a failed transfer

`transfer_logs.py` pulls everything the SLAC side will tell you about one
transfer: state and transitions, the cache log, both producer streams, and
`fastcache_api`'s own log over the S3DF IRI filesystem.

```sh
uv run transfer_logs.py <transfer_id>
uv run transfer_logs.py <transfer_id> --watch    # races the cleanup
```

`lclstream_api` compensates a failed transfer by deleting its work dir, so
the cache log can vanish within seconds of the failure -- `--watch` polls for
it and keeps the last copy it sees. That log is the only place the cache
process reports for itself, and reading it is what separated "the cache
crashed" from "something else marked it dead".
