"""Pure builders for the producer (lclstreamer) IRI job (functional core).

No IO: these functions transform immutable inputs into immutable values
(an updated ``Parameters``, a rendered YAML string, an iri-models ``JobSpec``).
The shell (clients/iri.py + workflows.py) performs the upload and submission.
"""

import re
import shlex
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import UUID

import yaml
from amsc_iri.models import JobAttributes, JobSpec, ResourceSpec
from pydantic import BaseModel, ConfigDict

from ...lclstreamer_param import (
    BinaryDataStreamingDataHandlerParameters,
    Parameters,
)

if TYPE_CHECKING:
    from ..config import LCLStreamerProducerSettings
    from .transfer import ProducerInputs

CONFIG_FILENAME = "lclstreamer.yaml"
# Producer stdout/stderr land next to the config in the per-transfer work dir.
PRODUCER_STDOUT_FILENAME = "output.txt"
PRODUCER_STDERR_FILENAME = "error.txt"

# TODO: use container field after we get that feature added to iri
# NOTE: environment intentionally left empty here -- the currently-deployed
# S3DF IRI compute adapter REPLACES (not merges) its baseline Slurm
# environment (PATH, no HOME) with JobSpec.environment whenever it's
# non-empty, which broke apptainer/PMIx startup. Until that adapter is fixed
# and redeployed, TMPDIR/OMPI_MCA_orte_tmpdir_base/--mpi=pmix are baked
# directly into build_legacy_launch_script() instead.
DEFAULT_JOB_SPEC: JobSpec = JobSpec(
    executable="/app/lclstreamer/.pixi/envs/psana2extmpi/bin/lclstreamer",
    launcher="srun",
    inherit_environment=True,
    resources=ResourceSpec(
        node_count=2,
        process_count=64,
        processes_per_node=32,
        cpu_cores_per_process=1,
        gpu_cores_per_process=None,
        exclusive_node_use=True,
    ),
    attributes=JobAttributes(
        duration=3600,
        queue_name="milano",
        reservation_id=None,
    ),
)


def inject_cache_handlers(params: Parameters, pull_uri: str) -> Parameters:
    """Replace the data handlers with a single ZMQ push to the cache socket."""

    sink = BinaryDataStreamingDataHandlerParameters(
        type="BinaryDataStreamingDataHandler",
        urls=[pull_uri],
        distribute=True,  # TODO: why?
        buffer=0,  # TODO: why?
        role="client",
        library="zmq",
        socket_type="push",
    )
    return params.model_copy(update={"data_handlers": [sink]})


# TODO: psana-env extraction is broken -- _PSANA_ENV never resolves to
# psana2extmpi even though the container is always psana2extmpi. Details:
# repo memory lclstream-producer-mpi-env.md.
class PsanaEnv(StrEnum):
    psana1 = "psana1"
    psana2 = "psana2"
    psana2extmpi = "psana2extmpi"  # external mpi, like in the container version


_PSANA_ENV: dict[str, PsanaEnv] = {
    "InternalEventSource": PsanaEnv.psana2,
    "Psana1EventSource": PsanaEnv.psana1,
    "Psana2EventSource": PsanaEnv.psana2,
}


def render_config_yaml(params: Parameters) -> str:
    return yaml.safe_dump(params.model_dump(mode="json"), sort_keys=False)


def parse_exp_run(source_identifier: str) -> tuple[str | None, str | None]:
    exp: str | None = None
    run: str | None = None
    _SOURCE_ID_SEP = re.compile(r"[,:]")  # split on commas or colons
    for raw in _SOURCE_ID_SEP.split(source_identifier):
        token = raw.strip()
        if token.startswith("exp="):
            exp = token[len("exp=") :].strip() or None
        elif token.startswith("run="):
            run = token[len("run=") :].strip() or None
    return exp, run


def short_id(transfer_id: UUID) -> str:
    return str(transfer_id)[:8]


def transfer_work_dir(
    settings: LCLStreamerProducerSettings, exp: str, run: str, transfer_id: UUID
) -> Path:
    """Per-transfer working directory."""
    instrument = exp[:3]
    return (
        Path(settings.data_base_dir)
        / instrument
        / exp
        / "scratch"
        / "lclstreamer"
        / f"lclstreamer_{exp}_{run}_{short_id(transfer_id)}"
    )


def producer_config_path(
    settings: LCLStreamerProducerSettings, exp: str, run: str, transfer_id: UUID
) -> Path:
    """Remote producer config filepath."""
    return transfer_work_dir(settings, exp, run, transfer_id) / CONFIG_FILENAME


def build_legacy_launch_script(
    *, launcher: str, container_image: str, config_path: Path, job_dir: Path
) -> str:
    """Bake the launcher/apptainer invocation into one script string.

    TODO: Workaround for the currently-deployed S3DF IRI compute adapter, which
    passes JobSpec.executable straight through as literal Slurm batch-script
    text and ignores `launcher`, `container`, `pre_launch`, and `post_launch`
    entirely. Once that adapter honors those fields drop this and go back to a
    bare `executable` path plus `launcher`/`container` on JobSpec.

    Also bakes HOME/USER/TMPDIR/OMPI_MCA_orte_tmpdir_base exports and
    `--mpi=pmix` directly here (rather than via JobSpec.environment) because
    the currently-deployed adapter replaces its whole baseline environment
    (dropping HOME) whenever JobSpec.environment is non-empty.

    Also pins PMIX_MCA_psec/gds so PMIx doesn't probe-and-fail its defaults
    (munge security component, ds12 pthread-lock datastore) before falling
    back.
    """
    return (
        "#!/bin/bash\n"
        "set -e\n"
        'export HOME="${HOME:-$(getent passwd "$(id -un)" | cut -d: -f6)}"\n'
        'export USER="${USER:-$(id -un)}"\n'
        "export TMPDIR=/tmp\n"
        "export OMPI_MCA_orte_tmpdir_base=/tmp\n"
        "export PMIX_MCA_psec=native\n"
        "export PMIX_MCA_gds=hash\n"
        # The adapter's minimal baseline PATH doesn't include S3DF's Slurm
        # install dir, so `srun` isn't found -- add it explicitly. See repo
        # memory lclstream-producer-mpi-env.md.
        'export PATH="/opt/slurm/slurm-curr/bin:$PATH"\n'
        # Resolve apptainer via PATH first, falling back to its known S3DF
        # location if PATH resolution fails for any reason.
        'APPTAINER_BIN="$(command -v apptainer 2>/dev/null || echo /usr/bin/apptainer)"\n'
        + f"mkdir -p {shlex.quote(str(job_dir))}\n"
        + f'{launcher} --mpi=pmix "$APPTAINER_BIN" run --bind /sdf:/sdf '
        f"{shlex.quote(container_image)} "
        f"lclstreamer --config {shlex.quote(str(config_path))}\n"
        "echo done\n"
    )


class ProducerPlan(BaseModel):
    """This is the plan to provision the producer: the jobspec plus
    the config file to upload to the IRI filesystem before submission."""

    model_config = ConfigDict(frozen=True)

    jobspec: JobSpec
    config_path: Path
    config_yaml: str


def build_producer_plan(
    params: Parameters,
    settings: LCLStreamerProducerSettings,
    *,
    name: str,
    exp: str,
    run: str,
    transfer_id: UUID,
) -> ProducerPlan:
    psana_env = _PSANA_ENV[params.event_source.type]
    psana_environment = settings.environments.get(psana_env) or {}

    job_dir = transfer_work_dir(settings, exp, run, transfer_id)
    config_path = producer_config_path(settings, exp, run, transfer_id)

    jobspec = DEFAULT_JOB_SPEC.model_copy(deep=True)
    jobspec.attributes.account = f"lcls:{exp}"  # pyrefly: ignore[missing-attribute]
    assert jobspec.launcher  # just for type checker, we know this is there
    jobspec.executable = build_legacy_launch_script(
        launcher=jobspec.launcher,
        container_image=settings.container_image,
        config_path=config_path,
        job_dir=job_dir,
    )
    jobspec.arguments = []
    jobspec.name = name
    jobspec.environment = {**(jobspec.environment or {}), **psana_environment}
    jobspec.directory = str(job_dir)
    jobspec.stdout_path = str(job_dir / PRODUCER_STDOUT_FILENAME)
    jobspec.stderr_path = str(job_dir / PRODUCER_STDERR_FILENAME)
    return ProducerPlan(
        jobspec=JobSpec.model_validate(jobspec),
        config_path=config_path,
        config_yaml=render_config_yaml(params),
    )


PRODUCER_JOB_NAME_PREFIX = "lclstream-producer-"


def plan_producer(
    inputs: ProducerInputs,
    settings: LCLStreamerProducerSettings,
    transfer_id: UUID,
) -> ProducerPlan:
    """Compose the full producer plan for a transfer."""
    params = inject_cache_handlers(inputs.parameters, inputs.endpoint.pull_uri)
    return build_producer_plan(
        params,
        settings,
        name=f"{PRODUCER_JOB_NAME_PREFIX}{short_id(transfer_id)}",
        exp=inputs.exp,
        run=inputs.run,
        transfer_id=transfer_id,
    )
