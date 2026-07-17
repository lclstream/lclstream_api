"""Pure builders for the producer (lclstreamer) IRI job (functional core).

No IO: these functions transform immutable inputs into immutable values
(an updated ``Parameters``, a rendered YAML string, an iri-models ``JobSpec``).
The shell (clients/iri.py + workflows.py) performs the upload and submission.
"""

import re
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import UUID

import yaml
from amsc_iri.models import JobAttributes, JobSpec, ResourceSpec
from pydantic import BaseModel, ConfigDict

from ...lclstreamer_param import (
    BinaryDataStreamingDataHandlerParameters,
    Parameters,
)
from .enums import CacheMode

if TYPE_CHECKING:
    from ..config import LCLStreamerProducerSettings
    from .transfer import ProducerInputs

CONFIG_FILENAME = "lclstreamer.yaml"
PRODUCER_STDOUT_FILENAME = "output.txt"
PRODUCER_STDERR_FILENAME = "error.txt"


def cache_idle_timeout_ms(mode: CacheMode) -> int | None:
    return -1 if mode is CacheMode.shared else None


# TODO: use container field after we get that feature added to iri
# NOTE: JobSpec.environment replaces the S3DF IRI compute
# adapter's own baseline Slurm environment whenever it's non-empty
# So this dict must be self-sufficient (later we remove PATH and use module load)
DEFAULT_JOB_SPEC: JobSpec = JobSpec(
    launcher="srun --mpi=pmix",
    inherit_environment=True,
    environment={
        "TMPDIR": "/tmp",
        "OMPI_MCA_orte_tmpdir_base": "/tmp",
        "PMIX_MCA_psec": "native",
        "PMIX_MCA_gds": "hash",
        "PATH": "/opt/slurm/slurm-curr/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
    },
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

APPTAINER_BIN = "/usr/bin/apptainer"


def _deep_merge(base: dict[str, Any], update: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in update.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def apply_job_spec_update(jobspec: JobSpec, update: JobSpec | None) -> JobSpec:
    if update is None:
        return jobspec
    merged = _deep_merge(jobspec.model_dump(), update.model_dump(exclude_unset=True))
    return JobSpec.model_validate(merged)


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
# psana2extmpi even though the container is always psana2extmpi.
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


def resolve_exp_run(params: Parameters) -> tuple[str, str]:
    exp, run = parse_exp_run(params.source_identifier)
    if exp is None or run is None:
        raise ValueError("source_identifier has no resolvable exp/run")
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


def shared_cache_dir(settings: LCLStreamerProducerSettings, exp: str) -> Path:
    """Directory for a shared (per-experiment) cache."""
    instrument = exp[:3]
    return (
        Path(settings.data_base_dir)
        / instrument
        / exp
        / "scratch"
        / "lclstreamer"
        / "shared_cache"
    )


class ProducerPlan(BaseModel):
    """This is the plan to provision the producer: the jobspec plus
    the config file to upload to the IRI filesystem before submission."""

    model_config = ConfigDict(frozen=True)

    jobspec: JobSpec
    config_path: Path
    config_yaml: str


def build_job_spec(
    params: Parameters,
    settings: LCLStreamerProducerSettings,
    *,
    name: str,
    exp: str,
    run: str,
    transfer_id: UUID,
    job_spec_override: JobSpec | None = None,
) -> JobSpec:
    """Resolve the full producer JobSpec for a transfer. Deterministic given
    its inputs (not dependent on the allocated cache), so it's computed once
    at transfer-creation time and the result is what gets persisted/submitted
    -- not recomputed later during provisioning.
    """
    psana_env = _PSANA_ENV[params.event_source.type]
    psana_environment = settings.environments.get(psana_env) or {}

    job_dir = transfer_work_dir(settings, exp, run, transfer_id)
    config_path = producer_config_path(settings, exp, run, transfer_id)

    jobspec = DEFAULT_JOB_SPEC.model_copy(deep=True)
    jobspec.attributes.account = f"lcls:{exp}"  # pyrefly: ignore[missing-attribute]
    jobspec.executable = APPTAINER_BIN
    jobspec.arguments = [
        "run",
        "--bind",
        "/sdf:/sdf",
        settings.container_image,
        "lclstreamer",
        "--config",
        str(config_path),
    ]
    jobspec.post_launch = "echo done"
    jobspec.name = name
    jobspec.environment = {**(jobspec.environment or {}), **psana_environment}
    jobspec.directory = str(job_dir)
    jobspec.stdout_path = str(job_dir / PRODUCER_STDOUT_FILENAME)
    jobspec.stderr_path = str(job_dir / PRODUCER_STDERR_FILENAME)
    return apply_job_spec_update(jobspec, job_spec_override)


def build_producer_plan(
    jobspec: JobSpec,
    params: Parameters,
    settings: LCLStreamerProducerSettings,
    *,
    exp: str,
    run: str,
    transfer_id: UUID,
) -> ProducerPlan:
    """Pair an already-resolved JobSpec (see ``build_job_spec``) with the
    config file to upload for this transfer."""
    return ProducerPlan(
        jobspec=jobspec,
        config_path=producer_config_path(settings, exp, run, transfer_id),
        config_yaml=render_config_yaml(params),
    )


PRODUCER_JOB_NAME_PREFIX = "lclstream-producer-"


def producer_job_name(transfer_id: UUID) -> str:
    return f"{PRODUCER_JOB_NAME_PREFIX}{short_id(transfer_id)}"


def plan_producer(
    inputs: ProducerInputs,
    settings: LCLStreamerProducerSettings,
    transfer_id: UUID,
) -> ProducerPlan:
    """Compose the full producer plan for a transfer."""
    params = inject_cache_handlers(inputs.parameters, inputs.endpoint.pull_uri)
    return build_producer_plan(
        inputs.job_spec,
        params,
        settings,
        exp=inputs.exp,
        run=inputs.run,
        transfer_id=transfer_id,
    )
