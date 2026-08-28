from collections.abc import Callable
from pathlib import Path
from uuid import UUID

import pytest
import yaml
from amsc_iri.models import JobAttributes, JobSpec

from lclstream_api.lclstreamer_param import Parameters
from lclstream_api.v2.config import LCLStreamerProducerSettings
from lclstream_api.v2.core.producer import (
    CACHE_SINK_LINGER_MS,
    CONFIG_FILENAME,
    CacheMode,
    apply_job_spec_update,
    build_job_spec,
    build_producer_plan,
    cache_idle_timeout_ms,
    inject_cache_handlers,
    parse_exp_run,
    producer_config_path,
    render_config_yaml,
    required_token_lifetime_seconds,
    short_id,
    transfer_work_dir,
)

ParamsFactory = Callable[..., Parameters]
SettingsFactory = Callable[..., LCLStreamerProducerSettings]

TRANSFER_ID = UUID("12345678-1234-5678-1234-567812345678")


@pytest.mark.parametrize(
    ("duration", "grace", "expected"),
    [(3600, 900, 4500), (1, 0, 1)],
)
def test_required_token_lifetime_seconds(
    duration: int, grace: int, expected: int
) -> None:
    job_spec = JobSpec(
        attributes=JobAttributes(account="lcls:mfxl1001", duration=duration)
    )
    assert required_token_lifetime_seconds(job_spec, grace) == expected


def test_required_token_lifetime_rejects_negative_grace() -> None:
    job_spec = JobSpec(attributes=JobAttributes(account="lcls:mfxl1001", duration=3600))
    with pytest.raises(ValueError, match="non-negative"):
        required_token_lifetime_seconds(job_spec, -1)


# ---------------------------------------------------------------------------
# parse_exp_run: pull exp/run out of a source_identifier
# ---------------------------------------------------------------------------

_PARSE_CASES = [
    ("comma-separated", "exp=mfxl1001,run=42", ("mfxl1001", "42")),
    ("colon-separated", "exp=mfxl1001:run=42", ("mfxl1001", "42")),
    ("whitespace-trimmed", " exp=mfxl1001 , run=42 ", ("mfxl1001", "42")),
    ("run-before-exp", "run=42,exp=mfxl1001", ("mfxl1001", "42")),
    ("only-exp", "exp=mfxl1001", ("mfxl1001", None)),
    ("only-run", "run=42", (None, "42")),
    ("empty-string", "", (None, None)),
    ("unrelated-tokens", "foo=bar,baz", (None, None)),
    ("empty-value-is-none", "exp=,run=42", (None, "42")),
]


@pytest.mark.parametrize(
    ("source_identifier", "expected"),
    [(c[1], c[2]) for c in _PARSE_CASES],
    ids=[c[0] for c in _PARSE_CASES],
)
def test_parse_exp_run(
    source_identifier: str, expected: tuple[str | None, str | None]
) -> None:
    assert parse_exp_run(source_identifier) == expected


# ---------------------------------------------------------------------------
# Deterministic on-filesystem paths
# ---------------------------------------------------------------------------


def test_short_id_is_first_eight_chars() -> None:
    assert short_id(TRANSFER_ID) == "12345678"


def test_producer_job_path_layout(
    make_producer_settings: SettingsFactory,
) -> None:
    settings = make_producer_settings(data_base_dir="/sdf/data/lcls/ds")
    path = transfer_work_dir(
        settings, exp="mfxl1001", run="42", transfer_id=TRANSFER_ID
    )
    assert path == Path(
        "/sdf/data/lcls/ds/mfx/mfxl1001/scratch/lclstreamer/lclstreamer_mfxl1001_42_12345678"
    )


def test_instrument_is_first_three_chars_of_exp(
    make_producer_settings: SettingsFactory,
) -> None:
    settings = make_producer_settings(data_base_dir="/base")
    path = transfer_work_dir(
        settings, exp="cxic00118", run="7", transfer_id=TRANSFER_ID
    )
    assert path.parts[1:3] == ("base", "cxi")


def test_config_path_is_job_dir_plus_filename(
    make_producer_settings: SettingsFactory,
) -> None:
    settings = make_producer_settings(data_base_dir="/base")
    job = transfer_work_dir(settings, exp="mfxl1001", run="42", transfer_id=TRANSFER_ID)
    cfg = producer_config_path(
        settings, exp="mfxl1001", run="42", transfer_id=TRANSFER_ID
    )
    assert cfg == job / CONFIG_FILENAME
    assert cfg.name == "lclstreamer.yaml"


# ---------------------------------------------------------------------------
# inject_cache_sink: lock the stream to the allocated cache socket
# ---------------------------------------------------------------------------


def test_inject_cache_sink_replaces_all_handlers_with_one_push(
    make_params: ParamsFactory,
) -> None:
    params = make_params(
        data_handlers=[
            {"type": "BinaryDataStreamingDataHandler", "urls": ["tcp://evil:9999"]},
            {"type": "BinaryFileWritingDataHandler", "file_prefix": "leak"},
        ]
    )
    out = inject_cache_handlers(params, "tcp://cache-host:5001")

    assert len(out.data_handlers) == 1
    sink = out.data_handlers[0]
    assert sink.type == "BinaryDataStreamingDataHandler"
    assert sink.urls == ["tcp://cache-host:5001"]
    assert sink.role == "client"
    assert sink.socket_type == "push"
    # LINGER 0 truncates every transfer's tail.
    assert sink.linger == CACHE_SINK_LINGER_MS
    assert sink.linger > 0


def test_inject_cache_sink_does_not_mutate_input(
    make_params: ParamsFactory,
) -> None:
    params = make_params()
    before = params.model_dump(mode="json")
    inject_cache_handlers(params, "tcp://cache-host:5001")
    assert params.model_dump(mode="json") == before


# ---------------------------------------------------------------------------
# render_config_yaml: serialize Parameters to a YAML string
# ---------------------------------------------------------------------------


def test_render_config_yaml_roundtrips(make_params: ParamsFactory) -> None:
    params = make_params()
    rendered = render_config_yaml(params)
    assert yaml.safe_load(rendered) == params.model_dump(mode="json")


# ---------------------------------------------------------------------------
# build_job_spec / build_producer_plan
# ---------------------------------------------------------------------------


def test_build_job_spec_and_producer_plan_assemble_jobspec_and_config(
    make_params: ParamsFactory,
    make_producer_settings: SettingsFactory,
) -> None:
    params = make_params()  # InternalEventSource -> psana2 environment
    settings = make_producer_settings(
        data_base_dir="/sdf/data/lcls/ds",
        environments={"psana2": {"PSANA_VERSION": "2"}},
    )

    jobspec = build_job_spec(
        params,
        settings,
        name="transfer-job",
        exp="mfxl1001",
        run="42",
        transfer_id=TRANSFER_ID,
    )
    plan = build_producer_plan(
        jobspec, params, settings, exp="mfxl1001", run="42", transfer_id=TRANSFER_ID
    )

    expected_dir = "/sdf/data/lcls/ds/mfx/mfxl1001/scratch/lclstreamer/lclstreamer_mfxl1001_42_12345678"
    assert str(plan.config_path) == f"{expected_dir}/{CONFIG_FILENAME}"
    assert plan.config_yaml == render_config_yaml(params)

    spec = plan.jobspec
    assert spec.attributes is not None
    assert spec.attributes.account == "lcls:mfxl1001"
    assert spec.name == "transfer-job"
    assert spec.executable == "/usr/bin/apptainer"
    assert spec.arguments == [
        "run",
        "--bind",
        "/sdf:/sdf",
        settings.container_image,
        "lclstreamer",
        "--config",
        str(plan.config_path),
    ]
    assert spec.launcher == "srun --mpi=pmix"
    assert spec.post_launch == "echo done"
    assert spec.pre_launch is None
    assert spec.directory == expected_dir
    assert spec.stdout_path == f"{expected_dir}/output.txt"
    assert spec.stderr_path == f"{expected_dir}/error.txt"
    assert spec.stdin_path is None
    assert spec.environment == {
        "TMPDIR": "/tmp",
        "OMPI_MCA_orte_tmpdir_base": "/tmp",
        "PMIX_MCA_psec": "native",
        "PMIX_MCA_gds": "hash",
        "PATH": "/opt/slurm/slurm-curr/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        "PSANA_VERSION": "2",
    }


def test_build_job_spec_without_matching_environment_keeps_defaults(
    make_params: ParamsFactory,
    make_producer_settings: SettingsFactory,
) -> None:
    """If no environment is configured for the resolved psana env, the jobspec
    environment still carries DEFAULT_JOB_SPEC's own MPI/PMIx/PATH vars
    (those are unconditional, not psana-version-gated)."""

    params = make_params()
    settings = make_producer_settings(environments={})  # nothing for psana2

    jobspec = build_job_spec(
        params, settings, name="job", exp="mfxl1001", run="42", transfer_id=TRANSFER_ID
    )
    assert jobspec.environment == {
        "TMPDIR": "/tmp",
        "OMPI_MCA_orte_tmpdir_base": "/tmp",
        "PMIX_MCA_psec": "native",
        "PMIX_MCA_gds": "hash",
        "PATH": "/opt/slurm/slurm-curr/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
    }


def test_build_job_spec_does_not_mutate_default_spec(
    make_params: ParamsFactory,
    make_producer_settings: SettingsFactory,
) -> None:
    """Building one jobspec must not bleed state into the shared
    ``DEFAULT_JOB_SPEC``."""

    params = make_params()
    settings = make_producer_settings()

    first = build_job_spec(
        params, settings, name="a", exp="mfxl1001", run="1", transfer_id=TRANSFER_ID
    )
    other_id = UUID("87654321-4321-8765-4321-876543218765")
    second = build_job_spec(
        params, settings, name="b", exp="cxic00118", run="9", transfer_id=other_id
    )

    assert first.name == "a"
    assert second.name == "b"
    assert first.attributes is not None
    assert second.attributes is not None
    assert first.attributes.account == "lcls:mfxl1001"
    assert second.attributes.account == "lcls:cxic00118"


# ---------------------------------------------------------------------------
# apply_job_spec_update / JobSpecUpdate: caller-supplied JobSpec overrides
# ---------------------------------------------------------------------------


def test_apply_job_spec_update_none_is_noop() -> None:
    base = JobSpec(name="job", attributes=JobAttributes(account="lcls:mfxl1001"))
    assert apply_job_spec_update(base, None) is base


def test_apply_job_spec_update_only_overrides_set_nested_fields() -> None:
    """Overriding account must not clobber siblings the update left unset."""
    base = JobSpec(
        attributes=JobAttributes(
            account="lcls:mfxl1001", queue_name="milano", duration=3600
        )
    )
    update = JobSpec(attributes=JobAttributes(account="lcls:public01"))

    merged = apply_job_spec_update(base, update)

    assert merged.attributes is not None
    assert merged.attributes.account == "lcls:public01"
    assert merged.attributes.queue_name == "milano"
    assert merged.attributes.duration == 3600


def test_apply_job_spec_update_merges_environment_additively() -> None:
    """``environment`` is a plain dict, not a submodel, but the deep merge
    still adds keys instead of replacing it."""
    base = JobSpec(environment={"TMPDIR": "/tmp", "PATH": "/bin"})
    update = JobSpec(environment={"FOO": "bar"})

    merged = apply_job_spec_update(base, update)

    assert merged.environment == {"TMPDIR": "/tmp", "PATH": "/bin", "FOO": "bar"}


def test_apply_job_spec_update_does_not_mutate_base() -> None:
    base = JobSpec(attributes=JobAttributes(account="lcls:mfxl1001"))
    update = JobSpec(attributes=JobAttributes(account="lcls:public01"))

    apply_job_spec_update(base, update)

    assert base.attributes is not None
    assert base.attributes.account == "lcls:mfxl1001"


def test_build_job_spec_applies_job_spec_update(
    make_params: ParamsFactory,
    make_producer_settings: SettingsFactory,
) -> None:
    """The account override wins over the exp-derived default; everything else
    stays computed."""
    params = make_params()
    settings = make_producer_settings(data_base_dir="/sdf/data/lcls/ds")
    override = JobSpec(attributes=JobAttributes(account="lcls:mfx101629726"))

    jobspec = build_job_spec(
        params,
        settings,
        name="transfer-job",
        exp="mfx100848724",
        run="51",
        transfer_id=TRANSFER_ID,
        job_spec_override=override,
    )

    assert jobspec.attributes is not None
    assert jobspec.attributes.account == "lcls:mfx101629726"
    assert jobspec.attributes.queue_name == "milano"
    assert jobspec.executable == "/usr/bin/apptainer"


@pytest.mark.parametrize(
    ("mode", "expected"),
    [
        (CacheMode.shared, -1),
        (CacheMode.per_transfer, None),
    ],
)
def test_cache_idle_timeout_ms(mode: CacheMode, expected: int | None) -> None:
    assert cache_idle_timeout_ms(mode) == expected
