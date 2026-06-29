from collections.abc import Callable
from pathlib import Path
from uuid import UUID

import pytest
import yaml

from lclstream_api.lclstreamer_param import Parameters
from lclstream_api.v2.config import LCLStreamerProducerSettings
from lclstream_api.v2.core.producer import (
    CONFIG_FILENAME,
    build_producer_plan,
    inject_cache_handlers,
    parse_exp_run,
    producer_config_path,
    render_config_yaml,
    short_id,
    transfer_work_dir,
)

ParamsFactory = Callable[..., Parameters]
SettingsFactory = Callable[..., LCLStreamerProducerSettings]

TRANSFER_ID = UUID("12345678-1234-5678-1234-567812345678")


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
# build_producer_plan: the full jobspec + config the shell needs
# ---------------------------------------------------------------------------


def test_build_producer_plan_assembles_jobspec_and_config(
    make_params: ParamsFactory,
    make_producer_settings: SettingsFactory,
) -> None:
    params = make_params()  # InternalEventSource -> psana2 environment
    settings = make_producer_settings(
        data_base_dir="/sdf/data/lcls/ds",
        environments={"psana2": {"PSANA_VERSION": "2"}},
    )

    plan = build_producer_plan(
        params,
        settings,
        name="transfer-job",
        exp="mfxl1001",
        run="42",
        transfer_id=TRANSFER_ID,
    )

    expected_dir = "/sdf/data/lcls/ds/mfx/mfxl1001/scratch/lclstreamer/lclstreamer_mfxl1001_42_12345678"
    assert str(plan.config_path) == f"{expected_dir}/{CONFIG_FILENAME}"
    assert plan.config_yaml == render_config_yaml(params)

    spec = plan.jobspec
    assert spec.attributes is not None
    assert spec.attributes.account == "lcls:mfxl1001"
    assert spec.name == "transfer-job"
    assert spec.arguments == []
    assert spec.pre_launch is None
    assert spec.executable is not None
    assert f"mkdir -p {expected_dir}\n" in spec.executable
    assert f"--config {plan.config_path}" in spec.executable
    assert '"$APPTAINER_BIN" run' in spec.executable
    assert spec.launcher == "srun"
    assert 'srun --mpi=pmix "$APPTAINER_BIN" run' in spec.executable
    assert "export TMPDIR=/tmp" in spec.executable
    assert "export OMPI_MCA_orte_tmpdir_base=/tmp" in spec.executable
    assert "export HOME=" in spec.executable
    assert "export PATH=" in spec.executable
    assert "/opt/slurm/slurm-curr/bin" in spec.executable
    assert "APPTAINER_BIN=" in spec.executable
    assert "/usr/bin/apptainer" in spec.executable
    assert "--bind /sdf:/sdf" in spec.executable
    assert spec.directory == expected_dir
    assert spec.stdout_path == f"{expected_dir}/output.txt"
    assert spec.stderr_path == f"{expected_dir}/error.txt"
    assert spec.stdin_path is None
    assert spec.environment == {"PSANA_VERSION": "2"}


def test_build_producer_plan_without_matching_environment_is_empty(
    make_params: ParamsFactory,
    make_producer_settings: SettingsFactory,
) -> None:
    """If no environment is configured for the resolved psana env, the jobspec
    environment is empty -- DEFAULT_JOB_SPEC carries no environment of its own
    (MPI/TMPDIR vars are baked into the script instead, see
    build_legacy_launch_script)."""

    params = make_params()
    settings = make_producer_settings(environments={})  # nothing for psana2

    plan = build_producer_plan(
        params,
        settings,
        name="job",
        exp="mfxl1001",
        run="42",
        transfer_id=TRANSFER_ID,
    )
    assert plan.jobspec.environment == {}


def test_build_producer_plan_does_not_mutate_default_spec(
    make_params: ParamsFactory,
    make_producer_settings: SettingsFactory,
) -> None:
    """Two plans must be independent: building one must not bleed state into the
    shared ``DEFAULT_JOB_SPEC`` and thus into the next."""

    params = make_params()
    settings = make_producer_settings()

    first = build_producer_plan(
        params, settings, name="a", exp="mfxl1001", run="1", transfer_id=TRANSFER_ID
    )
    other_id = UUID("87654321-4321-8765-4321-876543218765")
    second = build_producer_plan(
        params, settings, name="b", exp="cxic00118", run="9", transfer_id=other_id
    )

    assert first.jobspec.name == "a"
    assert second.jobspec.name == "b"
    assert first.jobspec.attributes is not None
    assert second.jobspec.attributes is not None
    assert first.jobspec.attributes.account == "lcls:mfxl1001"
    assert second.jobspec.attributes.account == "lcls:cxic00118"
    assert first.config_path != second.config_path
