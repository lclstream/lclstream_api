"""Guards the vendored lclstreamer parameter model against drift.

``_CustomBaseModel`` forbids extra fields, so an unvendored variant
becomes a 422. These pin what upstream has today.
"""

from collections.abc import Callable

import pytest
from pydantic import TypeAdapter, ValidationError

from lclstream_api.lclstreamer_param import (
    BinaryDataStreamingDataHandlerParameters,
    DataSerializerParameters,
    Parameters,
    ProcessingPipelineParameters,
    Psana1DetectorInterfaceParameters,
    Psana2DetectorInterfaceParameters,
)

_PIPELINES = TypeAdapter(ProcessingPipelineParameters)
_SERIALIZERS = TypeAdapter(DataSerializerParameters)

ParamsFactory = Callable[..., Parameters]


@pytest.mark.parametrize(
    "payload",
    [
        {"type": "BatchProcessingPipeline", "batch_size": 10},
        {"type": "CrystfelPreprocessingPipeline"},
    ],
)
def test_pipeline_variants_accepted(payload: dict) -> None:
    assert _PIPELINES.validate_python(payload).type == payload["type"]


@pytest.mark.parametrize(
    "payload",
    [
        {"type": "HDF5BinarySerializer", "compression_level": 3, "fields": {}},
        {"type": "MsgpackBinarySerializer"},
    ],
)
def test_serializer_variants_accepted(payload: dict) -> None:
    assert _SERIALIZERS.validate_python(payload).type == payload["type"]


def test_rejects_unmerged_upstream_fields() -> None:
    """These live on lclstreamer#58; no published .sif accepts them.

    Vendoring them made the API emit YAML the producer rejected. Re-add only
    once the PR merges and the producer image carries it.
    """
    for field, value in (
        ("linger", 30_000),
        ("photon_wavelength_source", "SIOC:SYS0:ML00:AO192"),
        ("spectrometer_source", "feespec.raw.hproj"),
    ):
        with pytest.raises(ValidationError):
            BinaryDataStreamingDataHandlerParameters(
                type="BinaryDataStreamingDataHandler",
                urls=["tcp://cache:5000"],
                **{field: value},
            )


def test_psana1_detector_accepts_dtype() -> None:
    detector = Psana1DetectorInterfaceParameters(
        type="Psana1DetectorInterface",
        psana_name="jungfrau",
        psana_fields="calib",
        dtype="float32",
    )
    assert detector.dtype == "float32"


def test_multiple_psana_fields_need_alias() -> None:
    with pytest.raises(ValidationError, match="alias"):
        Psana2DetectorInterfaceParameters(
            type="Psana2DetectorInterface",
            psana_name="jungfrau",
            psana_fields=["raw.calib", "raw.image"],
        )


def test_crystfel_requires_sources(make_params: ParamsFactory) -> None:
    """The producer refuses these at startup; reject them as a 422 instead."""
    with pytest.raises(ValidationError, match="photon_wavelength"):
        make_params(processing_pipeline={"type": "CrystfelPreprocessingPipeline"})
