"""Guards the vendored lclstreamer parameter model against drift.

``_CustomBaseModel`` forbids extra fields, so an unvendored variant
becomes a 422. These pin what upstream has today.
"""

import pytest
from pydantic import TypeAdapter, ValidationError

from lclstream_api.lclstreamer_param import (
    BinaryDataStreamingDataHandlerParameters,
    DataSerializerParameters,
    ProcessingPipelineParameters,
)

_PIPELINES = TypeAdapter(ProcessingPipelineParameters)
_SERIALIZERS = TypeAdapter(DataSerializerParameters)


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
