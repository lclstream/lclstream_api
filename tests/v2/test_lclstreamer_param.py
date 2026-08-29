"""Guards the vendored lclstreamer parameter model against drift.

``_CustomBaseModel`` forbids extra fields, so an unvendored variant
becomes a 422. These pin what upstream has today.
"""

import pytest
from pydantic import TypeAdapter

from lclstream_api.lclstreamer_param import (
    BinaryDataStreamingDataHandlerParameters,
    DataSerializerParameters,
    ProcessingPipelineParameters,
    SimplonBinarySerializerParameters,
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


def test_simplon_optional_sources() -> None:
    serializer = SimplonBinarySerializerParameters(
        type="SimplonBinarySerializer",
        data_source_to_serialize="detector_data",
        polarization_fraction=1.0,
        polarization_axis=[1.0, 0.0, 0.0],
        data_collection_rate="120 Hz",
        detector_name="jungfrau",
        detector_type="Jungfrau4M",
        photon_wavelength_source="SIOC:SYS0:ML00:AO192",
        spectrometer_source="feespec.raw.hproj",
    )
    assert serializer.photon_wavelength_source == "SIOC:SYS0:ML00:AO192"
    assert serializer.spectrometer_source == "feespec.raw.hproj"


def test_simplon_sources_none() -> None:
    """Both are optional upstream, so omitting them stays valid."""
    serializer = SimplonBinarySerializerParameters(
        type="SimplonBinarySerializer",
        data_source_to_serialize="detector_data",
        polarization_fraction=1.0,
        polarization_axis=[1.0, 0.0, 0.0],
        data_collection_rate="120 Hz",
        detector_name="jungfrau",
        detector_type="Jungfrau4M",
    )
    assert serializer.photon_wavelength_source is None
    assert serializer.spectrometer_source is None


def test_handler_accepts_linger() -> None:
    handler = BinaryDataStreamingDataHandlerParameters(
        type="BinaryDataStreamingDataHandler",
        urls=["tcp://cache:5000"],
        linger=-1,
    )
    assert handler.linger == -1
