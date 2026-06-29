import os

# Must run before importing lclstream_api.v2.config (built eagerly at import).
_DUMMY_ENV = {
    "LCLSTREAM_FASTCACHE_TOKEN_FILE": "/dev/null",
    "LCLSTREAM_FASTCACHE_CLIENT_CERT": "/dev/null",
    "LCLSTREAM_FASTCACHE_CLIENT_KEY": "/dev/null",
    "LCLSTREAM_IRI_S3DF_TOKEN_FILE": "/dev/null",
}
for _key, _value in _DUMMY_ENV.items():
    os.environ.setdefault(_key, _value)

from collections.abc import Callable

import pytest

from lclstream_api.lclstreamer_param import Parameters
from lclstream_api.v2.config import LCLStreamerProducerSettings

ParamsFactory = Callable[..., Parameters]
SettingsFactory = Callable[..., LCLStreamerProducerSettings]

# A minimal but valid lclstreamer config. ``InternalEventSource`` maps to the
# psana2 environment (see ``core.producer._PSANA_ENV``).
_BASE_PARAMS: dict = {
    "source_identifier": "exp=mfxl1001,run=42",
    "skip_incomplete_events": False,
    "event_source": {
        "type": "InternalEventSource",
        "number_of_events_to_generate": 100,
    },
    "data_sources": {
        "random": {
            "type": "GenericRandomNumpyArray",
            "array_shape": [20, 2],
            "array_dtype": "float32",
            "always_random": True,
        },
    },
    "processing_pipeline": {"type": "BatchProcessingPipeline", "batch_size": 10},
    "data_serializer": {
        "type": "HDF5BinarySerializer",
        "compression_level": 3,
        "fields": {"random": "random"},
    },
    "data_handlers": [
        {
            "type": "BinaryDataStreamingDataHandler",
            "urls": ["tcp://127.0.0.1:5000"],
        }
    ],
}


@pytest.fixture
def make_params() -> ParamsFactory:
    """Factory for a valid :class:`Parameters`, shallow-overriding top-level keys."""

    def _make(**overrides: object) -> Parameters:
        return Parameters.model_validate({**_BASE_PARAMS, **overrides})

    return _make


@pytest.fixture
def make_producer_settings() -> SettingsFactory:
    """Factory for producer settings without reading the process environment."""

    def _make(
        *,
        data_base_dir: str = "/sdf/data/lcls/ds",
        environments: dict[str, dict[str, str]] | None = None,
    ) -> LCLStreamerProducerSettings:
        return LCLStreamerProducerSettings(
            data_base_dir=data_base_dir,
            environments=environments or {},
        )

    return _make
