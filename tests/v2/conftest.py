from collections.abc import Callable, Iterator
from contextlib import asynccontextmanager
from typing import cast

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from lclstream_api.lclstreamer_param import Parameters
from lclstream_api.v2 import config
from lclstream_api.v2.config import LCLStreamerProducerSettings

ParamsFactory = Callable[..., Parameters]
SettingsFactory = Callable[..., LCLStreamerProducerSettings]

_SETTINGS_GETTERS = (
    config.get_database,
    config.get_credentials,
    config.get_dbos,
    config.get_fastcache,
    config.get_iri,
    config.get_producer,
    config.get_oidc,
    config.get_app,
)


@pytest.fixture(autouse=True)
def _clear_settings_cache() -> Iterator[None]:
    """Getters are ``lru_cache``d; clear them so env overrides never leak
    between tests."""
    for getter in _SETTINGS_GETTERS:
        getter.cache_clear()
    yield
    for getter in _SETTINGS_GETTERS:
        getter.cache_clear()


# ``InternalEventSource`` maps to the psana2 environment.
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
        home_base_dir: str = "/sdf/home",
        environments: dict[str, dict[str, str]] | None = None,
    ) -> LCLStreamerProducerSettings:
        return LCLStreamerProducerSettings(
            data_base_dir=data_base_dir,
            home_base_dir=home_base_dir,
            environments=environments or {},
        )

    return _make


class _FakeSession:
    @asynccontextmanager
    async def begin(self):
        yield self


@pytest.fixture
def fake_session() -> AsyncSession:
    """Stub session for tests that never touch the database."""
    return cast(AsyncSession, _FakeSession())
