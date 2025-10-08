import asyncio
import pytest

from lclstream_api.cache import cache_process

@pytest.mark.asyncio()
async def test_async():
    await asyncio.sleep(0.001)
