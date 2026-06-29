from . import fastcache, iri


def startup() -> None:
    fastcache.startup()
    iri.startup()


async def shutdown() -> None:
    await fastcache.shutdown()
    await iri.shutdown()


__all__ = ["fastcache", "iri", "shutdown", "startup"]
