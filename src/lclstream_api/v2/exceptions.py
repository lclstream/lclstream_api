from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse

from .models import CacheShutdownConflict


class NotFound(Exception):
    pass


class UpstreamError(Exception):
    """An upstream dependency (e.g. the IRI filesystem) failed unexpectedly."""


class CacheShutdownBlocked(Exception):
    """Other active transfers still use the cache."""

    def __init__(self, active_transfer_count: int) -> None:
        self.active_transfer_count = active_transfer_count
        super().__init__(
            f"{active_transfer_count} other active transfer(s) still use this cache"
        )


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(NotFound)
    async def _handle_not_found(_request: Request, exc: NotFound) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content={"detail": str(exc) or "Not found"},
        )

    @app.exception_handler(UpstreamError)
    async def _handle_upstream(_request: Request, exc: UpstreamError) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_502_BAD_GATEWAY,
            content={"detail": str(exc) or "Upstream dependency failed"},
        )

    @app.exception_handler(CacheShutdownBlocked)
    async def _handle_cache_shutdown_blocked(
        _request: Request, exc: CacheShutdownBlocked
    ) -> JSONResponse:
        body = CacheShutdownConflict(
            message=str(exc), active_transfer_count=exc.active_transfer_count
        )
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content=body.model_dump(mode="json"),
        )
