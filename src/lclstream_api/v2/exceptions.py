from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel


class NotFound(Exception):
    pass


class UpstreamError(Exception):
    """An upstream dependency failed unexpectedly."""


class DelegatedCredentialRejected(Exception):
    """IRI rejected a request-bound delegated credential."""


class CacheShutdownBlocked(Exception):
    """Other active transfers still use the cache."""

    def __init__(self, active_transfer_count: int) -> None:
        self.active_transfer_count = active_transfer_count
        super().__init__(
            f"{active_transfer_count} other active transfer(s) still use this cache"
        )


class CacheShutdownConflict(BaseModel):
    message: str
    active_transfer_count: int


class InsufficientTokenLifetime(Exception):
    """The caller's token cannot cover the requested producer lifecycle."""

    def __init__(self, *, required_seconds: int, remaining_seconds: int) -> None:
        self.required_seconds = required_seconds
        self.remaining_seconds = remaining_seconds
        super().__init__(
            "token lifetime is too short for this transfer "
            f"(required={required_seconds}s, remaining={remaining_seconds}s)"
        )


class InsufficientTokenLifetimeError(BaseModel):
    detail: str
    required_seconds: int
    remaining_seconds: int


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

    @app.exception_handler(DelegatedCredentialRejected)
    async def _handle_delegated_credential_rejected(
        _request: Request, exc: DelegatedCredentialRejected
    ) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_401_UNAUTHORIZED,
            content={"detail": str(exc) or "IRI rejected delegated credential"},
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

    @app.exception_handler(InsufficientTokenLifetime)
    async def _handle_insufficient_token_lifetime(
        _request: Request, exc: InsufficientTokenLifetime
    ) -> JSONResponse:
        body = InsufficientTokenLifetimeError(
            detail=str(exc),
            required_seconds=exc.required_seconds,
            remaining_seconds=exc.remaining_seconds,
        )
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            content=body.model_dump(mode="json"),
        )
