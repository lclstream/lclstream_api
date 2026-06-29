from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse


class NotFound(Exception):
    pass


class UpstreamError(Exception):
    """An upstream dependency (e.g. the IRI filesystem) failed unexpectedly."""


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
