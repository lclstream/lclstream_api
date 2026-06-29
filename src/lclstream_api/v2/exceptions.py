from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse


class NotFound(Exception):
    pass


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(NotFound)
    async def _handle_not_found(_request: Request, exc: NotFound) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content={"detail": str(exc) or "Not found"},
        )
