from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from dbos import DBOS, DBOSConfig
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from . import auth, config, db, workflows
from .clients import shutdown as clients_shutdown, startup as clients_startup
from .exceptions import register_exception_handlers
from .routers.v1.cache import router as cache_router
from .routers.v1.transfer import router as transfer_router


def build_dbos_config() -> DBOSConfig:
    system_database_url = str(config.get_database().url).replace("+psycopg", "")
    cfg = config.get_dbos()
    return DBOSConfig(
        name=cfg.name,
        system_database_url=system_database_url,
        dbos_system_schema=cfg.system_schema,
    )


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
    auth.validate_configuration()
    DBOS(config=build_dbos_config())
    await db.init_datasource()
    clients_startup()
    DBOS.launch()
    workflows.register_schedules()
    try:
        yield
    finally:
        await clients_shutdown()
        DBOS.destroy()


app = FastAPI(
    title="LCLStream API",
    summary="Durable lclstreamer-based data transfers.",
    lifespan=lifespan,
    root_path=config.get_app().root_path,
)

register_exception_handlers(app)
app.include_router(transfer_router)
app.include_router(cache_router)

# Opt-in only; production runs same-origin behind a gateway.
if cors_origins := config.get_app().cors_origins:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=cors_origins,
        allow_credentials=True,
        allow_methods=["GET", "POST", "DELETE"],
        allow_headers=["Authorization", "Content-Type"],
    )
