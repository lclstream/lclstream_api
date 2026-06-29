from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from dbos import DBOS, DBOSConfig
from fastapi import FastAPI

from . import config, db, workflows
from .exceptions import register_exception_handlers
from .routers.v1.transfer import router as transfer_router


def build_dbos_config() -> DBOSConfig:
    system_database_url = str(config.database.url).replace("+psycopg", "")
    return DBOSConfig(
        name=config.dbos.name,
        system_database_url=system_database_url,
        dbos_system_schema=config.dbos.system_schema,
    )


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
    DBOS(config=build_dbos_config())
    await db.init_datasource()
    workflows.startup()
    DBOS.launch()
    workflows.register_schedules()
    try:
        yield
    finally:
        await workflows.shutdown()
        DBOS.destroy()


app = FastAPI(
    title="LCLStream API",
    summary="Durable lclstreamer-based data transfers.",
    lifespan=lifespan,
    root_path=config.app.root_path,
)

register_exception_handlers(app)
app.include_router(transfer_router)
