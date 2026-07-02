"""Async application-database access.

The application OWNS the async engine (created eagerly at import from
``config.database.url``); the DBOS datasource borrows that same engine in
``init_datasource`` so both access paths share one connection pool:

* Durable workflows annotate their transaction bodies with ``@transaction`` so
  each runs exactly once on replay (recorded inside a workflow, a plain
  transaction otherwise). ``sql_session()`` is the session inside such a
  transaction.
* HTTP requests (routers -> service -> repo) use request-scoped sessions from
  ``get_session``; the service layer owns the ``session.begin()`` boundaries.
"""

from collections.abc import AsyncIterator, Callable, Coroutine
from functools import wraps
from typing import Any, Literal

from dbos import AsyncSQLAlchemyDatasource
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from .config import database
from .tables import Base

engine = create_async_engine(str(database.url))
async_session = async_sessionmaker(engine, expire_on_commit=False)

_datasource: AsyncSQLAlchemyDatasource | None = None


async def init_datasource() -> AsyncSQLAlchemyDatasource:
    global _datasource
    ds = await AsyncSQLAlchemyDatasource.create(str(database.url), engine=engine)
    # create tables
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    _datasource = ds
    return ds


def datasource() -> AsyncSQLAlchemyDatasource:
    if _datasource is None:
        raise RuntimeError(
            "datasource not configured; call init_datasource() at startup"
        )
    return _datasource


def sql_session() -> AsyncSession:
    return datasource().sql_session()


IsolationLevel = Literal["SERIALIZABLE", "REPEATABLE READ", "READ COMMITTED"]


def transaction[**P, R](
    *,
    name: str | None = None,
    isolation_level: IsolationLevel = "SERIALIZABLE",
) -> Callable[
    [Callable[P, Coroutine[Any, Any, R]]], Callable[P, Coroutine[Any, Any, R]]
]:
    """Run an async function as a datasource transaction step.

    We need this because the DBOS datasource is built on top of the app's async engine, so
    the app owns the engine and the datasource is not available until after startup.
    """

    def decorator(
        f: Callable[P, Coroutine[Any, Any, R]],
    ) -> Callable[P, Coroutine[Any, Any, R]]:
        options = {
            "name": name or f.__qualname__,
            "isolation_level": isolation_level,
        }

        @wraps(f)
        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            return await datasource().run_tx_step_async(options, f, *args, **kwargs)

        return wrapper

    return decorator


async def get_session() -> AsyncIterator[AsyncSession]:
    async with async_session() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
