"""Async database access.

The app owns the engine; the DBOS datasource borrows it, so both share
one connection pool.
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

from .config import get_database

engine = create_async_engine(str(get_database().url))
async_session = async_sessionmaker(engine, expire_on_commit=False)

_datasource: AsyncSQLAlchemyDatasource | None = None


async def init_datasource() -> AsyncSQLAlchemyDatasource:
    global _datasource
    ds = await AsyncSQLAlchemyDatasource.create(str(get_database().url), engine=engine)
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

    The datasource exists only after startup, so look it up per call.
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
