import logging
from collections.abc import AsyncIterator

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from config import DATABASE_URL, DB_POOL_MAX_SIZE, DB_POOL_TIMEOUT
from models import Base

logger = logging.getLogger("wallet.db")


def _ensure_async_db_url(url: str) -> str:
    if "+asyncpg" in url:
        return url
    if url.startswith("postgresql://"):
        return url.replace("postgresql://", "postgresql+asyncpg://", 1)
    return url


ASYNC_DATABASE_URL = _ensure_async_db_url(DATABASE_URL)

engine = create_async_engine(
    ASYNC_DATABASE_URL,
    pool_size=DB_POOL_MAX_SIZE,
    max_overflow=0,
    pool_timeout=DB_POOL_TIMEOUT,
    pool_pre_ping=True,
)
SessionLocal = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


async def get_session() -> AsyncIterator[AsyncSession]:
    async with SessionLocal() as session:
        yield session


async def init_db() -> None:
    """
    Create tables and index needed for wallet operations.
    Safe to run multiple times (idempotent).
    """
    logger.info("db_init_started")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("db_init_completed")


async def db_healthcheck() -> bool:
    """Return True if DB can be queried, else False."""
    try:
        async with SessionLocal() as session:
            await session.execute(text("SELECT 1;"))
        return True
    except Exception:
        logger.exception("db_healthcheck_failed")
        return False


async def close_db() -> None:
    await engine.dispose()
