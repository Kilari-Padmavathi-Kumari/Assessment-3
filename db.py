import logging

from psycopg_pool import AsyncConnectionPool

from config import DATABASE_URL, DB_POOL_MAX_SIZE, DB_POOL_MIN_SIZE, DB_POOL_TIMEOUT

pool = AsyncConnectionPool(
    conninfo=DATABASE_URL,
    open=False,
    min_size=DB_POOL_MIN_SIZE,
    max_size=DB_POOL_MAX_SIZE,
    timeout=DB_POOL_TIMEOUT,
)
logger = logging.getLogger("wallet.db")

SCHEMA_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS users (
        user_id TEXT PRIMARY KEY,
        password_hash TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """,
    """
    ALTER TABLE users
    ADD COLUMN IF NOT EXISTS password_hash TEXT NOT NULL DEFAULT '';
    """,
    """
    CREATE TABLE IF NOT EXISTS wallets (
        id BIGSERIAL PRIMARY KEY,
        user_id TEXT UNIQUE NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
        balance NUMERIC(18,2) NOT NULL DEFAULT 0 CHECK (balance >= 0),
        version INTEGER NOT NULL DEFAULT 0,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """,
    """
    ALTER TABLE wallets
    ADD COLUMN IF NOT EXISTS version INTEGER NOT NULL DEFAULT 0;
    """,
    """
    CREATE TABLE IF NOT EXISTS ledger_entries (
        id BIGSERIAL PRIMARY KEY,
        wallet_id BIGINT NOT NULL REFERENCES wallets(id) ON DELETE CASCADE,
        entry_type TEXT NOT NULL CHECK (entry_type IN ('credit', 'debit')),
        amount NUMERIC(18,2) NOT NULL CHECK (amount > 0),
        balance_after NUMERIC(18,2) NOT NULL CHECK (balance_after >= 0),
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_ledger_wallet_created_at
    ON ledger_entries(wallet_id, created_at DESC);
    """,
]


async def init_db() -> None:
    """
    Create tables and index needed for wallet operations.
    Safe to run multiple times (idempotent).
    """
    logger.info("db_init_started")
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            for index, statement in enumerate(SCHEMA_STATEMENTS, start=1):
                logger.debug("db_init_statement_%s", index)
                await cur.execute(statement)
        await conn.commit()
    logger.info("db_init_completed")


async def db_healthcheck() -> bool:
    """Return True if DB can be queried, else False."""
    try:
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT 1;")
                await cur.fetchone()
        return True
    except Exception:
        logger.exception("db_healthcheck_failed")
        return False
