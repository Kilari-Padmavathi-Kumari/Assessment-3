import logging

import psycopg
from fastapi import APIRouter, Depends, HTTPException, Query
from psycopg.rows import dict_row

from auth import get_current_user_id, hash_password
from db import pool
from schemas import (
    CreateUserRequest,
    CreateWalletRequest,
    LedgerEntryResponse,
    MoneyRequest,
    UserResponse,
    WalletBalanceResponse,
    WalletMutationResponse,
)

logger = logging.getLogger("wallet.routes")
router = APIRouter()
MAX_RETRIES = 5


async def _ensure_user_exists(cur, user_id: str) -> bool:
    """Check whether user exists in users table."""
    await cur.execute("SELECT 1 FROM users WHERE user_id = %s;", (user_id,))
    exists = await cur.fetchone() is not None
    logger.debug("user_exists_check user_id=%s exists=%s", user_id, exists)
    return exists


def _authorize_owner(token_user_id: str, requested_user_id: str) -> None:
    """Allow wallet operation only for token owner."""
    if token_user_id != requested_user_id:
        logger.warning(
            "wallet_forbidden token_user_id=%s requested_user_id=%s",
            token_user_id,
            requested_user_id,
        )
        raise HTTPException(status_code=403, detail="forbidden")


@router.post("/users", response_model=UserResponse, status_code=201, tags=["users"])
async def create_user(payload: CreateUserRequest) -> UserResponse:
    # Create a user profile row.
    user_id_str = payload.user_id
    logger.info("create_user_requested user_id=%s", user_id_str)
    try:
        async with pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    """
                    INSERT INTO users (user_id, password_hash)
                    VALUES (%s, %s)
                    ON CONFLICT (user_id) DO NOTHING
                    RETURNING user_id, created_at;
                    """,
                    (user_id_str, hash_password(payload.password)),
                )
                row = await cur.fetchone()
                await conn.commit()
    except psycopg.Error as exc:
        logger.exception("create_user_db_error user_id=%s error=%s", user_id_str, exc)
        raise HTTPException(status_code=500, detail="database error") from exc

    if row is None:
        logger.warning("create_user_conflict user_id=%s", user_id_str)
        raise HTTPException(status_code=409, detail="user already exists")

    logger.info("create_user_success user_id=%s", user_id_str)
    return UserResponse(**row)


@router.get("/users", response_model=list[UserResponse], tags=["users"])
async def list_users(
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> list[UserResponse]:
    # List users with pagination.
    try:
        async with pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    """
                    SELECT user_id, created_at
                    FROM users
                    ORDER BY created_at DESC, user_id DESC
                    LIMIT %s OFFSET %s;
                    """,
                    (limit, offset),
                )
                rows = await cur.fetchall()
    except psycopg.Error as exc:
        logger.exception("list_users_db_error limit=%s offset=%s error=%s", limit, offset, exc)
        raise HTTPException(status_code=500, detail="database error") from exc

    return [UserResponse(**row) for row in rows]


@router.get("/users/{user_id}", response_model=UserResponse, tags=["users"])
async def get_user(user_id: str) -> UserResponse:
    # Get one user by id.
    try:
        async with pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    "SELECT user_id, created_at FROM users WHERE user_id = %s;",
                    (user_id,),
                )
                row = await cur.fetchone()
    except psycopg.Error as exc:
        logger.exception("get_user_db_error user_id=%s error=%s", user_id, exc)
        raise HTTPException(status_code=500, detail="database error") from exc

    if row is None:
        raise HTTPException(status_code=404, detail="user not found")

    return UserResponse(**row)


@router.post("/wallets", response_model=WalletBalanceResponse, status_code=201, tags=["wallet"])
async def create_wallet(
    payload: CreateWalletRequest,
    token_user_id: str = Depends(get_current_user_id),
) -> WalletBalanceResponse:
    # Create wallet for an existing user.
    user_id_str = str(payload.user_id)
    _authorize_owner(token_user_id, user_id_str)
    logger.info("create_wallet_requested user_id=%s", user_id_str)
    try:
        async with pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                if not await _ensure_user_exists(cur, user_id_str):
                    await conn.rollback()
                    logger.warning("create_wallet_user_not_found user_id=%s", user_id_str)
                    raise HTTPException(status_code=404, detail="user not found")

                await cur.execute(
                    """
                    INSERT INTO wallets (user_id, balance)
                    VALUES (%s, 0)
                    ON CONFLICT (user_id) DO NOTHING
                    RETURNING user_id, balance, created_at;
                    """,
                    (user_id_str,),
                )
                row = await cur.fetchone()
                await conn.commit()
    except psycopg.Error as exc:
        logger.exception("create_wallet_db_error user_id=%s error=%s", user_id_str, exc)
        raise HTTPException(status_code=500, detail="database error") from exc

    if row is None:
        logger.warning("create_wallet_conflict user_id=%s", user_id_str)
        raise HTTPException(status_code=409, detail="wallet already exists")

    logger.info("create_wallet_success user_id=%s", user_id_str)
    return WalletBalanceResponse(**row)


@router.post("/wallets/{user_id}/credit", response_model=WalletMutationResponse, tags=["wallet"])
async def credit_wallet(
    user_id: str,
    payload: MoneyRequest,
    token_user_id: str = Depends(get_current_user_id),
) -> WalletMutationResponse:
    # Credit flow with optimistic concurrency control (OCC):
    # 1) read wallet + version
    # 2) update balance if version matches
    # 3) insert ledger entry in same transaction
    user_id_str = user_id
    _authorize_owner(token_user_id, user_id_str)
    logger.info("credit_requested user_id=%s amount=%s", user_id_str, payload.amount)
    try:
        for attempt in range(1, MAX_RETRIES + 1):
            async with pool.connection() as conn:
                async with conn.transaction():
                    async with conn.cursor(row_factory=dict_row) as cur:
                        await cur.execute(
                            "SELECT id, balance, version FROM wallets WHERE user_id = %s;",
                            (user_id_str,),
                        )
                        wallet_row = await cur.fetchone()

                        if wallet_row is None:
                            logger.warning("credit_wallet_not_found user_id=%s", user_id_str)
                            raise HTTPException(status_code=404, detail="wallet not found")

                        await cur.execute(
                            """
                            UPDATE wallets
                            SET balance = balance + %s,
                                version = version + 1
                            WHERE id = %s AND version = %s
                            RETURNING balance;
                            """,
                            (payload.amount, wallet_row["id"], wallet_row["version"]),
                        )
                        updated_wallet = await cur.fetchone()

                        if updated_wallet is None:
                            logger.info(
                                "credit_retry user_id=%s attempt=%s", user_id_str, attempt
                            )
                            continue

                        await cur.execute(
                            """
                            INSERT INTO ledger_entries (wallet_id, entry_type, amount, balance_after)
                            VALUES (%s, 'credit', %s, %s)
                            RETURNING id;
                            """,
                            (wallet_row["id"], payload.amount, updated_wallet["balance"]),
                        )
                        ledger_row = await cur.fetchone()
                        transaction_id = ledger_row["id"]
                        balance = updated_wallet["balance"]
                        logger.debug(
                            "credit_debug user_id=%s wallet_id=%s transaction_id=%s balance=%s",
                            user_id_str,
                            wallet_row["id"],
                            transaction_id,
                            balance,
                        )
                        return WalletMutationResponse(
                            user_id=user_id_str,
                            balance=balance,
                            transaction_id=transaction_id,
                        )
    except psycopg.Error as exc:
        logger.exception(
            "credit_wallet_db_error user_id=%s amount=%s error=%s", user_id_str, payload.amount, exc
        )
        raise HTTPException(status_code=500, detail="database error") from exc

    logger.warning("credit_conflict user_id=%s", user_id_str)
    raise HTTPException(status_code=409, detail="conflict, please retry")


@router.post("/wallets/{user_id}/debit", response_model=WalletMutationResponse, tags=["wallet"])
async def debit_wallet(
    user_id: str,
    payload: MoneyRequest,
    token_user_id: str = Depends(get_current_user_id),
) -> WalletMutationResponse:
    # Debit flow with optimistic concurrency control (OCC):
    # 1) read wallet + version
    # 2) if balance allows, update balance if version matches
    # 3) insert ledger entry in same transaction
    user_id_str = user_id
    _authorize_owner(token_user_id, user_id_str)
    logger.info("debit_requested user_id=%s amount=%s", user_id_str, payload.amount)
    try:
        for attempt in range(1, MAX_RETRIES + 1):
            async with pool.connection() as conn:
                async with conn.transaction():
                    async with conn.cursor(row_factory=dict_row) as cur:
                        await cur.execute(
                            "SELECT id, balance, version FROM wallets WHERE user_id = %s;",
                            (user_id_str,),
                        )
                        wallet_row = await cur.fetchone()

                        if wallet_row is None:
                            logger.warning("debit_wallet_not_found user_id=%s", user_id_str)
                            raise HTTPException(status_code=404, detail="wallet not found")

                        if wallet_row["balance"] < payload.amount:
                            logger.warning(
                                "debit_insufficient_funds user_id=%s amount=%s",
                                user_id_str,
                                payload.amount,
                            )
                            raise HTTPException(status_code=400, detail="insufficient funds")

                        new_balance = wallet_row["balance"] - payload.amount
                        await cur.execute(
                            """
                            UPDATE wallets
                            SET balance = %s,
                                version = version + 1
                            WHERE id = %s AND version = %s
                            RETURNING balance;
                            """,
                            (new_balance, wallet_row["id"], wallet_row["version"]),
                        )
                        updated_wallet = await cur.fetchone()

                        if updated_wallet is None:
                            logger.info(
                                "debit_retry user_id=%s attempt=%s", user_id_str, attempt
                            )
                            continue

                        await cur.execute(
                            """
                            INSERT INTO ledger_entries (wallet_id, entry_type, amount, balance_after)
                            VALUES (%s, 'debit', %s, %s)
                            RETURNING id;
                            """,
                            (wallet_row["id"], payload.amount, updated_wallet["balance"]),
                        )
                        ledger_row = await cur.fetchone()
                        transaction_id = ledger_row["id"]
                        balance = updated_wallet["balance"]
                        logger.debug(
                            "debit_debug user_id=%s wallet_id=%s transaction_id=%s balance=%s",
                            user_id_str,
                            wallet_row["id"],
                            transaction_id,
                            balance,
                        )
                        return WalletMutationResponse(
                            user_id=user_id_str,
                            balance=balance,
                            transaction_id=transaction_id,
                        )
    except psycopg.Error as exc:
        logger.exception(
            "debit_wallet_db_error user_id=%s amount=%s error=%s", user_id_str, payload.amount, exc
        )
        raise HTTPException(status_code=500, detail="database error") from exc

    logger.warning("debit_conflict user_id=%s", user_id_str)
    raise HTTPException(status_code=409, detail="conflict, please retry")

@router.get("/wallets/{user_id}/balance", response_model=WalletBalanceResponse, tags=["wallet"])
async def get_wallet_balance(
    user_id: str,
    token_user_id: str = Depends(get_current_user_id),
) -> WalletBalanceResponse:
    # Read current wallet balance.
    user_id_str = user_id
    _authorize_owner(token_user_id, user_id_str)
    logger.info("balance_requested user_id=%s", user_id_str)
    try:
        async with pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    "SELECT user_id, balance, created_at FROM wallets WHERE user_id = %s;",
                    (user_id_str,),
                )
                row = await cur.fetchone()
    except psycopg.Error as exc:
        logger.exception("balance_db_error user_id=%s error=%s", user_id_str, exc)
        raise HTTPException(status_code=500, detail="database error") from exc

    if row is None:
        logger.warning("balance_wallet_not_found user_id=%s", user_id_str)
        raise HTTPException(status_code=404, detail="wallet not found")

    return WalletBalanceResponse(**row)


@router.get("/wallets/{user_id}/ledger", response_model=list[LedgerEntryResponse], tags=["wallet"])
async def get_wallet_ledger(
    user_id: str,
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    token_user_id: str = Depends(get_current_user_id),
) -> list[LedgerEntryResponse]:
    # Read transaction history for one wallet.
    user_id_str = user_id
    _authorize_owner(token_user_id, user_id_str)
    logger.info("ledger_requested user_id=%s limit=%s offset=%s", user_id_str, limit, offset)
    try:
        async with pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute("SELECT id FROM wallets WHERE user_id = %s", (user_id_str,))
                wallet = await cur.fetchone()
                if wallet is None:
                    logger.warning("ledger_wallet_not_found user_id=%s", user_id_str)
                    raise HTTPException(status_code=404, detail="wallet not found")

                await cur.execute(
                    """
                    SELECT id, entry_type, amount, balance_after, created_at
                    FROM ledger_entries
                    WHERE wallet_id = %s
                    ORDER BY created_at DESC, id DESC
                    LIMIT %s OFFSET %s;
                    """,
                    (wallet["id"], limit, offset),
                )
                rows = await cur.fetchall()
    except psycopg.Error as exc:
        logger.exception(
            "ledger_db_error user_id=%s limit=%s offset=%s error=%s",
            user_id_str,
            limit,
            offset,
            exc,
        )
        raise HTTPException(status_code=500, detail="database error") from exc

    return [LedgerEntryResponse(**row) for row in rows]
