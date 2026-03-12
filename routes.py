import logging

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from auth import get_current_user_id, hash_password
from db import get_session
from models import LedgerEntry, User, Wallet
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


async def _ensure_user_exists(session: AsyncSession, user_id: str) -> bool:
    """Check whether user exists in users table."""
    result = await session.execute(select(User.user_id).where(User.user_id == user_id))
    exists = result.first() is not None
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
async def create_user(
    payload: CreateUserRequest,
    session: AsyncSession = Depends(get_session),
) -> UserResponse:
    # Create a user profile row.
    user_id_str = payload.user_id
    logger.info("create_user_requested user_id=%s", user_id_str)
    try:
        stmt = (
            insert(User)
            .values(user_id=user_id_str, password_hash=hash_password(payload.password))
            .on_conflict_do_nothing(index_elements=[User.user_id])
            .returning(User.user_id, User.created_at)
        )
        async with session.begin():
            result = await session.execute(stmt)
            row = result.first()
    except SQLAlchemyError as exc:
        logger.exception("create_user_db_error user_id=%s error=%s", user_id_str, exc)
        raise HTTPException(status_code=500, detail="database error") from exc

    if row is None:
        logger.warning("create_user_conflict user_id=%s", user_id_str)
        raise HTTPException(status_code=409, detail="user already exists")

    logger.info("create_user_success user_id=%s", user_id_str)
    return UserResponse(user_id=row.user_id, created_at=row.created_at)


@router.get("/users", response_model=list[UserResponse], tags=["users"])
async def list_users(
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    session: AsyncSession = Depends(get_session),
) -> list[UserResponse]:
    # List users with pagination.
    try:
        result = await session.execute(
            select(User)
            .order_by(User.created_at.desc(), User.user_id.desc())
            .limit(limit)
            .offset(offset)
        )
        users = result.scalars().all()
    except SQLAlchemyError as exc:
        logger.exception("list_users_db_error limit=%s offset=%s error=%s", limit, offset, exc)
        raise HTTPException(status_code=500, detail="database error") from exc

    return [UserResponse(user_id=user.user_id, created_at=user.created_at) for user in users]


@router.get("/users/{user_id}", response_model=UserResponse, tags=["users"])
async def get_user(
    user_id: str,
    session: AsyncSession = Depends(get_session),
) -> UserResponse:
    # Get one user by id.
    try:
        result = await session.execute(select(User).where(User.user_id == user_id))
        user = result.scalar_one_or_none()
    except SQLAlchemyError as exc:
        logger.exception("get_user_db_error user_id=%s error=%s", user_id, exc)
        raise HTTPException(status_code=500, detail="database error") from exc

    if user is None:
        raise HTTPException(status_code=404, detail="user not found")

    return UserResponse(user_id=user.user_id, created_at=user.created_at)


@router.post("/wallets", response_model=WalletBalanceResponse, status_code=201, tags=["wallet"])
async def create_wallet(
    payload: CreateWalletRequest,
    token_user_id: str = Depends(get_current_user_id),
    session: AsyncSession = Depends(get_session),
) -> WalletBalanceResponse:
    # Create wallet for an existing user.
    user_id_str = str(payload.user_id)
    _authorize_owner(token_user_id, user_id_str)
    logger.info("create_wallet_requested user_id=%s", user_id_str)
    try:
        async with session.begin():
            if not await _ensure_user_exists(session, user_id_str):
                logger.warning("create_wallet_user_not_found user_id=%s", user_id_str)
                raise HTTPException(status_code=404, detail="user not found")

            stmt = (
                insert(Wallet)
                .values(user_id=user_id_str, balance=0)
                .on_conflict_do_nothing(index_elements=[Wallet.user_id])
                .returning(Wallet.user_id, Wallet.balance, Wallet.created_at)
            )
            result = await session.execute(stmt)
            row = result.first()
    except SQLAlchemyError as exc:
        logger.exception("create_wallet_db_error user_id=%s error=%s", user_id_str, exc)
        raise HTTPException(status_code=500, detail="database error") from exc

    if row is None:
        logger.warning("create_wallet_conflict user_id=%s", user_id_str)
        raise HTTPException(status_code=409, detail="wallet already exists")

    logger.info("create_wallet_success user_id=%s", user_id_str)
    return WalletBalanceResponse(user_id=row.user_id, balance=row.balance, created_at=row.created_at)


@router.post("/wallets/{user_id}/credit", response_model=WalletMutationResponse, tags=["wallet"])
async def credit_wallet(
    user_id: str,
    payload: MoneyRequest,
    token_user_id: str = Depends(get_current_user_id),
    session: AsyncSession = Depends(get_session),
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
            async with session.begin():
                result = await session.execute(
                    select(Wallet.id, Wallet.balance, Wallet.version).where(
                        Wallet.user_id == user_id_str
                    )
                )
                wallet_row = result.first()

                if wallet_row is None:
                    logger.warning("credit_wallet_not_found user_id=%s", user_id_str)
                    raise HTTPException(status_code=404, detail="wallet not found")

                update_stmt = (
                    update(Wallet)
                    .where(Wallet.id == wallet_row.id, Wallet.version == wallet_row.version)
                    .values(balance=Wallet.balance + payload.amount, version=Wallet.version + 1)
                    .returning(Wallet.balance)
                )
                updated_result = await session.execute(update_stmt)
                updated_wallet = updated_result.first()

                if updated_wallet is None:
                    logger.info("credit_retry user_id=%s attempt=%s", user_id_str, attempt)
                    continue

                ledger = LedgerEntry(
                    wallet_id=wallet_row.id,
                    entry_type="credit",
                    amount=payload.amount,
                    balance_after=updated_wallet.balance,
                )
                session.add(ledger)
                await session.flush()

                transaction_id = ledger.id
                balance = updated_wallet.balance
                logger.debug(
                    "credit_debug user_id=%s wallet_id=%s transaction_id=%s balance=%s",
                    user_id_str,
                    wallet_row.id,
                    transaction_id,
                    balance,
                )
                return WalletMutationResponse(
                    user_id=user_id_str,
                    balance=balance,
                    transaction_id=transaction_id,
                )
    except SQLAlchemyError as exc:
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
    session: AsyncSession = Depends(get_session),
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
            async with session.begin():
                result = await session.execute(
                    select(Wallet.id, Wallet.balance, Wallet.version).where(
                        Wallet.user_id == user_id_str
                    )
                )
                wallet_row = result.first()

                if wallet_row is None:
                    logger.warning("debit_wallet_not_found user_id=%s", user_id_str)
                    raise HTTPException(status_code=404, detail="wallet not found")

                if wallet_row.balance < payload.amount:
                    logger.warning(
                        "debit_insufficient_funds user_id=%s amount=%s",
                        user_id_str,
                        payload.amount,
                    )
                    raise HTTPException(status_code=400, detail="insufficient funds")

                new_balance = wallet_row.balance - payload.amount
                update_stmt = (
                    update(Wallet)
                    .where(Wallet.id == wallet_row.id, Wallet.version == wallet_row.version)
                    .values(balance=new_balance, version=Wallet.version + 1)
                    .returning(Wallet.balance)
                )
                updated_result = await session.execute(update_stmt)
                updated_wallet = updated_result.first()

                if updated_wallet is None:
                    logger.info("debit_retry user_id=%s attempt=%s", user_id_str, attempt)
                    continue

                ledger = LedgerEntry(
                    wallet_id=wallet_row.id,
                    entry_type="debit",
                    amount=payload.amount,
                    balance_after=updated_wallet.balance,
                )
                session.add(ledger)
                await session.flush()

                transaction_id = ledger.id
                balance = updated_wallet.balance
                logger.debug(
                    "debit_debug user_id=%s wallet_id=%s transaction_id=%s balance=%s",
                    user_id_str,
                    wallet_row.id,
                    transaction_id,
                    balance,
                )
                return WalletMutationResponse(
                    user_id=user_id_str,
                    balance=balance,
                    transaction_id=transaction_id,
                )
    except SQLAlchemyError as exc:
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
    session: AsyncSession = Depends(get_session),
) -> WalletBalanceResponse:
    # Read current wallet balance.
    user_id_str = user_id
    _authorize_owner(token_user_id, user_id_str)
    logger.info("balance_requested user_id=%s", user_id_str)
    try:
        result = await session.execute(
            select(Wallet.user_id, Wallet.balance, Wallet.created_at).where(
                Wallet.user_id == user_id_str
            )
        )
        row = result.first()
    except SQLAlchemyError as exc:
        logger.exception("balance_db_error user_id=%s error=%s", user_id_str, exc)
        raise HTTPException(status_code=500, detail="database error") from exc

    if row is None:
        logger.warning("balance_wallet_not_found user_id=%s", user_id_str)
        raise HTTPException(status_code=404, detail="wallet not found")

    return WalletBalanceResponse(user_id=row.user_id, balance=row.balance, created_at=row.created_at)


@router.get("/wallets/{user_id}/ledger", response_model=list[LedgerEntryResponse], tags=["wallet"])
async def get_wallet_ledger(
    user_id: str,
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    token_user_id: str = Depends(get_current_user_id),
    session: AsyncSession = Depends(get_session),
) -> list[LedgerEntryResponse]:
    # Read transaction history for one wallet.
    user_id_str = user_id
    _authorize_owner(token_user_id, user_id_str)
    logger.info("ledger_requested user_id=%s limit=%s offset=%s", user_id_str, limit, offset)
    try:
        wallet_result = await session.execute(
            select(Wallet.id).where(Wallet.user_id == user_id_str)
        )
        wallet = wallet_result.first()
        if wallet is None:
            logger.warning("ledger_wallet_not_found user_id=%s", user_id_str)
            raise HTTPException(status_code=404, detail="wallet not found")

        result = await session.execute(
            select(LedgerEntry)
            .where(LedgerEntry.wallet_id == wallet.id)
            .order_by(LedgerEntry.created_at.desc(), LedgerEntry.id.desc())
            .limit(limit)
            .offset(offset)
        )
        rows = result.scalars().all()
    except SQLAlchemyError as exc:
        logger.exception(
            "ledger_db_error user_id=%s limit=%s offset=%s error=%s",
            user_id_str,
            limit,
            offset,
            exc,
        )
        raise HTTPException(status_code=500, detail="database error") from exc

    return [
        LedgerEntryResponse(
            id=row.id,
            entry_type=row.entry_type,
            amount=row.amount,
            balance_after=row.balance_after,
            created_at=row.created_at,
        )
        for row in rows
    ]
