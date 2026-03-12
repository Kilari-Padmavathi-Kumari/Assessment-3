import logging

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from auth import create_access_token, hash_password, verify_password
from db import get_session
from models import User
from schemas import LoginRequest, RegisterRequest, TokenResponse, UserResponse

logger = logging.getLogger("wallet.auth")
router = APIRouter(tags=["auth"])


@router.post("/auth/register", response_model=UserResponse, status_code=201)
async def register(
    payload: RegisterRequest,
    session: AsyncSession = Depends(get_session),
) -> UserResponse:
    """Register user with user_id and password."""
    try:
        stmt = (
            insert(User)
            .values(user_id=payload.user_id, password_hash=hash_password(payload.password))
            .on_conflict_do_nothing(index_elements=[User.user_id])
            .returning(User.user_id, User.created_at)
        )
        async with session.begin():
            result = await session.execute(stmt)
            row = result.first()
    except SQLAlchemyError as exc:
        logger.exception("register_db_error user_id=%s error=%s", payload.user_id, exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="database error",
        ) from exc

    if row is None:
        logger.warning("register_user_conflict user_id=%s", payload.user_id)
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="user already exists",
        )

    logger.info("register_success user_id=%s", payload.user_id)
    return UserResponse(user_id=row.user_id, created_at=row.created_at)


@router.post("/auth/login", response_model=TokenResponse)
async def login(
    payload: LoginRequest,
    session: AsyncSession = Depends(get_session),
) -> TokenResponse:
    """
    Simple login:
    - user must already exist in users table
    - password must be correct
    - then return JWT
    """
    try:
        result = await session.execute(
            User.__table__
            .select()
            .with_only_columns(User.user_id, User.password_hash)
            .where(User.user_id == payload.user_id)
        )
        user = result.first()
    except SQLAlchemyError as exc:
        logger.exception("login_db_error user_id=%s error=%s", payload.user_id, exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="database error",
        ) from exc

    if user is None:
        logger.warning("login_user_not_found user_id=%s", payload.user_id)
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="user not found",
        )

    if not verify_password(payload.password, user.password_hash):
        logger.warning("login_invalid_password user_id=%s", payload.user_id)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid credentials",
        )

    token = create_access_token(payload.user_id)
    logger.info("login_success user_id=%s", payload.user_id)
    return TokenResponse(access_token=token)
