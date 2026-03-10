import logging

import psycopg
from fastapi import APIRouter, HTTPException, status
from psycopg.rows import dict_row

from auth import create_access_token, hash_password, verify_password
from db import pool
from schemas import LoginRequest, RegisterRequest, TokenResponse, UserResponse

logger = logging.getLogger("wallet.auth")
router = APIRouter(tags=["auth"])


@router.post("/auth/register", response_model=UserResponse, status_code=201)
async def register(payload: RegisterRequest) -> UserResponse:
    """Register user with user_id and password."""
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
                    (payload.user_id, hash_password(payload.password)),
                )
                row = await cur.fetchone()
                await conn.commit()
    except psycopg.Error as exc:
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
    return UserResponse(**row)


@router.post("/auth/login", response_model=TokenResponse)
async def login(payload: LoginRequest) -> TokenResponse:
    """
    Simple login:
    - user must already exist in users table
    - password must be correct
    - then return JWT
    """
    try:
        async with pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    "SELECT user_id, password_hash FROM users WHERE user_id = %s;",
                    (payload.user_id,),
                )
                user = await cur.fetchone()
    except psycopg.Error as exc:
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

    if not verify_password(payload.password, user["password_hash"]):
        logger.warning("login_invalid_password user_id=%s", payload.user_id)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid credentials",
        )

    token = create_access_token(payload.user_id)
    logger.info("login_success user_id=%s", payload.user_id)
    return TokenResponse(access_token=token)
