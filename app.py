import logging
import secrets
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from auth_routes import router as auth_router
from config import APP_ENV, APP_NAME, APP_VERSION
from db import db_healthcheck, init_db, pool
from logging_setup import setup_logging
from routes import router
from schemas import HealthResponse

setup_logging()
logger = logging.getLogger("wallet.app")


@asynccontextmanager
async def lifespan(_app: FastAPI):
    # Application startup: open DB pool and ensure tables exist.
    logger.info("app_startup_begin")
    await pool.open()
    await init_db()
    logger.info("app_startup_complete")
    try:
        yield
    finally:
        # Application shutdown: close DB pool cleanly.
        logger.info("app_shutdown_begin")
        await pool.close()
        logger.info("app_shutdown_complete")


app = FastAPI(
    title=APP_NAME,
    version=APP_VERSION,
    lifespan=lifespan,
    openapi_tags=[
        {"name": "auth", "description": "Register and login APIs"},
        {"name": "users", "description": "User management APIs"},
        {"name": "wallet", "description": "Wallet and ledger APIs"},
        {"name": "health", "description": "Health check API"},
    ],
)
app.include_router(router)
app.include_router(auth_router)


@app.middleware("http")
async def request_logging_middleware(request: Request, call_next):
    # Correlate each request with a request-id for easier debugging.
    request_id = request.headers.get("x-request-id", secrets.token_hex(16))
    start = time.perf_counter()
    response = await call_next(request)
    elapsed_ms = (time.perf_counter() - start) * 1000

    response.headers["x-request-id"] = request_id
    logger.info(
        "request method=%s path=%s status=%s duration_ms=%.2f request_id=%s",
        request.method,
        request.url.path,
        response.status_code,
        elapsed_ms,
        request_id,
    )
    return response


@app.get("/health", response_model=HealthResponse, tags=["health"])
async def health() -> HealthResponse:
    # Health endpoint used by monitoring systems.
    healthy = await db_healthcheck()
    return HealthResponse(
        status="healthy" if healthy else "unhealthy",
        service=APP_NAME,
        environment=APP_ENV,
    )


@app.exception_handler(Exception)
def unhandled_exception_handler(_request: Request, exc: Exception) -> JSONResponse:
    # Fallback for unexpected exceptions.
    logger.exception("unhandled_exception: %s", exc)
    return JSONResponse(status_code=500, content={"detail": "internal server error"})
