from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
from typing import Annotated, Literal

from pydantic import BaseModel, Field, field_validator

from config import MONEY_QUANT

# Typed aliases used by request/response models.
MoneyValue = Annotated[
    Decimal,
    Field(gt=0, max_digits=18, decimal_places=2, json_schema_extra={"example": "100.00"}),
]
BalanceValue = Annotated[
    Decimal,
    Field(max_digits=18, decimal_places=2, json_schema_extra={"example": "60.00"}),
]


class CreateWalletRequest(BaseModel):
    user_id: str = Field(min_length=1, max_length=128)


class CreateUserRequest(BaseModel):
    user_id: str = Field(min_length=1, max_length=128)
    password: str = Field(min_length=6, max_length=128)


class UserResponse(BaseModel):
    user_id: str
    created_at: datetime


class LoginRequest(BaseModel):
    user_id: str = Field(min_length=1, max_length=128)
    password: str = Field(min_length=6, max_length=128)


class RegisterRequest(BaseModel):
    user_id: str = Field(min_length=1, max_length=128)
    password: str = Field(min_length=6, max_length=128)


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"


class MoneyRequest(BaseModel):
    amount: MoneyValue

    @field_validator("amount")
    @classmethod
    def validate_amount(cls, value: Decimal) -> Decimal:
        # Keep money in 2-decimal format (for predictable ledger values).
        if value <= 0:
            raise ValueError("amount must be greater than zero")
        return value.quantize(MONEY_QUANT, rounding=ROUND_HALF_UP)


class WalletBalanceResponse(BaseModel):
    user_id: str
    balance: BalanceValue
    created_at: datetime


class WalletMutationResponse(BaseModel):
    user_id: str
    balance: BalanceValue
    transaction_id: int


class LedgerEntryResponse(BaseModel):
    id: int
    entry_type: Literal["credit", "debit"]
    amount: MoneyValue
    balance_after: BalanceValue
    created_at: datetime


class HealthResponse(BaseModel):
    status: Literal["healthy", "unhealthy"]
    service: str
    environment: str
