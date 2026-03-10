from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import sys
import secrets
from decimal import Decimal
from pathlib import Path

import psycopg
import pytest
from dotenv import load_dotenv
from fastapi.testclient import TestClient

load_dotenv()
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app import app

DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://postgres:localhost@localhost:5432/newwallet_db"
)


@pytest.fixture(scope="module")
def client():
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture(scope="function", autouse=True)
def clean_db():
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "TRUNCATE TABLE ledger_entries, wallets, users RESTART IDENTITY CASCADE;"
            )
        conn.commit()


def test_health(client: TestClient):
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] in {"healthy", "unhealthy"}
    assert "service" in data
    assert "environment" in data


def create_user(client: TestClient, user_id: str, password: str = "pass1234") -> None:
    response = client.post("/users", json={"user_id": user_id, "password": password})
    assert response.status_code == 201


def auth_headers(client: TestClient, user_id: str, password: str = "pass1234") -> dict[str, str]:
    login = client.post("/auth/login", json={"user_id": user_id, "password": password})
    assert login.status_code == 200
    token = login.json()["access_token"]
    return {"Authorization": f"Bearer {token}"}


def test_auth_register_and_login(client: TestClient):
    user_id = f"user-{secrets.token_hex(4)}"
    register = client.post(
        "/auth/register",
        json={"user_id": user_id, "password": "pass1234"},
    )
    assert register.status_code == 201
    assert register.json()["user_id"] == user_id
    assert "created_at" in register.json()

    login = client.post("/auth/login", json={"user_id": user_id, "password": "pass1234"})
    assert login.status_code == 200
    assert "access_token" in login.json()


def test_create_wallet_and_duplicate(client: TestClient):
    user_id = f"user-{secrets.token_hex(4)}"
    create_user(client, user_id)
    headers = auth_headers(client, user_id)

    response = client.post("/wallets", json={"user_id": user_id}, headers=headers)
    assert response.status_code == 201
    assert Decimal(response.json()["balance"]) == Decimal("0.00")
    assert "created_at" in response.json()

    duplicate = client.post("/wallets", json={"user_id": user_id}, headers=headers)
    assert duplicate.status_code == 409


def test_create_get_list_user(client: TestClient):
    user_id = f"user-{secrets.token_hex(4)}"
    create = client.post("/users", json={"user_id": user_id, "password": "pass1234"})
    assert create.status_code == 201
    assert create.json()["user_id"] == user_id
    assert "created_at" in create.json()

    duplicate = client.post("/users", json={"user_id": user_id, "password": "pass1234"})
    assert duplicate.status_code == 409

    get_one = client.get(f"/users/{user_id}")
    assert get_one.status_code == 200
    assert get_one.json()["user_id"] == user_id
    assert "created_at" in get_one.json()

    list_users = client.get("/users")
    assert list_users.status_code == 200
    assert any(item["user_id"] == user_id for item in list_users.json())


def test_credit_debit_balance_and_ledger(client: TestClient):
    user_id = f"user-{secrets.token_hex(4)}"
    create_user(client, user_id)
    headers = auth_headers(client, user_id)
    assert client.post("/wallets", json={"user_id": user_id}, headers=headers).status_code == 201

    c1 = client.post(
        f"/wallets/{user_id}/credit",
        json={"amount": "100.00"},
        headers=headers,
    )
    assert c1.status_code == 200
    assert Decimal(c1.json()["balance"]) == Decimal("100.00")

    d1 = client.post(f"/wallets/{user_id}/debit", json={"amount": "40.00"}, headers=headers)
    assert d1.status_code == 200
    assert Decimal(d1.json()["balance"]) == Decimal("60.00")

    balance = client.get(f"/wallets/{user_id}/balance", headers=headers)
    assert balance.status_code == 200
    assert Decimal(balance.json()["balance"]) == Decimal("60.00")
    assert "created_at" in balance.json()

    ledger = client.get(f"/wallets/{user_id}/ledger", headers=headers)
    assert ledger.status_code == 200
    entries = ledger.json()
    assert len(entries) == 2
    assert {e["entry_type"] for e in entries} == {"credit", "debit"}


def test_debit_rejected_when_insufficient(client: TestClient):
    user_id = f"user-{secrets.token_hex(4)}"
    create_user(client, user_id)
    headers = auth_headers(client, user_id)
    assert client.post("/wallets", json={"user_id": user_id}, headers=headers).status_code == 201
    assert (
        client.post(
            f"/wallets/{user_id}/credit",
            json={"amount": "10.00"},
            headers=headers,
        ).status_code
        == 200
    )

    fail = client.post(f"/wallets/{user_id}/debit", json={"amount": "15.00"}, headers=headers)
    assert fail.status_code == 400
    assert fail.json()["detail"] == "insufficient funds"

    balance = client.get(f"/wallets/{user_id}/balance", headers=headers)
    assert Decimal(balance.json()["balance"]) == Decimal("10.00")
    assert "created_at" in balance.json()

    ledger = client.get(f"/wallets/{user_id}/ledger", headers=headers)
    assert len(ledger.json()) == 1
    assert ledger.json()[0]["entry_type"] == "credit"


def test_concurrent_debit_consistency(client: TestClient):
    user_id = f"user-{secrets.token_hex(4)}"
    # Phase 2 required scenario:
    # Wallet balance = 100, 50 concurrent debit requests, each debiting 10.
    initial_balance = Decimal("100.00")
    debit_amount = Decimal("10.00")
    request_count = 50
    expected_success = 10
    expected_failures = 40

    create_user(client, user_id)
    headers = auth_headers(client, user_id)
    assert client.post("/wallets", json={"user_id": user_id}, headers=headers).status_code == 201
    assert (
        client.post(
            f"/wallets/{user_id}/credit",
            json={"amount": str(initial_balance)},
            headers=headers,
        ).status_code
        == 200
    )

    def do_debit():
        for _ in range(5):
            response = client.post(
                f"/wallets/{user_id}/debit",
                json={"amount": str(debit_amount)},
                headers=headers,
            )
            if response.status_code != 409:
                return response.status_code
        return 409

    statuses: list[int] = []
    with ThreadPoolExecutor(max_workers=50) as executor:
        futures = [executor.submit(do_debit) for _ in range(request_count)]
        for future in as_completed(futures):
            statuses.append(future.result())

    success_count = statuses.count(200)
    insufficient_count = statuses.count(400)
    conflict_count = statuses.count(409)
    print(f"{success_count} passed {insufficient_count} fail")

    assert success_count == expected_success, (
        f"Expected {expected_success} successful debits, got {success_count}"
    )
    assert insufficient_count == expected_failures, (
        f"Expected {expected_failures} failed debits, got {insufficient_count}"
    )
    assert conflict_count == 0, f"Unexpected OCC conflicts after retries: {conflict_count}"
    assert len(statuses) == request_count, (
        f"Expected {request_count} total debit responses, got {len(statuses)}"
    )

    balance = client.get(f"/wallets/{user_id}/balance", headers=headers)
    assert balance.status_code == 200
    assert Decimal(balance.json()["balance"]) == Decimal("0.00")

    ledger = client.get(f"/wallets/{user_id}/ledger?limit=200", headers=headers)
    assert ledger.status_code == 200
    entries = ledger.json()
    debit_entries = [entry for entry in entries if entry["entry_type"] == "debit"]
    credit_entries = [entry for entry in entries if entry["entry_type"] == "credit"]
    assert len(debit_entries) == expected_success, (
        f"Expected {expected_success} debit ledger entries, got {len(debit_entries)}"
    )
    assert len(credit_entries) == 1


def test_create_wallet_blocks_cross_user_create(client: TestClient):
    user_id = f"user-{secrets.token_hex(4)}"
    owner_id = f"user-{secrets.token_hex(4)}"
    create_user(client, owner_id)
    headers = auth_headers(client, owner_id)
    response = client.post("/wallets", json={"user_id": user_id}, headers=headers)
    assert response.status_code == 403
    assert response.json()["detail"] == "forbidden"


def test_wallet_routes_require_jwt(client: TestClient):
    user_id = f"user-{secrets.token_hex(4)}"
    create_user(client, user_id)
    response = client.post("/wallets", json={"user_id": user_id})
    assert response.status_code == 403


def test_wallet_authorization_blocks_other_user_access(client: TestClient):
    owner = f"user-{secrets.token_hex(4)}"
    other = f"user-{secrets.token_hex(4)}"
    create_user(client, owner)
    create_user(client, other)

    owner_headers = auth_headers(client, owner)
    other_headers = auth_headers(client, other)
    assert client.post("/wallets", json={"user_id": owner}, headers=owner_headers).status_code == 201
    assert (
        client.post(f"/wallets/{owner}/credit", json={"amount": "25.00"}, headers=owner_headers).status_code
        == 200
    )

    blocked = client.get(f"/wallets/{owner}/balance", headers=other_headers)
    assert blocked.status_code == 403
    assert blocked.json()["detail"] == "forbidden"
