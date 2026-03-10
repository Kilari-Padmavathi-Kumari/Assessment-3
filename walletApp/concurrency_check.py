import asyncio
import logging
import sys
from collections import Counter
from decimal import Decimal

import psycopg
from psycopg.rows import dict_row

from config import MONEY_QUANT
from db import init_db, pool

logger = logging.getLogger("wallet.concurrency_check")

USER_ID = "phase2_user"
DEBIT_AMOUNT = Decimal("10.00")
START_BALANCE = Decimal("100.00")
DEBIT_REQUESTS = 50
MAX_RETRIES = 50


def _fmt_money(value: Decimal) -> str:
    return str(value.quantize(MONEY_QUANT))


async def _prepare_state() -> int:
    async with pool.connection() as conn:
        async with conn.transaction():
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    """
                    INSERT INTO users (user_id, password_hash)
                    VALUES (%s, %s)
                    ON CONFLICT (user_id) DO NOTHING;
                    """,
                    (USER_ID, "demo_hash"),
                )
                await cur.execute(
                    """
                    INSERT INTO wallets (user_id, balance, version)
                    VALUES (%s, %s, 0)
                    ON CONFLICT (user_id) DO UPDATE
                        SET balance = EXCLUDED.balance,
                            version = 0
                    RETURNING id;
                    """,
                    (USER_ID, START_BALANCE),
                )
                wallet_row = await cur.fetchone()
                wallet_id = wallet_row["id"]
                await cur.execute(
                    "DELETE FROM ledger_entries WHERE wallet_id = %s;",
                    (wallet_id,),
                )
                return wallet_id


async def _debit_once() -> tuple[bool, str | None]:
    for _ in range(MAX_RETRIES):
        async with pool.connection() as conn:
            async with conn.transaction():
                async with conn.cursor(row_factory=dict_row) as cur:
                    await cur.execute(
                        "SELECT id, balance, version FROM wallets WHERE user_id = %s;",
                        (USER_ID,),
                    )
                    wallet = await cur.fetchone()
                    if wallet is None:
                        return False, "Wallet not found"
                    if wallet["balance"] < DEBIT_AMOUNT:
                        return False, "Insufficient balance"

                    new_balance = wallet["balance"] - DEBIT_AMOUNT
                    await cur.execute(
                        """
                        UPDATE wallets
                        SET balance = %s,
                            version = version + 1
                        WHERE id = %s AND version = %s
                        RETURNING balance;
                        """,
                        (new_balance, wallet["id"], wallet["version"]),
                    )
                    updated = await cur.fetchone()
                    if updated is None:
                        await asyncio.sleep(0)
                        continue

                    await cur.execute(
                        """
                        INSERT INTO ledger_entries (wallet_id, entry_type, amount, balance_after)
                        VALUES (%s, 'debit', %s, %s);
                        """,
                        (wallet["id"], DEBIT_AMOUNT, updated["balance"]),
                    )
                    return True, None
    return False, "Conflict"


async def _get_final_state(wallet_id: int) -> tuple[Decimal, int]:
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT balance FROM wallets WHERE id = %s;",
                (wallet_id,),
            )
            row = await cur.fetchone()
            balance = row["balance"] if row else Decimal("0")

            await cur.execute(
                """
                SELECT COUNT(*) AS total
                FROM ledger_entries
                WHERE wallet_id = %s AND entry_type = 'debit';
                """,
                (wallet_id,),
            )
            count_row = await cur.fetchone()
            debit_entries = int(count_row["total"]) if count_row else 0
    return balance, debit_entries


async def run_check() -> int:
    await pool.open()
    try:
        await init_db()
        wallet_id = await _prepare_state()

        results = await asyncio.gather(*(_debit_once() for _ in range(DEBIT_REQUESTS)))
        successes = sum(1 for ok, _ in results if ok)
        failures = DEBIT_REQUESTS - successes
        failure_reasons = Counter(reason for ok, reason in results if not ok and reason)

        final_balance, debit_entries = await _get_final_state(wallet_id)

        passed = (
            successes == 10
            and failures == 40
            and final_balance.quantize(MONEY_QUANT) == Decimal("0.00")
            and debit_entries == 10
            and failure_reasons == {"Insufficient balance": 40}
        )

        print(f"PHASE2_CONCURRENCY_CHECK: {'PASS' if passed else 'FAIL'}")
        print(
            "successes={successes} failures={failures} final_balance={final_balance} "
            "debit_ledger_entries={debit_entries} failure_reasons={failure_reasons}".format(
                successes=successes,
                failures=failures,
                final_balance=_fmt_money(final_balance),
                debit_entries=debit_entries,
                failure_reasons=dict(failure_reasons),
            )
        )
        return 0 if passed else 1
    except psycopg.Error:
        logger.exception("concurrency_check_db_error")
        print("PHASE2_CONCURRENCY_CHECK: FAIL")
        print("successes=0 failures=0 final_balance=0.00 debit_ledger_entries=0 failure_reasons={}")
        return 1
    finally:
        await pool.close()


def main() -> None:
    if sys.platform.startswith("win"):
        # psycopg async requires selector loop on Windows.
        policy = getattr(asyncio, "WindowsSelectorEventLoopPolicy", None)
        if policy is not None:
            asyncio.set_event_loop_policy(policy())
    raise SystemExit(asyncio.run(run_check()))


if __name__ == "__main__":
    main()
