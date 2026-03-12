import asyncio
import logging
import sys
from collections import Counter
from decimal import Decimal

from sqlalchemy import delete, func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError

from config import MONEY_QUANT
from db import SessionLocal, close_db, init_db
from models import LedgerEntry, User, Wallet

logger = logging.getLogger("wallet.concurrency_check")

USER_ID = "phase2_user"
DEBIT_AMOUNT = Decimal("10.00")
START_BALANCE = Decimal("100.00")
DEBIT_REQUESTS = 50
MAX_RETRIES = 50


def _fmt_money(value: Decimal) -> str:
    return str(value.quantize(MONEY_QUANT))


async def _prepare_state() -> int:
    async with SessionLocal() as session:
        async with session.begin():
            user_stmt = (
                insert(User)
                .values(user_id=USER_ID, password_hash="demo_hash")
                .on_conflict_do_nothing(index_elements=[User.user_id])
            )
            await session.execute(user_stmt)

            wallet_stmt = (
                insert(Wallet)
                .values(user_id=USER_ID, balance=START_BALANCE, version=0)
                .on_conflict_do_update(
                    index_elements=[Wallet.user_id],
                    set_={"balance": START_BALANCE, "version": 0},
                )
                .returning(Wallet.id)
            )
            result = await session.execute(wallet_stmt)
            wallet_id = result.scalar_one()

            await session.execute(delete(LedgerEntry).where(LedgerEntry.wallet_id == wallet_id))
            return wallet_id


async def _debit_once() -> tuple[bool, str | None]:
    for _ in range(MAX_RETRIES):
        async with SessionLocal() as session:
            async with session.begin():
                result = await session.execute(
                    select(Wallet.id, Wallet.balance, Wallet.version).where(
                        Wallet.user_id == USER_ID
                    )
                )
                wallet = result.first()
                if wallet is None:
                    return False, "Wallet not found"
                if wallet.balance < DEBIT_AMOUNT:
                    return False, "Insufficient balance"

                new_balance = wallet.balance - DEBIT_AMOUNT
                update_stmt = (
                    update(Wallet)
                    .where(Wallet.id == wallet.id, Wallet.version == wallet.version)
                    .values(balance=new_balance, version=Wallet.version + 1)
                    .returning(Wallet.balance)
                )
                updated = await session.execute(update_stmt)
                if updated.first() is None:
                    await asyncio.sleep(0)
                    continue

                ledger = LedgerEntry(
                    wallet_id=wallet.id,
                    entry_type="debit",
                    amount=DEBIT_AMOUNT,
                    balance_after=new_balance,
                )
                session.add(ledger)
                return True, None
    return False, "Conflict"


async def _get_final_state(wallet_id: int) -> tuple[Decimal, int]:
    async with SessionLocal() as session:
        balance_result = await session.execute(
            select(Wallet.balance).where(Wallet.id == wallet_id)
        )
        balance = balance_result.scalar_one_or_none() or Decimal("0")

        count_result = await session.execute(
            select(func.count())
            .select_from(LedgerEntry)
            .where(LedgerEntry.wallet_id == wallet_id, LedgerEntry.entry_type == "debit")
        )
        debit_entries = int(count_result.scalar_one())
    return balance, debit_entries


async def run_check() -> int:
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
    except SQLAlchemyError:
        logger.exception("concurrency_check_db_error")
        print("PHASE2_CONCURRENCY_CHECK: FAIL")
        print("successes=0 failures=0 final_balance=0.00 debit_ledger_entries=0 failure_reasons={}")
        return 1
    finally:
        await close_db()


def main() -> None:
    if sys.platform.startswith("win"):
        # SQLAlchemy async requires selector loop on Windows for asyncpg.
        policy = getattr(asyncio, "WindowsSelectorEventLoopPolicy", None)
        if policy is not None:
            asyncio.set_event_loop_policy(policy())
    raise SystemExit(asyncio.run(run_check()))


if __name__ == "__main__":
    main()
