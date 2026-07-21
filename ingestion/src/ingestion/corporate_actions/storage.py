"""Postgres storage for corporate actions.

Corporate actions are tiny, slow-changing reference data refetched IN FULL on
every poll, so each sync is a transactional full replace per ticker. This keeps
the table exactly in sync with B3's current answer: amended events (e.g. a TBD
payment date gaining a real date) never leave orphan rows behind.
"""

import logging
import os

import psycopg

from ingestion.corporate_actions.api import CashDividend, StockDividend

logger = logging.getLogger(__name__)

SCHEMA_STATEMENTS = (
    """
    CREATE TABLE IF NOT EXISTS cash_dividends (
        ticker          text NOT NULL,
        isin            text NOT NULL,
        label           text NOT NULL,
        rate            numeric NOT NULL,
        approved_on     date NOT NULL,
        last_date_prior date NOT NULL,
        payment_date    date,
        related_to      text NOT NULL DEFAULT '',
        synced_at       timestamptz NOT NULL DEFAULT now()
    )
    """,
    "CREATE INDEX IF NOT EXISTS cash_dividends_ticker ON cash_dividends (ticker)",
    """
    CREATE TABLE IF NOT EXISTS stock_dividends (
        ticker          text NOT NULL,
        isin            text NOT NULL,
        label           text NOT NULL,
        factor          numeric NOT NULL,
        approved_on     date NOT NULL,
        last_date_prior date NOT NULL,
        synced_at       timestamptz NOT NULL DEFAULT now()
    )
    """,
    "CREATE INDEX IF NOT EXISTS stock_dividends_ticker ON stock_dividends (ticker)",
    # Serving copies maintained by RisingWave sinks (see risingwave/schema.sql).
    """
    CREATE TABLE IF NOT EXISTS last_price (
        ticker    text PRIMARY KEY,
        price     numeric NOT NULL,
        traded_at timestamptz NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS session_summary (
        ticker           text NOT NULL,
        trade_date       date NOT NULL,
        open             numeric,
        high             numeric,
        low              numeric,
        close            numeric,
        quantity         bigint,
        financial_volume numeric,
        trades           bigint,
        PRIMARY KEY (ticker, trade_date)
    )
    """,
)


def connect() -> psycopg.Connection:
    dsn = os.environ.get("DATABASE_URL", "postgresql://app:app@postgres:5432/b3")
    return psycopg.connect(dsn)


def ensure_schema(conn: psycopg.Connection) -> None:
    with conn.transaction():
        for statement in SCHEMA_STATEMENTS:
            conn.execute(statement)


def replace_corporate_actions(
    conn: psycopg.Connection,
    ticker: str,
    cash: list[CashDividend],
    stock: list[StockDividend],
) -> None:
    with conn.transaction(), conn.cursor() as cursor:
        cursor.execute("DELETE FROM cash_dividends WHERE ticker = %s", (ticker,))
        cursor.execute("DELETE FROM stock_dividends WHERE ticker = %s", (ticker,))
        cursor.executemany(
            """
            INSERT INTO cash_dividends
                (ticker, isin, label, rate, approved_on, last_date_prior,
                 payment_date, related_to)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            [
                (
                    ticker,
                    d.isin,
                    d.label,
                    d.rate,
                    d.approved_on,
                    d.last_date_prior,
                    d.payment_date,
                    d.related_to,
                )
                # dict.fromkeys: dedupe identical rows (frozen dataclasses hash),
                # preserving order.
                for d in dict.fromkeys(cash)
            ],
        )
        cursor.executemany(
            """
            INSERT INTO stock_dividends
                (ticker, isin, label, factor, approved_on, last_date_prior)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            [
                (ticker, s.isin, s.label, s.factor, s.approved_on, s.last_date_prior)
                for s in dict.fromkeys(stock)
            ],
        )
