"""Serialize trades and dead letters into the JSON events published to Kafka."""

import json
from datetime import date

from ingestion.trades.models import Trade

SCHEMA_VERSION = 1


def trade_event(trade: Trade, source: str) -> dict:
    return {
        "schema_version": SCHEMA_VERSION,
        "ticker": trade.ticker,
        "trade_id": trade.trade_id,
        "action": trade.action,
        # Money travels as a string: float would corrupt exact decimal values.
        "price": str(trade.price),
        "quantity": trade.quantity,
        "traded_at": trade.traded_at.isoformat(),
        "trade_date": trade.trade_date.isoformat(),
        "reference_date": trade.reference_date.isoformat(),
        "session_type": trade.session_type,
        "source": source,
    }


def serialize_trade(trade: Trade, source: str) -> bytes:
    return json.dumps(trade_event(trade, source), separators=(",", ":")).encode()


def dead_letter_event(
    raw_line: str, error: str, ticker: str, session_date: date, filename: str, source: str
) -> dict:
    """Envelope for lines that failed parsing: enough context to inspect and reprocess."""
    return {
        "schema_version": SCHEMA_VERSION,
        "ticker": ticker,
        "session_date": session_date.isoformat(),
        "filename": filename,
        "source": source,
        "error": error,
        "raw_line": raw_line,
    }


def serialize_dead_letter(
    raw_line: str, error: str, ticker: str, session_date: date, filename: str, source: str
) -> bytes:
    event = dead_letter_event(raw_line, error, ticker, session_date, filename, source)
    return json.dumps(event, separators=(",", ":")).encode()
