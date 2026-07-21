import json
from datetime import date

from ingestion.trades.events import dead_letter_event, serialize_trade, trade_event
from ingestion.trades.models import parse_trade

REGULAR = "2026-07-14;PETR4;0;41,200;5500;100719126;10;1;2026-07-14;85;8"


def test_event_carries_all_trade_fields():
    event = trade_event(parse_trade(REGULAR), source="intraday_poll")
    assert event == {
        "schema_version": 1,
        "ticker": "PETR4",
        "trade_id": 10,
        "action": 0,
        "price": "41.200",
        "quantity": 5500,
        "traded_at": "2026-07-14T13:07:19.126000+00:00",
        "trade_date": "2026-07-14",
        "reference_date": "2026-07-14",
        "session_type": 1,
        "source": "intraday_poll",
    }


def test_price_is_serialized_as_string():
    event = json.loads(serialize_trade(parse_trade(REGULAR), source="eod_file"))
    assert event["price"] == "41.200"
    assert isinstance(event["price"], str)


def test_serialized_event_is_compact_json():
    raw = serialize_trade(parse_trade(REGULAR), source="eod_file")
    assert b" " not in raw
    assert json.loads(raw)["source"] == "eod_file"


def test_dead_letter_event_carries_context_for_reprocessing():
    event = dead_letter_event(
        raw_line="garbage",
        error="expected 11 fields, got 1",
        ticker="PETR4",
        session_date=date(2026, 7, 14),
        filename="14-07-2026_NEGOCIOSAVISTA_PETR4_1046.txt",
        source="intraday_poll",
    )
    assert event == {
        "schema_version": 1,
        "ticker": "PETR4",
        "session_date": "2026-07-14",
        "filename": "14-07-2026_NEGOCIOSAVISTA_PETR4_1046.txt",
        "source": "intraday_poll",
        "error": "expected 11 fields, got 1",
        "raw_line": "garbage",
    }
