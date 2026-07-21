import json
from datetime import date

from ingestion.trades.models import parse_trade
from ingestion.trades.watermarks import (
    filter_new_trades,
    fold_watermark_records,
    updated_watermarks,
    watermark_key,
)

TRADE_10 = parse_trade("2026-07-14;PETR4;0;41,200;5500;100719126;10;1;2026-07-14;85;8")
TRADE_20 = parse_trade("2026-07-14;PETR4;0;41,120;300;101602106;20;1;2026-07-14;85;120")
CANCEL_10 = parse_trade("2026-07-14;PETR4;2;41,200;5500;100719126;10;1;2026-07-14;85;8")
# After-market row: belongs to the 2026-07-13 session, id sequence of that day.
AFTER_MARKET = parse_trade("2026-07-14;ITUB4;0;43,390;200;020118304;205660;6;2026-07-13;3;1618")


def test_watermark_key_includes_session_date():
    assert watermark_key("PETR4", date(2026, 7, 14)) == "PETR4:2026-07-14"


def test_filters_trades_at_or_below_watermark():
    watermarks = {"PETR4:2026-07-14": 10}
    assert filter_new_trades([TRADE_10, TRADE_20], watermarks) == [TRADE_20]


def test_unknown_key_keeps_everything():
    assert filter_new_trades([TRADE_10, TRADE_20], {}) == [TRADE_10, TRADE_20]


def test_cancellations_always_pass():
    # Cancellations reuse the original (old) trade id, so they never clear the
    # watermark; they must bypass it and rely on sink-side deduplication.
    watermarks = {"PETR4:2026-07-14": 999_999}
    assert filter_new_trades([CANCEL_10], watermarks) == [CANCEL_10]


def test_watermarks_are_per_session_not_per_ticker():
    # Yesterday's after-market id (205660) must not filter today's ids (10, 20).
    itub_today = parse_trade("2026-07-14;ITUB4;0;43,810;100;100335603;10;1;2026-07-14;23;90")
    watermarks = {"ITUB4:2026-07-13": 205660}
    assert filter_new_trades([AFTER_MARKET, itub_today], watermarks) == [itub_today]


def test_updated_watermarks_takes_max_per_session():
    updates = updated_watermarks([TRADE_10, TRADE_20, AFTER_MARKET])
    assert updates == {"PETR4:2026-07-14": 20, "ITUB4:2026-07-13": 205660}


def test_updated_watermarks_ignores_cancellations():
    assert updated_watermarks([CANCEL_10]) == {}


def test_fold_last_value_per_key_wins():
    records = [
        ("PETR4:2026-07-14", json.dumps({"max_trade_id": 100}).encode()),
        ("MGLU3:2026-07-14", json.dumps({"max_trade_id": 50}).encode()),
        ("PETR4:2026-07-14", json.dumps({"max_trade_id": 900}).encode()),
    ]
    assert fold_watermark_records(records) == {"PETR4:2026-07-14": 900, "MGLU3:2026-07-14": 50}


def test_fold_tombstone_clears_key():
    records = [
        ("PETR4:2026-07-13", json.dumps({"max_trade_id": 100}).encode()),
        ("PETR4:2026-07-13", None),
    ]
    assert fold_watermark_records(records) == {}
