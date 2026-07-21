from datetime import date
from decimal import Decimal

from api.main import session_item

ROW = {
    "trade_date": date(2026, 7, 21),
    "open": Decimal("41.000"),
    "high": Decimal("41.500"),
    "low": Decimal("40.800"),
    "close": Decimal("41.250"),
    "quantity": 12345600,
    "financial_volume": Decimal("508123456.780"),
    "trades": 33210,
}


def test_session_serializes_money_as_strings():
    item = session_item(ROW)
    assert item["date"] == "2026-07-21"
    assert item["open"] == "41.000"
    assert item["financial_volume"] == "508123456.780"
    assert item["quantity"] == 12345600
    assert item["trades"] == 33210


def test_session_before_first_regular_trade_has_null_ohlc():
    row = ROW | {"open": None, "high": None, "low": None, "close": None}
    item = session_item(row)
    assert item["open"] is None
    assert item["close"] is None
    assert item["quantity"] == 12345600  # totals still count pre-open/after-market rows
