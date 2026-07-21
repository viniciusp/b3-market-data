from datetime import date, timedelta
from decimal import Decimal

import pytest

from ingestion.trades.models import ACTION_CANCELLED, MalformedTradeError, parse_trade, parse_trades

# Real lines captured from B3 files on 2026-07-14.
REGULAR = "2026-07-14;PETR4;0;41,200;5500;100719126;10;1;2026-07-14;85;8"
AFTER_MARKET = "2026-07-14;ITUB4;0;43,390;200;020118304;205660;6;2026-07-13;3;1618"
EMPTY_PARTICIPANTS = "2026-07-10;BGIN26;0;329,200;1;090000014;80;1;2026-07-10;;"
CANCELLATION = "2026-07-14;PETR4;2;41,200;5500;100719126;10;1;2026-07-14;85;8"


def test_parses_regular_trade():
    trade = parse_trade(REGULAR)
    assert trade.ticker == "PETR4"
    assert trade.trade_id == 10
    assert trade.action == 0
    assert trade.price == Decimal("41.200")
    assert trade.quantity == 5500
    assert trade.session_type == 1
    assert trade.trade_date == date(2026, 7, 14)
    assert trade.reference_date == date(2026, 7, 14)


def test_price_is_decimal_not_float():
    assert isinstance(parse_trade(REGULAR).price, Decimal)


def test_traded_at_is_utc():
    # Source is Sao Paulo wall clock (10:07 -03:00); stored instant is UTC.
    traded_at = parse_trade(REGULAR).traded_at
    assert traded_at.utcoffset() == timedelta(0)
    assert (traded_at.hour, traded_at.minute, traded_at.second) == (13, 7, 19)
    assert traded_at.microsecond == 126_000


def test_after_market_trade_belongs_to_previous_session():
    # Files can carry after-market trades from the previous session:
    # trade_date (DataNegocio) differs from reference_date (DataReferencia).
    trade = parse_trade(AFTER_MARKET)
    assert trade.trade_date == date(2026, 7, 13)
    assert trade.reference_date == date(2026, 7, 14)
    assert trade.session_type == 6
    assert trade.traded_at.hour == 5  # 02:01 Sao Paulo


def test_parse_trades_partitions_malformed_lines_with_errors():
    trades, malformed = parse_trades([REGULAR, "garbage", AFTER_MARKET])
    assert [trade.ticker for trade in trades] == ["PETR4", "ITUB4"]
    [(line, error)] = malformed
    assert line == "garbage"
    assert "expected 11 fields" in error


def test_cancellation_reuses_original_trade_id():
    trade = parse_trade(CANCELLATION)
    assert trade.action == ACTION_CANCELLED
    assert trade.trade_id == 10


def test_empty_participant_fields_are_accepted():
    trade = parse_trade(EMPTY_PARTICIPANTS)
    assert trade.ticker == "BGIN26"
    assert trade.price == Decimal("329.200")


@pytest.mark.parametrize(
    "line",
    [
        "not;enough;fields",
        "2026-07-14;PETR4;0;not-a-price;5500;100719126;10;1;2026-07-14;85;8",
        "2026-07-14;PETR4;0;41,200;5500;1007;10;1;2026-07-14;85;8",  # short time
        "2026-07-14;PETR4;zero;41,200;5500;100719126;10;1;2026-07-14;85;8",
    ],
)
def test_malformed_lines_raise(line):
    with pytest.raises(MalformedTradeError):
        parse_trade(line)
