from datetime import UTC, date, datetime
from decimal import Decimal

from api.main import pending_item

ROW = {
    "ticker": "PETR4",
    "label": "JRS CAP PROPRIO",
    "rate": Decimal("0.35048636000"),
    "approved_on": date(2026, 5, 11),
    "last_date_prior": date(2026, 6, 1),
    "payment_date": date(2026, 8, 20),
    "status": "pending_payment",
    "price": Decimal("41.250"),
    "traded_at": datetime(2026, 7, 21, 13, 22, 14, tzinfo=UTC),
}


def test_jcp_nets_out_the_15_percent_withholding():
    item = pending_item(ROW)
    assert item["gross_per_share"] == "0.35048636000"
    assert Decimal(item["net_per_share"]) == Decimal("0.35048636000") * Decimal("0.85")
    assert "IRRF" in item["tax_note"]


def test_plain_dividend_has_no_withholding():
    item = pending_item({**ROW, "label": "DIVIDENDO"})
    assert item["net_per_share"] == item["gross_per_share"]
    assert "exempt" in item["tax_note"]


def test_tbd_payment_date_maps_to_null_plus_flag():
    item = pending_item({**ROW, "payment_date": None})
    assert item["payment_date"] is None
    assert item["payment_tbd"] is True


def test_yield_uses_last_stream_price():
    item = pending_item(ROW)
    assert item["yield_on_last_price"] == round(float(Decimal("0.35048636") / Decimal("41.250")), 6)
    assert item["last_price"] == "41.250"
    assert item["price_as_of"] == "2026-07-21T13:22:14+00:00"


def test_buy_by_is_the_last_cum_date():
    assert pending_item(ROW)["buy_by"] == "2026-06-01"
