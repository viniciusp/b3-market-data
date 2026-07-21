from datetime import date
from decimal import Decimal

import httpx
import pytest

from ingestion.corporate_actions.api import (
    STATUS_PAID,
    STATUS_PENDING_PAYMENT,
    STATUS_WITH_RIGHTS,
    CorporateActionsError,
    dividend_status,
    fetch_supplement,
    isin_matches_ticker,
    parse_cash_dividend,
    parse_stock_dividend,
    parse_supplement,
)

# Real payloads captured from B3's API during research (2026-07-14).
PETR_JCP = {
    "assetIssued": "BRPETRACNPR6",
    "paymentDate": "20/08/2026",
    "rate": "0,35048636000",
    "relatedTo": "Anual/2026",
    "approvedOn": "11/05/2026",
    "isinCode": "BRPETRACNPR6",
    "label": "JRS CAP PROPRIO",
    "lastDatePrior": "01/06/2026",
    "remarks": "",
}
BBDC_TBD = {
    "assetIssued": "BRBBDCACNOR1",
    "paymentDate": "31/12/9999",
    "rate": "0,27014672900",
    "relatedTo": "3º Trimestre/2025",
    "approvedOn": "18/09/2025",
    "isinCode": "BRBBDCACNOR1",
    "label": "JRS CAP PROPRIO",
    "lastDatePrior": "29/09/2025",
    "remarks": "",
}
MGLU_SPLIT = {
    "assetIssued": "BRMGLUACNOR2",
    "factor": "300,00000000000",
    "approvedOn": "07/10/2020",
    "isinCode": "BRMGLUACNOR2",
    "label": "DESDOBRAMENTO",
    "lastDatePrior": "13/10/2020",
    "remarks": "",
}


def test_parses_cash_dividend():
    dividend = parse_cash_dividend(PETR_JCP)
    assert dividend.isin == "BRPETRACNPR6"
    assert dividend.label == "JRS CAP PROPRIO"
    assert dividend.rate == Decimal("0.35048636000")
    assert dividend.approved_on == date(2026, 5, 11)
    assert dividend.last_date_prior == date(2026, 6, 1)
    assert dividend.payment_date == date(2026, 8, 20)


def test_payment_tbd_sentinel_becomes_none():
    assert parse_cash_dividend(BBDC_TBD).payment_date is None


def test_parses_stock_dividend():
    split = parse_stock_dividend(MGLU_SPLIT)
    assert split.label == "DESDOBRAMENTO"
    assert split.factor == Decimal("300.00000000000")


def test_factor_with_thousands_separator():
    # Real case: Bradesco's 2009 split ships factor "4.900,000..." (pt-br thousands dot).
    row = dict(MGLU_SPLIT, factor="4.900,00000000000")
    assert parse_stock_dividend(row).factor == Decimal("4900.00000000000")


def test_parse_supplement_partitions_bad_rows():
    raw = {"cashDividends": [PETR_JCP, {"garbage": True}], "stockDividends": [MGLU_SPLIT]}
    cash, stock, errors = parse_supplement(raw)
    assert len(cash) == 1
    assert len(stock) == 1
    assert len(errors) == 1


def test_status_with_rights_until_last_cum_date():
    dividend = parse_cash_dividend(PETR_JCP)
    assert dividend_status(dividend, date(2026, 6, 1)) == STATUS_WITH_RIGHTS


def test_status_pending_payment_after_ex_until_paid():
    dividend = parse_cash_dividend(PETR_JCP)
    assert dividend_status(dividend, date(2026, 7, 15)) == STATUS_PENDING_PAYMENT
    assert dividend_status(dividend, date(2026, 8, 20)) == STATUS_PAID


def test_status_tbd_payment_is_always_pending():
    dividend = parse_cash_dividend(BBDC_TBD)
    assert dividend_status(dividend, date(2027, 1, 1)) == STATUS_PENDING_PAYMENT


def test_isin_class_matching():
    assert isin_matches_ticker("BRPETRACNPR6", "PETR4")
    assert not isin_matches_ticker("BRPETRACNOR9", "PETR4")  # ON class, we track the PN
    assert isin_matches_ticker("BRMGLUACNOR2", "MGLU3")


def test_empty_body_is_an_error():
    # B3 answers HTTP 200 with an empty body for bad requests.
    def handler(request):
        return httpx.Response(200, content=b"")

    with (
        httpx.Client(transport=httpx.MockTransport(handler)) as client,
        pytest.raises(CorporateActionsError, match="empty body"),
    ):
        fetch_supplement("PETR", client)


def test_non_json_body_is_an_error():
    def handler(request):
        return httpx.Response(200, content=b"<html>blocked</html>")

    with (
        httpx.Client(transport=httpx.MockTransport(handler)) as client,
        pytest.raises(CorporateActionsError, match="non-JSON"),
    ):
        fetch_supplement("PETR", client)


def test_supplement_unwraps_single_element_list():
    def handler(request):
        return httpx.Response(200, json=[{"cashDividends": [PETR_JCP]}])

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        supplement = fetch_supplement("PETR", client)
    assert "cashDividends" in supplement
