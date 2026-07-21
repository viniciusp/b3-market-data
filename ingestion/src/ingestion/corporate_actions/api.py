"""Fetch corporate actions (dividends, splits) from B3's listed-companies API.

GET {BASE_URL}/{Method}/{payload} where payload is base64 of a compact JSON object.
The API signals errors as HTTP 200 with an EMPTY body, never as an error status.
"""

import base64
import json
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from urllib.parse import quote

import httpx

BASE_URL = "https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall"
PAYMENT_TBD_SENTINEL = "31/12/9999"  # "data a definir": a real pending event, never garbage

STATUS_WITH_RIGHTS = "with_rights"
STATUS_PENDING_PAYMENT = "pending_payment"
STATUS_PAID = "paid"


class CorporateActionsError(Exception):
    """The API answered with something unusable: empty body, bad JSON, missing fields."""


@dataclass(frozen=True)
class CashDividend:
    isin: str
    label: str  # DIVIDENDO | JRS CAP PROPRIO | RENDIMENTO
    rate: Decimal  # gross value per share (JCP is taxed 15% at source)
    approved_on: date
    last_date_prior: date  # last day trading WITH the right; ex is the next session
    payment_date: date | None  # None = payment date still to be defined
    related_to: str


@dataclass(frozen=True)
class StockDividend:
    isin: str
    label: str  # DESDOBRAMENTO | GRUPAMENTO | BONIFICACAO
    factor: Decimal
    approved_on: date
    last_date_prior: date


def fetch_supplement(issuing_company: str, client: httpx.Client) -> dict:
    payload = {"issuingCompany": issuing_company, "language": "pt-br"}
    encoded = base64.b64encode(json.dumps(payload, separators=(",", ":")).encode()).decode()
    # Base64 may contain '/' or '+', which would corrupt the URL path.
    response = client.get(f"{BASE_URL}/GetListedSupplementCompany/{quote(encoded, safe='=')}")
    response.raise_for_status()
    if not response.content:
        raise CorporateActionsError(f"{issuing_company}: empty body (bad request or API change)")
    try:
        body = response.json()
    except ValueError as exc:
        raise CorporateActionsError(f"{issuing_company}: non-JSON body") from exc
    if not body:
        raise CorporateActionsError(f"{issuing_company}: empty supplement")
    return body[0] if isinstance(body, list) else body


def parse_supplement(
    raw: dict,
) -> tuple[list[CashDividend], list[StockDividend], list[str]]:
    """Parse both event lists, partitioning out rows that do not match the schema."""
    errors: list[str] = []
    cash: list[CashDividend] = []
    stock: list[StockDividend] = []
    for row in raw.get("cashDividends") or []:
        try:
            cash.append(parse_cash_dividend(row))
        except CorporateActionsError as exc:
            errors.append(str(exc))
    for row in raw.get("stockDividends") or []:
        try:
            stock.append(parse_stock_dividend(row))
        except CorporateActionsError as exc:
            errors.append(str(exc))
    return cash, stock, errors


def parse_cash_dividend(raw: dict) -> CashDividend:
    try:
        return CashDividend(
            isin=raw["isinCode"],
            label=raw["label"],
            rate=_decimal(raw["rate"]),
            approved_on=_date_br(raw["approvedOn"]),
            last_date_prior=_date_br(raw["lastDatePrior"]),
            payment_date=(
                None if raw["paymentDate"] == PAYMENT_TBD_SENTINEL else _date_br(raw["paymentDate"])
            ),
            related_to=raw.get("relatedTo", ""),
        )
    except (KeyError, ValueError, InvalidOperation) as exc:
        raise CorporateActionsError(f"bad cash dividend row: {exc!r} in {raw!r}") from exc


def parse_stock_dividend(raw: dict) -> StockDividend:
    try:
        return StockDividend(
            isin=raw["isinCode"],
            label=raw["label"],
            factor=_decimal(raw["factor"]),
            approved_on=_date_br(raw["approvedOn"]),
            last_date_prior=_date_br(raw["lastDatePrior"]),
        )
    except (KeyError, ValueError, InvalidOperation) as exc:
        raise CorporateActionsError(f"bad stock dividend row: {exc!r} in {raw!r}") from exc


def dividend_status(dividend: CashDividend, today: date) -> str:
    if today <= dividend.last_date_prior:
        return STATUS_WITH_RIGHTS
    if dividend.payment_date is None or dividend.payment_date > today:
        return STATUS_PENDING_PAYMENT
    return STATUS_PAID


_CLASS_CODES = {"3": "ACNOR", "4": "ACNPR"}  # ON / PN; units (11) are not in the watchlist


def isin_matches_ticker(isin: str, ticker: str) -> bool:
    """BRPETRACNPR6 = BR + issuer root (PETR) + class code (ACNPR = PN) + check digit."""
    root, class_digit = ticker[:-1], ticker[-1]
    code = _CLASS_CODES.get(class_digit)
    return code is not None and isin.startswith(f"BR{root}{code}")


def _decimal(text: str) -> Decimal:
    # pt-br locale: dot is the thousands separator, comma is the decimal separator
    # (e.g. Bradesco's 2009 split factor "4.900,00000000000" = 4900).
    return Decimal(text.replace(".", "").replace(",", "."))


def _date_br(text: str) -> date:
    return datetime.strptime(text, "%d/%m/%Y").date()
