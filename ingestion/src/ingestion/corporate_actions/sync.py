"""Sync corporate actions from B3 into Postgres for every watched company."""

import logging
from datetime import UTC, datetime

import httpx

from ingestion.corporate_actions import storage
from ingestion.corporate_actions.api import (
    STATUS_PAID,
    CorporateActionsError,
    dividend_status,
    fetch_supplement,
    isin_matches_ticker,
    parse_supplement,
)
from ingestion.timezones import SAO_PAULO
from ingestion.watchlist import WATCHLIST

logger = logging.getLogger(__name__)


def sync_corporate_actions(conn, client: httpx.Client) -> int:
    """Refresh every watched company; returns the number of issuers that failed."""
    today = datetime.now(UTC).astimezone(SAO_PAULO).date()
    issuers = {t.issuing_company: t.ticker for t in WATCHLIST}
    failures = 0
    for issuing_company, ticker in issuers.items():
        try:
            supplement = fetch_supplement(issuing_company, client)
        except (CorporateActionsError, httpx.HTTPError) as exc:
            # Fetch failures keep last-known-good rows in Postgres untouched.
            logger.error("fetch failed issuer=%s reason=%s", issuing_company, exc)
            failures += 1
            continue
        all_cash, all_stock, errors = parse_supplement(supplement)
        for error in errors:
            logger.warning("malformed row issuer=%s error=%s", issuing_company, error)
        cash = [d for d in all_cash if isin_matches_ticker(d.isin, ticker)]
        stock = [s for s in all_stock if isin_matches_ticker(s.isin, ticker)]
        storage.replace_corporate_actions(conn, ticker, cash, stock)
        pending = [d for d in cash if dividend_status(d, today) != STATUS_PAID]
        for dividend in pending:
            logger.info(
                "%s ticker=%s type=%s gross=%s buy_by=%s payment=%s approved=%s",
                dividend_status(dividend, today).upper(),
                ticker,
                dividend.label,
                dividend.rate,
                dividend.last_date_prior,
                dividend.payment_date or "TBD",
                dividend.approved_on,
            )
        logger.info(
            "stored ticker=%s cash=%d stock=%d pending=%d",
            ticker,
            len(cash),
            len(stock),
            len(pending),
        )
    return failures
