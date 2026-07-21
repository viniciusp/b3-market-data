"""Download B3 per-ticker trade files (arquivos.b3.com.br/rapinegocios/tickercsv).

For past sessions the file is final; for the current session B3 serves a cumulative
~15-min-delayed snapshot whose inner filename carries the cutoff (e.g. `..._PETR4_1046.txt`).
"""

import io
import logging
import random
import zipfile
from dataclasses import dataclass
from datetime import date
from time import sleep

import httpx

logger = logging.getLogger(__name__)

BASE_URL = "https://arquivos.b3.com.br/rapinegocios/tickercsv"
REQUEST_TIMEOUT_SECONDS = 60
MAX_ATTEMPTS = 3
BACKOFF_BASE_SECONDS = 1.0


class TradeFileError(Exception):
    """Base class: a trade file could not be fetched this cycle."""


class FileUnavailableError(TradeFileError):
    """No usable file: not published yet (404) or expired (B3 answers 200 with empty body)."""


class FetchError(TradeFileError):
    """B3 kept failing (5xx, network errors) or answered with an unexpected status."""


@dataclass(frozen=True)
class TradeFile:
    ticker: str
    session_date: date
    filename: str
    lines: list[str]  # data rows, header excluded


def fetch_trade_file(ticker: str, session_date: date, client: httpx.Client) -> TradeFile:
    response = _get_with_retry(f"{BASE_URL}/{ticker}/{session_date.isoformat()}", client)
    if response.status_code == 404:
        raise FileUnavailableError(f"{ticker} {session_date}: not published (404)")
    if response.status_code != 200:
        raise FetchError(f"{ticker} {session_date}: unexpected HTTP {response.status_code}")
    ensure_valid_download(response.headers, response.content)
    filename, lines = extract_lines(response.content)
    return TradeFile(ticker=ticker, session_date=session_date, filename=filename, lines=lines)


def ensure_valid_download(headers: httpx.Headers | dict, content: bytes) -> None:
    # Expired files (>20 sessions old) come back as HTTP 200 with an EMPTY body and no
    # content-disposition; must be treated as unavailable, never as success.
    if not content or "content-disposition" not in headers:
        raise FileUnavailableError("empty body / missing content-disposition (expired file?)")


def extract_lines(zip_bytes: bytes) -> tuple[str, list[str]]:
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as archive:
        filename = archive.namelist()[0]
        text = archive.read(filename).decode("ascii")
    _header, *data = text.splitlines()
    return filename, data


def _get_with_retry(url: str, client: httpx.Client) -> httpx.Response:
    """Retry transient failures (5xx, network errors) with exponential backoff.

    4xx responses are semantic answers and return immediately. Attempts stay
    bounded because the polling cycle itself is the outer retry loop.
    """
    last_error = ""
    for attempt in range(MAX_ATTEMPTS):
        if attempt > 0:
            delay = BACKOFF_BASE_SECONDS * 2 ** (attempt - 1) + random.uniform(0, 0.5)
            logger.warning(
                "retrying url=%s attempt=%d/%d delay=%.1fs last_error=%s",
                url,
                attempt + 1,
                MAX_ATTEMPTS,
                delay,
                last_error,
            )
            sleep(delay)
        try:
            response = client.get(url)
        except httpx.TransportError as exc:
            last_error = repr(exc)
            continue
        if response.status_code >= 500:
            last_error = f"HTTP {response.status_code}"
            continue
        return response
    raise FetchError(f"{url}: giving up after {MAX_ATTEMPTS} attempts, last_error={last_error}")
