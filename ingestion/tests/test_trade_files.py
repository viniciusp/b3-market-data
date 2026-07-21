import io
import zipfile
from datetime import date

import httpx
import pytest

from ingestion.trades.files import (
    FetchError,
    FileUnavailableError,
    ensure_valid_download,
    extract_lines,
    fetch_trade_file,
)

HEADER = "DataReferencia;CodigoInstrumento;AcaoAtualizacao;PrecoNegocio;..."
ROW_1 = "2026-07-14;PETR4;0;41,200;5500;100719126;10;1;2026-07-14;85;8"
ROW_2 = "2026-07-14;PETR4;0;41,120;300;101602106;20;1;2026-07-14;85;120"


def zip_with(text: str, filename: str = "14-07-2026_NEGOCIOSAVISTA_PETR4_1046.txt") -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(filename, text)
    return buffer.getvalue()


def test_extract_lines_drops_header_and_keeps_data():
    filename, lines = extract_lines(zip_with(f"{HEADER}\n{ROW_1}\n{ROW_2}"))
    assert filename == "14-07-2026_NEGOCIOSAVISTA_PETR4_1046.txt"
    assert lines == [ROW_1, ROW_2]


def test_empty_body_is_unavailable_not_success():
    # Expired files return HTTP 200 with an empty body; must not be treated as success.
    with pytest.raises(FileUnavailableError):
        ensure_valid_download({"content-disposition": "attachment; ..."}, b"")


def test_missing_content_disposition_is_unavailable():
    with pytest.raises(FileUnavailableError):
        ensure_valid_download({}, zip_with(f"{HEADER}\n{ROW_1}"))


def test_valid_download_passes():
    ensure_valid_download({"content-disposition": "attachment; ..."}, zip_with(HEADER))


SESSION = date(2026, 7, 14)
OK_HEADERS = {"content-disposition": "attachment; filename=x.zip"}


def fetch_via(handler) -> object:
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        return fetch_trade_file("PETR4", SESSION, client)


@pytest.fixture(autouse=True)
def no_backoff_sleep(monkeypatch):
    monkeypatch.setattr("ingestion.trades.files.sleep", lambda _: None)


def test_5xx_is_retried_until_success():
    calls = []

    def handler(request):
        calls.append(request.url)
        if len(calls) < 3:
            return httpx.Response(504)
        return httpx.Response(200, content=zip_with(f"{HEADER}\n{ROW_1}"), headers=OK_HEADERS)

    trade_file = fetch_via(handler)
    assert len(calls) == 3
    assert trade_file.lines == [ROW_1]


def test_network_error_is_retried():
    calls = []

    def handler(request):
        calls.append(request.url)
        if len(calls) == 1:
            raise httpx.ConnectError("connection refused")
        return httpx.Response(200, content=zip_with(f"{HEADER}\n{ROW_1}"), headers=OK_HEADERS)

    trade_file = fetch_via(handler)
    assert len(calls) == 2
    assert trade_file.lines == [ROW_1]


def test_gives_up_after_max_attempts():
    calls = []

    def handler(request):
        calls.append(request.url)
        return httpx.Response(504)

    with pytest.raises(FetchError, match="giving up"):
        fetch_via(handler)
    assert len(calls) == 3


def test_404_is_not_retried():
    calls = []

    def handler(request):
        calls.append(request.url)
        return httpx.Response(404)

    with pytest.raises(FileUnavailableError):
        fetch_via(handler)
    assert len(calls) == 1


def test_unexpected_4xx_is_not_retried():
    def handler(request):
        return httpx.Response(403)

    with pytest.raises(FetchError, match="HTTP 403"):
        fetch_via(handler)
