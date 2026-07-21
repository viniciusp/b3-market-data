from ingestion.watchlist import WATCHLIST


def test_tracks_at_least_ten_companies():
    assert len({t.company for t in WATCHLIST}) >= 10


def test_includes_required_companies():
    companies = {t.company for t in WATCHLIST}
    assert "Petrobras" in companies
    assert "Magazine Luiza" in companies


def test_tickers_are_unique():
    tickers = [t.ticker for t in WATCHLIST]
    assert len(tickers) == len(set(tickers))
