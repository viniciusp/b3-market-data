"""Logging configuration shared by all ingestion entrypoints."""

import logging
import os


def configure_logging() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
    )
    # httpx logs one INFO line per request, duplicating our own fetch logs.
    logging.getLogger("httpx").setLevel(logging.WARNING)
