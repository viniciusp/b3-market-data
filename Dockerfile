FROM ghcr.io/astral-sh/uv:python3.14-bookworm-slim

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Dependency layer: rebuilt only when lockfile or manifests change.
COPY pyproject.toml uv.lock ./
COPY ingestion/pyproject.toml ingestion/pyproject.toml
COPY api/pyproject.toml api/pyproject.toml
RUN uv sync --frozen --no-dev --no-install-workspace

# Source layer.
COPY ingestion ingestion
COPY api api
RUN uv sync --frozen --no-dev

ENV PATH="/app/.venv/bin:$PATH"

CMD ["poller"]
