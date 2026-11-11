FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    LAKEHOUSE_HOME=/data \
    DAGSTER_HOME=/opt/dagster

WORKDIR /app
COPY --from=ghcr.io/astral-sh/uv:0.11.32 /uv /usr/local/bin/uv

# dependencies first, so code changes don't reinstall them
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project
COPY src ./src
RUN uv sync --frozen --no-dev

RUN mkdir -p /data "$DAGSTER_HOME" && touch "$DAGSTER_HOME/dagster.yaml"
ENV PATH="/app/.venv/bin:$PATH"
VOLUME ["/data"]
EXPOSE 3000

# default: the Dagster UI and daemon (schedules, automation). `docker compose run lakehouse lakehouse ...` for the CLI.
CMD ["dagster", "dev", "-h", "0.0.0.0", "-p", "3000", "-m", "lakehouse.definitions"]
