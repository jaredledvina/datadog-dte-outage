FROM ghcr.io/astral-sh/uv:python3.14-bookworm-slim AS builder

WORKDIR /app

# Enable bytecode compilation for faster startup
ENV UV_COMPILE_BYTECODE=1

# Copy from the cache instead of linking since it's a mounted cache
ENV UV_LINK_MODE=copy

# Copy dependency files first for better layer caching
COPY pyproject.toml uv.lock ./

# Install dependencies (without dev dependencies)
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project --no-dev

# Copy application code
COPY main.py ./

# Sync the project
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev

FROM python:3.14-slim-bookworm

WORKDIR /app

# Pull in the current ca-certificates (eMudhra intermediate compromise, Oct 2026)
RUN apt-get update \
    && apt-get install -y --no-install-recommends --only-upgrade ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Version info from build args
ARG APP_VERSION=dev
ENV APP_VERSION=${APP_VERSION}

# Create non-root user for security
RUN useradd --create-home --shell /bin/bash app

# Copy the virtual environment from builder
COPY --from=builder --chown=app:app /app/.venv /app/.venv

# Copy application code
COPY --chown=app:app main.py ./

# Use the virtual environment
ENV PATH="/app/.venv/bin:$PATH"

# Switch to non-root user
USER app

ENTRYPOINT ["python", "main.py"]
