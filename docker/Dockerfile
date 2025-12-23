# syntax=docker/dockerfile:1.4
# check=error=true
#
# Multi-version PostgreSQL client Dockerfile
# Build with:
#  - default: docker buildx build --build-arg PG_VERSION=15 -t padmy:pg15 -f docker/Dockerfile .
#  - with networkx: docker buildx build --build-arg PG_VERSION=15 --build-arg UV_PARAMS="--group network" -t padmy:pg15 -f docker/Dockerfile .
# Supported PG_VERSION values: 15, 16, 17, 18
#
FROM ghcr.io/astral-sh/uv:python3.14-alpine AS builder

WORKDIR /app

ENV UV_SYSTEM_PYTHON=1
ENV UV_PROJECT_ENVIRONMENT=/usr/local

ARG UV_PARAMS=""

RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-install-project --no-dev ${UV_PARAMS}

# Copy project files
COPY pyproject.toml uv.lock README.md ./
COPY padmy/ padmy/

# Install the project to system Python
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev ${UV_PARAMS}


FROM python:3.14-alpine AS main


WORKDIR /app

# PostgreSQL version to install (15, 16, 17 or 18)
ARG PG_VERSION=17

# Install PostgreSQL client based on version
# PG 15 requires v3.22/main repo, PG 16+ requires edge/main repo
RUN apk --no-cache add bash && \
    if [ "$PG_VERSION" = "15" ]; then \
        apk --no-cache add postgresql15-client \
            -X https://dl-cdn.alpinelinux.org/alpine/v3.22/main \
            -X https://dl-cdn.alpinelinux.org/alpine/v3.22/community; \
    else \
        apk --no-cache add postgresql${PG_VERSION}-client -X https://dl-cdn.alpinelinux.org/alpine/edge/main; \
    fi && \
    addgroup user && \
    adduser -s /bin/bash -D -G user user

COPY --from=builder /usr/local/lib/python3.14/site-packages/ /usr/local/lib/python3.14/site-packages/
COPY --from=builder /usr/local/bin/ /usr/local/bin/

COPY padmy/ padmy/

USER user

EXPOSE 5555

ENTRYPOINT ["python", "-m", "padmy"]
