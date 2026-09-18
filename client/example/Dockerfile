# syntax=docker/dockerfile:1
# The lclstream_api client only. CrystFEL runs from its own upstream image,
# so its arguments and geometry change without rebuilding anything.
FROM python:3.13-slim AS builder
# Pinned, per uv's Docker guide; bump deliberately.
COPY --from=ghcr.io/astral-sh/uv:0.12.17 /uv /bin/uv

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=0

# git for the client's git+https dependency, certs for git to verify it.
RUN apt-get update \
    && apt-get install -y --no-install-recommends git ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# TODO: point at @main once this branch merges
RUN --mount=type=cache,target=/root/.cache/uv \
    uv venv /app/.venv \
    && VIRTUAL_ENV=/app/.venv uv pip install \
        pydantic-settings \
        "lclstream-api-client @ git+https://github.com/lclstream/lclstream_api.git@crystfel-nersc#subdirectory=client/"

FROM python:3.13-slim
# The environment only; no build tools, no git, no uv.
COPY --from=builder /app/.venv /app/.venv
COPY transfer_client.py /app/transfer_client.py
# Config and token come from /cfg; nothing is written here.
WORKDIR /cfg
# Unbuffered, or the job log shows nothing until the process ends.
ENTRYPOINT ["/app/.venv/bin/python", "-u", "/app/transfer_client.py"]
