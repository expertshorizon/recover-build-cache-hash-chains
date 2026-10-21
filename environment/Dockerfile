FROM public.ecr.aws/docker/library/python:3.13-slim-bookworm@sha256:01f42367a0a94ad4bc17111776fd66e3500c1d87c15bbd6055b7371d39c124fb

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        asciinema \
        tmux \
    && rm -rf /var/lib/apt/lists/*

# requirements.lock is intentionally empty: the ledger package only uses the
# standard library (csv, sqlite3, hashlib, json). Kept as a no-op install so a
# future third-party dependency has a pinned lock file to land in.
COPY requirements.lock /tmp/requirements.lock
RUN pip install --no-cache-dir -r /tmp/requirements.lock

COPY app/src/ /app/src/
COPY app/data/ /app/data/

ENV PYTHONPATH=/app/src
