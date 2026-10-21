FROM public.ecr.aws/docker/library/python:3.13-slim-bookworm@sha256:01f42367a0a94ad4bc17111776fd66e3500c1d87c15bbd6055b7371d39c124fb

WORKDIR /tests

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        util-linux \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
        pytest==8.3.4 \
        pytest-json-ctrf==0.5.3

RUN useradd --system --uid 12000 --user-group --no-create-home --shell /usr/sbin/nologin sandbox \
    && mkdir -p /work \
    && chown root:root /work \
    && chmod 755 /work

RUN mkdir -p /app

COPY test_outputs.py conftest.py test.sh agent_bridge.py agent_runner.py /tests/
COPY holdout/ /tests/holdout/
RUN chmod 700 /tests /tests/holdout

RUN chmod +x /tests/test.sh

ENV PYTHONPATH=/tests
