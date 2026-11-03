FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY packages/sim-core /opt/packages/sim-core
COPY services/telemetry-consumer /app/services/telemetry-consumer
COPY schemas /app/schemas

RUN --mount=type=cache,target=/root/.cache/pip \
    pip install /opt/packages/sim-core \
    && pip install /app/services/telemetry-consumer \
    && useradd --create-home --uid 10008 appuser

USER appuser

CMD ["python", "-m", "telemetry_consumer.main"]
