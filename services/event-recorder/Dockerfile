FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY packages/sim-core /opt/packages/sim-core
COPY services/event-recorder /app/services/event-recorder
COPY schemas /app/schemas
COPY alembic.ini /app/alembic.ini

RUN --mount=type=cache,target=/root/.cache/pip \
    pip install /opt/packages/sim-core \
    && pip install /app/services/event-recorder \
    && useradd --create-home --uid 10002 appuser

USER appuser

CMD ["sh", "-c", "alembic -c /app/alembic.ini upgrade head && python -m event_recorder.main"]
