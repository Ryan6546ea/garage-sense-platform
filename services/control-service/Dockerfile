FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY packages/sim-core /opt/packages/sim-core
COPY services/control-service /opt/services/control-service
COPY schemas /app/schemas

RUN --mount=type=cache,target=/root/.cache/pip \
    pip install /opt/packages/sim-core \
    && pip install /opt/services/control-service \
    && useradd --create-home --uid 10003 appuser \
    && mkdir -p /var/lib/garage-simulation/exports \
    && chown -R appuser:appuser /var/lib/garage-simulation

USER appuser

CMD ["python", "-m", "control_service.main"]
