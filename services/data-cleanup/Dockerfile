FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
WORKDIR /app
COPY services/data-cleanup /app/services/data-cleanup
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install /app/services/data-cleanup \
    && useradd --create-home --uid 10009 appuser
USER appuser
CMD ["python", "-m", "data_cleanup.main"]
