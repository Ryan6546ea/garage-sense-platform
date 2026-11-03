"""Telemetry-consumer process entry point."""

import logging

import uvicorn

from telemetry_consumer.api import create_app
from telemetry_consumer.config import ConsumerSettings


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    settings = ConsumerSettings()
    uvicorn.run(create_app(settings), host=settings.api_host, port=settings.api_port)


if __name__ == "__main__":
    main()
