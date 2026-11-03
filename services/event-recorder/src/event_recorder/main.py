"""Process entry point for the event recorder."""

import logging

import uvicorn

from event_recorder.api import create_app
from event_recorder.config import RecorderSettings


def main() -> None:
    """Load strict configuration and run FastAPI."""

    logging.basicConfig(level=logging.INFO)
    settings = RecorderSettings()
    uvicorn.run(
        create_app(settings),
        host=settings.api_host,
        port=settings.api_port,
    )


if __name__ == "__main__":
    main()
