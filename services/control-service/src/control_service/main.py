"""Process entry point for the M5 control service."""

import logging

import uvicorn

from control_service.api import create_app
from control_service.config import ControlSettings


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    settings = ControlSettings()
    uvicorn.run(
        create_app(settings),
        host=settings.api_host,
        port=settings.api_port,
    )


if __name__ == "__main__":
    main()
