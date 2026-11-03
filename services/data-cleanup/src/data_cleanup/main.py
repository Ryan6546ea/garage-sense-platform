"""Run immediately on startup, then wait between retention cycles."""

import asyncio
import logging
import signal
from contextlib import suppress

from sqlalchemy.ext.asyncio import create_async_engine

from data_cleanup.config import CleanupSettings, load_policy
from data_cleanup.worker import run_cycle

LOGGER = logging.getLogger(__name__)


async def serve(settings: CleanupSettings, stop: asyncio.Event) -> None:
    policy = load_policy(settings.config_path)  # Invalid initial policy fails fast.
    engine = create_async_engine(
        settings.database_url, pool_pre_ping=True, pool_size=1, max_overflow=0,
        connect_args={"timeout": 10},
    )
    try:
        while not stop.is_set():
            try:
                policy = load_policy(settings.config_path)
            except (OSError, ValueError) as error:
                LOGGER.error("cleanup_config_invalid error=%s", type(error).__name__)
            else:
                await run_cycle(engine, policy, stop)
            with suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=policy.interval_seconds)
    finally:
        await engine.dispose()


async def main() -> None:
    settings = CleanupSettings()
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, lambda *_: loop.call_soon_threadsafe(stop.set))
    await serve(settings, stop)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    asyncio.run(main())
