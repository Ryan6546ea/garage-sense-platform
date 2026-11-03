"""Bounded internal HTTP catalog polling and consumer-before-producer handshake."""

import asyncio
import contextlib
import logging
from collections.abc import Awaitable, Callable
from typing import Literal
from uuid import uuid4

import httpx

from sim_core.catalog import CatalogSnapshot, RuntimeReport
from sim_core.identity_api import AuthSettings
from sim_core.registry import install_catalog

LOGGER = logging.getLogger(__name__)


def apply_identities(snapshot: CatalogSnapshot) -> None:
    install_catalog(
        tuple(item.definition() for item in snapshot.sensors),
        tuple(item.garage_id for item in snapshot.garages),
        frozenset(
            item.sensor_id
            for item in snapshot.sensors
            if item.desired_enabled and not item.archived
        ),
    )


class CatalogClient:
    def __init__(
        self,
        base_url: str,
        service: Literal["simulator", "consumer"],
        apply: Callable[[CatalogSnapshot], Awaitable[None]] | None = None,
    ) -> None:
        self._http = httpx.AsyncClient(
            base_url=base_url.rstrip("/"), timeout=5, trust_env=False,
            headers={"X-Service-Token": AuthSettings().service_token},
        )
        self._service = service
        self._apply = apply
        self._instance = str(uuid4())
        self._revision = -1
        self._task: asyncio.Task[None] | None = None
        self._lock = asyncio.Lock()
        self.healthy = False

    async def refresh(self) -> bool:
        async with self._lock:
            revision = await self._http.get("/api/v1/internal/registry/revision")
            revision.raise_for_status()
            if self._revision >= 0 and revision.json()["revision"] == self._revision:
                await self._report(None)
                self.healthy = True
                return True
            response = await self._http.get("/api/v1/internal/registry/snapshot")
            response.raise_for_status()
            snapshot = CatalogSnapshot.model_validate(response.json())
            if snapshot.revision != self._revision:
                if self._service == "simulator":
                    status = await self._http.get("/api/v1/internal/registry/runtime")
                    status.raise_for_status()
                    consumers = [
                        item for item in status.json()["items"] if item["service"] == "consumer"
                    ]
                    if not any(
                        item["fresh"]
                        and item["error"] is None
                        and item["appliedRevision"] >= snapshot.revision
                        for item in consumers
                    ):
                        return False
                # The producer callback installs identities under its publish lock.
                if self._apply is not None:
                    await self._apply(snapshot)
                else:
                    apply_identities(snapshot)
                self._revision = snapshot.revision
            await self._report(None)
            self.healthy = True
            return True

    async def _report(self, error: str | None) -> None:
        report = RuntimeReport(
            service=self._service,
            instance_id=self._instance,
            applied_revision=max(0, self._revision),
            error=error,
        )
        response = await self._http.post(
            "/api/v1/internal/registry/runtime", json=report.model_dump(by_alias=True)
        )
        response.raise_for_status()

    async def initialise(self) -> None:
        while True:
            try:
                if await self.refresh():
                    return
            except (httpx.HTTPError, ValueError):
                LOGGER.warning("waiting for registry service")
            await asyncio.sleep(2)

    async def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name=f"registry-{self._service}")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
        await self._http.aclose()

    async def _run(self) -> None:
        while True:
            try:
                await self.refresh()
            except Exception as exc:
                self.healthy = False
                LOGGER.exception("registry reconciliation failed")
                with contextlib.suppress(httpx.HTTPError):
                    await self._report(type(exc).__name__)
            await asyncio.sleep(2)
