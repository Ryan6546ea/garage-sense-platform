"""Bounded transactions, transaction-local retention exception, and graceful stop."""

import asyncio
import logging
from datetime import UTC, datetime, timedelta

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from data_cleanup.config import CleanupPolicy

LOGGER = logging.getLogger(__name__)
SENSOR_TABLES = (
    "telemetry_laser_distance_events",
    "telemetry_current_events",
    "telemetry_vibration_events",
    "telemetry_environment_events",
    "telemetry_vision_events",
    "telemetry_mechanism_state_events",
)
IDENTITIES = "telemetry_event_identities"
FAILURES = "telemetry_ingest_failures"


def deletion_sql(table: str) -> str:
    """Identifiers are allowlisted; all runtime values are bound parameters."""
    if table not in (*SENSOR_TABLES, IDENTITIES, FAILURES):
        raise ValueError("Unsupported retention table")
    predicates = ""
    join = ""
    locks = "t"
    references: list[tuple[str, str]] = []
    if table == IDENTITIES:
        references.extend((name, "id") for name in SENSOR_TABLES)
    if table != FAILURES:
        references.extend(
            (("replay_items", "source_event_id"), ("export_items", "source_event_id"))
        )
        predicates = " ".join(
            f"AND NOT EXISTS (SELECT 1 FROM {name} r WHERE r.{key} = t.id)"
            for name, key in references
        )
    if table in SENSOR_TABLES:
        join = "JOIN telemetry_event_identities i ON i.id = t.id"
        locks = "t, i"
    return f"""
        WITH candidates AS (
            SELECT t.id FROM {table} t {join}
            WHERE t.received_at < :cutoff {predicates}
            ORDER BY t.received_at, t.id
            LIMIT :batch_size FOR UPDATE OF {locks} SKIP LOCKED
        ), removed AS (
            DELETE FROM {table} t USING candidates c WHERE t.id = c.id RETURNING t.id
        ) SELECT id FROM removed
    """


async def delete_batch(
    engine: AsyncEngine, table: str, cutoff: datetime, policy: CleanupPolicy,
) -> int:
    async with engine.begin() as connection:
        await connection.execute(
            text("SELECT set_config('statement_timeout', :timeout, true)"),
            {"timeout": f"{policy.statement_timeout_seconds}s"},
        )
        # Serializes cleanup replicas per batch; ingestion remains independent.
        locked = await connection.scalar(text("SELECT pg_try_advisory_xact_lock(73100413)"))
        if not locked:
            return 0
        await connection.execute(
            text("SELECT set_config('garage.retention_cutoff', :cutoff, true)"),
            {"cutoff": cutoff.isoformat()},
        )
        result = await connection.execute(
            text(deletion_sql(table)), {"cutoff": cutoff, "batch_size": policy.batch_size},
        )
        ids = list(result.scalars())
        if ids and table in SENSOR_TABLES:
            # Delete identities in the same transaction while their row locks are held.
            # Concurrent snapshot creation must win the FK lock or fail atomically.
            await connection.execute(
                text(
                    "DELETE FROM telemetry_event_identities WHERE id = ANY(CAST(:ids AS bigint[]))"
                ),
                {"ids": ids},
            )
        return len(ids)


async def run_cycle(
    engine: AsyncEngine, policy: CleanupPolicy, stop: asyncio.Event,
    *, now: datetime | None = None,
) -> dict[str, int]:
    totals: dict[str, int] = {}
    if not policy.enabled:
        return totals
    current = now or datetime.now(UTC)
    if current.utcoffset() is None:
        raise ValueError("Retention time must have a UTC offset")
    targets = [(table, policy.sensor_retention_days) for table in (*SENSOR_TABLES, IDENTITIES)]
    if policy.ingest_failure_retention_days is not None:
        targets.append((FAILURES, policy.ingest_failure_retention_days))
    for table, days in targets:
        cutoff = current - timedelta(days=days)
        totals[table] = 0
        for _ in range(policy.max_batches_per_table):
            if stop.is_set():
                return totals
            try:
                count = await delete_batch(engine, table, cutoff, policy)
            except Exception as error:
                # Do not log driver exception text, which can include SQL parameters.
                LOGGER.error("cleanup_failed table=%s error=%s", table, type(error).__name__)
                break
            totals[table] += count
            if count < policy.batch_size:
                break
        LOGGER.info("cleanup table=%s cutoff=%s deleted=%s", table, cutoff, totals[table])
    return totals
