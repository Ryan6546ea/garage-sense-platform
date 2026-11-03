"""Transactional registration, immutable identities and append-only change audit."""

import json
from typing import Any
from uuid import uuid4

from sim_core.catalog import (
    TYPE_RULES,
    AreaCreate,
    CatalogSnapshot,
    DeviceCreate,
    GarageCreate,
    RegisteredSensor,
    RuntimeReport,
    SensorCreate,
    SensorPatch,
)
from sim_core.registry import install_catalog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


class CatalogConflict(ValueError):
    pass


class CatalogNotFound(ValueError):
    pass


def install_snapshot(snapshot: CatalogSnapshot) -> None:
    install_catalog(
        tuple(sensor.definition() for sensor in snapshot.sensors),
        tuple(garage.garage_id for garage in snapshot.garages),
        frozenset(
            sensor.sensor_id
            for sensor in snapshot.sensors
            if sensor.desired_enabled and not sensor.archived
        ),
    )


class CatalogRepository:
    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self.sessions = sessions

    async def revision(self) -> int:
        async with self.sessions() as session:
            return int(
                (
                    await session.execute(text("SELECT revision FROM registry_state WHERE id=1"))
                ).scalar_one()
            )

    async def snapshot(self) -> CatalogSnapshot:
        async with self.sessions.begin() as session:
            revision = (
                await session.execute(
                    text("SELECT revision FROM registry_state WHERE id = 1 FOR SHARE")
                )
            ).scalar_one()
            garages = (
                (
                    await session.execute(
                        text(
                            "SELECT garage_id, name, altitude_meters FROM garages ORDER BY "
                            "garage_id"
                        )
                    )
                )
                .mappings()
                .all()
            )
            sensors = (
                (
                    await session.execute(
                        text(
                            "SELECT sensor_id,device_id,garage_id,area_id,sensor_type,name,"
                            "point_code,role,"
                            "camera_id,topic,publish_interval_seconds,seed,parameters,desir"
                            "ed_enabled,"
                            "archived,config_version FROM sensor_instances ORDER BY sensor_id"
                        )
                    )
                )
                .mappings()
                .all()
            )
        return CatalogSnapshot(
            revision=revision,
            garages=[GarageCreate.model_validate(dict(row)) for row in garages],
            sensors=[RegisteredSensor.model_validate(dict(row)) for row in sensors],
        )

    async def _lock(self, session: AsyncSession) -> int:
        # Shared with execution lease acquisition: no check/start race.
        await session.execute(text("SELECT pg_advisory_xact_lock(7150015)"))
        return int(
            (
                await session.execute(
                    text("SELECT revision FROM registry_state WHERE id = 1 FOR UPDATE")
                )
            ).scalar_one()
        )

    async def _audit(
        self,
        session: AsyncSession,
        revision: int,
        entity: str,
        identifier: str,
        action: str,
        before: dict[str, Any] | None,
        after: dict[str, Any],
    ) -> None:
        await session.execute(
            text("UPDATE registry_state SET revision=:revision WHERE id=1"), {"revision": revision}
        )
        await session.execute(
            text(
                "INSERT INTO sensor_registration_events "
                "(revision,entity_type,entity_id,action,request_id,before_state,after_state) "
                "VALUES (:revision,:entity,:identifier,:action,:request_id,"
                "CAST(:before AS jsonb),CAST(:after AS jsonb))"
            ),
            {
                "revision": revision,
                "entity": entity,
                "identifier": identifier,
                "action": action,
                "request_id": uuid4(),
                "before": json.dumps(before) if before else None,
                "after": json.dumps(after),
            },
        )

    async def create_parent(
        self,
        payload: GarageCreate | AreaCreate | DeviceCreate,
        garage_id: str | None = None,
    ) -> dict[str, Any]:
        values = payload.model_dump()
        if isinstance(payload, GarageCreate):
            table, identifier = "garages", payload.garage_id
        elif isinstance(payload, AreaCreate):
            table, identifier = "garage_areas", f"{garage_id}/{payload.area_id}"
            values["garage_id"] = garage_id
        else:
            table, identifier = "devices", payload.device_id
            values["garage_id"] = garage_id
        async with self.sessions.begin() as session:
            revision = await self._lock(session)
            # All identifiers below come exclusively from the fixed models above.
            await session.execute(
                text(
                    f"INSERT INTO {table} ({','.join(values)}) "
                    f"VALUES ({','.join(':' + name for name in values)})"
                ),
                values,
            )
            await self._audit(
                session,
                revision + 1,
                table,
                identifier,
                "create",
                None,
                {**payload.model_dump(by_alias=True), "garageId": garage_id}
                if garage_id
                else payload.model_dump(by_alias=True),
            )
        return {
            **payload.model_dump(by_alias=True),
            **({"garageId": garage_id} if garage_id else {}),
        }

    async def create_sensor(self, garage_id: str, payload: SensorCreate) -> RegisteredSensor:
        async with self.sessions.begin() as session:
            revision = await self._lock(session)
            row = (
                (
                    await session.execute(
                        text(
                            "SELECT area_id FROM devices WHERE device_id=:device AND garage"
                            "_id=:garage"
                        ),
                        {"device": payload.device_id, "garage": garage_id},
                    )
                )
                .mappings()
                .first()
            )
            if row is None:
                raise CatalogNotFound("device does not belong to this garage")
            count = (
                await session.execute(text("SELECT count(*) FROM sensor_instances"))
            ).scalar_one()
            if count >= 10000:
                raise CatalogConflict("catalog capacity of 10000 sensor identities reached")
            topic_type = (
                "current"
                if payload.sensor_type == "current"
                else TYPE_RULES[payload.sensor_type][0]
            )
            sensor = RegisteredSensor(
                **payload.model_dump(),
                garage_id=garage_id,
                area_id=row["area_id"],
                topic=f"garage/{garage_id}/telemetry/{topic_type}/{payload.sensor_id}",
                config_version=1,
                archived=False,
            )
            values = sensor.model_dump()
            values["parameters"] = json.dumps(values["parameters"])
            placeholders = [
                "CAST(:parameters AS jsonb)" if key == "parameters" else ":" + key for key in values
            ]
            await session.execute(
                text(
                    f"INSERT INTO sensor_instances ({','.join(values)}) "
                    f"VALUES ({','.join(placeholders)})"
                ),
                values,
            )
            await self._audit(
                session,
                revision + 1,
                "sensor",
                sensor.sensor_id,
                "create",
                None,
                sensor.model_dump(by_alias=True),
            )
        return sensor

    async def change_sensor(
        self,
        sensor_id: str,
        version: int,
        action: str,
        patch: SensorPatch | None = None,
    ) -> RegisteredSensor:
        async with self.sessions.begin() as session:
            revision = await self._lock(session)
            row = (
                (
                    await session.execute(
                        text(
                            "SELECT sensor_id,device_id,garage_id,area_id,sensor_type,name,"
                            "point_code,role,"
                            "camera_id,topic,publish_interval_seconds,seed,parameters,desir"
                            "ed_enabled,"
                            "archived,config_version FROM sensor_instances WHERE sensor_id="
                            ":sensor"
                        ),
                        {"sensor": sensor_id},
                    )
                )
                .mappings()
                .first()
            )
            if row is None:
                raise CatalogNotFound("sensor does not exist")
            old = RegisteredSensor.model_validate(dict(row))
            if old.config_version != version:
                raise CatalogConflict("configVersion is stale; reload the sensor")
            if old.archived:
                raise CatalogConflict("archived sensor is immutable")
            # Existing scheduler/replay operates with a global simulator suspension.
            # Keep its exclusion boundary until per-garage execution is implemented.
            busy = (
                await session.execute(
                    text("SELECT EXISTS(SELECT 1 FROM environment_execution_leases)")
                )
            ).scalar_one()
            pending = (
                (
                    await session.execute(
                        text(
                            "SELECT configuration FROM simulation_runs WHERE status IN ('pe"
                            "nding','running','paused')"
                        )
                    )
                )
                .scalars()
                .all()
            )
            manual = (
                await session.execute(
                    text(
                        "SELECT EXISTS(SELECT 1 FROM simulation_scenario_activations "
                        "WHERE target_id IN (:target,:device,:garage) AND source='manual' "
                        "AND (status='pending' OR (status='succeeded' "
                        "AND expires_at > CURRENT_TIMESTAMP)))"
                    ),
                    {
                        "target": f"sensor:{sensor_id}",
                        "device": old.device_id,
                        "garage": f"{old.garage_id}-SIMULATION",
                    },
                )
            ).scalar_one()
            if (
                busy
                or manual
                or any(
                    sensor_id
                    in {
                        str(target).removeprefix("sensor:")
                        for target in cfg.get("allowedTargets", [])
                    }
                    or old.device_id in cfg.get("allowedTargets", [])
                    for cfg in pending
                )
            ):
                raise CatalogConflict(
                    "sensor is reserved by a pending/active run or replay; stop it first"
                )
            values = old.model_dump()
            if patch is not None:
                values.update(patch.model_dump(exclude_unset=True, exclude={"config_version"}))
            if action in {"enable", "disable", "archive"}:
                values["desired_enabled"] = action == "enable"
            if action == "archive":
                values["archived"] = True
            values["config_version"] = version + 1
            new = RegisteredSensor.model_validate(values)
            await session.execute(
                text(
                    "UPDATE sensor_instances SET name=:name,parameters=CAST(:parameters AS "
                    "jsonb),"
                    "publish_interval_seconds=:interval,seed=:seed,desired_enabled=:enabled,"
                    "archived=:archived,config_version=:version,updated_at=CURRENT_TIMESTAMP "
                    "WHERE sensor_id=:sensor"
                ),
                {
                    "sensor": sensor_id,
                    "name": new.name,
                    "parameters": json.dumps(new.parameters),
                    "interval": new.publish_interval_seconds,
                    "seed": new.seed,
                    "enabled": new.desired_enabled,
                    "archived": new.archived,
                    "version": new.config_version,
                },
            )
            await self._audit(
                session,
                revision + 1,
                "sensor",
                sensor_id,
                action,
                old.model_dump(by_alias=True),
                new.model_dump(by_alias=True),
            )
        return new

    async def runtime(self) -> list[dict[str, Any]]:
        async with self.sessions() as session:
            rows = (
                (
                    await session.execute(
                        text(
                            'SELECT service,instance_id AS "instanceId",applied_revision AS'
                            ' "appliedRevision",'
                            'error,reported_at AS "reportedAt",'
                            "reported_at > CURRENT_TIMESTAMP - interval '15 seconds' AS fresh "
                            "FROM registry_runtime_status ORDER BY service"
                        )
                    )
                )
                .mappings()
                .all()
            )
        return [dict(row) for row in rows]

    async def report(self, report: RuntimeReport) -> None:
        async with self.sessions.begin() as session:
            await session.execute(
                text(
                    "INSERT INTO registry_runtime_status (service,instance_id,applied_revis"
                    "ion,error) "
                    "VALUES (:service,:instance_id,:applied_revision,:error) "
                    "ON CONFLICT (service) DO UPDATE SET instance_id=EXCLUDED.instance_id,"
                    "applied_revision=EXCLUDED.applied_revision,error=EXCLUDED.error,"
                    "reported_at=CURRENT_TIMESTAMP"
                ),
                report.model_dump(),
            )

    async def parents(self, kind: str, garage_id: str) -> list[dict[str, Any]]:
        statement = (
            'SELECT area_id AS "areaId",name FROM garage_areas WHERE garage_id=:garage ORDE'
            "R BY area_id"
            if kind == "areas"
            else 'SELECT device_id AS "deviceId",area_id AS "areaId",name FROM devices '
            "WHERE garage_id=:garage ORDER BY device_id"
        )
        async with self.sessions() as session:
            rows = (await session.execute(text(statement), {"garage": garage_id})).mappings().all()
        return [dict(row) for row in rows]

    async def audit(self, sensor_id: str, after: int, limit: int) -> list[dict[str, Any]]:
        async with self.sessions() as session:
            rows = (
                (
                    await session.execute(
                        text(
                            'SELECT id,revision,action,request_id AS "requestId",before_sta'
                            'te AS "beforeState",'
                            'after_state AS "afterState",occurred_at AS "occurredAt" '
                            "FROM sensor_registration_events WHERE entity_type='sensor' AND"
                            " entity_id=:sensor "
                            "AND id>:after ORDER BY id LIMIT :limit"
                        ),
                        {"sensor": sensor_id, "after": after, "limit": limit},
                    )
                )
                .mappings()
                .all()
            )
        return [dict(row) for row in rows]
