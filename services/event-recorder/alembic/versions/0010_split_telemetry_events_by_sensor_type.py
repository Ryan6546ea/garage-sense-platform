"""Replace telemetry_events with typed tables and a read-only union view.

Revision ID: 0010
Revises: 0009
Create Date: 2026-09-16
"""

from collections.abc import Sequence
from typing import Any

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0010"
down_revision: str | None = "0009"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

TABLES = (
    "telemetry_laser_distance_events",
    "telemetry_current_events",
    "telemetry_vibration_events",
    "telemetry_environment_events",
    "telemetry_vision_event_events",
    "telemetry_mechanism_state_events",
)

COMMON_INSERT_COLUMNS = """
    id, message_id, topic, qos, retain, garage_id, area_id, device_id,
    sensor_id, point_code, scenario_id, status, quality, source_time,
    received_at, seq, action_stage, payload
"""

COMMON_SELECT_COLUMNS = """
    id, message_id, topic, qos, retain, garage_id, area_id, device_id,
    sensor_id, point_code, scenario_id, status, quality, source_time,
    received_at, seq, COALESCE(payload->>'actionStage', 'unknown'), payload
"""


def _common_columns() -> tuple[sa.Column[Any], ...]:
    return (
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("message_id", sa.Text(), nullable=False),
        sa.Column("topic", sa.Text(), nullable=False),
        sa.Column("qos", sa.SmallInteger(), nullable=False),
        sa.Column("retain", sa.Boolean(), nullable=False),
        sa.Column("garage_id", sa.Text(), nullable=False),
        sa.Column("area_id", sa.Text(), nullable=False),
        sa.Column("device_id", sa.Text(), nullable=False),
        sa.Column("sensor_id", sa.Text(), nullable=False),
        sa.Column("point_code", sa.Text(), nullable=False),
        sa.Column("scenario_id", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("quality", sa.Text(), nullable=False),
        sa.Column("source_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("seq", sa.BigInteger(), nullable=False),
        sa.Column("action_stage", sa.Text(), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    )


def _create_typed_table(
    table_name: str,
    *specific_columns: sa.Column[Any],
    checks: tuple[sa.CheckConstraint, ...] = (),
) -> None:
    op.create_table(
        table_name,
        *_common_columns(),
        *specific_columns,
        *checks,
        sa.ForeignKeyConstraint(
            ["id"],
            ["telemetry_event_identities.id"],
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("message_id", name=f"uq_{table_name}_message_id"),
    )
    op.create_index(
        f"ix_{table_name}_source",
        table_name,
        ["source_time", "id"],
    )
    op.create_index(
        f"ix_{table_name}_device_source",
        table_name,
        ["device_id", "source_time", "id"],
    )
    op.create_index(
        f"ix_{table_name}_garage_source",
        table_name,
        ["garage_id", "source_time", "id"],
    )
    op.create_index(f"ix_{table_name}_received", table_name, ["received_at", "id"])


def _copy_common(table_name: str, sensor_type: str, specific_columns: str, values: str) -> None:
    op.execute(
        f"""
        INSERT INTO {table_name} ({COMMON_INSERT_COLUMNS}, {specific_columns})
        SELECT {COMMON_SELECT_COLUMNS}, {values}
        FROM telemetry_events
        WHERE sensor_type = '{sensor_type}'
        """
    )


def _create_union_view() -> None:
    selects = []
    for table_name, sensor_type in zip(
        TABLES,
        (
            "laser_distance",
            "current",
            "vibration",
            "environment",
            "vision_event",
            "mechanism_state",
        ),
        strict=True,
    ):
        selects.append(
            "SELECT id, message_id, topic, qos, retain, garage_id, area_id, "
            "device_id, sensor_id, point_code, "
            f"'{sensor_type}'::text AS sensor_type, scenario_id, status, quality, "
            "source_time, received_at, seq, action_stage, payload "
            f"FROM {table_name}"
        )
    op.execute("CREATE VIEW typed_telemetry_events AS " + " UNION ALL ".join(selects))


def upgrade() -> None:
    op.create_table(
        "telemetry_event_identities",
        sa.Column("id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("message_id", sa.Text(), nullable=False),
        sa.Column("sensor_type", sa.Text(), nullable=False),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "sensor_type IN ('laser_distance','current','vibration','environment',"
            "'vision_event','mechanism_state')",
            name="ck_telemetry_event_identities_sensor_type",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("message_id", name="uq_telemetry_event_identities_message_id"),
    )
    op.create_index(
        "ix_telemetry_event_identities_received",
        "telemetry_event_identities",
        ["received_at", "id"],
    )

    _create_typed_table(
        "telemetry_laser_distance_events",
        sa.Column("vehicle_present", sa.Boolean(), nullable=False),
        sa.Column("distance_m", sa.Double()),
        sa.Column("displacement_m", sa.Double()),
        sa.Column("position_status", sa.Text(), nullable=False),
    )
    _create_typed_table(
        "telemetry_current_events",
        sa.Column("unit", sa.Text(), nullable=False),
        sa.Column("phase", sa.Text(), nullable=False),
        sa.Column("sample_rate_hz", sa.Integer(), nullable=False),
        sa.Column("sample_count", sa.Integer(), nullable=False),
        sa.Column("window_start_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("window_end_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("current_samples", postgresql.ARRAY(sa.Double()), nullable=False),
        checks=(
            sa.CheckConstraint("sample_count >= 0", name="ck_telemetry_current_sample_count"),
            sa.CheckConstraint(
                "cardinality(current_samples) = sample_count",
                name="ck_telemetry_current_samples_length",
            ),
            sa.CheckConstraint(
                "window_end_time >= window_start_time",
                name="ck_telemetry_current_window_order",
            ),
        ),
    )
    _create_typed_table(
        "telemetry_vibration_events",
        sa.Column("unit", sa.Text(), nullable=False),
        sa.Column("sample_rate_hz", sa.Integer(), nullable=False),
        sa.Column("sample_count", sa.Integer(), nullable=False),
        sa.Column("samples", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("features", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    )
    _create_typed_table(
        "telemetry_environment_events",
        sa.Column("temperature", sa.Double()),
        sa.Column("temperature_unit", sa.Text(), nullable=False),
        sa.Column("humidity", sa.Double()),
        sa.Column("humidity_unit", sa.Text(), nullable=False),
        sa.Column("pressure_kpa", sa.Double()),
        sa.Column("altitude_meters", sa.Double()),
        sa.Column("condensation_risk", sa.Boolean()),
    )
    _create_typed_table(
        "telemetry_vision_event_events",
        sa.Column("camera_id", sa.Text(), nullable=False),
        sa.Column("person_detected", sa.Boolean(), nullable=False),
        sa.Column("vehicle_detected", sa.Boolean(), nullable=False),
        sa.Column("vehicle_position", sa.Text(), nullable=False),
        sa.Column("confidence", sa.Double(), nullable=False),
    )
    _create_typed_table(
        "telemetry_mechanism_state_events",
        sa.Column("plc_state", sa.Text(), nullable=False),
        sa.Column("at_position", sa.Boolean()),
        sa.Column("lift_limit_active", sa.Boolean()),
        sa.Column("shift_limit_active", sa.Boolean()),
        sa.Column("interlock_closed", sa.Boolean()),
        sa.Column("emergency_stop", sa.Boolean()),
        sa.Column("load_kg", sa.Double()),
    )

    op.execute(
        """
        INSERT INTO telemetry_event_identities (id, message_id, sensor_type, received_at)
        SELECT id, message_id, sensor_type, received_at FROM telemetry_events
        """
    )
    op.execute(
        """
        SELECT setval(
          pg_get_serial_sequence('telemetry_event_identities', 'id'),
          COALESCE((SELECT MAX(id) FROM telemetry_event_identities), 1),
          EXISTS (SELECT 1 FROM telemetry_event_identities)
        )
        """
    )
    _copy_common(
        "telemetry_laser_distance_events",
        "laser_distance",
        "vehicle_present, distance_m, displacement_m, position_status",
        "(payload #>> '{data,vehiclePresent}')::boolean, "
        "NULLIF(payload #>> '{data,distanceM}', '')::double precision, "
        "NULLIF(payload #>> '{data,displacementM}', '')::double precision, "
        "payload #>> '{data,positionStatus}'",
    )
    _copy_common(
        "telemetry_current_events",
        "current",
        "unit, phase, sample_rate_hz, sample_count, window_start_time, "
        "window_end_time, current_samples",
        "payload #>> '{data,unit}', payload #>> '{data,phase}', "
        "(payload #>> '{data,sampleRateHz}')::integer, "
        "COALESCE(NULLIF(payload #>> '{data,sampleCount}', '')::integer, "
        "jsonb_array_length(COALESCE(payload #> '{data,currentSamples}', "
        "payload #> '{data,samples}'))), "
        "COALESCE(NULLIF(payload #>> '{data,windowStartTime}', '')::timestamptz, source_time), "
        "COALESCE(NULLIF(payload #>> '{data,windowEndTime}', '')::timestamptz, source_time), "
        "ARRAY(SELECT jsonb_array_elements_text(COALESCE(payload #> '{data,currentSamples}', "
        "payload #> '{data,samples}'))::double precision)",
    )
    _copy_common(
        "telemetry_vibration_events",
        "vibration",
        "unit, sample_rate_hz, sample_count, samples, features",
        "payload #>> '{data,unit}', (payload #>> '{data,sampleRateHz}')::integer, "
        "(payload #>> '{data,sampleCount}')::integer, payload #> '{data,samples}', "
        "payload #> '{data,features}'",
    )
    _copy_common(
        "telemetry_environment_events",
        "environment",
        "temperature, temperature_unit, humidity, humidity_unit, pressure_kpa, "
        "altitude_meters, condensation_risk",
        "NULLIF(payload #>> '{data,temperature}', '')::double precision, "
        "payload #>> '{data,temperatureUnit}', "
        "NULLIF(payload #>> '{data,humidity}', '')::double precision, "
        "payload #>> '{data,humidityUnit}', "
        "NULLIF(payload #>> '{data,pressureKPa}', '')::double precision, "
        "NULLIF(payload #>> '{data,altitudeMeters}', '')::double precision, "
        "NULLIF(payload #>> '{data,condensationRisk}', '')::boolean",
    )
    _copy_common(
        "telemetry_vision_event_events",
        "vision_event",
        "camera_id, person_detected, vehicle_detected, vehicle_position, confidence",
        "payload #>> '{data,cameraId}', "
        "(payload #>> '{data,personDetected}')::boolean, "
        "(payload #>> '{data,vehicleDetected}')::boolean, "
        "payload #>> '{data,vehiclePosition}', "
        "(payload #>> '{data,confidence}')::double precision",
    )
    _copy_common(
        "telemetry_mechanism_state_events",
        "mechanism_state",
        "plc_state, at_position, lift_limit_active, shift_limit_active, "
        "interlock_closed, emergency_stop, load_kg",
        "payload #>> '{data,plcState}', "
        "NULLIF(payload #>> '{data,atPosition}', '')::boolean, "
        "NULLIF(payload #>> '{data,liftLimitActive}', '')::boolean, "
        "NULLIF(payload #>> '{data,shiftLimitActive}', '')::boolean, "
        "NULLIF(payload #>> '{data,interlockClosed}', '')::boolean, "
        "NULLIF(payload #>> '{data,emergencyStop}', '')::boolean, "
        "NULLIF(payload #>> '{data,loadKg}', '')::double precision",
    )

    op.drop_constraint(
        "replay_items_source_event_id_fkey", "replay_items", type_="foreignkey"
    )
    op.drop_constraint(
        "export_items_source_event_id_fkey", "export_items", type_="foreignkey"
    )
    op.create_foreign_key(
        "replay_items_source_event_id_fkey",
        "replay_items",
        "telemetry_event_identities",
        ["source_event_id"],
        ["id"],
        ondelete="RESTRICT",
    )
    op.create_foreign_key(
        "export_items_source_event_id_fkey",
        "export_items",
        "telemetry_event_identities",
        ["source_event_id"],
        ["id"],
        ondelete="RESTRICT",
    )
    op.drop_table("telemetry_events")
    _create_union_view()

    op.execute(
        """
        CREATE FUNCTION reject_typed_telemetry_mutation() RETURNS trigger AS $$
        BEGIN
          RAISE EXCEPTION 'typed telemetry events are append-only';
        END;
        $$ LANGUAGE plpgsql
        """
    )
    for table_name in ("telemetry_event_identities", *TABLES):
        op.execute(
            f"CREATE TRIGGER trg_{table_name}_append_only "
            f"BEFORE UPDATE OR DELETE ON {table_name} "
            "FOR EACH ROW EXECUTE FUNCTION reject_typed_telemetry_mutation()"
        )


def downgrade() -> None:
    for table_name in ("telemetry_event_identities", *TABLES):
        op.execute(f"DROP TRIGGER trg_{table_name}_append_only ON {table_name}")
    op.execute("DROP FUNCTION reject_typed_telemetry_mutation()")

    op.create_table(
        "telemetry_events",
        sa.Column("id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("message_id", sa.Text(), nullable=False),
        sa.Column("topic", sa.Text(), nullable=False),
        sa.Column("qos", sa.SmallInteger(), nullable=False),
        sa.Column("retain", sa.Boolean(), nullable=False),
        sa.Column("garage_id", sa.Text(), nullable=False),
        sa.Column("area_id", sa.Text(), nullable=False),
        sa.Column("device_id", sa.Text(), nullable=False),
        sa.Column("sensor_id", sa.Text(), nullable=False),
        sa.Column("point_code", sa.Text(), nullable=False),
        sa.Column("sensor_type", sa.Text(), nullable=False),
        sa.Column("scenario_id", sa.Text(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("quality", sa.Text(), nullable=False),
        sa.Column("source_time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("seq", sa.BigInteger(), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.execute(
        """
        INSERT INTO telemetry_events (
          id, message_id, topic, qos, retain, garage_id, area_id, device_id,
          sensor_id, point_code, sensor_type, scenario_id, status, quality,
          source_time, received_at, seq, payload
        )
        SELECT id, message_id, topic, qos, retain, garage_id, area_id, device_id,
          sensor_id, point_code, sensor_type, scenario_id, status, quality,
          source_time, received_at, seq, payload
        FROM typed_telemetry_events
        """
    )
    op.execute("DROP VIEW typed_telemetry_events")
    op.create_index(
        "ux_telemetry_events_message_id",
        "telemetry_events",
        ["message_id"],
        unique=True,
    )
    op.create_index(
        "ix_telemetry_events_device_source",
        "telemetry_events",
        ["device_id", "source_time"],
    )
    op.create_index(
        "ix_telemetry_events_garage_source",
        "telemetry_events",
        ["garage_id", "source_time"],
    )
    op.create_index(
        "ix_telemetry_events_sensor_type_source",
        "telemetry_events",
        ["sensor_type", "source_time"],
    )
    op.create_index(
        "ix_telemetry_events_received_at", "telemetry_events", ["received_at"]
    )
    op.execute(
        "CREATE INDEX ix_telemetry_events_device_received_latest "
        "ON telemetry_events (device_id, received_at DESC, id DESC)"
    )
    op.execute(
        "SELECT setval("
        "pg_get_serial_sequence('telemetry_events', 'id'), "
        "COALESCE((SELECT MAX(id) FROM telemetry_events), 1), "
        "EXISTS (SELECT 1 FROM telemetry_events))"
    )
    op.drop_constraint(
        "replay_items_source_event_id_fkey", "replay_items", type_="foreignkey"
    )
    op.drop_constraint(
        "export_items_source_event_id_fkey", "export_items", type_="foreignkey"
    )
    op.create_foreign_key(
        "replay_items_source_event_id_fkey",
        "replay_items",
        "telemetry_events",
        ["source_event_id"],
        ["id"],
        ondelete="RESTRICT",
    )
    op.create_foreign_key(
        "export_items_source_event_id_fkey",
        "export_items",
        "telemetry_events",
        ["source_event_id"],
        ["id"],
        ondelete="RESTRICT",
    )
    for table_name in reversed(TABLES):
        op.drop_table(table_name)
    op.drop_table("telemetry_event_identities")
