"""Persistent garage/device/sensor registry and runtime reconciliation status."""

import re
from collections.abc import Sequence

from alembic import op

revision: str = "0015"
down_revision: str | None = "0014"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

TELEMETRY_TABLES = (
    "telemetry_laser_distance_events",
    "telemetry_current_events",
    "telemetry_vibration_events",
    "telemetry_environment_events",
    "telemetry_vision_events",
    "telemetry_mechanism_state_events",
)


def upgrade() -> None:
    ddl = """
        CREATE TABLE registry_state (
            id integer PRIMARY KEY CHECK (id = 1),
            revision bigint NOT NULL CHECK (revision >= 0)
        );
        INSERT INTO registry_state VALUES (1, 0);
        CREATE TABLE garages (
            garage_id text PRIMARY KEY,
            name text NOT NULL CHECK (length(trim(name)) > 0),
            altitude_meters double precision NOT NULL CHECK (altitude_meters BETWEEN 0 AND 10000),
            created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE garage_areas (
            garage_id text NOT NULL REFERENCES garages(garage_id) ON DELETE RESTRICT,
            area_id text NOT NULL,
            name text NOT NULL CHECK (length(trim(name)) > 0),
            PRIMARY KEY (garage_id, area_id)
        );
        CREATE TABLE devices (
            device_id text PRIMARY KEY,
            garage_id text NOT NULL,
            area_id text NOT NULL,
            name text NOT NULL CHECK (length(trim(name)) > 0),
            UNIQUE (device_id, garage_id, area_id),
            FOREIGN KEY (garage_id, area_id) REFERENCES garage_areas(garage_id, area_id)
                ON DELETE RESTRICT
        );
        CREATE INDEX ix_devices_garage ON devices(garage_id, device_id);
        CREATE TABLE sensor_instances (
            sensor_id text PRIMARY KEY,
            device_id text NOT NULL,
            garage_id text NOT NULL,
            area_id text NOT NULL,
            sensor_type text NOT NULL CHECK (sensor_type IN (
                'laser_distance','current','vibration','environment','vision_event','mechanism_state'
            )),
            name text NOT NULL CHECK (length(trim(name)) > 0),
            point_code text NOT NULL,
            role text CHECK (role IN ('LIFT','SHIFT')),
            camera_id text,
            topic text NOT NULL UNIQUE,
            publish_interval_seconds double precision NOT NULL
                CHECK (publish_interval_seconds BETWEEN 0.1 AND 86400),
            seed integer NOT NULL CHECK (seed >= 0),
            parameters jsonb NOT NULL CHECK (jsonb_typeof(parameters) = 'object'),
            desired_enabled boolean NOT NULL,
            archived boolean NOT NULL DEFAULT false,
            config_version integer NOT NULL DEFAULT 1 CHECK (config_version > 0),
            created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
            CHECK (NOT archived OR NOT desired_enabled),
            FOREIGN KEY (device_id, garage_id, area_id)
                REFERENCES devices(device_id, garage_id, area_id) ON DELETE RESTRICT
        );
        CREATE INDEX ix_sensor_instances_garage ON sensor_instances(garage_id, sensor_id);
        CREATE INDEX ix_sensor_instances_device ON sensor_instances(device_id, sensor_id);
        CREATE TABLE sensor_registration_events (
            id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            revision bigint NOT NULL,
            entity_type text NOT NULL,
            entity_id text NOT NULL,
            action text NOT NULL,
            request_id uuid NOT NULL,
            before_state jsonb,
            after_state jsonb NOT NULL,
            occurred_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX ix_sensor_registration_events_entity
            ON sensor_registration_events(entity_type, entity_id, id);
        CREATE TABLE registry_runtime_status (
            service text PRIMARY KEY CHECK (service IN ('simulator','consumer')),
            instance_id text NOT NULL,
            applied_revision bigint NOT NULL CHECK (applied_revision >= 0),
            error text,
            reported_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE FUNCTION protect_sensor_identity() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF TG_OP = 'DELETE' THEN
                RAISE EXCEPTION 'sensor identities must be archived, not deleted';
            END IF;
            IF (NEW.sensor_id, NEW.device_id, NEW.garage_id, NEW.area_id, NEW.sensor_type,
                NEW.point_code, NEW.role, NEW.camera_id, NEW.topic)
                IS DISTINCT FROM
               (OLD.sensor_id, OLD.device_id, OLD.garage_id, OLD.area_id, OLD.sensor_type,
                OLD.point_code, OLD.role, OLD.camera_id, OLD.topic) THEN
                RAISE EXCEPTION 'sensor identity is immutable';
            END IF;
            IF OLD.archived AND NOT NEW.archived THEN
                RAISE EXCEPTION 'archived sensor cannot be restored';
            END IF;
            RETURN NEW;
        END $$;
        CREATE TRIGGER sensor_identity_immutable BEFORE UPDATE OR DELETE ON sensor_instances
            FOR EACH ROW EXECUTE FUNCTION protect_sensor_identity();
        CREATE FUNCTION protect_registration_audit() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            RAISE EXCEPTION 'registration audit is append-only';
        END $$;
        CREATE TRIGGER registration_audit_append_only
            BEFORE UPDATE OR DELETE OR TRUNCATE ON sensor_registration_events
            FOR EACH STATEMENT EXECUTE FUNCTION protect_registration_audit();
    """
    for statement in re.split(r";\s*(?=CREATE |INSERT )", ddl.strip()):
        op.execute(statement)
    for table in TELEMETRY_TABLES:
        op.create_index(f"ix_{table}_sensor_received", table, ["sensor_id", "received_at", "id"])

    for statement in SEED_SQL:
        op.execute(statement)


# Frozen defaults: later edits to YAML cannot change migration history.
SEED_SQL = (
    (
        "INSERT INTO garages (garage_id,name,altitude_meters) VALUES ('G00"
        "1','标准车库合成配置',1900)"
    ),
    ("INSERT INTO garage_areas VALUES ('G001','A-PRESENCE','A-PRESENCE')"),
    ("INSERT INTO devices VALUES ('G001-LASER-001','G001','A-PRESENCE','G001-LASER-001')"),
    (
        "INSERT INTO sensor_instances (sensor_id,device_id,garage_id,area_"
        "id,sensor_type,name,point_code,role,camera_id,topic,publish_inter"
        "val_seconds,seed,parameters,desired_enabled) VALUES ('G001-LASER-"
        "001','G001-LASER-001','G001','A-PRESENCE','laser_distance','G001-"
        "LASER-001','VEHICLE_DISTANCE',NULL,NULL,'garage/G001/telemetry/la"
        'ser/G001-LASER-001\',1,1001,\'{"vehicleDistanceM": 1.2, "clearDista'
        "nceM\": 5.0}',true)"
    ),
    ("INSERT INTO garage_areas VALUES ('G001','A-LIFT','A-LIFT')"),
    ("INSERT INTO devices VALUES ('G001-MOTOR-LIFT-001','G001','A-LIFT','G001-MOTOR-LIFT-001')"),
    (
        "INSERT INTO sensor_instances (sensor_id,device_id,garage_id,area_"
        "id,sensor_type,name,point_code,role,camera_id,topic,publish_inter"
        "val_seconds,seed,parameters,desired_enabled) VALUES ('G001-CUR-LI"
        "FT-001','G001-MOTOR-LIFT-001','G001','A-LIFT','current','G001-CUR"
        "-LIFT-001','LIFT_MOTOR_CURRENT','LIFT',NULL,'garage/G001/telemetr"
        'y/current/G001-MOTOR-LIFT-001\',1,1002,\'{"batchSamples": 10, "samp'
        "leRateHz\": 10}',true)"
    ),
    ("INSERT INTO garage_areas VALUES ('G001','A-SHIFT','A-SHIFT')"),
    ("INSERT INTO devices VALUES ('G001-MOTOR-SHIFT-001','G001','A-SHIFT','G001-MOTOR-SHIFT-001')"),
    (
        "INSERT INTO sensor_instances (sensor_id,device_id,garage_id,area_"
        "id,sensor_type,name,point_code,role,camera_id,topic,publish_inter"
        "val_seconds,seed,parameters,desired_enabled) VALUES ('G001-CUR-SH"
        "IFT-001','G001-MOTOR-SHIFT-001','G001','A-SHIFT','current','G001-"
        "CUR-SHIFT-001','SHIFT_MOTOR_CURRENT','SHIFT',NULL,'garage/G001/te"
        "lemetry/current/G001-MOTOR-SHIFT-001',1,1003,'{\"batchSamples\": 10"
        ', "sampleRateHz": 10}\',true)'
    ),
    ("INSERT INTO devices VALUES ('G001-VIB-001','G001','A-LIFT','G001-VIB-001')"),
    (
        "INSERT INTO sensor_instances (sensor_id,device_id,garage_id,area_"
        "id,sensor_type,name,point_code,role,camera_id,topic,publish_inter"
        "val_seconds,seed,parameters,desired_enabled) VALUES ('G001-VIB-00"
        "1','G001-VIB-001','G001','A-LIFT','vibration','G001-VIB-001','LIF"
        "T_MECHANISM_VIBRATION',NULL,NULL,'garage/G001/telemetry/vibration"
        '/G001-VIB-001\',1,1004,\'{"sampleCount": 100, "sampleRateHz": 100, '
        '"normalAmplitude": 0.3}\',true)'
    ),
    ("INSERT INTO garage_areas VALUES ('G001','A-ENV','A-ENV')"),
    ("INSERT INTO devices VALUES ('G001-TH-001','G001','A-ENV','G001-TH-001')"),
    (
        "INSERT INTO sensor_instances (sensor_id,device_id,garage_id,area_"
        "id,sensor_type,name,point_code,role,camera_id,topic,publish_inter"
        "val_seconds,seed,parameters,desired_enabled) VALUES ('G001-TH-001"
        "','G001-TH-001','G001','A-ENV','environment','G001-TH-001','GARAG"
        "E_TEMP_HUMIDITY',NULL,NULL,'garage/G001/telemetry/environment/G00"
        '1-TH-001\',2,1005,\'{"temperatureC": 24, "humidityRh": 55, "pressur'
        'eKPa": 81, "altitudeMeters": 1900}\',true)'
    ),
    ("INSERT INTO garage_areas VALUES ('G001','A-SAFETY','A-SAFETY')"),
    ("INSERT INTO devices VALUES ('G001-VISION-001','G001','A-SAFETY','G001-VISION-001')"),
    (
        "INSERT INTO sensor_instances (sensor_id,device_id,garage_id,area_"
        "id,sensor_type,name,point_code,role,camera_id,topic,publish_inter"
        "val_seconds,seed,parameters,desired_enabled) VALUES ('G001-VISION"
        "-001','G001-VISION-001','G001','A-SAFETY','vision_event','G001-VI"
        "SION-001','VISION_SAFETY_STATE',NULL,'G001-CAM-002','garage/G001/"
        "telemetry/vision/G001-VISION-001',1,1006,'{}',true)"
    ),
    ("INSERT INTO garage_areas VALUES ('G001','A-MECHANISM','A-MECHANISM')"),
    ("INSERT INTO devices VALUES ('G001-STATE-001','G001','A-MECHANISM','G001-STATE-001')"),
    (
        "INSERT INTO sensor_instances (sensor_id,device_id,garage_id,area_"
        "id,sensor_type,name,point_code,role,camera_id,topic,publish_inter"
        "val_seconds,seed,parameters,desired_enabled) VALUES ('G001-STATE-"
        "001','G001-STATE-001','G001','A-MECHANISM','mechanism_state','G00"
        "1-STATE-001','MECHANISM_STATE',NULL,NULL,'garage/G001/telemetry/s"
        "tate/G001-STATE-001',1,1007,'{\"loadKg\": 1400}',true)"
    ),
    (
        "INSERT INTO garages (garage_id,name,altitude_meters) VALUES ('G00"
        "2','高海拔车库合成配置',3300)"
    ),
    ("INSERT INTO garage_areas VALUES ('G002','A-PRESENCE','A-PRESENCE')"),
    ("INSERT INTO devices VALUES ('G002-LASER-001','G002','A-PRESENCE','G002-LASER-001')"),
    (
        "INSERT INTO sensor_instances (sensor_id,device_id,garage_id,area_"
        "id,sensor_type,name,point_code,role,camera_id,topic,publish_inter"
        "val_seconds,seed,parameters,desired_enabled) VALUES ('G002-LASER-"
        "001','G002-LASER-001','G002','A-PRESENCE','laser_distance','G002-"
        "LASER-001','VEHICLE_DISTANCE',NULL,NULL,'garage/G002/telemetry/la"
        'ser/G002-LASER-001\',1.2,2001,\'{"vehicleDistanceM": 1.35, "clearDi'
        "stanceM\": 5.5}',true)"
    ),
    ("INSERT INTO garage_areas VALUES ('G002','A-LIFT','A-LIFT')"),
    ("INSERT INTO devices VALUES ('G002-MOTOR-LIFT-001','G002','A-LIFT','G002-MOTOR-LIFT-001')"),
    (
        "INSERT INTO sensor_instances (sensor_id,device_id,garage_id,area_"
        "id,sensor_type,name,point_code,role,camera_id,topic,publish_inter"
        "val_seconds,seed,parameters,desired_enabled) VALUES ('G002-CUR-LI"
        "FT-001','G002-MOTOR-LIFT-001','G002','A-LIFT','current','G002-CUR"
        "-LIFT-001','LIFT_MOTOR_CURRENT','LIFT',NULL,'garage/G002/telemetr"
        'y/current/G002-MOTOR-LIFT-001\',1.2,2002,\'{"batchSamples": 12, "sa'
        "mpleRateHz\": 12}',true)"
    ),
    ("INSERT INTO garage_areas VALUES ('G002','A-SHIFT','A-SHIFT')"),
    ("INSERT INTO devices VALUES ('G002-MOTOR-SHIFT-001','G002','A-SHIFT','G002-MOTOR-SHIFT-001')"),
    (
        "INSERT INTO sensor_instances (sensor_id,device_id,garage_id,area_"
        "id,sensor_type,name,point_code,role,camera_id,topic,publish_inter"
        "val_seconds,seed,parameters,desired_enabled) VALUES ('G002-CUR-SH"
        "IFT-001','G002-MOTOR-SHIFT-001','G002','A-SHIFT','current','G002-"
        "CUR-SHIFT-001','SHIFT_MOTOR_CURRENT','SHIFT',NULL,'garage/G002/te"
        "lemetry/current/G002-MOTOR-SHIFT-001',1.2,2003,'{\"batchSamples\": "
        '12, "sampleRateHz": 12}\',true)'
    ),
    ("INSERT INTO devices VALUES ('G002-VIB-001','G002','A-LIFT','G002-VIB-001')"),
    (
        "INSERT INTO sensor_instances (sensor_id,device_id,garage_id,area_"
        "id,sensor_type,name,point_code,role,camera_id,topic,publish_inter"
        "val_seconds,seed,parameters,desired_enabled) VALUES ('G002-VIB-00"
        "1','G002-VIB-001','G002','A-LIFT','vibration','G002-VIB-001','LIF"
        "T_MECHANISM_VIBRATION',NULL,NULL,'garage/G002/telemetry/vibration"
        '/G002-VIB-001\',1.2,2004,\'{"sampleCount": 120, "sampleRateHz": 120'
        ', "normalAmplitude": 0.36}\',true)'
    ),
    ("INSERT INTO garage_areas VALUES ('G002','A-ENV','A-ENV')"),
    ("INSERT INTO devices VALUES ('G002-TH-001','G002','A-ENV','G002-TH-001')"),
    (
        "INSERT INTO sensor_instances (sensor_id,device_id,garage_id,area_"
        "id,sensor_type,name,point_code,role,camera_id,topic,publish_inter"
        "val_seconds,seed,parameters,desired_enabled) VALUES ('G002-TH-001"
        "','G002-TH-001','G002','A-ENV','environment','G002-TH-001','GARAG"
        "E_TEMP_HUMIDITY',NULL,NULL,'garage/G002/telemetry/environment/G00"
        '2-TH-001\',2.4,2005,\'{"temperatureC": 18, "humidityRh": 48, "press'
        'ureKPa": 68, "altitudeMeters": 3300}\',true)'
    ),
    ("INSERT INTO garage_areas VALUES ('G002','A-SAFETY','A-SAFETY')"),
    ("INSERT INTO devices VALUES ('G002-VISION-001','G002','A-SAFETY','G002-VISION-001')"),
    (
        "INSERT INTO sensor_instances (sensor_id,device_id,garage_id,area_"
        "id,sensor_type,name,point_code,role,camera_id,topic,publish_inter"
        "val_seconds,seed,parameters,desired_enabled) VALUES ('G002-VISION"
        "-001','G002-VISION-001','G002','A-SAFETY','vision_event','G002-VI"
        "SION-001','VISION_SAFETY_STATE',NULL,'G002-CAM-002','garage/G002/"
        "telemetry/vision/G002-VISION-001',1.2,2006,'{}',true)"
    ),
    ("INSERT INTO garage_areas VALUES ('G002','A-MECHANISM','A-MECHANISM')"),
    ("INSERT INTO devices VALUES ('G002-STATE-001','G002','A-MECHANISM','G002-STATE-001')"),
    (
        "INSERT INTO sensor_instances (sensor_id,device_id,garage_id,area_"
        "id,sensor_type,name,point_code,role,camera_id,topic,publish_inter"
        "val_seconds,seed,parameters,desired_enabled) VALUES ('G002-STATE-"
        "001','G002-STATE-001','G002','A-MECHANISM','mechanism_state','G00"
        "2-STATE-001','MECHANISM_STATE',NULL,NULL,'garage/G002/telemetry/s"
        "tate/G002-STATE-001',1.2,2007,'{\"loadKg\": 1250}',true)"
    ),
    ("UPDATE registry_state SET revision = 1 WHERE id = 1"),
    (
        "INSERT INTO sensor_registration_events (revision,entity_type,enti"
        "ty_id,action,request_id,after_state) SELECT 1,'sensor',sensor_id,"
        "'seed','00000000-0000-4000-8000-000000000015'::uuid,to_jsonb(s) F"
        "ROM sensor_instances s"
    ),
)


def downgrade() -> None:
    raise RuntimeError(
        "registry downgrade would destroy identity and audit history; restore backup"
    )
