"""Standardize business table and telemetry union-view names.

Revision ID: 0011
Revises: 0010
Create Date: 2026-09-17
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0011"
down_revision: str | None = "0010"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


TABLE_RENAMES = (
    ("scenario_activations", "simulation_scenario_activations"),
    ("run_events", "simulation_run_events"),
    ("hikvision_adapter_state", "camera_hikvision_adapter_state"),
    ("virtual_controller_state", "controller_runtime_states"),
    ("controller_feedback_outbox", "controller_feedback_outbox_messages"),
    ("telemetry_vision_event_events", "telemetry_vision_events"),
)

CONSTRAINT_RENAMES = (
    (
        "simulation_scenario_activations",
        "scenario_activations_pkey",
        "simulation_scenario_activations_pkey",
    ),
    (
        "simulation_scenario_activations",
        "scenario_activations_run_id_fkey",
        "simulation_scenario_activations_run_id_fkey",
    ),
    (
        "simulation_scenario_activations",
        "ck_scenario_activations_source",
        "ck_simulation_scenario_activations_source",
    ),
    (
        "simulation_scenario_activations",
        "ck_scenario_activations_status",
        "ck_simulation_scenario_activations_status",
    ),
    (
        "simulation_scenario_activations",
        "ck_scenario_activations_duration_positive",
        "ck_simulation_scenario_activations_duration_positive",
    ),
    (
        "simulation_scenario_activations",
        "uq_scenario_activations_run_sequence",
        "uq_simulation_scenario_activations_run_sequence",
    ),
    ("simulation_run_events", "run_events_pkey", "simulation_run_events_pkey"),
    (
        "simulation_run_events",
        "run_events_run_id_fkey",
        "simulation_run_events_run_id_fkey",
    ),
    (
        "simulation_run_events",
        "uq_run_events_run_sequence",
        "uq_simulation_run_events_run_sequence",
    ),
    (
        "camera_hikvision_adapter_state",
        "hikvision_adapter_state_pkey",
        "camera_hikvision_adapter_state_pkey",
    ),
    (
        "camera_hikvision_adapter_state",
        "ck_hikvision_adapter_version",
        "ck_camera_hikvision_adapter_state_version",
    ),
    (
        "controller_runtime_states",
        "virtual_controller_state_pkey",
        "controller_runtime_states_pkey",
    ),
    (
        "controller_runtime_states",
        "ck_virtual_controller_state_version",
        "ck_controller_runtime_states_version",
    ),
    (
        "controller_feedback_outbox_messages",
        "controller_feedback_outbox_pkey",
        "controller_feedback_outbox_messages_pkey",
    ),
    (
        "controller_feedback_outbox_messages",
        "controller_feedback_outbox_execution_job_id_fkey",
        "controller_feedback_outbox_messages_execution_job_id_fkey",
    ),
    (
        "controller_feedback_outbox_messages",
        "ck_controller_outbox_feedback_sequence",
        "ck_controller_feedback_outbox_messages_feedback_sequence",
    ),
    (
        "controller_feedback_outbox_messages",
        "ck_controller_outbox_attempt_count",
        "ck_controller_feedback_outbox_messages_attempt_count",
    ),
    (
        "controller_feedback_outbox_messages",
        "ck_controller_outbox_status",
        "ck_controller_feedback_outbox_messages_status",
    ),
    (
        "controller_feedback_outbox_messages",
        "ck_controller_outbox_published_at",
        "ck_controller_feedback_outbox_messages_published_at",
    ),
    (
        "controller_feedback_outbox_messages",
        "uq_controller_outbox_feedback_id",
        "uq_controller_feedback_outbox_messages_feedback_id",
    ),
    (
        "controller_feedback_outbox_messages",
        "uq_controller_outbox_command_sequence",
        "uq_controller_feedback_outbox_messages_command_sequence",
    ),
    (
        "telemetry_vision_events",
        "telemetry_vision_event_events_pkey",
        "telemetry_vision_events_pkey",
    ),
    (
        "telemetry_vision_events",
        "telemetry_vision_event_events_id_fkey",
        "telemetry_vision_events_id_fkey",
    ),
    (
        "telemetry_vision_events",
        "uq_telemetry_vision_event_events_message_id",
        "uq_telemetry_vision_events_message_id",
    ),
)

INDEX_RENAMES = (
    (
        "ux_scenario_activations_request_id",
        "ux_simulation_scenario_activations_request_id",
    ),
    (
        "ix_scenario_activations_run_sequence",
        "ix_simulation_scenario_activations_run_sequence",
    ),
    (
        "ix_scenario_activations_target_requested",
        "ix_simulation_scenario_activations_target_requested",
    ),
    ("ix_run_events_occurred_at", "ix_simulation_run_events_occurred_at"),
    (
        "ix_controller_outbox_claim",
        "ix_controller_feedback_outbox_messages_claim",
    ),
    (
        "ix_telemetry_vision_event_events_source",
        "ix_telemetry_vision_events_source",
    ),
    (
        "ix_telemetry_vision_event_events_device_source",
        "ix_telemetry_vision_events_device_source",
    ),
    (
        "ix_telemetry_vision_event_events_garage_source",
        "ix_telemetry_vision_events_garage_source",
    ),
    (
        "ix_telemetry_vision_event_events_received",
        "ix_telemetry_vision_events_received",
    ),
)

TRIGGER_RENAMES = (
    (
        "simulation_run_events",
        "trg_run_events_append_only",
        "trg_simulation_run_events_append_only",
    ),
    (
        "controller_feedback_outbox_messages",
        "trg_controller_outbox_payload_immutable",
        "trg_controller_feedback_outbox_messages_payload_immutable",
    ),
    (
        "telemetry_vision_events",
        "trg_telemetry_vision_event_events_append_only",
        "trg_telemetry_vision_events_append_only",
    ),
)

FUNCTION_RENAMES = (
    ("reject_run_event_mutation", "reject_simulation_run_event_mutation"),
    (
        "reject_controller_outbox_payload_change",
        "reject_controller_feedback_outbox_message_payload_change",
    ),
    ("reject_typed_telemetry_mutation", "reject_telemetry_event_mutation"),
)


def _rename_constraint(table: str, old: str, new: str) -> None:
    op.execute(f'ALTER TABLE "{table}" RENAME CONSTRAINT "{old}" TO "{new}"')


def _rename_index(old: str, new: str) -> None:
    op.execute(f'ALTER INDEX "{old}" RENAME TO "{new}"')


def _rename_trigger(table: str, old: str, new: str) -> None:
    op.execute(f'ALTER TRIGGER "{old}" ON "{table}" RENAME TO "{new}"')


def _rename_function(old: str, new: str) -> None:
    op.execute(f'ALTER FUNCTION "{old}"() RENAME TO "{new}"')


def upgrade() -> None:
    for old, new in TABLE_RENAMES:
        op.rename_table(old, new)
    op.execute("ALTER VIEW typed_telemetry_events RENAME TO telemetry_all_events")
    for table, old, new in CONSTRAINT_RENAMES:
        _rename_constraint(table, old, new)
    for old, new in INDEX_RENAMES:
        _rename_index(old, new)
    for table, old, new in TRIGGER_RENAMES:
        _rename_trigger(table, old, new)
    for old, new in FUNCTION_RENAMES:
        _rename_function(old, new)


def downgrade() -> None:
    for old, new in reversed(FUNCTION_RENAMES):
        _rename_function(new, old)
    for table, old, new in reversed(TRIGGER_RENAMES):
        _rename_trigger(table, new, old)
    for old, new in reversed(INDEX_RENAMES):
        _rename_index(new, old)
    for table, old, new in reversed(CONSTRAINT_RENAMES):
        _rename_constraint(table, new, old)
    op.execute("ALTER VIEW telemetry_all_events RENAME TO typed_telemetry_events")
    for old, new in reversed(TABLE_RENAMES):
        op.rename_table(new, old)
