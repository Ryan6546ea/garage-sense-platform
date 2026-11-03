"""Strict user-management contracts shared by API, schema exports and clients."""

from datetime import datetime
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

Role = Literal["viewer", "operator", "admin"]
AccountStatus = Literal["pending", "active", "disabled"]
LogScope = Literal["own", "garage", "all"]


class IdentityModel(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True, serialize_by_alias=True)


class Credentials(IdentityModel):
    username: str = Field(min_length=3, max_length=64, pattern=r"^[a-zA-Z0-9_.-]+$")
    password: str = Field(min_length=12, max_length=128, repr=False)

    @field_validator("username")
    @classmethod
    def normalize_username(cls, value: str) -> str:
        return value.lower()


class Registration(Credentials):
    display_name: str = Field(alias="displayName", min_length=1, max_length=80)

    @field_validator("display_name")
    @classmethod
    def nonblank_name(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("displayName must not be blank")
        return value.strip()


class AccessUpdate(IdentityModel):
    version: int = Field(ge=1)
    role: Role
    status: AccountStatus
    garage_ids: list[str] = Field(alias="garageIds", max_length=1000)
    log_scope: LogScope = Field(alias="logScope")
    log_export: bool = Field(alias="logExport")

    @model_validator(mode="after")
    def unique_garages(self) -> Self:
        if len(set(self.garage_ids)) != len(self.garage_ids):
            raise ValueError("garageIds must not contain duplicates")
        if any(not value or len(value) > 100 for value in self.garage_ids):
            raise ValueError("invalid garageId")
        return self


class UserView(AccessUpdate):
    user_id: str = Field(alias="userId")
    username: str
    display_name: str = Field(alias="displayName")
    created_at: datetime = Field(alias="createdAt")


class SessionView(IdentityModel):
    user: UserView
    csrf_token: str = Field(alias="csrfToken")


class PasswordChange(IdentityModel):
    current_password: str = Field(alias="currentPassword", min_length=1, max_length=128, repr=False)
    new_password: str = Field(alias="newPassword", min_length=12, max_length=128, repr=False)


class PasswordReset(IdentityModel):
    version: int = Field(ge=1)
    new_password: str = Field(alias="newPassword", min_length=12, max_length=128, repr=False)


class AuditView(IdentityModel):
    event_id: int = Field(alias="eventId")
    actor_id: str | None = Field(alias="actorId")
    actor: str
    action: str
    target: str
    garage_ids: list[str] = Field(alias="garageIds")
    outcome: str
    occurred_at: datetime = Field(alias="occurredAt")
    details: dict[str, object]


class UserPage(IdentityModel):
    items: list[UserView]
    next_cursor: str | None = Field(alias="nextCursor")


class AuditPage(IdentityModel):
    items: list[AuditView]
    next_cursor: int | None = Field(alias="nextCursor")


class GarageAccessView(IdentityModel):
    garage_ids: list[str] = Field(alias="garageIds")
