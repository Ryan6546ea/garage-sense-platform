"""Durable user accounts, revocable sessions and append-only identity audit."""

import asyncio
import hashlib
import secrets
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

from argon2 import PasswordHasher
from argon2.exceptions import VerificationError
from fastapi import HTTPException
from sqlalchemy import JSON, DateTime, ForeignKey, Integer, String, delete, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from sim_core.identity import AccessUpdate, UserView

HASHER = PasswordHasher(time_cost=3, memory_cost=65536, parallelism=1)
DUMMY_HASH = HASHER.hash(secrets.token_urlsafe(32))


def digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


async def password_hash(value: str) -> str:
    return await asyncio.to_thread(HASHER.hash, value)


async def password_matches(encoded: str, value: str) -> bool:
    def verify() -> bool:
        try:
            return HASHER.verify(encoded, value)
        except VerificationError:
            return False

    return await asyncio.to_thread(verify)


class IdentityBase(DeclarativeBase):
    pass


class User(IdentityBase):
    __tablename__ = "users"
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    username: Mapped[str] = mapped_column(String(64), unique=True)
    display_name: Mapped[str] = mapped_column(String(80))
    password_hash: Mapped[str] = mapped_column(String(255))
    role: Mapped[str] = mapped_column(String(20))
    status: Mapped[str] = mapped_column(String(20))
    garage_ids: Mapped[list[str]] = mapped_column(JSON)
    log_scope: Mapped[str] = mapped_column(String(20))
    log_export: Mapped[bool]
    version: Mapped[int] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class UserSession(IdentityBase):
    __tablename__ = "user_sessions"
    token_hash: Mapped[str] = mapped_column(String(64), primary_key=True)
    user_id: Mapped[str] = mapped_column(ForeignKey("users.id"), index=True)
    csrf_token: Mapped[str] = mapped_column(String(64))
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)


class UserAudit(IdentityBase):
    __tablename__ = "user_audit_events"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    actor_id: Mapped[str | None] = mapped_column(String(36), index=True)
    actor: Mapped[str] = mapped_column(String(64))
    action: Mapped[str] = mapped_column(String(120))
    target: Mapped[str] = mapped_column(String(500))
    garage_ids: Mapped[list[str]] = mapped_column(JSON)
    outcome: Mapped[str] = mapped_column(String(40))
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    details: Mapped[dict[str, Any]] = mapped_column(JSON)


class AuthThrottle(IdentityBase):
    __tablename__ = "auth_throttles"
    key: Mapped[str] = mapped_column(String(64), primary_key=True)
    count: Mapped[int] = mapped_column(Integer)
    reset_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


def user_view(user: User) -> UserView:
    return UserView.model_validate(
        {
            "userId": user.id,
            "username": user.username,
            "displayName": user.display_name,
            "role": user.role,
            "status": user.status,
            "garageIds": user.garage_ids,
            "logScope": user.log_scope,
            "logExport": user.log_export,
            "version": user.version,
            "createdAt": user.created_at,
        }
    )


def audit(
    session: AsyncSession,
    actor: User | None,
    action: str,
    target: str,
    *,
    garages: list[str] | None = None,
    outcome: str = "success",
    details: dict[str, Any] | None = None,
) -> None:
    session.add(
        UserAudit(
            actor_id=actor.id if actor else None,
            actor=actor.username if actor else "anonymous",
            action=action,
            target=target[:500],
            garage_ids=garages or [],
            outcome=outcome,
            occurred_at=datetime.now(UTC),
            details=details or {},
        )
    )


async def identity_lock(session: AsyncSession) -> None:
    # Serializes privilege changes/bootstrap across processes, including the last-admin check.
    if session.bind is not None and session.bind.dialect.name == "postgresql":
        await session.execute(text("SELECT pg_advisory_xact_lock(160016)"))


class IdentityStore:
    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self.sessions = sessions

    async def authenticate(self, token: str | None) -> tuple[User, UserSession]:
        if not token or len(token) > 200:
            raise HTTPException(401, "请先登录")
        async with self.sessions() as session:
            row = (
                await session.execute(
                    select(User, UserSession)
                    .join(UserSession, UserSession.user_id == User.id)
                    .where(
                        UserSession.token_hash == digest(token),
                        UserSession.expires_at > datetime.now(UTC),
                        User.status == "active",
                    )
                )
            ).first()
            if row is None:
                raise HTTPException(401, "登录已失效, 请重新登录")
            return row[0], row[1]

    async def throttle(self, key: str, limit: int = 10) -> None:
        now = datetime.now(UTC)
        exceeded = False
        async with self.sessions.begin() as session:
            await identity_lock(session)
            await session.execute(delete(AuthThrottle).where(AuthThrottle.reset_at <= now))
            row = await session.get(AuthThrottle, digest(key))
            if row is None:
                session.add(
                    AuthThrottle(key=digest(key), count=1, reset_at=now + timedelta(minutes=10))
                )
            elif row.count >= limit:
                exceeded = True
            else:
                row.count += 1
        if exceeded:
            raise HTTPException(429, "尝试次数过多, 请十分钟后重试", headers={"Retry-After": "600"})

    async def update_access(self, actor_id: str, user_id: str, data: AccessUpdate) -> UserView:
        async with self.sessions.begin() as session:
            await identity_lock(session)
            actor = await session.get(User, actor_id, populate_existing=True)
            if actor is None or actor.role != "admin" or actor.status != "active":
                raise HTTPException(403, "需要管理员权限")
            user = await session.get(User, user_id)
            if user is None:
                raise HTTPException(404, "账号不存在")
            if user.version != data.version:
                raise HTTPException(409, "账号已被其他管理员修改, 请刷新")
            if (
                user.role == "admin"
                and user.status == "active"
                and (data.role != "admin" or data.status != "active")
            ):
                admins = (
                    await session.scalars(
                        select(User.id).where(User.role == "admin", User.status == "active")
                    )
                ).all()
                if len(admins) <= 1:
                    raise HTTPException(409, "不能禁用或降级最后一位管理员")
            before = user_view(user).model_dump(mode="json")
            for field in ("role", "status", "garage_ids", "log_scope", "log_export"):
                setattr(user, field, getattr(data, field))
            user.version += 1
            await session.execute(delete(UserSession).where(UserSession.user_id == user.id))
            audit(
                session,
                actor,
                "account.access_changed",
                user.id,
                details={
                    "before": before,
                    "after": user_view(user).model_dump(mode="json"),
                },
            )
            return user_view(user)

    async def bootstrap(self, username: str, name: str, password: str) -> None:
        encoded = await password_hash(password)
        async with self.sessions.begin() as session:
            await identity_lock(session)
            if await session.scalar(select(User.id).where(User.role == "admin")):
                raise ValueError("管理员已存在, 初始化命令不能再次使用")
            user = User(
                id=str(uuid4()),
                username=username,
                display_name=name,
                password_hash=encoded,
                role="admin",
                status="active",
                garage_ids=[],
                log_scope="all",
                log_export=True,
                version=1,
                created_at=datetime.now(UTC),
            )
            session.add(user)
            await session.flush()
            audit(session, user, "account.bootstrap", user.id)
