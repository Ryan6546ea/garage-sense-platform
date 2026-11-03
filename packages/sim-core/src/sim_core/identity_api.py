"""Account lifecycle API. Passwords/tokens never enter audit records."""

import secrets
from datetime import UTC, datetime, timedelta
from typing import Annotated
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Query, Request, Response
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import delete, select
from sqlalchemy.exc import IntegrityError

from sim_core.identity import (
    AccessUpdate,
    AuditPage,
    AuditView,
    Credentials,
    GarageAccessView,
    PasswordChange,
    PasswordReset,
    Registration,
    SessionView,
    UserPage,
    UserView,
)
from sim_core.identity_store import (
    DUMMY_HASH,
    IdentityStore,
    User,
    UserAudit,
    UserSession,
    audit,
    digest,
    identity_lock,
    password_hash,
    password_matches,
    user_view,
)

COOKIE = "garage_session"


class AuthSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="AUTH_", env_file=".env", extra="ignore")
    enabled: bool = True
    cookie_secure: bool = False
    session_seconds: int = Field(default=28800, ge=300, le=86400)
    service_token: str = Field(default="", repr=False)


def principal(request: Request) -> User:
    user = getattr(request.state, "identity", None)
    if not isinstance(user, User):
        raise HTTPException(401, "请先登录")
    return user


def administrator(request: Request) -> User:
    user = principal(request)
    if user.role != "admin":
        raise HTTPException(403, "需要管理员权限")
    return user


def audit_view(row: UserAudit) -> AuditView:
    return AuditView(
        event_id=row.id,
        actor_id=row.actor_id,
        actor=row.actor,
        action=row.action,
        target=row.target,
        garage_ids=row.garage_ids,
        outcome=row.outcome,
        occurred_at=row.occurred_at,
        details=row.details,
    )


def create_identity_router(store: IdentityStore, settings: AuthSettings) -> APIRouter:
    router = APIRouter(prefix="/api/v1", tags=["identity"])

    @router.post("/auth/register", response_model=UserView, status_code=201)
    async def register(data: Registration, request: Request) -> UserView:
        peer = request.client.host if request.client else "unknown"
        await store.throttle(f"register:{peer}", 5)
        encoded = await password_hash(data.password)
        try:
            async with store.sessions.begin() as session:
                user = User(
                    id=str(uuid4()),
                    username=data.username,
                    display_name=data.display_name,
                    password_hash=encoded,
                    role="viewer",
                    status="pending",
                    garage_ids=[],
                    log_scope="own",
                    log_export=False,
                    version=1,
                    created_at=datetime.now(UTC),
                )
                session.add(user)
                await session.flush()
                audit(session, user, "account.registered", user.id)
                return user_view(user)
        except IntegrityError as exc:
            raise HTTPException(409, "该账号无法注册, 请更换用户名") from exc

    @router.post("/auth/login", response_model=SessionView)
    async def login(data: Credentials, request: Request, response: Response) -> SessionView:
        peer = request.client.host if request.client else "unknown"
        await store.throttle(f"login-ip:{peer}", 60)
        await store.throttle(f"login-account:{data.username}")
        async with store.sessions() as session:
            user = await session.scalar(select(User).where(User.username == data.username))
            encoded = user.password_hash if user else DUMMY_HASH
        valid = await password_matches(encoded, data.password)
        async with store.sessions.begin() as session:
            await identity_lock(session)
            user = await session.scalar(select(User).where(User.username == data.username))
            if (
                not valid
                or user is None
                or user.status != "active"
                or user.password_hash != encoded
            ):
                audit(session, user, "session.login", "login", outcome="denied")
                result = None
            else:
                now = datetime.now(UTC)
                await session.execute(delete(UserSession).where(UserSession.expires_at <= now))
                # Bound per-account sessions and rotate a previous browser session.
                previous = request.cookies.get(COOKIE)
                if previous:
                    await session.execute(
                        delete(UserSession).where(UserSession.token_hash == digest(previous))
                    )
                active = (
                    await session.scalars(
                        select(UserSession)
                        .where(UserSession.user_id == user.id)
                        .order_by(UserSession.expires_at.desc())
                    )
                ).all()
                for old in active[4:]:
                    await session.delete(old)
                token, csrf = secrets.token_urlsafe(32), secrets.token_urlsafe(32)
                session.add(
                    UserSession(
                        token_hash=digest(token),
                        user_id=user.id,
                        csrf_token=csrf,
                        expires_at=now + timedelta(seconds=settings.session_seconds),
                    )
                )
                audit(session, user, "session.login", user.id)
                result = SessionView(user=user_view(user), csrf_token=csrf)
        if result is None:
            raise HTTPException(401, "账号或密码错误, 或账号尚未启用")
        response.set_cookie(
            COOKIE,
            token,
            httponly=True,
            secure=settings.cookie_secure,
            samesite="strict",
            max_age=settings.session_seconds,
            path="/",
        )
        return result

    @router.get("/auth/me", response_model=SessionView)
    async def me(request: Request) -> SessionView:
        return SessionView(
            user=user_view(principal(request)), csrf_token=request.state.identity_session.csrf_token
        )

    @router.get("/auth/garages", response_model=GarageAccessView)
    async def garages(request: Request) -> GarageAccessView:
        from sim_core.registry import registered_garages

        user = principal(request)
        return GarageAccessView(
            garage_ids=[
                value
                for value in registered_garages()
                if user.role == "admin" or value in user.garage_ids
            ]
        )

    @router.post("/auth/logout", status_code=204)
    async def logout(request: Request, response: Response) -> None:
        user = principal(request)
        async with store.sessions.begin() as session:
            await session.execute(
                delete(UserSession).where(
                    UserSession.token_hash == request.state.identity_session.token_hash
                )
            )
            audit(session, user, "session.logout", user.id)
        response.delete_cookie(
            COOKIE, path="/", secure=settings.cookie_secure, httponly=True, samesite="strict"
        )

    @router.post("/auth/password", status_code=204)
    async def change_password(data: PasswordChange, request: Request, response: Response) -> None:
        actor = principal(request)
        await store.throttle(f"password:{actor.id}")
        if not await password_matches(actor.password_hash, data.current_password):
            raise HTTPException(400, "当前密码不正确")
        encoded = await password_hash(data.new_password)
        async with store.sessions.begin() as session:
            await identity_lock(session)
            user = await session.get(User, actor.id)
            if user is None or user.version != actor.version:
                raise HTTPException(409, "账号已变更, 请重新登录")
            user.password_hash = encoded
            user.version += 1
            await session.execute(delete(UserSession).where(UserSession.user_id == user.id))
            audit(session, user, "account.password_changed", user.id)
        response.delete_cookie(COOKIE, path="/")

    @router.get("/users", response_model=UserPage)
    async def users(
        request: Request, after: str = "", limit: Annotated[int, Query(ge=1, le=100)] = 50
    ) -> UserPage:
        administrator(request)
        async with store.sessions() as session:
            rows = (
                await session.scalars(
                    select(User).where(User.id > after).order_by(User.id).limit(limit + 1)
                )
            ).all()
            return UserPage(
                items=[user_view(row) for row in rows[:limit]],
                next_cursor=rows[limit - 1].id if len(rows) > limit else None,
            )

    @router.patch("/users/{user_id}/access", response_model=UserView)
    async def access(user_id: str, data: AccessUpdate, request: Request) -> UserView:
        actor = administrator(request)
        # Registry is authoritative; an empty scope grants no garage access to non-admins.
        from sim_core.registry import registered_garages

        if set(data.garage_ids) - set(registered_garages()):
            raise HTTPException(422, "包含未注册的车库")
        return await store.update_access(actor.id, user_id, data)

    @router.post("/users/{user_id}/password", status_code=204)
    async def reset(user_id: str, data: PasswordReset, request: Request) -> None:
        actor = administrator(request)
        encoded = await password_hash(data.new_password)
        async with store.sessions.begin() as session:
            await identity_lock(session)
            fresh_actor = await session.get(User, actor.id)
            if fresh_actor is None or fresh_actor.role != "admin" or fresh_actor.status != "active":
                raise HTTPException(403, "需要管理员权限")
            user = await session.get(User, user_id)
            if user is None:
                raise HTTPException(404, "账号不存在")
            if user.version != data.version:
                raise HTTPException(409, "账号已变更, 请刷新")
            user.password_hash = encoded
            user.version += 1
            await session.execute(delete(UserSession).where(UserSession.user_id == user.id))
            audit(session, fresh_actor, "account.password_reset", user.id)

    async def audit_page(request: Request, before: int | None, limit: int) -> AuditPage:
        user = principal(request)
        async with store.sessions() as session:
            query = select(UserAudit).order_by(UserAudit.id.desc())
            if before is not None:
                query = query.where(UserAudit.id < before)
            if user.log_scope == "own":
                query = query.where(UserAudit.actor_id == user.id)
            # Bounded scanning: filtering never reveals cross-garage entries or their counts.
            rows = (await session.scalars(query.limit(limit))).all()
            visible = [
                row
                for row in rows
                if user.log_scope != "garage"
                or (row.garage_ids and set(row.garage_ids).issubset(user.garage_ids))
            ]
            return AuditPage(
                items=[audit_view(row) for row in visible],
                next_cursor=rows[-1].id if len(rows) == limit else None,
            )

    @router.get("/audit-logs", response_model=AuditPage)
    async def logs(
        request: Request,
        before: Annotated[int | None, Query(ge=1)] = None,
        limit: Annotated[int, Query(ge=1, le=200)] = 100,
    ) -> AuditPage:
        return await audit_page(request, before, limit)

    @router.get("/audit-logs/export")
    async def export_logs(
        request: Request,
        before: Annotated[int | None, Query(ge=1)] = None,
        limit: Annotated[int, Query(ge=1, le=200)] = 200,
    ) -> Response:
        user = principal(request)
        if not user.log_export:
            raise HTTPException(403, "没有日志导出权限")
        page = await audit_page(request, before, limit)
        async with store.sessions.begin() as session:
            audit(
                session,
                user,
                "audit.exported",
                "audit-logs",
                details={"count": len(page.items), "nextCursor": page.next_cursor},
            )
        return Response(
            page.model_dump_json(by_alias=True),
            media_type="application/json",
            headers={"Content-Disposition": 'attachment; filename="audit-logs.json"'},
        )

    return router
