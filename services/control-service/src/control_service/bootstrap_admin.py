"""One-time administrator initialization, with credentials entered privately."""

import asyncio
import getpass

from sim_core.identity import Registration
from sim_core.identity_store import IdentityStore

from control_service.config import ControlSettings
from control_service.repository import Database


async def bootstrap(data: Registration) -> None:
    database = Database(ControlSettings())
    try:
        await IdentityStore(database.sessions).bootstrap(
            data.username, data.display_name, data.password
        )
    finally:
        await database.dispose()


def main() -> None:
    username = input("管理员用户名: ")
    name = input("显示名称: ")
    password = getpass.getpass("密码(至少 12 位): ")
    if password != getpass.getpass("再次输入密码: "):
        raise SystemExit("两次密码不一致")
    data = Registration(username=username, display_name=name, password=password)
    asyncio.run(bootstrap(data))
    print("管理员已创建, 可以登录。")


if __name__ == "__main__":
    main()
