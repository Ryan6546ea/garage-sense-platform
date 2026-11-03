# 独立后端

这是从 garage-simulation 提取的独立 Python 3.12 后端，可以把整个目录复制到其他位置运行。

包含：账号注册、审核、登录、权限和日志；车库/区域/设备/传感器注册；六类 MQTT
遥测校验与入库；历史及摄入失败查询；异步导出；数据保留清理；PostgreSQL 和 Mosquitto。

不包含前端、遥测模拟器、摄像头模拟、测试视频、虚拟控制器，也不开放场景控制、随机测试、
回放注入和虚拟指令接口。无模拟器客户端或相关后台任务。

保留 `sim-core` 名称是为了兼容共享契约，它不是模拟器。保留完整数据库迁移链、历史模型和
历史审计导出，避免破坏原有表及契约；新数据库仍会由原迁移写入示例车库/设备目录，
但不会生成遥测数据。告警等未实现的业务不会因提取自动补齐。

## 启动

需要 Docker Desktop（Linux containers）和 Docker Compose。

```powershell
Copy-Item .env.example .env
# 编辑 .env，替换 POSTGRES_PASSWORD、MQTT_PASSWORD 和 AUTH_SERVICE_TOKEN。
docker compose --env-file .env config --quiet
docker compose --env-file .env up -d --build
docker compose --env-file .env exec -it control-service python -m control_service.bootstrap_admin
```

默认端口保持原示例：以 `.env` 中 CONTROL_HTTP_PORT、EVENT_RECORDER_HTTP_PORT、
TELEMETRY_CONSUMER_HTTP_PORT、MQTT_PORT 和 POSTGRES_PORT 为准。已有原项目运行时，
先调整这些宿主端口。独立 Compose 项目名为 `garage-backend`，使用独立数据卷。
不会自动复制旧数据库、账号或遥测记录。

业务 API 在 control-service 的 `/api/v1`，历史 API 在 event-recorder 的 `/api/v1`。
每个 HTTP 服务都有 `/docs` 和 `/openapi.json`。API 保持原有身份鉴权；前端独立接入时，
建议由自己的同源代理转发 `/api/control` 和 `/api/history`，并去掉对应代理前缀。
本包不配置跨域 Cookie 或 CORS，也不提供公网部署配置。

外部数据生产者向 Mosquitto 发布符合 `schemas/` 的消息；参考 `examples/`。
没有生产者时查询为空是正常状态。设备需与注册目录对应。

```powershell
docker compose --env-file .env ps
docker compose --env-file .env logs --tail 100
docker compose --env-file .env down
```

`down` 保留数据卷；不要在需要保留数据时添加 `-v`。

## Python 开发

安装 Python 3.12 和 uv 后运行 `uv sync`。配置本地数据库/MQTT 地址和服务环境变量后，
可运行 `uv run python -m control_service.main` 等入口。推荐首先使用 Compose，
它会负责迁移顺序、内部服务地址及配置文件挂载。
