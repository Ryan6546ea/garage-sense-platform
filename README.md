
# 数据链路
目前简易数据链路如下：
场景配置/控制服务→模拟器/硬件 → MQTT →分类消费与校验→PostgreSQL分类型表。

更明确的话，请看下方

## **MQTT通信协议处理**
即使是同一套系统，只要拆成独立运行的服务，也可能需要通信协议。

 **模拟器/硬件**：`telemetry-simulator`
在整个MQTT通信协议中，属于publisher。
流程：
```
读取配置和场景
    ↓
生成符合项目数据契约的模拟数据
    ↓
序列化为 JSON，并编码为字节
    ↓
调用 aiomqtt 客户端库发布
    ↓
aiomqtt客户端库组装 MQTT 报文，通过已有连接发送给 Broker
```

输入：
**模拟参数**：设备标识、传感器类型、场景、采样频率等，决定生成什么数据。
**Broker 连接配置**：地址、端口、用户名、密码、客户端标识等，决定连接哪个 Broker。
**发布主题（Topic）**：决定消息归入哪个分类

输出：
**Topic**：消息分类，Broker 根据它匹配订阅者
**Payload**：JSON 编码后的业务数据，如设备标识、时间、测量值
**QoS交付等级**：使用 QoS 1，提供至少一次交付机制，可能重复
**Retain**：使用 False，不把本次消息设为该主题的保留消息

```
注意地址取决于程序运行在哪里：
模拟器运行在 Compose 容器内 → mosquitto:1883
客户端运行在宿主机上       → localhost:映射端口
```
**MQTT 客户端库 `aiomqtt`** 是本模块引用的外部MQTT组装库

---

**Eclipse Mosquitto 2 MQTT Broker**
在两个不同程序之间分发消息
流程如下：
```
接收发布消息
    ↓
读取主题，匹配订阅关系
    ↓
消费者在线 → 向它发送消息
消费者离线且存在符合条件的持久会话 → 排队等待交付
```
配置：
```
消费者已设置固定ID
clean_session=False #相当于告诉 Broker：其离线后，可以以相同身份重新登录
```

```
# ===== 磁盘持久化 =====

# 保存会话、订阅、待交付消息、保留消息等相关状态
persistence true

# 持久化文件的保存目录
persistence_location /mosquitto/data/

# 自动保存周期：5 秒
autosave_interval 5


# ===== 日志 =====

# 日志输出到标准输出，供 Docker 收集
log_dest stdout

# 记录客户端连接、断开事件
connection_messages true


# ===== 网络监听 =====

# 在容器内所有 IPv4 网络接口上监听 1883 端口
listener 1883 0.0.0.0


# ===== 连接认证 =====

# 禁止匿名连接
allow_anonymous false

# 用户名和密码验证文件
password_file /tmp/mosquitto.passwd
```

|配置|作用|
|---|---|
|`listener 1883 0.0.0.0`|在容器内所有 IPv4 网络接口上监听 1883 端口|
|`allow_anonymous false`|禁止匿名连接|
|`password_file`|指定验证用户名和密码所用的文件|
|`persistence true`|开启相关会话和消息状态的磁盘保存|
|`persistence_location`|指定保存目录|
|`autosave_interval 5`|设置自动保存周期为 5 秒|
|`log_dest stdout`|将日志输出到容器标准输出，方便查看|
|`connection_messages true`|记录客户端连接和断开事件|

输入：MQTT 报文
发布者PUBLISH 报文：主题、载荷、QoS、Retain 等消费者
SUBSCRIBE 报文：主题过滤器、请求的 QoS 等
各客户端：连接、心跳、确认、取消订阅等报文

输出：MQTT 报文

---

**telemetry consumer**
即是consumer-用于处理消息，也是subscriber用于接收指定主题的消息

它是你编写的业务服务，使用现成的 `aiomqtt` 客户端库与 Mosquitto 通信
订阅→解析 → 校验 → 去重 → 分类入库

**1. 订阅六类遥测数据**

当前代码为每种类型建立独立的 MQTT 连接、队列和处理任务：

| 数据类型 | 订阅主题                               |
| ---- | ---------------------------------- |
| 激光距离 | `garage/+/telemetry/laser/+`       |
| 电机电流 | `garage/+/telemetry/current/+`     |
| 振动   | `garage/+/telemetry/vibration/+`   |
| 环境数据 | `garage/+/telemetry/environment/+` |
| 视觉事件 | `garage/+/telemetry/vision/+`      |
| 机构状态 | `garage/+/telemetry/state/+`       |
**2. 将消息载荷解析成业务数据，并检查合法性**

Broker 能转发一条消息，不代表其中的业务内容正确。消费者会检查：

- 载荷是否超过大小限制。
- 能否解析为 JSON 对象。
- 是否符合 Pydantic 模型和 JSON Schema。
- 设备信息、主题和载荷是否符合语义约定。
- 载荷中的传感器类型是否与当前处理类型一致。

例如，一条消息发布到激光主题，载荷却声明为电流数据，就会被拒绝进入正常遥测表。

输入： MQTT报文
输出：PostgreSQL数据库表

**3. 去重并按类型写入数据库**

QoS 1 可能造成重复交付，所以消费者使用 **`messageId`** 去重。

```
第一次收到 messageId=A → 写入
再次收到 messageId=A   → 识别为重复，不再新增
```

它先在统一的消息身份表中登记，再写入对应的遥测表；这两步位于同一数据库事务中。

**4. 隔离错误和积压**

各类型有独立的有界队列及处理任务，减少某一类消息异常对其他类型的影响。

|情况|当前处理方式|
|---|---|
|JSON、契约或语义不合法|尝试记录到失败表，继续处理下一条|
|失败载荷过大|截断保存，并记录原始大小和完整内容的哈希|
|某类型队列已满|拒绝该条入队，记录日志和失败指标|
|数据库写入失败或处理超时|记录错误，更新健康状态和指标|

**因此，它不保证“MQTT 收到的每条消息最终都能入库”。** 当前代码的队列满、写入失败等路径，并没有自动将该消息放入持久重试队列。

**5. 提供健康检查和运行指标**

|接口|作用|
|---|---|
|`/health/live`|检查服务是否能响应|
|`/health/ready`|检查数据库、MQTT 连接、处理任务及写入状态等是否就绪|
|`/metrics`|提供接收量、入库量、重复量、失败量、队列长度和处理耗时等指标|

这些接口帮助判断：到底是 Broker 没连上、数据不合法、队列积压，还是数据库写入失败。[服务接口 (line 60)](E:/1留学/2资本积累/C.车库/garage-simulation/services/telemetry-consumer/src/telemetry_consumer/api.py:60)

---

**模拟器中的指令消息采用http控制**
**命令提交走 HTTP、反馈走 MQTT：先受理，再异步报告结果**

项目实际采用的流程：

```
用户／控制台
    │ HTTP：提交虚拟命令
    ▼
control-service
    │ HTTP：把命令交给虚拟控制器
    ▼
virtual-controller-mock
    │ 保存执行计划及待发送反馈
    │ HTTP：返回受理结果
    │
    │ 稍后，通过 MQTT 发布执行反馈
    ▼
Mosquitto
    │ 转发给已订阅的控制服务
    ▼
control-service 更新命令状态
```

这里的“虚拟控制器”只模拟结果，不操作真实硬件。

用一个例子理解：

```
HTTP 请求：请执行虚拟升降命令，commandId=123
HTTP 响应：命令已受理

过一会儿，MQTT 反馈：
commandId=123 对应的执行结果为成功
```

**“已受理”和“已完成”是两件事。** HTTP 响应让调用方知道请求是否被接收；后续 MQTT 消息报告执行进展或最终结果。

为了应对 MQTT 暂时不可用，虚拟控制器先把待发送反馈存入数据库里的 **Outbox（待发送消息表）**，再尝试发布，收到 Broker 确认后标记为已发布。这样，反馈不只存在于进程内存中。

这是一种项目设计选择，并非协议强制：命令和反馈都可以采用其他通信方案。你的项目选择 HTTP 来处理明确的提交请求，用 MQTT 来交付稍后产生的异步反馈。

---

# 
# 独立后端

这是从 garage-simulation 提取的独立 Python 3.12 后端，可以把整个目录复制到其他位置运行。

包含：账号注册、审核、登录、权限和日志；车库/区域/设备/传感器注册；六类 MQTT
遥测校验与入库；历史及摄入失败查询；异步导出；数据保留清理；PostgreSQL 和 Mosquitto。

保留 `sim-core` 名称是为了兼容共享契约，它不是模拟器。

## 后端文件夹标注

以下按当前已拆出的独立后端 `garage-simulation/artifacts/garage-backend/` 整理，路径均相对于该后端根目录。前文数据链路中出现的模拟器、虚拟控制器属于完整模拟平台，不在这份独立后端中。

```text
garage-backend/
├── config/                         # 服务运行配置
│   └── mosquitto/                  # MQTT Broker 配置：监听、认证、会话持久化和日志
├── examples/                       # JSON 请求、响应及消息示例，方便开发和联调
│   ├── commands/                   # 保留的虚拟命令及反馈契约示例，供历史兼容参考
│   ├── identity/                   # 注册、登录、用户权限、会话及操作日志示例
│   ├── registration/               # 车库、区域、设备、传感器注册及目录同步示例
│   └── telemetry/                  # 激光、电流、振动、环境、视觉、机构状态六类遥测示例
├── packages/                       # 多个服务共同依赖的 Python 包
│   └── sim-core/                   # 共享数据契约、类型注册表、账号鉴权和注册目录客户端
│       └── src/                    # 共享包的源码根目录
│           └── sim_core/           # Python 实际导入的包名，集中定义校验模型和公共逻辑
├── schemas/                        # JSON Schema：描述字段、类型、必填项及数据约束
└── services/                       # 可独立运行、由 Compose 组织的后端服务
    ├── control-service/            # 业务 API：账号权限、注册目录、异步导出及下载
    │   └── src/                    # 该服务的源码根目录
    │       └── control_service/    # 路由、业务逻辑、数据库访问、导出任务和管理员初始化
    ├── telemetry-consumer/         # MQTT 摄入服务：订阅、校验、去重、分类入库及失败隔离
    │   └── src/                    # 该服务的源码根目录
    │       └── telemetry_consumer/ # 消费任务、数据库访问、健康检查和 Prometheus 指标
    ├── event-recorder/             # 历史查询服务，同时承载共享数据库迁移
    │   ├── alembic/                # 数据库迁移环境和迁移脚本模板
    │   │   └── versions/           # 按版本执行的建表、改表、索引及初始数据迁移脚本
    │   └── src/                    # 该服务的源码根目录
    │       └── event_recorder/     # 遥测历史、摄入失败查询、数据库模型及校验等模块
    └── data-cleanup/               # 数据保留清理后台服务，不提供 HTTP API
        └── src/                    # 该服务的源码根目录
            └── data_cleanup/      # 定时分批清理过期遥测数据，可配置清理摄入失败记录
```

### 查找和修改入口

| 要做的事情 | 优先查看的位置 |
| --- | --- |
| 修改账号、会话、权限或操作日志逻辑 | `packages/sim-core/src/sim_core/` 中的 `identity*.py`；账号接口由 control-service 挂载 |
| 修改车库、区域、设备、传感器注册 API | `services/control-service/src/control_service/` 中的 `catalog_api.py` 和 `catalog_repository.py` |
| 修改 MQTT 消息消费和写库逻辑 | `services/telemetry-consumer/src/telemetry_consumer/` |
| 修改遥测历史或摄入失败查询接口 | `services/event-recorder/src/event_recorder/` |
| 修改导出任务、文件格式或下载逻辑 | `services/control-service/src/control_service/` 中的 `export_*.py` |
| 修改数据库表结构 | `services/event-recorder/alembic/versions/`，通过新增迁移维护版本演进 |
| 修改数据保留周期和清理批次 | `config/data-cleanup.toml`；执行逻辑位于 `services/data-cleanup/src/data_cleanup/` |
| 修改 MQTT Broker 设置 | `config/mosquitto/mosquitto.conf` |
| 修改遥测及接口数据格式 | 同步检查 `packages/sim-core/src/sim_core/`、`schemas/`、`examples/`，并更新原项目对应契约测试 |

### 目录与运行数据的区别

- `src/` 是 Python 源码布局层；其下一层的下划线目录（如 `control_service/`）才是 Python 导入包。服务外层的连字符目录（如 `control-service/`）还放置 `Dockerfile` 和 `pyproject.toml`。
- `sim-core` 沿用原项目名称，承担共享契约与公共逻辑，不负责生成模拟遥测。
- `examples/commands/`、`examples/registration/` 的部分运行/回放示例，以及 `schemas/` 和迁移目录中的历史内容，为兼容而保留。独立后端不因此提供虚拟命令、随机测试或回放注入接口；以实际挂载的 API 为准。
- PostgreSQL 数据、Mosquitto 持久化状态及导出文件由 Compose 配置的数据卷保存，不属于上述源码目录；`.env` 保存本机配置，不应提交真实凭据。
- `.git/` 是版本控制元数据；若本地生成 `.venv/`、`__pycache__/` 或工具缓存目录，它们分别用于依赖环境、Python 缓存或检查缓存，不是业务模块。

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

## API 接口

以下接口按本目录实际源码整理。JSON 字段使用 camelCase；路径中的 `{garage_id}`、
`{sensor_id}` 等需替换为实际标识。请求字段和响应结构以运行服务的 OpenAPI 为准。

### 服务地址

| 服务 | 默认本地地址 | 用途 |
| --- | --- | --- |
| control-service | `http://127.0.0.1:8002` | 账号权限、设备注册、导出 |
| event-recorder | `http://127.0.0.1:8001` | 遥测历史、摄入失败查询 |
| telemetry-consumer | `http://127.0.0.1:8006` | 消费者健康检查、运行指标 |

端口可通过 `.env` 中对应的 `*_HTTP_PORT` 修改。三个服务均提供 `/docs`、`/redoc`、
`/openapi.json`；control-service 和 event-recorder 开启鉴权后，文档入口也需要登录会话。
data-cleanup 是后台任务，没有 HTTP API。

### 登录与鉴权

默认 `AUTH_ENABLED=true`。先创建管理员，再调用 `POST /api/v1/auth/login`。
登录成功会设置 `garage_session` Cookie，响应包含 `user` 和 `csrfToken`。
后续请求携带 Cookie；除登录和注册外，写操作还需发送 `X-CSRF-Token` 请求头。
浏览器请求必须满足服务端同源检查，建议使用前文的同源反向代理。

`/health/live`、`/health/ready` 无需登录。内部注册接口使用
`X-Service-Token: <AUTH_SERVICE_TOKEN>`，不要把服务令牌放入前端代码。
用户接口仍会检查角色、车库范围和日志权限；登录不代表拥有所有操作权限。
关闭 `AUTH_ENABLED` 会同时取消账号路由的注册，不能在该模式调用登录 API。

PowerShell 登录及查询示例（密码交互输入，不写入脚本）：

```powershell
$api = 'http://127.0.0.1:8002'
$history = 'http://127.0.0.1:8001'
$credential = Get-Credential -Message '输入已创建的管理员账号和密码'
$body = @{
    username = $credential.UserName
    password = $credential.GetNetworkCredential().Password
} | ConvertTo-Json
$login = Invoke-RestMethod -Method Post -Uri "$api/api/v1/auth/login" `
    -ContentType 'application/json' -Body $body -SessionVariable garageSession
$csrfHeaders = @{ 'X-CSRF-Token' = $login.csrfToken }
Invoke-RestMethod -Uri "$api/api/v1/auth/me" -WebSession $garageSession
Invoke-RestMethod -Uri "$history/api/v1/telemetry/events?garageId=G001&limit=20" `
    -WebSession $garageSession
# 写操作示例：注销当前会话。
Invoke-RestMethod -Method Post -Uri "$api/api/v1/auth/logout" `
    -WebSession $garageSession -Headers $csrfHeaders
```

### 账号、权限和日志

以下接口位于 control-service，路径前缀为 `/api/v1`。

| 方法 | 路径（接在前缀后） | 用途 |
| --- | --- | --- |
| POST | `/auth/register` | 注册账号，等待管理员审核 |
| POST | `/auth/login` | 登录并获取会话、CSRF token |
| GET | `/auth/me` | 获取当前用户和 CSRF token |
| GET | `/auth/garages` | 获取当前用户可访问的车库 |
| POST | `/auth/logout` | 注销当前会话 |
| POST | `/auth/password` | 修改本人密码 |
| GET | `/users` | 管理员查询用户 |
| PATCH | `/users/{user_id}/access` | 管理员审核、启停用户及修改角色、授权范围 |
| POST | `/users/{user_id}/password` | 管理员重置用户密码 |
| GET | `/audit-logs` | 按授权范围查询操作日志 |
| GET | `/audit-logs/export` | 按日志导出权限导出日志 |

请求及响应示例见 `examples/identity/`，对应契约见 `schemas/identity-*.schema.json`。

### 车库、设备和传感器注册

以下接口位于 control-service，路径前缀为 `/api/v1`。

| 方法 | 路径（接在前缀后） | 用途 |
| --- | --- | --- |
| GET | `/sensor-types` | 查询支持的传感器类型和参数 Schema |
| GET / POST | `/garages` | 查询 / 创建车库 |
| GET / POST | `/garages/{garage_id}/areas` | 查询 / 创建区域 |
| GET / POST | `/garages/{garage_id}/devices` | 查询 / 创建设备 |
| GET / POST | `/garages/{garage_id}/sensors` | 查询 / 注册传感器 |
| GET | `/sensors/{sensor_id}` | 查询传感器配置和运行应用状态 |
| PATCH | `/sensors/{sensor_id}` | 更新传感器配置 |
| POST | `/sensors/{sensor_id}/enable` | 请求启用 |
| POST | `/sensors/{sensor_id}/disable` | 请求停用 |
| POST | `/sensors/{sensor_id}/archive` | 请求归档 |
| GET | `/sensors/{sensor_id}/registration-events` | 查询注册配置变更流水 |

传感器列表支持 `sensorType`、`archived`、`after`、`limit`；返回的 `nextCursor`
作为下一页的 `after`，`limit` 为 1–200。配置修改及启停、归档请求携带
`configVersion`，字段示例见 `examples/registration/`。
这些接口变更注册目录，不代表向真实设备下发物理控制命令。

当前传感器详情中的 `applied` 仍沿用双运行端确认条件；独立后端没有模拟器，
不能仅凭该字段为 false 判断消费者故障，应结合消费者健康接口及 runtime 报告检查。

### 内部注册同步

以下接口位于 control-service，供后端服务调用，均使用 `X-Service-Token`。

| 方法 | 完整路径 | 用途 |
| --- | --- | --- |
| GET | `/api/v1/internal/registry/snapshot` | 获取注册目录快照 |
| GET | `/api/v1/internal/registry/revision` | 获取目录版本 |
| GET | `/api/v1/internal/registry/runtime` | 获取运行端应用报告 |
| POST | `/api/v1/internal/registry/runtime` | 上报运行端应用状态 |

### 遥测历史

以下接口位于 event-recorder。

| 方法 | 完整路径 | 用途 |
| --- | --- | --- |
| GET | `/api/v1/telemetry/events` | 分页查询遥测历史 |
| GET | `/api/v1/telemetry/events/{message_id}` | 按消息 ID 查询单条事件 |
| GET | `/api/v1/telemetry/ingest-failures` | 查询摄入失败，仅 `EVENT_RECORDER_ENVIRONMENT=test` 可用，其他环境返回 404 |

历史列表支持 `garageId`、`sensorId`、`deviceId`、`sensorType`、`scenarioId`、
`status`、`quality`、`sourceTimeFrom`、`sourceTimeTo`、`limit`、`cursor`。
时间须带 UTC 偏移，例如 `2026-11-02T00:00:00Z`。
`limit` 为 1–1000，默认 100；响应包含 `items` 和 `nextCursor`，下一页将
`nextCursor` 作为 `cursor` 传入。摄入失败列表仅支持 `limit`，范围 1–100。

六类 `sensorType` 为 `laser_distance`、`current`、`vibration`、`environment`、
`vision_event`、`mechanism_state`。数据通过 MQTT 接收，本包不提供 HTTP 遥测写入接口。

### 异步导出

以下接口位于 control-service。

| 方法 | 完整路径 | 用途 |
| --- | --- | --- |
| POST | `/api/v1/exports` | 创建导出任务，返回 202 |
| GET | `/api/v1/exports` | 查询导出任务列表 |
| GET | `/api/v1/exports/{export_id}` | 查询任务状态及结果 |
| GET | `/api/v1/exports/{export_id}/events` | 查询导出任务事件 |
| POST | `/api/v1/exports/{export_id}/cancel` | 请求取消任务 |
| GET | `/api/v1/exports/{export_id}/download` | 下载已完成的导出文件 |
| HEAD | `/api/v1/exports/{export_id}/download` | 查询下载响应头 |

任务创建后轮询详情，完成后再下载。按时间范围导出遥测可使用 JSONL 或 CSV；
`source` 的示例见 `examples/registration/export-time-range.json`，它只是完整创建请求中的
一个字段，完整请求结构见 `/docs` 的 `CreateExportRequest`。
导出模块保留历史运行审计兼容能力，但本包不提供创建模拟运行或回放的接口。

### 健康检查与指标

三个 HTTP 服务均有 `GET /health/live` 和 `GET /health/ready`。
telemetry-consumer 额外提供 `GET /metrics`，返回 Prometheus 格式指标，无需用户会话。
readiness 失败通常返回 503，应结合日志检查数据库、MQTT 连接或后台任务状态。
