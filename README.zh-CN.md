# MPE DB 插件

![CI](https://github.com/multi-protocol-flow/mpe-plugin-db/actions/workflows/ci.yml/badge.svg)

> **定位**：本插件是 MPE 的**官方协议插件**之一——不依赖宿主仓库的任何代码，
> 只依赖公开的 `mpe-plugin-sdk`（Sidecar 进程 + JSON-RPC over stdio，git tag `v0.1.0`）。
> 经 sqlx 为 MPE 提供 SQLite / MySQL / PostgreSQL 数据库操作。
>
> 本仓库独立于宿主（宿主仓库 `.gitignore` 忽略 `/plugins/`、宿主从不构建它），
> 由本仓库自己的 CI（GitHub Actions → Release 产物）构建与发布。

---

## 0. 一分钟理解插件如何工作

```
宿主 (mpe / mpe-cli)                    插件进程 (本 crate)
   │  扫描 plugins/ 目录                       │
   │  ── describe ───────────────────────────► │  返回节点描述（类型、端口、config schema）
   │  ◄─────────── 节点清单 ──────────────────  │
   │  ── execute(config, execution_id) ──────► │  执行数据库操作
   │  ◄─────────── 结果 / 变量更新 ───────────  │
   │  ── flowEnded(execution_id) ────────────► │  释放 per-execution 连接池
```

- **传输**：stdin/stdout，一行一个 JSON（JSON-RPC 2.0，LF 分隔）
- **驻留**：`capabilities.streaming: true` → 进程常驻，连接池跨执行复用
- **单节点验证**：`db:connect` 声明 `capabilities.single_node: true` →
  宿主 `mpe run-node` / GUI 测试按钮无需改宿主代码即可连通性验证
- **无共享内存**：插件是独立进程，宿主只能通过 JSON 传值

## 1. 项目结构

```
mpe-plugin-db/
├── Cargo.toml            # 独立 package，不依赖宿主 workspace
├── plugin.json           # 宿主扫描用清单（启动描述、驻留模式）
├── .github/workflows/ci.yml  # 三平台构建 + 离线测试 + Release 打包
├── src/
│   ├── main.rs           # mpe_plugin_main! 入口
│   ├── lib.rs            # DbPlugin：Plugin trait 实现（describe/execute/flow_ended）+ 单元测试
│   ├── i18n.rs           # MPE_LOCALE 驱动的中英文案
│   ├── pool.rs           # per-execution sqlx 连接池 + 空闲 reaper
│   └── nodes/
│       ├── mod.rs        # 各节点 execute 分派 + connection_uuid 解析
│       ├── connect.rs    # db:connect
│       ├── query.rs      # db:query
│       ├── begin.rs      # db:begin
│       ├── commit.rs     # db:commit
│       ├── rollback.rs   # db:rollback
│       └── close.rs      # db:close
└── tests/
    └── fixtures/         # 示例流程（sqlite/mysql/postgres/事务），供宿主侧测试用
```

## 2. 节点类型

| type_id | 端口 | 说明 |
|---------|------|------|
| `db:connect` | in/true/false | 建立连接池。`driver` 枚举 `sqlite\|mysql\|postgres`；`connection_string`（sqlite：文件路径，空 = 内存库；mysql/postgres：URL），缺省时回退 `host`/`port`/`database`/`username`/`password` 字段；可选 `min_connections`/`max_connections`、`test_query`、`connect_timeout_ms`。声明 `single_node: true` |
| `db:query` | in/true/false | 执行 SQL。必填 `connection_uuid`（宿主 `x-node-selector` 渲染为对 `db:connect` 节点的选择器）、`sql`（format: sql）；`params` 数组对应 `?` 占位符；`fetch_mode` `all\|one\|scalar`；可选 `result_to` 变量与 `timeout_ms` |
| `db:begin` | in/true/false | 开启事务；可选 `isolation_level`（`READ UNCOMMITTED\|READ COMMITTED\|REPEATABLE READ\|SERIALIZABLE`） |
| `db:commit` | in/true/false | 在所选连接上提交事务 |
| `db:rollback` | in/true/false | 在所选连接上回滚事务 |
| `db:close` | in/out | 释放连接（可选 `connection_uuid` 选中单个，未选释放整个 execution） |

操作节点（`connect`/`query`/`begin`/`commit`/`rollback`）带
`in`/`true`/`false` 三端口：成功路径经宿主默认路由到 `true`，失败由宿主
`on_error` 策略（默认 RouteToFalse）处理。`db:close` 保持单个 `in`/`out` 形态。

`db:query`/`db:begin`/`db:commit`/`db:rollback`/`db:close` 的 `connection_uuid`
字段由宿主 `x-node-selector` 渲染为流程内 `db:connect` 节点的选择器；选中后
宿主把 connect 节点的 instance id 注入 config，插件据此从注册表取连接池条目。

## 3. Cargo.toml（独立构建）

```toml
[package]
name = "mpe-plugin-db"
version = "0.1.0"
edition = "2021"

[dependencies]
mpe-plugin-sdk = { git = "https://github.com/multi-protocol-flow/mpe-plugin-sdk.git", tag = "v0.1.0" }
sqlx = { version = "0.8", default-features = false, features = ["runtime-tokio", "any", "sqlite", "mysql", "postgres", "json", "chrono", "rust_decimal"] }
serde = { version = "1", features = ["derive"] }
serde_json = { version = "1", features = ["preserve_order"] }
tokio = { version = "1", features = ["full"] }
log = "0.4"
chrono = { version = "0.4", features = ["serde"] }
parking_lot = "0.12"

[[bin]]
name = "mpe_plugin_db"
path = "src/main.rs"
```

注意：sqlx 的 `sqlite` feature 自带 bundled libsqlite3-sys，crate 在三个 CI
平台无需系统 SQLite 即可编译。

## 4. 构建与测试

> **plugin.json 的 `entry.command`**：值为市场契约形式的相对路径 `./mpe_plugin_db`。
> 当前宿主以自身 CWD 解析该命令（不 chdir 到插件目录）。仓库内开发调试时，若需宿主
> 直接拉起本插件，可把 command 改为本机绝对路径，或在插件目录下启动宿主。

```bash
# 构建 release 产物（宿主通过 plugin.json entry.command 拉起）
# 仓库根（mpe-plugin-db）下执行
cargo build --release

# 离线单元测试（无需数据库）：describe 注册、x-node-selector 接线、
# password/sql schema 格式
cargo test
```

## 5. 安装与验证（宿主侧）

```bash
# 开发期：构建产物放仓库根（宿主按 MPE_PLUGIN_DIR 或数据目录 plugins 扫描）
# 仓库根（mpe-plugin-db）下执行
cargo build --release

# 单节点连接性验证（capability 驱动，无需宿主白名单改动）
mpe run-node '{"type":"db:connect","driver":"sqlite","connection_string":""}'
# （或传 sqlite 文件路径；mysql/postgres 传 URL）
```

全流程执行（connect → query → close、事务）可从 `tests/fixtures/db_sqlite_plugin_flow.json`
fixture 流程经 `mpe run --flow` 运行，或在 MPE 编辑器中组装。

## 6. 设计要点

- **连接池隔离**：sqlx 连接池按 `(execution_id, connection_uuid)` 复合键隔离，
  并发执行互不串扰。
- **空闲 reaper**：后台任务在 execution 结束后回收空闲连接并关闭连接池。
- **双路径释放**：`flow_ended` 钩子（宿主在完成时广播）+ `db:close` 节点都会释放
  连接池；幂等设计（释放缺失条目是 no-op）。
- **事务**：begin/commit/rollback 节点按连接管理事务，`db:begin` 可选隔离级别。
- **连接串优先**：`connection_string` 优先；缺省时按 `host`/`port`/`database`/
  `username`/`password` 拼 URL。
- **获取模式**：`db:query` 经 `fetch_mode`（`all`/`one`/`scalar`）返回多行/单行/
  标量，可用 `result_to` 存入流程变量。
- **类型化驱动**：使用 sqlx `sqlite`/`mysql`/`postgres` 类型化驱动而非 `Any`
  驱动——后者的 MySQL 类型白名单会排除 TINYINT/DECIMAL/DATETIME 等。

## 7. 与宿主的关系

- 依赖：仅 `mpe-plugin-sdk`（`mpe-plugin-sdk::protocol` wire 契约），**绝不**依赖
  `flow-engine-core` 等宿主类型。
- 交互：describe/execute/validate/shutdown RPC + log 通知 + flowEnded 通知。
- 宿主适配点：`capabilities.single_node` 由宿主 `execute_single_node` /
  `mpe run-node` 的能力门读取（`NodePlugin::supports_single_node()` +
  `SidecarPlugin` 透传），插件侧无需感知宿主实现细节。

## 许可证

MIT，详见 [LICENSE](./LICENSE)。
