//! SQLite/MySQL/PostgreSQL sidecar plugin for MPE.
//!
//! Third-party plugin crate: depends only on `mpe-plugin-sdk` (which in turn
//! bundles the shared wire contract (`mpe-plugin-sdk::protocol`)). Never imports host
//! types (`flow-engine-core`, `flow-engine-plugin`, ...) — the plugin is a
//! separate process speaking JSON-RPC 2.0 over stdio.
//!
//! Node types: `db:connect` / `db:query` / `db:begin` / `db:commit` /
//! `db:rollback` / `db:close`. `execute` dispatches by the `config["type"]`
//! discriminator. The five two-state nodes declare `in`/`true`/`false`
//! tri-ports (success routes via the host default routing to `true`, failure
//! goes through the host `on_error` strategy); `db:close` keeps the single
//! `in`/`out` shape.
//!
//! Pool selection: pools are keyed by the composite `(execution_id,
//! connection_uuid)` (see `pool.rs`). The connect node registers under its
//! own instance id (`ctx.node_instance_id()`); operation nodes pick their
//! pool via the `connection_uuid` field the host resolves into their config
//! from the schema's `x-node-selector`. A connect node with an empty
//! `connection_uuid` falls back to its own instance id (legacy-flow
//! compatibility); operation nodes with an empty `connection_uuid` fail
//! explicitly (an intentional deviation from the old built-in's silent skip).

use std::future::Future;
use std::sync::Arc;

use mpe_plugin_sdk::prelude::*;

mod i18n;
mod nodes;
mod pool;
use pool::DbPoolRegistry;

/// Process-level plugin singleton: node registry + per-execution pool.
pub struct DbPlugin {
    pub pool: Arc<DbPoolRegistry>,
}

impl Default for DbPlugin {
    fn default() -> Self {
        // Typed drivers (sqlx sqlite/mysql/postgres) need no install step —
        // unlike the `Any` driver, which we deliberately do NOT use (its
        // MySQL type whitelist excludes TINYINT/DECIMAL/DATETIME/etc.).
        Self {
            pool: Arc::new(DbPoolRegistry::default()),
        }
    }
}

impl DbPlugin {
    /// Spawns the idle reaper with sensible defaults (30 s idle, 10 s tick)
    /// — called lazily on the first `db:connect` so the plugin process
    /// starts no background tasks before it is actually used. The pool is
    /// `Arc` so the reaper task owns a reference independently of the plugin.
    fn ensure_reaper(&self) {
        use std::sync::Once;
        static STARTED: Once = Once::new();
        STARTED.call_once(|| {
            self.pool.spawn_reaper(
                std::time::Duration::from_secs(30),
                std::time::Duration::from_secs(10),
            );
        });
    }
}

/// Builds one db node description with host-facing metadata.
///
/// `default_config` is injected into a freshly added node; `properties` is
/// the JSON-Schema `properties` map and `required` the required field list.
/// Multi-line SQL uses `"format": "sql"` (the host `SchemaConfigPanel`'s
/// textarea), passwords `"format": "password"` (masked input).
#[allow(clippy::too_many_arguments)]
fn db_node(
    type_id: &str,
    display_name: &str,
    icon: &str,
    color: &str,
    default_config: serde_json::Value,
    properties: serde_json::Value,
    required: &[&str],
    ports: Vec<PortDescription>,
    capabilities: Option<PluginCapabilities>,
) -> NodeDescription {
    let mut node = NodeDescription::new(type_id, display_name);
    node.category = Some("database".to_string());
    node.icon = Some(icon.to_string());
    node.color = Some(color.to_string());
    node.ports = ports;
    node.default_config = default_config;
    node.capabilities = capabilities.unwrap_or_default();
    node.config_schema = Some(serde_json::json!({
        "type": "object",
        "properties": properties,
        "required": required,
    }));
    node
}

/// 操作节点（connect/query/begin/commit/rollback）的双输出端口：
/// `in`(输入) + `true`(成功) + `false`(失败)。失败不在此路由：节点返回
/// `ExecuteResult::fail` 后宿主按 `on_error` 策略（默认 RouteToFalse）自动
/// 走 `false` 端口。宿主默认路由（wrapper `default_success_port`）把
/// 成功路径路由到 `true`。
fn operation_ports() -> Vec<PortDescription> {
    vec![
        PortDescription::new("in", i18n::t("输入", "Input"), PORT_KIND_IN),
        PortDescription::new("true", i18n::t("成功", "Success"), PORT_KIND_OUT),
        PortDescription::new("false", i18n::t("失败", "Failure"), PORT_KIND_OUT),
    ]
}

/// `db:close` 的单输出端口：`in`(输入) + `out`(输出)。
fn close_ports() -> Vec<PortDescription> {
    vec![
        PortDescription::new("in", i18n::t("输入", "Input"), PORT_KIND_IN),
        PortDescription::new("out", i18n::t("输出", "Output"), PORT_KIND_OUT),
    ]
}

/// 操作节点 config_schema 里的 `connection_uuid` 字段：宿主 `x-node-selector`
/// 渲染为对流程内 `db:connect` 节点的选择器，选中后把 connect 节点的
/// instance id 注入 config。
fn connection_uuid_property() -> serde_json::Value {
    serde_json::json!({
        "type": "string",
        "title": i18n::t("连接节点", "Connection node"),
        "description": i18n::t("选择 db:connect 连接节点", "Select the db:connect node"),
        "x-node-selector": { "node_type": "db:connect" }
    })
}

/// 连接参数（db:connect 的连接串部分）——host/port/database/username/
/// password 仅在 connection_string 未提供时用于拼 URL。
///
/// 不暴露 `connection_uuid`：connect 节点总是注册到自己的 instance id
/// （旧流程 config 里遗留的显式 `connection_uuid` 仍由 connect.rs 运行时
/// 兼容读取，但面板不再要求/展示该输入）。
fn host_connection_properties() -> serde_json::Value {
    serde_json::json!({
        "driver": {
            "type": "string",
            "enum": ["sqlite", "mysql", "postgres"],
            "title": i18n::t("驱动", "Driver"),
            "description": i18n::t("数据库驱动", "Database driver"),
        },
        "connection_string": {
            "type": "string",
            "title": i18n::t("连接串", "Connection string"),
            "description": i18n::t("连接串（sqlite 用文件路径，空 = 内存数据库；mysql/postgres 用 URL）", "Connection string (sqlite: file path, empty = in-memory; mysql/postgres: URL)"),
        },
        "host": { "type": "string", "title": i18n::t("主机", "Host"), "description": i18n::t("主机（连接串为空时用）", "Host (used when connection string is empty)") },
        "port": { "type": "integer", "title": i18n::t("端口", "Port"), "description": i18n::t("端口", "Port") },
        "database": { "type": "string", "title": i18n::t("数据库名", "Database"), "description": i18n::t("数据库名", "Database name") },
        "username": { "type": "string", "title": i18n::t("用户名", "Username"), "description": i18n::t("用户名", "Username") },
        "password": { "type": "string", "format": "password", "title": i18n::t("密码", "Password"), "description": i18n::t("密码", "Password") },
        "min_connections": { "type": "integer", "title": i18n::t("最小连接数", "Min connections"), "description": i18n::t("最小连接数", "Min connections") },
        "max_connections": { "type": "integer", "title": i18n::t("最大连接数", "Max connections"), "description": i18n::t("最大连接数", "Max connections") },
        "test_query": { "type": "string", "title": i18n::t("连接测试查询", "Test query"), "description": i18n::t("连接测试查询（可选）", "Connection test query (optional)") },
        "connect_timeout_ms": { "type": "integer", "title": i18n::t("连接超时 (ms)", "Connect timeout (ms)"), "description": i18n::t("连接超时（毫秒）", "Connect timeout (ms)") },
    })
}

impl Plugin for DbPlugin {
    fn describe(&self) -> Vec<NodeDescription> {
        vec![
            // `db:connect` — 建立连接池 + 测试查询。
            db_node(
                "db:connect",
                "DB Connect",
                "database",
                "#10b981",
                serde_json::json!({
                    "driver": "sqlite",
                    "connection_string": "",
                    "max_connections": 10,
                    "connect_timeout_ms": 5000,
                }),
                host_connection_properties(),
                &["driver"],
                operation_ports(),
                // db:connect 可经宿主单节点路径（execute_single_node /
                // mpe run node）做连接性验证 → 声明 single_node capability。
                Some(PluginCapabilities {
                    single_node: true,
                    ..Default::default()
                }),
            ),
            // `db:query` — SQL + 参数 + fetch 模式。
            db_node(
                "db:query",
                "DB Query",
                "search",
                "#10b981",
                serde_json::json!({
                    "sql": "",
                    "params": [],
                    "fetch_mode": "all",
                }),
                serde_json::json!({
                    "connection_uuid": connection_uuid_property(),
                    "sql": { "type": "string", "format": "sql", "title": i18n::t("SQL 语句", "SQL"), "description": i18n::t("SQL 语句", "SQL statement") },
                    "params": { "type": "array", "items": { "type": "string" }, "title": i18n::t("SQL 参数", "Parameters"), "description": i18n::t("SQL 参数（? 占位符）", "SQL parameters (for ? placeholders)") },
                    "fetch_mode": { "type": "string", "enum": ["all", "one", "scalar"], "title": i18n::t("获取模式", "Fetch mode"), "description": i18n::t("结果获取模式", "Result fetch mode") },
                    "result_to": { "type": "string", "title": i18n::t("结果变量名", "Result variable"), "description": i18n::t("结果存储变量名（可选）", "Variable to store the result in (optional)") },
                    "timeout_ms": { "type": "integer", "title": i18n::t("查询超时 (ms)", "Query timeout (ms)"), "description": i18n::t("查询超时（毫秒）", "Query timeout (ms)") },
                }),
                &["connection_uuid", "sql"],
                operation_ports(),
                None,
            ),
            // `db:begin` — 事务开始。
            db_node(
                "db:begin",
                "DB Begin",
                "play",
                "#10b981",
                serde_json::json!({}),
                serde_json::json!({
                    "connection_uuid": connection_uuid_property(),
                    "isolation_level": { "type": "string", "enum": ["READ UNCOMMITTED", "READ COMMITTED", "REPEATABLE READ", "SERIALIZABLE"], "title": i18n::t("隔离级别", "Isolation level"), "description": i18n::t("隔离级别（可选）", "Isolation level (optional)") },
                }),
                &["connection_uuid"],
                operation_ports(),
                None,
            ),
            // `db:commit` — 提交事务。
            db_node(
                "db:commit",
                "DB Commit",
                "check",
                "#10b981",
                serde_json::json!({}),
                serde_json::json!({
                    "connection_uuid": connection_uuid_property(),
                }),
                &["connection_uuid"],
                operation_ports(),
                None,
            ),
            // `db:rollback` — 回滚事务。
            db_node(
                "db:rollback",
                "DB Rollback",
                "rotate-ccw",
                "#10b981",
                serde_json::json!({}),
                serde_json::json!({
                    "connection_uuid": connection_uuid_property(),
                }),
                &["connection_uuid"],
                operation_ports(),
                None,
            ),
            // `db:close` — 主动释放连接（可选 connection_uuid 选中单个，
            // 未选则释放当前执行的全部连接）。
            db_node(
                "db:close",
                "DB Close",
                "plug",
                "#10b981",
                serde_json::json!({}),
                serde_json::json!({
                    "connection_uuid": connection_uuid_property(),
                }),
                &[],
                close_ports(),
                None,
            ),
        ]
    }

    // The runtime spawns execute futures, so the future must be Send; an
    // `async fn` cannot express that bound on stable (see `Plugin`).
    #[allow(clippy::manual_async_fn)]
    fn execute(&self, ctx: &mut ExecuteContext) -> impl Future<Output = ExecuteResult> + Send {
        async move {
            let node_type = ctx
                .config()
                .get("type")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default();
            match node_type {
                "db:connect" => {
                    self.ensure_reaper();
                    nodes::connect::execute(ctx, &self.pool).await
                }
                "db:query" => nodes::query::execute(ctx, &self.pool).await,
                "db:begin" => nodes::begin::execute(ctx, &self.pool).await,
                "db:commit" => nodes::commit::execute(ctx, &self.pool).await,
                "db:rollback" => nodes::rollback::execute(ctx, &self.pool).await,
                "db:close" => nodes::close::execute(ctx, &self.pool).await,
                other => ExecuteResult::fail(
                    i18n::t("未知节点类型", "Unknown node type").to_string() + &format!(" `{other}`"),
                ),
            }
        }
    }

    // Flow-completion hook — release path 1: the host broadcasts flowEnded
    // when a flow execution finishes (success, failure, or cancellation),
    // and the plugin releases the per-execution pools here. The `db:close`
    // node is release path 2 for flows that disconnect mid-run; both may
    // fire for one execution, and `release` is a no-op on a missing entry,
    // so double-release is harmless.
    fn flow_ended(&self, execution_id: &str) {
        self.pool.release(execution_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Describe must register exactly the 6 db nodes with the documented
    /// type ids and port shapes.
    #[test]
    fn describe_registers_six_db_nodes() {
        let plugin = DbPlugin::default();
        let nodes = plugin.describe();
        let ids: Vec<&str> = nodes.iter().map(|n| n.type_id.as_str()).collect();
        assert_eq!(
            ids,
            vec![
                "db:connect",
                "db:query",
                "db:begin",
                "db:commit",
                "db:rollback",
                "db:close"
            ]
        );
        for node in &nodes {
            let ports = &node.ports;
            assert!(
                ports.iter().any(|p| p.id == "in"),
                "{} must have an in port",
                node.type_id
            );
        }
        // Two-state nodes carry true/false; close carries only out.
        let connect = &nodes[0];
        let ids: Vec<&str> = connect
            .ports
            .iter()
            .map(|p| p.id.as_str())
            .collect();
        assert!(ids.contains(&"true") && ids.contains(&"false"));
        let close = &nodes[5];
        let ids: Vec<&str> = close
            .ports
            .iter()
            .map(|p| p.id.as_str())
            .collect();
        assert!(ids.contains(&"out") && !ids.contains(&"true"));
    }

    /// Every operation node's schema must declare `connection_uuid` with the
    /// `x-node-selector` pointing at `db:connect` (except connect itself).
    #[test]
    fn operation_nodes_carry_connection_selector() {
        let plugin = DbPlugin::default();
        let nodes = plugin.describe();
        for node in nodes.iter().skip(1) {
            let schema = node.config_schema.as_ref().expect("schema present");
            let selector = schema
                .pointer("/properties/connection_uuid/x-node-selector/node_type")
                .and_then(serde_json::Value::as_str);
            assert_eq!(selector, Some("db:connect"), "{} selector", node.type_id);
        }
    }

    /// Config schema capabilities the host panel relies on: password mask on
    /// the password field, sql format (textarea) on the query field.
    #[test]
    fn schema_uses_password_and_sql_formats() {
        let plugin = DbPlugin::default();
        let nodes = plugin.describe();
        let connect = &nodes[0];
        assert_eq!(
            connect
                .config_schema
                .as_ref()
                .unwrap()
                .pointer("/properties/password/format")
                .and_then(serde_json::Value::as_str),
            Some("password")
        );
        let query = &nodes[1];
        assert_eq!(
            query
                .config_schema
                .as_ref()
                .unwrap()
                .pointer("/properties/sql/format")
                .and_then(serde_json::Value::as_str),
            Some("sql")
        );
    }

    /// db:connect 的面板不暴露 connection_uuid：连接节点总是注册到自身
    /// instance id，旧流程 config 里的显式值仅由运行时兼容读取。
    #[test]
    fn connect_schema_has_no_connection_uuid_field() {
        let plugin = DbPlugin::default();
        let nodes = plugin.describe();
        assert!(
            nodes[0]
                .config_schema
                .as_ref()
                .unwrap()
                .pointer("/properties/connection_uuid")
                .is_none(),
            "db:connect must not expose a connection_uuid input"
        );
    }

    /// 每个字段都必须提供不带下划线的 title——前端 label 回退到 key 时会
    /// 显示 connection_string 这类带下划线的字段名。
    #[test]
    fn all_properties_have_underscore_free_titles() {
        let plugin = DbPlugin::default();
        for node in plugin.describe() {
            let schema = node.config_schema.as_ref().expect("schema present");
            let properties = schema["properties"]
                .as_object()
                .expect("properties object");
            for (key, prop) in properties {
                let title = prop.get("title").and_then(serde_json::Value::as_str);
                assert!(
                    title.is_some_and(|t| !t.is_empty() && !t.contains('_')),
                    "{}: property `{key}` must have an underscore-free title, got {title:?}",
                    node.type_id
                );
            }
        }
    }

    /// 面板字段顺序 = schema 声明顺序（serde_json preserve_order 保证）：
    /// db:connect 驱动/连接串在前；db:query 连接选择器、SQL 在最前。
    #[test]
    fn properties_preserve_declared_order() {
        let plugin = DbPlugin::default();
        let nodes = plugin.describe();
        let keys = |node: &NodeDescription| -> Vec<String> {
            node.config_schema.as_ref().unwrap()["properties"]
                .as_object()
                .unwrap()
                .keys()
                .cloned()
                .collect()
        };
        assert_eq!(
            keys(&nodes[0]),
            vec![
                "driver",
                "connection_string",
                "host",
                "port",
                "database",
                "username",
                "password",
                "min_connections",
                "max_connections",
                "test_query",
                "connect_timeout_ms",
            ]
        );
        assert_eq!(
            keys(&nodes[1]),
            vec![
                "connection_uuid",
                "sql",
                "params",
                "fetch_mode",
                "result_to",
                "timeout_ms",
            ]
        );
    }

    /// db:connect 默认配置：connection_string 为空（sqlite 空串运行时按
    /// 内存库处理），不再把 :memory: 写进新节点。
    #[test]
    fn connect_default_config_has_empty_connection_string() {
        let plugin = DbPlugin::default();
        let nodes = plugin.describe();
        let default = &nodes[0].default_config;
        assert_eq!(default["driver"], "sqlite");
        assert_eq!(
            default["connection_string"].as_str(),
            Some(""),
            "default connection_string must be empty, not :memory:"
        );
    }
}
