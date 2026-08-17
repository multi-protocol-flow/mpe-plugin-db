//! `db:connect` node — establishes a database pool with an optional test
//! query and idempotent pool reuse.
//!
//! Config: `{ driver, connection_string, host?, port?, database?,
//! username?, password?, min_connections?, max_connections?, test_query?,
//! connect_timeout_ms? }`. `driver` is required (`sqlite` | `mysql` |
//! `postgres`); the connection target is `connection_string` when present,
//! else assembled from host/port/database/username/password.
//!
//! Output: `{ connected: true, connection_id, connection_uuid, request,
//! timing }`.
//!
//! Idempotency: a second connect for the same `(execution_id,
//! connection_uuid)` with the SAME target reuses the pooled entry; a
//! DIFFERENT target replaces it (surfaced via `ctx.log("warn", …)`).

use std::time::Instant;

use mpe_plugin_sdk::prelude::*;

use std::sync::Arc;

use crate::pool::DbPoolRegistry;

/// Default connect + statement timeout in milliseconds.
const DEFAULT_TIMEOUT_MS: u64 = 5000;
/// Default max connections when `max_connections` is missing.
const DEFAULT_MAX_CONNECTIONS: u32 = 10;

/// Thin `ExecuteContext` adapter: extracts `execution_id` (fallback
/// `"default"` for SDK tests that send none) and the connection selector
/// (`connection_uuid` — the host sends `node_instance_id` = the connect
/// node's own uuid, so operation nodes can pick THIS pool via their
/// `connection_uuid` config), then delegates to [`connect_core`].
pub async fn execute(ctx: &mut ExecuteContext, pool: &Arc<DbPoolRegistry>) -> ExecuteResult {
    let exec_id = ctx.execution_id().unwrap_or("default").to_string();
    let connection_uuid = ctx
        .config()
        .get("connection_uuid")
        .and_then(serde_json::Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| ctx.node_instance_id().unwrap_or("default").to_string());
    let config = ctx.config().clone();
    match connect_core(pool, &exec_id, &connection_uuid, &config).await {
        Ok((result, replaced)) => {
            if replaced {
                ctx.log(
                    "warn",
                    crate::i18n::t("连接目标变化，已替换旧连接", "Connection target changed; old connection replaced"),
                );
            }
            result
        }
        Err(msg) => {
            ctx.log("error", &msg);
            ExecuteResult::fail(msg)
        }
    }
}

/// Builds the sqlx pool connection URL from the connect config.
///
/// `connection_string` wins when present; otherwise the URL is assembled
/// from host/port/database/username/password (mysql/postgres). SQLite uses
/// `sqlite://<path>` with `:memory:` mapped to `sqlite::memory:`; an empty
/// connection_string with the sqlite driver falls back to an in-memory
/// database (`sqlite::memory:`) so the blank panel default still connects.
fn build_connection_url(driver: &str, config: &serde_json::Value) -> Result<String, String> {
    let connection_string = config
        .get("connection_string")
        .and_then(serde_json::Value::as_str)
        .filter(|s| !s.is_empty());
    if let Some(cs) = connection_string {
        return match driver {
            "sqlite" => Ok(if cs == ":memory:" {
                "sqlite::memory:".to_string()
            } else {
                format!("sqlite://{cs}")
            }),
            "mysql" => Ok(cs.to_string()),
            "postgres" => Ok(cs.to_string()),
            other => Err(crate::i18n::t("不支持的驱动", "unsupported driver").to_string()
                + &format!(" `{other}`")),
        };
    }
    let host = config.get("host").and_then(serde_json::Value::as_str);
    // Port accepts number or string (`{{var}}`-resolved values arrive as
    // strings); an unparseable value is an explicit error, never a silent
    // fallback to the driver default. `0`/absent → driver default below.
    let port = match config.get("port") {
        Some(serde_json::Value::Number(n)) => n.as_u64().ok_or_else(|| {
            crate::i18n::t("端口无效", "Invalid port number").to_string()
        })?,
        Some(serde_json::Value::String(s)) if !s.is_empty() => s.parse::<u64>().map_err(|_| {
            crate::i18n::t("端口无效", "Invalid port number").to_string()
        })?,
        _ => 0,
    };
    let database = config.get("database").and_then(serde_json::Value::as_str);
    let username = config.get("username").and_then(serde_json::Value::as_str);
    let password = config.get("password").and_then(serde_json::Value::as_str);
    match driver {
        "sqlite" => Ok("sqlite::memory:".to_string()),
        "mysql" => {
            let host = host.ok_or_else(|| crate::i18n::t("缺少 host", "missing host").to_string())?;
            let database = database
                .ok_or_else(|| crate::i18n::t("缺少 database", "missing database").to_string())?;
            let port = if port == 0 { 3306 } else { port };
            Ok(format!(
                "mysql://{}:{}@{}:{}/{}",
                username.unwrap_or(""),
                password.unwrap_or(""),
                host,
                port,
                database
            ))
        }
        "postgres" => {
            let host = host.ok_or_else(|| crate::i18n::t("缺少 host", "missing host").to_string())?;
            let database = database
                .ok_or_else(|| crate::i18n::t("缺少 database", "missing database").to_string())?;
            let port = if port == 0 { 5432 } else { port };
            Ok(format!(
                "postgres://{}:{}@{}:{}/{}",
                username.unwrap_or(""),
                password.unwrap_or(""),
                host,
                port,
                database
            ))
        }
        other => Err(crate::i18n::t("不支持的驱动", "unsupported driver").to_string()
            + &format!(" `{other}`")),
    }
}

/// Core connect logic, testable without an [`ExecuteContext`].
///
/// Returns `(result, replaced)`; `replaced` is `true` only when an existing
/// pool with a DIFFERENT target was swapped out.
async fn connect_core(
    pool: &DbPoolRegistry,
    exec_id: &str,
    connection_uuid: &str,
    config: &serde_json::Value,
) -> Result<(ExecuteResult, bool), String> {
    let driver = config
        .get("driver")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| crate::i18n::t("缺少 driver", "missing driver").to_string())?;
    let url = build_connection_url(driver, config)?;
    let timeout_ms = config
        .get("connect_timeout_ms")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(DEFAULT_TIMEOUT_MS);
    let max_connections = config
        .get("max_connections")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(DEFAULT_MAX_CONNECTIONS as u64) as u32;
    let min_connections = config
        .get("min_connections")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0) as u32;
    let test_query = config
        .get("test_query")
        .and_then(serde_json::Value::as_str)
        .filter(|s| !s.is_empty());

    // Idempotent reuse: the registry's `connect()` compares the target
    // internally — same target returns `replaced=false` without creating a
    // new pool, so no fast-path check is needed here.

    let start = Instant::now();
    // SQLite `:memory:` is PER-CONNECTION state — every connection in a pool
    // sees its own private empty database. Force a single connection for
    // in-memory SQLite so tables created on the pool remain visible to
    // later queries (also matches the built-in semantics).
    let effective_max = if driver == "sqlite" && url.contains(":memory:") {
        1
    } else {
        max_connections
    };
    let timeout = std::time::Duration::from_millis(timeout_ms);
    // Build the pool with the driver's TYPED options. The sqlx `Any` driver
    // is NOT used: its MySQL type whitelist excludes TINYINT/DECIMAL/
    // DATETIME/JSON etc. and fails during describe (see pool.rs module doc).
    let conn_pool = match driver {
        "sqlite" => {
            let p = sqlx::sqlite::SqlitePoolOptions::new()
                .max_connections(effective_max)
                .min_connections(min_connections.min(effective_max))
                .acquire_timeout(timeout)
                .connect(&url)
                .await
                .map_err(|e| {
                    let msg = crate::i18n::t("数据库连接失败", "database connect failed");
                    format!("{msg} (`{url}`): {e}")
                })?;
            crate::pool::DbPool::Sqlite(p)
        }
        "mysql" => {
            let p = sqlx::mysql::MySqlPoolOptions::new()
                .max_connections(effective_max)
                .min_connections(min_connections.min(effective_max))
                .acquire_timeout(timeout)
                .connect(&url)
                .await
                .map_err(|e| {
                    let msg = crate::i18n::t("数据库连接失败", "database connect failed");
                    format!("{msg} (`{url}`): {e}")
                })?;
            crate::pool::DbPool::MySql(p)
        }
        "postgres" => {
            let p = sqlx::postgres::PgPoolOptions::new()
                .max_connections(effective_max)
                .min_connections(min_connections.min(effective_max))
                .acquire_timeout(timeout)
                .connect(&url)
                .await
                .map_err(|e| {
                    let msg = crate::i18n::t("数据库连接失败", "database connect failed");
                    format!("{msg} (`{url}`): {e}")
                })?;
            crate::pool::DbPool::Postgres(p)
        }
        other => {
            return Err(crate::i18n::t("不支持的驱动", "unsupported driver").to_string()
                + &format!(" `{other}`"));
        }
    };

    // Optional test query verifies the server is reachable BEFORE
    // registering the pool (sqlx pools are lazy).
    if let Some(tq) = test_query {
        let _ = &conn_pool;
        let result = match &conn_pool {
            crate::pool::DbPool::Sqlite(p) => {
                sqlx::query(tq).execute(p).await.map(|_| ())
            }
            crate::pool::DbPool::MySql(p) => {
                sqlx::query(tq).execute(p).await.map(|_| ())
            }
            crate::pool::DbPool::Postgres(p) => {
                sqlx::query(tq).execute(p).await.map(|_| ())
            }
        };
        result.map_err(|e| {
            let msg = crate::i18n::t("连接测试查询失败", "connection test query failed");
            format!("{msg}: {e}")
        })?;
    }

    let replaced = pool.connect(exec_id, connection_uuid, &url, driver, conn_pool);
    let total_ms = start.elapsed().as_millis() as u64;
    Ok((
        ExecuteResult::ok(serde_json::json!({
            "connected": true,
            "connection_id": connection_uuid,
            "connection_uuid": connection_uuid,
            "request": {
                "connection_uuid": connection_uuid,
                "test_query": test_query,
                "connect_timeout_ms": timeout_ms,
            },
            "timing": { "connect_ms": total_ms, "total_ms": total_ms },
        })),
        replaced,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_url_sqlite_memory() {
        let config = serde_json::json!({ "driver": "sqlite", "connection_string": ":memory:" });
        assert_eq!(
            build_connection_url("sqlite", &config).expect("url"),
            "sqlite::memory:"
        );
    }

    #[test]
    fn build_url_sqlite_empty_is_memory() {
        let config = serde_json::json!({ "driver": "sqlite", "connection_string": "" });
        assert_eq!(
            build_connection_url("sqlite", &config).expect("url"),
            "sqlite::memory:"
        );
    }

    #[test]
    fn build_url_sqlite_path() {
        let config = serde_json::json!({ "driver": "sqlite", "connection_string": "/tmp/x.db" });
        assert_eq!(
            build_connection_url("sqlite", &config).expect("url"),
            "sqlite:///tmp/x.db"
        );
    }

    #[test]
    fn build_url_mysql_components() {
        let config = serde_json::json!({
            "driver": "mysql",
            "host": "db.local",
            "port": 3307,
            "database": "app",
            "username": "root",
            "password": "pw",
        });
        assert_eq!(
            build_connection_url("mysql", &config).expect("url"),
            "mysql://root:pw@db.local:3307/app"
        );
    }

    #[test]
    fn build_url_missing_host_is_error() {
        let config = serde_json::json!({ "driver": "mysql", "database": "app" });
        assert!(build_connection_url("mysql", &config).is_err());
    }

    #[test]
    fn build_url_unsupported_driver() {
        let config = serde_json::json!({ "driver": "oracle", "connection_string": "x" });
        assert!(build_connection_url("oracle", &config).is_err());
    }

    #[tokio::test]
    async fn connect_sqlite_memory() {
        let registry = DbPoolRegistry::default();
        let config = serde_json::json!({
            "driver": "sqlite",
            "connection_string": ":memory:",
        });
        let (result, replaced) = connect_core(&registry, "exec-1", "conn-a", &config)
            .await
            .expect("connect");
        assert!(!replaced);
        assert_eq!(result.success, true);
        assert!(registry.pool("exec-1", "conn-a").is_some());
        // Idempotent second connect with the same target.
        let (_, replaced) = connect_core(&registry, "exec-1", "conn-a", &config)
            .await
            .expect("connect");
        assert!(!replaced, "same target must not report replacement");
    }
}
