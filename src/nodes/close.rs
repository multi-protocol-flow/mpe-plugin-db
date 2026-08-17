//! `db:close` node — releases the pool for the key.
//!
//! Config: `{ connection_uuid? }`. With a `connection_uuid` only that pool is
//! released; without one, EVERY pool of the current execution is released
//! (the built-in `db_close` required a uuid; here the missing value releases
//! the whole execution — matches the mongo plugin's `mongo:close` shape and
//! is a superset of the old single-uuid behavior). Single `out` port.

use mpe_plugin_sdk::prelude::*;

use std::sync::Arc;

use crate::pool::DbPoolRegistry;

/// Thin adapter around [`close_core`].
pub async fn execute(ctx: &mut ExecuteContext, pool: &Arc<DbPoolRegistry>) -> ExecuteResult {
    let exec_id = ctx.execution_id().unwrap_or("default").to_string();
    let config = ctx.config().clone();
    close_core(pool, &exec_id, &config)
}

/// Core close logic — synchronous (releasing a pool is a map remove).
fn close_core(
    pool: &DbPoolRegistry,
    exec_id: &str,
    config: &serde_json::Value,
) -> ExecuteResult {
    let connection_uuid = config
        .get("connection_uuid")
        .and_then(serde_json::Value::as_str)
        .filter(|s| !s.is_empty());
    match connection_uuid {
        Some(uuid) => {
            pool.release_connection(exec_id, uuid);
            ExecuteResult::ok(serde_json::json!({
                "closed": true,
                "connection_uuid": uuid,
            }))
        }
        None => {
            pool.release(exec_id);
            ExecuteResult::ok(serde_json::json!({
                "closed": true,
                "all_connections": true,
            }))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pool::DbPool;

    #[tokio::test]
    async fn close_single_connection() {
        let registry = DbPoolRegistry::default();
        let pool = DbPool::Sqlite(
            sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("pool")
            );
        registry.connect("exec-1", "conn-a", "sqlite::memory:", "sqlite", pool);
        let pool_b = DbPool::Sqlite(
            sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("pool")
            );
        registry.connect("exec-1", "conn-b", "sqlite::memory:", "sqlite", pool_b);

        let result = close_core(
            &registry,
            "exec-1",
            &serde_json::json!({ "connection_uuid": "conn-a" }),
        );
        assert_eq!(result.success, true);
        assert!(registry.pool("exec-1", "conn-a").is_none());
        assert!(
            registry.pool("exec-1", "conn-b").is_some(),
            "other connection must survive"
        );
    }

    #[tokio::test]
    async fn close_releases_all_of_execution() {
        let registry = DbPoolRegistry::default();
        for uuid in ["conn-a", "conn-b"] {
            let pool = DbPool::Sqlite(
                sqlx::sqlite::SqlitePoolOptions::new()
                    .max_connections(1)
                    .connect("sqlite::memory:")
                    .await
                    .expect("pool"),
            );
            registry.connect("exec-1", uuid, "sqlite::memory:", "sqlite", pool);
        }
        let result = close_core(&registry, "exec-1", &serde_json::json!({}));
        assert_eq!(result.success, true);
        assert!(registry.pool("exec-1", "conn-a").is_none());
        assert!(registry.pool("exec-1", "conn-b").is_none());
    }

    #[tokio::test]
    async fn close_idempotent() {
        let registry = DbPoolRegistry::default();
        let result = close_core(
            &registry,
            "exec-1",
            &serde_json::json!({ "connection_uuid": "conn-a" }),
        );
        assert_eq!(result.success, true, "close on a missing pool is a no-op success");
    }
}
