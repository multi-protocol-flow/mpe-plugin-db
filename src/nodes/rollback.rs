//! `db:rollback` node — rolls back the active transaction for the key.
//!
//! Config: `{ connection_uuid }`. An empty/missing `connection_uuid` fails
//! explicitly; rolling back without an active transaction is an error.

use mpe_plugin_sdk::prelude::*;

use std::sync::Arc;

use crate::pool::DbPoolRegistry;

/// Thin adapter around [`rollback_core`].
pub async fn execute(ctx: &mut ExecuteContext, pool: &Arc<DbPoolRegistry>) -> ExecuteResult {
    let exec_id = ctx.execution_id().unwrap_or("default").to_string();
    let config = ctx.config().clone();
    match rollback_core(pool, &exec_id, &config).await {
        Ok(result) => result,
        Err(msg) => {
            ctx.log("error", &msg);
            ExecuteResult::fail(msg)
        }
    }
}

/// Core rollback logic, testable without an [`ExecuteContext`].
async fn rollback_core(
    pool: &DbPoolRegistry,
    exec_id: &str,
    config: &serde_json::Value,
) -> Result<ExecuteResult, String> {
    let connection_uuid = config
        .get("connection_uuid")
        .and_then(serde_json::Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| crate::i18n::t("缺少 connection_uuid", "connection_uuid is required").to_string())?;
    pool.rollback(exec_id, connection_uuid).await?;
    Ok(ExecuteResult::ok(serde_json::json!({
        "rolled_back": true,
        "connection_uuid": connection_uuid,
    })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pool::DbPool;

    #[tokio::test]
    async fn rollback_without_begin_fails() {
        let registry = DbPoolRegistry::default();
        let pool = DbPool::Sqlite(
            sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("pool")
            );
        registry.connect("exec-1", "conn-a", "sqlite::memory:", "sqlite", pool);
        let err = rollback_core(
            &registry,
            "exec-1",
            &serde_json::json!({ "connection_uuid": "conn-a" }),
        )
        .await
        .expect_err("rollback without a transaction must error explicitly");
        assert_eq!(err, "no active transaction");
    }

    #[tokio::test]
    async fn rollback_after_begin_succeeds() {
        let registry = DbPoolRegistry::default();
        let pool = DbPool::Sqlite(
            sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("pool")
            );
        registry.connect("exec-1", "conn-a", "sqlite::memory:", "sqlite", pool);
        registry.begin("exec-1", "conn-a", None).await.expect("begin");
        let result = rollback_core(
            &registry,
            "exec-1",
            &serde_json::json!({ "connection_uuid": "conn-a" }),
        )
        .await
        .expect("rollback");
        assert_eq!(result.success, true);
    }
}
