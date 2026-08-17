//! `db:begin` node — starts a transaction on the pool's dedicated
//! connection, recording `Active` state for the key.
//!
//! Config: `{ connection_uuid, isolation_level? }`. An empty
//! `connection_uuid` fails explicitly; a second begin on the same key is an
//! error (double-begin is a config mistake, never a silent no-op).

use mpe_plugin_sdk::prelude::*;

use std::sync::Arc;

use crate::pool::DbPoolRegistry;

/// Thin adapter around [`begin_core`] (see `connect.rs` for the rationale).
pub async fn execute(ctx: &mut ExecuteContext, pool: &Arc<DbPoolRegistry>) -> ExecuteResult {
    let exec_id = ctx.execution_id().unwrap_or("default").to_string();
    let config = ctx.config().clone();
    match begin_core(pool, &exec_id, &config).await {
        Ok(result) => result,
        Err(msg) => {
            ctx.log("error", &msg);
            ExecuteResult::fail(msg)
        }
    }
}

/// Core begin logic, testable without an [`ExecuteContext`].
async fn begin_core(
    pool: &DbPoolRegistry,
    exec_id: &str,
    config: &serde_json::Value,
) -> Result<ExecuteResult, String> {
    let connection_uuid = config
        .get("connection_uuid")
        .and_then(serde_json::Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| crate::i18n::t("缺少 connection_uuid", "connection_uuid is required").to_string())?;
    let isolation_level = config
        .get("isolation_level")
        .and_then(serde_json::Value::as_str)
        .filter(|s| !s.is_empty());
    let driver = pool.driver_of(exec_id, connection_uuid).unwrap_or_default();

    // Isolation level is applied by the registry's `begin()` ON the
    // transaction's own connection (MySQL: before START TRANSACTION on the
    // acquired session; Postgres: after BEGIN inside the transaction block).
    // SQLite has no isolation statement (always SERIALIZABLE) — `SET
    // TRANSACTION ISOLATION LEVEL` is a syntax error there, so skip. The
    // legacy built-in stored snake_case values (`read_committed`) in flow
    // files; the SQL standard expects `READ COMMITTED`, so normalize.
    let normalized = if driver == "sqlite" {
        None
    } else {
        isolation_level.map(normalize_isolation_level)
    };
    pool.begin(exec_id, connection_uuid, normalized.as_deref()).await?;

    Ok(ExecuteResult::ok(serde_json::json!({
        "started": true,
        "connection_uuid": connection_uuid,
    })))
}

/// Normalizes snake_case / lowercase isolation levels to SQL-standard names.
///
/// Legacy flow files store `read_committed`, `repeatable_read`, etc.;
/// MySQL/PostgreSQL expect `READ COMMITTED` / `REPEATABLE READ`.
fn normalize_isolation_level(level: &str) -> String {
    match level.to_ascii_lowercase().replace('-', "_").as_str() {
        "read_uncommitted" => "READ UNCOMMITTED".to_string(),
        "read_committed" => "READ COMMITTED".to_string(),
        "repeatable_read" => "REPEATABLE READ".to_string(),
        "serializable" => "SERIALIZABLE".to_string(),
        _ => level.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pool::DbPool;

    #[tokio::test]
    async fn begin_and_commit_sqlite() {
        let registry = DbPoolRegistry::default();
        let pool = DbPool::Sqlite(
            sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("pool")
            );
        registry.connect("exec-1", "conn-a", "sqlite::memory:", "sqlite", pool);

        let result = begin_core(
            &registry,
            "exec-1",
            &serde_json::json!({ "connection_uuid": "conn-a" }),
        )
        .await
        .expect("begin");
        assert_eq!(result.success, true);

        let err = begin_core(
            &registry,
            "exec-1",
            &serde_json::json!({ "connection_uuid": "conn-a" }),
        )
        .await
        .expect_err("double begin must error");
        assert_eq!(err, "transaction already active (double begin)");
    }

    #[tokio::test]
    async fn begin_missing_uuid_fails() {
        let registry = DbPoolRegistry::default();
        let err = begin_core(
            &registry,
            "exec-1",
            &serde_json::json!({}),
        )
        .await
        .expect_err("missing uuid must error");
        assert_eq!(err, "connection_uuid is required");
    }
}
