//! `db:query` node — executes SQL with positional params, honoring the
//! `fetch_mode` (`all` | `one` | `scalar`) and optionally storing the result
//! into a flow variable (`result_to`).
//!
//! Config: `{ connection_uuid, sql, params?, fetch_mode?, result_to?,
//! timeout_ms? }`. `connection_uuid` and `sql` are required — an empty
//! `connection_uuid` fails explicitly (intentional deviation from the old
//! built-in's silent skip).
//!
//! Output mirrors the built-in shape: SELECT → `{ rows, row_count,
//! affected_rows, columns, truncated, timing }`; DML → `{ rows: [],
//! row_count: 0, affected_rows, last_insert_id, timing }`.

use std::time::Instant;

use mpe_plugin_sdk::prelude::*;

use std::sync::Arc;

use crate::pool::DbPoolRegistry;

/// Max result rows kept in `rows` (built-in parity: 10000; extras dropped
/// with `truncated: true`).
const MAX_ROWS: usize = 10000;

/// Extracts `connection_uuid` from the config; an empty/missing value is an
/// explicit error (db-plugin semantics, §3.4).
fn require_connection_uuid(config: &serde_json::Value) -> Result<String, String> {
    config
        .get("connection_uuid")
        .and_then(serde_json::Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .ok_or_else(|| crate::i18n::t("缺少 connection_uuid", "connection_uuid is required").to_string())
}

/// Parses `params` into a Vec of JSON values. Accepts a JSON array, or a
/// JSON-encoded array string (the host panel stores text until it parses as
/// a valid array); anything else is treated as no params.
fn parse_params(config: &serde_json::Value) -> Vec<serde_json::Value> {
    match config.get("params") {
        Some(serde_json::Value::Array(a)) => a.clone(),
        Some(serde_json::Value::String(s)) => serde_json::from_str(s).unwrap_or_default(),
        _ => Vec::new(),
    }
}

/// Executes `sql` on the pool (or the active transaction connection) with
/// `params`, returning the result JSON matching the built-in output shape.
pub async fn execute(ctx: &mut ExecuteContext, pool: &Arc<DbPoolRegistry>) -> ExecuteResult {
    let exec_id = ctx.execution_id().unwrap_or("default").to_string();
    let config = ctx.config().clone();
    match execute_core(pool, &exec_id, &config).await {
        Ok((result, in_tx)) => {
            if in_tx {
                let _ = &result;
            }
            // result_to: store the query output into a flow variable (A10
            // contract — flat top-level names only).
            if let Some(name) = config
                .get("result_to")
                .and_then(serde_json::Value::as_str)
                .filter(|s| !s.is_empty())
            {
                if let Some(data) = &result.output_data {
                    ctx.set_variable(name, data.clone());
                }
            }
            result
        }
        Err(msg) => {
            ctx.log("error", &msg);
            ExecuteResult::fail(msg)
        }
    }
}

/// Core query logic, testable without an [`ExecuteContext`].
async fn execute_core(
    pool: &DbPoolRegistry,
    exec_id: &str,
    config: &serde_json::Value,
) -> Result<(ExecuteResult, bool), String> {
    let connection_uuid = require_connection_uuid(config)?;
    let sql = config
        .get("sql")
        .and_then(serde_json::Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| crate::i18n::t("缺少 sql", "sql is required").to_string())?;
    let params = parse_params(config);
    let fetch_mode = config
        .get("fetch_mode")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("all");
    let start = Instant::now();

    // DML vs SELECT: sqlx Any exposes rows only for statements that return
    // them; INSERT/UPDATE/DELETE report rows_affected. Classify by the first
    // keyword so the result shape matches the built-in (SELECT → rows,
    // else → affected_rows).
    let is_select = {
        let head = sql.trim_start().to_ascii_lowercase();
        head.starts_with("select")
            || head.starts_with("with")
            || head.starts_with("pragma")
            || head.starts_with("show")
            || head.starts_with("explain")
    };

    let (output, in_tx) = if is_select {
        let (rows, in_tx) = pool
            .query_fetch(exec_id, &connection_uuid, sql, &params)
            .await?;
        let total = rows.len();
        let (kept, truncated) = if total > MAX_ROWS {
            (rows[..MAX_ROWS].to_vec(), true)
        } else {
            (rows, false)
        };
        // Column names come from the first row's JSON keys (the pool
        // converts each row to an object keyed by column name).
        let columns: Vec<String> = kept
            .first()
            .and_then(serde_json::Value::as_object)
            .map(|obj| obj.keys().cloned().collect())
            .unwrap_or_default();
        let (processed_rows, row_count) = match fetch_mode {
            "one" => {
                if kept.is_empty() {
                    (vec![], 0)
                } else {
                    (vec![kept[0].clone()], 1)
                }
            }
            "scalar" => {
                if kept.is_empty() {
                    (vec![], 0)
                } else {
                    // 第一行第一列
                    let first = &kept[0];
                    let scalar = if let serde_json::Value::Object(map) = first {
                        map.values().next().cloned().unwrap_or(serde_json::Value::Null)
                    } else {
                        first.clone()
                    };
                    (vec![scalar], 1)
                }
            }
            _ => {
                let len = kept.len();
                (kept, len)
            }
        };
        (
            serde_json::json!({
                "success": true,
                "rows": processed_rows,
                "row_count": row_count,
                "affected_rows": 0,
                "columns": columns,
                "truncated": truncated,
                "timing": { "total_ms": start.elapsed().as_millis() as u64 },
            }),
            in_tx,
        )
    } else {
        let (affected, in_tx) = pool
            .query_execute(exec_id, &connection_uuid, sql, &params)
            .await?;
        (
            serde_json::json!({
                "success": true,
                "rows": [],
                "row_count": 0,
                "affected_rows": affected,
                "last_insert_id": serde_json::Value::Null,
                "timing": { "total_ms": start.elapsed().as_millis() as u64 },
            }),
            in_tx,
        )
    };

    Ok((ExecuteResult::ok(output), in_tx))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pool::DbPool;

    #[test]
    fn require_connection_uuid_errors_on_empty() {
        let config = serde_json::json!({ "connection_uuid": "" });
        assert!(require_connection_uuid(&config).is_err());
        let config = serde_json::json!({});
        assert!(require_connection_uuid(&config).is_err());
        let config = serde_json::json!({ "connection_uuid": "conn-1" });
        assert_eq!(require_connection_uuid(&config).expect("uuid"), "conn-1");
    }

    #[test]
    fn parse_params_handles_missing() {
        let config = serde_json::json!({ "sql": "SELECT 1" });
        assert!(parse_params(&config).is_empty());
    }

    #[tokio::test]
    async fn query_sqlite_end_to_end() {
        let registry = DbPoolRegistry::default();
        let pool = DbPool::Sqlite(
            sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("pool")
            );
        registry.connect("exec-1", "conn-a", "sqlite::memory:", "sqlite", pool);
        let pool = registry.pool("exec-1", "conn-a").expect("pool");
        match &pool {
            DbPool::Sqlite(p) => {
                sqlx::query("CREATE TABLE t (id INTEGER, name TEXT)")
                    .execute(p)
                    .await
                    .expect("create");
            }
            _ => panic!("expected sqlite pool"),
        }

        // Insert (DML) via the node path.
        let (result, _) = execute_core(
            &registry,
            "exec-1",
            &serde_json::json!({
                "connection_uuid": "conn-a",
                "sql": "INSERT INTO t VALUES (1, 'a')",
            }),
        )
        .await
        .expect("insert");
        assert_eq!(result.success, true);
        let affected = result
            .output_data
            .as_ref()
            .and_then(|o| o.get("affected_rows"))
            .and_then(serde_json::Value::as_u64);
        assert_eq!(affected, Some(1));

        // Select all.
        let (result, _) = execute_core(
            &registry,
            "exec-1",
            &serde_json::json!({
                "connection_uuid": "conn-a",
                "sql": "SELECT * FROM t",
            }),
        )
        .await
        .expect("select");
        let rows = result
            .output_data
            .as_ref()
            .and_then(|o| o.get("rows"))
            .and_then(serde_json::Value::as_array)
            .expect("rows");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("name").and_then(serde_json::Value::as_str), Some("a"));

        // Scalar fetch mode.
        let (result, _) = execute_core(
            &registry,
            "exec-1",
            &serde_json::json!({
                "connection_uuid": "conn-a",
                "sql": "SELECT id FROM t",
                "fetch_mode": "scalar",
            }),
        )
        .await
        .expect("scalar");
        let rows = result
            .output_data
            .as_ref()
            .and_then(|o| o.get("rows"))
            .and_then(serde_json::Value::as_array)
            .expect("rows");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], serde_json::json!(1));
    }

    #[tokio::test]
    async fn query_missing_pool_fails() {
        let registry = DbPoolRegistry::default();
        let err = execute_core(
            &registry,
            "exec-1",
            &serde_json::json!({
                "connection_uuid": "nope",
                "sql": "SELECT 1",
            }),
        )
        .await
        .expect_err("missing pool must surface an error");
        assert!(!err.is_empty());
    }
}
