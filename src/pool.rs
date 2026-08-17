//! Per-execution database connection pool + transaction state machine.
//!
//! Keys ready-to-use pools by the composite `(execution_id,
//! connection_uuid)` pair so that:
//!
//! - concurrent flow executions never share a pool (each `db:connect` node
//!   in its own execution gets its own sqlx pool; sharing one would
//!   interleave unrelated queries and let one execution's `close` tear down
//!   another's connections), and
//! - multiple `db:connect` nodes can coexist inside ONE execution —
//!   operation nodes pick their pool via the `connection_uuid` the host
//!   resolves into their config (the connect node's own instance id, via the
//!   schema's `x-node-selector`).
//!
//! Transaction routing (per docs/db-plugin.md §3.5): `db:begin` pulls ONE
//! dedicated connection out of the pool (`sqlx::Pool::begin` — the
//! `Transaction` owns that connection exclusively); while a transaction is
//! `Active` for a key, every `db:query` on that key executes against the
//! transaction's connection (so it sees the uncommitted state and rolls
//! back together with it), not against the pool. `commit`/`rollback` finish
//! the transaction and return the connection to the pool.
//!
//! Driver note (fix for the `Any`-driver TINYINT failure): the sqlx `Any`
//! driver maps MySQL types through a whitelist (`sqlx-mysql any.rs`) that
//! EXCLUDES `TINYINT`/`DECIMAL`/`DATETIME`/`JSON`/etc., failing during
//! `describe()` with `Any driver does not support MySql type ...`. This
//! crate therefore uses the three TYPED drivers (`Pool<Sqlite>` /
//! `Pool<MySql>` / `Pool<Postgres>`) behind a small enum, so every MySQL
//! column type decodes natively (`i64` covers `TINYINT`, `u8` covers
//! `TINYINT UNSIGNED`).
//!
//! Lifecycle: the plugin is resident (`capabilities.streaming: true`), so
//! this registry lives for the process lifetime. Per-execution resources are
//! released on the host's `flowEnded` notification (fire-and-forget, via the
//! SDK `Plugin::flow_ended` hook) and by the `db:close` node; a background
//! reaper additionally evicts stale idle pools (age-based) up to a global
//! count cap, skipping pools with an active transaction.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use sqlx::mysql::{MySql, MySqlPool};
use sqlx::postgres::{PgPool, Postgres};
use sqlx::sqlite::{Sqlite, SqlitePool};
use sqlx::Transaction;

/// One of the three typed sqlx pools.
#[derive(Clone)]
pub enum DbPool {
    Sqlite(SqlitePool),
    MySql(MySqlPool),
    Postgres(PgPool),
}

/// One of the three typed sqlx transactions (each owns a dedicated
/// connection until commit/rollback).
enum DbTx {
    Sqlite(Transaction<'static, Sqlite>),
    MySql(Transaction<'static, MySql>),
    Postgres(Transaction<'static, Postgres>),
}

/// A ready-to-use sqlx pool bound to one flow execution.
struct PoolEntry {
    pool: DbPool,
    /// Last time any operation touched this pool (feeds the idle reaper).
    last_used_at: Instant,
    /// Connection string this pool was created from (idempotency key).
    connection_string: String,
    /// Driver name (`sqlite` | `mysql` | `postgres`).
    driver: String,
}

/// In-flight transaction state for one pool key.
enum TransactionState {
    /// `BEGIN` issued; the transaction owns a dedicated connection.
    Active(DbTx),
}

/// Execution + connection-keyed registry of database pools, shared across
/// executions in this plugin process.
pub struct DbPoolRegistry {
    pools: Mutex<HashMap<String, PoolEntry>>,
    transactions: Mutex<HashMap<String, TransactionState>>,
    /// Global pool-count cap (LRU-by-age eviction of idle pools; pools with
    /// an active transaction are exempt). Guards against unbounded
    /// accumulation under rapid re-execution / load testing.
    max_pools: usize,
}

impl Default for DbPoolRegistry {
    fn default() -> Self {
        Self::new(MAX_POOLS)
    }
}

/// Global pool-count cap: bounds the number of live sqlx pools per plugin
/// process. Under sustained load (new `execution_id` per run) this prevents
/// unbounded connection growth; eviction only ever removes IDLE pools.
const MAX_POOLS: usize = 64;

/// Composite pool key: `execution_id` and `connection_uuid` are opaque host
/// strings, `:` is a safe separator because neither contains it (the host
/// generates both as plain uuids/ids).
fn pool_key(execution_id: &str, connection_uuid: &str) -> String {
    format!("{execution_id}:{connection_uuid}")
}

impl DbPoolRegistry {
    /// Creates an empty registry with a global pool-count cap.
    pub fn new(max_pools: usize) -> Self {
        Self {
            pools: Mutex::new(HashMap::new()),
            transactions: Mutex::new(HashMap::new()),
            max_pools,
        }
    }

    /// Registers a ready `pool` for `(execution_id, connection_uuid)`.
    ///
    /// Idempotent: a second connect with the same connection string reuses
    /// the existing pool. A DIFFERENT connection string replaces the stale
    /// entry (returns `true` for the caller to surface a warning).
    pub fn connect(
        &self,
        execution_id: &str,
        connection_uuid: &str,
        connection_string: &str,
        driver: &str,
        pool: DbPool,
    ) -> bool {
        let key = pool_key(execution_id, connection_uuid);
        let mut pools = self.pools.lock();
        let replaced = match pools.get(&key) {
            Some(existing) if existing.connection_string == connection_string => {
                // Same target: reuse, do not swap the pool handle.
                pools.get_mut(&key).expect("key present").last_used_at = Instant::now();
                return false;
            }
            Some(_) => {
                // Different target: drop the stale pool first.
                pools.remove(&key);
                true
            }
            None => false,
        };
        // Evict the oldest idle pool when at cap. Pools with an active
        // transaction are exempt (their connection is checked out — closing
        // mid-transaction would roll back silently server-side).
        if pools.len() >= self.max_pools {
            let txs = self.transactions.lock();
            let mut oldest: Option<(String, Instant)> = None;
            for (k, entry) in pools.iter() {
                if txs.contains_key(k) {
                    continue;
                }
                if oldest
                    .as_ref()
                    .map(|(_, t)| entry.last_used_at < *t)
                    .unwrap_or(true)
                {
                    oldest = Some((k.clone(), entry.last_used_at));
                }
            }
            if let Some((oldest_key, _)) = oldest {
                pools.remove(&oldest_key);
            }
        }
        pools.insert(
            key,
            PoolEntry {
                pool,
                last_used_at: Instant::now(),
                connection_string: connection_string.to_string(),
                driver: driver.to_string(),
            },
        );
        replaced
    }

    /// Returns the pool for `(execution_id, connection_uuid)`, refreshing
    /// its `last_used_at`, or `None` when not established (or released).
    pub fn pool(&self, execution_id: &str, connection_uuid: &str) -> Option<DbPool> {
        let key = pool_key(execution_id, connection_uuid);
        let mut pools = self.pools.lock();
        let entry = pools.get_mut(&key)?;
        entry.last_used_at = Instant::now();
        Some(entry.pool.clone())
    }

    /// Returns the pool's driver name for `(execution_id, connection_uuid)`,
    /// or `None` when the pool is not established.
    pub fn driver_of(&self, execution_id: &str, connection_uuid: &str) -> Option<String> {
        let key = pool_key(execution_id, connection_uuid);
        let pools = self.pools.lock();
        pools.get(&key).map(|entry| entry.driver.clone())
    }

    /// Begins a transaction on the pool's dedicated connection, recording
    /// `Active` state for the key.
    ///
    /// `isolation_level` (already normalized to SQL-standard names by the
    /// caller) is applied ON the transaction's own connection: MySQL needs
    /// `SET TRANSACTION ISOLATION LEVEL` before `START TRANSACTION` on the
    /// same session, Postgres needs it inside the transaction block after
    /// `BEGIN`. SQLite has no isolation statement (always SERIALIZABLE) and
    /// skips it.
    ///
    /// `Err` when the pool is missing or a transaction is already active
    /// (double `begin` is a config error, never a silent no-op).
    pub async fn begin(
        &self,
        execution_id: &str,
        connection_uuid: &str,
        isolation_level: Option<&str>,
    ) -> Result<(), String> {
        let key = pool_key(execution_id, connection_uuid);
        let pool = self.pool(execution_id, connection_uuid).ok_or_else(|| {
            crate::i18n::t("连接不存在", "connection not found").to_string()
        })?;
        // Double-begin check must not hold the lock across the await below:
        // the transaction map guard would make this future non-Send.
        {
            let txs = self.transactions.lock();
            if txs.contains_key(&key) {
                return Err(crate::i18n::t(
                    "事务已存在（重复 begin）",
                    "transaction already active (double begin)",
                )
                .to_string());
            }
        }
        // `acquire()` pulls one connection OUT of the pool; the transaction
        // owns it exclusively until commit/rollback.
        let tx = match pool {
            DbPool::Sqlite(p) => {
                DbTx::Sqlite(p.begin().await.map_err(|e| e.to_string())?)
            }
            DbPool::MySql(p) => {
                // MySQL: `SET TRANSACTION ISOLATION LEVEL` applies to the
                // NEXT transaction on the SESSION, so it must run on the
                // pinned connection BEFORE START TRANSACTION (mid-transaction
                // it errors with 1568). `Transaction::begin` accepts an owned
                // `PoolConnection` via `MaybePoolConnection`, yielding a
                // `'static` transaction on THIS connection.
                let mut conn = p.acquire().await.map_err(|e| e.to_string())?;
                if let Some(level) = isolation_level {
                    let sql = format!("SET TRANSACTION ISOLATION LEVEL {level}");
                    sqlx::query(&sql)
                        .execute(&mut *conn)
                        .await
                        .map_err(|e| e.to_string())?;
                }
                DbTx::MySql(
                    sqlx::Transaction::begin(
                        sqlx::pool::MaybePoolConnection::PoolConnection(conn),
                        None,
                    )
                    .await
                    .map_err(|e| e.to_string())?,
                )
            }
            DbPool::Postgres(p) => {
                // Postgres: `SET TRANSACTION ISOLATION LEVEL` is a no-op
                // outside a transaction block — run it INSIDE the tx on its
                // dedicated connection, right after BEGIN.
                let mut tx = p.begin().await.map_err(|e| e.to_string())?;
                if let Some(level) = isolation_level {
                    let sql = format!("SET TRANSACTION ISOLATION LEVEL {level}");
                    sqlx::query(&sql)
                        .execute(&mut *tx)
                        .await
                        .map_err(|e| e.to_string())?;
                }
                DbTx::Postgres(tx)
            }
        };
        self.transactions.lock().insert(key, TransactionState::Active(tx));
        Ok(())
    }

    /// Executes `sql` against the transaction's dedicated connection when a
    /// transaction is active for the key, else against the pool. Fetches ALL
    /// result rows (SELECT-style statements), each converted to a JSON
    /// object keyed by column name.
    ///
    /// Returns `(rows, in_transaction)` — `in_transaction` is `true` when
    /// executed inside a transaction (callers can note it in the report).
    pub async fn query_fetch(
        &self,
        execution_id: &str,
        connection_uuid: &str,
        sql: &str,
        params: &[serde_json::Value],
    ) -> Result<(Vec<serde_json::Value>, bool), String> {
        let key = pool_key(execution_id, connection_uuid);
        let mut tx_out: Option<DbTx> = None;
        {
            let mut txs = self.transactions.lock();
            if let Some(TransactionState::Active(tx)) = txs.remove(&key) {
                tx_out = Some(tx);
            }
        }
        if let Some(mut tx) = tx_out {
            let rows = tx_fetch(&mut tx, sql, params).await?;
            self.transactions
                .lock()
                .insert(key, TransactionState::Active(tx));
            return Ok((rows, true));
        }
        let pool = self.pool(execution_id, connection_uuid).ok_or_else(|| {
            crate::i18n::t("连接不存在", "connection not found").to_string()
        })?;
        let rows = pool_fetch(&pool, sql, params).await?;
        Ok((rows, false))
    }

    /// Executes `sql` (DML: INSERT/UPDATE/DELETE) against the transaction's
    /// dedicated connection when a transaction is active for the key, else
    /// against the pool. Returns `(affected_rows, in_transaction)`.
    pub async fn query_execute(
        &self,
        execution_id: &str,
        connection_uuid: &str,
        sql: &str,
        params: &[serde_json::Value],
    ) -> Result<(u64, bool), String> {
        let key = pool_key(execution_id, connection_uuid);
        let mut tx_out: Option<DbTx> = None;
        {
            let mut txs = self.transactions.lock();
            if let Some(TransactionState::Active(tx)) = txs.remove(&key) {
                tx_out = Some(tx);
            }
        }
        if let Some(mut tx) = tx_out {
            let affected = tx_execute(&mut tx, sql, params).await?;
            self.transactions
                .lock()
                .insert(key, TransactionState::Active(tx));
            return Ok((affected, true));
        }
        let pool = self.pool(execution_id, connection_uuid).ok_or_else(|| {
            crate::i18n::t("连接不存在", "connection not found").to_string()
        })?;
        let affected = pool_execute(&pool, sql, params).await?;
        Ok((affected, false))
    }

    /// Commits the active transaction for the key.
    pub async fn commit(&self, execution_id: &str, connection_uuid: &str) -> Result<(), String> {
        let key = pool_key(execution_id, connection_uuid);
        let tx = self
            .transactions
            .lock()
            .remove(&key)
            .ok_or_else(|| crate::i18n::t("无活跃事务", "no active transaction").to_string())?;
        match tx {
            TransactionState::Active(tx) => match tx {
                DbTx::Sqlite(t) => t.commit().await.map_err(|e| e.to_string()),
                DbTx::MySql(t) => t.commit().await.map_err(|e| e.to_string()),
                DbTx::Postgres(t) => t.commit().await.map_err(|e| e.to_string()),
            },
        }
    }

    /// Rolls back the active transaction for the key.
    pub async fn rollback(&self, execution_id: &str, connection_uuid: &str) -> Result<(), String> {
        let key = pool_key(execution_id, connection_uuid);
        let tx = self
            .transactions
            .lock()
            .remove(&key)
            .ok_or_else(|| crate::i18n::t("无活跃事务", "no active transaction").to_string())?;
        match tx {
            TransactionState::Active(tx) => match tx {
                DbTx::Sqlite(t) => t.rollback().await.map_err(|e| e.to_string()),
                DbTx::MySql(t) => t.rollback().await.map_err(|e| e.to_string()),
                DbTx::Postgres(t) => t.rollback().await.map_err(|e| e.to_string()),
            },
        }
    }

    /// Drops EVERY pool and transaction registered under `execution_id`
    /// (close node without a specific selection / `flow_ended` callback). A
    /// no-op when the execution has no entries.
    pub fn release(&self, execution_id: &str) {
        let prefix = format!("{execution_id}:");
        let mut pools = self.pools.lock();
        let mut txs = self.transactions.lock();
        let keys: Vec<String> = pools
            .keys()
            .filter(|k| k.starts_with(&prefix))
            .cloned()
            .collect();
        for key in keys {
            // Dropping the pool closes its connections (sqlx closes on
            // drop); dropping an active transaction rolls back server-side.
            pools.remove(&key);
            txs.remove(&key);
        }
    }

    /// Drops a single pool of `execution_id` (close node with a selected
    /// `connection_uuid`). A no-op when the entry is absent.
    pub fn release_connection(&self, execution_id: &str, connection_uuid: &str) {
        let key = pool_key(execution_id, connection_uuid);
        let mut pools = self.pools.lock();
        let mut txs = self.transactions.lock();
        pools.remove(&key);
        txs.remove(&key);
    }

    /// Spawns the background idle reaper: evicts pools idle for longer than
    /// `idle_timeout` (skipping pools with an active transaction) every
    /// `tick`. Runs until the plugin process exits. Takes `Arc<Self>` so the
    /// task owns the registry independently of the caller's borrow.
    pub fn spawn_reaper(self: &Arc<Self>, idle_timeout: Duration, tick: Duration) {
        let registry = Arc::clone(self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tick);
            loop {
                interval.tick().await;
                registry.reap_idle(idle_timeout);
            }
        });
    }

    /// Evicts pools idle for longer than `idle_timeout`, skipping pools with
    /// an active transaction (a long `begin → slow step → commit` must not
    /// be torn down mid-flight — the server would implicitly roll back).
    fn reap_idle(&self, idle_timeout: Duration) {
        let now = Instant::now();
        let mut pools = self.pools.lock();
        let txs = self.transactions.lock();
        let stale: Vec<String> = pools
            .iter()
            .filter(|(k, entry)| {
                !txs.contains_key(*k) && now.duration_since(entry.last_used_at) >= idle_timeout
            })
            .map(|(k, _)| k.clone())
            .collect();
        for key in stale {
            pools.remove(&key);
        }
    }
}

// ============================================================================
// Driver-dispatching query execution
// ============================================================================

/// Fetches ALL rows from the typed pool, converting each to a JSON object.
async fn pool_fetch(
    pool: &DbPool,
    sql: &str,
    params: &[serde_json::Value],
) -> Result<Vec<serde_json::Value>, String> {
    match pool {
        DbPool::Sqlite(p) => sqlite_fetch(p, sql, params).await,
        DbPool::MySql(p) => mysql_fetch(p, sql, params).await,
        DbPool::Postgres(p) => postgres_fetch(p, sql, params).await,
    }
}

/// Executes DML on the typed pool, returning affected rows.
async fn pool_execute(
    pool: &DbPool,
    sql: &str,
    params: &[serde_json::Value],
) -> Result<u64, String> {
    match pool {
        DbPool::Sqlite(p) => sqlite_execute(p, sql, params).await,
        DbPool::MySql(p) => mysql_execute(p, sql, params).await,
        DbPool::Postgres(p) => postgres_execute(p, sql, params).await,
    }
}

/// Fetches ALL rows from a transaction's dedicated connection.
async fn tx_fetch(
    tx: &mut DbTx,
    sql: &str,
    params: &[serde_json::Value],
) -> Result<Vec<serde_json::Value>, String> {
    match tx {
        DbTx::Sqlite(t) => sqlite_fetch_tx(t, sql, params).await,
        DbTx::MySql(t) => mysql_fetch_tx(t, sql, params).await,
        DbTx::Postgres(t) => postgres_fetch_tx(t, sql, params).await,
    }
}

/// Executes DML on a transaction's dedicated connection.
async fn tx_execute(
    tx: &mut DbTx,
    sql: &str,
    params: &[serde_json::Value],
) -> Result<u64, String> {
    match tx {
        DbTx::Sqlite(t) => sqlite_execute_tx(t, sql, params).await,
        DbTx::MySql(t) => mysql_execute_tx(t, sql, params).await,
        DbTx::Postgres(t) => postgres_execute_tx(t, sql, params).await,
    }
}


/// One typed `Sqlite` row → JSON object keyed by column name.
fn sqlite_row_to_json(row: &sqlx::sqlite::SqliteRow) -> Result<serde_json::Value, String> {
use sqlx::{Column, Row};
    
    let mut obj = serde_json::Map::new();
    for (i, column) in row.columns().iter().enumerate() {
        let name = column.name().to_string();
        let value = sqlite_decode_cell(row, i)?;
        obj.insert(name, value);
    }
    Ok(serde_json::Value::Object(obj))
}

/// One typed `Sqlite` cell → JSON value by trying the scalar types in order.
///
/// NULL check first (numeric `try_get` fails on NULL); i64 → u64 → f64 →
/// bool → String covers every native scalar of the driver.
fn sqlite_decode_cell(row: &sqlx::sqlite::SqliteRow, idx: usize) -> Result<serde_json::Value, String> {
use sqlx::{Row, ValueRef};
    
    if row.try_get_raw(idx).map(|r| r.is_null()).unwrap_or(false) {
        return Ok(serde_json::Value::Null);
    }
    if let Ok(v) = row.try_get::<i64, _>(idx) {
        return Ok(serde_json::json!(v));
    }
    if let Ok(v) = row.try_get::<u64, _>(idx) {
        return Ok(serde_json::json!(v));
    }
    if let Ok(v) = row.try_get::<f64, _>(idx) {
        return Ok(serde_json::json!(v));
    }
    if let Ok(v) = row.try_get::<bool, _>(idx) {
        return Ok(serde_json::json!(v));
    }
    if let Ok(v) = row.try_get::<String, _>(idx) {
        return Ok(serde_json::json!(v));
    }
    if let Ok(v) = row.try_get::<Option<String>, _>(idx) {
        return Ok(v.map(serde_json::Value::String).unwrap_or(serde_json::Value::Null));
    }
    Ok(serde_json::Value::Null)
}

/// SELECT on the `Sqlite` pool.
async fn sqlite_fetch(
    pool: &SqlitePool,
    sql: &str,
    params: &[serde_json::Value],
) -> Result<Vec<serde_json::Value>, String> {
    let query = build_query_sqlite(sql, params)?;
    let rows = query.fetch_all(pool).await.map_err(|e| e.to_string())?;
    let mut out = Vec::with_capacity(rows.len());
    for row in &rows {
        out.push(sqlite_row_to_json(row)?);
    }
    Ok(out)
}

/// DML on the `Sqlite` pool.
async fn sqlite_execute(
    pool: &SqlitePool,
    sql: &str,
    params: &[serde_json::Value],
) -> Result<u64, String> {
    let query = build_query_sqlite(sql, params)?;
    let result = query.execute(pool).await.map_err(|e| e.to_string())?;
    Ok(result.rows_affected())
}

/// SELECT on a `Sqlite` transaction's dedicated connection.
async fn sqlite_fetch_tx(
    conn: &mut sqlx::sqlite::SqliteConnection,
    sql: &str,
    params: &[serde_json::Value],
) -> Result<Vec<serde_json::Value>, String> {
    let query = build_query_sqlite(sql, params)?;
    let rows = query.fetch_all(conn).await.map_err(|e| e.to_string())?;
    let mut out = Vec::with_capacity(rows.len());
    for row in &rows {
        out.push(sqlite_row_to_json(row)?);
    }
    Ok(out)
}

/// DML on a `Sqlite` transaction's dedicated connection.
async fn sqlite_execute_tx(
    conn: &mut sqlx::sqlite::SqliteConnection,
    sql: &str,
    params: &[serde_json::Value],
) -> Result<u64, String> {
    let query = build_query_sqlite(sql, params)?;
    let result = query.execute(conn).await.map_err(|e| e.to_string())?;
    Ok(result.rows_affected())
}

/// One typed `MySql` row → JSON object keyed by column name.
fn mysql_row_to_json(row: &sqlx::mysql::MySqlRow) -> Result<serde_json::Value, String> {
use sqlx::{Column, Row};
    
    let mut obj = serde_json::Map::new();
    for (i, column) in row.columns().iter().enumerate() {
        let name = column.name().to_string();
        let value = mysql_decode_cell(row, i)?;
        obj.insert(name, value);
    }
    Ok(serde_json::Value::Object(obj))
}

/// One typed `MySql` cell → JSON value by trying the scalar types in order.
///
/// NULL check first (numeric `try_get` fails on NULL); i64 → u64 → f64 →
/// bool → String covers every native scalar of the driver.
fn mysql_decode_cell(row: &sqlx::mysql::MySqlRow, idx: usize) -> Result<serde_json::Value, String> {
use sqlx::{Row, ValueRef};
    
    if row.try_get_raw(idx).map(|r| r.is_null()).unwrap_or(false) {
        return Ok(serde_json::Value::Null);
    }
    if let Ok(v) = row.try_get::<i64, _>(idx) {
        return Ok(serde_json::json!(v));
    }
    if let Ok(v) = row.try_get::<u64, _>(idx) {
        return Ok(serde_json::json!(v));
    }
    if let Ok(v) = row.try_get::<f64, _>(idx) {
        return Ok(serde_json::json!(v));
    }
    // DECIMAL/NUMERIC: exact decimal type (String/f64 don't match it).
    if let Ok(v) = row.try_get::<sqlx::types::Decimal, _>(idx) {
        return Ok(serde_json::json!(v.to_string()));
    }
    if let Ok(v) = row.try_get::<bool, _>(idx) {
        return Ok(serde_json::json!(v));
    }
    if let Ok(v) = row.try_get::<String, _>(idx) {
        return Ok(serde_json::json!(v));
    }
    if let Ok(v) = row.try_get::<Option<String>, _>(idx) {
        return Ok(v.map(serde_json::Value::String).unwrap_or(serde_json::Value::Null));
    }
    Ok(serde_json::Value::Null)
}

/// SELECT on the `MySql` pool.
async fn mysql_fetch(
    pool: &MySqlPool,
    sql: &str,
    params: &[serde_json::Value],
) -> Result<Vec<serde_json::Value>, String> {
    let query = build_query_mysql(sql, params)?;
    let rows = query.fetch_all(pool).await.map_err(|e| e.to_string())?;
    let mut out = Vec::with_capacity(rows.len());
    for row in &rows {
        out.push(mysql_row_to_json(row)?);
    }
    Ok(out)
}

/// DML on the `MySql` pool.
async fn mysql_execute(
    pool: &MySqlPool,
    sql: &str,
    params: &[serde_json::Value],
) -> Result<u64, String> {
    let query = build_query_mysql(sql, params)?;
    let result = query.execute(pool).await.map_err(|e| e.to_string())?;
    Ok(result.rows_affected())
}

/// SELECT on a `MySql` transaction's dedicated connection.
async fn mysql_fetch_tx(
    conn: &mut sqlx::mysql::MySqlConnection,
    sql: &str,
    params: &[serde_json::Value],
) -> Result<Vec<serde_json::Value>, String> {
    let query = build_query_mysql(sql, params)?;
    let rows = query.fetch_all(conn).await.map_err(|e| e.to_string())?;
    let mut out = Vec::with_capacity(rows.len());
    for row in &rows {
        out.push(mysql_row_to_json(row)?);
    }
    Ok(out)
}

/// DML on a `MySql` transaction's dedicated connection.
async fn mysql_execute_tx(
    conn: &mut sqlx::mysql::MySqlConnection,
    sql: &str,
    params: &[serde_json::Value],
) -> Result<u64, String> {
    let query = build_query_mysql(sql, params)?;
    let result = query.execute(conn).await.map_err(|e| e.to_string())?;
    Ok(result.rows_affected())
}

/// One typed `Postgres` row → JSON object keyed by column name.
fn postgres_row_to_json(row: &sqlx::postgres::PgRow) -> Result<serde_json::Value, String> {
use sqlx::{Column, Row};
    
    let mut obj = serde_json::Map::new();
    for (i, column) in row.columns().iter().enumerate() {
        let name = column.name().to_string();
        let value = postgres_decode_cell(row, i)?;
        obj.insert(name, value);
    }
    Ok(serde_json::Value::Object(obj))
}

/// One typed `Postgres` cell → JSON value by trying the scalar types in order.
///
/// NULL check first (numeric `try_get` fails on NULL); i64 → u64 → f64 →
/// bool → String covers every native scalar of the driver.
fn postgres_decode_cell(row: &sqlx::postgres::PgRow, idx: usize) -> Result<serde_json::Value, String> {
use sqlx::{Row, ValueRef};
    
    if row.try_get_raw(idx).map(|r| r.is_null()).unwrap_or(false) {
        return Ok(serde_json::Value::Null);
    }
    if let Ok(v) = row.try_get::<i64, _>(idx) {
        return Ok(serde_json::json!(v));
    }
    if let Ok(v) = row.try_get::<f64, _>(idx) {
        return Ok(serde_json::json!(v));
    }
    // NUMERIC/DECIMAL: exact decimal type (String/f64 don't match it).
    if let Ok(v) = row.try_get::<sqlx::types::Decimal, _>(idx) {
        return Ok(serde_json::json!(v.to_string()));
    }
    if let Ok(v) = row.try_get::<bool, _>(idx) {
        return Ok(serde_json::json!(v));
    }
    if let Ok(v) = row.try_get::<String, _>(idx) {
        return Ok(serde_json::json!(v));
    }
    if let Ok(v) = row.try_get::<Option<String>, _>(idx) {
        return Ok(v.map(serde_json::Value::String).unwrap_or(serde_json::Value::Null));
    }
    Ok(serde_json::Value::Null)
}

/// SELECT on the `Postgres` pool.
async fn postgres_fetch(
    pool: &PgPool,
    sql: &str,
    params: &[serde_json::Value],
) -> Result<Vec<serde_json::Value>, String> {
    let query = build_query_postgres(sql, params)?;
    let rows = query.fetch_all(pool).await.map_err(|e| e.to_string())?;
    let mut out = Vec::with_capacity(rows.len());
    for row in &rows {
        out.push(postgres_row_to_json(row)?);
    }
    Ok(out)
}

/// DML on the `Postgres` pool.
async fn postgres_execute(
    pool: &PgPool,
    sql: &str,
    params: &[serde_json::Value],
) -> Result<u64, String> {
    let query = build_query_postgres(sql, params)?;
    let result = query.execute(pool).await.map_err(|e| e.to_string())?;
    Ok(result.rows_affected())
}

/// SELECT on a `Postgres` transaction's dedicated connection.
async fn postgres_fetch_tx(
    conn: &mut sqlx::postgres::PgConnection,
    sql: &str,
    params: &[serde_json::Value],
) -> Result<Vec<serde_json::Value>, String> {
    let query = build_query_postgres(sql, params)?;
    let rows = query.fetch_all(conn).await.map_err(|e| e.to_string())?;
    let mut out = Vec::with_capacity(rows.len());
    for row in &rows {
        out.push(postgres_row_to_json(row)?);
    }
    Ok(out)
}

/// DML on a `Postgres` transaction's dedicated connection.
async fn postgres_execute_tx(
    conn: &mut sqlx::postgres::PgConnection,
    sql: &str,
    params: &[serde_json::Value],
) -> Result<u64, String> {
    let query = build_query_postgres(sql, params)?;
    let result = query.execute(conn).await.map_err(|e| e.to_string())?;
    Ok(result.rows_affected())
}

/// Coerces a string parameter to its JSON-typed value when it parses as
/// JSON, mirroring the removed built-in executor (`"1"` → number, `"true"`
/// → bool, `"null"` → null); unparseable strings stay strings. A string
/// like `"123"` therefore binds as an integer (Postgres needs `$1 = integer`
/// against integer columns).
fn coerce_param(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::String(s) => {
            serde_json::from_str(&s).unwrap_or_else(|_| serde_json::Value::String(s))
        }
        other => other,
    }
}

/// Generates ONE driver's query builder. Each binds JSON params onto a
/// typed `sqlx::query::<$driver>` — the generic `DB: Database` form cannot
/// satisfy `Encode<'q, DB>` at compile time, so each driver gets its own.
macro_rules! build_query_impl {
    ($name:ident, $driver:ident) => {
        fn $name<'q>(
            sql: &'q str,
            params: &'q [serde_json::Value],
        ) -> Result<
            sqlx::query::Query<'q, $driver, <$driver as sqlx::Database>::Arguments<'q>>,
            String,
        > {
            let mut query = sqlx::query::<$driver>(sql);
            for param in params {
                match coerce_param(param.clone()) {
                    serde_json::Value::Null => {
                        query = query.bind(None::<String>);
                    }
                    serde_json::Value::Bool(b) => {
                        query = query.bind(b);
                    }
                    serde_json::Value::Number(n) => {
                        if let Some(i) = n.as_i64() {
                            query = query.bind(i);
                        } else if let Some(f) = n.as_f64() {
                            query = query.bind(f);
                        } else {
                            query = query.bind(n.to_string());
                        }
                    }
                    serde_json::Value::String(s) => {
                        query = query.bind(s.clone());
                    }
                    other => {
                        return Err(format!("unsupported parameter type: {other}"));
                    }
                }
            }
            Ok(query)
        }
    };
}

build_query_impl!(build_query_sqlite, Sqlite);
build_query_impl!(build_query_mysql, MySql);
build_query_impl!(build_query_postgres, Postgres);

#[cfg(test)]
mod tests {
    use super::*;

    /// A registry with no reaper and a generous cap for tests.
    fn test_registry() -> DbPoolRegistry {
        DbPoolRegistry::new(64)
    }

    /// Creates a single-connection in-memory SQLite pool (SQLite `:memory:`
    /// is per-connection state; one connection keeps tables visible).
    async fn sqlite_pool() -> SqlitePool {
        sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("sqlite memory pool")
    }

    #[tokio::test]
    async fn pool_key_isolation_between_executions() {
        let registry = test_registry();
        let pool_a = DbPool::Sqlite(sqlite_pool().await);
        let pool_b = DbPool::Sqlite(sqlite_pool().await);

        registry.connect("exec-1", "conn-a", "sqlite::memory:", "sqlite", pool_a);
        registry.connect("exec-2", "conn-b", "sqlite::memory:", "sqlite", pool_b);

        assert!(registry.pool("exec-1", "conn-a").is_some());
        assert!(registry.pool("exec-2", "conn-b").is_some());
        // Cross-execution lookup must NOT resolve.
        assert!(registry.pool("exec-1", "conn-b").is_none());
    }

    #[tokio::test]
    async fn same_key_reuses_pool() {
        let registry = test_registry();
        let pool = DbPool::Sqlite(sqlite_pool().await);

        let replaced = registry.connect("exec-1", "conn-a", "sqlite::memory:", "sqlite", pool.clone());
        assert!(!replaced, "fresh insert must not report replacement");
        let replaced = registry.connect("exec-1", "conn-a", "sqlite::memory:", "sqlite", pool);
        assert!(!replaced, "same target reuse must not report replacement");
        assert!(registry.pool("exec-1", "conn-a").is_some());
    }

    #[tokio::test]
    async fn different_target_replaces_pool() {
        let registry = test_registry();
        let pool_a = DbPool::Sqlite(sqlite_pool().await);
        let pool_b = DbPool::Sqlite(sqlite_pool().await);

        let replaced = registry.connect("exec-1", "conn-a", "target-a", "sqlite", pool_a);
        assert!(!replaced);
        let replaced = registry.connect("exec-1", "conn-a", "target-b", "sqlite", pool_b);
        assert!(replaced, "different target must report replacement");
    }

    #[tokio::test]
    async fn release_removes_all_of_execution() {
        let registry = test_registry();
        for uuid in ["conn-a", "conn-b"] {
            let pool = DbPool::Sqlite(sqlite_pool().await);
            registry.connect("exec-1", uuid, "sqlite::memory:", "sqlite", pool);
        }
        let pool_c = DbPool::Sqlite(sqlite_pool().await);
        registry.connect("exec-2", "conn-c", "sqlite::memory:", "sqlite", pool_c);

        assert!(registry.pool("exec-1", "conn-a").is_some());
        registry.release("exec-1");
        assert!(registry.pool("exec-1", "conn-a").is_none());
        assert!(registry.pool("exec-1", "conn-b").is_none());
        assert!(
            registry.pool("exec-2", "conn-c").is_some(),
            "other execution untouched"
        );
    }

    #[tokio::test]
    async fn transaction_routes_queries_and_commits() {
        let registry = test_registry();
        let pool = DbPool::Sqlite(sqlite_pool().await);
        registry.connect("exec-1", "conn-a", "sqlite::memory:", "sqlite", pool);

        // Create a table on the pool (autocommit).
        let pool = registry.pool("exec-1", "conn-a").expect("pool");
        pool_execute(&pool, "CREATE TABLE t (v INTEGER)", &[])
            .await
            .expect("create table");

        // Begin → insert inside the transaction (not yet visible outside).
        registry.begin("exec-1", "conn-a", None).await.expect("begin");
        let (_affected, in_tx) = registry
            .query_execute("exec-1", "conn-a", "INSERT INTO t VALUES (42)", &[])
            .await
            .expect("insert in tx");
        assert!(in_tx, "insert must run on the transaction connection");

        // Commit → the row is now visible on the pool.
        registry.commit("exec-1", "conn-a").await.expect("commit");
        let rows = pool_fetch(&pool, "SELECT COUNT(*) AS c FROM t", &[])
            .await
            .expect("count");
        assert_eq!(rows[0].get("c").and_then(serde_json::Value::as_u64), Some(1));
    }

    #[tokio::test]
    async fn rollback_discards_transaction() {
        let registry = test_registry();
        let pool = DbPool::Sqlite(sqlite_pool().await);
        registry.connect("exec-1", "conn-a", "sqlite::memory:", "sqlite", pool);
        let pool = registry.pool("exec-1", "conn-a").expect("pool");
        pool_execute(&pool, "CREATE TABLE t (v INTEGER)", &[])
            .await
            .expect("create table");

        registry.begin("exec-1", "conn-a", None).await.expect("begin");
        registry
            .query_execute("exec-1", "conn-a", "INSERT INTO t VALUES (1)", &[])
            .await
            .expect("insert in tx");
        registry.rollback("exec-1", "conn-a").await.expect("rollback");

        let rows = pool_fetch(&pool, "SELECT COUNT(*) AS c FROM t", &[])
            .await
            .expect("count");
        assert_eq!(rows[0].get("c").and_then(serde_json::Value::as_u64), Some(0));
    }

    #[tokio::test]
    async fn double_begin_is_error() {
        let registry = test_registry();
        let pool = DbPool::Sqlite(sqlite_pool().await);
        registry.connect("exec-1", "conn-a", "sqlite::memory:", "sqlite", pool);

        registry.begin("exec-1", "conn-a", None).await.expect("begin");
        let err = registry
            .begin("exec-1", "conn-a", None)
            .await
            .expect_err("double begin must fail");
        assert!(!err.is_empty());
    }

    #[tokio::test]
    async fn missing_pool_is_error() {
        let registry = test_registry();
        assert!(registry.pool("exec-1", "nope").is_none());
    }

    #[tokio::test]
    async fn reap_skips_active_transaction() {
        let registry = test_registry();
        let pool = DbPool::Sqlite(sqlite_pool().await);
        registry.connect("exec-1", "conn-a", "sqlite::memory:", "sqlite", pool);
        registry.begin("exec-1", "conn-a", None).await.expect("begin");

        // Age the pool beyond the idle timeout WITHOUT touching last_used_at
        // (the begin path refreshed it, so backdate it manually).
        {
            let mut pools = registry.pools.lock();
            pools
                .get_mut("exec-1:conn-a")
                .expect("pool present")
                .last_used_at = Instant::now() - Duration::from_secs(3600);
        }

        // Reap with a tiny idle timeout: the pool must survive because its
        // transaction is active.
        registry.reap_idle(Duration::from_millis(1));
        assert!(
            registry.pool("exec-1", "conn-a").is_some(),
            "pool with an active transaction must not be reaped"
        );

        // After commit (transaction gone) + aging, the pool is reaped.
        registry.commit("exec-1", "conn-a").await.expect("commit");
        {
            let mut pools = registry.pools.lock();
            pools
                .get_mut("exec-1:conn-a")
                .expect("pool present")
                .last_used_at = Instant::now() - Duration::from_secs(3600);
        }
        registry.reap_idle(Duration::from_millis(1));
        assert!(
            registry.pool("exec-1", "conn-a").is_none(),
            "idle pool without a transaction must be reaped"
        );
    }
}
