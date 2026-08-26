# MPE DB Plugin

![CI](https://github.com/multi-protocol-flow/mpe-plugin-db/actions/workflows/ci.yml/badge.svg)

> **Positioning**: this plugin is one of MPE's **official protocol plugins** — it
> depends on none of the host repository's code, only the public
> `mpe-plugin-sdk` (sidecar process + JSON-RPC over stdio, git tag `v0.1.0`).
> It adds SQLite / MySQL / PostgreSQL database operations to MPE via sqlx.
>
> This repository is independent of the host (the host repository's `.gitignore`
> ignores `/plugins/` and the host never builds it). It is built and released by
> this repository's own CI (GitHub Actions → Release artifacts).

---

## 0. How the plugin works in one minute

```
Host (mpe / mpe-cli)                    Plugin process (this crate)
   │  scans plugins/ dir                       │
   │  ── describe ───────────────────────────► │  returns node descriptions (type, ports, config schema)
   │  ◄─────────── node list ─────────────────  │
   │  ── execute(config, execution_id) ──────► │  runs the database operation
   │  ◄─────────── result / variable updates ─  │
   │  ── flowEnded(execution_id) ────────────► │  releases the per-execution connection pools
```

- **Transport**: stdin/stdout, one JSON document per line (JSON-RPC 2.0, LF-framed)
- **Resident**: `capabilities.streaming: true` → the process stays alive, the
  connection pools are reused across executions
- **Single-node verification**: `db:connect` declares
  `capabilities.single_node: true` → the host's `mpe run-node` / GUI test button
  can verify reachability without any host code change
- **No shared memory**: the plugin is a separate process; the host can only pass
  values as JSON

## 1. Project structure

```
mpe-plugin-db/
├── Cargo.toml            # standalone package, no host workspace dependency
├── plugin.json           # manifest scanned by the host (launch description, residency mode)
├── .github/workflows/ci.yml  # 3-platform build + offline tests + Release packaging
├── src/
│   ├── main.rs           # mpe_plugin_main! entry point
│   ├── lib.rs            # DbPlugin: Plugin trait impl (describe/execute/flow_ended) + unit tests
│   ├── i18n.rs           # MPE_LOCALE-driven zh-CN / en-US copy
│   ├── pool.rs           # per-execution sqlx connection pools + idle reaper
│   └── nodes/
│       ├── mod.rs        # per-node execute dispatch + connection_uuid resolution
│       ├── connect.rs    # db:connect
│       ├── query.rs      # db:query
│       ├── begin.rs      # db:begin
│       ├── commit.rs     # db:commit
│       ├── rollback.rs   # db:rollback
│       └── close.rs      # db:close
└── tests/
    └── fixtures/         # example flows (sqlite/mysql/postgres/transaction) for host-side testing
```

## 2. Node types

| type_id | ports | description |
|---------|-------|-------------|
| `db:connect` | in/true/false | establish a connection pool. `driver` enum `sqlite\|mysql\|postgres`; `connection_string` (sqlite: file path, empty = in-memory; mysql/postgres: URL), falling back to `host`/`port`/`database`/`username`/`password` fields when absent; optional `min_connections`/`max_connections`, `test_query`, `connect_timeout_ms`. Declares `single_node: true` |
| `db:query` | in/true/false | run SQL. Required `connection_uuid` (rendered by the host's `x-node-selector` as a selector for `db:connect` nodes), `sql` (format: sql); `params` array matching `?` placeholders; `fetch_mode` `all\|one\|scalar`; optional `result_to` variable and `timeout_ms` |
| `db:begin` | in/true/false | start a transaction; optional `isolation_level` (`READ UNCOMMITTED\|READ COMMITTED\|REPEATABLE READ\|SERIALIZABLE`) |
| `db:commit` | in/true/false | commit the transaction on the selected connection |
| `db:rollback` | in/true/false | roll back the transaction on the selected connection |
| `db:close` | in/out | release connections (optional `connection_uuid` selects one; omitted releases the whole execution) |

The operation nodes (`connect`/`query`/`begin`/`commit`/`rollback`) carry
`in`/`true`/`false` tri-ports: success routes via the host default to `true`,
failure goes through the host `on_error` strategy (default RouteToFalse).
`db:close` keeps the single `in`/`out` shape.

`db:query`/`db:begin`/`db:commit`/`db:rollback`/`db:close`'s `connection_uuid`
field is rendered by the host's `x-node-selector` as a selector for `db:connect`
nodes in the flow; once selected, the host injects the connect node's instance
id into the config and the plugin fetches the pool entry from its registry.

## 3. Cargo.toml (standalone build)

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

Note: sqlx's `sqlite` feature bundles libsqlite3-sys, so the crate compiles on
all three CI platforms without a system SQLite install.

## 4. Build and test

> **plugin.json `entry.command`**: a relative path in marketplace-contract form,
> `./mpe_plugin_db`. The current host resolves the command against its own CWD
> (it does not chdir into the plugin directory). When developing in this
> repository, if you want the host to launch the plugin directly, either change
> `command` to an absolute path on your machine, or start the host from the
> plugin directory.

```bash
# build the release binary (the host launches it via plugin.json entry.command)
# run from the repository root (mpe-plugin-db)
cargo build --release

# offline unit tests (no database needed): describe registration,
# x-node-selector wiring, password/sql schema formats
cargo test
```

## 5. Install and verify (host side)

```bash
# development: build into the repository root (the host scans MPE_PLUGIN_DIR or
# the plugins directory under its data dir)
# run from the repository root (mpe-plugin-db)
cargo build --release

# single-node reachability verification (capability-driven, no host whitelist change)
mpe run-node '{"type":"db:connect","driver":"sqlite","connection_string":""}'
# (or pass a sqlite file path; mysql/postgres take a URL)
```

Full flow execution (connect → query → close, transactions) can be run from the
`tests/fixtures/db_sqlite_plugin_flow.json` fixture flow via `mpe run --flow`,
or assembled in the MPE editor.

## 6. Design notes

- **Pool isolation**: sqlx connection pools are keyed by the compound
  `(execution_id, connection_uuid)`; concurrent executions never interfere.
- **Idle reaper**: a background task reclaims idle connections and shuts down
  pools after the execution finishes.
- **Dual release paths**: the `flow_ended` hook (host broadcasts on completion)
  and the `db:close` node both release pools; idempotent by design (releasing a
  missing entry is a no-op).
- **Transactions**: begin/commit/rollback nodes manage a transaction per
  connection, with an optional isolation level on `db:begin`.
- **Connection string precedence**: `connection_string` wins; when absent the
  plugin assembles the URL from `host`/`port`/`database`/`username`/`password`.
- **Fetch modes**: `db:query` returns rows, a single row, or a scalar value via
  `fetch_mode` (`all`/`one`/`scalar`) and can store the result into a flow
  variable with `result_to`.
- **Typed drivers**: sqlx `sqlite`/`mysql`/`postgres` typed drivers are used
  instead of the `Any` driver, whose MySQL type whitelist would exclude
  TINYINT/DECIMAL/DATETIME/etc.

## 7. Relationship with the host

- Dependency: only `mpe-plugin-sdk` (`mpe-plugin-sdk::protocol` wire contract) —
  **never** host types such as `flow-engine-core`.
- Interaction: describe/execute/validate/shutdown RPCs + log notifications +
  flowEnded notifications.
- Host adaptation point: `capabilities.single_node` is read by the host's
  `execute_single_node` / `mpe run-node` capability gate
  (`NodePlugin::supports_single_node()` + `SidecarPlugin` passthrough); the
  plugin never needs to know host implementation details.
