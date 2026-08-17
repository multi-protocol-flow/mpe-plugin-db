//! Node implementations, one file per `db:*` node type.
//!
//! Each module exposes `execute(ctx, pool) -> ExecuteResult` delegating to a
//! `*_core(pool, exec_id, config)` function that is testable without an
//! [`ExecuteContext`] (the SDK's `ExecuteContext::from_params` is
//! `pub(crate)`, so plugin tests cannot construct one — see decisions.md).

pub mod begin;
pub mod close;
pub mod commit;
pub mod connect;
pub mod query;
pub mod rollback;
