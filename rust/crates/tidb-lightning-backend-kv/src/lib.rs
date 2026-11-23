//! Go `pkg/lightning/backend/kv`: the encoder that turns an imported row
//! into the KV pairs the table layer would write for it, under a session
//! of its own (`Session`) whose transaction keeps the pairs instead of
//! storing them.
//!
//! Ported: `allocator.go`, `base.go`, `context.go`, `session.go` and
//! `sql2kv.go`. `kv2sql.go` (the decoder conflict resolution reads) comes
//! with its caller. Go's `table.Table` carries the allocators; here the
//! encoder takes them beside the `TableInfo`.

pub mod allocator;
pub mod base;
pub mod session;
pub mod sql2kv;

pub use allocator::{AllocatorType, PanickingAllocator, PanickingAllocators};
pub use base::{get_auto_record_id, is_auto_inc_col, BaseKVEncoder, EncodeError};
pub use session::Session;
pub use sql2kv::{Pairs, TableKVEncoder};

#[cfg(test)]
mod tests;

// Go's test binary links `planner/core`, whose `init` sets `expression.BuildSimpleExpr`.
#[cfg(test)]
tidb_planner_init::link!();
