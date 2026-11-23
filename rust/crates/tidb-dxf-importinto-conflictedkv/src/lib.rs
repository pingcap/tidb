// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/dxf/importinto/conflictedkv`: the KVs of conflicted rows in an
//! IMPORT INTO with global sort.
//!
//! The encode, merge-sort and ingest steps record duplicated data and unique
//! index KVs to the sort store and remove duplicated plain index KVs (see
//! `tidb-ingestor-globalsort`). The collect-conflicts step reads the recorded
//! KVs, re-encodes each conflicted row, adds up the checksum of those rows and
//! writes them to `conflicted-rows/` for the user ([`Collector`]). The
//! conflict-resolution step re-encodes the rows the same way and deletes all
//! their KVs from the cluster ([`Deleter`]). Post-process subtracts the
//! collected checksum from the encode step's before it compares with the
//! table's.
//!
//! A data KV decodes to its row. A unique index KV only names the handle, so
//! its row is read from the cluster; a row that is not there was a
//! conflicted data KV, handled with the data KV group. One row can have
//! several conflicted unique index KVs, so [`KeyFilter`] keeps the row keys
//! already handled, in memory up to a bound. Past the bound the checksum
//! cannot be exact and post-process skips it.

mod collector;
mod deleter;
mod handler;
mod row_handle;

use std::sync::Arc;

pub use collector::{CollectResult, Collector, MAX_CONFLICT_ROW_FILE_SIZE};
pub use deleter::{Deleter, BUFFERED_KEY_COUNT_LIMIT, BUFFERED_KEY_SIZE_LIMIT};
pub use handler::{
    recv, BaseHandler, ConflictKVHandler, EncodedRowHandler, LazyRefreshedSnapshot, PairReceiver, TrafficRecorder,
    BUFFERED_HANDLE_LIMIT,
};
pub use row_handle::{BoundedKeySet, KeyFilter};

/// A failure of this package.
pub type Error = Arc<dyn std::error::Error + Send + Sync>;

/// A Go `errors.New` / `errors.Errorf` error.
#[derive(Debug)]
struct OtherError(String);

impl std::fmt::Display for OtherError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for OtherError {}

pub(crate) fn other(message: impl Into<String>) -> Error {
    Arc::new(OtherError(message.into()))
}

/// `Result` with this package's [`Error`].
pub type Result<T> = std::result::Result<T, Error>;

/// Go `kv.Handle` of a key, as the table layer names it. A partition handle
/// gives its row handle, like Go's embedded `Handle`.
pub(crate) fn table_handle(handle: &tidb_txnkv::Handle) -> tidb_table::tables::TableHandle {
    match handle {
        tidb_txnkv::Handle::Int(h) => tidb_table::tables::TableHandle::Int(h.value()),
        tidb_txnkv::Handle::Common(h) => tidb_table::tables::TableHandle::Common(h.encoded().to_vec()),
        tidb_txnkv::Handle::Partition(p) => table_handle(p.inner()),
    }
}

#[cfg(test)]
mod tests;

// Go's test binary links `planner/core`, whose `init` sets `expression.BuildSimpleExpr`.
#[cfg(test)]
tidb_planner_init::link!();
