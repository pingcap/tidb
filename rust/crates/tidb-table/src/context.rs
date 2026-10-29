// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Go `pkg/table/context`: what the table layer reads from the statement that
//! writes or reads through it. The executor's statement context implements
//! it, and [`StaticMutateContext`] does where no session drives the table
//! (Go's `exprstatic`-backed contexts); the table layer never sees the
//! session or the executor.

use tidb_datatype::ConversionFlags;

mod static_mutate;

pub use static_mutate::{resolve_statement_clock, StaticMutateContext};

/// Go `table.MutateContext` together with its expression context
/// (`GetExprCtx`), which is the supertrait.
pub trait MutateContext: tidb_expr::Columns + Send + Sync {
    /// The implementation, for the layer that built it to read back.
    fn as_any(&self) -> &dyn std::any::Any;

    /// Go `TypeCtx().Flags()` for a statement that writes rows.
    fn write_conversion_flags(&self) -> ConversionFlags;
    /// Whether the statement runs under a strict SQL mode.
    fn strict(&self) -> bool;
    /// Go `ErrCtx` under `IGNORE`.
    fn ignore_err(&self) -> bool;
    /// Go `collate.NewCollationEnabled()` as the statement saw it.
    fn new_collation_enabled(&self) -> bool;

    /// Go `GetStatisticsSupport().UpdatePhysicalTableDelta`.
    fn update_table_delta(&self, physical_table_id: i64, delta: i64, count: i64);

    /// Go `GetMutateBuffers`: the transaction's staged writes.
    fn staged_writes(&self) -> &tidb_kv::staged_writes::StagedWrites;

    /// A shared handle to [`Self::staged_writes`].
    fn staged_writes_handle(&self) -> std::sync::Arc<tidb_kv::staged_writes::StagedWrites>;

    /// Whether the statement runs in a pessimistic transaction.
    fn pessimistic_transaction(&self) -> bool;

    /// Whether the pessimistic transaction defers its constraint check to prewrite.
    fn pessimistic_check_in_prewrite(&self) -> bool;

    /// Whether the statement runs in an optimistic transaction.
    fn optimistic_transaction(&self) -> bool;

    /// Whether a write maintains indices it does not touch (inside a transaction, or under a foreign-key cascade).
    fn writes_untouched_indices(&self) -> bool;

    /// Go `GetRowIDShardGenerator`: the shard a batch of row ids takes.
    fn next_row_id_shard(&self, count: u64) -> u64;

    /// Go `GetTemporaryTableSupport().GetTemporaryTableSizeLimit`.
    fn tmp_table_max_size(&self) -> i64;

    /// The statement's memory and disk quota.
    fn statement_memory(&self) -> tidb_kv::mem_quota::StatementMemory;

    /// The snapshot read options a read of `avg_row_bytes`-byte rows takes.
    fn snapshot_read_options(&self, avg_row_bytes: f64) -> tidb_txnkv::SnapshotReadOptions;

    /// Go's reorganization type flags.
    fn reorg_default_conversion_flags(&self) -> ConversionFlags;

    /// Takes the statement's warnings recorded so far.
    fn take_warnings(&self) -> Vec<tidb_util::context::SqlWarn>;

    /// Go `AppendWarning` / `AppendNote` at `level`.
    fn append_leveled(&self, level: tidb_util::context::WarningLevel, code: u16, message: &str);

    /// Go's SELECT type flags.
    fn query_default_conversion_flags(&self) -> ConversionFlags;
    /// Go's default-arm type flags (SHOW, ANALYZE).
    fn show_default_conversion_flags(&self) -> ConversionFlags;
    /// Go `DistSQLContext.MaxKeysRead`'s statement budget.
    fn keys_read(&self) -> &tidb_kv::keys_read::KeysReadBudget;
    /// Go's reorganization context derived from this statement.
    fn for_reorg(&self) -> std::sync::Arc<dyn MutateContext>;
    /// This context with the new-collation switch set to `enabled`.
    fn with_new_collation_enabled(&self, enabled: bool) -> std::sync::Arc<dyn MutateContext>;
    /// An owned handle to this context.
    fn to_shared(&self) -> std::sync::Arc<dyn MutateContext>;

    /// Go `AppendWarning`.
    fn append_warning_parts(&self, code: u16, message: &str) {
        self.append_warning(code, message);
    }

    /// The session time zone the statement evaluates in.
    fn session_zone(&self) -> tidb_datatype::SessionTimeZone {
        self.time_zone()
    }
}
