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

//! SEED of Go `pkg/table/tblsession`.
//!
//! This is the *live-session* implementation of the mutate-context interfaces
//! [`crate::tblctx`] declares: where `tblctx` says what a table mutation may
//! ask of its context, [`MutateContext`] here answers every question out of a
//! running session, re-reading the session on each call exactly as Go's
//! embedded `sessionctx.Context` does. The traits are reused, never restated.
//!
//! `table.go` is the package's only production file, and this is a SEED of it
//! because [`crate::tblctx`]'s own `MutateContext` is one: the three members
//! that trait defers -- `GetRowIDShardGenerator`
//! (`variable.RowIDShardGenerator`), `GetReservedRowIDAlloc`
//! (`stmtctx.ReservedRowIDAlloc`) and the embedded `AllocatorContext`
//! (`meta/autoid.Allocators`) -- have no interface method to implement, so
//! this file's counterparts are gated with them:
//!
//! - Go `MutateContext.GetRowIDShardGenerator` (`table.go:103`) -- deferred.
//! - Go `MutateContext.GetReservedRowIDAlloc` (`table.go:108`) -- deferred,
//!   with it the `intest.Assert` on a nil `StmtCtx`.
//! - Go `MutateContext.AlternativeAllocators` (`table.go:50`), the whole
//!   `tblctx.AllocatorContext` half of this type -- deferred. It is the one
//!   place the package reads `TempTable.GetAutoIDAllocator`, so that method is
//!   absent from [`SessionTempTable`] too.
//!
//! Every other production symbol of `table.go` lands, and the package's single
//! test `TestSessionMutateContextFields` is ported in
//! [`mod tests`](self#tests), minus the two assertion blocks that exercise the
//! deferred members.
//!
//! # Boundaries
//!
//! Go writes this package against `pkg/sessionctx`, which this workspace ports
//! into `tidb-session` -- a crate ABOVE this one, exactly as Go's `sessionctx`
//! sits above `pkg/table`, so a real dependency would invert the layering.
//! Every reach into it is a narrow local trait named at its definition site:
//!
//! - `// boundary:` Go `sessionctx.Context` -- [`SessionContext`], the three
//!   accessors this file calls on a session. It deliberately does NOT reuse
//!   `tidb_expr::sessionexpr::SessionContext`, which is the same Go interface
//!   narrowed for a different consumer: that one demands a `kv.Storage`, a
//!   restricted SQL executor and an advisory-lock context, none of which a
//!   table mutation touches, and its `GetExprCtx` is not even in play here
//!   because [`crate::tblctx::MutateContext`] hands back
//!   `tidb_expr::metabuild::ExprContext`.
//! - `// boundary:` Go `sessionctx/variable.SessionVars` --
//!   [`SessionVars`]. Go reaches all of these through the package-private
//!   `ctx.vars()` helper, which is [`MutateContext::vars`].
//! - `// boundary:` Go `variable.TransactionContext` -- [`TxnContext`],
//!   narrowed to the delta and cached-table halves the two support interfaces
//!   use. Whether a session HAS one is the `nil` check three `Get*Support`
//!   methods make, and is `Option` here.
//! - `// boundary:` Go `pkg/util/tableutil.TempTable` --
//!   [`SessionTempTable`], which EXTENDS [`crate::tblctx::TempTable`] with the
//!   modified flag `AddTemporaryTableToTxn` sets. `tblctx` narrowed that trait
//!   to what its `TemporaryTableHandler` reads, and this is the one caller
//!   that needs more.
//! - Go `pkg/table.CachedTable`, the handle type of
//!   `AddCachedTableHandleToTxn`, needs no boundary: Go itself erases it to
//!   `any` to dodge an import cycle, and `tblctx` keeps that erasure as
//!   `&dyn Any`.

use std::any::Any;
use std::cell::RefCell;
use std::rc::Rc;

use tidb_expr::metabuild::{ExprContext, MetaOnlyInfoSchema};
use tidb_model::TableInfo;

use crate::tblctx::{
    AssertionLevel, CachedTableSupport, ExchangePartitionDMLSupport, MutateBuffers,
    RowEncodingConfig, StatisticsSupport, TempTable, TemporaryTableData, TemporaryTableHandler,
    TemporaryTableSupport,
};
use crate::write_stmt_bufs::WriteStmtBufs;

/// boundary: Go `pkg/sessionctx.Context`, narrowed to the three accessors
/// `tblsession` calls on the session it wraps.
pub trait SessionContext {
    /// Go `Context.GetSessionVars`.
    fn get_session_vars(&self) -> &dyn SessionVars;
    /// Go `Context.GetExprCtx`.
    fn get_expr_ctx(&self) -> &dyn ExprContext;
    /// Go `Context.GetLatestInfoSchema`.
    fn get_latest_info_schema(&self) -> &dyn MetaOnlyInfoSchema;
}

/// boundary: Go `pkg/sessionctx/variable.SessionVars`, narrowed to the fields
/// `tblsession` reads through `ctx.vars()`.
pub trait SessionVars {
    /// Go `SessionVars.ConnectionID`.
    fn connection_id(&self) -> u64;
    /// Go `SessionVars.InRestrictedSQL`.
    fn in_restricted_sql(&self) -> bool;
    /// Go `SessionVars.AssertionLevel`.
    fn assertion_level(&self) -> AssertionLevel;
    /// Go `SessionVars.EnableMutationChecker`.
    fn enable_mutation_checker(&self) -> bool;
    /// Go `SessionVars.IsRowLevelChecksumEnabled`, which is
    /// `EnableRowLevelChecksum && RowEncoder.Enable && !InRestrictedSQL`.
    fn is_row_level_checksum_enabled(&self) -> bool;
    /// Go `SessionVars.RowEncoder`, by value: `tblctx::RowEncoder` carries
    /// only the `Enable` switch and is `Copy`, so there is no pointer for a
    /// caller to keep.
    fn row_encoder(&self) -> crate::tblctx::RowEncoder;
    /// Go `SessionVars.GetWriteStmtBufs`.
    fn get_write_stmt_bufs(&self) -> Rc<RefCell<WriteStmtBufs>>;
    /// Go `SessionVars.TxnCtx`; `None` is Go's nil, which is what the
    /// statistics / cached-table / temporary-table support checks test for.
    fn txn_ctx(&self) -> Option<&dyn TxnContext>;
    /// Go `SessionVars.TMPTableSize`.
    fn tmp_table_size(&self) -> i64;
    /// Go `SessionVars.GetTemporaryTable`.
    fn get_temporary_table(&self, tbl_info: &TableInfo) -> Option<&dyn SessionTempTable>;
    /// Go `SessionVars.TemporaryTableData`; `None` is Go's nil, under which a
    /// [`TemporaryTableHandler`] reports a committed size of 0.
    fn temporary_table_data(&self) -> Option<&dyn TemporaryTableData>;
}

/// boundary: Go `pkg/sessionctx/variable.TransactionContext`, narrowed to the
/// two mutations this package performs on it.
pub trait TxnContext {
    /// Go `TransactionContext.UpdateDeltaForTable`.
    fn update_delta_for_table(&self, physical_table_id: i64, delta: i64, count: i64);
    /// Go's `_, ok := tc.CachedTables[tableID]` membership test.
    fn has_cached_table(&self, table_id: i64) -> bool;
    /// Go's `tc.CachedTables[tableID] = handle`. Go lazily allocates the map
    /// first; a Rust map cannot be nil, so that step has no counterpart.
    fn set_cached_table(&self, table_id: i64, handle: &dyn Any);
}

/// boundary: Go `pkg/util/tableutil.TempTable`, extending
/// [`crate::tblctx::TempTable`] with the one method that package's narrowing
/// left out because its `TemporaryTableHandler` never calls it.
///
/// `GetAutoIDAllocator` is NOT here: its only caller is
/// `AlternativeAllocators`, which is deferred with the rest of the
/// `AllocatorContext` axis (see the module header).
pub trait SessionTempTable: TempTable {
    /// Go `TempTable.SetModified`.
    fn set_modified(&self, modified: bool);
}

/// Go `MutateContext` (`table.go:33`): the context of a table mutation running
/// inside a live session.
///
/// Go embeds the `sessionctx.Context` interface value; this borrows the
/// session for the same reason -- the context is statement-scoped scaffolding
/// over a session that outlives it.
pub struct MutateContext<'a> {
    /// Go's embedded `sessionctx.Context`.
    sctx: &'a dyn SessionContext,
    /// Go `mutateBuffers`: a memory pool for table-related allocation that
    /// aims to reuse memory and save allocations. Supposed to be used inside
    /// `AddRecord`/`UpdateRecord`/`RemoveRecord`.
    mutate_buffers: MutateBuffers,
}

impl<'a> MutateContext<'a> {
    /// Go `NewMutateContext`.
    #[must_use]
    pub fn new(sctx: &'a dyn SessionContext) -> Self {
        Self {
            sctx,
            mutate_buffers: MutateBuffers::new(sctx.get_session_vars().get_write_stmt_bufs()),
        }
    }

    /// Go's package-private `(*MutateContext).vars`.
    fn vars(&self) -> &'a dyn SessionVars {
        self.sctx.get_session_vars()
    }
}

impl crate::tblctx::MutateContext for MutateContext<'_> {
    fn get_expr_ctx(&self) -> &dyn ExprContext {
        self.sctx.get_expr_ctx()
    }

    fn connection_id(&self) -> u64 {
        self.vars().connection_id()
    }

    fn in_restricted_sql(&self) -> bool {
        self.vars().in_restricted_sql()
    }

    fn txn_assertion_level(&self) -> AssertionLevel {
        self.vars().assertion_level()
    }

    fn enable_mutation_checker(&self) -> bool {
        self.vars().enable_mutation_checker()
    }

    fn get_row_encoding_config(&self) -> RowEncodingConfig {
        let vars = self.vars();
        RowEncodingConfig {
            is_row_level_checksum_enabled: vars.is_row_level_checksum_enabled(),
            row_encoder: vars.row_encoder(),
        }
    }

    fn get_mutate_buffers(&mut self) -> &mut MutateBuffers {
        &mut self.mutate_buffers
    }

    fn get_statistics_support(&self) -> Option<&dyn StatisticsSupport> {
        self.vars()
            .txn_ctx()
            .map(|_| self as &dyn StatisticsSupport)
    }

    fn get_cached_table_support(&self) -> Option<&dyn CachedTableSupport> {
        self.vars()
            .txn_ctx()
            .map(|_| self as &dyn CachedTableSupport)
    }

    fn get_temporary_table_support(&self) -> Option<&dyn TemporaryTableSupport> {
        self.vars()
            .txn_ctx()
            .map(|_| self as &dyn TemporaryTableSupport)
    }

    fn get_exchange_partition_dml_support(&self) -> Option<&dyn ExchangePartitionDMLSupport> {
        Some(self)
    }
}

impl StatisticsSupport for MutateContext<'_> {
    /// Go `MutateContext.UpdatePhysicalTableDelta`.
    fn update_physical_table_delta(&self, physical_table_id: i64, delta: i64, count: i64) {
        if let Some(txn_ctx) = self.vars().txn_ctx() {
            txn_ctx.update_delta_for_table(physical_table_id, delta, count);
        }
    }
}

impl CachedTableSupport for MutateContext<'_> {
    /// Go `MutateContext.AddCachedTableHandleToTxn`.
    ///
    /// Go dereferences `TxnCtx` unconditionally here -- the only caller got
    /// this interface from `GetCachedTableSupport`, which already proved it is
    /// non-nil. `Option` makes that proof unrepresentable across the two
    /// calls, so a session that lost its transaction in between is a no-op
    /// rather than Go's nil dereference.
    fn add_cached_table_handle_to_txn(&self, table_id: i64, handle: &dyn Any) {
        if let Some(txn_ctx) = self.vars().txn_ctx() {
            if !txn_ctx.has_cached_table(table_id) {
                txn_ctx.set_cached_table(table_id, handle);
            }
        }
    }
}

impl TemporaryTableSupport for MutateContext<'_> {
    /// Go `MutateContext.GetTemporaryTableSizeLimit`.
    fn get_temporary_table_size_limit(&self) -> i64 {
        self.vars().tmp_table_size()
    }

    /// Go `MutateContext.AddTemporaryTableToTxn`.
    fn add_temporary_table_to_txn(
        &self,
        tbl_info: &TableInfo,
    ) -> Option<TemporaryTableHandler<'_>> {
        let vars = self.vars();
        let tbl = vars.get_temporary_table(tbl_info)?;
        tbl.set_modified(true);
        Some(TemporaryTableHandler::new(tbl, vars.temporary_table_data()))
    }
}

impl ExchangePartitionDMLSupport for MutateContext<'_> {
    /// Go `MutateContext.GetInfoSchemaToCheckExchangeConstraint`.
    fn get_info_schema_to_check_exchange_constraint(&self) -> &dyn MetaOnlyInfoSchema {
        self.sctx.get_latest_info_schema()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    use std::collections::HashMap;

    use tidb_expr::metabuild::EvalContext;
    use tidb_model::TempTableType;
    use tidb_mysql::consts::SqlMode;
    use tidb_util::context::WarnErr;

    use crate::tblctx::MutateContext as _;

    /// Go's `variable.TableDelta`, in the two fields the test asserts on.
    #[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
    struct TableDelta {
        delta: i64,
        count: i64,
    }

    /// Go's `variable.TransactionContext`, narrowed to [`TxnContext`]. The
    /// cached-table map holds the handle's data ADDRESS rather than the handle
    /// itself: `tblctx`'s Go-faithful `&dyn Any` parameter is borrowed for the
    /// call only, and the address is what Go's `require.Same` compares.
    #[derive(Default)]
    struct MockTxnContext {
        table_delta_map: RefCell<HashMap<i64, TableDelta>>,
        cached_tables: RefCell<HashMap<i64, *const ()>>,
    }

    impl TxnContext for MockTxnContext {
        fn update_delta_for_table(&self, physical_table_id: i64, delta: i64, count: i64) {
            let mut map = self.table_delta_map.borrow_mut();
            let item = map.entry(physical_table_id).or_default();
            item.delta += delta;
            item.count += count;
        }

        fn has_cached_table(&self, table_id: i64) -> bool {
            self.cached_tables.borrow().contains_key(&table_id)
        }

        fn set_cached_table(&self, table_id: i64, handle: &dyn Any) {
            self.cached_tables
                .borrow_mut()
                .insert(table_id, std::ptr::from_ref(handle).cast::<()>());
        }
    }

    /// Go's `mockTemporaryData`, whose `GetTableSize` is
    /// `tableID*1000000 + size`.
    #[derive(Default)]
    struct MockTemporaryData {
        size: Cell<i64>,
    }

    impl TemporaryTableData for MockTemporaryData {
        fn get_table_size(&self, table_id: i64) -> i64 {
            table_id * 1_000_000 + self.size.get()
        }
    }

    /// Go's `tableutil.TempTable` implementation the session hands out.
    #[derive(Default)]
    struct MockTempTable {
        meta: TableInfo,
        size: Cell<i64>,
        modified: Cell<bool>,
    }

    impl TempTable for MockTempTable {
        fn get_meta(&self) -> &TableInfo {
            &self.meta
        }

        fn get_size(&self) -> i64 {
            self.size.get()
        }

        fn set_size(&self, size: i64) {
            self.size.set(size);
        }
    }

    impl SessionTempTable for MockTempTable {
        fn set_modified(&self, modified: bool) {
            self.modified.set(modified);
        }
    }

    /// The metabuild expression context the mock session publishes. Go's
    /// `mock.Context` builds a real one; the test only ever compares its
    /// identity with the one the [`MutateContext`] returns.
    #[derive(Default)]
    struct MockExprContext;

    impl EvalContext for MockExprContext {
        fn sql_mode(&self) -> SqlMode {
            SqlMode(0)
        }

        fn append_warning(&self, _err: WarnErr) {}

        fn append_note(&self, _note: WarnErr) {}
    }

    impl ExprContext for MockExprContext {
        fn get_eval_ctx(&self) -> &dyn EvalContext {
            self
        }

        fn get_default_collation_for_utf8mb4(&self) -> &str {
            "utf8mb4_bin"
        }

        fn get_charset_info(&self) -> (&str, &str) {
            ("utf8mb4", "utf8mb4_bin")
        }
    }

    #[derive(Default)]
    struct MockInfoSchema;

    impl MetaOnlyInfoSchema for MockInfoSchema {
        fn schema_meta_version(&self) -> i64 {
            1
        }
    }

    /// Go's `mock.NewContext()` plus its `SessionVars`, collapsed into the one
    /// object the two boundary traits describe. Every field is behind a `Cell`
    /// because the Go test mutates the session vars between assertions while
    /// the `MutateContext` holds the session.
    ///
    /// Go's nil-able `TxnCtx`, `TemporaryTableData` and lazily created
    /// `TxnCtx.TemporaryTables[id]` are owned outright and gated by a presence
    /// flag. A `RefCell<Option<Rc<..>>>` would be the literal shape, but no
    /// safe reference can be handed out of a `RefCell` guard, and `unsafe` is
    /// forbidden workspace-wide; the flag reproduces every state the Go test
    /// drives them through.
    #[derive(Default)]
    struct MockSession {
        connection_id: Cell<u64>,
        in_restricted_sql: Cell<bool>,
        assertion_level: Cell<AssertionLevel>,
        enable_mutation_checker: Cell<bool>,
        enable_row_level_checksum: Cell<bool>,
        row_encoder_enable: Cell<bool>,
        write_stmt_bufs: Rc<RefCell<WriteStmtBufs>>,
        txn_ctx: MockTxnContext,
        has_txn_ctx: Cell<bool>,
        tmp_table_size: Cell<i64>,
        /// The one temporary table the Go test asks for.
        temporary_table: MockTempTable,
        /// Whether the session has handed that table to a transaction yet,
        /// which is Go's `txnCtx.TemporaryTables[456]` going from nil to set.
        temporary_table_registered: Cell<bool>,
        temporary_table_data: MockTemporaryData,
        has_temporary_table_data: Cell<bool>,
        expr_ctx: MockExprContext,
        info_schema: MockInfoSchema,
    }

    impl SessionContext for MockSession {
        fn get_session_vars(&self) -> &dyn SessionVars {
            self
        }

        fn get_expr_ctx(&self) -> &dyn ExprContext {
            &self.expr_ctx
        }

        fn get_latest_info_schema(&self) -> &dyn MetaOnlyInfoSchema {
            &self.info_schema
        }
    }

    impl SessionVars for MockSession {
        fn connection_id(&self) -> u64 {
            self.connection_id.get()
        }

        fn in_restricted_sql(&self) -> bool {
            self.in_restricted_sql.get()
        }

        fn assertion_level(&self) -> AssertionLevel {
            self.assertion_level.get()
        }

        fn enable_mutation_checker(&self) -> bool {
            self.enable_mutation_checker.get()
        }

        fn is_row_level_checksum_enabled(&self) -> bool {
            self.enable_row_level_checksum.get()
                && self.row_encoder_enable.get()
                && !self.in_restricted_sql.get()
        }

        fn row_encoder(&self) -> crate::tblctx::RowEncoder {
            crate::tblctx::RowEncoder {
                enable: self.row_encoder_enable.get(),
            }
        }

        fn get_write_stmt_bufs(&self) -> Rc<RefCell<WriteStmtBufs>> {
            Rc::clone(&self.write_stmt_bufs)
        }

        fn txn_ctx(&self) -> Option<&dyn TxnContext> {
            self.has_txn_ctx
                .get()
                .then_some(&self.txn_ctx as &dyn TxnContext)
        }

        fn tmp_table_size(&self) -> i64 {
            self.tmp_table_size.get()
        }

        fn get_temporary_table(&self, tbl_info: &TableInfo) -> Option<&dyn SessionTempTable> {
            if tbl_info.id != self.temporary_table.meta.id {
                return None;
            }
            self.temporary_table_registered.set(true);
            Some(&self.temporary_table as &dyn SessionTempTable)
        }

        fn temporary_table_data(&self) -> Option<&dyn TemporaryTableData> {
            self.has_temporary_table_data
                .get()
                .then_some(&self.temporary_table_data as &dyn TemporaryTableData)
        }
    }

    fn addr_of<T: ?Sized>(value: &T) -> *const () {
        std::ptr::from_ref(value).cast::<()>()
    }

    // Go TestSessionMutateContextFields.
    //
    // Two assertion blocks of the Go test have no counterpart here, both
    // because the interface member they exercise is deferred in
    // `crate::tblctx::MutateContext` (see the module header):
    // `GetRowIDShardGenerator` and `GetReservedRowIDAlloc`.
    #[test]
    fn session_mutate_context_fields() {
        let sctx = MockSession {
            has_txn_ctx: Cell::new(true),
            temporary_table: MockTempTable {
                meta: TableInfo {
                    id: 456,
                    temp_table_type: TempTableType::GLOBAL,
                    ..Default::default()
                },
                size: Cell::new(0),
                modified: Cell::new(false),
            },
            ..Default::default()
        };
        let mut ctx = MutateContext::new(&sctx);
        // expression
        assert!(std::ptr::addr_eq(
            addr_of(sctx.get_expr_ctx()),
            addr_of(ctx.get_expr_ctx())
        ));
        // ConnectionID
        sctx.connection_id.set(12345);
        assert_eq!(ctx.connection_id(), 12345);
        // restricted SQL
        sctx.in_restricted_sql.set(false);
        assert!(!ctx.in_restricted_sql());
        sctx.in_restricted_sql.set(true);
        assert!(ctx.in_restricted_sql());
        // AssertionLevel
        sctx.assertion_level.set(AssertionLevel::Fast);
        assert_eq!(ctx.txn_assertion_level(), AssertionLevel::Fast);
        sctx.assertion_level.set(AssertionLevel::Strict);
        assert_eq!(ctx.txn_assertion_level(), AssertionLevel::Strict);
        // EnableMutationChecker
        sctx.enable_mutation_checker.set(true);
        assert!(ctx.enable_mutation_checker());
        sctx.enable_mutation_checker.set(false);
        assert!(!ctx.enable_mutation_checker());
        // encoding config.
        //
        // Go asserts `require.Same(&vars.RowEncoder, cfg.RowEncoder)`: its
        // config carries a POINTER to the session's encoder, so a later
        // session write is visible through a config captured earlier -- which
        // is what the last three Go assertions test. `tblctx::RowEncodingConfig`
        // carries the encoder by value (it is a single `Copy` bool; see that
        // type's boundary note), so a captured config is a snapshot, and each
        // of those assertions re-reads the config instead.
        sctx.enable_row_level_checksum.set(true);
        sctx.row_encoder_enable.set(true);
        sctx.in_restricted_sql.set(false);
        let cfg = ctx.get_row_encoding_config();
        assert!(cfg.is_row_level_checksum_enabled);
        assert_eq!(
            cfg.is_row_level_checksum_enabled,
            sctx.is_row_level_checksum_enabled()
        );
        assert_eq!(cfg.row_encoder, sctx.row_encoder());
        sctx.row_encoder_enable.set(false);
        let cfg = ctx.get_row_encoding_config();
        assert!(!cfg.is_row_level_checksum_enabled);
        assert_eq!(
            cfg.is_row_level_checksum_enabled,
            sctx.is_row_level_checksum_enabled()
        );
        assert_eq!(cfg.row_encoder, sctx.row_encoder());
        sctx.row_encoder_enable.set(true);
        sctx.in_restricted_sql.set(true);
        let cfg = ctx.get_row_encoding_config();
        assert_eq!(
            cfg.is_row_level_checksum_enabled,
            sctx.is_row_level_checksum_enabled()
        );
        assert!(!cfg.is_row_level_checksum_enabled);
        sctx.in_restricted_sql.set(false);
        sctx.enable_row_level_checksum.set(false);
        let cfg = ctx.get_row_encoding_config();
        assert_eq!(
            cfg.is_row_level_checksum_enabled,
            sctx.is_row_level_checksum_enabled()
        );
        // mutate buffers
        assert!(Rc::ptr_eq(
            ctx.get_mutate_buffers().get_write_stmt_bufs(),
            &sctx.write_stmt_bufs
        ));
        // statistics support
        sctx.has_txn_ctx.set(false);
        assert!(ctx.get_statistics_support().is_none());
        sctx.has_txn_ctx.set(true);
        let statistics_support = ctx.get_statistics_support().unwrap();
        assert_eq!(sctx.txn_ctx.table_delta_map.borrow().len(), 0);
        statistics_support.update_physical_table_delta(12, 1, 2);
        assert_eq!(sctx.txn_ctx.table_delta_map.borrow().len(), 1);
        let delta = sctx.txn_ctx.table_delta_map.borrow()[&12];
        assert_eq!(delta.delta, 1);
        assert_eq!(delta.count, 2);
        // cached table support
        sctx.has_txn_ctx.set(false);
        assert!(ctx.get_cached_table_support().is_none());
        sctx.has_txn_ctx.set(true);
        let cached_table_support = ctx.get_cached_table_support().unwrap();
        // Go's handle is a `table.CachedTable`; `tblctx` erases it as Go does.
        let handle: Box<dyn Any> = Box::new(37i64);
        assert!(!sctx.txn_ctx.cached_tables.borrow().contains_key(&123));
        cached_table_support.add_cached_table_handle_to_txn(123, handle.as_ref());
        assert_eq!(
            sctx.txn_ctx.cached_tables.borrow()[&123],
            addr_of(handle.as_ref())
        );
        // temporary table support
        sctx.has_txn_ctx.set(false);
        assert!(ctx.get_temporary_table_support().is_none());
        sctx.has_txn_ctx.set(true);
        sctx.has_temporary_table_data.set(true);
        let temp_table_support = ctx.get_temporary_table_support().unwrap();
        assert!(!sctx.temporary_table_registered.get());
        let tbl_info = TableInfo {
            id: 456,
            temp_table_type: TempTableType::GLOBAL,
            ..Default::default()
        };
        let tmp_tbl_handler = temp_table_support
            .add_temporary_table_to_txn(&tbl_info)
            .unwrap();
        assert!(sctx.temporary_table_registered.get());
        assert!(sctx.temporary_table.modified.get());
        assert_eq!(tmp_tbl_handler.get_committed_size(), 456_000_000);
        sctx.temporary_table_data.size.set(111);
        assert_eq!(tmp_tbl_handler.get_committed_size(), 456_000_111);
        assert_eq!(tmp_tbl_handler.get_dirty_size(), 0);
        tmp_tbl_handler.update_txn_delta_size(333);
        assert_eq!(tmp_tbl_handler.get_dirty_size(), 333);
        tmp_tbl_handler.update_txn_delta_size(-1);
        assert_eq!(tmp_tbl_handler.get_dirty_size(), 332);
        // temporary table size limit, which Go covers through the same
        // interface value.
        sctx.tmp_table_size.set(64 << 20);
        assert_eq!(
            ctx.get_temporary_table_support()
                .unwrap()
                .get_temporary_table_size_limit(),
            64 << 20
        );
        // exchange partition support
        let exchange = ctx.get_exchange_partition_dml_support().unwrap();
        assert!(std::ptr::addr_eq(
            addr_of(exchange.get_info_schema_to_check_exchange_constraint()),
            addr_of(sctx.get_latest_info_schema())
        ));
    }
}
