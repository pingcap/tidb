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

//! Referential integrity: Go's `pkg/executor/foreign_key.go`
//! (`FKCheckExec` / `FKCascadeExec`) and the statement half of
//! `pkg/executor/adapter.go` that runs them (`handleStmtForeignKeyTrigger`,
//! `handleForeignKeyTrigger`, `handleForeignKeyCascade`).
//!
//! A DML statement builds one [`WithForeignKeyTrigger`] from the FK checks
//! and cascades its plan selected (`physicalop` `BuildOn*FKTriggers`). Each
//! row it writes hands the trigger the row's foreign-key values: a check
//! turns them into the KEYS it will look up (`addRowNeedToCheck`), a
//! cascade keeps the VALUES (`onDeleteRow` / `onUpdateRow`). Once every row
//! is written, [`handle_stmt_foreign_key_trigger`] runs each check against
//! the transaction (`doCheck`: point keys through one batch get, key
//! prefixes through the transaction's iterator, and the found keys locked)
//! and then each cascade, which builds a `DELETE` or `UPDATE` of the child
//! table (`GenCascadeDeleteAST` / `GenCascadeUpdateAST`), plans it
//! (`OptimizeForForeignKeyCascade`), runs it as an ordinary DML executor and
//! handles that statement's own triggers one level deeper.
//!
//! # The rules, each re-confirmed via `rust/difftests/gorun`
//!
//! * **MATCH SIMPLE** (MySQL/TiDB's only implemented mode): a child row whose
//!   referencing columns contain ANY `NULL` is not checked at all, composite
//!   keys included (`fkValueHelper.hasNullValue`).
//! * **Parent-side triggering**: a `DELETE` of a referenced row triggers, and
//!   so does an `UPDATE` that actually CHANGES a referenced value
//!   (`updateRowNeedToCheck`).
//! * **`CASCADE` is transitive**: each cascade statement's own triggers run
//!   at the next depth, up to `maxForeignKeyCascadeDepth` (3008).
//! * **`SET NULL`** is an `UPDATE` of the child setting its referencing
//!   columns to `NULL`; it recurses into the child's dependents as any
//!   update does.
//! * **`RESTRICT`, `NO ACTION`, `SET DEFAULT`, and no clause at all** are a
//!   `check_not_exist` check of the child table.
//! * **`IGNORE`** checks each row before it is written (`checkFKIgnoreErr`,
//!   `checkRows`): a violation skips the row with a warning.
//!
//! # The DDL-time rules
//!
//! * A constraint may not name a **VIRTUAL generated column** on either side
//!   (3733), and a **STORED generated CHILD column** may not carry an action
//!   that would WRITE it (3104). See [`crate::ddl::table_constraints`].
//! * A constraint can be **added and dropped after the fact**
//!   (`ALTER TABLE ... ADD/DROP FOREIGN KEY`, see [`crate::ddl::alter_table`]).
//! * The **index a constraint relies on may not be dropped** (1553). See
//!   [`check_index_needed`].
//! * **`foreign_key_checks = 0`** plans no trigger at all, so every row-level
//!   rule above is off; it is not retroactive.
//!
//! # NOT MODELLED (documented)
//!
//! * The statement stage owns rollback of the statement and its cascades
//!   (Go's `fk_sp_` savepoint, `handleFKTriggerError`).
//! * `tidb_foreign_key_check_in_shared_lock`: a pessimistic transaction locks
//!   the checked keys exclusively, Go's behaviour with the switch at its
//!   default `OFF`; and the deferred lock's time is not attributed back to
//!   the checks' runtime stats (`updateFKCheckLockStats`).

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use crate::driver::physical_builder::PhysicalRuntimeStats;
use crate::driver::DriverError;
use tidb_datatype::{Datum, FieldType};
use tidb_hack::GoCaseCompare;
use tidb_infoschema::{SessionExtendedInfoSchema, TableEntry};
use tidb_planner_core_base::PlanIdAllocator;
use tidb_planner_core_operator_physicalop::{FkTriggerKind, FkTriggerNode, PhysicalPlan};
use tidb_table::tables::{TableCommon, TableHandle};
use tidb_txnkv::Key;

/// Go `maxForeignKeyCascadeDepth`: the deepest a cascade may recurse before
/// Go raises `ErrForeignKeyCascadeDepthExceeded` (3008).
const MAX_FOREIGN_KEY_CASCADE_DEPTH: usize = 15;

/// Go `maxHandleFKValueInOneCascade`: the most foreign-key values one cascade
/// statement handles (issue #38631); more take further statements.
const MAX_HANDLE_FK_VALUE_IN_ONE_CASCADE: usize = 1024;

/// Go `buildTblID2FKCheckExecs` / `buildTblID2FKCascadeExecs`, and the
/// `WithForeignKeyTrigger` interface of the DML executor that holds them: the
/// checks and cascades of one DML statement.
pub(crate) struct WithForeignKeyTrigger {
    fk_checks: Vec<FKCheckExec>,
    fk_cascades: Vec<FKCascadeExec>,
}

impl WithForeignKeyTrigger {
    /// Go `buildFKCheckExecs` / `buildFKCascadeExecs` over the plan's
    /// checks and cascades.
    pub(crate) fn build(
        triggers: &[FkTriggerNode],
        catalog: &SessionExtendedInfoSchema,
    ) -> Result<Self, DriverError> {
        let mut fk_checks = Vec::new();
        let mut fk_cascades = Vec::new();
        for trigger in triggers {
            match trigger.kind {
                FkTriggerKind::ChildCheck | FkTriggerKind::ParentCheck => {
                    fk_checks.push(FKCheckExec::build(trigger));
                }
                FkTriggerKind::Cascade { .. } => {
                    fk_cascades.push(FKCascadeExec::build(trigger, catalog)?);
                }
            }
        }
        Ok(Self {
            fk_checks,
            fk_cascades,
        })
    }

    /// A statement with no checks or cascades.
    pub(crate) fn empty() -> Self {
        Self {
            fk_checks: Vec::new(),
            fk_cascades: Vec::new(),
        }
    }

    /// Whether a check or cascade acts on the rows of `database.table`.
    pub(crate) fn has_triggers(&self, database: &str, table: &str) -> bool {
        self.fk_checks
            .iter()
            .any(|check| targets(&check.plan, database, table))
            || self
                .fk_cascades
                .iter()
                .any(|cascade| targets(&cascade.plan, database, table))
    }

    /// Go `WithForeignKeyTrigger.HasFKCascades`.
    pub(crate) fn has_fk_cascades(&self) -> bool {
        !self.fk_cascades.is_empty()
    }

    /// Go `ExecStmt.prepareFKCascadeContext`'s
    /// `ForeignKeyTriggerCtx.HasFKCascades = true`, before the statement
    /// writes: every update it and its cascades make then writes its untouched
    /// index entries too, so a later cascade's union scan sees the row.
    pub(crate) fn prepare_fk_cascade_context(&self, ctx: &crate::StmtContext) {
        if self.has_fk_cascades() {
            ctx.mark_fk_cascades();
        }
    }

    /// Go `InsertValues.addRecordWithAutoIDHint`'s `insertRowNeedToCheck`
    /// loop over the table's checks.
    pub(crate) fn insert_row_need_to_check(
        &mut self,
        catalog: &SessionExtendedInfoSchema,
        database: &str,
        table: &str,
        row: &[Datum],
        ctx: &crate::StmtContext,
    ) -> Result<(), DriverError> {
        for fkc in self.checks_of(database, table) {
            fkc.insert_row_need_to_check(catalog, row, ctx)?;
        }
        Ok(())
    }

    /// The tail of Go `updateRecord`: a row the statement did not write under
    /// IGNORE hands its checks the change (`updateRowNeedToCheck`), and every
    /// cascade sees it (`onUpdateRow`).
    pub(crate) fn on_update_row(
        &mut self,
        catalog: &SessionExtendedInfoSchema,
        database: &str,
        table: &str,
        old: &[Datum],
        new: &[Datum],
        ignore: bool,
        ctx: &crate::StmtContext,
    ) -> Result<(), DriverError> {
        if !ignore {
            for fkc in self.checks_of(database, table) {
                fkc.update_row_need_to_check(catalog, old, new, ctx)?;
            }
        }
        for fkc in self.cascades_of(database, table) {
            fkc.on_update_row(old, new, ctx)?;
        }
        Ok(())
    }

    /// Go `onRemoveRowForFK`.
    pub(crate) fn on_remove_row_for_fk(
        &mut self,
        catalog: &SessionExtendedInfoSchema,
        database: &str,
        table: &str,
        data: &[Datum],
        ignore: bool,
        ctx: &crate::StmtContext,
    ) -> Result<(), DriverError> {
        if !ignore {
            for fkc in self.checks_of(database, table) {
                fkc.delete_row_need_to_check(catalog, data, ctx)?;
            }
        }
        for fkc in self.cascades_of(database, table) {
            fkc.on_delete_row(data, ctx)?;
        }
        Ok(())
    }

    /// Go `checkFKIgnoreErr`: the table's checks over one row, a violation
    /// a warning; whether the row is to be skipped.
    pub(crate) fn check_fk_ignore_err(
        &mut self,
        catalog: &SessionExtendedInfoSchema,
        database: &str,
        table: &str,
        row: &[Datum],
        ctx: &crate::StmtContext,
    ) -> Result<bool, DriverError> {
        let mut rows = [ToBeCheckedRow {
            row: row.to_vec(),
            ignored: false,
        }];
        self.check_rows(catalog, database, table, &mut rows, ctx)?;
        Ok(rows[0].ignored)
    }

    /// Go `batchCheckAndInsert`'s `fkc.checkRows` over the table's checks.
    pub(crate) fn check_rows(
        &mut self,
        catalog: &SessionExtendedInfoSchema,
        database: &str,
        table: &str,
        rows: &mut [ToBeCheckedRow],
        ctx: &crate::StmtContext,
    ) -> Result<(), DriverError> {
        let mut store = crate::statement_store(ctx);
        for fkc in self.checks_of(database, table) {
            fkc.check_rows(catalog, &mut *store, rows, ctx)?;
        }
        Ok(())
    }

    fn checks_of<'s>(
        &'s mut self,
        database: &'s str,
        table: &'s str,
    ) -> impl Iterator<Item = &'s mut FKCheckExec> + 's {
        self.fk_checks
            .iter_mut()
            .filter(move |fkc| targets(&fkc.plan, database, table))
    }

    fn cascades_of<'s>(
        &'s mut self,
        database: &'s str,
        table: &'s str,
    ) -> impl Iterator<Item = &'s mut FKCascadeExec> + 's {
        self.fk_cascades
            .iter_mut()
            .filter(move |fkc| targets(&fkc.plan, database, table))
    }
}

/// Go `toBeCheckedRow`'s row and `ignored` mark, as `checkRows` reads them.
pub(crate) struct ToBeCheckedRow {
    /// The candidate row.
    pub(crate) row: Vec<Datum>,
    /// Whether a check skipped it.
    pub(crate) ignored: bool,
}

/// The table a trigger reads its row values from: the statement's own table,
/// which declares a child check's constraint and is referenced by a parent
/// check's or a cascade's.
fn targets(node: &FkTriggerNode, database: &str, table: &str) -> bool {
    let (db, name) = if node.kind == FkTriggerKind::ChildCheck {
        (&node.child_database, &node.child_table)
    } else {
        (&node.parent_database, &node.parent_table)
    };
    db.go_lower_eq(database) && name.go_lower_eq(table)
}

/// Go `fkValueHelper`.
struct FkValueHelper {
    cols_offsets: Vec<usize>,
    fk_values_set: HashSet<Vec<u8>>,
}

impl FkValueHelper {
    fn new(cols_offsets: Vec<usize>) -> Self {
        Self {
            cols_offsets,
            fk_values_set: HashSet::new(),
        }
    }

    /// Go `fetchFKValuesWithCheck`: the row's values, or `None` when one is
    /// NULL or the statement already handed these values over.
    fn fetch_fk_values_with_check(
        &mut self,
        row: &[Datum],
        ctx: &crate::StmtContext,
    ) -> Result<Option<Vec<Datum>>, DriverError> {
        let vals = self.fetch_fk_values(row)?;
        if has_null_value(&vals) {
            return Ok(None);
        }
        let key = encode_key(&vals, ctx.new_collation_enabled(), ctx)?;
        if !self.fk_values_set.insert(key) {
            return Ok(None);
        }
        Ok(Some(vals))
    }

    /// Go `fetchFKValues`.
    fn fetch_fk_values(&self, row: &[Datum]) -> Result<Vec<Datum>, DriverError> {
        self.cols_offsets
            .iter()
            .map(|offset| {
                row.get(*offset).cloned().ok_or_else(|| {
                    DriverError::from(tidb_table::table_errors::ERR_INDEX_OUT_BOUND.gen_with_stack_by_args(&[
                        "".into(),
                        (*offset as i64).into(),
                        format!("{row:?}").into(),
                    ]))
                })
            })
            .collect()
    }
}

/// Go `fkValueHelper.hasNullValue`: MATCH SIMPLE skips a value with a NULL.
fn has_null_value(vals: &[Datum]) -> bool {
    vals.iter().any(Datum::is_null)
}

/// Go `codec.EncodeKey(sc.TimeZone(), nil, vals...)` + `sc.HandleError`.
fn encode_key(
    vals: &[Datum],
    use_new_collation: bool,
    ctx: &crate::StmtContext,
) -> Result<Vec<u8>, DriverError> {
    tidb_codec::Encoder::new(use_new_collation)
        .encode_key_in_timezone(&ctx.session_zone(), vals)
        .map_err(|error| DriverError::unsupported(format!("foreign key value encoding failed: {error:?}")))
}

/// Go `oldVals[i].Compare(sc.TypeCtx(), &newVals[i], collate.GetCollator(...))`
/// over every value: a comparison error means the value must be checked.
fn same_values(left: &[Datum], right: &[Datum], ctx: &crate::StmtContext) -> bool {
    let zone = ctx.session_zone();
    let context = tidb_datatype::ConversionContext::new(
        tidb_exprctx::Columns::type_flags(ctx),
        tidb_datatype::ConversionLocation::from_time_zone(&zone),
        &tidb_context::IGNORE_CONVERSION_WARNINGS,
    );
    left.len() == right.len()
        && left.iter().zip(right).all(|(left, right)| {
            let (ordering, error) = left.compare_with_context(
                right,
                left.collation().unwrap_or(tidb_charset::Collation::Binary),
                &context,
                &zone,
            );
            error.is_none() && ordering == std::cmp::Ordering::Equal
        })
}

/// The `table.Table` a check or cascade names: its storage half.
fn kv_table<'c>(
    catalog: &'c SessionExtendedInfoSchema,
    database: &str,
    table: &str,
) -> Result<&'c TableCommon, DriverError> {
    match catalog.get_in(database, table) {
        Some(TableEntry::Kv(kv)) => Ok(kv),
        _ => Err(DriverError::unsupported(
            "foreign key table changed after planning",
        )),
    }
}

/// Go `FKCheckExec`: a foreign-key check of the rows one table's writes
/// touched. A child-side check (`FK`, `CheckExist`) requires each written
/// key to exist in the parent; a parent-side one (`ReferredFK`) requires
/// each withdrawn key to be absent from the child.
pub(crate) struct FKCheckExec {
    /// Go `*physicalop.FKCheck`.
    plan: FkTriggerNode,
    helper: FkValueHelper,
    to_be_checked_keys: Vec<Key>,
    to_be_checked_prefix_keys: Vec<Key>,
    to_be_locked_keys: Vec<Key>,
    check_rows_cache: HashMap<Vec<u8>, bool>,
    stats: Option<FKCheckRuntimeStats>,
}

impl FKCheckExec {
    /// Go `buildFKCheckExec`: the offsets of the constraint's columns in the
    /// statement's rows (`FK.Cols` / `ReferredFK.Cols`).
    fn build(plan: &FkTriggerNode) -> Self {
        let cols_offsets = if plan.kind == FkTriggerKind::ChildCheck {
            plan.child_offsets.clone()
        } else {
            plan.parent_offsets.clone()
        };
        Self {
            plan: plan.clone(),
            helper: FkValueHelper::new(cols_offsets),
            to_be_checked_keys: Vec::new(),
            to_be_checked_prefix_keys: Vec::new(),
            to_be_locked_keys: Vec::new(),
            check_rows_cache: HashMap::new(),
            stats: None,
        }
    }

    /// Go `FKCheck.CheckExist`.
    fn check_exist(&self) -> bool {
        self.plan.kind == FkTriggerKind::ChildCheck
    }

    /// Go `FKCheck.Tbl`: the table whose keys are checked.
    fn tbl<'c>(&self, catalog: &'c SessionExtendedInfoSchema) -> Result<&'c TableCommon, DriverError> {
        if self.check_exist() {
            kv_table(catalog, &self.plan.parent_database, &self.plan.parent_table)
        } else {
            kv_table(catalog, &self.plan.child_database, &self.plan.child_table)
        }
    }

    /// Go `FKCheck.FailedErr`, built when the check was planned.
    fn failed_err(&self) -> DriverError {
        if self.check_exist() {
            DriverError::from(
                tidb_plannererrors::ERR_NO_REFERENCED_ROW2
                    .fast_gen_by_args(&[self.plan.fk_string.clone().into()]),
            )
        } else {
            DriverError::from(
                tidb_plannererrors::ERR_ROW_IS_REFERENCED2
                    .gen_with_stack_by_args(&[self.plan.fk_string.clone().into()]),
            )
        }
    }

    /// Go `insertRowNeedToCheck`.
    fn insert_row_need_to_check(
        &mut self,
        catalog: &SessionExtendedInfoSchema,
        row: &[Datum],
        ctx: &crate::StmtContext,
    ) -> Result<(), DriverError> {
        if !self.check_exist() {
            // Insert into parent table doesn't need to do foreign key check.
            return Ok(());
        }
        self.add_row_need_to_check(catalog, row, ctx)
    }

    /// Go `updateRowNeedToCheck`.
    fn update_row_need_to_check(
        &mut self,
        catalog: &SessionExtendedInfoSchema,
        old_row: &[Datum],
        new_row: &[Datum],
        ctx: &crate::StmtContext,
    ) -> Result<(), DriverError> {
        let new_vals = self.helper.fetch_fk_values(new_row)?;
        let old_vals = self.helper.fetch_fk_values(old_row)?;
        if same_values(&old_vals, &new_vals, ctx) {
            // If the old fk value and the new fk value are the same, no need
            // to check.
            return Ok(());
        }
        if self.check_exist() {
            self.add_row_need_to_check(catalog, new_row, ctx)
        } else {
            self.add_row_need_to_check(catalog, old_row, ctx)
        }
    }

    /// Go `deleteRowNeedToCheck`.
    fn delete_row_need_to_check(
        &mut self,
        catalog: &SessionExtendedInfoSchema,
        row: &[Datum],
        ctx: &crate::StmtContext,
    ) -> Result<(), DriverError> {
        self.add_row_need_to_check(catalog, row, ctx)
    }

    /// Go `addRowNeedToCheck`.
    fn add_row_need_to_check(
        &mut self,
        catalog: &SessionExtendedInfoSchema,
        row: &[Datum],
        ctx: &crate::StmtContext,
    ) -> Result<(), DriverError> {
        let Some(vals) = self.helper.fetch_fk_values_with_check(row, ctx)? else {
            return Ok(());
        };
        let (key, is_prefix) = self.build_check_key_from_fk_value(catalog, vals, ctx)?;
        if is_prefix {
            self.to_be_checked_prefix_keys.push(key);
        } else {
            self.to_be_checked_keys.push(key);
        }
        Ok(())
    }

    /// Go `doCheck`, run once the statement wrote all its rows.
    fn do_check(
        &mut self,
        catalog: &SessionExtendedInfoSchema,
        ctx: &crate::StmtContext,
    ) -> Result<(), DriverError> {
        let source = ctx.executor_runtime_stats_source().cloned();
        if source.is_some() {
            self.stats = Some(FKCheckRuntimeStats::default());
        }
        let result = self.check(catalog, ctx);
        if let (Some(source), Some(stats)) = (source, self.stats.take()) {
            source.register_stats(i64::from(self.plan.id), Box::new(stats));
        }
        result
    }

    fn check(
        &mut self,
        catalog: &SessionExtendedInfoSchema,
        ctx: &crate::StmtContext,
    ) -> Result<(), DriverError> {
        if self.to_be_checked_keys.is_empty()
            && self.to_be_checked_prefix_keys.is_empty()
            && self.to_be_locked_keys.is_empty()
        {
            return Ok(());
        }
        let start = Instant::now();
        let keys = self.to_be_checked_keys.len() + self.to_be_checked_prefix_keys.len();
        let result = self.check_and_lock(catalog, ctx, start);
        if let Some(stats) = &mut self.stats {
            stats.keys = keys;
            stats.total = start.elapsed();
        }
        result
    }

    fn check_and_lock(
        &mut self,
        catalog: &SessionExtendedInfoSchema,
        ctx: &crate::StmtContext,
        start: Instant,
    ) -> Result<(), DriverError> {
        let mut store = crate::statement_store(ctx);
        self.check_keys(&mut *store)?;
        self.check_index_keys(catalog, &mut *store)?;
        if let Some(stats) = &mut self.stats {
            stats.check = start.elapsed();
        }
        if self.to_be_locked_keys.is_empty() {
            return Ok(());
        }
        // Keys to be locked in a pessimistic transaction are locked with the
        // statement's other keys once its writes are done (Go
        // `handlePessimisticDML`), since a share-locked key may not be a
        // primary key for now.
        if ctx.pessimistic_transaction() {
            if let Some(selected) = ctx.selected_lock_keys() {
                for key in &self.to_be_locked_keys {
                    selected.insert(key.as_bytes().to_vec());
                }
            }
            return Ok(());
        }
        // Go `doLockKeys` in an optimistic transaction sends no request but
        // flags the keys locked, so the commit writes their LOCK records.
        let wait = ctx
            .selected_lock_keys()
            .map(|selected| selected.default_wait())
            .unwrap_or_default();
        let locked = store.do_lock_keys(&self.to_be_locked_keys, wait);
        if let Some(stats) = &mut self.stats {
            stats.lock = start.elapsed().saturating_sub(stats.check);
        }
        locked.map_err(DriverError::from)
    }

    /// Go `buildCheckKeyFromFKValue`: the key the values name in `Tbl`, and
    /// whether it is only a prefix of the keys to look for.
    fn build_check_key_from_fk_value(
        &self,
        catalog: &SessionExtendedInfoSchema,
        vals: Vec<Datum>,
        ctx: &crate::StmtContext,
    ) -> Result<(Key, bool), DriverError> {
        let tbl = self.tbl(catalog)?;
        if self.plan.idx_is_primary_key {
            let handle = self.build_handle_from_fk_values(tbl, &vals, ctx)?;
            let key = Key::from_bytes(tidb_tablecodec::table_key::encode_row_key_with_handle(
                tbl.table_id(),
                &handle.record_handle(),
            ));
            return Ok((key, !self.plan.idx_is_exclusive));
        }
        let index_id = self
            .plan
            .lookup_index
            .ok_or_else(|| DriverError::unsupported("foreign key check has no index"))?;
        let (key, distinct) = tbl
            .gen_index_key(index_id, vals, None, tbl.table_id(), &ctx.session_zone())
            .map_err(crate::driver::kv_write_error)?;
        Ok((Key::from_bytes(key), !(distinct && self.plan.idx_is_exclusive)))
    }

    /// Go `buildHandleFromFKValues`.
    fn build_handle_from_fk_values(
        &self,
        tbl: &TableCommon,
        vals: &[Datum],
        ctx: &crate::StmtContext,
    ) -> Result<TableHandle, DriverError> {
        if vals.len() == 1 && self.plan.lookup_index.is_none() {
            // Go `vals[0].GetInt64()`, which reads an unsigned value's bits.
            return match &vals[0] {
                Datum::Int(value) => Ok(TableHandle::Int(*value)),
                Datum::UInt(value) => Ok(TableHandle::Int(*value as i64)),
                other => Err(DriverError::unsupported(format!(
                    "foreign key integer handle is invalid: {other:?}"
                ))),
            };
        }
        let handle_bytes = encode_key(vals, tbl.use_new_collation(), ctx)?;
        tidb_txnkv::CommonHandle::new(handle_bytes.clone()).map_err(|error| {
            DriverError::unsupported(format!("foreign key common handle is invalid: {error:?}"))
        })?;
        Ok(TableHandle::Common(handle_bytes))
    }

    /// Go `checkKeys`.
    fn check_keys(&mut self, store: &mut dyn tidb_kv::storage::TableStorage) -> Result<(), DriverError> {
        if self.to_be_checked_keys.is_empty() {
            return Ok(());
        }
        let values = prefetch_keys(store, &self.to_be_checked_keys)?;
        for index in 0..self.to_be_checked_keys.len() {
            let key = self.to_be_checked_keys[index].clone();
            self.check_key(key, &values)?;
        }
        Ok(())
    }

    /// Go `checkKey`: `txn.Get` over the keys `prefetchKeys` read.
    fn check_key(&mut self, key: Key, values: &HashMap<Key, Vec<u8>>) -> Result<(), DriverError> {
        let exists = values.contains_key(&key);
        if self.check_exist() {
            // Go `checkKeyExist`.
            if !exists {
                return Err(self.failed_err());
            }
            self.to_be_locked_keys.push(key);
            return Ok(());
        }
        // Go `checkKeyNotExist`.
        if exists {
            return Err(self.failed_err());
        }
        Ok(())
    }

    /// Go `checkIndexKeys`.
    fn check_index_keys(
        &mut self,
        catalog: &SessionExtendedInfoSchema,
        store: &mut dyn tidb_kv::storage::TableStorage,
    ) -> Result<(), DriverError> {
        for index in 0..self.to_be_checked_prefix_keys.len() {
            let key = self.to_be_checked_prefix_keys[index].clone();
            self.check_prefix_key(catalog, store, &key)?;
        }
        Ok(())
    }

    /// Go `checkPrefixKey`.
    fn check_prefix_key(
        &mut self,
        catalog: &SessionExtendedInfoSchema,
        store: &mut dyn tidb_kv::storage::TableStorage,
        key: &Key,
    ) -> Result<(), DriverError> {
        let found = get_index_key_value_in_table(store, key)?;
        if self.check_exist() {
            return self.check_prefix_key_exist(catalog, found);
        }
        if found.as_ref().is_some_and(|(_, value)| !value.is_empty()) {
            // If check not exist, but the key is exist, return failedErr.
            return Err(self.failed_err());
        }
        Ok(())
    }

    /// Go `checkPrefixKeyExist`: the row the key found is the one to lock.
    fn check_prefix_key_exist(
        &mut self,
        catalog: &SessionExtendedInfoSchema,
        found: Option<(Key, Vec<u8>)>,
    ) -> Result<(), DriverError> {
        let Some((key, value)) = found.filter(|(_, value)| !value.is_empty()) else {
            return Err(self.failed_err());
        };
        let tbl = self.tbl(catalog)?;
        let index = self
            .plan
            .lookup_index
            .and_then(|id| tbl.indexes().iter().find(|index| index.read().id == id));
        match index {
            // The prefix of a clustered primary key is a record key prefix.
            Some(index) if !(index.read().primary && !tbl.common_handle_offsets().is_empty()) => {
                let handle = tidb_table::tables::index_entry_handle(
                    index,
                    key.as_bytes(),
                    &value,
                    !tbl.common_handle_offsets().is_empty(),
                )
                .map_err(|error| crate::driver::kv_read_error("foreign key index decode failed", error))?;
                self.to_be_locked_keys.push(Key::from_bytes(
                    tidb_tablecodec::table_key::encode_row_key_with_handle(
                        tbl.table_id(),
                        &handle.record_handle(),
                    ),
                ));
            }
            _ => self.to_be_locked_keys.push(key),
        }
        Ok(())
    }

    /// Go `checkRows`, the IGNORE form of the check: a row whose key fails
    /// is skipped with `FailedErr` as a warning, each key checked once.
    fn check_rows(
        &mut self,
        catalog: &SessionExtendedInfoSchema,
        store: &mut dyn tidb_kv::storage::TableStorage,
        rows: &mut [ToBeCheckedRow],
        ctx: &crate::StmtContext,
    ) -> Result<(), DriverError> {
        let source = ctx.executor_runtime_stats_source().cloned();
        if source.is_some() {
            self.stats = Some(FKCheckRuntimeStats::default());
        }
        let result = self.check_rows_inner(catalog, store, rows, ctx);
        if let (Some(source), Some(stats)) = (source, self.stats.take()) {
            source.register_stats(i64::from(self.plan.id), Box::new(stats));
        }
        result
    }

    fn check_rows_inner(
        &mut self,
        catalog: &SessionExtendedInfoSchema,
        store: &mut dyn tidb_kv::storage::TableStorage,
        rows: &mut [ToBeCheckedRow],
        ctx: &crate::StmtContext,
    ) -> Result<(), DriverError> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut fk_check_keys: Vec<Option<(Key, bool)>> = Vec::with_capacity(rows.len());
        let mut prefetch: Vec<Key> = Vec::with_capacity(rows.len());
        for row in rows.iter() {
            if row.ignored {
                fk_check_keys.push(None);
                continue;
            }
            let vals = self.helper.fetch_fk_values(&row.row)?;
            if has_null_value(&vals) {
                fk_check_keys.push(None);
                continue;
            }
            let (key, is_prefix) = self.build_check_key_from_fk_value(catalog, vals, ctx)?;
            if !is_prefix {
                prefetch.push(key.clone());
            }
            fk_check_keys.push(Some((key, is_prefix)));
        }
        let values = if prefetch.is_empty() {
            HashMap::new()
        } else {
            prefetch_keys(store, &prefetch)?
        };
        for (index, fk_check_key) in fk_check_keys.into_iter().enumerate() {
            let Some((key, is_prefix)) = fk_check_key else {
                continue;
            };
            let cache_key = key.as_bytes().to_vec();
            if let Some(ignore) = self.check_rows_cache.get(&cache_key) {
                if *ignore {
                    rows[index].ignored = true;
                    append_warning(self.failed_err(), ctx);
                }
                continue;
            }
            let result = if is_prefix {
                self.check_prefix_key(catalog, store, &key)
            } else {
                self.check_key(key, &values)
            };
            let failed = result.is_err();
            if failed {
                rows[index].ignored = true;
                append_warning(self.failed_err(), ctx);
            }
            self.check_rows_cache.insert(cache_key, failed);
            if let Some(stats) = &mut self.stats {
                stats.keys += 1;
            }
        }
        Ok(())
    }
}

/// `sc.AppendWarning(err)`.
fn append_warning(error: DriverError, ctx: &crate::StmtContext) {
    let warning = error.to_mysql_error();
    ctx.append_sql_warn(tidb_context::SqlWarn::from_error(
        tidb_context::WarningLevel::Warning,
        warning.code,
        warning.message.clone(),
        warning.identity,
    ));
}

/// Go `prefetchKeys`: one `BatchGet` over the keys, whose answers the
/// following `Get`s read (`txn.BatchGet` fills the snapshot cache they hit).
fn prefetch_keys(
    store: &mut dyn tidb_kv::storage::TableStorage,
    keys: &[Key],
) -> Result<HashMap<Key, Vec<u8>>, DriverError> {
    store.batch_get(keys).map_err(DriverError::from)
}

/// Go `getIndexKeyValueInTable`: the first entry under the prefix `key` the
/// transaction holds -- its own writes over its snapshot, a key it deleted
/// hidden -- which is what the transaction's iterator yields.
fn get_index_key_value_in_table(
    store: &mut dyn tidb_kv::storage::TableStorage,
    key: &Key,
) -> Result<Option<(Key, Vec<u8>)>, DriverError> {
    let upper = key.prefix_next();
    let found = store
        .first(Some(key), Some(&upper))
        .map_err(DriverError::from)?;
    Ok(found.filter(|(found, _)| found.has_prefix(key)))
}

/// Go `UpdatedValuesCouple`.
#[derive(Clone, Debug)]
pub(crate) struct UpdatedValuesCouple {
    pub(crate) new_values: Vec<Datum>,
    pub(crate) old_values_list: Vec<Vec<Datum>>,
}

/// Go `FKCascadeExec`: the values a statement withdrew from a referenced
/// table, and the cascade statements it builds over the child table.
pub(crate) struct FKCascadeExec {
    /// Go `*physicalop.FKCascade`.
    plan: FkTriggerNode,
    helper: FkValueHelper,
    /// Go `fkCols`: the child's referencing columns, by name and type.
    fk_cols: Vec<(String, FieldType)>,
    /// Go `fkIdx.Name`: the child index the cascade statement uses.
    fk_idx: Option<String>,
    /// On delete statement, `fk_values` stores the delete foreign key values.
    /// On update statement and the foreign key cascade is `SET NULL`,
    /// `fk_values` stores the old foreign key values.
    fk_values: Vec<Vec<Datum>>,
    /// Go `fkUpdatedValuesMap`: new-value-key => `UpdatedValuesCouple`, in
    /// the order the statement first wrote each new value (Go's map has
    /// none).
    fk_updated_values_map: Vec<(Vec<u8>, UpdatedValuesCouple)>,
    stats: Option<FKCascadeRuntimeStats>,
    /// Go `FKCascade.CascadePlans`, collected for `EXPLAIN ANALYZE`.
    cascade_plans: Vec<Box<PhysicalPlan>>,
}

/// The cascade statement Go's `buildFKCascadePlan` generates.
pub(crate) enum FKCascadeStmt {
    /// `GenCascadeDeleteAST`.
    Delete(tidb_ast::DeleteStmt),
    /// `GenCascadeSetNullAST` / `GenCascadeUpdateAST`.
    Update(tidb_ast::UpdateStmt),
}

impl FKCascadeExec {
    /// Go `executorBuilder.buildFKCascadeExec`.
    fn build(plan: &FkTriggerNode, catalog: &SessionExtendedInfoSchema) -> Result<Self, DriverError> {
        let child = kv_table(catalog, &plan.child_database, &plan.child_table)?;
        let fk_cols = plan
            .child_offsets
            .iter()
            .map(|offset| {
                child
                    .columns()
                    .get(*offset)
                    .map(|column| {
                        let column = column.read();
                        (column.name.original().to_owned(), column.field_type.clone())
                    })
                    .ok_or_else(|| DriverError::unsupported("foreign key column is not found"))
            })
            .collect::<Result<_, _>>()?;
        let fk_idx = plan.lookup_index.and_then(|id| {
            child
                .indexes()
                .iter()
                .find(|index| index.read().id == id)
                .map(|index| index.read().name.original().to_owned())
        });
        Ok(Self {
            plan: plan.clone(),
            helper: FkValueHelper::new(plan.parent_offsets.clone()),
            fk_cols,
            fk_idx,
            fk_values: Vec::new(),
            fk_updated_values_map: Vec::new(),
            stats: None,
            cascade_plans: Vec::new(),
        })
    }

    /// Go `FKCascade.Tp == FKCascadeOnDelete`, and whether the referential
    /// action of that event is `SET NULL` (else `CASCADE`).
    fn tp(&self) -> (bool, bool) {
        match self.plan.kind {
            FkTriggerKind::Cascade { on_delete, set_null } => (on_delete, set_null),
            _ => unreachable!("an FKCascadeExec is built from a cascade"),
        }
    }

    /// Go `onDeleteRow`.
    fn on_delete_row(&mut self, row: &[Datum], ctx: &crate::StmtContext) -> Result<(), DriverError> {
        if let Some(vals) = self.helper.fetch_fk_values_with_check(row, ctx)? {
            self.fk_values.push(vals);
        }
        Ok(())
    }

    /// Go `onUpdateRow`.
    fn on_update_row(
        &mut self,
        old_row: &[Datum],
        new_row: &[Datum],
        ctx: &crate::StmtContext,
    ) -> Result<(), DriverError> {
        let Some(old_vals) = self.helper.fetch_fk_values_with_check(old_row, ctx)? else {
            return Ok(());
        };
        // Go reads `fk.OnUpdate`; an update's cascade is the on-update one.
        let (_, set_null) = self.tp();
        if set_null {
            self.fk_values.push(old_vals);
            return Ok(());
        }
        let new_vals = self.helper.fetch_fk_values(new_row)?;
        let new_vals_key = encode_key(&new_vals, ctx.new_collation_enabled(), ctx)?;
        match self
            .fk_updated_values_map
            .iter_mut()
            .find(|(key, _)| *key == new_vals_key)
        {
            Some((_, couple)) => couple.old_values_list.push(old_vals),
            None => self.fk_updated_values_map.push((
                new_vals_key,
                UpdatedValuesCouple {
                    new_values: new_vals,
                    old_values_list: vec![old_vals],
                },
            )),
        }
        Ok(())
    }

    /// Go `buildFKCascadePlan`'s statement half: the next cascade statement,
    /// or `None` once every value was handled.
    fn build_fk_cascade_stmt(&mut self) -> Result<Option<FKCascadeStmt>, DriverError> {
        if self.fk_values.is_empty() && self.fk_updated_values_map.is_empty() {
            return Ok(None);
        }
        let schema = self.plan.child_database.clone();
        let table = self.plan.child_table.clone();
        let index = self.fk_idx.clone();
        let (on_delete, set_null) = self.tp();
        let stmt = match (on_delete, set_null) {
            (true, false) => {
                let fk_values = self.fetch_on_delete_or_update_fk_values();
                Some(FKCascadeStmt::Delete(gen_cascade_delete_ast(
                    &schema,
                    &table,
                    index.as_deref(),
                    &self.fk_cols,
                    fk_values,
                )))
            }
            (_, true) => {
                let fk_values = self.fetch_on_delete_or_update_fk_values();
                Some(FKCascadeStmt::Update(gen_cascade_set_null_ast(
                    &schema,
                    &table,
                    index.as_deref(),
                    &self.fk_cols,
                    fk_values,
                )))
            }
            (false, false) => match self.fetch_updated_values_couple() {
                Some(couple) if !couple.new_values.is_empty() => {
                    if let Some(stats) = &mut self.stats {
                        stats.keys += couple.old_values_list.len();
                    }
                    Some(FKCascadeStmt::Update(gen_cascade_update_ast(
                        &schema,
                        &table,
                        index.as_deref(),
                        &self.fk_cols,
                        couple,
                    )))
                }
                _ => None,
            },
        };
        stmt.map(Some).ok_or_else(|| {
            DriverError::from(tidb_terror::MysqlError::unknown(format!(
                "generate foreign key cascade ast failed, {}",
                u8::from(!on_delete)
            )))
        })
    }

    /// Go `fetchOnDeleteOrUpdateFKValues`.
    fn fetch_on_delete_or_update_fk_values(&mut self) -> Vec<Vec<Datum>> {
        let fk_values = if self.fk_values.len() <= MAX_HANDLE_FK_VALUE_IN_ONE_CASCADE {
            std::mem::take(&mut self.fk_values)
        } else {
            let rest = self.fk_values.split_off(MAX_HANDLE_FK_VALUE_IN_ONE_CASCADE);
            std::mem::replace(&mut self.fk_values, rest)
        };
        if let Some(stats) = &mut self.stats {
            stats.keys += fk_values.len();
        }
        fk_values
    }

    /// Go `fetchUpdatedValuesCouple`.
    fn fetch_updated_values_couple(&mut self) -> Option<UpdatedValuesCouple> {
        let (_, couple) = self.fk_updated_values_map.first_mut()?;
        if couple.old_values_list.len() <= MAX_HANDLE_FK_VALUE_IN_ONE_CASCADE {
            return Some(self.fk_updated_values_map.remove(0).1);
        }
        let rest = couple
            .old_values_list
            .split_off(MAX_HANDLE_FK_VALUE_IN_ONE_CASCADE);
        let result = UpdatedValuesCouple {
            new_values: couple.new_values.clone(),
            old_values_list: std::mem::replace(&mut couple.old_values_list, rest),
        };
        Some(result)
    }
}

/// Go `GenCascadeDeleteAST`.
pub(crate) fn gen_cascade_delete_ast(
    schema: &str,
    table: &str,
    idx: Option<&str>,
    cols: &[(String, FieldType)],
    fk_values: Vec<Vec<Datum>>,
) -> tidb_ast::DeleteStmt {
    tidb_ast::DeleteStmt {
        with: None,
        hints: Vec::new(),
        priority: tidb_ast::StatementPriority::default(),
        quick: false,
        ignore: false,
        kind: tidb_ast::DeleteKind::Single(gen_table_refs_ast(schema, table, idx)),
        where_clause: Some(gen_where_condition_ast(cols, fk_values)),
        order_by: Vec::new(),
        limit: None,
        returning: tidb_ast::SelectFieldList::default(),
    }
}

/// Go `GenCascadeSetNullAST`.
pub(crate) fn gen_cascade_set_null_ast(
    schema: &str,
    table: &str,
    idx: Option<&str>,
    cols: &[(String, FieldType)],
    fk_values: Vec<Vec<Datum>>,
) -> tidb_ast::UpdateStmt {
    let couple = UpdatedValuesCouple {
        new_values: vec![Datum::Null; cols.len()],
        old_values_list: fk_values,
    };
    gen_cascade_update_ast(schema, table, idx, cols, couple)
}

/// Go `GenCascadeUpdateAST`.
pub(crate) fn gen_cascade_update_ast(
    schema: &str,
    table: &str,
    idx: Option<&str>,
    cols: &[(String, FieldType)],
    couple: UpdatedValuesCouple,
) -> tidb_ast::UpdateStmt {
    let assignments = cols
        .iter()
        .zip(couple.new_values)
        .map(|((name, field_type), datum)| tidb_ast::Assignment {
            col: vec![name.clone()],
            value: value_expr(datum, field_type),
        })
        .collect();
    tidb_ast::UpdateStmt {
        with: None,
        hints: Vec::new(),
        priority: tidb_ast::StatementPriority::default(),
        ignore: false,
        kind: tidb_ast::UpdateKind::Single(gen_table_refs_ast(schema, table, idx)),
        assignments,
        where_clause: Some(gen_where_condition_ast(cols, couple.old_values_list)),
        order_by: Vec::new(),
        limit: None,
        returning: tidb_ast::SelectFieldList::default(),
    }
}

/// Go `&driver.ValueExpr{Datum: d}` with `v.Type = col.FieldType`.
fn value_expr(datum: Datum, field_type: &FieldType) -> tidb_ast::Expr {
    tidb_expr::ValueExprDatum {
        datum,
        field_type: field_type.clone(),
    }
    .into_expr()
}

/// Go `genTableRefsAST`: the child table, with `USE INDEX (idx)` when the
/// constraint has an index.
fn gen_table_refs_ast(schema: &str, table: &str, idx: Option<&str>) -> tidb_ast::TableRef {
    tidb_ast::TableRef {
        identity: tidb_ast::TableIdentity::new(),
        name: vec![schema.to_owned(), table.to_owned()],
        partitions: Vec::new(),
        alias: None,
        as_of: None,
        hints: idx
            .filter(|idx| !idx.is_empty())
            .map(|idx| tidb_ast::IndexHint {
                kind: tidb_ast::IndexHintKind::Use,
                scope: tidb_ast::IndexHintScope::All,
                indexes: vec![idx.to_owned()],
            })
            .into_iter()
            .collect(),
        sample: None,
    }
}

/// Go `genWhereConditionAst`: `col IN (v, ...)` or `(c1, c2) IN ((v1, v2),
/// ...)`.
fn gen_where_condition_ast(cols: &[(String, FieldType)], fk_values: Vec<Vec<Datum>>) -> tidb_ast::Expr {
    if cols.len() > 1 {
        return gen_where_condition_ast_for_multi_column(cols, fk_values);
    }
    let list = fk_values
        .into_iter()
        .map(|mut fk_vals| value_expr(fk_vals.swap_remove(0), &cols[0].1))
        .collect();
    tidb_ast::Expr::In {
        expr: Box::new(tidb_ast::Expr::Column(vec![cols[0].0.clone()])),
        list,
        not: false,
    }
}

/// Go `genWhereConditionAstForMultiColumn`.
fn gen_where_condition_ast_for_multi_column(
    cols: &[(String, FieldType)],
    fk_values: Vec<Vec<Datum>>,
) -> tidb_ast::Expr {
    let col_values = cols
        .iter()
        .map(|(name, _)| tidb_ast::Expr::Column(vec![name.clone()]))
        .collect();
    let list = fk_values
        .into_iter()
        .map(|fk_vals| {
            tidb_ast::Expr::Row(
                fk_vals
                    .into_iter()
                    .zip(cols)
                    .map(|(value, (_, field_type))| value_expr(value, field_type))
                    .collect(),
            )
        })
        .collect();
    tidb_ast::Expr::In {
        expr: Box::new(tidb_ast::Expr::Row(col_values)),
        list,
        not: false,
    }
}

/// Go `ExecStmt.handleStmtForeignKeyTrigger`: the statement's triggers at
/// depth 1, once it wrote all its rows. `triggers` are the plan's, which
/// receive the cascade plans `EXPLAIN ANALYZE` shows; `plan_id` is where
/// the statement's plan left its plan ids. The statement stage rolls the
/// statement back when this fails (Go's `handleFKTriggerError`).
pub(crate) fn handle_stmt_foreign_key_trigger(
    fk: WithForeignKeyTrigger,
    triggers: &mut [FkTriggerNode],
    plan_id: i32,
    catalog: &mut SessionExtendedInfoSchema,
    ctx: &crate::StmtContext,
    runtime: Option<&mut PhysicalRuntimeStats>,
) -> Result<(), DriverError> {
    if fk.fk_checks.is_empty() && fk.fk_cascades.is_empty() {
        return Ok(());
    }
    let plan_ids = PlanIdAllocator::continuing_from(plan_id);
    handle_foreign_key_trigger(fk, triggers, &plan_ids, catalog, ctx, runtime, 1)
}

/// Go `ExecStmt.handleForeignKeyTrigger`: every check, then every cascade.
fn handle_foreign_key_trigger(
    mut fk: WithForeignKeyTrigger,
    triggers: &mut [FkTriggerNode],
    plan_ids: &PlanIdAllocator,
    catalog: &mut SessionExtendedInfoSchema,
    ctx: &crate::StmtContext,
    mut runtime: Option<&mut PhysicalRuntimeStats>,
    depth: usize,
) -> Result<(), DriverError> {
    for fk_check in &mut fk.fk_checks {
        fk_check.do_check(catalog, ctx)?;
    }
    for fk_cascade in &mut fk.fk_cascades {
        let handled =
            handle_foreign_key_cascade(fk_cascade, plan_ids, catalog, ctx, runtime.as_deref_mut(), depth);
        // Go appends each cascade plan to `FKCascade.CascadePlans` as it
        // builds it.
        if let Some(trigger) = triggers.iter_mut().find(|trigger| trigger.id == fk_cascade.plan.id) {
            trigger.cascade_plans.append(&mut fk_cascade.cascade_plans);
        }
        handled?;
    }
    Ok(())
}

/// Go `ExecStmt.handleForeignKeyCascade`: build a cascade statement over at
/// most `maxHandleFKValueInOneCascade` values, run it, handle its own
/// triggers one level deeper, and go on until no value is left.
fn handle_foreign_key_cascade(
    fkc: &mut FKCascadeExec,
    plan_ids: &PlanIdAllocator,
    catalog: &mut SessionExtendedInfoSchema,
    ctx: &crate::StmtContext,
    mut runtime: Option<&mut PhysicalRuntimeStats>,
    depth: usize,
) -> Result<(), DriverError> {
    let source = ctx.executor_runtime_stats_source().cloned();
    if source.is_some() {
        fkc.stats = Some(FKCascadeRuntimeStats::default());
    }
    let mut started = None;
    let mut handling = false;
    let result = (|| {
        if fkc.fk_values.is_empty() && fkc.fk_updated_values_map.is_empty() {
            return Ok(());
        }
        if depth > MAX_FOREIGN_KEY_CASCADE_DEPTH {
            return Err(DriverError::from(
                tidb_exeerrors::ERR_FOREIGN_KEY_CASCADE_DEPTH_EXCEEDED
                    .gen_with_stack_by_args(&[MAX_FOREIGN_KEY_CASCADE_DEPTH.into()]),
            ));
        }
        // Go `StmtCtx.InHandleForeignKeyTrigger`, reset by its defer.
        ctx.set_in_handle_foreign_key_trigger(true);
        handling = true;
        started = Some(Instant::now());
        loop {
            // Go `buildExecutor`: `buildFKCascadePlan`, then the executor.
            let Some(stmt) = fkc.build_fk_cascade_stmt()? else {
                return Ok(());
            };
            check_fk_cascade_table_lock(fkc, &stmt, catalog, ctx)?;
            ctx.set_plans_fk_cascade(true);
            let planned = crate::driver::physical_fk_cascade_plan(&stmt, catalog, ctx, plan_ids);
            ctx.set_plans_fk_cascade(false);
            let mut plan = Box::new(planned?);
            let sub = crate::driver::run_fk_cascade(&stmt, &mut plan, catalog, ctx, runtime.as_deref_mut())?;
            let PhysicalPlan::Dml(root) = &mut *plan else {
                unreachable!("a cascade statement plans a DML root");
            };
            let handled = handle_foreign_key_trigger(
                sub,
                &mut root.fk_triggers,
                plan_ids,
                catalog,
                ctx,
                runtime.as_deref_mut(),
                depth + 1,
            );
            if runtime.is_some() {
                fkc.cascade_plans.push(plan);
            }
            handled?;
        }
    })();
    if handling {
        ctx.set_in_handle_foreign_key_trigger(false);
    }
    if let (Some(stats), Some(started)) = (&mut fkc.stats, started) {
        stats.total += started.elapsed();
    }
    if let (Some(source), Some(stats)) = (source, fkc.stats.take()) {
        source.register_stats(i64::from(fkc.plan.id), Box::new(stats));
    }
    result
}

/// Go `OptimizeForForeignKeyCascade`'s `CheckTableLock` over the visits of
/// the cascade statement it builds: the child `DELETE` or `UPDATE` reads the
/// child table and then writes it (`buildDelete` / `buildUpdate`).
fn check_fk_cascade_table_lock(
    fkc: &FKCascadeExec,
    stmt: &FKCascadeStmt,
    catalog: &SessionExtendedInfoSchema,
    ctx: &crate::StmtContext,
) -> Result<(), DriverError> {
    let Some(locked_tables) = ctx.locked_tables() else {
        return Ok(());
    };
    if locked_tables.is_empty() && !catalog.has_table_locks() {
        return Ok(());
    }
    let write = match stmt {
        FKCascadeStmt::Delete(_) => tidb_mysql::DeletePriv,
        FKCascadeStmt::Update(_) => tidb_mysql::UpdatePriv,
    };
    let (db, table) = (&fkc.plan.child_database, &fkc.plan.child_table);
    tidb_lock::check_visits(
        &tidb_lock::Checker::new(locked_tables, catalog),
        &[(db, table, tidb_mysql::SelectPriv), (db, table, write)],
    )
}

/// Go `FKCheckRuntimeStats`.
#[derive(Clone, Debug, Default)]
pub(crate) struct FKCheckRuntimeStats {
    pub(crate) total: Duration,
    pub(crate) check: Duration,
    pub(crate) lock: Duration,
    pub(crate) keys: usize,
}

impl tidb_execdetails::RuntimeStats for FKCheckRuntimeStats {
    fn string(&self) -> String {
        let mut buf = String::with_capacity(32);
        buf.push_str("total:");
        buf.push_str(&tidb_execdetails::format_duration(self.total));
        if !self.check.is_zero() {
            buf.push_str(", check:");
            buf.push_str(&tidb_execdetails::format_duration(self.check));
        }
        if !self.lock.is_zero() {
            buf.push_str(", lock:");
            buf.push_str(&tidb_execdetails::format_duration(self.lock));
        }
        if self.keys > 0 {
            buf.push_str(", foreign_keys:");
            buf.push_str(&self.keys.to_string());
        }
        buf
    }

    fn merge(&mut self, other: &dyn tidb_execdetails::RuntimeStats) {
        let Some(tmp) = other.as_any().downcast_ref::<Self>() else {
            return;
        };
        self.total += tmp.total;
        self.check += tmp.check;
        self.lock += tmp.lock;
        self.keys += tmp.keys;
    }

    fn clone_box(&self) -> Box<dyn tidb_execdetails::RuntimeStats> {
        Box::new(self.clone())
    }

    fn tp(&self) -> i32 {
        tidb_execdetails::TP_FK_CHECK_RUNTIME_STATS
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Go `FKCascadeRuntimeStats`.
#[derive(Clone, Debug, Default)]
pub(crate) struct FKCascadeRuntimeStats {
    pub(crate) total: Duration,
    pub(crate) keys: usize,
}

impl tidb_execdetails::RuntimeStats for FKCascadeRuntimeStats {
    fn string(&self) -> String {
        let mut buf = String::with_capacity(32);
        buf.push_str("total:");
        buf.push_str(&tidb_execdetails::format_duration(self.total));
        if self.keys > 0 {
            buf.push_str(", foreign_keys:");
            buf.push_str(&self.keys.to_string());
        }
        buf
    }

    fn merge(&mut self, other: &dyn tidb_execdetails::RuntimeStats) {
        let Some(tmp) = other.as_any().downcast_ref::<Self>() else {
            return;
        };
        self.total += tmp.total;
        self.keys += tmp.keys;
    }

    fn clone_box(&self) -> Box<dyn tidb_execdetails::RuntimeStats> {
        Box::new(self.clone())
    }

    fn tp(&self) -> i32 {
        tidb_execdetails::TP_FK_CASCADE_RUNTIME_STATS
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Go `buildFKCheckForReferredFK`'s index, computed on demand: every
/// `(schema, table, foreign key)` whose constraint REFERS to `database.table`.
pub(crate) fn referring(
    catalog: &SessionExtendedInfoSchema,
    database: &str,
    table: &str,
) -> Vec<(String, String, tidb_model::GoShared<tidb_model::table::FKInfo>)> {
    // Sysbench and the normal TiDB bootstrap have no foreign-key declarations.
    // Avoid rebuilding and sorting the complete catalog path list for every
    // UPDATE/DELETE in that case; catalogs that do declare one retain the
    // deterministic scan below.
    if !catalog.has_foreign_keys() {
        return Vec::new();
    }
    let mut found = Vec::new();
    for (child_db, child_table, entry) in catalog.table_entries() {
        let TableEntry::Kv(child) = entry else {
            continue;
        };
        for foreign_key in child.meta().read().foreign_keys.iter_deref() {
            let key = foreign_key.read();
            if key.state == tidb_model::SchemaState::PUBLIC
                && key.version >= tidb_model::table::FK_VERSION1
                && key.ref_schema.original().go_lower_eq(database)
                && key.ref_table.original().go_lower_eq(table)
            {
                drop(key);
                found.push((child_db.clone(), child_table, foreign_key));
            }
        }
    }
    // Preserve the existing schema/table cascade order and declaration order
    // within each child. Only matching constraints need owned metadata: the
    // cascade mutates the catalog after this borrow ends. Unrelated tables
    // contribute no name, column, or constraint copies.
    found.sort_by(|left, right| (&left.0, left.1).cmp(&(&right.0, right.1)));
    found
        .into_iter()
        .map(|(database, table, key)| (database, table.to_owned(), key))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_datatype::FieldTypeCode;

    fn restore_delete(stmt: tidb_ast::DeleteStmt) -> String {
        tidb_ast::Stmt::Dml(tidb_ast::NodeBox::new(tidb_ast::DmlStmt::Delete(Box::new(stmt)))).restore()
    }

    fn restore_update(stmt: tidb_ast::UpdateStmt) -> String {
        tidb_ast::Stmt::Dml(tidb_ast::NodeBox::new(tidb_ast::DmlStmt::Update(Box::new(stmt)))).restore()
    }

    fn varchar() -> FieldType {
        let mut field_type = FieldType::new(FieldTypeCode::Varchar);
        field_type.set_charset_name("utf8mb4");
        field_type.set_collation_name("utf8mb4_bin");
        field_type
    }

    /// Go `TestForeignKeyGenerateCascadeAST`'s statements, restored by
    /// `format.DefaultRestoreFlags` (captured from Go).
    #[test]
    fn gen_cascade_asts_restore_as_go() {
        let fk_values = || {
            vec![
                vec![Datum::Int(1), Datum::new_string_datum("a")],
                vec![Datum::Int(2), Datum::new_string_datum("b")],
            ]
        };
        let cols = vec![
            ("a".to_owned(), FieldType::new(FieldTypeCode::LongLong)),
            ("name".to_owned(), varchar()),
        ];
        assert_eq!(
            restore_delete(gen_cascade_delete_ast("test", "t2", None, &cols, fk_values())),
            "DELETE FROM `test`.`t2` WHERE ROW(`a`,`name`) IN (ROW(1,_UTF8MB4'a'),ROW(2,_UTF8MB4'b'))"
        );
        assert_eq!(
            restore_delete(gen_cascade_delete_ast("test", "t2", Some("idx"), &cols, fk_values())),
            "DELETE FROM `test`.`t2` USE INDEX (`idx`) WHERE ROW(`a`,`name`) IN (ROW(1,_UTF8MB4'a'),ROW(2,_UTF8MB4'b'))"
        );
        assert_eq!(
            restore_update(gen_cascade_set_null_ast("test", "t2", Some("idx"), &cols, fk_values())),
            "UPDATE `test`.`t2` USE INDEX (`idx`) SET `a`=NULL, `name`=NULL WHERE ROW(`a`,`name`) IN (ROW(1,_UTF8MB4'a'),ROW(2,_UTF8MB4'b'))"
        );
        let couple = UpdatedValuesCouple {
            new_values: vec![Datum::Int(10), Datum::new_string_datum("aa")],
            old_values_list: fk_values(),
        };
        assert_eq!(
            restore_update(gen_cascade_update_ast("test", "t2", Some("idx"), &cols, couple)),
            "UPDATE `test`.`t2` USE INDEX (`idx`) SET `a`=10, `name`=_UTF8MB4'aa' WHERE ROW(`a`,`name`) IN (ROW(1,_UTF8MB4'a'),ROW(2,_UTF8MB4'b'))"
        );
        let cols = vec![("a".to_owned(), FieldType::new(FieldTypeCode::LongLong))];
        assert_eq!(
            restore_delete(gen_cascade_delete_ast(
                "test",
                "t2",
                Some("idx"),
                &cols,
                vec![vec![Datum::Int(1)], vec![Datum::Int(2)]],
            )),
            "DELETE FROM `test`.`t2` USE INDEX (`idx`) WHERE `a` IN (1,2)"
        );
    }

    /// Go `FKCheckRuntimeStats.String` / `FKCascadeRuntimeStats.String`.
    #[test]
    fn fk_runtime_stats_strings_follow_go() {
        use tidb_execdetails::RuntimeStats as _;
        let check = FKCheckRuntimeStats {
            total: Duration::from_micros(37),
            check: Duration::from_nanos(11_800),
            lock: Duration::from_nanos(25_100),
            keys: 3,
        };
        assert_eq!(check.string(), "total:37µs, check:11.8µs, lock:25.1µs, foreign_keys:3");
        assert_eq!(FKCheckRuntimeStats::default().string(), "total:0s");
        let mut merged = FKCheckRuntimeStats {
            keys: 3,
            ..FKCheckRuntimeStats::default()
        };
        merged.merge(&FKCheckRuntimeStats::default());
        assert_eq!(merged.string(), "total:0s, foreign_keys:3");
        let cascade = FKCascadeRuntimeStats {
            total: Duration::from_nanos(130_700),
            keys: 4,
        };
        assert_eq!(cascade.string(), "total:130.7µs, foreign_keys:4");
        assert_eq!(FKCascadeRuntimeStats::default().string(), "total:0s");
    }
}
