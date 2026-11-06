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

//! Multi-table `UPDATE` and `DELETE`.
//!
//! Go `executor.UpdateExec` (its `tblColPosInfos` path) and
//! `executor.DeleteExec.deleteMultiTablesByChunk` both read ONE joined row
//! stream and write back to several base tables, so each output row has to
//! carry the identity of the base row every table contributed. Go gets that
//! from `TblColPosInfo.HandleCols`, a handle column the planner adds to the
//! join's schema per target table. The shared physical child carries those
//! identities through every read operator. At the write boundary [`SourceRow`]
//! separates them from values. Read-only virtual inputs contribute no handle.
//!
//! The rules below were captured from a real TiDB session (`mockstore`
//! `session.Execute`, reading affected rows off `StmtCtx`), not inferred:
//!
//! * **UPDATE-ONCE.** A base row reachable through several join paths is
//!   written once: `UPDATE a JOIN b ON a.id = b.aid SET a.x = a.x + 1` with
//!   two `b` rows per `a` row leaves `a.x` at `x+1`, not `x+2`. Go keys
//!   `updatedRowKeys` by (target position, handle) and skips a repeat --
//!   but only when the FIRST visit actually CHANGED the row, which is why
//!   [`UpdateOnce`] remembers a bool rather than mere presence. Current Go
//!   disables this skip for partitioned heaps, where physical rows can share
//!   the same bare handle; `UpdateExec.prepare` owns that exception.
//! * **Assignments read the ORIGINAL row, across tables.**
//!   `UPDATE s1, s2 SET s1.x = s2.y, s2.y = s1.x` swaps the two values: both
//!   right-hand sides see the joined row as the statement found it. This is
//!   the single-table `SET` rule (`compute_updated_row`) widened to the whole
//!   join, so it is one reading, not two.
//! * **Affected rows are CHANGED rows, summed over target tables.**
//!   `SET q1.v = q1.v, q2.v = 5` reports 1, not 2. A join matching nothing
//!   reports 0.
//! * **An outer join's NULL-padded side is not written.** Go's
//!   `unmatchedOuterRow` reads the handle column and skips a NULL one;
//!   `UPDATE y1 LEFT JOIN y2 ON ... SET y2.v = 9` touches only the rows `y2`
//!   really had.
//! * **The same physical table joined under two aliases is two targets.**
//!   `UPDATE z1 AS p JOIN z1 AS q ON p.id = q.id SET p.v = p.v + 1,
//!   q.v = q.v + 10` reports 4 for a two-row table and stores `q`'s value:
//!   the update-once key is the target POSITION, so each alias writes the
//!   row once. Their assigned columns merge by base-table/row identity;
//!   a later assignment to the same column wins.
//! * **DELETE dedups by physical TABLE, not by position.** Go's `tblRowMap`
//!   is keyed by `TblID`, so `DELETE t1, t1 FROM ...` removes each row once
//!   and reports 1. A target row reachable through several join paths is
//!   likewise deleted once.
//! * **A DELETE target is named by its ALIAS when it has one.**
//!   `DELETE x FROM f1 AS x JOIN f2` works; `DELETE f1 FROM f1 AS x JOIN f2`
//!   is Go's `ERROR 1109 Unknown table 'f1' in MULTI DELETE`, as is naming a
//!   table the `FROM` never mentions. A schema-qualified `DELETE test.t1
//!   FROM t1 ...` resolves when the table is not aliased. `DELETE a FROM ...`
//!   and `DELETE FROM a USING ...` are the same statement.
//! * **`ORDER BY`/`LIMIT`.** The parser already carries this: a multi-table
//!   `DELETE` rejects both with a syntax error (Go errno 1064), and so does
//!   a COMMA-joined `UPDATE` (Go errno 1221, "Incorrect usage of UPDATE and
//!   LIMIT"). An explicitly `JOIN`ed `UPDATE` accepts them, and the `LIMIT`
//!   caps the JOINED ROWS the statement reaches -- the same "rows reached,
//!   not rows changed" reading the single-table path already has.
//!
//! * **A derived table is a READ source, never a target.** Go builds the
//!   whole `FROM` (`buildResultSetNode`) before it decides what is writable,
//!   and decides that separately: `updatableTableListResolver.Leave` adds a
//!   `TableSource` to the updatable list only when
//!   `v.Source.(*ast.TableName)` succeeds, so a subquery source is simply
//!   absent from it. `buildUpdateLists` then turns that absence into an
//!   error at the `SET` column -- "1: update (select * from t1) t1 set b =
//!   1111111 ----- (no updatable table here) ... subQuery is not counted as
//!   updatable table", `ErrNonUpdatableTable` (1288). `DELETE` reaches the
//!   same place through `collectTableName`, whose `canUpdate` is likewise
//!   the `*ast.TableName` type assertion. So `UPDATE t t1, (SELECT ...) t2
//!   SET t1.b = t2.b` WRITES, and only a `SET`/`DELETE` target naming `t2`
//!   is refused.
//!
//! A `NATURAL`/`USING` join uses the same equality and naming rules as the
//! query path, while retaining both physical columns and their row identities
//! for the write phase. Go likewise restores the full child schema for
//! `UPDATE`/`DELETE` after building the coalesced join condition.
//!
//! `LATERAL` derived sources use the shared physical Apply executor. The derived side remains read-only because it has no base
//! row identity.

use std::collections::BTreeMap;

use super::dml::update_record::UpdateRecords;
use super::*;
use tidb_table::tables::TableHandle;
use tidb_hack::GoCaseCompare;
use tidb_hack::GoToLower;

/// What the executor needs of one updated base table beside the planner's
/// layout: its columns, default metadata and write policy.
struct UpdatedTable {
    columns: Vec<(String, FieldType)>,
    default_meta: Vec<super::dml::ColumnDefaultMeta>,
    /// Generated offsets in the complete writable row.
    generated_columns: Vec<usize>,
    /// Go `UpdateExec.prepare` keeps partitioned heap rows eligible even when
    /// another physical row has the same bare handle.
    skip_repeated_updates: bool,
    selected_partitions: Option<Vec<i64>>,
}

/// Go's `PARTITION (...)` selection on one reference: the names are resolved
/// once, against the table, and an unknown one is `ErrUnknownPartition`; a
/// clause on a table without partitions is its own error.
fn selected_partition_ids(
    partitions: &[String],
    entry: &TableEntry,
    name: &str,
) -> Result<Option<Vec<i64>>, DriverError> {
    let Some(first) = partitions.first() else {
        return Ok(None);
    };
    let TableEntry::Kv(kv) = entry else {
        return Err(DriverError::from(
            tidb_table::table_errors::ERR_UNKNOWN_PARTITION
                .gen_with_stack_by_args(&[first.clone().into(), name.to_owned().into()]),
        ));
    };
    let Some(_) = kv.partition() else {
        return Err(DriverError::from(
            tidb_plannererrors::ERR_PARTITION_CLAUSE_ON_NONPARTITIONED
                .gen_with_stack_by_args(&[]),
        ));
    };
    tidb_planner_rule::ids_for_selected_partitions(
                    &kv.meta().read().get_partition_info().expect("partitioned").read(),
                    partitions)
        .map(Some)
        .map_err(|partition| {
            DriverError::from(
                tidb_table::table_errors::ERR_UNKNOWN_PARTITION
                    .gen_with_stack_by_args(&[partition.into(), name.to_owned().into()]),
            )
        })
}

/// Runs a multi-table `UPDATE`, returning MySQL's affected-row count. The
/// read, its row layout and every assignment's expression are the planner's
/// (Go `buildUpdate`).
#[allow(clippy::too_many_arguments)]
pub(crate) fn run_multi_update(
    update: &tidb_ast::UpdateStmt,
    catalog: &mut SessionExtendedInfoSchema,
    ctx: &crate::StmtContext,
    physical_plan: Option<&mut tidb_planner_core_operator_physicalop::PhysicalPlan>,
    layout: (
        &[tidb_planner_core_operator_physicalop::UpdateTablePos],
        &[tidb_planner_core_operator_physicalop::UpdateAssign],
    ),
    expressions: &[Option<Expression>],
    fk_triggers: &[tidb_planner_core_operator_physicalop::FkTriggerNode],
    dml: &super::physical_builder::DmlExec,
    runtime: Option<&mut super::physical_builder::PhysicalRuntimeStats>,
) -> Result<u64, DriverError> {
    let (tables, assigns) = layout;
    if tables.is_empty() {
        return Err(DriverError::unsupported(
            "a multi-table UPDATE plan carries no table layout",
        ));
    }
    let collect_stats = super::dml::CollectUpdateStats::install(ctx, dml.update_stats());
    let plan = physical_plan
        .ok_or_else(|| DriverError::unsupported("a multi-table write has no read plan"))?;
    let mut updated = Vec::with_capacity(tables.len());
    for table in tables {
        let entry = catalog
            .get_in(&table.database, &table.table)
            .ok_or_else(|| {
                DriverError::from(
                    tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                        .gen_with_stack_by_args(&[
                            (&table.database).into(),
                            (&table.table).into(),
                        ]),
                )
            })?;
        let TableEntry::Kv(kv) = entry else {
            return Err(DriverError::unsupported(
                "a multi-table UPDATE target is not a base table",
            ));
        };
        let heap = matches!(table.handle, tidb_planner_core_operator_physicalop::DeleteHandle::Extra(_));
        // The planner's assignment offsets and the composed rows address
        // the stored row, `TableInfo.Columns` with any hidden column.
        updated.push(UpdatedTable {
            columns: kv
                .columns()
                .iter()
                .map(|column| (column.read().name.original().to_owned(), column.read().field_type.clone()))
                .collect(),
            default_meta: super::dml::stored_column_metadata(entry),
            generated_columns: kv
                .columns()
                .iter()
                .enumerate()
                .filter_map(|(offset, column)| column.generated.is_some().then_some(offset))
                .collect(),
            skip_repeated_updates: !heap || kv.partition().is_none(),
            selected_partitions: selected_partition_ids(
                &table.partitions,
                entry,
                &table.table,
            )?,
        });
    }
    check_update_list(tables, assigns, &updated, catalog)?;
    let on_update_now: Vec<super::dml::PreparedOnUpdateNow> = updated
        .iter()
        .enumerate()
        .map(|(slot, table)| {
            super::dml::PreparedOnUpdateNow::new(
                &table.default_meta,
                assigns
                    .iter()
                    .filter(move |assign| assign.table == slot)
                    .map(|assign| assign.column),
            )
        })
        .collect::<Result<_, _>>()?;
    let field_types: Vec<FieldType> = plan
        .schema()
        .ok_or_else(|| DriverError::unsupported("a multi-table read has no schema"))?
        .columns
        .iter()
        .map(|column| {
            column
                .ret_type
                .clone()
                .ok_or_else(|| DriverError::unsupported("a multi-table read column has no type"))
        })
        .collect::<Result<_, _>>()?;
    let delete_layout: Vec<tidb_planner_core_operator_physicalop::DeleteTablePos> = tables
        .iter()
        .map(|table| tidb_planner_core_operator_physicalop::DeleteTablePos {
            table_id: table.table_id,
            database: table.database.clone(),
            table: table.table.clone(),
            stored_width: table.stored_width,
            columns: table.columns.clone(),
            handle: table.handle.clone(),
        })
        .collect();
    // Go keeps updatedRowKeys per target position, but mergedRowData per
    // base table and handle. Neither map can stand in for the other.
    let targets: Vec<Option<(String, String)>> = tables
        .iter()
        .enumerate()
        .map(|(slot, table)| {
            assigns
                .iter()
                .any(|assign| assign.table == slot && assign.column < table.stored_width)
                .then(|| (table.database.go_to_lower(), table.table.go_to_lower()))
        })
        .collect();
    let multiple: Vec<_> = targets
        .iter()
        .map(|target| {
            target.as_ref().is_some_and(|target| {
                targets
                    .iter()
                    .flatten()
                    .filter(|other| *other == target)
                    .count()
                    > 1
            })
        })
        .collect();
    // Go `UpdateExec.virtualAssignmentsOffset < len(OrderedList)`: the
    // statement appended generated-column assignments (`buildUpdateLists`),
    // so the generated columns merge too (`mergeGenerated`).
    let has_virtual_assignments = tables.iter().enumerate().any(|(slot, table)| {
        let assigned: Vec<String> = assigns
            .iter()
            .filter(|assign| assign.table == slot)
            .filter_map(|assign| updated[slot].columns.get(assign.column).map(|(name, _)| name.clone()))
            .collect();
        match catalog.get_in(&table.database, &table.table) {
            Some(TableEntry::Kv(kv)) => {
                let infos: Vec<_> = kv.columns().iter().map(|column| column.info.clone()).collect();
                !assigned.is_empty()
                    && !tidb_planner::plan_builder::generated_assignments::update_list_generated_columns(
                        &infos, &assigned,
                    )
                    .is_empty()
            }
            _ => false,
        }
    });
    // Go `UpdateExec.mergedRowData`, per base table.
    let mut merged: BTreeMap<(String, String), tidb_txnkv::MemAwareHandleMap<Vec<Datum>>> =
        BTreeMap::new();
    // Go `UpdateExec.updatedRowKeys`, per target position.
    let mut updated_row_keys: Vec<tidb_txnkv::MemAwareHandleMap<bool>> =
        tables.iter().map(|_| tidb_txnkv::MemAwareHandleMap::new()).collect();
    let mut records = UpdateRecords::new(fk_triggers);
    let mut matched_rows = 0u64;
    let mut touched_rows = 0u64;
    let mut changed_rows = 0u64;
    let split = delete_source_splitter(&delete_layout, catalog, ctx, true)?;
    let mut row_index = 0usize;
    // Go `UpdateExec.updateRows`: each row is composed and written before the
    // child is asked for its next chunk.
    let collected = super::physical_builder::execute_dml_source_writing(
        plan,
        catalog,
        ctx,
        dml,
        runtime.is_some(),
        |catalog, row| {
            let row = split(row)?;
        let ids = &row.ids;
        let chunk = row_chunk(&row.raw, &field_types)?;
        let evaluated = assigns
            .iter()
            .map(|assign| {
                let table = &tables[assign.table];
                let extra = assign.column == table.stored_width;
                if !extra && ids[assign.table].is_none() {
                    return Ok(None);
                }
                let expression = expressions
                    .get(assign.expr)
                    .and_then(Option::as_ref)
                    .ok_or_else(|| DriverError::unsupported("an UPDATE assignment was not planned"))?;
                let value = expression
                    .eval(ctx, chunk.get_row(0))
                    .map_err(|error| DriverError::Exec(ExecError::Eval(error)))?;
                if extra {
                    return Ok(None);
                }
                let columns = &updated[assign.table].columns;
                handle_update_error(
                    value,
                    &columns[assign.column].1,
                    &columns[assign.column].0,
                    row_index,
                    ctx,
                )
                .map(Some)
                .map_err(DriverError::from)
            })
            .collect::<Result<Vec<_>, DriverError>>()?;
        let mut prepared = Vec::with_capacity(tables.len());
        // Compose every target from the same input row before merging aliases.
        for (slot, table) in updated.iter().enumerate() {
            let Some(id) = ids[slot].as_ref().filter(|_| targets[slot].is_some()) else {
                prepared.push(None);
                continue;
            };
            if table.skip_repeated_updates
                && updated_row_keys[slot].get(&super::dml::kv_handle(id)?) == Some(&true)
            {
                prepared.push(None);
                continue;
            }
            let mut old = row.stored[slot].clone();
            let mut new = old.clone();
            for (assign, value) in assigns.iter().zip(&evaluated) {
                if assign.table == slot {
                    if let Some(value) = value {
                        new[assign.column] = value.clone();
                    }
                }
            }
            if multiple[slot] {
                // Go `mergeNonGenerated`.
                let handle = super::dml::kv_handle(id)?;
                let rows = merged
                    .entry(targets[slot].clone().expect("an assigned base table"))
                    .or_default();
                let merged_data = match rows.get(&handle) {
                    Some(previous) => {
                        let mut previous = previous.clone();
                        for (column, meta) in table.default_meta.iter().enumerate() {
                            if meta.generated {
                                continue;
                            }
                            old[column] = previous[column].clone();
                            if assigns
                                .iter()
                                .any(|a| a.table == slot && a.column == column)
                            {
                                previous[column] = new[column].clone();
                            } else {
                                new[column] = previous[column].clone();
                            }
                        }
                        previous
                    }
                    None => new.clone(),
                };
                let mut mem_delta = tidb_datatype::estimated_mem_usage(&merged_data, 1)
                    + i64::try_from(handle.extra_mem_size()).unwrap_or(i64::MAX);
                mem_delta += rows.set(handle, merged_data);
                dml.mem_tracker
                    .consume_checked(mem_delta)
                    .map_err(DriverError::from)?;
            }
            prepared.push(Some((old, new)));
        }
        for (slot, prepared) in prepared.iter_mut().enumerate() {
            let Some((old, new)) = prepared else { continue };
            let table = &updated[slot];
            let id = ids[slot].as_ref().expect("prepared target has an identity");
            let (database, name) = targets[slot]
                .as_ref()
                .expect("prepared target is a base table");
            let handle = super::dml::kv_handle(id)?;
            let merge_key = (database.clone(), name.clone());
            // Go `mergeGenerated(..., beforeEval=true)`: it sets the merged
            // row back unchanged, charging its estimate and the handle's.
            if multiple[slot] && has_virtual_assignments {
                let previous = merged[&merge_key]
                    .get(&handle)
                    .cloned()
                    .expect("non-generated merge ran first");
                for &column in &table.generated_columns {
                    old[column] = previous[column].clone();
                    new[column] = previous[column].clone();
                }
                dml.mem_tracker
                    .consume_checked(
                        tidb_datatype::estimated_mem_usage(&previous, 1)
                            + i64::try_from(handle.extra_mem_size()).unwrap_or(i64::MAX),
                    )
                    .map_err(DriverError::from)?;
            }
            if updated_row_keys[slot].get(&handle).is_none() {
                matched_rows += 1;
            }
            on_update_now[slot].apply(old, new, ctx, chunk.get_row(0))?;
            collect_stats.before_write();
            let outcome = records.write(
                catalog,
                database,
                name,
                id,
                old,
                new,
                table.selected_partitions.as_deref(),
                update.ignore,
                super::GeneratedWrite::Update { row_index },
                &assigns
                    .iter()
                    .filter(|assign| assign.table == slot)
                    .map(|assign| assign.column)
                    .collect::<Vec<_>>(),
                ctx,
            )?;
            // Go `mergeGenerated(..., beforeEval=false)`.
            if multiple[slot] && has_virtual_assignments {
                let rows = merged.get_mut(&merge_key).expect("non-generated merge ran first");
                let mut previous = rows.get(&handle).cloned().expect("non-generated merge ran first");
                for &column in &table.generated_columns {
                    previous[column] = new[column].clone();
                }
                let mut mem_delta = tidb_datatype::estimated_mem_usage(&previous, 1)
                    + i64::try_from(handle.extra_mem_size()).unwrap_or(i64::MAX);
                mem_delta += rows.set(handle.clone(), previous);
                dml.mem_tracker
                    .consume_checked(mem_delta)
                    .map_err(DriverError::from)?;
            }
            changed_rows += u64::from(outcome.changed());
            touched_rows += u64::from(outcome.touched());
            if !outcome.ignored() {
                let rows = &mut updated_row_keys[slot];
                let exist = rows.get(&handle).is_some();
                let extra = i64::try_from(handle.extra_mem_size()).unwrap_or(i64::MAX);
                let mut mem_delta = rows.set(handle, outcome.changed());
                if !exist {
                    mem_delta += extra;
                }
                dml.mem_tracker
                    .consume_checked(mem_delta)
                    .map_err(DriverError::from)?;
            }
        }
            row_index += 1;
            Ok(())
        },
    )?;
    if let Some(runtime) = runtime {
        runtime.extend(collected);
    }
    records.finish(catalog, ctx)?;
    ctx.set_message(format!(
        "Rows matched: {matched_rows}  Changed: {changed_rows}  Warnings: {}",
        ctx.warning_count()
    ));
    Ok(if ctx.client_found_rows() {
        touched_rows
    } else {
        changed_rows
    })
}

/// Go `CheckUpdateList`: one physical table updated through two aliases may
/// change neither its primary key nor its partition columns through either
/// of them (MySQL's `ER_MULTI_UPDATE_KEY_CONFLICT`). Aliases are visited in
/// join order, and the error names the first alias and the conflicting one.
fn check_update_list(
    tables: &[tidb_planner_core_operator_physicalop::UpdateTablePos],
    assigns: &[tidb_planner_core_operator_physicalop::UpdateAssign],
    updated: &[UpdatedTable],
    catalog: &SessionExtendedInfoSchema,
) -> Result<(), DriverError> {
    struct UpdatedAlias<'a> {
        name: &'a str,
        pk_updated: bool,
        partition_col_updated: bool,
    }
    let mut updated_from_other_alias: BTreeMap<(String, String), UpdatedAlias<'_>> =
        BTreeMap::new();
    for (slot, table) in tables.iter().enumerate() {
        let mut columns = assigns
            .iter()
            .filter(|assign| assign.table == slot)
            .filter_map(|assign| updated[slot].columns.get(assign.column))
            .peekable();
        if columns.peek().is_none() {
            continue;
        }
        let partition_columns = match catalog.table_in(&table.database, &table.table) {
            Some(TableEntry::Kv(kv)) => kv
                .partition()
                .map(|spec| spec.dependencies.clone())
                .unwrap_or_default(),
            _ => Vec::new(),
        };
        let (mut pk_updated, mut partition_col_updated) = (false, false);
        for (column, field_type) in columns {
            pk_updated |= field_type.has_flag(tidb_datatype::FieldTypeFlags::PRI_KEY);
            partition_col_updated |= partition_columns
                .iter()
                .any(|partition_column| partition_column.go_lower_eq(column));
        }
        let key = (
            tidb_hack::go_to_lower(&table.database),
            tidb_hack::go_to_lower(&table.table),
        );
        match updated_from_other_alias.get(&key) {
            Some(other) => {
                if other.pk_updated
                    || pk_updated
                    || other.partition_col_updated
                    || partition_col_updated
                {
                    return Err(DriverError::Mysql(MysqlError::from_terror(
                        &tidb_plannererrors::ERR_MULTI_UPDATE_KEY_CONFLICT
                            .gen_with_stack_by_args(&[
                                other.name.clone().into(),
                                table.visible.clone().into(),
                            ]),
                    )));
                }
            }
            None => {
                updated_from_other_alias.insert(
                    key,
                    UpdatedAlias {
                        name: &table.visible,
                        pk_updated,
                        partition_col_updated,
                    },
                );
            }
        }
    }
    Ok(())
}

/// Runs a multi-table `DELETE`, returning the number of removed rows.
pub(crate) fn run_multi_delete(
    delete: &tidb_ast::DeleteStmt,
    catalog: &mut SessionExtendedInfoSchema,
    ctx: &crate::StmtContext,
    physical_plan: Option<&mut tidb_planner_core_operator_physicalop::PhysicalPlan>,
    tables: &[tidb_planner_core_operator_physicalop::DeleteTablePos],
    fk_triggers: &[tidb_planner_core_operator_physicalop::FkTriggerNode],
    dml: &super::physical_builder::DmlExec,
    runtime: Option<&mut super::physical_builder::PhysicalRuntimeStats>,
) -> Result<u64, DriverError> {
    if tables.is_empty() {
        return Err(DriverError::unsupported(
            "a multi-table DELETE plan carries no table layout",
        ));
    }
    // Go `deleteMultiTablesByChunk`'s `tblRowMap`: each target table's rows
    // by handle. It is keyed by TABLE, so a row reachable through several
    // join paths -- or named twice in the target list -- is removed once,
    // and two aliases of one table are still one table here (unlike UPDATE,
    // whose key is the target position).
    let mut tbl_row_map: BTreeMap<(String, String), tidb_txnkv::MemAwareHandleMap<HandleInfoPair>> =
        BTreeMap::new();
    {
        let plan = physical_plan
            .ok_or_else(|| DriverError::unsupported("a multi-table write has no read plan"))?;
        dml.next(|| {
            visit_delete_source_rows(tables, plan, catalog, ctx, runtime, dml, |row, joined_row_mem| {
                // Go `composeTblRowMap`.
                let mut total_mem_delta = 0;
                for (table, (id, stored)) in tables.iter().zip(row.ids.into_iter().zip(row.stored)) {
                    // Go `unmatchedOuterRow`.
                    let Some(id) = id else {
                        continue;
                    };
                    let handle = super::dml::kv_handle(&id)?;
                    let rows = tbl_row_map
                        .entry((table.database.clone(), table.table.clone()))
                        .or_default();
                    let exist = rows.get(&handle).is_some();
                    let extra = i64::try_from(handle.extra_mem_size()).unwrap_or(i64::MAX);
                    let mut mem_delta = rows.set(handle, HandleInfoPair { id, stored });
                    if !exist {
                        mem_delta += joined_row_mem + extra;
                    }
                    total_mem_delta += mem_delta;
                }
                dml.mem_tracker
                    .consume_checked(total_mem_delta)
                    .map_err(DriverError::from)
            })
        })?;
    }

    let mut records = super::dml::delete_record::DeleteRecords::new(fk_triggers);
    let mut deleted = 0;
    // The rest of Go `DeleteExec.Next`: `removeRowsInTblRowMap`, after the
    // whole read. Go ranges its maps in no defined order; these go by table,
    // then handle.
    dml.next(|| -> Result<(), DriverError> {
        for ((database, name), rows) in &tbl_row_map {
            let mut doomed = Vec::new();
            rows.range(|_, pair| {
                doomed.push((pair.id.clone(), pair.stored.clone()));
                true
            });
            doomed.sort_by(|left, right| left.0.cmp(&right.0));
            for (id, old) in doomed {
                deleted += u64::from(records.write(catalog, database, name, &id, &old, delete.ignore, ctx)?);
            }
        }
        Ok(())
    })?;
    records.finish(catalog, ctx)?;

    Ok(deleted)
}

/// Go `handleInfoPair`: a `tblRowMap` row -- its table's columns of the
/// joined row (`handleVal`) -- here with the identity it is removed under.
struct HandleInfoPair {
    id: TableHandle,
    stored: Vec<Datum>,
}

impl tidb_hack::MapValueLayout for HandleInfoPair {
    // Go `struct{ handleVal []types.Datum; posInfo *TblColPosInfo }`.
    const SOURCE_SIZE: usize = 32;
    const SOURCE_ALIGN: usize = 8;
}

/// A row of a multi-table DELETE's read, split by Go's `TblColPosInfos`: each
/// deleted table's row identity (`None` where an outer join NULL-padded that
/// side) and its stored columns, NULL where the read pruned one.
struct DeleteSourceRow {
    ids: Vec<Option<TableHandle>>,
    stored: Vec<Vec<Datum>>,
    /// The read's row as the planner's expressions see it; kept only when
    /// the caller evaluates assignments over it (a multi-table UPDATE).
    raw: Vec<Datum>,
}

/// Splits a multi-table write's read row by the table layouts the planner
/// decided (`logical_plan_builder.go:6200`): each target's row identity and
/// stored columns, with the joined row itself when `keep_raw` (an UPDATE
/// evaluates its assignments over it).
fn delete_source_splitter<'a>(
    tables: &'a [tidb_planner_core_operator_physicalop::DeleteTablePos],
    catalog: &SessionExtendedInfoSchema,
    ctx: &crate::StmtContext,
    keep_raw: bool,
) -> Result<impl Fn(Vec<Datum>) -> Result<DeleteSourceRow, DriverError> + 'a, DriverError> {
    use tidb_planner_core_operator_physicalop::DeleteHandle;
    let kv_tables = tables
        .iter()
        .map(|table| match catalog.get_in(&table.database, &table.table) {
            Some(TableEntry::Kv(kv)) => Ok(kv.clone()),
            _ => Err(DriverError::unsupported("a multi-table DELETE target is not a base table")),
        })
        .collect::<Result<Vec<_>, _>>()?;
    let zone = ctx.session_zone();
    let integer = |datum: &Datum| match datum {
        Datum::Int(value) => Some(TableHandle::Int(*value)),
        Datum::UInt(value) => Some(TableHandle::Int(*value as i64)),
        _ => None,
    };
    Ok(move |row: Vec<Datum>| {
        let mut ids = Vec::with_capacity(tables.len());
        let mut stored = Vec::with_capacity(tables.len());
        for (table, kv) in tables.iter().zip(&kv_tables) {
            let cell = |position: usize| {
                row.get(position).ok_or_else(|| {
                    DriverError::unsupported(format!(
                        "a multi-table read returned {} columns, expected position {position}",
                        row.len()
                    ))
                })
            };
            let id = match &table.handle {
                DeleteHandle::Int(position) | DeleteHandle::Extra(position) => {
                    integer(cell(*position)?)
                }
                DeleteHandle::Common(positions) => {
                    let values = positions
                        .iter()
                        .map(|position| cell(*position).cloned())
                        .collect::<Result<Vec<Datum>, _>>()?;
                    if values.iter().any(|value| matches!(value, Datum::Null)) {
                        None
                    } else {
                        Some(
                            kv.common_handle_of_values(&values, &zone)
                                .map_err(kv_write_error)?,
                        )
                    }
                }
            };
            ids.push(id);
            let mut part = vec![Datum::Null; table.stored_width];
            for (position, offset) in &table.columns {
                part[*offset] = cell(*position)?.clone();
            }
            stored.push(part);
        }
        let raw = if keep_raw { row } else { Vec::new() };
        Ok(DeleteSourceRow { ids, stored, raw })
    })
}

/// Go `DeleteExec.deleteMultiTablesByChunk`'s read: hands `visit` each row
/// of the planned read, split ([`delete_source_splitter`]), with the joined
/// row's `types.EstimatedMemUsage(joinedRow, 1)`; each chunk is charged to
/// the executor's tracker while it is read.
fn visit_delete_source_rows(
    tables: &[tidb_planner_core_operator_physicalop::DeleteTablePos],
    plan: &mut tidb_planner_core_operator_physicalop::PhysicalPlan,
    catalog: &SessionExtendedInfoSchema,
    ctx: &crate::StmtContext,
    runtime: Option<&mut super::physical_builder::PhysicalRuntimeStats>,
    dml: &super::physical_builder::DmlExec,
    mut visit: impl FnMut(DeleteSourceRow, i64) -> Result<(), DriverError>,
) -> Result<(), DriverError> {
    let split = delete_source_splitter(tables, catalog, ctx, false)?;
    let collected = super::physical_builder::execute_dml_source(
        plan,
        catalog,
        ctx,
        runtime.is_some(),
        Some(dml),
        true,
        |row| {
            let joined_row_mem = tidb_datatype::estimated_mem_usage(&row, 1);
            visit(split(row)?, joined_row_mem)
        },
    )?;
    if let Some(runtime) = runtime {
        runtime.extend(collected);
    }
    Ok(())
}

/// Go `buildUpdate`/`buildDelete`'s read: the `FROM` join with the `WHERE`,
/// `ORDER BY` and `LIMIT` above it, as one SELECT for the planner, carrying
/// the statement's `WITH` clause and hints (`pushTableHints(stmt.TableHints,
/// 0)`).
pub(crate) fn multi_dml_select(
    with: Option<&tidb_ast::WithClause>,
    hints: &[tidb_ast::Hint],
    from: &tidb_ast::Join,
    where_clause: Option<&tidb_ast::Expr>,
    order_by: &[tidb_ast::OrderItem],
    limit: Option<&tidb_ast::Limit>,
) -> tidb_ast::QueryStmt {
    tidb_ast::QueryStmt::Select(Box::new(tidb_planner::plan_builder::dml_source::dml_source_select(
        with,
        hints,
        from.clone(),
        where_clause,
        order_by,
        limit,
    )))
}

/// Plan all writable targets before executing any source. Aliases of one
/// physical table contribute to one FK policy, as Go's tblID2UpdateColumns
/// and per-table FK maps do.
pub(crate) fn multi_dml_physical_plan(
    dml: MultiDmlRef<'_>,
    catalog: &SessionExtendedInfoSchema,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<tidb_planner_core_operator_physicalop::PhysicalPlan, DriverError> {
    use super::fk_trigger_plan::FkPlanSpec;
    let mut specs: BTreeMap<(i64, String, String), FkPlanSpec> = BTreeMap::new();
    let mut add_target = |database: String, table: String, column: Option<String>| {
        let identity = match catalog.get_in(&database, &table) {
            Some(TableEntry::Kv(table)) => (table.table_id(), String::new(), String::new()),
            _ => (0, database.go_to_lower(), table.go_to_lower()),
        };
        let spec = specs.entry(identity).or_insert_with(|| FkPlanSpec {
            database,
            table,
            ..FkPlanSpec::default()
        });
        if let Some(column) = column {
            if !spec
                .updated_cols
                .iter()
                .any(|existing| existing.go_lower_eq(&column))
            {
                spec.updated_cols.push(column);
            }
        }
    };
    let mut delete_multi = None;
    let mut multi_update = None;
    let (operator, select) = match dml {
        MultiDmlRef::Update(update) => {
            let tidb_ast::UpdateKind::Multi { from, .. } = &update.kind else {
                return Err(DriverError::unsupported(
                    "multi-table UPDATE plan requires a join",
                ));
            };
            let select = multi_dml_select(
                update.with.as_ref(),
                &update.hints,
                from,
                update.where_clause.as_ref(),
                &update.order_by,
                update.limit.as_ref(),
            );
            // Go's preprocessor (1248 for an unnamed derived table) runs
            // before the planner builds anything.
            super::set_opr::validate_query_usage(&select, ctx)?;
            for (database, table, column) in
                super::planner_bridge::update_target_columns(update, catalog, current_db, ctx)?
            {
                add_target(database, table, Some(column));
            }
            multi_update = Some(update);
            ("Update", select)
        }
        MultiDmlRef::Delete(delete) => {
            let tidb_ast::DeleteKind::Multi { from, .. } = &delete.kind else {
                return Err(DriverError::unsupported(
                    "multi-table DELETE plan requires a join",
                ));
            };
            let select = multi_dml_select(
                delete.with.as_ref(),
                &delete.hints,
                from,
                delete.where_clause.as_ref(),
                &[],
                None,
            );
            super::set_opr::validate_query_usage(&select, ctx)?;
            for target in super::planner_bridge::delete_targets(delete, catalog, current_db, ctx)
                .map_err(super::planner_error_to_driver)?
            {
                add_target(target.database, target.table, None);
            }
            delete_multi = Some(delete_multi_input(delete, catalog, current_db, ctx)?);
            ("Delete", select)
        }
    };
    let specs: Vec<_> = specs.into_values().collect();
    let keep = match delete_multi {
        Some(input) => super::dml::DmlKeep::MultiDelete(input),
        None => super::dml::DmlKeep::Auto,
    };
    super::dml::physical_multi_dml_plan(
        operator,
        &select,
        multi_update,
        catalog,
        current_db,
        ctx,
        &specs,
        keep,
    )
}

/// What the planner needs to prune a multi-table DELETE's read and lay out
/// its targets' rows (Go `pruneAndBuildColPositionInfoForDelete`): the
/// resolved target tables, and whether a foreign key references any of them,
/// which declines the pruning for every table.
fn delete_multi_input(
    delete: &tidb_ast::DeleteStmt,
    catalog: &SessionExtendedInfoSchema,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<tidb_planner::plan_builder::delete_pruning::DeleteMultiInput, DriverError> {
    use tidb_planner::plan_builder::delete_pruning::DeleteMultiInput;
    let targets = super::planner_bridge::delete_targets(delete, catalog, current_db, ctx)
        .map_err(super::planner_error_to_driver)?;
    let has_fk = ctx.foreign_key_checks()
        && targets.iter().any(|target| {
            !crate::foreign_key::referring(catalog, &target.database, &target.table).is_empty()
        });
    Ok(DeleteMultiInput { targets, has_fk })
}

/// The EXPLAIN plan of a multi-table `UPDATE`/`DELETE`: Go's `Update`/
/// `Delete` root over the optimized join `SelectPlan`. `None` for a
/// single-table statement, which keeps its own builder.
pub(crate) fn multi_dml_explain_plan(
    dml: MultiDmlRef<'_>,
    catalog: &SessionExtendedInfoSchema,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<Option<tidb_planner_core_operator_physicalop::PhysicalPlan>, DriverError> {
    match dml {
        MultiDmlRef::Update(update) => {
            let tidb_ast::UpdateKind::Multi { .. } = &update.kind else {
                return Ok(None);
            };
            multi_dml_physical_plan(MultiDmlRef::Update(update), catalog, current_db, ctx).map(Some)
        }
        MultiDmlRef::Delete(delete) => {
            let tidb_ast::DeleteKind::Multi { .. } = &delete.kind else {
                return Ok(None);
            };
            multi_dml_physical_plan(MultiDmlRef::Delete(delete), catalog, current_db, ctx).map(Some)
        }
    }
}

/// Which multi-table write [`multi_dml_explain_plan`] plans.
#[derive(Clone, Copy)]
pub(crate) enum MultiDmlRef<'a> {
    Update(&'a tidb_ast::UpdateStmt),
    Delete(&'a tidb_ast::DeleteStmt),
}
