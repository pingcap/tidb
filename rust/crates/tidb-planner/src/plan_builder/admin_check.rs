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

//! Go `planbuilder.go`'s `ADMIN CHECK TABLE` / `ADMIN CHECK INDEX` plan:
//! `buildAdminCheckTable`, `buildPhysicalIndexLookUpReaders` and
//! `buildPhysicalIndexLookUpReader` -- one full-range index lookup per
//! checked index (and partition), whose table scan names the handle's
//! offsets (`HandleIdx`) for the lookup's value comparison.

use std::collections::BTreeMap;

use tidb_datatype::{FieldType, FieldTypeCode, FieldTypeFlags};
use tidb_expr::column::Column;
use tidb_expr::schema::Schema;
use tidb_infoschema::table_source::SourceTable;
use tidb_model::{GoShared, IndexInfo, SchemaState, TableInfo};
use tidb_planner_core_base::PlanError;
use tidb_planner_core_operator_physicalop::task::{CopTask, Task};
use tidb_planner_core_operator_physicalop::{PhysicalIndexScan, PhysicalPlan, PhysicalTableScan};

use super::PlanBuilder;
use crate::resolve_indices::ResolveIndices;
use tidb_exprctx::Columns;
use tidb_infoschema::table_source::TableSource;

/// Go `plannercore.CheckTable`.
#[derive(Debug)]
pub struct CheckTable {
    /// Go `DBName`.
    pub db_name: String,
    /// Go `Table`'s metadata.
    pub table_info: GoShared<TableInfo>,
    /// Go `IndexInfos`: the checked indexes.
    pub index_infos: Vec<GoShared<IndexInfo>>,
    /// Go `IndexLookUpReaders`: one `PhysicalIndexLookUpReader` per checked
    /// index (and partition).
    pub index_look_up_readers: Vec<PhysicalPlan>,
    /// Go `CheckIndex`: `ADMIN CHECK INDEX`.
    pub check_index: bool,
}

impl<S: TableSource, C: Columns> PlanBuilder<'_, S, C> {
    /// Go `buildAdminCheckTable`.
    ///
    /// # Errors
    ///
    /// Go's: an absent table, an absent or non-public `ADMIN CHECK INDEX`
    /// index, or a reader that does not build.
    pub fn build_admin_check_table(
        &mut self,
        db_name: &str,
        table_name: &str,
        index: Option<&str>,
    ) -> Result<CheckTable, PlanError> {
        let Some(table) = self.source.find_table(db_name, table_name) else {
            return Err(PlanError::from(
                tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                    .fast_gen_by_args(&[db_name.into(), table_name.into()]),
            ));
        };
        let table = table.clone();
        let table_info = table
            .table_info
            .clone()
            .ok_or_else(|| PlanError::internal("an admin-checked table carries its TableInfo"))?;
        let indices: Vec<GoShared<IndexInfo>> = match index {
            Some(index_name) => {
                let idx_name = index_name.to_lowercase();
                let Some(idx) = table_info
                    .read()
                    .indices
                    .iter_deref()
                    .find(|index| index.read().name.lowercase() == idx_name)
                else {
                    return Err(PlanError::internal(format!("secondary index {index_name} does not exist")));
                };
                if idx.read().state != SchemaState::PUBLIC {
                    return Err(PlanError::internal(format!(
                        "index {index_name} state {} isn't public",
                        idx.read().state
                    )));
                }
                vec![idx]
            }
            None => table_info.read().indices.iter_deref().collect(),
        };
        let (index_look_up_readers, index_infos) =
            self.build_physical_index_look_up_readers(db_name, &table, &table_info, &indices)?;
        Ok(CheckTable {
            db_name: db_name.to_owned(),
            table_info,
            index_infos,
            index_look_up_readers,
            check_index: index.is_some(),
        })
    }

    /// Go `buildPhysicalIndexLookUpReaders`: the public indexes' readers (a
    /// clustered primary key is the record key and is skipped), one a
    /// partition for a partitioned table's local index; under `FOR UPDATE`
    /// or `READ-COMMITTED` reads only the indexes public in the latest schema.
    fn build_physical_index_look_up_readers(
        &mut self,
        db_name: &str,
        table: &SourceTable,
        table_info: &GoShared<TableInfo>,
        indices: &[GoShared<IndexInfo>],
    ) -> Result<(Vec<PhysicalPlan>, Vec<GoShared<IndexInfo>>), PlanError> {
        let tbl_info = table_info.read().clone();
        let mut index_infos = Vec::with_capacity(tbl_info.indices.len());
        let mut index_look_up_readers = Vec::with_capacity(tbl_info.indices.len());
        let mut check = (self.is_for_update_read || !self.index_lookup_push_down_session.repeatable_read)
            && self.ctx.connection_id().is_some_and(|id| id > 0);
        let mut latest_indexes: Option<BTreeMap<i64, GoShared<IndexInfo>>> = None;
        for idx in indices {
            let idx_info = idx.read().clone();
            if tbl_info.is_common_handle && idx_info.primary {
                // Skip checking clustered index.
                continue;
            }
            if idx_info.state != SchemaState::PUBLIC {
                continue;
            }
            if check && latest_indexes.is_none() {
                let (indexes, still_check) = tidb_planner_util_domainmisc::get_latest_index_info(
                    self.source.latest_index_schema(),
                    tbl_info.id,
                    self.source.schema_meta_version(),
                )
                .map_err(|error| PlanError::internal(error.to_string()))?;
                latest_indexes = Some(indexes.unwrap_or_default());
                check = still_check;
            }
            if check {
                let public = latest_indexes
                    .as_ref()
                    .and_then(|latest| latest.get(&idx_info.id))
                    .is_some_and(|latest| latest.read().state == SchemaState::PUBLIC);
                if !public {
                    continue;
                }
            }
            index_infos.push(idx.clone());
            // For partition tables except global index.
            if tbl_info.get_partition_info().is_some() && !idx_info.global {
                for def_id in table.partition_definition_ids() {
                    let reader =
                        self.build_physical_index_look_up_reader(db_name, table, &tbl_info, def_id, true, idx)?;
                    index_look_up_readers.push(reader);
                }
                continue;
            }
            // For non-partition tables.
            let reader = self.build_physical_index_look_up_reader(db_name, table, &tbl_info, tbl_info.id, false, idx)?;
            index_look_up_readers.push(reader);
        }
        if index_look_up_readers.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }
        Ok((index_look_up_readers, index_infos))
    }

    /// Go `buildPhysicalIndexLookUpReader`: the full-range lookup over one
    /// index of one physical table, whose table scan reads the index's
    /// columns and the handle (`HandleIdx`).
    fn build_physical_index_look_up_reader(
        &mut self,
        db_name: &str,
        table: &SourceTable,
        tbl_info: &TableInfo,
        physical_id: i64,
        is_partition: bool,
        idx: &GoShared<IndexInfo>,
    ) -> Result<PhysicalPlan, PlanError> {
        let full_expr_cols = self.admin_check_table_schema(db_name, table, tbl_info)?;
        // Go `tryGetPkExtraColumn`.
        let extra = (!tbl_info.is_common_handle && !tbl_info.pk_is_handle).then(|| {
            let info = tidb_model::ColumnInfo::new_extra_handle_col_info();
            let mut column = Column::new(self.column_ids.alloc(), FieldType::new(FieldTypeCode::LongLong));
            column.id = tidb_model::column::EXTRA_HANDLE_ID;
            (GoShared::new(info), column)
        });
        // Go `tryGetPkHandleCol`.
        let pk_handle = if tbl_info.pk_is_handle {
            tbl_info.columns.iter_deref().enumerate().find_map(|(i, c)| {
                c.read()
                    .field_type
                    .has_flag(FieldTypeFlags::PRI_KEY)
                    .then(|| (c.clone(), full_expr_cols.columns.get(i).cloned()))
            })
        } else {
            None
        };
        // Go `tryGetCommonHandleCols`.
        let common = if tbl_info.is_common_handle {
            let pk = tidb_table::tables::scan_from_infos::find_primary_index(tbl_info)
                .ok_or_else(|| PlanError::internal("a clustered table has a primary key"))?;
            let (cols, _) =
                tidb_planner_util::column::index_info_2_full_cols(&tbl_info.columns.iter_deref().collect::<Vec<_>>(), &full_expr_cols.columns, &pk.read());
            let common_cols: Vec<Column> = cols.into_iter().flatten().collect();
            let infos: Vec<GoShared<tidb_model::ColumnInfo>> = pk
                .read()
                .columns
                .iter_deref()
                .filter_map(|ic| tbl_info.columns.get(usize::try_from(ic.read().offset).ok()?))
                .collect();
            Some((infos, common_cols))
        } else {
            None
        };
        let idx_info = idx.read().clone();
        // Go `getIndexColumnInfos`.
        let idx_col_infos: Vec<GoShared<tidb_model::ColumnInfo>> = idx_info
            .columns
            .iter_deref()
            .filter_map(|ic| tbl_info.columns.get(usize::try_from(ic.read().offset).ok()?))
            .collect();
        // Go `getIndexColsSchema`.
        let idx_col_schema = Schema::new(
            idx_info
                .columns
                .iter_deref()
                .filter_map(|ic| {
                    let name = ic.read().name.lowercase().to_owned();
                    tbl_info
                        .columns
                        .iter_deref()
                        .position(|c| c.read().name.lowercase() == name)
                        .and_then(|i| full_expr_cols.columns.get(i).cloned())
                })
                .collect(),
        );
        let (idx_cols, idx_col_lens) =
            tidb_planner_util::column::index_info_2_prefix_cols(&idx_col_infos, &idx_col_schema.columns, &idx_info);
        let pseudo_hist_coll = std::sync::Arc::new(tidb_stats::table::pseudo_hist_coll(physical_id, false));
        let offset = self.select_offset();
        let mut is = PhysicalIndexScan {
            base: tidb_planner_core_operator_physicalop::BasePhysicalPlan::new(self.plan_ids, "IndexScan", offset),
            table: Some(GoShared::new(tbl_info.clone())),
            table_as_name: Some(tbl_info.name.original().to_owned()),
            db_name: db_name.to_owned(),
            columns: idx_col_infos.clone(),
            index: Some(idx.clone()),
            index_id: idx_info.id,
            index_name: idx_info.name.original().to_owned(),
            idx_cols,
            idx_col_lens,
            data_source_schema: Some(Box::new(idx_col_schema.clone())),
            ranges: tidb_ranger::points::full_range(),
            physical_table_id: physical_id,
            is_partition,
            tbl_col_hists: Some(std::sync::Arc::clone(&pseudo_hist_coll)),
            ..PhysicalIndexScan::default()
        };
        // There is no alternative plan choices, so just use pseudo stats to avoid panic.
        is.base.base.set_stats(Some(
            tidb_planner_property::stats_info::StatsInfo::new(0.0, [])
                .with_hist_coll(std::sync::Arc::clone(&pseudo_hist_coll)),
        ));
        if let Some((infos, _)) = &common {
            is.columns.extend(infos.iter().cloned());
        }
        let mut idx_expr_cols: Vec<Option<Column>> = is.idx_cols.iter().cloned().map(Some).collect();
        idx_expr_cols.extend(common.iter().flat_map(|(_, cols)| cols.iter().cloned().map(Some)));
        let column_ids = self.column_ids;
        is.init_schema(&idx_expr_cols, true, &|| Ok(column_ids.alloc()))?;

        // It's double read case.
        let mut ts = PhysicalTableScan {
            base: tidb_planner_core_operator_physicalop::BasePhysicalPlan::new(self.plan_ids, "TableScan", offset),
            table: Some(GoShared::new(tbl_info.clone())),
            table_as_name: Some(tbl_info.name.original().to_owned()),
            db_name: db_name.to_owned(),
            table_name: tbl_info.name.original().to_owned(),
            columns: idx_col_infos.clone(),
            physical_table_id: physical_id,
            is_partition,
            tbl_col_hists: Some(pseudo_hist_coll),
            ..PhysicalTableScan::default()
        };
        ts.base.base.set_schema(Some(idx_col_schema.clone()));
        // Go `ts.Columns = ExpandVirtualColumn(ts.Columns, ts.Schema(),
        // ts.Table.Columns)`, before the handle is appended, so the
        // cop-to-root conversion finds nothing more to expand.
        let mut plan = PhysicalPlan::TableScan(ts);
        tidb_planner_core_operator_physicalop::task::expand_virtual_column(&mut plan)?;
        let PhysicalPlan::TableScan(mut ts) = plan else {
            return Err(PlanError::internal("virtual column expansion keeps the table scan"));
        };
        let mut ts_columns = std::mem::take(&mut ts.columns);
        let mut ts_schema = ts.base.base.schema().map(|schema| schema.columns.clone()).unwrap_or_default();
        let mut handle_idx = Vec::new();
        if let Some((info, column)) = &extra {
            ts_columns.push(info.clone());
            ts_schema.push(column.clone());
            handle_idx = vec![ts_columns.len() - 1];
        } else if let Some((info, column)) = &pk_handle {
            ts_columns.push(info.clone());
            if let Some(column) = column {
                ts_schema.push(column.clone());
            }
            handle_idx = vec![ts_columns.len() - 1];
        } else if let Some((infos, cols)) = &common {
            for (pk_offset, info) in infos.iter().enumerate() {
                let id = info.read().id;
                if let Some(i) = ts_columns.iter().position(|c| c.read().id == id) {
                    handle_idx.push(i);
                } else {
                    ts_columns.push(info.clone());
                    if let Some(column) = cols.get(pk_offset) {
                        ts_schema.push(column.clone());
                    }
                    handle_idx.push(ts_columns.len() - 1);
                }
            }
        }
        ts.columns = ts_columns;
        ts.handle_idx = handle_idx;
        // Go `AddExtraPhysTblIDColumn` for a global index ("Not adding the
        // ExtraPhysTblID if already exists").
        if idx_info.global && !ts.columns.iter().any(|c| c.read().id == tidb_model::column::EXTRA_PHYS_TBL_ID) {
            ts.columns.push(GoShared::new(tidb_model::ColumnInfo::new_extra_phys_tbl_id_col_info()));
            let mut column = Column::new(column_ids.alloc(), FieldType::new(FieldTypeCode::LongLong));
            column.id = tidb_model::column::EXTRA_PHYS_TBL_ID;
            column.orig_name = "_tidb_tid".to_owned();
            ts_schema.push(column);
        }
        ts.base.base.set_schema(Some(Schema::new(ts_schema)));

        let extra_handle_col = extra.map(|(_, column)| column);
        let common_handle_cols = common.map(|(_, cols)| cols).unwrap_or_default();
        let cop = CopTask {
            index_plan: Some(Box::new(PhysicalPlan::IndexScan(is))),
            table_plan: Some(Box::new(PhysicalPlan::TableScan(ts))),
            extra_handle_col,
            common_handle_cols,
            ..CopTask::default()
        };
        let Task::Root(mut root) = Task::Cop(cop).into_root_task(self.plan_ids)? else {
            return Err(PlanError::internal("a cop task converts to a root task"));
        };
        let mut plan = root
            .take_plan()
            .ok_or_else(|| PlanError::internal("the admin-check lookup has no plan"))?;
        plan.resolve_indices()?;
        Ok(plan)
    }

    /// Go `expression.TableInfo2SchemaAndNames(ctx, dbName, tblInfo)`'s
    /// schema: a column per public table column, each virtual generated
    /// column with its expression.
    fn admin_check_table_schema(
        &self,
        db_name: &str,
        table: &SourceTable,
        tbl_info: &TableInfo,
    ) -> Result<Schema, PlanError> {
        let table_name = tbl_info.name.original().to_owned();
        let mut schema_columns = Vec::new();
        let mut names = Vec::new();
        for col in tbl_info.cols().iter_deref() {
            let col = col.read();
            let name = self.table_field_name(db_name, &table_name, &table_name, col.name.original(), false, col.hidden);
            let mut column = Column::new(self.column_ids.alloc(), col.field_type.clone());
            column.id = col.id;
            column.index = col.offset;
            column.orig_name = name.display_name();
            column.is_hidden = col.hidden;
            schema_columns.push(column);
            names.push(name);
        }
        let mut schema = Schema::new(schema_columns);
        let no_markers = BTreeMap::new();
        for (offset, source_column) in table.columns.iter().enumerate() {
            if !source_column.read().is_virtual_generated() {
                continue;
            }
            let Some(expression) = source_column.generated_expr().cloned() else {
                continue;
            };
            let Some(at) = schema.columns.iter().position(|c| c.id == source_column.read().id) else {
                continue;
            };
            let original = self.allow_build_cast_array.replace(true);
            let rewritten = self.rewrite_scalar(&expression, &schema, &names, &no_markers);
            self.allow_build_cast_array.set(original);
            let _ = offset;
            schema.columns[at].virtual_expr = Some(Box::new(rewritten?));
        }
        Ok(schema)
    }
}
