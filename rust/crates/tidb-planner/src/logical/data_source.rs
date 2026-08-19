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

//! Go `pkg/planner/core/operator/logicalop/logical_datasource.go`:
//! `DataSource`, the leaf that reads one (physical) table.
//!
//! SEED of `pkg/planner/core`. The ACCESS PATHS are
//! [`crate::access_path::DataSourceAccessPath`], already in this crate and
//! reused rather than restated; the path-to-task lowering stays in
//! [`crate::logical_data_source`] / [`crate::logical_data_source_task`], which
//! this operator feeds rather than duplicates.
//!
//! # Narrowings, by name
//!
//! * `Table table.Table`, `TableInfo *model.TableInfo`, `Columns
//!   []*model.ColumnInfo`, `IS infoschema.InfoSchema`, `StatisticTable
//!   *statistics.Table`, `TblColHists *statistics.HistColl`,
//!   `SampleInfo *tablesampler.TableSampleInfo`. The catalogue and statistics
//!   handles are not transcreated into this crate. What the ported bodies
//!   actually READ off them is kept as explicit fields: the table id, the
//!   names, the partition definition, `pk_is_handle`, and the per-column
//!   metadata in [`DataSourceColumn`].
//! * `AstIndexHints` / `IndexHints` / `IndexMergeHints` are
//!   `[]h.HintedIndex`; the hint catalogue is not transcreated, so only the
//!   RESOLVED [`DataSource::prefer_store_type`] survives.
//! * `HandleCols util.HandleCols` is an interface over an int handle or a
//!   common handle. [`crate::handle_cols`] already models both identities;
//!   this operator holds the handle COLUMNS, which is what the ported bodies
//!   need, plus [`DataSource::handle_is_int`].

use tidb_expr::column::Column;
use tidb_expr::expression::{CorrelatedColumn, Expression};
use tidb_expr::schema::Schema;
use tidb_expr::simple_expr::{extract_columns_from_expressions, extract_cor_columns};

use crate::access_path::DataSourceAccessPath;
use crate::logical::schema_producer;
use crate::logical::BaseLogicalPlan;
use crate::plan_base::PossiblePropertiesInfo;
use crate::stats_info::StatsInfo;

/// Go `model.ExtraHandleID`: the id of the implicit `_tidb_rowid` column.
pub const EXTRA_HANDLE_ID: i64 = -1;

/// What the ported `DataSource` bodies read off a `*model.ColumnInfo`.
#[derive(Clone, Debug, Default)]
pub struct DataSourceColumn {
    /// Go `ColumnInfo.ID`.
    pub id: i64,
    /// Go `ColumnInfo.Name.O`.
    pub name: String,
    /// Go `mysql.HasPriKeyFlag(col.GetFlag())`.
    pub is_primary_key: bool,
}

/// Go `logicalop.DataSource` (`logical_datasource.go:58`).
#[derive(Clone, Debug, Default)]
pub struct DataSource {
    /// The shared logical base.
    pub base: BaseLogicalPlan,
    /// Go `TableInfo.ID`.
    pub table_id: i64,
    /// Go `TableInfo.Name.O`.
    pub table_name: String,
    /// Go `TableAsName`: the `AS` alias, when the query gave one.
    pub table_as_name: Option<String>,
    /// Go `DBName`.
    pub db_name: String,
    /// Go `PhysicalTableID`: the partition's id, or the table's.
    pub physical_table_id: i64,
    /// Go `PartitionDefIdx`: which partition definition this reads.
    pub partition_def_idx: Option<usize>,
    /// The partition definition names, so [`Self::explain_info`] can name the
    /// one `partition_def_idx` selects. Go reads
    /// `TableInfo.GetPartitionInfo().Definitions`.
    pub partition_definition_names: Vec<String>,
    /// Go `Columns`, in schema order.
    pub columns: Vec<DataSourceColumn>,
    /// Go `PushedDownConds`: the conditions the storage layer will evaluate.
    pub pushed_down_conds: Vec<Expression>,
    /// Go `AllConds`: every condition on this table, pushed down or not.
    pub all_conds: Vec<Expression>,
    /// Go `getPossibleAccessPaths`' answer at BUILD time — the newborn form
    /// (`crate::access_path::PossiblePath`), before ranger and statistics
    /// grow it. Go mutates one `util.AccessPath` through both stages; this
    /// port keeps the stages as two typed lists, and the grown lists below
    /// stay empty until the costing seam fills them.
    pub enumerated_paths: Vec<crate::access_path::PossiblePath>,
    /// The catalog's index metadata, in the SAME order
    /// [`Self::enumerated_paths`]' `Index { index }` offsets address — what
    /// Go reads off `ds.TableInfo.Indices` when it fills `path.IdxCols`
    /// (`fillIndexPath`). Filled beside the newborn path list.
    pub indexes: Vec<crate::plan_builder::catalog::SourceIndex>,
    /// Go `AllPossibleAccessPaths`, reusing this crate's own path model.
    pub all_possible_access_paths: Vec<DataSourceAccessPath>,
    /// Go `PossibleAccessPaths`: the pruned subset the optimizer enumerates.
    pub possible_access_paths: Vec<DataSourceAccessPath>,
    /// Go `TableInfo.PKIsHandle`.
    pub pk_is_handle: bool,
    /// Go `HandleCols`' columns; empty when the table has no usable handle.
    pub handle_cols: Vec<Column>,
    /// Whether Go's `HandleCols.IsInt()` holds.
    pub handle_is_int: bool,
    /// Go `CommonHandleCols`.
    pub common_handle_cols: Vec<Column>,
    /// Go `CommonHandleLens`.
    pub common_handle_lens: Vec<i64>,
    /// Go `PreferStoreType`: the resolved `READ_FROM_STORAGE` decision.
    pub prefer_store_type: i32,
    /// Go `IsForUpdateRead`.
    pub is_for_update_read: bool,
    /// Go `ContainExprPrefixUk`: a `tidb_shard()` prefix unique key exists, so
    /// its generated column must never be pruned.
    pub contain_expr_prefix_uk: bool,
    /// Go `ColsRequiringFullLen`, rebuilt by [`Self::prune_columns_local`].
    pub cols_requiring_full_len: Vec<Column>,
    /// Go `AccessPathMinSelectivity`.
    pub access_path_min_selectivity: f64,
    /// Go `AskedColumnGroup`.
    pub asked_column_group: Vec<Vec<Column>>,
    /// Go `InterestingColumns`.
    pub interesting_columns: Vec<Column>,
    /// Go `TableStats`: the table-level profile before any filtering.
    pub table_stats: Option<StatsInfo>,
    /// Whether the table has an available TiFlash replica; Go computes this
    /// through `TableInfo.TiFlashReplica` plus the hypothetical-replica
    /// session state, both of which are outside this crate.
    pub has_tiflash_replica: bool,
}

impl DataSource {
    /// Go `plancodec.TypeTableScan`, as `DataSource.Init` sets it.
    pub const TYPE: &'static str = "DataSource";

    /// Go `DataSource.Init(ctx, offset)` (`logical_datasource.go:155`).
    #[must_use]
    pub fn new(base: BaseLogicalPlan, table_id: i64, table_name: impl Into<String>) -> Self {
        Self {
            base,
            table_id,
            physical_table_id: table_id,
            table_name: table_name.into(),
            ..Self::default()
        }
    }

    /// Go `DataSource.ExplainInfo()` (`logical_datasource.go:163`): the table
    /// name — the ALIAS when there is one — and the partition, if any.
    ///
    /// Ported whole: every input is a field of this operator.
    #[must_use]
    pub fn explain_info(&self) -> String {
        let table_name = match &self.table_as_name {
            Some(alias) if !alias.is_empty() => alias.as_str(),
            _ => self.table_name.as_str(),
        };
        let mut buffer = format!("table:{table_name}");
        if let Some(idx) = self.partition_def_idx {
            if let Some(name) = self.partition_definition_names.get(idx) {
                buffer.push_str(&format!(", partition:{name}"));
            }
        }
        buffer
    }

    /// Go `DataSource.ExtractCorrelatedCols()`
    /// (`logical_datasource.go:377`): the correlated columns of the
    /// PUSHED-DOWN conditions only.
    #[must_use]
    pub fn extract_correlated_cols(&self) -> Vec<CorrelatedColumn> {
        let mut cor_cols = Vec::with_capacity(self.pushed_down_conds.len());
        for expr in &self.pushed_down_conds {
            cor_cols.extend(extract_cor_columns(expr));
        }
        cor_cols
    }

    /// Go `DataSource.HasTiFlash()` (`logical_datasource.go:271`).
    ///
    /// The replica test lives on `TableInfo`, which is narrowed out; the
    /// resolved answer is [`Self::has_tiflash_replica`].
    #[must_use]
    pub const fn has_tiflash(&self) -> bool {
        self.has_tiflash_replica
    }

    /// Go `DataSource.GetPKIsHandleCol()` (`logical_datasource.go:599`) via
    /// `getPKIsHandleColFromSchema` (`:580`): the integer primary key column,
    /// present only when the table is `PKIsHandle` and the column survived
    /// pruning.
    #[must_use]
    pub fn get_pk_is_handle_col<'a>(&self, self_schema: &'a Schema) -> Option<&'a Column> {
        if !self.pk_is_handle {
            return None;
        }
        let position = self
            .columns
            .iter()
            .position(|column| column.is_primary_key)?;
        self_schema.columns.get(position)
    }

    /// Go `DataSource.PredicatePushDown(predicates)`'s LOCAL half
    /// (`logical_datasource.go:185`): a data source ACCEPTS every predicate,
    /// recording all of them in `AllConds` and keeping the pushable ones in
    /// `PushedDownConds`.
    ///
    /// The split is the caller's: Go asks
    /// `expression.PushDownExprs(pushDownCtx, predicates, kv.UnSpecified)`,
    /// which consults the store's function whitelist. The whitelist lives in
    /// `tidb_expr::pushdown_catalog`; this method takes the already-partitioned
    /// result so the operator never guesses what a store supports.
    ///
    /// Returns the predicates the PARENT must still apply, which is Go's first
    /// return value.
    pub fn predicate_push_down_local(
        &mut self,
        pushable: Vec<Expression>,
        not_pushable: Vec<Expression>,
    ) -> Vec<Expression> {
        self.all_conds = pushable
            .iter()
            .cloned()
            .chain(not_pushable.iter().cloned())
            .collect();
        self.pushed_down_conds = pushable;
        not_pushable
    }

    /// Go `DataSource.PruneColumns(parentUsedCols)`'s LOCAL half
    /// (`logical_datasource.go:200`).
    ///
    /// A column survives when the parent uses it, when one of `AllConds` reads
    /// it, or when it is the generated column of a `tidb_shard()` prefix
    /// unique key. `ColsRequiringFullLen` is rebuilt from the PARENT's use
    /// only, because a column kept solely for `AllConds` must not force a
    /// full-length index read.
    ///
    /// Returns whether the schema became empty, which is the condition under
    /// which Go forces one handle column back in — a decision that needs the
    /// catalogue and so belongs to the caller.
    pub fn prune_columns_local(
        &mut self,
        parent_used_cols: &[Column],
        schema: &mut Schema,
    ) -> bool {
        let used = schema_producer::get_used_list(parent_used_cols, schema);
        let expr_cols = extract_columns_from_expressions(&self.all_conds, None);
        let expr_used = schema_producer::get_used_list(&expr_cols, schema);

        self.cols_requiring_full_len = schema
            .columns
            .iter()
            .enumerate()
            .filter(|(i, column)| {
                used[*i] || (self.contain_expr_prefix_uk && is_shard_column(column))
            })
            .map(|(_, column)| column.clone())
            .collect();

        for i in (0..used.len()).rev() {
            if used[i] || expr_used[i] {
                continue;
            }
            if self.contain_expr_prefix_uk && is_shard_column(&schema.columns[i]) {
                continue;
            }
            schema.columns.remove(i);
            if i < self.columns.len() {
                self.columns.remove(i);
            }
        }

        // Go: once the int handle no longer appears in the schema, the handle
        // is unusable and must be forgotten, so that a later pass can pick a
        // fresh one instead of silently reading `_tidb_rowid`.
        if self.handle_is_int
            && self
                .handle_cols
                .first()
                .is_some_and(|handle| schema.column_index(handle) == -1)
        {
            self.handle_cols.clear();
            self.handle_is_int = false;
        }
        schema.columns.is_empty()
    }

    /// Go `DataSource.PreparePossibleProperties(_, _)`
    /// (`logical_datasource.go:343`): every access path offers its index-column
    /// prefix as an order, plus each suffix after an equality-matched prefix,
    /// and an int-handle path offers the handle column.
    ///
    /// # Blocked
    ///
    /// Go's `hasTiFlash` here is
    /// `tiflashInIsolationRead && !preferTiKVOnly && HasTiFlash() &&
    /// IsMPPAllowed()`. The isolation-read engine set and `IsMPPAllowed` are
    /// session state; the caller passes the resolved
    /// `tiflash_in_isolation_read` and `mpp_allowed` here so nothing is
    /// invented, and the `preferTiKVOnly` half is computed from
    /// [`Self::prefer_store_type`].
    pub fn prepare_possible_properties(
        &mut self,
        orders: Vec<Vec<Column>>,
        tiflash_in_isolation_read: bool,
        mpp_allowed: bool,
    ) -> PossiblePropertiesInfo {
        let prefer_tikv_only = self.prefer_store_type & PREFER_TIKV != 0
            && self.prefer_store_type & PREFER_TIFLASH == 0;
        let has_tiflash =
            tiflash_in_isolation_read && !prefer_tikv_only && self.has_tiflash() && mpp_allowed;
        self.base.set_has_tiflash(has_tiflash);
        PossiblePropertiesInfo {
            orders,
            has_tiflash,
        }
    }

    /// Go `DataSource.BuildKeyInfo(selfSchema, _)`
    /// (`logical_datasource.go:278`): the table's own keys, taken from the
    /// index definitions and the integer primary key.
    ///
    /// # Blocked
    ///
    /// The index walk is `ruleutil.CheckIndexCanBeKey(index, ds.Columns,
    /// selfSchema)` over `ds.Table.Meta().Indices`, plus a
    /// `domainmisc.GetLatestIndexInfo` re-read under READ COMMITTED. Neither
    /// `table.Table` nor `pkg/domain` is transcreated. The dependency-closed
    /// half — the `PKIsHandle` primary key — is ported whole, and the index
    /// keys are the caller's to supply as `index_keys`.
    pub fn build_key_info(&self, self_schema: &mut Schema, index_keys: Vec<Vec<Column>>) {
        self_schema.pk_or_uk = index_keys;
        if !self.pk_is_handle {
            return;
        }
        if let Some(position) = self.columns.iter().position(|column| column.is_primary_key) {
            if let Some(column) = self_schema.columns.get(position) {
                self_schema.pk_or_uk.push(vec![column.clone()]);
            }
        }
    }

    /// Go `DataSource.DeriveStats(...)` (`logical_datasource.go:336`), which
    /// forwards to `utilfuncp.DeriveStats4DataSource(ds)`.
    ///
    /// # Blocked
    ///
    /// `DeriveStats4DataSource` (`pkg/planner/core/stats.go`) needs the
    /// statistics handle, the histogram collection, and selectivity
    /// estimation. The dependency-closed part — a data source with no
    /// conditions has exactly its table profile — is what runs here, and the
    /// filtered case returns the table profile UNSCALED with `false`, so a
    /// caller can tell it was not estimated.
    pub fn derive_stats(&mut self) -> Option<(StatsInfo, bool)> {
        let table_stats = self.table_stats.clone()?;
        self.base.base.set_stats(Some(table_stats.clone()));
        Some((table_stats.clone(), self.all_conds.is_empty()))
    }
}

/// Go `h.PreferTiKV` (`pkg/util/hint`).
pub const PREFER_TIKV: i32 = 1;
/// Go `h.PreferTiFlash`.
pub const PREFER_TIFLASH: i32 = 1 << 1;

/// Go `expression.GcColumnExprIsTidbShard(col.VirtualExpr)`: the column is the
/// generated column of a `tidb_shard()` prefix unique key.
#[must_use]
pub fn is_shard_column(column: &Column) -> bool {
    matches!(
        column.virtual_expr.as_deref(),
        Some(Expression::ScalarFunction(function)) if function.func_name.lowercase() == "tidb_shard"
    )
}

impl DataSource {
    /// This operator's own fields with NO children; see
    /// [`crate::logical::LogicalPlan::clone_shallow`]. A `DataSource` is a
    /// leaf, so this differs from [`Clone`] only in dropping an empty vector.
    #[must_use]
    pub fn clone_shallow(&self) -> Self {
        Self {
            base: self.base.shell(),
            table_id: self.table_id,
            table_name: self.table_name.clone(),
            table_as_name: self.table_as_name.clone(),
            db_name: self.db_name.clone(),
            physical_table_id: self.physical_table_id,
            partition_def_idx: self.partition_def_idx,
            partition_definition_names: self.partition_definition_names.clone(),
            columns: self.columns.clone(),
            pushed_down_conds: self.pushed_down_conds.clone(),
            all_conds: self.all_conds.clone(),
            enumerated_paths: self.enumerated_paths.clone(),
            indexes: self.indexes.clone(),
            all_possible_access_paths: self.all_possible_access_paths.clone(),
            possible_access_paths: self.possible_access_paths.clone(),
            pk_is_handle: self.pk_is_handle,
            handle_cols: self.handle_cols.clone(),
            handle_is_int: self.handle_is_int,
            common_handle_cols: self.common_handle_cols.clone(),
            common_handle_lens: self.common_handle_lens.clone(),
            prefer_store_type: self.prefer_store_type,
            is_for_update_read: self.is_for_update_read,
            contain_expr_prefix_uk: self.contain_expr_prefix_uk,
            cols_requiring_full_len: self.cols_requiring_full_len.clone(),
            access_path_min_selectivity: self.access_path_min_selectivity,
            asked_column_group: self.asked_column_group.clone(),
            interesting_columns: self.interesting_columns.clone(),
            table_stats: self.table_stats.clone(),
            has_tiflash_replica: self.has_tiflash_replica,
        }
    }
}
