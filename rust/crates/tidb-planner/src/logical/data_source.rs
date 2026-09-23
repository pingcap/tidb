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
//! * The pruning rule reads resolved FORCE facts and query-block-matched
//!   `IndexMergeHints` from this leaf instead of re-reading syntax.
//! * `HandleCols util.HandleCols` is an interface over an int handle or a
//!   common handle. [`crate::handle_cols`] already models both identities;
//!   this operator holds the handle COLUMNS, which is what the ported bodies
//!   need, plus [`DataSource::handle_is_int`].

use tidb_datatype::{Collation, Datum};
use tidb_expr::column::Column;
use tidb_expr::expr_util::normal_form::split_cnf_items;
use tidb_expr::expression::{CorrelatedColumn, Expression};
use tidb_expr::rewriter::ColumnResolver;
use tidb_expr::schema::Schema;
use tidb_expr::simple_expr::{
    extract_columns_from_expressions, extract_cor_columns, parse_simple_expr, BuildOptions,
};

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
    /// Go `mysql.HasNotNullFlag(col.GetFlag())`.
    ///
    /// `rule/util.CheckIndexCanBeKey` deliberately reads the table-column
    /// metadata rather than the corresponding expression column's type.
    pub is_not_null: bool,
}

/// Go `h.HintedIndex` fields consumed by partition processing and index-merge
/// path pruning.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DataSourceIndexMergeHint {
    /// Go `IndexHint.IndexNames`.
    pub index_names: Vec<String>,
    /// Go `HintedIndex.Partitions`.
    pub partitions: Vec<String>,
    /// Go `Restore2IndexHint(HintIndexMerge, hint)`, used in warnings.
    pub restored: String,
}

/// Go `h.HintedIndex` fields used by ordinary path resolution and static
/// partition processing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataSourceIndexHint {
    /// Go `IndexHint.HintType`.
    pub kind: tidb_ast::IndexHintKind,
    /// Go `IndexHint.IndexNames`.
    pub index_names: Vec<String>,
    /// Go `HintedIndex.Partitions`.
    pub partitions: Vec<String>,
    /// Go `HintedIndex.PushDownLookUp`.
    pub push_down_lookup: bool,
    /// Whether the hint is `ORDER_INDEX`.
    pub force_keep_order: bool,
    /// Whether the hint is `NO_ORDER_INDEX`.
    pub force_no_keep_order: bool,
    /// Go `Restore2IndexHint(HintedIndex.HintTypeString(), hint)`.
    pub restored: String,
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
    /// Go `SampleInfo`.
    pub sample_info: Option<crate::table_sampler::TableSampleInfo>,
    /// Go `PartitionDefIdx`: which partition definition this reads.
    pub partition_def_idx: Option<usize>,
    /// Go `PartitionNames`: an explicit `PARTITION (p, ...)` restriction.
    pub partition_names: Vec<String>,
    /// The partition definition names, so [`Self::explain_info`] can name the
    /// one `partition_def_idx` selects. Go reads
    /// `TableInfo.GetPartitionInfo().Definitions`.
    pub partition_definition_names: Vec<String>,
    /// Physical IDs index-parallel to [`Self::partition_definition_names`].
    pub partition_definition_ids: Vec<i64>,
    /// Go `PhysPlanPartInfo` after dynamic partition pruning, retained so
    /// reader access objects report the partitions selected by the same
    /// predicates used for execution.
    pub dynamic_partition_access: Option<crate::access::DynamicPartitionAccessObject>,
    /// Go `Columns`, in schema order.
    pub columns: Vec<DataSourceColumn>,
    /// Go `TblCols`: the original table columns before logical pruning.
    /// Physical table-scan costing uses this complete row width even when
    /// the scan only returns a narrow projected schema.
    pub table_columns: Vec<Column>,
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
    /// The unfiltered public paths from which static partition children rerun
    /// Go `getPossibleAccessPaths` after partition-scoped hint selection.
    pub public_enumerated_paths: Vec<crate::access_path::PossiblePath>,
    /// Go `AstIndexHints`: table-syntax hints, which apply to every child.
    pub ast_index_hints: Vec<tidb_ast::IndexHint>,
    /// Go `IndexHints`: comment-style hints matched to this DataSource.
    pub index_hints: Vec<DataSourceIndexHint>,
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
    /// Latest-schema public index IDs used by `ExtractFD` for a connected
    /// `FOR UPDATE` read when the domain schema changed.
    pub fd_latest_public_index_ids: Option<std::collections::BTreeSet<i64>>,
    /// Go `DataSource.ExtractFD` returns the PK-only set when its latest-index
    /// lookup fails.
    pub fd_latest_index_lookup_failed: bool,
    /// Go `TableInfo.IsCommonHandle`.
    pub is_common_handle: bool,
    /// Go `TableInfo.CommonHandleVersion`.
    pub common_handle_version: u16,
    /// Go `TableInfo.TempTableType != model.TempTableNone`.
    pub is_temporary: bool,
    /// Go `TableInfo.TableCacheStatusType != model.TableCacheStatusDisable`.
    pub is_cached: bool,
    /// Whether Go `TableInfo.Affinity` is non-nil.
    pub has_affinity: bool,
    /// Session/transaction inputs to Go's lookup-pushdown support check.
    pub index_lookup_push_down_session: crate::access_path::IndexLookupPushDownSession,
    /// Whether TiKV is present in Go `SessionVars.IsolationReadEngines`.
    pub tikv_in_isolation_read: bool,
    /// Raw `tidb_isolation_read_engines`, retained for Go-compatible index
    /// hint diagnostics during static partition re-resolution.
    pub isolation_read_engines_value: String,
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
    /// Go `PreferPartitions`, keyed by `h.PreferTiKV` / `h.PreferTiFlash`.
    pub prefer_partitions: std::collections::BTreeMap<i32, Vec<String>>,
    /// Go `IsForUpdateRead`.
    pub is_for_update_read: bool,
    /// Go `ContainExprPrefixUk`: a `tidb_shard()` prefix unique key exists, so
    /// its generated column must never be pruned.
    pub contain_expr_prefix_uk: bool,
    /// Go `ColsRequiringFullLen`; None means column pruning has not run.
    /// Some(empty) means no parent requires a full column value.
    pub cols_requiring_full_len: Option<Vec<Column>>,
    /// Go `AccessPathMinSelectivity`.
    pub access_path_min_selectivity: f64,
    /// Go `AskedColumnGroup`.
    pub asked_column_group: Vec<Vec<Column>>,
    /// Go `InterestingColumns`.
    pub interesting_columns: Vec<Column>,
    /// Index IDs whose access paths have Go `AccessPath.Forced` set.
    pub forced_index_ids: std::collections::BTreeSet<i64>,
    /// Index IDs carrying Go `ForceKeepOrder`.
    pub force_keep_order_index_ids: std::collections::BTreeSet<i64>,
    /// Index IDs carrying Go `ForceNoKeepOrder`.
    pub force_no_keep_order_index_ids: std::collections::BTreeSet<i64>,
    /// Whether the TiKV table path carries Go `ForceKeepOrder`.
    pub force_keep_order_table_path: bool,
    /// Whether the TiKV table path carries Go `ForceNoKeepOrder`.
    pub force_no_keep_order_table_path: bool,
    /// Go `AccessPath.IndexLookUpPushDownBy`, keyed by index ID.
    pub index_lookup_push_down_by:
        std::collections::BTreeMap<i64, crate::access_path::IndexLookupPushDownBy>,
    /// Index IDs carrying Go `AccessPath.NoncacheableReason` after
    /// `CheckPartialIndexes` rejects the general cached-plan proof.
    pub partial_index_noncacheable_ids: std::collections::BTreeSet<i64>,
    /// Go `forceNoIndexLookUpPushDown`, set by a matching
    /// `NO_INDEX_LOOKUP_PUSHDOWN` hint before paths are enumerated.
    pub force_no_index_lookup_push_down: bool,
    /// Go `IndexMergeHints`. An empty `index_names` list is a general
    /// `USE_INDEX_MERGE(table)` hint; `partitions` scopes it per static child.
    pub index_merge_hints: Vec<DataSourceIndexMergeHint>,
    /// Go fix control 52869 after session-variable resolution.
    pub prefer_index_merge_by_fix_control: bool,
    /// Go `TableStats`: the table-level profile before any filtering.
    pub table_stats: Option<StatsInfo>,
    /// Go table access path's already-derived `CountAfterAccess`.
    pub table_path_count_after_access: Option<f64>,
    /// Go `AccessPath.IsSingleScan`, fixed when stats are first derived.
    /// Later pruning must not change the reader family of an existing path.
    pub index_path_single_scan: std::collections::BTreeMap<i64, bool>,
    /// Go index access paths' already-derived `CountAfterAccess`, by index id.
    pub index_path_count_after_access: std::collections::BTreeMap<i64, f64>,
    /// Unadjusted index cardinality bounds retained for Go skyline risk comparison.
    pub index_path_row_estimates:
        std::collections::BTreeMap<i64, crate::cardinality::row_count_column::RowEstimate>,
    /// Go ColAndIdxExistenceMap.HasAnalyzed, independent of payload eviction.
    pub analyzed_index_ids: std::collections::BTreeSet<i64>,
    /// The table/session facts that Go retains on `PhysicalTableScan` for
    /// `getTableScanPenalty`.
    pub table_scan_penalty: crate::plan_cost_ver2::TableScanPenaltyInput,
    /// Whether the table has an available TiFlash replica; Go computes this
    /// through `TableInfo.TiFlashReplica` plus the hypothetical-replica
    /// session state, both of which are outside this crate.
    pub has_tiflash_replica: bool,
}

impl DataSource {
    /// Go `ruleutil.CheckIndexCanBeKey` plus the public-index walk in
    /// `DataSource.BuildKeyInfo`. The source column list is pruned in lockstep
    /// with `self_schema`, so matching by column name also rejects a key whose
    /// column no longer appears in this plan.
    #[must_use]
    pub fn index_keys(&self, self_schema: &Schema) -> (Vec<Vec<Column>>, Vec<Vec<Column>>) {
        let mut strong = Vec::new();
        let mut nullable = Vec::new();
        for index in &self.indexes {
            if !index.unique || !index.is_public {
                continue;
            }
            if self
                .fd_latest_public_index_ids
                .as_ref()
                .is_some_and(|indexes| !indexes.contains(&index.id))
            {
                continue;
            }
            let (nullable_key, strong_key) =
                super::rule_util::check_index_can_be_key(index, &self.columns, self_schema);
            if let Some(key) = strong_key {
                strong.push(key);
            } else if let Some(key) = nullable_key {
                nullable.push(key);
            }
        }
        (strong, nullable)
    }

    /// Go `plancodec.TypeTableScan`, as `DataSource.Init` sets it.
    pub const TYPE: &'static str = "DataSource";

    /// Resolves an index column after logical column pruning. Catalog index
    /// offsets address the original table column list, while `columns` and
    /// the logical schema are pruned together; the column name is the stable
    /// identity between those two layouts.
    #[must_use]
    pub fn schema_column_for_index_column(
        &self,
        index_column: &crate::plan_builder::catalog::SourceIndexColumn,
    ) -> Option<&Column> {
        let position = self
            .columns
            .iter()
            .position(|column| column.name.eq_ignore_ascii_case(&index_column.name))?;
        self.base.base.schema()?.columns.get(position)
    }

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
    /// recording all of them in `AllConds` in the order they arrived and
    /// keeping the store-supported ones in `PushedDownConds`.
    ///
    /// The split asks
    /// `expression.PushDownExprs(pushDownCtx, predicates, kv.UnSpecified)`,
    /// which consults the store's function whitelist. That whitelist lives in
    /// [`crate::pushdown`] and [`tidb_expr::pushdown_catalog`].
    ///
    /// The caller has already run `Conds2TableDual` over `AllConds`, which is
    /// Go's order (`logical_datasource.go:366-369`): a constant-NULL predicate
    /// becomes a `TableDual` before the split, so a pushable `col > NULL`
    /// cannot hide the empty result.
    ///
    /// Returns the predicates the PARENT must still apply, which is Go's first
    /// return value.
    pub fn predicate_push_down_local(&mut self, predicates: Vec<Expression>) -> Vec<Expression> {
        self.all_conds = predicates;
        let (pushable, not_pushable): (Vec<_>, Vec<_>) =
            self.all_conds.iter().cloned().partition(|predicate| {
                crate::pushdown::can_exprs_push_down_tikv(std::slice::from_ref(predicate))
            });
        self.pushed_down_conds = pushable;
        not_pushable
    }

    /// Go `DataSource.CheckPartialIndexes`, at the same post-predicate-pushdown
    /// and pre-statistics phase as `deriveStats4DataSource`.
    pub fn check_partial_indexes(
        &mut self,
        resolver: &dyn ColumnResolver,
        use_plan_cache: bool,
        opt_prefix_index_single_scan: bool,
    ) {
        let Some(schema) = self.base.base.schema().cloned() else {
            return;
        };
        let names = self
            .columns
            .iter()
            .map(|column| {
                tidb_datatype::FieldName::new(tidb_datatype::FieldNameMetadata {
                    table: tidb_datatype::IdentifierMetadata::new(&self.table_name),
                    column: tidb_datatype::IdentifierMetadata::new(&column.name),
                    ..tidb_datatype::FieldNameMetadata::default()
                })
            })
            .collect::<Vec<_>>();
        let options = BuildOptions::new().with_input_schema_and_names(schema, names);
        let mut removed_ids = std::collections::BTreeSet::new();
        let mut partial_index_used_hint = false;
        let mut has_partial_index = false;

        for path in &self.enumerated_paths {
            let crate::access_path::PossiblePath::Index { index } = path else {
                continue;
            };
            let Some(metadata) = self.indexes.get(*index) else {
                continue;
            };
            if metadata.condition_expr_string.is_empty() {
                continue;
            }
            has_partial_index = true;
            let predicates = parse_simple_expr(resolver, &metadata.condition_expr_string, &options)
                .map(|expression| split_cnf_items(&expression));
            let Ok(predicates) = predicates else {
                removed_ids.insert(metadata.id);
                continue;
            };
            if !crate::partidx::check_constraints(
                opt_prefix_index_single_scan,
                &predicates,
                &self.pushed_down_conds,
            ) {
                removed_ids.insert(metadata.id);
                continue;
            }
            if self.forced_index_ids.contains(&metadata.id) {
                partial_index_used_hint = true;
            }
            if use_plan_cache
                && !crate::partidx::always_meet_constraints(&predicates, &self.pushed_down_conds)
            {
                self.partial_index_noncacheable_ids.insert(metadata.id);
            }
        }
        if !has_partial_index || (removed_ids.is_empty() && !partial_index_used_hint) {
            return;
        }

        let keep_index_id = |index_id: i64| {
            !removed_ids.contains(&index_id)
                && (!partial_index_used_hint || self.forced_index_ids.contains(&index_id))
        };
        self.enumerated_paths.retain(|path| match path {
            crate::access_path::PossiblePath::Index { index } => self
                .indexes
                .get(*index)
                .is_some_and(|metadata| keep_index_id(metadata.id)),
            crate::access_path::PossiblePath::Table { .. }
            | crate::access_path::PossiblePath::TiFlashTable => !partial_index_used_hint,
        });
        self.all_possible_access_paths.retain(|path| match path {
            DataSourceAccessPath::Index(index) => keep_index_id(index.candidate().index_id),
            DataSourceAccessPath::Table(_) | DataSourceAccessPath::IndexMerge => {
                !partial_index_used_hint
            }
        });
        self.possible_access_paths.retain(|path| match path {
            DataSourceAccessPath::Index(index) => keep_index_id(index.candidate().index_id),
            DataSourceAccessPath::Table(_) | DataSourceAccessPath::IndexMerge => {
                !partial_index_used_hint
            }
        });
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
    /// Returns whether pruning initially emptied the schema. As in Go, one
    /// handle column is forced back into the retained schema before return so
    /// TiKV can report the row count for queries such as `SELECT 1 FROM t`.
    pub fn prune_columns_local(
        &mut self,
        parent_used_cols: &[Column],
        schema: &mut Schema,
    ) -> bool {
        let used = schema_producer::get_used_list(parent_used_cols, schema);
        let expr_cols = extract_columns_from_expressions(&self.all_conds, None);
        let expr_used = schema_producer::get_used_list(&expr_cols, schema);

        self.cols_requiring_full_len = Some(
            schema
                .columns
                .iter()
                .enumerate()
                .filter(|(i, column)| {
                    used[*i] || (self.contain_expr_prefix_uk && is_shard_column(column))
                })
                .map(|(_, column)| column.clone())
                .collect(),
        );

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

        let emptied = schema.columns.is_empty();
        if emptied {
            // Go `preferKeyColumnFromTable`: an ordinary table first reuses
            // its live handle, then the immutable PK handle on a later prune
            // pass, and finally the implicit row id. `table_columns` is this
            // port's immutable copy of the original DataSource schema.
            let forced = self
                .handle_cols
                .first()
                .or_else(|| {
                    self.pk_is_handle.then(|| {
                        self.table_columns.iter().find(|column| {
                            column.ret_type.as_ref().is_some_and(|field_type| {
                                field_type.has_flag(tidb_datatype::FieldTypeFlags::PRI_KEY)
                            })
                        })
                    })?
                })
                .or_else(|| {
                    self.table_columns
                        .iter()
                        .find(|column| column.id == EXTRA_HANDLE_ID)
                })
                .or_else(|| self.table_columns.first())
                .cloned();
            if let Some(forced) = forced {
                let name = forced
                    .orig_name
                    .rsplit('.')
                    .next()
                    .filter(|name| !name.is_empty())
                    .unwrap_or("_tidb_rowid")
                    .to_owned();
                let is_primary_key = forced.ret_type.as_ref().is_some_and(|field_type| {
                    field_type.has_flag(tidb_datatype::FieldTypeFlags::PRI_KEY)
                });
                self.columns.push(DataSourceColumn {
                    id: forced.id,
                    name,
                    is_primary_key,
                    is_not_null: forced.ret_type.as_ref().is_some_and(|field_type| {
                        field_type.has_flag(tidb_datatype::FieldTypeFlags::NOT_NULL)
                    }),
                });
                schema.columns.push(forced);
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
        emptied
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
        // Go `deriveStatsByFilter` (`pkg/planner/core/stats.go:576`) scales the
        // table profile by `cardinality.Selectivity` over the pushed-down
        // conditions. Master's walk turns `not(isnull(col))` into the column's
        // not-null range and counts it from the histogram, i.e. a ratio of
        // `(total - null_count) / total`; `isnull(col)` alone counts
        // `null_count / total`. Other condition shapes in this port keep the
        // unscaled table profile they had before this correction.
        let mut ratio = 1.0;
        for condition in &self.pushed_down_conds {
            if let Some(selectivity) =
                is_null_condition_selectivity(condition, &table_stats)
            {
                ratio *= selectivity;
            }
        }
        let scaled = if ratio < 1.0 {
            table_stats.scale_by_expect_cnt(table_stats.row_count() * ratio, 1.0)
        } else {
            table_stats.clone()
        };
        self.base.base.set_stats(Some(scaled.clone()));
        Some((scaled, self.all_conds.is_empty()))
    }
}

/// The isnull/not-isnull arm of Go master `cardinality.Selectivity`
/// (`pkg/planner/cardinality/selectivity.go`): master's ranger turns
/// `not(isnull(col))` into the column's not-null range and
/// `GetRowCountByColumnRanges` counts it from the histogram, so the condition
/// selects `(total - null_count) / total` rows; `isnull(col)` alone selects
/// `null_count / total`. Returns `None` for every other shape so the caller
/// keeps its previous estimate.
pub(crate) fn is_null_condition_selectivity(
    condition: &Expression,
    stats: &StatsInfo,
) -> Option<f64> {
    let Expression::ScalarFunction(function) = condition else {
        return None;
    };
    let name = function.func_name.lowercase();
    let inner = if name == "not" {
        match function.get_args().first()? {
            Expression::ScalarFunction(inner) => inner,
            _ => return None,
        }
    } else if name == "isnull" {
        function
    } else {
        return None;
    };
    if inner.func_name.lowercase() != "isnull" {
        return None;
    }
    let Expression::Column(column) = inner.get_args().first()? else {
        return None;
    };
    let hist_coll = stats.hist_coll()?;
    let column_stats = hist_coll.histogram(column.unique_id)?;
    let total = stats.row_count();
    if total <= 0.0 {
        return None;
    }
    if name == "not" {
        // Master `GetRowCountByColumnRanges` (`row_count_column.go:229`)
        // counts `not(isnull(col))` over the not-null range [MinNotNull,
        // MaxValue]. Both bounds sit outside the histogram's [min, max], so
        // the count runs through `Histogram.OutOfRangeRowCount`'s heuristic —
        // slightly below the raw table count (TPC-DS q6/q45: 1158371.5 over
        // customer).
        let ranges = vec![crate::cardinality::row_count_estimator::ColumnRange {
            low: Datum::MinNotNull,
            high: Datum::MaxValue,
            low_exclude: false,
            high_exclude: false,
        }];
        let estimate = crate::cardinality::row_count_estimator::get_column_row_count(
            column_stats,
            &ranges,
            Collation::Binary,
            total as i64,
            0,
            false,
            crate::cardinality::row_count_estimator::EstimatorOptions::default(),
        );
        Some((estimate.est / total).clamp(0.0, 1.0))
    } else {
        let null_ratio =
            (column_stats.histogram.null_count as f64 / total).clamp(0.0, 1.0);
        Some(null_ratio)
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
            sample_info: self.sample_info.clone(),
            partition_def_idx: self.partition_def_idx,
            partition_names: self.partition_names.clone(),
            partition_definition_names: self.partition_definition_names.clone(),
            partition_definition_ids: self.partition_definition_ids.clone(),
            dynamic_partition_access: self.dynamic_partition_access.clone(),
            columns: self.columns.clone(),
            table_columns: self.table_columns.clone(),
            pushed_down_conds: self.pushed_down_conds.clone(),
            all_conds: self.all_conds.clone(),
            enumerated_paths: self.enumerated_paths.clone(),
            public_enumerated_paths: self.public_enumerated_paths.clone(),
            ast_index_hints: self.ast_index_hints.clone(),
            index_hints: self.index_hints.clone(),
            indexes: self.indexes.clone(),
            all_possible_access_paths: self.all_possible_access_paths.clone(),
            possible_access_paths: self.possible_access_paths.clone(),
            pk_is_handle: self.pk_is_handle,
            fd_latest_public_index_ids: self.fd_latest_public_index_ids.clone(),
            fd_latest_index_lookup_failed: self.fd_latest_index_lookup_failed,
            is_common_handle: self.is_common_handle,
            common_handle_version: self.common_handle_version,
            is_temporary: self.is_temporary,
            is_cached: self.is_cached,
            has_affinity: self.has_affinity,
            index_lookup_push_down_session: self.index_lookup_push_down_session,
            tikv_in_isolation_read: self.tikv_in_isolation_read,
            isolation_read_engines_value: self.isolation_read_engines_value.clone(),
            handle_cols: self.handle_cols.clone(),
            handle_is_int: self.handle_is_int,
            common_handle_cols: self.common_handle_cols.clone(),
            common_handle_lens: self.common_handle_lens.clone(),
            prefer_store_type: self.prefer_store_type,
            prefer_partitions: self.prefer_partitions.clone(),
            is_for_update_read: self.is_for_update_read,
            contain_expr_prefix_uk: self.contain_expr_prefix_uk,
            cols_requiring_full_len: self.cols_requiring_full_len.clone(),
            access_path_min_selectivity: self.access_path_min_selectivity,
            asked_column_group: self.asked_column_group.clone(),
            interesting_columns: self.interesting_columns.clone(),
            forced_index_ids: self.forced_index_ids.clone(),
            force_keep_order_index_ids: self.force_keep_order_index_ids.clone(),
            force_no_keep_order_index_ids: self.force_no_keep_order_index_ids.clone(),
            force_keep_order_table_path: self.force_keep_order_table_path,
            force_no_keep_order_table_path: self.force_no_keep_order_table_path,
            index_lookup_push_down_by: self.index_lookup_push_down_by.clone(),
            partial_index_noncacheable_ids: self.partial_index_noncacheable_ids.clone(),
            force_no_index_lookup_push_down: self.force_no_index_lookup_push_down,
            index_merge_hints: self.index_merge_hints.clone(),
            prefer_index_merge_by_fix_control: self.prefer_index_merge_by_fix_control,
            table_stats: self.table_stats.clone(),
            table_path_count_after_access: self.table_path_count_after_access,
            index_path_single_scan: self.index_path_single_scan.clone(),
            index_path_count_after_access: self.index_path_count_after_access.clone(),
            index_path_row_estimates: self.index_path_row_estimates.clone(),
            analyzed_index_ids: self.analyzed_index_ids.clone(),
            table_scan_penalty: self.table_scan_penalty,
            has_tiflash_replica: self.has_tiflash_replica,
        }
    }
}

/// Go `DataSource.IsSingleScan`: parent outputs require complete values;
/// a direct IS NULL argument can use a prefix when the session allows it.
pub(crate) fn index_path_is_single_scan(
    ds: &crate::logical::DataSource,
    source_index: &crate::plan_builder::catalog::SourceIndex,
    opt_prefix_index_single_scan: bool,
) -> bool {
    if opt_prefix_index_single_scan {
        if let Some(required) = &ds.cols_requiring_full_len {
            return required
                .iter()
                .all(|column| index_covers_expression_column(ds, source_index, column, false))
                && ds
                    .all_conds
                    .iter()
                    .all(|condition| index_covers_condition(ds, source_index, condition, true));
        }
    }
    (0..ds.columns.len()).all(|position| index_covers_column(ds, source_index, position, false))
}

fn index_covers_expression_column(
    ds: &crate::logical::DataSource,
    index: &crate::plan_builder::catalog::SourceIndex,
    column: &tidb_expr::column::Column,
    ignore_len: bool,
) -> bool {
    ds.base
        .base
        .schema()
        .and_then(|schema| {
            schema
                .columns
                .iter()
                .position(|candidate| candidate.unique_id == column.unique_id)
        })
        .is_some_and(|position| index_covers_column(ds, index, position, ignore_len))
}

pub(crate) fn index_covers_condition(
    ds: &crate::logical::DataSource,
    index: &crate::plan_builder::catalog::SourceIndex,
    condition: &tidb_expr::expression::Expression,
    opt_prefix_index_single_scan: bool,
) -> bool {
    use tidb_expr::expression::Expression;
    match condition {
        Expression::Column(column) => index_covers_expression_column(ds, index, column, false),
        Expression::ScalarFunction(function) => {
            if opt_prefix_index_single_scan && function.func_name.lowercase() == "isnull" {
                if let [Expression::Column(column)] = function.args.as_slice() {
                    return index_covers_expression_column(ds, index, column, true);
                }
            }
            function.args.iter().all(|argument| {
                index_covers_condition(ds, index, argument, opt_prefix_index_single_scan)
            })
        }
        Expression::Constant(_) | Expression::CorrelatedColumn(_) => true,
    }
}

fn index_covers_column(
    ds: &crate::logical::DataSource,
    source_index: &crate::plan_builder::catalog::SourceIndex,
    position: usize,
    ignore_len: bool,
) -> bool {
    let Some(column) = ds.columns.get(position) else {
        return false;
    };
    if (ds.pk_is_handle && column.is_primary_key)
        || column.id == tidb_model::column::EXTRA_HANDLE_ID
        || column.id == tidb_model::column::EXTRA_PHYS_TBL_ID
    {
        return true;
    }
    let schema_column = ds
        .base
        .base
        .schema()
        .and_then(|schema| schema.columns.get(position));
    let field_type = schema_column.and_then(|column| column.ret_type.as_ref());
    let full_length = |length: i64| {
        ignore_len
            || length == tidb_datatype::UNSPECIFIED_LENGTH
            || field_type.is_some_and(|field_type| length == field_type.flen())
    };
    if source_index.columns.iter().any(|index_column| {
        full_length(index_column.length) && index_column.name.eq_ignore_ascii_case(&column.name)
    }) {
        return true;
    }
    let covered_by_handle = schema_column.is_some_and(|column| {
        ds.common_handle_cols
            .iter()
            .zip(&ds.common_handle_lens)
            .any(|(handle, length)| handle.unique_id == column.unique_id && full_length(*length))
    });
    covered_by_handle
        && !(ds.common_handle_version == 0
            && tidb_datatype::new_collation_enabled()
            && field_type.is_some_and(|field_type| {
                field_type.eval_type() == tidb_datatype::EvalType::String
                    && !field_type.has_flag(tidb_datatype::FieldTypeFlags::BINARY)
            }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stats_info::HistColl;
    use std::sync::Arc;
    use tidb_ast::CiString;
    use tidb_datatype::{FieldType, FieldTypeCode};
    use tidb_expr::scalar_function::ScalarFunction;
    use tidb_stats::histogram::{Bucket, Histogram};

    fn call(name: &str, arguments: Vec<Expression>) -> Expression {
        Expression::ScalarFunction(ScalarFunction::new(
            CiString::new(name),
            FieldType::new(FieldTypeCode::LongLong),
            arguments,
        ))
    }

    fn profile(row_count: f64, null_count: i64) -> StatsInfo {
        // One bucket covering values [1, 1000] with 900 rows; the not-null
        // range estimate for such a histogram runs through the same
        // OutOfRangeRowCount heuristic master applies to
        // `not(isnull(col))`'s [MinNotNull, MaxValue] range.
        let hist_coll = HistColl::new(false, row_count as i64, std::iter::empty())
            .with_histograms([(
                1,
                Arc::new(crate::cardinality::row_count_estimator::ColumnStats {
                    histogram: Histogram {
                        id: 1,
                        ndv: 900,
                        null_count,
                        last_update_version: 0,
                        tot_col_size: 0,
                        correlation: 0.0,
                        buckets: vec![Bucket {
                            count: row_count as i64 - null_count,
                            repeat: 0,
                            ndv: 900,
                            lower_bound: Datum::Int(1),
                            upper_bound: Datum::Int(1000),
                        }],
                    },
                    topn: None,
                    cms: None,
                    stats_ver: 2,
                    unsigned: false,
                }),
            )]);
        StatsInfo::new(row_count, std::iter::empty()).with_hist_coll(hist_coll)
    }

    #[test]
    fn not_isnull_selects_the_histogram_not_null_ratio() {
        // Go master `deriveStatsByFilter` scales the datasource profile by
        // `cardinality.Selectivity` over the pushed conditions; master counts
        // `not(isnull(col))` as the column's not-null range, i.e.
        // (total - null_count) / total, and `isnull(col)` alone as
        // null_count / total.
        let stats = profile(1_000.0, 100);
        let column = Column::new(1, FieldType::new(FieldTypeCode::LongLong));
        let condition = call(
            "not",
            vec![call("isnull", vec![Expression::Column(column.clone())])],
        );
        let selectivity = is_null_condition_selectivity(&condition, &stats)
            .expect("not(isnull(col)) is an isnull-class condition");
        // Master counts the condition over the not-null range through
        // `Histogram.OutOfRangeRowCount`; the wiring must answer the same
        // column-estimator value.
        let column_stats = stats
            .hist_coll()
            .and_then(|h| h.histogram(column.unique_id))
            .expect("column histogram");
        let ranges = vec![crate::cardinality::row_count_estimator::ColumnRange {
            low: Datum::MinNotNull,
            high: Datum::MaxValue,
            low_exclude: false,
            high_exclude: false,
        }];
        let estimate = crate::cardinality::row_count_estimator::get_column_row_count(
            column_stats,
            &ranges,
            Collation::Binary,
            1000,
            0,
            false,
            crate::cardinality::row_count_estimator::EstimatorOptions::default(),
        );
        let expected = (estimate.est / 1000.0).clamp(0.0, 1.0);
        assert!(
            (selectivity - expected).abs() < 1e-9,
            "selectivity {selectivity} vs estimator {expected}"
        );
        assert!(selectivity > 0.0 && selectivity <= 1.0);

        let condition = call("isnull", vec![Expression::Column(column)]);
        let selectivity = is_null_condition_selectivity(&condition, &stats)
            .expect("isnull(col) is an isnull-class condition");
        assert!((selectivity - 0.1).abs() < 1e-9);
    }

    #[test]
    fn other_condition_shapes_keep_the_previous_estimate() {
        // Only the isnull family is covered: every other shape answers None so
        // the caller keeps the unscaled table profile it used before.
        let stats = profile(1_000.0, 100);
        let unknown_column = call(
            "not",
            vec![call(
                "isnull",
                vec![Expression::Column(Column::new(
                    99,
                    FieldType::new(FieldTypeCode::LongLong),
                ))],
            )],
        );
        assert_eq!(is_null_condition_selectivity(&unknown_column, &stats), None);

        let plain_column = call(
            "isnull",
            vec![Expression::Constant(
                tidb_expr::constant::Constant::new(
                    tidb_datatype::Datum::Int(1),
                    FieldType::new(FieldTypeCode::LongLong),
                ),
            )],
        );
        assert_eq!(is_null_condition_selectivity(&plain_column, &stats), None);
    }
}
