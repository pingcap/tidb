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
//! SEED of `pkg/planner/core`. The access paths are
//! `tidb_planner_util::path`'s, enumerated into
//! [`DataSource::all_possible_access_paths`] and derived into
//! [`DataSource::possible_access_paths`].
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
//!   common handle. [`tidb_planner_util::handle_cols`] already models both identities;
//!   this operator holds the handle COLUMNS, which is what the ported bodies
//!   need, plus [`DataSource::handle_is_int`].

use tidb_planner_property::stats_info::HistCollExt as _;
#[cfg(test)]
use tidb_datatype::Datum;
use tidb_charset::Collation;
use tidb_expr::column::Column;
use tidb_expr::expr_util::normal_form::split_cnf_items;
use tidb_expr::expression::{CorrelatedColumn, Expression};
use tidb_expr::rewriter::ColumnResolver;
use tidb_expr::schema::Schema;
use tidb_expr::simple_expr::{
    extract_columns_from_expressions, extract_cor_columns, parse_simple_expr, BuildOptions,
};

use crate::schema_producer;
use crate::BaseLogicalPlan;
use tidb_planner_core_base::PossiblePropertiesInfo;
use tidb_planner_property::stats_info::StatsInfo;

/// Go `model.ExtraHandleID`: the id of the implicit `_tidb_rowid` column.
pub const EXTRA_HANDLE_ID: i64 = -1;

/// Go `DataSource.Columns`' element: the table's shared `*model.ColumnInfo`.
pub type DataSourceColumn = tidb_model::GoShared<tidb_model::ColumnInfo>;

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

/// One entry of Go's `PossibleAccessPaths` after derivation. Go's list holds
/// pointers: an ordinary path is the same object `AllPossibleAccessPaths`
/// holds, an index-merge path one only this list holds.
#[derive(Clone, Debug)]
pub enum PossibleAccessPath {
    /// The `AllPossibleAccessPaths` entry at this position.
    Shared(usize),
    /// An index-merge path `generateIndexMergePath` added.
    IndexMerge(tidb_planner_util::path::AccessPath),
}

/// Go `logicalop.DataSource` (`logical_datasource.go:58`).
#[derive(Clone, Debug, Default)]
pub struct DataSource {
    /// The shared logical base.
    pub base: BaseLogicalPlan,
    /// Go `DataSource.TableInfo`: the table's shared `*model.TableInfo`.
    pub table_info: Option<tidb_model::GoShared<tidb_model::TableInfo>>,
    /// Go `TableAsName`: the `AS` alias, when the query gave one.
    pub table_as_name: Option<String>,
    /// Go `DBName`.
    pub db_name: String,
    /// Go `PhysicalTableID`: the partition's id, or the table's.
    pub physical_table_id: i64,
    /// Go `SampleInfo`.
    pub sample_info: Option<tidb_planner_tablesampler::TableSampleInfo>,
    /// Go `PartitionDefIdx`: which partition definition this reads.
    pub partition_def_idx: Option<usize>,
    /// Go `PartitionNames`: an explicit `PARTITION (p, ...)` restriction.
    pub partition_names: Vec<String>,
    /// Go `PhysPlanPartInfo` after dynamic partition pruning, retained so
    /// reader access objects report the partitions selected by the same
    /// predicates used for execution.
    pub dynamic_partition_access: Option<tidb_planner_core_access::DynamicPartitionAccessObject>,
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
    /// Go `AllPossibleAccessPaths`: `getPossibleAccessPaths`' answer, grown
    /// in place by plan building and statistics derivation. Go's
    /// `PossibleAccessPaths` equals it until derivation adds the index-merge
    /// paths ([`Self::possible_access_paths`]).
    pub all_possible_access_paths: Vec<tidb_planner_util::path::AccessPath>,
    /// The unfiltered public paths from which static partition children rerun
    /// Go `getPossibleAccessPaths` after partition-scoped hint selection.
    pub public_enumerated_paths: Vec<tidb_planner_util::path::AccessPath>,
    /// Go `AstIndexHints`: table-syntax hints, which apply to every child.
    pub ast_index_hints: Vec<tidb_ast::IndexHint>,
    /// Go `IndexHints`: comment-style hints matched to this DataSource.
    pub index_hints: Vec<DataSourceIndexHint>,
    /// Latest-schema public index IDs used by `ExtractFD` for a connected
    /// `FOR UPDATE` read when the domain schema changed.
    pub fd_latest_public_index_ids: Option<std::collections::BTreeSet<i64>>,
    /// Go `DataSource.ExtractFD` returns the PK-only set when its latest-index
    /// lookup fails.
    pub fd_latest_index_lookup_failed: bool,
    /// Go `ds.Table.Type().IsClusterTable()`: the scan reads the TiDB store.
    pub is_cluster_table: bool,
    /// Session/transaction inputs to Go's lookup-pushdown support check.
    pub index_lookup_push_down_session: tidb_sessionctx::IndexLookupPushDownSession,
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
    /// Go `forceNoIndexLookUpPushDown`, set by a matching
    /// `NO_INDEX_LOOKUP_PUSHDOWN` hint before paths are enumerated.
    pub force_no_index_lookup_push_down: bool,
    /// Go `IndexMergeHints`. An empty `index_names` list is a general
    /// `USE_INDEX_MERGE(table)` hint; `partitions` scopes it per static child.
    pub index_merge_hints: Vec<DataSourceIndexMergeHint>,
    /// Go fix control 52869 after session-variable resolution.
    pub prefer_index_merge_by_fix_control: bool,
    /// Go `generateOtherIndexMerge`'s `skipRangeScanCheck`: fix control
    /// 52869, true when unset.
    pub index_merge_skip_range_scan_check: bool,
    /// Go `TableStats`: the table-level profile before any filtering.
    pub table_stats: Option<StatsInfo>,
    /// Go `PossibleAccessPaths` after statistics derivation: the grown paths
    /// plus the index-merge paths `generateIndexMergePath` adds, which
    /// physical search consumes; read through
    /// [`Self::possible_paths`]. None until derived.
    pub possible_access_paths: Option<Vec<PossibleAccessPath>>,
    /// Go `StatisticTable`: the table's statistics `initStats` attached, if
    /// any (None is Go's pseudo table).
    pub statistic_table: Option<std::sync::Arc<tidb_stats::Table>>,
    /// The table/session facts that Go retains on `PhysicalTableScan` for
    /// `getTableScanPenalty`.
    pub table_scan_penalty: TableScanPenaltyInput,
    /// Whether the table has an available TiFlash replica; Go computes this
    /// through `TableInfo.TiFlashReplica` plus the hypothetical-replica
    /// session state, both of which are outside this crate.
    pub has_tiflash_replica: bool,
}


/// The inputs `getTableScanPenalty` reads off the scan and the session.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct TableScanPenaltyInput {
    /// Whether the scan carries `RangeInfo` (an index-join probe range).
    pub has_range_info: bool,
    /// `tidb_opt_prefer_range_scan`.
    pub allow_prefer_range_scan: bool,
    /// Whether the table's histogram collection is pseudo.
    pub pseudo_stats: bool,
    /// `GetAnalyzeRowCount()` truncated to an integer, as Go does.
    pub analyze_row_count: i64,
    /// The table's modify count.
    pub modify_count: i64,
    /// Whether partition pruning conditions exist (a partition-level scan).
    pub has_partition_scan: bool,
    /// Whether the statement used `USE`/`FORCE INDEX`.
    pub has_index_force: bool,
    /// Go `getPlanCostVer24PhysicalTableScan`'s `unsignedIntHandle` (an
    /// unsigned integer primary key is the handle), which
    /// `ranger.HasFullRange` reads: such a table's full range starts at 0.
    pub unsigned_int_handle: bool,
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
        for index in &self.table_indices() {
            if !index.read().unique || !index.read().is_public() {
                continue;
            }
            if self
                .fd_latest_public_index_ids
                .as_ref()
                .is_some_and(|indexes| !indexes.contains(&index.read().id))
            {
                continue;
            }
            let (nullable_key, strong_key) = tidb_planner_core_rule_util::check_index_can_be_key(
                index,
                &self.columns,
                self_schema,
            );
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

    /// Resolves an index column after logical column pruning. SessionExtendedInfoSchema index
    /// offsets address the original table column list, while `columns` and
    /// the logical schema are pruned together; the column name is the stable
    /// identity between those two layouts.
    #[must_use]
    pub fn schema_column_for_index_column(
        &self,
        index_column: &tidb_model::IndexColumn,
    ) -> Option<&Column> {
        let position = self
            .columns
            .iter()
            .position(|column| tidb_mysql::lowercase_equal(column.read().name.original(), index_column.name.original()))?;
        self.base.base.schema()?.columns.get(position)
    }

    /// Go `IndexInfo2FullCols`: retain unresolved positions and normalize
    /// full-length prefixes before either pruning or range construction.
    #[must_use]
    pub fn index_info_2_full_cols(
        &self,
        index: &tidb_model::GoShared<tidb_model::IndexInfo>,
    ) -> Vec<Option<(Column, i64)>> {
        index
            .read().columns
            .iter_deref()
            .map(|key| {
                let key = key.read();
                self.schema_column_for_index_column(&key)
                    .cloned()
                    .map(|column| {
                        let length = if column
                            .ret_type
                            .as_ref()
                            .is_some_and(|ty| ty.flen() == key.length)
                        {
                            tidb_datatype::UNSPECIFIED_LENGTH
                        } else {
                            key.length
                        };
                        (column, length)
                    })
            })
            .collect()
    }

    /// The first path over index `id` -- a secondary index's own path, or a
    /// clustered table's TiKV path for its PRIMARY index -- whose fields Go
    /// grows in place.
    #[must_use]
    pub fn path_by_index_id(&self, id: i64) -> Option<&tidb_planner_util::path::AccessPath> {
        self.all_possible_access_paths
            .iter()
            .find(|path| path.index.as_ref().is_some_and(|index| index.read().id == id))
    }

    /// Go `TableInfo.GetPartitionInfo().Definitions[i].ID`.
    #[must_use]
    pub fn partition_definition_ids(&self) -> Vec<i64> {
        self.partition_definitions(|definition| definition.id)
    }

    /// Go `TableInfo.GetPartitionInfo().Definitions[i].Name.O`.
    #[must_use]
    pub fn partition_definition_names(&self) -> Vec<String> {
        self.partition_definitions(|definition| definition.name.original().to_owned())
    }

    fn partition_definitions<U>(&self, map: impl FnMut(&tidb_model::PartitionDefinition) -> U) -> Vec<U> {
        self.table_info
            .as_ref()
            .and_then(|info| info.read().get_partition_info())
            .map(|partition| partition.read().definitions.map_visible(map))
            .unwrap_or_default()
    }

    /// Sets the partition definitions' ids on a copy of `TableInfo` (fixtures).
    pub fn set_partition_definition_ids(&mut self, ids: Vec<i64>) {
        self.update_partition_definitions(ids.len(), |definition, position| definition.id = ids[position]);
    }

    /// Sets the partition definitions' names on a copy of `TableInfo` (fixtures).
    pub fn set_partition_definition_names(&mut self, names: Vec<String>) {
        self.update_partition_definitions(names.len(), |definition, position| {
            definition.name = tidb_ast::CiString::new(names[position].clone());
        });
    }

    fn update_partition_definitions(
        &mut self,
        len: usize,
        mut update: impl FnMut(&mut tidb_model::PartitionDefinition, usize),
    ) {
        self.update_table_info(|info| {
            let mut partition = info
                .partition
                .as_ref()
                .map(|partition| partition.read().clone())
                .unwrap_or_default();
            let mut definitions = partition.definitions.snapshot();
            definitions.resize_with(len, Default::default);
            for (position, definition) in definitions.iter_mut().enumerate() {
                update(definition, position);
            }
            partition.definitions = definitions.into();
            partition.enable = true;
            info.partition = Some(tidb_model::GoShared::new(partition));
        });
    }

    /// Go `TableInfo.ID`.
    #[must_use]
    pub fn table_id(&self) -> i64 {
        self.table_info.as_ref().map_or(0, |info| info.read().id)
    }

    /// Go `TableInfo.Name.O`.
    #[must_use]
    pub fn table_name(&self) -> String {
        self.table_info
            .as_ref()
            .map_or_else(String::new, |info| info.read().name.original().to_owned())
    }

    /// Sets `TableInfo.ID` on a copy.
    pub fn set_table_id(&mut self, id: i64) {
        self.update_table_info(|info| info.id = id);
    }

    /// Sets `TableInfo.Name` on a copy.
    pub fn set_table_name(&mut self, name: impl Into<String>) {
        let name = tidb_ast::CiString::new(name.into());
        self.update_table_info(|info| info.name = name);
    }

    /// Go `TableInfo.PKIsHandle`.
    #[must_use]
    pub fn pk_is_handle(&self) -> bool {
        self.table_info.as_ref().is_some_and(|info| info.read().pk_is_handle)
    }

    /// Go `TableInfo.IsCommonHandle`.
    #[must_use]
    pub fn is_common_handle(&self) -> bool {
        self.table_info.as_ref().is_some_and(|info| info.read().is_common_handle)
    }

    /// Go `TableInfo.CommonHandleVersion`.
    #[must_use]
    pub fn common_handle_version(&self) -> u16 {
        self.table_info.as_ref().map_or(0, |info| info.read().common_handle_version)
    }

    /// Go `TableInfo.TempTableType != model.TempTableNone`.
    #[must_use]
    pub fn is_temporary(&self) -> bool {
        self.table_info
            .as_ref()
            .is_some_and(|info| info.read().temp_table_type != tidb_model::TempTableType::NONE)
    }

    /// Go `TableInfo.TempTableType == model.TempTableLocal`.
    #[must_use]
    pub fn is_local_temporary(&self) -> bool {
        self.table_info
            .as_ref()
            .is_some_and(|info| info.read().temp_table_type == tidb_model::TempTableType::LOCAL)
    }

    /// Go `TableInfo.TableCacheStatusType != model.TableCacheStatusDisable`.
    #[must_use]
    pub fn is_cached(&self) -> bool {
        self.table_info.as_ref().is_some_and(|info| {
            info.read().table_cache_status_type != tidb_model::TableCacheStatusType::DISABLE
        })
    }

    /// Whether Go `TableInfo.Affinity` is non-nil.
    #[must_use]
    pub fn has_affinity(&self) -> bool {
        self.table_info.as_ref().is_some_and(|info| info.read().affinity.is_some())
    }

    /// Marks `TableInfo` temporary (GLOBAL unless already LOCAL), or not, on
    /// a copy.
    pub fn set_is_temporary(&mut self, value: bool) {
        self.update_table_info(|info| {
            if !value {
                info.temp_table_type = tidb_model::TempTableType::NONE;
            } else if info.temp_table_type == tidb_model::TempTableType::NONE {
                info.temp_table_type = tidb_model::TempTableType::GLOBAL;
            }
        });
    }

    /// Marks `TableInfo` a LOCAL temporary table, or a GLOBAL one when it was
    /// LOCAL, on a copy.
    pub fn set_is_local_temporary(&mut self, value: bool) {
        self.update_table_info(|info| {
            if value {
                info.temp_table_type = tidb_model::TempTableType::LOCAL;
            } else if info.temp_table_type == tidb_model::TempTableType::LOCAL {
                info.temp_table_type = tidb_model::TempTableType::GLOBAL;
            }
        });
    }

    /// Sets `TableInfo.TableCacheStatusType` enabled or disabled, on a copy.
    pub fn set_is_cached(&mut self, value: bool) {
        self.update_table_info(|info| {
            info.table_cache_status_type = if value {
                tidb_model::TableCacheStatusType::ENABLE
            } else {
                tidb_model::TableCacheStatusType::DISABLE
            };
        });
    }

    /// Gives `TableInfo` an `Affinity`, or clears it, on a copy.
    pub fn set_has_affinity(&mut self, value: bool) {
        self.update_table_info(|info| {
            info.affinity = value.then(|| tidb_model::GoShared::new(tidb_model::TableAffinityInfo::default()));
        });
    }

    /// Sets `TableInfo.PKIsHandle` on a copy (see [`Self::update_table_info`]).
    pub fn set_pk_is_handle(&mut self, value: bool) {
        self.update_table_info(|info| info.pk_is_handle = value);
    }

    /// Sets `TableInfo.IsCommonHandle` on a copy.
    pub fn set_is_common_handle(&mut self, value: bool) {
        self.update_table_info(|info| info.is_common_handle = value);
    }

    /// Sets `TableInfo.CommonHandleVersion` on a copy.
    pub fn set_common_handle_version(&mut self, value: u16) {
        self.update_table_info(|info| info.common_handle_version = value);
    }

    /// Points `TableInfo` at an updated copy, for a fixture or a synthesized
    /// table: the shared `TableInfo` other occurrences hold is never written
    /// through.
    pub fn update_table_info(&mut self, update: impl FnOnce(&mut tidb_model::TableInfo)) {
        let mut info = self
            .table_info
            .as_ref()
            .map(|info| info.read().clone())
            .unwrap_or_default();
        update(&mut info);
        self.table_info = Some(tidb_model::GoShared::new(info));
    }

    /// Go `PossibleAccessPaths` after derivation, each entry the live path:
    /// an ordinary one is the very `AllPossibleAccessPaths` entry, as Go's
    /// two lists share pointers. None until derived.
    #[must_use]
    pub fn possible_paths(&self) -> Option<Vec<&tidb_planner_util::path::AccessPath>> {
        let paths = self.possible_access_paths.as_ref()?;
        Some(
            paths
                .iter()
                .map(|path| match path {
                    PossibleAccessPath::Shared(position) => &self.all_possible_access_paths[*position],
                    PossibleAccessPath::IndexMerge(path) => path,
                })
                .collect(),
        )
    }

    /// Go `ds.TableInfo.Indices`.
    #[must_use]
    pub fn table_indices(&self) -> Vec<tidb_model::GoShared<tidb_model::IndexInfo>> {
        self.table_info
            .as_ref()
            .map(|info| info.read().indices.iter_deref().collect())
            .unwrap_or_default()
    }

    /// Points `ds.TableInfo` at a copy whose `Indices` are `indexes`, for a
    /// fixture without a catalog. The shared `TableInfo` is never written
    /// through: other occurrences keep theirs.
    pub fn set_table_indices(&mut self, indexes: Vec<tidb_model::GoShared<tidb_model::IndexInfo>>) {
        let mut info = self
            .table_info
            .as_ref()
            .map(|info| info.read().clone())
            .unwrap_or_default();
        info.indices =
            tidb_model::GoSharedPointerSlice::from_handles(indexes.into_iter().map(Some).collect());
        self.table_info = Some(tidb_model::GoShared::new(info));
    }

    /// Go `ds.StatisticTable.ColAndIdxExistenceMap.HasAnalyzed(id, true)`,
    /// independent of payload eviction.
    #[must_use]
    pub fn index_has_analyzed(&self, id: i64) -> bool {
        self.statistic_table
            .as_ref()
            .and_then(|table| table.existence_map.as_ref())
            .is_some_and(|map| {
                map.read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .has_analyzed(id, true)
            })
    }

    /// The TiKV table path (Go `IsTiKVTablePath`).
    #[must_use]
    pub fn tikv_table_path(&self) -> Option<&tidb_planner_util::path::AccessPath> {
        self.all_possible_access_paths.iter().find(|path| path.is_tikv_table_path())
    }

    /// [`Self::path_by_index_id`], for updating the path in place.
    pub fn path_by_index_id_mut(
        &mut self,
        id: i64,
    ) -> Option<&mut tidb_planner_util::path::AccessPath> {
        self.all_possible_access_paths
            .iter_mut()
            .find(|path| path.index.as_ref().is_some_and(|index| index.read().id == id))
    }

    /// Go plan building initializes full declared keys before pruning and
    /// statistics loading; ranger resolves the current schema again later.
    pub fn initialize_index_columns(&mut self) {
        for position in 0..self.all_possible_access_paths.len() {
            let path = &self.all_possible_access_paths[position];
            // Go: every path but an int-handle one (`!path.IsIntHandlePath`).
            let Some(index) = path.index.clone() else {
                continue;
            };
            let columns = self.index_info_2_full_cols(&index);
            // Go `buildDataSource` (`logical_plan_builder.go:5316`): only a
            // unique secondary index of more than one column whose first
            // column is a `tidb_shard()` generated column.
            let is_uk_shard_index_path =
                !path.is_table_path()
                    && columns
                        .first()
                        .and_then(Option::as_ref)
                        .is_some_and(|(column, _)| is_shard_column(column))
                    && index.read().columns.len() > 1
                    && index.read().unique;
            self.contain_expr_prefix_uk |= is_uk_shard_index_path;
            let path = &mut self.all_possible_access_paths[position];
            path.full_idx_cols = Some(columns);
            path.is_uk_shard_index_path = is_uk_shard_index_path;
        }
    }

    /// Go `addPrefix4ShardIndexes` (`rule_predicate_push_down.go:62`): for
    /// every shard index path, in path order, `WHERE a = 10` gains
    /// `tidb_shard(a) = tidb_shard(10)` and `a IN (10, 20)` becomes the
    /// OR of such pairs. A failed rewrite keeps the original conditions.
    #[must_use]
    pub fn add_prefix_4_shard_indexes(
        &self,
        conds: Vec<Expression>,
        regard_null_as_point: bool,
    ) -> Vec<Expression> {
        if !self.contain_expr_prefix_uk {
            return conds;
        }
        let mut new_conds = conds.clone();
        for path in &self.all_possible_access_paths {
            if !path.is_uk_shard_index_path {
                continue;
            }
            let Some(index) = path.index_info() else {
                continue;
            };
            match self.add_expr_prefix_cond(index, new_conds, regard_null_as_point) {
                Ok(rewritten) => new_conds = rewritten,
                Err(_) => return conds,
            }
        }
        new_conds
    }

    /// Go `addExprPrefixCond`: the index's prefix columns
    /// (`IndexInfo2PrefixCols`) drive `exprPrefixAdder.addExprPrefix4ShardIndex`.
    fn add_expr_prefix_cond(
        &self,
        index: &tidb_model::GoShared<tidb_model::IndexInfo>,
        conds: Vec<Expression>,
        regard_null_as_point: bool,
    ) -> Result<Vec<Expression>, tidb_ranger::points::PointBuilderError> {
        let idx_cols: Vec<Column> = self
            .index_info_2_full_cols(index)
            .into_iter()
            .map_while(|column| column.map(|(column, _)| column))
            .collect();
        if idx_cols.is_empty() {
            return Ok(conds);
        }
        // Go `addExprPrefix4ShardIndex`: a lone OR condition is rewritten
        // per DNF item, anything else as one CNF list.
        if let [Expression::ScalarFunction(function)] = conds.as_slice() {
            if function.func_name.lowercase() == "or" {
                return Ok(add_expr_prefix4_dnf_cond(
                    function,
                    &idx_cols,
                    regard_null_as_point,
                ));
            }
        }
        tidb_ranger::detacher::add_expr4_eq_and_in_condition(
            &conds,
            &idx_cols,
            regard_null_as_point,
        )
    }

    /// Go `recordUsedItemStatsStatus` for one column or index of this source:
    /// a `PseudoTable` holds the public, non-hidden columns and the public
    /// indexes.
    pub fn record_used_item(
        &self,
        recorder: Option<&dyn crate::plan_context::UsedStatsRecorder>,
        item_id: i64,
        is_index: bool,
    ) {
        let Some(recorder) = recorder else {
            return;
        };
        let in_pseudo_table = if is_index {
            self.table_indices()
                .iter()
                .any(|index| index.read().id == item_id && index.read().is_public())
        } else {
            self.table_columns
                .iter()
                .any(|column| column.id == item_id && !column.is_hidden)
        };
        recorder.record_used_item(
            self.physical_table_id,
            &self.table_name(),
            in_pseudo_table,
            item_id,
            is_index,
        );
    }

    /// Go `deriveStatsByFilter(ds, conds, ds.AllPossibleAccessPaths)`'s
    /// `filledPaths` (`stats.go:577`): every possible index path with its
    /// `IdxCols`, which `fillIndexPath` has just filled (the collection's
    /// `Idx2ColUniqueIDs`, handle included); and a common-handle table path's
    /// PRIMARY index with none, because Go fills table paths only after this
    /// estimate. The PRIMARY index therefore contributes no `Selectivity`
    /// node to the data source's own row count.
    #[must_use]
    pub fn derive_stats_filled_paths(&self) -> tidb_planner_cardinality::selectivity::FilledPaths {
        let hist_coll = self.table_stats.as_ref().and_then(StatsInfo::hist_coll);
        let mut paths = tidb_planner_cardinality::selectivity::FilledPaths::new();
        for path in &self.all_possible_access_paths {
            // Go `Selectivity` keys every path with an `Index` (`selectivity.go:145`).
            let Some(index) = &path.index else {
                continue;
            };
            let columns = if path.is_table_path() {
                Vec::new()
            } else {
                // The path's `IdxCols`: the schema columns behind the
                // collection's ids, a prefix up to the first absent one.
                let schema = self.base.base.schema();
                hist_coll
                    .map(|coll| {
                        coll.index_columns(index.read().id)
                            .iter()
                            .map_while(|id| {
                                schema?
                                    .columns
                                    .iter()
                                    .find(|column| column.unique_id == *id)
                                    .cloned()
                            })
                            .collect()
                    })
                    .unwrap_or_default()
            };
            paths.insert(index.read().id, columns);
        }
        paths
    }

    /// Go `fillIndexPath`'s suffix (`stats.go:177-197`): a complete,
    /// non-unique secondary key gains the signed integer handle, in the path
    /// columns and in `HistColl.Idx2ColUniqueIDs`. A clustered common handle
    /// is never appended there; Go extends with it only while matching a
    /// sort property (`find_best_task.go:1105`).
    #[must_use]
    pub fn handle_cols_to_append(
        &self,
        index: &tidb_model::GoShared<tidb_model::IndexInfo>,
        declared: &[(Column, i64)],
    ) -> Vec<(Column, i64)> {
        if index.read().unique || index.read().primary || declared.len() != index.read().columns.len() {
            return Vec::new();
        }
        let Some(handle) = self
            .base
            .base
            .schema()
            .and_then(|schema| self.get_pk_is_handle_col(schema))
        else {
            return Vec::new();
        };
        if handle.ret_type.as_ref().is_some_and(|ty| ty.is_unsigned())
            || declared.iter().any(|(column, _)| {
                column.id == EXTRA_HANDLE_ID || column.unique_id == handle.unique_id
            })
        {
            return Vec::new();
        }
        vec![(handle.clone(), tidb_datatype::UNSPECIFIED_LENGTH)]
    }

    /// Usable leading index columns plus Go's eligible appended handle suffix.
    /// This is the ranger layout; physical scan output may need more columns.
    #[must_use]
    pub fn index_range_columns(
        &self,
        index: &tidb_model::GoShared<tidb_model::IndexInfo>,
    ) -> Vec<(Column, i64)> {
        let mut columns = self
            .index_info_2_full_cols(index)
            .into_iter()
            .map_while(std::convert::identity)
            .collect::<Vec<_>>();
        let suffix = self.handle_cols_to_append(index, &columns);
        columns.extend(suffix);
        columns
    }

    /// Go `DataSource.Init(ctx, offset)` (`logical_datasource.go:155`).
    #[must_use]
    pub fn new(base: BaseLogicalPlan, table_id: i64, table_name: impl Into<String>) -> Self {
        Self {
            base,
            table_info: Some(tidb_model::GoShared::new(tidb_model::TableInfo {
                id: table_id,
                name: tidb_ast::CiString::new(table_name.into()),
                ..tidb_model::TableInfo::default()
            })),
            physical_table_id: table_id,
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
            Some(alias) if !alias.is_empty() => alias.clone(),
            _ => self.table_name(),
        };
        let mut buffer = format!("table:{table_name}");
        if let Some(idx) = self.partition_def_idx {
            if let Some(name) = self.partition_definition_names().get(idx) {
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
    /// `getPKIsHandleColFromSchema` (`:580`): the integer primary key column
    /// of a `PKIsHandle` table, otherwise the `_tidb_rowid` column when the
    /// data source still carries it. Either way it must have survived pruning.
    #[must_use]
    pub fn get_pk_is_handle_col<'a>(&self, self_schema: &'a Schema) -> Option<&'a Column> {
        let position = if self.pk_is_handle() {
            self.columns
                .iter()
                .position(|column| column.read().field_type.has_flag(tidb_datatype::FieldTypeFlags::PRI_KEY))?
        } else {
            self.columns
                .iter()
                .position(|column| column.read().id == EXTRA_HANDLE_ID)?
        };
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
    /// [`tidb_expr::infer_pushdown`] and [`tidb_expr::pushdown_catalog`].
    ///
    /// The caller has already run `Conds2TableDual` over `AllConds`, which is
    /// Go's order (`logical_datasource.go:366-369`): a constant-NULL predicate
    /// becomes a `TableDual` before the split, so a pushable `col > NULL`
    /// cannot hide the empty result.
    ///
    /// Returns the predicates the PARENT must still apply, which is Go's first
    /// return value.
    pub fn predicate_push_down_local(
        &mut self,
        predicates: Vec<Expression>,
        blacklist: &tidb_expr::infer_pushdown::ExprPushDownBlacklist,
        warnings: Option<&dyn tidb_exprctx::Columns>,
    ) -> Vec<Expression> {
        self.possible_access_paths = None;
        if predicates.is_empty() && !self.pushed_down_conds.is_empty() {
            // The deferred CTE seed re-optimisation re-runs the pushdown over
            // an already-optimised tree; such a re-visit carries no new
            // predicates and must NOT wipe the conditions an earlier pass
            // absorbed — they back this DataSource's row estimates (q30's
            // d_year filter showed full-table numbers when they were lost).
            return Vec::new();
        }
        self.all_conds = predicates;
        // Go's `PushDownExprs` reports each refused scalar function through
        // the session's `PushDownContext`: the statement's warnings under
        // EXPLAIN (`warnings`), the extra warnings otherwise.
        let report = |message: String| {
            if let Some(warnings) = warnings {
                warnings.append_warning(1105, &message);
            }
        };
        let (pushable, not_pushable): (Vec<_>, Vec<_>) =
            self.all_conds.iter().cloned().partition(|predicate| {
                tidb_expr::infer_pushdown::can_exprs_push_down_reporting(
                    std::slice::from_ref(predicate),
                    tidb_kv::store_type::StoreType::Unspecified,
                    blacklist,
                    &report,
                )
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
                    table: tidb_datatype::IdentifierMetadata::new(&self.table_name()),
                    column: tidb_datatype::IdentifierMetadata::new(column.read().name.original()),
                    ..tidb_datatype::FieldNameMetadata::default()
                })
            })
            .collect::<Vec<_>>();
        let options = BuildOptions::new().with_input_schema_and_names(schema, names);
        let mut removed_ids = std::collections::BTreeSet::new();
        let mut partial_index_used_hint = false;
        let mut has_partial_index = false;

        for path in &mut self.all_possible_access_paths {
            let Some(metadata) = path.index_info().cloned() else {
                continue;
            };
            if metadata.read().condition_expr_string.is_empty() {
                continue;
            }
            has_partial_index = true;
            let predicates = parse_simple_expr(resolver, &metadata.read().condition_expr_string, &options)
                .map(|expression| split_cnf_items(&expression));
            let Ok(predicates) = predicates else {
                removed_ids.insert(metadata.read().id);
                continue;
            };
            if !tidb_planner_core_partidx::check_constraints(
                opt_prefix_index_single_scan,
                &predicates,
                &self.pushed_down_conds,
            ) {
                removed_ids.insert(metadata.read().id);
                continue;
            }
            if path.forced {
                partial_index_used_hint = true;
            }
            if use_plan_cache
                && !tidb_planner_core_partidx::always_meet_constraints(&predicates, &self.pushed_down_conds)
            {
                path.noncacheable_reason = Some("IndexScan of partial index is uncacheable".to_owned());
            }
        }
        if !has_partial_index || (removed_ids.is_empty() && !partial_index_used_hint) {
            return;
        }

        self.possible_access_paths = None;
        self.all_possible_access_paths.retain(|path| match path.index_info() {
            Some(index) => {
                !removed_ids.contains(&index.read().id) && (!partial_index_used_hint || path.forced)
            }
            None => !partial_index_used_hint,
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

        // Go `originColumns := ds.Columns`.
        let origin_columns = self.columns.clone();
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
                    self.pk_is_handle().then(|| {
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
                // Go `preferKeyColumnFromTable`'s `*model.ColumnInfo`: the
                // column's own, from the columns before pruning or the table,
                // and `NewExtraHandleColInfo()` for the implicit row id.
                let info = origin_columns
                    .iter()
                    .find(|info| info.read().id == forced.id)
                    .cloned()
                    .or_else(|| {
                        self.table_info
                            .as_ref()
                            .and_then(|table| table.read().find_column_by_id(forced.id))
                    })
                    .unwrap_or_else(|| {
                        assert_eq!(
                            forced.id, EXTRA_HANDLE_ID,
                            "a forced column is the table's or its row id"
                        );
                        tidb_model::GoShared::new(tidb_model::ColumnInfo::new_extra_handle_col_info())
                    });
                self.columns.push(info);
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
        if !self.pk_is_handle() {
            return;
        }
        if let Some(position) = self.columns.iter().position(|column| column.read().field_type.has_flag(tidb_datatype::FieldTypeFlags::PRI_KEY)) {
            if let Some(column) = self_schema.columns.get(position) {
                self_schema.pk_or_uk.push(vec![column.clone()]);
            }
        }
    }
}

/// Test oracle for the isnull/not-isnull arm of Go master `cardinality.Selectivity`
/// (`pkg/planner/cardinality/selectivity.go`): master's ranger turns
/// `not(isnull(col))` into the column's not-null range and
/// `GetRowCountByColumnRanges` estimates it against `HistColl.RealtimeCount`
/// and `HistColl.ModifyCount`. `isnull(col)` alone uses the histogram's null
/// count. Production datasource derivation uses the shared Selectivity path;
/// this narrow helper is retained only for the range-estimator regression tests.
#[cfg(test)]
fn is_null_condition_selectivity(
    condition: &Expression,
    stats: &StatsInfo,
) -> Option<f64> {
    is_null_condition_selectivity_with_options(
        condition,
        stats,
        tidb_planner_cardinality::row_count_estimator::EstimatorOptions::default(),
    )
}

#[cfg(test)]
fn is_null_condition_selectivity_with_options(
    condition: &Expression,
    stats: &StatsInfo,
    options: tidb_planner_cardinality::row_count_estimator::EstimatorOptions,
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
    let column_stats = hist_coll.histogram_for_estimation(column.unique_id)?;
    let column_stats = column_stats
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let realtime_row_count = hist_coll.realtime_count;
    let total = realtime_row_count as f64;
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
        let ranges = vec![tidb_planner_cardinality::row_count_estimator::ColumnRange {
            low: Datum::MinNotNull,
            high: Datum::MaxValue,
            low_exclude: false,
            high_exclude: false,
        }];
        let estimate = tidb_planner_cardinality::row_count_estimator::get_column_row_count(
            &column_stats,
            &ranges,
            Collation::Binary,
            realtime_row_count,
            hist_coll.modify_count,
            false,
            &options,
        )
        .ok()?;
        Some((estimate.est / total).clamp(0.0, 1.0))
    } else {
        let null_ratio = (column_stats.histogram.null_count as f64 / total).clamp(0.0, 1.0);
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

/// Go `exprPrefixAdder.addExprPrefix4DNFCond`: an AND item is rewritten as
/// its CNF list, an EQ or IN item alone, and any other item kept; an error
/// in any item keeps the whole condition.
fn add_expr_prefix4_dnf_cond(
    condition: &tidb_expr::scalar_function::ScalarFunction,
    idx_cols: &[Column],
    regard_null_as_point: bool,
) -> Vec<Expression> {
    let original = || vec![Expression::ScalarFunction(condition.clone())];
    let mut new_access_items = Vec::new();
    for item in tidb_expr::expr_util::normal_form::flatten_dnf_conditions(condition) {
        let accesses = match &item {
            Expression::ScalarFunction(function) if function.func_name.lowercase() == "and" => {
                tidb_expr::expr_util::normal_form::flatten_cnf_conditions(function)
            }
            Expression::ScalarFunction(function)
                if matches!(function.func_name.lowercase(), "eq" | "in") =>
            {
                vec![item.clone()]
            }
            _ => {
                new_access_items.push(item);
                continue;
            }
        };
        match tidb_ranger::detacher::add_expr4_eq_and_in_condition(
            &accesses,
            idx_cols,
            regard_null_as_point,
        ) {
            Ok(accesses) => {
                new_access_items.extend(tidb_expr::simple_expr::compose_cnf_condition(accesses));
            }
            Err(_) => return original(),
        }
    }
    tidb_expr::simple_expr::compose_dnf_condition(new_access_items)
        .into_iter()
        .collect()
}

impl DataSource {
    /// This operator's own fields with NO children; see
    /// [`crate::LogicalPlan::clone_shallow`]. A `DataSource` is a
    /// leaf, so this differs from [`Clone`] only in dropping an empty vector.
    #[must_use]
    pub fn clone_shallow(&self) -> Self {
        Self {
            base: self.base.shell(),
            table_info: self.table_info.clone(),
            table_as_name: self.table_as_name.clone(),
            db_name: self.db_name.clone(),
            physical_table_id: self.physical_table_id,
            sample_info: self.sample_info.clone(),
            partition_def_idx: self.partition_def_idx,
            partition_names: self.partition_names.clone(),
            dynamic_partition_access: self.dynamic_partition_access.clone(),
            columns: self.columns.clone(),
            table_columns: self.table_columns.clone(),
            pushed_down_conds: self.pushed_down_conds.clone(),
            all_conds: self.all_conds.clone(),
            all_possible_access_paths: self.all_possible_access_paths.clone(),
            public_enumerated_paths: self.public_enumerated_paths.clone(),
            ast_index_hints: self.ast_index_hints.clone(),
            index_hints: self.index_hints.clone(),
            fd_latest_public_index_ids: self.fd_latest_public_index_ids.clone(),
            fd_latest_index_lookup_failed: self.fd_latest_index_lookup_failed,
            is_cluster_table: self.is_cluster_table,
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
            force_no_index_lookup_push_down: self.force_no_index_lookup_push_down,
            index_merge_hints: self.index_merge_hints.clone(),
            prefer_index_merge_by_fix_control: self.prefer_index_merge_by_fix_control,
            index_merge_skip_range_scan_check: self.index_merge_skip_range_scan_check,
            table_stats: self.table_stats.clone(),
            possible_access_paths: self.possible_access_paths.clone(),
            statistic_table: self.statistic_table.clone(),
            table_scan_penalty: self.table_scan_penalty,
            has_tiflash_replica: self.has_tiflash_replica,
        }
    }
}

/// Go `DataSource.IsSingleScan`: parent outputs require complete values;
/// a direct IS NULL argument can use a prefix when the session allows it.
pub fn index_path_is_single_scan(
    ds: &crate::DataSource,
    source_index: &tidb_model::GoShared<tidb_model::IndexInfo>,
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
    ds: &crate::DataSource,
    index: &tidb_model::GoShared<tidb_model::IndexInfo>,
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

/// Whether `index` covers every column `condition` reads (Go
/// `isIndexCoveringCondition` over the path's columns).
pub fn index_covers_condition(
    ds: &crate::DataSource,
    index: &tidb_model::GoShared<tidb_model::IndexInfo>,
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
    ds: &crate::DataSource,
    source_index: &tidb_model::GoShared<tidb_model::IndexInfo>,
    position: usize,
    ignore_len: bool,
) -> bool {
    let Some(column) = ds.columns.get(position) else {
        return false;
    };
    if (ds.pk_is_handle() && column.read().field_type.has_flag(tidb_datatype::FieldTypeFlags::PRI_KEY))
        || column.read().id == tidb_model::column::EXTRA_HANDLE_ID
        || column.read().id == tidb_model::column::EXTRA_PHYS_TBL_ID
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
    // Go `isIndexColsCoveringCol` matches by `EqualByExprAndID`: another
    // expression index's hidden column with the same expression is covered
    // too.
    if source_index.read().columns.iter_deref().any(|index_column| {
        let index_column = index_column.read();
        full_length(index_column.length)
            && (tidb_mysql::lowercase_equal(index_column.name.original(), column.read().name.original())
                || schema_column.is_some_and(|schema_column| {
                    ds.schema_column_for_index_column(&index_column)
                        .is_some_and(|indexed| schema_column.equal_by_expr_and_id_column(indexed))
                }))
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
        && !(ds.common_handle_version() == 0
            && tidb_datatype::new_collation_enabled()
            && field_type.is_some_and(|field_type| {
                field_type.eval_type() == tidb_datatype::EvalType::String
                    && !field_type.has_flag(tidb_datatype::FieldTypeFlags::BINARY)
            }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_planner_cardinality::derive_stats::StatsInfoScale;
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
        let hist_coll =
            tidb_planner_property::stats_info::loaded_hist_coll_for_test(false, row_count as i64, 0, [(1, tidb_stats::column::Column {
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
                        ..Default::default()
                    },
                    top_n: None,
                    cmsketch: None,
                    stats_version: 2,
                    ..Default::default()
                })], []);
        StatsInfo::new(row_count, std::iter::empty()).with_hist_coll(hist_coll)
    }

    #[test]
    fn not_isnull_uses_histogram_range_selectivity() {
        // Go master `deriveStatsByFilter` scales the datasource profile by
        // `cardinality.Selectivity` over the pushed conditions. The negative
        // predicate uses the histogram range estimate; `isnull(col)` uses its
        // null count.
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
        let hist_coll = stats.hist_coll().expect("histogram collection");
        let column_stats = hist_coll
            .histogram(column.unique_id)
            .expect("column histogram");
        let column_stats = &*column_stats.read().unwrap();
        let ranges = vec![tidb_planner_cardinality::row_count_estimator::ColumnRange {
            low: Datum::MinNotNull,
            high: Datum::MaxValue,
            low_exclude: false,
            high_exclude: false,
        }];
        let estimate = tidb_planner_cardinality::row_count_estimator::get_column_row_count(
            column_stats,
            &ranges,
            Collation::Binary,
            hist_coll.realtime_count,
            hist_coll.modify_count,
            false,
            &tidb_planner_cardinality::row_count_estimator::EstimatorOptions::default(),
        )
        .unwrap();
        let expected = (estimate.est / hist_coll.realtime_count as f64).clamp(0.0, 1.0);
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
    fn not_isnull_uses_histogram_realtime_count_after_expected_count_scaling() {
        let table_stats = profile(1_000.0, 100);
        // Go StatsInfo.ScaleByExpectCnt retains the HistColl; cardinality
        // selectivity still estimates against HistColl.RealtimeCount.
        let scaled_stats = table_stats.scale_by_expect_cnt(100.0, 1.0);
        let column = Column::new(1, FieldType::new(FieldTypeCode::LongLong));
        let condition = call(
            "not",
            vec![call("isnull", vec![Expression::Column(column.clone())])],
        );
        let actual = is_null_condition_selectivity(&condition, &scaled_stats)
            .expect("not(isnull(col)) is an isnull-class condition");

        let hist_coll = table_stats.hist_coll().expect("histogram collection");
        let column_stats = hist_coll
            .histogram(column.unique_id)
            .expect("column histogram");
        let column_stats = &*column_stats.read().unwrap();
        let ranges = [tidb_planner_cardinality::row_count_estimator::ColumnRange {
            low: Datum::MinNotNull,
            high: Datum::MaxValue,
            low_exclude: false,
            high_exclude: false,
        }];
        let estimate = tidb_planner_cardinality::row_count_estimator::get_column_row_count(
            column_stats,
            &ranges,
            Collation::Binary,
            hist_coll.realtime_count,
            hist_coll.modify_count,
            false,
            &tidb_planner_cardinality::row_count_estimator::EstimatorOptions::default(),
        )
        .unwrap();
        let expected = (estimate.est / hist_coll.realtime_count as f64).clamp(0.0, 1.0);
        let wrong_count_estimate = tidb_planner_cardinality::row_count_estimator::get_column_row_count(
            column_stats,
            &ranges,
            Collation::Binary,
            scaled_stats.row_count() as i64,
            hist_coll.modify_count,
            false,
            &tidb_planner_cardinality::row_count_estimator::EstimatorOptions::default(),
        )
        .unwrap();
        let wrong_count = (wrong_count_estimate.est / scaled_stats.row_count()).clamp(0.0, 1.0);

        assert!((actual - expected).abs() < 1e-9, "{actual} vs {expected}");
        assert_ne!(
            expected, wrong_count,
            "fixture must distinguish the Go input count"
        );
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
            vec![Expression::Constant(tidb_expr::constant::Constant::new(
                tidb_datatype::Datum::Int(1),
                FieldType::new(FieldTypeCode::LongLong),
            ))],
        );
        assert_eq!(is_null_condition_selectivity(&plain_column, &stats), None);
    }
}
