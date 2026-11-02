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

//! Go `pkg/planner/util/path.go`: an access path's state as the planner
//! derives it -- the newborn candidate (`AccessPath`), the filled table and
//! index paths with their detached ranges and counts, and the index-merge
//! paths. Deriving them from a `DataSource` is core's (`stats.go`,
//! `indexmerge_path.go`), the planner's `access_path`.

use tidb_expr::column::Column;
use tidb_expr::expression::Expression;


/// Go `util.AccessPath`: one candidate way of reading a `DataSource`, born
/// at enumeration (`getPossibleAccessPaths`) with its store, index and handle
/// kind, and grown in place by plan building and statistics derivation.
/// Index-merge paths are `AccessPath`s too, with partial paths.
#[derive(Clone, Debug, Default)]
pub struct AccessPath {
    /// Go `Index`: the index the path reads, or a common-handle table path's
    /// PRIMARY index (`fillContentForTablePath`).
    pub index: Option<tidb_model::GoShared<tidb_model::IndexInfo>>,
    /// Go `StoreType`.
    pub store_type: tidb_kv::store_type::StoreType,
    /// Go `IsIntHandlePath`.
    pub is_int_handle_path: bool,
    /// Go `IsCommonHandlePath`.
    pub is_common_handle_path: bool,
    /// Go `FullIdxCols` / `FullIdxColLens`: declared by `buildDataSource`,
    /// then replaced by `fillIndexPath` with the handle suffix appended. None
    /// until declared; unresolved positions remain None instead of
    /// shortening the key.
    pub full_idx_cols: Option<Vec<Option<(Column, i64)>>>,
    /// Go `IdxCols` / `IdxColLens`: the usable key prefix `fillIndexPath`
    /// derives (eligible handles included), which ranges are built over.
    pub idx_cols: Vec<(Column, i64)>,
    /// Go `IsUkShardIndexPath`: a unique secondary index of more than one
    /// column led by a `tidb_shard()` generated column.
    pub is_uk_shard_index_path: bool,
    /// Go `IsSingleScan`, evaluated during pruning and refreshed at the first
    /// logical statistics derivation, before final column pruning.
    /// None means the covering decision has not been derived yet.
    pub is_single_scan: Option<bool>,
    /// Go `CountAfterAccess` with its `MinCountAfterAccess` /
    /// `MaxCountAfterAccess` skyline bounds, adjusted together.
    pub row_estimate: Option<tidb_stats::RowEstimate>,
    /// Go `Ranges`: an index path's ranges, set by `fillIndexPath`.
    pub ranges: tidb_ranger::types::Ranges,
    /// Go `AccessConds`.
    pub access_conds: Vec<Expression>,
    /// Go `EqCondCount`.
    pub eq_cond_count: usize,
    /// Go `EqOrInCondCount`.
    pub eq_or_in_cond_count: usize,
    /// Go `IsDNFCond`.
    pub is_dnf_cond: bool,
    /// Go `MinAccessCondsForDNFCond`.
    pub min_access_conds_for_dnf_cond: i64,
    /// Go `IndexFilters`: filters evaluated before the table lookup.
    pub index_filters: Vec<Expression>,
    /// Go `CountAfterIndex`; None until `fillIndexPath`'s caller estimates it
    /// (Go leaves 0 on a path it never filled).
    pub count_after_index: Option<f64>,
    /// How many trailing `AccessConds` `SplitCorColAccessCondFromFilters`
    /// moved from the filters: `index_col = correlated_col` equalities, at
    /// most one closing range, whose ranges are rebuilt per execution.
    pub correlated_access_count: usize,
    /// The TiKV table path's `deriveTablePathStats` result: ranges, access
    /// conditions, table filters and counts.
    pub filled_table: Option<FilledTablePath>,
    /// Go `Forced`: a USE/FORCE INDEX hint names this path.
    pub forced: bool,
    /// Go `ForceKeepOrder` (`ORDER_INDEX` hint).
    pub force_keep_order: bool,
    /// Go `ForceNoKeepOrder` (`NO_ORDER_INDEX` hint).
    pub force_no_keep_order: bool,
    /// Go `IndexLookUpPushDownBy`.
    pub index_lookup_push_down_by: IndexLookUpPushDownByType,
    /// Go `NoncacheableReason`: why a plan reading this path may not enter
    /// the plan cache.
    pub noncacheable_reason: Option<String>,    /// Go `KeepIndexMergeORSourceFilter`: an OR-branch alternative holding
    /// this path still needs the source OR filter above the merge.
    pub keep_index_merge_or_source_filter: bool,    /// Go `PartialIndexPaths`: a finished index merge's partial paths -- the
    /// hinted normal intersection and the multi-valued index paths.
    pub partial_index_paths: Vec<AccessPath>,
    /// Go `PartialAlternativeIndexPaths`: a union index merge's OR branches,
    /// their alternatives, each a list of partial paths -- one table or
    /// index path, or one path per value over a multi-valued index. They
    /// converge only when physical properties are known.
    pub partial_alternative_index_paths: Vec<Vec<Vec<AccessPath>>>,
    /// Go `IndexMergeORSourceFilter`: the OR condition a union merge was
    /// built from.
    pub index_merge_or_source_filter: Option<Expression>,
    /// Go `TableFilters`: an index path's filters that need the table row
    /// or a full prefix-index value, or an index merge's filters above the
    /// partial paths.
    pub table_filters: Vec<Expression>,
    /// Go `IndexMergeIsIntersection`.
    pub index_merge_is_intersection: bool,
    /// Go `IndexMergeAccessMVIndex`.
    pub index_merge_access_mv_index: bool,
}

impl AccessPath {
    /// Go `fillContentForTablePath` over a path of `store_type`: a
    /// common-handle table's path carries its PRIMARY index.
    #[must_use]
    pub fn table_path(
        store_type: tidb_kv::store_type::StoreType,
        is_common_handle: bool,
        primary_index: Option<tidb_model::GoShared<tidb_model::IndexInfo>>,
    ) -> Self {
        Self {
            index: if is_common_handle { primary_index } else { None },
            store_type,
            is_int_handle_path: !is_common_handle,
            is_common_handle_path: is_common_handle,
            ..Self::default()
        }
    }

    /// An index path (`&util.AccessPath{Index: index}`).
    #[must_use]
    pub fn index_path(index: tidb_model::GoShared<tidb_model::IndexInfo>) -> Self {
        Self {
            index: Some(index),
            ..Self::default()
        }
    }

    /// Go `CountAfterAccess`, from the same estimate as its uncertainty bounds.
    #[must_use]
    pub fn count_after_access(&self) -> Option<f64> {
        self.row_estimate.map(|estimate| estimate.est)
    }

    /// Go `IsTablePath`: a handle path, or a TiFlash path over an index.
    #[must_use]
    pub fn is_table_path(&self) -> bool {
        self.is_int_handle_path
            || self.is_common_handle_path
            || (self.index.is_some() && self.is_tiflash_path())
    }

    /// Go `IsTiKVTablePath`.
    #[must_use]
    pub fn is_tikv_table_path(&self) -> bool {
        (self.is_int_handle_path || self.is_common_handle_path)
            && self.store_type == tidb_kv::store_type::StoreType::TiKv
    }

    /// Go `IsTiFlashSimpleTablePath`.
    #[must_use]
    pub fn is_tiflash_simple_table_path(&self) -> bool {
        self.is_tiflash_path() && self.index.is_none()
    }

    /// Go `IsUndetermined`: a multi-valued or partial index path, whose
    /// usability is only known later.
    #[must_use]
    pub fn is_undetermined(&self) -> bool {
        if self.is_table_path() {
            return false;
        }
        self.index.as_ref().is_some_and(|index| {
            let index = index.read();
            index.mv_index || !index.condition_expr_string.is_empty()
        })
    }

    /// A path served by TiFlash (Go `StoreType == kv.TiFlash`).
    #[must_use]
    pub fn is_tiflash_path(&self) -> bool {
        self.store_type == tidb_kv::store_type::StoreType::TiFlash
    }

    /// An index path's `IndexInfo` (`!IsTablePath()`'s `Index`).
    #[must_use]
    pub fn index_info(&self) -> Option<&tidb_model::GoShared<tidb_model::IndexInfo>> {
        if self.is_table_path() {
            None
        } else {
            self.index.as_ref()
        }
    }
}

/// Go compares `AccessPath.Index` by pointer: two paths over the same index,
/// store and handle kind are the same path.
impl PartialEq for AccessPath {
    fn eq(&self, other: &Self) -> bool {
        self.store_type == other.store_type
            && self.is_int_handle_path == other.is_int_handle_path
            && self.is_common_handle_path == other.is_common_handle_path
            && match (&self.index, &other.index) {
                (Some(left), Some(right)) => left.ptr_eq(right),
                (None, None) => true,
                _ => false,
            }
    }
}

impl Eq for AccessPath {}

/// Go `util.IndexLookUpPushDownByType`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum IndexLookUpPushDownByType {
    /// Go `IndexLookUpPushDownNone`.
    #[default]
    None,
    /// Go `IndexLookUpPushDownByHint`.
    Hint,
    /// Go `IndexLookUpPushDownBySysVar`.
    SysVar,
}

/// Logical integer/common-handle ranges and residuals shared by table candidates.
#[derive(Clone, Debug)]
pub struct FilledTablePath {
    /// Integer handle present in the source schema, including an explicit rowid.
    pub handle_column: Option<Column>,
    /// The integer ranger's conversion type.
    pub handle_type: tidb_datatype::FieldType,
    /// Usable leading clustered-primary-key columns and their prefix lengths.
    pub common_columns: Vec<(Column, i64)>,
    /// Access ranges, absorbed conditions and residual table filters.
    pub detached: tidb_ranger::detacher::DetachRangeResult,
    /// Access count before a physical property's LIMIT/probe adjustment.
    pub count_after_access: Option<f64>,
    /// Go's retained lower access-count bound.
    pub min_count_after_access: f64,
    /// Go's retained upper access-count bound.
    pub max_count_after_access: f64,
    /// Access conditions over a correlated column, whose ranges the executor
    /// rebuilds per outer row: the integer handle's `pk = correlated_col`
    /// (`deriveTablePathStats`), or the common handle's split prefix.
    pub correlated_access_count: usize,
}



