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
//! derives it -- the newborn candidate (`PossiblePath`), the filled table and
//! index paths with their detached ranges and counts, and the index-merge
//! paths. Deriving them from a `DataSource` is core's (`stats.go`,
//! `indexmerge_path.go`), the planner's `access_path`.

use tidb_expr::column::Column;
use tidb_expr::expression::Expression;

/// Occurrence-local state for one ordinary index access path. Go retains
/// these fields together on `util.AccessPath`; a datasource occurrence owns
/// the record independently of shared catalog metadata.
#[derive(Clone, Debug, Default)]
pub struct IndexPathState {
    /// Logically detached ranges and filter stages shared by ordinary and merge paths.
    pub filled: Option<FilledIndexPath>,
    /// Go's FullIdxCols/FullIdxColLens at plan construction, before ranger
    /// appends handles. None denotes the static metadata-only pruning path;
    /// unresolved positions remain None instead of shortening the key.
    pub declared_columns: Option<Vec<Option<(tidb_expr::column::Column, i64)>>>,
    /// Access estimate and its skyline risk bounds, adjusted together.
    pub row_estimate: Option<tidb_stats::RowEstimate>,
    /// Go `IsSingleScan`, evaluated during pruning and refreshed at the first
    /// logical statistics derivation, before final column pruning.
    /// None means the covering decision has not been derived yet.
    pub is_single_scan: Option<bool>,
    /// Go `IsUkShardIndexPath`: a unique secondary index of more than one
    /// column led by a `tidb_shard()` generated column.
    pub is_uk_shard_index_path: bool,
}

impl IndexPathState {
    /// Go `CountAfterAccess`, from the same estimate as its uncertainty bounds.
    #[must_use]
    pub fn count_after_access(&self) -> Option<f64> {
        self.row_estimate.map(|estimate| estimate.est)
    }
}

/// Candidate identities and merge alternatives retained after logical derivation.
/// Ordinary index identities refer to the datasource's filled index states.
#[derive(Clone, Debug)]
pub enum DerivedAccessPath {
    /// An ordinary table/index candidate from the logical pruning boundary.
    Ordinary(PossiblePath),
    /// OR alternatives converge only when physical properties are known.
    Union(UnionIndexMergePath),
    /// A finished index merge (Go `PartialIndexPaths`): the hinted normal
    /// intersection and the multi-valued index paths, whose residual
    /// predicates attach on the table side.
    IndexMerge(IndexMergePath),
}

/// Go's post-derivation PossibleAccessPaths and their shared minimum selectivity.
#[derive(Clone, Debug)]
pub struct DerivedAccessPaths {
    /// Integer/common-handle state derived before property search.
    pub table_path: Option<FilledTablePath>,
    /// Ordinary and merge candidates compete through one retained collection.
    pub paths: Vec<DerivedAccessPath>,
    /// Includes merge estimates before ordered-index expected-count adjustment.
    pub min_selectivity: f64,
    /// Go `generateIndexMergePath`'s `maxRowCount`: the largest index-merge
    /// `CountAfterAccess`, present only when merges were built from more
    /// conditions (`AllConds`) than the DataSource statistics saw
    /// (`PushedDownConds`). The statistics are rescaled down to it.
    pub index_merge_max_count: Option<f64>,
}

/// What one enumerated path reads, at enumeration time.
///
/// Go's `util.AccessPath` is born nearly empty — `{StoreType, Index,
/// IsIntHandlePath/IsCommonHandlePath}` — and only later grows ranges and
/// counts (`deriveStatsByFilter`). This type is that newborn form;
/// [`DataSourceAccessPath`] above is the grown, costing-time form the bounded
/// scanner builds. Two stages, as in Go, where one struct mutates through
/// both.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PossiblePath {
    /// The table path, always first (`publicPaths[0]`).
    Table {
        /// Go `IsIntHandlePath`: the table is not common-handle.
        is_int_handle: bool,
        /// Go `fillContentForTablePath`: a common-handle table path carries
        /// its PRIMARY index, by offset into the catalog's index list.
        primary_index: Option<usize>,
    },
    /// Go `genTiFlashPath`: a table path served by an available TiFlash
    /// replica.
    TiFlashTable,
    /// One public index, by offset into the catalog's index list.
    Index {
        /// The offset into `SourceTable::indexes`.
        index: usize,
    },
}

/// Go `util.IndexLookUpPushDownByType`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum IndexLookupPushDownBy {
    /// Go `IndexLookUpPushDownNone`.
    #[default]
    None,
    /// Go `IndexLookUpPushDownByHint`.
    Hint,
    /// Go `IndexLookUpPushDownBySysVar`.
    SysVar,
}

/// Go's detached ranges and the subsequent index/table filter partition.
#[derive(Clone, Debug)]
pub struct FilledIndexPath {
    /// Usable key layout at logical derivation, including eligible handles.
    pub columns: Vec<(Column, i64)>,
    /// Full index key layout, retaining unresolved positions and handle suffixes.
    pub full_columns: Vec<Option<(Column, i64)>>,
    /// Range expressions and access-coverage metadata from the same detach run.
    pub detached: tidb_ranger::detacher::DetachRangeResult,
    /// Filters evaluated before the table lookup.
    pub index_filters: Vec<Expression>,
    /// Filters requiring the table row or a full prefix-index value.
    pub table_filters: Vec<Expression>,
    /// Go CountAfterIndex; absent when the access estimate is unavailable.
    pub count_after_index: Option<f64>,
    /// How many trailing access conditions `SplitCorColAccessCondFromFilters`
    /// moved from the filters: `index_col = correlated_col` equalities, at
    /// most one closing range, whose ranges are rebuilt per execution.
    pub correlated_access_count: usize,
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

/// One viable partial access for a single disjunct.
#[derive(Clone, Debug)]
pub enum Partial {
    /// A secondary-index range scan; rows are (index cols..., handle...).
    Index {
        /// The index, by offset into the catalog's index list.
        index_pos: usize,
        /// The filled ranges and filters.
        filled: FilledIndexPath,
        /// Go `CountAfterAccess`.
        rows: f64,
        /// Whether the disjunct must still be applied above the merge.
        keep_source_filter: bool,
    },
    /// A table-handle alternative retaining residuals until schema narrowing.
    Table {
        /// The filled handle ranges.
        filled: FilledTablePath,
        /// Go `CountAfterAccess`.
        rows: f64,
        /// Whether the disjunct must still be applied above the merge.
        keep_source_filter: bool,
    },
    /// Go's MV alternative (`buildIntoAccessPath`'s MV arm): one partial path
    /// per value over a multi-valued index, whose handles unite.
    MvIndex {
        /// The multi-valued index, by offset into the catalog's index list.
        index_pos: usize,
        /// One partial path per value.
        paths: Vec<IndexMergePartial>,
        /// Whether the disjunct must still be applied above the merge.
        keep_source_filter: bool,
    },
}

impl IndexMergePartial {
    /// Go `CountAfterAccess`.
    pub fn rows(&self) -> f64 {
        self.path.count_after_access().unwrap_or(0.0)
    }

    /// The partial's filled ranges and filters.
    pub fn filled(&self) -> &FilledIndexPath {
        self.path
            .filled
            .as_ref()
            .expect("index merge partials are filled logical paths")
    }
}

/// Logical access ranges and counts, prepared before physical path costing.
#[derive(Clone, Debug)]
pub struct UnionIndexMergePath {
    /// Per disjunct, the partial accesses it may take.
    pub alternatives: Vec<Vec<Partial>>,
    /// Go `CountAfterAccess`.
    pub count_after_access: f64,
    /// The OR condition the merge was built from.
    pub source_filter: Expression,
    /// Go `TableFilters`.
    pub table_filters: Vec<Expression>,
    /// Whether the path may enter the plan cache.
    pub use_plan_cache: bool,
}

/// One index partial of a finished index merge (Go `PartialIndexPaths[i]`).
#[derive(Clone, Debug)]
pub struct IndexMergePartial {
    /// The index, by offset into the catalog's index list.
    pub index_pos: usize,
    /// Its filled state.
    pub path: IndexPathState,
}

/// Go's finished index-merge `AccessPath` (`PartialIndexPaths` set): the
/// partials are fixed at logical derivation, unlike the OR alternatives of
/// [`UnionIndexMergePath`] that converge only under physical properties.
#[derive(Clone, Debug)]
pub struct IndexMergePath {
    /// Go `PartialIndexPaths`.
    pub partials: Vec<IndexMergePartial>,
    /// Go `CountAfterAccess`.
    pub count_after_access: f64,
    /// Go `TableFilters`.
    pub table_filters: Vec<Expression>,
    /// Go `IndexMergeIsIntersection`.
    pub is_intersection: bool,
    /// Go `IndexMergeAccessMVIndex`.
    pub access_mv_index: bool,
    /// Go `NoncacheableReason`/`SetSkipPlanCache` for a path whose ranges
    /// depend on parameter values.
    pub noncacheable_reason: Option<String>,
}
