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

//! Pre-build row-count derivation, Go's `RecursiveDeriveStats`.
//!
//! The join-reorder DP solver costs a candidate tree purely from
//! `StatsInfo().RowCount` values: `baseNodeCumCost` sums the row counts over a
//! node's whole subtree (`rule_join_reorder.go:651-657`) and `calcJoinCumCost`
//! adds the join's own row count to both children's cumulative costs
//! (`rule_join_reorder.go:978-980`). Reproducing the DP's choice therefore
//! needs nothing more than a faithful per-node row count, which is what this
//! module derives.
//!
//! Node kinds are the ones a `t1, t5, (select ... from t2 join t3) dt` shape
//! reaches: `DataSource`, `Selection`, `Projection`, `Aggregation` and inner
//! `Join`. Each rule below is the Go body, not a re-derivation:
//!
//! * `DataSource` -- `deriveStats4DataSource` (`core/stats.go:110-168`) sets
//!   `ds.stats = ds.TableStats.Scale(vars, Selectivity(pushedDownConds))`,
//!   where `TableStats` is built by `initStats` (`core/stats.go:538-574`) with
//!   `RowCount = RealtimeCount` and one `EstimateColumnNDV` per schema column.
//! * `Selection` -- `LogicalSelection.DeriveStats`
//!   (`logicalop/logical_selection.go:227-240`) is a *flat*
//!   `Scale(vars, SelectionFactor)`. It does **not** consult the per-conjunct
//!   selectivity machinery; only the `DataSource` does, for the conditions
//!   that were pushed into it.
//! * `Projection` -- `LogicalProjection.DeriveStats`
//!   (`logicalop/logical_projection.go:278-305`) passes the child's row count
//!   through unchanged and re-derives one NDV per output expression.
//! * `Aggregation` -- `LogicalAggregation.DeriveStats`
//!   (`logicalop/logical_aggregation.go:219-246`) estimates the group-key NDV
//!   and uses that number as both its row count and every output column's NDV.
//! * `Join` -- `LogicalJoin.DeriveStats` (`logicalop/logical_join.go:560-616`)
//!   takes `EstimateFullJoinRowCount` for an inner join and clamps every
//!   inherited column NDV to the join's own row count.

use std::collections::{BTreeMap, BTreeSet};

use crate::cardinality::join::{
    estimate_full_join_row_count, FullJoinRowCountInput, JoinKeyEstimate,
};
use crate::cardinality::ndv::GroupNdv;
use crate::cost_factors::SELECTION_FACTOR;

/// Go `cardinality.distinctFactor` (`cardinality/ndv.go:35`), the NDV a column
/// with no loaded histogram is assumed to have, as a fraction of the table's
/// realtime row count (`EstimateColumnNDV`, `cardinality/ndv.go:39-53`).
pub const DISTINCT_FACTOR: f64 = 0.8;

/// Go `vardef.DefOptRiskScaleNDVSkewRatio` (`vardef/tidb_vars.go:1471`).
///
/// The default is `1.0`, so `ScaleNDV` returns the *skewed* estimate --
/// `originalNDV * selectedRows / originalRows` -- and the uniform branch never
/// contributes in a default session. Both branches are still ported, because
/// the variable is settable.
pub const DEF_SCALE_NDV_SKEW_RATIO: f64 = 1.0;

/// A column identity: Go's `expression.Column.UniqueID`, an `int64`.
///
/// Formerly `u64` — the crate's one outlier spelling, kept only for the
/// executor's DP driver. Unified to `i64` with the `StatsInfo` merge; the
/// alias survives because that driver imports it by this name.
pub type ColumnId = i64;

/// The statistics profile — ONE port now. This module's former private
/// `StatsInfo` (the second port of `property/stats_info.go`, keyed `u64`)
/// is deleted; what the DP rules read is the same profile every plan node
/// carries.
pub use crate::stats_info::StatsInfo;

/// Go `cardinality.ScaleNDV` (`cardinality/ndv.go:215-262`).
///
/// `skew_ratio` blends the skewed estimate with the uniform one; the session
/// default is [`DEF_SCALE_NDV_SKEW_RATIO`].
#[must_use]
pub fn scale_ndv(
    original_ndv: f64,
    original_rows: f64,
    selected_rows: f64,
    skew_ratio: f64,
) -> f64 {
    let uniform = estimate_uniform_ndv(original_ndv, original_rows, selected_rows);
    let skewed = estimate_skewed_ndv(original_ndv, original_rows, selected_rows);
    skewed * skew_ratio + uniform * (1.0 - skew_ratio)
}

/// Go `estimateUniformNDV` (`cardinality/ndv.go:234-254`).
fn estimate_uniform_ndv(original_ndv: f64, original_rows: f64, selected_rows: f64) -> f64 {
    if original_rows <= 0.0 || selected_rows <= 0.0 || original_ndv <= 0.0 {
        return 0.0;
    }
    if selected_rows >= original_rows {
        return original_ndv;
    }
    let selectivity = selected_rows / original_rows;
    let rows_per_value = original_rows / original_ndv;
    let not_selected_poss_per_value = (1.0 - selectivity).powf(rows_per_value);
    let new_ndv = original_ndv * (1.0 - not_selected_poss_per_value);
    new_ndv.max(1.0).min(selected_rows)
}

/// Go `estimateSkewedNDV` (`cardinality/ndv.go:257-262`).
fn estimate_skewed_ndv(original_ndv: f64, original_rows: f64, selected_rows: f64) -> f64 {
    if original_rows <= 0.0 {
        return 0.0;
    }
    original_ndv * selected_rows / original_rows
}

/// Go `EstimateColsNDVWithMatchedLen` (`cardinality/ndv.go:87-123`), production
/// path.
///
/// Returns `(ndv, matched_len)`. An empty column list is Go's early
/// `return 1.0, 1`. For one column, conservative and exponential agree, so the
/// source returns the naive estimate directly. For several columns the
/// production default applies: `DefOptRiskGroupNDVSkewRatio` is `0.0`
/// (`vardef/tidb_vars.go:1472`), so the `skewRatio > 0` branch is not taken and
/// the *conservative* (naive max) estimate is returned with `matched_len = 1`.
///
/// The exponential-backoff blend behind a non-zero group-NDV skew ratio is
/// deliberately out of scope here. The backoff itself is already ported as
/// [`apply_exponential_backoff`](crate::cardinality::apply_exponential_backoff),
/// but nothing calls it from here because the production ratio is zero.
#[must_use]
pub fn estimate_cols_ndv_with_matched_len(cols: &[ColumnId], profile: &StatsInfo) -> (f64, usize) {
    if cols.is_empty() {
        return (1.0, 1);
    }
    let mut sorted_cols = cols.to_vec();
    sorted_cols.sort_unstable();
    if let Some(group) = profile.group_ndvs().iter().find(|group| {
        let mut group_cols = group.columns.clone();
        group_cols.sort_unstable();
        group_cols == sorted_cols
    }) {
        return (group.ndv.max(1.0), group.columns.len());
    }

    let mut max_ndv = 1.0_f64;
    for col in cols {
        if let Some(ndv) = profile.col_ndvs().get(col) {
            if *ndv > 0.0 {
                max_ndv = max_ndv.max(*ndv);
            }
        }
    }
    (max_ndv, 1)
}
