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

//! Go `disableReuseChunkIfNeeded` (`pkg/planner/core/optimizer.go`), the last
//! `postOptimize` step that decides whether the statement may keep reusing
//! the session's chunk allocator.
//!
//! The walk goes top-down through the root-side operators and stops at the
//! first reader or point get: only those materialize rows into the reusable
//! chunk. One whose output carries an unbounded column (BLOB, LONGBLOB, JSON,
//! VECTOR) clears the allocator; one with a VARCHAR-like column wider than
//! 1000 does too, unless the host has at least 120 GiB and the reusable
//! chunk's estimated size stays within `MaxBlobWidth * 2`.

use super::PhysicalPlan;
use crate::stats_info::StatsInfo;
use tidb_datatype::FieldTypeCode;
use tidb_expr::column::Column;

/// Go `MaxMemoryLimitForOverlongType`.
const MAX_MEMORY_LIMIT_FOR_OVERLONG_TYPE: u64 = 120 << 30;
/// Go `maxFlenForOverlongType`: `mysql.MaxBlobWidth * 2`.
const MAX_FLEN_FOR_OVERLONG_TYPE: f64 = 16_777_216.0 * 2.0;

/// Whether `plan` clears the statement's reusable chunk allocator. `mem_total`
/// is `memory.MemTotal()`, `None` when it could not be read; the caller owns
/// Go's `IsAllocValid` guard.
#[must_use]
pub fn should_disable_reuse_chunk(
    plan: &PhysicalPlan,
    max_chunk_size: usize,
    mem_total: Option<u64>,
) -> bool {
    match plan {
        PhysicalPlan::TableReader(_)
        | PhysicalPlan::IndexReader(_)
        | PhysicalPlan::IndexLookUpReader(_)
        | PhysicalPlan::IndexMergeReader(_)
        | PhysicalPlan::PointGet(_)
        | PhysicalPlan::BatchPointGet(_) => {
            should_skip_reuse_chunk_for_physical_plan(plan, max_chunk_size, mem_total)
        }
        // Other operators do not read data; the walk continues into every
        // child until one of them clears the allocator.
        _ => plan
            .children()
            .iter()
            .any(|child| should_disable_reuse_chunk(child, max_chunk_size, mem_total)),
    }
}

/// Go `shouldSkipReuseChunkForPhysicalPlan`.
fn should_skip_reuse_chunk_for_physical_plan(
    plan: &PhysicalPlan,
    max_chunk_size: usize,
    mem_total: Option<u64>,
) -> bool {
    let Some(schema) = plan.schema() else {
        return false;
    };
    let mut total_flen = 0_i64;
    let mut overlong_columns = Vec::with_capacity(schema.columns.len());
    for column in &schema.columns {
        let Some(field_type) = column.get_static_type() else {
            continue;
        };
        match field_type.code() {
            // Still treated as unbounded: the old conservative behavior.
            FieldTypeCode::LongBlob
            | FieldTypeCode::Blob
            | FieldTypeCode::Json
            | FieldTypeCode::VectorFloat32 => return true,
            FieldTypeCode::VarString
            | FieldTypeCode::Varchar
            | FieldTypeCode::TinyBlob
            | FieldTypeCode::MediumBlob => {
                if field_type.flen() <= 1000 {
                    continue;
                }
                total_flen += field_type.flen();
                overlong_columns.push(column);
            }
            _ => {}
        }
    }
    if total_flen == 0 {
        return false;
    }
    !allow_reuse_chunk_for_overlong_type(
        plan,
        &overlong_columns,
        total_flen,
        max_chunk_size,
        mem_total,
    )
}

/// Go `allowReuseChunkForOverlongType`: the host-memory gate, then the
/// retained size of one reusable chunk rather than the whole result.
fn allow_reuse_chunk_for_overlong_type(
    plan: &PhysicalPlan,
    overlong_columns: &[&Column],
    total_flen: i64,
    max_chunk_size: usize,
    mem_total: Option<u64>,
) -> bool {
    if !mem_total.is_some_and(|total| total > 0 && total >= MAX_MEMORY_LIMIT_FOR_OVERLONG_TYPE) {
        return false;
    }
    let (estimated_rows, has_trusted_stats) = estimate_reusable_chunk_rows_for_overlong_type(plan);
    if estimated_rows <= 0.0 {
        return false;
    }
    let rows_per_reusable_chunk = match plan {
        PhysicalPlan::PointGet(_) => estimated_rows,
        _ => estimated_rows.min(max_chunk_size as f64),
    };
    let stats = plan.base().base.stats_info();
    let mut estimated_bytes_per_row = total_flen as f64;
    if has_trusted_stats && has_usable_overlong_type_size_stats(stats, overlong_columns) {
        // Real stats give the observed average size, not the schema's worst case.
        estimated_bytes_per_row = average_row_size(stats, overlong_columns);
    }
    rows_per_reusable_chunk * estimated_bytes_per_row <= MAX_FLEN_FOR_OVERLONG_TYPE
}

/// Go `hasUsableOverlongTypeSizeStats`.
fn has_usable_overlong_type_size_stats(
    stats: Option<&StatsInfo>,
    overlong_columns: &[&Column],
) -> bool {
    let Some(hist_coll) = stats.and_then(StatsInfo::hist_coll) else {
        return false;
    };
    if hist_coll.pseudo() || hist_coll.realtime_count() == 0 || hist_coll.column_count() == 0 {
        return false;
    }
    overlong_columns.iter().all(|column| {
        hist_coll.column(column.unique_id).is_some_and(|stats| {
            // Keep the schema fallback where the column would still use
            // EstimateTypeWidth.
            stats.is_handle
                || stats.total_column_size != 0
                || stats.null_count == hist_coll.realtime_count()
        })
    })
}

/// Go `getAvgRowSize` over the overlong columns.
fn average_row_size(stats: Option<&StatsInfo>, columns: &[&Column]) -> f64 {
    let hist_coll = stats.and_then(StatsInfo::hist_coll);
    let row_size_columns = columns
        .iter()
        .map(|column| {
            let estimated_width = column.get_static_type().map_or(0.0, |field_type| {
                tidb_chunk::codec::estimate_type_width(field_type) as f64
            });
            match hist_coll.and_then(|hist_coll| hist_coll.column(column.unique_id)) {
                Some(stats) => {
                    crate::cardinality::row_size::RowSizeColumn::with_stats(stats, estimated_width)
                }
                None => crate::cardinality::row_size::RowSizeColumn {
                    stats: None,
                    estimated_width,
                },
            }
        })
        .collect::<Vec<_>>();
    crate::plan_cost_ver2::plan_avg_row_size(
        &row_size_columns,
        hist_coll.map(|hist_coll| (hist_coll.pseudo(), hist_coll.realtime_count())),
    )
}

/// Go `estimateReusableChunkRowsForOverlongType`: the row bound and whether
/// the caller may use observed average sizes.
fn estimate_reusable_chunk_rows_for_overlong_type(plan: &PhysicalPlan) -> (f64, bool) {
    let Some(stats) = plan.base().base.stats_info() else {
        return (0.0, false);
    };
    if stats.row_count() <= 0.0 {
        return (0.0, false);
    }
    let rows = stats.row_count().ceil();
    let hist_coll = stats.hist_coll();
    match plan {
        PhysicalPlan::PointGet(_) => (rows, true),
        PhysicalPlan::BatchPointGet(_) => (rows, hist_coll.is_some_and(|hist| !hist.pseudo())),
        PhysicalPlan::TableReader(_)
        | PhysicalPlan::IndexReader(_)
        | PhysicalPlan::IndexLookUpReader(_)
        | PhysicalPlan::IndexMergeReader(_) => match hist_coll {
            Some(hist)
                if !hist.pseudo() && hist.realtime_count() != 0 && hist.column_count() != 0 =>
            {
                (rows, true)
            }
            _ => (0.0, false),
        },
        _ => (0.0, false),
    }
}
