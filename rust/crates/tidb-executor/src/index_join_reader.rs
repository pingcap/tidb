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

//! Go `dataReaderBuilder`'s index-join half (`pkg/executor/builder.go`):
//! `BuildExecutorForIndexJoin` builds an index join's inner side for one
//! task's lookup contents, its readers over `buildKvRangesForIndexJoin`.

use std::sync::Arc;

use tidb_driver_error::ExecError;
use tidb_executor_join::index_lookup::IndexJoinProbes;
use tidb_planner_core_operator_physicalop::ColWithCmpFuncManager;
use tidb_ranger::types::Range;
use tidb_txnkv::KeyRange;

/// Go `buildKvRangesForIndexJoin`: each lookup content's keys written into
/// the join's template `ranges` at their index offsets (`keyOff2IdxOff`),
/// and, with a next-column comparison (`cwc`), the template's last slot
/// rebuilt from each content's comparison values and the task's ranges
/// unioned. `index_id` -1 is the common handle.
///
/// The probe keys arrive in the index's column order, so the `j`-th key
/// fills the `j`-th smallest of the index offsets.
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_kv_ranges_for_index_join(
    zone: &tidb_datatype::SessionTimeZone,
    table_id: i64,
    index_id: i64,
    probes: &IndexJoinProbes,
    ranges: &[Range],
    key_off2_idx_off: &[i64],
    cwc: Option<&ColWithCmpFuncManager>,
    mem_tracker: Option<&Arc<tidb_memory::Tracker>>,
) -> Result<Vec<KeyRange>, ExecError> {
    let encode_err = |error| ExecError::internal(format!("index-join range encoding: {error:?}"));
    if ranges.is_empty() {
        return Ok(Vec::new());
    }
    let mut idx_offs: Vec<usize> =
        key_off2_idx_off.iter().filter_map(|off| usize::try_from(*off).ok()).collect();
    idx_offs.sort_unstable();
    let last_pos = ranges[0].low_val.len() - 1;
    let mut ranges = ranges.to_vec();
    let mut kv_ranges = Vec::with_capacity(ranges.len() * probes.keys.len());
    let mut tmp_datum_ranges = Vec::with_capacity(probes.keys.len());
    for (keys, bounds) in probes.keys.iter().zip(probes.bound_values.iter().chain(std::iter::repeat(&Vec::new()))) {
        for ran in &mut ranges {
            for (key, idx_off) in keys.iter().zip(&idx_offs) {
                ran.low_val[*idx_off] = key.clone();
                ran.high_val[*idx_off] = key.clone();
            }
        }
        let Some(cwc) = cwc else {
            let tmp = if index_id == -1 {
                tidb_distsql::common_handle_ranges_to_kv_ranges(Some(zone), table_id, &ranges)
            } else {
                index_ranges_to_kv_ranges(zone, table_id, index_id, &ranges, mem_tracker)
            }
            .map_err(encode_err)?;
            kv_ranges.extend(tmp);
            continue;
        };
        let next_col_ranges = cwc
            .build_ranges_by_row(bounds)
            .map_err(|error| ExecError::internal(error.to_string()))?;
        for next_col_ran in next_col_ranges {
            for ran in &mut ranges {
                ran.low_val[last_pos] = next_col_ran.low_val[0].clone();
                ran.high_val[last_pos] = next_col_ran.high_val[0].clone();
                ran.low_exclude = next_col_ran.low_exclude;
                ran.high_exclude = next_col_ran.high_exclude;
                ran.collators.clone_from(&next_col_ran.collators);
                tmp_datum_ranges.push(ran.clone());
            }
        }
    }
    if let (Some(first), Some(tracker)) = (kv_ranges.first(), mem_tracker) {
        // Go `2 * cap(kvRanges[0].StartKey) * len(kvRanges)`: a key is made
        // at exactly its length.
        tracker.consume((2 * first.start_key.len() * kv_ranges.len()) as i64);
    }
    if let (Some(first), Some(tracker)) = (tmp_datum_ranges.first(), mem_tracker) {
        tracker.consume(2 * tidb_datatype::estimated_mem_usage(&first.low_val, tmp_datum_ranges.len()));
    }
    if cwc.is_none() {
        kv_ranges.sort_by(|a, b| a.start_key.cmp(&b.start_key));
        return Ok(kv_ranges);
    }
    let tmp_datum_ranges = tidb_ranger::ranger::union_ranges(tmp_datum_ranges, true)
        .map_err(|error| ExecError::internal(format!("index-join range union: {error}")))?;
    if index_id == -1 {
        return tidb_distsql::common_handle_ranges_to_kv_ranges(Some(zone), table_id, &tmp_datum_ranges)
            .map_err(encode_err);
    }
    index_ranges_to_kv_ranges(zone, table_id, index_id, &tmp_datum_ranges, mem_tracker)
        .map_err(encode_err)
}

/// Go `distsql.IndexRangesToKVRangesWithInterruptSignal` over one table,
/// charging `mem_tracker` when there is one.
fn index_ranges_to_kv_ranges(
    zone: &tidb_datatype::SessionTimeZone,
    table_id: i64,
    index_id: i64,
    ranges: &[Range],
    mem_tracker: Option<&Arc<tidb_memory::Tracker>>,
) -> Result<Vec<KeyRange>, tidb_distsql::KvRequestBuildError> {
    match mem_tracker {
        Some(tracker) => {
            tidb_distsql::index_ranges_to_kv_ranges_with_mem_tracker(Some(zone), table_id, index_id, ranges, tracker)
        }
        None => Ok(tidb_distsql::index_ranges_to_kv_ranges(Some(zone), &[table_id], index_id, ranges)?
            .into_iter()
            .flatten()
            .collect()),
    }
}
