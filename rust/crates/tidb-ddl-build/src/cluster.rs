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

//! Go `pkg/ddl/cluster.go`'s key-range helpers (FLASHBACK CLUSTER).

use tidb_txnkv::{Key, KeyRange};

/// A DDL key range together with the flashback-cluster exclusion marker.
///
/// This is Go `keyRangeMayExclude` from `pkg/ddl/cluster.go`. The range
/// planner guarantees sorted, non-overlapping input; the marker only controls
/// whether the range contributes to the output or terminates the current
/// continuous run.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KeyRangeMayExclude {
    /// Half-open range to include or exclude.
    pub range: KeyRange,
    /// When true, flushes the current run and is omitted from the result.
    pub exclude: bool,
}

/// Drops excluded ranges and coalesces each surviving continuous run.
///
/// This is Go `mergeContinuousKeyRanges` (`pkg/ddl/cluster.go:330`). The
/// caller supplies ranges sorted by start key and without overlap; an
/// excluded range represents a known gap, so it closes the current run rather
/// than being merged across. Keys are cloned because Go's returned
/// `kv.KeyRange` owns the slice values independently of the input wrappers.
#[must_use]
pub fn merge_continuous_key_ranges(ranges: &[KeyRangeMayExclude]) -> Vec<KeyRange> {
    let mut result = Vec::with_capacity(1);
    let mut continuous_start: Option<Key> = None;
    let mut continuous_end: Option<Key> = None;

    for item in ranges {
        if item.exclude {
            if let (Some(start), Some(end)) = (continuous_start.take(), continuous_end.take()) {
                result.push(KeyRange::new(start, end));
            }
            continue;
        }

        if continuous_start.is_none() {
            continuous_start = Some(item.range.start_key.clone());
        }
        continuous_end = Some(item.range.end_key.clone());
    }

    if let (Some(start), Some(end)) = (continuous_start, continuous_end) {
        result.push(KeyRange::new(start, end));
    }
    result
}
