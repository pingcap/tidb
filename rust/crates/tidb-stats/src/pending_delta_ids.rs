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

//! Pending table-ID selection from
//! `pkg/statistics/handle/usage/session_stats_collect.go`.
//!
//! The source helper operates on the keys of a table-delta map.  This leaf
//! accepts those already-materialized keys and owns only deterministic
//! filtering, target de-duplication, and ordering.  Delta values, session
//! collectors, SQL execution, and persistence remain external boundaries.

use std::collections::HashSet;

/// Select pending table IDs for a stats-delta dump.
///
/// An empty target list selects every pending map key.  Otherwise only target
/// IDs that are present in `pending_ids` are selected; duplicate targets are
/// ignored and the result is always sorted in ascending order.  The source
/// map has unique keys, so callers should provide `pending_ids` as unique IDs.
#[must_use]
pub fn collect_pending_delta_ids(pending_ids: &[i64], target_ids: &[i64]) -> Vec<i64> {
    if target_ids.is_empty() {
        let mut ids = pending_ids.to_vec();
        ids.sort_unstable();
        return ids;
    }

    let pending: HashSet<i64> = pending_ids.iter().copied().collect();
    let mut seen = HashSet::with_capacity(target_ids.len());
    let mut ids = Vec::with_capacity(target_ids.len());
    for &id in target_ids {
        if pending.contains(&id) && seen.insert(id) {
            ids.push(id);
        }
    }
    ids.sort_unstable();
    ids
}
