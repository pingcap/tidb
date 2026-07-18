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

//! Dependency-closed equality for statistics table snapshots.
//!
//! This leaf ports the comparison contract in
//! `pkg/statistics/handle/internal/testutil.go::AssertTableEqual` without
//! taking ownership of TiDB's table, histogram, CMSketch, TopN, or existence
//! map implementations.  Callers provide canonical opaque payloads for those
//! structures and this module compares their identities, cardinalities, and
//! encoded values.

/// Opaque payloads for one column or index statistics item.
///
/// The byte vectors are caller-owned canonical encodings.  Keeping encoding
/// outside this leaf mirrors the Go helper's use of `HistogramEqual`,
/// `CMSketch.Equal`, and `TopN.Equal` while making the equality boundary
/// deterministic and straightforward to exercise in Rust.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatsItemSnapshot {
    /// Column or index identifier used by the source table maps.
    pub id: i64,
    /// Canonical histogram payload.
    pub histogram: Vec<u8>,
    /// Canonical CMSketch payload; `None` represents a nil CMSketch.
    pub cmsketch: Option<Vec<u8>>,
    /// Canonical TopN payload; `None` represents a nil TopN.
    pub topn: Option<Vec<u8>>,
}

/// Values compared by [`stats_table_snapshots_equal`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatsTableSnapshot {
    /// Source `statistics.Table.RealtimeCount`.
    pub realtime_count: i64,
    /// Source `statistics.Table.ModifyCount`.
    pub modify_count: i64,
    /// Column statistics keyed by source column ID.
    pub columns: Vec<StatsItemSnapshot>,
    /// Index statistics keyed by source index ID.
    pub indices: Vec<StatsItemSnapshot>,
    /// Canonical caller-owned encoding of `ColAndIdxExistenceMap`.
    pub existence: Vec<u8>,
}

fn sorted_items(items: &[StatsItemSnapshot]) -> Vec<&StatsItemSnapshot> {
    let mut sorted = items.iter().collect::<Vec<_>>();
    sorted.sort_by_key(|item| item.id);
    sorted
}

fn item_lists_equal(left: &[StatsItemSnapshot], right: &[StatsItemSnapshot]) -> bool {
    // Sorting by ID makes this equivalent to comparing the source's maps,
    // independent of map/vector iteration order.  Duplicate IDs remain
    // visible instead of being silently discarded by a conversion.
    sorted_items(left) == sorted_items(right)
}

/// Compares the count, cardinality, per-item payloads, and existence map of
/// two caller-owned statistics table snapshots.
#[must_use]
pub fn stats_table_snapshots_equal(left: &StatsTableSnapshot, right: &StatsTableSnapshot) -> bool {
    left.realtime_count == right.realtime_count
        && left.modify_count == right.modify_count
        && item_lists_equal(&left.columns, &right.columns)
        && item_lists_equal(&left.indices, &right.indices)
        && left.existence == right.existence
}
