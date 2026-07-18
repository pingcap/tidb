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

//! Source-backed tests for `AssertTableEqual`'s snapshot contract.
//!
//! The Go helper is exercised by
//! `pkg/statistics/handle/handletest/statstest/stats_test.go:307`
//! (`TestStatsStoreAndLoad`), with the helper call at line 333.

use tidb_stats::{stats_table_snapshots_equal, StatsItemSnapshot, StatsTableSnapshot};

fn item(
    id: i64,
    histogram: &[u8],
    cmsketch: Option<&[u8]>,
    topn: Option<&[u8]>,
) -> StatsItemSnapshot {
    StatsItemSnapshot {
        id,
        histogram: histogram.to_vec(),
        cmsketch: cmsketch.map(ToOwned::to_owned),
        topn: topn.map(ToOwned::to_owned),
    }
}

fn snapshot(
    columns: Vec<StatsItemSnapshot>,
    indices: Vec<StatsItemSnapshot>,
) -> StatsTableSnapshot {
    StatsTableSnapshot {
        realtime_count: 42,
        modify_count: 7,
        columns,
        indices,
        existence: vec![0x01, 0x02],
    }
}

#[test]
fn source_stats_table_snapshot_is_order_independent() {
    let left = snapshot(
        vec![
            item(2, b"h2", Some(b"c2"), Some(b"t2")),
            item(1, b"h1", None, None),
        ],
        vec![item(8, b"ih8", Some(b"ic8"), Some(b"it8"))],
    );
    let right = snapshot(
        vec![
            item(1, b"h1", None, None),
            item(2, b"h2", Some(b"c2"), Some(b"t2")),
        ],
        vec![item(8, b"ih8", Some(b"ic8"), Some(b"it8"))],
    );

    assert!(stats_table_snapshots_equal(&left, &right));
}

#[test]
fn source_stats_table_snapshot_checks_counts_and_cardinality() {
    let left = snapshot(vec![item(1, b"h1", None, None)], vec![]);
    let mut right = left.clone();
    right.realtime_count += 1;
    assert!(!stats_table_snapshots_equal(&left, &right));

    right = left.clone();
    right.columns.push(item(2, b"h2", None, None));
    assert!(!stats_table_snapshots_equal(&left, &right));

    right = left.clone();
    right.indices.push(item(3, b"h3", None, None));
    assert!(!stats_table_snapshots_equal(&left, &right));
}

#[test]
fn source_stats_table_snapshot_checks_opaque_payloads_and_existence() {
    let left = snapshot(vec![item(1, b"h1", None, Some(b"t1"))], vec![]);

    let mut right = left.clone();
    right.columns[0].histogram = b"changed".to_vec();
    assert!(!stats_table_snapshots_equal(&left, &right));

    right = left.clone();
    right.columns[0].cmsketch = Some(Vec::new());
    assert!(!stats_table_snapshots_equal(&left, &right));

    right = left.clone();
    right.existence.push(0x03);
    assert!(!stats_table_snapshots_equal(&left, &right));
}
