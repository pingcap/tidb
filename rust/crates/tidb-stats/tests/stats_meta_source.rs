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

//! Source-backed tests for stats-meta count metadata.

use tidb_stats::{stats_meta_counts, stats_meta_query, StatsMetaCounts};

#[test]
fn stats_meta_query_preserves_locking_selector() {
    assert_eq!(
        stats_meta_query(false),
        "select count, modify_count from mysql.stats_meta where table_id = %?"
    );
    assert_eq!(
        stats_meta_query(true),
        "select count, modify_count from mysql.stats_meta where table_id = %? for update"
    );
}

#[test]
fn stats_meta_missing_row_is_null_zero_sentinel() {
    assert_eq!(stats_meta_counts(None), StatsMetaCounts::missing());
}

#[test]
fn stats_meta_present_row_preserves_signed_conversion() {
    assert_eq!(
        stats_meta_counts(Some((u64::MAX, -17))),
        StatsMetaCounts {
            count: -1,
            modify_count: -17,
            is_null: false,
        }
    );
}
