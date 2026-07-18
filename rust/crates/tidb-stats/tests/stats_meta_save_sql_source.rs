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

//! Source-backed tests for batch stats-meta SQL assembly.

use tidb_stats::{stats_meta_save_sql, StatsMetaSaveUpdate};

#[test]
fn source_save_meta_sql_preserves_false_and_true_refresh_paths() {
    let updates = [
        StatsMetaSaveUpdate::new(7, 8, 9),
        StatsMetaSaveUpdate::new(-2, i64::MAX, i64::MIN),
    ];
    assert_eq!(
        stats_meta_save_sql(42, false, &updates),
        "insert into mysql.stats_meta (version, table_id, count, modify_count) values (42, 7, 8, 9),(42, -2, 9223372036854775807, -9223372036854775808) on duplicate key update version = values(version), modify_count = values(modify_count), count = values(count)"
    );
    assert_eq!(
        stats_meta_save_sql(42, true, &updates),
        "insert into mysql.stats_meta (version, table_id, count, modify_count, last_stats_histograms_version) values (42, 7, 8, 9, 42),(42, -2, 9223372036854775807, -9223372036854775808, 42) on duplicate key update version = values(version), modify_count = values(modify_count), count = values(count), last_stats_histograms_version = values(last_stats_histograms_version)"
    );
}

#[test]
fn source_save_meta_sql_preserves_empty_input_spacing() {
    assert_eq!(
        stats_meta_save_sql(1, false, &[]),
        "insert into mysql.stats_meta (version, table_id, count, modify_count) values  on duplicate key update version = values(version), modify_count = values(modify_count), count = values(count)"
    );
    assert_eq!(
        stats_meta_save_sql(1, true, &[]),
        "insert into mysql.stats_meta (version, table_id, count, modify_count, last_stats_histograms_version) values  on duplicate key update version = values(version), modify_count = values(modify_count), count = values(count), last_stats_histograms_version = values(last_stats_histograms_version)"
    );
}
