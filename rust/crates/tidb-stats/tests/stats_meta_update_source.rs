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

//! Source-backed tests for statistics-meta update SQL assembly.

use tidb_stats::{
    stats_meta_update_sql, DeltaUpdate, StatsMetaVersionUpdate, UPDATE_STATS_META_VERSION_QUERY,
};

#[test]
fn source_update_partitions_locked_and_unlocked_deltas() {
    let sql = stats_meta_update_sql(
        42,
        &[
            DeltaUpdate::new(7, 3, 5, true),
            DeltaUpdate::new(8, 4, 6, false),
            DeltaUpdate::new(9, 5, -2, false),
        ],
    );
    assert_eq!(
        sql.locked_select.as_deref(),
        Some("select * from mysql.stats_table_locked where table_id in (7) for update")
    );
    assert_eq!(
        sql.unlocked_select.as_deref(),
        Some("select * from mysql.stats_meta where table_id in (8,9) for update")
    );
    assert_eq!(
        sql.locked_insert.as_deref(),
        Some("insert into mysql.stats_table_locked (version, table_id, modify_count, count) values (42, 7, 3, 5) on duplicate key update version = values(version), modify_count = modify_count + values(modify_count), count = count + values(count)")
    );
    assert_eq!(
        sql.unlocked_positive_insert.as_deref(),
        Some("insert into mysql.stats_meta (version, table_id, modify_count, count) values (42, 8, 4, 6) on duplicate key update version = values(version), modify_count = modify_count + values(modify_count), count = count + values(count)")
    );
    assert_eq!(
        sql.unlocked_negative_insert.as_deref(),
        Some("insert into mysql.stats_meta (version, table_id, modify_count, count) values (42, 9, 5, 2) on duplicate key update version = values(version), modify_count = modify_count + values(modify_count), count = if(count > values(count), count - values(count), 0)")
    );
    assert_eq!(sql.cache_invalidate_ids, vec![8, 9]);
}

#[test]
fn source_update_preserves_order_duplicates_and_min_int_delta() {
    let sql = stats_meta_update_sql(
        u64::MAX,
        &[
            DeltaUpdate::new(-1, i64::MIN, i64::MIN, false),
            DeltaUpdate::new(-1, i64::MAX, 0, false),
        ],
    );
    assert_eq!(
        sql.unlocked_select.as_deref(),
        Some("select * from mysql.stats_meta where table_id in (-1,-1) for update")
    );
    assert_eq!(
        sql.unlocked_positive_insert.as_deref(),
        Some("insert into mysql.stats_meta (version, table_id, modify_count, count) values (18446744073709551615, -1, 9223372036854775807, 0) on duplicate key update version = values(version), modify_count = modify_count + values(modify_count), count = count + values(count)")
    );
    assert_eq!(
        sql.unlocked_negative_insert.as_deref(),
        Some("insert into mysql.stats_meta (version, table_id, modify_count, count) values (18446744073709551615, -1, -9223372036854775808, -9223372036854775808) on duplicate key update version = values(version), modify_count = modify_count + values(modify_count), count = if(count > values(count), count - values(count), 0)")
    );
    assert_eq!(sql.cache_invalidate_ids, vec![-1, -1]);
}

#[test]
fn source_update_empty_input_is_a_noop() {
    assert_eq!(stats_meta_update_sql(1, &[]), Default::default());
}

#[test]
fn source_version_update_query_and_argument_order() {
    let update = StatsMetaVersionUpdate::new(123, -9);
    assert_eq!(
        UPDATE_STATS_META_VERSION_QUERY,
        "update mysql.stats_meta set version=%?, last_stats_histograms_version=%? where table_id =%?"
    );
    assert_eq!(update.parameters(), (123, 123, -9));
}
