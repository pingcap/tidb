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

//! Source-backed tests for bootstrap statistics SQL generation.

use tidb_stats::{gen_init_stats_histograms_sql, gen_init_stats_meta_sql, HistSqlOptions};

#[test]
fn source_histograms_sql_all_records_matches_go() {
    let options = HistSqlOptions::for_table_ids(&[]).expect("empty IDs are valid");
    assert_eq!(
        gen_init_stats_histograms_sql(&options),
        "select /*+ ORDER_INDEX(mysql.stats_histograms,tbl) */ HIGH_PRIORITY table_id, is_index, hist_id, distinct_count, version, null_count, cm_sketch, tot_col_size, stats_ver, correlation from mysql.stats_histograms order by table_id"
    );
}

#[test]
fn source_histograms_sql_paging_matches_go() {
    let options = HistSqlOptions::for_paging(100, 200).expect("range is valid");
    assert_eq!(
        gen_init_stats_histograms_sql(&options),
        "select /*+ ORDER_INDEX(mysql.stats_histograms,tbl) */ HIGH_PRIORITY table_id, is_index, hist_id, distinct_count, version, null_count, cm_sketch, tot_col_size, stats_ver, correlation from mysql.stats_histograms where table_id >= 100 and table_id < 200 order by table_id"
    );
}

#[test]
fn source_histograms_sql_table_ids_matches_go() {
    let options = HistSqlOptions::for_table_ids(&[5, 2, 7]).expect("IDs are valid");
    assert_eq!(
        gen_init_stats_histograms_sql(&options),
        "select /*+ ORDER_INDEX(mysql.stats_histograms,tbl) */ HIGH_PRIORITY table_id, is_index, hist_id, distinct_count, version, null_count, cm_sketch, tot_col_size, stats_ver, correlation from mysql.stats_histograms where table_id in (5,2,7) order by table_id"
    );
}

#[test]
fn source_meta_sql_all_records_matches_go() {
    assert_eq!(
        gen_init_stats_meta_sql(&[]),
        "select HIGH_PRIORITY version, table_id, modify_count, count, snapshot, last_stats_histograms_version from mysql.stats_meta"
    );
}

#[test]
fn source_meta_sql_table_ids_matches_go() {
    assert_eq!(
        gen_init_stats_meta_sql(&[5, 2, 7]),
        "select HIGH_PRIORITY version, table_id, modify_count, count, snapshot, last_stats_histograms_version from mysql.stats_meta where table_id in (5,2,7)"
    );
}

#[test]
fn source_histogram_options_reject_invalid_source_states() {
    assert!(HistSqlOptions::for_paging(200, 100).is_none());
    assert!(HistSqlOptions::for_table_ids(&[-1]).is_none());
}
