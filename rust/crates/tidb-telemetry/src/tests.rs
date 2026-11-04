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

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use chrono::TimeZone;
use tidb_datatype::Datum;

use super::*;
use crate::data_window::{get_window_data, go_rfc3339_nano};
use crate::ttl::TtlUsageCounter;

struct MockSession {
    globals: HashMap<&'static str, &'static str>,
    rows: Vec<Vec<Datum>>,
}

impl Session for MockSession {
    fn get_global_sys_var(&self, name: &str) -> Result<String, String> {
        self.globals
            .get(name)
            .map(|value| (*value).to_owned())
            .ok_or_else(|| format!("unknown {name}"))
    }

    fn cost_model_version(&self) -> i64 {
        2
    }

    fn enable_paging(&self) -> bool {
        true
    }

    fn exec_restricted_sql(&self, sql: &str) -> Result<Vec<Vec<Datum>>, String> {
        if sql.contains("TIDB_PK_TYPE") {
            Ok(self.rows.clone())
        } else {
            Ok(Vec::new())
        }
    }

    fn refresh_txn_ctx(&mut self) -> Result<(), String> {
        Ok(())
    }

    fn info_schema(&self) -> Arc<tidb_infoschema::SessionExtendedInfoSchema> {
        Arc::new(tidb_infoschema::SessionExtendedInfoSchema::new())
    }
}

/// Go `getWindowData` aggregates up to an hour of minute sub windows into
/// one window, and `RotateSubWindow` takes (and zeroes) the counters.
#[test]
fn sub_windows_aggregate_by_hour_and_rotation_takes_the_counters() {
    CURRENT_EXECUTE_COUNT.store(3, Ordering::Relaxed);
    GLOBAL_BUILTIN_FUNCTIONS_USAGE.collect(&BuiltinFunctionsUsage(HashMap::from([("PlusInt".to_owned(), 2)])));
    rotate_sub_window();
    assert_eq!(CURRENT_EXECUTE_COUNT.load(Ordering::Relaxed), 0);
    for _ in 0..60 {
        CURRENT_EXECUTE_COUNT.store(1, Ordering::Relaxed);
        rotate_sub_window();
    }
    let windows = get_window_data();
    assert_eq!(windows.len(), 2, "61 sub windows make one full hour and one more");
    assert_eq!(windows[0].execute_count, 3 + 59);
    assert_eq!(windows[1].execute_count, 1);
    assert_eq!(windows[0].builtin_functions_usage.0.get("PlusInt"), Some(&2));
    assert_eq!(GLOBAL_BUILTIN_FUNCTIONS_USAGE.dump(), BuiltinFunctionsUsage::default());
}

/// Go `UpdateTableHistWithDeleteRows` / `UpdateTableHistWithDelayTime` count
/// into the first bucket whose bound exceeds the value.
#[test]
fn ttl_histograms_count_into_the_first_bucket_above_the_value() {
    let mut counter = TtlUsageCounter {
        table_hist_with_delete_rows: vec![
            TtlHistItem { less_than: Some(10), ..Default::default() },
            TtlHistItem { less_than_max: true, ..Default::default() },
        ],
        table_hist_with_delay_time: vec![
            TtlHistItem { less_than: Some(1), ..Default::default() },
            TtlHistItem { less_than_max: true, ..Default::default() },
        ],
        ..Default::default()
    };
    counter.update_table_hist_with_delete_rows(9);
    counter.update_table_hist_with_delete_rows(10);
    counter.update_table_hist_with_delay_time(3, i64::MAX);
    assert_eq!(
        counter.table_hist_with_delete_rows.iter().map(|item| item.count).collect::<Vec<_>>(),
        [1, 1]
    );
    assert_eq!(
        counter.table_hist_with_delay_time.iter().map(|item| item.count).collect::<Vec<_>>(),
        [0, 3]
    );
    let json = serde_json::to_string(&counter.table_hist_with_delay_time).unwrap();
    assert_eq!(json, r#"[{"less_than":1,"count":0},{"less_than_max":true,"count":3}]"#);
}

/// Go `time.Time` JSON is RFC 3339 with trailing nanosecond zeros trimmed.
#[test]
fn window_begin_at_is_gos_rfc3339_nano() {
    let offset = chrono::FixedOffset::east_opt(8 * 3600).unwrap();
    let time = offset.with_ymd_and_hms(2026, 11, 4, 9, 30, 0).unwrap() + chrono::Duration::nanoseconds(120_000_000);
    let local = time.with_timezone(&chrono::Local);
    let text = go_rfc3339_nano(&local);
    assert!(text.contains(".12"), "{text}");
    assert!(!text.contains(".120"), "{text}");
}

/// Go `getFeatureUsage` over a session: the clustered-index rows, the
/// global variables and the session's own cost model and paging.
#[test]
fn feature_usage_reads_the_sessions_globals_and_tables() {
    let mut session = MockSession {
        globals: HashMap::from([
            (tidb_vardef::tidb_vars::TIDB_ENABLE_ASYNC_COMMIT, "ON"),
            (tidb_vardef::tidb_vars::TIDB_ENABLE1_PC, "OFF"),
            (tidb_vardef::tidb_vars::TIDB_TXN_ASSERTION_LEVEL, "FAST"),
            (tidb_vardef::tidb_vars::TIDB_ENABLE_MDL, "ON"),
            (tidb_vardef::tidb_vars::TIDB_STORE_BATCH_SIZE, "4"),
        ]),
        rows: vec![
            vec![Datum::String(tidb_datatype::StringDatum::new(b"CLUSTERED".as_slice(), tidb_charset::Collation::Utf8Mb4Bin))],
            vec![Datum::String(tidb_datatype::StringDatum::new(b"NONCLUSTERED".as_slice(), tidb_charset::Collation::Utf8Mb4Bin))],
        ],
    };
    let usage = get_feature_usage(&mut session).unwrap();
    assert_eq!(usage.new_cluster_index.num_clustered_tables, 1);
    assert_eq!(usage.new_cluster_index.num_total_tables, 2);
    assert!(usage.txn.async_commit_used);
    assert!(!usage.txn.one_pc_used);
    assert_eq!(usage.txn.assertion_level, "FAST");
    assert!(usage.ddl_usage_counter.metadata_lock_used);
    assert_eq!(usage.store_batch_copr_usage.batch_size, 4);
    assert!(usage.enable_cost_model_ver2);
    assert!(usage.enable_paging);
    let json = serde_json::to_value(&usage).unwrap();
    assert_eq!(json["newClusterIndex"]["numClusteredTables"], 1);
    assert_eq!(json["DDLUsageCounter"]["metadata_lock_used"], true);
    assert_eq!(json["storeBatchCopr"]["batch_size"], 4);
}
