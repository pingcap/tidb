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

//! `INFORMATION_SCHEMA.INSPECTION_RESULT` against the mock tables and the
//! expected rows of Go's `pkg/executor/inspection_result_test.go`.

use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard, PoisonError};

use tidb_datatype::{CoreTime, Datum, Time, TimeType};
use tidb_executor::memtable_reader::TableSnapshot;

use crate::tests_support::{row_text, warnings_of};
use crate::Session;

/// Go `types.MakeDatums`'s string.
fn s(text: &str) -> Datum {
    Datum::Bytes(text.as_bytes().to_vec())
}

/// Go `parseTime(t, tk.Session(), str)`: a `DATETIME(6)`.
fn dt(text: &str) -> Datum {
    let field = |range: std::ops::Range<usize>| text[range].parse::<u8>().unwrap();
    let time = CoreTime::from_date(
        text[0..4].parse().unwrap(),
        field(5..7),
        field(8..10),
        field(11..13),
        field(14..16),
        field(17..19),
        0,
    );
    Datum::Time(Time::new(time, TimeType::DateTime, 6).unwrap())
}

/// The mocks are the process's: one inspection at a time.
static MOCKS: Mutex<()> = Mutex::new(());

/// The lock every test that sets the process's metric and cluster mocks
/// holds.
pub(crate) fn lock_mocks() -> MutexGuard<'static, ()> {
    MOCKS.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Go `createInspectionContext(t, mockData, configurations)`: the metric
/// tables and the cluster tables an inspection reads.
struct InspectionContext {
    session: Session,
    _mocks: MutexGuard<'static, ()>,
}

impl InspectionContext {
    fn new(metrics: HashMap<String, Vec<Vec<Datum>>>, tables: HashMap<String, Vec<Vec<Datum>>>) -> Self {
        let mocks = lock_mocks();
        let tables = tables
            .into_iter()
            .map(|(name, mut rows)| {
                // Go reads a mocked row by column offset, so its tests leave
                // CLUSTER_INFO's trailing columns out.
                if name == "CLUSTER_INFO" {
                    rows.iter_mut().for_each(|row| row.resize(8, Datum::Null));
                }
                (name, TableSnapshot { rows, err: None })
            })
            .collect();
        tidb_executor::inspection_result::mock::set_inspection_tables(Some(tables));
        let mut context = Self {
            session: Session::new(),
            _mocks: mocks,
        };
        context.set_metrics(metrics);
        context.session.run("use test").unwrap();
        context
    }

    fn set_metrics(&mut self, metrics: HashMap<String, Vec<Vec<Datum>>>) {
        tidb_executor::metrics_reader::mock::set_metrics_table_data(Some(metrics));
    }

    /// The statement's rows as Go's `testkit.Rows` writes them, and no
    /// warning.
    fn query(&mut self, sql: &str) -> Vec<String> {
        let rows = row_text(self.session.run(sql));
        assert_eq!(warnings_of(&self.session), [], "{sql}");
        rows.into_iter().map(|row| row.join(" ")).collect()
    }
}

impl Drop for InspectionContext {
    fn drop(&mut self) {
        tidb_executor::inspection_result::mock::set_inspection_tables(None);
        tidb_executor::metrics_reader::mock::set_metrics_table_data(None);
        tidb_executor::cluster_state::mock::set_cluster_log_server_info(None);
    }
}

/// Go `TestInspectionResult`.
#[test]
fn the_config_and_version_rules_report_inconsistencies_like_go() {
    let mut context = InspectionContext::new(inspection_result_metrics(), inspection_result_tables());
    let cases = [
        (
            "select rule, item, type, value, reference, severity, details from information_schema.inspection_result where rule in ('config', 'version')",
            INSPECTION_RESULT_CONFIG_VERSION,
        ),
        (
            "select rule, item, type, value, reference, severity, details from information_schema.inspection_result where rule in ('config', 'version') and item in ('coprocessor.high', 'git_hash') and type='tikv'",
            INSPECTION_RESULT_TIKV,
        ),
        (
            "select rule, item, type, value, reference, severity, details from information_schema.inspection_result where rule='config'",
            INSPECTION_RESULT_CONFIG,
        ),
        (
            "select rule, item, type, value, reference, severity, details from information_schema.inspection_result where rule='version' and item='git_hash' and type in ('pd', 'tidb')",
            INSPECTION_RESULT_VERSION,
        ),
    ];
    for (sql, rows) in cases {
        assert_eq!(context.query(sql), rows, "{sql}");
    }
}

/// Go `TestThresholdCheckInspection`.
#[test]
fn the_threshold_check_rule_reports_busy_tikv_threads_like_go() {
    let mut context = InspectionContext::new(threshold_check_metrics(), default_tables());
    assert_eq!(
        context.query("select /*+ time_range('2020-02-12 10:35:00','2020-02-12 10:37:00') */ item, type, instance,status_address, value, reference, details from information_schema.inspection_result where rule='threshold-check' order by item"),
        THRESHOLD_CHECK
    );

    // construct some mock normal data
    let mut metrics = threshold_check_metrics();
    metrics.extend(threshold_check_normal_cpu());
    context.set_metrics(metrics);
    assert_eq!(
        context.query("select /*+ time_range('2020-02-12 10:35:00','2020-02-12 10:37:00') */ item, type, instance,status_address, value, reference from information_schema.inspection_result where rule='threshold-check' order by item"),
        ["grpc-cpu tikv tikv-0 tikv-0s 7.42 < 7.20, config: server.grpc-concurrency=8"]
    );
}

/// Go `TestThresholdCheckInspection2`.
#[test]
fn the_threshold_check_rule_reports_slow_durations_and_low_cache_hits_like_go() {
    let mut context = InspectionContext::new(threshold_check2_metrics(), default_tables());
    assert_eq!(
        context.query("select /*+ time_range('2020-02-12 10:35:00','2020-02-12 10:37:00') */ item, type, instance, status_address, value, reference, details from information_schema.inspection_result where rule='threshold-check' order by item"),
        THRESHOLD_CHECK2
    );
}

/// Go `TestThresholdCheckInspection3`.
#[test]
fn the_threshold_check_rule_reports_store_imbalance_and_leader_drops_like_go() {
    let mut context = InspectionContext::new(threshold_check3_metrics(), default_tables());
    assert_eq!(
        context.query(
            "select /*+ time_range('2020-02-14 04:20:00','2020-02-14 05:23:00') */
		item, type, instance,status_address, value, reference, details from information_schema.inspection_result
		where rule='threshold-check' and item in ('leader-score-balance','region-score-balance','region-count','region-health','store-available-balance','leader-drop')
		order by item"
        ),
        THRESHOLD_CHECK3
    );
}

/// Go `TestCriticalErrorInspection`.
#[test]
fn the_critical_error_rule_reports_errors_and_downed_servers_like_go() {
    let mut context = InspectionContext::new(critical_error_metrics(), default_tables());
    // Go's `mockClusterLogServerInfo` names diagnostics servers over empty
    // log files: the cluster has no log line to report.
    tidb_executor::cluster_state::mock::set_cluster_log_server_info(Some(Vec::new()));
    assert_eq!(
        context.query("select /*+ time_range('2020-02-12 10:35:00','2020-02-12 10:37:00') */ item, instance,status_address, value, details from information_schema.inspection_result where rule='critical-error'"),
        CRITICAL_ERROR
    );
}

/// Go `TestNodeLoadInspection`.
#[test]
fn the_node_load_rule_reports_loaded_nodes_like_go() {
    let mut context = InspectionContext::new(node_load_metrics(), default_tables());
    assert_eq!(
        context.query(
            "select /*+ time_range('2020-02-14 04:20:00','2020-02-14 05:23:00') */
		item, type, instance, value, reference, details from information_schema.inspection_result
		where rule='node-load' order by item, value"
        ),
        NODE_LOAD
    );
}

/// Go `TestConfigCheckOfStorageBlockCacheSize`.
#[test]
fn the_config_rule_reports_a_block_cache_larger_than_the_nodes_memory_share_like_go() {
    let mut context = InspectionContext::new(block_cache_metrics(), block_cache_tables());
    assert_eq!(
        context.query("select  /*+ time_range('2020-02-14 04:20:00','2020-02-14 05:23:00') */ * from information_schema.inspection_result where rule='config' and item='storage.block-cache.capacity' order by value"),
        BLOCK_CACHE
    );
}

fn inspection_result_tables() -> HashMap<String, Vec<Vec<Datum>>> {
    HashMap::from([
        (
            "CLUSTER_CONFIG".to_owned(),
            vec![
                vec![s("tidb"), s("192.168.3.22:4000"), s("ddl.lease"), s("1")],
                vec![s("tidb"), s("192.168.3.23:4000"), s("ddl.lease"), s("2")],
                vec![s("tidb"), s("192.168.3.24:4000"), s("ddl.lease"), s("1")],
                vec![s("tidb"), s("192.168.3.25:4000"), s("ddl.lease"), s("1")],
                vec![s("tidb"), s("192.168.3.24:4000"), s("status.status-port"), s("10080")],
                vec![s("tidb"), s("192.168.3.25:4000"), s("status.status-port"), s("10081")],
                vec![s("tidb"), s("192.168.3.24:4000"), s("log.slow-threshold"), s("0")],
                vec![s("tidb"), s("192.168.3.25:4000"), s("log.slow-threshold"), s("1")],
                vec![s("tikv"), s("192.168.3.32:26600"), s("coprocessor.high"), s("8")],
                vec![s("tikv"), s("192.168.3.33:26600"), s("coprocessor.high"), s("8")],
                vec![s("tikv"), s("192.168.3.34:26600"), s("coprocessor.high"), s("7")],
                vec![s("tikv"), s("192.168.3.35:26600"), s("coprocessor.high"), s("7")],
                vec![s("tikv"), s("192.168.3.35:26600"), s("raftstore.sync-log"), s("false")],
                vec![s("pd"), s("192.168.3.32:2379"), s("scheduler.limit"), s("3")],
                vec![s("pd"), s("192.168.3.33:2379"), s("scheduler.limit"), s("3")],
                vec![s("pd"), s("192.168.3.34:2379"), s("scheduler.limit"), s("3")],
                vec![s("pd"), s("192.168.3.35:2379"), s("scheduler.limit"), s("3")],
                vec![s("pd"), s("192.168.3.34:2379"), s("advertise-client-urls"), s("0")],
                vec![s("pd"), s("192.168.3.35:2379"), s("advertise-client-urls"), s("1")],
                vec![s("pd"), s("192.168.3.34:2379"), s("advertise-peer-urls"), s("0")],
                vec![s("pd"), s("192.168.3.35:2379"), s("advertise-peer-urls"), s("1")],
                vec![s("pd"), s("192.168.3.34:2379"), s("client-urls"), s("0")],
                vec![s("pd"), s("192.168.3.35:2379"), s("client-urls"), s("1")],
                vec![s("pd"), s("192.168.3.34:2379"), s("log.file.filename"), s("0")],
                vec![s("pd"), s("192.168.3.35:2379"), s("log.file.filename"), s("1")],
                vec![s("pd"), s("192.168.3.34:2379"), s("metric.job"), s("0")],
                vec![s("pd"), s("192.168.3.35:2379"), s("metric.job"), s("1")],
                vec![s("pd"), s("192.168.3.34:2379"), s("name"), s("0")],
                vec![s("pd"), s("192.168.3.35:2379"), s("name"), s("1")],
                vec![s("pd"), s("192.168.3.34:2379"), s("peer-urls"), s("0")],
                vec![s("pd"), s("192.168.3.35:2379"), s("peer-urls"), s("1")],
            ],
        ),
        (
            "CLUSTER_INFO".to_owned(),
            vec![
                vec![s("tidb"), s("192.168.1.11:1234"), s("192.168.1.11:1234"), s("4.0"), s("a234c")],
                vec![s("tidb"), s("192.168.1.12:1234"), s("192.168.1.11:1234"), s("4.0"), s("a234d")],
                vec![s("tidb"), s("192.168.1.13:1234"), s("192.168.1.11:1234"), s("4.0"), s("a234e")],
                vec![s("tikv"), s("192.168.1.21:1234"), s("192.168.1.21:1234"), s("4.0"), s("c234d")],
                vec![s("tikv"), s("192.168.1.22:1234"), s("192.168.1.22:1234"), s("4.0"), s("c234d")],
                vec![s("tikv"), s("192.168.1.23:1234"), s("192.168.1.23:1234"), s("4.0"), s("c234e")],
                vec![s("pd"), s("192.168.1.31:1234"), s("192.168.1.31:1234"), s("4.0"), s("m234c")],
                vec![s("pd"), s("192.168.1.32:1234"), s("192.168.1.32:1234"), s("4.0"), s("m234d")],
                vec![s("pd"), s("192.168.1.33:1234"), s("192.168.1.33:1234"), s("4.0"), s("m234e")],
            ],
        ),
        (
            "CLUSTER_HARDWARE".to_owned(),
            vec![
                vec![s("tikv"), s("192.168.1.22:1234"), s("disk"), s("sda"), s("used-percent"), s("80")],
                vec![s("tikv"), s("192.168.1.23:1234"), s("disk"), s("sdb"), s("used-percent"), s("50")],
                vec![s("pd"), s("192.168.1.31:1234"), s("cpu"), s("cpu"), s("cpu-logical-cores"), s("1")],
                vec![s("pd"), s("192.168.1.32:1234"), s("cpu"), s("cpu"), s("cpu-logical-cores"), s("4")],
                vec![s("pd"), s("192.168.1.33:1234"), s("cpu"), s("cpu"), s("cpu-logical-cores"), s("10")],
            ],
        ),
        (
            "CLUSTER_SYSTEMINFO".to_owned(),
            vec![
                vec![s("pd"), s("pd-0"), s("system"), s("kernel"), s("transparent_hugepage_enabled"), s("always madvise [never]")],
                vec![s("tikv"), s("tikv-2"), s("system"), s("kernel"), s("transparent_hugepage_enabled"), s("[always] madvise never")],
                vec![s("tidb"), s("tidb-0"), s("system"), s("kernel"), s("transparent_hugepage_enabled"), s("always madvise [never]")],
            ],
        ),
    ])
}

fn inspection_result_metrics() -> HashMap<String, Vec<Vec<Datum>>> {
    HashMap::from([
        (
            "node_total_memory".to_owned(),
            vec![
                vec![dt("2020-02-14 05:20:00"), s("192.168.3.33:26600"), Datum::Real(53687091200.0)],
                vec![dt("2020-02-14 05:20:00"), s("192.168.3.34:26600"), Datum::Real(53687091200.0)],
                vec![dt("2020-02-14 05:20:00"), s("192.168.3.35:26600"), Datum::Real(53687091200.0)],
            ],
        ),
    ])
}

const INSPECTION_RESULT_CONFIG_VERSION: &[&str] = &[
    "config coprocessor.high tikv inconsistent consistent warning 192.168.3.32:26600,192.168.3.33:26600 config value is 8\n192.168.3.34:26600,192.168.3.35:26600 config value is 7",
    "config ddl.lease tidb inconsistent consistent warning 192.168.3.22:4000,192.168.3.24:4000,192.168.3.25:4000 config value is 1\n192.168.3.23:4000 config value is 2",
    "config log.slow-threshold tidb 0 > 0 warning slow-threshold = 0 will record every query to slow log, it may affect performance",
    "config log.slow-threshold tidb inconsistent consistent warning 192.168.3.24:4000 config value is 0\n192.168.3.25:4000 config value is 1",
    "config raftstore.sync-log tikv false true warning sync-log should be true to avoid recover region when the machine breaks down",
    "config transparent_hugepage_enabled tikv [always] madvise never always madvise [never] warning Transparent HugePages can cause memory allocation delays during runtime, TiDB recommends that you disable Transparent HugePages on all TiDB servers",
    "version git_hash pd inconsistent consistent critical the cluster has 3 different pd versions, execute the sql to see more detail: select * from information_schema.cluster_info where type='pd'",
    "version git_hash tidb inconsistent consistent critical the cluster has 3 different tidb versions, execute the sql to see more detail: select * from information_schema.cluster_info where type='tidb'",
    "version git_hash tikv inconsistent consistent critical the cluster has 2 different tikv versions, execute the sql to see more detail: select * from information_schema.cluster_info where type='tikv'",
];

const INSPECTION_RESULT_TIKV: &[&str] = &[
    "config coprocessor.high tikv inconsistent consistent warning 192.168.3.32:26600,192.168.3.33:26600 config value is 8\n192.168.3.34:26600,192.168.3.35:26600 config value is 7",
    "version git_hash tikv inconsistent consistent critical the cluster has 2 different tikv versions, execute the sql to see more detail: select * from information_schema.cluster_info where type='tikv'",
];

const INSPECTION_RESULT_CONFIG: &[&str] = &[
    "config coprocessor.high tikv inconsistent consistent warning 192.168.3.32:26600,192.168.3.33:26600 config value is 8\n192.168.3.34:26600,192.168.3.35:26600 config value is 7",
    "config ddl.lease tidb inconsistent consistent warning 192.168.3.22:4000,192.168.3.24:4000,192.168.3.25:4000 config value is 1\n192.168.3.23:4000 config value is 2",
    "config log.slow-threshold tidb 0 > 0 warning slow-threshold = 0 will record every query to slow log, it may affect performance",
    "config log.slow-threshold tidb inconsistent consistent warning 192.168.3.24:4000 config value is 0\n192.168.3.25:4000 config value is 1",
    "config raftstore.sync-log tikv false true warning sync-log should be true to avoid recover region when the machine breaks down",
    "config transparent_hugepage_enabled tikv [always] madvise never always madvise [never] warning Transparent HugePages can cause memory allocation delays during runtime, TiDB recommends that you disable Transparent HugePages on all TiDB servers",
];

const INSPECTION_RESULT_VERSION: &[&str] = &[
    "version git_hash pd inconsistent consistent critical the cluster has 3 different pd versions, execute the sql to see more detail: select * from information_schema.cluster_info where type='pd'",
    "version git_hash tidb inconsistent consistent critical the cluster has 3 different tidb versions, execute the sql to see more detail: select * from information_schema.cluster_info where type='tidb'",
];

fn default_tables() -> HashMap<String, Vec<Vec<Datum>>> {
    HashMap::from([
        (
            "CLUSTER_CONFIG".to_owned(),
            vec![
                vec![s("tikv"), s("tikv-0"), s("raftstore.apply-pool-size"), s("2")],
                vec![s("tikv"), s("tikv-0"), s("raftstore.store-pool-size"), s("2")],
                vec![s("tikv"), s("tikv-0"), s("readpool.coprocessor.high-concurrency"), s("4")],
                vec![s("tikv"), s("tikv-0"), s("readpool.coprocessor.low-concurrency"), s("4")],
                vec![s("tikv"), s("tikv-0"), s("readpool.coprocessor.normal-concurrency"), s("4")],
                vec![s("tikv"), s("tikv-1"), s("readpool.coprocessor.normal-concurrency"), s("8")],
                vec![s("tikv"), s("tikv-0"), s("readpool.storage.high-concurrency"), s("4")],
                vec![s("tikv"), s("tikv-0"), s("readpool.storage.low-concurrency"), s("4")],
                vec![s("tikv"), s("tikv-0"), s("readpool.storage.normal-concurrency"), s("4")],
                vec![s("tikv"), s("tikv-0"), s("server.grpc-concurrency"), s("8")],
                vec![s("tikv"), s("tikv-0"), s("storage.scheduler-worker-pool-size"), s("6")],
            ],
        ),
        (
            "CLUSTER_INFO".to_owned(),
            vec![
                vec![s("pd"), s("pd-0"), s("pd-0"), s("4.0"), s("a234c"), Datum::Null, s("")],
                vec![s("tidb"), s("tidb-0"), s("tidb-0s"), s("4.0"), s("a234c"), Datum::Null, s("")],
                vec![s("tidb"), s("tidb-1"), s("tidb-1s"), s("4.0"), s("a234c"), Datum::Null, s("")],
                vec![s("tikv"), s("tikv-0"), s("tikv-0s"), s("4.0"), s("a234c"), Datum::Null, s("")],
                vec![s("tikv"), s("tikv-1"), s("tikv-1s"), s("4.0"), s("a234c"), Datum::Null, s("")],
                vec![s("tikv"), s("tikv-2"), s("tikv-2s"), s("4.0"), s("a234c"), Datum::Null, s("")],
            ],
        ),
        (
            "CLUSTER_SYSTEMINFO".to_owned(),
            vec![
                vec![s("pd"), s("pd-0"), s("system"), s("kernel"), s("transparent_hugepage_enabled"), s("always madvise [never]")],
                vec![s("tikv"), s("tikv-2"), s("system"), s("kernel"), s("transparent_hugepage_enabled"), s("always madvise [never]")],
                vec![s("tidb"), s("tidb-0"), s("system"), s("kernel"), s("transparent_hugepage_enabled"), s("always madvise [never]")],
            ],
        ),
    ])
}

fn threshold_check_metrics() -> HashMap<String, Vec<Vec<Datum>>> {
    HashMap::from([
        (
            "tikv_thread_cpu".to_owned(),
            vec![
                vec![dt("2020-02-14 05:20:00"), s("tikv-0s"), s("cop_normal0"), Datum::Real(10.0)],
                vec![dt("2020-02-14 05:20:00"), s("tikv-0s"), s("cop_normal1"), Datum::Real(10.0)],
                vec![dt("2020-02-14 05:20:00"), s("tikv-1s"), s("cop_normal0"), Datum::Real(10.0)],
                vec![dt("2020-02-14 05:20:00"), s("tikv-0s"), s("cop_high1"), Datum::Real(10.0)],
                vec![dt("2020-02-14 05:20:00"), s("tikv-0s"), s("cop_high2"), Datum::Real(10.0)],
                vec![dt("2020-02-14 05:21:00"), s("tikv-0s"), s("cop_high1"), Datum::Real(5.0)],
                vec![dt("2020-02-14 05:22:00"), s("tikv-0s"), s("cop_high1"), Datum::Real(1.0)],
                vec![dt("2020-02-14 05:20:00"), s("tikv-0s"), s("cop_low1"), Datum::Real(10.0)],
                vec![dt("2020-02-14 05:20:00"), s("tikv-0s"), s("grpc_1"), Datum::Real(10.0)],
                vec![dt("2020-02-14 05:20:00"), s("tikv-0s"), s("raftstore_1"), Datum::Real(10.0)],
                vec![dt("2020-02-14 05:20:00"), s("tikv-0s"), s("apply_0"), Datum::Real(10.0)],
                vec![dt("2020-02-14 05:20:00"), s("tikv-0s"), s("store_read_norm1"), Datum::Real(10.0)],
                vec![dt("2020-02-14 05:20:00"), s("tikv-0s"), s("store_read_high2"), Datum::Real(10.0)],
                vec![dt("2020-02-14 05:20:00"), s("tikv-0s"), s("store_read_low0"), Datum::Real(10.0)],
                vec![dt("2020-02-14 05:20:00"), s("tikv-0s"), s("sched_2"), Datum::Real(10.0)],
                vec![dt("2020-02-14 05:20:00"), s("tikv-0s"), s("split_check"), Datum::Real(10.0)],
            ],
        ),
        ("pd_tso_wait_duration".to_owned(), Vec::new()),
        ("tidb_get_token_duration".to_owned(), Vec::new()),
        ("tidb_load_schema_duration".to_owned(), Vec::new()),
        ("tikv_scheduler_command_duration".to_owned(), Vec::new()),
        ("tikv_handle_snapshot_duration".to_owned(), Vec::new()),
        ("tikv_storage_async_request_duration".to_owned(), Vec::new()),
        ("tikv_engine_write_duration".to_owned(), Vec::new()),
        ("tikv_engine_max_get_duration".to_owned(), Vec::new()),
        ("tikv_engine_max_seek_duration".to_owned(), Vec::new()),
        ("tikv_scheduler_pending_commands".to_owned(), Vec::new()),
        ("tikv_block_index_cache_hit".to_owned(), Vec::new()),
        ("tikv_block_data_cache_hit".to_owned(), Vec::new()),
        ("tikv_block_filter_cache_hit".to_owned(), Vec::new()),
        ("pd_scheduler_store_status".to_owned(), Vec::new()),
        ("pd_region_health".to_owned(), Vec::new()),
    ])
}

const THRESHOLD_CHECK: &[&str] = &[
    "apply-cpu tikv tikv-0 tikv-0s 10.00 < 1.60, config: raftstore.apply-pool-size=2 the 'apply-cpu' max cpu-usage of tikv-0s tikv is too high",
    "coprocessor-high-cpu tikv tikv-0 tikv-0s 20.00 < 3.60, config: readpool.coprocessor.high-concurrency=4 the 'coprocessor-high-cpu' max cpu-usage of tikv-0s tikv is too high",
    "coprocessor-low-cpu tikv tikv-0 tikv-0s 10.00 < 3.60, config: readpool.coprocessor.low-concurrency=4 the 'coprocessor-low-cpu' max cpu-usage of tikv-0s tikv is too high",
    "coprocessor-normal-cpu tikv tikv-0 tikv-0s 20.00 < 3.60, config: readpool.coprocessor.normal-concurrency=4 the 'coprocessor-normal-cpu' max cpu-usage of tikv-0s tikv is too high",
    "coprocessor-normal-cpu tikv tikv-1 tikv-1s 10.00 < 7.20, config: readpool.coprocessor.normal-concurrency=8 the 'coprocessor-normal-cpu' max cpu-usage of tikv-1s tikv is too high",
    "grpc-cpu tikv tikv-0 tikv-0s 10.00 < 7.20, config: server.grpc-concurrency=8 the 'grpc-cpu' max cpu-usage of tikv-0s tikv is too high",
    "raftstore-cpu tikv tikv-0 tikv-0s 10.00 < 1.60, config: raftstore.store-pool-size=2 the 'raftstore-cpu' max cpu-usage of tikv-0s tikv is too high",
    "scheduler-worker-cpu tikv tikv-0 tikv-0s 10.00 < 5.10, config: storage.scheduler-worker-pool-size=6 the 'scheduler-worker-cpu' max cpu-usage of tikv-0s tikv is too high",
    "split-check-cpu tikv tikv-0 tikv-0s 10.00 < 0.00 the 'split-check-cpu' max cpu-usage of tikv-0s tikv is too high",
    "storage-readpool-high-cpu tikv tikv-0 tikv-0s 10.00 < 3.60, config: readpool.storage.high-concurrency=4 the 'storage-readpool-high-cpu' max cpu-usage of tikv-0s tikv is too high",
    "storage-readpool-low-cpu tikv tikv-0 tikv-0s 10.00 < 3.60, config: readpool.storage.low-concurrency=4 the 'storage-readpool-low-cpu' max cpu-usage of tikv-0s tikv is too high",
    "storage-readpool-normal-cpu tikv tikv-0 tikv-0s 10.00 < 3.60, config: readpool.storage.normal-concurrency=4 the 'storage-readpool-normal-cpu' max cpu-usage of tikv-0s tikv is too high",
];

fn threshold_check_normal_cpu() -> HashMap<String, Vec<Vec<Datum>>> {
    HashMap::from([
        (
            "tikv_thread_cpu".to_owned(),
            vec![
                vec![dt("2020-02-14 05:20:00"), s("tikv-0s"), s("cop_normal0"), Datum::Real(1.0)],
                vec![dt("2020-02-14 05:20:00"), s("tikv-0s"), s("cop_high1"), Datum::Real(0.1)],
                vec![dt("2020-02-14 05:20:00"), s("tikv-0s"), s("cop_low1"), Datum::Real(1.0)],
                vec![dt("2020-02-14 05:20:00"), s("tikv-0s"), s("grpc_1"), Datum::Real(7.21)],
                vec![dt("2020-02-14 05:20:00"), s("tikv-0s"), s("grpc_2"), Datum::Real(0.21)],
                vec![dt("2020-02-14 05:20:00"), s("tikv-0s"), s("raftstore_1"), Datum::Real(1.0)],
                vec![dt("2020-02-14 05:20:00"), s("tikv-0s"), s("apply_0"), Datum::Real(1.0)],
                vec![dt("2020-02-14 05:20:00"), s("tikv-0s"), s("store_read_norm1"), Datum::Real(1.0)],
                vec![dt("2020-02-14 05:20:00"), s("tikv-0s"), s("store_read_high2"), Datum::Real(1.0)],
                vec![dt("2020-02-14 05:20:00"), s("tikv-0s"), s("store_read_low0"), Datum::Real(1.0)],
                vec![dt("2020-02-14 05:20:00"), s("tikv-0s"), s("sched_2"), Datum::Real(0.3)],
                vec![dt("2020-02-14 05:20:00"), s("tikv-0s"), s("split_check"), Datum::Real(0.5)],
            ],
        ),
    ])
}

fn threshold_check2_metrics() -> HashMap<String, Vec<Vec<Datum>>> {
    HashMap::from([
        (
            "pd_tso_wait_duration".to_owned(),
            vec![
                vec![dt("2020-02-14 05:20:00"), s("pd-0"), Datum::Real(0.999), Datum::Real(0.06)],
            ],
        ),
        (
            "tidb_get_token_duration".to_owned(),
            vec![
                vec![dt("2020-02-14 05:20:00"), s("tidb-0s"), Datum::Real(0.999), Datum::Real(20000.0)],
            ],
        ),
        (
            "tidb_load_schema_duration".to_owned(),
            vec![
                vec![dt("2020-02-14 05:20:00"), s("tidb-0s"), Datum::Real(0.99), Datum::Real(2.0)],
            ],
        ),
        (
            "tikv_scheduler_command_duration".to_owned(),
            vec![
                vec![dt("2020-02-14 05:20:00"), s("tikv-0s"), s("get"), Datum::Real(0.99), Datum::Real(2.0)],
                vec![dt("2020-02-14 05:20:00"), s("tikv-0s"), s("write"), Datum::Real(0.99), Datum::Real(5.0)],
            ],
        ),
        (
            "tikv_handle_snapshot_duration".to_owned(),
            vec![
                vec![dt("2020-02-14 05:20:00"), s("tikv-0s"), s("gen"), Datum::Real(0.999), Datum::Real(40.0)],
                vec![dt("2020-02-14 05:20:00"), s("tikv-0s"), s("read"), Datum::Real(0.999), Datum::Real(10.0)],
            ],
        ),
        (
            "tikv_storage_async_request_duration".to_owned(),
            vec![
                vec![dt("2020-02-14 05:20:00"), s("tikv-0s"), s("write"), Datum::Real(0.999), Datum::Real(0.2)],
                vec![dt("2020-02-14 05:20:00"), s("tikv-0s"), s("snapshot"), Datum::Real(0.999), Datum::Real(0.06)],
            ],
        ),
        (
            "tikv_engine_write_duration".to_owned(),
            vec![
                vec![dt("2020-02-14 05:20:00"), s("tikv-0s"), s("write_max"), s("kv"), Datum::Real(200000.0)],
            ],
        ),
        (
            "tikv_engine_max_get_duration".to_owned(),
            vec![
                vec![dt("2020-02-14 05:20:00"), s("tikv-0s"), s("get_max"), s("kv"), Datum::Real(60000.0)],
            ],
        ),
        (
            "tikv_engine_max_seek_duration".to_owned(),
            vec![
                vec![dt("2020-02-14 05:20:00"), s("tikv-0s"), s("seek_max"), s("raft"), Datum::Real(60000.0)],
            ],
        ),
        (
            "tikv_scheduler_pending_commands".to_owned(),
            vec![
                vec![dt("2020-02-14 05:20:00"), s("tikv-0s"), Datum::Real(1001.0)],
            ],
        ),
        (
            "tikv_block_index_cache_hit".to_owned(),
            vec![
                vec![dt("2020-02-14 05:20:00"), s("tikv-0s"), s("kv"), Datum::Real(0.94)],
            ],
        ),
        (
            "tikv_block_data_cache_hit".to_owned(),
            vec![
                vec![dt("2020-02-14 05:20:00"), s("tikv-0s"), s("kv"), Datum::Real(0.79)],
            ],
        ),
        (
            "tikv_block_filter_cache_hit".to_owned(),
            vec![
                vec![dt("2020-02-14 05:20:00"), s("tikv-0s"), s("kv"), Datum::Real(0.93)],
            ],
        ),
        ("tikv_thread_cpu".to_owned(), Vec::new()),
        ("pd_scheduler_store_status".to_owned(), Vec::new()),
        ("pd_region_health".to_owned(), Vec::new()),
    ])
}

const THRESHOLD_CHECK2: &[&str] = &[
    "data-block-cache-hit tikv tikv-0 tikv-0s 0.790 > 0.800 min data-block-cache-hit rate of tikv-0s tikv is too low",
    "filter-block-cache-hit tikv tikv-0 tikv-0s 0.930 > 0.950 min filter-block-cache-hit rate of tikv-0s tikv is too low",
    "get-token-duration tidb tidb-0 tidb-0s 0.020 < 0.001 max duration of tidb-0s tidb get-token-duration is too slow",
    "handle-snapshot-duration tikv tikv-0 tikv-0s 40.000 < 30.000 max duration of tikv-0s tikv handle-snapshot-duration is too slow",
    "index-block-cache-hit tikv tikv-0 tikv-0s 0.940 > 0.950 min index-block-cache-hit rate of tikv-0s tikv is too low",
    "load-schema-duration tidb tidb-0 tidb-0s 2.000 < 1.000 max duration of tidb-0s tidb load-schema-duration is too slow",
    "rocksdb-get-duration tikv tikv-0 tikv-0s 0.060 < 0.050 max duration of tikv-0s tikv rocksdb-get-duration is too slow",
    "rocksdb-seek-duration tikv tikv-0 tikv-0s 0.060 < 0.050 max duration of tikv-0s tikv rocksdb-seek-duration is too slow",
    "rocksdb-write-duration tikv tikv-0 tikv-0s 0.200 < 0.100 max duration of tikv-0s tikv rocksdb-write-duration is too slow",
    "scheduler-cmd-duration tikv tikv-0 tikv-0s 5.000 < 0.100 max duration of tikv-0s tikv scheduler-cmd-duration is too slow",
    "scheduler-pending-cmd-count tikv tikv-0 tikv-0s 1001.000 < 1000.000  tikv-0s tikv scheduler has too many pending commands",
    "storage-snapshot-duration tikv tikv-0 tikv-0s 0.060 < 0.050 max duration of tikv-0s tikv storage-snapshot-duration is too slow",
    "storage-write-duration tikv tikv-0 tikv-0s 0.200 < 0.100 max duration of tikv-0s tikv storage-write-duration is too slow",
    "tso-duration tidb pd-0 pd-0 0.060 < 0.050 max duration of pd-0 tidb tso-duration is too slow",
];

fn threshold_check3_metrics() -> HashMap<String, Vec<Vec<Datum>>> {
    HashMap::from([
        (
            "pd_scheduler_store_status".to_owned(),
            vec![
                vec![dt("2020-02-14 05:20:00"), s("pd-0"), s("tikv-0"), s("0"), s("leader_score"), Datum::Real(100.0)],
                vec![dt("2020-02-14 05:20:00"), s("pd-0"), s("tikv-1"), s("1"), s("leader_score"), Datum::Real(50.0)],
                vec![dt("2020-02-14 05:21:00"), s("pd-0"), s("tikv-0"), s("0"), s("leader_score"), Datum::Real(99.0)],
                vec![dt("2020-02-14 05:21:00"), s("pd-0"), s("tikv-1"), s("1"), s("leader_score"), Datum::Real(51.0)],
                vec![dt("2020-02-14 05:20:00"), s("pd-0"), s("tikv-0"), s("0"), s("region_score"), Datum::Real(100.0)],
                vec![dt("2020-02-14 05:20:00"), s("pd-0"), s("tikv-1"), s("1"), s("region_score"), Datum::Real(90.0)],
                vec![dt("2020-02-14 05:20:00"), s("pd-0"), s("tikv-0"), s("0"), s("store_available"), Datum::Real(100.0)],
                vec![dt("2020-02-14 05:20:00"), s("pd-0"), s("tikv-1"), s("1"), s("store_available"), Datum::Real(70.0)],
                vec![dt("2020-02-14 05:20:00"), s("pd-0"), s("tikv-0"), s("0"), s("region_count"), Datum::Real(20001.0)],
                vec![dt("2020-02-14 05:20:00"), s("pd-0"), s("tikv-0"), s("0"), s("leader_count"), Datum::Real(10000.0)],
                vec![dt("2020-02-14 05:21:00"), s("pd-0"), s("tikv-0"), s("0"), s("leader_count"), Datum::Real(5000.0)],
                vec![dt("2020-02-14 05:22:00"), s("pd-0"), s("tikv-0"), s("0"), s("leader_count"), Datum::Real(5000.0)],
                vec![dt("2020-02-14 05:20:00"), s("pd-0"), s("tikv-1"), s("0"), s("leader_count"), Datum::Real(5000.0)],
                vec![dt("2020-02-14 05:21:00"), s("pd-0"), s("tikv-1"), s("0"), s("leader_count"), Datum::Real(10000.0)],
                vec![dt("2020-02-14 05:20:00"), s("pd-0"), s("tikv-2"), s("0"), s("leader_count"), Datum::Real(10000.0)],
                vec![dt("2020-02-14 05:21:00"), s("pd-0"), s("tikv-2"), s("0"), s("leader_count"), Datum::Real(0.0)],
            ],
        ),
        (
            "pd_region_health".to_owned(),
            vec![
                vec![dt("2020-02-14 05:20:00"), s("pd-0"), s("extra-peer-region-count"), Datum::Real(40.0)],
                vec![dt("2020-02-14 05:20:00"), s("pd-0"), s("learner-peer-region-count"), Datum::Real(40.0)],
                vec![dt("2020-02-14 05:20:00"), s("pd-0"), s("pending-peer-region-count"), Datum::Real(30.0)],
            ],
        ),
    ])
}

const THRESHOLD_CHECK3: &[&str] = &[
    "leader-drop tikv tikv-2 tikv-2s 10000 <= 50 tikv-2 tikv has too many leader-drop around time 2020-02-14 05:21:00.000000, leader count from 10000 drop to 0",
    "leader-drop tikv tikv-0 tikv-0s 5000 <= 50 tikv-0 tikv has too many leader-drop around time 2020-02-14 05:21:00.000000, leader count from 10000 drop to 5000",
    "leader-score-balance tikv tikv-1 tikv-1s 50.00% < 5.00% tikv-0 max leader_score is 100.00, much more than tikv-1 min leader_score 50.00",
    "region-count tikv tikv-0 tikv-0s 20001.00 <= 20000 tikv-0 tikv has too many regions",
    "region-health pd pd-0 pd-0 110.00 < 100 the count of extra-perr and learner-peer and pending-peer are 110, it means the scheduling is too frequent or too slow",
    "region-score-balance tikv tikv-1 tikv-1s 10.00% < 5.00% tikv-0 max region_score is 100.00, much more than tikv-1 min region_score 90.00",
    "store-available-balance tikv tikv-1 tikv-1s 30.00% < 20.00% tikv-0 max store_available is 100.00, much more than tikv-1 min store_available 70.00",
];

fn critical_error_metrics() -> HashMap<String, Vec<Vec<Datum>>> {
    HashMap::from([
        (
            "tikv_critical_error_total_count".to_owned(),
            vec![
                vec![dt("2020-02-12 10:35:00"), s("tikv-0s"), s("type1"), Datum::Real(0.0)],
                vec![dt("2020-02-12 10:36:00"), s("tikv-1s"), s("type1"), Datum::Real(1.0)],
                vec![dt("2020-02-12 10:37:00"), s("tikv-2s"), s("type2"), Datum::Real(5.0)],
            ],
        ),
        (
            "tidb_panic_count_total_count".to_owned(),
            vec![
                vec![dt("2020-02-12 10:35:00"), s("tidb-0s"), Datum::Real(4.0)],
                vec![dt("2020-02-12 10:36:00"), s("tidb-0s"), Datum::Real(0.0)],
                vec![dt("2020-02-12 10:37:00"), s("tidb-1s"), Datum::Real(1.0)],
            ],
        ),
        (
            "tidb_binlog_error_total_count".to_owned(),
            vec![
                vec![dt("2020-02-12 10:35:00"), s("tidb-1s"), Datum::Real(4.0)],
                vec![dt("2020-02-12 10:36:00"), s("tidb-2s"), Datum::Real(0.0)],
                vec![dt("2020-02-12 10:37:00"), s("tidb-3s"), Datum::Real(1.0)],
            ],
        ),
        (
            "tikv_scheduler_is_busy_total_count".to_owned(),
            vec![
                vec![dt("2020-02-12 10:35:00"), s("tikv-0s"), s("db1"), s("type1"), s("stage1"), Datum::Real(1.0)],
                vec![dt("2020-02-12 10:36:00"), s("tikv-0s"), s("db2"), s("type1"), s("stage2"), Datum::Real(2.0)],
                vec![dt("2020-02-12 10:37:00"), s("tikv-1s"), s("db1"), s("type2"), s("stage1"), Datum::Real(3.0)],
                vec![dt("2020-02-12 10:38:00"), s("tikv-0s"), s("db1"), s("type1"), s("stage2"), Datum::Real(4.0)],
                vec![dt("2020-02-12 10:39:00"), s("tikv-0s"), s("db2"), s("type1"), s("stage1"), Datum::Real(5.0)],
                vec![dt("2020-02-12 10:40:00"), s("tikv-1s"), s("db1"), s("type2"), s("stage2"), Datum::Real(6.0)],
            ],
        ),
        (
            "tikv_coprocessor_is_busy_total_count".to_owned(),
            vec![
                vec![dt("2020-02-12 10:35:00"), s("tikv-0s"), s("db1"), Datum::Real(1.0)],
                vec![dt("2020-02-12 10:36:00"), s("tikv-0s"), s("db2"), Datum::Real(2.0)],
                vec![dt("2020-02-12 10:37:00"), s("tikv-1s"), s("db1"), Datum::Real(3.0)],
                vec![dt("2020-02-12 10:38:00"), s("tikv-0s"), s("db1"), Datum::Real(4.0)],
                vec![dt("2020-02-12 10:39:00"), s("tikv-0s"), s("db2"), Datum::Real(5.0)],
                vec![dt("2020-02-12 10:40:00"), s("tikv-1s"), s("db1"), Datum::Real(6.0)],
            ],
        ),
        (
            "tikv_channel_full_total_count".to_owned(),
            vec![
                vec![dt("2020-02-12 10:35:00"), s("tikv-0s"), s("db1"), s("type1"), Datum::Real(1.0)],
                vec![dt("2020-02-12 10:36:00"), s("tikv-0s"), s("db2"), s("type1"), Datum::Real(2.0)],
                vec![dt("2020-02-12 10:37:00"), s("tikv-1s"), s("db1"), s("type2"), Datum::Real(3.0)],
                vec![dt("2020-02-12 10:38:00"), s("tikv-0s"), s("db1"), s("type1"), Datum::Real(4.0)],
                vec![dt("2020-02-12 10:39:00"), s("tikv-0s"), s("db2"), s("type1"), Datum::Real(5.0)],
                vec![dt("2020-02-12 10:40:00"), s("tikv-1s"), s("db1"), s("type2"), Datum::Real(6.0)],
            ],
        ),
        (
            "tikv_engine_write_stall".to_owned(),
            vec![
                vec![dt("2020-02-12 10:35:00"), s("tikv-0s"), s("kv"), Datum::Real(1.0)],
                vec![dt("2020-02-12 10:36:00"), s("tikv-0s"), s("raft"), Datum::Real(2.0)],
                vec![dt("2020-02-12 10:37:00"), s("tikv-1s"), s("reason3"), Datum::Real(3.0)],
            ],
        ),
        (
            "up".to_owned(),
            vec![
                vec![dt("2020-02-12 10:35:00"), s("tikv-0s"), s("tikv"), Datum::Real(1.0)],
                vec![dt("2020-02-12 10:36:00"), s("tikv-0s"), s("tikv"), Datum::Real(0.0)],
                vec![dt("2020-02-12 10:37:00"), s("tidb-0s"), s("tidb"), Datum::Real(0.0)],
                vec![dt("2020-02-12 10:37:00"), s("tidb-1s"), s("tidb"), Datum::Real(0.0)],
                vec![dt("2020-02-12 10:38:00"), s("tidb-1s"), s("tidb"), Datum::Real(1.0)],
            ],
        ),
    ])
}

const CRITICAL_ERROR: &[&str] = &[
    "server-down tikv-0 tikv-0s  tikv tikv-0s disconnect with prometheus around time '2020-02-12 10:36:00.000000'",
    "server-down tidb-1 tidb-1s  tidb tidb-1s disconnect with prometheus around time '2020-02-12 10:37:00.000000'",
    "channel-is-full tikv-1 tikv-1s 9.00(db1, type2) the total number of errors about 'channel-is-full' is too many",
    "coprocessor-is-busy tikv-1 tikv-1s 9.00(db1) the total number of errors about 'coprocessor-is-busy' is too many",
    "channel-is-full tikv-0 tikv-0s 7.00(db2, type1) the total number of errors about 'channel-is-full' is too many",
    "coprocessor-is-busy tikv-0 tikv-0s 7.00(db2) the total number of errors about 'coprocessor-is-busy' is too many",
    "scheduler-is-busy tikv-1 tikv-1s 6.00(db1, type2, stage2) the total number of errors about 'scheduler-is-busy' is too many",
    "channel-is-full tikv-0 tikv-0s 5.00(db1, type1) the total number of errors about 'channel-is-full' is too many",
    "coprocessor-is-busy tikv-0 tikv-0s 5.00(db1) the total number of errors about 'coprocessor-is-busy' is too many",
    "critical-error tikv-2 tikv-2s 5.00(type2) the total number of errors about 'critical-error' is too many",
    "scheduler-is-busy tikv-0 tikv-0s 5.00(db2, type1, stage1) the total number of errors about 'scheduler-is-busy' is too many",
    "binlog-error tidb-1 tidb-1s 4.00 the total number of errors about 'binlog-error' is too many",
    "panic-count tidb-0 tidb-0s 4.00 the total number of errors about 'panic-count' is too many",
    "scheduler-is-busy tikv-0 tikv-0s 4.00(db1, type1, stage2) the total number of errors about 'scheduler-is-busy' is too many",
    "scheduler-is-busy tikv-1 tikv-1s 3.00(db1, type2, stage1) the total number of errors about 'scheduler-is-busy' is too many",
    "tikv_engine_write_stall tikv-1 tikv-1s 3.00(reason3) the total number of errors about 'tikv_engine_write_stall' is too many",
    "scheduler-is-busy tikv-0 tikv-0s 2.00(db2, type1, stage2) the total number of errors about 'scheduler-is-busy' is too many",
    "tikv_engine_write_stall tikv-0 tikv-0s 2.00(raft) the total number of errors about 'tikv_engine_write_stall' is too many",
    "binlog-error  tidb-3s 1.00 the total number of errors about 'binlog-error' is too many",
    "critical-error tikv-1 tikv-1s 1.00(type1) the total number of errors about 'critical-error' is too many",
    "panic-count tidb-1 tidb-1s 1.00 the total number of errors about 'panic-count' is too many",
    "scheduler-is-busy tikv-0 tikv-0s 1.00(db1, type1, stage1) the total number of errors about 'scheduler-is-busy' is too many",
    "tikv_engine_write_stall tikv-0 tikv-0s 1.00(kv) the total number of errors about 'tikv_engine_write_stall' is too many",
];

fn node_load_metrics() -> HashMap<String, Vec<Vec<Datum>>> {
    HashMap::from([
        (
            "node_load1".to_owned(),
            vec![
                vec![dt("2020-02-14 05:20:00"), s("node-0"), Datum::Real(28.1)],
                vec![dt("2020-02-14 05:20:00"), s("node-1"), Datum::Real(13.0)],
                vec![dt("2020-02-14 05:21:00"), s("node-0"), Datum::Real(10.0)],
            ],
        ),
        (
            "node_load5".to_owned(),
            vec![
                vec![dt("2020-02-14 05:20:00"), s("node-0"), Datum::Real(27.9)],
                vec![dt("2020-02-14 05:20:00"), s("node-1"), Datum::Real(14.1)],
                vec![dt("2020-02-14 05:21:00"), s("node-0"), Datum::Real(0.0)],
            ],
        ),
        (
            "node_load15".to_owned(),
            vec![
                vec![dt("2020-02-14 05:20:00"), s("node-0"), Datum::Real(30.0)],
                vec![dt("2020-02-14 05:20:00"), s("node-1"), Datum::Real(14.1)],
                vec![dt("2020-02-14 05:21:00"), s("node-0"), Datum::Real(20.0)],
            ],
        ),
        (
            "node_virtual_cpus".to_owned(),
            vec![
                vec![dt("2020-02-14 05:21:00"), s("node-0"), Datum::Real(40.0)],
                vec![dt("2020-02-14 05:21:00"), s("node-1"), Datum::Real(20.0)],
            ],
        ),
        (
            "node_memory_usage".to_owned(),
            vec![
                vec![dt("2020-02-14 05:21:00"), s("node-0"), Datum::Real(80.0)],
                vec![dt("2020-02-14 05:21:00"), s("node-1"), Datum::Real(60.0)],
                vec![dt("2020-02-14 05:22:00"), s("node-0"), Datum::Real(60.0)],
            ],
        ),
        (
            "node_memory_swap_used".to_owned(),
            vec![
                vec![dt("2020-02-14 05:21:00"), s("node-0"), Datum::Real(0.0)],
                vec![dt("2020-02-14 05:21:00"), s("node-1"), Datum::Real(1.0)],
                vec![dt("2020-02-14 05:22:00"), s("node-1"), Datum::Real(0.0)],
            ],
        ),
        (
            "node_disk_usage".to_owned(),
            vec![
                vec![dt("2020-02-14 05:21:00"), s("node-0"), s("/dev/nvme0"), Datum::Real(80.0)],
                vec![dt("2020-02-14 05:22:00"), s("node-0"), s("/dev/nvme0"), Datum::Real(50.0)],
                vec![dt("2020-02-14 05:21:00"), s("node-0"), s("tmpfs"), Datum::Real(80.0)],
                vec![dt("2020-02-14 05:22:00"), s("node-0"), s("tmpfs"), Datum::Real(50.0)],
            ],
        ),
    ])
}

const NODE_LOAD: &[&str] = &[
    "cpu-load1 node node-0 28.1 < 28.0 cpu-load1 should less than (cpu_logical_cores * 0.7)",
    "cpu-load15 node node-1 14.1 < 14.0 cpu-load15 should less than (cpu_logical_cores * 0.7)",
    "cpu-load15 node node-0 30.0 < 28.0 cpu-load15 should less than (cpu_logical_cores * 0.7)",
    "cpu-load5 node node-1 14.1 < 14.0 cpu-load5 should less than (cpu_logical_cores * 0.7)",
    "disk-usage node node-0 80.0% < 70% the disk-usage of /dev/nvme0 is too high",
    "swap-memory-used node node-1 1.0 0 ",
    "virtual-memory-usage node node-0 80.0% < 70% the memory-usage is too high",
];

fn block_cache_tables() -> HashMap<String, Vec<Vec<Datum>>> {
    HashMap::from([
        (
            "CLUSTER_CONFIG".to_owned(),
            vec![
                vec![s("tikv"), s("192.168.3.33:26600"), s("storage.block-cache.capacity"), s("10GiB")],
                vec![s("tikv"), s("192.168.3.33:26700"), s("storage.block-cache.capacity"), s("20GiB")],
                vec![s("tikv"), s("192.168.3.34:26600"), s("storage.block-cache.capacity"), s("1TiB")],
                vec![s("tikv"), s("192.168.3.35:26700"), s("storage.block-cache.capacity"), s("20GiB")],
            ],
        ),
    ])
}

fn block_cache_metrics() -> HashMap<String, Vec<Vec<Datum>>> {
    HashMap::from([
        (
            "node_total_memory".to_owned(),
            vec![
                vec![dt("2020-02-14 05:20:00"), s("192.168.3.33:26600"), Datum::Real(53687091200.0)],
                vec![dt("2020-02-14 05:20:00"), s("192.168.3.34:26600"), Datum::Real(53687091200.0)],
                vec![dt("2020-02-14 05:20:00"), s("192.168.3.35:26600"), Datum::Real(53687091200.0)],
            ],
        ),
    ])
}

const BLOCK_CACHE: &[&str] = &[
    "config storage.block-cache.capacity tikv 192.168.3.34  1099511627776 < 24159191040 warning There are 1 TiKV server in 192.168.3.34 node, the total 'storage.block-cache.capacity' of TiKV is more than (0.45 * total node memory)",
    "config storage.block-cache.capacity tikv 192.168.3.33  32212254720 < 24159191040 warning There are 2 TiKV server in 192.168.3.33 node, the total 'storage.block-cache.capacity' of TiKV is more than (0.45 * total node memory)",
];
