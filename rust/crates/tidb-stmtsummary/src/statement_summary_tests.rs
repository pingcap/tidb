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

use std::thread;

use tidb_exec::exec_details::{
    CommitDetails, CommitDetailsInner, CopExecDetails, ResolveLockDetail, ScanDetail, TimeDetail,
};

use super::*;
use crate::reader::tests::new_stmt_summary_reader_for_test;

/// Go `boTxnLockName`.
const BO_TXN_LOCK_NAME: &str = "txnlock";

/// Go `fakePlanDigestGenerator`.
fn fake_plan_digest_generator() -> String {
    "point_get".to_owned()
}

/// Go `mockLazyInfo`.
#[derive(Clone, Debug, Default)]
struct MockLazyInfo {
    original_sql: String,
    plan: String,
    hint_str: String,
    plan_error: bool,
    bin_plan: String,
    plan_digest: String,
    binding_sql: String,
    binding_digest: String,
}

impl StmtExecLazyInfo for MockLazyInfo {
    fn original_sql(&self) -> String {
        self.original_sql.clone()
    }

    fn encoded_plan(&self) -> Result<(String, String), EncodedPlanError> {
        if self.plan_error {
            return Err(EncodedPlanError("mock plan encoding error".to_owned()));
        }
        Ok((self.plan.clone(), self.hint_str.clone()))
    }

    fn binary_plan(&self) -> String {
        self.bin_plan.clone()
    }

    fn plan_digest(&self) -> String {
        self.plan_digest.clone()
    }

    fn binding_sql_and_digest(&self) -> (String, String) {
        (self.binding_sql.clone(), self.binding_digest.clone())
    }
}

fn mock_lazy_info(
    original_sql: &str,
    binding_sql: &str,
    binding_digest: &str,
) -> Arc<MockLazyInfo> {
    Arc::new(MockLazyInfo {
        original_sql: original_sql.to_owned(),
        binding_sql: binding_sql.to_owned(),
        binding_digest: binding_digest.to_owned(),
        ..MockLazyInfo::default()
    })
}

/// Go `time.Date(2019, 1, 1, h, m, s, 10, time.UTC)`.
fn start_time(hour: u32, minute: u32, second: u32) -> DateTime<Utc> {
    DateTime::from_timestamp(
        chrono::NaiveDate::from_ymd_opt(2019, 1, 1)
            .unwrap()
            .and_hms_opt(hour, minute, second)
            .unwrap()
            .and_utc()
            .timestamp(),
        10,
    )
    .unwrap()
}

/// Go `generateAnyExecInfo`.
pub(crate) fn generate_any_exec_info() -> StmtExecInfo {
    let sc = StmtSummaryStmtCtx {
        stmt_type: "Select".to_owned(),
        tables: vec![
            TableEntry {
                db: "db1".to_owned(),
                table: "tb1".to_owned(),
            },
            TableEntry {
                db: "db2".to_owned(),
                table: "tb2".to_owned(),
            },
        ],
        index_names: vec!["a".to_owned()],
        ..StmtSummaryStmtCtx::default()
    };
    sc.is_tikv.store(true, Ordering::SeqCst);
    sc.is_tiflash.store(true, Ordering::SeqCst);
    sc.add_affected_rows(10000);

    StmtExecInfo {
        schema_name: "schema_name".to_owned(),
        charset: String::new(),
        collation: String::new(),
        normalized_sql: "normalized_sql".to_owned(),
        digest: "digest".to_owned(),
        prev_sql: String::new(),
        prev_sql_digest: String::new(),
        plan_digest: "plan_digest".to_owned(),
        user: "user".to_owned(),
        total_latency: Duration::from_nanos(10000),
        parse_latency: Duration::from_nanos(100),
        compile_latency: Duration::from_nanos(1000),
        stmt_ctx: Arc::new(sc),
        cop_tasks: Some(CopTasksSummary {
            num_cop_tasks: 10,
            max_process_address: "127".to_owned(),
            max_process_time: Duration::from_nanos(15000),
            tot_process_time: Duration::from_nanos(10000),
            max_wait_address: "128".to_owned(),
            max_wait_time: Duration::from_nanos(1500),
            tot_wait_time: Duration::from_nanos(1000),
        }),
        exec_detail: ExecDetails {
            request_count: 10,
            commit_detail: Some(CommitDetails {
                get_commit_ts_time: Duration::from_nanos(100),
                prewrite_time: Duration::from_nanos(10000),
                commit_time: Duration::from_nanos(1000),
                local_latch_time: Duration::from_nanos(10),
                detail: CommitDetailsInner {
                    commit_backoff_time_ns: 200,
                    prewrite_backoff_types: vec![BO_TXN_LOCK_NAME.to_owned()],
                    ..CommitDetailsInner::default()
                },
                write_keys: 20000,
                write_size: 200_000,
                prewrite_region_num: 20,
                transaction_retry: 2,
                resolve_lock: ResolveLockDetail {
                    resolve_lock_time_ns: 2000,
                },
                ..CommitDetails::default()
            }),
            cop_exec_details: CopExecDetails {
                backoff_time: Duration::from_nanos(80),
                scan_detail: Some(ScanDetail {
                    total_keys: 1000,
                    processed_keys: 500,
                    rocksdb_delete_skipped_count: 100,
                    rocksdb_key_skipped_count: 10,
                    rocksdb_block_cache_hit_count: 10,
                    rocksdb_block_read_count: 10,
                    rocksdb_block_read_bytes: 1000,
                    ..ScanDetail::default()
                }),
                time_detail: TimeDetail {
                    process_time: Duration::from_nanos(500),
                    wait_time: Duration::from_nanos(50),
                    ..TimeDetail::default()
                },
                callee_address: "129".to_owned(),
                ..CopExecDetails::default()
            },
            ..ExecDetails::default()
        },
        mem_max: 10000,
        mem_arbitration: 10000.0,
        disk_max: 10000,
        start_time: start_time(10, 10, 10),
        is_internal: false,
        succeed: true,
        plan_in_cache: false,
        plan_in_binding: false,
        exec_retry_count: 0,
        exec_retry_time: Duration::ZERO,
        write_sql_resp_duration: Duration::ZERO,
        result_rows: 0,
        tikv_exec_details: Some(ExecDetailsSnapshot {
            traffic: TrafficDetailsSnapshot {
                sent_kv_total: 10,
                received_kv_total: 1000,
                received_kv_cross_zone: 1,
                sent_kv_cross_zone: 100,
                ..TrafficDetailsSnapshot::default()
            },
            ..ExecDetailsSnapshot::default()
        }),
        prepared: false,
        keyspace_name: String::new(),
        keyspace_id: 0,
        resource_group_name: "rg1".to_owned(),
        ru_detail: Some(Arc::new(RuDetails::new_with(
            1.1,
            2.5,
            Duration::from_millis(2),
        ))),
        total_ru_v2: 23456.0,
        cpu_usages: CpuUsages {
            tidb_cpu_time: 20,
            tikv_cpu_time: 100,
        },
        plan_cache_unqualified: String::new(),
        lazy_info: mock_lazy_info("original_sql1", "binding_sql1", "binding_digest1"),
    }
}

/// Go `matchStmtSummaryByDigest`.
fn match_stmt_summary_by_digest(first: &StmtSummaryByDigest, second: &StmtSummaryByDigest) -> bool {
    if first.schema_name != second.schema_name
        || first.digest != second.digest
        || first.normalized_sql != second.normalized_sql
        || first.plan_digest != second.plan_digest
        || first.table_names != second.table_names
        || !first.stmt_type.eq_ignore_ascii_case(&second.stmt_type)
    {
        return false;
    }
    if first.history.len() != second.history.len() {
        return false;
    }
    for (ele1, ele2) in first.history.iter().zip(second.history.iter()) {
        let e1 = ele1.lock().unwrap();
        let e2 = ele2.lock().unwrap();
        let (s1, s2) = (&e1.stats, &e2.stats);
        if e1.begin_time != e2.begin_time
            || e1.end_time != e2.end_time
            || s1.sample_sql != s2.sample_sql
            || s1.sample_plan != s2.sample_plan
            || s1.prev_sql != s2.prev_sql
            || s1.exec_count != s2.exec_count
            || s1.sum_errors != s2.sum_errors
            || s1.sum_warnings != s2.sum_warnings
            || s1.sum_latency != s2.sum_latency
            || s1.max_latency != s2.max_latency
            || s1.min_latency != s2.min_latency
            || s1.sum_parse_latency != s2.sum_parse_latency
            || s1.max_parse_latency != s2.max_parse_latency
            || s1.sum_compile_latency != s2.sum_compile_latency
            || s1.max_compile_latency != s2.max_compile_latency
            || s1.sum_num_cop_tasks != s2.sum_num_cop_tasks
            || s1.sum_cop_process_time != s2.sum_cop_process_time
            || s1.max_cop_process_time != s2.max_cop_process_time
            || s1.max_cop_process_address != s2.max_cop_process_address
            || s1.sum_cop_wait_time != s2.sum_cop_wait_time
            || s1.max_cop_wait_time != s2.max_cop_wait_time
            || s1.max_cop_wait_address != s2.max_cop_wait_address
            || s1.sum_process_time != s2.sum_process_time
            || s1.max_process_time != s2.max_process_time
            || s1.sum_wait_time != s2.sum_wait_time
            || s1.max_wait_time != s2.max_wait_time
            || s1.sum_backoff_time != s2.sum_backoff_time
            || s1.max_backoff_time != s2.max_backoff_time
            || s1.sum_total_keys != s2.sum_total_keys
            || s1.max_total_keys != s2.max_total_keys
            || s1.sum_processed_keys != s2.sum_processed_keys
            || s1.max_processed_keys != s2.max_processed_keys
            || s1.sum_get_commit_ts_time != s2.sum_get_commit_ts_time
            || s1.max_get_commit_ts_time != s2.max_get_commit_ts_time
            || s1.sum_prewrite_time != s2.sum_prewrite_time
            || s1.max_prewrite_time != s2.max_prewrite_time
            || s1.sum_commit_time != s2.sum_commit_time
            || s1.max_commit_time != s2.max_commit_time
            || s1.sum_local_latch_time != s2.sum_local_latch_time
            || s1.max_local_latch_time != s2.max_local_latch_time
            || s1.sum_commit_backoff_time != s2.sum_commit_backoff_time
            || s1.max_commit_backoff_time != s2.max_commit_backoff_time
            || s1.sum_resolve_lock_time != s2.sum_resolve_lock_time
            || s1.max_resolve_lock_time != s2.max_resolve_lock_time
            || s1.sum_write_keys != s2.sum_write_keys
            || s1.max_write_keys != s2.max_write_keys
            || s1.sum_write_size != s2.sum_write_size
            || s1.max_write_size != s2.max_write_size
            || s1.sum_prewrite_region_num != s2.sum_prewrite_region_num
            || s1.max_prewrite_region_num != s2.max_prewrite_region_num
            || s1.sum_txn_retry != s2.sum_txn_retry
            || s1.max_txn_retry != s2.max_txn_retry
            || s1.sum_backoff_times != s2.sum_backoff_times
            || s1.sum_mem != s2.sum_mem
            || s1.max_mem != s2.max_mem
            || (s1.sum_mem_arbitration - s2.sum_mem_arbitration).abs() > f64::EPSILON
            || (s1.max_mem_arbitration - s2.max_mem_arbitration).abs() > f64::EPSILON
            || s1.sum_affected_rows != s2.sum_affected_rows
            || s1.first_seen != s2.first_seen
            || s1.last_seen != s2.last_seen
            || s1.resource_group_name != s2.resource_group_name
            || s1.ru != s2.ru
            || s1.network != s2.network
            || s1.storage_kv != s2.storage_kv
            || s1.storage_mpp != s2.storage_mpp
        {
            return false;
        }
        if s1.backoff_types != s2.backoff_types {
            return false;
        }
        if s1.index_names != s2.index_names {
            return false;
        }
    }
    true
}

/// Go `TestSetUp`.
#[test]
fn test_set_up() {
    let ss_map = StmtSummaryByDigestMap::new();
    ss_map.set_enabled(true);
    ss_map.set_refresh_interval(1800);
    ss_map.set_history_size(24);
    assert!(ss_map.enabled());
    assert_eq!(ss_map.refresh_interval(), 1800);
    assert_eq!(ss_map.history_size(), 24);
}

/// Go `TestAddStatement`.
#[test]
#[allow(clippy::too_many_lines)]
fn test_add_statement() {
    let ss_map = StmtSummaryByDigestMap::new();
    let now = unix_now();
    ss_map.set_begin_time_for_cur_interval(now + 60);

    let sc = Arc::new(StmtSummaryStmtCtx {
        stmt_type: "Select".to_owned(),
        tables: vec![
            TableEntry {
                db: "db1".to_owned(),
                table: "tb1".to_owned(),
            },
            TableEntry {
                db: "db2".to_owned(),
                table: "tb2".to_owned(),
            },
        ],
        index_names: vec!["a".to_owned(), "b".to_owned()],
        ..StmtSummaryStmtCtx::default()
    });

    // first statement
    let mut info1 = generate_any_exec_info();
    info1
        .exec_detail
        .commit_detail
        .as_mut()
        .unwrap()
        .detail
        .prewrite_backoff_types = Vec::new();
    let mut key = StmtDigestKey::new();
    key.init(
        &info1.schema_name,
        &info1.digest,
        "",
        &info1.plan_digest,
        &info1.resource_group_name,
        "",
    );
    let (sample_plan, _) = info1.lazy_info.encoded_plan().unwrap();
    let commit1 = info1.exec_detail.commit_detail.clone().unwrap();
    let cop1 = info1.cop_tasks.clone().unwrap();
    let scan1 = info1
        .exec_detail
        .cop_exec_details
        .scan_detail
        .clone()
        .unwrap();
    let time1 = info1.exec_detail.cop_exec_details.time_detail.clone();
    let tikv1 = info1.tikv_exec_details.unwrap();
    let ru1 = info1.ru_detail.as_ref().unwrap();

    let mut expected_element = StmtSummaryByDigestElement {
        begin_time: now + 60,
        end_time: now + 1860,
        stats: StmtSummaryStats {
            sample_sql: info1.lazy_info.original_sql(),
            sample_plan,
            index_names: info1.stmt_ctx.index_names.clone(),
            exec_count: 1,
            sum_latency: info1.total_latency,
            max_latency: info1.total_latency,
            min_latency: info1.total_latency,
            sum_parse_latency: info1.parse_latency,
            max_parse_latency: info1.parse_latency,
            sum_compile_latency: info1.compile_latency,
            max_compile_latency: info1.compile_latency,
            sum_num_cop_tasks: cop1.num_cop_tasks,
            sum_cop_process_time: cop1.tot_process_time,
            max_cop_process_time: cop1.max_process_time,
            max_cop_process_address: cop1.max_process_address.clone(),
            sum_cop_wait_time: cop1.tot_wait_time,
            max_cop_wait_time: cop1.max_wait_time,
            max_cop_wait_address: cop1.max_wait_address.clone(),
            sum_process_time: time1.process_time,
            max_process_time: time1.process_time,
            sum_wait_time: time1.wait_time,
            max_wait_time: time1.wait_time,
            sum_backoff_time: info1.exec_detail.cop_exec_details.backoff_time,
            max_backoff_time: info1.exec_detail.cop_exec_details.backoff_time,
            sum_total_keys: scan1.total_keys,
            max_total_keys: scan1.total_keys,
            sum_processed_keys: scan1.processed_keys,
            max_processed_keys: scan1.processed_keys,
            sum_get_commit_ts_time: commit1.get_commit_ts_time,
            max_get_commit_ts_time: commit1.get_commit_ts_time,
            sum_prewrite_time: commit1.prewrite_time,
            max_prewrite_time: commit1.prewrite_time,
            sum_commit_time: commit1.commit_time,
            max_commit_time: commit1.commit_time,
            sum_local_latch_time: commit1.local_latch_time,
            max_local_latch_time: commit1.local_latch_time,
            sum_commit_backoff_time: commit1.detail.commit_backoff_time_ns,
            max_commit_backoff_time: commit1.detail.commit_backoff_time_ns,
            sum_resolve_lock_time: commit1.resolve_lock.resolve_lock_time_ns,
            max_resolve_lock_time: commit1.resolve_lock.resolve_lock_time_ns,
            sum_write_keys: commit1.write_keys as i64,
            max_write_keys: commit1.write_keys as i64,
            sum_write_size: commit1.write_size as i64,
            max_write_size: commit1.write_size as i64,
            sum_prewrite_region_num: i64::from(commit1.prewrite_region_num),
            max_prewrite_region_num: commit1.prewrite_region_num,
            sum_txn_retry: commit1.transaction_retry as i64,
            max_txn_retry: commit1.transaction_retry as i64,
            backoff_types: HashMap::new(),
            sum_mem: info1.mem_max,
            max_mem: info1.mem_max,
            sum_disk: info1.disk_max,
            max_disk: info1.disk_max,
            sum_affected_rows: info1.stmt_ctx.affected_rows(),
            first_seen: info1.start_time,
            last_seen: info1.start_time,
            ru: StmtRuSummary {
                sum_rru: ru1.read_ru(),
                max_rru: ru1.read_ru(),
                sum_wru: ru1.write_ru(),
                max_wru: ru1.write_ru(),
                sum_ru_wait_duration: ru1.ru_wait_duration(),
                max_ru_wait_duration: ru1.ru_wait_duration(),
                sum_ru_v2: info1.total_ru_v2,
                max_ru_v2: info1.total_ru_v2,
            },
            resource_group_name: info1.resource_group_name.clone(),
            network: StmtNetworkTrafficSummary {
                unpacked_bytes_sent_tikv_total: tikv1.traffic.sent_kv_total,
                unpacked_bytes_received_tikv_total: tikv1.traffic.received_kv_total,
                unpacked_bytes_sent_tikv_cross_zone: tikv1.traffic.sent_kv_cross_zone,
                unpacked_bytes_received_tikv_cross_zone: tikv1.traffic.received_kv_cross_zone,
                unpacked_bytes_sent_tiflash_total: tikv1.traffic.sent_mpp_total,
                unpacked_bytes_received_tiflash_total: tikv1.traffic.received_mpp_total,
                unpacked_bytes_sent_tiflash_cross_zone: tikv1.traffic.sent_mpp_cross_zone,
                unpacked_bytes_received_tiflash_cross_zone: tikv1.traffic.received_mpp_cross_zone,
            },
            storage_kv: info1.stmt_ctx.is_tikv.load(Ordering::SeqCst),
            storage_mpp: info1.stmt_ctx.is_tiflash.load(Ordering::SeqCst),
            sum_mem_arbitration: info1.mem_arbitration,
            max_mem_arbitration: info1.mem_arbitration,
            ..StmtSummaryStats::default()
        },
    };

    let build_expected = |element: &StmtSummaryByDigestElement, info: &StmtExecInfo| {
        let mut history = VecDeque::new();
        history.push_back(Arc::new(Mutex::new(element.clone())));
        StmtSummaryByDigest {
            schema_name: info.schema_name.clone(),
            stmt_type: info.stmt_ctx.stmt_type.clone(),
            digest: info.digest.clone(),
            normalized_sql: info.normalized_sql.clone(),
            plan_digest: info.plan_digest.clone(),
            table_names: "db1.tb1,db2.tb2".to_owned(),
            history,
            ..StmtSummaryByDigest::default()
        }
    };

    let expected_summary = build_expected(&expected_element, &info1);
    ss_map.add_statement(&info1);
    let summary = ss_map.summary_map_get(&key).expect("summary must exist");
    assert!(match_stmt_summary_by_digest(
        &summary.lock().unwrap(),
        &expected_summary
    ));

    // Second statement is similar with the first statement, and its values
    // are greater than that of the first statement.
    let info2 = StmtExecInfo {
        schema_name: "schema_name".to_owned(),
        normalized_sql: "normalized_sql".to_owned(),
        digest: "digest".to_owned(),
        plan_digest: "plan_digest".to_owned(),
        user: "user2".to_owned(),
        total_latency: Duration::from_nanos(20000),
        parse_latency: Duration::from_nanos(200),
        compile_latency: Duration::from_nanos(2000),
        cop_tasks: Some(CopTasksSummary {
            num_cop_tasks: 20,
            max_process_address: "200".to_owned(),
            max_process_time: Duration::from_nanos(25000),
            tot_process_time: Duration::from_nanos(40000),
            max_wait_address: "201".to_owned(),
            max_wait_time: Duration::from_nanos(2500),
            tot_wait_time: Duration::from_nanos(40000),
        }),
        exec_detail: ExecDetails {
            request_count: 20,
            commit_detail: Some(CommitDetails {
                get_commit_ts_time: Duration::from_nanos(500),
                prewrite_time: Duration::from_nanos(50000),
                commit_time: Duration::from_nanos(5000),
                local_latch_time: Duration::from_nanos(50),
                detail: CommitDetailsInner {
                    commit_backoff_time_ns: 1000,
                    prewrite_backoff_types: vec![BO_TXN_LOCK_NAME.to_owned()],
                    ..CommitDetailsInner::default()
                },
                write_keys: 100_000,
                write_size: 1_000_000,
                prewrite_region_num: 100,
                transaction_retry: 10,
                resolve_lock: ResolveLockDetail {
                    resolve_lock_time_ns: 10000,
                },
                ..CommitDetails::default()
            }),
            cop_exec_details: CopExecDetails {
                backoff_time: Duration::from_nanos(180),
                scan_detail: Some(ScanDetail {
                    total_keys: 6000,
                    processed_keys: 1500,
                    rocksdb_delete_skipped_count: 100,
                    rocksdb_key_skipped_count: 10,
                    rocksdb_block_cache_hit_count: 10,
                    rocksdb_block_read_count: 10,
                    rocksdb_block_read_bytes: 1000,
                    ..ScanDetail::default()
                }),
                time_detail: TimeDetail {
                    process_time: Duration::from_nanos(1500),
                    wait_time: Duration::from_nanos(150),
                    ..TimeDetail::default()
                },
                callee_address: "202".to_owned(),
                ..CopExecDetails::default()
            },
            ..ExecDetails::default()
        },
        stmt_ctx: Arc::clone(&sc),
        mem_max: 20000,
        disk_max: 20000,
        start_time: start_time(10, 10, 20),
        succeed: true,
        ru_detail: Some(Arc::new(RuDetails::new_with(
            123.0,
            45.6,
            Duration::from_secs(2),
        ))),
        total_ru_v2: 34567.0,
        tikv_exec_details: Some(ExecDetailsSnapshot {
            traffic: TrafficDetailsSnapshot {
                sent_kv_total: 100,
                received_kv_total: 200,
                ..TrafficDetailsSnapshot::default()
            },
            ..ExecDetailsSnapshot::default()
        }),
        resource_group_name: "rg1".to_owned(),
        lazy_info: mock_lazy_info("original_sql2", "binding_sql2", "binding_digest2"),
        mem_arbitration: 30000.0,
        charset: String::new(),
        collation: String::new(),
        prev_sql: String::new(),
        prev_sql_digest: String::new(),
        is_internal: false,
        plan_in_cache: false,
        plan_in_binding: false,
        exec_retry_count: 0,
        exec_retry_time: Duration::ZERO,
        write_sql_resp_duration: Duration::ZERO,
        result_rows: 0,
        prepared: false,
        keyspace_name: String::new(),
        keyspace_id: 0,
        cpu_usages: CpuUsages::default(),
        plan_cache_unqualified: String::new(),
    };
    info2.stmt_ctx.add_affected_rows(200);
    let commit2 = info2.exec_detail.commit_detail.clone().unwrap();
    let cop2 = info2.cop_tasks.clone().unwrap();
    let scan2 = info2
        .exec_detail
        .cop_exec_details
        .scan_detail
        .clone()
        .unwrap();
    let time2 = info2.exec_detail.cop_exec_details.time_detail.clone();
    let ru2 = info2.ru_detail.as_ref().unwrap();
    {
        let s = &mut expected_element.stats;
        s.exec_count += 1;
        s.sum_latency += info2.total_latency;
        s.max_latency = info2.total_latency;
        s.sum_parse_latency += info2.parse_latency;
        s.max_parse_latency = info2.parse_latency;
        s.sum_compile_latency += info2.compile_latency;
        s.max_compile_latency = info2.compile_latency;
        s.sum_num_cop_tasks += cop2.num_cop_tasks;
        s.sum_cop_process_time += cop2.tot_process_time;
        s.max_cop_process_time = cop2.max_process_time;
        s.max_cop_process_address
            .clone_from(&cop2.max_process_address);
        s.sum_cop_wait_time += cop2.tot_wait_time;
        s.max_cop_wait_time = cop2.max_wait_time;
        s.max_cop_wait_address.clone_from(&cop2.max_wait_address);
        s.sum_process_time += time2.process_time;
        s.max_process_time = time2.process_time;
        s.sum_wait_time += time2.wait_time;
        s.max_wait_time = time2.wait_time;
        s.sum_backoff_time += info2.exec_detail.cop_exec_details.backoff_time;
        s.max_backoff_time = info2.exec_detail.cop_exec_details.backoff_time;
        s.sum_total_keys += scan2.total_keys;
        s.max_total_keys = scan2.total_keys;
        s.sum_processed_keys += scan2.processed_keys;
        s.max_processed_keys = scan2.processed_keys;
        s.sum_get_commit_ts_time += commit2.get_commit_ts_time;
        s.max_get_commit_ts_time = commit2.get_commit_ts_time;
        s.sum_prewrite_time += commit2.prewrite_time;
        s.max_prewrite_time = commit2.prewrite_time;
        s.sum_commit_time += commit2.commit_time;
        s.max_commit_time = commit2.commit_time;
        s.sum_local_latch_time += commit2.local_latch_time;
        s.max_local_latch_time = commit2.local_latch_time;
        s.sum_commit_backoff_time += commit2.detail.commit_backoff_time_ns;
        s.max_commit_backoff_time = commit2.detail.commit_backoff_time_ns;
        s.sum_resolve_lock_time += commit2.resolve_lock.resolve_lock_time_ns;
        s.max_resolve_lock_time = commit2.resolve_lock.resolve_lock_time_ns;
        s.sum_write_keys += commit2.write_keys as i64;
        s.max_write_keys = commit2.write_keys as i64;
        s.sum_write_size += commit2.write_size as i64;
        s.max_write_size = commit2.write_size as i64;
        s.sum_prewrite_region_num += i64::from(commit2.prewrite_region_num);
        s.max_prewrite_region_num = commit2.prewrite_region_num;
        s.sum_txn_retry += commit2.transaction_retry as i64;
        s.max_txn_retry = commit2.transaction_retry as i64;
        s.sum_backoff_times += 1;
        s.backoff_types.insert(BO_TXN_LOCK_NAME.to_owned(), 1);
        s.sum_mem += info2.mem_max;
        s.max_mem = info2.mem_max;
        s.max_mem_arbitration = info2.mem_arbitration;
        s.sum_mem_arbitration += info2.mem_arbitration;
        s.sum_disk += info2.disk_max;
        s.max_disk = info2.disk_max;
        s.sum_affected_rows += info2.stmt_ctx.affected_rows();
        s.last_seen = info2.start_time;
        s.ru.sum_rru += ru2.read_ru();
        s.ru.max_rru = ru2.read_ru();
        s.ru.sum_wru += ru2.write_ru();
        s.ru.max_wru = ru2.write_ru();
        s.ru.sum_ru_wait_duration += ru2.ru_wait_duration();
        s.ru.max_ru_wait_duration = ru2.ru_wait_duration();
        s.ru.sum_ru_v2 += info2.total_ru_v2;
        s.ru.max_ru_v2 = info2.total_ru_v2;
        s.network.add(info2.tikv_exec_details.as_ref());
        s.storage_kv = info2.stmt_ctx.is_tikv.load(Ordering::SeqCst);
        s.storage_mpp = info2.stmt_ctx.is_tiflash.load(Ordering::SeqCst);
    }

    let expected_summary = build_expected(&expected_element, &info1);
    ss_map.add_statement(&info2);
    let summary = ss_map.summary_map_get(&key).expect("summary must exist");
    assert!(match_stmt_summary_by_digest(
        &summary.lock().unwrap(),
        &expected_summary
    ));

    // Third statement is similar with the first statement, and its values
    // are less than that of the first statement.
    let info3 = StmtExecInfo {
        user: String::from("user3"),
        total_latency: Duration::from_nanos(1000),
        parse_latency: Duration::from_nanos(50),
        compile_latency: Duration::from_nanos(500),
        cop_tasks: Some(CopTasksSummary {
            num_cop_tasks: 2,
            max_process_address: "300".to_owned(),
            max_process_time: Duration::from_nanos(350),
            tot_process_time: Duration::from_nanos(200),
            max_wait_address: "301".to_owned(),
            max_wait_time: Duration::from_nanos(250),
            tot_wait_time: Duration::from_nanos(40),
        }),
        exec_detail: ExecDetails {
            request_count: 2,
            commit_detail: Some(CommitDetails {
                get_commit_ts_time: Duration::from_nanos(50),
                prewrite_time: Duration::from_nanos(5000),
                commit_time: Duration::from_nanos(500),
                local_latch_time: Duration::from_nanos(5),
                detail: CommitDetailsInner {
                    commit_backoff_time_ns: 100,
                    prewrite_backoff_types: vec![BO_TXN_LOCK_NAME.to_owned()],
                    ..CommitDetailsInner::default()
                },
                write_keys: 10000,
                write_size: 100_000,
                prewrite_region_num: 10,
                transaction_retry: 1,
                resolve_lock: ResolveLockDetail {
                    resolve_lock_time_ns: 1000,
                },
                ..CommitDetails::default()
            }),
            cop_exec_details: CopExecDetails {
                backoff_time: Duration::from_nanos(18),
                scan_detail: Some(ScanDetail {
                    total_keys: 600,
                    processed_keys: 150,
                    rocksdb_delete_skipped_count: 100,
                    rocksdb_key_skipped_count: 10,
                    rocksdb_block_cache_hit_count: 10,
                    rocksdb_block_read_count: 10,
                    rocksdb_block_read_bytes: 1000,
                    ..ScanDetail::default()
                }),
                time_detail: TimeDetail {
                    process_time: Duration::from_nanos(150),
                    wait_time: Duration::from_nanos(15),
                    ..TimeDetail::default()
                },
                callee_address: "302".to_owned(),
                ..CopExecDetails::default()
            },
            ..ExecDetails::default()
        },
        stmt_ctx: Arc::clone(&sc),
        mem_max: 200,
        disk_max: 200,
        start_time: start_time(10, 10, 0),
        ru_detail: Some(Arc::new(RuDetails::new_with(
            0.12,
            0.34,
            Duration::from_micros(5),
        ))),
        total_ru_v2: 123.0,
        tikv_exec_details: Some(ExecDetailsSnapshot {
            traffic: TrafficDetailsSnapshot {
                sent_kv_total: 1,
                received_kv_total: 300,
                sent_mpp_total: 1,
                received_mpp_total: 300,
                ..TrafficDetailsSnapshot::default()
            },
            ..ExecDetailsSnapshot::default()
        }),
        lazy_info: mock_lazy_info("original_sql3", "binding_sql3", "binding_digest3"),
        mem_arbitration: 200.0,
        ..info2
    };
    info3.stmt_ctx.add_affected_rows(20000);
    let commit3 = info3.exec_detail.commit_detail.clone().unwrap();
    let cop3 = info3.cop_tasks.clone().unwrap();
    let scan3 = info3
        .exec_detail
        .cop_exec_details
        .scan_detail
        .clone()
        .unwrap();
    let time3 = info3.exec_detail.cop_exec_details.time_detail.clone();
    let ru3 = info3.ru_detail.as_ref().unwrap();
    {
        let s = &mut expected_element.stats;
        s.exec_count += 1;
        s.sum_latency += info3.total_latency;
        s.min_latency = info3.total_latency;
        s.sum_parse_latency += info3.parse_latency;
        s.sum_compile_latency += info3.compile_latency;
        s.sum_num_cop_tasks += cop3.num_cop_tasks;
        s.sum_cop_process_time += cop3.tot_process_time;
        s.sum_cop_wait_time += cop3.tot_wait_time;
        s.sum_process_time += time3.process_time;
        s.sum_wait_time += time3.wait_time;
        s.sum_backoff_time += info3.exec_detail.cop_exec_details.backoff_time;
        s.sum_total_keys += scan3.total_keys;
        s.sum_processed_keys += scan3.processed_keys;
        s.sum_get_commit_ts_time += commit3.get_commit_ts_time;
        s.sum_prewrite_time += commit3.prewrite_time;
        s.sum_commit_time += commit3.commit_time;
        s.sum_local_latch_time += commit3.local_latch_time;
        s.sum_commit_backoff_time += commit3.detail.commit_backoff_time_ns;
        s.sum_resolve_lock_time += commit3.resolve_lock.resolve_lock_time_ns;
        s.sum_write_keys += commit3.write_keys as i64;
        s.sum_write_size += commit3.write_size as i64;
        s.sum_prewrite_region_num += i64::from(commit3.prewrite_region_num);
        s.sum_txn_retry += commit3.transaction_retry as i64;
        s.sum_backoff_times += 1;
        s.backoff_types.insert(BO_TXN_LOCK_NAME.to_owned(), 2);
        s.sum_mem += info3.mem_max;
        s.sum_mem_arbitration += info3.mem_arbitration;
        s.sum_disk += info3.disk_max;
        s.sum_affected_rows += info3.stmt_ctx.affected_rows();
        s.first_seen = info3.start_time;
        s.ru.sum_rru += ru3.read_ru();
        s.ru.sum_wru += ru3.write_ru();
        s.ru.sum_ru_wait_duration += ru3.ru_wait_duration();
        s.ru.sum_ru_v2 += info3.total_ru_v2;
        s.network.add(info3.tikv_exec_details.as_ref());
        s.storage_kv = info3.stmt_ctx.is_tikv.load(Ordering::SeqCst);
        s.storage_mpp = info3.stmt_ctx.is_tiflash.load(Ordering::SeqCst);
    }

    let expected_summary = build_expected(&expected_element, &info1);
    ss_map.add_statement(&info3);
    let summary = ss_map.summary_map_get(&key).expect("summary must exist");
    assert!(match_stmt_summary_by_digest(
        &summary.lock().unwrap(),
        &expected_summary
    ));

    // Fourth statement is in a different schema. Go aliases the pointer, so
    // the mutations land on `info1` itself.
    info1.schema_name = "schema2".to_owned();
    info1.exec_detail.commit_detail = None;
    let mut key = StmtDigestKey::new();
    key.init(
        &info1.schema_name,
        &info1.digest,
        "",
        &info1.plan_digest,
        &info1.resource_group_name,
        "",
    );
    ss_map.add_statement(&info1);
    assert_eq!(ss_map.summary_map_size(), 2);
    assert!(ss_map.summary_map_get(&key).is_some());

    // Fifth statement has a different digest.
    info1.digest = "digest2".to_owned();
    key.init(
        &info1.schema_name,
        &info1.digest,
        "",
        &info1.plan_digest,
        &info1.resource_group_name,
        "",
    );
    ss_map.add_statement(&info1);
    assert_eq!(ss_map.summary_map_size(), 3);
    assert!(ss_map.summary_map_get(&key).is_some());

    // Sixth statement has a different plan digest.
    info1.plan_digest = "plan_digest2".to_owned();
    key.init(
        &info1.schema_name,
        &info1.digest,
        "",
        &info1.plan_digest,
        &info1.resource_group_name,
        "",
    );
    ss_map.add_statement(&info1);
    assert_eq!(ss_map.summary_map_size(), 4);
    assert!(ss_map.summary_map_get(&key).is_some());

    // Test for plan too large
    info1.plan_digest = "plan_digest7".to_owned();
    let buf = "a".repeat(MAX_ENCODED_PLAN_SIZE_IN_BYTES.load(Ordering::SeqCst) + 1);
    let original_sql = info1.lazy_info.original_sql();
    info1.lazy_info = Arc::new(MockLazyInfo {
        original_sql: original_sql.clone(),
        plan: buf,
        binding_sql: original_sql,
        ..MockLazyInfo::default()
    });
    key.init(
        &info1.schema_name,
        &info1.digest,
        "",
        &info1.plan_digest,
        &info1.resource_group_name,
        "",
    );
    ss_map.add_statement(&info1);
    assert_eq!(ss_map.summary_map_size(), 5);
    let value = ss_map.summary_map_get(&key).expect("summary must exist");
    let stmt = value.lock().unwrap();
    let hash = key.hash();
    assert!(hash
        .windows(stmt.schema_name.len())
        .any(|w| w == stmt.schema_name.as_bytes()));
    assert!(hash
        .windows(stmt.digest.len())
        .any(|w| w == stmt.digest.as_bytes()));
    assert!(hash
        .windows(stmt.plan_digest.len())
        .any(|w| w == stmt.plan_digest.as_bytes()));
    let element = stmt.history.back().unwrap().lock().unwrap();
    assert_eq!(element.stats.sample_plan, PLAN_DISCARDED_ENCODED);
}

/// Go `TestAddStatementParallel`.
///
/// Go shares one reader across the goroutines; scoped threads let the
/// borrowing reader be shared the same way.
#[test]
fn test_add_statement_parallel() {
    let ss_map = StmtSummaryByDigestMap::new();
    let now = unix_now();
    // to disable expiration
    ss_map.set_begin_time_for_cur_interval(now + 60);

    let threads = 8;
    let loops = 32;
    let reader = new_stmt_summary_reader_for_test(&ss_map);
    thread::scope(|scope| {
        for _ in 0..threads {
            scope.spawn(|| {
                let mut info = generate_any_exec_info();

                // Add 32 times with different digest.
                for i in 0..loops {
                    info.digest = format!("digest{i}");
                    ss_map.add_statement(&info);
                }

                // There would be 32 summaries.
                let datums = reader.get_stmt_summary_current_rows();
                assert_eq!(datums.len(), loops);
            });
        }
    });

    let datums = reader.get_stmt_summary_current_rows();
    assert_eq!(datums.len(), loops);
}

/// Go `TestAddStatementPlanEncodeError`: a lazy plan encoding failure
/// still records the statement with the discarded-plan marker.
#[test]
fn test_add_statement_plan_encode_error_uses_discarded_marker() {
    let ss_map = StmtSummaryByDigestMap::new();
    ss_map.set_begin_time_for_cur_interval(unix_now() + 60);

    let mut info = generate_any_exec_info();
    info.lazy_info = Arc::new(MockLazyInfo {
        original_sql: "select 1".to_owned(),
        plan_error: true,
        ..MockLazyInfo::default()
    });
    ss_map.add_statement(&info);

    let mut key = StmtDigestKey::new();
    key.init(
        &info.schema_name,
        &info.digest,
        &info.prev_sql_digest,
        &info.plan_digest,
        &info.resource_group_name,
        "",
    );
    let summary = ss_map.summary_map_get(&key).expect("summary must exist");
    let summary = summary.lock().unwrap();
    let element = summary
        .history
        .back()
        .expect("history must exist")
        .lock()
        .unwrap();
    assert_eq!(element.stats.sample_plan, PLAN_DISCARDED_ENCODED);
    assert_eq!(element.stats.plan_hint, "");
    assert_eq!(element.stats.exec_count, 1);
}

/// Go `AddStatement` initializes `isInternal` from the first statement and
/// then narrows it with AND; `ClearInternal` removes only pure-internal
/// summaries.
#[test]
fn test_internal_summary_initialization_and_clear() {
    let ss_map = StmtSummaryByDigestMap::new();
    ss_map.set_enabled_internal_query(true);
    ss_map.set_begin_time_for_cur_interval(unix_now() + 60);

    let mut internal_only = generate_any_exec_info();
    internal_only.digest = "internal_only".to_owned();
    internal_only.is_internal = true;
    ss_map.add_statement(&internal_only);

    let mut normal_old = generate_any_exec_info();
    normal_old.digest = "normal_old".to_owned();
    ss_map.add_statement(&normal_old);

    let mut normal_new = generate_any_exec_info();
    normal_new.digest = "normal_new".to_owned();
    ss_map.add_statement(&normal_new);

    let mut mixed_internal = generate_any_exec_info();
    mixed_internal.digest = "mixed".to_owned();
    mixed_internal.is_internal = true;
    ss_map.add_statement(&mixed_internal);
    let mut mixed_external = generate_any_exec_info();
    mixed_external.digest = mixed_internal.digest.clone();
    ss_map.add_statement(&mixed_external);

    let before_clear: Vec<String> = ss_map
        .summary_map_values()
        .into_iter()
        .map(|summary| summary.lock().unwrap().digest.clone())
        .collect();
    let mixed_summary = ss_map
        .summary_map_values()
        .into_iter()
        .find(|summary| summary.lock().unwrap().digest == mixed_internal.digest)
        .expect("mixed summary must exist");
    assert!(!mixed_summary.lock().unwrap().is_internal);

    ss_map.set_enabled_internal_query(false);
    assert_eq!(ss_map.summary_map_size(), before_clear.len() - 1);
    let after_clear: Vec<String> = ss_map
        .summary_map_values()
        .into_iter()
        .map(|summary| summary.lock().unwrap().digest.clone())
        .collect();
    let expected_after_clear: Vec<String> = before_clear
        .into_iter()
        .filter(|digest| digest != &internal_only.digest)
        .collect();
    assert_eq!(after_clear, expected_after_clear);
    let mut internal_key = StmtDigestKey::new();
    internal_key.init(
        &internal_only.schema_name,
        &internal_only.digest,
        &internal_only.prev_sql_digest,
        &internal_only.plan_digest,
        &internal_only.resource_group_name,
        "",
    );
    assert!(ss_map.summary_map_get(&internal_key).is_none());
    assert!(after_clear
        .iter()
        .any(|digest| digest == &mixed_internal.digest));
}

/// Go `TestMaxStmtCount`.
#[test]
fn test_max_stmt_count() {
    let ss_map = StmtSummaryByDigestMap::new();
    let now = unix_now();
    // to disable expiration
    ss_map.set_begin_time_for_cur_interval(now + 60);

    // Test the original value and modify it.
    assert_eq!(ss_map.max_stmt_count(), 3000);
    ss_map.set_max_stmt_count(10).unwrap();
    assert_eq!(ss_map.max_stmt_count(), 10);

    // 100 digests
    let mut info = generate_any_exec_info();
    let loops = 100;
    for i in 0..loops {
        info.digest = format!("digest{i}");
        ss_map.add_statement(&info);
    }

    // Summary count should be MaxStmtCount.
    assert_eq!(ss_map.summary_map_size(), 10);

    // LRU cache should work.
    for i in (loops - 10)..loops {
        let mut key = StmtDigestKey::new();
        key.init(
            &info.schema_name,
            &format!("digest{i}"),
            "",
            &info.plan_digest,
            &info.resource_group_name,
            "",
        );
        assert!(ss_map.summary_map_get(&key).is_some());
    }

    // Change to a bigger value.
    ss_map.set_max_stmt_count(50).unwrap();
    for i in 0..loops {
        info.digest = format!("digest{i}");
        ss_map.add_statement(&info);
    }
    assert_eq!(ss_map.summary_map_size(), 50);

    // Change to a smaller value.
    ss_map.set_max_stmt_count(10).unwrap();
    for i in 0..loops {
        info.digest = format!("digest{i}");
        ss_map.add_statement(&info);
    }
    assert_eq!(ss_map.summary_map_size(), 10);

    ss_map.set_max_stmt_count(3000).unwrap();
}

/// Go `TestMaxSQLLength`.
#[test]
fn test_max_sql_length() {
    let ss_map = StmtSummaryByDigestMap::new();
    let now = unix_now();
    // to disable expiration
    ss_map.set_begin_time_for_cur_interval(now + 60);

    // Test the original value and modify it.
    let max_sql_length = ss_map.max_sql_length();
    assert_eq!(max_sql_length, 32768);

    // Create a long SQL
    let length = max_sql_length * 10;
    let str = "a".repeat(length);

    let mut info = generate_any_exec_info();
    info.lazy_info = Arc::new(MockLazyInfo {
        original_sql: str.clone(),
        ..MockLazyInfo::default()
    });
    info.normalized_sql.clone_from(&str);
    ss_map.add_statement(&info);

    let mut key = StmtDigestKey::new();
    key.init(
        &info.schema_name,
        &info.digest,
        "",
        &info.plan_digest,
        &info.resource_group_name,
        "",
    );
    let value = ss_map.summary_map_get(&key).expect("summary must exist");

    let expected_sql = format!("{}(len:{length})", "a".repeat(max_sql_length));
    let summary = value.lock().unwrap();
    assert_eq!(summary.normalized_sql, expected_sql);
    let element = summary.history.back().unwrap().lock().unwrap();
    assert_eq!(element.stats.sample_sql, expected_sql);
    drop(element);
    drop(summary);

    ss_map.set_max_sql_length(100);
    assert_eq!(ss_map.max_sql_length(), 100);
    ss_map.set_max_sql_length(10);
    assert_eq!(ss_map.max_sql_length(), 10);
    ss_map.set_max_sql_length(32768);
    assert_eq!(ss_map.max_sql_length(), 32768);
}

/// Go `TestFormatSQLClone`.
#[test]
fn test_format_sql_clone() {
    let base = "x".repeat(1024);
    let sub = &base[100..200];

    let formatted = format_sql(sub);

    assert_eq!(formatted, sub);
    // Verify that the formatted string is a true clone, not pointing to the
    // same underlying data.
    assert!(!std::ptr::eq(sub.as_ptr(), formatted.as_ptr()));
}

/// Go `TestSetMaxStmtCountParallel`.
#[test]
fn test_set_max_stmt_count_parallel() {
    let ss_map = StmtSummaryByDigestMap::new();
    let now = unix_now();
    // to disable expiration
    ss_map.set_begin_time_for_cur_interval(now + 60);

    let threads = 8;
    const LOOPS: i32 = 20;
    fn add_stmt(ss_map: &StmtSummaryByDigestMap) {
        let mut info = generate_any_exec_info();

        // Add 20 times with different digest.
        for i in 0..LOOPS {
            info.digest = format!("digest{i}");
            ss_map.add_statement(&info);
        }
    }

    thread::scope(|scope| {
        for _ in 0..threads {
            scope.spawn(|| add_stmt(&ss_map));
        }
        scope.spawn(|| {
            // Turn down MaxStmtCount one by one.
            for i in (1..=10).rev() {
                ss_map.set_max_stmt_count(i).unwrap();
            }
        });
    });

    // add stmt again to make sure evict occurs after SetMaxStmtCount.
    add_stmt(&ss_map);

    let reader = new_stmt_summary_reader_for_test(&ss_map);
    let datums = reader.get_stmt_summary_current_rows();
    // due to evictions happened in cache, an additional record will be
    // appended to the table.
    assert_eq!(datums.len(), 2);

    ss_map.set_max_stmt_count(3000).unwrap();
}

/// Go's Prometheus gauges narrow to a sink that keeps the last values
/// published, which is what `readGaugeValue` reads back.
#[derive(Clone, Default)]
struct RecordingWindowMetricsSink(Arc<Mutex<(f64, f64)>>);

impl WindowMetricsSink for RecordingWindowMetricsSink {
    fn set_window_metrics(&self, record_count: f64, evicted_count: f64) {
        *self.0.lock().unwrap() = (record_count, evicted_count);
    }
}

/// A map wired exactly as `StmtSummaryByDigestMap::new` wires one, except
/// that the window metrics land in a readable sink.
fn map_with_recorded_metrics() -> (StmtSummaryByDigestMap, RecordingWindowMetricsSink) {
    let evicted = Arc::new(Mutex::new(StmtSummaryByDigestEvicted::new()));
    let recorder = RecordingWindowMetricsSink::default();
    let ss_map = StmtSummaryByDigestMap::with_sinks(Box::new(evicted), Box::new(recorder.clone()));
    (ss_map, recorder)
}

/// Go `TestStmtSummaryMetrics`.
///
/// Go reads `metrics.StmtSummaryWindowRecordCount` /
/// `...WindowEvictedCount` off the process-global Prometheus registry; the
/// sink this map publishes to holds the same two numbers.
#[test]
fn test_stmt_summary_metrics() {
    let (ss_map, metrics) = map_with_recorded_metrics();
    ss_map.set_max_stmt_count(2).unwrap();

    ss_map.set_begin_time_for_cur_interval(unix_now() + 60);
    let mut info = generate_any_exec_info();

    info.digest = "digest1".to_owned();
    ss_map.add_statement(&info);
    info.digest = "digest2".to_owned();
    ss_map.add_statement(&info);
    assert_eq!(*metrics.0.lock().unwrap(), (2.0, 0.0));

    info.digest = "digest3".to_owned();
    ss_map.add_statement(&info);
    assert_eq!(*metrics.0.lock().unwrap(), (2.0, 1.0));

    ss_map.set_begin_time_for_cur_interval(unix_now() - ss_map.refresh_interval() - 1);
    info.digest = "digest3".to_owned();
    ss_map.add_statement(&info);
    assert_eq!(*metrics.0.lock().unwrap(), (2.0, 0.0));

    ss_map.clear();
    assert_eq!(*metrics.0.lock().unwrap(), (0.0, 0.0));
}

/// Go `TestStmtSummaryMetricsAfterCapacityChange`.
#[test]
fn test_stmt_summary_metrics_after_capacity_change() {
    let (ss_map, metrics) = map_with_recorded_metrics();
    ss_map.set_max_stmt_count(3).unwrap();

    ss_map.set_begin_time_for_cur_interval(unix_now() + 60);
    let mut info = generate_any_exec_info();

    info.digest = "digest1".to_owned();
    ss_map.add_statement(&info);
    info.digest = "digest2".to_owned();
    ss_map.add_statement(&info);
    info.digest = "digest3".to_owned();
    ss_map.add_statement(&info);
    assert_eq!(*metrics.0.lock().unwrap(), (3.0, 0.0));

    ss_map.set_max_stmt_count(1).unwrap();
    assert_eq!(*metrics.0.lock().unwrap(), (1.0, 0.0));
}

/// Go `TestDisableStmtSummary`.
#[test]
fn test_disable_stmt_summary() {
    let ss_map = StmtSummaryByDigestMap::new();
    let now = unix_now();

    ss_map.set_enabled(false);
    ss_map.set_begin_time_for_cur_interval(now + 60);

    let mut info1 = generate_any_exec_info();
    ss_map.add_statement(&info1);
    let reader = new_stmt_summary_reader_for_test(&ss_map);
    let datums = reader.get_stmt_summary_current_rows();
    assert_eq!(datums.len(), 0);

    ss_map.set_enabled(true);

    ss_map.add_statement(&info1);
    let datums = reader.get_stmt_summary_current_rows();
    assert_eq!(datums.len(), 1);

    ss_map.set_begin_time_for_cur_interval(now + 60);

    info1.lazy_info = mock_lazy_info("original_sql2", "binding_sql1", "binding_digest1");
    info1.normalized_sql = "normalized_sql2".to_owned();
    info1.digest = "digest2".to_owned();
    ss_map.add_statement(&info1);
    let datums = reader.get_stmt_summary_current_rows();
    assert_eq!(datums.len(), 2);

    // Unset
    ss_map.set_enabled(false);
    ss_map.set_begin_time_for_cur_interval(now + 60);
    ss_map.add_statement(&info1);
    let datums = reader.get_stmt_summary_current_rows();
    assert_eq!(datums.len(), 0);

    // Unset
    ss_map.set_enabled(false);
    ss_map.set_enabled(true);

    ss_map.set_begin_time_for_cur_interval(now + 60);
    ss_map.add_statement(&info1);
    let datums = reader.get_stmt_summary_current_rows();
    assert_eq!(datums.len(), 1);

    // Set back.
    ss_map.set_enabled(true);
}

/// Go `TestEnableSummaryParallel`.
#[test]
fn test_enable_summary_parallel() {
    let ss_map = StmtSummaryByDigestMap::new();

    let threads = 8;
    let loops = 32;
    let reader = new_stmt_summary_reader_for_test(&ss_map);
    thread::scope(|scope| {
        for _ in 0..threads {
            scope.spawn(|| {
                let info = generate_any_exec_info();
                // Add 32 times with the same digest.
                for i in 0..loops {
                    // Sometimes enable it and sometimes disable it.
                    ss_map.set_enabled(i % 2 == 0);
                    ss_map.add_statement(&info);
                    // Try to read it.
                    let _ = reader.get_stmt_summary_history_rows();
                }
                ss_map.set_enabled(true);
            });
        }
    });
    // Ensure that there's no deadlock.

    // Ensure that it's enabled at last.
    assert!(ss_map.enabled());
}

/// Go `TestFormatBackoffTypes`.
#[test]
fn test_format_backoff_types() {
    let mut backoff_map: HashMap<String, i64> = HashMap::new();
    assert_eq!(format_backoff_types(&backoff_map), None);
    backoff_map.insert("pdrpc".to_owned(), 1);
    assert_eq!(
        format_backoff_types(&backoff_map).as_deref(),
        Some("pdrpc:1")
    );
    backoff_map.insert("txnlock".to_owned(), 2);

    assert_eq!(
        format_backoff_types(&backoff_map).as_deref(),
        Some("txnlock:2,pdrpc:1")
    );
}

/// Go `TestRefreshCurrentSummary`.
#[test]
fn test_refresh_current_summary() {
    let ss_map = StmtSummaryByDigestMap::new();
    let now = unix_now();

    ss_map.set_begin_time_for_cur_interval(now + 10);
    let info1 = generate_any_exec_info();
    let mut key = StmtDigestKey::new();
    key.init(
        &info1.schema_name,
        &info1.digest,
        "",
        &info1.plan_digest,
        &info1.resource_group_name,
        "",
    );
    ss_map.add_statement(&info1);
    assert_eq!(ss_map.summary_map_size(), 1);
    let value = ss_map.summary_map_get(&key).expect("summary must exist");
    let element = value
        .lock()
        .unwrap()
        .history
        .back()
        .map(Arc::clone)
        .unwrap();
    assert_eq!(
        element.lock().unwrap().begin_time,
        ss_map.begin_time_for_cur_interval()
    );
    assert_eq!(element.lock().unwrap().stats.exec_count, 1);

    ss_map.set_begin_time_for_cur_interval(now - 1900);
    element.lock().unwrap().begin_time = now - 1900;
    ss_map.add_statement(&info1);
    assert_eq!(ss_map.summary_map_size(), 1);
    let value = ss_map.summary_map_get(&key).expect("summary must exist");
    assert_eq!(value.lock().unwrap().history.len(), 2);
    let element = value
        .lock()
        .unwrap()
        .history
        .back()
        .map(Arc::clone)
        .unwrap();
    assert!(element.lock().unwrap().begin_time > now - 1900);
    assert_eq!(element.lock().unwrap().stats.exec_count, 1);

    ss_map.set_refresh_interval(10);
    ss_map.set_begin_time_for_cur_interval(now - 20);
    element.lock().unwrap().begin_time = now - 20;
    ss_map.add_statement(&info1);
    assert_eq!(value.lock().unwrap().history.len(), 3);
}

/// Go `TestSummaryHistory`.
#[test]
fn test_summary_history() {
    let ss_map = StmtSummaryByDigestMap::new();
    let now = unix_now();
    ss_map.set_refresh_interval(10);
    ss_map.set_history_size(10);

    let mut info1 = generate_any_exec_info();
    let mut key = StmtDigestKey::new();
    key.init(
        &info1.schema_name,
        &info1.digest,
        "",
        &info1.plan_digest,
        &info1.resource_group_name,
        "",
    );
    for i in 0..11_i64 {
        ss_map.set_begin_time_for_cur_interval(now + (i + 1) * 10);
        ss_map.add_statement(&info1);
        assert_eq!(ss_map.summary_map_size(), 1);
        let value = ss_map.summary_map_get(&key).expect("summary must exist");
        let ssbd = value.lock().unwrap();
        if i < 10 {
            assert_eq!(ssbd.history.len(), usize::try_from(i + 1).unwrap());
            let element = ssbd.history.back().unwrap().lock().unwrap();
            assert_eq!(element.begin_time, ss_map.begin_time_for_cur_interval());
            assert_eq!(element.stats.exec_count, 1);
        } else {
            assert_eq!(ssbd.history.len(), 10);
            assert_eq!(
                ssbd.history.back().unwrap().lock().unwrap().begin_time,
                ss_map.begin_time_for_cur_interval()
            );
            assert_eq!(
                ssbd.history.front().unwrap().lock().unwrap().begin_time,
                now + 20
            );
        }
    }

    let reader = new_stmt_summary_reader_for_test(&ss_map);
    let datum = reader.get_stmt_summary_history_rows();
    assert_eq!(datum.len(), 10);

    let value = ss_map.summary_map_get(&key).expect("summary must exist");
    let summary = value.lock().unwrap();
    let history = summary.collect_history_summaries(None, 2);
    let begin_times: Vec<i64> = history
        .iter()
        .map(|element| element.lock().unwrap().begin_time)
        .collect();
    assert_eq!(begin_times, vec![now + 100, now + 110]);
    drop(summary);

    ss_map.set_history_size(5);
    let datum = reader.get_stmt_summary_history_rows();
    assert_eq!(datum.len(), 5);

    ss_map.clear_history();
    let summary = value.lock().unwrap();
    assert_eq!(summary.history.len(), 1);
    assert_eq!(
        summary.history.back().unwrap().lock().unwrap().begin_time,
        now + 110
    );
    drop(summary);

    // test eviction
    ss_map.clear();
    ss_map.set_max_stmt_count(1).unwrap();
    // insert first digest
    for i in 0..6_i64 {
        ss_map.set_begin_time_for_cur_interval(now + i * 10);
        ss_map.add_statement(&info1);
        assert_eq!(ss_map.summary_map_size(), 1);
        assert_eq!(ss_map.evicted().unwrap().lock().unwrap().history().len(), 0);
    }
    // insert another digest to evict it
    info1.digest = "bandit digest".to_owned();
    ss_map.add_statement(&info1);
    assert_eq!(ss_map.summary_map_size(), 1);
    // length of `other` should not longer than historySize.
    assert_eq!(ss_map.evicted().unwrap().lock().unwrap().history().len(), 5);
    let datum = reader.get_stmt_summary_history_rows();
    // length of STATEMENT_SUMMARY_HISTORY == (history in cache) + (history
    // evicted)
    assert_eq!(datum.len(), 6);

    ss_map.set_max_stmt_count(3000).unwrap();
    ss_map.set_refresh_interval(1800);
    ss_map.set_history_size(24);
}

/// Go `TestPrevSQL`.
#[test]
fn test_prev_sql() {
    let ss_map = StmtSummaryByDigestMap::new();
    let now = unix_now();
    // to disable expiration
    ss_map.set_begin_time_for_cur_interval(now + 60);

    let mut info1 = generate_any_exec_info();
    info1.prev_sql = "prevSQL".to_owned();
    info1.prev_sql_digest = "prevSQLDigest".to_owned();
    ss_map.add_statement(&info1);
    let mut key = StmtDigestKey::new();
    key.init(
        &info1.schema_name,
        &info1.digest,
        &info1.prev_sql_digest,
        &info1.plan_digest,
        &info1.resource_group_name,
        "",
    );
    assert_eq!(ss_map.summary_map_size(), 1);
    assert!(ss_map.summary_map_get(&key).is_some());

    // same prevSQL
    ss_map.add_statement(&info1);
    assert_eq!(ss_map.summary_map_size(), 1);

    // different prevSQL
    info1.prev_sql = "prevSQL1".to_owned();
    info1.prev_sql_digest = "prevSQLDigest1".to_owned();
    ss_map.add_statement(&info1);
    assert_eq!(ss_map.summary_map_size(), 2);
    key.init(
        &info1.schema_name,
        &info1.digest,
        &info1.prev_sql_digest,
        &info1.plan_digest,
        &info1.resource_group_name,
        "",
    );
    assert!(ss_map.summary_map_get(&key).is_some());
}

/// Go `TestEndTime`.
#[test]
fn test_end_time() {
    let ss_map = StmtSummaryByDigestMap::new();
    let now = unix_now();
    ss_map.set_begin_time_for_cur_interval(now - 100);

    let info1 = generate_any_exec_info();
    ss_map.add_statement(&info1);
    let mut key = StmtDigestKey::new();
    key.init(
        &info1.schema_name,
        &info1.digest,
        "",
        &info1.plan_digest,
        &info1.resource_group_name,
        "",
    );
    assert_eq!(ss_map.summary_map_size(), 1);
    let value = ss_map.summary_map_get(&key).expect("summary must exist");
    {
        let ssbd = value.lock().unwrap();
        let element = ssbd.history.back().unwrap().lock().unwrap();
        assert_eq!(element.begin_time, now - 100);
        assert_eq!(element.end_time, now + 1700);
    }

    ss_map.set_refresh_interval(3600);
    ss_map.add_statement(&info1);
    {
        let ssbd = value.lock().unwrap();
        assert_eq!(ssbd.history.len(), 1);
        let element = ssbd.history.back().unwrap().lock().unwrap();
        assert_eq!(element.begin_time, now - 100);
        assert_eq!(element.end_time, now + 3500);
    }

    ss_map.set_refresh_interval(60);
    ss_map.add_statement(&info1);
    let now2 = unix_now();
    {
        let ssbd = value.lock().unwrap();
        assert_eq!(ssbd.history.len(), 2);
        let element = ssbd.history.front().unwrap().lock().unwrap();
        assert_eq!(element.begin_time, now - 100);
        assert!(element.end_time >= now);
        assert!(element.end_time <= now2);
        drop(element);
        let element = ssbd.history.back().unwrap().lock().unwrap();
        assert!(element.begin_time >= now - 60);
        assert!(element.begin_time <= now2);
        assert_eq!(element.end_time - element.begin_time, 60);
    }

    ss_map.set_refresh_interval(1800);
}

/// Go `TestPointGet`.
#[test]
fn test_point_get() {
    let ss_map = StmtSummaryByDigestMap::new();
    let now = unix_now();
    ss_map.set_begin_time_for_cur_interval(now - 100);

    let mut info1 = generate_any_exec_info();
    info1.plan_digest = String::new();
    info1.lazy_info = Arc::new(MockLazyInfo {
        original_sql: "original_sql1".to_owned(),
        plan: fake_plan_digest_generator(),
        binding_sql: "binding_sql1".to_owned(),
        binding_digest: "binding_digest1".to_owned(),
        ..MockLazyInfo::default()
    });
    ss_map.add_statement(&info1);
    let mut key = StmtDigestKey::new();
    key.init(
        &info1.schema_name,
        &info1.digest,
        "",
        "",
        &info1.resource_group_name,
        "",
    );
    assert_eq!(ss_map.summary_map_size(), 1);
    let value = ss_map.summary_map_get(&key).expect("summary must exist");
    let element = value
        .lock()
        .unwrap()
        .history
        .back()
        .map(Arc::clone)
        .unwrap();
    assert_eq!(element.lock().unwrap().stats.exec_count, 1);

    ss_map.add_statement(&info1);
    assert_eq!(element.lock().unwrap().stats.exec_count, 2);
}

/// Go `TestAddStatementGroupByUser`.
#[test]
fn test_add_statement_group_by_user() {
    let ss_map = StmtSummaryByDigestMap::new();

    let mut info1 = generate_any_exec_info();
    info1.user = "alice".to_owned();
    let mut info2 = generate_any_exec_info();
    info2.user = "bob".to_owned();

    // Flag off: both statements collapse into one record.
    ss_map.add_statement(&info1);
    ss_map.add_statement(&info2);
    assert_eq!(ss_map.summary_map_size(), 1);

    // Flipping the flag clears prior data (different grouping key).
    ss_map.set_group_by_user(true);
    assert_eq!(ss_map.summary_map_size(), 0);

    ss_map.add_statement(&info1);
    ss_map.add_statement(&info2);
    ss_map.add_statement(&info1);
    assert_eq!(ss_map.summary_map_size(), 2);

    // With grouping ON, each record's authUsers must hold exactly one user
    // — the one that groups it.
    let mut seen: HashSet<String> = HashSet::new();
    for value in ss_map.summary_map_values() {
        let ssbd = value.lock().unwrap();
        let element = ssbd.history.front().unwrap().lock().unwrap();
        assert_eq!(element.stats.auth_users.len(), 1);
        for user in &element.stats.auth_users {
            seen.insert(user.clone());
        }
    }
    assert!(seen.contains("alice"));
    assert!(seen.contains("bob"));

    // Flipping back off clears again, and re-emitted records merge users.
    ss_map.set_group_by_user(false);
    assert_eq!(ss_map.summary_map_size(), 0);
    ss_map.add_statement(&info1);
    ss_map.add_statement(&info2);
    assert_eq!(ss_map.summary_map_size(), 1);
    for value in ss_map.summary_map_values() {
        let ssbd = value.lock().unwrap();
        let element = ssbd.history.front().unwrap().lock().unwrap();
        assert_eq!(element.stats.auth_users.len(), 2);
    }
}

/// Go `TestStmtDigestKeyBoundary`.
#[test]
fn test_stmt_digest_key_boundary() {
    let mut k1 = StmtDigestKey::new();
    k1.init("schema", "digest", "prev", "plan", "rg", "alice");
    let mut k2 = StmtDigestKey::new();
    k2.init("schema", "digest", "prev", "plan", "rga", "lice");
    assert_ne!(
        k1.hash(),
        k2.hash(),
        "user segment must have an unambiguous boundary"
    );

    // user="" leaves the hash equal to the legacy 5-field layout.
    let mut off = StmtDigestKey::new();
    off.init("schema", "digest", "prev", "plan", "rg", "");
    let mut legacy: Vec<u8> = Vec::new();
    legacy.extend_from_slice(b"digest");
    legacy.extend_from_slice(b"schema");
    legacy.extend_from_slice(b"prev");
    legacy.extend_from_slice(b"plan");
    legacy.extend_from_slice(b"rg");
    assert_eq!(off.hash(), legacy.as_slice());
}

#[deny(unused_must_use)]
#[test]
fn go_v1_statement_summary_returns_can_be_ignored() {
    StmtDigestKey::new();
    let key = StmtDigestKey::new();
    key.hash();

    StmtSummaryStmtCtx::new();
    let ctx = StmtSummaryStmtCtx::new();
    ctx.affected_rows();
    ctx.warning_count();

    let info = generate_any_exec_info();
    let _ = new_stmt_summary_stats(&info);
    let _ = StmtSummaryByDigestElement::new(&info, 0, 60, 0, 0);

    let summary = StmtSummaryByDigest::default();
    summary.collect_history_summaries(None, 1);

    StmtSummaryByDigestMap::new();
    StmtSummaryByDigestMap::with_sinks(Box::new(NoopEvictedSink), Box::new(NoopWindowMetricsSink));
    let map = StmtSummaryByDigestMap::new();
    map.enabled();
    map.enabled_internal();
    map.history_enabled();
    map.refresh_interval();
    map.history_size();
    map.group_by_user();
    map.max_stmt_count();
    map.max_sql_length();
    map.begin_time_for_cur_interval();
    map.summary_map_size();
    map.summary_map_values();
    let _ = map.normalized_sql_for_digest("");
    let _ = map.evicted();
    let _ = map.summary_map_get(&key);

    format_sql("select 1");
    avg_int(1, 1);
    avg_float(1, 1);
    avg_float4_uint(1, 1);
    avg_sum_float(1.0, 1);
}
