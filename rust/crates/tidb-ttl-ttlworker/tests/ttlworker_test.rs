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

//! The session-free Go tests of `pkg/ttl/ttlworker` (`del_test.go`,
//! `task_manager_test.go`, `timer_test.go`, `job_manager_test.go`). The tests
//! that drive a live TiDB run in `tidb-session` over the embedded store.

use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tidb_ast::CiString;
use tidb_datatype::Datum;
use tidb_model::{GoShared, TTLInfo, TableInfo};
use tidb_timer::go_time::GoTime;
use tidb_ttl::cache::table::PhysicalTable;
use tidb_ttl::cache::task::{TTLTask, TTLTaskState, TaskStatus};
use tidb_ttl_client::context::Context;
use tidb_ttl_ttlworker::del::{new_ttl_del_retry_buffer, TtlDelRetryItem, TtlDeleteTask, DEL_MAX_RETRY, DEL_RETRY_BUFFER_SIZE, DEL_RETRY_INTERVAL};
use tidb_ttl_ttlworker::job_manager::{gc_ttl_table_status_gc_sql, summarize_task_result_with_error, TtlSummary};
use tidb_ttl_ttlworker::scan::{Task, TtlScanTask, TtlStatistics};
use tidb_ttl_ttlworker::task_manager::{
    get_max_running_tasks_limit, marshal_task_state, RunningScanTask, WAIT_TASK_PROCESS_ROWS_TIMEOUT,
};
use tidb_ttl_ttlworker::timer_sync::{build_timer_key_with_id, get_timer_tags, get_ttl_schedule_policy};

/// Go `newMockTTLTbl`'s name, which is all these tests read of the table.
fn new_mock_ttl_tbl(name: &str) -> PhysicalTable {
    PhysicalTable {
        schema: CiString::new("test"),
        table_info: GoShared::new(TableInfo {
            name: CiString::new(name),
            ..TableInfo::default()
        }),
        ..PhysicalTable::default()
    }
}

/// Go `newMockDeleteTask`.
fn new_mock_delete_task(tbl: PhysicalTable, rows: Vec<Vec<Datum>>, expire: GoTime) -> TtlDeleteTask {
    TtlDeleteTask {
        job_id: "test_job".to_owned(),
        scan_id: 1,
        tbl,
        expire,
        rows,
        statistics: Arc::new(TtlStatistics::default()),
    }
}

/// Go `TestTTLDelRetryBuffer`.
#[test]
fn test_ttl_del_retry_buffer() {
    let create_task = |name: &str| {
        let task = new_mock_delete_task(new_mock_ttl_tbl(name), vec![Vec::new(); 10], GoTime::from_unix_milli(0));
        let rows = task.rows.clone();
        let statistics = Arc::clone(&task.statistics);
        (task, rows, statistics)
    };
    let should_not_do_retry = |_: &TtlDelRetryItem| -> Vec<Vec<Datum>> { panic!("should not do retry") };

    let start = Instant::now();
    let tm = Arc::new(Mutex::new(start));
    let advance = |d: Duration| *tm.lock().unwrap() += d;
    let mut buffer = new_ttl_del_retry_buffer();
    assert_eq!(DEL_RETRY_BUFFER_SIZE, 128);
    assert_eq!(DEL_MAX_RETRY, 3);
    assert_eq!(DEL_RETRY_INTERVAL, Duration::from_secs(5));
    let clock = Arc::clone(&tm);
    buffer.configure(3, 2, Duration::from_secs(10), Box::new(move || *clock.lock().unwrap()));

    // add success task
    let (task1, rows1, statics1) = create_task("t1");
    buffer.record_task_result(&task1, Vec::new());
    assert_eq!(buffer.len(), 0);
    buffer.do_retry(should_not_do_retry);
    assert_eq!(statics1.error_rows.load(Ordering::SeqCst), 0);

    // add a task with 1 failed rows
    buffer.record_task_result(&task1, rows1[..1].to_vec());
    assert_eq!(buffer.len(), 1);
    buffer.do_retry(should_not_do_retry);
    assert_eq!(statics1.error_rows.load(Ordering::SeqCst), 0);

    // add another task with 2 failed rows
    advance(Duration::from_secs(1));
    let (task2, rows2, statics2) = create_task("t2");
    buffer.record_task_result(&task2, rows2[..2].to_vec());
    assert_eq!(buffer.len(), 2);
    buffer.do_retry(should_not_do_retry);
    assert_eq!(statics2.error_rows.load(Ordering::SeqCst), 0);

    // add another task with 3 failed rows
    advance(Duration::from_secs(1));
    let (task3, rows3, statics3) = create_task("t3");
    buffer.record_task_result(&task3, rows3[..3].to_vec());
    assert_eq!(buffer.len(), 3);
    buffer.do_retry(should_not_do_retry);
    assert_eq!(statics3.error_rows.load(Ordering::SeqCst), 0);

    // add new task will eliminate old tasks
    advance(Duration::from_secs(1));
    let (task4, rows4, statics4) = create_task("t4");
    buffer.record_task_result(&task4, rows4[..4].to_vec());
    assert_eq!(buffer.len(), 3);
    buffer.do_retry(should_not_do_retry);
    assert_eq!(statics4.error_rows.load(Ordering::SeqCst), 0);
    assert_eq!(statics1.error_rows.load(Ordering::SeqCst), 1);

    // poll up-to-date tasks
    advance(Duration::from_secs(10) - Duration::from_millis(1));
    let tasks = Mutex::new(Vec::new());
    let do_retry_success = |item: &TtlDelRetryItem| -> Vec<Vec<Datum>> {
        item.task.statistics.inc_success_rows(item.task.rows.len());
        tasks.lock().unwrap().push(item.task.clone());
        Vec::new()
    };
    let next_interval = buffer.do_retry(do_retry_success);
    assert_eq!(next_interval, Duration::from_millis(1));
    {
        let tasks = tasks.lock().unwrap();
        assert_eq!(tasks.len(), 2);
        assert_eq!(tasks[0].tbl.name().lowercase(), "t2");
        assert_eq!(tasks[0].expire, GoTime::from_unix_milli(0));
        assert_eq!(tasks[0].rows.len(), 2);
        assert_eq!(tasks[1].tbl.name().lowercase(), "t3");
        assert_eq!(tasks[1].rows.len(), 3);
    }
    assert_eq!(statics2.success_rows.load(Ordering::SeqCst), 2);
    assert_eq!(statics2.error_rows.load(Ordering::SeqCst), 0);
    assert_eq!(buffer.len(), 1);
    assert_eq!(statics3.success_rows.load(Ordering::SeqCst), 3);
    assert_eq!(statics3.error_rows.load(Ordering::SeqCst), 0);
    assert_eq!(statics4.success_rows.load(Ordering::SeqCst), 0);
    assert_eq!(statics4.error_rows.load(Ordering::SeqCst), 0);

    // poll next
    advance(Duration::from_millis(1));
    tasks.lock().unwrap().clear();
    let next_interval = buffer.do_retry(do_retry_success);
    assert_eq!(next_interval, Duration::from_secs(10));
    {
        let tasks = tasks.lock().unwrap();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].tbl.name().lowercase(), "t4");
        assert_eq!(tasks[0].rows.len(), 4);
    }
    assert_eq!(buffer.len(), 0);
    assert_eq!(statics4.success_rows.load(Ordering::SeqCst), 4);
    assert_eq!(statics4.error_rows.load(Ordering::SeqCst), 0);

    // test retry max count
    let retry_cnt = Mutex::new(0);
    let do_retry_fail = |item: &TtlDelRetryItem| -> Vec<Vec<Datum>> {
        *retry_cnt.lock().unwrap() += 1;
        item.task.statistics.success_rows.fetch_add(1, Ordering::SeqCst);
        item.task.rows[1..].to_vec()
    };
    let (task5, rows5, statics5) = create_task("t5");
    buffer.record_task_result(&task5, rows5[..5].to_vec());
    assert_eq!(buffer.len(), 1);
    advance(Duration::from_secs(10));
    let next_interval = buffer.do_retry(do_retry_fail);
    assert_eq!(next_interval, Duration::from_secs(10));
    assert_eq!(statics5.success_rows.load(Ordering::SeqCst), 1);
    assert_eq!(statics5.error_rows.load(Ordering::SeqCst), 0);
    assert_eq!(*retry_cnt.lock().unwrap(), 1);
    advance(Duration::from_secs(10));
    buffer.do_retry(do_retry_fail);
    assert_eq!(statics5.success_rows.load(Ordering::SeqCst), 2);
    assert_eq!(statics5.error_rows.load(Ordering::SeqCst), 3);
    assert_eq!(*retry_cnt.lock().unwrap(), 2);
    assert_eq!(buffer.len(), 0);

    // test task should be immutable
    assert_eq!(task5.rows.len(), 10);

    // test drain
    assert_eq!(buffer.len(), 0);
    let (task6, rows6, statics6) = create_task("t6");
    buffer.record_task_result(&task6, rows6[..7].to_vec());
    assert_eq!(buffer.len(), 1);
    assert_eq!(statics6.success_rows.load(Ordering::SeqCst), 0);
    assert_eq!(statics6.error_rows.load(Ordering::SeqCst), 0);
    buffer.drain();
    assert_eq!(buffer.len(), 0);
    assert_eq!(statics6.success_rows.load(Ordering::SeqCst), 0);
    assert_eq!(statics6.error_rows.load(Ordering::SeqCst), 7);
}

fn running_task(state: Option<TTLTaskState>) -> RunningScanTask {
    let (ctx, cancel) = Context::with_cancel(&Context::background());
    RunningScanTask {
        scan_task: Arc::new(TtlScanTask {
            ctx,
            task: Task {
                task: TTLTask {
                    job_id: "test-job-id".to_owned(),
                    scan_id: 1,
                    state,
                    ..TTLTask::default()
                },
                ..Task::default()
            },
            tbl: new_mock_ttl_tbl("t1"),
            statistics: Arc::new(TtlStatistics::default()),
        }),
        cancel,
        result: None,
    }
}

/// Go `TestTaskFinishedCondition`.
#[test]
fn test_task_finished_condition() {
    let mut task = running_task(None);
    let logger = tidb_logutil::bg_logger();
    let statistics = Arc::clone(&task.scan_task.statistics);

    // result == nil means it is not finished, even if all rows processed
    assert!(task.result.is_none());
    assert!(!task.finished(&logger));
    statistics.total_rows.store(10, Ordering::SeqCst);
    statistics.success_rows.store(10, Ordering::SeqCst);
    assert!(!task.finished(&logger));

    for result_err in [None, Some("mockErr".to_owned())] {
        // result != nil but not all rows processed means it is not finished
        statistics.success_rows.store(0, Ordering::SeqCst);
        statistics.error_rows.store(0, Ordering::SeqCst);
        task.result = Some(task.scan_task.result(result_err.clone()));
        let result_unix = task.result.as_ref().unwrap().time.unix();
        assert!((result_unix - GoTime::now().unix()).abs() <= 5);
        assert!(!task.finished(&logger));
        statistics.success_rows.store(8, Ordering::SeqCst);
        statistics.error_rows.store(1, Ordering::SeqCst);
        assert!(!task.finished(&logger));

        // result != nil but time out means it is finished
        let mut result = task.scan_task.result(result_err.clone());
        result.time = GoTime::now().add(-(WAIT_TASK_PROCESS_ROWS_TIMEOUT.as_nanos() as i64) - 1_000_000_000);
        task.result = Some(result);
        assert!(task.finished(&logger));

        // result != nil and processed rows are more that total rows means it is finished
        statistics.success_rows.store(8, Ordering::SeqCst);
        statistics.error_rows.store(3, Ordering::SeqCst);
        assert!(task.finished(&logger));
    }
}

/// Go `dumpNewTaskState` adds a taken-over task's previous state, and keeps
/// the scan error.
#[test]
fn dump_new_task_state_adds_the_previous_owners_progress() {
    let mut task = running_task(Some(TTLTaskState {
        total_rows: 100,
        success_rows: 60,
        error_rows: 10,
        ..TTLTaskState::default()
    }));
    task.scan_task.statistics.total_rows.store(5, Ordering::SeqCst);
    task.scan_task.statistics.success_rows.store(4, Ordering::SeqCst);
    task.scan_task.statistics.error_rows.store(1, Ordering::SeqCst);
    task.result = Some(task.scan_task.result(Some("mockErr".to_owned())));
    let state = task.dump_new_task_state();
    assert_eq!((state.total_rows, state.success_rows, state.error_rows), (75, 64, 11));
    assert_eq!(state.scan_task_err, "mockErr");
    // Go `json.Marshal(state)`: `prev_owner` is `omitempty`.
    assert_eq!(
        marshal_task_state(&state),
        r#"{"total_rows":75,"success_rows":64,"error_rows":11,"scan_task_err":"mockErr"}"#
    );
    let resigned = TTLTaskState {
        previous_owner: "node1".to_owned(),
        ..TTLTaskState::default()
    };
    assert_eq!(
        marshal_task_state(&resigned),
        r#"{"total_rows":0,"success_rows":0,"error_rows":0,"scan_task_err":"","prev_owner":"node1"}"#
    );
}

/// A store whose region cache knows `count` TiKV stores.
#[derive(Debug)]
struct MockTikvStore(Option<usize>);

impl tidb_kv::region_map::RegionStore for MockTikvStore {
    fn split_regions(&self, _keys: Vec<Vec<u8>>, _scatter: bool, _table_id: Option<i64>) -> (Vec<u64>, Option<String>) {
        (Vec::new(), None)
    }

    fn wait_scatter_region_finish(&self, _region_id: u64, _backoff_ms: i64) -> Result<(), String> {
        Ok(())
    }

    fn check_region_in_scattering(&self, _region_id: u64) -> Result<bool, String> {
        Ok(false)
    }

    fn load_regions_in_key_range(
        &self,
        _start: &[u8],
        _end: &[u8],
    ) -> Result<Vec<tidb_kv::region_map::RegionMeta>, String> {
        Ok(Vec::new())
    }

    fn tikv_store_count(&self) -> Option<usize> {
        self.0
    }
}

/// Go `TestGetMaxRunningTasksLimit`.
#[test]
fn test_get_max_running_tasks_limit() {
    let store = |count: Option<usize>| -> tidb_ttl_ttlworker::Store { Some(Arc::new(MockTikvStore(count))) };
    tidb_vardef::TTL_RUNNING_TASKS.store(1, Ordering::SeqCst);
    assert_eq!(get_max_running_tasks_limit(&store(None)), 1);

    tidb_vardef::TTL_RUNNING_TASKS.store(2, Ordering::SeqCst);
    assert_eq!(get_max_running_tasks_limit(&store(None)), 2);

    tidb_vardef::TTL_RUNNING_TASKS.store(-1, Ordering::SeqCst);
    assert_eq!(get_max_running_tasks_limit(&None), 256);
    assert_eq!(get_max_running_tasks_limit(&store(None)), 256);

    // two TiKV stores (Go's third store is TiFlash, which the count excludes)
    assert_eq!(get_max_running_tasks_limit(&store(Some(2))), 2);
    // Go `getScanSplitCnt`: one range per TiKV store once there are more
    // than `splitScanCount`.
    assert_eq!(tidb_ttl_ttlworker::config::get_scan_split_cnt(&store(Some(2))), 64);
    assert_eq!(tidb_ttl_ttlworker::config::get_scan_split_cnt(&store(Some(100))), 100);
}

/// Go `TestGetTTLSchedulePolicy`.
#[test]
fn test_get_ttl_schedule_policy() {
    // normal case
    let (tp, expr) = get_ttl_schedule_policy(&TTLInfo {
        job_interval: "12h".to_owned(),
        ..TTLInfo::default()
    });
    assert_eq!(tp.as_str(), "INTERVAL");
    assert_eq!(expr, "12h");
    tidb_timer::timer::create_sched_event_policy(&tp, &expr).unwrap();

    // empty job interval
    let (tp, expr) = get_ttl_schedule_policy(&TTLInfo::default());
    assert_eq!(tp.as_str(), "INTERVAL");
    assert_eq!(expr, tidb_model::OLD_DEFAULT_TTL_JOB_INTERVAL);
    tidb_timer::timer::create_sched_event_policy(&tp, &expr).unwrap();
}

/// Go `buildTimerKeyWithID` and `getTimerTags`.
#[test]
fn timer_keys_and_tags_are_gos() {
    assert_eq!(build_timer_key_with_id(1, 2), "/tidb/ttl/physical_table/1/2");
    let tbl = TableInfo {
        name: CiString::new("T1"),
        ..TableInfo::default()
    };
    assert_eq!(get_timer_tags(&CiString::new("Db"), &tbl, None), ["db=Db", "table=T1"]);
}

/// Go `summarizeTaskResultWithError` and `gcTTLTableStatusGCSQL`.
#[test]
fn summaries_and_gc_sql_are_gos() {
    let task = |status: &str, state: Option<TTLTaskState>| Task {
        task: TTLTask {
            status: TaskStatus(status.to_owned()),
            state,
            ..TTLTask::default()
        },
        ..Task::default()
    };
    let tasks = [
        task(
            TaskStatus::FINISHED,
            Some(TTLTaskState {
                total_rows: 3,
                success_rows: 2,
                error_rows: 1,
                scan_task_err: "err1".to_owned(),
                ..TTLTaskState::default()
            }),
        ),
        task(TaskStatus::RUNNING, None),
        task(TaskStatus::WAITING, None),
    ];
    let summary = summarize_task_result_with_error(&tasks, Some("job is timeout".to_owned())).unwrap();
    assert_eq!(
        summary.summary_text,
        r#"{"total_rows":3,"success_rows":2,"error_rows":1,"total_scan_task":3,"scheduled_scan_task":2,"finished_scan_task":1,"scan_task_err":"job is timeout; err1"}"#
    );
    let clean = summarize_task_result_with_error(&tasks[1..], None).unwrap();
    assert_eq!(
        clean.summary_text,
        r#"{"total_rows":0,"success_rows":0,"error_rows":0,"total_scan_task":2,"scheduled_scan_task":1,"finished_scan_task":0}"#
    );
    let parsed: TtlSummary = serde_json::from_str(&summary.summary_text).unwrap();
    assert_eq!(parsed.scan_task_err, "job is timeout; err1");

    assert_eq!(
        gc_ttl_table_status_gc_sql(&[]),
        "DELETE FROM mysql.tidb_ttl_table_status WHERE current_job_status IS NULL"
    );
    assert_eq!(
        gc_ttl_table_status_gc_sql(&[1, 2]),
        "DELETE FROM mysql.tidb_ttl_table_status WHERE current_job_status IS NULL AND table_id NOT IN (1,2)"
    );
}
