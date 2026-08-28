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

//! Port of `pkg/domain/plan_replayer_handle_test.go` (origin/master):
//! `TestPlanReplayerHandleCollectTask` (:30), `TestPlanReplayerHandleDumpTask`
//! (:71), `TestPlanReplayerGC` (:138), and `TestInsertPlanReplayerStatus`
//! (:171), against `tidb_domain::plan_replayer` — the transcreation of
//! `pkg/domain/plan_replayer.go`.
//!
//! The Go tests drive a full `testkit.CreateMockStoreAndDomain` stack and
//! speak SQL to `mysql.plan_replayer_task` / `mysql.plan_replayer_status`.
//! This tier has neither a store nor an internal-session executor, so the two
//! executor boundaries (`InternalSqlExecutor`, `RestrictedSqlExecutor`) are
//! scripted with a shared in-test model of the two tables, the same pattern
//! the transcreation's own `mod tests` uses. The table model reproduces the
//! exact rows the Go test inserts:
//!
//! - a `plan_replayer_status` row with a `token` and no `fail_reason` is a
//!   SUCCESS row — it satisfies the `fail_reason is null` probe of
//!   `checkUnHandledReplayerTask` (`plan_replayer.go:520-539`) and hides the
//!   task;
//! - a row with a `fail_reason` does not satisfy that probe, so the task
//!   stays collectable.
//!
//! The capture side of `TestPlanReplayerHandleDumpTask` (the executor firing
//! `SendTask` when a query matching a registered task runs) lives in
//! `pkg/executor` and is not transcreated; the port drives Go's own
//! `SendTask` entry point (`plan_replayer.go:223`) instead, which is where
//! the test-observable removal of the task key happens.

#![cfg(test)]

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use chrono::Utc;

use tidb_domain::plan_replayer::{
    check_unhandled_replayer_task_sql, insert_plan_replayer_status, join_host_port, parse_time,
    DumpFileGcChecker, DumpFileStorage, GcStatusHook, InternalSqlExecutor, PlanReplayerDumpTask,
    PlanReplayerDumpTaskStatus, PlanReplayerDumper, PlanReplayerError, PlanReplayerHandle,
    PlanReplayerStatusRecord, PlanReplayerTaskCollectorHandle, PlanReplayerTaskDumpWorker,
    PlanReplayerTaskKey, RestrictedSqlExecutor, ServerInfo, ServerInfoSource,
    COLLECT_ALL_TASKS_SQL, DELETE_STATUS_BY_TOKEN_SQL, INSERT_SUCCESS_STATUS_SQL,
};

/// A shared model of `mysql.plan_replayer_task` (digest pairs) and
/// `mysql.plan_replayer_status` (success rows per sql_digest), plus a log of
/// every restricted statement.
#[derive(Default)]
struct TableState {
    /// Rows of `mysql.plan_replayer_task`, in insertion order.
    task_rows: RefCell<Vec<(String, String)>>,
    /// For `checkUnHandledReplayerTask`: the number of SUCCESS status rows
    /// (`fail_reason IS NULL`) per sql_digest. A Go status row carrying a
    /// `fail_reason` never satisfies the probe, so it never adds an entry.
    success_status_rows: RefCell<std::collections::HashMap<String, usize>>,
    /// `(sql, params)` of every `ExecRestrictedSQL` call, in order.
    statements: RefCell<Vec<(String, Vec<String>)>>,
}

#[derive(Default, Clone)]
struct MockExec {
    state: Rc<TableState>,
}

impl MockExec {
    fn record(&self, sql: &str, params: &[&str]) {
        self.state.statements.borrow_mut().push((
            sql.to_owned(),
            params.iter().map(|p| (*p).to_owned()).collect(),
        ));
    }
}

impl InternalSqlExecutor for MockExec {
    fn query_row_count(&self, sql: &str) -> Result<Option<usize>, PlanReplayerError> {
        // The probe statement itself is recorded so tests can assert which
        // keys were checked.
        self.record(sql, &[]);
        // Parse the probed sql_digest out of the statement the production
        // code builds (`check_unhandled_replayer_task_sql`).
        let digest = sql
            .split("sql_digest = '")
            .nth(1)
            .and_then(|rest| rest.split('\'').next())
            .unwrap_or_default();
        Ok(Some(
            self.state
                .success_status_rows
                .borrow()
                .get(digest)
                .copied()
                .unwrap_or(0),
        ))
    }

    fn query_digest_pairs(
        &self,
        sql: &str,
    ) -> Result<Option<Vec<(String, String)>>, PlanReplayerError> {
        assert_eq!(sql, COLLECT_ALL_TASKS_SQL);
        let rows = self.state.task_rows.borrow();
        if rows.is_empty() {
            // Go: an empty table still yields a non-nil, zero-row record set.
            return Ok(Some(Vec::new()));
        }
        Ok(Some(rows.clone()))
    }
}

impl RestrictedSqlExecutor for MockExec {
    fn exec_restricted_sql(&self, sql: &str, params: &[&str]) -> Result<(), PlanReplayerError> {
        self.record(sql, params);
        Ok(())
    }
}

/// A dumper that always succeeds and hands back Go's
/// `replayer_%v_%v.zip`-shaped file name (`pkg/util/replayer/replayer.go:100`).
#[derive(Default)]
struct MockDumper {
    dumped: RefCell<Vec<PlanReplayerDumpTask>>,
}

impl PlanReplayerDumper for MockDumper {
    fn generate_plan_replayer_file(
        &self,
        _is_capture: bool,
        _is_continues_capture: bool,
        _enable_historical_stats_for_capture: bool,
    ) -> Result<String, PlanReplayerError> {
        Ok(format!("replayer_key_{}.zip", Utc::now().timestamp_nanos_opt().unwrap()))
    }

    fn dump_plan_replayer_info(
        &self,
        task: &PlanReplayerDumpTask,
    ) -> Result<(), PlanReplayerError> {
        self.dumped.borrow_mut().push(task.clone());
        Ok(())
    }
}

struct MockInfo;

impl ServerInfoSource for MockInfo {
    fn get_server_info(&self) -> Result<ServerInfo, PlanReplayerError> {
        Ok(ServerInfo {
            ip: "127.0.0.1".to_owned(),
            port: 4000,
        })
    }
}

/// The Go test's registered task pair (`'123','123'`), as a key.
fn key_123() -> PlanReplayerTaskKey {
    PlanReplayerTaskKey::new("123", "123")
}

/// Go `pkg/domain/plan_replayer_handle_test.go:30::TestPlanReplayerHandleCollectTask`.
///
/// Four scenarios over the same two-table model, in the Go test's order:
/// one task with no status rows collects; an empty task table collects
/// nothing; a task with a SUCCESS status row (token, no fail_reason) is
/// hidden while its sibling collects; a task whose status row carries a
/// fail_reason stays collectable.
#[test]
fn plan_replayer_handle_collect_task() {
    // assert 1 task
    let exec = MockExec::default();
    *exec.state.task_rows.borrow_mut() = vec![("123".to_owned(), "123".to_owned())];
    let collector = PlanReplayerTaskCollectorHandle::new(exec.clone());
    collector.collect_plan_replayer_task().unwrap();
    assert_eq!(collector.get_tasks(), vec![key_123()]);

    // assert no task
    exec.state.task_rows.borrow_mut().clear();
    collector.collect_plan_replayer_task().unwrap();
    assert!(collector.get_tasks().is_empty());

    // assert 1 unhandled task: '123' has a SUCCESS status row (token='123').
    *exec.state.task_rows.borrow_mut() = vec![
        ("123".to_owned(), "123".to_owned()),
        ("345".to_owned(), "345".to_owned()),
    ];
    exec.state
        .success_status_rows
        .borrow_mut()
        .insert("123".to_owned(), 1);
    collector.collect_plan_replayer_task().unwrap();
    let tasks = collector.get_tasks();
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0], PlanReplayerTaskKey::new("345", "345"));

    // assert 2 unhandled tasks: '123' status row now carries fail_reason,
    // so the `fail_reason is null` probe stops seeing it.
    exec.state.success_status_rows.borrow_mut().clear();
    collector.collect_plan_replayer_task().unwrap();
    let tasks = collector.get_tasks();
    assert_eq!(tasks.len(), 2);
    assert!(tasks.contains(&key_123()));
    assert!(tasks.contains(&PlanReplayerTaskKey::new("345", "345")));

    // The probe that hid '123' in scenario 3 was the exact statement Go
    // builds with fmt.Sprintf (plan_replayer.go:523-525).
    assert!(exec
        .state
        .statements
        .borrow()
        .iter()
        .any(|(sql, _)| *sql == check_unhandled_replayer_task_sql(&key_123())));
}

/// Go `pkg/domain/plan_replayer_handle_test.go:71::TestPlanReplayerHandleDumpTask`.
///
/// The executor-side capture is not transcreated, so Go's `SendTask`
/// (`plan_replayer.go:223`) is driven directly — that is the entry point the
/// capture path calls, and where the non-continuous task key is removed.
/// Arm 1 (concrete plan digest): collect → capture sends and removes the
/// memory task → the worker dumps it → the running set drains → a re-collect
/// finds the SUCCESS status row the dump wrote and collects nothing.
/// Arm 2 (plan digest `*`): the task is a continuous capture, `SendTask`
/// does not remove it, and it is still in memory after a successful dump —
/// Go's "assert capture * task still remained".
#[test]
fn plan_replayer_handle_dump_task() {
    let exec = MockExec::default();
    let sql_digest = "sql-digest-1";
    let plan_digest = "plan-digest-1";

    // register task
    *exec.state.task_rows.borrow_mut() =
        vec![(sql_digest.to_owned(), plan_digest.to_owned())];
    let handle = PlanReplayerHandle::new(PlanReplayerTaskCollectorHandle::new(exec.clone()), 1);
    handle.collector.collect_plan_replayer_task().unwrap();
    assert_eq!(handle.collector.get_tasks().len(), 1);

    // capture task and dump: the executor's capture path calls SendTask.
    let task = PlanReplayerDumpTask {
        key: PlanReplayerTaskKey::new(sql_digest, plan_digest),
        is_capture: true,
        is_continues_capture: false,
        ..PlanReplayerDumpTask::default()
    };
    assert!(handle.send_task(task.clone()));
    // assert memory task consumed by the send (plan_replayer.go:231-234)
    assert!(handle.collector.get_tasks().is_empty());

    let drained = handle.dump_handle.drain_task().expect("task in channel");
    assert_eq!(drained.key, task.key);
    let status = handle.dump_handle.get_task_status();
    let dumper = MockDumper::default();
    let worker = PlanReplayerTaskDumpWorker::new(exec.clone(), dumper, Arc::clone(&status));
    let (check, occupy, success) = worker.handle_task(&drained);
    assert!(check && occupy && success);
    assert_eq!(status.running_task_status_len(), 0);
    assert!(handle.collector.get_tasks().is_empty());

    // The dump wrote a SUCCESS status row (TestInsertPlanReplayerStatus's
    // table): model it, and the re-collect must find nothing.
    exec.state
        .success_status_rows
        .borrow_mut()
        .insert(sql_digest.to_owned(), 1);
    handle.collector.collect_plan_replayer_task().unwrap();
    assert!(handle.collector.get_tasks().is_empty());

    // clean the task and register a `*` (continuous capture) task.
    status.clean_finished_task_status();
    exec.state.success_status_rows.borrow_mut().clear();
    *exec.state.task_rows.borrow_mut() = vec![(sql_digest.to_owned(), "*".to_owned())];
    handle.collector.collect_plan_replayer_task().unwrap();
    assert_eq!(handle.collector.get_tasks().len(), 1);

    let star_task = PlanReplayerDumpTask {
        key: PlanReplayerTaskKey::new(sql_digest, "*"),
        is_capture: true,
        is_continues_capture: true,
        ..PlanReplayerDumpTask::default()
    };
    assert!(handle.send_task(star_task.clone()));
    // A continuous capture task KEEPS its memory entry (the removal is
    // gated on `!task.IsContinuesCapture`, plan_replayer.go:231).
    assert_eq!(handle.collector.get_tasks(), vec![star_task.key.clone()]);

    let drained = handle.dump_handle.drain_task().expect("task in channel");
    let (check, occupy, success) = worker.handle_task(&drained);
    assert!(check && occupy && success);
    assert_eq!(status.running_task_status_len(), 0);
    // assert capture * task still remained
    assert_eq!(handle.collector.get_tasks(), vec![star_task.key]);
}

/// Go `pkg/domain/plan_replayer_handle_test.go:138::TestPlanReplayerGC`.
///
/// One dump file `replayer_single_xxxxxx_<UnixNano>.zip` created just before
/// the GC round, a status row keyed by that token, and a finished capture
/// task recorded. `GCDumpFiles(ctx, 0, 0)` must delete the file, delete the
/// status row by its token (Go asserts the table is empty afterwards), and
/// clear the finished-task set — the `sctx != nil` arm of
/// `gcDumpFilesByPath` (`plan_replayer.go:126-130`).
#[test]
fn plan_replayer_gc() {
    let exec = MockExec::default();
    let status = Arc::new(PlanReplayerDumpTaskStatus::default());
    // A finished continuous-capture task, to observe clearFinishedTask.
    let finished = PlanReplayerDumpTask {
        key: PlanReplayerTaskKey::new("123", "capture"),
        is_capture: true,
        is_continues_capture: true,
        ..PlanReplayerDumpTask::default()
    };
    status.set_task_finished(&finished);
    assert!(status.check_task_key_finished_before(&finished));

    let start_time = Utc::now();
    let file_name = format!(
        "replayer_single_xxxxxx_{}.zip",
        start_time.timestamp_nanos_opt().unwrap()
    );
    // Go: path = filepath.Join(replayer.GetPlanReplayerDirName(), fileName),
    // and GetPlanReplayerDirName() == "replayer" (replayer.go:34).
    let path = format!("replayer/{file_name}");
    let storage = MockStorage {
        files: RefCell::new(vec![path.clone()]),
    };

    let mut checker: DumpFileGcChecker<MockExec> =
        DumpFileGcChecker::new(chrono::Duration::zero(), vec!["replayer".to_owned()]);
    checker.setup_status_hook(GcStatusHook {
        sctx: exec.clone(),
        task_status: Arc::clone(&status),
    });

    let results = checker.gc_dump_files(&storage, start_time, chrono::Duration::zero(), chrono::Duration::zero());
    assert!(results.iter().all(|r| r.is_ok()), "{results:?}");

    // The file is gone (Go: storage.FileExists == false) ...
    assert!(storage.files.borrow().is_empty());
    // ... the status row was deleted by its token — the base name, not the
    // path (Go: `select count(*) from mysql.plan_replayer_status` == 0) ...
    let statements = exec.state.statements.borrow();
    let (sql, params) = statements
        .iter()
        .find(|(sql, _)| sql == DELETE_STATUS_BY_TOKEN_SQL)
        .expect("status delete issued");
    assert_eq!(*sql, DELETE_STATUS_BY_TOKEN_SQL);
    assert_eq!(params, &vec![file_name.clone()]);
    // ... and the finished-task set was cleared.
    assert!(!status.check_task_key_finished_before(&finished));
    // The deleted file's name carries a parseable timestamp, which is what
    // made it eligible (`parseTime`, plan_replayer.go:59).
    assert_eq!(parse_time(&file_name).unwrap(), start_time);
}

/// Go `pkg/domain/plan_replayer_handle_test.go:171::TestInsertPlanReplayerStatus`.
///
/// The Go test dumps a statement whose text contains a single quote (in
/// `SUBSTRING_INDEX(tableA.columnC, '_', 1)`) and then requires exactly one
/// `mysql.plan_replayer_status` row whose `origin_sql` is NOT NULL — "We
/// should store the origin sql correctly". The insert half of that flow is
/// `insertPlanReplayerStatus` (`plan_replayer.go:144`); the port runs it
/// with the Go test's record and requires one success-shaped INSERT whose
/// bound parameters carry the origin SQL verbatim (quote included) plus the
/// `instance` from `net.JoinHostPort` of the server info. Because the
/// statement is parameterized (`%?`), the quote needs no escaping — that is
/// the contract.
#[test]
fn insert_plan_replayer_status_stores_the_origin_sql_verbatim() {
    let exec = MockExec::default();
    let origin_sql = "\nSELECT * from tableA where SUBSTRING_INDEX(tableA.columnC, '_', 1) = tableA.columnA\n";
    let record = PlanReplayerStatusRecord {
        sql_digest: "sql-digest-1".to_owned(),
        plan_digest: "plan-digest-1".to_owned(),
        origin_sql: origin_sql.to_owned(),
        token: "replayer_key_1.zip".to_owned(),
        failed_reason: String::new(),
    };

    insert_plan_replayer_status(&exec, &MockInfo, &[record]);

    let statements = exec.state.statements.borrow();
    assert_eq!(statements.len(), 1, "exactly one insert: {statements:?}");
    let (sql, params) = &statements[0];
    assert_eq!(sql, INSERT_SUCCESS_STATUS_SQL);
    assert_eq!(
        params,
        &vec![
            "sql-digest-1".to_owned(),
            "plan-digest-1".to_owned(),
            origin_sql.to_owned(),
            "replayer_key_1.zip".to_owned(),
            // net.JoinHostPort("127.0.0.1", 4000)
            join_host_port("127.0.0.1", 4000),
        ]
    );
    assert!(params[2].contains('\''), "the single quote survives");
}

/// Scripted `DumpFileStorage` for [`plan_replayer_gc`].
struct MockStorage {
    files: RefCell<Vec<String>>,
}

impl DumpFileStorage for MockStorage {
    fn walk_dir(&self, sub_dir: &str) -> Result<Vec<String>, PlanReplayerError> {
        Ok(self
            .files
            .borrow()
            .iter()
            .filter(|f| f.starts_with(sub_dir))
            .cloned()
            .collect())
    }

    fn delete_file(&self, file_name: &str) -> Result<(), PlanReplayerError> {
        self.files.borrow_mut().retain(|f| f != file_name);
        Ok(())
    }
}
