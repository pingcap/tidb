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

//! Go `pkg/util/expensivequery`'s handle over this package's sessions.

use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tidb_expensivequery::Handle;
use tidb_sessmgr::{ProcessKillTarget, ProcessRegistry};
use crate::tests_support::row_text;
use crate::Session;
use tidb_sqlkiller::KillSignal;

/// A connection's kill target holding its running command's
/// cancellation, as the server's `ConnectionCancellation` does.
#[derive(Default)]
struct Command(Mutex<Option<tidb_sqlkiller::StatementCancellation>>);

impl ProcessKillTarget for Command {
    fn cancel_query(&self) {
        self.cancel_query_with(KillSignal::QueryInterrupted);
    }

    fn kill_connection(&self) {
        self.cancel_query();
    }

    fn cancel_query_with(&self, signal: KillSignal) {
        if let Some(active) = self.0.lock().unwrap().as_ref() {
            active.cancel_with(signal);
        }
    }
}

/// One served connection with the watchdog running over it.
fn served_session() -> (
    Session,
    Arc<Command>,
    mpsc::Sender<()>,
    std::thread::JoinHandle<()>,
) {
    let registry = ProcessRegistry::default();
    let command = Arc::new(Command::default());
    let mut session = Session::new();
    let target: Arc<dyn ProcessKillTarget> = command.clone();
    session.attach_process(
        7,
        registry.register(7, "root".into(), "%".into(), "test".into(), Some(target)),
    );
    let (exit, receiver) = mpsc::channel();
    let handle = Handle::new(receiver);
    handle.set_session_manager(Arc::new(registry));
    let thread = std::thread::spawn(move || handle.run());
    (session, command, exit, thread)
}

fn run_command(session: &mut Session, command: &Command, sql: &str) -> Vec<Vec<String>> {
    *command.0.lock().unwrap() = Some(session.begin_query_cancellation());
    let rows = row_text(session.run(sql));
    *command.0.lock().unwrap() = None;
    rows
}

/// Go's watchdog kills a SELECT past its `max_execution_time`, and
/// `SLEEP` answers 1 for the interrupted wait (`executor/prepared`'s
/// TestPreparedStmtWithHint, issue 18535). Without the watchdog the
/// statement slept its full three seconds.
#[test]
fn max_execution_time_interrupts_a_running_select() {
    let (mut session, command, exit, thread) = served_session();
    let started = Instant::now();
    assert_eq!(
        run_command(
            &mut session,
            &command,
            "select /*+ max_execution_time(100) */ sleep(3)"
        ),
        [["1"]]
    );
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "{:?}",
        started.elapsed()
    );

    // The same limit from the session variable, and through a prepared
    // statement; a statement without a limit sleeps out its time.
    session
        .run("prepare stmt from 'select /*+ max_execution_time(100) */ sleep(3)'")
        .unwrap();
    let started = Instant::now();
    assert_eq!(run_command(&mut session, &command, "execute stmt"), [["1"]]);
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "{:?}",
        started.elapsed()
    );
    session.run("set max_execution_time = 100").unwrap();
    assert_eq!(
        run_command(&mut session, &command, "select sleep(3)"),
        [["1"]]
    );
    session.run("set max_execution_time = 0").unwrap();
    assert_eq!(
        run_command(&mut session, &command, "select sleep(0.3)"),
        [["0"]]
    );

    // A statement reading a table keeps the signal past `SLEEP` (Go
    // `doSleep` resets it only without table IDs) and fails with it.
    session.run("create table t (a int)").unwrap();
    session.run("insert into t values (1), (2), (3)").unwrap();
    *command.0.lock().unwrap() = Some(session.begin_query_cancellation());
    let started = Instant::now();
    let error = session
        .run("select /*+ max_execution_time(100) */ sleep(1) from t")
        .unwrap_err();
    *command.0.lock().unwrap() = None;
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(
        error.to_string(),
        "Query execution was interrupted, maximum statement execution time exceeded"
    );

    exit.send(()).unwrap();
    thread.join().unwrap();
}

/// Go `SetProcessInfo` publishes the statement's resource group, a
/// `resource_group()` hint included; Rust had published the group before
/// resolving the hint, so the process list lagged one statement
/// (`ddl/resource_group`).
#[test]
fn the_process_list_shows_the_statement_resource_group() {
    let (mut session, command, exit, thread) = served_session();
    session
        .run("set global tidb_enable_resource_control='on'")
        .unwrap();
    // As ddl/resource_group does: a hint naming a group that does not
    // exist falls back to the session's group.
    session
        .run("create resource group rg1 ru_per_sec=1000")
        .unwrap();
    assert_eq!(
        run_command(
            &mut session,
            &command,
            "select /*+ resource_group(rg1) */ RESOURCE_GROUP from information_schema.processlist"
        ),
        [["rg1"]]
    );
    assert_eq!(
        run_command(
            &mut session,
            &command,
            "select RESOURCE_GROUP from information_schema.processlist"
        ),
        [["default"]]
    );
    session
        .run("set global tidb_enable_resource_control=default")
        .unwrap();
    exit.send(()).unwrap();
    thread.join().unwrap();
}

/// Go `ShowProcess().MaxExecutionTime` after the last statement.
fn process_max_execution_time(session: &Session) -> u64 {
    session
        .process_plan_info
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .max_execution_time
}

/// Go `TestDMLMaxExecutionTime` (b82bed1eca2), its "configuration",
/// "prepared" and "excluded modes" cases: `tidb_dml_max_execution_time`
/// is the timeout of transactional DML and COMMIT, independent of the
/// SELECT-only `max_execution_time`, and statements that can commit
/// incrementally take none.
#[test]
fn test_dml_max_execution_time() {
    let run = |session: &mut Session, sql: &str| {
        if let Err(error) = session.run(sql) {
            panic!("{sql}: {error:?}");
        }
    };
    // configuration
    let mut tk = Session::new();
    run(&mut tk, "create table dml_timeout (id int primary key, v int)");
    assert_eq!(crate::tests_support::row_text(tk.run("select @@tidb_dml_max_execution_time")), [["0"]]);
    run(&mut tk, "set @@tidb_dml_max_execution_time = 60000");
    assert_eq!(tk.vars.dml_max_execution_time(), 60000);
    // The TiDB-specific DML timeout and MySQL-compatible SELECT timeout are
    // independent.
    run(&mut tk, "set @@max_execution_time = 30000");
    run(&mut tk, "select 1");
    assert_eq!(process_max_execution_time(&tk), 30000);
    run(&mut tk, "insert into dml_timeout values (1, 1)");
    assert_eq!(process_max_execution_time(&tk), 60000);
    // SET_VAR applies to the complete autocommit DML, then is restored.
    run(&mut tk, "insert /*+ set_var(tidb_dml_max_execution_time=90000) */ into dml_timeout values (2, 2)");
    assert_eq!(process_max_execution_time(&tk), 90000);
    assert!(crate::tests_support::row_text(tk.run("show warnings")).is_empty());
    run(&mut tk, "update dml_timeout set v = v + 1 where id = 1");
    assert_eq!(process_max_execution_time(&tk), 60000);
    // COMMIT has its own timeout budget.
    run(&mut tk, "begin");
    run(&mut tk, "delete from dml_timeout where id = 2");
    run(&mut tk, "commit");
    assert_eq!(process_max_execution_time(&tk), 60000);
    run(&mut tk, "set @@tidb_dml_max_execution_time = 0");
    run(&mut tk, "update dml_timeout set v = v + 1 where id = 1");
    assert_eq!(process_max_execution_time(&tk), 0);
    run(&mut tk, "set @@max_execution_time = 0");
    run(&mut tk, "set @@tidb_dml_max_execution_time = 60000");
    run(&mut tk, "select 1");
    assert_eq!(process_max_execution_time(&tk), 0);

    // prepared
    let mut tk = Session::new();
    run(&mut tk, "create table prepared_dml_timeout (id int primary key, v int)");
    run(&mut tk, "insert into prepared_dml_timeout values (1, 1)");
    run(&mut tk, "set @@max_execution_time = 30000");
    run(&mut tk, "set @@tidb_dml_max_execution_time = 60000");
    run(&mut tk, "prepare prepared_select from 'select 1'");
    run(&mut tk, "execute prepared_select");
    assert_eq!(process_max_execution_time(&tk), 30000);
    run(&mut tk, "deallocate prepare prepared_select");
    run(&mut tk, "prepare prepared_insert from 'insert into prepared_dml_timeout values (7, 7)'");
    run(&mut tk, "execute prepared_insert");
    assert_eq!(process_max_execution_time(&tk), 60000);
    run(&mut tk, "deallocate prepare prepared_insert");
    run(&mut tk, "prepare prepared_point_get from 'select * from prepared_dml_timeout where id = 1'");
    for _ in 0..2 {
        assert_eq!(crate::tests_support::row_text(tk.run("execute prepared_point_get")), [["1", "1"]]);
        assert_eq!(process_max_execution_time(&tk), 30000);
    }
    run(&mut tk, "deallocate prepare prepared_point_get");
    run(&mut tk, "prepare prepared_commit from 'commit'");
    run(&mut tk, "begin");
    run(&mut tk, "insert into prepared_dml_timeout values (3, 3)");
    run(&mut tk, "execute prepared_commit");
    assert_eq!(process_max_execution_time(&tk), 60000);
    run(&mut tk, "deallocate prepare prepared_commit");

    // excluded modes
    let mut tk = Session::new();
    run(&mut tk, "create table excluded_dml_timeout (id int primary key, v int)");
    run(&mut tk, "set @@max_execution_time = 30000");
    run(&mut tk, "set @@tidb_dml_max_execution_time = 60000");
    // EXPLAIN ANALYZE DML commits before returning a result set.
    run(&mut tk, "explain analyze insert into excluded_dml_timeout values (4, 4)");
    assert_eq!(process_max_execution_time(&tk), 0);
    // Deprecated batch DML can commit earlier batches.
    let original_enable_batch_dml = tidb_vardef::ENABLE_BATCH_DML.swap(true, std::sync::atomic::Ordering::Relaxed);
    run(&mut tk, "set tidb_batch_insert = ON");
    run(&mut tk, "set tidb_dml_batch_size = 1");
    run(&mut tk, "insert into excluded_dml_timeout values (5, 5), (6, 6)");
    assert_eq!(process_max_execution_time(&tk), 0);
    // Batch insert is disabled inside an explicit transaction.
    run(&mut tk, "begin");
    run(&mut tk, "insert into excluded_dml_timeout values (9, 9)");
    assert_eq!(process_max_execution_time(&tk), 60000);
    run(&mut tk, "rollback");
    tidb_vardef::ENABLE_BATCH_DML.store(original_enable_batch_dml, std::sync::atomic::Ordering::Relaxed);
    run(&mut tk, "set tidb_batch_insert = OFF");
    run(&mut tk, "set tidb_dml_batch_size = 0");
    run(&mut tk, "set tidb_batch_commit = ON");
    run(&mut tk, "begin");
    run(&mut tk, "insert into excluded_dml_timeout values (8, 8)");
    assert_eq!(process_max_execution_time(&tk), 0);
    run(&mut tk, "commit");
    assert_eq!(process_max_execution_time(&tk), 0);
}

/// The watchdog half of Go `TestDMLMaxExecutionTime` / `TestConnDML...`
/// (b82bed1eca2): a transactional DML past `tidb_dml_max_execution_time`
/// is killed with `ErrMaxExecTimeExceeded` and its rows do not land, while
/// `max_execution_time` alone does not limit it.
#[test]
fn dml_max_execution_time_interrupts_a_running_dml() {
    let (mut session, command, exit, thread) = served_session();
    session.run("create table t (a int)").unwrap();
    session.run("create table dst (a int)").unwrap();
    session.run("insert into t values (1), (2), (3)").unwrap();
    session.run("set tidb_dml_max_execution_time = 100").unwrap();
    *command.0.lock().unwrap() = Some(session.begin_query_cancellation());
    let started = Instant::now();
    let error = session.run("insert into dst select sleep(1) from t").unwrap_err();
    *command.0.lock().unwrap() = None;
    assert!(started.elapsed() < Duration::from_secs(2), "{:?}", started.elapsed());
    assert_eq!(
        error.to_string(),
        "Query execution was interrupted, maximum statement execution time exceeded"
    );
    assert_eq!(row_text(session.run("select count(*) from dst")), [["0"]]);

    // max_execution_time is SELECT-only: the same DML runs to its end.
    session.run("set tidb_dml_max_execution_time = 0").unwrap();
    session.run("set max_execution_time = 100").unwrap();
    *command.0.lock().unwrap() = Some(session.begin_query_cancellation());
    session.run("insert into dst select sleep(0.3) from t").unwrap();
    *command.0.lock().unwrap() = None;
    assert_eq!(row_text(session.run("select count(*) from dst")), [["3"]]);

    exit.send(()).unwrap();
    thread.join().unwrap();
}

/// Go `newLockCtx`'s deadline from the statement's timeout (b82bed1eca2,
/// `getMaxExecutionDeadline`): a pessimistic DML waiting on a lock gives up
/// at `tidb_dml_max_execution_time`, not at `innodb_lock_wait_timeout`.
#[test]
fn dml_max_execution_time_bounds_a_lock_wait() {
    let mut holder = Session::new();
    holder.run("create table locked (id int primary key, v int)").unwrap();
    holder.run("insert into locked values (1, 1)").unwrap();
    let domain = crate::domain::mock_store::domain_of(&holder).expect("a mock store session");
    let mut waiter = domain.new_session().expect("a second session");
    waiter.run("use test").unwrap();
    holder.run("begin pessimistic").unwrap();
    holder.run("select * from locked where id = 1 for update").unwrap();
    waiter.run("set innodb_lock_wait_timeout = 10").unwrap();
    waiter.run("set tidb_dml_max_execution_time = 200").unwrap();
    waiter.run("begin pessimistic").unwrap();
    let started = Instant::now();
    let error = waiter.run("update locked set v = 2 where id = 1").unwrap_err().to_mysql_error();
    assert!(started.elapsed() < Duration::from_secs(5), "{:?}", started.elapsed());
    assert_eq!(error.code, 3024, "{}", error.message);
    waiter.run("rollback").unwrap();
    holder.run("rollback").unwrap();
}

/// Go `TestNonTransactionalDmlIgnoreMaxExecutionTime` (with b82bed1eca2's
/// rows): a non-transactional DML and its split statements run without
/// either timeout; the next ordinary DML has its DML budget again.
#[test]
fn test_non_transactional_dml_ignore_max_execution_time() {
    let mut tk = Session::new();
    for sql in [
        "set @@tidb_max_chunk_size=10",
        "set @@max_execution_time=1000",
        "set @@tidb_dml_max_execution_time=1000",
        "create table t(a int, b int, key(a))",
    ] {
        tk.run(sql).unwrap();
    }
    for i in 0..100 {
        tk.run(&format!("insert into t values ({i}, {})", i * 2)).unwrap();
    }
    tk.run("batch on a limit 10 update t set b = b + 1 where b > 0").unwrap();
    assert_eq!(process_max_execution_time(&tk), 0);
    assert_eq!(tk.vars.dml_max_execution_time(), 1000);
    tk.run("update t set b = b + 1 where a = 0").unwrap();
    assert_eq!(process_max_execution_time(&tk), 1000);
}
