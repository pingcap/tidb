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
