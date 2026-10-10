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

//! Go `pkg/util/expensivequery`: the server's watchdog over running
//! statements. Every 100ms it walks the process list, logs statements and
//! transactions that run past the expensive thresholds, and kills a SELECT
//! past its `max_execution_time`, an auto-analyze past
//! `tidb_max_auto_analyze_time`, and a statement a runaway rule ends.
//!
//! Go keeps `ExpensiveLogTime` and `ExpensiveTxnLogTime` on the shared
//! `ProcessInfo` pointer; the Rust process list hands out snapshots, so the
//! handle keeps those two log times itself, keyed the same way: per
//! statement (Go's `SetProcessInfo` starts each statement with a zero time)
//! and per transaction (it carries the time over while `CurTxnStartTS`
//! stays).

use std::collections::HashMap;
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tidb_log::{Field, Value};
use tidb_util::memoryusagealarm::{gen_log_fields, ProcessInfo, SessionManager};

/// Go `Handle`: the handler for expensive queries.
pub struct Handle {
    exit: Mutex<Receiver<()>>,
    sm: Mutex<Option<Arc<dyn SessionManager>>>,
}

/// The log times Go stores on `ProcessInfo`, per connection.
#[derive(Default)]
struct LogTimes {
    /// Go `ExpensiveLogTime`, with the statement start it belongs to.
    query: HashMap<u64, (Option<Instant>, Instant)>,
    /// Go `ExpensiveTxnLogTime`, with the transaction it belongs to.
    txn: HashMap<u64, (u64, Instant)>,
}

impl Handle {
    /// Go `NewExpensiveQueryHandle`.
    #[must_use]
    pub fn new(exit: Receiver<()>) -> Self {
        Self {
            exit: Mutex::new(exit),
            sm: Mutex::new(None),
        }
    }

    /// Go `Handle.SetSessionManager`.
    pub fn set_session_manager(&self, sm: Arc<dyn SessionManager>) -> &Self {
        *self
            .sm
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(sm);
        self
    }

    /// Go `Handle.Run`: checks the running statements every 100ms until the
    /// exit channel fires or closes.
    pub fn run(&self) {
        const TICK_INTERVAL: Duration = Duration::from_millis(100);
        let sm = self
            .sm
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            .expect("session manager must be set before the expensive-query handle runs");
        let exit = self
            .exit
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (mut threshold, mut txn_threshold) = thresholds();
        let mut last_metric_time: Option<Instant> = None;
        let mut log_times = LogTimes::default();
        let mut next_tick = Instant::now() + TICK_INTERVAL;
        loop {
            match exit.recv_timeout(next_tick.saturating_duration_since(Instant::now())) {
                Err(RecvTimeoutError::Timeout) => {}
                Ok(()) | Err(RecvTimeoutError::Disconnected) => return,
            }
            next_tick += TICK_INTERVAL;
            let now = Instant::now();
            // The metrics reporting interval is generally 15 seconds.
            let need_metrics = last_metric_time
                .is_none_or(|last| now.duration_since(last) > Duration::from_secs(15));
            if need_metrics {
                last_metric_time = Some(now);
            }
            check_processes(
                sm.as_ref(),
                threshold,
                txn_threshold,
                need_metrics,
                &mut log_times,
            );
            (threshold, txn_threshold) = thresholds();
        }
    }

    /// Go `Handle.LogOnQueryExceedMemQuota`: logs the statement of `conn_id`
    /// when it exceeds its memory quota.
    pub fn log_on_query_exceed_mem_quota(&self, conn_id: u64) {
        if tidb_util::logutil::get_level() > tidb_log::Level::Warn {
            return;
        }
        // The out-of-memory SQL may be internal SQL run while bootstrapping,
        // before the session manager is set.
        let sm = self
            .sm
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let Some(sm) = sm else {
            tidb_util::logutil::bg_logger().info(
                "expensive_query during bootstrap phase",
                &[Field::new("conn", Value::U64(conn_id))],
            );
            return;
        };
        let Some(info) = sm.get_process_info(conn_id) else {
            return;
        };
        log_expensive_query(since(&info), &info, "memory exceeds quota");
    }
}

/// Go `vardef.ExpensiveQueryTimeThreshold` and `ExpensiveTxnTimeThreshold`,
/// in seconds.
fn thresholds() -> (u64, u64) {
    let config = tidb_config::config_tree::config::get_global_config();
    (
        config.instance.expensive_query_time_threshold,
        config.instance.expensive_txn_time_threshold,
    )
}

/// How long ago `info`'s statement started (Go `time.Since(info.Time)`).
fn since(info: &ProcessInfo) -> Duration {
    match info.started_instant {
        Some(started) => started.elapsed(),
        None => (chrono::Utc::now() - info.time)
            .to_std()
            .unwrap_or_default(),
    }
}

/// One tick of Go `Handle.Run` over the process list.
fn check_processes(
    sm: &dyn SessionManager,
    threshold: u64,
    txn_threshold: u64,
    need_metrics: bool,
    log_times: &mut LogTimes,
) {
    let warn_enabled = tidb_util::logutil::get_level() <= tidb_log::Level::Warn;
    let processes = sm.show_process_list();
    log_times
        .query
        .retain(|id, _| processes.iter().any(|info| info.id == *id));
    log_times.txn.retain(|id, (start_ts, _)| {
        processes
            .iter()
            .any(|info| info.id == *id && info.cur_txn_start_ts == *start_ts)
    });
    for info in &processes {
        if info.cur_txn_start_ts != 0 {
            let txn_cost_time = info
                .cur_txn_create_time
                .map_or(Duration::ZERO, |created| created.elapsed());
            if txn_cost_time >= Duration::from_secs(txn_threshold) {
                if need_metrics {
                    let label = if info.in_restricted_sql {
                        "internal"
                    } else {
                        "general"
                    };
                    tidb_executor::metrics::ONGOING_TXN_DURATION
                        .with_label_values(&[label])
                        .observe(txn_cost_time.as_secs_f64());
                }
                let logged = log_times
                    .txn
                    .get(&info.id)
                    .filter(|(start_ts, _)| *start_ts == info.cur_txn_start_ts)
                    .map(|(_, at)| *at);
                if logged.is_none_or(|at| at.elapsed() > Duration::from_secs(10 * 60))
                    && warn_enabled
                {
                    log_expensive_query(txn_cost_time, info, "expensive_txn");
                    log_times
                        .txn
                        .insert(info.id, (info.cur_txn_start_ts, Instant::now()));
                }
            }
        }
        if info.info.is_empty() {
            continue;
        }
        let cost_time = since(info);
        let logged = log_times
            .query
            .get(&info.id)
            .filter(|(statement, _)| *statement == info.started_instant)
            .map(|(_, at)| *at);
        if logged.is_none_or(|at| at.elapsed() > Duration::from_secs(60))
            && cost_time >= Duration::from_secs(threshold)
            && warn_enabled
        {
            log_expensive_query(cost_time, info, "expensive_query");
            log_times
                .query
                .insert(info.id, (info.started_instant, Instant::now()));
        }
        if info.max_execution_time > 0 && cost_time > Duration::from_millis(info.max_execution_time)
        {
            tidb_util::logutil::bg_logger().warn(
                "execution timeout, kill it",
                &[
                    Field::new("costTime", duration_value(cost_time)),
                    Field::new(
                        "maxExecutionTime",
                        duration_value(Duration::from_millis(info.max_execution_time)),
                    ),
                    Field::new("processInfo", Value::Str(process_info_text(info))),
                ],
            );
            sm.kill(info.id, true, true, false);
        }
        if tidb_stats_handle_util::GLOBAL_AUTO_ANALYZE_PROCESS_LIST.contains(info.id) {
            let max_auto_analyze_time =
                tidb_vardef::MAX_AUTO_ANALYZE_TIME.load(std::sync::atomic::Ordering::SeqCst);
            if max_auto_analyze_time > 0
                && cost_time > Duration::from_secs(max_auto_analyze_time.unsigned_abs())
            {
                tidb_util::logutil::bg_logger().warn(
                    "auto analyze timeout, kill it",
                    &[
                        Field::new("costTime", duration_value(cost_time)),
                        Field::new(
                            "maxAutoAnalyzeTime",
                            duration_value(Duration::from_secs(
                                max_auto_analyze_time.unsigned_abs(),
                            )),
                        ),
                        Field::new("processInfo", Value::Str(process_info_text(info))),
                    ],
                );
                sm.kill(info.id, true, false, false);
            }
        }
        if let Some(rule_kill_action) = &info.runaway_rule_kill_action {
            if let Some(cause) = rule_kill_action() {
                tidb_util::logutil::bg_logger().warn(
                    "runaway query timeout",
                    &[
                        Field::new("costTime", duration_value(cost_time)),
                        Field::new("groupName", Value::Str(info.resource_group_name.clone())),
                        Field::new("exceedCause", Value::Str(cause)),
                        Field::new("processInfo", Value::Str(process_info_text(info))),
                    ],
                );
                sm.kill(info.id, true, false, true);
            }
        }
    }
}

/// Go `logExpensiveQuery`.
fn log_expensive_query(cost_time: Duration, info: &ProcessInfo, msg: &str) {
    let cost_time = chrono::Duration::from_std(cost_time).unwrap_or(chrono::Duration::MAX);
    tidb_util::logutil::bg_logger().warn(msg, &gen_log_fields(cost_time, info));
}

/// Go `zap.Duration`.
fn duration_value(duration: Duration) -> Value {
    Value::Duration(i64::try_from(duration.as_nanos()).unwrap_or(i64::MAX))
}

/// Go `ProcessInfo.String()` over `ToRowForShow(false)`: a NULL cell prints
/// `<nil>`, and the statement is cut to its first 100 characters. A watched
/// statement is always a running command, `Query`.
fn process_info_text(info: &ProcessInfo) -> String {
    let nil = || "<nil>".to_owned();
    let host = if info.port.is_empty() {
        info.host.clone()
    } else if info.host.contains(':') {
        format!("[{}]:{}", info.host, info.port)
    } else {
        format!("{}:{}", info.host, info.port)
    };
    format!(
        "{{id:{}, user:{}, host:{}, db:{}, command:Query, time:{}, state:{}, info:{}}}",
        info.id,
        info.user,
        host,
        if info.db.is_empty() {
            nil()
        } else {
            info.db.clone()
        },
        since(info).as_secs(),
        info.state,
        if info.info.is_empty() {
            nil()
        } else {
            crate::process::truncate_process_info(&info.info, false)
        }
    )
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use super::Handle;
    use crate::process::{ProcessKillTarget, ProcessRegistry};
    use crate::tests_support::row_text;
    use crate::Session;
    use tidb_util::sqlkiller::KillSignal;

    /// A connection's kill target holding its running command's
    /// cancellation, as the server's `ConnectionCancellation` does.
    #[derive(Default)]
    struct Command(Mutex<Option<tidb_executor::StatementCancellation>>);

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
}
