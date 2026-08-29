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

//! Server-level memory-limit controller and its last-50 operation history.

use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use tidb_datatype::{core_time_from_datetime, Datum, Time, TimeType};
use tidb_log::{Field, Value};

use crate::logutil::bg_logger;
use crate::memory::Tracker;
use crate::memoryusagealarm::{ProcessInfo, SessionManager};
use crate::sqlkiller::KillSignal;

// Process global Observation indicators for memory limit (Go package vars).

/// Go `MemoryMaxUsed`.
pub static MEMORY_MAX_USED: AtomicU64 = AtomicU64::new(0);
/// Go `SessionKillLast` (`atomicutil.Time`; `None` is Go's zero time).
pub static SESSION_KILL_LAST: Mutex<Option<DateTime<Utc>>> = Mutex::new(None);
/// Go `SessionKillTotal`.
pub static SESSION_KILL_TOTAL: AtomicI64 = AtomicI64::new(0);
/// Go `IsKilling`.
pub static IS_KILLING: AtomicBool = AtomicBool::new(false);
/// Go `GlobalMemoryOpsHistoryManager`.
pub static GLOBAL_MEMORY_OPS_HISTORY_MANAGER: MemoryOpsHistoryManager =
    MemoryOpsHistoryManager::new();

/// Go `sessionToBeKilled`: the cross-tick state of one in-flight kill.
#[derive(Default)]
struct SessionToBeKilled {
    is_killing: bool,
    sql_start_time: Option<DateTime<Utc>>,
    session_id: u64,
    session_tracker: Option<Arc<Tracker>>,

    kill_start_time: Option<DateTime<Utc>>,
    last_log_time: Option<DateTime<Utc>>,
}

impl SessionToBeKilled {
    /// Go `sessionToBeKilled.reset`.
    fn reset(&mut self) {
        self.is_killing = false;
        self.sql_start_time = None;
        self.session_id = 0;
        self.session_tracker = None;
        self.kill_start_time = None;
        self.last_log_time = None;
    }
}

/// Handler for the server memory limit.
pub struct Handle {
    exit: mpsc::Receiver<()>,
    session_manager: Option<Arc<dyn SessionManager>>,
}

/// Builds a new server memory limit handler.
#[must_use]
pub fn new_server_memory_limit_handle(exit: mpsc::Receiver<()>) -> Handle {
    Handle {
        exit,
        session_manager: None,
    }
}

impl Handle {
    /// Sets the manager used to fetch all active sessions.
    pub fn set_session_manager(&mut self, manager: Arc<dyn SessionManager>) -> &mut Self {
        self.session_manager = Some(manager);
        self
    }

    /// Runs the server memory checker until the server exit signal is set.
    pub fn run(self) {
        let manager = self
            .session_manager
            .expect("session manager must be set before Handle::run");
        let mut session_to_be_killed = SessionToBeKilled::default();
        loop {
            match self.exit.recv_timeout(Duration::from_millis(100)) {
                Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => return,
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    crate::memory::handle_global_mem_arbitrator_runtime();
                    kill_sess_if_needed(
                        &mut session_to_be_killed,
                        crate::memory::SERVER_MEMORY_LIMIT.load(Ordering::SeqCst),
                        manager.as_ref(),
                    );
                }
            }
        }
    }
}

/// Go `%.100v` / `%.256v`: truncate a string to at most `n` characters.
fn truncate_chars(s: &str, n: usize) -> &str {
    match s.char_indices().nth(n) {
        Some((idx, _)) => &s[..idx],
        None => s,
    }
}

fn kill_sess_if_needed(s: &mut SessionToBeKilled, bt: u64, sm: &dyn SessionManager) {
    let now = Utc::now();
    if s.is_killing {
        'check: {
            if let Some(info) = sm.get_process_info(s.session_id) {
                if Some(info.time) == s.sql_start_time {
                    let last_log = s
                        .last_log_time
                        .unwrap_or(super::memoryusagealarm::ZERO_TIME);
                    if now - last_log > chrono::Duration::seconds(5) {
                        let kill_start = s
                            .kill_start_time
                            .unwrap_or(super::memoryusagealarm::ZERO_TIME);
                        let seconds = (now - kill_start).num_seconds();
                        bg_logger().warn(
                            &format!(
                                "global memory controller failed to kill the top-consumer in {seconds}s"
                            ),
                            &[
                                Field::new("conn", Value::U64(info.id)),
                                Field::new("sql digest", Value::Str(info.digest.clone())),
                                Field::new(
                                    "sql text",
                                    Value::Str(truncate_chars(&info.info, 100).to_owned()),
                                ),
                                Field::new(
                                    "sql memory usage",
                                    Value::I64(
                                        info.mem_tracker
                                            .as_ref()
                                            .map_or(0, |t| t.bytes_consumed()),
                                    ),
                                ),
                            ],
                        );
                        s.last_log_time = Some(now);

                        if seconds >= 60 {
                            // If the SQL cannot be terminated after 60
                            // seconds, it may be stuck in the network stack
                            // while writing packets to the client,
                            // encountering some bugs that cause it to hang,
                            // or failing to detect the kill signal. In this
                            // case, the resources can be reclaimed by
                            // calling the `Finish` method, and then we can
                            // start looking for the next SQL with the
                            // largest memory usage.
                            bg_logger().warn(
                                &format!(
                                    "global memory controller failed to kill the top-consumer in {seconds} seconds. Attempting to force close the executors."
                                ),
                                &[],
                            );
                            if let Some(tracker) = &s.session_tracker {
                                tracker.killer.finish_result_set();
                            }
                            break 'check; // Go `goto Succ`
                        }
                    }
                    return;
                }
            }
        }
        // Go label `Succ`. Note the source resets first, so the
        // CompareAndSwap below compares the top-1 slot against nil — kept
        // faithful.
        s.reset();
        IS_KILLING.store(false, Ordering::SeqCst);
        crate::memory::MEM_USAGE_TOP1_TRACKER.compare_and_swap(s.session_tracker.as_ref(), None);
        bg_logger().warn(
            "global memory controller killed the top1 memory consumer successfully",
            &[],
        );
    }

    if bt == 0 {
        return;
    }

    let instance_stats = crate::memory::read_mem_stats();
    let heap_inuse = u64::try_from(instance_stats.heap_inuse).unwrap_or(0);
    if heap_inuse > MEMORY_MAX_USED.load(Ordering::SeqCst) {
        MEMORY_MAX_USED.store(heap_inuse, Ordering::SeqCst);
    }

    if crate::memory::using_global_mem_arbitration() {
        return;
    }

    let limit_sess_min_size =
        crate::memory::SERVER_MEMORY_LIMIT_SESS_MIN_SIZE.load(Ordering::SeqCst);
    if heap_inuse > bt {
        let mut t = crate::memory::MEM_USAGE_TOP1_TRACKER.load();
        if let Some(tracker) = t.clone() {
            let session_id = tracker.session_id();
            let mem_usage = tracker.bytes_consumed();
            // If the memory usage of the top1 session is less than
            // tidb_server_memory_limit_sess_min_size, we do not need to kill
            // it.
            if (mem_usage as u64) < limit_sess_min_size {
                crate::memory::MEM_USAGE_TOP1_TRACKER.compare_and_swap(Some(&tracker), None);
                t = None;
            } else if let Some(info) = sm.get_process_info(session_id) {
                bg_logger().warn(
                    "global memory controller tries to kill the top1 memory consumer",
                    &[
                        Field::new("conn", Value::U64(info.id)),
                        Field::new("sql digest", Value::Str(info.digest.clone())),
                        Field::new(
                            "sql text",
                            Value::Str(truncate_chars(&info.info, 100).to_owned()),
                        ),
                        Field::new("tidb_server_memory_limit", Value::U64(bt)),
                        Field::new("heap inuse", Value::U64(heap_inuse)),
                        Field::new(
                            "sql memory usage",
                            Value::I64(info.mem_tracker.as_ref().map_or(0, |t| t.bytes_consumed())),
                        ),
                    ],
                );
                s.session_id = session_id;
                s.sql_start_time = Some(info.time);
                s.is_killing = true;
                s.session_tracker = Some(Arc::clone(&tracker));
                tracker
                    .killer
                    .send_kill_signal(KillSignal::ServerMemoryExceeded);

                let kill_time = now;
                SESSION_KILL_TOTAL.fetch_add(1, Ordering::SeqCst);
                *SESSION_KILL_LAST.lock().unwrap_or_else(|e| e.into_inner()) = Some(kill_time);
                IS_KILLING.store(true, Ordering::SeqCst);
                GLOBAL_MEMORY_OPS_HISTORY_MANAGER.record_one(&info, kill_time, bt, heap_inuse);
                s.last_log_time = Some(now);
                s.kill_start_time = Some(now);
            }
        }
        // If no one larger than tidb_server_memory_limit_sess_min_size is
        // found, we will not kill any one.
        if t.is_none() {
            if s.last_log_time.is_none() {
                s.last_log_time = Some(now);
            }
            if now - s.last_log_time.unwrap_or(now) < chrono::Duration::seconds(5) {
                return;
            }
            bg_logger().warn(
                "global memory controller tries to kill the top1 memory consumer, but no one larger than tidb_server_memory_limit_sess_min_size is found",
                &[Field::new(
                    "tidb_server_memory_limit_sess_min_size",
                    Value::U64(limit_sess_min_size),
                )],
            );
            s.last_log_time = Some(now);
        }
    }
}

const HISTORY_CAP: usize = 50;

/// One kill's remembered row, Go `memoryOpsHistory`.
struct MemoryOpsHistory {
    kill_time: DateTime<Utc>,
    memory_limit: u64,
    memory_current: u64,
    /// id,user,host,db,command,time,state,info,digest,mem,... in Go
    /// `ProcessInfo.ToRow` order.
    process_info_datum: Vec<Datum>,
}

struct MemoryOpsState {
    infos: [Option<MemoryOpsHistory>; HISTORY_CAP],
    offsets: usize,
}

/// Go `memoryOpsHistoryManager`: the last-50 kill history ring.
pub struct MemoryOpsHistoryManager {
    state: Mutex<MemoryOpsState>,
}

impl MemoryOpsHistoryManager {
    const fn new() -> MemoryOpsHistoryManager {
        MemoryOpsHistoryManager {
            state: Mutex::new(MemoryOpsState {
                infos: [const { None }; HISTORY_CAP],
                offsets: 0,
            }),
        }
    }

    fn record_one(
        &self,
        info: &ProcessInfo,
        kill_time: DateTime<Utc>,
        memory_limit: u64,
        memory_current: u64,
    ) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let op = MemoryOpsHistory {
            kill_time,
            memory_limit,
            memory_current,
            process_info_datum: process_info_to_row(info, kill_time),
        };
        // Go takes a value copy of the SQL-text datum before applying the
        // `%.256v` truncation, so the truncation never reaches the stored
        // row — kept faithful by not truncating here.
        //
        // Only record the last 50 history ops.
        let offsets = state.offsets;
        state.infos[offsets] = Some(op);
        state.offsets += 1;
        if state.offsets >= HISTORY_CAP {
            state.offsets = 0;
        }
    }

    /// Go `memoryOpsHistoryManager.GetRows`: TIME, OPS, MEMORY_LIMIT,
    /// MEMORY_CURRENT, PROCESSID, MEM, DISK, CLIENT, DB, USER, SQL_DIGEST,
    /// SQL_TEXT.
    pub fn get_rows(&self) -> Vec<Vec<Datum>> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let mut rows = Vec::with_capacity(state.infos.len());
        for i in 0..state.infos.len() {
            let pos = (state.offsets + i) % state.infos.len();
            let Some(info) = &state.infos[pos] else {
                // Go skips zero `killTime` entries (never-written slots).
                continue;
            };
            let kill_time = Time::new(
                core_time_from_datetime(info.kill_time),
                TimeType::DateTime,
                0,
            )
            .expect("fsp 0 is always valid");
            rows.push(vec![
                Datum::new_time(kill_time),           // TIME
                Datum::new_string("SessionKill"),     // OPS
                Datum::new_uint(info.memory_limit),   // MEMORY_LIMIT
                Datum::new_uint(info.memory_current), // MEMORY_CURRENT
                info.process_info_datum[0].clone(),   // PROCESSID
                info.process_info_datum[9].clone(),   // MEM
                info.process_info_datum[13].clone(),  // DISK
                info.process_info_datum[2].clone(),   // CLIENT
                info.process_info_datum[3].clone(),   // DB
                info.process_info_datum[1].clone(),   // USER
                info.process_info_datum[8].clone(),   // SQL_DIGEST
                info.process_info_datum[7].clone(),   // SQL_TEXT
            ]);
        }
        rows
    }

    #[cfg(test)]
    fn offsets(&self) -> usize {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).offsets
    }
}

/// Go `net.JoinHostPort`.
fn join_host_port(host: &str, port: &str) -> String {
    if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

/// Go `ProcessInfo.ToRow(tz)` (via `ToRowForShow(true)`) over the narrowed
/// snapshot, in the source's 20-column order so the history indices match.
///
/// boundary: command (`mysql.Command2Str`), state (`serverStatus2Str`), the
/// `StmtCtx` memory-arbitration columns, and `ppcpuusage.CPUUsages` are not
/// modeled by the snapshot; command/state land as NULL and the CPU times as
/// zero.
fn process_info_to_row(info: &ProcessInfo, now: DateTime<Utc>) -> Vec<Datum> {
    let info_datum = if info.info.is_empty() {
        Datum::Null
    } else {
        Datum::new_string(info.info.as_bytes())
    };
    let elapsed = (now - info.time).num_seconds().max(0) as u64;
    let db = if info.db.is_empty() {
        Datum::Null
    } else {
        Datum::new_string(info.db.as_bytes())
    };
    let host = if info.port.is_empty() {
        info.host.clone()
    } else {
        join_host_port(&info.host, &info.port)
    };
    let bytes_consumed = info.mem_tracker.as_ref().map_or(0, |t| t.bytes_consumed());
    let disk_consumed = info.disk_tracker.as_ref().map_or(0, |t| t.bytes_consumed());
    let txn_start = if info.cur_txn_start_ts > 0 {
        // Go `oracle.GetTimeFromTS`: physical milliseconds in the high bits.
        let physical_ms = (info.cur_txn_start_ts >> 18) as i64;
        let physical = DateTime::<Utc>::from_timestamp_millis(physical_ms)
            .unwrap_or(super::memoryusagealarm::ZERO_TIME);
        format!(
            "{}({})",
            physical.format("%m-%d %H:%M:%S%.3f"),
            info.cur_txn_start_ts
        )
    } else {
        String::new()
    };
    vec![
        Datum::new_uint(info.id),                               // 0 id
        Datum::new_string(info.user.as_bytes()),                // 1 user
        Datum::new_string(host.as_bytes()),                     // 2 host
        db,                                                     // 3 db
        Datum::Null,                                            // 4 command (boundary)
        Datum::new_uint(elapsed),                               // 5 time
        Datum::Null,                                            // 6 state (boundary)
        info_datum,                                             // 7 info
        Datum::new_string(info.digest.as_bytes()),              // 8 digest
        Datum::new_int(bytes_consumed),                         // 9 mem
        Datum::Null,                                            // 10 mem arbitration (boundary)
        Datum::Null,                             // 11 wait arbitrate start (boundary)
        Datum::Null,                             // 12 wait arbitrate bytes (boundary)
        Datum::new_int(disk_consumed),           // 13 disk
        Datum::new_string(txn_start.as_bytes()), // 14 txn start
        Datum::new_string(info.resource_group_name.as_bytes()), // 15 resource group
        Datum::new_string(info.session_alias.as_bytes()), // 16 session alias
        Datum::new_uint(info.affected_rows),     // 17 affected rows
        Datum::new_int(0),                       // 18 tidb cpu (boundary)
        Datum::new_int(0),                       // 19 tikv cpu (boundary)
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn datum_str(d: &Datum) -> String {
        match d {
            Datum::String(s) => s.as_utf8().expect("utf8").to_owned(),
            other => panic!("expected string datum, got {other:?}"),
        }
    }

    fn datum_i64(d: &Datum) -> i64 {
        // Go `Datum.GetInt64` reads the raw integer word for both int and
        // uint kinds.
        match d {
            Datum::Int(v) => *v,
            Datum::UInt(v) => *v as i64,
            other => panic!("expected integer datum, got {other:?}"),
        }
    }

    /// Go test `TestMemoryUsageOpsHistory`.
    #[test]
    fn test_memory_usage_ops_history() {
        let gen_info = |i: i64| ProcessInfo {
            id: i as u64,
            db: (2 * i).to_string(),
            user: (3 * i).to_string(),
            host: (4 * i).to_string(),
            digest: (5 * i).to_string(),
            info: (6 * i).to_string(),
            ..ProcessInfo::default()
        };

        for i in 0..3i64 {
            let info = gen_info(i);
            GLOBAL_MEMORY_OPS_HISTORY_MANAGER.record_one(&info, Utc::now(), i as u64, 2 * i as u64);
        }

        let check_result = |datums: &[Datum], i: i64| {
            assert_eq!(datum_str(&datums[1]), "SessionKill");
            assert_eq!(datum_i64(&datums[2]), i);
            assert_eq!(datum_i64(&datums[3]), 2 * i);
            assert_eq!(datum_i64(&datums[4]), i);
            assert_eq!(datum_str(&datums[7]), (4 * i).to_string());
            assert_eq!(datum_str(&datums[8]), (2 * i).to_string());
            assert_eq!(datum_str(&datums[9]), (3 * i).to_string());
            assert_eq!(datum_str(&datums[10]), (5 * i).to_string());
            assert_eq!(datum_str(&datums[11]), (6 * i).to_string());
        };

        let rows = GLOBAL_MEMORY_OPS_HISTORY_MANAGER.get_rows();
        assert_eq!(3, rows.len());
        for i in 0..3i64 {
            check_result(&rows[i as usize], i);
        }
        // Test evict
        for i in 3..53i64 {
            let info = gen_info(i);
            GLOBAL_MEMORY_OPS_HISTORY_MANAGER.record_one(&info, Utc::now(), i as u64, 2 * i as u64);
        }
        let rows = GLOBAL_MEMORY_OPS_HISTORY_MANAGER.get_rows();
        assert_eq!(50, rows.len());
        for i in 3..53i64 {
            check_result(&rows[(i - 3) as usize], i);
        }
        assert_eq!(GLOBAL_MEMORY_OPS_HISTORY_MANAGER.offsets(), 3);
    }
}
