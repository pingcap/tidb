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

//! SEED of Go `pkg/util/memoryusagealarm`: the OOM-risk monitor that records
//! running SQL and profiles when memory usage crosses the alarm ratio.
//!
//! Narrowings (each named against the Go source):
//! - `Handle.Run`'s 100ms ticker goroutine and `exitCh` become the explicit
//!   [`Handle::tick`] step, which tests (and a server loop) drive directly.
//! - Ambient clock reads (`time.Now` / `time.Since` in `updateVariable`,
//!   `alarm4ExcessiveMemUsage`, and `needRecord`) become passed-in `now`
//!   timestamps.
//! - Ambient memory/runtime reads (`memory.ServerMemoryLimit`,
//!   `memory.MemTotal`, `memory.MemUsed`, `memory.ReadMemStats`,
//!   `memory.UsingGlobalMemArbitration`) and the `vardef.OOMAction` session
//!   variable are narrowed behind [`MemoryStateProvider`] snapshot inputs.
//! - `recordProfile`/`write` (runtime/pprof heap profile) and
//!   `recordGoroutineProfile` (`runtime.Stack` all-goroutine dump) are Go
//!   runtime facilities with no Rust equivalent; the dump side effect is
//!   isolated behind the injected [`ProfileRecorder`] trait and the
//!   `running_sql` record stays pure text built by `getTop10SqlInfo` ports.
//! - `TiDBConfigProvider` (reads `vardef.MemoryUsageAlarmRatio`,
//!   `vardef.MemoryUsageAlarmKeepRecordNum`, and `config.GetGlobalConfig`)
//!   stays in the server layer; only the [`ConfigProvider`] seam lands here.
//! - `util.GenLogFields` (Go `pkg/util`) is transcribed here as
//!   [`gen_log_fields`] over the narrowed [`ProcessInfo`] snapshot; the
//!   `ExecDetails`/`CopTasksDetails` zap fields, `mem_arbitration` fields,
//!   the `RefCountOfStmtCtx.TryIncrease` guard, and `parser.Normalize` SQL
//!   redaction are not modeled by the snapshot and are dropped.
//! - Go tests `TestRecordGoroutineProfileWithBackgroundGoroutine` and
//!   `BenchmarkRecordGoroutineProfile` assert on Go-runtime stack-dump text
//!   (`"goroutine "`, `"created by"`) and are skipped with the
//!   `recordGoroutineProfile` boundary above.
//!
//! This module also hosts the one shared narrow seam for Go
//! `pkg/session/sessmgr`: [`SessionManager`] and the [`ProcessInfo`]
//! snapshot, reused by `crate::servermemorylimit`.

use std::collections::BTreeMap;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::{DateTime, Duration as ChronoDuration, SecondsFormat, Utc};
use tidb_datatype::EXPLAIN_FORMAT_ROW;
use tidb_log::{Field, Value};

use crate::logutil::bg_logger;
use crate::memory::{format_bytes, Tracker};
use crate::plancodec::decode_binary_plan_for_connection;

/// Go zero `time.Time` stand-in for the narrowed chrono timestamps.
pub(crate) const ZERO_TIME: DateTime<Utc> = DateTime::<Utc>::UNIX_EPOCH;

// boundary: Go `sessmgr.OOMAlarmVariablesInfo`.
/// OOM-alarm-relevant session variables captured on a process entry.
#[derive(Clone, Copy, Debug, Default)]
pub struct OOMAlarmVariablesInfo {
    /// Go `SessionAnalyzeVersion`.
    pub session_analyze_version: i64,
    /// Go `SessionEnabledRateLimitAction`.
    pub session_enabled_rate_limit_action: bool,
    /// Go `SessionMemQuotaQuery`.
    pub session_mem_quota_query: i64,
}

// boundary: Go `sessmgr.ProcessInfo`, narrowed to the fields
// `pkg/util/memoryusagealarm` and `pkg/util/servermemorylimit` read. Not
// modeled: `StmtCtx` (only its `AffectedRows()` survives as a plain field),
// `RefCountOfStmtCtx`, `SQLCPUUsage`, `Plan`, command/state enums.
/// Snapshot of one live connection, the shared seam over Go
/// `sessmgr.ProcessInfo`.
#[derive(Clone)]
pub struct ProcessInfo {
    /// Go `ID`.
    pub id: u64,
    /// Go `User`.
    pub user: String,
    /// Go `Host`.
    pub host: String,
    /// Go `Port`.
    pub port: String,
    /// Go `DB`.
    pub db: String,
    /// Go `Digest`.
    pub digest: String,
    /// Go `Info` (the SQL text).
    pub info: String,
    /// Go `Time` (statement start time).
    pub time: DateTime<Utc>,
    /// Go `MemTracker`.
    pub mem_tracker: Option<Arc<Tracker>>,
    /// Go `DiskTracker`.
    pub disk_tracker: Option<Arc<Tracker>>,
    /// Go `CurTxnStartTS`.
    pub cur_txn_start_ts: u64,
    /// Go `ResourceGroupName`.
    pub resource_group_name: String,
    /// Go `SessionAlias`.
    pub session_alias: String,
    /// Go `BriefBinaryPlan`.
    pub brief_binary_plan: String,
    /// Go `TableIDs`.
    pub table_ids: Vec<i64>,
    /// Go `IndexNames`.
    pub index_names: Vec<String>,
    // boundary: Go `StatsInfo` is a closure over `Plan`; narrowed to its
    // materialized result (ordered for deterministic log output).
    /// Materialized Go `StatsInfo(info.Plan)` result.
    pub stats_info: BTreeMap<String, u64>,
    // boundary: Go reads this through `StmtCtx.AffectedRows()`.
    /// Affected row count of the current statement.
    pub affected_rows: u64,
    /// Go `OOMAlarmVariablesInfo`.
    pub oom_alarm_variables_info: OOMAlarmVariablesInfo,
}

impl Default for ProcessInfo {
    fn default() -> Self {
        ProcessInfo {
            id: 0,
            user: String::new(),
            host: String::new(),
            port: String::new(),
            db: String::new(),
            digest: String::new(),
            info: String::new(),
            time: ZERO_TIME,
            mem_tracker: None,
            disk_tracker: None,
            cur_txn_start_ts: 0,
            resource_group_name: String::new(),
            session_alias: String::new(),
            brief_binary_plan: String::new(),
            table_ids: Vec::new(),
            index_names: Vec::new(),
            stats_info: BTreeMap::new(),
            affected_rows: 0,
            oom_alarm_variables_info: OOMAlarmVariablesInfo::default(),
        }
    }
}

// boundary: Go `sessmgr.Manager`, narrowed to the two methods these two
// packages call; `ShowProcessList`'s `map[uint64]*ProcessInfo` flattens to a
// vec of snapshots.
/// The session-manager seam over Go `sessmgr.Manager`.
pub trait SessionManager: Send + Sync {
    /// Go `Manager.ShowProcessList`.
    fn show_process_list(&self) -> Vec<Arc<ProcessInfo>>;
    /// Go `Manager.GetProcessInfo`.
    fn get_process_info(&self, id: u64) -> Option<Arc<ProcessInfo>>;
}

/// Go `ConfigProvider`: memory usage alarm configuration values.
pub trait ConfigProvider: Send + Sync {
    /// Go `GetMemoryUsageAlarmRatio`.
    fn get_memory_usage_alarm_ratio(&self) -> f64;
    /// Go `GetMemoryUsageAlarmKeepRecordNum`.
    fn get_memory_usage_alarm_keep_record_num(&self) -> i64;
    /// Go `GetLogDir`.
    fn get_log_dir(&self) -> PathBuf;
    /// Go `GetComponentName`.
    fn get_component_name(&self) -> String;
}

/// Instance memory readings, Go `memory.ReadMemStats`' consumed subset.
#[derive(Clone, Copy, Debug, Default)]
pub struct InstanceMemStats {
    /// Go `runtime.MemStats.HeapAlloc`.
    pub heap_alloc: u64,
    /// Go `runtime.MemStats.HeapInuse`.
    pub heap_inuse: u64,
}

// boundary: Go ambient reads narrowed to snapshot inputs —
// `memory.ServerMemoryLimit.Load()`, `memory.MemTotal()`,
// `memory.MemUsed()`, `memory.ReadMemStats()`,
// `memory.UsingGlobalMemArbitration()`, and `vardef.OOMAction.Load()`.
/// Provider of the ambient memory/runtime state the Go source reads from
/// package globals.
pub trait MemoryStateProvider: Send + Sync {
    /// Go `memory.ServerMemoryLimit.Load()`.
    fn server_memory_limit(&self) -> u64;
    /// Go `memory.MemTotal()`.
    fn mem_total(&self) -> Result<u64, String>;
    /// Go `memory.MemUsed()`.
    fn mem_used(&self) -> Result<u64, String>;
    /// Go `memory.ReadMemStats()`.
    fn read_mem_stats(&self) -> InstanceMemStats;
    /// Go `memory.UsingGlobalMemArbitration()`.
    fn using_global_mem_arbitration(&self) -> bool {
        false
    }
    /// Go `vardef.OOMAction.Load()` (default `vardef.DefTiDBMemOOMAction`).
    fn oom_action(&self) -> String {
        "CANCEL".to_owned()
    }
}

// boundary: Go `recordProfile`/`write` (runtime/pprof "heap" profile) and
// `recordGoroutineProfile` (`runtime.Stack` dump into a 64MB buffer) are Go
// runtime facilities; a production implementation belongs to a layer that
// owns an allocator/thread profiler.
/// The injected profile-dump side effect of Go `recordProfile` +
/// `recordGoroutineProfile`.
pub trait ProfileRecorder: Send + Sync {
    /// Writes the runtime profiles under `record_dir`.
    fn record_profile(&self, record_dir: &Path) -> Result<(), String>;
}

/// Go `AlarmReason`: why a record was (or was not) taken.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AlarmReason {
    /// Go `GrowTooFast`: memory increasing too fast.
    GrowTooFast,
    /// Go `ExceedAlarmRatio`: memory used exceeds the threshold.
    ExceedAlarmRatio,
    /// Go `NoReason`: no alarm.
    NoReason,
}

impl fmt::Display for AlarmReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Go `AlarmReason.String()` string table, byte for byte.
        let text = match self {
            AlarmReason::GrowTooFast => "memory usage grows too fast",
            AlarmReason::ExceedAlarmRatio => "memory usage exceeds alarm ratio",
            AlarmReason::NoReason => "no reason",
        };
        f.write_str(text)
    }
}

/// Go `Handle`: the handler for memory usage alarm.
pub struct Handle {
    record: MemoryUsageAlarm,
    sm: Option<Arc<dyn SessionManager>>,
}

impl Handle {
    /// Go `NewMemoryUsageAlarmHandle` (the `exitCh` is dropped with the
    /// goroutine loop; see the module narrowings).
    pub fn new(
        config_provider: Arc<dyn ConfigProvider>,
        mem_state: Arc<dyn MemoryStateProvider>,
        profile_recorder: Arc<dyn ProfileRecorder>,
    ) -> Handle {
        Handle {
            record: MemoryUsageAlarm::new(config_provider, mem_state, profile_recorder),
            sm: None,
        }
    }

    /// Go `Handle.SetSessionManager`.
    pub fn set_session_manager(&mut self, sm: Arc<dyn SessionManager>) -> &mut Handle {
        self.sm = Some(sm);
        self
    }

    /// One iteration of Go `Handle.Run`'s 100ms ticker loop
    /// (`record.alarm4ExcessiveMemUsage(*sm)`).
    pub fn tick(&mut self, now: DateTime<Utc>) {
        let sm = self.sm.clone();
        self.record.alarm4_excessive_mem_usage(sm.as_deref(), now);
    }
}

/// Go `memoryUsageAlarm`.
struct MemoryUsageAlarm {
    last_check_time: DateTime<Utc>,
    last_update_variable_time: DateTime<Utc>,
    err: Option<String>,
    config_provider: Arc<dyn ConfigProvider>,
    mem_state: Arc<dyn MemoryStateProvider>,
    profile_recorder: Arc<dyn ProfileRecorder>,
    base_record_dir: PathBuf,
    last_record_dir_name: Vec<PathBuf>,
    last_record_mem_used: u64,
    memory_usage_alarm_ratio: f64,
    memory_usage_alarm_keep_record_num: i64,
    server_memory_limit: u64,
    is_server_memory_limit_set: bool,
    initialized: bool,
}

impl MemoryUsageAlarm {
    /// Zero-value construction plus the injected seams (Go builds the zero
    /// `memoryUsageAlarm{configProvider: ...}` struct literal).
    fn new(
        config_provider: Arc<dyn ConfigProvider>,
        mem_state: Arc<dyn MemoryStateProvider>,
        profile_recorder: Arc<dyn ProfileRecorder>,
    ) -> MemoryUsageAlarm {
        MemoryUsageAlarm {
            last_check_time: ZERO_TIME,
            last_update_variable_time: ZERO_TIME,
            err: None,
            config_provider,
            mem_state,
            profile_recorder,
            base_record_dir: PathBuf::new(),
            last_record_dir_name: Vec::new(),
            last_record_mem_used: 0,
            memory_usage_alarm_ratio: 0.0,
            memory_usage_alarm_keep_record_num: 0,
            server_memory_limit: 0,
            is_server_memory_limit_set: false,
            initialized: false,
        }
    }

    /// Go `memoryUsageAlarm.updateVariable`.
    fn update_variable(&mut self, now: DateTime<Utc>) {
        if now - self.last_update_variable_time < ChronoDuration::seconds(60) {
            return;
        }
        self.memory_usage_alarm_ratio = self.config_provider.get_memory_usage_alarm_ratio();
        self.memory_usage_alarm_keep_record_num = self
            .config_provider
            .get_memory_usage_alarm_keep_record_num();
        self.server_memory_limit = self.mem_state.server_memory_limit();
        if self.server_memory_limit != 0 {
            self.is_server_memory_limit_set = true;
        } else {
            match self.mem_state.mem_total() {
                Ok(total) => self.server_memory_limit = total,
                Err(err) => {
                    self.err = Some(err.clone());
                    bg_logger().error(
                        "get system total memory fail",
                        &[Field::new(
                            "error",
                            Value::Error {
                                basic: err,
                                verbose: None,
                            },
                        )],
                    );
                    return;
                }
            }
            self.is_server_memory_limit_set = false;
        }
        self.last_update_variable_time = now;
    }

    /// Go `memoryUsageAlarm.initMemoryUsageAlarmRecord`.
    fn init_memory_usage_alarm_record(&mut self, now: DateTime<Utc>) {
        self.last_check_time = ZERO_TIME;
        self.last_update_variable_time = ZERO_TIME;
        self.update_variable(now);
        let tidb_log_dir = self.config_provider.get_log_dir();
        self.base_record_dir = tidb_log_dir.join("oom_record");
        // boundary: Go `disk.CheckAndCreateDir` (stat, then MkdirAll 0750);
        // the Rust disk port does not export it, so the same two steps are
        // inlined here.
        if let Err(err) = check_and_create_dir(&self.base_record_dir) {
            self.err = Some(err);
            return;
        }
        // Read last records.
        let record_dirs = match fs::read_dir(&self.base_record_dir) {
            Ok(dirs) => dirs,
            Err(err) => {
                self.err = Some(err.to_string());
                return;
            }
        };
        for dir in record_dirs.flatten() {
            let file_name = dir.file_name();
            let name = file_name.to_string_lossy();
            if name.contains("record") {
                self.last_record_dir_name
                    .push(self.base_record_dir.join(&*file_name));
            }
        }
        self.initialized = true;
    }

    /// Go `memoryUsageAlarm.alarm4ExcessiveMemUsage`.
    fn alarm4_excessive_mem_usage(&mut self, sm: Option<&dyn SessionManager>, now: DateTime<Utc>) {
        if self.mem_state.using_global_mem_arbitration() {
            return;
        }
        if !self.initialized {
            self.init_memory_usage_alarm_record(now);
            if self.err.is_some() {
                return;
            }
        } else {
            self.update_variable(now);
        }
        if self.memory_usage_alarm_ratio <= 0.0 || self.memory_usage_alarm_ratio >= 1.0 {
            return;
        }
        let instance_stats = self.mem_state.read_mem_stats();
        let memory_usage = if self.is_server_memory_limit_set {
            instance_stats.heap_alloc
        } else {
            match self.mem_state.mem_used() {
                Ok(used) => used,
                Err(err) => {
                    self.err = Some(err.clone());
                    bg_logger().error(
                        "get system memory usage fail",
                        &[Field::new(
                            "error",
                            Value::Error {
                                basic: err,
                                verbose: None,
                            },
                        )],
                    );
                    return;
                }
            }
        };

        // TODO(from Go source): Consider NextGC to record SQLs.
        let (need_record, reason) = self.need_record(memory_usage, now);
        if need_record {
            self.last_check_time = now;
            self.last_record_mem_used = memory_usage;
            self.do_record(memory_usage, instance_stats.heap_alloc, sm, reason);
            self.try_remove_redundant_records();
        }
    }

    /// Go `memoryUsageAlarm.needRecord`.
    fn need_record(&self, memory_usage: u64, now: DateTime<Utc>) -> (bool, AlarmReason) {
        // At least 60 seconds between two recordings that memory usage is
        // less than threshold (default 70% system memory). If the memory is
        // still exceeded, only records once. If the memory used ratio
        // recorded this time is 0.1 higher than last time, we will force
        // record this time.
        if memory_usage as f64 <= self.server_memory_limit as f64 * self.memory_usage_alarm_ratio {
            return (false, AlarmReason::NoReason);
        }

        let interval = now - self.last_check_time;
        let mem_diff = memory_usage as i64 - self.last_record_mem_used as i64;
        if interval > ChronoDuration::seconds(60) {
            return (true, AlarmReason::ExceedAlarmRatio);
        }
        if mem_diff as f64 > 0.1 * self.server_memory_limit as f64 {
            return (true, AlarmReason::GrowTooFast);
        }
        (false, AlarmReason::NoReason)
    }

    /// Go `memoryUsageAlarm.doRecord`.
    fn do_record(
        &mut self,
        mem_usage: u64,
        instance_memory_usage: u64,
        sm: Option<&dyn SessionManager>,
        alarm_reason: AlarmReason,
    ) {
        let component_name = self.config_provider.get_component_name();
        let mut fields = Vec::with_capacity(6);
        fields.push(Field::new(
            format!("is {component_name}_memory_limit set"),
            Value::Bool(self.is_server_memory_limit_set),
        ));
        if self.is_server_memory_limit_set {
            fields.push(Field::new(
                format!("{component_name}_memory_limit"),
                Value::U64(self.server_memory_limit),
            ));
            fields.push(Field::new(
                format!("{component_name} memory usage"),
                Value::U64(mem_usage),
            ));
        } else {
            fields.push(Field::new(
                "system memory total",
                Value::U64(self.server_memory_limit),
            ));
            fields.push(Field::new("system memory usage", Value::U64(mem_usage)));
            fields.push(Field::new(
                format!("{component_name} memory usage"),
                Value::U64(instance_memory_usage),
            ));
        }
        fields.push(Field::new(
            "memory-usage-alarm-ratio",
            Value::F64(self.memory_usage_alarm_ratio),
        ));
        fields.push(Field::new(
            "record path",
            Value::Str(self.base_record_dir.to_string_lossy().into_owned()),
        ));
        bg_logger().warn(
            &format!(
                "{component_name} has the risk of OOM because of {alarm_reason}. \
                 Running profiles will be recorded in record path"
            ),
            &fields,
        );
        let record_dir = self.base_record_dir.join(format!(
            "record{}",
            self.last_check_time
                .to_rfc3339_opts(SecondsFormat::Secs, true)
        ));
        if let Err(err) = check_and_create_dir(&record_dir) {
            self.err = Some(err);
            return;
        }
        self.last_record_dir_name.push(record_dir.clone());
        if let Some(sm) = sm {
            if let Err(err) = self.record_sql(sm, &record_dir) {
                self.err = Some(err);
                return;
            }
        }
        if let Err(err) = self.profile_recorder.record_profile(&record_dir) {
            self.err = Some(err);
        }
    }

    /// Go `memoryUsageAlarm.tryRemoveRedundantRecords`.
    fn try_remove_redundant_records(&mut self) {
        let keep = usize::try_from(self.memory_usage_alarm_keep_record_num).unwrap_or(0);
        while self.last_record_dir_name.len() > keep {
            if let Err(err) = fs::remove_dir_all(&self.last_record_dir_name[0]) {
                bg_logger().error(
                    "remove temp files failed",
                    &[Field::new(
                        "error",
                        Value::Error {
                            basic: err.to_string(),
                            verbose: None,
                        },
                    )],
                );
            }
            self.last_record_dir_name.remove(0);
        }
    }

    /// Go `memoryUsageAlarm.printTop10SqlInfo`, with the `*os.File` narrowed
    /// to building the full text before one write.
    fn print_top10_sql_info(&self, pinfo: &[Arc<ProcessInfo>]) -> String {
        let mut out = String::new();
        out.push_str("The 10 SQLs with the most memory usage for OOM analysis\n");
        out.push_str(&self.get_top10_sql_info_by_memory_usage(pinfo));
        out.push_str("The 10 SQLs with the most time usage for OOM analysis\n");
        out.push_str(&self.get_top10_sql_info_by_cost_time(pinfo));
        out
    }

    /// Go `memoryUsageAlarm.getTop10SqlInfo`.
    fn get_top10_sql_info(
        &self,
        cmp: impl Fn(&Arc<ProcessInfo>, &Arc<ProcessInfo>) -> std::cmp::Ordering,
        pinfo: &[Arc<ProcessInfo>],
    ) -> String {
        let mut list: Vec<Arc<ProcessInfo>> = pinfo.to_vec();
        list.sort_by(cmp);
        let mut buf = String::new();
        let oom_action = self.mem_state.oom_action();
        let server_memory_limit = self.mem_state.server_memory_limit();
        let mut total_cnt = 10;
        for (i, info) in list.iter().enumerate() {
            if total_cnt == 0 {
                break;
            }
            buf.push_str(&format!("SQL {i}: \n"));
            let mut fields = gen_log_fields(self.last_check_time - info.time, info, false);
            fields.push(Field::new(
                "tidb_mem_oom_action",
                Value::Str(oom_action.clone()),
            ));
            fields.push(Field::new(
                "tidb_server_memory_limit",
                Value::U64(server_memory_limit),
            ));
            fields.push(Field::new(
                "tidb_mem_quota_query",
                Value::I64(info.oom_alarm_variables_info.session_mem_quota_query),
            ));
            fields.push(Field::new(
                "tidb_analyze_version",
                Value::I64(info.oom_alarm_variables_info.session_analyze_version),
            ));
            fields.push(Field::new(
                "tidb_enable_rate_limit_action",
                Value::Bool(
                    info.oom_alarm_variables_info
                        .session_enabled_rate_limit_action,
                ),
            ));
            fields.push(Field::new(
                "current_analyze_plan",
                Value::Str(get_plan_string(info)),
            ));
            for field in &fields {
                // Go switches on the zapcore field type and only prints
                // string/uint/int/bool payloads; every field still emits its
                // trailing newline.
                match &field.value {
                    Value::Str(s) => buf.push_str(&format!("{}: {}", field.key, s)),
                    Value::U64(v) => buf.push_str(&format!("{}: {}", field.key, v)),
                    Value::I64(v) => buf.push_str(&format!("{}: {}", field.key, v)),
                    Value::Bool(v) => buf.push_str(&format!("{}: {}", field.key, v)),
                    _ => {}
                }
                buf.push('\n');
            }
            total_cnt -= 1;
        }
        buf.push('\n');
        buf
    }

    /// Go `memoryUsageAlarm.getTop10SqlInfoByMemoryUsage`.
    fn get_top10_sql_info_by_memory_usage(&self, pinfo: &[Arc<ProcessInfo>]) -> String {
        self.get_top10_sql_info(
            |i, j| {
                let i_max = i.mem_tracker.as_ref().map_or(0, |t| t.max_consumed());
                let j_max = j.mem_tracker.as_ref().map_or(0, |t| t.max_consumed());
                j_max.cmp(&i_max)
            },
            pinfo,
        )
    }

    /// Go `memoryUsageAlarm.getTop10SqlInfoByCostTime`.
    fn get_top10_sql_info_by_cost_time(&self, pinfo: &[Arc<ProcessInfo>]) -> String {
        self.get_top10_sql_info(|i, j| i.time.cmp(&j.time), pinfo)
    }

    /// Go `memoryUsageAlarm.recordSQL`.
    fn record_sql(&self, sm: &dyn SessionManager, record_dir: &Path) -> Result<(), String> {
        let process_info = sm.show_process_list();
        let pinfo: Vec<Arc<ProcessInfo>> = process_info
            .into_iter()
            .filter(|info| !info.info.is_empty())
            .collect();
        let file_name = record_dir.join("running_sql");
        let text = self.print_top10_sql_info(&pinfo);
        fs::write(&file_name, text).map_err(|err| {
            bg_logger().error(
                "create oom record file fail",
                &[Field::new(
                    "error",
                    Value::Error {
                        basic: err.to_string(),
                        verbose: None,
                    },
                )],
            );
            err.to_string()
        })
    }
}

/// Go `getPlanString`.
fn get_plan_string(info: &ProcessInfo) -> String {
    let rows = decode_binary_plan_for_connection(
        info.brief_binary_plan.as_bytes(),
        EXPLAIN_FORMAT_ROW,
        true,
    )
    .unwrap_or_default();
    let mut buf = String::new();
    buf.push_str("|id|estRows|task|access object|operator info|");
    for row in rows {
        buf.push_str("\n|");
        for col in row {
            buf.push_str(&format!("{col}|"));
        }
    }
    buf
}

/// Go `util.GenLogFields` (`pkg/util/util.go`) over the narrowed snapshot.
///
/// boundary: `ExecDetails`/`CopTasksDetails` zap fields, `mem_arbitration`
/// fields, the `RefCountOfStmtCtx.TryIncrease` nil return, and
/// `parser.Normalize` redaction are not modeled (see module narrowings).
fn gen_log_fields(
    cost_time: ChronoDuration,
    info: &ProcessInfo,
    need_truncate_sql: bool,
) -> Vec<Field> {
    let mut log_fields = Vec::with_capacity(20);
    let secs = cost_time.num_microseconds().unwrap_or(i64::MAX) as f64 / 1_000_000.0;
    log_fields.push(Field::new(
        "cost_time",
        Value::Str(format!("{}s", format_go_float(secs))),
    ));
    if !info.stats_info.is_empty() {
        let mut buf = String::new();
        let mut first_comma = false;
        for (k, v) in &info.stats_info {
            let v_str = if *v == 0 {
                "pseudo".to_owned()
            } else {
                v.to_string()
            };
            if first_comma {
                buf.push_str(&format!(",{k}:{v_str}"));
            } else {
                buf.push_str(&format!("{k}:{v_str}"));
                first_comma = true;
            }
        }
        log_fields.push(Field::new("stats", Value::Str(buf)));
    }
    if info.id != 0 {
        log_fields.push(Field::new("conn", Value::U64(info.id)));
    }
    if !info.user.is_empty() {
        log_fields.push(Field::new("user", Value::Str(info.user.clone())));
    }
    if !info.db.is_empty() {
        log_fields.push(Field::new("database", Value::Str(info.db.clone())));
    }
    if !info.table_ids.is_empty() {
        // Go renders `fmt.Sprintf("%v", []int64)` then swaps spaces for
        // commas: `[1,2]`.
        let ids: Vec<String> = info.table_ids.iter().map(ToString::to_string).collect();
        log_fields.push(Field::new(
            "table_ids",
            Value::Str(format!("[{}]", ids.join(","))),
        ));
    }
    if !info.index_names.is_empty() {
        log_fields.push(Field::new(
            "index_names",
            Value::Str(format!("[{}]", info.index_names.join(","))),
        ));
    }
    log_fields.push(Field::new(
        "txn_start_ts",
        Value::U64(info.cur_txn_start_ts),
    ));
    if let Some(mem_tracker) = &info.mem_tracker {
        log_fields.push(Field::new(
            "mem_max",
            Value::Str(format!(
                "{} Bytes ({})",
                mem_tracker.max_consumed(),
                format_bytes(mem_tracker.max_consumed())
            )),
        ));
    }

    const LOG_SQL_LEN: usize = 1024 * 8;
    let mut sql = info.info.clone();
    if sql.len() > LOG_SQL_LEN && need_truncate_sql {
        let full_len = sql.len();
        let mut cut = LOG_SQL_LEN;
        while !sql.is_char_boundary(cut) {
            cut -= 1;
        }
        sql = format!("{} len({})", &sql[..cut], full_len);
    }
    log_fields.push(Field::new("sql", Value::Str(sql)));
    log_fields.push(Field::new(
        "session_alias",
        Value::Str(info.session_alias.clone()),
    ));
    log_fields.push(Field::new("affected rows", Value::U64(info.affected_rows)));
    log_fields
}

/// Go `strconv.FormatFloat(v, 'f', -1, 64)`: shortest decimal round-trip
/// without an exponent, which Rust's `Display` for whole and fractional
/// values in this range matches once integral values drop the `.0`.
fn format_go_float(v: f64) -> String {
    if v == v.trunc() && v.abs() < 9.007_199_254_740_992e15 {
        format!("{}", v as i64)
    } else {
        format!("{v}")
    }
}

/// Go `disk.CheckAndCreateDir` (see the boundary note at the call site).
fn check_and_create_dir(path: &Path) -> Result<(), String> {
    if path.exists() {
        return Ok(());
    }
    fs::create_dir_all(path).map_err(|err| err.to_string())?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o750))
            .map_err(|err| err.to_string())?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use chrono::TimeZone;

    use super::*;
    use crate::memory::Tracker;

    /// Go test `MockConfigProvider` (interior mutability replaces the Go
    /// test's direct field writes on the shared mock).
    struct MockConfigProvider {
        log_dir: PathBuf,
        component_name: String,
        ratio: std::sync::Mutex<f64>,
        keep_num: std::sync::atomic::AtomicI64,
    }

    impl MockConfigProvider {
        fn new(ratio: f64, keep_num: i64, component_name: &str) -> MockConfigProvider {
            MockConfigProvider {
                log_dir: PathBuf::new(),
                component_name: component_name.to_owned(),
                ratio: std::sync::Mutex::new(ratio),
                keep_num: std::sync::atomic::AtomicI64::new(keep_num),
            }
        }
    }

    impl ConfigProvider for MockConfigProvider {
        fn get_memory_usage_alarm_ratio(&self) -> f64 {
            *self.ratio.lock().unwrap()
        }

        fn get_memory_usage_alarm_keep_record_num(&self) -> i64 {
            self.keep_num.load(Ordering::SeqCst)
        }

        fn get_log_dir(&self) -> PathBuf {
            if self.log_dir.as_os_str().is_empty() {
                return std::env::temp_dir();
            }
            self.log_dir.clone()
        }

        fn get_component_name(&self) -> String {
            if self.component_name.is_empty() {
                return "test-component".to_owned();
            }
            self.component_name.clone()
        }
    }

    /// Test stand-in for the Go `memory` package globals: the Go tests store
    /// into `memory.ServerMemoryLimit` directly; here the same knob lives on
    /// the injected provider (module narrowing).
    #[derive(Default)]
    struct MockMemoryState {
        server_memory_limit: AtomicU64,
    }

    impl MemoryStateProvider for MockMemoryState {
        fn server_memory_limit(&self) -> u64 {
            self.server_memory_limit.load(Ordering::SeqCst)
        }

        fn mem_total(&self) -> Result<u64, String> {
            // Any positive stand-in for the machine total works: the Go
            // assertions scale against `record.serverMemoryLimit`.
            Ok(16 << 30)
        }

        fn mem_used(&self) -> Result<u64, String> {
            Ok(0)
        }

        fn read_mem_stats(&self) -> InstanceMemStats {
            InstanceMemStats::default()
        }
    }

    struct NoopProfileRecorder;

    impl ProfileRecorder for NoopProfileRecorder {
        fn record_profile(&self, _record_dir: &Path) -> Result<(), String> {
            Ok(())
        }
    }

    fn new_record(mock_state: Arc<MockMemoryState>, ratio: f64, keep_num: i64) -> MemoryUsageAlarm {
        MemoryUsageAlarm::new(
            Arc::new(MockConfigProvider::new(ratio, keep_num, "test-component")),
            mock_state,
            Arc::new(NoopProfileRecorder),
        )
    }

    fn new_record_with_config(
        config: Arc<MockConfigProvider>,
        mock_state: Arc<MockMemoryState>,
    ) -> MemoryUsageAlarm {
        MemoryUsageAlarm::new(config, mock_state, Arc::new(NoopProfileRecorder))
    }

    /// Go test `TestIfNeedDoRecord`.
    #[test]
    fn test_if_need_do_record() {
        let mut record = new_record(Arc::new(MockMemoryState::default()), 0.7, 5);
        record.init_memory_usage_alarm_record(Utc::now());
        assert!(record.err.is_none());

        // mem usage ratio < 70% will not be recorded
        let mem_used = (0.69 * record.server_memory_limit as f64) as u64;
        let (need_record, reason) = record.need_record(mem_used, Utc::now());
        assert!(!need_record);
        assert_eq!(AlarmReason::NoReason, reason);

        // mem usage ratio > 70% will not be recorded
        let mem_used = (0.71 * record.server_memory_limit as f64) as u64;
        let (need_record, reason) = record.need_record(mem_used, Utc::now());
        assert!(need_record);
        assert_eq!(AlarmReason::ExceedAlarmRatio, reason);
        record.last_check_time = Utc::now();
        record.last_record_mem_used = mem_used;

        // check time - last record time < 60s will not be recorded
        let mem_used = (0.71 * record.server_memory_limit as f64) as u64;
        let (need_record, reason) = record.need_record(mem_used, Utc::now());
        assert!(!need_record);
        assert_eq!(AlarmReason::NoReason, reason);

        // check time - last record time > 60s will be recorded
        record.last_check_time -= ChronoDuration::seconds(60);
        let mem_used = (0.71 * record.server_memory_limit as f64) as u64;
        let (need_record, reason) = record.need_record(mem_used, Utc::now());
        assert!(need_record);
        assert_eq!(AlarmReason::ExceedAlarmRatio, reason);
        record.last_check_time = Utc::now();
        record.last_record_mem_used = mem_used;

        // mem usage ratio - last mem usage ratio < 10% will not be recorded
        let mem_used = (0.80 * record.server_memory_limit as f64) as u64;
        let (need_record, reason) = record.need_record(mem_used, Utc::now());
        assert!(!need_record);
        assert_eq!(AlarmReason::NoReason, reason);

        // mem usage ratio - last mem usage ratio > 10% will be recorded even
        // though check time - last record time < 60s
        let mem_used = (0.82 * record.server_memory_limit as f64) as u64;
        let (need_record, reason) = record.need_record(mem_used, Utc::now());
        assert!(need_record);
        assert_eq!(AlarmReason::GrowTooFast, reason);
    }

    /// Go test helper `genTime`: seconds after `time.Date(1970, 1, 0, ...)`,
    /// which normalizes to 1969-12-31 UTC (one day before the epoch).
    fn gen_time(sec: i64) -> DateTime<Utc> {
        let min_start_time = -86_400;
        Utc.timestamp_opt(min_start_time + sec, 0).unwrap()
    }

    /// Go test helper `genMockProcessInfoList`.
    fn gen_mock_process_info_list(
        mem_consume_list: &[i64],
        start_time_list: &[DateTime<Utc>],
        size: usize,
    ) -> Vec<Arc<ProcessInfo>> {
        let mut process_info_list = Vec::with_capacity(size);
        for i in 0..size {
            let tracker = Tracker::new(0, 0);
            tracker.consume(mem_consume_list[i]);
            let process_info = ProcessInfo {
                time: start_time_list[i],
                mem_tracker: Some(tracker),
                ..ProcessInfo::default()
            };
            process_info_list.push(Arc::new(process_info));
        }
        process_info_list
    }

    /// Go test `TestGetTop10Sql`. Expected strings are byte-exact from the
    /// Go literals.
    #[test]
    fn test_get_top10_sql() {
        let mut record = new_record(Arc::new(MockMemoryState::default()), 0.7, 5);
        record.init_memory_usage_alarm_record(Utc::now());
        record.last_check_time = gen_time(123_456);

        let process_info_list = gen_mock_process_info_list(
            &[1000, 87_263_523, 34_223],
            &[gen_time(1234), gen_time(123_456), gen_time(12)],
            3,
        );
        let actual = record.get_top10_sql_info_by_memory_usage(&process_info_list);
        assert_eq!(
            "SQL 0: \ncost_time: 0s\ntxn_start_ts: 0\nmem_max: 87263523 Bytes (83.2 MB)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\nSQL 1: \ncost_time: 123444s\ntxn_start_ts: 0\nmem_max: 34223 Bytes (33.4 KB)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\nSQL 2: \ncost_time: 122222s\ntxn_start_ts: 0\nmem_max: 1000 Bytes (1000 Bytes)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\n\n",
            actual
        );
        let actual = record.get_top10_sql_info_by_cost_time(&process_info_list);
        assert_eq!("SQL 0: \ncost_time: 123444s\ntxn_start_ts: 0\nmem_max: 34223 Bytes (33.4 KB)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\nSQL 1: \ncost_time: 122222s\ntxn_start_ts: 0\nmem_max: 1000 Bytes (1000 Bytes)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\nSQL 2: \ncost_time: 0s\ntxn_start_ts: 0\nmem_max: 87263523 Bytes (83.2 MB)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\n\n", actual);

        let process_info_list = gen_mock_process_info_list(
            &[
                1000,
                87_263_523,
                34_223,
                532_355,
                123_225_151,
                231_231_515,
                12_312,
                12_515_134_234,
                232,
                12_414,
                15_263_236,
                123_123_123,
                15,
            ],
            &[
                gen_time(1234),
                gen_time(123_456),
                gen_time(12),
                gen_time(3241),
                gen_time(12_515),
                gen_time(3215),
                gen_time(61_314),
                gen_time(12_234),
                gen_time(1123),
                gen_time(512),
                gen_time(11_111),
                gen_time(22_222),
                gen_time(5512),
            ],
            13,
        );
        let actual = record.get_top10_sql_info_by_memory_usage(&process_info_list);
        assert_eq!("SQL 0: \ncost_time: 111222s\ntxn_start_ts: 0\nmem_max: 12515134234 Bytes (11.7 GB)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\nSQL 1: \ncost_time: 120241s\ntxn_start_ts: 0\nmem_max: 231231515 Bytes (220.5 MB)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\nSQL 2: \ncost_time: 110941s\ntxn_start_ts: 0\nmem_max: 123225151 Bytes (117.5 MB)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\nSQL 3: \ncost_time: 101234s\ntxn_start_ts: 0\nmem_max: 123123123 Bytes (117.4 MB)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\nSQL 4: \ncost_time: 0s\ntxn_start_ts: 0\nmem_max: 87263523 Bytes (83.2 MB)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\nSQL 5: \ncost_time: 112345s\ntxn_start_ts: 0\nmem_max: 15263236 Bytes (14.6 MB)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\nSQL 6: \ncost_time: 120215s\ntxn_start_ts: 0\nmem_max: 532355 Bytes (519.9 KB)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\nSQL 7: \ncost_time: 123444s\ntxn_start_ts: 0\nmem_max: 34223 Bytes (33.4 KB)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\nSQL 8: \ncost_time: 122944s\ntxn_start_ts: 0\nmem_max: 12414 Bytes (12.1 KB)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\nSQL 9: \ncost_time: 62142s\ntxn_start_ts: 0\nmem_max: 12312 Bytes (12.0 KB)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\n\n", actual);
        let actual = record.get_top10_sql_info_by_cost_time(&process_info_list);
        assert_eq!("SQL 0: \ncost_time: 123444s\ntxn_start_ts: 0\nmem_max: 34223 Bytes (33.4 KB)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\nSQL 1: \ncost_time: 122944s\ntxn_start_ts: 0\nmem_max: 12414 Bytes (12.1 KB)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\nSQL 2: \ncost_time: 122333s\ntxn_start_ts: 0\nmem_max: 232 Bytes (232 Bytes)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\nSQL 3: \ncost_time: 122222s\ntxn_start_ts: 0\nmem_max: 1000 Bytes (1000 Bytes)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\nSQL 4: \ncost_time: 120241s\ntxn_start_ts: 0\nmem_max: 231231515 Bytes (220.5 MB)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\nSQL 5: \ncost_time: 120215s\ntxn_start_ts: 0\nmem_max: 532355 Bytes (519.9 KB)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\nSQL 6: \ncost_time: 117944s\ntxn_start_ts: 0\nmem_max: 15 Bytes (15 Bytes)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\nSQL 7: \ncost_time: 112345s\ntxn_start_ts: 0\nmem_max: 15263236 Bytes (14.6 MB)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\nSQL 8: \ncost_time: 111222s\ntxn_start_ts: 0\nmem_max: 12515134234 Bytes (11.7 GB)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\nSQL 9: \ncost_time: 110941s\ntxn_start_ts: 0\nmem_max: 123225151 Bytes (117.5 MB)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\n\n", actual);
    }

    /// Go test `TestUpdateVariables`. Adaptation: Go stores into the
    /// `memory.ServerMemoryLimit` package global; the narrowed provider knob
    /// takes its place.
    #[test]
    fn test_update_variables() {
        let mock_config = Arc::new(MockConfigProvider::new(0.3, 3, "test-component"));
        let mock_state = Arc::new(MockMemoryState::default());
        mock_state.server_memory_limit.store(1024, Ordering::SeqCst);

        let mut record = new_record_with_config(Arc::clone(&mock_config), Arc::clone(&mock_state));

        record.init_memory_usage_alarm_record(Utc::now());
        assert_eq!(0.3, record.config_provider.get_memory_usage_alarm_ratio());
        assert_eq!(
            3,
            record
                .config_provider
                .get_memory_usage_alarm_keep_record_num()
        );
        assert_eq!(1024, record.server_memory_limit);

        *mock_config.ratio.lock().unwrap() = 0.6;
        mock_config.keep_num.store(6, Ordering::SeqCst);
        mock_state.server_memory_limit.store(2048, Ordering::SeqCst);

        record.update_variable(Utc::now());
        assert_eq!(0.6, record.config_provider.get_memory_usage_alarm_ratio());
        assert_eq!(
            6,
            record
                .config_provider
                .get_memory_usage_alarm_keep_record_num()
        );
        assert_eq!(1024, record.server_memory_limit);
        record.last_update_variable_time -= ChronoDuration::seconds(60);
        record.update_variable(Utc::now());
        assert_eq!(0.6, record.config_provider.get_memory_usage_alarm_ratio());
        assert_eq!(
            6,
            record
                .config_provider
                .get_memory_usage_alarm_keep_record_num()
        );
        assert_eq!(2048, record.server_memory_limit);
    }
}
