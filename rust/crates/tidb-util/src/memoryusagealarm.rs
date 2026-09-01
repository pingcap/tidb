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

//! Go `pkg/util/memoryusagealarm`: the 100ms OOM-risk monitor that records
//! running SQL, heap state, and thread stacks when memory crosses the alarm
//! ratio.
//!
//! The shared [`SessionManager`] and [`ProcessInfo`] types are Rust's crate
//! boundary for Go `pkg/session/sessmgr`; `crate::servermemorylimit` consumes
//! the same live process list.

use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, Duration as ChronoDuration, Local, SecondsFormat, TimeZone, Utc};
use tidb_datatype::EXPLAIN_FORMAT_ROW;
use tidb_log::{Field, Value};

use crate::logutil::bg_logger;
use crate::memory::{format_bytes, Tracker};
use crate::plancodec::decode_binary_plan_for_connection;

/// Go zero `time.Time`: midnight UTC on January 1, year 1.
pub(crate) fn zero_time() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(1, 1, 1, 0, 0, 0)
        .single()
        .expect("Go zero time is representable by chrono")
}

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
    /// Go `RedactSQL`.
    pub redact_sql: tidb_parser::RedactMode,
    /// Go `Time` (statement start time).
    pub time: DateTime<Utc>,
    /// The monotonic reading embedded in a live Go `time.Time`. Test fixtures
    /// built with `time.Unix` have no such reading and leave this as `None`.
    pub started_instant: Option<Instant>,
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
    // materialized result.
    /// Materialized Go `StatsInfo(info.Plan)` result.
    pub stats_info: HashMap<String, u64>,
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
            redact_sql: tidb_parser::RedactMode::Disabled,
            time: zero_time(),
            started_instant: None,
            mem_tracker: None,
            disk_tracker: None,
            cur_txn_start_ts: 0,
            resource_group_name: String::new(),
            session_alias: String::new(),
            brief_binary_plan: String::new(),
            table_ids: Vec::new(),
            index_names: Vec::new(),
            stats_info: HashMap::new(),
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

/// Go `TiDBConfigProvider`.
#[derive(Clone, Copy, Debug, Default)]
pub struct TiDBConfigProvider;

impl ConfigProvider for TiDBConfigProvider {
    fn get_memory_usage_alarm_ratio(&self) -> f64 {
        tidb_vardef::memory_usage_alarm_ratio()
    }

    fn get_memory_usage_alarm_keep_record_num(&self) -> i64 {
        tidb_vardef::MEMORY_USAGE_ALARM_KEEP_RECORD_NUM.load(Ordering::SeqCst)
    }

    fn get_log_dir(&self) -> PathBuf {
        let filename = tidb_config::config_tree::config::get_global_config()
            .log
            .file
            .filename;
        let path = Path::new(&filename);
        path.parent().map_or_else(
            || {
                if path.has_root() {
                    path.to_path_buf()
                } else {
                    PathBuf::new()
                }
            },
            Path::to_path_buf,
        )
    }

    fn get_component_name(&self) -> String {
        "tidb-server".to_owned()
    }
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
    exit: crossbeam_channel::Receiver<()>,
    // The outer option is Go's nil atomic pointer (SetSessionManager was
    // never called); the inner option is a stored nil sessmgr.Manager.
    sm: std::sync::Mutex<Option<Option<Arc<dyn SessionManager>>>>,
    config_provider: Arc<dyn ConfigProvider>,
}

impl Handle {
    /// Go `NewMemoryUsageAlarmHandle`.
    pub fn new(
        exit: crossbeam_channel::Receiver<()>,
        config_provider: Arc<dyn ConfigProvider>,
    ) -> Handle {
        Handle {
            exit,
            sm: std::sync::Mutex::new(None),
            config_provider,
        }
    }

    /// Go `Handle.SetSessionManager`.
    pub fn set_session_manager(&self, sm: Option<Arc<dyn SessionManager>>) -> &Handle {
        *self
            .sm
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(sm);
        self
    }

    /// Go `Handle.Run`: samples every 100ms until the exit channel fires.
    pub fn run(&self) {
        let ticker = crossbeam_channel::tick(Duration::from_millis(100));
        let sm = self
            .sm
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let mut record = MemoryUsageAlarm::new(Arc::clone(&self.config_provider));
        loop {
            crossbeam_channel::select! {
                recv(ticker) -> _ => {
                    record.alarm4_excessive_mem_usage(
                        sm.as_ref()
                            .expect("session manager must be set before the first alarm tick")
                            .as_deref(),
                    );
                }
                recv(self.exit) -> _ => return,
            }
        }
    }
}

/// Go `memoryUsageAlarm`.
struct MemoryUsageAlarm {
    last_check_time: DateTime<Utc>,
    last_check_instant: Option<Instant>,
    last_update_variable_time: DateTime<Utc>,
    last_update_variable_instant: Option<Instant>,
    err: Option<String>,
    config_provider: Arc<dyn ConfigProvider>,
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
    /// Go's zero `memoryUsageAlarm{configProvider: ...}` construction.
    fn new(config_provider: Arc<dyn ConfigProvider>) -> MemoryUsageAlarm {
        MemoryUsageAlarm {
            last_check_time: zero_time(),
            last_check_instant: None,
            last_update_variable_time: zero_time(),
            last_update_variable_instant: None,
            err: None,
            config_provider,
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
    fn update_variable(&mut self) {
        let update_is_fresh = self.last_update_variable_instant.map_or_else(
            || Utc::now() - self.last_update_variable_time < ChronoDuration::seconds(60),
            |instant| instant.elapsed() < Duration::from_secs(60),
        );
        if update_is_fresh {
            return;
        }
        self.memory_usage_alarm_ratio = self.config_provider.get_memory_usage_alarm_ratio();
        self.memory_usage_alarm_keep_record_num = self
            .config_provider
            .get_memory_usage_alarm_keep_record_num();
        self.server_memory_limit = crate::memory::SERVER_MEMORY_LIMIT.load(Ordering::SeqCst);
        if self.server_memory_limit != 0 {
            self.is_server_memory_limit_set = true;
        } else {
            match crate::memory::mem_total() {
                Ok(total) => {
                    self.server_memory_limit = total;
                    self.err = None;
                }
                Err(err) => {
                    self.err = Some(err.to_string());
                    bg_logger().error(
                        "get system total memory fail",
                        &[Field::new(
                            "error",
                            Value::Error {
                                basic: err.to_string(),
                                verbose: None,
                            },
                        )],
                    );
                    return;
                }
            }
            self.is_server_memory_limit_set = false;
        }
        self.last_update_variable_time = Utc::now();
        self.last_update_variable_instant = Some(Instant::now());
    }

    /// Go `memoryUsageAlarm.initMemoryUsageAlarmRecord`.
    fn init_memory_usage_alarm_record(&mut self) {
        self.last_check_time = zero_time();
        self.last_check_instant = None;
        self.last_update_variable_time = zero_time();
        self.last_update_variable_instant = None;
        self.update_variable();
        let tidb_log_dir = self.config_provider.get_log_dir();
        self.base_record_dir = tidb_log_dir.join("oom_record");
        match crate::disk::check_and_create_dir(&self.base_record_dir) {
            Ok(()) => self.err = None,
            Err(err) => {
                self.err = Some(err.to_string());
                return;
            }
        }
        // Read last records.
        let record_dirs = match fs::read_dir(&self.base_record_dir) {
            Ok(dirs) => dirs,
            Err(err) => {
                self.err = Some(err.to_string());
                return;
            }
        };
        let mut record_dirs = match record_dirs.collect::<Result<Vec<_>, _>>() {
            Ok(dirs) => dirs,
            Err(err) => {
                self.err = Some(err.to_string());
                return;
            }
        };
        record_dirs.sort_by_key(std::fs::DirEntry::file_name);
        for dir in record_dirs {
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
    fn alarm4_excessive_mem_usage(&mut self, sm: Option<&dyn SessionManager>) {
        if crate::memory::using_global_mem_arbitration() {
            return;
        }
        if !self.initialized {
            self.init_memory_usage_alarm_record();
            if self.err.is_some() {
                return;
            }
        } else {
            self.update_variable();
        }
        if self.memory_usage_alarm_ratio <= 0.0 || self.memory_usage_alarm_ratio >= 1.0 {
            return;
        }
        let instance_stats = crate::memory::read_mem_stats();
        let memory_usage = if self.is_server_memory_limit_set {
            u64::try_from(instance_stats.heap_alloc).unwrap_or_default()
        } else {
            match crate::memory::mem_used() {
                Ok(used) => {
                    self.err = None;
                    used
                }
                Err(err) => {
                    self.err = Some(err.to_string());
                    bg_logger().error(
                        "get system memory usage fail",
                        &[Field::new(
                            "error",
                            Value::Error {
                                basic: err.to_string(),
                                verbose: None,
                            },
                        )],
                    );
                    return;
                }
            }
        };

        // TODO(from Go source): Consider NextGC to record SQLs.
        let (need_record, reason) = self.need_record(memory_usage);
        if need_record {
            self.last_check_time = Utc::now();
            self.last_check_instant = Some(Instant::now());
            self.last_record_mem_used = memory_usage;
            self.do_record(
                memory_usage,
                u64::try_from(instance_stats.heap_alloc).unwrap_or_default(),
                sm,
                reason,
            );
            self.try_remove_redundant_records();
        }
    }

    /// Go `memoryUsageAlarm.needRecord`.
    fn need_record(&self, memory_usage: u64) -> (bool, AlarmReason) {
        // At least 60 seconds between two recordings that memory usage is
        // less than threshold (default 70% system memory). If the memory is
        // still exceeded, only records once. If the memory used ratio
        // recorded this time is 0.1 higher than last time, we will force
        // record this time.
        if memory_usage as f64 <= self.server_memory_limit as f64 * self.memory_usage_alarm_ratio {
            return (false, AlarmReason::NoReason);
        }

        let interval_exceeds_minimum = self.last_check_instant.map_or_else(
            || Utc::now() - self.last_check_time > ChronoDuration::seconds(60),
            |instant| instant.elapsed() > Duration::from_secs(60),
        );
        let mem_diff = (memory_usage as i64).wrapping_sub(self.last_record_mem_used as i64);
        if interval_exceeds_minimum {
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
                .with_timezone(&Local)
                .to_rfc3339_opts(SecondsFormat::Secs, true)
        ));
        match crate::disk::check_and_create_dir(&record_dir) {
            Ok(()) => self.err = None,
            Err(err) => {
                self.err = Some(err.to_string());
                return;
            }
        }
        self.last_record_dir_name.push(record_dir.clone());
        if let Some(sm) = sm {
            match self.record_sql(sm, &record_dir) {
                Ok(()) => self.err = None,
                Err(err) => {
                    self.err = Some(err);
                    return;
                }
            }
        }
        match record_profile(&record_dir) {
            Ok(()) => self.err = None,
            Err(err) => self.err = Some(err.to_string()),
        }
    }

    /// Go `memoryUsageAlarm.tryRemoveRedundantRecords`.
    fn try_remove_redundant_records(&mut self) {
        while (self.last_record_dir_name.len() as i64) > self.memory_usage_alarm_keep_record_num {
            if let Err(err) = remove_all(&self.last_record_dir_name[0]) {
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

    /// Go `memoryUsageAlarm.printTop10SqlInfo`.
    fn print_top10_sql_info(&self, pinfo: &mut [Arc<ProcessInfo>], file: &mut fs::File) {
        let mut write = |text: &str, message: &str| {
            if let Err(err) = file.write_all(text.as_bytes()) {
                bg_logger().error(
                    message,
                    &[Field::new(
                        "error",
                        Value::Error {
                            basic: err.to_string(),
                            verbose: None,
                        },
                    )],
                );
            }
        };
        write(
            "The 10 SQLs with the most memory usage for OOM analysis\n",
            "write top 10 memory sql info fail",
        );
        write(
            &self.get_top10_sql_info_by_memory_usage(pinfo),
            "write top 10 memory sql info fail",
        );
        write(
            "The 10 SQLs with the most time usage for OOM analysis\n",
            "write top 10 time cost sql info fail",
        );
        write(
            &self.get_top10_sql_info_by_cost_time(pinfo),
            "write top 10 time cost sql info fail",
        );
    }

    /// Go `memoryUsageAlarm.getTop10SqlInfo`.
    fn get_top10_sql_info(
        &self,
        cmp: impl Fn(&Arc<ProcessInfo>, &Arc<ProcessInfo>) -> std::cmp::Ordering,
        pinfo: &mut [Arc<ProcessInfo>],
    ) -> String {
        pinfo.sort_unstable_by(cmp);
        let mut buf = String::new();
        let oom_action = tidb_vardef::oom_action();
        let server_memory_limit = crate::memory::SERVER_MEMORY_LIMIT.load(Ordering::SeqCst);
        let mut total_cnt = 10;
        for (i, info) in pinfo.iter().enumerate() {
            if total_cnt == 0 {
                break;
            }
            buf.push_str(&format!("SQL {i}: \n"));
            let mut fields = gen_log_fields(self.cost_time(info), info);
            fields.push(Field::new(
                "tidb_mem_oom_action",
                Value::Str(oom_action.to_owned()),
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
    fn get_top10_sql_info_by_memory_usage(&self, pinfo: &mut [Arc<ProcessInfo>]) -> String {
        self.get_top10_sql_info(
            |i, j| {
                let i_max = i
                    .mem_tracker
                    .as_ref()
                    .expect("running process has a memory tracker")
                    .max_consumed();
                let j_max = j
                    .mem_tracker
                    .as_ref()
                    .expect("running process has a memory tracker")
                    .max_consumed();
                j_max.cmp(&i_max)
            },
            pinfo,
        )
    }

    /// Go `memoryUsageAlarm.getTop10SqlInfoByCostTime`.
    fn get_top10_sql_info_by_cost_time(&self, pinfo: &mut [Arc<ProcessInfo>]) -> String {
        self.get_top10_sql_info(
            |i, j| match (i.started_instant, j.started_instant) {
                (Some(i), Some(j)) => i.cmp(&j),
                _ => i.time.cmp(&j.time),
            },
            pinfo,
        )
    }

    fn cost_time(&self, info: &ProcessInfo) -> ChronoDuration {
        match (self.last_check_instant, info.started_instant) {
            (Some(check), Some(start)) => match check.checked_duration_since(start) {
                Some(duration) => ChronoDuration::from_std(duration).unwrap_or(ChronoDuration::MAX),
                None => -ChronoDuration::from_std(start.duration_since(check))
                    .unwrap_or(ChronoDuration::MAX),
            },
            _ => self.last_check_time - info.time,
        }
    }

    /// Go `memoryUsageAlarm.recordSQL`.
    fn record_sql(&self, sm: &dyn SessionManager, record_dir: &Path) -> Result<(), String> {
        let process_info = sm.show_process_list();
        let mut pinfo: Vec<Arc<ProcessInfo>> = process_info
            .into_iter()
            .filter(|info| !info.info.is_empty())
            .collect();
        let file_name = record_dir.join("running_sql");
        let mut file = fs::File::create(&file_name).map_err(|err| {
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
        })?;
        self.print_top10_sql_info(&mut pinfo, &mut file);
        Ok(())
    }
}

/// Native `os.RemoveAll`: remove a directory tree or one non-directory entry,
/// and treat an already-missing path as success.
fn remove_all(path: &Path) -> io::Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_dir() => fs::remove_dir_all(path),
        Ok(_) => fs::remove_file(path),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

/// Go `memoryUsageAlarm.recordProfile`.
fn record_profile(record_dir: &Path) -> io::Result<()> {
    write_heap_profile(record_dir)?;
    record_thread_profile(record_dir)
}

/// Native sampled allocation profile written under Go's `heap` profile
/// filename.
fn write_heap_profile(record_dir: &Path) -> io::Result<()> {
    let profile = record_dir.join("heap");
    if let Err(err) = fs::File::create(&profile) {
        log_profile_error("create heap profile file fail", &err);
        return Err(err);
    }
    #[cfg(feature = "jemalloc")]
    {
        return tidb_allocator_stats::dump(&profile).inspect_err(|err| {
            log_profile_error("write heap profile file fail", err);
        });
    }

    #[cfg(not(feature = "jemalloc"))]
    {
        let err = io::Error::new(
            io::ErrorKind::Unsupported,
            "heap profiling requires the production jemalloc build",
        );
        log_profile_error("write heap profile file fail", &err);
        Err(err)
    }
}

const GOROUTINE_PROFILE_BUFFER_SIZE: usize = 1 << 26;

struct StackProfileBuffer {
    bytes: Vec<u8>,
    written: usize,
    truncated: bool,
}

impl StackProfileBuffer {
    fn new() -> Self {
        Self {
            // Go uses `make([]byte, 1<<26)`, so retain its eager 64 MiB
            // allocation rather than allowing Vec to grow on demand.
            bytes: vec![0; GOROUTINE_PROFILE_BUFFER_SIZE],
            written: 0,
            truncated: false,
        }
    }

    fn written(&self) -> &[u8] {
        &self.bytes[..self.written]
    }
}

impl Write for StackProfileBuffer {
    fn write(&mut self, source: &[u8]) -> io::Result<usize> {
        let remaining = self.bytes.len().saturating_sub(self.written);
        let copied = remaining.min(source.len());
        self.bytes[self.written..self.written + copied].copy_from_slice(&source[..copied]);
        self.written += copied;
        self.truncated |= copied != source.len();
        // `runtime.Stack` truncates rather than reporting a write failure.
        Ok(source.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Native thread stacks written with Go's fixed 64 MiB all-goroutine buffer.
fn record_thread_profile(record_dir: &Path) -> io::Result<()> {
    let mut file = fs::File::create(record_dir.join("goroutine")).inspect_err(|err| {
        log_profile_error("create goroutine profile file fail", err);
    })?;
    let mut stack = StackProfileBuffer::new();
    let result = (|| -> io::Result<()> {
        #[cfg(target_os = "linux")]
        {
            let mut tasks = fs::read_dir("/proc/self/task")?.collect::<Result<Vec<_>, _>>()?;
            tasks.sort_by_key(std::fs::DirEntry::file_name);
            for task in tasks {
                let id = task.file_name();
                writeln!(stack, "thread {}:", id.to_string_lossy())?;
                if let Ok(name) = fs::read_to_string(task.path().join("comm")) {
                    writeln!(stack, "name {}", name.trim_end())?;
                }
                if let Ok(task_stack) = fs::read_to_string(task.path().join("stack")) {
                    stack.write_all(task_stack.as_bytes())?;
                }
                writeln!(stack)?;
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            writeln!(stack, "thread {:?}:", std::thread::current().id())?;
            writeln!(stack, "{:?}", std::backtrace::Backtrace::force_capture())?;
        }
        if stack.truncated {
            bg_logger().warn(
                "goroutine stack trace is too large, truncating",
                &[Field::new("size", Value::I64(stack.written as i64))],
            );
        }
        file.write_all(stack.written())?;
        Ok(())
    })();
    if let Err(err) = &result {
        log_profile_error("write goroutine profile file fail", err);
    }
    result
}

fn log_profile_error(message: &str, err: &io::Error) {
    bg_logger().error(
        message,
        &[Field::new(
            "error",
            Value::Error {
                basic: err.to_string(),
                verbose: None,
            },
        )],
    );
}

/// Package-private benchmark bridge for Go's unexported
/// `recordGoroutineProfile` benchmark target.
#[cfg(feature = "testexport")]
pub fn record_goroutine_profile_for_benchmark(record_dir: &Path) -> io::Result<()> {
    record_thread_profile(record_dir)
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

/// Go `util.GenLogFields` (`pkg/util/util.go`) over an ownership-safe process
/// snapshot. Rust snapshots the statement fields while the registry lock is
/// held, so Go's `RefCountOfStmtCtx.TryIncrease` lifetime guard has no
/// separate runtime branch here.
fn gen_log_fields(cost_time: ChronoDuration, info: &ProcessInfo) -> Vec<Field> {
    let mut log_fields = Vec::with_capacity(20);
    let nanos = cost_time.num_nanoseconds().unwrap_or_else(|| {
        if cost_time < ChronoDuration::zero() {
            i64::MIN
        } else {
            i64::MAX
        }
    });
    let secs = nanos as f64 / 1_000_000_000.0;
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
    let sql = tidb_parser::normalize(&info.info, info.redact_sql);
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

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
                log_dir: std::env::temp_dir(),
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

    struct RestoreServerMemoryLimit(u64);

    impl RestoreServerMemoryLimit {
        fn set(value: u64) -> Self {
            Self(crate::memory::SERVER_MEMORY_LIMIT.swap(value, Ordering::SeqCst))
        }
    }

    impl Drop for RestoreServerMemoryLimit {
        fn drop(&mut self) {
            crate::memory::SERVER_MEMORY_LIMIT.store(self.0, Ordering::SeqCst);
        }
    }

    fn new_record(ratio: f64, keep_num: i64) -> MemoryUsageAlarm {
        MemoryUsageAlarm::new(Arc::new(MockConfigProvider::new(
            ratio,
            keep_num,
            "test-component",
        )))
    }

    /// Go test `TestIfNeedDoRecord`.
    #[test]
    fn test_if_need_do_record() {
        let _test_guard = crate::global_logger_test_guard();
        let _restore = RestoreServerMemoryLimit::set(16 << 30);
        let mut record = new_record(0.7, 5);
        record.init_memory_usage_alarm_record();

        // mem usage ratio < 70% will not be recorded
        let mem_used = (0.69 * record.server_memory_limit as f64) as u64;
        let (need_record, reason) = record.need_record(mem_used);
        assert!(!need_record);
        assert_eq!(AlarmReason::NoReason, reason);

        // mem usage ratio > 70% will not be recorded
        let mem_used = (0.71 * record.server_memory_limit as f64) as u64;
        let (need_record, reason) = record.need_record(mem_used);
        assert!(need_record);
        assert_eq!(AlarmReason::ExceedAlarmRatio, reason);
        record.last_check_time = Utc::now();
        record.last_check_instant = Some(Instant::now());
        record.last_record_mem_used = mem_used;

        // check time - last record time < 60s will not be recorded
        let mem_used = (0.71 * record.server_memory_limit as f64) as u64;
        let (need_record, reason) = record.need_record(mem_used);
        assert!(!need_record);
        assert_eq!(AlarmReason::NoReason, reason);

        // check time - last record time > 60s will be recorded
        record.last_check_time -= ChronoDuration::seconds(60);
        record.last_check_instant = record
            .last_check_instant
            .and_then(|instant| instant.checked_sub(Duration::from_secs(60)));
        let mem_used = (0.71 * record.server_memory_limit as f64) as u64;
        let (need_record, reason) = record.need_record(mem_used);
        assert!(need_record);
        assert_eq!(AlarmReason::ExceedAlarmRatio, reason);
        record.last_check_time = Utc::now();
        record.last_check_instant = Some(Instant::now());
        record.last_record_mem_used = mem_used;

        // mem usage ratio - last mem usage ratio < 10% will not be recorded
        let mem_used = (0.80 * record.server_memory_limit as f64) as u64;
        let (need_record, reason) = record.need_record(mem_used);
        assert!(!need_record);
        assert_eq!(AlarmReason::NoReason, reason);

        // mem usage ratio - last mem usage ratio > 10% will be recorded even
        // though check time - last record time < 60s
        let mem_used = (0.82 * record.server_memory_limit as f64) as u64;
        let (need_record, reason) = record.need_record(mem_used);
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
        let _test_guard = crate::global_logger_test_guard();
        let _restore = RestoreServerMemoryLimit::set(0);
        let mut record = new_record(0.7, 5);
        record.init_memory_usage_alarm_record();
        record.last_check_time = gen_time(123_456);

        let mut process_info_list = gen_mock_process_info_list(
            &[1000, 87_263_523, 34_223],
            &[gen_time(1234), gen_time(123_456), gen_time(12)],
            3,
        );
        let actual = record.get_top10_sql_info_by_memory_usage(&mut process_info_list);
        assert_eq!(
            "SQL 0: \ncost_time: 0s\ntxn_start_ts: 0\nmem_max: 87263523 Bytes (83.2 MB)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\nSQL 1: \ncost_time: 123444s\ntxn_start_ts: 0\nmem_max: 34223 Bytes (33.4 KB)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\nSQL 2: \ncost_time: 122222s\ntxn_start_ts: 0\nmem_max: 1000 Bytes (1000 Bytes)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\n\n",
            actual
        );
        let actual = record.get_top10_sql_info_by_cost_time(&mut process_info_list);
        assert_eq!("SQL 0: \ncost_time: 123444s\ntxn_start_ts: 0\nmem_max: 34223 Bytes (33.4 KB)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\nSQL 1: \ncost_time: 122222s\ntxn_start_ts: 0\nmem_max: 1000 Bytes (1000 Bytes)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\nSQL 2: \ncost_time: 0s\ntxn_start_ts: 0\nmem_max: 87263523 Bytes (83.2 MB)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\n\n", actual);

        let mut process_info_list = gen_mock_process_info_list(
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
        let actual = record.get_top10_sql_info_by_memory_usage(&mut process_info_list);
        assert_eq!("SQL 0: \ncost_time: 111222s\ntxn_start_ts: 0\nmem_max: 12515134234 Bytes (11.7 GB)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\nSQL 1: \ncost_time: 120241s\ntxn_start_ts: 0\nmem_max: 231231515 Bytes (220.5 MB)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\nSQL 2: \ncost_time: 110941s\ntxn_start_ts: 0\nmem_max: 123225151 Bytes (117.5 MB)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\nSQL 3: \ncost_time: 101234s\ntxn_start_ts: 0\nmem_max: 123123123 Bytes (117.4 MB)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\nSQL 4: \ncost_time: 0s\ntxn_start_ts: 0\nmem_max: 87263523 Bytes (83.2 MB)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\nSQL 5: \ncost_time: 112345s\ntxn_start_ts: 0\nmem_max: 15263236 Bytes (14.6 MB)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\nSQL 6: \ncost_time: 120215s\ntxn_start_ts: 0\nmem_max: 532355 Bytes (519.9 KB)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\nSQL 7: \ncost_time: 123444s\ntxn_start_ts: 0\nmem_max: 34223 Bytes (33.4 KB)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\nSQL 8: \ncost_time: 122944s\ntxn_start_ts: 0\nmem_max: 12414 Bytes (12.1 KB)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\nSQL 9: \ncost_time: 62142s\ntxn_start_ts: 0\nmem_max: 12312 Bytes (12.0 KB)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\n\n", actual);
        let actual = record.get_top10_sql_info_by_cost_time(&mut process_info_list);
        assert_eq!("SQL 0: \ncost_time: 123444s\ntxn_start_ts: 0\nmem_max: 34223 Bytes (33.4 KB)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\nSQL 1: \ncost_time: 122944s\ntxn_start_ts: 0\nmem_max: 12414 Bytes (12.1 KB)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\nSQL 2: \ncost_time: 122333s\ntxn_start_ts: 0\nmem_max: 232 Bytes (232 Bytes)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\nSQL 3: \ncost_time: 122222s\ntxn_start_ts: 0\nmem_max: 1000 Bytes (1000 Bytes)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\nSQL 4: \ncost_time: 120241s\ntxn_start_ts: 0\nmem_max: 231231515 Bytes (220.5 MB)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\nSQL 5: \ncost_time: 120215s\ntxn_start_ts: 0\nmem_max: 532355 Bytes (519.9 KB)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\nSQL 6: \ncost_time: 117944s\ntxn_start_ts: 0\nmem_max: 15 Bytes (15 Bytes)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\nSQL 7: \ncost_time: 112345s\ntxn_start_ts: 0\nmem_max: 15263236 Bytes (14.6 MB)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\nSQL 8: \ncost_time: 111222s\ntxn_start_ts: 0\nmem_max: 12515134234 Bytes (11.7 GB)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\nSQL 9: \ncost_time: 110941s\ntxn_start_ts: 0\nmem_max: 123225151 Bytes (117.5 MB)\nsql: \nsession_alias: \naffected rows: 0\ntidb_mem_oom_action: CANCEL\ntidb_server_memory_limit: 0\ntidb_mem_quota_query: 0\ntidb_analyze_version: 0\ntidb_enable_rate_limit_action: false\ncurrent_analyze_plan: |id|estRows|task|access object|operator info|\n\n", actual);
    }

    /// Go test `TestUpdateVariables`.
    #[test]
    fn test_update_variables() {
        let _test_guard = crate::global_logger_test_guard();
        let _restore = RestoreServerMemoryLimit::set(1024);
        let mock_config = Arc::new(MockConfigProvider::new(0.3, 3, "test-component"));
        let config_provider: Arc<dyn ConfigProvider> = mock_config.clone();
        let mut record = MemoryUsageAlarm::new(config_provider);

        record.init_memory_usage_alarm_record();
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
        crate::memory::SERVER_MEMORY_LIMIT.store(2048, Ordering::SeqCst);

        record.update_variable();
        assert_eq!(0.6, record.config_provider.get_memory_usage_alarm_ratio());
        assert_eq!(
            6,
            record
                .config_provider
                .get_memory_usage_alarm_keep_record_num()
        );
        assert_eq!(1024, record.server_memory_limit);
        record.last_update_variable_time -= ChronoDuration::seconds(60);
        record.last_update_variable_instant = record
            .last_update_variable_instant
            .and_then(|instant| instant.checked_sub(Duration::from_secs(60)));
        record.update_variable();
        assert_eq!(0.6, record.config_provider.get_memory_usage_alarm_ratio());
        assert_eq!(
            6,
            record
                .config_provider
                .get_memory_usage_alarm_keep_record_num()
        );
        assert_eq!(2048, record.server_memory_limit);
    }

    /// Go test `TestRecordGoroutineProfileWithBackgroundGoroutine`.
    #[test]
    fn test_record_goroutine_profile_with_background_goroutine() {
        let record_dir = tempfile::tempdir().unwrap();
        record_thread_profile(record_dir.path()).unwrap();
        let content = fs::read_to_string(record_dir.path().join("goroutine")).unwrap();
        assert!(!content.is_empty());
        assert!(content.contains("thread "));
    }
}
