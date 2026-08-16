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

//! Transcreation of Go `pkg/util/logutil`: the global/slow-query/general
//! loggers over the unified log format, contextual log fields, and the
//! sampled-logger factories.
//!
//! Faithful adaptations, none changing the on-disk log contract:
//! - zap loggers become [`Logger`] handles rendering through `tidb-log`'s
//!   `TextEncoder`; the caller (`file:line`) comes from `#[track_caller]`
//!   at the emit call site, filtered exactly like Go `getCallerString`.
//! - Go `context.Context` value plumbing (`WithConnID`, `WithFields`, ...)
//!   becomes explicit [`Logger`] composition: each helper returns a logger
//!   carrying the fields, which callers thread instead of a context.
//! - Loggers that reuse the global filename share one [`SharedSink`] (the
//!   syncer-identity contract the Go tests assert); separately constructed
//!   dedicated loggers retain independent writers even when their filenames
//!   happen to match.
//! - `initGRPCLogger`, opentracing `Event`/`SetTag`, and the
//!   runtime/trace `WithTraceLogger` tee are Go-ecosystem integrations
//!   with no behavioral log-format contract: not ported.
//! - `hex.go`'s reflection pretty-printer is ported over an explicit
//!   [`hex::PrettyValue`] tree (Rust has no runtime struct reflection);
//!   proto types build the tree via their generated field lists.

pub mod file_sink;
pub mod hex;

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU8, Ordering::SeqCst};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use chrono::{DateTime, FixedOffset, Local};
use tidb_log::{Entry, Field, Level, TextEncoder};

use file_sink::{RotatingFile, SharedSink, Sink};

/// Default size of log files in MB (Go `DefaultLogMaxSize`).
pub const DEFAULT_LOG_MAX_SIZE: i64 = 300;
/// Default log format (Go `DefaultLogFormat`).
pub const DEFAULT_LOG_FORMAT: &str = "text";
/// Default slow log threshold in milliseconds (Go `DefaultSlowThreshold`).
pub const DEFAULT_SLOW_THRESHOLD: u64 = 300;
/// Default slow txn log threshold (Go `DefaultSlowTxnThreshold`).
pub const DEFAULT_SLOW_TXN_THRESHOLD: u64 = 0;
/// Default max query length in the log (Go `DefaultQueryLogMaxLen`).
pub const DEFAULT_QUERY_LOG_MAX_LEN: u64 = 4096;
/// Default for recording plan in slow log (Go `DefaultRecordPlanInSlowLog`).
pub const DEFAULT_RECORD_PLAN_IN_SLOW_LOG: u32 = 1;
/// Default for enabling the slow log (Go `DefaultTiDBEnableSlowLog`).
pub const DEFAULT_TIDB_ENABLE_SLOW_LOG: bool = true;

/// Log field name for category (Go `LogFieldCategory`).
pub const LOG_FIELD_CATEGORY: &str = "category";
/// Log field name for connection id (Go `LogFieldConn`).
pub const LOG_FIELD_CONN: &str = "conn";
/// Log field name for session alias (Go `LogFieldSessionAlias`).
pub const LOG_FIELD_SESSION_ALIAS: &str = "session_alias";

/// File log config (Go `FileLogConfig` wrapping `pingcap/log`'s).
pub type FileLogConfig = tidb_log::FileLogConfig;

/// Creates a `FileLogConfig` with a max size (Go `NewFileLogConfig`).
pub fn new_file_log_config(max_size: i64) -> FileLogConfig {
    FileLogConfig {
        max_size,
        ..Default::default()
    }
}

/// Log config (Go `LogConfig` wrapping `pingcap/log`'s `Config`).
#[derive(Clone, PartialEq, Debug, Default)]
pub struct LogConfig {
    /// The wrapped `pingcap/log` config.
    pub config: tidb_log::Config,
    /// Slow query log filename; defaults to the file log config on empty.
    pub slow_query_file: String,
    /// General log filename; defaults to the file log config on empty.
    pub general_log_file: String,
}

/// Creates a `LogConfig` (Go `NewLogConfig`).
pub fn new_log_config(
    level: &str,
    format: &str,
    slow_query_file: &str,
    general_log_file: &str,
    file_cfg: FileLogConfig,
    disable_timestamp: bool,
) -> LogConfig {
    LogConfig {
        config: tidb_log::Config {
            level: level.to_string(),
            format: format.to_string(),
            disable_timestamp,
            file: file_cfg,
            ..Default::default()
        },
        slow_query_file: slow_query_file.to_string(),
        general_log_file: general_log_file.to_string(),
    }
}

/// Slow log time format is RFC3339Nano (Go `SlowLogTimeFormat`).
pub fn format_slow_log_time(t: &DateTime<FixedOffset>) -> String {
    let mut formatted = t.format("%Y-%m-%dT%H:%M:%S").to_string();
    let nanos = t.timestamp_subsec_nanos();
    if nanos != 0 {
        let mut fraction = format!("{nanos:09}");
        while fraction.ends_with('0') {
            fraction.pop();
        }
        formatted.push('.');
        formatted.push_str(&fraction);
    }
    if t.offset().local_minus_utc() == 0 {
        formatted.push('Z');
    } else {
        formatted.push_str(&t.format("%:z").to_string());
    }
    formatted
}

fn level_ord(l: Level) -> u8 {
    match l {
        Level::Debug => 0,
        Level::Info => 1,
        Level::Warn => 2,
        Level::Error => 3,
        Level::DPanic => 4,
        Level::Panic => 5,
        Level::Fatal => 6,
    }
}

/// A settable level shared between loggers (zap `AtomicLevel`).
#[derive(Clone)]
pub struct AtomicLevel(Arc<AtomicU8>);

impl AtomicLevel {
    fn new(l: Level) -> AtomicLevel {
        AtomicLevel(Arc::new(AtomicU8::new(level_ord(l))))
    }
    fn enabled(&self, l: Level) -> bool {
        level_ord(l) >= self.0.load(SeqCst)
    }
    fn set(&self, l: Level) {
        self.0.store(level_ord(l), SeqCst);
    }
    /// The current level, for tests/introspection.
    pub fn get(&self) -> Level {
        match self.0.load(SeqCst) {
            0 => Level::Debug,
            1 => Level::Info,
            2 => Level::Warn,
            3 => Level::Error,
            4 => Level::DPanic,
            5 => Level::Panic,
            _ => Level::Fatal,
        }
    }
}

/// Parses a zap level string, case-insensitively.
pub fn parse_level(s: &str) -> Result<Level, String> {
    match s.to_lowercase().as_str() {
        "debug" => Ok(Level::Debug),
        "info" | "" => Ok(Level::Info),
        "warn" => Ok(Level::Warn),
        "error" => Ok(Level::Error),
        "dpanic" => Ok(Level::DPanic),
        "panic" => Ok(Level::Panic),
        "fatal" => Ok(Level::Fatal),
        _ => Err(format!("unrecognized level: {s:?}")),
    }
}

#[derive(Clone)]
enum Encoding {
    Unified,
    /// Slow-log format: `# Time: <RFC3339Nano>\n<message>\n`.
    SlowLog,
}

struct LoggerInner {
    encoder: TextEncoder,
    encoding: Encoding,
    sink: SharedSink,
    level: AtomicLevel,
}

/// A logger handle (the zap `*zap.Logger` stand-in).
#[derive(Clone)]
pub struct Logger {
    inner: Arc<LoggerInner>,
}

impl Logger {
    fn new(
        encoder: TextEncoder,
        encoding: Encoding,
        sink: SharedSink,
        level: AtomicLevel,
    ) -> Logger {
        Logger {
            inner: Arc::new(LoggerInner {
                encoder,
                encoding,
                sink,
                level,
            }),
        }
    }

    /// A no-op-ish default logger writing to stdout at info level.
    pub fn stdout() -> Logger {
        Logger::new(
            TextEncoder::default(),
            Encoding::Unified,
            Arc::new(Mutex::new(Sink::Stdout)),
            AtomicLevel::new(Level::Info),
        )
    }

    /// Returns a logger with additional context fields (zap `With`).
    pub fn with_fields(&self, fields: &[Field]) -> Logger {
        Logger {
            inner: Arc::new(LoggerInner {
                encoder: self.inner.encoder.with_fields(fields),
                encoding: self.inner.encoding.clone(),
                sink: Arc::clone(&self.inner.sink),
                level: self.inner.level.clone(),
            }),
        }
    }

    /// The logger's level (test surface).
    pub fn level(&self) -> Level {
        self.inner.level.get()
    }

    /// Whether the two loggers share one write syncer (test surface for
    /// the Go syncer-identity assertions).
    pub fn same_sink(&self, other: &Logger) -> bool {
        Arc::ptr_eq(&self.inner.sink, &other.inner.sink)
    }

    fn enabled(&self, level: Level) -> bool {
        self.inner.level.enabled(level)
    }

    #[track_caller]
    fn emit(&self, level: Level, msg: &str, fields: &[Field]) {
        if !self.enabled(level) {
            return;
        }
        let loc = std::panic::Location::caller();
        let now: DateTime<FixedOffset> = Local::now().fixed_offset();
        let line = match self.inner.encoding {
            Encoding::Unified => {
                let ent = Entry {
                    time: now,
                    level,
                    logger_name: String::new(),
                    caller: Some((loc.file().to_string(), loc.line())),
                    message: msg.to_string(),
                    stack: String::new(),
                };
                self.inner.encoder.encode_entry(&ent, fields)
            }
            Encoding::SlowLog => {
                format!("# Time: {}\n{}\n", format_slow_log_time(&now), msg)
            }
        };
        self.inner.sink.lock().unwrap().write_line(&line);
    }

    /// Debug-level log.
    #[track_caller]
    pub fn debug(&self, msg: &str, fields: &[Field]) {
        self.emit(Level::Debug, msg, fields);
    }
    /// Info-level log.
    #[track_caller]
    pub fn info(&self, msg: &str, fields: &[Field]) {
        self.emit(Level::Info, msg, fields);
    }
    /// Warn-level log.
    #[track_caller]
    pub fn warn(&self, msg: &str, fields: &[Field]) {
        self.emit(Level::Warn, msg, fields);
    }
    /// Error-level log.
    #[track_caller]
    pub fn error(&self, msg: &str, fields: &[Field]) {
        self.emit(Level::Error, msg, fields);
    }
}

struct Globals {
    global: Logger,
    slow_query: Logger,
    general: Logger,
    err_verbose: Logger,
    level: AtomicLevel,
}

static GLOBALS: RwLock<Option<Globals>> = RwLock::new(None);

fn read_globals<R>(f: impl FnOnce(&Globals) -> R) -> R {
    {
        let g = GLOBALS.read().unwrap();
        if let Some(g) = g.as_ref() {
            return f(g);
        }
    }
    let mut w = GLOBALS.write().unwrap();
    if w.is_none() {
        let level = AtomicLevel::new(Level::Info);
        let l = Logger::new(
            TextEncoder::default(),
            Encoding::Unified,
            Arc::new(Mutex::new(Sink::Stdout)),
            level.clone(),
        );
        *w = Some(Globals {
            global: l.clone(),
            slow_query: l.clone(),
            general: l.clone(),
            err_verbose: l,
            level,
        });
    }
    f(w.as_ref().unwrap())
}

/// The background logger (Go `BgLogger`).
pub fn bg_logger() -> Logger {
    read_globals(|g| g.global.clone())
}

/// The slow query logger (Go `SlowQueryLogger`).
pub fn slow_query_logger() -> Logger {
    read_globals(|g| g.slow_query.clone())
}

/// The general logger (Go `GeneralLogger`).
pub fn general_logger() -> Logger {
    read_globals(|g| g.general.clone())
}

/// The always-error-verbose logger (Go `ErrVerboseLogger`).
pub fn err_verbose_logger() -> Logger {
    read_globals(|g| g.err_verbose.clone())
}

/// Sets the global log level (Go `SetLevel`).
pub fn set_level(level: &str) -> Result<(), String> {
    let l = parse_level(level)?;
    read_globals(|g| g.level.set(l));
    Ok(())
}

/// The current global level (Go `log.GetLevel`, test surface).
pub fn get_level() -> Level {
    read_globals(|g| g.level.get())
}

fn validate_compression(c: &str) -> Result<bool, String> {
    match c {
        "" => Ok(false),
        "gzip" => Ok(true),
        other => Err(format!("unsupported compression: {other}")),
    }
}

fn build_sink(file: &FileLogConfig) -> Result<SharedSink, String> {
    if file.filename.is_empty() {
        return Ok(Arc::new(Mutex::new(Sink::Stdout)));
    }
    let compress = validate_compression(&file.compression)?;
    let rf = RotatingFile::open(
        Path::new(&file.filename),
        file.max_size,
        file.max_backups,
        compress,
    )
    .map_err(|e| e.to_string())?;
    Ok(Arc::new(Mutex::new(Sink::File(rf))))
}

// Go `newSlowQueryLogConfig`/`newGeneralLogConfig`: same file settings,
// dedicated filename, level pinned to the default (info).
fn sub_file_config(cfg: &LogConfig, filename: &str) -> FileLogConfig {
    let mut file = cfg.config.file.clone();
    if !filename.is_empty() {
        file.filename = filename.to_string();
    }
    file
}

/// Initializes global, slow-query, and general loggers (Go `InitLogger`).
pub fn init_logger(cfg: &LogConfig) -> Result<(), String> {
    init_logger_with_core_fields(cfg, &[])
}

/// Go `InitLogger(cfg, zap.WrapCore(core.With(fields)))`: initializes the
/// logger after attaching fields to its core, so every emitted entry carries
/// them even when the call site supplies no context fields.
pub fn init_logger_with_core_fields(cfg: &LogConfig, core_fields: &[Field]) -> Result<(), String> {
    let level = AtomicLevel::new(parse_level(&cfg.config.level)?);

    let encoder = TextEncoder::new(&cfg.config)?.with_fields(core_fields);
    let global_sink = build_sink(&cfg.config.file)?;
    let global = Logger::new(
        encoder,
        Encoding::Unified,
        Arc::clone(&global_sink),
        level.clone(),
    );

    // Error-verbose logger: same sink, never suppresses verbose errors.
    let err_verbose = if !cfg.config.disable_error_verbose {
        global.clone()
    } else {
        let mut c = cfg.config.clone();
        c.disable_error_verbose = false;
        Logger::new(
            TextEncoder::new(&c)?.with_fields(core_fields),
            Encoding::Unified,
            Arc::clone(&global_sink),
            level.clone(),
        )
    };

    // Slow/general loggers reuse the global level and sink when their
    // configured filename is empty or names the global file. Dedicated
    // loggers are initialized independently at the default info level.
    let slow_query =
        if cfg.slow_query_file.is_empty() || cfg.slow_query_file == cfg.config.file.filename {
            Logger::new(
                TextEncoder::default(),
                Encoding::SlowLog,
                Arc::clone(&global_sink),
                level.clone(),
            )
        } else {
            let file = sub_file_config(cfg, &cfg.slow_query_file);
            let sink = build_sink(&file)?;
            Logger::new(
                TextEncoder::default(),
                Encoding::SlowLog,
                sink,
                AtomicLevel::new(Level::Info),
            )
        };

    let general =
        if cfg.general_log_file.is_empty() || cfg.general_log_file == cfg.config.file.filename {
            global.clone()
        } else {
            let file = sub_file_config(cfg, &cfg.general_log_file);
            let sink = build_sink(&file)?;
            Logger::new(
                TextEncoder::new(&cfg.config)?,
                Encoding::Unified,
                sink,
                AtomicLevel::new(Level::Info),
            )
        };

    *GLOBALS.write().unwrap() = Some(Globals {
        global,
        slow_query,
        general,
        err_verbose,
        level,
    });
    Ok(())
}

/// Replaces the global loggers (Go `ReplaceLogger`).
pub fn replace_logger(cfg: &LogConfig) -> Result<(), String> {
    let level = AtomicLevel::new(parse_level(&cfg.config.level)?);

    let global_sink = build_sink(&cfg.config.file)?;
    let global = Logger::new(
        TextEncoder::new(&cfg.config)?,
        Encoding::Unified,
        global_sink,
        level.clone(),
    );

    // Go ReplaceLogger calls both dedicated constructors unconditionally.
    // Each constructor initializes its own write syncer, even when an empty
    // sub-logger filename resolves to the same path as the global logger.
    let slow_file = sub_file_config(cfg, &cfg.slow_query_file);
    let slow_sink = build_sink(&slow_file)?;
    let slow_query = Logger::new(
        TextEncoder::default(),
        Encoding::SlowLog,
        slow_sink,
        AtomicLevel::new(Level::Info),
    );

    let general_file = sub_file_config(cfg, &cfg.general_log_file);
    let general_sink = build_sink(&general_file)?;
    let general = Logger::new(
        TextEncoder::new(&cfg.config)?,
        Encoding::Unified,
        general_sink,
        AtomicLevel::new(Level::Info),
    );

    // Go ReplaceLogger does not update errVerboseLogger.
    let err_verbose = err_verbose_logger();
    *GLOBALS.write().unwrap() = Some(Globals {
        global,
        slow_query,
        general,
        err_verbose,
        level,
    });

    let config = serde_json::to_string(&cfg.config).map_err(|e| e.to_string())?;
    bg_logger().info(
        &format!("replaced global logger with config: {config}"),
        &[],
    );
    Ok(())
}

// ----- contextual loggers (Go context plumbing -> logger composition) -----

/// Go `WithConnID`: a logger carrying `conn`.
pub fn with_conn_id(base: &Logger, conn_id: u64) -> Logger {
    base.with_fields(&[Field::new(LOG_FIELD_CONN, tidb_log::Value::U64(conn_id))])
}

/// Go `WithSessionAlias`.
pub fn with_session_alias(base: &Logger, alias: &str) -> Logger {
    base.with_fields(&[Field::new(
        LOG_FIELD_SESSION_ALIAS,
        tidb_log::Value::Str(alias.to_string()),
    )])
}

/// Go `WithCategory`.
pub fn with_category(base: &Logger, category: &str) -> Logger {
    base.with_fields(&[Field::new(
        LOG_FIELD_CATEGORY,
        tidb_log::Value::Str(category.to_string()),
    )])
}

/// Trace info attached to logs. Go's `logutil` imports this straight from
/// `pkg/util/tracing`, so the type has one owner here too.
pub use crate::tracing::TraceInfo;

/// Go `fieldsFromTraceInfo`.
pub fn fields_from_trace_info(info: Option<&TraceInfo>) -> Vec<Field> {
    let Some(info) = info else { return Vec::new() };
    let mut fields = Vec::with_capacity(2);
    if info.connection_id != 0 {
        fields.push(Field::new(
            LOG_FIELD_CONN,
            tidb_log::Value::U64(info.connection_id),
        ));
    }
    if !info.session_alias.is_empty() {
        fields.push(Field::new(
            LOG_FIELD_SESSION_ALIAS,
            tidb_log::Value::Str(info.session_alias.clone()),
        ));
    }
    fields
}

/// Go `LoggerWithTraceInfo`.
pub fn logger_with_trace_info(logger: &Logger, info: Option<&TraceInfo>) -> Logger {
    let fields = fields_from_trace_info(info);
    if fields.is_empty() {
        logger.clone()
    } else {
        logger.with_fields(&fields)
    }
}

/// Go `WithTraceFields`: conn + session_alias unconditionally.
pub fn with_trace_fields(base: &Logger, info: Option<&TraceInfo>) -> Logger {
    match info {
        None => base.clone(),
        Some(info) => base.with_fields(&[
            Field::new(LOG_FIELD_CONN, tidb_log::Value::U64(info.connection_id)),
            Field::new(
                LOG_FIELD_SESSION_ALIAS,
                tidb_log::Value::Str(info.session_alias.clone()),
            ),
        ]),
    }
}

// ----- env proxies -----

/// One proxy env observation.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ProxyField {
    /// Field key (`http_proxy` / `https_proxy` / `no_proxy`).
    pub key: &'static str,
    /// The env value.
    pub value: String,
}

/// Go `proxyFields` (`httpproxy.FromEnvironment` reads uppercase first, then
/// lowercase).
pub fn proxy_fields() -> Vec<ProxyField> {
    let get = |upper: &str, lower: &str| {
        std::env::var(upper)
            .ok()
            .filter(|v| !v.is_empty())
            .or_else(|| std::env::var(lower).ok().filter(|v| !v.is_empty()))
    };
    let mut fields = Vec::with_capacity(3);
    if let Some(v) = get("HTTP_PROXY", "http_proxy") {
        fields.push(ProxyField {
            key: "http_proxy",
            value: v,
        });
    }
    if let Some(v) = get("HTTPS_PROXY", "https_proxy") {
        fields.push(ProxyField {
            key: "https_proxy",
            value: v,
        });
    }
    if let Some(v) = get("NO_PROXY", "no_proxy") {
        fields.push(ProxyField {
            key: "no_proxy",
            value: v,
        });
    }
    fields
}

/// Logs proxy env variables (Go `LogEnvVariables`).
pub fn log_env_variables() {
    let fields = proxy_fields();
    if !fields.is_empty() {
        let rendered: Vec<Field> = fields
            .iter()
            .map(|f| Field::new(f.key, tidb_log::Value::Str(f.value.clone())))
            .collect();
        bg_logger().info("using proxy config", &rendered);
    }
}

// ----- sampled loggers -----

struct SampleWindow {
    started: Instant,
    count: u64,
}

struct SamplerState {
    windows: HashMap<(Level, u16), SampleWindow>,
}

fn sampler_bucket(message: &str) -> u16 {
    const OFFSET_32: u32 = 2_166_136_261;
    const PRIME_32: u32 = 16_777_619;
    const COUNTERS_PER_LEVEL: u32 = 4096;

    let hash = message.bytes().fold(OFFSET_32, |hash, byte| {
        (hash ^ u32::from(byte)).wrapping_mul(PRIME_32)
    });
    (hash % COUNTERS_PER_LEVEL) as u16
}

/// A sampling logger: logs the first `first` entries per level and hashed
/// message bucket per `tick` window (zap `NewSamplerWithOptions`, whose fixed
/// 4096 counters per level can intentionally collide, with thereafter=0).
#[derive(Clone)]
pub struct SampledLogger {
    inner: Logger,
    tick: Duration,
    first: u64,
    state: Arc<Mutex<SamplerState>>,
}

impl SampledLogger {
    /// Info-level sampled log.
    #[track_caller]
    pub fn info(&self, msg: &str, fields: &[Field]) {
        if self.inner.enabled(Level::Info) && self.admit(Level::Info, msg) {
            self.inner.info(msg, fields);
        }
    }
    /// Debug-level sampled log.
    #[track_caller]
    pub fn debug(&self, msg: &str, fields: &[Field]) {
        if self.inner.enabled(Level::Debug) && self.admit(Level::Debug, msg) {
            self.inner.debug(msg, fields);
        }
    }
    /// Warn-level sampled log.
    #[track_caller]
    pub fn warn(&self, msg: &str, fields: &[Field]) {
        if self.inner.enabled(Level::Warn) && self.admit(Level::Warn, msg) {
            self.inner.warn(msg, fields);
        }
    }
    /// Error-level sampled log.
    #[track_caller]
    pub fn error(&self, msg: &str, fields: &[Field]) {
        if self.inner.enabled(Level::Error) && self.admit(Level::Error, msg) {
            self.inner.error(msg, fields);
        }
    }

    fn admit(&self, level: Level, msg: &str) -> bool {
        let mut st = self.state.lock().unwrap();
        let now = Instant::now();
        let window = st
            .windows
            .entry((level, sampler_bucket(msg)))
            .or_insert(SampleWindow {
                started: now,
                count: 0,
            });
        if now.duration_since(window.started) >= self.tick {
            window.started = now;
            window.count = 0;
        }
        window.count = window.count.wrapping_add(1);
        window.count <= self.first
    }
}

fn sampled(base: Logger, tick: Duration, first: u64, fields: &[Field]) -> SampledLogger {
    let mut with = fields.to_vec();
    with.push(Field::new("sampled", tidb_log::Value::Str(String::new())));
    SampledLogger {
        inner: base.with_fields(&with),
        tick,
        first,
        state: Arc::new(Mutex::new(SamplerState {
            windows: HashMap::new(),
        })),
    }
}

/// Go `SampleLoggerFactory`: one shared sampled logger per factory.
pub fn sample_logger_factory(
    tick: Duration,
    first: u64,
    fields: Vec<Field>,
) -> impl Fn() -> SampledLogger {
    let logger: Arc<Mutex<Option<SampledLogger>>> = Arc::new(Mutex::new(None));
    move || {
        let mut slot = logger.lock().unwrap();
        slot.get_or_insert_with(|| sampled(bg_logger(), tick, first, &fields))
            .clone()
    }
}

/// Go `SampleErrVerboseLoggerFactory`.
pub fn sample_err_verbose_logger_factory(
    tick: Duration,
    first: u64,
    fields: Vec<Field>,
) -> impl Fn() -> SampledLogger {
    let logger: Arc<Mutex<Option<SampledLogger>>> = Arc::new(Mutex::new(None));
    move || {
        let mut slot = logger.lock().unwrap();
        slot.get_or_insert_with(|| sampled(err_verbose_logger(), tick, first, &fields))
            .clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_log::Value;

    // The Go test binary runs these sequentially; the global-logger state
    // demands the same here. The guard is crate-wide because any test that
    // logs through the global logger can land a line in the file these tests
    // read back.
    use crate::global_logger_test_guard as guard;

    // Log-line patterns from Go main_test.go (adapted: the caller is a
    // Rust source file).
    const PATTERN_BASE: &str = r"\[\d\d\d\d/\d\d/\d\d \d\d:\d\d:\d\d\.\d\d\d (\+|-)\d\d:\d\d\] \[(FATAL|ERROR|WARN|INFO|DEBUG)\] \[[\w.\-]+:\d+\] \[.*\]";

    fn temp_file(name: &str) -> String {
        let dir = std::env::temp_dir().join(format!("tidb_logutil_test_{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        dir.join(name).to_string_lossy().to_string()
    }

    fn test_logger_output(logger: &Logger, file: &str, pattern: &str) {
        logger.debug(
            "debug msg",
            &[Field::new("test with key", Value::Str("true".into()))],
        );
        logger.info(
            "info msg",
            &[Field::new("test with key", Value::Str("true".into()))],
        );
        logger.warn(
            "warn msg",
            &[Field::new("test with key", Value::Str("true".into()))],
        );
        logger.error(
            "error msg",
            &[Field::new("test with key", Value::Str("true".into()))],
        );

        let content = std::fs::read_to_string(file).unwrap();
        let re = regex::Regex::new(pattern).unwrap();
        let mut lines = 0;
        for line in content.lines() {
            assert!(re.is_match(line), "line {line:?} !~ {pattern}");
            assert!(!line.contains("stack"));
            assert!(!line.contains("errorVerbose"));
            lines += 1;
        }
        // info/warn/error pass the level filter; debug is filtered.
        assert_eq!(lines, 3);
        let _ = std::fs::remove_file(file);
    }

    // Go TestZapLoggerWithKeys (context-field routing).
    #[test]
    fn zap_logger_with_keys() {
        let _g = guard();
        let filename = temp_file("zap_log_keys.log");
        let file_cfg = FileLogConfig {
            filename: filename.clone(),
            max_size: 4096,
            ..Default::default()
        };
        let conf = new_log_config("info", DEFAULT_LOG_FORMAT, "", "", file_cfg, false);
        init_logger(&conf).unwrap();

        let with_conn_pattern = format!(r"{PATTERN_BASE} \[conn=.*\] (\[.*=.*\])");
        let with_trace_pattern =
            format!(r"{PATTERN_BASE} \[conn=.*\] \[session_alias=.*\] (\[.*=.*\])");
        let key_val_pattern = format!(r"{PATTERN_BASE} \[ctxKey=.*\] (\[.*=.*\])");

        let logger = with_conn_id(&bg_logger(), 123);
        test_logger_output(&logger, &filename, &with_conn_pattern);

        init_logger(&conf).unwrap();
        let logger = with_session_alias(&with_conn_id(&bg_logger(), 123), "alias123");
        test_logger_output(&logger, &filename, &with_trace_pattern);

        init_logger(&conf).unwrap();
        let logger = bg_logger().with_fields(&[
            Field::new("conn", Value::I64(123)),
            Field::new("session_alias", Value::Str("alias456".into())),
        ]);
        test_logger_output(&logger, &filename, &with_trace_pattern);

        init_logger(&conf).unwrap();
        let logger = with_trace_fields(
            &bg_logger(),
            Some(&TraceInfo {
                connection_id: 456,
                session_alias: "alias789".into(),
                trace_id: Vec::new(),
            }),
        );
        test_logger_output(&logger, &filename, &with_trace_pattern);

        init_logger(&conf).unwrap();
        let logger = logger_with_trace_info(
            &bg_logger(),
            Some(&TraceInfo {
                connection_id: 789,
                session_alias: "alias012".into(),
                trace_id: Vec::new(),
            }),
        );
        test_logger_output(&logger, &filename, &with_trace_pattern);

        init_logger(&conf).unwrap();
        let logger = logger_with_trace_info(&bg_logger(), None);
        test_logger_output(&logger, &filename, &format!("{PATTERN_BASE} (\\[.*=.*\\])"));

        init_logger(&conf).unwrap();
        let logger =
            bg_logger().with_fields(&[Field::new("ctxKey", Value::Str("ctxValue".into()))]);
        test_logger_output(&logger, &filename, &key_val_pattern);
    }

    /// Source: `pkg/util/logutil/log_test.go::TestZapLoggerWithCore`.
    #[test]
    fn test_zap_logger_with_core() {
        let _g = guard();
        let filename = temp_file("zap_log_core.log");
        let file_cfg = FileLogConfig {
            filename: filename.clone(),
            max_size: 4096,
            ..Default::default()
        };
        let conf = new_log_config("info", DEFAULT_LOG_FORMAT, "", "", file_cfg, false);
        init_logger_with_core_fields(
            &conf,
            &[Field::new("coreKey", Value::Str("coreValue".into()))],
        )
        .unwrap();

        let core_pattern = format!(r"{PATTERN_BASE} \[coreKey=.*\] (\[.*=.*\])");
        test_logger_output(&bg_logger(), &filename, &core_pattern);
    }

    // Go TestFieldsFromTraceInfo.
    #[test]
    fn trace_info_fields() {
        assert!(fields_from_trace_info(None).is_empty());
        assert!(fields_from_trace_info(Some(&TraceInfo::default())).is_empty());
        let f = fields_from_trace_info(Some(&TraceInfo {
            connection_id: 1,
            session_alias: String::new(),
            trace_id: Vec::new(),
        }));
        assert_eq!(f.len(), 1);
        assert_eq!(f[0].key, "conn");
        let f = fields_from_trace_info(Some(&TraceInfo {
            connection_id: 0,
            session_alias: "alias123".into(),
            trace_id: Vec::new(),
        }));
        assert_eq!(f.len(), 1);
        assert_eq!(f[0].key, "session_alias");
        let f = fields_from_trace_info(Some(&TraceInfo {
            connection_id: 1,
            session_alias: "alias123".into(),
            trace_id: Vec::new(),
        }));
        assert_eq!(f.len(), 2);
    }

    #[test]
    fn slow_log_time_matches_rfc3339_nano() {
        let cases = [
            ("2026-08-12T00:00:05Z", "2026-08-12T00:00:05Z"),
            ("2026-08-12T08:00:05+08:00", "2026-08-12T08:00:05+08:00"),
            (
                "2026-08-12T08:00:05.120000000+08:00",
                "2026-08-12T08:00:05.12+08:00",
            ),
            (
                "2026-08-12T08:00:05.123456789+08:00",
                "2026-08-12T08:00:05.123456789+08:00",
            ),
        ];
        for (input, expected) in cases {
            let timestamp = DateTime::parse_from_rfc3339(input).unwrap();
            assert_eq!(format_slow_log_time(&timestamp), expected);
        }
    }

    // Go TestSetLevel.
    #[test]
    fn set_level_test() {
        let _g = guard();
        *GLOBALS.write().unwrap() = None;
        set_level("warn").unwrap();
        assert_eq!(bg_logger().level(), tidb_log::Level::Warn);

        let conf = new_log_config(
            "info",
            DEFAULT_LOG_FORMAT,
            "",
            "",
            FileLogConfig::default(),
            false,
        );
        init_logger(&conf).unwrap();
        assert_eq!(get_level(), tidb_log::Level::Info);
        set_level("warn").unwrap();
        assert_eq!(get_level(), tidb_log::Level::Warn);
        set_level("Error").unwrap();
        assert_eq!(get_level(), tidb_log::Level::Error);
        set_level("DEBUG").unwrap();
        assert_eq!(get_level(), tidb_log::Level::Debug);
    }

    // Go TestSlowQueryLoggerAndGeneralLoggerCreation (dedicated level
    // pinning) and TestSlowQueryLoggerAndGeneralUseSameLogFileName (shared
    // sink + shared global level + both formats in one file).
    #[test]
    fn slow_query_and_general_loggers() {
        let _g = guard();
        let filename = temp_file("same_file.log");
        let file_cfg = FileLogConfig {
            filename: filename.clone(),
            max_size: 10,
            max_days: 10,
            max_backups: 10,
            ..Default::default()
        };
        let conf = new_log_config("error", DEFAULT_LOG_FORMAT, "", "", file_cfg, false);
        init_logger(&conf).unwrap();

        // Same effective filename reuses the global level and write syncer.
        assert_eq!(slow_query_logger().level(), tidb_log::Level::Error);
        assert_eq!(general_logger().level(), tidb_log::Level::Error);
        assert!(slow_query_logger().same_sink(&general_logger()));

        let conf = new_log_config(
            "info",
            DEFAULT_LOG_FORMAT,
            "",
            "",
            FileLogConfig {
                filename: filename.clone(),
                max_size: 10,
                max_days: 10,
                max_backups: 10,
                ..Default::default()
            },
            false,
        );
        init_logger(&conf).unwrap();
        slow_query_logger().info("123", &[]);
        general_logger().info("GENERAL LOG", &[Field::new("test", Value::I64(123))]);

        let content = std::fs::read_to_string(&filename).unwrap();
        assert!(content.contains("# Time"), "content: {content}");
        assert!(content.contains("GENERAL LOG"));
        let _ = std::fs::remove_file(&filename);

        // Dedicated files ignore the global level and pin both loggers to info.
        let slow_file = temp_file("slow.log");
        let general_file = temp_file("general.log");
        let file_cfg = FileLogConfig {
            filename: filename.clone(),
            max_size: 10,
            ..Default::default()
        };
        let conf = new_log_config(
            "warn",
            DEFAULT_LOG_FORMAT,
            &slow_file,
            &general_file,
            file_cfg,
            false,
        );
        init_logger(&conf).unwrap();
        assert_eq!(slow_query_logger().level(), tidb_log::Level::Info);
        assert_eq!(general_logger().level(), tidb_log::Level::Info);
        assert!(!slow_query_logger().same_sink(&bg_logger()));
        assert!(!general_logger().same_sink(&bg_logger()));
        let _ = std::fs::remove_file(&filename);
        let _ = std::fs::remove_file(&slow_file);
        let _ = std::fs::remove_file(&general_file);

        let shared_dedicated_file = temp_file("shared_dedicated.log");
        let conf = new_log_config(
            "warn",
            DEFAULT_LOG_FORMAT,
            &shared_dedicated_file,
            &shared_dedicated_file,
            FileLogConfig {
                filename: filename.clone(),
                max_size: 10,
                ..Default::default()
            },
            false,
        );
        init_logger(&conf).unwrap();
        assert!(!slow_query_logger().same_sink(&general_logger()));
        let _ = std::fs::remove_file(&filename);
        let _ = std::fs::remove_file(&shared_dedicated_file);
    }

    #[test]
    fn slow_query_fields_preserve_slow_log_encoding() {
        let _g = guard();
        let filename = temp_file("slow_fields.log");
        let file_cfg = FileLogConfig {
            filename: filename.clone(),
            max_size: 4096,
            ..Default::default()
        };
        let conf = new_log_config("info", DEFAULT_LOG_FORMAT, "", "", file_cfg, false);
        init_logger(&conf).unwrap();

        slow_query_logger()
            .with_fields(&[Field::new("conn", Value::U64(1))])
            .info("slow fields", &[]);
        let content = std::fs::read_to_string(&filename).unwrap();
        assert!(content.starts_with("# Time:"), "content: {content}");
        assert!(content.contains("slow fields"), "content: {content}");
        let _ = std::fs::remove_file(&filename);
    }

    #[test]
    fn equal_empty_filenames_share_stdout_sink() {
        let _g = guard();
        let conf = new_log_config(
            "info",
            DEFAULT_LOG_FORMAT,
            "",
            "",
            FileLogConfig::default(),
            false,
        );
        init_logger(&conf).unwrap();
        assert!(bg_logger().same_sink(&slow_query_logger()));
        assert!(bg_logger().same_sink(&general_logger()));
    }

    // Go TestCompressedLog.
    #[test]
    fn compressed_log() {
        let _g = guard();
        let filename = temp_file("compressed.log");
        let mut file_cfg = FileLogConfig {
            filename: filename.clone(),
            max_size: 10,
            max_days: 10,
            max_backups: 10,
            compression: "xxx".into(),
            ..Default::default()
        };
        let conf = new_log_config(
            "warn",
            DEFAULT_LOG_FORMAT,
            &filename,
            "",
            file_cfg.clone(),
            false,
        );
        assert!(init_logger(&conf).is_err());

        file_cfg.compression = "gzip".into();
        let conf = new_log_config("warn", DEFAULT_LOG_FORMAT, &filename, "", file_cfg, false);
        init_logger(&conf).unwrap();
        let _ = std::fs::remove_file(&filename);
    }

    // Go TestGlobalLoggerReplace.
    #[test]
    fn global_logger_replace() {
        let _g = guard();
        let filename = temp_file("replace.log");
        let file_cfg = FileLogConfig {
            filename: filename.clone(),
            max_size: 4096,
            ..Default::default()
        };
        let mut conf = new_log_config("info", DEFAULT_LOG_FORMAT, "", "", file_cfg, false);
        init_logger(&conf).unwrap();
        conf.config.file.max_days = 14;
        replace_logger(&conf).unwrap();

        // Go ReplaceLogger always recreates the slow-query and general
        // loggers through their dedicated constructors, even when their
        // configured filenames are empty and resolve to the global file.
        assert_eq!(slow_query_logger().level(), tidb_log::Level::Info);
        assert_eq!(general_logger().level(), tidb_log::Level::Info);
        assert!(!slow_query_logger().same_sink(&bg_logger()));
        assert!(!general_logger().same_sink(&bg_logger()));
        assert!(!slow_query_logger().same_sink(&general_logger()));
        assert!(!err_verbose_logger().same_sink(&bg_logger()));
        let _ = std::fs::remove_file(&filename);
    }

    // Go TestProxyFields: exhaust env combinations.
    #[test]
    fn proxy_fields_test() {
        let _g = guard();
        let envs = ["http_proxy", "https_proxy", "no_proxy"];
        let uppers = ["HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY"];
        let presets = [
            "http://127.0.0.1:8080",
            "https://127.0.0.1:8443",
            "localhost,127.0.0.1",
        ];
        for mask in 0..=0b111u32 {
            for (l, u) in envs.iter().zip(uppers.iter()) {
                std::env::remove_var(l);
                std::env::remove_var(u);
            }
            for i in 0..3 {
                if (1 << i) & mask != 0 {
                    std::env::set_var(envs[i as usize], presets[i as usize]);
                }
            }
            for field in proxy_fields() {
                let idx = envs.iter().position(|e| *e == field.key).unwrap();
                assert_ne!((1 << idx) & mask, 0);
                assert_eq!(presets[idx], field.value);
            }
        }
        for (l, u) in envs.iter().zip(uppers.iter()) {
            std::env::remove_var(l);
            std::env::remove_var(u);
        }

        std::env::set_var("http_proxy", "http://lower.example");
        std::env::set_var("HTTP_PROXY", "http://upper.example");
        assert_eq!(proxy_fields()[0].value, "http://upper.example");
        std::env::remove_var("http_proxy");
        std::env::remove_var("HTTP_PROXY");
    }

    // Go TestSampleLoggerFactory.
    #[test]
    fn sample_logger_factory_test() {
        let _g = guard();
        let filename = temp_file("sampled.log");
        let file_cfg = FileLogConfig {
            filename: filename.clone(),
            max_size: 4096,
            ..Default::default()
        };
        let conf = new_log_config("info", DEFAULT_LOG_FORMAT, "", "", file_cfg, false);
        init_logger(&conf).unwrap();
        let fac = sample_logger_factory(
            Duration::from_secs(60),
            3,
            vec![Field::new(LOG_FIELD_CATEGORY, Value::Str("ddl".into()))],
        );
        for _ in 0..100 {
            fac().info("sample log test", &[]);
        }
        let content = std::fs::read_to_string(&filename).unwrap();
        assert_eq!(content.matches("sample log test").count(), 3);
        let _ = std::fs::remove_file(&filename);
    }

    #[test]
    fn sampled_logger_separates_levels() {
        let _g = guard();
        let filename = temp_file("sampled_levels.log");
        let file_cfg = FileLogConfig {
            filename: filename.clone(),
            max_size: 4096,
            ..Default::default()
        };
        let conf = new_log_config("debug", DEFAULT_LOG_FORMAT, "", "", file_cfg, false);
        init_logger(&conf).unwrap();
        let logger = sample_logger_factory(Duration::from_secs(60), 1, Vec::new())();
        logger.debug("same message", &[]);
        logger.info("same message", &[]);
        logger.warn("same message", &[]);
        logger.error("same message", &[]);

        let content = std::fs::read_to_string(&filename).unwrap();
        assert_eq!(
            content.matches("same message").count(),
            4,
            "content: {content}"
        );
        let _ = std::fs::remove_file(&filename);
    }

    #[test]
    fn sampled_logger_does_not_count_disabled_entries() {
        let _g = guard();
        let filename = temp_file("sampled_disabled.log");
        let file_cfg = FileLogConfig {
            filename: filename.clone(),
            max_size: 4096,
            ..Default::default()
        };
        let conf = new_log_config("warn", DEFAULT_LOG_FORMAT, "", "", file_cfg, false);
        init_logger(&conf).unwrap();
        let logger = sample_logger_factory(Duration::from_secs(60), 1, Vec::new())();

        logger.info("enabled later", &[]);
        set_level("info").unwrap();
        logger.info("enabled later", &[]);

        let content = std::fs::read_to_string(&filename).unwrap();
        assert_eq!(
            content.matches("enabled later").count(),
            1,
            "content: {content}"
        );
        let _ = std::fs::remove_file(&filename);
    }

    #[test]
    fn sampled_logger_uses_source_buckets() {
        let _g = guard();
        let filename = temp_file("sampled_buckets.log");
        let file_cfg = FileLogConfig {
            filename: filename.clone(),
            max_size: 4096,
            ..Default::default()
        };
        let conf = new_log_config("info", DEFAULT_LOG_FORMAT, "", "", file_cfg, false);
        init_logger(&conf).unwrap();

        let mut seen = HashMap::new();
        let (first, collision) = (0..=4096)
            .find_map(|i| {
                let message = format!("message {i}");
                let bucket = sampler_bucket(&message);
                seen.insert(bucket, message.clone())
                    .map(|previous| (previous, message))
            })
            .unwrap();

        let logger = sample_logger_factory(Duration::from_secs(60), 1, Vec::new())();
        logger.info(&first, &[]);
        logger.info(&collision, &[]);

        let content = std::fs::read_to_string(&filename).unwrap();
        assert!(content.contains(&first), "content: {content}");
        assert!(!content.contains(&collision), "content: {content}");
        let _ = std::fs::remove_file(&filename);
    }

    #[test]
    fn sampled_logger_resets_each_message_window_independently() {
        let _g = guard();
        let filename = temp_file("sampled_windows.log");
        let file_cfg = FileLogConfig {
            filename: filename.clone(),
            max_size: 4096,
            ..Default::default()
        };
        let conf = new_log_config("info", DEFAULT_LOG_FORMAT, "", "", file_cfg, false);
        init_logger(&conf).unwrap();
        let logger = sample_logger_factory(Duration::from_secs(120), 1, Vec::new())();
        logger.info("message a", &[]);
        logger.info("message b", &[]);
        logger.info("message a", &[]);
        logger.info("message b", &[]);

        logger
            .state
            .lock()
            .unwrap()
            .windows
            .get_mut(&(Level::Info, sampler_bucket("message a")))
            .unwrap()
            .started = Instant::now() - Duration::from_secs(121);
        logger.info("message a", &[]);
        logger.info("message b", &[]);

        let content = std::fs::read_to_string(&filename).unwrap();
        assert_eq!(
            content.matches("message a").count(),
            2,
            "content: {content}"
        );
        assert_eq!(
            content.matches("message b").count(),
            1,
            "content: {content}"
        );
        let _ = std::fs::remove_file(&filename);
    }

    // pingcap/log TestRotateLog (the rotation contract lives in the Rust
    // file sink).
    #[test]
    fn rotate_log() {
        let _g = guard();
        let dir = std::env::temp_dir().join(format!("tidb_rotate_test_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let filename = dir.join("test.log").to_string_lossy().to_string();
        let file_cfg = FileLogConfig {
            filename: filename.clone(),
            max_size: 1,
            ..Default::default()
        };
        let conf = new_log_config("info", DEFAULT_LOG_FORMAT, "", "", file_cfg, false);
        init_logger(&conf).unwrap();

        let logger = bg_logger();
        let mut data = String::new();
        for i in 1..=(1024 * 1024) {
            if i % 1000 != 0 {
                data.push('d');
                continue;
            }
            logger.info(&data, &[]);
            data.clear();
        }
        let files: Vec<_> = std::fs::read_dir(&dir).unwrap().flatten().collect();
        assert_eq!(files.len(), 2, "expected rotation to produce 2 files");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
