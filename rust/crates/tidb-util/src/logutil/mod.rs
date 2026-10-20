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
mod tracing_bridge;

use std::collections::HashMap;
use std::io;
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use chrono::{DateTime, FixedOffset, Local};
use tidb_log::{Entry, Field, Level, LoggerOptions, TextEncoder, WriteSyncer};

use crate::stringutil::go_to_lower;
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

/// A settable level shared between loggers (zap `AtomicLevel`). After
/// [`init_logger`] it is the very level of the `pingcap/log` global logger
/// (Go `log.ReplaceGlobals(gl, props)` publishes `props.Level`), so
/// [`set_level`] and `log.SetLevel` move one threshold.
#[derive(Clone)]
pub struct AtomicLevel(tidb_log::AtomicLevel);

impl AtomicLevel {
    fn new(l: Level) -> AtomicLevel {
        AtomicLevel(tidb_log::AtomicLevel::new(l))
    }
    fn enabled(&self, l: Level) -> bool {
        l >= self.0.get()
    }
    fn set(&self, l: Level) {
        self.0.set(l);
    }
    /// The current level, for tests/introspection.
    pub fn get(&self) -> Level {
        self.0.get()
    }
}

/// Parses a zap level string, case-insensitively.
pub fn parse_level(s: &str) -> Result<Level, String> {
    match go_to_lower(s).as_str() {
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
    // The zap write syncer over `sink`: buffering (`is-buffered`) and the
    // write timeout (`timeout`) wrap it exactly as `pingcap/log` wraps
    // lumberjack.
    output: Arc<dyn WriteSyncer>,
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
        output: Arc<dyn WriteSyncer>,
        level: AtomicLevel,
    ) -> Logger {
        Logger {
            inner: Arc::new(LoggerInner {
                encoder,
                encoding,
                sink,
                output,
                level,
            }),
        }
    }

    /// A no-op-ish default logger writing to stdout at info level.
    pub fn stdout() -> Logger {
        let sink: SharedSink = Arc::new(Mutex::new(Sink::Stdout));
        Logger::new(
            TextEncoder::default(),
            Encoding::Unified,
            Arc::clone(&sink),
            sink_syncer(&sink),
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
                output: Arc::clone(&self.inner.output),
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

    /// Whether an entry at `level` passes this logger's level gate (zap
    /// `Core.Enabled`).
    pub fn enabled(&self, level: Level) -> bool {
        self.inner.level.enabled(level)
    }

    #[track_caller]
    fn emit(&self, level: Level, msg: &str, fields: &[Field]) {
        let loc = std::panic::Location::caller();
        self.emit_at(level, loc.file(), loc.line(), msg, fields);
    }

    /// Writes one entry whose caller is `file:line` (the zap caller of an
    /// entry that did not come from a `#[track_caller]` call site).
    pub fn emit_at(&self, level: Level, file: &str, line: u32, msg: &str, fields: &[Field]) {
        if !self.enabled(level) {
            return;
        }
        let now: DateTime<FixedOffset> = Local::now().fixed_offset();
        let encoded = match self.inner.encoding {
            Encoding::Unified => {
                let ent = Entry {
                    time: now,
                    level,
                    logger_name: String::new(),
                    caller: Some((file.to_string(), line)),
                    message: msg.to_string(),
                    stack: String::new(),
                };
                self.inner.encoder.encode_entry(&ent, fields)
            }
            Encoding::SlowLog => {
                format!("# Time: {}\n{}\n", format_slow_log_time(&now), msg)
            }
        };
        let _ = self.inner.output.write(encoded.as_bytes());
        // zap's ioCore syncs after entries above ErrorLevel.
        if level > Level::Error {
            let _ = self.inner.output.sync();
        }
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
        let sink: SharedSink = Arc::new(Mutex::new(Sink::Stdout));
        let l = Logger::new(
            TextEncoder::default(),
            Encoding::Unified,
            Arc::clone(&sink),
            sink_syncer(&sink),
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
    tracing_bridge::level_changed();
    Ok(())
}

/// The current global level (Go `log.GetLevel`, test surface).
pub fn get_level() -> Level {
    read_globals(|g| g.level.get())
}

/// Flushes the global loggers' buffered output (Go `log.Sync`).
pub fn sync() -> io::Result<()> {
    read_globals(|g| g.global.inner.output.sync())
}

// zap `WriteSyncer` over a shared sink.
struct SinkSyncer(SharedSink);

impl WriteSyncer for SinkSyncer {
    fn write(&self, bytes: &[u8]) -> io::Result<usize> {
        self.0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .write_bytes(bytes)
            .map(|()| bytes.len())
    }

    fn sync(&self) -> io::Result<()> {
        self.0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .sync()
    }
}

fn sink_syncer(sink: &SharedSink) -> Arc<dyn WriteSyncer> {
    Arc::new(SinkSyncer(Arc::clone(sink)))
}

// `pingcap/log.initFileLog`: the file checks run before the compression
// setting is validated.
fn build_sink(file: &FileLogConfig) -> Result<SharedSink, String> {
    if file.filename.is_empty() {
        return Ok(Arc::new(Mutex::new(Sink::Stdout)));
    }
    let mut rf = RotatingFile::open_with_max_days(
        Path::new(&file.filename),
        file.max_size,
        file.max_backups,
        file.max_days,
        false,
    )
    .map_err(|e| e.to_string())?;
    rf.compress = match file.compression.as_str() {
        "" => false,
        "gzip" => true,
        other => return Err(format!("can't set compression to `{other}`")),
    };
    Ok(Arc::new(Mutex::new(Sink::File(rf))))
}

// `pingcap/log.InitLogger`'s output: the file sink, buffered when
// `is-buffered` is set (stdout is never buffered).
fn build_output(file: &FileLogConfig, sink: &SharedSink) -> Arc<dyn WriteSyncer> {
    let output = sink_syncer(sink);
    if !file.filename.is_empty() && file.is_buffered {
        tidb_log::buffered_write_syncer(output, file.buffer_size, file.buffer_flush_interval)
    } else {
        output
    }
}

// `pingcap/log.InitLogger` for a dedicated logger whose entries this crate
// encodes itself: the sink, its write syncer with the configured write
// timeout (`InitLoggerWithWriteSyncer`), and a fresh level from `level`.
fn init_dedicated(
    config: &tidb_log::Config,
) -> Result<(SharedSink, Arc<dyn WriteSyncer>, AtomicLevel), String> {
    let sink = build_sink(&config.file)?;
    let level = AtomicLevel::new(parse_level(&config.level)?);
    let mut output = build_output(&config.file, &sink);
    if config.timeout > 0 {
        output = tidb_log::lock_with_timeout(Arc::new(DynSyncer(output)), config.timeout as u64);
    }
    Ok((sink, output, level))
}

// Adapts a shared trait object to `lock_with_timeout`'s sized parameter.
struct DynSyncer(Arc<dyn WriteSyncer>);

impl WriteSyncer for DynSyncer {
    fn write(&self, bytes: &[u8]) -> io::Result<usize> {
        self.0.write(bytes)
    }

    fn sync(&self) -> io::Result<()> {
        self.0.sync()
    }
}

// Go `newSlowQueryLogConfig`/`newGeneralLogConfig`: the global config with
// the level reset to the default (info) and, when one is configured, the
// dedicated filename.
fn sub_log_config(cfg: &LogConfig, filename: &str) -> tidb_log::Config {
    let mut config = cfg.config.clone();
    config.level = String::new();
    if !filename.is_empty() {
        config.file.filename = filename.to_string();
    }
    config
}

// Go `log.InitLogger(&cfg.Config, opts...)` with logutil's
// `zap.AddStacktrace(zapcore.FatalLevel)` option and the wrapped core's
// fields, followed by `log.ReplaceGlobals(gl, props)`: the `pingcap/log`
// global (`tidb_log::info` and friends) writes through the same sink, the
// same timeout-wrapped syncer and the same level as [`bg_logger`].
fn init_global(
    cfg: &LogConfig,
    core_fields: &[Field],
) -> Result<(SharedSink, tidb_log::ZapProperties), String> {
    let sink = build_sink(&cfg.config.file)?;
    let output = build_output(&cfg.config.file, &sink);
    let (gl, props) =
        tidb_log::init_logger_with_write_syncer(&cfg.config, Arc::clone(&output), output)?;
    let gl = gl
        .with_options(LoggerOptions::default().with_stacktrace_level(Level::Fatal))
        .with_fields(core_fields);
    let _ = tidb_log::replace_globals(gl);
    Ok((sink, props))
}

/// Initializes global, slow-query, and general loggers (Go `InitLogger`).
pub fn init_logger(cfg: &LogConfig) -> Result<(), String> {
    init_logger_with_core_fields(cfg, &[])
}

/// Go `InitLogger(cfg, zap.WrapCore(core.With(fields)))`: initializes the
/// logger after attaching fields to its core, so every emitted entry carries
/// them even when the call site supplies no context fields.
pub fn init_logger_with_core_fields(cfg: &LogConfig, core_fields: &[Field]) -> Result<(), String> {
    let (global_sink, props) = init_global(cfg, core_fields)?;
    let level = AtomicLevel(props.level.clone());
    let encoder = TextEncoder::new(&cfg.config)?.with_fields(core_fields);
    let global = Logger::new(
        encoder,
        Encoding::Unified,
        Arc::clone(&global_sink),
        Arc::clone(&props.syncer),
        level.clone(),
    );

    // Error-verbose logger: same sink, never suppresses verbose errors. Go
    // builds it with `InitLoggerWithWriteSyncer(&newLogCfg, props.Syncer,
    // props.ErrSyncer)`, which parses its own level from the config: it
    // does not follow later `SetLevel` calls.
    let err_verbose = if cfg.config.format == "json" || !cfg.config.disable_error_verbose {
        global.clone()
    } else {
        let mut c = cfg.config.clone();
        c.disable_error_verbose = false;
        let mut output = Arc::clone(&props.syncer);
        if c.timeout > 0 {
            output = tidb_log::lock_with_timeout(Arc::new(DynSyncer(output)), c.timeout as u64);
        }
        Logger::new(
            TextEncoder::new(&c)?.with_fields(core_fields),
            Encoding::Unified,
            Arc::clone(&global_sink),
            output,
            AtomicLevel::new(parse_level(&c.level)?),
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
                Arc::clone(&props.syncer),
                level.clone(),
            )
        } else {
            let (sink, output, level) = init_dedicated(&sub_log_config(cfg, &cfg.slow_query_file))?;
            Logger::new(
                TextEncoder::default(),
                Encoding::SlowLog,
                sink,
                output,
                level,
            )
        };

    let general = if cfg.general_log_file.is_empty()
        || cfg.general_log_file == cfg.config.file.filename
    {
        global.clone()
    } else {
        let (sink, output, level) = init_dedicated(&sub_log_config(cfg, &cfg.general_log_file))?;
        Logger::new(
            TextEncoder::new(&cfg.config)?,
            Encoding::Unified,
            sink,
            output,
            level,
        )
    };

    *GLOBALS.write().unwrap() = Some(Globals {
        global,
        slow_query,
        general,
        err_verbose,
        level,
    });
    tracing_bridge::install();
    Ok(())
}

/// Replaces the global loggers (Go `ReplaceLogger`).
pub fn replace_logger(cfg: &LogConfig) -> Result<(), String> {
    let (global_sink, props) = init_global(cfg, &[])?;
    let level = AtomicLevel(props.level.clone());
    let global = Logger::new(
        TextEncoder::new(&cfg.config)?,
        Encoding::Unified,
        global_sink,
        Arc::clone(&props.syncer),
        level.clone(),
    );

    // Go ReplaceLogger calls both dedicated constructors unconditionally.
    // Each constructor initializes its own write syncer, even when an empty
    // sub-logger filename resolves to the same path as the global logger.
    let (slow_sink, slow_output, slow_level) =
        init_dedicated(&sub_log_config(cfg, &cfg.slow_query_file))?;
    let slow_query = Logger::new(
        TextEncoder::default(),
        Encoding::SlowLog,
        slow_sink,
        slow_output,
        slow_level,
    );

    let (general_sink, general_output, general_level) =
        init_dedicated(&sub_log_config(cfg, &cfg.general_log_file))?;
    let general = Logger::new(
        TextEncoder::new(&cfg.config)?,
        Encoding::Unified,
        general_sink,
        general_output,
        general_level,
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
    tracing_bridge::install();

    let config = serde_json::to_string(&cfg.config).map_err(|e| e.to_string())?;
    tidb_log::s().infof(format_args!("replaced global logger with config: {config}"));
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

    /// Source: `pingcap/log.initFileLog` forwards `FileLogConfig.MaxDays` to
    /// lumberjack's age-based cleanup. An invalid lookalike filename must not
    /// be treated as a lumberjack backup.
    #[test]
    fn max_days_prunes_only_expired_lumberjack_backups() {
        let _g = guard();
        let dir = std::env::temp_dir().join(format!(
            "tidb_logutil_max_days_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let filename = dir.join("max_days.log");
        let expired = dir.join("max_days-2000-01-01T00-00-00.000.log");
        let lookalike = dir.join("max_days-not-a-timestamp.log");
        let directory = dir.join("max_days-2000-01-01T00-00-00.001.log");
        std::fs::write(&filename, b"seed").unwrap();
        std::fs::write(&expired, b"expired").unwrap();
        std::fs::write(&lookalike, b"keep").unwrap();
        std::fs::create_dir(&directory).unwrap();

        let conf = new_log_config(
            "info",
            DEFAULT_LOG_FORMAT,
            "",
            "",
            FileLogConfig {
                filename: filename.to_string_lossy().to_string(),
                max_size: 1,
                max_days: 1,
                ..Default::default()
            },
            false,
        );
        init_logger(&conf).unwrap();
        // lumberjack mills (age-prunes) when the first write opens the
        // existing file, before any rotation.
        bg_logger().info("open the existing log", &[]);

        assert!(!expired.exists(), "expired lumberjack backup was retained");
        assert!(lookalike.exists(), "non-backup lookalike was deleted");
        assert!(directory.exists(), "backup-looking directory was deleted");
        let _ = std::fs::remove_dir_all(dir);
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

    fn fresh_dir(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "tidb_logutil_{name}_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn file_config(filename: &std::path::Path, max_size: i64) -> FileLogConfig {
        FileLogConfig {
            filename: filename.to_string_lossy().to_string(),
            max_size,
            ..Default::default()
        }
    }

    /// `pingcap/log.initFileLog` probes the path and removes the probe;
    /// lumberjack creates the file on the first write with mode 0600.
    #[test]
    fn log_file_is_created_on_first_write_with_lumberjack_mode() {
        let _g = guard();
        let dir = fresh_dir("create");
        let filename = dir.join("nested").join("tidb.log");
        let conf = new_log_config(
            "info",
            DEFAULT_LOG_FORMAT,
            "",
            "",
            file_config(&filename, 300),
            false,
        );
        init_logger(&conf).unwrap();
        assert!(dir.join("nested").is_dir(), "initFileLog creates the dir");
        assert!(!filename.exists(), "lumberjack opens the file lazily");
        bg_logger().info("first line", &[]);
        assert!(filename.exists());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&filename).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600);
        }

        let conf = new_log_config(
            "info",
            DEFAULT_LOG_FORMAT,
            "",
            "",
            file_config(&dir, 300),
            false,
        );
        assert_eq!(
            init_logger(&conf).err().as_deref(),
            Some("can't use directory as log file name")
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    /// The unified log format line exactly as `pingcap/log`'s text encoder
    /// lays it out: `[time] [LEVEL] [file:line] [message] [k=v]...`, with
    /// the message and keys/values quoted only when they need it.
    #[test]
    fn unified_line_shape_matches_pingcap_log() {
        let _g = guard();
        let dir = fresh_dir("shape");
        let filename = dir.join("tidb.log");
        let conf = new_log_config(
            "info",
            DEFAULT_LOG_FORMAT,
            "",
            "",
            file_config(&filename, 300),
            false,
        );
        init_logger(&conf).unwrap();
        let line_no = line!() + 1;
        bg_logger().info(
            "server is running MySQL protocol",
            &[
                Field::new("addr", Value::Str("0.0.0.0:4000".into())),
                Field::new("listening on addr", Value::Str("a b".into())),
                Field::new("quote", Value::Str("x\"y".into())),
                Field::new("conn", Value::U64(7)),
            ],
        );
        bg_logger().info("acquireServerID", &[]);
        let content = std::fs::read_to_string(&filename).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        let header =
            regex::Regex::new(r"^\[\d{4}/\d\d/\d\d \d\d:\d\d:\d\d\.\d{3} [+-]\d\d:\d\d\] ")
                .unwrap();
        assert!(header.is_match(lines[0]), "{}", lines[0]);
        let (_, rest) = lines[0].split_once("] ").unwrap();
        assert_eq!(
            rest,
            format!(
                "[INFO] [mod.rs:{line_no}] [\"server is running MySQL protocol\"] \
                 [addr=0.0.0.0:4000] [\"listening on addr\"=\"a b\"] [quote=\"x\\\"y\"] \
                 [conn=7]"
            )
        );
        assert!(lines[1].ends_with("] [acquireServerID]"), "{}", lines[1]);
        let _ = std::fs::remove_dir_all(dir);
    }

    /// Go `log.ReplaceGlobals(gl, props)`: the `pingcap/log` global writes to
    /// the same file at the same level, `SetLevel` moves both, and logutil's
    /// `AddStacktrace(FatalLevel)` keeps stacks off ERROR entries.
    #[test]
    fn pingcap_log_global_shares_file_and_level() {
        let _g = guard();
        let dir = fresh_dir("global");
        let filename = dir.join("tidb.log");
        let conf = new_log_config(
            "warn",
            DEFAULT_LOG_FORMAT,
            "",
            "",
            file_config(&filename, 300),
            false,
        );
        init_logger(&conf).unwrap();
        tidb_log::info("filtered info", &[]);
        bg_logger().info("filtered bg info", &[]);
        tidb_log::warn("global warn", &[]);
        tidb_log::error("global error", &[]);
        set_level("info").unwrap();
        assert_eq!(tidb_log::get_level(), Level::Info);
        tidb_log::info("global info after set level", &[]);
        let content = std::fs::read_to_string(&filename).unwrap();
        assert!(!content.contains("filtered"), "{content}");
        assert!(content.contains("[WARN] [mod.rs:"), "{content}");
        assert!(content.contains("[\"global warn\"]"), "{content}");
        assert!(
            content.contains("[\"global info after set level\"]"),
            "{content}"
        );
        assert!(!content.contains("stack="), "{content}");
        assert_eq!(content.lines().count(), 3, "{content}");
        let _ = std::fs::remove_dir_all(dir);
    }

    /// lumberjack's thresholds: opening an existing file rotates when
    /// `size + len >= max`, an open file rotates when `size + len > max`,
    /// and a single write longer than `max` is refused without rotating.
    #[test]
    fn rotation_thresholds_follow_lumberjack() {
        let _g = guard();
        let dir = fresh_dir("threshold");
        let filename = dir.join("tidb.log");
        let max = 1024 * 1024;
        let mut sink =
            file_sink::RotatingFile::open(&filename, 1, 0, false).expect("rotating file");
        // Existing file: exactly `max - 4` bytes; a 4-byte write reaches max.
        std::fs::write(&filename, vec![b'a'; max - 4]).unwrap();
        sink.write(b"bbbb").unwrap();
        let backups = || {
            std::fs::read_dir(&dir)
                .unwrap()
                .flatten()
                .filter(|entry| entry.file_name() != "tidb.log")
                .count()
        };
        assert_eq!(backups(), 1, "size + len >= max rotates on open");
        assert_eq!(std::fs::read(&filename).unwrap(), b"bbbb");

        // Open file: filling exactly to max does not rotate; one more byte
        // does.
        sink.write(&vec![b'c'; max - 4]).unwrap();
        assert_eq!(backups(), 1);
        assert_eq!(std::fs::metadata(&filename).unwrap().len(), max as u64);
        sink.write(b"d").unwrap();
        assert_eq!(backups(), 2, "size + len > max rotates");

        let error = sink.write(&vec![b'e'; max + 1]).unwrap_err();
        assert_eq!(
            error.to_string(),
            format!("write length {} exceeds maximum file size {max}", max + 1)
        );
        assert_eq!(backups(), 2);
        assert_eq!(std::fs::read(&filename).unwrap(), b"d");
        let _ = std::fs::remove_dir_all(dir);
    }

    /// `max-backups` keeps the newest backups, counting `x.log` and
    /// `x.log.gz` as one.
    #[test]
    fn max_backups_keeps_newest_backups() {
        let _g = guard();
        let dir = fresh_dir("backups");
        let filename = dir.join("tidb.log");
        for (name, body) in [
            ("tidb-2026-01-01T00-00-00.000.log", "oldest"),
            ("tidb-2026-01-02T00-00-00.000.log", "middle"),
            ("tidb-2026-01-03T00-00-00.000.log.gz", "newest"),
        ] {
            std::fs::write(dir.join(name), body).unwrap();
        }
        std::fs::write(&filename, b"seed").unwrap();
        let mut sink =
            file_sink::RotatingFile::open(&filename, 1, 2, false).expect("rotating file");
        sink.write(b"line\n").unwrap();
        assert!(!dir.join("tidb-2026-01-01T00-00-00.000.log").exists());
        assert!(dir.join("tidb-2026-01-02T00-00-00.000.log").exists());
        assert!(dir.join("tidb-2026-01-03T00-00-00.000.log.gz").exists());
        let _ = std::fs::remove_dir_all(dir);
    }

    /// `tracing` events from ported call sites land in the log file as
    /// unified-format entries with their structured fields.
    #[test]
    fn tracing_events_reach_the_log_file() {
        let _g = guard();
        let dir = fresh_dir("tracing");
        let filename = dir.join("tidb.log");
        let conf = new_log_config(
            "info",
            DEFAULT_LOG_FORMAT,
            "",
            "",
            file_config(&filename, 300),
            false,
        );
        init_logger(&conf).unwrap();
        let line_no = line!() + 1;
        ::tracing::warn!(error = %"disk full", "spill to disk failed");
        ::tracing::debug!("filtered debug");
        let content = std::fs::read_to_string(&filename).unwrap();
        assert!(
            content.ends_with(&format!(
                "[WARN] [mod.rs:{line_no}] [\"spill to disk failed\"] [error=\"disk full\"]\n"
            )),
            "{content}"
        );
        assert!(!content.contains("filtered"), "{content}");

        // `SetLevel` reaches `tracing`'s cached static filter too.
        set_level("debug").unwrap();
        ::tracing::trace!("trace logs at debug");
        let content = std::fs::read_to_string(&filename).unwrap();
        assert!(
            content.contains("[DEBUG] [mod.rs:") && content.contains("[\"trace logs at debug\"]"),
            "{content}"
        );
        let _ = std::fs::remove_dir_all(dir);
    }
}
