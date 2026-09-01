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

//! Lightning logging from Go `pkg/lightning/log`.

use std::error::Error;
use std::fmt;
use std::io;
use std::ops::Deref;
use std::path::Path;
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::{Duration, Instant};

use chrono::Local;
use serde::{Deserialize, Serialize};
use tidb_log::{
    AtomicLevel, Entry, Field, Level, Logger as ZapLogger, LoggerOptions, TextEncoder, TextIoCore,
    Value, WriteSyncer,
};

const DEFAULT_LOG_LEVEL: &str = "info";
const DEFAULT_LOG_MAX_DAYS: isize = 7;
const DEFAULT_LOG_MAX_SIZE: isize = 512;

/// The environment variable controlling gRPC debug logging.
const GRPC_DEBUG_ENV_NAME: &str = "GRPC_DEBUG";

/// Lightning log configuration.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    /// Log level.
    pub level: String,
    /// Log filename; empty or `-` selects standard output.
    pub file: String,
    /// Maximum size of one file in MB.
    #[serde(rename = "max-size")]
    pub file_max_size: isize,
    /// Maximum number of days to retain logs.
    #[serde(rename = "max-days")]
    pub file_max_days: isize,
    /// Maximum number of old files to retain.
    #[serde(rename = "max-backups")]
    pub file_max_backups: isize,
    /// Whether logs from all packages and gRPC debug logs are enabled.
    #[serde(rename = "enable-diagnose-logs")]
    pub enable_diagnose_logs: bool,
}

impl Config {
    /// Applies Lightning's defaults and warning-level alias.
    pub fn adjust(&mut self) {
        if self.level.is_empty() {
            self.level = DEFAULT_LOG_LEVEL.to_owned();
        }
        if self.level == "warning" {
            self.level = "warn".to_owned();
        }
        if self.file_max_size == 0 {
            self.file_max_size = DEFAULT_LOG_MAX_SIZE;
        }
        if self.file_max_days == 0 {
            self.file_max_days = DEFAULT_LOG_MAX_DAYS;
        }
    }
}

#[derive(Clone)]
enum Backend {
    Zap(ZapLogger),
    Test {
        encoder: TextEncoder,
        buffer: Arc<TestBuffer>,
    },
}

/// A Lightning logger wrapping the canonical structured logger.
#[derive(Clone)]
pub struct Logger {
    backend: Backend,
    fields: Vec<Field>,
    filters: Arc<Vec<String>>,
}

impl Logger {
    fn from_zap(logger: ZapLogger) -> Self {
        Self {
            backend: Backend::Zap(logger),
            fields: Vec::new(),
            filters: Arc::new(Vec::new()),
        }
    }

    fn filtered(mut self, filters: Vec<String>) -> Self {
        self.filters = Arc::new(filters);
        self
    }

    /// Reports whether this logger uses package filtering.
    pub fn is_filtered(&self) -> bool {
        !self.filters.is_empty()
    }

    /// Returns a child carrying structured context fields.
    pub fn with(&self, fields: &[Field]) -> Self {
        let mut child = self.clone();
        child.fields.extend_from_slice(fields);
        child
    }

    /// Returns a child with an appended logger-name segment.
    pub fn named(&self, name: &str) -> Self {
        let mut child = self.clone();
        if let Backend::Zap(logger) = &self.backend {
            child.backend = Backend::Zap(logger.named(name));
        }
        child
    }

    fn caller_allowed(&self, caller: &str) -> bool {
        self.filters.is_empty()
            || self
                .filters
                .iter()
                .any(|filter| caller.contains(filter.as_str()))
    }

    fn enabled(&self, level: Level) -> bool {
        match &self.backend {
            Backend::Zap(logger) => logger.enabled(level),
            Backend::Test { .. } => true,
        }
    }

    fn qualified_caller(file: &str) -> String {
        let file = file.replace('\\', "/");
        let Some((crate_name, source)) = file
            .split_once("/rust/crates/")
            .map(|(_, path)| path)
            .or_else(|| file.split_once("crates/").map(|(_, path)| path))
            .and_then(|path| path.split_once("/src/"))
        else {
            return file;
        };
        let source = source.trim_end_matches(".rs").replace('_', "/");
        match crate_name {
            "tidb-br" => format!("github.com/pingcap/tidb/br/{source}"),
            "tidb-ingestor" => format!("github.com/pingcap/tidb/pkg/ingestor/{source}"),
            "tidb-pd-client" => format!("github.com/tikv/pd/client/{source}"),
            "tidb-util" if source.starts_with("lightning/") => {
                format!("github.com/pingcap/tidb/pkg/{source}")
            }
            _ if source.ends_with("main") => "main.main".to_owned(),
            _ if crate_name.starts_with("tidb-lightning-") => format!(
                "github.com/pingcap/tidb/pkg/lightning/{}/{}",
                crate_name
                    .trim_start_matches("tidb-lightning-")
                    .replace('-', "/"),
                source
            ),
            _ => format!("github.com/pingcap/tidb/pkg/{crate_name}/{source}"),
        }
    }

    fn write_test(
        encoder: &TextEncoder,
        buffer: &TestBuffer,
        level: Level,
        message: &str,
        fields: &[Field],
    ) {
        let entry = Entry {
            time: Local::now().fixed_offset(),
            level,
            logger_name: String::new(),
            caller: None,
            message: message.to_owned(),
            stack: String::new(),
        };
        let encoded = encoder
            .encode_entry(&entry, fields)
            .replacen("\"level\":", "\"$lvl\":", 1)
            .replacen("\"message\":", "\"$msg\":", 1);
        buffer.push(&encoded);
    }

    #[track_caller]
    fn write_unfiltered(&self, level: Level, message: &str, fields: &[Field]) {
        let mut all_fields = self.fields.clone();
        all_fields.extend_from_slice(fields);
        match &self.backend {
            Backend::Zap(logger) => logger.log_at(level, message, &all_fields),
            Backend::Test { encoder, buffer } => {
                Self::write_test(encoder, buffer, level, message, &all_fields)
            }
        }
    }

    #[track_caller]
    fn write(&self, level: Level, message: &str, fields: &[Field]) {
        if !self.enabled(level) {
            return;
        }
        let caller = Self::qualified_caller(std::panic::Location::caller().file());
        if self.caller_allowed(&caller) {
            self.write_unfiltered(level, message, fields);
        }
    }

    /// Logs at an explicit level.
    #[track_caller]
    pub fn log(&self, level: Level, message: &str, fields: &[Field]) {
        self.write(level, message, fields);
    }

    /// Logs at debug level.
    #[track_caller]
    pub fn debug(&self, message: &str, fields: &[Field]) {
        self.write(Level::Debug, message, fields);
    }

    /// Logs at info level.
    #[track_caller]
    pub fn info(&self, message: &str, fields: &[Field]) {
        self.write(Level::Info, message, fields);
    }

    /// Logs at warning level.
    #[track_caller]
    pub fn warn(&self, message: &str, fields: &[Field]) {
        self.write(Level::Warn, message, fields);
    }

    /// Logs at error level.
    #[track_caller]
    pub fn error(&self, message: &str, fields: &[Field]) {
        self.write(Level::Error, message, fields);
    }

    /// Flushes buffered output.
    pub fn sync(&self) -> io::Result<()> {
        match &self.backend {
            Backend::Zap(logger) => logger.sync(),
            Backend::Test { .. } => Ok(()),
        }
    }

    /// Begins a timed task at `level`.
    #[track_caller]
    pub fn begin(&self, level: Level, name: &str) -> Task {
        self.log(level, &format!("{name} start"), &[]);
        Task {
            logger: self.clone(),
            level,
            name: name.to_owned(),
            since: Instant::now(),
        }
    }
}

/// Wraps a canonical logger as a Lightning logger.
pub fn wrap(logger: ZapLogger) -> Logger {
    Logger::from_zap(logger)
}

#[derive(Debug)]
struct NullSink;

impl WriteSyncer for NullSink {
    fn write(&self, bytes: &[u8]) -> io::Result<usize> {
        Ok(bytes.len())
    }
}

fn default_state() -> AppState {
    let config = tidb_log::Config {
        level: "info".to_owned(),
        disable_caller: true,
        disable_stacktrace: true,
        ..Default::default()
    };
    let (logger, properties) = tidb_log::init_test_logger(Arc::new(NullSink), &config)
        .expect("default Lightning logger config is valid");
    AppState {
        logger: Logger::from_zap(logger),
        level: properties.level,
    }
}

struct AppState {
    logger: Logger,
    level: AtomicLevel,
}

fn app_state() -> &'static RwLock<AppState> {
    static STATE: OnceLock<RwLock<AppState>> = OnceLock::new();
    STATE.get_or_init(|| RwLock::new(default_state()))
}

/// Initializes Lightning and TiDB library logging.
pub fn init_logger(config: &Config, _level: &str) -> Result<(), String> {
    let filters = if config.enable_diagnose_logs {
        std::env::set_var(GRPC_DEBUG_ENV_NAME, "true");
        Vec::new()
    } else {
        vec![
            "github.com/pingcap/tidb/br/".to_owned(),
            "/lightning/".to_owned(),
            "/ingestor/ingestctrl".to_owned(),
            "main.main".to_owned(),
            "github.com/tikv/pd/client".to_owned(),
        ]
    };

    let tidb_config = crate::logutil::LogConfig {
        config: tidb_log::Config {
            level: "fatal".to_owned(),
            ..Default::default()
        },
        ..Default::default()
    };
    crate::logutil::init_logger(&tidb_config)?;

    if !config.file.is_empty() && config.file != "-" && Path::new(&config.file).is_dir() {
        return Err("can't use directory as log file name".to_owned());
    }

    let mut logger_config = tidb_log::Config {
        level: if config.level.is_empty() {
            DEFAULT_LOG_LEVEL.to_owned()
        } else {
            config.level.clone()
        },
        disable_caller: false,
        ..Default::default()
    };
    if !config.file.is_empty() && config.file != "-" {
        logger_config.file = tidb_log::FileLogConfig {
            filename: config.file.clone(),
            max_size: config.file_max_size as i64,
            max_days: config.file_max_days as i64,
            max_backups: config.file_max_backups as i64,
            ..Default::default()
        };
    }
    let (logger, properties) = tidb_log::init_logger(&logger_config)?;
    let logger = logger.with_options(LoggerOptions::default().with_stacktrace_level(Level::DPanic));
    let logger = if filters.is_empty() {
        logger
    } else {
        let global_filters = filters.clone();
        logger.with_caller_filter(move |file| {
            let caller = Logger::qualified_caller(file);
            global_filters
                .iter()
                .any(|filter| caller.contains(filter.as_str()))
        })
    };
    let _ = tidb_log::replace_globals(logger.clone());
    *app_state()
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = AppState {
        logger: Logger::from_zap(logger).filtered(filters),
        level: properties.level,
    };
    Ok(())
}

/// Replaces the Lightning application logger.
pub fn set_app_logger(logger: ZapLogger) {
    let logger = logger.with_options(LoggerOptions::default().with_stacktrace_level(Level::DPanic));
    app_state()
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .logger = Logger::from_zap(logger);
}

/// Returns the current Lightning logger.
pub fn l() -> Logger {
    app_state()
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .logger
        .clone()
}

/// Returns the current global Lightning log level.
pub fn level() -> Level {
    app_state()
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .level
        .get()
}

/// Changes the global Lightning level and returns the previous value.
pub fn set_level(new_level: Level) -> Level {
    let state = app_state()
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let old_level = state.level.get();
    state.level.set(new_level);
    old_level
}

/// Creates a child of the global logger with structured fields.
pub fn with(fields: &[Field]) -> Logger {
    l().with(fields)
}

/// Creates the short error field used by Lightning.
pub fn short_error(error: Option<&dyn Error>) -> Option<Field> {
    error.map(|error| Field::new("error", Value::Str(error.to_string())))
}

/// The native sentinel corresponding to Go `context.Canceled`.
#[derive(Clone, Copy, Debug)]
pub struct ContextCanceled;

impl fmt::Display for ContextCanceled {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("context canceled")
    }
}

impl Error for ContextCanceled {}

/// Native representation of Smithy's cancellation wrapper.
#[derive(Debug)]
pub struct SmithyCanceledError {
    source: Box<dyn Error + Send + Sync>,
}

impl SmithyCanceledError {
    /// Creates a Smithy cancellation wrapper.
    pub fn new(source: impl Error + Send + Sync + 'static) -> Self {
        Self {
            source: Box::new(source),
        }
    }
}

impl fmt::Display for SmithyCanceledError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.source.fmt(formatter)
    }
}

impl Error for SmithyCanceledError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.source.as_ref())
    }
}

/// Native representation of Smithy's operation-error wrapper.
#[derive(Debug)]
pub struct SmithyOperationError {
    /// Service identifier.
    pub service_id: String,
    /// Operation name.
    pub operation_name: String,
    source: Box<dyn Error + Send + Sync>,
}

impl SmithyOperationError {
    /// Creates an operation-error wrapper.
    pub fn new(
        service_id: impl Into<String>,
        operation_name: impl Into<String>,
        source: impl Error + Send + Sync + 'static,
    ) -> Self {
        Self {
            service_id: service_id.into(),
            operation_name: operation_name.into(),
            source: Box::new(source),
        }
    }
}

impl fmt::Display for SmithyOperationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.source.fmt(formatter)
    }
}

impl Error for SmithyOperationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.source.as_ref())
    }
}

/// Reports whether an error is caused by context cancellation.
pub fn is_context_canceled_error(error: Option<&(dyn Error + 'static)>) -> bool {
    let mut current = error;
    while let Some(error) = current {
        if error.downcast_ref::<ContextCanceled>().is_some()
            || error.downcast_ref::<SmithyCanceledError>().is_some()
            || error
                .downcast_ref::<tonic::Status>()
                .is_some_and(|status| status.code() == tonic::Code::Cancelled)
        {
            return true;
        }
        current = error.source();
    }
    false
}

/// A caller-qualified entry accepted by [`FilterCore`].
pub struct FilterEntry {
    /// Log level.
    pub level: Level,
    /// Log message.
    pub message: String,
    /// Package-qualified caller function.
    pub caller_function: String,
}

/// A logging core which writes only callers containing an allowed package.
#[derive(Clone)]
pub struct FilterCore {
    core: TextIoCore,
    filters: Arc<Vec<String>>,
}

impl FilterCore {
    /// Creates a filtered wrapper around `core`.
    pub fn new(core: TextIoCore, allow_packages: impl IntoIterator<Item = String>) -> Self {
        Self {
            core,
            filters: Arc::new(allow_packages.into_iter().collect()),
        }
    }

    /// Returns a filtered core with structured context.
    pub fn with(&self, fields: &[Field]) -> Self {
        Self {
            core: self.core.with(fields),
            filters: Arc::clone(&self.filters),
        }
    }

    /// Reports whether the wrapped core enables the entry's level.
    pub fn check(&self, entry: &FilterEntry) -> bool {
        self.core.enabled(entry.level)
    }

    /// Writes an entry only when its caller function matches a filter.
    pub fn write(&self, entry: &FilterEntry, fields: &[Field]) -> Result<(), String> {
        if !self
            .filters
            .iter()
            .any(|filter| entry.caller_function.contains(filter.as_str()))
        {
            return Ok(());
        }
        self.core.write(
            &Entry {
                time: Local::now().fixed_offset(),
                level: entry.level,
                logger_name: String::new(),
                caller: None,
                message: entry.message.clone(),
                stack: String::new(),
            },
            fields,
        )
    }
}

/// Buffer returned by [`make_test_logger`].
#[derive(Default, Debug)]
pub struct TestBuffer(Mutex<String>);

impl TestBuffer {
    fn push(&self, value: &str) {
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push_str(value);
    }

    /// Returns captured output without trailing newlines.
    pub fn stripped(&self) -> String {
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .trim_end_matches('\n')
            .to_owned()
    }
}

/// Creates the source JSON test logger and its buffer.
pub fn make_test_logger() -> (Logger, Arc<TestBuffer>) {
    let config = tidb_log::Config {
        level: "debug".to_owned(),
        format: "json".to_owned(),
        disable_timestamp: true,
        disable_caller: true,
        disable_stacktrace: true,
        ..Default::default()
    };
    let encoder = TextEncoder::new(&config).expect("test logger config is valid");
    let buffer = Arc::new(TestBuffer::default());
    (
        Logger {
            backend: Backend::Test {
                encoder,
                buffer: Arc::clone(&buffer),
            },
            fields: Vec::new(),
            filters: Arc::new(Vec::new()),
        },
        buffer,
    )
}

/// A timed task logger.
pub struct Task {
    logger: Logger,
    level: Level,
    name: String,
    since: Instant,
}

impl Deref for Task {
    type Target = Logger;

    fn deref(&self) -> &Self::Target {
        &self.logger
    }
}

impl Task {
    /// Ends a task, demoting cancellation to debug and logging short errors.
    #[track_caller]
    pub fn end(
        &self,
        failed_level: Level,
        error: Option<&(dyn Error + 'static)>,
        extra_fields: &[Field],
    ) -> Duration {
        let elapsed = self.since.elapsed();
        let (level, verb, fields) = if error.is_none() {
            (self.level, " completed", extra_fields.to_vec())
        } else if is_context_canceled_error(error) {
            (Level::Debug, " canceled", Vec::new())
        } else {
            (failed_level, " failed", Vec::new())
        };
        let mut fields = fields;
        fields.push(Field::new(
            "takeTime",
            Value::Duration(elapsed.as_nanos() as i64),
        ));
        if let Some(field) = short_error(error.map(|error| error as &dyn Error)) {
            fields.push(field);
        }
        self.logger
            .log(level, &format!("{}{verb}", self.name), &fields);
        elapsed
    }

    /// Ends a task without cancellation handling and logs a full error field.
    #[track_caller]
    pub fn end2(
        &self,
        failed_level: Level,
        error: Option<&(dyn Error + 'static)>,
        extra_fields: &[Field],
    ) -> Duration {
        let elapsed = self.since.elapsed();
        let (level, verb, mut fields) = if error.is_some() {
            (failed_level, " failed", Vec::new())
        } else {
            (self.level, " completed", extra_fields.to_vec())
        };
        fields.push(Field::new(
            "takeTime",
            Value::Duration(elapsed.as_nanos() as i64),
        ));
        if let Some(error) = error {
            fields.push(Field::new(
                "error",
                Value::Error {
                    basic: error.to_string(),
                    verbose: Some(format!("{error:?}")),
                },
            ));
        }
        self.logger
            .log(level, &format!("{}{verb}", self.name), &fields);
        elapsed
    }
}

/// Begins an info-level task from a canonical logger.
#[track_caller]
pub fn begin_task(logger: ZapLogger, name: &str) -> Task {
    Logger::from_zap(logger).begin(Level::Info, name)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn number_fields() -> Vec<Field> {
        vec![
            Field::new("number", Value::I64(123456)),
            Field::new(
                "array",
                Value::Array(vec![Value::I64(7), Value::I64(8), Value::I64(9)]),
            ),
        ]
    }

    #[test]
    fn filter() {
        let (logger, buffer) = make_test_logger();
        logger.warn("the message", &number_fields());
        assert_eq!(
            buffer.stripped(),
            r#"{"$lvl":"WARN","$msg":"the message","number":123456,"array":[7,8,9]}"#
        );

        let (logger, buffer) = make_test_logger();
        logger
            .filtered(vec!["github.com/pingcap/br/".to_owned()])
            .warn("the message", &number_fields());
        assert!(buffer.stripped().is_empty());

        let (logger, buffer) = make_test_logger();
        logger
            .filtered(vec!["/lightning/".to_owned()])
            .with(&[Field::new("a", Value::Str("b".to_owned()))])
            .warn("the message", &number_fields());
        assert_eq!(
            buffer.stripped(),
            r#"{"$lvl":"WARN","$msg":"the message","a":"b","number":123456,"array":[7,8,9]}"#
        );

        let (logger, buffer) = make_test_logger();
        let logger = logger
            .filtered(vec!["github.com/pingcap/br/".to_owned()])
            .with(&[Field::new("a", Value::Str("b".to_owned()))]);
        logger.warn("the message", &number_fields());
        logger.warn(
            "the message",
            &[Field::new(
                "stack",
                Value::Str("github.com/pingcap/tidb/br/".to_owned()),
            )],
        );
        assert!(buffer.stripped().is_empty());

        let sink = Arc::new(tidb_log::MemorySink::default());
        let config = tidb_log::Config {
            level: "debug".to_owned(),
            format: "json".to_owned(),
            disable_timestamp: true,
            disable_caller: true,
            disable_stacktrace: true,
            ..Default::default()
        };
        let encoder = TextEncoder::new(&config).unwrap();
        let output: Arc<dyn WriteSyncer> = sink.clone();
        let core = TextIoCore::new(encoder, output, AtomicLevel::new(Level::Debug));
        let entry = FilterEntry {
            level: Level::Warn,
            message: "retryable write".to_owned(),
            caller_function:
                "github.com/pingcap/tidb/pkg/ingestor/ingestctrl.(*regionJobBaseWorker).runJob"
                    .to_owned(),
        };
        let core = FilterCore::new(core, ["/ingestor/ingestctrl".to_owned()]);
        core.write(&entry, &[]).unwrap();
        assert!(sink.string().contains("retryable write"));

        let sink = Arc::new(tidb_log::MemorySink::default());
        let encoder = TextEncoder::new(&config).unwrap();
        let output: Arc<dyn WriteSyncer> = sink.clone();
        let core = FilterCore::new(
            TextIoCore::new(encoder, output, AtomicLevel::new(Level::Debug)),
            ["/ingestor/ingestctrl/".to_owned()],
        );
        core.write(&entry, &[]).unwrap();
        assert!(sink.string().is_empty());
    }

    #[test]
    fn config_adjust() {
        let mut config = Config::default();
        config.adjust();
        assert_eq!(config.level, "info");
        config.file = ".".to_owned();
        assert_eq!(
            init_logger(&config, "info").unwrap_err(),
            "can't use directory as log file name"
        );
    }

    #[test]
    fn test_logger() {
        let (logger, buffer) = make_test_logger();
        logger.warn("the message", &number_fields());
        assert_eq!(
            buffer.stripped(),
            r#"{"$lvl":"WARN","$msg":"the message","number":123456,"array":[7,8,9]}"#
        );
    }

    #[test]
    fn init_stdout_logger() {
        #[cfg(not(windows))]
        use std::fs::File;
        #[cfg(not(windows))]
        use std::io::Read;

        std::env::remove_var(GRPC_DEBUG_ENV_NAME);
        #[cfg(not(windows))]
        let saved_stdout = rustix::io::dup(std::io::stdout()).unwrap();
        #[cfg(not(windows))]
        let (reader, writer) = rustix::pipe::pipe().unwrap();
        #[cfg(not(windows))]
        rustix::stdio::dup2_stdout(&writer).unwrap();

        let message = "logger is initialized to stdout";
        let mut config = Config {
            file: "-".to_owned(),
            ..Default::default()
        };
        init_logger(&config, "info").unwrap();
        l().info(message, &[]);
        l().sync().unwrap();

        #[cfg(not(windows))]
        {
            rustix::stdio::dup2_stdout(&saved_stdout).unwrap();
            drop(writer);
            let mut output = String::new();
            File::from(reader).read_to_string(&mut output).unwrap();
            assert!(output.contains(message));
        }

        assert!(std::env::var_os(GRPC_DEBUG_ENV_NAME).is_none());
        assert!(l().is_filtered());
        config.enable_diagnose_logs = true;
        init_logger(&config, "info").unwrap();
        assert!(!l().is_filtered());
        assert_eq!(std::env::var(GRPC_DEBUG_ENV_NAME).unwrap(), "true");
        std::env::remove_var(GRPC_DEBUG_ENV_NAME);
    }

    #[derive(Debug)]
    struct Annotated {
        source: ContextCanceled,
    }

    impl fmt::Display for Annotated {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("foo: context canceled")
        }
    }

    impl Error for Annotated {
        fn source(&self) -> Option<&(dyn Error + 'static)> {
            Some(&self.source)
        }
    }

    #[test]
    fn is_context_canceled_error() {
        let canceled = ContextCanceled;
        assert!(super::is_context_canceled_error(Some(&canceled)));

        let grpc = tonic::Status::cancelled("");
        assert!(super::is_context_canceled_error(Some(&grpc)));

        let annotated = Annotated {
            source: ContextCanceled,
        };
        assert!(super::is_context_canceled_error(Some(&annotated)));

        let smithy = SmithyCanceledError::new(ContextCanceled);
        assert!(super::is_context_canceled_error(Some(&smithy)));

        let operation = SmithyOperationError::new("TestService", "TestOperation", ContextCanceled);
        assert!(super::is_context_canceled_error(Some(&operation)));

        assert!(!super::is_context_canceled_error(None));
    }
}
