// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

//! Logger initialization, output sinks, buffering, rotation, and timeout
//! behavior transcreated from `log.go`.

use std::backtrace::Backtrace;
use std::collections::HashMap;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, TryLockError, Weak};
use std::time::{Duration, Instant, SystemTime};

use chrono::Local;
use flate2::write::GzEncoder;
use flate2::Compression;

use crate::config::SamplingConfig;
use crate::zap_text_core::{AtomicLevel, TextIoCore, WriteSyncer};
use crate::{Config, Entry, Field, Level, TextEncoder};

/// Name registered by the Go package for its text encoder.
pub const ZAP_ENCODING_NAME: &str = "pingcap-log";

/// Mutable runtime properties returned by logger initialization.
#[derive(Clone)]
pub struct ZapProperties {
    /// The core shared by the returned logger.
    pub core: TextIoCore,
    /// The dynamically adjustable level.
    pub level: AtomicLevel,
    /// Primary output.
    pub syncer: Arc<dyn WriteSyncer>,
    /// Internal-error output.
    pub error_syncer: Arc<dyn WriteSyncer>,
}

impl fmt::Debug for ZapProperties {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ZapProperties")
            .field("level", &self.level)
            .finish_non_exhaustive()
    }
}

/// Options applied to a cloned logger.
#[derive(Clone, Copy, Debug, Default)]
pub struct LoggerOptions {
    caller_skip: usize,
    stacktrace_level: Option<Level>,
}

impl LoggerOptions {
    /// Mirrors `zap.AddCallerSkip`.
    pub fn with_caller_skip(mut self, caller_skip: usize) -> Self {
        self.caller_skip = caller_skip;
        self
    }

    /// Mirrors `zap.AddStacktrace`.
    pub fn with_stacktrace_level(mut self, level: Level) -> Self {
        self.stacktrace_level = Some(level);
        self
    }
}

#[derive(Debug)]
struct SampleWindow {
    started: Instant,
    count: u64,
}

#[derive(Debug)]
struct Sampler {
    initial: u64,
    thereafter: u64,
    windows: Mutex<HashMap<(Level, String), SampleWindow>>,
}

impl Sampler {
    fn new(config: &SamplingConfig) -> Self {
        Self {
            initial: config.initial.max(0) as u64,
            thereafter: config.thereafter.max(0) as u64,
            windows: Mutex::new(HashMap::new()),
        }
    }

    fn enabled(&self, level: Level, message: &str) -> bool {
        let mut windows = self
            .windows
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let now = Instant::now();
        let window = windows
            .entry((level, message.to_owned()))
            .or_insert(SampleWindow {
                started: now,
                count: 0,
            });
        if now.duration_since(window.started) >= Duration::from_secs(1) {
            window.started = now;
            window.count = 0;
        }
        window.count += 1;
        window.count <= self.initial
            || (self.thereafter > 0
                && (window.count - self.initial).is_multiple_of(self.thereafter))
    }
}

/// A clonable structured logger.
#[derive(Clone)]
pub struct Logger {
    core: TextIoCore,
    error_output: Arc<dyn WriteSyncer>,
    caller_enabled: bool,
    caller_skip: usize,
    stacktrace_level: Option<Level>,
    development: bool,
    sampler: Option<Arc<Sampler>>,
}

/// A convenience wrapper corresponding to zap's `SugaredLogger`.
#[derive(Clone, Debug)]
pub struct SugaredLogger(Logger);

impl SugaredLogger {
    /// Logs an unstructured debug message.
    #[track_caller]
    pub fn debug(&self, message: &str) {
        self.0.debug(message, &[]);
    }

    /// Logs an unstructured info message.
    #[track_caller]
    pub fn info(&self, message: &str) {
        self.0.info(message, &[]);
    }

    /// Logs an unstructured warning message.
    #[track_caller]
    pub fn warn(&self, message: &str) {
        self.0.warn(message, &[]);
    }

    /// Logs an unstructured error message.
    #[track_caller]
    pub fn error(&self, message: &str) {
        self.0.error(message, &[]);
    }

    /// Logs a formatted info message.
    #[track_caller]
    pub fn infof(&self, arguments: fmt::Arguments<'_>) {
        self.0.info(&arguments.to_string(), &[]);
    }

    /// Logs an info message with structured fields.
    #[track_caller]
    pub fn infow(&self, message: &str, fields: &[Field]) {
        self.0.info(message, fields);
    }

    /// Logs a debug message with structured fields.
    #[track_caller]
    pub fn debugw(&self, message: &str, fields: &[Field]) {
        self.0.debug(message, fields);
    }

    /// Logs a warning message with structured fields.
    #[track_caller]
    pub fn warnw(&self, message: &str, fields: &[Field]) {
        self.0.warn(message, fields);
    }

    /// Logs an error message with structured fields.
    #[track_caller]
    pub fn errorw(&self, message: &str, fields: &[Field]) {
        self.0.error(message, fields);
    }

    /// Logs and panics with `message`.
    #[track_caller]
    pub fn panic(&self, message: &str) -> ! {
        self.0.panic(message, &[])
    }

    /// Logs and exits the process with status 1.
    #[track_caller]
    pub fn fatal(&self, message: &str) -> ! {
        self.0.fatal(message, &[])
    }

    /// Flushes buffered output.
    pub fn sync(&self) -> io::Result<()> {
        self.0.sync()
    }
}

impl fmt::Debug for Logger {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Logger")
            .field("level", &self.core.level().get())
            .field("caller_enabled", &self.caller_enabled)
            .field("caller_skip", &self.caller_skip)
            .field("stacktrace_level", &self.stacktrace_level)
            .field("development", &self.development)
            .finish_non_exhaustive()
    }
}

impl Logger {
    /// Returns a convenience wrapper for unstructured and `*w` logging.
    pub fn sugar(&self) -> SugaredLogger {
        SugaredLogger(self.clone())
    }

    /// Returns a child with structured context fields.
    pub fn with_fields(&self, fields: &[Field]) -> Self {
        let mut clone = self.clone();
        clone.core = self.core.with(fields);
        clone
    }

    /// Returns a child with zap-style options.
    pub fn with_options(&self, options: LoggerOptions) -> Self {
        let mut clone = self.clone();
        clone.caller_skip = clone.caller_skip.saturating_add(options.caller_skip);
        if let Some(level) = options.stacktrace_level {
            clone.stacktrace_level = Some(level);
        }
        clone
    }

    /// Returns the shared level controller.
    pub fn level_controller(&self) -> AtomicLevel {
        self.core.level()
    }

    /// Flushes buffered output.
    pub fn sync(&self) -> io::Result<()> {
        self.core.sync()
    }

    /// Logs at debug level.
    #[track_caller]
    pub fn debug(&self, message: &str, fields: &[Field]) {
        self.log(Level::Debug, message, fields);
    }

    /// Logs at info level.
    #[track_caller]
    pub fn info(&self, message: &str, fields: &[Field]) {
        self.log(Level::Info, message, fields);
    }

    /// Logs at warn level.
    #[track_caller]
    pub fn warn(&self, message: &str, fields: &[Field]) {
        self.log(Level::Warn, message, fields);
    }

    /// Logs at error level.
    #[track_caller]
    pub fn error(&self, message: &str, fields: &[Field]) {
        self.log(Level::Error, message, fields);
    }

    /// Logs and panics with `message`.
    #[track_caller]
    pub fn panic(&self, message: &str, fields: &[Field]) -> ! {
        self.log(Level::Panic, message, fields);
        panic!("{message}")
    }

    /// Logs and exits the process with status 1.
    #[track_caller]
    pub fn fatal(&self, message: &str, fields: &[Field]) -> ! {
        self.log(Level::Fatal, message, fields);
        let _ = self.sync();
        std::process::exit(1)
    }

    #[track_caller]
    fn log(&self, level: Level, message: &str, fields: &[Field]) {
        if !self.core.enabled(level) {
            return;
        }
        if let Some(sampler) = &self.sampler {
            if !sampler.enabled(level, message) {
                return;
            }
        }
        let location = std::panic::Location::caller();
        let caller = if self.caller_enabled && self.caller_skip == 0 {
            Some((location.file().to_owned(), location.line()))
        } else {
            None
        };
        let stack = self
            .stacktrace_level
            .filter(|threshold| level >= *threshold)
            .map(|_| Backtrace::force_capture().to_string())
            .unwrap_or_default();
        let entry = Entry {
            time: Local::now().fixed_offset(),
            level,
            logger_name: String::new(),
            caller,
            message: message.to_owned(),
            stack,
        };
        if let Err(error) = self.core.write(&entry, fields) {
            let _ = self.error_output.write(format!("{error}\n").as_bytes());
        }
        if level == Level::DPanic && self.development {
            panic!("{message}");
        }
    }
}

/// Initializes a logger using stdout or the configured rotating file.
pub fn init_logger(config: &Config) -> Result<(Logger, ZapProperties), String> {
    let output: Arc<dyn WriteSyncer> = if config.file.filename.is_empty() {
        Arc::new(StdoutSink)
    } else {
        let file = Arc::new(FileSink::new(&config.file)?);
        if config.file.is_buffered {
            BufferedSyncer::wrap(
                file,
                config.file.buffer_size,
                config.file.buffer_flush_interval,
            )
        } else {
            file
        }
    };
    let error_output: Arc<dyn WriteSyncer> = if config.error_output_path.is_empty() {
        output.clone()
    } else {
        open_sink(&config.error_output_path)?
    };
    init_logger_with_write_syncer(config, output, error_output)
}

/// Initializes a logger for tests with an in-memory or custom sink.
pub fn init_test_logger<W>(sink: Arc<W>, config: &Config) -> Result<(Logger, ZapProperties), String>
where
    W: WriteSyncer,
{
    let output: Arc<dyn WriteSyncer> = sink;
    init_logger_with_write_syncer(config, output.clone(), output)
}

/// Initializes a logger with explicit output and error sinks.
pub fn init_logger_with_write_syncer(
    config: &Config,
    mut output: Arc<dyn WriteSyncer>,
    mut error_output: Arc<dyn WriteSyncer>,
) -> Result<(Logger, ZapProperties), String> {
    let level = Level::parse(&config.level)?;
    let encoder = TextEncoder::new(config)?;
    if config.timeout > 0 {
        output = TimeoutSyncer::wrap(output, config.timeout as u64);
        error_output = TimeoutSyncer::wrap(error_output, config.timeout as u64);
    }
    let atomic_level = AtomicLevel::new(level);
    let core = TextIoCore::new(encoder, output.clone(), atomic_level.clone());
    let stacktrace_level = if config.disable_stacktrace {
        None
    } else if config.development {
        Some(Level::Warn)
    } else {
        Some(Level::Error)
    };
    let logger = Logger {
        core,
        error_output: error_output.clone(),
        caller_enabled: !config.disable_caller,
        caller_skip: 0,
        stacktrace_level,
        development: config.development,
        sampler: config
            .sampling
            .as_ref()
            .map(|sampling| Arc::new(Sampler::new(sampling))),
    };
    let properties = ZapProperties {
        core: logger.core.clone(),
        level: atomic_level,
        syncer: output,
        error_syncer: error_output,
    };
    Ok((logger, properties))
}

fn open_sink(path: &str) -> Result<Arc<dyn WriteSyncer>, String> {
    match path {
        "stdout" => Ok(Arc::new(StdoutSink)),
        "stderr" => Ok(Arc::new(StderrSink)),
        path => Ok(Arc::new(PlainFileSink::new(path)?)),
    }
}

#[derive(Debug)]
struct StdoutSink;

impl WriteSyncer for StdoutSink {
    fn write(&self, bytes: &[u8]) -> io::Result<usize> {
        let mut stdout = io::stdout().lock();
        stdout.write(bytes)
    }

    fn sync(&self) -> io::Result<()> {
        io::stdout().lock().flush()
    }
}

#[derive(Debug)]
struct StderrSink;

impl WriteSyncer for StderrSink {
    fn write(&self, bytes: &[u8]) -> io::Result<usize> {
        let mut stderr = io::stderr().lock();
        stderr.write(bytes)
    }

    fn sync(&self) -> io::Result<()> {
        io::stderr().lock().flush()
    }
}

#[derive(Debug)]
struct PlainFileSink(Mutex<File>);

impl PlainFileSink {
    fn new(path: &str) -> Result<Self, String> {
        if let Some(parent) = Path::new(path).parent() {
            fs::create_dir_all(parent).map_err(|error| error.to_string())?;
        }
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|error| error.to_string())?;
        Ok(Self(Mutex::new(file)))
    }
}

impl WriteSyncer for PlainFileSink {
    fn write(&self, bytes: &[u8]) -> io::Result<usize> {
        self.0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .write(bytes)
    }

    fn sync(&self) -> io::Result<()> {
        self.0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .flush()
    }
}

/// In-memory test sink corresponding to `zap_test_logger.go`.
#[derive(Default, Debug)]
pub struct MemorySink(Mutex<Vec<u8>>);

impl MemorySink {
    /// Returns all captured bytes as lossily decoded UTF-8.
    pub fn string(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().unwrap_or_else(|error| error.into_inner()))
            .into_owned()
    }

    /// Returns the last encoded line without its newline.
    pub fn last_line(&self) -> Option<String> {
        self.string().lines().next_back().map(str::to_owned)
    }
}

impl WriteSyncer for MemorySink {
    fn write(&self, bytes: &[u8]) -> io::Result<usize> {
        self.0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .write(bytes)
    }
}

struct TimeoutSyncer {
    inner: Arc<dyn WriteSyncer>,
    lock: Mutex<()>,
    timeout: Duration,
}

impl TimeoutSyncer {
    fn wrap(inner: Arc<dyn WriteSyncer>, timeout_seconds: u64) -> Arc<dyn WriteSyncer> {
        Arc::new(Self {
            inner,
            lock: Mutex::new(()),
            timeout: Duration::from_secs(timeout_seconds),
        })
    }

    fn with_lock<T>(
        &self,
        operation: impl FnOnce(&dyn WriteSyncer) -> io::Result<T>,
    ) -> io::Result<T> {
        let deadline = Instant::now() + self.timeout;
        loop {
            match self.lock.try_lock() {
                Ok(_guard) => return operation(self.inner.as_ref()),
                Err(TryLockError::Poisoned(error)) => {
                    let _guard = error.into_inner();
                    return operation(self.inner.as_ref());
                }
                Err(TryLockError::WouldBlock) if Instant::now() >= deadline => {
                    panic!(
                        "Timeout of {}s when trying to write log",
                        self.timeout.as_secs()
                    );
                }
                Err(TryLockError::WouldBlock) => std::thread::sleep(Duration::from_millis(10)),
            }
        }
    }
}

impl WriteSyncer for TimeoutSyncer {
    fn write(&self, bytes: &[u8]) -> io::Result<usize> {
        self.with_lock(|inner| inner.write(bytes))
    }

    fn sync(&self) -> io::Result<()> {
        self.with_lock(WriteSyncer::sync)
    }
}

/// Wraps a sink with source-compatible concurrent-write timeout behavior.
pub fn lock_with_timeout<W>(sink: Arc<W>, timeout_seconds: u64) -> Arc<dyn WriteSyncer>
where
    W: WriteSyncer,
{
    let sink: Arc<dyn WriteSyncer> = sink;
    TimeoutSyncer::wrap(sink, timeout_seconds)
}

#[derive(Debug)]
struct FileSinkState {
    sequence: u64,
}

#[derive(Debug)]
struct FileSink {
    filename: PathBuf,
    max_size: u64,
    max_days: i64,
    max_backups: i64,
    compress: bool,
    state: Mutex<FileSinkState>,
}

impl FileSink {
    fn new(config: &crate::FileLogConfig) -> Result<Self, String> {
        let filename = PathBuf::from(&config.filename);
        let parent = filename.parent().unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent)
            .map_err(|error| format!("cannot create log directory: {error}"))?;
        ensure_writable_directory(parent)?;

        match fs::metadata(&filename) {
            Ok(metadata) if metadata.is_dir() => {
                return Err("can't use directory as log file name".to_owned());
            }
            Ok(metadata) => {
                ensure_writable_file(&filename, &metadata)?;
                OpenOptions::new()
                    .append(true)
                    .open(&filename)
                    .map_err(|error| format!("can't write to log file: {error}"))?;
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                File::create(&filename)
                    .map_err(|error| format!("can't create log file: {error}"))?;
                fs::remove_file(&filename).map_err(|error| error.to_string())?;
            }
            Err(error) => return Err(format!("error checking log file: {error}")),
        }

        let compress = match config.compression.as_str() {
            "" => false,
            "gzip" => true,
            other => return Err(format!("can't set compression to `{other}`")),
        };
        let max_size_mb = if config.max_size == 0 {
            crate::DEFAULT_LOG_MAX_SIZE
        } else {
            config.max_size
        };
        Ok(Self {
            filename,
            max_size: max_size_mb.max(1) as u64 * 1024 * 1024,
            max_days: config.max_days,
            max_backups: config.max_backups,
            compress,
            state: Mutex::new(FileSinkState { sequence: 0 }),
        })
    }

    fn rotate(&self, state: &mut FileSinkState) -> io::Result<()> {
        state.sequence += 1;
        let rotated = PathBuf::from(format!("{}.{}", self.filename.display(), state.sequence));
        fs::rename(&self.filename, &rotated)?;
        if self.compress {
            let gzip_path = PathBuf::from(format!("{}.gz", rotated.display()));
            let mut input = File::open(&rotated)?;
            let output = File::create(&gzip_path)?;
            let mut encoder = GzEncoder::new(output, Compression::default());
            io::copy(&mut input, &mut encoder)?;
            encoder.finish()?;
            fs::remove_file(rotated)?;
        }
        self.cleanup_backups()
    }

    fn cleanup_backups(&self) -> io::Result<()> {
        let parent = self.filename.parent().unwrap_or_else(|| Path::new("."));
        let base = self
            .filename
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or_default();
        let mut backups: Vec<_> = fs::read_dir(parent)?
            .filter_map(Result::ok)
            .filter(|entry| {
                entry
                    .file_name()
                    .to_str()
                    .is_some_and(|name| name.starts_with(&format!("{base}.")))
            })
            .collect();
        backups.sort_by_key(|entry| {
            std::cmp::Reverse(
                entry
                    .metadata()
                    .and_then(|metadata| metadata.modified())
                    .unwrap_or(SystemTime::UNIX_EPOCH),
            )
        });
        let expiry = (self.max_days > 0).then(|| {
            SystemTime::now()
                .checked_sub(Duration::from_secs(self.max_days as u64 * 24 * 60 * 60))
                .unwrap_or(SystemTime::UNIX_EPOCH)
        });
        for (index, entry) in backups.into_iter().enumerate() {
            let too_many = self.max_backups > 0 && index >= self.max_backups as usize;
            let too_old = expiry.is_some_and(|expiry| {
                entry
                    .metadata()
                    .and_then(|metadata| metadata.modified())
                    .map(|modified| modified < expiry)
                    .unwrap_or(false)
            });
            if too_many || too_old {
                fs::remove_file(entry.path())?;
            }
        }
        Ok(())
    }
}

impl WriteSyncer for FileSink {
    fn write(&self, bytes: &[u8]) -> io::Result<usize> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let current_size = fs::metadata(&self.filename)
            .map(|metadata| metadata.len())
            .unwrap_or(0);
        if current_size > 0 && current_size.saturating_add(bytes.len() as u64) > self.max_size {
            self.rotate(&mut state)?;
        }
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.filename)?
            .write(bytes)
    }

    fn sync(&self) -> io::Result<()> {
        match OpenOptions::new().append(true).open(&self.filename) {
            Ok(file) => file.sync_all(),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
        }
    }
}

#[cfg(unix)]
fn ensure_writable_directory(path: &Path) -> Result<(), String> {
    use std::os::unix::fs::PermissionsExt;

    let mode = fs::metadata(path)
        .map_err(|error| error.to_string())?
        .permissions()
        .mode();
    if mode & 0o222 == 0 {
        return Err(format!("permission denied: {}", path.display()));
    }
    Ok(())
}

#[cfg(not(unix))]
fn ensure_writable_directory(_path: &Path) -> Result<(), String> {
    Ok(())
}

#[cfg(unix)]
fn ensure_writable_file(path: &Path, metadata: &fs::Metadata) -> Result<(), String> {
    use std::os::unix::fs::PermissionsExt;

    if metadata.permissions().mode() & 0o222 == 0 {
        return Err(format!("permission denied: {}", path.display()));
    }
    Ok(())
}

#[cfg(not(unix))]
fn ensure_writable_file(_path: &Path, _metadata: &fs::Metadata) -> Result<(), String> {
    Ok(())
}

struct BufferedSyncer {
    inner: Arc<dyn WriteSyncer>,
    buffer: Mutex<Vec<u8>>,
    max_size: usize,
}

impl BufferedSyncer {
    fn wrap(
        inner: Arc<dyn WriteSyncer>,
        configured_size: i64,
        configured_interval_ns: i64,
    ) -> Arc<dyn WriteSyncer> {
        let max_size = if configured_size > 0 {
            configured_size as usize
        } else {
            256 * 1024
        };
        let interval = if configured_interval_ns > 0 {
            Duration::from_nanos(configured_interval_ns as u64)
        } else {
            Duration::from_secs(30)
        };
        let syncer = Arc::new(Self {
            inner,
            buffer: Mutex::new(Vec::with_capacity(max_size)),
            max_size,
        });
        let weak: Weak<Self> = Arc::downgrade(&syncer);
        std::thread::spawn(move || loop {
            std::thread::sleep(interval);
            let Some(syncer) = weak.upgrade() else {
                break;
            };
            let _ = syncer.flush();
        });
        syncer
    }

    fn flush(&self) -> io::Result<()> {
        let bytes = {
            let mut buffer = self
                .buffer
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if buffer.is_empty() {
                return self.inner.sync();
            }
            std::mem::take(&mut *buffer)
        };
        self.inner.write(&bytes)?;
        self.inner.sync()
    }
}

impl WriteSyncer for BufferedSyncer {
    fn write(&self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() >= self.max_size {
            self.flush()?;
            return self.inner.write(bytes);
        }
        let should_flush = {
            let mut buffer = self
                .buffer
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            buffer.extend_from_slice(bytes);
            buffer.len() >= self.max_size
        };
        if should_flush {
            self.flush()?;
        }
        Ok(bytes.len())
    }

    fn sync(&self) -> io::Result<()> {
        self.flush()
    }
}
