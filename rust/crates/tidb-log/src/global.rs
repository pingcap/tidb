// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

//! Process-global logger exports transcreated from `global.go` and the global
//! portion of `log.go`.

use std::sync::{OnceLock, RwLock};

use crate::{init_logger, Config, Field, Level, Logger, SugaredLogger};

fn globals() -> &'static RwLock<Logger> {
    static GLOBALS: OnceLock<RwLock<Logger>> = OnceLock::new();
    GLOBALS.get_or_init(|| {
        let config = Config {
            level: "info".to_owned(),
            ..Config::default()
        };
        let (logger, _) = init_logger(&config).expect("default logger config must be valid");
        RwLock::new(logger)
    })
}

/// Returns the current global logger.
pub fn l() -> Logger {
    globals()
        .read()
        .unwrap_or_else(|error| error.into_inner())
        .clone()
}

/// Returns the current global sugared logger.
pub fn s() -> SugaredLogger {
    l().sugar()
}

/// A one-shot restoration handle returned by [`replace_globals`].
#[derive(Debug)]
pub struct GlobalRestore(Option<Logger>);

impl GlobalRestore {
    /// Restores the logger active before replacement.
    pub fn restore(mut self) {
        if let Some(previous) = self.0.take() {
            *globals().write().unwrap_or_else(|error| error.into_inner()) = previous;
        }
    }
}

/// Replaces the process-global logger and returns a restoration handle.
pub fn replace_globals(logger: Logger) -> GlobalRestore {
    let mut global = globals().write().unwrap_or_else(|error| error.into_inner());
    GlobalRestore(Some(std::mem::replace(&mut *global, logger)))
}

/// Flushes the global logger.
pub fn sync() -> std::io::Result<()> {
    l().sync()?;
    s().sync()
}

/// Changes the global logging level.
pub fn set_level(level: Level) {
    l().level_controller().set(level);
}

/// Returns the global logging level.
pub fn get_level() -> Level {
    l().level_controller().get()
}

/// Returns a child of the global logger with structured fields.
pub fn with(fields: &[Field]) -> Logger {
    l().with_fields(fields)
}

/// Logs at debug level through the global logger.
#[track_caller]
pub fn debug(message: &str, fields: &[Field]) {
    l().debug(message, fields);
}

/// Logs at info level through the global logger.
#[track_caller]
pub fn info(message: &str, fields: &[Field]) {
    l().info(message, fields);
}

/// Logs at warn level through the global logger.
#[track_caller]
pub fn warn(message: &str, fields: &[Field]) {
    l().warn(message, fields);
}

/// Logs at error level through the global logger.
#[track_caller]
pub fn error(message: &str, fields: &[Field]) {
    l().error(message, fields);
}

/// Logs and panics through the global logger.
#[track_caller]
pub fn panic(message: &str, fields: &[Field]) -> ! {
    l().panic(message, fields)
}

/// Logs and exits through the global logger.
#[track_caller]
pub fn fatal(message: &str, fields: &[Field]) -> ! {
    l().fatal(message, fields)
}
