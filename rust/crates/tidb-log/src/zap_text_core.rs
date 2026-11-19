// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

//! Rust counterpart of `zap_text_core.go`: an encoder, a write/sync target,
//! and a dynamically adjustable level gate.

use std::io;
use std::sync::{Arc, RwLock};

use crate::{Entry, Field, Level, TextEncoder};

/// A thread-safe log destination equivalent to zap's `WriteSyncer`.
/// zap `zapcore.Core`: where a logger's entries go. A [`crate::Logger`] is
/// a core plus its options (`zap.New(core, opts...)`); a test swaps the
/// global logger's core to observe entries (`zap.New(hookCore)`).
pub trait Core: Send + Sync + 'static {
    /// zap `Enabled`: whether an entry at `level` is written.
    fn enabled(&self, level: Level) -> bool;
    /// zap `With`: a core carrying `fields` as context.
    fn with(&self, fields: &[Field]) -> Arc<dyn Core>;
    /// zap `Write`: writes one entry (already checked by [`Core::enabled`]).
    fn write(&self, entry: &Entry, fields: &[Field]) -> Result<(), String>;
    /// zap `Sync`.
    fn sync(&self) -> io::Result<()> {
        Ok(())
    }
    /// The core's level controller (zap's `AtomicLevel` as its
    /// `LevelEnabler`); a core with a fixed level answers a fresh one.
    fn level(&self) -> AtomicLevel;
}

/// zap `zapcore.WriteSyncer`: an output entries are written to.
pub trait WriteSyncer: Send + Sync + 'static {
    /// Writes one encoded log entry.
    fn write(&self, bytes: &[u8]) -> io::Result<usize>;

    /// Flushes durable or buffered state.
    fn sync(&self) -> io::Result<()> {
        Ok(())
    }
}

/// Dynamically adjustable level shared by a logger and its properties.
#[derive(Clone, Debug)]
pub struct AtomicLevel(Arc<RwLock<Level>>);

impl AtomicLevel {
    /// Creates a level controller.
    pub fn new(level: Level) -> Self {
        Self(Arc::new(RwLock::new(level)))
    }

    /// Returns the current threshold.
    pub fn get(&self) -> Level {
        *self.0.read().unwrap_or_else(|error| error.into_inner())
    }

    /// Changes the threshold.
    pub fn set(&self, level: Level) {
        *self.0.write().unwrap_or_else(|error| error.into_inner()) = level;
    }
}

/// The source-shaped logging core.
#[derive(Clone)]
pub struct TextIoCore {
    encoder: TextEncoder,
    output: Arc<dyn WriteSyncer>,
    level: AtomicLevel,
}

impl TextIoCore {
    /// Creates a core that writes encoded entries to `output`.
    pub fn new(encoder: TextEncoder, output: Arc<dyn WriteSyncer>, level: AtomicLevel) -> Self {
        Self {
            encoder,
            output,
            level,
        }
    }

    /// Returns whether an entry at `level` is enabled.
    pub fn enabled(&self, level: Level) -> bool {
        level >= self.level.get()
    }

    /// Returns a cloned core with additional logger context.
    pub fn with(&self, fields: &[Field]) -> Self {
        Self {
            encoder: self.encoder.with_fields(fields),
            output: self.output.clone(),
            level: self.level.clone(),
        }
    }

    /// Encodes and writes one entry. Disabled levels are a no-op.
    pub fn write(&self, entry: &Entry, fields: &[Field]) -> Result<(), String> {
        if !self.enabled(entry.level) {
            return Ok(());
        }
        let output = self.encoder.try_encode_entry(entry, fields)?;
        self.output
            .write(output.as_bytes())
            .map(|_| ())
            .map_err(|error| error.to_string())?;
        if entry.level > Level::Error {
            let _ = self.output.sync();
        }
        Ok(())
    }

    /// Flushes the output.
    pub fn sync(&self) -> io::Result<()> {
        self.output.sync()
    }

    /// Returns the shared level controller.
    pub fn level(&self) -> AtomicLevel {
        self.level.clone()
    }
}

impl Core for TextIoCore {
    fn enabled(&self, level: Level) -> bool {
        TextIoCore::enabled(self, level)
    }
    fn with(&self, fields: &[Field]) -> Arc<dyn Core> {
        Arc::new(TextIoCore::with(self, fields))
    }
    fn write(&self, entry: &Entry, fields: &[Field]) -> Result<(), String> {
        TextIoCore::write(self, entry, fields)
    }
    fn sync(&self) -> io::Result<()> {
        TextIoCore::sync(self)
    }
    fn level(&self) -> AtomicLevel {
        TextIoCore::level(self)
    }
}
