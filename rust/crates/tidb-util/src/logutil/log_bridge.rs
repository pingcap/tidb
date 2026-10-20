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

//! Routes `log` crate records into the global logger.
//!
//! client-rust, the Rust client-go, logs through the `log` facade. Go's
//! client-go logs through zap into TiDB's global logger (`log.L()`), so its
//! lines land in TiDB's log file in the unified format at client-go's own
//! levels, filtered by TiDB's configured level. This logger is the Rust seam
//! that does the same: a record's formatted arguments are the entry message
//! and its level maps onto zap's (zap has no trace level, so TRACE logs at
//! debug, as in the `tracing` bridge).
//!
//! The caller slot cannot name a Go `file.go:line` for a Rust call site; it
//! carries the record's Rust source file and line instead, shortened like
//! every other caller by `getCallerString`. A record without a file names
//! its module path.

use std::sync::Once;

use tidb_log::Level;

/// Installs the logger as the process-wide `log` logger, once. It reads the
/// current global logger per record, so later `init_logger`/`replace_logger`
/// calls take effect without reinstalling.
pub(super) fn install() {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        let _ = ::log::set_logger(&UnifiedLogLogger);
    });
    level_changed();
}

/// Re-reads the global level into `log`'s static filter, so records below
/// it (client-rust's per-request debug/trace records) are skipped at their
/// call sites instead of reaching the logger.
pub(super) fn level_changed() {
    ::log::set_max_level(max_level(super::get_level()));
}

fn max_level(level: Level) -> ::log::LevelFilter {
    match level {
        Level::Debug => ::log::LevelFilter::Trace,
        Level::Info => ::log::LevelFilter::Info,
        Level::Warn => ::log::LevelFilter::Warn,
        _ => ::log::LevelFilter::Error,
    }
}

fn level_of(level: ::log::Level) -> Level {
    match level {
        ::log::Level::Error => Level::Error,
        ::log::Level::Warn => Level::Warn,
        ::log::Level::Info => Level::Info,
        ::log::Level::Debug | ::log::Level::Trace => Level::Debug,
    }
}

struct UnifiedLogLogger;

impl ::log::Log for UnifiedLogLogger {
    fn enabled(&self, metadata: &::log::Metadata<'_>) -> bool {
        super::bg_logger().enabled(level_of(metadata.level()))
    }

    fn log(&self, record: &::log::Record<'_>) {
        let logger = super::bg_logger();
        let level = level_of(record.level());
        if !logger.enabled(level) {
            return;
        }
        let file = record.file().or(record.module_path()).unwrap_or_default();
        let message = record.args().to_string();
        logger.emit_at(
            level,
            file,
            record.line().unwrap_or_default(),
            &message,
            &[],
        );
    }

    fn flush(&self) {
        let _ = super::sync();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn levels_map_onto_zap_levels_with_trace_at_debug() {
        assert_eq!(level_of(::log::Level::Error), Level::Error);
        assert_eq!(level_of(::log::Level::Warn), Level::Warn);
        assert_eq!(level_of(::log::Level::Info), Level::Info);
        assert_eq!(level_of(::log::Level::Debug), Level::Debug);
        assert_eq!(level_of(::log::Level::Trace), Level::Debug);
        assert_eq!(max_level(Level::Debug), ::log::LevelFilter::Trace);
        assert_eq!(max_level(Level::Info), ::log::LevelFilter::Info);
        assert_eq!(max_level(Level::Warn), ::log::LevelFilter::Warn);
        assert_eq!(max_level(Level::Error), ::log::LevelFilter::Error);
        assert_eq!(max_level(Level::Fatal), ::log::LevelFilter::Error);
    }
}
