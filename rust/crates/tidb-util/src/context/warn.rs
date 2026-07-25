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

//! Transcreation of Go `pkg/util/context/warn.go`.

use std::fmt;
use std::sync::Mutex;

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use tidb_error::terror::TerrorError;

/// Level "Error" for `SHOW WARNINGS`.
pub const WARN_LEVEL_ERROR: &str = "Error";
/// Level "Warning" for `SHOW WARNINGS`.
pub const WARN_LEVEL_WARNING: &str = "Warning";
/// Level "Note" for `SHOW WARNINGS`.
pub const WARN_LEVEL_NOTE: &str = "Note";

/// The warning payload: Go's open `error` value, which the JSON form already
/// splits into a typed terror or a bare message. `errors.Cause` unwrapping has
/// no counterpart because Rust carries no `errors.Trace` wrapper layers.
#[derive(Clone, Debug)]
pub enum WarnErr {
    /// A typed `*terror.Error` (serialized in terror's compatible JSON form).
    Terror(TerrorError),
    /// Any other error, reduced to its message.
    Message(String),
}

impl fmt::Display for WarnErr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WarnErr::Terror(e) => e.fmt(f),
            WarnErr::Message(m) => f.write_str(m),
        }
    }
}

impl From<TerrorError> for WarnErr {
    fn from(e: TerrorError) -> Self {
        WarnErr::Terror(e)
    }
}

impl From<String> for WarnErr {
    fn from(m: String) -> Self {
        WarnErr::Message(m)
    }
}

impl From<&str> for WarnErr {
    fn from(m: &str) -> Self {
        WarnErr::Message(m.to_string())
    }
}

/// Relates a SQL warning and its level (Go `SQLWarn`).
#[derive(Clone, Debug)]
pub struct SqlWarn {
    /// The `SHOW WARNINGS` level.
    pub level: String,
    /// The warning payload.
    pub err: WarnErr,
}

/// Go `jsonSQLWarn`: the wire shape of a serialized warning.
#[derive(Serialize, Deserialize)]
struct JsonSqlWarn {
    level: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    err: Option<TerrorError>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    msg: String,
}

impl Serialize for SqlWarn {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let wire = match &self.err {
            // Only the innermost error matters (Go `errors.Cause`).
            WarnErr::Terror(e) => JsonSqlWarn {
                level: self.level.clone(),
                err: Some(e.clone()),
                msg: String::new(),
            },
            WarnErr::Message(m) => JsonSqlWarn {
                level: self.level.clone(),
                err: None,
                msg: m.clone(),
            },
        };
        wire.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for SqlWarn {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let wire = JsonSqlWarn::deserialize(deserializer)?;
        if wire.level.is_empty() {
            return Err(D::Error::custom("SQL warning requires a level"));
        }
        Ok(SqlWarn {
            level: wire.level,
            err: match wire.err {
                Some(e) => WarnErr::Terror(e),
                None => WarnErr::Message(wire.msg),
            },
        })
    }
}

/// Adds warnings (Go `WarnAppender`).
pub trait WarnAppender {
    /// Appends a warning.
    fn append_warning(&self, err: WarnErr);
    /// Appends a warning with level `Note`.
    fn append_note(&self, err: WarnErr);
}

/// Appends and reads warnings (Go `WarnHandler`).
pub trait WarnHandler: WarnAppender {
    /// Gets the warning count.
    fn warning_count(&self) -> usize;
    /// Truncates warnings beginning from `start` and returns the truncated
    /// tail. Deprecated in the source for the same read-after-truncate hazards.
    fn truncate_warnings(&self, start: usize) -> Vec<SqlWarn>;
    /// Copies the warnings out (Go reuses `dst`'s capacity; an owned `Vec` is
    /// the Rust boundary — the aliasing-avoidance Go tests pin is inherent to
    /// ownership here).
    fn copy_warnings(&self) -> Vec<SqlWarn>;
}

/// Detailed warning control (Go `WarnHandlerExt`).
pub trait WarnHandlerExt: WarnHandler {
    /// Appends multiple warnings.
    fn append_warnings(&self, warns: Vec<SqlWarn>);
    /// Appends a warning with level `Error`.
    fn append_error(&self, err: WarnErr);
    /// Gets all warnings (an owned copy; Go returns the internal slice with a
    /// do-not-modify contract that ownership makes unnecessary).
    fn get_warnings(&self) -> Vec<SqlWarn>;
    /// Resets all warnings directly.
    fn set_warnings(&self, warns: Vec<SqlWarn>);
    /// Returns the number of `Error`-level warnings and the total count.
    fn num_error_warnings(&self) -> (u16, usize);
}

/// The static warning store (Go `StaticWarnHandler`): a mutex-guarded list
/// capped at `u16::MAX` entries.
#[derive(Default)]
pub struct StaticWarnHandler {
    warnings: Mutex<Vec<SqlWarn>>,
}

impl StaticWarnHandler {
    /// Creates a handler preallocating `slice_cap` entries.
    #[must_use]
    pub fn new(slice_cap: usize) -> Self {
        StaticWarnHandler {
            warnings: Mutex::new(Vec::with_capacity(slice_cap)),
        }
    }

    /// Creates a handler copying the warnings from `h` (Go
    /// `NewStaticWarnHandlerWithHandler`; Go's nil handler is `None`).
    #[must_use]
    pub fn with_handler(h: Option<&dyn WarnHandler>) -> Self {
        match h {
            None => Self::new(0),
            Some(h) => StaticWarnHandler {
                warnings: Mutex::new(h.copy_warnings()),
            },
        }
    }

    /// Resets the warnings of this handler.
    pub fn reset(&self) {
        self.warnings.lock().unwrap().clear();
    }

    fn append_with_level(&self, level: &str, err: WarnErr) {
        let mut warnings = self.warnings.lock().unwrap();
        if warnings.len() < usize::from(u16::MAX) {
            warnings.push(SqlWarn {
                level: level.to_string(),
                err,
            });
        }
    }
}

impl WarnAppender for StaticWarnHandler {
    fn append_warning(&self, err: WarnErr) {
        self.append_with_level(WARN_LEVEL_WARNING, err);
    }

    fn append_note(&self, err: WarnErr) {
        self.append_with_level(WARN_LEVEL_NOTE, err);
    }
}

impl WarnHandler for StaticWarnHandler {
    fn warning_count(&self) -> usize {
        self.warnings.lock().unwrap().len()
    }

    fn truncate_warnings(&self, start: usize) -> Vec<SqlWarn> {
        let mut warnings = self.warnings.lock().unwrap();
        if start >= warnings.len() {
            return Vec::new();
        }
        warnings.split_off(start)
    }

    fn copy_warnings(&self) -> Vec<SqlWarn> {
        self.warnings.lock().unwrap().clone()
    }
}

impl WarnHandlerExt for StaticWarnHandler {
    fn append_warnings(&self, warns: Vec<SqlWarn>) {
        let mut warnings = self.warnings.lock().unwrap();
        if warnings.len() < usize::from(u16::MAX) {
            warnings.extend(warns);
        }
    }

    fn append_error(&self, err: WarnErr) {
        self.append_with_level(WARN_LEVEL_ERROR, err);
    }

    fn get_warnings(&self) -> Vec<SqlWarn> {
        self.warnings.lock().unwrap().clone()
    }

    fn set_warnings(&self, warns: Vec<SqlWarn>) {
        *self.warnings.lock().unwrap() = warns;
    }

    fn num_error_warnings(&self) -> (u16, usize) {
        let warnings = self.warnings.lock().unwrap();
        let num_error = warnings
            .iter()
            .filter(|w| w.level == WARN_LEVEL_ERROR)
            .count() as u16;
        (num_error, warnings.len())
    }
}

/// A [`WarnHandler`] which does nothing (Go `IgnoreWarn`).
pub struct IgnoreWarn;

impl WarnAppender for IgnoreWarn {
    fn append_warning(&self, _err: WarnErr) {}
    fn append_note(&self, _err: WarnErr) {}
}

impl WarnHandler for IgnoreWarn {
    fn warning_count(&self) -> usize {
        0
    }
    fn truncate_warnings(&self, _start: usize) -> Vec<SqlWarn> {
        Vec::new()
    }
    fn copy_warnings(&self) -> Vec<SqlWarn> {
        Vec::new()
    }
}

/// A function-backed appender (Go `NewFuncWarnAppenderForTest`); the source
/// flags it as a test convenience, not a production path.
pub struct FuncWarnAppender<F: Fn(&str, WarnErr)> {
    f: F,
}

impl<F: Fn(&str, WarnErr)> FuncWarnAppender<F> {
    /// Creates the appender around `f(level, err)`.
    pub fn new(f: F) -> Self {
        FuncWarnAppender { f }
    }
}

impl<F: Fn(&str, WarnErr)> WarnAppender for FuncWarnAppender<F> {
    fn append_warning(&self, err: WarnErr) {
        (self.f)(WARN_LEVEL_WARNING, err);
    }

    fn append_note(&self, err: WarnErr) {
        (self.f)(WARN_LEVEL_NOTE, err);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_error::terror::ERR_RESULT_UNDETERMINED;

    fn warn(level: &str, err: WarnErr) -> SqlWarn {
        SqlWarn {
            level: level.to_string(),
            err,
        }
    }

    // Go `TestSQLWarn`: JSON round-trip preserves level and error text for
    // both bare messages and typed terrors. Go's `errors.Trace` layers have no
    // Rust counterpart (Cause-unwrapping is structural there, absent here).
    #[test]
    fn sql_warn_json_round_trip() {
        let terror = ERR_RESULT_UNDETERMINED
            .generate_with_stack(format!("{} unknown", ERR_RESULT_UNDETERMINED.message()));
        let warns = vec![
            warn(WARN_LEVEL_ERROR, WarnErr::from("any error")),
            warn(WARN_LEVEL_WARNING, WarnErr::Terror(terror)),
            warn(WARN_LEVEL_NOTE, WarnErr::from("EOF")),
        ];

        let data = serde_json::to_string(&warns).unwrap();
        let round: Vec<SqlWarn> = serde_json::from_str(&data).unwrap();

        assert_eq!(round.len(), warns.len());
        for (i, (before, after)) in warns.iter().zip(&round).enumerate() {
            assert_eq!(before.level, after.level, "{i}");
            assert_eq!(before.err.to_string(), after.err.to_string(), "{i}");
        }
    }

    // Go `TestIgnoreWarn`.
    #[test]
    fn ignore_warn() {
        assert_eq!(IgnoreWarn.warning_count(), 0);
        IgnoreWarn.append_warning(WarnErr::from("warn0"));
        assert_eq!(IgnoreWarn.warning_count(), 0);
        assert!(IgnoreWarn.copy_warnings().is_empty());
        IgnoreWarn.append_warning(WarnErr::from("warn1"));
        assert!(IgnoreWarn.truncate_warnings(0).is_empty());
        assert_eq!(IgnoreWarn.warning_count(), 0);
    }

    // Go `TestStaticWarnHandler`. The Go test additionally asserts that every
    // returned slice has a DIFFERENT backing array from the internal one
    // (`warnSliceInnerArrayEqual`); Rust ownership makes aliasing
    // unrepresentable, so those assertions hold by construction.
    #[test]
    fn static_warn_handler() {
        let h = StaticWarnHandler::new(0);
        assert_eq!(h.warning_count(), 0);
        for i in 0..4 {
            h.append_warning(WarnErr::Message(format!("warn{i}")));
        }
        assert_eq!(h.warning_count(), 4);

        let expected: Vec<String> = (0..4).map(|i| format!("warn{i}")).collect();
        let texts =
            |ws: &[SqlWarn]| -> Vec<String> { ws.iter().map(|w| w.err.to_string()).collect() };

        let got = h.copy_warnings();
        assert_eq!(texts(&got), expected);
        assert!(got.iter().all(|w| w.level == WARN_LEVEL_WARNING));
        assert_eq!(h.warning_count(), 4);

        // Truncate with start out of range.
        assert!(h.truncate_warnings(4).is_empty());
        assert!(h.truncate_warnings(5).is_empty());
        assert_eq!(h.warning_count(), 4);

        // Truncate in range returns the tail and keeps the head.
        let got = h.truncate_warnings(2);
        assert_eq!(texts(&got), expected[2..]);
        assert_eq!(texts(&h.copy_warnings()), expected[..2]);
        assert_eq!(h.warning_count(), 2);

        // Truncate at 0 drains everything.
        let got = h.truncate_warnings(0);
        assert_eq!(texts(&got), expected[..2]);
        assert_eq!(h.warning_count(), 0);
        assert!(h.copy_warnings().is_empty());
    }

    // Go `TestCopyWarnHandler`.
    #[test]
    fn copy_warn_handler() {
        let h1 = StaticWarnHandler::new(0);
        for i in 0..3 {
            h1.append_warning(WarnErr::Message(format!("warn{i}")));
        }

        let h2 = StaticWarnHandler::with_handler(Some(&h1));
        assert_eq!(h2.warning_count(), 3);
        let ws = h2.get_warnings();
        for (i, w) in ws.iter().enumerate() {
            assert_eq!(w.level, WARN_LEVEL_WARNING);
            assert_eq!(w.err.to_string(), format!("warn{i}"));
        }

        let h2 = StaticWarnHandler::with_handler(None);
        assert_eq!(h2.warning_count(), 0);
    }

    // The u16::MAX cap and the extension APIs (AppendWarnings/AppendError/
    // SetWarnings/NumErrorWarnings) from the source, uncovered by Go's tests.
    #[test]
    fn handler_ext_and_cap() {
        let h = StaticWarnHandler::new(0);
        h.append_error(WarnErr::from("e0"));
        h.append_note(WarnErr::from("n0"));
        h.append_warnings(vec![
            warn(WARN_LEVEL_WARNING, WarnErr::from("w0")),
            warn(WARN_LEVEL_ERROR, WarnErr::from("e1")),
        ]);
        assert_eq!(h.num_error_warnings(), (2, 4));

        h.set_warnings(vec![warn(WARN_LEVEL_NOTE, WarnErr::from("only"))]);
        assert_eq!(h.num_error_warnings(), (0, 1));
        h.reset();
        assert_eq!(h.warning_count(), 0);
    }
}
