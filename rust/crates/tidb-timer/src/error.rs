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

//! Transcreation of Go `pkg/timer/api/error.go`.
//!
//! Go declares four `errors.New` sentinels and compares them with
//! `errors.ErrorEqual`. Rust has no process-wide error identity, so the
//! sentinels become variants of [`TimerError`] and identity comparison is
//! `==` on the variant (see [`TimerError::error_equal`]). Every other error
//! this package raises is a plain formatted message, exactly as Go's
//! `errors.New`/`errors.Errorf`/`errors.Wrapf` produce.

use std::fmt;

/// The error type of `pkg/timer/api`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TimerError {
    /// Go `ErrTimerNotExist`: the specified timer does not exist.
    TimerNotExist,
    /// Go `ErrTimerExists`: the specified timer already exists.
    TimerExists,
    /// Go `ErrVersionNotMatch`: the timer's version does not match.
    VersionNotMatch,
    /// Go `ErrEventIDNotMatch`: the timer's event id does not match.
    EventIDNotMatch,
    /// Any other `errors.New`/`errors.Errorf`/`errors.Wrapf` message.
    Message(String),
}

impl TimerError {
    /// Builds a [`TimerError::Message`] from anything printable.
    pub fn message(text: impl Into<String>) -> Self {
        Self::Message(text.into())
    }

    /// Go `errors.Wrapf(err, format, ...)`: prepends context to the cause with
    /// pingcap/errors' `"<message>: <cause>"` spelling.
    pub fn wrap(self, context: impl fmt::Display) -> Self {
        Self::Message(format!("{context}: {self}"))
    }

    /// Go `errors.ErrorEqual(sentinel, err)` restricted to this package's use:
    /// every call site compares against one of the four sentinels.
    pub fn error_equal(&self, other: &Self) -> bool {
        self == other
    }
}

impl fmt::Display for TimerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TimerNotExist => formatter.write_str("timer not exist"),
            Self::TimerExists => formatter.write_str("timer already exists"),
            Self::VersionNotMatch => formatter.write_str("timer version not match"),
            Self::EventIDNotMatch => formatter.write_str("timer event id not match"),
            Self::Message(text) => formatter.write_str(text),
        }
    }
}

impl std::error::Error for TimerError {}

/// The package's `Result` alias.
pub type Result<T> = std::result::Result<T, TimerError>;
