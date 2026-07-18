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

//! Typed session-setting values and their source defaults.

/// TiDB's three-valued `tidb_enable_noop_functions` session switch.
///
/// It controls compatibility-only features such as `tx_read_only`: `OFF`
/// rejects attempts to turn those features on, `ON` accepts them, and `WARN`
/// accepts them while the live Session appends the source diagnostic directly
/// to its canonical statement status. Retaining the state rather than merely
/// a boolean keeps the Go validation boundary explicit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum NoopFunctionsMode {
    #[default]
    Off,
    On,
    Warn,
}

impl NoopFunctionsMode {
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::Off => "OFF",
            Self::On => "ON",
            Self::Warn => "WARN",
        }
    }
}

/// TiDB's session-scoped tidb_multi_statement_mode setting.
///
/// This is deliberately distinct from NoopFunctionsMode even though both
/// source enums spell their three values OFF/ON/WARN: this one controls
/// client-protocol multi-statement handling in real TiDB, a boundary the seed
/// executor does not expose. Retaining its typed session state keeps source
/// validation and SQL readback exact without pretending this executor accepts
/// a semicolon-delimited client request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum MultiStatementMode {
    #[default]
    Off,
    On,
    Warn,
}

impl MultiStatementMode {
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::Off => "OFF",
            Self::On => "ON",
            Self::Warn => "WARN",
        }
    }
}

/// TiDB's bounded `div_precision_increment` session value.
///
/// Go registers this as an unsigned system variable with range `0..=30` and
/// default `4` (`pkg/sessionctx/variable/sysvar.go`). Keeping its nonzero
/// default in a newtype makes `Database::default()` correct too, instead of
/// letting Rust's primitive `u8` default silently become a wrong scale.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DivPrecisionIncrement(u8);

impl DivPrecisionIncrement {
    pub(crate) const DEFAULT: Self = Self(4);

    pub(crate) const fn new(value: u8) -> Self {
        Self(value)
    }

    pub(crate) const fn value(self) -> u8 {
        self.0
    }
}

impl Default for DivPrecisionIncrement {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// TiDB's `sql_select_limit` session value. `u64::MAX` is both the source
/// default and the no-limit sentinel; a newtype keeps that invariant intact
/// under `Database::default()` rather than letting Rust's primitive default
/// silently turn every query into `LIMIT 0`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SqlSelectLimit(u64);

impl SqlSelectLimit {
    pub(crate) const UNLIMITED: Self = Self(u64::MAX);

    pub(crate) const fn new(value: u64) -> Self {
        Self(value)
    }

    pub(crate) const fn value(self) -> u64 {
        self.0
    }
}

impl Default for SqlSelectLimit {
    fn default() -> Self {
        Self::UNLIMITED
    }
}

/// The session's `time_zone` setting, kept as a sum type so the three
/// source-observable zero-offset forms cannot accidentally collapse. Go's
/// `timeutil.ParseTimeZone` canonicalizes `SYSTEM`, preserves the spelling
/// of the loaded `UTC` location, and formats fixed zones as offsets.
///
/// The seed deliberately has no IANA timezone database. `System` and `Utc`
/// therefore both contribute a deterministic zero offset to clock evaluation;
/// their distinct readback labels are nevertheless real session state. Named
/// IANA zones remain explicitly unsupported rather than pretending they are
/// fixed offsets.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) enum TimeZoneSetting {
    #[default]
    System,
    Utc(String),
    FixedOffset(i32),
}

impl TimeZoneSetting {
    pub(crate) const fn offset_seconds(&self) -> i32 {
        match self {
            Self::System | Self::Utc(_) => 0,
            Self::FixedOffset(seconds) => *seconds,
        }
    }

    pub(crate) fn readback(&self) -> String {
        match self {
            Self::System => "SYSTEM".to_string(),
            Self::Utc(label) => label.clone(),
            Self::FixedOffset(seconds) => crate::session_runtime::format_tz_offset(*seconds),
        }
    }
}

/// TiDB's session-scoped timestamp setting.
///
/// The source keeps the original normalized text for non-default values,
/// while its numeric interpretation drives every current-time function.
/// Dynamic corresponds exactly to the stored default string "0": its
/// readback is the current statement's cached wall clock rather than a
/// literal zero. Keeping both facts together prevents @@timestamp from
/// drifting from the clock used by NOW() and its siblings.
#[derive(Debug, Clone, PartialEq, Default)]
pub(crate) enum TimestampSetting {
    #[default]
    Dynamic,
    Fixed {
        epoch: f64,
        readback: String,
    },
}

/// TiDB's session-scoped foreign-key enforcement switch. Its default is ON;
/// using an enum instead of a bare bool preserves that non-Rust-default
/// invariant under `Database::default()`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum ForeignKeyChecks {
    #[default]
    Enabled,
    Disabled,
}

impl ForeignKeyChecks {
    pub(crate) const fn is_enabled(self) -> bool {
        matches!(self, Self::Enabled)
    }
}
