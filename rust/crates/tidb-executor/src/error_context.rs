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

//! Statement-flag derivation of the error-context policy from
//! `pkg/sessionctx/stmtctx/stmtctx.go`.
//!
//! The `pkg/errctx` package itself -- [`Level`], [`ErrGroup`], [`LevelMap`],
//! the error-code group table, the warning-handling `Context`, and
//! `ResolveErrLevel` -- is fully transcreated in [`tidb_error::errctx`] and
//! re-exported here; this module no longer carries its own copy. What this
//! module owns is the stmtctx side: the typed statement flags
//! ([`ErrorContextFlags`]) and the `newErrCtx`-shaped derivation of a group
//! [`LevelMap`] from them ([`ErrorContext`]), which is what the executor
//! push-down path consumes.

pub use tidb_error::errctx::{resolve_err_level, ErrGroup, Level, LevelMap};

use tidb_datatype::ConversionFlags;

/// Statement flags that directly affect the Go error-context policy.
///
/// The first two flags have an explicit precedence in Go: ignore truncation
/// wins over truncate-as-warning. `ignore_zero_in_date` is retained as a
/// typed conversion flag but does not select a separate `ErrGroup` in the
/// source `errctx` map. `divided_by_zero_as_warning` is the source
/// `FlagDividedByZeroAsWarning` push-down result, which is derived from the
/// divided-by-zero group level rather than from a type flag.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ErrorContextFlags {
    conversion: ConversionFlags,
    divided_by_zero_as_warning: bool,
}

impl ErrorContextFlags {
    /// Creates flags with all policy bits disabled.
    pub const fn new() -> Self {
        Self {
            conversion: ConversionFlags::from_bits(0),
            divided_by_zero_as_warning: false,
        }
    }

    /// Sets the source `FlagIgnoreTruncateErr` bit.
    #[must_use]
    pub const fn with_ignore_truncate(self, value: bool) -> Self {
        Self {
            conversion: self.conversion.with_ignore_truncate_err(value),
            ..self
        }
    }

    /// Sets the source `FlagTruncateAsWarning` bit.
    #[must_use]
    pub const fn with_truncate_as_warning(self, value: bool) -> Self {
        Self {
            conversion: self.conversion.with_truncate_as_warning(value),
            ..self
        }
    }

    /// Sets the source `FlagIgnoreZeroInDateErr` bit.
    #[must_use]
    pub const fn with_ignore_zero_in_date(self, value: bool) -> Self {
        Self {
            conversion: self.conversion.with_ignore_zero_in_date_err(value),
            ..self
        }
    }

    /// Sets the source divided-by-zero warning push-down bit.
    #[must_use]
    pub const fn with_divided_by_zero_as_warning(self, value: bool) -> Self {
        Self {
            divided_by_zero_as_warning: value,
            ..self
        }
    }

    /// Returns whether truncation errors are ignored.
    pub const fn ignore_truncate(self) -> bool {
        self.conversion.ignore_truncate_err()
    }

    /// Returns whether truncation errors become warnings.
    pub const fn truncate_as_warning(self) -> bool {
        self.conversion.truncate_as_warning()
    }

    /// Returns whether zero-in-date conversion errors are ignored.
    pub const fn ignore_zero_in_date(self) -> bool {
        self.conversion.ignore_zero_in_date_err()
    }

    /// Returns whether division-by-zero is pushed down as a warning.
    pub const fn divided_by_zero_as_warning(self) -> bool {
        self.divided_by_zero_as_warning
    }

    /// Returns the authoritative datatype conversion flags.
    pub const fn conversion_flags(self) -> ConversionFlags {
        self.conversion
    }
}

/// Pure error handling policy for one statement.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ErrorContext {
    levels: LevelMap,
    flags: ErrorContextFlags,
}

impl ErrorContext {
    /// Creates the strict source default: every known group returns an error.
    pub const fn new() -> Self {
        Self {
            levels: LevelMap::strict(),
            flags: ErrorContextFlags::new(),
        }
    }

    /// Creates a context from already-normalized group levels and flags.
    pub const fn with_levels(levels: LevelMap, flags: ErrorContextFlags) -> Self {
        Self { levels, flags }
    }

    /// Derives the group policy from source statement flags.
    ///
    /// This mirrors `newErrCtx`: ignore-truncate wins over
    /// truncate-as-warning, while the divided-by-zero warning bit changes
    /// only that group. All other groups remain strict unless an owner adds a
    /// level explicitly.
    pub const fn from_flags(flags: ErrorContextFlags) -> Self {
        let truncate_level = resolve_err_level(
            flags.conversion.ignore_truncate_err(),
            flags.conversion.truncate_as_warning(),
        );
        let divided_level = resolve_err_level(false, flags.divided_by_zero_as_warning);
        let levels = LevelMap::strict()
            .with_level(ErrGroup::Truncate, truncate_level)
            .with_level(ErrGroup::DividedByZero, divided_level);
        Self { levels, flags }
    }

    /// Creates the source `StatementContext` default before per-statement
    /// push-down flags are applied. Go's `DefaultStmtErrLevels` starts
    /// divided-by-zero at warning level (`stmtctx.go:561-563`), while the
    /// zero-valued `LevelMap` itself is strict. Keeping this constructor
    /// separate from [`Self::from_flags`] preserves that two-stage boundary.
    pub const fn statement_defaults() -> Self {
        let flags = ErrorContextFlags::new().with_divided_by_zero_as_warning(true);
        Self::from_flags(flags)
    }

    /// Returns the fixed group-level map.
    pub const fn levels(self) -> LevelMap {
        self.levels
    }

    /// Returns one group's handling level.
    pub const fn level_for(self, group: ErrGroup) -> Level {
        self.levels.get(group)
    }

    /// Returns the retained source conversion/push-down flags.
    pub const fn flags(self) -> ErrorContextFlags {
        self.flags
    }

    /// Returns a copy with one group changed, leaving the original untouched.
    #[must_use]
    pub const fn with_group_level(self, group: ErrGroup, level: Level) -> Self {
        Self {
            levels: self.levels.with_level(group, level),
            flags: self.flags,
        }
    }

    /// Returns a copy with an entire group map replaced.
    #[must_use]
    pub const fn with_group_levels(self, levels: LevelMap) -> Self {
        Self {
            levels,
            flags: self.flags,
        }
    }

    /// Returns a strict copy while retaining the source flags.
    #[must_use]
    pub const fn with_strict_group_levels(self) -> Self {
        Self {
            levels: LevelMap::strict(),
            flags: self.flags,
        }
    }

    /// Converts one group's level into the action an error producer should
    /// take. No warning is appended here; the statement owner owns that sink.
    pub const fn disposition(self, group: ErrGroup) -> ErrorDisposition {
        ErrorDisposition::from_level(self.level_for(group))
    }
}

/// The action selected by [`ErrorContext::disposition`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorDisposition {
    /// Return the error directly.
    Return,
    /// Append a warning and continue.
    Warn,
    /// Ignore the error and continue.
    Ignore,
}

impl ErrorDisposition {
    /// Maps a group [`Level`] to the action an error producer should take.
    #[must_use]
    pub const fn from_level(level: Level) -> Self {
        match level {
            Level::Error => Self::Return,
            Level::Warn => Self::Warn,
            Level::Ignore => Self::Ignore,
        }
    }
}
