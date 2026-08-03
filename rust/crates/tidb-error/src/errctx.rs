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

//! Complete transcreation of `pkg/errctx/context.go`.
//!
//! The package decides, per statement, how each group of well-known errors is
//! published: returned to the caller, appended as a warning, or ignored. It
//! owns the error-code-to-group table (`errGroupMap`), the copy-on-write
//! [`Context`] with its warning-handler wiring, and the `ignore`/`warn` flag
//! resolution rule.
//!
//! Two dependency seams from Go are mirrored locally because only their
//! interface surface is used here:
//! - `pkg/util/context.WarnAppender` becomes the [`WarnAppender`] trait (and
//!   `contextutil.IgnoreWarn` becomes [`IgnoreWarn`]);
//! - `"pingcap/errors".ErrorGroup` (satisfied by `go.uber.org/multierr`)
//!   becomes the concrete [`MultiError`], which [`Context::handle_error`]
//!   detects by downcast exactly where Go type-asserts the interface.
//!
//! Go's `intest.Assert(handler != nil)` nil checks disappear: the handler is
//! an `Arc<dyn WarnAppender>`, which cannot be null.

use std::error::Error;
use std::fmt;
use std::ops::{Index, IndexMut};
use std::sync::{Arc, LazyLock};

use crate::terror::{root_cause, TerrorError};
use crate::tidb::errcode;

/// A shared error value, standing in for Go's `error` interface value in the
/// positions where `errctx` stores or returns one error in several places.
pub type SharedError = Arc<dyn Error + Send + Sync + 'static>;

/// Source `Level`: the behavior for each error.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
#[repr(u8)]
pub enum Level {
    /// `LevelError`: the error will be returned.
    #[default]
    Error = 0,
    /// `LevelWarn`: the error is regarded as a warning.
    Warn = 1,
    /// `LevelIgnore`: the error is ignored.
    Ignore = 2,
}

/// Source `ErrGroup`: groups errors according to how they are handled.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[repr(usize)]
pub enum ErrGroup {
    /// `ErrGroupTruncate`: truncated / out-of-range / malformed-value errors.
    Truncate = 0,
    /// `ErrGroupDupKey`: duplicate key errors.
    DupKey = 1,
    /// `ErrGroupBadNull`: bad null errors.
    BadNull = 2,
    /// `ErrGroupNoDefault`: no default value errors.
    NoDefault = 3,
    /// `ErrGroupDividedByZero`: divided by zero errors.
    DividedByZero = 4,
    /// `ErrGroupAutoIncReadFailed`: auto increment read failed errors.
    AutoIncReadFailed = 5,
    /// `ErrGroupNoMatchedPartition`: no partition is matched errors.
    NoMatchedPartition = 6,
}

impl ErrGroup {
    /// Source `errGroupCount`.
    pub const COUNT: usize = 7;

    /// Every group in source declaration order.
    pub const ALL: [Self; Self::COUNT] = [
        Self::Truncate,
        Self::DupKey,
        Self::BadNull,
        Self::NoDefault,
        Self::DividedByZero,
        Self::AutoIncReadFailed,
        Self::NoMatchedPartition,
    ];

    const fn index(self) -> usize {
        self as usize
    }
}

/// Source `errGroupMap`, keyed by error code. Go builds a hash map in
/// `init()`; a match over the same registered codes removes the mutable
/// global while keeping the identical membership.
#[must_use]
pub fn err_group_for_code(code: isize) -> Option<ErrGroup> {
    let Ok(code) = u16::try_from(code) else {
        return None;
    };
    #[allow(non_upper_case_globals)]
    Some(match code {
        errcode::ErrTruncatedWrongValue
        | errcode::ErrDataTooLong
        | errcode::ErrTruncatedWrongValueForField
        | errcode::ErrWarnDataOutOfRange
        | errcode::ErrDataOutOfRange
        | errcode::ErrBadNumber
        | errcode::ErrWrongValueForType
        | errcode::ErrDatetimeFunctionOverflow
        | errcode::WarnDataTruncated
        | errcode::ErrIncorrectDatetimeValue => ErrGroup::Truncate,
        errcode::ErrBadNull | errcode::ErrWarnNullToNotnull => ErrGroup::BadNull,
        errcode::ErrNoDefaultForField => ErrGroup::NoDefault,
        errcode::ErrDivisionByZero => ErrGroup::DividedByZero,
        errcode::ErrAutoincReadFailed => ErrGroup::AutoIncReadFailed,
        errcode::ErrNoPartitionForGivenValue | errcode::ErrRowDoesNotMatchGivenPartitionSet => {
            ErrGroup::NoMatchedPartition
        }
        errcode::ErrDupEntry => ErrGroup::DupKey,
        _ => return None,
    })
}

/// Source `LevelMap`: the fixed map from [`ErrGroup`] to [`Level`]. Go's
/// zero value (all `LevelError`) is [`LevelMap::strict`] / `Default`.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct LevelMap([Level; ErrGroup::COUNT]);

impl LevelMap {
    /// Go's zero-valued `LevelMap`: every group returns its error.
    pub const fn strict() -> Self {
        Self([Level::Error; ErrGroup::COUNT])
    }

    /// Returns a group's level.
    pub const fn get(self, group: ErrGroup) -> Level {
        self.0[group.index()]
    }

    /// Returns a copy with one group's level replaced.
    #[must_use]
    pub const fn with_level(self, group: ErrGroup, level: Level) -> Self {
        let mut levels = self.0;
        levels[group.index()] = level;
        Self(levels)
    }

    /// Returns all levels in source declaration order.
    pub const fn as_array(self) -> [Level; ErrGroup::COUNT] {
        self.0
    }
}

impl Default for LevelMap {
    fn default() -> Self {
        Self::strict()
    }
}

impl Index<ErrGroup> for LevelMap {
    type Output = Level;

    fn index(&self, group: ErrGroup) -> &Self::Output {
        &self.0[group.index()]
    }
}

impl IndexMut<ErrGroup> for LevelMap {
    fn index_mut(&mut self, group: ErrGroup) -> &mut Self::Output {
        &mut self.0[group.index()]
    }
}

/// Source `contextutil.WarnAppender`: the capability to add a warning.
///
/// Mirrored from `pkg/util/context/warn.go` because `errctx` needs exactly
/// this interface surface; warning storage, ordering, and caps stay with the
/// session/statement owner that implements it.
pub trait WarnAppender: Send + Sync {
    /// Appends a warning (level `Warning`).
    fn append_warning(&self, err: SharedError);
    /// Appends a warning with level `Note`.
    fn append_note(&self, err: SharedError);
}

/// Source `contextutil.IgnoreWarn`: discards every warning.
#[derive(Clone, Copy, Debug, Default)]
pub struct IgnoreWarn;

impl WarnAppender for IgnoreWarn {
    fn append_warning(&self, _err: SharedError) {}
    fn append_note(&self, _err: SharedError) {}
}

/// Concrete stand-in for `"pingcap/errors".ErrorGroup` (the interface
/// `go.uber.org/multierr` satisfies): several errors traveling as one value.
/// [`Context::handle_error`] detects it by downcast, as Go type-asserts.
#[derive(Clone, Debug, Default)]
pub struct MultiError(Vec<SharedError>);

impl MultiError {
    /// Combines errors into one group value, mirroring `multierr.Append`.
    #[must_use]
    pub fn new(errors: Vec<SharedError>) -> Self {
        Self(errors)
    }

    /// Source `Errors()`: the contained errors in order.
    #[must_use]
    pub fn errors(&self) -> &[SharedError] {
        &self.0
    }
}

impl fmt::Display for MultiError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        // multierr joins the messages with "; ".
        let mut first = true;
        for error in &self.0 {
            if !first {
                formatter.write_str("; ")?;
            }
            first = false;
            write!(formatter, "{error}")?;
        }
        Ok(())
    }
}

impl Error for MultiError {}

/// Source `Context`: defines how to handle an error.
#[derive(Clone)]
pub struct Context {
    level_map: LevelMap,
    warn_handler: Arc<dyn WarnAppender>,
}

impl fmt::Debug for Context {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Context")
            .field("level_map", &self.level_map)
            .finish_non_exhaustive()
    }
}

impl Context {
    /// Source `LevelMap()`: the context's group-to-level map.
    #[must_use]
    pub const fn level_map(&self) -> LevelMap {
        self.level_map
    }

    /// Source `LevelForGroup`: the level for a specified group.
    #[must_use]
    pub const fn level_for_group(&self, err_group: ErrGroup) -> Level {
        self.level_map.get(err_group)
    }

    /// Source `WithStrictErrGroupLevel`: a copy that returns the error
    /// directly for every kind of error.
    #[must_use]
    pub fn with_strict_err_group_level(&self) -> Self {
        Self {
            level_map: LevelMap::strict(),
            warn_handler: Arc::clone(&self.warn_handler),
        }
    }

    /// Source `WithErrGroupLevel`: a copy with one group's level replaced.
    #[must_use]
    pub fn with_err_group_level(&self, err_group: ErrGroup, level: Level) -> Self {
        Self {
            level_map: self.level_map.with_level(err_group, level),
            warn_handler: Arc::clone(&self.warn_handler),
        }
    }

    /// Source `WithErrGroupLevels`: a copy with the whole map replaced.
    #[must_use]
    pub fn with_err_group_levels(&self, levels: LevelMap) -> Self {
        Self {
            level_map: levels,
            warn_handler: Arc::clone(&self.warn_handler),
        }
    }

    /// Source `AppendWarning`: appends the error as a warning.
    pub fn append_warning(&self, err: SharedError) {
        self.warn_handler.append_warning(err);
    }

    /// Source `AppendNote`: appends the error as a warning with level `Note`.
    pub fn append_note(&self, err: SharedError) {
        self.warn_handler.append_note(err);
    }

    /// Source `HandleError`. See [`Self::handle_error_with_alias`] for the
    /// per-error logic. A [`MultiError`] group is handled error by error, and
    /// the first error found is returned.
    #[must_use]
    pub fn handle_error(&self, err: Option<SharedError>) -> Option<SharedError> {
        let err = err?;
        // The function of handling an error group is placed in `handle_error`
        // but not in `handle_error_with_alias`, because it's hard to give a
        // proper error and warn alias for an error group.
        if let Some(errs) = err.downcast_ref::<MultiError>() {
            for single_err in errs.errors() {
                let single_err = self.handle_error_with_alias(
                    Some(single_err.clone()),
                    single_err.clone(),
                    single_err.clone(),
                );
                // If the one error is found, just return it, matching TiDB's
                // original behavior before `errctx` handled multiple errors.
                if single_err.is_some() {
                    return single_err;
                }
            }
            return None;
        }
        self.handle_error_with_alias(Some(err.clone()), err.clone(), err)
    }

    /// Source `HandleErrorWithAlias`:
    /// 1. If `internal_err` is not a terror, or its code is not in the group
    ///    table, or the group level is [`Level::Error`], `err` is returned.
    /// 2. If the level is [`Level::Warn`], `warn_err` is appended as a
    ///    warning and `None` is returned.
    /// 3. If the level is [`Level::Ignore`], `None` is returned.
    #[must_use]
    pub fn handle_error_with_alias(
        &self,
        internal_err: Option<SharedError>,
        err: SharedError,
        warn_err: SharedError,
    ) -> Option<SharedError> {
        let internal_err = internal_err?;
        // `errors.Cause`: follow wrappers to the root error.
        let cause = root_cause(internal_err.as_ref());
        let Some(terror) = cause.downcast_ref::<TerrorError>() else {
            return Some(err);
        };
        let Some(err_group) = err_group_for_code(terror.code().value()) else {
            return Some(err);
        };
        match self.level_map.get(err_group) {
            Level::Error => return Some(err),
            Level::Warn => self.append_warning(warn_err),
            Level::Ignore => {}
        }
        None
    }
}

/// Source `NewContext`: creates an error context to handle the errors and
/// warnings, starting from the strict zero-valued level map.
#[must_use]
pub fn new_context(handler: Arc<dyn WarnAppender>) -> Context {
    new_context_with_levels(LevelMap::strict(), handler)
}

/// Source `NewContextWithLevels`.
#[must_use]
pub fn new_context_with_levels(levels: LevelMap, handler: Arc<dyn WarnAppender>) -> Context {
    Context {
        level_map: levels,
        warn_handler: handler,
    }
}

/// Source `StrictNoWarningContext`: returns all errors directly and ignores
/// all warnings.
pub static STRICT_NO_WARNING_CONTEXT: LazyLock<Context> =
    LazyLock::new(|| new_context(Arc::new(IgnoreWarn)));

/// Source `ResolveErrLevel`: resolves the error level according to the
/// `ignore` and `warn` flags. `ignore` always wins if both are set.
#[must_use]
pub const fn resolve_err_level(ignore: bool, warn: bool) -> Level {
    if ignore {
        Level::Ignore
    } else if warn {
        Level::Warn
    } else {
        Level::Error
    }
}
