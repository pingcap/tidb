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

//! Typed truncation handling from `pkg/types/truncate.go`.

use tidb_error::mysql::SqlError;
use tidb_error::terror::TerrorError;
use tidb_error::tidb::errcode;

use crate::{ConversionContext, ConversionFlags, STRICT_FLAGS};

/// The two `types.Flags` decisions consumed by Go's `Context.HandleTruncate`.
///
/// This compatibility wrapper stores the shared [`ConversionFlags`] value;
/// it does not maintain a second pair of policy booleans.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TruncationPolicy {
    flags: ConversionFlags,
}

impl TruncationPolicy {
    /// The strict source behavior: return recognized truncation errors.
    pub const STRICT: Self = Self {
        flags: STRICT_FLAGS,
    };

    /// Creates the exact two-flag policy.
    pub const fn new(ignore_error: bool, error_as_warning: bool) -> Self {
        Self {
            flags: STRICT_FLAGS
                .with_ignore_truncate_err(ignore_error)
                .with_truncate_as_warning(error_as_warning),
        }
    }

    /// Applies `Context.HandleTruncate` to a typed SQL error.
    ///
    /// `None` is Go's nil error. Unrecognized error numbers are always
    /// returned. For recognized errors, ignore wins over warning exactly as in
    /// the source. The callback is the imported `WarnAppender` boundary; its
    /// storage, publication, and IgnoreWarn implementation remain owned by the
    /// session warning slice.
    pub fn handle(
        self,
        error: Option<SqlError>,
        mut append_warning: impl FnMut(SqlError),
    ) -> Option<SqlError> {
        let error = error?;
        if !is_truncation_error_code(error.code) {
            return Some(error);
        }
        match truncation_disposition(self.flags) {
            TruncationDisposition::Ignore => None,
            TruncationDisposition::Warn => {
                append_warning(error);
                None
            }
            TruncationDisposition::Return => Some(error),
        }
    }
}

impl ConversionContext<'_> {
    /// Source `Context.HandleTruncate`, preserving the generated terror
    /// identity when returning or publishing the error.
    pub fn handle_truncate(&self, error: Option<TerrorError>) -> Option<TerrorError> {
        let error = error?;
        let Ok(code) = u16::try_from(error.code().value()) else {
            return Some(error);
        };
        if !is_truncation_error_code(code) {
            return Some(error);
        }
        match truncation_disposition(self.flags()) {
            TruncationDisposition::Ignore => None,
            TruncationDisposition::Warn => {
                self.append_warning(error);
                None
            }
            TruncationDisposition::Return => Some(error),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TruncationDisposition {
    Return,
    Warn,
    Ignore,
}

const fn truncation_disposition(flags: ConversionFlags) -> TruncationDisposition {
    if flags.ignore_truncate_err() {
        TruncationDisposition::Ignore
    } else if flags.truncate_as_warning() {
        TruncationDisposition::Warn
    } else {
        TruncationDisposition::Return
    }
}

/// The exact error-number allowlist from `Context.HandleTruncate`.
pub const fn is_truncation_error_code(code: u16) -> bool {
    matches!(
        code,
        errcode::ErrTruncatedWrongValue
            | errcode::ErrDataTooLong
            | errcode::ErrTruncatedWrongValueForField
            | errcode::ErrWarnDataOutOfRange
            | errcode::ErrDataOutOfRange
            | errcode::ErrBadNumber
            | errcode::ErrWrongValueForType
            | errcode::ErrDatetimeFunctionOverflow
            | errcode::WarnDataTruncated
            | errcode::ErrIncorrectDatetimeValue
    )
}
