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

//! Variable-error code identities from `pkg/sessionctx/variable/error.go`.
//!
//! The Go source constructs typed `dbterror` values around these MySQL/TiDB
//! error numbers. This leaf ports the numeric code registry only; constructors,
//! message templates, SQLSTATE, formatting, and warning/error publication
//! remain session/protocol responsibilities.

use tidb_error::{mysql, tidb};

/// Numeric error code used by a variable/session error.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SysVarErrorCode(u16);

impl SysVarErrorCode {
    /// `ErrWarnDeprecatedSyntax` (MySQL 1287).
    pub const WARN_DEPRECATED_SYNTAX: Self = Self(mysql::errcode::ErrWarnDeprecatedSyntax);
    /// `ErrSnapshotTooOld` (TiDB 8055).
    pub const SNAPSHOT_TOO_OLD: Self = Self(tidb::errcode::ErrSnapshotTooOld);
    /// `ErrUnsupportedValueForVar` (TiDB 8047).
    pub const UNSUPPORTED_VALUE_FOR_VAR: Self = Self(tidb::errcode::ErrUnsupportedValueForVar);
    /// `ErrUnknownSystemVar` (MySQL 1193).
    pub const UNKNOWN_SYSTEM_VAR: Self = Self(mysql::errcode::ErrUnknownSystemVariable);
    /// `ErrIncorrectScope` (MySQL 1238).
    pub const INCORRECT_SCOPE: Self = Self(mysql::errcode::ErrIncorrectGlobalLocalVar);
    /// `ErrUnknownTimeZone` (MySQL 1298).
    pub const UNKNOWN_TIME_ZONE: Self = Self(mysql::errcode::ErrUnknownTimeZone);
    /// `ErrReadOnly` (MySQL 1621).
    pub const READ_ONLY: Self = Self(mysql::errcode::ErrVariableIsReadonly);
    /// `ErrWrongValueForVar` (MySQL 1231).
    pub const WRONG_VALUE_FOR_VAR: Self = Self(mysql::errcode::ErrWrongValueForVar);
    /// `ErrWrongTypeForVar` (MySQL 1232).
    pub const WRONG_TYPE_FOR_VAR: Self = Self(mysql::errcode::ErrWrongTypeForVar);
    /// `ErrTruncatedWrongValue` (MySQL 1292).
    pub const TRUNCATED_WRONG_VALUE: Self = Self(mysql::errcode::ErrTruncatedWrongValue);
    /// `ErrMaxPreparedStmtCountReached` (MySQL 1461).
    pub const MAX_PREPARED_STMT_COUNT_REACHED: Self =
        Self(mysql::errcode::ErrMaxPreparedStmtCountReached);
    /// `ErrUnsupportedIsolationLevel` (TiDB 8048).
    pub const UNSUPPORTED_ISOLATION_LEVEL: Self = Self(tidb::errcode::ErrUnsupportedIsolationLevel);
    /// `errGlobalVariable` (MySQL 1229).
    pub const GLOBAL_VARIABLE: Self = Self(mysql::errcode::ErrGlobalVariable);
    /// `errLocalVariable` (MySQL 1228).
    pub const LOCAL_VARIABLE: Self = Self(mysql::errcode::ErrLocalVariable);
    /// `ErrNotSupportedYet` used by variable-specific errors (MySQL 1235).
    pub const NOT_SUPPORTED_YET: Self = Self(mysql::errcode::ErrNotSupportedYet);
    /// `ErrNotValidPassword` (MySQL 1819).
    pub const NOT_VALID_PASSWORD: Self = Self(mysql::errcode::ErrNotValidPassword);
    /// `ErrVariableNoLongerSupported` (TiDB 8136).
    pub const VARIABLE_NO_LONGER_SUPPORTED: Self =
        Self(tidb::errcode::ErrVariableNoLongerSupported);
    /// `ErrInvalidDefaultUTF8MB4Collation` (MySQL 3721).
    pub const INVALID_DEFAULT_UTF8MB4_COLLATION: Self =
        Self(tidb::errcode::ErrInvalidDefaultUTF8MB4Collation);
    /// `ErrWarnDeprecatedSyntaxNoReplacement` (MySQL 1681).
    pub const WARN_DEPRECATED_SYNTAX_NO_REPLACEMENT: Self =
        Self(mysql::errcode::ErrWarnDeprecatedSyntaxNoReplacement);

    /// Returns the numeric source error code.
    #[must_use]
    pub const fn code(self) -> u16 {
        self.0
    }
}
