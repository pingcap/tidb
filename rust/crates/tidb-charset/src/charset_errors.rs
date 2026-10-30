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

//! Error prototypes defined by the Go package(s) named per entry.
//!
//! GENERATED mechanically from the Go sources by scanning every
//! package-level `Class*.NewStd` / `NewStdErr` definition; the (class,
//! code) set of every module is cross-checked against the real Go
//! registry (`terror.ErrClassToMySQLCodes`) by
//! `tests/catalog_source.rs`. Do not hand-edit entries.

use std::sync::LazyLock;

use tidb_terror::catalog_support::{std_err, Class};
use tidb_terror::TerrorError;
use tidb_mysql::errors::errcode;

/// Source `parser/charset.ErrInvalidCharacterString` (`pkg/parser/charset/encoding_base.go:29`).
pub static ERR_INVALID_CHARACTER_STRING: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Parser, errcode::ErrInvalidCharacterString));
/// Source `parser/charset.ErrUnknownCollation` (`pkg/parser/charset/charset.go:29`).
pub static ERR_UNKNOWN_COLLATION: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Ddl, errcode::ErrUnknownCollation));
/// Source `parser/charset.ErrCollationCharsetMismatch` (`pkg/parser/charset/charset.go:31`).
pub static ERR_COLLATION_CHARSET_MISMATCH: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Ddl, errcode::ErrCollationCharsetMismatch));

#[cfg(test)]
mod go_class_code_tests {
    use tidb_mysql::errors::errcode;
    use tidb_terror::TerrorClass;

    /// Each prototype carries the class and code of the Go definition it
    /// transcribes (`Class.NewStd(code)`), so `Error()` prints Go's
    /// `[class:code]`.
    #[test]
    fn prototypes_carry_their_go_class_and_code() {
        assert_eq!(
            super::ERR_INVALID_CHARACTER_STRING.class(),
            TerrorClass::Parser,
            "charset::ERR_INVALID_CHARACTER_STRING"
        );
        assert_eq!(
            super::ERR_INVALID_CHARACTER_STRING
                .code()
                .value(),
            errcode::ErrInvalidCharacterString as isize,
            "charset::ERR_INVALID_CHARACTER_STRING"
        );
        assert_eq!(
            super::ERR_UNKNOWN_COLLATION.class(),
            TerrorClass::Ddl,
            "charset::ERR_UNKNOWN_COLLATION"
        );
        assert_eq!(
            super::ERR_UNKNOWN_COLLATION.code().value(),
            errcode::ErrUnknownCollation as isize,
            "charset::ERR_UNKNOWN_COLLATION"
        );
        assert_eq!(
            super::ERR_COLLATION_CHARSET_MISMATCH.class(),
            TerrorClass::Ddl,
            "charset::ERR_COLLATION_CHARSET_MISMATCH"
        );
        assert_eq!(
            super::ERR_COLLATION_CHARSET_MISMATCH
                .code()
                .value(),
            errcode::ErrCollationCharsetMismatch as isize,
            "charset::ERR_COLLATION_CHARSET_MISMATCH"
        );
    }
}
