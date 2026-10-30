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

/// Source `parser.ErrWarnOptimizerHintUnsupportedHint` (`pkg/parser/hintparser.go:1066`).
pub static ERR_WARN_OPTIMIZER_HINT_UNSUPPORTED_HINT: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Parser, errcode::ErrWarnOptimizerHintUnsupportedHint));
/// Source `parser.ErrWarnOptimizerHintInvalidToken` (`pkg/parser/hintparser.go:1067`).
pub static ERR_WARN_OPTIMIZER_HINT_INVALID_TOKEN: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Parser, errcode::ErrWarnOptimizerHintInvalidToken));
/// Source `parser.ErrWarnMemoryQuotaOverflow` (`pkg/parser/hintparser.go:1068`).
pub static ERR_WARN_MEMORY_QUOTA_OVERFLOW: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Parser, errcode::ErrWarnMemoryQuotaOverflow));
/// Source `parser.ErrWarnOptimizerHintParseError` (`pkg/parser/hintparser.go:1069`).
pub static ERR_WARN_OPTIMIZER_HINT_PARSE_ERROR: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Parser, errcode::ErrWarnOptimizerHintParseError));
/// Source `parser.ErrWarnOptimizerHintInvalidInteger` (`pkg/parser/hintparser.go:1070`).
pub static ERR_WARN_OPTIMIZER_HINT_INVALID_INTEGER: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Parser, errcode::ErrWarnOptimizerHintInvalidInteger));
/// Source `parser.ErrWarnOptimizerHintWrongPos` (`pkg/parser/hintparser.go:1071`).
pub static ERR_WARN_OPTIMIZER_HINT_WRONG_POS: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Parser, errcode::ErrWarnOptimizerHintWrongPos));
/// Source `parser.ErrSyntax` (`pkg/parser/parser_api.go:29`).
pub static ERR_SYNTAX: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Parser, errcode::ErrSyntax));
/// Source `parser.ErrParse` (`pkg/parser/parser_api.go:31`).
pub static ERR_PARSE: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Parser, errcode::ErrParse));
/// Source `parser.ErrUnknownCharacterSet` (`pkg/parser/parser_api.go:33`).
pub static ERR_UNKNOWN_CHARACTER_SET: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Parser, errcode::ErrUnknownCharacterSet));
/// Source `parser.ErrInvalidYearColumnLength` (`pkg/parser/parser_api.go:35`).
pub static ERR_INVALID_YEAR_COLUMN_LENGTH: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Parser, errcode::ErrInvalidYearColumnLength));
/// Source `parser.ErrWrongArguments` (`pkg/parser/parser_api.go:37`).
pub static ERR_WRONG_ARGUMENTS: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Parser, errcode::ErrWrongArguments));
/// Source `parser.ErrWrongFieldTerminators` (`pkg/parser/parser_api.go:39`).
pub static ERR_WRONG_FIELD_TERMINATORS: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Parser, errcode::ErrWrongFieldTerminators));
/// Source `parser.ErrTooBigDisplayWidth` (`pkg/parser/parser_api.go:41`).
pub static ERR_TOO_BIG_DISPLAY_WIDTH: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Parser, errcode::ErrTooBigDisplaywidth));
/// Source `parser.ErrTooBigPrecision` (`pkg/parser/parser_api.go:43`).
pub static ERR_TOO_BIG_PRECISION: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Parser, errcode::ErrTooBigPrecision));
/// Source `parser.ErrUnknownAlterLock` (`pkg/parser/parser_api.go:45`).
pub static ERR_UNKNOWN_ALTER_LOCK: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Parser, errcode::ErrUnknownAlterLock));
/// Source `parser.ErrUnknownAlterAlgorithm` (`pkg/parser/parser_api.go:47`).
pub static ERR_UNKNOWN_ALTER_ALGORITHM: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Parser, errcode::ErrUnknownAlterAlgorithm));
/// Source `parser.ErrNullInValuesLessThan` (`pkg/parser/parser_api.go:49`).
pub static ERR_NULL_IN_VALUES_LESS_THAN: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Parser, errcode::ErrNullInValuesLessThan));
/// Source `parser.ErrWrongValue` (`pkg/parser/parser_api.go:51`).
pub static ERR_WRONG_VALUE: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Parser, errcode::ErrWrongValue));
/// Source `parser.ErrWarnDeprecatedSyntax` (`pkg/parser/parser_api.go:53`).
pub static ERR_WARN_DEPRECATED_SYNTAX: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Parser, errcode::ErrWarnDeprecatedSyntax));
/// Source `parser.ErrWarnDeprecatedSyntaxNoReplacement` (`pkg/parser/parser_api.go:55`).
pub static ERR_WARN_DEPRECATED_SYNTAX_NO_REPLACEMENT: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Parser, errcode::ErrWarnDeprecatedSyntaxNoReplacement));
/// Source `parser.ErrWrongUsage` (`pkg/parser/parser_api.go:57`).
pub static ERR_WRONG_USAGE: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Parser, errcode::ErrWrongUsage));
/// Source `parser.ErrWrongDBName` (`pkg/parser/parser_api.go:59`).
pub static ERR_WRONG_DB_NAME: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Parser, errcode::ErrWrongDBName));

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
            super::ERR_WARN_OPTIMIZER_HINT_UNSUPPORTED_HINT.class(),
            TerrorClass::Parser,
            "parser::ERR_WARN_OPTIMIZER_HINT_UNSUPPORTED_HINT"
        );
        assert_eq!(
            super::ERR_WARN_OPTIMIZER_HINT_UNSUPPORTED_HINT
                .code()
                .value(),
            errcode::ErrWarnOptimizerHintUnsupportedHint as isize,
            "parser::ERR_WARN_OPTIMIZER_HINT_UNSUPPORTED_HINT"
        );
        assert_eq!(
            super::ERR_WARN_OPTIMIZER_HINT_INVALID_TOKEN.class(),
            TerrorClass::Parser,
            "parser::ERR_WARN_OPTIMIZER_HINT_INVALID_TOKEN"
        );
        assert_eq!(
            super::ERR_WARN_OPTIMIZER_HINT_INVALID_TOKEN
                .code()
                .value(),
            errcode::ErrWarnOptimizerHintInvalidToken as isize,
            "parser::ERR_WARN_OPTIMIZER_HINT_INVALID_TOKEN"
        );
        assert_eq!(
            super::ERR_WARN_MEMORY_QUOTA_OVERFLOW.class(),
            TerrorClass::Parser,
            "parser::ERR_WARN_MEMORY_QUOTA_OVERFLOW"
        );
        assert_eq!(
            super::ERR_WARN_MEMORY_QUOTA_OVERFLOW
                .code()
                .value(),
            errcode::ErrWarnMemoryQuotaOverflow as isize,
            "parser::ERR_WARN_MEMORY_QUOTA_OVERFLOW"
        );
        assert_eq!(
            super::ERR_WARN_OPTIMIZER_HINT_PARSE_ERROR.class(),
            TerrorClass::Parser,
            "parser::ERR_WARN_OPTIMIZER_HINT_PARSE_ERROR"
        );
        assert_eq!(
            super::ERR_WARN_OPTIMIZER_HINT_PARSE_ERROR
                .code()
                .value(),
            errcode::ErrWarnOptimizerHintParseError as isize,
            "parser::ERR_WARN_OPTIMIZER_HINT_PARSE_ERROR"
        );
        assert_eq!(
            super::ERR_WARN_OPTIMIZER_HINT_INVALID_INTEGER.class(),
            TerrorClass::Parser,
            "parser::ERR_WARN_OPTIMIZER_HINT_INVALID_INTEGER"
        );
        assert_eq!(
            super::ERR_WARN_OPTIMIZER_HINT_INVALID_INTEGER
                .code()
                .value(),
            errcode::ErrWarnOptimizerHintInvalidInteger as isize,
            "parser::ERR_WARN_OPTIMIZER_HINT_INVALID_INTEGER"
        );
        assert_eq!(
            super::ERR_WARN_OPTIMIZER_HINT_WRONG_POS.class(),
            TerrorClass::Parser,
            "parser::ERR_WARN_OPTIMIZER_HINT_WRONG_POS"
        );
        assert_eq!(
            super::ERR_WARN_OPTIMIZER_HINT_WRONG_POS
                .code()
                .value(),
            errcode::ErrWarnOptimizerHintWrongPos as isize,
            "parser::ERR_WARN_OPTIMIZER_HINT_WRONG_POS"
        );
        assert_eq!(
            super::ERR_SYNTAX.class(),
            TerrorClass::Parser,
            "parser::ERR_SYNTAX"
        );
        assert_eq!(
            super::ERR_SYNTAX.code().value(),
            errcode::ErrSyntax as isize,
            "parser::ERR_SYNTAX"
        );
        assert_eq!(
            super::ERR_PARSE.class(),
            TerrorClass::Parser,
            "parser::ERR_PARSE"
        );
        assert_eq!(
            super::ERR_PARSE.code().value(),
            errcode::ErrParse as isize,
            "parser::ERR_PARSE"
        );
        assert_eq!(
            super::ERR_UNKNOWN_CHARACTER_SET.class(),
            TerrorClass::Parser,
            "parser::ERR_UNKNOWN_CHARACTER_SET"
        );
        assert_eq!(
            super::ERR_UNKNOWN_CHARACTER_SET.code().value(),
            errcode::ErrUnknownCharacterSet as isize,
            "parser::ERR_UNKNOWN_CHARACTER_SET"
        );
        assert_eq!(
            super::ERR_INVALID_YEAR_COLUMN_LENGTH.class(),
            TerrorClass::Parser,
            "parser::ERR_INVALID_YEAR_COLUMN_LENGTH"
        );
        assert_eq!(
            super::ERR_INVALID_YEAR_COLUMN_LENGTH
                .code()
                .value(),
            errcode::ErrInvalidYearColumnLength as isize,
            "parser::ERR_INVALID_YEAR_COLUMN_LENGTH"
        );
        assert_eq!(
            super::ERR_WRONG_ARGUMENTS.class(),
            TerrorClass::Parser,
            "parser::ERR_WRONG_ARGUMENTS"
        );
        assert_eq!(
            super::ERR_WRONG_ARGUMENTS.code().value(),
            errcode::ErrWrongArguments as isize,
            "parser::ERR_WRONG_ARGUMENTS"
        );
        assert_eq!(
            super::ERR_WRONG_FIELD_TERMINATORS.class(),
            TerrorClass::Parser,
            "parser::ERR_WRONG_FIELD_TERMINATORS"
        );
        assert_eq!(
            super::ERR_WRONG_FIELD_TERMINATORS.code().value(),
            errcode::ErrWrongFieldTerminators as isize,
            "parser::ERR_WRONG_FIELD_TERMINATORS"
        );
        assert_eq!(
            super::ERR_TOO_BIG_DISPLAY_WIDTH.class(),
            TerrorClass::Parser,
            "parser::ERR_TOO_BIG_DISPLAY_WIDTH"
        );
        assert_eq!(
            super::ERR_TOO_BIG_DISPLAY_WIDTH.code().value(),
            errcode::ErrTooBigDisplaywidth as isize,
            "parser::ERR_TOO_BIG_DISPLAY_WIDTH"
        );
        assert_eq!(
            super::ERR_TOO_BIG_PRECISION.class(),
            TerrorClass::Parser,
            "parser::ERR_TOO_BIG_PRECISION"
        );
        assert_eq!(
            super::ERR_TOO_BIG_PRECISION.code().value(),
            errcode::ErrTooBigPrecision as isize,
            "parser::ERR_TOO_BIG_PRECISION"
        );
        assert_eq!(
            super::ERR_UNKNOWN_ALTER_LOCK.class(),
            TerrorClass::Parser,
            "parser::ERR_UNKNOWN_ALTER_LOCK"
        );
        assert_eq!(
            super::ERR_UNKNOWN_ALTER_LOCK.code().value(),
            errcode::ErrUnknownAlterLock as isize,
            "parser::ERR_UNKNOWN_ALTER_LOCK"
        );
        assert_eq!(
            super::ERR_UNKNOWN_ALTER_ALGORITHM.class(),
            TerrorClass::Parser,
            "parser::ERR_UNKNOWN_ALTER_ALGORITHM"
        );
        assert_eq!(
            super::ERR_UNKNOWN_ALTER_ALGORITHM.code().value(),
            errcode::ErrUnknownAlterAlgorithm as isize,
            "parser::ERR_UNKNOWN_ALTER_ALGORITHM"
        );
        assert_eq!(
            super::ERR_NULL_IN_VALUES_LESS_THAN.class(),
            TerrorClass::Parser,
            "parser::ERR_NULL_IN_VALUES_LESS_THAN"
        );
        assert_eq!(
            super::ERR_NULL_IN_VALUES_LESS_THAN.code().value(),
            errcode::ErrNullInValuesLessThan as isize,
            "parser::ERR_NULL_IN_VALUES_LESS_THAN"
        );
        assert_eq!(
            super::ERR_WRONG_VALUE.class(),
            TerrorClass::Parser,
            "parser::ERR_WRONG_VALUE"
        );
        assert_eq!(
            super::ERR_WRONG_VALUE.code().value(),
            errcode::ErrWrongValue as isize,
            "parser::ERR_WRONG_VALUE"
        );
        assert_eq!(
            super::ERR_WARN_DEPRECATED_SYNTAX.class(),
            TerrorClass::Parser,
            "parser::ERR_WARN_DEPRECATED_SYNTAX"
        );
        assert_eq!(
            super::ERR_WARN_DEPRECATED_SYNTAX.code().value(),
            errcode::ErrWarnDeprecatedSyntax as isize,
            "parser::ERR_WARN_DEPRECATED_SYNTAX"
        );
        assert_eq!(
            super::ERR_WARN_DEPRECATED_SYNTAX_NO_REPLACEMENT.class(),
            TerrorClass::Parser,
            "parser::ERR_WARN_DEPRECATED_SYNTAX_NO_REPLACEMENT"
        );
        assert_eq!(
            super::ERR_WARN_DEPRECATED_SYNTAX_NO_REPLACEMENT
                .code()
                .value(),
            errcode::ErrWarnDeprecatedSyntaxNoReplacement as isize,
            "parser::ERR_WARN_DEPRECATED_SYNTAX_NO_REPLACEMENT"
        );
        assert_eq!(
            super::ERR_WRONG_USAGE.class(),
            TerrorClass::Parser,
            "parser::ERR_WRONG_USAGE"
        );
        assert_eq!(
            super::ERR_WRONG_USAGE.code().value(),
            errcode::ErrWrongUsage as isize,
            "parser::ERR_WRONG_USAGE"
        );
        assert_eq!(
            super::ERR_WRONG_DB_NAME.class(),
            TerrorClass::Parser,
            "parser::ERR_WRONG_DB_NAME"
        );
        assert_eq!(
            super::ERR_WRONG_DB_NAME.code().value(),
            errcode::ErrWrongDBName as isize,
            "parser::ERR_WRONG_DB_NAME"
        );
    }
}
