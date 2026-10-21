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

use super::{std_err, Class};
use crate::terror::TerrorError;
use crate::tidb::errcode;

/// Source `table.ErrColumnCantNull` (`pkg/table/table.go:71`).
pub static ERR_COLUMN_CANT_NULL: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Table, errcode::ErrBadNull));
/// Source `table.ErrUnknownColumn` (`pkg/table/table.go:73`).
pub static ERR_UNKNOWN_COLUMN: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Table, errcode::ErrBadField));
/// Source `table.errDuplicateColumn` (`pkg/table/table.go:74`).
pub static ERR_DUPLICATE_COLUMN: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Table, errcode::ErrFieldSpecifiedTwice));
/// Source `table.ErrWarnNullToNotnull` (`pkg/table/table.go:77`).
pub static ERR_WARN_NULL_TO_NOTNULL: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Executor, errcode::ErrWarnNullToNotnull));
/// Source `table.errGetDefaultFailed` (`pkg/table/table.go:79`).
pub static ERR_GET_DEFAULT_FAILED: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Table, errcode::ErrFieldGetDefaultFailed));
/// Source `table.ErrNoDefaultValue` (`pkg/table/table.go:83`).
pub static ERR_NO_DEFAULT_VALUE: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Table, errcode::ErrNoDefaultForField));
/// Source `table.ErrIndexOutBound` (`pkg/table/table.go:85`).
pub static ERR_INDEX_OUT_BOUND: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Table, errcode::ErrIndexOutBound));
/// Source `table.ErrUnsupportedOp` (`pkg/table/table.go:87`).
pub static ERR_UNSUPPORTED_OP: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Table, errcode::ErrUnsupportedOp));
/// Source `table.ErrRowNotFound` (`pkg/table/table.go:89`).
pub static ERR_ROW_NOT_FOUND: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Table, errcode::ErrRowNotFound));
/// Source `table.ErrTableStateCantNone` (`pkg/table/table.go:91`).
pub static ERR_TABLE_STATE_CANT_NONE: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Table, errcode::ErrTableStateCantNone));
/// Source `table.ErrColumnStateCantNone` (`pkg/table/table.go:93`).
pub static ERR_COLUMN_STATE_CANT_NONE: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Table, errcode::ErrColumnStateCantNone));
/// Source `table.ErrColumnStateNonPublic` (`pkg/table/table.go:95`).
pub static ERR_COLUMN_STATE_NON_PUBLIC: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Table, errcode::ErrColumnStateNonPublic));
/// Source `table.ErrIndexStateCantNone` (`pkg/table/table.go:97`).
pub static ERR_INDEX_STATE_CANT_NONE: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Table, errcode::ErrIndexStateCantNone));
/// Source `table.ErrInvalidRecordKey` (`pkg/table/table.go:99`).
pub static ERR_INVALID_RECORD_KEY: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Table, errcode::ErrInvalidRecordKey));
/// Source `table.ErrTruncatedWrongValueForField` (`pkg/table/table.go:101`).
pub static ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Table, errcode::ErrTruncatedWrongValueForField));
/// Source `table.ErrUnknownPartition` (`pkg/table/table.go:103`).
pub static ERR_UNKNOWN_PARTITION: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Table, errcode::ErrUnknownPartition));
/// Source `table.ErrNoPartitionForGivenValue` (`pkg/table/table.go:105`).
pub static ERR_NO_PARTITION_FOR_GIVEN_VALUE: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Table, errcode::ErrNoPartitionForGivenValue));
/// Source `table.ErrLockOrActiveTransaction` (`pkg/table/table.go:107`).
pub static ERR_LOCK_OR_ACTIVE_TRANSACTION: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Table, errcode::ErrLockOrActiveTransaction));
/// Source `table.ErrSequenceHasRunOut` (`pkg/table/table.go:109`).
pub static ERR_SEQUENCE_HAS_RUN_OUT: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Table, errcode::ErrSequenceRunOut));
/// Source `table.ErrRowDoesNotMatchGivenPartitionSet` (`pkg/table/table.go:111`).
pub static ERR_ROW_DOES_NOT_MATCH_GIVEN_PARTITION_SET: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Table, errcode::ErrRowDoesNotMatchGivenPartitionSet));
/// Source `table.ErrTempTableFull` (`pkg/table/table.go:113`).
pub static ERR_TEMP_TABLE_FULL: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Table, errcode::ErrRecordFileFull));
/// Source `table.ErrOptOnCacheTable` (`pkg/table/table.go:115`).
pub static ERR_OPT_ON_CACHE_TABLE: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Ddl, errcode::ErrOptOnCacheTable));
/// Source `table.ErrCheckConstraintViolated` (`pkg/table/table.go:117`).
pub static ERR_CHECK_CONSTRAINT_VIOLATED: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Table, errcode::ErrCheckConstraintViolated));
