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

/// Source `parser/ast.ErrNoParts` (`pkg/parser/ast/ddl.go:4418`).
pub static ERR_NO_PARTS: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Ddl, errcode::ErrNoParts));
/// Source `parser/ast.ErrPartitionColumnList` (`pkg/parser/ast/ddl.go:4419`).
pub static ERR_PARTITION_COLUMN_LIST: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Ddl, errcode::ErrPartitionColumnList));
/// Source `parser/ast.ErrPartitionRequiresValues` (`pkg/parser/ast/ddl.go:4420`).
pub static ERR_PARTITION_REQUIRES_VALUES: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Ddl, errcode::ErrPartitionRequiresValues));
/// Source `parser/ast.ErrPartitionsMustBeDefined` (`pkg/parser/ast/ddl.go:4421`).
pub static ERR_PARTITIONS_MUST_BE_DEFINED: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Ddl, errcode::ErrPartitionsMustBeDefined));
/// Source `parser/ast.ErrPartitionWrongNoPart` (`pkg/parser/ast/ddl.go:4422`).
pub static ERR_PARTITION_WRONG_NO_PART: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Ddl, errcode::ErrPartitionWrongNoPart));
/// Source `parser/ast.ErrPartitionWrongNoSubpart` (`pkg/parser/ast/ddl.go:4423`).
pub static ERR_PARTITION_WRONG_NO_SUBPART: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Ddl, errcode::ErrPartitionWrongNoSubpart));
/// Source `parser/ast.ErrPartitionWrongValues` (`pkg/parser/ast/ddl.go:4424`).
pub static ERR_PARTITION_WRONG_VALUES: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Ddl, errcode::ErrPartitionWrongValues));
/// Source `parser/ast.ErrRowSinglePartitionField` (`pkg/parser/ast/ddl.go:4425`).
pub static ERR_ROW_SINGLE_PARTITION_FIELD: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Ddl, errcode::ErrRowSinglePartitionField));
/// Source `parser/ast.ErrSubpartition` (`pkg/parser/ast/ddl.go:4426`).
pub static ERR_SUBPARTITION: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Ddl, errcode::ErrSubpartition));
/// Source `parser/ast.ErrSystemVersioningWrongPartitions` (`pkg/parser/ast/ddl.go:4427`).
pub static ERR_SYSTEM_VERSIONING_WRONG_PARTITIONS: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Ddl, errcode::ErrSystemVersioningWrongPartitions));
/// Source `parser/ast.ErrTooManyValues` (`pkg/parser/ast/ddl.go:4428`).
pub static ERR_TOO_MANY_VALUES: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Ddl, errcode::ErrTooManyValues));
/// Source `parser/ast.ErrWrongPartitionTypeExpectedSystemTime` (`pkg/parser/ast/ddl.go:4429`).
pub static ERR_WRONG_PARTITION_TYPE_EXPECTED_SYSTEM_TIME: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Ddl, errcode::ErrWrongPartitionTypeExpectedSystemTime));
/// Source `parser/ast.ErrUnknownCharacterSet` (`pkg/parser/ast/ddl.go:4430`).
pub static ERR_UNKNOWN_CHARACTER_SET: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Ddl, errcode::ErrUnknownCharacterSet));
/// Source `parser/ast.ErrCoalescePartitionNoPartition` (`pkg/parser/ast/ddl.go:4431`).
pub static ERR_COALESCE_PARTITION_NO_PARTITION: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Ddl, errcode::ErrCoalescePartitionNoPartition));
/// Source `parser/ast.ErrWrongUsage` (`pkg/parser/ast/ddl.go:4432`).
pub static ERR_WRONG_USAGE: LazyLock<TerrorError> =
    LazyLock::new(|| std_err(Class::Ddl, errcode::ErrWrongUsage));

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
            super::ERR_NO_PARTS.class(),
            TerrorClass::Ddl,
            "ast::ERR_NO_PARTS"
        );
        assert_eq!(
            super::ERR_NO_PARTS.code().value(),
            errcode::ErrNoParts as isize,
            "ast::ERR_NO_PARTS"
        );
        assert_eq!(
            super::ERR_PARTITION_COLUMN_LIST.class(),
            TerrorClass::Ddl,
            "ast::ERR_PARTITION_COLUMN_LIST"
        );
        assert_eq!(
            super::ERR_PARTITION_COLUMN_LIST.code().value(),
            errcode::ErrPartitionColumnList as isize,
            "ast::ERR_PARTITION_COLUMN_LIST"
        );
        assert_eq!(
            super::ERR_PARTITION_REQUIRES_VALUES.class(),
            TerrorClass::Ddl,
            "ast::ERR_PARTITION_REQUIRES_VALUES"
        );
        assert_eq!(
            super::ERR_PARTITION_REQUIRES_VALUES.code().value(),
            errcode::ErrPartitionRequiresValues as isize,
            "ast::ERR_PARTITION_REQUIRES_VALUES"
        );
        assert_eq!(
            super::ERR_PARTITIONS_MUST_BE_DEFINED.class(),
            TerrorClass::Ddl,
            "ast::ERR_PARTITIONS_MUST_BE_DEFINED"
        );
        assert_eq!(
            super::ERR_PARTITIONS_MUST_BE_DEFINED.code().value(),
            errcode::ErrPartitionsMustBeDefined as isize,
            "ast::ERR_PARTITIONS_MUST_BE_DEFINED"
        );
        assert_eq!(
            super::ERR_PARTITION_WRONG_NO_PART.class(),
            TerrorClass::Ddl,
            "ast::ERR_PARTITION_WRONG_NO_PART"
        );
        assert_eq!(
            super::ERR_PARTITION_WRONG_NO_PART.code().value(),
            errcode::ErrPartitionWrongNoPart as isize,
            "ast::ERR_PARTITION_WRONG_NO_PART"
        );
        assert_eq!(
            super::ERR_PARTITION_WRONG_NO_SUBPART.class(),
            TerrorClass::Ddl,
            "ast::ERR_PARTITION_WRONG_NO_SUBPART"
        );
        assert_eq!(
            super::ERR_PARTITION_WRONG_NO_SUBPART.code().value(),
            errcode::ErrPartitionWrongNoSubpart as isize,
            "ast::ERR_PARTITION_WRONG_NO_SUBPART"
        );
        assert_eq!(
            super::ERR_PARTITION_WRONG_VALUES.class(),
            TerrorClass::Ddl,
            "ast::ERR_PARTITION_WRONG_VALUES"
        );
        assert_eq!(
            super::ERR_PARTITION_WRONG_VALUES.code().value(),
            errcode::ErrPartitionWrongValues as isize,
            "ast::ERR_PARTITION_WRONG_VALUES"
        );
        assert_eq!(
            super::ERR_ROW_SINGLE_PARTITION_FIELD.class(),
            TerrorClass::Ddl,
            "ast::ERR_ROW_SINGLE_PARTITION_FIELD"
        );
        assert_eq!(
            super::ERR_ROW_SINGLE_PARTITION_FIELD.code().value(),
            errcode::ErrRowSinglePartitionField as isize,
            "ast::ERR_ROW_SINGLE_PARTITION_FIELD"
        );
        assert_eq!(
            super::ERR_SUBPARTITION.class(),
            TerrorClass::Ddl,
            "ast::ERR_SUBPARTITION"
        );
        assert_eq!(
            super::ERR_SUBPARTITION.code().value(),
            errcode::ErrSubpartition as isize,
            "ast::ERR_SUBPARTITION"
        );
        assert_eq!(
            super::ERR_SYSTEM_VERSIONING_WRONG_PARTITIONS.class(),
            TerrorClass::Ddl,
            "ast::ERR_SYSTEM_VERSIONING_WRONG_PARTITIONS"
        );
        assert_eq!(
            super::ERR_SYSTEM_VERSIONING_WRONG_PARTITIONS
                .code()
                .value(),
            errcode::ErrSystemVersioningWrongPartitions as isize,
            "ast::ERR_SYSTEM_VERSIONING_WRONG_PARTITIONS"
        );
        assert_eq!(
            super::ERR_TOO_MANY_VALUES.class(),
            TerrorClass::Ddl,
            "ast::ERR_TOO_MANY_VALUES"
        );
        assert_eq!(
            super::ERR_TOO_MANY_VALUES.code().value(),
            errcode::ErrTooManyValues as isize,
            "ast::ERR_TOO_MANY_VALUES"
        );
        assert_eq!(
            super::ERR_WRONG_PARTITION_TYPE_EXPECTED_SYSTEM_TIME.class(),
            TerrorClass::Ddl,
            "ast::ERR_WRONG_PARTITION_TYPE_EXPECTED_SYSTEM_TIME"
        );
        assert_eq!(
            super::ERR_WRONG_PARTITION_TYPE_EXPECTED_SYSTEM_TIME
                .code()
                .value(),
            errcode::ErrWrongPartitionTypeExpectedSystemTime as isize,
            "ast::ERR_WRONG_PARTITION_TYPE_EXPECTED_SYSTEM_TIME"
        );
        assert_eq!(
            super::ERR_UNKNOWN_CHARACTER_SET.class(),
            TerrorClass::Ddl,
            "ast::ERR_UNKNOWN_CHARACTER_SET"
        );
        assert_eq!(
            super::ERR_UNKNOWN_CHARACTER_SET.code().value(),
            errcode::ErrUnknownCharacterSet as isize,
            "ast::ERR_UNKNOWN_CHARACTER_SET"
        );
        assert_eq!(
            super::ERR_COALESCE_PARTITION_NO_PARTITION.class(),
            TerrorClass::Ddl,
            "ast::ERR_COALESCE_PARTITION_NO_PARTITION"
        );
        assert_eq!(
            super::ERR_COALESCE_PARTITION_NO_PARTITION
                .code()
                .value(),
            errcode::ErrCoalescePartitionNoPartition as isize,
            "ast::ERR_COALESCE_PARTITION_NO_PARTITION"
        );
        assert_eq!(
            super::ERR_WRONG_USAGE.class(),
            TerrorClass::Ddl,
            "ast::ERR_WRONG_USAGE"
        );
        assert_eq!(
            super::ERR_WRONG_USAGE.code().value(),
            errcode::ErrWrongUsage as isize,
            "ast::ERR_WRONG_USAGE"
        );
    }
}
