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

//! The Go-package error prototypes in `tidb_error::catalog` carry the class
//! and code of the Go definition they transcribe (`Class.NewStd(code)`), so
//! `Error()` prints the `[class:code]` Go prints. The table below is the
//! generator's own manifest (package, constant, `terror.Class*`, errno name).

use tidb_error::catalog;
use tidb_terror::TerrorClass;
use tidb_errno::errcode;

#[test]
fn catalog_prototypes_carry_their_go_class_and_code() {
    let mut checked = 0;
    assert_eq!(
        catalog::admin::ERR_ADMIN_CHECK_TABLE.class(),
        TerrorClass::Admin,
        "admin::ERR_ADMIN_CHECK_TABLE"
    );
    assert_eq!(
        catalog::admin::ERR_ADMIN_CHECK_TABLE.code().value(),
        errcode::ErrAdminCheckTable as isize,
        "admin::ERR_ADMIN_CHECK_TABLE"
    );
    checked += 1;
    assert_eq!(
        catalog::ast::ERR_NO_PARTS.class(),
        TerrorClass::Ddl,
        "ast::ERR_NO_PARTS"
    );
    assert_eq!(
        catalog::ast::ERR_NO_PARTS.code().value(),
        errcode::ErrNoParts as isize,
        "ast::ERR_NO_PARTS"
    );
    checked += 1;
    assert_eq!(
        catalog::ast::ERR_PARTITION_COLUMN_LIST.class(),
        TerrorClass::Ddl,
        "ast::ERR_PARTITION_COLUMN_LIST"
    );
    assert_eq!(
        catalog::ast::ERR_PARTITION_COLUMN_LIST.code().value(),
        errcode::ErrPartitionColumnList as isize,
        "ast::ERR_PARTITION_COLUMN_LIST"
    );
    checked += 1;
    assert_eq!(
        catalog::ast::ERR_PARTITION_REQUIRES_VALUES.class(),
        TerrorClass::Ddl,
        "ast::ERR_PARTITION_REQUIRES_VALUES"
    );
    assert_eq!(
        catalog::ast::ERR_PARTITION_REQUIRES_VALUES.code().value(),
        errcode::ErrPartitionRequiresValues as isize,
        "ast::ERR_PARTITION_REQUIRES_VALUES"
    );
    checked += 1;
    assert_eq!(
        catalog::ast::ERR_PARTITIONS_MUST_BE_DEFINED.class(),
        TerrorClass::Ddl,
        "ast::ERR_PARTITIONS_MUST_BE_DEFINED"
    );
    assert_eq!(
        catalog::ast::ERR_PARTITIONS_MUST_BE_DEFINED.code().value(),
        errcode::ErrPartitionsMustBeDefined as isize,
        "ast::ERR_PARTITIONS_MUST_BE_DEFINED"
    );
    checked += 1;
    assert_eq!(
        catalog::ast::ERR_PARTITION_WRONG_NO_PART.class(),
        TerrorClass::Ddl,
        "ast::ERR_PARTITION_WRONG_NO_PART"
    );
    assert_eq!(
        catalog::ast::ERR_PARTITION_WRONG_NO_PART.code().value(),
        errcode::ErrPartitionWrongNoPart as isize,
        "ast::ERR_PARTITION_WRONG_NO_PART"
    );
    checked += 1;
    assert_eq!(
        catalog::ast::ERR_PARTITION_WRONG_NO_SUBPART.class(),
        TerrorClass::Ddl,
        "ast::ERR_PARTITION_WRONG_NO_SUBPART"
    );
    assert_eq!(
        catalog::ast::ERR_PARTITION_WRONG_NO_SUBPART.code().value(),
        errcode::ErrPartitionWrongNoSubpart as isize,
        "ast::ERR_PARTITION_WRONG_NO_SUBPART"
    );
    checked += 1;
    assert_eq!(
        catalog::ast::ERR_PARTITION_WRONG_VALUES.class(),
        TerrorClass::Ddl,
        "ast::ERR_PARTITION_WRONG_VALUES"
    );
    assert_eq!(
        catalog::ast::ERR_PARTITION_WRONG_VALUES.code().value(),
        errcode::ErrPartitionWrongValues as isize,
        "ast::ERR_PARTITION_WRONG_VALUES"
    );
    checked += 1;
    assert_eq!(
        catalog::ast::ERR_ROW_SINGLE_PARTITION_FIELD.class(),
        TerrorClass::Ddl,
        "ast::ERR_ROW_SINGLE_PARTITION_FIELD"
    );
    assert_eq!(
        catalog::ast::ERR_ROW_SINGLE_PARTITION_FIELD.code().value(),
        errcode::ErrRowSinglePartitionField as isize,
        "ast::ERR_ROW_SINGLE_PARTITION_FIELD"
    );
    checked += 1;
    assert_eq!(
        catalog::ast::ERR_SUBPARTITION.class(),
        TerrorClass::Ddl,
        "ast::ERR_SUBPARTITION"
    );
    assert_eq!(
        catalog::ast::ERR_SUBPARTITION.code().value(),
        errcode::ErrSubpartition as isize,
        "ast::ERR_SUBPARTITION"
    );
    checked += 1;
    assert_eq!(
        catalog::ast::ERR_SYSTEM_VERSIONING_WRONG_PARTITIONS.class(),
        TerrorClass::Ddl,
        "ast::ERR_SYSTEM_VERSIONING_WRONG_PARTITIONS"
    );
    assert_eq!(
        catalog::ast::ERR_SYSTEM_VERSIONING_WRONG_PARTITIONS
            .code()
            .value(),
        errcode::ErrSystemVersioningWrongPartitions as isize,
        "ast::ERR_SYSTEM_VERSIONING_WRONG_PARTITIONS"
    );
    checked += 1;
    assert_eq!(
        catalog::ast::ERR_TOO_MANY_VALUES.class(),
        TerrorClass::Ddl,
        "ast::ERR_TOO_MANY_VALUES"
    );
    assert_eq!(
        catalog::ast::ERR_TOO_MANY_VALUES.code().value(),
        errcode::ErrTooManyValues as isize,
        "ast::ERR_TOO_MANY_VALUES"
    );
    checked += 1;
    assert_eq!(
        catalog::ast::ERR_WRONG_PARTITION_TYPE_EXPECTED_SYSTEM_TIME.class(),
        TerrorClass::Ddl,
        "ast::ERR_WRONG_PARTITION_TYPE_EXPECTED_SYSTEM_TIME"
    );
    assert_eq!(
        catalog::ast::ERR_WRONG_PARTITION_TYPE_EXPECTED_SYSTEM_TIME
            .code()
            .value(),
        errcode::ErrWrongPartitionTypeExpectedSystemTime as isize,
        "ast::ERR_WRONG_PARTITION_TYPE_EXPECTED_SYSTEM_TIME"
    );
    checked += 1;
    assert_eq!(
        catalog::ast::ERR_UNKNOWN_CHARACTER_SET.class(),
        TerrorClass::Ddl,
        "ast::ERR_UNKNOWN_CHARACTER_SET"
    );
    assert_eq!(
        catalog::ast::ERR_UNKNOWN_CHARACTER_SET.code().value(),
        errcode::ErrUnknownCharacterSet as isize,
        "ast::ERR_UNKNOWN_CHARACTER_SET"
    );
    checked += 1;
    assert_eq!(
        catalog::ast::ERR_COALESCE_PARTITION_NO_PARTITION.class(),
        TerrorClass::Ddl,
        "ast::ERR_COALESCE_PARTITION_NO_PARTITION"
    );
    assert_eq!(
        catalog::ast::ERR_COALESCE_PARTITION_NO_PARTITION
            .code()
            .value(),
        errcode::ErrCoalescePartitionNoPartition as isize,
        "ast::ERR_COALESCE_PARTITION_NO_PARTITION"
    );
    checked += 1;
    assert_eq!(
        catalog::ast::ERR_WRONG_USAGE.class(),
        TerrorClass::Ddl,
        "ast::ERR_WRONG_USAGE"
    );
    assert_eq!(
        catalog::ast::ERR_WRONG_USAGE.code().value(),
        errcode::ErrWrongUsage as isize,
        "ast::ERR_WRONG_USAGE"
    );
    checked += 1;
    assert_eq!(
        catalog::autoid::ERR_INVALID_TABLE_ID.class(),
        TerrorClass::Autoid,
        "autoid::ERR_INVALID_TABLE_ID"
    );
    assert_eq!(
        catalog::autoid::ERR_INVALID_TABLE_ID.code().value(),
        errcode::ErrInvalidTableID as isize,
        "autoid::ERR_INVALID_TABLE_ID"
    );
    checked += 1;
    assert_eq!(
        catalog::autoid::ERR_INVALID_INCREMENT_AND_OFFSET.class(),
        TerrorClass::Autoid,
        "autoid::ERR_INVALID_INCREMENT_AND_OFFSET"
    );
    assert_eq!(
        catalog::autoid::ERR_INVALID_INCREMENT_AND_OFFSET
            .code()
            .value(),
        errcode::ErrInvalidIncrementAndOffset as isize,
        "autoid::ERR_INVALID_INCREMENT_AND_OFFSET"
    );
    checked += 1;
    assert_eq!(
        catalog::autoid::ERR_NOT_IMPLEMENTED.class(),
        TerrorClass::Autoid,
        "autoid::ERR_NOT_IMPLEMENTED"
    );
    assert_eq!(
        catalog::autoid::ERR_NOT_IMPLEMENTED.code().value(),
        errcode::ErrNotImplemented as isize,
        "autoid::ERR_NOT_IMPLEMENTED"
    );
    checked += 1;
    assert_eq!(
        catalog::autoid::ERR_AUTOINC_READ_FAILED.class(),
        TerrorClass::Autoid,
        "autoid::ERR_AUTOINC_READ_FAILED"
    );
    assert_eq!(
        catalog::autoid::ERR_AUTOINC_READ_FAILED.code().value(),
        errcode::ErrAutoincReadFailed as isize,
        "autoid::ERR_AUTOINC_READ_FAILED"
    );
    checked += 1;
    assert_eq!(
        catalog::autoid::ERR_WRONG_AUTO_KEY.class(),
        TerrorClass::Autoid,
        "autoid::ERR_WRONG_AUTO_KEY"
    );
    assert_eq!(
        catalog::autoid::ERR_WRONG_AUTO_KEY.code().value(),
        errcode::ErrWrongAutoKey as isize,
        "autoid::ERR_WRONG_AUTO_KEY"
    );
    checked += 1;
    assert_eq!(
        catalog::autoid::ERR_INVALID_ALLOCATOR_TYPE.class(),
        TerrorClass::Autoid,
        "autoid::ERR_INVALID_ALLOCATOR_TYPE"
    );
    assert_eq!(
        catalog::autoid::ERR_INVALID_ALLOCATOR_TYPE.code().value(),
        errcode::ErrUnknownAllocatorType as isize,
        "autoid::ERR_INVALID_ALLOCATOR_TYPE"
    );
    checked += 1;
    assert_eq!(
        catalog::autoid::ERR_AUTO_RAND_READ_FAILED.class(),
        TerrorClass::Autoid,
        "autoid::ERR_AUTO_RAND_READ_FAILED"
    );
    assert_eq!(
        catalog::autoid::ERR_AUTO_RAND_READ_FAILED.code().value(),
        errcode::ErrAutoRandReadFailed as isize,
        "autoid::ERR_AUTO_RAND_READ_FAILED"
    );
    checked += 1;
    assert_eq!(
        catalog::charset::ERR_INVALID_CHARACTER_STRING.class(),
        TerrorClass::Parser,
        "charset::ERR_INVALID_CHARACTER_STRING"
    );
    assert_eq!(
        catalog::charset::ERR_INVALID_CHARACTER_STRING
            .code()
            .value(),
        errcode::ErrInvalidCharacterString as isize,
        "charset::ERR_INVALID_CHARACTER_STRING"
    );
    checked += 1;
    assert_eq!(
        catalog::charset::ERR_UNKNOWN_COLLATION.class(),
        TerrorClass::Ddl,
        "charset::ERR_UNKNOWN_COLLATION"
    );
    assert_eq!(
        catalog::charset::ERR_UNKNOWN_COLLATION.code().value(),
        errcode::ErrUnknownCollation as isize,
        "charset::ERR_UNKNOWN_COLLATION"
    );
    checked += 1;
    assert_eq!(
        catalog::charset::ERR_COLLATION_CHARSET_MISMATCH.class(),
        TerrorClass::Ddl,
        "charset::ERR_COLLATION_CHARSET_MISMATCH"
    );
    assert_eq!(
        catalog::charset::ERR_COLLATION_CHARSET_MISMATCH
            .code()
            .value(),
        errcode::ErrCollationCharsetMismatch as isize,
        "charset::ERR_COLLATION_CHARSET_MISMATCH"
    );
    checked += 1;
    assert_eq!(
        catalog::collate::ERR_UNSUPPORTED_COLLATION.class(),
        TerrorClass::Ddl,
        "collate::ERR_UNSUPPORTED_COLLATION"
    );
    assert_eq!(
        catalog::collate::ERR_UNSUPPORTED_COLLATION.code().value(),
        errcode::ErrUnknownCollation as isize,
        "collate::ERR_UNSUPPORTED_COLLATION"
    );
    checked += 1;
    assert_eq!(
        catalog::collate::ERR_ILLEGAL_MIX_COLLATION.class(),
        TerrorClass::Expression,
        "collate::ERR_ILLEGAL_MIX_COLLATION"
    );
    assert_eq!(
        catalog::collate::ERR_ILLEGAL_MIX_COLLATION.code().value(),
        errcode::ErrCantAggregateNcollations as isize,
        "collate::ERR_ILLEGAL_MIX_COLLATION"
    );
    checked += 1;
    assert_eq!(
        catalog::collate::ERR_ILLEGAL_MIX2_COLLATION.class(),
        TerrorClass::Expression,
        "collate::ERR_ILLEGAL_MIX2_COLLATION"
    );
    assert_eq!(
        catalog::collate::ERR_ILLEGAL_MIX2_COLLATION.code().value(),
        errcode::ErrCantAggregate2collations as isize,
        "collate::ERR_ILLEGAL_MIX2_COLLATION"
    );
    checked += 1;
    assert_eq!(
        catalog::collate::ERR_ILLEGAL_MIX3_COLLATION.class(),
        TerrorClass::Expression,
        "collate::ERR_ILLEGAL_MIX3_COLLATION"
    );
    assert_eq!(
        catalog::collate::ERR_ILLEGAL_MIX3_COLLATION.code().value(),
        errcode::ErrCantAggregate3collations as isize,
        "collate::ERR_ILLEGAL_MIX3_COLLATION"
    );
    checked += 1;
    assert_eq!(
        catalog::consistency::ERR_ADMIN_CHECK_INCONSISTENT.class(),
        TerrorClass::Admin,
        "consistency::ERR_ADMIN_CHECK_INCONSISTENT"
    );
    assert_eq!(
        catalog::consistency::ERR_ADMIN_CHECK_INCONSISTENT
            .code()
            .value(),
        errcode::ErrDataInconsistent as isize,
        "consistency::ERR_ADMIN_CHECK_INCONSISTENT"
    );
    checked += 1;
    assert_eq!(
        catalog::consistency::ERR_LOOKUP_INCONSISTENT.class(),
        TerrorClass::Executor,
        "consistency::ERR_LOOKUP_INCONSISTENT"
    );
    assert_eq!(
        catalog::consistency::ERR_LOOKUP_INCONSISTENT.code().value(),
        errcode::ErrDataInconsistentMismatchCount as isize,
        "consistency::ERR_LOOKUP_INCONSISTENT"
    );
    checked += 1;
    assert_eq!(
        catalog::consistency::ERR_ADMIN_CHECK_INCONSISTENT_WITH_COL_INFO.class(),
        TerrorClass::Executor,
        "consistency::ERR_ADMIN_CHECK_INCONSISTENT_WITH_COL_INFO"
    );
    assert_eq!(
        catalog::consistency::ERR_ADMIN_CHECK_INCONSISTENT_WITH_COL_INFO
            .code()
            .value(),
        errcode::ErrDataInconsistentMismatchIndex as isize,
        "consistency::ERR_ADMIN_CHECK_INCONSISTENT_WITH_COL_INFO"
    );
    checked += 1;
    assert_eq!(
        catalog::domain::ERR_INFO_SCHEMA_EXPIRED.class(),
        TerrorClass::Domain,
        "domain::ERR_INFO_SCHEMA_EXPIRED"
    );
    assert_eq!(
        catalog::domain::ERR_INFO_SCHEMA_EXPIRED.code().value(),
        errcode::ErrInfoSchemaExpired as isize,
        "domain::ERR_INFO_SCHEMA_EXPIRED"
    );
    checked += 1;
    assert_eq!(
        catalog::domain::ERR_INFO_SCHEMA_CHANGED.class(),
        TerrorClass::Domain,
        "domain::ERR_INFO_SCHEMA_CHANGED"
    );
    assert_eq!(
        catalog::domain::ERR_INFO_SCHEMA_CHANGED.code().value(),
        errcode::ErrInfoSchemaChanged as isize,
        "domain::ERR_INFO_SCHEMA_CHANGED"
    );
    checked += 1;
    assert_eq!(
        catalog::executor::ERR_CHECK_PARTIAL_INDEX_WITHOUT_FAST_CHECK.class(),
        TerrorClass::Executor,
        "executor::ERR_CHECK_PARTIAL_INDEX_WITHOUT_FAST_CHECK"
    );
    assert_eq!(
        catalog::executor::ERR_CHECK_PARTIAL_INDEX_WITHOUT_FAST_CHECK
            .code()
            .value(),
        errcode::ErrCheckPartialIndexWithoutFastCheck as isize,
        "executor::ERR_CHECK_PARTIAL_INDEX_WITHOUT_FAST_CHECK"
    );
    checked += 1;
    assert_eq!(
        catalog::expression::ERR_CANNOT_CONVERT_STRING.class(),
        TerrorClass::Expression,
        "expression::ERR_CANNOT_CONVERT_STRING"
    );
    assert_eq!(
        catalog::expression::ERR_CANNOT_CONVERT_STRING
            .code()
            .value(),
        errcode::ErrCannotConvertString as isize,
        "expression::ERR_CANNOT_CONVERT_STRING"
    );
    checked += 1;
    assert_eq!(
        catalog::expression::ERR_INCORRECT_PARAMETER_COUNT.class(),
        TerrorClass::Expression,
        "expression::ERR_INCORRECT_PARAMETER_COUNT"
    );
    assert_eq!(
        catalog::expression::ERR_INCORRECT_PARAMETER_COUNT
            .code()
            .value(),
        errcode::ErrWrongParamcountToNativeFct as isize,
        "expression::ERR_INCORRECT_PARAMETER_COUNT"
    );
    checked += 1;
    assert_eq!(
        catalog::expression::ERR_DIVISION_BY_ZERO.class(),
        TerrorClass::Expression,
        "expression::ERR_DIVISION_BY_ZERO"
    );
    assert_eq!(
        catalog::expression::ERR_DIVISION_BY_ZERO.code().value(),
        errcode::ErrDivisionByZero as isize,
        "expression::ERR_DIVISION_BY_ZERO"
    );
    checked += 1;
    assert_eq!(
        catalog::expression::ERR_REGEXP.class(),
        TerrorClass::Expression,
        "expression::ERR_REGEXP"
    );
    assert_eq!(
        catalog::expression::ERR_REGEXP.code().value(),
        errcode::ErrRegexp as isize,
        "expression::ERR_REGEXP"
    );
    checked += 1;
    assert_eq!(
        catalog::expression::ERR_OPERAND_COLUMNS.class(),
        TerrorClass::Expression,
        "expression::ERR_OPERAND_COLUMNS"
    );
    assert_eq!(
        catalog::expression::ERR_OPERAND_COLUMNS.code().value(),
        errcode::ErrOperandColumns as isize,
        "expression::ERR_OPERAND_COLUMNS"
    );
    checked += 1;
    assert_eq!(
        catalog::expression::ERR_CUT_VALUE_GROUP_CONCAT.class(),
        TerrorClass::Expression,
        "expression::ERR_CUT_VALUE_GROUP_CONCAT"
    );
    assert_eq!(
        catalog::expression::ERR_CUT_VALUE_GROUP_CONCAT
            .code()
            .value(),
        errcode::ErrCutValueGroupConcat as isize,
        "expression::ERR_CUT_VALUE_GROUP_CONCAT"
    );
    checked += 1;
    assert_eq!(
        catalog::expression::ERR_FUNCTIONS_NOOP_IMPL.class(),
        TerrorClass::Expression,
        "expression::ERR_FUNCTIONS_NOOP_IMPL"
    );
    assert_eq!(
        catalog::expression::ERR_FUNCTIONS_NOOP_IMPL.code().value(),
        errcode::ErrNotSupportedYet as isize,
        "expression::ERR_FUNCTIONS_NOOP_IMPL"
    );
    checked += 1;
    assert_eq!(
        catalog::expression::ERR_INVALID_ARGUMENT_FOR_LOGARITHM.class(),
        TerrorClass::Expression,
        "expression::ERR_INVALID_ARGUMENT_FOR_LOGARITHM"
    );
    assert_eq!(
        catalog::expression::ERR_INVALID_ARGUMENT_FOR_LOGARITHM
            .code()
            .value(),
        errcode::ErrInvalidArgumentForLogarithm as isize,
        "expression::ERR_INVALID_ARGUMENT_FOR_LOGARITHM"
    );
    checked += 1;
    assert_eq!(
        catalog::expression::ERR_INCORRECT_TYPE.class(),
        TerrorClass::Expression,
        "expression::ERR_INCORRECT_TYPE"
    );
    assert_eq!(
        catalog::expression::ERR_INCORRECT_TYPE.code().value(),
        errcode::ErrIncorrectType as isize,
        "expression::ERR_INCORRECT_TYPE"
    );
    checked += 1;
    assert_eq!(
        catalog::expression::ERR_INVALID_TYPE_FOR_JSON.class(),
        TerrorClass::Expression,
        "expression::ERR_INVALID_TYPE_FOR_JSON"
    );
    assert_eq!(
        catalog::expression::ERR_INVALID_TYPE_FOR_JSON
            .code()
            .value(),
        errcode::ErrInvalidTypeForJSON as isize,
        "expression::ERR_INVALID_TYPE_FOR_JSON"
    );
    checked += 1;
    assert_eq!(
        catalog::expression::ERR_INVALID_TABLE_SAMPLE.class(),
        TerrorClass::Expression,
        "expression::ERR_INVALID_TABLE_SAMPLE"
    );
    assert_eq!(
        catalog::expression::ERR_INVALID_TABLE_SAMPLE.code().value(),
        errcode::ErrInvalidTableSample as isize,
        "expression::ERR_INVALID_TABLE_SAMPLE"
    );
    checked += 1;
    assert_eq!(
        catalog::expression::ERR_NOT_SUPPORTED_YET.class(),
        TerrorClass::Expression,
        "expression::ERR_NOT_SUPPORTED_YET"
    );
    assert_eq!(
        catalog::expression::ERR_NOT_SUPPORTED_YET.code().value(),
        errcode::ErrNotSupportedYet as isize,
        "expression::ERR_NOT_SUPPORTED_YET"
    );
    checked += 1;
    assert_eq!(
        catalog::expression::ERR_INVALID_JSON_FOR_FUNC_INDEX.class(),
        TerrorClass::Expression,
        "expression::ERR_INVALID_JSON_FOR_FUNC_INDEX"
    );
    assert_eq!(
        catalog::expression::ERR_INVALID_JSON_FOR_FUNC_INDEX
            .code()
            .value(),
        errcode::ErrInvalidJSONValueForFuncIndex as isize,
        "expression::ERR_INVALID_JSON_FOR_FUNC_INDEX"
    );
    checked += 1;
    assert_eq!(
        catalog::expression::ERR_DATA_OUT_OF_RANGE_FUNC_INDEX.class(),
        TerrorClass::Expression,
        "expression::ERR_DATA_OUT_OF_RANGE_FUNC_INDEX"
    );
    assert_eq!(
        catalog::expression::ERR_DATA_OUT_OF_RANGE_FUNC_INDEX
            .code()
            .value(),
        errcode::ErrDataOutOfRangeFunctionalIndex as isize,
        "expression::ERR_DATA_OUT_OF_RANGE_FUNC_INDEX"
    );
    checked += 1;
    assert_eq!(
        catalog::expression::ERR_FUNC_INDEX_DATA_IS_TOO_LONG.class(),
        TerrorClass::Expression,
        "expression::ERR_FUNC_INDEX_DATA_IS_TOO_LONG"
    );
    assert_eq!(
        catalog::expression::ERR_FUNC_INDEX_DATA_IS_TOO_LONG
            .code()
            .value(),
        errcode::ErrFunctionalIndexDataIsTooLong as isize,
        "expression::ERR_FUNC_INDEX_DATA_IS_TOO_LONG"
    );
    checked += 1;
    assert_eq!(
        catalog::expression::ERR_FUNCTION_NOT_EXISTS.class(),
        TerrorClass::Expression,
        "expression::ERR_FUNCTION_NOT_EXISTS"
    );
    assert_eq!(
        catalog::expression::ERR_FUNCTION_NOT_EXISTS.code().value(),
        errcode::ErrSpDoesNotExist as isize,
        "expression::ERR_FUNCTION_NOT_EXISTS"
    );
    checked += 1;
    assert_eq!(
        catalog::expression::ERR_ZLIB_Z_DATA.class(),
        TerrorClass::Expression,
        "expression::ERR_ZLIB_Z_DATA"
    );
    assert_eq!(
        catalog::expression::ERR_ZLIB_Z_DATA.code().value(),
        errcode::ErrZlibZData as isize,
        "expression::ERR_ZLIB_Z_DATA"
    );
    checked += 1;
    assert_eq!(
        catalog::expression::ERR_ZLIB_Z_BUF.class(),
        TerrorClass::Expression,
        "expression::ERR_ZLIB_Z_BUF"
    );
    assert_eq!(
        catalog::expression::ERR_ZLIB_Z_BUF.code().value(),
        errcode::ErrZlibZBuf as isize,
        "expression::ERR_ZLIB_Z_BUF"
    );
    checked += 1;
    assert_eq!(
        catalog::expression::ERR_INCORRECT_ARGS.class(),
        TerrorClass::Expression,
        "expression::ERR_INCORRECT_ARGS"
    );
    assert_eq!(
        catalog::expression::ERR_INCORRECT_ARGS.code().value(),
        errcode::ErrWrongArguments as isize,
        "expression::ERR_INCORRECT_ARGS"
    );
    checked += 1;
    assert_eq!(
        catalog::expression::ERR_UNKNOWN_CHARACTER_SET.class(),
        TerrorClass::Expression,
        "expression::ERR_UNKNOWN_CHARACTER_SET"
    );
    assert_eq!(
        catalog::expression::ERR_UNKNOWN_CHARACTER_SET
            .code()
            .value(),
        errcode::ErrUnknownCharacterSet as isize,
        "expression::ERR_UNKNOWN_CHARACTER_SET"
    );
    checked += 1;
    assert_eq!(
        catalog::expression::ERR_DEFAULT_VALUE.class(),
        TerrorClass::Expression,
        "expression::ERR_DEFAULT_VALUE"
    );
    assert_eq!(
        catalog::expression::ERR_DEFAULT_VALUE.code().value(),
        errcode::ErrInvalidDefault as isize,
        "expression::ERR_DEFAULT_VALUE"
    );
    checked += 1;
    assert_eq!(
        catalog::expression::ERR_DEPRECATED_SYNTAX_NO_REPLACEMENT.class(),
        TerrorClass::Expression,
        "expression::ERR_DEPRECATED_SYNTAX_NO_REPLACEMENT"
    );
    assert_eq!(
        catalog::expression::ERR_DEPRECATED_SYNTAX_NO_REPLACEMENT
            .code()
            .value(),
        errcode::ErrWarnDeprecatedSyntaxNoReplacement as isize,
        "expression::ERR_DEPRECATED_SYNTAX_NO_REPLACEMENT"
    );
    checked += 1;
    assert_eq!(
        catalog::expression::ERR_WARN_ALLOWED_PACKET_OVERFLOWED.class(),
        TerrorClass::Expression,
        "expression::ERR_WARN_ALLOWED_PACKET_OVERFLOWED"
    );
    assert_eq!(
        catalog::expression::ERR_WARN_ALLOWED_PACKET_OVERFLOWED
            .code()
            .value(),
        errcode::ErrWarnAllowedPacketOverflowed as isize,
        "expression::ERR_WARN_ALLOWED_PACKET_OVERFLOWED"
    );
    checked += 1;
    assert_eq!(
        catalog::expression::ERR_WARN_OPTION_IGNORED.class(),
        TerrorClass::Expression,
        "expression::ERR_WARN_OPTION_IGNORED"
    );
    assert_eq!(
        catalog::expression::ERR_WARN_OPTION_IGNORED.code().value(),
        errcode::WarnOptionIgnored as isize,
        "expression::ERR_WARN_OPTION_IGNORED"
    );
    checked += 1;
    assert_eq!(
        catalog::expression::ERR_TRUNCATED_WRONG_VALUE.class(),
        TerrorClass::Expression,
        "expression::ERR_TRUNCATED_WRONG_VALUE"
    );
    assert_eq!(
        catalog::expression::ERR_TRUNCATED_WRONG_VALUE
            .code()
            .value(),
        errcode::ErrTruncatedWrongValue as isize,
        "expression::ERR_TRUNCATED_WRONG_VALUE"
    );
    checked += 1;
    assert_eq!(
        catalog::expression::ERR_UNKNOWN_LOCALE.class(),
        TerrorClass::Expression,
        "expression::ERR_UNKNOWN_LOCALE"
    );
    assert_eq!(
        catalog::expression::ERR_UNKNOWN_LOCALE.code().value(),
        errcode::ErrUnknownLocale as isize,
        "expression::ERR_UNKNOWN_LOCALE"
    );
    checked += 1;
    assert_eq!(
        catalog::expression::ERR_NON_UNIQ.class(),
        TerrorClass::Expression,
        "expression::ERR_NON_UNIQ"
    );
    assert_eq!(
        catalog::expression::ERR_NON_UNIQ.code().value(),
        errcode::ErrNonUniq as isize,
        "expression::ERR_NON_UNIQ"
    );
    checked += 1;
    assert_eq!(
        catalog::expression::ERR_WRONG_VALUE_FOR_TYPE.class(),
        TerrorClass::Expression,
        "expression::ERR_WRONG_VALUE_FOR_TYPE"
    );
    assert_eq!(
        catalog::expression::ERR_WRONG_VALUE_FOR_TYPE.code().value(),
        errcode::ErrWrongValueForType as isize,
        "expression::ERR_WRONG_VALUE_FOR_TYPE"
    );
    checked += 1;
    assert_eq!(
        catalog::expression::ERR_UNKNOWN.class(),
        TerrorClass::Expression,
        "expression::ERR_UNKNOWN"
    );
    assert_eq!(
        catalog::expression::ERR_UNKNOWN.code().value(),
        errcode::ErrUnknown as isize,
        "expression::ERR_UNKNOWN"
    );
    checked += 1;
    assert_eq!(
        catalog::expression::ERR_SPECIFIC_ACCESS_DENIED.class(),
        TerrorClass::Expression,
        "expression::ERR_SPECIFIC_ACCESS_DENIED"
    );
    assert_eq!(
        catalog::expression::ERR_SPECIFIC_ACCESS_DENIED
            .code()
            .value(),
        errcode::ErrSpecificAccessDenied as isize,
        "expression::ERR_SPECIFIC_ACCESS_DENIED"
    );
    checked += 1;
    assert_eq!(
        catalog::expression::ERR_USER_LOCK_DEADLOCK.class(),
        TerrorClass::Expression,
        "expression::ERR_USER_LOCK_DEADLOCK"
    );
    assert_eq!(
        catalog::expression::ERR_USER_LOCK_DEADLOCK.code().value(),
        errcode::ErrUserLockDeadlock as isize,
        "expression::ERR_USER_LOCK_DEADLOCK"
    );
    checked += 1;
    assert_eq!(
        catalog::expression::ERR_USER_LOCK_WRONG_NAME.class(),
        TerrorClass::Expression,
        "expression::ERR_USER_LOCK_WRONG_NAME"
    );
    assert_eq!(
        catalog::expression::ERR_USER_LOCK_WRONG_NAME.code().value(),
        errcode::ErrUserLockWrongName as isize,
        "expression::ERR_USER_LOCK_WRONG_NAME"
    );
    checked += 1;
    assert_eq!(
        catalog::expression::ERR_JSON_IN_BOOLEAN_CONTEXT.class(),
        TerrorClass::Expression,
        "expression::ERR_JSON_IN_BOOLEAN_CONTEXT"
    );
    assert_eq!(
        catalog::expression::ERR_JSON_IN_BOOLEAN_CONTEXT
            .code()
            .value(),
        errcode::ErrJSONInBooleanContext as isize,
        "expression::ERR_JSON_IN_BOOLEAN_CONTEXT"
    );
    checked += 1;
    assert_eq!(
        catalog::expression::ERR_BAD_NULL.class(),
        TerrorClass::Expression,
        "expression::ERR_BAD_NULL"
    );
    assert_eq!(
        catalog::expression::ERR_BAD_NULL.code().value(),
        errcode::ErrBadNull as isize,
        "expression::ERR_BAD_NULL"
    );
    checked += 1;
    assert_eq!(
        catalog::expression::ERR_SEQUENCE_ACCESS_DENIED.class(),
        TerrorClass::Expression,
        "expression::ERR_SEQUENCE_ACCESS_DENIED"
    );
    assert_eq!(
        catalog::expression::ERR_SEQUENCE_ACCESS_DENIED
            .code()
            .value(),
        errcode::ErrTableaccessDenied as isize,
        "expression::ERR_SEQUENCE_ACCESS_DENIED"
    );
    checked += 1;
    assert_eq!(
        catalog::expression::ERR_UNSUPPORTED_JSON_COMPARISON.class(),
        TerrorClass::Expression,
        "expression::ERR_UNSUPPORTED_JSON_COMPARISON"
    );
    assert_eq!(
        catalog::expression::ERR_UNSUPPORTED_JSON_COMPARISON
            .code()
            .value(),
        errcode::ErrNotSupportedYet as isize,
        "expression::ERR_UNSUPPORTED_JSON_COMPARISON"
    );
    checked += 1;
    assert_eq!(
        catalog::hint::ERR_WARN_CONFLICTING_HINT.class(),
        TerrorClass::Optimizer,
        "hint::ERR_WARN_CONFLICTING_HINT"
    );
    assert_eq!(
        catalog::hint::ERR_WARN_CONFLICTING_HINT.code().value(),
        errcode::ErrWarnConflictingHint as isize,
        "hint::ERR_WARN_CONFLICTING_HINT"
    );
    checked += 1;
    assert_eq!(
        catalog::infoschema::ERR_DATABASE_EXISTS.class(),
        TerrorClass::Schema,
        "infoschema::ERR_DATABASE_EXISTS"
    );
    assert_eq!(
        catalog::infoschema::ERR_DATABASE_EXISTS.code().value(),
        errcode::ErrDBCreateExists as isize,
        "infoschema::ERR_DATABASE_EXISTS"
    );
    checked += 1;
    assert_eq!(
        catalog::infoschema::ERR_DATABASE_DROP_EXISTS.class(),
        TerrorClass::Schema,
        "infoschema::ERR_DATABASE_DROP_EXISTS"
    );
    assert_eq!(
        catalog::infoschema::ERR_DATABASE_DROP_EXISTS.code().value(),
        errcode::ErrDBDropExists as isize,
        "infoschema::ERR_DATABASE_DROP_EXISTS"
    );
    checked += 1;
    assert_eq!(
        catalog::infoschema::ERR_ACCESS_DENIED.class(),
        TerrorClass::Schema,
        "infoschema::ERR_ACCESS_DENIED"
    );
    assert_eq!(
        catalog::infoschema::ERR_ACCESS_DENIED.code().value(),
        errcode::ErrAccessDenied as isize,
        "infoschema::ERR_ACCESS_DENIED"
    );
    checked += 1;
    assert_eq!(
        catalog::infoschema::ERR_DATABASE_NOT_EXISTS.class(),
        TerrorClass::Schema,
        "infoschema::ERR_DATABASE_NOT_EXISTS"
    );
    assert_eq!(
        catalog::infoschema::ERR_DATABASE_NOT_EXISTS.code().value(),
        errcode::ErrBadDB as isize,
        "infoschema::ERR_DATABASE_NOT_EXISTS"
    );
    checked += 1;
    assert_eq!(
        catalog::infoschema::ERR_PLACEMENT_POLICY_EXISTS.class(),
        TerrorClass::Schema,
        "infoschema::ERR_PLACEMENT_POLICY_EXISTS"
    );
    assert_eq!(
        catalog::infoschema::ERR_PLACEMENT_POLICY_EXISTS
            .code()
            .value(),
        errcode::ErrPlacementPolicyExists as isize,
        "infoschema::ERR_PLACEMENT_POLICY_EXISTS"
    );
    checked += 1;
    assert_eq!(
        catalog::infoschema::ERR_PLACEMENT_POLICY_NOT_EXISTS.class(),
        TerrorClass::Schema,
        "infoschema::ERR_PLACEMENT_POLICY_NOT_EXISTS"
    );
    assert_eq!(
        catalog::infoschema::ERR_PLACEMENT_POLICY_NOT_EXISTS
            .code()
            .value(),
        errcode::ErrPlacementPolicyNotExists as isize,
        "infoschema::ERR_PLACEMENT_POLICY_NOT_EXISTS"
    );
    checked += 1;
    assert_eq!(
        catalog::infoschema::ERR_RESOURCE_GROUP_EXISTS.class(),
        TerrorClass::Schema,
        "infoschema::ERR_RESOURCE_GROUP_EXISTS"
    );
    assert_eq!(
        catalog::infoschema::ERR_RESOURCE_GROUP_EXISTS
            .code()
            .value(),
        errcode::ErrResourceGroupExists as isize,
        "infoschema::ERR_RESOURCE_GROUP_EXISTS"
    );
    checked += 1;
    assert_eq!(
        catalog::infoschema::ERR_RESOURCE_GROUP_NOT_EXISTS.class(),
        TerrorClass::Schema,
        "infoschema::ERR_RESOURCE_GROUP_NOT_EXISTS"
    );
    assert_eq!(
        catalog::infoschema::ERR_RESOURCE_GROUP_NOT_EXISTS
            .code()
            .value(),
        errcode::ErrResourceGroupNotExists as isize,
        "infoschema::ERR_RESOURCE_GROUP_NOT_EXISTS"
    );
    checked += 1;
    assert_eq!(
        catalog::infoschema::ERR_RESOURCE_GROUP_INVALID_BACKGROUND_TASK_NAME.class(),
        TerrorClass::Executor,
        "infoschema::ERR_RESOURCE_GROUP_INVALID_BACKGROUND_TASK_NAME"
    );
    assert_eq!(
        catalog::infoschema::ERR_RESOURCE_GROUP_INVALID_BACKGROUND_TASK_NAME
            .code()
            .value(),
        errcode::ErrResourceGroupInvalidBackgroundTaskName as isize,
        "infoschema::ERR_RESOURCE_GROUP_INVALID_BACKGROUND_TASK_NAME"
    );
    checked += 1;
    assert_eq!(
        catalog::infoschema::ERR_RESOURCE_GROUP_INVALID_FOR_ROLE.class(),
        TerrorClass::Schema,
        "infoschema::ERR_RESOURCE_GROUP_INVALID_FOR_ROLE"
    );
    assert_eq!(
        catalog::infoschema::ERR_RESOURCE_GROUP_INVALID_FOR_ROLE
            .code()
            .value(),
        errcode::ErrResourceGroupInvalidForRole as isize,
        "infoschema::ERR_RESOURCE_GROUP_INVALID_FOR_ROLE"
    );
    checked += 1;
    assert_eq!(
        catalog::infoschema::ERR_RESERVED_SYNTAX.class(),
        TerrorClass::Schema,
        "infoschema::ERR_RESERVED_SYNTAX"
    );
    assert_eq!(
        catalog::infoschema::ERR_RESERVED_SYNTAX.code().value(),
        errcode::ErrReservedSyntax as isize,
        "infoschema::ERR_RESERVED_SYNTAX"
    );
    checked += 1;
    assert_eq!(
        catalog::infoschema::ERR_TABLE_EXISTS.class(),
        TerrorClass::Schema,
        "infoschema::ERR_TABLE_EXISTS"
    );
    assert_eq!(
        catalog::infoschema::ERR_TABLE_EXISTS.code().value(),
        errcode::ErrTableExists as isize,
        "infoschema::ERR_TABLE_EXISTS"
    );
    checked += 1;
    assert_eq!(
        catalog::infoschema::ERR_TABLE_DROP_EXISTS.class(),
        TerrorClass::Schema,
        "infoschema::ERR_TABLE_DROP_EXISTS"
    );
    assert_eq!(
        catalog::infoschema::ERR_TABLE_DROP_EXISTS.code().value(),
        errcode::ErrBadTable as isize,
        "infoschema::ERR_TABLE_DROP_EXISTS"
    );
    checked += 1;
    assert_eq!(
        catalog::infoschema::ERR_SEQUENCE_DROP_EXISTS.class(),
        TerrorClass::Schema,
        "infoschema::ERR_SEQUENCE_DROP_EXISTS"
    );
    assert_eq!(
        catalog::infoschema::ERR_SEQUENCE_DROP_EXISTS.code().value(),
        errcode::ErrUnknownSequence as isize,
        "infoschema::ERR_SEQUENCE_DROP_EXISTS"
    );
    checked += 1;
    assert_eq!(
        catalog::infoschema::ERR_COLUMN_NOT_EXISTS.class(),
        TerrorClass::Schema,
        "infoschema::ERR_COLUMN_NOT_EXISTS"
    );
    assert_eq!(
        catalog::infoschema::ERR_COLUMN_NOT_EXISTS.code().value(),
        errcode::ErrBadField as isize,
        "infoschema::ERR_COLUMN_NOT_EXISTS"
    );
    checked += 1;
    assert_eq!(
        catalog::infoschema::ERR_COLUMN_EXISTS.class(),
        TerrorClass::Schema,
        "infoschema::ERR_COLUMN_EXISTS"
    );
    assert_eq!(
        catalog::infoschema::ERR_COLUMN_EXISTS.code().value(),
        errcode::ErrDupFieldName as isize,
        "infoschema::ERR_COLUMN_EXISTS"
    );
    checked += 1;
    assert_eq!(
        catalog::infoschema::ERR_KEY_NAME_DUPLICATE.class(),
        TerrorClass::Schema,
        "infoschema::ERR_KEY_NAME_DUPLICATE"
    );
    assert_eq!(
        catalog::infoschema::ERR_KEY_NAME_DUPLICATE.code().value(),
        errcode::ErrDupKeyName as isize,
        "infoschema::ERR_KEY_NAME_DUPLICATE"
    );
    checked += 1;
    assert_eq!(
        catalog::infoschema::ERR_NONUNIQ_TABLE.class(),
        TerrorClass::Schema,
        "infoschema::ERR_NONUNIQ_TABLE"
    );
    assert_eq!(
        catalog::infoschema::ERR_NONUNIQ_TABLE.code().value(),
        errcode::ErrNonuniqTable as isize,
        "infoschema::ERR_NONUNIQ_TABLE"
    );
    checked += 1;
    assert_eq!(
        catalog::infoschema::ERR_MULTIPLE_PRI_KEY.class(),
        TerrorClass::Schema,
        "infoschema::ERR_MULTIPLE_PRI_KEY"
    );
    assert_eq!(
        catalog::infoschema::ERR_MULTIPLE_PRI_KEY.code().value(),
        errcode::ErrMultiplePriKey as isize,
        "infoschema::ERR_MULTIPLE_PRI_KEY"
    );
    checked += 1;
    assert_eq!(
        catalog::infoschema::ERR_TOO_MANY_KEY_PARTS.class(),
        TerrorClass::Schema,
        "infoschema::ERR_TOO_MANY_KEY_PARTS"
    );
    assert_eq!(
        catalog::infoschema::ERR_TOO_MANY_KEY_PARTS.code().value(),
        errcode::ErrTooManyKeyParts as isize,
        "infoschema::ERR_TOO_MANY_KEY_PARTS"
    );
    checked += 1;
    assert_eq!(
        catalog::infoschema::ERR_FOREIGN_KEY_NOT_EXISTS.class(),
        TerrorClass::Schema,
        "infoschema::ERR_FOREIGN_KEY_NOT_EXISTS"
    );
    assert_eq!(
        catalog::infoschema::ERR_FOREIGN_KEY_NOT_EXISTS
            .code()
            .value(),
        errcode::ErrCantDropFieldOrKey as isize,
        "infoschema::ERR_FOREIGN_KEY_NOT_EXISTS"
    );
    checked += 1;
    assert_eq!(
        catalog::infoschema::ERR_TABLE_NOT_LOCKED_FOR_WRITE.class(),
        TerrorClass::Schema,
        "infoschema::ERR_TABLE_NOT_LOCKED_FOR_WRITE"
    );
    assert_eq!(
        catalog::infoschema::ERR_TABLE_NOT_LOCKED_FOR_WRITE
            .code()
            .value(),
        errcode::ErrTableNotLockedForWrite as isize,
        "infoschema::ERR_TABLE_NOT_LOCKED_FOR_WRITE"
    );
    checked += 1;
    assert_eq!(
        catalog::infoschema::ERR_TABLE_NOT_LOCKED.class(),
        TerrorClass::Schema,
        "infoschema::ERR_TABLE_NOT_LOCKED"
    );
    assert_eq!(
        catalog::infoschema::ERR_TABLE_NOT_LOCKED.code().value(),
        errcode::ErrTableNotLocked as isize,
        "infoschema::ERR_TABLE_NOT_LOCKED"
    );
    checked += 1;
    assert_eq!(
        catalog::infoschema::ERR_TABLE_NOT_EXISTS.class(),
        TerrorClass::Schema,
        "infoschema::ERR_TABLE_NOT_EXISTS"
    );
    assert_eq!(
        catalog::infoschema::ERR_TABLE_NOT_EXISTS.code().value(),
        errcode::ErrNoSuchTable as isize,
        "infoschema::ERR_TABLE_NOT_EXISTS"
    );
    checked += 1;
    assert_eq!(
        catalog::infoschema::ERR_KEY_NOT_EXISTS.class(),
        TerrorClass::Schema,
        "infoschema::ERR_KEY_NOT_EXISTS"
    );
    assert_eq!(
        catalog::infoschema::ERR_KEY_NOT_EXISTS.code().value(),
        errcode::ErrKeyDoesNotExist as isize,
        "infoschema::ERR_KEY_NOT_EXISTS"
    );
    checked += 1;
    assert_eq!(
        catalog::infoschema::ERR_CANNOT_ADD_FOREIGN.class(),
        TerrorClass::Schema,
        "infoschema::ERR_CANNOT_ADD_FOREIGN"
    );
    assert_eq!(
        catalog::infoschema::ERR_CANNOT_ADD_FOREIGN.code().value(),
        errcode::ErrCannotAddForeign as isize,
        "infoschema::ERR_CANNOT_ADD_FOREIGN"
    );
    checked += 1;
    assert_eq!(
        catalog::infoschema::ERR_FOREIGN_KEY_ON_PARTITIONED.class(),
        TerrorClass::Schema,
        "infoschema::ERR_FOREIGN_KEY_ON_PARTITIONED"
    );
    assert_eq!(
        catalog::infoschema::ERR_FOREIGN_KEY_ON_PARTITIONED
            .code()
            .value(),
        errcode::ErrForeignKeyOnPartitioned as isize,
        "infoschema::ERR_FOREIGN_KEY_ON_PARTITIONED"
    );
    checked += 1;
    assert_eq!(
        catalog::infoschema::ERR_FOREIGN_KEY_NOT_MATCH.class(),
        TerrorClass::Schema,
        "infoschema::ERR_FOREIGN_KEY_NOT_MATCH"
    );
    assert_eq!(
        catalog::infoschema::ERR_FOREIGN_KEY_NOT_MATCH
            .code()
            .value(),
        errcode::ErrWrongFkDef as isize,
        "infoschema::ERR_FOREIGN_KEY_NOT_MATCH"
    );
    checked += 1;
    assert_eq!(
        catalog::infoschema::ERR_INDEX_EXISTS.class(),
        TerrorClass::Schema,
        "infoschema::ERR_INDEX_EXISTS"
    );
    assert_eq!(
        catalog::infoschema::ERR_INDEX_EXISTS.code().value(),
        errcode::ErrDupIndex as isize,
        "infoschema::ERR_INDEX_EXISTS"
    );
    checked += 1;
    assert_eq!(
        catalog::infoschema::ERR_USER_DROP_EXISTS.class(),
        TerrorClass::Schema,
        "infoschema::ERR_USER_DROP_EXISTS"
    );
    assert_eq!(
        catalog::infoschema::ERR_USER_DROP_EXISTS.code().value(),
        errcode::ErrBadUser as isize,
        "infoschema::ERR_USER_DROP_EXISTS"
    );
    checked += 1;
    assert_eq!(
        catalog::infoschema::ERR_USER_ALREADY_EXISTS.class(),
        TerrorClass::Schema,
        "infoschema::ERR_USER_ALREADY_EXISTS"
    );
    assert_eq!(
        catalog::infoschema::ERR_USER_ALREADY_EXISTS.code().value(),
        errcode::ErrUserAlreadyExists as isize,
        "infoschema::ERR_USER_ALREADY_EXISTS"
    );
    checked += 1;
    assert_eq!(
        catalog::infoschema::ERR_TABLE_LOCKED.class(),
        TerrorClass::Schema,
        "infoschema::ERR_TABLE_LOCKED"
    );
    assert_eq!(
        catalog::infoschema::ERR_TABLE_LOCKED.code().value(),
        errcode::ErrTableLocked as isize,
        "infoschema::ERR_TABLE_LOCKED"
    );
    checked += 1;
    assert_eq!(
        catalog::infoschema::ERR_WRONG_OBJECT.class(),
        TerrorClass::Schema,
        "infoschema::ERR_WRONG_OBJECT"
    );
    assert_eq!(
        catalog::infoschema::ERR_WRONG_OBJECT.code().value(),
        errcode::ErrWrongObject as isize,
        "infoschema::ERR_WRONG_OBJECT"
    );
    checked += 1;
    assert_eq!(
        catalog::infoschema::ERR_ADMIN_CHECK_TABLE.class(),
        TerrorClass::Schema,
        "infoschema::ERR_ADMIN_CHECK_TABLE"
    );
    assert_eq!(
        catalog::infoschema::ERR_ADMIN_CHECK_TABLE.code().value(),
        errcode::ErrAdminCheckTable as isize,
        "infoschema::ERR_ADMIN_CHECK_TABLE"
    );
    checked += 1;
    assert_eq!(
        catalog::infoschema::ERR_EMPTY_DATABASE.class(),
        TerrorClass::Schema,
        "infoschema::ERR_EMPTY_DATABASE"
    );
    assert_eq!(
        catalog::infoschema::ERR_EMPTY_DATABASE.code().value(),
        errcode::ErrBadDB as isize,
        "infoschema::ERR_EMPTY_DATABASE"
    );
    checked += 1;
    assert_eq!(
        catalog::infoschema::ERR_FORBID_SCHEMA_CHANGE.class(),
        TerrorClass::Schema,
        "infoschema::ERR_FORBID_SCHEMA_CHANGE"
    );
    assert_eq!(
        catalog::infoschema::ERR_FORBID_SCHEMA_CHANGE.code().value(),
        errcode::ErrForbidSchemaChange as isize,
        "infoschema::ERR_FORBID_SCHEMA_CHANGE"
    );
    checked += 1;
    assert_eq!(
        catalog::infoschema::ERR_TABLE_WITHOUT_PRIMARY_KEY.class(),
        TerrorClass::Schema,
        "infoschema::ERR_TABLE_WITHOUT_PRIMARY_KEY"
    );
    assert_eq!(
        catalog::infoschema::ERR_TABLE_WITHOUT_PRIMARY_KEY
            .code()
            .value(),
        errcode::ErrTableWithoutPrimaryKey as isize,
        "infoschema::ERR_TABLE_WITHOUT_PRIMARY_KEY"
    );
    checked += 1;
    assert_eq!(
        catalog::infoschema::ERR_FOREIGN_KEY_CANNOT_USE_VIRTUAL_COLUMN.class(),
        TerrorClass::Schema,
        "infoschema::ERR_FOREIGN_KEY_CANNOT_USE_VIRTUAL_COLUMN"
    );
    assert_eq!(
        catalog::infoschema::ERR_FOREIGN_KEY_CANNOT_USE_VIRTUAL_COLUMN
            .code()
            .value(),
        errcode::ErrForeignKeyCannotUseVirtualColumn as isize,
        "infoschema::ERR_FOREIGN_KEY_CANNOT_USE_VIRTUAL_COLUMN"
    );
    checked += 1;
    assert_eq!(
        catalog::infoschema::ERR_FOREIGN_KEY_CANNOT_OPEN_PARENT.class(),
        TerrorClass::Schema,
        "infoschema::ERR_FOREIGN_KEY_CANNOT_OPEN_PARENT"
    );
    assert_eq!(
        catalog::infoschema::ERR_FOREIGN_KEY_CANNOT_OPEN_PARENT
            .code()
            .value(),
        errcode::ErrForeignKeyCannotOpenParent as isize,
        "infoschema::ERR_FOREIGN_KEY_CANNOT_OPEN_PARENT"
    );
    checked += 1;
    assert_eq!(
        catalog::infoschema::ERR_FOREIGN_KEY_NO_COLUMN_IN_PARENT.class(),
        TerrorClass::Schema,
        "infoschema::ERR_FOREIGN_KEY_NO_COLUMN_IN_PARENT"
    );
    assert_eq!(
        catalog::infoschema::ERR_FOREIGN_KEY_NO_COLUMN_IN_PARENT
            .code()
            .value(),
        errcode::ErrForeignKeyNoColumnInParent as isize,
        "infoschema::ERR_FOREIGN_KEY_NO_COLUMN_IN_PARENT"
    );
    checked += 1;
    assert_eq!(
        catalog::infoschema::ERR_FOREIGN_KEY_NO_INDEX_IN_PARENT.class(),
        TerrorClass::Schema,
        "infoschema::ERR_FOREIGN_KEY_NO_INDEX_IN_PARENT"
    );
    assert_eq!(
        catalog::infoschema::ERR_FOREIGN_KEY_NO_INDEX_IN_PARENT
            .code()
            .value(),
        errcode::ErrForeignKeyNoIndexInParent as isize,
        "infoschema::ERR_FOREIGN_KEY_NO_INDEX_IN_PARENT"
    );
    checked += 1;
    assert_eq!(
        catalog::infoschema::ERR_FOREIGN_KEY_COLUMN_NOT_NULL.class(),
        TerrorClass::Schema,
        "infoschema::ERR_FOREIGN_KEY_COLUMN_NOT_NULL"
    );
    assert_eq!(
        catalog::infoschema::ERR_FOREIGN_KEY_COLUMN_NOT_NULL
            .code()
            .value(),
        errcode::ErrForeignKeyColumnNotNull as isize,
        "infoschema::ERR_FOREIGN_KEY_COLUMN_NOT_NULL"
    );
    checked += 1;
    assert_eq!(
        catalog::infoschema::ERR_RESOURCE_GROUP_SUPPORT_DISABLED.class(),
        TerrorClass::Schema,
        "infoschema::ERR_RESOURCE_GROUP_SUPPORT_DISABLED"
    );
    assert_eq!(
        catalog::infoschema::ERR_RESOURCE_GROUP_SUPPORT_DISABLED
            .code()
            .value(),
        errcode::ErrResourceGroupSupportDisabled as isize,
        "infoschema::ERR_RESOURCE_GROUP_SUPPORT_DISABLED"
    );
    checked += 1;
    assert_eq!(
        catalog::infoschema::ERR_CHECK_CONSTRAINT_DUP_NAME.class(),
        TerrorClass::Schema,
        "infoschema::ERR_CHECK_CONSTRAINT_DUP_NAME"
    );
    assert_eq!(
        catalog::infoschema::ERR_CHECK_CONSTRAINT_DUP_NAME
            .code()
            .value(),
        errcode::ErrCheckConstraintDupName as isize,
        "infoschema::ERR_CHECK_CONSTRAINT_DUP_NAME"
    );
    checked += 1;
    assert_eq!(
        catalog::infoschema::ERR_PROTECTED_TABLE_MODE.class(),
        TerrorClass::Schema,
        "infoschema::ERR_PROTECTED_TABLE_MODE"
    );
    assert_eq!(
        catalog::infoschema::ERR_PROTECTED_TABLE_MODE.code().value(),
        errcode::ErrProtectedTableMode as isize,
        "infoschema::ERR_PROTECTED_TABLE_MODE"
    );
    checked += 1;
    assert_eq!(
        catalog::infoschema::ERR_INVALID_TABLE_MODE_SET.class(),
        TerrorClass::Schema,
        "infoschema::ERR_INVALID_TABLE_MODE_SET"
    );
    assert_eq!(
        catalog::infoschema::ERR_INVALID_TABLE_MODE_SET
            .code()
            .value(),
        errcode::ErrInvalidTableModeSet as isize,
        "infoschema::ERR_INVALID_TABLE_MODE_SET"
    );
    checked += 1;
    assert_eq!(
        catalog::infosync::ERR_HTTP_SERVICE_ERROR.class(),
        TerrorClass::Domain,
        "infosync::ERR_HTTP_SERVICE_ERROR"
    );
    assert_eq!(
        catalog::infosync::ERR_HTTP_SERVICE_ERROR.code().value(),
        errcode::ErrHTTPServiceError as isize,
        "infosync::ERR_HTTP_SERVICE_ERROR"
    );
    checked += 1;
    assert_eq!(
        catalog::infosync::ERR_PROMETHEUS_ADDR_IS_NOT_SET.class(),
        TerrorClass::Domain,
        "infosync::ERR_PROMETHEUS_ADDR_IS_NOT_SET"
    );
    assert_eq!(
        catalog::infosync::ERR_PROMETHEUS_ADDR_IS_NOT_SET
            .code()
            .value(),
        errcode::ErrPrometheusAddrIsNotSet as isize,
        "infosync::ERR_PROMETHEUS_ADDR_IS_NOT_SET"
    );
    checked += 1;
    assert_eq!(
        catalog::kv::ERR_NOT_EXIST.class(),
        TerrorClass::Kv,
        "kv::ERR_NOT_EXIST"
    );
    assert_eq!(
        catalog::kv::ERR_NOT_EXIST.code().value(),
        errcode::ErrNotExist as isize,
        "kv::ERR_NOT_EXIST"
    );
    checked += 1;
    assert_eq!(
        catalog::kv::ERR_TXN_RETRYABLE.class(),
        TerrorClass::Kv,
        "kv::ERR_TXN_RETRYABLE"
    );
    assert_eq!(
        catalog::kv::ERR_TXN_RETRYABLE.code().value(),
        errcode::ErrTxnRetryable as isize,
        "kv::ERR_TXN_RETRYABLE"
    );
    checked += 1;
    assert_eq!(
        catalog::kv::ERR_CANNOT_SET_NIL_VALUE.class(),
        TerrorClass::Kv,
        "kv::ERR_CANNOT_SET_NIL_VALUE"
    );
    assert_eq!(
        catalog::kv::ERR_CANNOT_SET_NIL_VALUE.code().value(),
        errcode::ErrCannotSetNilValue as isize,
        "kv::ERR_CANNOT_SET_NIL_VALUE"
    );
    checked += 1;
    assert_eq!(
        catalog::kv::ERR_INVALID_TXN.class(),
        TerrorClass::Kv,
        "kv::ERR_INVALID_TXN"
    );
    assert_eq!(
        catalog::kv::ERR_INVALID_TXN.code().value(),
        errcode::ErrInvalidTxn as isize,
        "kv::ERR_INVALID_TXN"
    );
    checked += 1;
    assert_eq!(
        catalog::kv::ERR_TXN_TOO_LARGE.class(),
        TerrorClass::Kv,
        "kv::ERR_TXN_TOO_LARGE"
    );
    assert_eq!(
        catalog::kv::ERR_TXN_TOO_LARGE.code().value(),
        errcode::ErrTxnTooLarge as isize,
        "kv::ERR_TXN_TOO_LARGE"
    );
    checked += 1;
    assert_eq!(
        catalog::kv::ERR_ENTRY_TOO_LARGE.class(),
        TerrorClass::Kv,
        "kv::ERR_ENTRY_TOO_LARGE"
    );
    assert_eq!(
        catalog::kv::ERR_ENTRY_TOO_LARGE.code().value(),
        errcode::ErrEntryTooLarge as isize,
        "kv::ERR_ENTRY_TOO_LARGE"
    );
    checked += 1;
    assert_eq!(
        catalog::kv::ERR_KEY_TOO_LARGE.class(),
        TerrorClass::Kv,
        "kv::ERR_KEY_TOO_LARGE"
    );
    assert_eq!(
        catalog::kv::ERR_KEY_TOO_LARGE.code().value(),
        errcode::ErrKeyTooLarge as isize,
        "kv::ERR_KEY_TOO_LARGE"
    );
    checked += 1;
    assert_eq!(
        catalog::kv::ERR_KEY_EXISTS.class(),
        TerrorClass::Kv,
        "kv::ERR_KEY_EXISTS"
    );
    assert_eq!(
        catalog::kv::ERR_KEY_EXISTS.code().value(),
        errcode::ErrDupEntry as isize,
        "kv::ERR_KEY_EXISTS"
    );
    checked += 1;
    assert_eq!(
        catalog::kv::ERR_NOT_IMPLEMENTED.class(),
        TerrorClass::Kv,
        "kv::ERR_NOT_IMPLEMENTED"
    );
    assert_eq!(
        catalog::kv::ERR_NOT_IMPLEMENTED.code().value(),
        errcode::ErrNotImplemented as isize,
        "kv::ERR_NOT_IMPLEMENTED"
    );
    checked += 1;
    assert_eq!(
        catalog::kv::ERR_WRITE_CONFLICT.class(),
        TerrorClass::Kv,
        "kv::ERR_WRITE_CONFLICT"
    );
    assert_eq!(
        catalog::kv::ERR_WRITE_CONFLICT.code().value(),
        errcode::ErrWriteConflict as isize,
        "kv::ERR_WRITE_CONFLICT"
    );
    checked += 1;
    assert_eq!(
        catalog::kv::ERR_WRITE_CONFLICT_IN_TI_DB.class(),
        TerrorClass::Kv,
        "kv::ERR_WRITE_CONFLICT_IN_TI_DB"
    );
    assert_eq!(
        catalog::kv::ERR_WRITE_CONFLICT_IN_TI_DB.code().value(),
        errcode::ErrWriteConflictInTiDB as isize,
        "kv::ERR_WRITE_CONFLICT_IN_TI_DB"
    );
    checked += 1;
    assert_eq!(
        catalog::kv::ERR_SHARED_LOCK_LOST.class(),
        TerrorClass::TiKv,
        "kv::ERR_SHARED_LOCK_LOST"
    );
    assert_eq!(
        catalog::kv::ERR_SHARED_LOCK_LOST.code().value(),
        errcode::ErrSharedLockLost as isize,
        "kv::ERR_SHARED_LOCK_LOST"
    );
    checked += 1;
    assert_eq!(
        catalog::kv::ERR_LOCK_EXPIRE.class(),
        TerrorClass::TiKv,
        "kv::ERR_LOCK_EXPIRE"
    );
    assert_eq!(
        catalog::kv::ERR_LOCK_EXPIRE.code().value(),
        errcode::ErrLockExpire as isize,
        "kv::ERR_LOCK_EXPIRE"
    );
    checked += 1;
    assert_eq!(
        catalog::kv::ERR_ASSERTION_FAILED.class(),
        TerrorClass::TiKv,
        "kv::ERR_ASSERTION_FAILED"
    );
    assert_eq!(
        catalog::kv::ERR_ASSERTION_FAILED.code().value(),
        errcode::ErrAssertionFailed as isize,
        "kv::ERR_ASSERTION_FAILED"
    );
    checked += 1;
    assert_eq!(
        catalog::memory::ERR_MEM_EXCEED_THRESHOLD.class(),
        TerrorClass::Util,
        "memory::ERR_MEM_EXCEED_THRESHOLD"
    );
    assert_eq!(
        catalog::memory::ERR_MEM_EXCEED_THRESHOLD.code().value(),
        errcode::ErrMemExceedThreshold as isize,
        "memory::ERR_MEM_EXCEED_THRESHOLD"
    );
    checked += 1;
    assert_eq!(
        catalog::meta::ERR_DB_EXISTS.class(),
        TerrorClass::Meta,
        "meta::ERR_DB_EXISTS"
    );
    assert_eq!(
        catalog::meta::ERR_DB_EXISTS.code().value(),
        errcode::ErrDBCreateExists as isize,
        "meta::ERR_DB_EXISTS"
    );
    checked += 1;
    assert_eq!(
        catalog::meta::ERR_DB_NOT_EXISTS.class(),
        TerrorClass::Meta,
        "meta::ERR_DB_NOT_EXISTS"
    );
    assert_eq!(
        catalog::meta::ERR_DB_NOT_EXISTS.code().value(),
        errcode::ErrBadDB as isize,
        "meta::ERR_DB_NOT_EXISTS"
    );
    checked += 1;
    assert_eq!(
        catalog::meta::ERR_POLICY_EXISTS.class(),
        TerrorClass::Meta,
        "meta::ERR_POLICY_EXISTS"
    );
    assert_eq!(
        catalog::meta::ERR_POLICY_EXISTS.code().value(),
        errcode::ErrPlacementPolicyExists as isize,
        "meta::ERR_POLICY_EXISTS"
    );
    checked += 1;
    assert_eq!(
        catalog::meta::ERR_POLICY_NOT_EXISTS.class(),
        TerrorClass::Meta,
        "meta::ERR_POLICY_NOT_EXISTS"
    );
    assert_eq!(
        catalog::meta::ERR_POLICY_NOT_EXISTS.code().value(),
        errcode::ErrPlacementPolicyNotExists as isize,
        "meta::ERR_POLICY_NOT_EXISTS"
    );
    checked += 1;
    assert_eq!(
        catalog::meta::ERR_MASKING_POLICY_EXISTS.class(),
        TerrorClass::Meta,
        "meta::ERR_MASKING_POLICY_EXISTS"
    );
    assert_eq!(
        catalog::meta::ERR_MASKING_POLICY_EXISTS.code().value(),
        errcode::ErrMaskingPolicyExists as isize,
        "meta::ERR_MASKING_POLICY_EXISTS"
    );
    checked += 1;
    assert_eq!(
        catalog::meta::ERR_MASKING_POLICY_NOT_EXISTS.class(),
        TerrorClass::Meta,
        "meta::ERR_MASKING_POLICY_NOT_EXISTS"
    );
    assert_eq!(
        catalog::meta::ERR_MASKING_POLICY_NOT_EXISTS.code().value(),
        errcode::ErrMaskingPolicyNotExists as isize,
        "meta::ERR_MASKING_POLICY_NOT_EXISTS"
    );
    checked += 1;
    assert_eq!(
        catalog::meta::ERR_MASKING_POLICY_EXPR_INVALID_COLUMN.class(),
        TerrorClass::Meta,
        "meta::ERR_MASKING_POLICY_EXPR_INVALID_COLUMN"
    );
    assert_eq!(
        catalog::meta::ERR_MASKING_POLICY_EXPR_INVALID_COLUMN
            .code()
            .value(),
        errcode::ErrMaskingPolicyExprInvalidColumn as isize,
        "meta::ERR_MASKING_POLICY_EXPR_INVALID_COLUMN"
    );
    checked += 1;
    assert_eq!(
        catalog::meta::ERR_RESOURCE_GROUP_EXISTS.class(),
        TerrorClass::Meta,
        "meta::ERR_RESOURCE_GROUP_EXISTS"
    );
    assert_eq!(
        catalog::meta::ERR_RESOURCE_GROUP_EXISTS.code().value(),
        errcode::ErrResourceGroupExists as isize,
        "meta::ERR_RESOURCE_GROUP_EXISTS"
    );
    checked += 1;
    assert_eq!(
        catalog::meta::ERR_RESOURCE_GROUP_NOT_EXISTS.class(),
        TerrorClass::Meta,
        "meta::ERR_RESOURCE_GROUP_NOT_EXISTS"
    );
    assert_eq!(
        catalog::meta::ERR_RESOURCE_GROUP_NOT_EXISTS.code().value(),
        errcode::ErrResourceGroupNotExists as isize,
        "meta::ERR_RESOURCE_GROUP_NOT_EXISTS"
    );
    checked += 1;
    assert_eq!(
        catalog::meta::ERR_TABLE_EXISTS.class(),
        TerrorClass::Meta,
        "meta::ERR_TABLE_EXISTS"
    );
    assert_eq!(
        catalog::meta::ERR_TABLE_EXISTS.code().value(),
        errcode::ErrTableExists as isize,
        "meta::ERR_TABLE_EXISTS"
    );
    checked += 1;
    assert_eq!(
        catalog::meta::ERR_TABLE_NOT_EXISTS.class(),
        TerrorClass::Meta,
        "meta::ERR_TABLE_NOT_EXISTS"
    );
    assert_eq!(
        catalog::meta::ERR_TABLE_NOT_EXISTS.code().value(),
        errcode::ErrNoSuchTable as isize,
        "meta::ERR_TABLE_NOT_EXISTS"
    );
    checked += 1;
    assert_eq!(
        catalog::meta::ERR_DDL_REORG_ELEMENT_NOT_EXIST.class(),
        TerrorClass::Meta,
        "meta::ERR_DDL_REORG_ELEMENT_NOT_EXIST"
    );
    assert_eq!(
        catalog::meta::ERR_DDL_REORG_ELEMENT_NOT_EXIST
            .code()
            .value(),
        errcode::ErrDDLReorgElementNotExist as isize,
        "meta::ERR_DDL_REORG_ELEMENT_NOT_EXIST"
    );
    checked += 1;
    assert_eq!(
        catalog::meta::ERR_INVALID_STRING.class(),
        TerrorClass::Meta,
        "meta::ERR_INVALID_STRING"
    );
    assert_eq!(
        catalog::meta::ERR_INVALID_STRING.code().value(),
        errcode::ErrInvalidCharacterString as isize,
        "meta::ERR_INVALID_STRING"
    );
    checked += 1;
    assert_eq!(
        catalog::param::ERR_UNKNOWN_FIELD_TYPE.class(),
        TerrorClass::Server,
        "param::ERR_UNKNOWN_FIELD_TYPE"
    );
    assert_eq!(
        catalog::param::ERR_UNKNOWN_FIELD_TYPE.code().value(),
        errcode::ErrUnknownFieldType as isize,
        "param::ERR_UNKNOWN_FIELD_TYPE"
    );
    checked += 1;
    assert_eq!(
        catalog::parser::ERR_WARN_OPTIMIZER_HINT_UNSUPPORTED_HINT.class(),
        TerrorClass::Parser,
        "parser::ERR_WARN_OPTIMIZER_HINT_UNSUPPORTED_HINT"
    );
    assert_eq!(
        catalog::parser::ERR_WARN_OPTIMIZER_HINT_UNSUPPORTED_HINT
            .code()
            .value(),
        errcode::ErrWarnOptimizerHintUnsupportedHint as isize,
        "parser::ERR_WARN_OPTIMIZER_HINT_UNSUPPORTED_HINT"
    );
    checked += 1;
    assert_eq!(
        catalog::parser::ERR_WARN_OPTIMIZER_HINT_INVALID_TOKEN.class(),
        TerrorClass::Parser,
        "parser::ERR_WARN_OPTIMIZER_HINT_INVALID_TOKEN"
    );
    assert_eq!(
        catalog::parser::ERR_WARN_OPTIMIZER_HINT_INVALID_TOKEN
            .code()
            .value(),
        errcode::ErrWarnOptimizerHintInvalidToken as isize,
        "parser::ERR_WARN_OPTIMIZER_HINT_INVALID_TOKEN"
    );
    checked += 1;
    assert_eq!(
        catalog::parser::ERR_WARN_MEMORY_QUOTA_OVERFLOW.class(),
        TerrorClass::Parser,
        "parser::ERR_WARN_MEMORY_QUOTA_OVERFLOW"
    );
    assert_eq!(
        catalog::parser::ERR_WARN_MEMORY_QUOTA_OVERFLOW
            .code()
            .value(),
        errcode::ErrWarnMemoryQuotaOverflow as isize,
        "parser::ERR_WARN_MEMORY_QUOTA_OVERFLOW"
    );
    checked += 1;
    assert_eq!(
        catalog::parser::ERR_WARN_OPTIMIZER_HINT_PARSE_ERROR.class(),
        TerrorClass::Parser,
        "parser::ERR_WARN_OPTIMIZER_HINT_PARSE_ERROR"
    );
    assert_eq!(
        catalog::parser::ERR_WARN_OPTIMIZER_HINT_PARSE_ERROR
            .code()
            .value(),
        errcode::ErrWarnOptimizerHintParseError as isize,
        "parser::ERR_WARN_OPTIMIZER_HINT_PARSE_ERROR"
    );
    checked += 1;
    assert_eq!(
        catalog::parser::ERR_WARN_OPTIMIZER_HINT_INVALID_INTEGER.class(),
        TerrorClass::Parser,
        "parser::ERR_WARN_OPTIMIZER_HINT_INVALID_INTEGER"
    );
    assert_eq!(
        catalog::parser::ERR_WARN_OPTIMIZER_HINT_INVALID_INTEGER
            .code()
            .value(),
        errcode::ErrWarnOptimizerHintInvalidInteger as isize,
        "parser::ERR_WARN_OPTIMIZER_HINT_INVALID_INTEGER"
    );
    checked += 1;
    assert_eq!(
        catalog::parser::ERR_WARN_OPTIMIZER_HINT_WRONG_POS.class(),
        TerrorClass::Parser,
        "parser::ERR_WARN_OPTIMIZER_HINT_WRONG_POS"
    );
    assert_eq!(
        catalog::parser::ERR_WARN_OPTIMIZER_HINT_WRONG_POS
            .code()
            .value(),
        errcode::ErrWarnOptimizerHintWrongPos as isize,
        "parser::ERR_WARN_OPTIMIZER_HINT_WRONG_POS"
    );
    checked += 1;
    assert_eq!(
        catalog::parser::ERR_SYNTAX.class(),
        TerrorClass::Parser,
        "parser::ERR_SYNTAX"
    );
    assert_eq!(
        catalog::parser::ERR_SYNTAX.code().value(),
        errcode::ErrSyntax as isize,
        "parser::ERR_SYNTAX"
    );
    checked += 1;
    assert_eq!(
        catalog::parser::ERR_PARSE.class(),
        TerrorClass::Parser,
        "parser::ERR_PARSE"
    );
    assert_eq!(
        catalog::parser::ERR_PARSE.code().value(),
        errcode::ErrParse as isize,
        "parser::ERR_PARSE"
    );
    checked += 1;
    assert_eq!(
        catalog::parser::ERR_UNKNOWN_CHARACTER_SET.class(),
        TerrorClass::Parser,
        "parser::ERR_UNKNOWN_CHARACTER_SET"
    );
    assert_eq!(
        catalog::parser::ERR_UNKNOWN_CHARACTER_SET.code().value(),
        errcode::ErrUnknownCharacterSet as isize,
        "parser::ERR_UNKNOWN_CHARACTER_SET"
    );
    checked += 1;
    assert_eq!(
        catalog::parser::ERR_INVALID_YEAR_COLUMN_LENGTH.class(),
        TerrorClass::Parser,
        "parser::ERR_INVALID_YEAR_COLUMN_LENGTH"
    );
    assert_eq!(
        catalog::parser::ERR_INVALID_YEAR_COLUMN_LENGTH
            .code()
            .value(),
        errcode::ErrInvalidYearColumnLength as isize,
        "parser::ERR_INVALID_YEAR_COLUMN_LENGTH"
    );
    checked += 1;
    assert_eq!(
        catalog::parser::ERR_WRONG_ARGUMENTS.class(),
        TerrorClass::Parser,
        "parser::ERR_WRONG_ARGUMENTS"
    );
    assert_eq!(
        catalog::parser::ERR_WRONG_ARGUMENTS.code().value(),
        errcode::ErrWrongArguments as isize,
        "parser::ERR_WRONG_ARGUMENTS"
    );
    checked += 1;
    assert_eq!(
        catalog::parser::ERR_WRONG_FIELD_TERMINATORS.class(),
        TerrorClass::Parser,
        "parser::ERR_WRONG_FIELD_TERMINATORS"
    );
    assert_eq!(
        catalog::parser::ERR_WRONG_FIELD_TERMINATORS.code().value(),
        errcode::ErrWrongFieldTerminators as isize,
        "parser::ERR_WRONG_FIELD_TERMINATORS"
    );
    checked += 1;
    assert_eq!(
        catalog::parser::ERR_TOO_BIG_DISPLAY_WIDTH.class(),
        TerrorClass::Parser,
        "parser::ERR_TOO_BIG_DISPLAY_WIDTH"
    );
    assert_eq!(
        catalog::parser::ERR_TOO_BIG_DISPLAY_WIDTH.code().value(),
        errcode::ErrTooBigDisplaywidth as isize,
        "parser::ERR_TOO_BIG_DISPLAY_WIDTH"
    );
    checked += 1;
    assert_eq!(
        catalog::parser::ERR_TOO_BIG_PRECISION.class(),
        TerrorClass::Parser,
        "parser::ERR_TOO_BIG_PRECISION"
    );
    assert_eq!(
        catalog::parser::ERR_TOO_BIG_PRECISION.code().value(),
        errcode::ErrTooBigPrecision as isize,
        "parser::ERR_TOO_BIG_PRECISION"
    );
    checked += 1;
    assert_eq!(
        catalog::parser::ERR_UNKNOWN_ALTER_LOCK.class(),
        TerrorClass::Parser,
        "parser::ERR_UNKNOWN_ALTER_LOCK"
    );
    assert_eq!(
        catalog::parser::ERR_UNKNOWN_ALTER_LOCK.code().value(),
        errcode::ErrUnknownAlterLock as isize,
        "parser::ERR_UNKNOWN_ALTER_LOCK"
    );
    checked += 1;
    assert_eq!(
        catalog::parser::ERR_UNKNOWN_ALTER_ALGORITHM.class(),
        TerrorClass::Parser,
        "parser::ERR_UNKNOWN_ALTER_ALGORITHM"
    );
    assert_eq!(
        catalog::parser::ERR_UNKNOWN_ALTER_ALGORITHM.code().value(),
        errcode::ErrUnknownAlterAlgorithm as isize,
        "parser::ERR_UNKNOWN_ALTER_ALGORITHM"
    );
    checked += 1;
    assert_eq!(
        catalog::parser::ERR_NULL_IN_VALUES_LESS_THAN.class(),
        TerrorClass::Parser,
        "parser::ERR_NULL_IN_VALUES_LESS_THAN"
    );
    assert_eq!(
        catalog::parser::ERR_NULL_IN_VALUES_LESS_THAN.code().value(),
        errcode::ErrNullInValuesLessThan as isize,
        "parser::ERR_NULL_IN_VALUES_LESS_THAN"
    );
    checked += 1;
    assert_eq!(
        catalog::parser::ERR_WRONG_VALUE.class(),
        TerrorClass::Parser,
        "parser::ERR_WRONG_VALUE"
    );
    assert_eq!(
        catalog::parser::ERR_WRONG_VALUE.code().value(),
        errcode::ErrWrongValue as isize,
        "parser::ERR_WRONG_VALUE"
    );
    checked += 1;
    assert_eq!(
        catalog::parser::ERR_WARN_DEPRECATED_SYNTAX.class(),
        TerrorClass::Parser,
        "parser::ERR_WARN_DEPRECATED_SYNTAX"
    );
    assert_eq!(
        catalog::parser::ERR_WARN_DEPRECATED_SYNTAX.code().value(),
        errcode::ErrWarnDeprecatedSyntax as isize,
        "parser::ERR_WARN_DEPRECATED_SYNTAX"
    );
    checked += 1;
    assert_eq!(
        catalog::parser::ERR_WARN_DEPRECATED_SYNTAX_NO_REPLACEMENT.class(),
        TerrorClass::Parser,
        "parser::ERR_WARN_DEPRECATED_SYNTAX_NO_REPLACEMENT"
    );
    assert_eq!(
        catalog::parser::ERR_WARN_DEPRECATED_SYNTAX_NO_REPLACEMENT
            .code()
            .value(),
        errcode::ErrWarnDeprecatedSyntaxNoReplacement as isize,
        "parser::ERR_WARN_DEPRECATED_SYNTAX_NO_REPLACEMENT"
    );
    checked += 1;
    assert_eq!(
        catalog::parser::ERR_WRONG_USAGE.class(),
        TerrorClass::Parser,
        "parser::ERR_WRONG_USAGE"
    );
    assert_eq!(
        catalog::parser::ERR_WRONG_USAGE.code().value(),
        errcode::ErrWrongUsage as isize,
        "parser::ERR_WRONG_USAGE"
    );
    checked += 1;
    assert_eq!(
        catalog::parser::ERR_WRONG_DB_NAME.class(),
        TerrorClass::Parser,
        "parser::ERR_WRONG_DB_NAME"
    );
    assert_eq!(
        catalog::parser::ERR_WRONG_DB_NAME.code().value(),
        errcode::ErrWrongDBName as isize,
        "parser::ERR_WRONG_DB_NAME"
    );
    checked += 1;
    assert_eq!(
        catalog::parser_types::ERR_INVALID_DEFAULT.class(),
        TerrorClass::Types,
        "parser_types::ERR_INVALID_DEFAULT"
    );
    assert_eq!(
        catalog::parser_types::ERR_INVALID_DEFAULT.code().value(),
        errcode::ErrInvalidDefault as isize,
        "parser_types::ERR_INVALID_DEFAULT"
    );
    checked += 1;
    assert_eq!(
        catalog::parser_types::ERR_DATA_OUT_OF_RANGE.class(),
        TerrorClass::Types,
        "parser_types::ERR_DATA_OUT_OF_RANGE"
    );
    assert_eq!(
        catalog::parser_types::ERR_DATA_OUT_OF_RANGE.code().value(),
        errcode::ErrDataOutOfRange as isize,
        "parser_types::ERR_DATA_OUT_OF_RANGE"
    );
    checked += 1;
    assert_eq!(
        catalog::parser_types::ERR_TRUNCATED_WRONG_VALUE.class(),
        TerrorClass::Types,
        "parser_types::ERR_TRUNCATED_WRONG_VALUE"
    );
    assert_eq!(
        catalog::parser_types::ERR_TRUNCATED_WRONG_VALUE
            .code()
            .value(),
        errcode::ErrTruncatedWrongValue as isize,
        "parser_types::ERR_TRUNCATED_WRONG_VALUE"
    );
    checked += 1;
    assert_eq!(
        catalog::parser_types::ERR_ILLEGAL_VALUE_FOR_TYPE.class(),
        TerrorClass::Types,
        "parser_types::ERR_ILLEGAL_VALUE_FOR_TYPE"
    );
    assert_eq!(
        catalog::parser_types::ERR_ILLEGAL_VALUE_FOR_TYPE
            .code()
            .value(),
        errcode::ErrIllegalValueForType as isize,
        "parser_types::ERR_ILLEGAL_VALUE_FOR_TYPE"
    );
    checked += 1;
    assert_eq!(
        catalog::plugin::ERR_INVALID_PLUGIN_ID.class(),
        TerrorClass::Plugin,
        "plugin::ERR_INVALID_PLUGIN_ID"
    );
    assert_eq!(
        catalog::plugin::ERR_INVALID_PLUGIN_ID.code().value(),
        errcode::ErrInvalidPluginID as isize,
        "plugin::ERR_INVALID_PLUGIN_ID"
    );
    checked += 1;
    assert_eq!(
        catalog::plugin::ERR_INVALID_PLUGIN_MANIFEST.class(),
        TerrorClass::Plugin,
        "plugin::ERR_INVALID_PLUGIN_MANIFEST"
    );
    assert_eq!(
        catalog::plugin::ERR_INVALID_PLUGIN_MANIFEST.code().value(),
        errcode::ErrInvalidPluginManifest as isize,
        "plugin::ERR_INVALID_PLUGIN_MANIFEST"
    );
    checked += 1;
    assert_eq!(
        catalog::plugin::ERR_INVALID_PLUGIN_NAME.class(),
        TerrorClass::Plugin,
        "plugin::ERR_INVALID_PLUGIN_NAME"
    );
    assert_eq!(
        catalog::plugin::ERR_INVALID_PLUGIN_NAME.code().value(),
        errcode::ErrInvalidPluginName as isize,
        "plugin::ERR_INVALID_PLUGIN_NAME"
    );
    checked += 1;
    assert_eq!(
        catalog::plugin::ERR_INVALID_PLUGIN_VERSION.class(),
        TerrorClass::Plugin,
        "plugin::ERR_INVALID_PLUGIN_VERSION"
    );
    assert_eq!(
        catalog::plugin::ERR_INVALID_PLUGIN_VERSION.code().value(),
        errcode::ErrInvalidPluginVersion as isize,
        "plugin::ERR_INVALID_PLUGIN_VERSION"
    );
    checked += 1;
    assert_eq!(
        catalog::plugin::ERR_DUPLICATE_PLUGIN.class(),
        TerrorClass::Plugin,
        "plugin::ERR_DUPLICATE_PLUGIN"
    );
    assert_eq!(
        catalog::plugin::ERR_DUPLICATE_PLUGIN.code().value(),
        errcode::ErrDuplicatePlugin as isize,
        "plugin::ERR_DUPLICATE_PLUGIN"
    );
    checked += 1;
    assert_eq!(
        catalog::plugin::ERR_REQUIRE_VERSION_CHECK_FAIL.class(),
        TerrorClass::Plugin,
        "plugin::ERR_REQUIRE_VERSION_CHECK_FAIL"
    );
    assert_eq!(
        catalog::plugin::ERR_REQUIRE_VERSION_CHECK_FAIL
            .code()
            .value(),
        errcode::ErrRequireVersionCheckFail as isize,
        "plugin::ERR_REQUIRE_VERSION_CHECK_FAIL"
    );
    checked += 1;
    assert_eq!(
        catalog::privileges::ERR_INVALID_PRIVILEGE_TYPE.class(),
        TerrorClass::Privilege,
        "privileges::ERR_INVALID_PRIVILEGE_TYPE"
    );
    assert_eq!(
        catalog::privileges::ERR_INVALID_PRIVILEGE_TYPE
            .code()
            .value(),
        errcode::ErrInvalidPrivilegeType as isize,
        "privileges::ERR_INVALID_PRIVILEGE_TYPE"
    );
    checked += 1;
    assert_eq!(
        catalog::privileges::ERR_NONEXISTING_GRANT.class(),
        TerrorClass::Privilege,
        "privileges::ERR_NONEXISTING_GRANT"
    );
    assert_eq!(
        catalog::privileges::ERR_NONEXISTING_GRANT.code().value(),
        errcode::ErrNonexistingGrant as isize,
        "privileges::ERR_NONEXISTING_GRANT"
    );
    checked += 1;
    assert_eq!(
        catalog::privileges::ERR_LOAD_PRIVILEGE.class(),
        TerrorClass::Privilege,
        "privileges::ERR_LOAD_PRIVILEGE"
    );
    assert_eq!(
        catalog::privileges::ERR_LOAD_PRIVILEGE.code().value(),
        errcode::ErrLoadPrivilege as isize,
        "privileges::ERR_LOAD_PRIVILEGE"
    );
    checked += 1;
    assert_eq!(
        catalog::privileges::ERR_ACCESS_DENIED.class(),
        TerrorClass::Privilege,
        "privileges::ERR_ACCESS_DENIED"
    );
    assert_eq!(
        catalog::privileges::ERR_ACCESS_DENIED.code().value(),
        errcode::ErrAccessDenied as isize,
        "privileges::ERR_ACCESS_DENIED"
    );
    checked += 1;
    assert_eq!(
        catalog::privileges::ERR_ACCOUNT_HAS_BEEN_LOCKED.class(),
        TerrorClass::Privilege,
        "privileges::ERR_ACCOUNT_HAS_BEEN_LOCKED"
    );
    assert_eq!(
        catalog::privileges::ERR_ACCOUNT_HAS_BEEN_LOCKED
            .code()
            .value(),
        errcode::ErrAccountHasBeenLocked as isize,
        "privileges::ERR_ACCOUNT_HAS_BEEN_LOCKED"
    );
    checked += 1;
    assert_eq!(
        catalog::privileges::ER_USER_ACCESS_DENIED_FOR_USER_ACCOUNT_BLOCKED_BY_PASSWORD_LOCK
            .class(),
        TerrorClass::Privilege,
        "privileges::ER_USER_ACCESS_DENIED_FOR_USER_ACCOUNT_BLOCKED_BY_PASSWORD_LOCK"
    );
    assert_eq!(
        catalog::privileges::ER_USER_ACCESS_DENIED_FOR_USER_ACCOUNT_BLOCKED_BY_PASSWORD_LOCK
            .code()
            .value(),
        errcode::ErUserAccessDeniedForUserAccountBlockedByPasswordLock as isize,
        "privileges::ER_USER_ACCESS_DENIED_FOR_USER_ACCOUNT_BLOCKED_BY_PASSWORD_LOCK"
    );
    checked += 1;
    assert_eq!(
        catalog::privileges::ERR_MUST_CHANGE_PASSWORD_LOGIN.class(),
        TerrorClass::Privilege,
        "privileges::ERR_MUST_CHANGE_PASSWORD_LOGIN"
    );
    assert_eq!(
        catalog::privileges::ERR_MUST_CHANGE_PASSWORD_LOGIN
            .code()
            .value(),
        errcode::ErrMustChangePasswordLogin as isize,
        "privileges::ERR_MUST_CHANGE_PASSWORD_LOGIN"
    );
    checked += 1;
    assert_eq!(
        catalog::server::ERR_UNKNOWN_FIELD_TYPE.class(),
        TerrorClass::Server,
        "server::ERR_UNKNOWN_FIELD_TYPE"
    );
    assert_eq!(
        catalog::server::ERR_UNKNOWN_FIELD_TYPE.code().value(),
        errcode::ErrUnknownFieldType as isize,
        "server::ERR_UNKNOWN_FIELD_TYPE"
    );
    checked += 1;
    assert_eq!(
        catalog::servererr::ERR_INVALID_TYPE.class(),
        TerrorClass::Server,
        "servererr::ERR_INVALID_TYPE"
    );
    assert_eq!(
        catalog::servererr::ERR_INVALID_TYPE.code().value(),
        errcode::ErrInvalidType as isize,
        "servererr::ERR_INVALID_TYPE"
    );
    checked += 1;
    assert_eq!(
        catalog::servererr::ERR_INVALID_SEQUENCE.class(),
        TerrorClass::Server,
        "servererr::ERR_INVALID_SEQUENCE"
    );
    assert_eq!(
        catalog::servererr::ERR_INVALID_SEQUENCE.code().value(),
        errcode::ErrInvalidSequence as isize,
        "servererr::ERR_INVALID_SEQUENCE"
    );
    checked += 1;
    assert_eq!(
        catalog::servererr::ERR_NOT_ALLOWED_COMMAND.class(),
        TerrorClass::Server,
        "servererr::ERR_NOT_ALLOWED_COMMAND"
    );
    assert_eq!(
        catalog::servererr::ERR_NOT_ALLOWED_COMMAND.code().value(),
        errcode::ErrNotAllowedCommand as isize,
        "servererr::ERR_NOT_ALLOWED_COMMAND"
    );
    checked += 1;
    assert_eq!(
        catalog::servererr::ERR_ACCESS_DENIED.class(),
        TerrorClass::Server,
        "servererr::ERR_ACCESS_DENIED"
    );
    assert_eq!(
        catalog::servererr::ERR_ACCESS_DENIED.code().value(),
        errcode::ErrAccessDenied as isize,
        "servererr::ERR_ACCESS_DENIED"
    );
    checked += 1;
    assert_eq!(
        catalog::servererr::ERR_ACCESS_DENIED_NO_PASSWORD.class(),
        TerrorClass::Server,
        "servererr::ERR_ACCESS_DENIED_NO_PASSWORD"
    );
    assert_eq!(
        catalog::servererr::ERR_ACCESS_DENIED_NO_PASSWORD
            .code()
            .value(),
        errcode::ErrAccessDeniedNoPassword as isize,
        "servererr::ERR_ACCESS_DENIED_NO_PASSWORD"
    );
    checked += 1;
    assert_eq!(
        catalog::servererr::ERR_CON_COUNT.class(),
        TerrorClass::Server,
        "servererr::ERR_CON_COUNT"
    );
    assert_eq!(
        catalog::servererr::ERR_CON_COUNT.code().value(),
        errcode::ErrConCount as isize,
        "servererr::ERR_CON_COUNT"
    );
    checked += 1;
    assert_eq!(
        catalog::servererr::ERR_TOO_MANY_USER_CONNECTIONS.class(),
        TerrorClass::Server,
        "servererr::ERR_TOO_MANY_USER_CONNECTIONS"
    );
    assert_eq!(
        catalog::servererr::ERR_TOO_MANY_USER_CONNECTIONS
            .code()
            .value(),
        errcode::ErrTooManyUserConnections as isize,
        "servererr::ERR_TOO_MANY_USER_CONNECTIONS"
    );
    checked += 1;
    assert_eq!(
        catalog::servererr::ERR_SECURE_TRANSPORT_REQUIRED.class(),
        TerrorClass::Server,
        "servererr::ERR_SECURE_TRANSPORT_REQUIRED"
    );
    assert_eq!(
        catalog::servererr::ERR_SECURE_TRANSPORT_REQUIRED
            .code()
            .value(),
        errcode::ErrSecureTransportRequired as isize,
        "servererr::ERR_SECURE_TRANSPORT_REQUIRED"
    );
    checked += 1;
    assert_eq!(
        catalog::servererr::ERR_USER_PREFIX_MISMATCH.class(),
        TerrorClass::Server,
        "servererr::ERR_USER_PREFIX_MISMATCH"
    );
    assert_eq!(
        catalog::servererr::ERR_USER_PREFIX_MISMATCH.code().value(),
        errcode::ErrUserPrefixMismatch as isize,
        "servererr::ERR_USER_PREFIX_MISMATCH"
    );
    checked += 1;
    assert_eq!(
        catalog::servererr::ERR_MULTI_STATEMENT_DISABLED.class(),
        TerrorClass::Server,
        "servererr::ERR_MULTI_STATEMENT_DISABLED"
    );
    assert_eq!(
        catalog::servererr::ERR_MULTI_STATEMENT_DISABLED
            .code()
            .value(),
        errcode::ErrMultiStatementDisabled as isize,
        "servererr::ERR_MULTI_STATEMENT_DISABLED"
    );
    checked += 1;
    assert_eq!(
        catalog::servererr::ERR_NEW_ABORTING_CONNECTION.class(),
        TerrorClass::Server,
        "servererr::ERR_NEW_ABORTING_CONNECTION"
    );
    assert_eq!(
        catalog::servererr::ERR_NEW_ABORTING_CONNECTION
            .code()
            .value(),
        errcode::ErrNewAbortingConnection as isize,
        "servererr::ERR_NEW_ABORTING_CONNECTION"
    );
    checked += 1;
    assert_eq!(
        catalog::servererr::ERR_NOT_SUPPORTED_AUTH_MODE.class(),
        TerrorClass::Server,
        "servererr::ERR_NOT_SUPPORTED_AUTH_MODE"
    );
    assert_eq!(
        catalog::servererr::ERR_NOT_SUPPORTED_AUTH_MODE
            .code()
            .value(),
        errcode::ErrNotSupportedAuthMode as isize,
        "servererr::ERR_NOT_SUPPORTED_AUTH_MODE"
    );
    checked += 1;
    assert_eq!(
        catalog::servererr::ERR_NET_PACKET_TOO_LARGE.class(),
        TerrorClass::Server,
        "servererr::ERR_NET_PACKET_TOO_LARGE"
    );
    assert_eq!(
        catalog::servererr::ERR_NET_PACKET_TOO_LARGE.code().value(),
        errcode::ErrNetPacketTooLarge as isize,
        "servererr::ERR_NET_PACKET_TOO_LARGE"
    );
    checked += 1;
    assert_eq!(
        catalog::servererr::ERR_MUST_CHANGE_PASSWORD.class(),
        TerrorClass::Server,
        "servererr::ERR_MUST_CHANGE_PASSWORD"
    );
    assert_eq!(
        catalog::servererr::ERR_MUST_CHANGE_PASSWORD.code().value(),
        errcode::ErrMustChangePassword as isize,
        "servererr::ERR_MUST_CHANGE_PASSWORD"
    );
    checked += 1;
    assert_eq!(
        catalog::servererr::ERR_SERVER_SHUTDOWN.class(),
        TerrorClass::Server,
        "servererr::ERR_SERVER_SHUTDOWN"
    );
    assert_eq!(
        catalog::servererr::ERR_SERVER_SHUTDOWN.code().value(),
        errcode::ErrServerShutdown as isize,
        "servererr::ERR_SERVER_SHUTDOWN"
    );
    checked += 1;
    assert_eq!(
        catalog::session::ERR_FOR_UPDATE_CANT_RETRY.class(),
        TerrorClass::Session,
        "session::ERR_FOR_UPDATE_CANT_RETRY"
    );
    assert_eq!(
        catalog::session::ERR_FOR_UPDATE_CANT_RETRY.code().value(),
        errcode::ErrForUpdateCantRetry as isize,
        "session::ERR_FOR_UPDATE_CANT_RETRY"
    );
    checked += 1;
    assert_eq!(
        catalog::session::ERR_NON_TRANSACTIONAL_JOB_FAILURE.class(),
        TerrorClass::Session,
        "session::ERR_NON_TRANSACTIONAL_JOB_FAILURE"
    );
    assert_eq!(
        catalog::session::ERR_NON_TRANSACTIONAL_JOB_FAILURE
            .code()
            .value(),
        errcode::ErrNonTransactionalJobFailure as isize,
        "session::ERR_NON_TRANSACTIONAL_JOB_FAILURE"
    );
    checked += 1;
    assert_eq!(
        catalog::session::ERR_RESULT_IS_EMPTY.class(),
        TerrorClass::Executor,
        "session::ERR_RESULT_IS_EMPTY"
    );
    assert_eq!(
        catalog::session::ERR_RESULT_IS_EMPTY.code().value(),
        errcode::ErrResultIsEmpty as isize,
        "session::ERR_RESULT_IS_EMPTY"
    );
    checked += 1;
    assert_eq!(
        catalog::sessionstates::ERR_CANNOT_MIGRATE_SESSION.class(),
        TerrorClass::Session,
        "sessionstates::ERR_CANNOT_MIGRATE_SESSION"
    );
    assert_eq!(
        catalog::sessionstates::ERR_CANNOT_MIGRATE_SESSION
            .code()
            .value(),
        errcode::ErrCannotMigrateSession as isize,
        "sessionstates::ERR_CANNOT_MIGRATE_SESSION"
    );
    checked += 1;
    assert_eq!(
        catalog::statistics::ERR_QUERY_INTERRUPTED.class(),
        TerrorClass::Executor,
        "statistics::ERR_QUERY_INTERRUPTED"
    );
    assert_eq!(
        catalog::statistics::ERR_QUERY_INTERRUPTED.code().value(),
        errcode::ErrQueryInterrupted as isize,
        "statistics::ERR_QUERY_INTERRUPTED"
    );
    checked += 1;
    assert_eq!(
        catalog::structure::ERR_INVALID_HASH_KEY_FLAG.class(),
        TerrorClass::Structure,
        "structure::ERR_INVALID_HASH_KEY_FLAG"
    );
    assert_eq!(
        catalog::structure::ERR_INVALID_HASH_KEY_FLAG.code().value(),
        errcode::ErrInvalidHashKeyFlag as isize,
        "structure::ERR_INVALID_HASH_KEY_FLAG"
    );
    checked += 1;
    assert_eq!(
        catalog::structure::ERR_INVALID_LIST_INDEX.class(),
        TerrorClass::Structure,
        "structure::ERR_INVALID_LIST_INDEX"
    );
    assert_eq!(
        catalog::structure::ERR_INVALID_LIST_INDEX.code().value(),
        errcode::ErrInvalidListIndex as isize,
        "structure::ERR_INVALID_LIST_INDEX"
    );
    checked += 1;
    assert_eq!(
        catalog::structure::ERR_INVALID_LIST_META_DATA.class(),
        TerrorClass::Structure,
        "structure::ERR_INVALID_LIST_META_DATA"
    );
    assert_eq!(
        catalog::structure::ERR_INVALID_LIST_META_DATA
            .code()
            .value(),
        errcode::ErrInvalidListMetaData as isize,
        "structure::ERR_INVALID_LIST_META_DATA"
    );
    checked += 1;
    assert_eq!(
        catalog::structure::ERR_WRITE_ON_SNAPSHOT.class(),
        TerrorClass::Structure,
        "structure::ERR_WRITE_ON_SNAPSHOT"
    );
    assert_eq!(
        catalog::structure::ERR_WRITE_ON_SNAPSHOT.code().value(),
        errcode::ErrWriteOnSnapshot as isize,
        "structure::ERR_WRITE_ON_SNAPSHOT"
    );
    checked += 1;
    assert_eq!(
        catalog::table::ERR_COLUMN_CANT_NULL.class(),
        TerrorClass::Table,
        "table::ERR_COLUMN_CANT_NULL"
    );
    assert_eq!(
        catalog::table::ERR_COLUMN_CANT_NULL.code().value(),
        errcode::ErrBadNull as isize,
        "table::ERR_COLUMN_CANT_NULL"
    );
    checked += 1;
    assert_eq!(
        catalog::table::ERR_UNKNOWN_COLUMN.class(),
        TerrorClass::Table,
        "table::ERR_UNKNOWN_COLUMN"
    );
    assert_eq!(
        catalog::table::ERR_UNKNOWN_COLUMN.code().value(),
        errcode::ErrBadField as isize,
        "table::ERR_UNKNOWN_COLUMN"
    );
    checked += 1;
    assert_eq!(
        catalog::table::ERR_DUPLICATE_COLUMN.class(),
        TerrorClass::Table,
        "table::ERR_DUPLICATE_COLUMN"
    );
    assert_eq!(
        catalog::table::ERR_DUPLICATE_COLUMN.code().value(),
        errcode::ErrFieldSpecifiedTwice as isize,
        "table::ERR_DUPLICATE_COLUMN"
    );
    checked += 1;
    assert_eq!(
        catalog::table::ERR_WARN_NULL_TO_NOTNULL.class(),
        TerrorClass::Executor,
        "table::ERR_WARN_NULL_TO_NOTNULL"
    );
    assert_eq!(
        catalog::table::ERR_WARN_NULL_TO_NOTNULL.code().value(),
        errcode::ErrWarnNullToNotnull as isize,
        "table::ERR_WARN_NULL_TO_NOTNULL"
    );
    checked += 1;
    assert_eq!(
        catalog::table::ERR_GET_DEFAULT_FAILED.class(),
        TerrorClass::Table,
        "table::ERR_GET_DEFAULT_FAILED"
    );
    assert_eq!(
        catalog::table::ERR_GET_DEFAULT_FAILED.code().value(),
        errcode::ErrFieldGetDefaultFailed as isize,
        "table::ERR_GET_DEFAULT_FAILED"
    );
    checked += 1;
    assert_eq!(
        catalog::table::ERR_NO_DEFAULT_VALUE.class(),
        TerrorClass::Table,
        "table::ERR_NO_DEFAULT_VALUE"
    );
    assert_eq!(
        catalog::table::ERR_NO_DEFAULT_VALUE.code().value(),
        errcode::ErrNoDefaultForField as isize,
        "table::ERR_NO_DEFAULT_VALUE"
    );
    checked += 1;
    assert_eq!(
        catalog::table::ERR_INDEX_OUT_BOUND.class(),
        TerrorClass::Table,
        "table::ERR_INDEX_OUT_BOUND"
    );
    assert_eq!(
        catalog::table::ERR_INDEX_OUT_BOUND.code().value(),
        errcode::ErrIndexOutBound as isize,
        "table::ERR_INDEX_OUT_BOUND"
    );
    checked += 1;
    assert_eq!(
        catalog::table::ERR_UNSUPPORTED_OP.class(),
        TerrorClass::Table,
        "table::ERR_UNSUPPORTED_OP"
    );
    assert_eq!(
        catalog::table::ERR_UNSUPPORTED_OP.code().value(),
        errcode::ErrUnsupportedOp as isize,
        "table::ERR_UNSUPPORTED_OP"
    );
    checked += 1;
    assert_eq!(
        catalog::table::ERR_ROW_NOT_FOUND.class(),
        TerrorClass::Table,
        "table::ERR_ROW_NOT_FOUND"
    );
    assert_eq!(
        catalog::table::ERR_ROW_NOT_FOUND.code().value(),
        errcode::ErrRowNotFound as isize,
        "table::ERR_ROW_NOT_FOUND"
    );
    checked += 1;
    assert_eq!(
        catalog::table::ERR_TABLE_STATE_CANT_NONE.class(),
        TerrorClass::Table,
        "table::ERR_TABLE_STATE_CANT_NONE"
    );
    assert_eq!(
        catalog::table::ERR_TABLE_STATE_CANT_NONE.code().value(),
        errcode::ErrTableStateCantNone as isize,
        "table::ERR_TABLE_STATE_CANT_NONE"
    );
    checked += 1;
    assert_eq!(
        catalog::table::ERR_COLUMN_STATE_CANT_NONE.class(),
        TerrorClass::Table,
        "table::ERR_COLUMN_STATE_CANT_NONE"
    );
    assert_eq!(
        catalog::table::ERR_COLUMN_STATE_CANT_NONE.code().value(),
        errcode::ErrColumnStateCantNone as isize,
        "table::ERR_COLUMN_STATE_CANT_NONE"
    );
    checked += 1;
    assert_eq!(
        catalog::table::ERR_COLUMN_STATE_NON_PUBLIC.class(),
        TerrorClass::Table,
        "table::ERR_COLUMN_STATE_NON_PUBLIC"
    );
    assert_eq!(
        catalog::table::ERR_COLUMN_STATE_NON_PUBLIC.code().value(),
        errcode::ErrColumnStateNonPublic as isize,
        "table::ERR_COLUMN_STATE_NON_PUBLIC"
    );
    checked += 1;
    assert_eq!(
        catalog::table::ERR_INDEX_STATE_CANT_NONE.class(),
        TerrorClass::Table,
        "table::ERR_INDEX_STATE_CANT_NONE"
    );
    assert_eq!(
        catalog::table::ERR_INDEX_STATE_CANT_NONE.code().value(),
        errcode::ErrIndexStateCantNone as isize,
        "table::ERR_INDEX_STATE_CANT_NONE"
    );
    checked += 1;
    assert_eq!(
        catalog::table::ERR_INVALID_RECORD_KEY.class(),
        TerrorClass::Table,
        "table::ERR_INVALID_RECORD_KEY"
    );
    assert_eq!(
        catalog::table::ERR_INVALID_RECORD_KEY.code().value(),
        errcode::ErrInvalidRecordKey as isize,
        "table::ERR_INVALID_RECORD_KEY"
    );
    checked += 1;
    assert_eq!(
        catalog::table::ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD.class(),
        TerrorClass::Table,
        "table::ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD"
    );
    assert_eq!(
        catalog::table::ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD
            .code()
            .value(),
        errcode::ErrTruncatedWrongValueForField as isize,
        "table::ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD"
    );
    checked += 1;
    assert_eq!(
        catalog::table::ERR_UNKNOWN_PARTITION.class(),
        TerrorClass::Table,
        "table::ERR_UNKNOWN_PARTITION"
    );
    assert_eq!(
        catalog::table::ERR_UNKNOWN_PARTITION.code().value(),
        errcode::ErrUnknownPartition as isize,
        "table::ERR_UNKNOWN_PARTITION"
    );
    checked += 1;
    assert_eq!(
        catalog::table::ERR_NO_PARTITION_FOR_GIVEN_VALUE.class(),
        TerrorClass::Table,
        "table::ERR_NO_PARTITION_FOR_GIVEN_VALUE"
    );
    assert_eq!(
        catalog::table::ERR_NO_PARTITION_FOR_GIVEN_VALUE
            .code()
            .value(),
        errcode::ErrNoPartitionForGivenValue as isize,
        "table::ERR_NO_PARTITION_FOR_GIVEN_VALUE"
    );
    checked += 1;
    assert_eq!(
        catalog::table::ERR_LOCK_OR_ACTIVE_TRANSACTION.class(),
        TerrorClass::Table,
        "table::ERR_LOCK_OR_ACTIVE_TRANSACTION"
    );
    assert_eq!(
        catalog::table::ERR_LOCK_OR_ACTIVE_TRANSACTION
            .code()
            .value(),
        errcode::ErrLockOrActiveTransaction as isize,
        "table::ERR_LOCK_OR_ACTIVE_TRANSACTION"
    );
    checked += 1;
    assert_eq!(
        catalog::table::ERR_SEQUENCE_HAS_RUN_OUT.class(),
        TerrorClass::Table,
        "table::ERR_SEQUENCE_HAS_RUN_OUT"
    );
    assert_eq!(
        catalog::table::ERR_SEQUENCE_HAS_RUN_OUT.code().value(),
        errcode::ErrSequenceRunOut as isize,
        "table::ERR_SEQUENCE_HAS_RUN_OUT"
    );
    checked += 1;
    assert_eq!(
        catalog::table::ERR_ROW_DOES_NOT_MATCH_GIVEN_PARTITION_SET.class(),
        TerrorClass::Table,
        "table::ERR_ROW_DOES_NOT_MATCH_GIVEN_PARTITION_SET"
    );
    assert_eq!(
        catalog::table::ERR_ROW_DOES_NOT_MATCH_GIVEN_PARTITION_SET
            .code()
            .value(),
        errcode::ErrRowDoesNotMatchGivenPartitionSet as isize,
        "table::ERR_ROW_DOES_NOT_MATCH_GIVEN_PARTITION_SET"
    );
    checked += 1;
    assert_eq!(
        catalog::table::ERR_TEMP_TABLE_FULL.class(),
        TerrorClass::Table,
        "table::ERR_TEMP_TABLE_FULL"
    );
    assert_eq!(
        catalog::table::ERR_TEMP_TABLE_FULL.code().value(),
        errcode::ErrRecordFileFull as isize,
        "table::ERR_TEMP_TABLE_FULL"
    );
    checked += 1;
    assert_eq!(
        catalog::table::ERR_OPT_ON_CACHE_TABLE.class(),
        TerrorClass::Ddl,
        "table::ERR_OPT_ON_CACHE_TABLE"
    );
    assert_eq!(
        catalog::table::ERR_OPT_ON_CACHE_TABLE.code().value(),
        errcode::ErrOptOnCacheTable as isize,
        "table::ERR_OPT_ON_CACHE_TABLE"
    );
    checked += 1;
    assert_eq!(
        catalog::table::ERR_CHECK_CONSTRAINT_VIOLATED.class(),
        TerrorClass::Table,
        "table::ERR_CHECK_CONSTRAINT_VIOLATED"
    );
    assert_eq!(
        catalog::table::ERR_CHECK_CONSTRAINT_VIOLATED.code().value(),
        errcode::ErrCheckConstraintViolated as isize,
        "table::ERR_CHECK_CONSTRAINT_VIOLATED"
    );
    checked += 1;
    assert_eq!(
        catalog::tablecodec::ERR_INVALID_KEY.class(),
        TerrorClass::XEval,
        "tablecodec::ERR_INVALID_KEY"
    );
    assert_eq!(
        catalog::tablecodec::ERR_INVALID_KEY.code().value(),
        errcode::ErrInvalidKey as isize,
        "tablecodec::ERR_INVALID_KEY"
    );
    checked += 1;
    assert_eq!(
        catalog::tablecodec::ERR_INVALID_RECORD_KEY.class(),
        TerrorClass::XEval,
        "tablecodec::ERR_INVALID_RECORD_KEY"
    );
    assert_eq!(
        catalog::tablecodec::ERR_INVALID_RECORD_KEY.code().value(),
        errcode::ErrInvalidRecordKey as isize,
        "tablecodec::ERR_INVALID_RECORD_KEY"
    );
    checked += 1;
    assert_eq!(
        catalog::tablecodec::ERR_INVALID_INDEX_KEY.class(),
        TerrorClass::XEval,
        "tablecodec::ERR_INVALID_INDEX_KEY"
    );
    assert_eq!(
        catalog::tablecodec::ERR_INVALID_INDEX_KEY.code().value(),
        errcode::ErrInvalidIndexKey as isize,
        "tablecodec::ERR_INVALID_INDEX_KEY"
    );
    checked += 1;
    assert_eq!(
        catalog::tables::ERR_INCONSISTENT_ROW_VALUE.class(),
        TerrorClass::Table,
        "tables::ERR_INCONSISTENT_ROW_VALUE"
    );
    assert_eq!(
        catalog::tables::ERR_INCONSISTENT_ROW_VALUE.code().value(),
        errcode::ErrInconsistentRowValue as isize,
        "tables::ERR_INCONSISTENT_ROW_VALUE"
    );
    checked += 1;
    assert_eq!(
        catalog::tables::ERR_INCONSISTENT_HANDLE.class(),
        TerrorClass::Table,
        "tables::ERR_INCONSISTENT_HANDLE"
    );
    assert_eq!(
        catalog::tables::ERR_INCONSISTENT_HANDLE.code().value(),
        errcode::ErrInconsistentHandle as isize,
        "tables::ERR_INCONSISTENT_HANDLE"
    );
    checked += 1;
    assert_eq!(
        catalog::tables::ERR_INCONSISTENT_INDEXED_VALUE.class(),
        TerrorClass::Table,
        "tables::ERR_INCONSISTENT_INDEXED_VALUE"
    );
    assert_eq!(
        catalog::tables::ERR_INCONSISTENT_INDEXED_VALUE
            .code()
            .value(),
        errcode::ErrInconsistentIndexedValue as isize,
        "tables::ERR_INCONSISTENT_INDEXED_VALUE"
    );
    checked += 1;
    assert_eq!(
        catalog::tiflashcompute::ERR_TOPO_FETCHER.class(),
        TerrorClass::Util,
        "tiflashcompute::ERR_TOPO_FETCHER"
    );
    assert_eq!(
        catalog::tiflashcompute::ERR_TOPO_FETCHER.code().value(),
        errcode::ErrInternal as isize,
        "tiflashcompute::ERR_TOPO_FETCHER"
    );
    checked += 1;
    assert_eq!(
        catalog::tikv::ERR_TOKEN_LIMIT.class(),
        TerrorClass::TiKv,
        "tikv::ERR_TOKEN_LIMIT"
    );
    assert_eq!(
        catalog::tikv::ERR_TOKEN_LIMIT.code().value(),
        errcode::ErrTiKVStoreLimit as isize,
        "tikv::ERR_TOKEN_LIMIT"
    );
    checked += 1;
    assert_eq!(
        catalog::tikv::ERR_TI_KV_SERVER_TIMEOUT.class(),
        TerrorClass::TiKv,
        "tikv::ERR_TI_KV_SERVER_TIMEOUT"
    );
    assert_eq!(
        catalog::tikv::ERR_TI_KV_SERVER_TIMEOUT.code().value(),
        errcode::ErrTiKVServerTimeout as isize,
        "tikv::ERR_TI_KV_SERVER_TIMEOUT"
    );
    checked += 1;
    assert_eq!(
        catalog::tikv::ERR_TI_FLASH_SERVER_TIMEOUT.class(),
        TerrorClass::TiKv,
        "tikv::ERR_TI_FLASH_SERVER_TIMEOUT"
    );
    assert_eq!(
        catalog::tikv::ERR_TI_FLASH_SERVER_TIMEOUT.code().value(),
        errcode::ErrTiFlashServerTimeout as isize,
        "tikv::ERR_TI_FLASH_SERVER_TIMEOUT"
    );
    checked += 1;
    assert_eq!(
        catalog::tikv::ERR_TXN_ABORTED_BY_GC.class(),
        TerrorClass::TiKv,
        "tikv::ERR_TXN_ABORTED_BY_GC"
    );
    assert_eq!(
        catalog::tikv::ERR_TXN_ABORTED_BY_GC.code().value(),
        errcode::ErrTxnAbortedByGC as isize,
        "tikv::ERR_TXN_ABORTED_BY_GC"
    );
    checked += 1;
    assert_eq!(
        catalog::tikv::ERR_TI_KV_STALE_COMMAND.class(),
        TerrorClass::TiKv,
        "tikv::ERR_TI_KV_STALE_COMMAND"
    );
    assert_eq!(
        catalog::tikv::ERR_TI_KV_STALE_COMMAND.code().value(),
        errcode::ErrTiKVStaleCommand as isize,
        "tikv::ERR_TI_KV_STALE_COMMAND"
    );
    checked += 1;
    assert_eq!(
        catalog::tikv::ERR_QUERY_INTERRUPTED.class(),
        TerrorClass::TiKv,
        "tikv::ERR_QUERY_INTERRUPTED"
    );
    assert_eq!(
        catalog::tikv::ERR_QUERY_INTERRUPTED.code().value(),
        errcode::ErrQueryInterrupted as isize,
        "tikv::ERR_QUERY_INTERRUPTED"
    );
    checked += 1;
    assert_eq!(
        catalog::tikv::ERR_TI_KV_MAX_TIMESTAMP_NOT_SYNCED.class(),
        TerrorClass::TiKv,
        "tikv::ERR_TI_KV_MAX_TIMESTAMP_NOT_SYNCED"
    );
    assert_eq!(
        catalog::tikv::ERR_TI_KV_MAX_TIMESTAMP_NOT_SYNCED
            .code()
            .value(),
        errcode::ErrTiKVMaxTimestampNotSynced as isize,
        "tikv::ERR_TI_KV_MAX_TIMESTAMP_NOT_SYNCED"
    );
    checked += 1;
    assert_eq!(
        catalog::tikv::ERR_LOCK_ACQUIRE_FAIL_AND_NO_WAIT_SET.class(),
        TerrorClass::TiKv,
        "tikv::ERR_LOCK_ACQUIRE_FAIL_AND_NO_WAIT_SET"
    );
    assert_eq!(
        catalog::tikv::ERR_LOCK_ACQUIRE_FAIL_AND_NO_WAIT_SET
            .code()
            .value(),
        errcode::ErrLockAcquireFailAndNoWaitSet as isize,
        "tikv::ERR_LOCK_ACQUIRE_FAIL_AND_NO_WAIT_SET"
    );
    checked += 1;
    assert_eq!(
        catalog::tikv::ERR_RESOLVE_LOCK_TIMEOUT.class(),
        TerrorClass::TiKv,
        "tikv::ERR_RESOLVE_LOCK_TIMEOUT"
    );
    assert_eq!(
        catalog::tikv::ERR_RESOLVE_LOCK_TIMEOUT.code().value(),
        errcode::ErrResolveLockTimeout as isize,
        "tikv::ERR_RESOLVE_LOCK_TIMEOUT"
    );
    checked += 1;
    assert_eq!(
        catalog::tikv::ERR_LOCK_WAIT_TIMEOUT.class(),
        TerrorClass::TiKv,
        "tikv::ERR_LOCK_WAIT_TIMEOUT"
    );
    assert_eq!(
        catalog::tikv::ERR_LOCK_WAIT_TIMEOUT.code().value(),
        errcode::ErrLockWaitTimeout as isize,
        "tikv::ERR_LOCK_WAIT_TIMEOUT"
    );
    checked += 1;
    assert_eq!(
        catalog::tikv::ERR_TI_KV_SERVER_BUSY.class(),
        TerrorClass::TiKv,
        "tikv::ERR_TI_KV_SERVER_BUSY"
    );
    assert_eq!(
        catalog::tikv::ERR_TI_KV_SERVER_BUSY.code().value(),
        errcode::ErrTiKVServerBusy as isize,
        "tikv::ERR_TI_KV_SERVER_BUSY"
    );
    checked += 1;
    assert_eq!(
        catalog::tikv::ERR_TI_FLASH_SERVER_BUSY.class(),
        TerrorClass::TiKv,
        "tikv::ERR_TI_FLASH_SERVER_BUSY"
    );
    assert_eq!(
        catalog::tikv::ERR_TI_FLASH_SERVER_BUSY.code().value(),
        errcode::ErrTiFlashServerBusy as isize,
        "tikv::ERR_TI_FLASH_SERVER_BUSY"
    );
    checked += 1;
    assert_eq!(
        catalog::tikv::ERR_PD_SERVER_TIMEOUT.class(),
        TerrorClass::TiKv,
        "tikv::ERR_PD_SERVER_TIMEOUT"
    );
    assert_eq!(
        catalog::tikv::ERR_PD_SERVER_TIMEOUT.code().value(),
        errcode::ErrPDServerTimeout as isize,
        "tikv::ERR_PD_SERVER_TIMEOUT"
    );
    checked += 1;
    assert_eq!(
        catalog::tikv::ERR_REGION_UNAVAILABLE.class(),
        TerrorClass::TiKv,
        "tikv::ERR_REGION_UNAVAILABLE"
    );
    assert_eq!(
        catalog::tikv::ERR_REGION_UNAVAILABLE.code().value(),
        errcode::ErrRegionUnavailable as isize,
        "tikv::ERR_REGION_UNAVAILABLE"
    );
    checked += 1;
    assert_eq!(
        catalog::tikv::ERR_RESOURCE_GROUP_NOT_EXISTS.class(),
        TerrorClass::TiKv,
        "tikv::ERR_RESOURCE_GROUP_NOT_EXISTS"
    );
    assert_eq!(
        catalog::tikv::ERR_RESOURCE_GROUP_NOT_EXISTS.code().value(),
        errcode::ErrResourceGroupNotExists as isize,
        "tikv::ERR_RESOURCE_GROUP_NOT_EXISTS"
    );
    checked += 1;
    assert_eq!(
        catalog::tikv::ERR_RESOURCE_GROUP_CONFIG_UNAVAILABLE.class(),
        TerrorClass::TiKv,
        "tikv::ERR_RESOURCE_GROUP_CONFIG_UNAVAILABLE"
    );
    assert_eq!(
        catalog::tikv::ERR_RESOURCE_GROUP_CONFIG_UNAVAILABLE
            .code()
            .value(),
        errcode::ErrResourceGroupConfigUnavailable as isize,
        "tikv::ERR_RESOURCE_GROUP_CONFIG_UNAVAILABLE"
    );
    checked += 1;
    assert_eq!(
        catalog::tikv::ERR_RESOURCE_GROUP_THROTTLED.class(),
        TerrorClass::TiKv,
        "tikv::ERR_RESOURCE_GROUP_THROTTLED"
    );
    assert_eq!(
        catalog::tikv::ERR_RESOURCE_GROUP_THROTTLED.code().value(),
        errcode::ErrResourceGroupThrottled as isize,
        "tikv::ERR_RESOURCE_GROUP_THROTTLED"
    );
    checked += 1;
    assert_eq!(
        catalog::tikv::ERR_UNKNOWN.class(),
        TerrorClass::TiKv,
        "tikv::ERR_UNKNOWN"
    );
    assert_eq!(
        catalog::tikv::ERR_UNKNOWN.code().value(),
        errcode::ErrUnknown as isize,
        "tikv::ERR_UNKNOWN"
    );
    checked += 1;
    assert_eq!(
        catalog::timeutil::ERR_UNKNOWN_TIME_ZONE.class(),
        TerrorClass::Variable,
        "timeutil::ERR_UNKNOWN_TIME_ZONE"
    );
    assert_eq!(
        catalog::timeutil::ERR_UNKNOWN_TIME_ZONE.code().value(),
        errcode::ErrUnknownTimeZone as isize,
        "timeutil::ERR_UNKNOWN_TIME_ZONE"
    );
    checked += 1;
    assert_eq!(
        catalog::types::ERR_INVALID_JSON_TEXT.class(),
        TerrorClass::Json,
        "types::ERR_INVALID_JSON_TEXT"
    );
    assert_eq!(
        catalog::types::ERR_INVALID_JSON_TEXT.code().value(),
        errcode::ErrInvalidJSONText as isize,
        "types::ERR_INVALID_JSON_TEXT"
    );
    checked += 1;
    assert_eq!(
        catalog::types::ERR_INVALID_JSON_TYPE.class(),
        TerrorClass::Json,
        "types::ERR_INVALID_JSON_TYPE"
    );
    assert_eq!(
        catalog::types::ERR_INVALID_JSON_TYPE.code().value(),
        errcode::ErrInvalidJSONType as isize,
        "types::ERR_INVALID_JSON_TYPE"
    );
    checked += 1;
    assert_eq!(
        catalog::types::ERR_INVALID_JSON_TEXT_IN_PARAM.class(),
        TerrorClass::Json,
        "types::ERR_INVALID_JSON_TEXT_IN_PARAM"
    );
    assert_eq!(
        catalog::types::ERR_INVALID_JSON_TEXT_IN_PARAM
            .code()
            .value(),
        errcode::ErrInvalidJSONTextInParam as isize,
        "types::ERR_INVALID_JSON_TEXT_IN_PARAM"
    );
    checked += 1;
    assert_eq!(
        catalog::types::ERR_INVALID_JSON_PATH.class(),
        TerrorClass::Json,
        "types::ERR_INVALID_JSON_PATH"
    );
    assert_eq!(
        catalog::types::ERR_INVALID_JSON_PATH.code().value(),
        errcode::ErrInvalidJSONPath as isize,
        "types::ERR_INVALID_JSON_PATH"
    );
    checked += 1;
    assert_eq!(
        catalog::types::ERR_INVALID_JSON_CHARSET.class(),
        TerrorClass::Json,
        "types::ERR_INVALID_JSON_CHARSET"
    );
    assert_eq!(
        catalog::types::ERR_INVALID_JSON_CHARSET.code().value(),
        errcode::ErrInvalidJSONCharset as isize,
        "types::ERR_INVALID_JSON_CHARSET"
    );
    checked += 1;
    assert_eq!(
        catalog::types::ERR_INVALID_JSON_DATA.class(),
        TerrorClass::Json,
        "types::ERR_INVALID_JSON_DATA"
    );
    assert_eq!(
        catalog::types::ERR_INVALID_JSON_DATA.code().value(),
        errcode::ErrInvalidJSONData as isize,
        "types::ERR_INVALID_JSON_DATA"
    );
    checked += 1;
    assert_eq!(
        catalog::types::ERR_INVALID_JSON_PATH_MULTIPLE_SELECTION.class(),
        TerrorClass::Json,
        "types::ERR_INVALID_JSON_PATH_MULTIPLE_SELECTION"
    );
    assert_eq!(
        catalog::types::ERR_INVALID_JSON_PATH_MULTIPLE_SELECTION
            .code()
            .value(),
        errcode::ErrInvalidJSONPathMultipleSelection as isize,
        "types::ERR_INVALID_JSON_PATH_MULTIPLE_SELECTION"
    );
    checked += 1;
    assert_eq!(
        catalog::types::ERR_INVALID_JSON_CONTAINS_PATH_TYPE.class(),
        TerrorClass::Json,
        "types::ERR_INVALID_JSON_CONTAINS_PATH_TYPE"
    );
    assert_eq!(
        catalog::types::ERR_INVALID_JSON_CONTAINS_PATH_TYPE
            .code()
            .value(),
        errcode::ErrInvalidJSONContainsPathType as isize,
        "types::ERR_INVALID_JSON_CONTAINS_PATH_TYPE"
    );
    checked += 1;
    assert_eq!(
        catalog::types::ERR_JSON_DOCUMENT_NULL_KEY.class(),
        TerrorClass::Json,
        "types::ERR_JSON_DOCUMENT_NULL_KEY"
    );
    assert_eq!(
        catalog::types::ERR_JSON_DOCUMENT_NULL_KEY.code().value(),
        errcode::ErrJSONDocumentNULLKey as isize,
        "types::ERR_JSON_DOCUMENT_NULL_KEY"
    );
    checked += 1;
    assert_eq!(
        catalog::types::ERR_JSON_DOCUMENT_TOO_DEEP.class(),
        TerrorClass::Json,
        "types::ERR_JSON_DOCUMENT_TOO_DEEP"
    );
    assert_eq!(
        catalog::types::ERR_JSON_DOCUMENT_TOO_DEEP.code().value(),
        errcode::ErrJSONDocumentTooDeep as isize,
        "types::ERR_JSON_DOCUMENT_TOO_DEEP"
    );
    checked += 1;
    assert_eq!(
        catalog::types::ERR_JSON_OBJECT_KEY_TOO_LONG.class(),
        TerrorClass::Types,
        "types::ERR_JSON_OBJECT_KEY_TOO_LONG"
    );
    assert_eq!(
        catalog::types::ERR_JSON_OBJECT_KEY_TOO_LONG.code().value(),
        errcode::ErrJSONObjectKeyTooLong as isize,
        "types::ERR_JSON_OBJECT_KEY_TOO_LONG"
    );
    checked += 1;
    assert_eq!(
        catalog::types::ERR_INVALID_JSON_PATH_ARRAY_CELL.class(),
        TerrorClass::Json,
        "types::ERR_INVALID_JSON_PATH_ARRAY_CELL"
    );
    assert_eq!(
        catalog::types::ERR_INVALID_JSON_PATH_ARRAY_CELL
            .code()
            .value(),
        errcode::ErrInvalidJSONPathArrayCell as isize,
        "types::ERR_INVALID_JSON_PATH_ARRAY_CELL"
    );
    checked += 1;
    assert_eq!(
        catalog::types::ERR_UNSUPPORTED_SECOND_ARGUMENT_TYPE.class(),
        TerrorClass::Json,
        "types::ERR_UNSUPPORTED_SECOND_ARGUMENT_TYPE"
    );
    assert_eq!(
        catalog::types::ERR_UNSUPPORTED_SECOND_ARGUMENT_TYPE
            .code()
            .value(),
        errcode::ErrUnsupportedSecondArgumentType as isize,
        "types::ERR_UNSUPPORTED_SECOND_ARGUMENT_TYPE"
    );
    checked += 1;
    assert_eq!(
        catalog::types::ERR_DATA_TOO_LONG.class(),
        TerrorClass::Types,
        "types::ERR_DATA_TOO_LONG"
    );
    assert_eq!(
        catalog::types::ERR_DATA_TOO_LONG.code().value(),
        errcode::ErrDataTooLong as isize,
        "types::ERR_DATA_TOO_LONG"
    );
    checked += 1;
    assert_eq!(
        catalog::types::ERR_ILLEGAL_VALUE_FOR_TYPE.class(),
        TerrorClass::Types,
        "types::ERR_ILLEGAL_VALUE_FOR_TYPE"
    );
    assert_eq!(
        catalog::types::ERR_ILLEGAL_VALUE_FOR_TYPE.code().value(),
        errcode::ErrIllegalValueForType as isize,
        "types::ERR_ILLEGAL_VALUE_FOR_TYPE"
    );
    checked += 1;
    assert_eq!(
        catalog::types::ERR_TRUNCATED.class(),
        TerrorClass::Types,
        "types::ERR_TRUNCATED"
    );
    assert_eq!(
        catalog::types::ERR_TRUNCATED.code().value(),
        errcode::WarnDataTruncated as isize,
        "types::ERR_TRUNCATED"
    );
    checked += 1;
    assert_eq!(
        catalog::types::ERR_OVERFLOW.class(),
        TerrorClass::Types,
        "types::ERR_OVERFLOW"
    );
    assert_eq!(
        catalog::types::ERR_OVERFLOW.code().value(),
        errcode::ErrDataOutOfRange as isize,
        "types::ERR_OVERFLOW"
    );
    checked += 1;
    assert_eq!(
        catalog::types::ERR_DIV_BY_ZERO.class(),
        TerrorClass::Types,
        "types::ERR_DIV_BY_ZERO"
    );
    assert_eq!(
        catalog::types::ERR_DIV_BY_ZERO.code().value(),
        errcode::ErrDivisionByZero as isize,
        "types::ERR_DIV_BY_ZERO"
    );
    checked += 1;
    assert_eq!(
        catalog::types::ERR_TOO_BIG_DISPLAY_WIDTH.class(),
        TerrorClass::Types,
        "types::ERR_TOO_BIG_DISPLAY_WIDTH"
    );
    assert_eq!(
        catalog::types::ERR_TOO_BIG_DISPLAY_WIDTH.code().value(),
        errcode::ErrTooBigDisplaywidth as isize,
        "types::ERR_TOO_BIG_DISPLAY_WIDTH"
    );
    checked += 1;
    assert_eq!(
        catalog::types::ERR_TOO_BIG_FIELD_LENGTH.class(),
        TerrorClass::Types,
        "types::ERR_TOO_BIG_FIELD_LENGTH"
    );
    assert_eq!(
        catalog::types::ERR_TOO_BIG_FIELD_LENGTH.code().value(),
        errcode::ErrTooBigFieldlength as isize,
        "types::ERR_TOO_BIG_FIELD_LENGTH"
    );
    checked += 1;
    assert_eq!(
        catalog::types::ERR_TOO_BIG_SET.class(),
        TerrorClass::Types,
        "types::ERR_TOO_BIG_SET"
    );
    assert_eq!(
        catalog::types::ERR_TOO_BIG_SET.code().value(),
        errcode::ErrTooBigSet as isize,
        "types::ERR_TOO_BIG_SET"
    );
    checked += 1;
    assert_eq!(
        catalog::types::ERR_TOO_BIG_SCALE.class(),
        TerrorClass::Types,
        "types::ERR_TOO_BIG_SCALE"
    );
    assert_eq!(
        catalog::types::ERR_TOO_BIG_SCALE.code().value(),
        errcode::ErrTooBigScale as isize,
        "types::ERR_TOO_BIG_SCALE"
    );
    checked += 1;
    assert_eq!(
        catalog::types::ERR_TOO_BIG_PRECISION.class(),
        TerrorClass::Types,
        "types::ERR_TOO_BIG_PRECISION"
    );
    assert_eq!(
        catalog::types::ERR_TOO_BIG_PRECISION.code().value(),
        errcode::ErrTooBigPrecision as isize,
        "types::ERR_TOO_BIG_PRECISION"
    );
    checked += 1;
    assert_eq!(
        catalog::types::ERR_BAD_NUMBER.class(),
        TerrorClass::Types,
        "types::ERR_BAD_NUMBER"
    );
    assert_eq!(
        catalog::types::ERR_BAD_NUMBER.code().value(),
        errcode::ErrBadNumber as isize,
        "types::ERR_BAD_NUMBER"
    );
    checked += 1;
    assert_eq!(
        catalog::types::ERR_INVALID_FIELD_SIZE.class(),
        TerrorClass::Types,
        "types::ERR_INVALID_FIELD_SIZE"
    );
    assert_eq!(
        catalog::types::ERR_INVALID_FIELD_SIZE.code().value(),
        errcode::ErrInvalidFieldSize as isize,
        "types::ERR_INVALID_FIELD_SIZE"
    );
    checked += 1;
    assert_eq!(
        catalog::types::ERR_M_BIGGER_THAN_D.class(),
        TerrorClass::Types,
        "types::ERR_M_BIGGER_THAN_D"
    );
    assert_eq!(
        catalog::types::ERR_M_BIGGER_THAN_D.code().value(),
        errcode::ErrMBiggerThanD as isize,
        "types::ERR_M_BIGGER_THAN_D"
    );
    checked += 1;
    assert_eq!(
        catalog::types::ERR_WARN_DATA_OUT_OF_RANGE.class(),
        TerrorClass::Types,
        "types::ERR_WARN_DATA_OUT_OF_RANGE"
    );
    assert_eq!(
        catalog::types::ERR_WARN_DATA_OUT_OF_RANGE.code().value(),
        errcode::ErrWarnDataOutOfRange as isize,
        "types::ERR_WARN_DATA_OUT_OF_RANGE"
    );
    checked += 1;
    assert_eq!(
        catalog::types::ERR_DUPLICATED_VALUE_IN_TYPE.class(),
        TerrorClass::Types,
        "types::ERR_DUPLICATED_VALUE_IN_TYPE"
    );
    assert_eq!(
        catalog::types::ERR_DUPLICATED_VALUE_IN_TYPE.code().value(),
        errcode::ErrDuplicatedValueInType as isize,
        "types::ERR_DUPLICATED_VALUE_IN_TYPE"
    );
    checked += 1;
    assert_eq!(
        catalog::types::ERR_DATETIME_FUNCTION_OVERFLOW.class(),
        TerrorClass::Types,
        "types::ERR_DATETIME_FUNCTION_OVERFLOW"
    );
    assert_eq!(
        catalog::types::ERR_DATETIME_FUNCTION_OVERFLOW
            .code()
            .value(),
        errcode::ErrDatetimeFunctionOverflow as isize,
        "types::ERR_DATETIME_FUNCTION_OVERFLOW"
    );
    checked += 1;
    assert_eq!(
        catalog::types::ERR_CAST_AS_SIGNED_OVERFLOW.class(),
        TerrorClass::Types,
        "types::ERR_CAST_AS_SIGNED_OVERFLOW"
    );
    assert_eq!(
        catalog::types::ERR_CAST_AS_SIGNED_OVERFLOW.code().value(),
        errcode::ErrCastAsSignedOverflow as isize,
        "types::ERR_CAST_AS_SIGNED_OVERFLOW"
    );
    checked += 1;
    assert_eq!(
        catalog::types::ERR_CAST_NEG_INT_AS_UNSIGNED.class(),
        TerrorClass::Types,
        "types::ERR_CAST_NEG_INT_AS_UNSIGNED"
    );
    assert_eq!(
        catalog::types::ERR_CAST_NEG_INT_AS_UNSIGNED.code().value(),
        errcode::ErrCastNegIntAsUnsigned as isize,
        "types::ERR_CAST_NEG_INT_AS_UNSIGNED"
    );
    checked += 1;
    assert_eq!(
        catalog::types::ERR_INVALID_YEAR_FORMAT.class(),
        TerrorClass::Types,
        "types::ERR_INVALID_YEAR_FORMAT"
    );
    assert_eq!(
        catalog::types::ERR_INVALID_YEAR_FORMAT.code().value(),
        errcode::ErrInvalidYearFormat as isize,
        "types::ERR_INVALID_YEAR_FORMAT"
    );
    checked += 1;
    assert_eq!(
        catalog::types::ERR_INVALID_YEAR.class(),
        TerrorClass::Types,
        "types::ERR_INVALID_YEAR"
    );
    assert_eq!(
        catalog::types::ERR_INVALID_YEAR.code().value(),
        errcode::ErrInvalidYear as isize,
        "types::ERR_INVALID_YEAR"
    );
    checked += 1;
    assert_eq!(
        catalog::types::ERR_TRUNCATED_WRONG_VAL.class(),
        TerrorClass::Types,
        "types::ERR_TRUNCATED_WRONG_VAL"
    );
    assert_eq!(
        catalog::types::ERR_TRUNCATED_WRONG_VAL.code().value(),
        errcode::ErrTruncatedWrongValue as isize,
        "types::ERR_TRUNCATED_WRONG_VAL"
    );
    checked += 1;
    assert_eq!(
        catalog::types::ERR_INVALID_WEEK_MODE_FORMAT.class(),
        TerrorClass::Types,
        "types::ERR_INVALID_WEEK_MODE_FORMAT"
    );
    assert_eq!(
        catalog::types::ERR_INVALID_WEEK_MODE_FORMAT.code().value(),
        errcode::ErrInvalidWeekModeFormat as isize,
        "types::ERR_INVALID_WEEK_MODE_FORMAT"
    );
    checked += 1;
    assert_eq!(
        catalog::types::ERR_WRONG_FIELD_SPEC.class(),
        TerrorClass::Types,
        "types::ERR_WRONG_FIELD_SPEC"
    );
    assert_eq!(
        catalog::types::ERR_WRONG_FIELD_SPEC.code().value(),
        errcode::ErrWrongFieldSpec as isize,
        "types::ERR_WRONG_FIELD_SPEC"
    );
    checked += 1;
    assert_eq!(
        catalog::types::ERR_SYNTAX.class(),
        TerrorClass::Types,
        "types::ERR_SYNTAX"
    );
    assert_eq!(
        catalog::types::ERR_SYNTAX.code().value(),
        errcode::ErrParse as isize,
        "types::ERR_SYNTAX"
    );
    checked += 1;
    assert_eq!(
        catalog::types::ERR_WRONG_VALUE.class(),
        TerrorClass::Types,
        "types::ERR_WRONG_VALUE"
    );
    assert_eq!(
        catalog::types::ERR_WRONG_VALUE.code().value(),
        errcode::ErrTruncatedWrongValue as isize,
        "types::ERR_WRONG_VALUE"
    );
    checked += 1;
    assert_eq!(
        catalog::types::ERR_WRONG_VALUE2.class(),
        TerrorClass::Types,
        "types::ERR_WRONG_VALUE2"
    );
    assert_eq!(
        catalog::types::ERR_WRONG_VALUE2.code().value(),
        errcode::ErrWrongValue as isize,
        "types::ERR_WRONG_VALUE2"
    );
    checked += 1;
    assert_eq!(
        catalog::types::ERR_WRONG_VALUE_FOR_TYPE.class(),
        TerrorClass::Types,
        "types::ERR_WRONG_VALUE_FOR_TYPE"
    );
    assert_eq!(
        catalog::types::ERR_WRONG_VALUE_FOR_TYPE.code().value(),
        errcode::ErrWrongValueForType as isize,
        "types::ERR_WRONG_VALUE_FOR_TYPE"
    );
    checked += 1;
    assert_eq!(
        catalog::types::ERR_PARTITION_STATS_MISSING.class(),
        TerrorClass::Types,
        "types::ERR_PARTITION_STATS_MISSING"
    );
    assert_eq!(
        catalog::types::ERR_PARTITION_STATS_MISSING.code().value(),
        errcode::ErrPartitionStatsMissing as isize,
        "types::ERR_PARTITION_STATS_MISSING"
    );
    checked += 1;
    assert_eq!(
        catalog::types::ERR_PARTITION_COLUMN_STATS_MISSING.class(),
        TerrorClass::Types,
        "types::ERR_PARTITION_COLUMN_STATS_MISSING"
    );
    assert_eq!(
        catalog::types::ERR_PARTITION_COLUMN_STATS_MISSING
            .code()
            .value(),
        errcode::ErrPartitionColumnStatsMissing as isize,
        "types::ERR_PARTITION_COLUMN_STATS_MISSING"
    );
    checked += 1;
    assert_eq!(
        catalog::types::ERR_INCORRECT_DATETIME_VALUE.class(),
        TerrorClass::Types,
        "types::ERR_INCORRECT_DATETIME_VALUE"
    );
    assert_eq!(
        catalog::types::ERR_INCORRECT_DATETIME_VALUE.code().value(),
        errcode::ErrIncorrectDatetimeValue as isize,
        "types::ERR_INCORRECT_DATETIME_VALUE"
    );
    checked += 1;
    assert_eq!(
        catalog::types::ERR_JSON_BAD_ONE_OR_ALL_ARG.class(),
        TerrorClass::Types,
        "types::ERR_JSON_BAD_ONE_OR_ALL_ARG"
    );
    assert_eq!(
        catalog::types::ERR_JSON_BAD_ONE_OR_ALL_ARG.code().value(),
        errcode::ErrJSONBadOneOrAllArg as isize,
        "types::ERR_JSON_BAD_ONE_OR_ALL_ARG"
    );
    checked += 1;
    assert_eq!(
        catalog::types::ERR_JSON_VACUOUS_PATH.class(),
        TerrorClass::Types,
        "types::ERR_JSON_VACUOUS_PATH"
    );
    assert_eq!(
        catalog::types::ERR_JSON_VACUOUS_PATH.code().value(),
        errcode::ErrJSONVacuousPath as isize,
        "types::ERR_JSON_VACUOUS_PATH"
    );
    checked += 1;
    assert_eq!(
        catalog::types::ERR_TIMESTAMP_IN_DST_TRANSITION.class(),
        TerrorClass::Executor,
        "types::ERR_TIMESTAMP_IN_DST_TRANSITION"
    );
    assert_eq!(
        catalog::types::ERR_TIMESTAMP_IN_DST_TRANSITION
            .code()
            .value(),
        errcode::ErrTimeStampInDSTTransition as isize,
        "types::ERR_TIMESTAMP_IN_DST_TRANSITION"
    );
    checked += 1;
    assert_eq!(
        catalog::variable::ERR_WARN_DEPRECATED_SYNTAX.class(),
        TerrorClass::Variable,
        "variable::ERR_WARN_DEPRECATED_SYNTAX"
    );
    assert_eq!(
        catalog::variable::ERR_WARN_DEPRECATED_SYNTAX.code().value(),
        errcode::ErrWarnDeprecatedSyntax as isize,
        "variable::ERR_WARN_DEPRECATED_SYNTAX"
    );
    checked += 1;
    assert_eq!(
        catalog::variable::ERR_SNAPSHOT_TOO_OLD.class(),
        TerrorClass::Variable,
        "variable::ERR_SNAPSHOT_TOO_OLD"
    );
    assert_eq!(
        catalog::variable::ERR_SNAPSHOT_TOO_OLD.code().value(),
        errcode::ErrSnapshotTooOld as isize,
        "variable::ERR_SNAPSHOT_TOO_OLD"
    );
    checked += 1;
    assert_eq!(
        catalog::variable::ERR_UNSUPPORTED_VALUE_FOR_VAR.class(),
        TerrorClass::Variable,
        "variable::ERR_UNSUPPORTED_VALUE_FOR_VAR"
    );
    assert_eq!(
        catalog::variable::ERR_UNSUPPORTED_VALUE_FOR_VAR
            .code()
            .value(),
        errcode::ErrUnsupportedValueForVar as isize,
        "variable::ERR_UNSUPPORTED_VALUE_FOR_VAR"
    );
    checked += 1;
    assert_eq!(
        catalog::variable::ERR_UNKNOWN_SYSTEM_VAR.class(),
        TerrorClass::Variable,
        "variable::ERR_UNKNOWN_SYSTEM_VAR"
    );
    assert_eq!(
        catalog::variable::ERR_UNKNOWN_SYSTEM_VAR.code().value(),
        errcode::ErrUnknownSystemVariable as isize,
        "variable::ERR_UNKNOWN_SYSTEM_VAR"
    );
    checked += 1;
    assert_eq!(
        catalog::variable::ERR_INCORRECT_SCOPE.class(),
        TerrorClass::Variable,
        "variable::ERR_INCORRECT_SCOPE"
    );
    assert_eq!(
        catalog::variable::ERR_INCORRECT_SCOPE.code().value(),
        errcode::ErrIncorrectGlobalLocalVar as isize,
        "variable::ERR_INCORRECT_SCOPE"
    );
    checked += 1;
    assert_eq!(
        catalog::variable::ERR_UNKNOWN_TIME_ZONE.class(),
        TerrorClass::Variable,
        "variable::ERR_UNKNOWN_TIME_ZONE"
    );
    assert_eq!(
        catalog::variable::ERR_UNKNOWN_TIME_ZONE.code().value(),
        errcode::ErrUnknownTimeZone as isize,
        "variable::ERR_UNKNOWN_TIME_ZONE"
    );
    checked += 1;
    assert_eq!(
        catalog::variable::ERR_READ_ONLY.class(),
        TerrorClass::Variable,
        "variable::ERR_READ_ONLY"
    );
    assert_eq!(
        catalog::variable::ERR_READ_ONLY.code().value(),
        errcode::ErrVariableIsReadonly as isize,
        "variable::ERR_READ_ONLY"
    );
    checked += 1;
    assert_eq!(
        catalog::variable::ERR_WRONG_VALUE_FOR_VAR.class(),
        TerrorClass::Variable,
        "variable::ERR_WRONG_VALUE_FOR_VAR"
    );
    assert_eq!(
        catalog::variable::ERR_WRONG_VALUE_FOR_VAR.code().value(),
        errcode::ErrWrongValueForVar as isize,
        "variable::ERR_WRONG_VALUE_FOR_VAR"
    );
    checked += 1;
    assert_eq!(
        catalog::variable::ERR_WRONG_TYPE_FOR_VAR.class(),
        TerrorClass::Variable,
        "variable::ERR_WRONG_TYPE_FOR_VAR"
    );
    assert_eq!(
        catalog::variable::ERR_WRONG_TYPE_FOR_VAR.code().value(),
        errcode::ErrWrongTypeForVar as isize,
        "variable::ERR_WRONG_TYPE_FOR_VAR"
    );
    checked += 1;
    assert_eq!(
        catalog::variable::ERR_TRUNCATED_WRONG_VALUE.class(),
        TerrorClass::Variable,
        "variable::ERR_TRUNCATED_WRONG_VALUE"
    );
    assert_eq!(
        catalog::variable::ERR_TRUNCATED_WRONG_VALUE.code().value(),
        errcode::ErrTruncatedWrongValue as isize,
        "variable::ERR_TRUNCATED_WRONG_VALUE"
    );
    checked += 1;
    assert_eq!(
        catalog::variable::ERR_MAX_PREPARED_STMT_COUNT_REACHED.class(),
        TerrorClass::Variable,
        "variable::ERR_MAX_PREPARED_STMT_COUNT_REACHED"
    );
    assert_eq!(
        catalog::variable::ERR_MAX_PREPARED_STMT_COUNT_REACHED
            .code()
            .value(),
        errcode::ErrMaxPreparedStmtCountReached as isize,
        "variable::ERR_MAX_PREPARED_STMT_COUNT_REACHED"
    );
    checked += 1;
    assert_eq!(
        catalog::variable::ERR_UNSUPPORTED_ISOLATION_LEVEL.class(),
        TerrorClass::Variable,
        "variable::ERR_UNSUPPORTED_ISOLATION_LEVEL"
    );
    assert_eq!(
        catalog::variable::ERR_UNSUPPORTED_ISOLATION_LEVEL
            .code()
            .value(),
        errcode::ErrUnsupportedIsolationLevel as isize,
        "variable::ERR_UNSUPPORTED_ISOLATION_LEVEL"
    );
    checked += 1;
    assert_eq!(
        catalog::variable::ERR_UNKNOWN_SYSTEM_VARIABLE.class(),
        TerrorClass::Variable,
        "variable::ERR_UNKNOWN_SYSTEM_VARIABLE"
    );
    assert_eq!(
        catalog::variable::ERR_UNKNOWN_SYSTEM_VARIABLE
            .code()
            .value(),
        errcode::ErrUnknownSystemVariable as isize,
        "variable::ERR_UNKNOWN_SYSTEM_VARIABLE"
    );
    checked += 1;
    assert_eq!(
        catalog::variable::ERR_GLOBAL_VARIABLE.class(),
        TerrorClass::Variable,
        "variable::ERR_GLOBAL_VARIABLE"
    );
    assert_eq!(
        catalog::variable::ERR_GLOBAL_VARIABLE.code().value(),
        errcode::ErrGlobalVariable as isize,
        "variable::ERR_GLOBAL_VARIABLE"
    );
    checked += 1;
    assert_eq!(
        catalog::variable::ERR_LOCAL_VARIABLE.class(),
        TerrorClass::Variable,
        "variable::ERR_LOCAL_VARIABLE"
    );
    assert_eq!(
        catalog::variable::ERR_LOCAL_VARIABLE.code().value(),
        errcode::ErrLocalVariable as isize,
        "variable::ERR_LOCAL_VARIABLE"
    );
    checked += 1;
    assert_eq!(
        catalog::variable::ERR_VALUE_NOT_SUPPORTED_WHEN.class(),
        TerrorClass::Variable,
        "variable::ERR_VALUE_NOT_SUPPORTED_WHEN"
    );
    assert_eq!(
        catalog::variable::ERR_VALUE_NOT_SUPPORTED_WHEN
            .code()
            .value(),
        errcode::ErrNotSupportedYet as isize,
        "variable::ERR_VALUE_NOT_SUPPORTED_WHEN"
    );
    checked += 1;
    assert_eq!(
        catalog::variable::ERR_SET_GLOBAL_MAX_ALLOWED_PACKET.class(),
        TerrorClass::Variable,
        "variable::ERR_SET_GLOBAL_MAX_ALLOWED_PACKET"
    );
    assert_eq!(
        catalog::variable::ERR_SET_GLOBAL_MAX_ALLOWED_PACKET
            .code()
            .value(),
        errcode::ErrNotSupportedYet as isize,
        "variable::ERR_SET_GLOBAL_MAX_ALLOWED_PACKET"
    );
    checked += 1;
    assert_eq!(
        catalog::variable::ERR_NOT_SUPPORTED_IN_NEXT_GEN.class(),
        TerrorClass::Variable,
        "variable::ERR_NOT_SUPPORTED_IN_NEXT_GEN"
    );
    assert_eq!(
        catalog::variable::ERR_NOT_SUPPORTED_IN_NEXT_GEN
            .code()
            .value(),
        errcode::ErrNotSupportedYet as isize,
        "variable::ERR_NOT_SUPPORTED_IN_NEXT_GEN"
    );
    checked += 1;
    assert_eq!(
        catalog::variable::ERR_NOT_VALID_PASSWORD.class(),
        TerrorClass::Executor,
        "variable::ERR_NOT_VALID_PASSWORD"
    );
    assert_eq!(
        catalog::variable::ERR_NOT_VALID_PASSWORD.code().value(),
        errcode::ErrNotValidPassword as isize,
        "variable::ERR_NOT_VALID_PASSWORD"
    );
    checked += 1;
    assert_eq!(
        catalog::variable::ERR_FUNCTIONS_NOOP_IMPL.class(),
        TerrorClass::Variable,
        "variable::ERR_FUNCTIONS_NOOP_IMPL"
    );
    assert_eq!(
        catalog::variable::ERR_FUNCTIONS_NOOP_IMPL.code().value(),
        errcode::ErrNotSupportedYet as isize,
        "variable::ERR_FUNCTIONS_NOOP_IMPL"
    );
    checked += 1;
    assert_eq!(
        catalog::variable::ERR_VARIABLE_NO_LONGER_SUPPORTED.class(),
        TerrorClass::Variable,
        "variable::ERR_VARIABLE_NO_LONGER_SUPPORTED"
    );
    assert_eq!(
        catalog::variable::ERR_VARIABLE_NO_LONGER_SUPPORTED
            .code()
            .value(),
        errcode::ErrVariableNoLongerSupported as isize,
        "variable::ERR_VARIABLE_NO_LONGER_SUPPORTED"
    );
    checked += 1;
    assert_eq!(
        catalog::variable::ERR_INVALID_DEFAULT_UTF8_MB4_COLLATION.class(),
        TerrorClass::Variable,
        "variable::ERR_INVALID_DEFAULT_UTF8_MB4_COLLATION"
    );
    assert_eq!(
        catalog::variable::ERR_INVALID_DEFAULT_UTF8_MB4_COLLATION
            .code()
            .value(),
        errcode::ErrInvalidDefaultUTF8MB4Collation as isize,
        "variable::ERR_INVALID_DEFAULT_UTF8_MB4_COLLATION"
    );
    checked += 1;
    assert_eq!(
        catalog::variable::ERR_WARN_DEPRECATED_SYNTAX_NO_REPLACEMENT.class(),
        TerrorClass::Variable,
        "variable::ERR_WARN_DEPRECATED_SYNTAX_NO_REPLACEMENT"
    );
    assert_eq!(
        catalog::variable::ERR_WARN_DEPRECATED_SYNTAX_NO_REPLACEMENT
            .code()
            .value(),
        errcode::ErrWarnDeprecatedSyntaxNoReplacement as isize,
        "variable::ERR_WARN_DEPRECATED_SYNTAX_NO_REPLACEMENT"
    );
    checked += 1;
    assert_eq!(
        catalog::variable::ERR_WARN_DEPRECATED_SYNTAX_SIMPLE_MSG.class(),
        TerrorClass::Variable,
        "variable::ERR_WARN_DEPRECATED_SYNTAX_SIMPLE_MSG"
    );
    assert_eq!(
        catalog::variable::ERR_WARN_DEPRECATED_SYNTAX_SIMPLE_MSG
            .code()
            .value(),
        errcode::ErrWarnDeprecatedSyntaxNoReplacement as isize,
        "variable::ERR_WARN_DEPRECATED_SYNTAX_SIMPLE_MSG"
    );
    checked += 1;
    assert_eq!(
        catalog::workloadrepo::ERR_WRONG_VALUE_FOR_VAR.class(),
        TerrorClass::Util,
        "workloadrepo::ERR_WRONG_VALUE_FOR_VAR"
    );
    assert_eq!(
        catalog::workloadrepo::ERR_WRONG_VALUE_FOR_VAR
            .code()
            .value(),
        errcode::ErrWrongValueForVar as isize,
        "workloadrepo::ERR_WRONG_VALUE_FOR_VAR"
    );
    checked += 1;
    assert_eq!(
        catalog::workloadrepo::ERR_UNSUPPORTED_ETCD_REQUIRED.class(),
        TerrorClass::Util,
        "workloadrepo::ERR_UNSUPPORTED_ETCD_REQUIRED"
    );
    assert_eq!(
        catalog::workloadrepo::ERR_UNSUPPORTED_ETCD_REQUIRED
            .code()
            .value(),
        errcode::ErrNotSupportedYet as isize,
        "workloadrepo::ERR_UNSUPPORTED_ETCD_REQUIRED"
    );
    checked += 1;
    assert_eq!(
        catalog::workloadrepo::ERR_WORKLOAD_NOT_STARTED.class(),
        TerrorClass::Util,
        "workloadrepo::ERR_WORKLOAD_NOT_STARTED"
    );
    assert_eq!(
        catalog::workloadrepo::ERR_WORKLOAD_NOT_STARTED
            .code()
            .value(),
        errcode::ErrNotSupportedYet as isize,
        "workloadrepo::ERR_WORKLOAD_NOT_STARTED"
    );
    checked += 1;
    assert_eq!(
        catalog::workloadrepo::ERR_COULD_NOT_START_SNAPSHOT.class(),
        TerrorClass::Util,
        "workloadrepo::ERR_COULD_NOT_START_SNAPSHOT"
    );
    assert_eq!(
        catalog::workloadrepo::ERR_COULD_NOT_START_SNAPSHOT
            .code()
            .value(),
        errcode::ErrUnknown as isize,
        "workloadrepo::ERR_COULD_NOT_START_SNAPSHOT"
    );
    checked += 1;
    assert_eq!(checked, 337);
}
