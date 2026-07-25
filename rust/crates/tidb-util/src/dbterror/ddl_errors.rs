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

//! The complete `ddl_terror.go` DDL error table (228 prototypes).
//!
//! GENERATED mechanically from `pkg/util/dbterror/ddl_terror.go`; every entry
//! is verified against `dbterror_go_fixture.txt` (a dump from the real Go
//! package) by `super::tests::ddl_errors_match_go_fixture`. Do not hand-edit
//! individual messages — regenerate and re-dump instead.
//!
//! Three shapes, mirroring the source:
//! - `ddl_std_errors!` entries are `ClassDDL.NewStd(code)` (catalog message
//!   and redaction metadata).
//! - `new_std_err(codeA, catalog_message(codeB))` entries register under one
//!   code while carrying another code's catalog message (e.g.
//!   `ErrWaitReorgTimeout`).
//! - `new_plain_err` entries are `NewStdErr(code, Message(text, nil))`:
//!   a composed literal (usually the `ErrUnsupportedDDLOperation` template
//!   with its `%s` pre-filled) carrying no redaction positions.

use std::sync::LazyLock;

use tidb_error::terror::TerrorError;
use tidb_error::tidb::errcode;

use super::{catalog_message, CLASS_DDL};

macro_rules! ddl_std_errors {
    ($($(#[$doc:meta])* $name:ident => $code:path,)+) => {
        $(
            $(#[$doc])*
            pub static $name: LazyLock<TerrorError> =
                LazyLock::new(|| CLASS_DDL.new_std($code));
        )+
    };
}

ddl_std_errors! {
    /// Source `dbterror.ErrInvalidWorker`.
    ERR_INVALID_WORKER => errcode::ErrInvalidDDLWorker,
    /// Source `dbterror.ErrNotOwner`.
    ERR_NOT_OWNER => errcode::ErrNotOwner,
    /// Source `dbterror.ErrCantDecodeRecord`.
    ERR_CANT_DECODE_RECORD => errcode::ErrCantDecodeRecord,
    /// Source `dbterror.ErrInvalidDDLJob`.
    ERR_INVALID_DDL_JOB => errcode::ErrInvalidDDLJob,
    /// Source `dbterror.ErrCancelledDDLJob`.
    ERR_CANCELLED_DDL_JOB => errcode::ErrCancelledDDLJob,
    /// Source `dbterror.ErrPausedDDLJob`.
    ERR_PAUSED_DDL_JOB => errcode::ErrPausedDDLJob,
    /// Source `dbterror.ErrBDRRestrictedDDL`.
    ERR_BDR_RESTRICTED_DDL => errcode::ErrBDRRestrictedDDL,
    /// Source `dbterror.ErrDDLAutoPausedByKVDiskFull`.
    ERR_DDL_AUTO_PAUSED_BY_KV_DISK_FULL => errcode::ErrDDLAutoPausedByKVDiskFull,
    /// Source `dbterror.ErrInvalidStoreVer`.
    ERR_INVALID_STORE_VER => errcode::ErrInvalidStoreVersion,
    /// Source `dbterror.ErrRepairTableFail`.
    ERR_REPAIR_TABLE_FAIL => errcode::ErrRepairTable,
    /// Source `dbterror.ErrCantDropColWithCheckConstraint`.
    ERR_CANT_DROP_COL_WITH_CHECK_CONSTRAINT => errcode::ErrDependentByCheckConstraint,
    /// Source `dbterror.ErrUnsupportedEngineAttribute`.
    ERR_UNSUPPORTED_ENGINE_ATTRIBUTE => errcode::ErrEngineAttributeNotSupported,
    /// Source `dbterror.ErrModifyColumnReferencedByPartialCondition`.
    ERR_MODIFY_COLUMN_REFERENCED_BY_PARTIAL_CONDITION => errcode::ErrModifyColumnReferencedByPartialCondition,
    /// Source `dbterror.ErrBlobKeyWithoutLength`.
    ERR_BLOB_KEY_WITHOUT_LENGTH => errcode::ErrBlobKeyWithoutLength,
    /// Source `dbterror.ErrKeyPart0`.
    ERR_KEY_PART0 => errcode::ErrKeyPart0,
    /// Source `dbterror.ErrIncorrectPrefixKey`.
    ERR_INCORRECT_PREFIX_KEY => errcode::ErrWrongSubKey,
    /// Source `dbterror.ErrTooLongKey`.
    ERR_TOO_LONG_KEY => errcode::ErrTooLongKey,
    /// Source `dbterror.ErrKeyColumnDoesNotExits`.
    ERR_KEY_COLUMN_DOES_NOT_EXITS => errcode::ErrKeyColumnDoesNotExits,
    /// Source `dbterror.ErrInvalidDDLJobVersion`.
    ERR_INVALID_DDL_JOB_VERSION => errcode::ErrInvalidDDLJobVersion,
    /// Source `dbterror.ErrInvalidUseOfNull`.
    ERR_INVALID_USE_OF_NULL => errcode::ErrInvalidUseOfNull,
    /// Source `dbterror.ErrTooManyFields`.
    ERR_TOO_MANY_FIELDS => errcode::ErrTooManyFields,
    /// Source `dbterror.ErrTooManyKeys`.
    ERR_TOO_MANY_KEYS => errcode::ErrTooManyKeys,
    /// Source `dbterror.ErrInvalidSplitRegionRanges`.
    ERR_INVALID_SPLIT_REGION_RANGES => errcode::ErrInvalidSplitRegionRanges,
    /// Source `dbterror.ErrReorgPanic`.
    ERR_REORG_PANIC => errcode::ErrReorgPanic,
    /// Source `dbterror.ErrFkColumnCannotDrop`.
    ERR_FK_COLUMN_CANNOT_DROP => errcode::ErrFkColumnCannotDrop,
    /// Source `dbterror.ErrFkColumnCannotDropChild`.
    ERR_FK_COLUMN_CANNOT_DROP_CHILD => errcode::ErrFkColumnCannotDropChild,
    /// Source `dbterror.ErrFKIncompatibleColumns`.
    ERR_FK_INCOMPATIBLE_COLUMNS => errcode::ErrFKIncompatibleColumns,
    /// Source `dbterror.ErrOnlyOnRangeListPartition`.
    ERR_ONLY_ON_RANGE_LIST_PARTITION => errcode::ErrOnlyOnRangeListPartition,
    /// Source `dbterror.ErrWrongKeyColumn`.
    ERR_WRONG_KEY_COLUMN => errcode::ErrWrongKeyColumn,
    /// Source `dbterror.ErrWrongKeyColumnFunctionalIndex`.
    ERR_WRONG_KEY_COLUMN_FUNCTIONAL_INDEX => errcode::ErrWrongKeyColumnFunctionalIndex,
    /// Source `dbterror.ErrWrongFKOptionForGeneratedColumn`.
    ERR_WRONG_FK_OPTION_FOR_GENERATED_COLUMN => errcode::ErrWrongFKOptionForGeneratedColumn,
    /// Source `dbterror.ErrUnsupportedOnGeneratedColumn`.
    ERR_UNSUPPORTED_ON_GENERATED_COLUMN => errcode::ErrUnsupportedOnGeneratedColumn,
    /// Source `dbterror.ErrGeneratedColumnNonPrior`.
    ERR_GENERATED_COLUMN_NON_PRIOR => errcode::ErrGeneratedColumnNonPrior,
    /// Source `dbterror.ErrDependentByGeneratedColumn`.
    ERR_DEPENDENT_BY_GENERATED_COLUMN => errcode::ErrDependentByGeneratedColumn,
    /// Source `dbterror.ErrJSONUsedAsKey`.
    ERR_JSON_USED_AS_KEY => errcode::ErrJSONUsedAsKey,
    /// Source `dbterror.ErrBlobCantHaveDefault`.
    ERR_BLOB_CANT_HAVE_DEFAULT => errcode::ErrBlobCantHaveDefault,
    /// Source `dbterror.ErrTooLongIndexComment`.
    ERR_TOO_LONG_INDEX_COMMENT => errcode::ErrTooLongIndexComment,
    /// Source `dbterror.ErrTooLongTableComment`.
    ERR_TOO_LONG_TABLE_COMMENT => errcode::ErrTooLongTableComment,
    /// Source `dbterror.ErrTooLongFieldComment`.
    ERR_TOO_LONG_FIELD_COMMENT => errcode::ErrTooLongFieldComment,
    /// Source `dbterror.ErrTooLongTablePartitionComment`.
    ERR_TOO_LONG_TABLE_PARTITION_COMMENT => errcode::ErrTooLongTablePartitionComment,
    /// Source `dbterror.ErrInvalidDefaultValue`.
    ERR_INVALID_DEFAULT_VALUE => errcode::ErrInvalidDefault,
    /// Source `dbterror.ErrDefValGeneratedNamedFunctionIsNotAllowed`.
    ERR_DEF_VAL_GENERATED_NAMED_FUNCTION_IS_NOT_ALLOWED => errcode::ErrDefValGeneratedNamedFunctionIsNotAllowed,
    /// Source `dbterror.ErrGeneratedColumnRefAutoInc`.
    ERR_GENERATED_COLUMN_REF_AUTO_INC => errcode::ErrGeneratedColumnRefAutoInc,
    /// Source `dbterror.ErrExpressionIndexCanNotRefer`.
    ERR_EXPRESSION_INDEX_CAN_NOT_REFER => errcode::ErrFunctionalIndexRefAutoIncrement,
    /// Source `dbterror.ErrGeneratedColumnFunctionIsNotAllowed`.
    ERR_GENERATED_COLUMN_FUNCTION_IS_NOT_ALLOWED => errcode::ErrGeneratedColumnFunctionIsNotAllowed,
    /// Source `dbterror.ErrGeneratedColumnRowValueIsNotAllowed`.
    ERR_GENERATED_COLUMN_ROW_VALUE_IS_NOT_ALLOWED => errcode::ErrGeneratedColumnRowValueIsNotAllowed,
    /// Source `dbterror.ErrFunctionalIndexFunctionIsNotAllowed`.
    ERR_FUNCTIONAL_INDEX_FUNCTION_IS_NOT_ALLOWED => errcode::ErrFunctionalIndexFunctionIsNotAllowed,
    /// Source `dbterror.ErrFunctionalIndexRowValueIsNotAllowed`.
    ERR_FUNCTIONAL_INDEX_ROW_VALUE_IS_NOT_ALLOWED => errcode::ErrFunctionalIndexRowValueIsNotAllowed,
    /// Source `dbterror.ErrWindowInvalidWindowFuncUse`.
    ERR_WINDOW_INVALID_WINDOW_FUNC_USE => errcode::ErrWindowInvalidWindowFuncUse,
    /// Source `dbterror.ErrDupKeyName`.
    ERR_DUP_KEY_NAME => errcode::ErrDupKeyName,
    /// Source `dbterror.ErrFkDupName`.
    ERR_FK_DUP_NAME => errcode::ErrFkDupName,
    /// Source `dbterror.ErrPKIndexCantBeInvisible`.
    ERR_PK_INDEX_CANT_BE_INVISIBLE => errcode::ErrPKIndexCantBeInvisible,
    /// Source `dbterror.ErrColumnBadNull`.
    ERR_COLUMN_BAD_NULL => errcode::ErrBadNull,
    /// Source `dbterror.ErrBadField`.
    ERR_BAD_FIELD => errcode::ErrBadField,
    /// Source `dbterror.ErrCantRemoveAllFields`.
    ERR_CANT_REMOVE_ALL_FIELDS => errcode::ErrCantRemoveAllFields,
    /// Source `dbterror.ErrCantDropFieldOrKey`.
    ERR_CANT_DROP_FIELD_OR_KEY => errcode::ErrCantDropFieldOrKey,
    /// Source `dbterror.ErrInvalidOnUpdate`.
    ERR_INVALID_ON_UPDATE => errcode::ErrInvalidOnUpdate,
    /// Source `dbterror.ErrTooLongIdent`.
    ERR_TOO_LONG_IDENT => errcode::ErrTooLongIdent,
    /// Source `dbterror.ErrWrongDBName`.
    ERR_WRONG_DB_NAME => errcode::ErrWrongDBName,
    /// Source `dbterror.ErrWrongTableName`.
    ERR_WRONG_TABLE_NAME => errcode::ErrWrongTableName,
    /// Source `dbterror.ErrWrongColumnName`.
    ERR_WRONG_COLUMN_NAME => errcode::ErrWrongColumnName,
    /// Source `dbterror.ErrWrongPartitionName`.
    ERR_WRONG_PARTITION_NAME => errcode::ErrWrongPartitionName,
    /// Source `dbterror.ErrWrongUsage`.
    ERR_WRONG_USAGE => errcode::ErrWrongUsage,
    /// Source `dbterror.ErrInvalidGroupFuncUse`.
    ERR_INVALID_GROUP_FUNC_USE => errcode::ErrInvalidGroupFuncUse,
    /// Source `dbterror.ErrTableMustHaveColumns`.
    ERR_TABLE_MUST_HAVE_COLUMNS => errcode::ErrTableMustHaveColumns,
    /// Source `dbterror.ErrWrongNameForIndex`.
    ERR_WRONG_NAME_FOR_INDEX => errcode::ErrWrongNameForIndex,
    /// Source `dbterror.ErrUnknownCharacterSet`.
    ERR_UNKNOWN_CHARACTER_SET => errcode::ErrUnknownCharacterSet,
    /// Source `dbterror.ErrUnknownCollation`.
    ERR_UNKNOWN_COLLATION => errcode::ErrUnknownCollation,
    /// Source `dbterror.ErrCollationCharsetMismatch`.
    ERR_COLLATION_CHARSET_MISMATCH => errcode::ErrCollationCharsetMismatch,
    /// Source `dbterror.ErrPrimaryCantHaveNull`.
    ERR_PRIMARY_CANT_HAVE_NULL => errcode::ErrPrimaryCantHaveNull,
    /// Source `dbterror.ErrErrorOnRename`.
    ERR_ERROR_ON_RENAME => errcode::ErrErrorOnRename,
    /// Source `dbterror.ErrViewSelectClause`.
    ERR_VIEW_SELECT_CLAUSE => errcode::ErrViewSelectClause,
    /// Source `dbterror.ErrViewSelectVariable`.
    ERR_VIEW_SELECT_VARIABLE => errcode::ErrViewSelectVariable,
    /// Source `dbterror.ErrNotAllowedTypeInPartition`.
    ERR_NOT_ALLOWED_TYPE_IN_PARTITION => errcode::ErrFieldTypeNotAllowedAsPartitionField,
    /// Source `dbterror.ErrPartitionMgmtOnNonpartitioned`.
    ERR_PARTITION_MGMT_ON_NONPARTITIONED => errcode::ErrPartitionMgmtOnNonpartitioned,
    /// Source `dbterror.ErrDropPartitionNonExistent`.
    ERR_DROP_PARTITION_NON_EXISTENT => errcode::ErrDropPartitionNonExistent,
    /// Source `dbterror.ErrSameNamePartition`.
    ERR_SAME_NAME_PARTITION => errcode::ErrSameNamePartition,
    /// Source `dbterror.ErrSameNamePartitionField`.
    ERR_SAME_NAME_PARTITION_FIELD => errcode::ErrSameNamePartitionField,
    /// Source `dbterror.ErrRangeNotIncreasing`.
    ERR_RANGE_NOT_INCREASING => errcode::ErrRangeNotIncreasing,
    /// Source `dbterror.ErrPartitionMaxvalue`.
    ERR_PARTITION_MAXVALUE => errcode::ErrPartitionMaxvalue,
    /// Source `dbterror.ErrMaxvalueInValuesIn`.
    ERR_MAXVALUE_IN_VALUES_IN => errcode::ErrMaxvalueInValuesIn,
    /// Source `dbterror.ErrDropLastPartition`.
    ERR_DROP_LAST_PARTITION => errcode::ErrDropLastPartition,
    /// Source `dbterror.ErrTooManyPartitions`.
    ERR_TOO_MANY_PARTITIONS => errcode::ErrTooManyPartitions,
    /// Source `dbterror.ErrPartitionConstDomain`.
    ERR_PARTITION_CONST_DOMAIN => errcode::ErrPartitionConstDomain,
    /// Source `dbterror.ErrPartitionFunctionIsNotAllowed`.
    ERR_PARTITION_FUNCTION_IS_NOT_ALLOWED => errcode::ErrPartitionFunctionIsNotAllowed,
    /// Source `dbterror.ErrPartitionFuncNotAllowed`.
    ERR_PARTITION_FUNC_NOT_ALLOWED => errcode::ErrPartitionFuncNotAllowed,
    /// Source `dbterror.ErrUniqueKeyNeedAllFieldsInPf`.
    ERR_UNIQUE_KEY_NEED_ALL_FIELDS_IN_PF => errcode::ErrUniqueKeyNeedAllFieldsInPf,
    /// Source `dbterror.ErrWrongExprInPartitionFunc`.
    ERR_WRONG_EXPR_IN_PARTITION_FUNC => errcode::ErrWrongExprInPartitionFunc,
    /// Source `dbterror.ErrWarnDataTruncated`.
    ERR_WARN_DATA_TRUNCATED => errcode::WarnDataTruncated,
    /// Source `dbterror.ErrCoalesceOnlyOnHashPartition`.
    ERR_COALESCE_ONLY_ON_HASH_PARTITION => errcode::ErrCoalesceOnlyOnHashPartition,
    /// Source `dbterror.ErrViewWrongList`.
    ERR_VIEW_WRONG_LIST => errcode::ErrViewWrongList,
    /// Source `dbterror.ErrAlterOperationNotSupported`.
    ERR_ALTER_OPERATION_NOT_SUPPORTED => errcode::ErrAlterOperationNotSupportedReason,
    /// Source `dbterror.ErrWrongObject`.
    ERR_WRONG_OBJECT => errcode::ErrWrongObject,
    /// Source `dbterror.ErrTableCantHandleFt`.
    ERR_TABLE_CANT_HANDLE_FT => errcode::ErrTableCantHandleFt,
    /// Source `dbterror.ErrFieldNotFoundPart`.
    ERR_FIELD_NOT_FOUND_PART => errcode::ErrFieldNotFoundPart,
    /// Source `dbterror.ErrWrongTypeColumnValue`.
    ERR_WRONG_TYPE_COLUMN_VALUE => errcode::ErrWrongTypeColumnValue,
    /// Source `dbterror.ErrValuesIsNotIntType`.
    ERR_VALUES_IS_NOT_INT_TYPE => errcode::ErrValuesIsNotIntType,
    /// Source `dbterror.ErrFunctionalIndexPrimaryKey`.
    ERR_FUNCTIONAL_INDEX_PRIMARY_KEY => errcode::ErrFunctionalIndexPrimaryKey,
    /// Source `dbterror.ErrFunctionalIndexOnField`.
    ERR_FUNCTIONAL_INDEX_ON_FIELD => errcode::ErrFunctionalIndexOnField,
    /// Source `dbterror.ErrInvalidAutoRandom`.
    ERR_INVALID_AUTO_RANDOM => errcode::ErrInvalidAutoRandom,
    /// Source `dbterror.ErrUnsupportedConstraintCheck`.
    ERR_UNSUPPORTED_CONSTRAINT_CHECK => errcode::ErrUnsupportedConstraintCheck,
    /// Source `dbterror.ErrDerivedMustHaveAlias`.
    ERR_DERIVED_MUST_HAVE_ALIAS => errcode::ErrDerivedMustHaveAlias,
    /// Source `dbterror.ErrNullInValuesLessThan`.
    ERR_NULL_IN_VALUES_LESS_THAN => errcode::ErrNullInValuesLessThan,
    /// Source `dbterror.ErrSequenceRunOut`.
    ERR_SEQUENCE_RUN_OUT => errcode::ErrSequenceRunOut,
    /// Source `dbterror.ErrSequenceInvalidData`.
    ERR_SEQUENCE_INVALID_DATA => errcode::ErrSequenceInvalidData,
    /// Source `dbterror.ErrSequenceAccessFail`.
    ERR_SEQUENCE_ACCESS_FAIL => errcode::ErrSequenceAccessFail,
    /// Source `dbterror.ErrNotSequence`.
    ERR_NOT_SEQUENCE => errcode::ErrNotSequence,
    /// Source `dbterror.ErrUnknownSequence`.
    ERR_UNKNOWN_SEQUENCE => errcode::ErrUnknownSequence,
    /// Source `dbterror.ErrSequenceUnsupportedTableOption`.
    ERR_SEQUENCE_UNSUPPORTED_TABLE_OPTION => errcode::ErrSequenceUnsupportedTableOption,
    /// Source `dbterror.ErrColumnTypeUnsupportedNextValue`.
    ERR_COLUMN_TYPE_UNSUPPORTED_NEXT_VALUE => errcode::ErrColumnTypeUnsupportedNextValue,
    /// Source `dbterror.ErrAddColumnWithSequenceAsDefault`.
    ERR_ADD_COLUMN_WITH_SEQUENCE_AS_DEFAULT => errcode::ErrAddColumnWithSequenceAsDefault,
    /// Source `dbterror.ErrPartitionExchangePartTable`.
    ERR_PARTITION_EXCHANGE_PART_TABLE => errcode::ErrPartitionExchangePartTable,
    /// Source `dbterror.ErrPartitionExchangeTempTable`.
    ERR_PARTITION_EXCHANGE_TEMP_TABLE => errcode::ErrPartitionExchangeTempTable,
    /// Source `dbterror.ErrTablesDifferentMetadata`.
    ERR_TABLES_DIFFERENT_METADATA => errcode::ErrTablesDifferentMetadata,
    /// Source `dbterror.ErrRowDoesNotMatchPartition`.
    ERR_ROW_DOES_NOT_MATCH_PARTITION => errcode::ErrRowDoesNotMatchPartition,
    /// Source `dbterror.ErrPartitionExchangeForeignKey`.
    ERR_PARTITION_EXCHANGE_FOREIGN_KEY => errcode::ErrPartitionExchangeForeignKey,
    /// Source `dbterror.ErrCheckNoSuchTable`.
    ERR_CHECK_NO_SUCH_TABLE => errcode::ErrCheckNoSuchTable,
    /// Source `dbterror.ErrPartitionExchangeDifferentOption`.
    ERR_PARTITION_EXCHANGE_DIFFERENT_OPTION => errcode::ErrPartitionExchangeDifferentOption,
    /// Source `dbterror.ErrTableOptionUnionUnsupported`.
    ERR_TABLE_OPTION_UNION_UNSUPPORTED => errcode::ErrTableOptionUnionUnsupported,
    /// Source `dbterror.ErrTableOptionInsertMethodUnsupported`.
    ERR_TABLE_OPTION_INSERT_METHOD_UNSUPPORTED => errcode::ErrTableOptionInsertMethodUnsupported,
    /// Source `dbterror.ErrInvalidPlacementPolicyCheck`.
    ERR_INVALID_PLACEMENT_POLICY_CHECK => errcode::ErrPlacementPolicyCheck,
    /// Source `dbterror.ErrPlacementPolicyWithDirectOption`.
    ERR_PLACEMENT_POLICY_WITH_DIRECT_OPTION => errcode::ErrPlacementPolicyWithDirectOption,
    /// Source `dbterror.ErrPlacementPolicyInUse`.
    ERR_PLACEMENT_POLICY_IN_USE => errcode::ErrPlacementPolicyInUse,
    /// Source `dbterror.ErrMultipleDefConstInListPart`.
    ERR_MULTIPLE_DEF_CONST_IN_LIST_PART => errcode::ErrMultipleDefConstInListPart,
    /// Source `dbterror.ErrTruncatedWrongValue`.
    ERR_TRUNCATED_WRONG_VALUE => errcode::ErrTruncatedWrongValue,
    /// Source `dbterror.ErrWarnDataOutOfRange`.
    ERR_WARN_DATA_OUT_OF_RANGE => errcode::ErrWarnDataOutOfRange,
    /// Source `dbterror.ErrTooLongValueForType`.
    ERR_TOO_LONG_VALUE_FOR_TYPE => errcode::ErrTooLongValueForType,
    /// Source `dbterror.ErrUnknownEngine`.
    ERR_UNKNOWN_ENGINE => errcode::ErrUnknownStorageEngine,
    /// Source `dbterror.ErrPartitionNoTemporary`.
    ERR_PARTITION_NO_TEMPORARY => errcode::ErrPartitionNoTemporary,
    /// Source `dbterror.ErrOptOnTemporaryTable`.
    ERR_OPT_ON_TEMPORARY_TABLE => errcode::ErrOptOnTemporaryTable,
    /// Source `dbterror.ErrOptOnCacheTable`.
    ERR_OPT_ON_CACHE_TABLE => errcode::ErrOptOnCacheTable,
    /// Source `dbterror.ErrInvalidAttributesSpec`.
    ERR_INVALID_ATTRIBUTES_SPEC => errcode::ErrInvalidAttributesSpec,
    /// Source `dbterror.ErrFunctionalIndexOnJSONOrGeometryFunction`.
    ERR_FUNCTIONAL_INDEX_ON_JSON_OR_GEOMETRY_FUNCTION => errcode::ErrFunctionalIndexOnJSONOrGeometryFunction,
    /// Source `dbterror.ErrDependentByFunctionalIndex`.
    ERR_DEPENDENT_BY_FUNCTIONAL_INDEX => errcode::ErrDependentByFunctionalIndex,
    /// Source `dbterror.ErrFunctionalIndexOnBlob`.
    ERR_FUNCTIONAL_INDEX_ON_BLOB => errcode::ErrFunctionalIndexOnBlob,
    /// Source `dbterror.ErrDependentByPartitionFunctional`.
    ERR_DEPENDENT_BY_PARTITION_FUNCTIONAL => errcode::ErrDependentByPartitionFunctional,
    /// Source `dbterror.ErrAutoConvert`.
    ERR_AUTO_CONVERT => errcode::ErrAutoConvert,
    /// Source `dbterror.ErrWrongStringLength`.
    ERR_WRONG_STRING_LENGTH => errcode::ErrWrongStringLength,
    /// Source `dbterror.ErrBinlogUnsafeSystemFunction`.
    ERR_BINLOG_UNSAFE_SYSTEM_FUNCTION => errcode::ErrBinlogUnsafeSystemFunction,
    /// Source `dbterror.ErrDDLJobNotFound`.
    ERR_DDL_JOB_NOT_FOUND => errcode::ErrDDLJobNotFound,
    /// Source `dbterror.ErrCancelFinishedDDLJob`.
    ERR_CANCEL_FINISHED_DDL_JOB => errcode::ErrCancelFinishedDDLJob,
    /// Source `dbterror.ErrCannotCancelDDLJob`.
    ERR_CANNOT_CANCEL_DDL_JOB => errcode::ErrCannotCancelDDLJob,
    /// Source `dbterror.ErrCannotPauseDDLJob`.
    ERR_CANNOT_PAUSE_DDL_JOB => errcode::ErrCannotPauseDDLJob,
    /// Source `dbterror.ErrCannotResumeDDLJob`.
    ERR_CANNOT_RESUME_DDL_JOB => errcode::ErrCannotResumeDDLJob,
    /// Source `dbterror.ErrDDLSetting`.
    ERR_DDL_SETTING => errcode::ErrDDLSetting,
    /// Source `dbterror.ErrIngestFailed`.
    ERR_INGEST_FAILED => errcode::ErrIngestFailed,
    /// Source `dbterror.ErrIngestCheckEnvFailed`.
    ERR_INGEST_CHECK_ENV_FAILED => errcode::ErrIngestCheckEnvFailed,
    /// Source `dbterror.ErrColumnInChange`.
    ERR_COLUMN_IN_CHANGE => errcode::ErrColumnInChange,
    /// Source `dbterror.ErrDropIndexNeededInForeignKey`.
    ERR_DROP_INDEX_NEEDED_IN_FOREIGN_KEY => errcode::ErrDropIndexNeededInForeignKey,
    /// Source `dbterror.ErrForeignKeyCannotDropParent`.
    ERR_FOREIGN_KEY_CANNOT_DROP_PARENT => errcode::ErrForeignKeyCannotDropParent,
    /// Source `dbterror.ErrTruncateIllegalForeignKey`.
    ERR_TRUNCATE_ILLEGAL_FOREIGN_KEY => errcode::ErrTruncateIllegalForeignKey,
    /// Source `dbterror.ErrForeignKeyColumnCannotChange`.
    ERR_FOREIGN_KEY_COLUMN_CANNOT_CHANGE => errcode::ErrForeignKeyColumnCannotChange,
    /// Source `dbterror.ErrForeignKeyColumnCannotChangeChild`.
    ERR_FOREIGN_KEY_COLUMN_CANNOT_CHANGE_CHILD => errcode::ErrForeignKeyColumnCannotChangeChild,
    /// Source `dbterror.ErrNoReferencedRow2`.
    ERR_NO_REFERENCED_ROW2 => errcode::ErrNoReferencedRow2,
    /// Source `dbterror.ErrUnsupportedColumnInTTLConfig`.
    ERR_UNSUPPORTED_COLUMN_IN_TTL_CONFIG => errcode::ErrUnsupportedColumnInTTLConfig,
    /// Source `dbterror.ErrTTLColumnCannotDrop`.
    ERR_TTL_COLUMN_CANNOT_DROP => errcode::ErrTTLColumnCannotDrop,
    /// Source `dbterror.ErrSetTTLOptionForNonTTLTable`.
    ERR_SET_TTL_OPTION_FOR_NON_TTL_TABLE => errcode::ErrSetTTLOptionForNonTTLTable,
    /// Source `dbterror.ErrTempTableNotAllowedWithTTL`.
    ERR_TEMP_TABLE_NOT_ALLOWED_WITH_TTL => errcode::ErrTempTableNotAllowedWithTTL,
    /// Source `dbterror.ErrUnsupportedTTLReferencedByFK`.
    ERR_UNSUPPORTED_TTL_REFERENCED_BY_FK => errcode::ErrUnsupportedTTLReferencedByFK,
    /// Source `dbterror.ErrUnsupportedPrimaryKeyTypeWithTTL`.
    ERR_UNSUPPORTED_PRIMARY_KEY_TYPE_WITH_TTL => errcode::ErrUnsupportedPrimaryKeyTypeWithTTL,
    /// Source `dbterror.ErrNotSupportedYet`.
    ERR_NOT_SUPPORTED_YET => errcode::ErrNotSupportedYet,
    /// Source `dbterror.ErrColumnCheckConstraintReferOther`.
    ERR_COLUMN_CHECK_CONSTRAINT_REFER_OTHER => errcode::ErrColumnCheckConstraintReferencesOtherColumn,
    /// Source `dbterror.ErrTableCheckConstraintReferUnknown`.
    ERR_TABLE_CHECK_CONSTRAINT_REFER_UNKNOWN => errcode::ErrTableCheckConstraintReferUnknown,
    /// Source `dbterror.ErrConstraintNotFound`.
    ERR_CONSTRAINT_NOT_FOUND => errcode::ErrConstraintNotFound,
    /// Source `dbterror.ErrCheckConstraintIsViolated`.
    ERR_CHECK_CONSTRAINT_IS_VIOLATED => errcode::ErrCheckConstraintViolated,
    /// Source `dbterror.ErrCheckConstraintNamedFuncIsNotAllowed`.
    ERR_CHECK_CONSTRAINT_NAMED_FUNC_IS_NOT_ALLOWED => errcode::ErrCheckConstraintNamedFunctionIsNotAllowed,
    /// Source `dbterror.ErrCheckConstraintFuncIsNotAllowed`.
    ERR_CHECK_CONSTRAINT_FUNC_IS_NOT_ALLOWED => errcode::ErrCheckConstraintFunctionIsNotAllowed,
    /// Source `dbterror.ErrCheckConstraintVariables`.
    ERR_CHECK_CONSTRAINT_VARIABLES => errcode::ErrCheckConstraintVariables,
    /// Source `dbterror.ErrCheckConstraintRefersAutoIncrementColumn`.
    ERR_CHECK_CONSTRAINT_REFERS_AUTO_INCREMENT_COLUMN => errcode::ErrCheckConstraintRefersAutoIncrementColumn,
    /// Source `dbterror.ErrCheckConstraintUsingFKReferActionColumn`.
    ERR_CHECK_CONSTRAINT_USING_FK_REFER_ACTION_COLUMN => errcode::ErrCheckConstraintClauseUsingFKReferActionColumn,
    /// Source `dbterror.ErrNonBooleanExprForCheckConstraint`.
    ERR_NON_BOOLEAN_EXPR_FOR_CHECK_CONSTRAINT => errcode::ErrNonBooleanExprForCheckConstraint,
    /// Source `dbterror.ErrCheckConstraintDupName`.
    ERR_CHECK_CONSTRAINT_DUP_NAME => errcode::ErrCheckConstraintDupName,
    /// Source `dbterror.ErrGlobalIndexNotExplicitlySet`.
    ERR_GLOBAL_INDEX_NOT_EXPLICITLY_SET => errcode::ErrGlobalIndexNotExplicitlySet,
    /// Source `dbterror.ErrWarnGlobalIndexNeedManuallyAnalyze`.
    ERR_WARN_GLOBAL_INDEX_NEED_MANUALLY_ANALYZE => errcode::ErrWarnGlobalIndexNeedManuallyAnalyze,
    /// Source `dbterror.ErrEngineAttributeInvalidFormat`.
    ERR_ENGINE_ATTRIBUTE_INVALID_FORMAT => errcode::ErrEngineAttributeInvalidFormat,
    /// Source `dbterror.ErrStorageClassInvalidSpec`.
    ERR_STORAGE_CLASS_INVALID_SPEC => errcode::ErrStorageClassInvalidSpec,
    /// Source `dbterror.ErrInvalidTableAffinity`.
    ERR_INVALID_TABLE_AFFINITY => errcode::ErrInvalidAffinityOption,
    /// Source `dbterror.ErrForbiddenDDL`.
    ERR_FORBIDDEN_DDL => errcode::ErrForbiddenDDL,
}

/// Source `dbterror.ErrRunMultiSchemaChanges`.
pub static ERR_RUN_MULTI_SCHEMA_CHANGES: LazyLock<TerrorError> = LazyLock::new(|| {
    CLASS_DDL.new_plain_err(
        errcode::ErrUnsupportedDDLOperation,
        "Unsupported multi schema change for %s",
    )
});
/// Source `dbterror.ErrOperateSameColumn`.
pub static ERR_OPERATE_SAME_COLUMN: LazyLock<TerrorError> = LazyLock::new(|| {
    CLASS_DDL.new_plain_err(
        errcode::ErrUnsupportedDDLOperation,
        "Unsupported operate same column '%s'",
    )
});
/// Source `dbterror.ErrOperateSameIndex`.
pub static ERR_OPERATE_SAME_INDEX: LazyLock<TerrorError> = LazyLock::new(|| {
    CLASS_DDL.new_plain_err(
        errcode::ErrUnsupportedDDLOperation,
        "Unsupported operate same index '%s'",
    )
});
/// Source `dbterror.ErrWaitReorgTimeout`.
pub static ERR_WAIT_REORG_TIMEOUT: LazyLock<TerrorError> = LazyLock::new(|| {
    CLASS_DDL.new_std_err(
        errcode::ErrLockWaitTimeout,
        catalog_message(errcode::ErrWaitReorgTimeout),
    )
});
/// Source `dbterror.ErrUnsupportedAddColumnarIndex`.
pub static ERR_UNSUPPORTED_ADD_COLUMNAR_INDEX: LazyLock<TerrorError> = LazyLock::new(|| {
    CLASS_DDL.new_plain_err(
        errcode::ErrUnsupportedDDLOperation,
        "Unsupported add columnar index: %s",
    )
});
/// Source `dbterror.ErrUnsupportedAddVectorIndex`.
pub static ERR_UNSUPPORTED_ADD_VECTOR_INDEX: LazyLock<TerrorError> = LazyLock::new(|| {
    CLASS_DDL.new_plain_err(
        errcode::ErrUnsupportedDDLOperation,
        "Unsupported add vector index: %s",
    )
});
/// Source `dbterror.ErrCantDropColWithIndex`.
pub static ERR_CANT_DROP_COL_WITH_INDEX: LazyLock<TerrorError> = LazyLock::new(|| {
    CLASS_DDL.new_plain_err(
        errcode::ErrUnsupportedDDLOperation,
        "Unsupported drop column with index",
    )
});
/// Source `dbterror.ErrCantDropColWithAutoInc`.
pub static ERR_CANT_DROP_COL_WITH_AUTO_INC: LazyLock<TerrorError> = LazyLock::new(|| {
    CLASS_DDL.new_plain_err(errcode::ErrUnsupportedDDLOperation, "Unsupported can't remove column with auto_increment when @@tidb_allow_remove_auto_inc disabled")
});
/// Source `dbterror.ErrUnsupportedAddColumn`.
pub static ERR_UNSUPPORTED_ADD_COLUMN: LazyLock<TerrorError> = LazyLock::new(|| {
    CLASS_DDL.new_plain_err(
        errcode::ErrUnsupportedDDLOperation,
        "Unsupported add column",
    )
});
/// Source `dbterror.ErrUnsupportedModifyColumn`.
pub static ERR_UNSUPPORTED_MODIFY_COLUMN: LazyLock<TerrorError> = LazyLock::new(|| {
    CLASS_DDL.new_plain_err(
        errcode::ErrUnsupportedDDLOperation,
        "Unsupported modify column: %s",
    )
});
/// Source `dbterror.ErrUnsupportedModifyCharset`.
pub static ERR_UNSUPPORTED_MODIFY_CHARSET: LazyLock<TerrorError> = LazyLock::new(|| {
    CLASS_DDL.new_plain_err(errcode::ErrUnsupportedDDLOperation, "Unsupported modify %s")
});
/// Source `dbterror.ErrUnsupportedModifyCollation`.
pub static ERR_UNSUPPORTED_MODIFY_COLLATION: LazyLock<TerrorError> = LazyLock::new(|| {
    CLASS_DDL.new_plain_err(
        errcode::ErrUnsupportedDDLOperation,
        "Unsupported modifying collation from %s to %s",
    )
});
/// Source `dbterror.ErrUnsupportedPKHandle`.
pub static ERR_UNSUPPORTED_PK_HANDLE: LazyLock<TerrorError> = LazyLock::new(|| {
    CLASS_DDL.new_plain_err(
        errcode::ErrUnsupportedDDLOperation,
        "Unsupported drop integer primary key",
    )
});
/// Source `dbterror.ErrUnsupportedCharset`.
pub static ERR_UNSUPPORTED_CHARSET: LazyLock<TerrorError> = LazyLock::new(|| {
    CLASS_DDL.new_plain_err(
        errcode::ErrUnsupportedDDLOperation,
        "Unsupported charset %s and collate %s",
    )
});
/// Source `dbterror.ErrUnsupportedShardRowIDBits`.
pub static ERR_UNSUPPORTED_SHARD_ROW_ID_BITS: LazyLock<TerrorError> = LazyLock::new(|| {
    CLASS_DDL.new_plain_err(
        errcode::ErrUnsupportedDDLOperation,
        "Unsupported shard_row_id_bits for table with primary key as row id",
    )
});
/// Source `dbterror.ErrUnsupportedAlterTableWithValidation`.
pub static ERR_UNSUPPORTED_ALTER_TABLE_WITH_VALIDATION: LazyLock<TerrorError> =
    LazyLock::new(|| {
        CLASS_DDL.new_plain_err(
            errcode::ErrUnsupportedDDLOperation,
            "ALTER TABLE WITH VALIDATION is currently unsupported",
        )
    });
/// Source `dbterror.ErrUnsupportedAlterTableWithoutValidation`.
pub static ERR_UNSUPPORTED_ALTER_TABLE_WITHOUT_VALIDATION: LazyLock<TerrorError> =
    LazyLock::new(|| {
        CLASS_DDL.new_plain_err(
            errcode::ErrUnsupportedDDLOperation,
            "ALTER TABLE WITHOUT VALIDATION is currently unsupported",
        )
    });
/// Source `dbterror.ErrUnsupportedAlterTableOption`.
pub static ERR_UNSUPPORTED_ALTER_TABLE_OPTION: LazyLock<TerrorError> = LazyLock::new(|| {
    CLASS_DDL.new_plain_err(
        errcode::ErrUnsupportedDDLOperation,
        "This type of ALTER TABLE is currently unsupported",
    )
});
/// Source `dbterror.ErrUnsupportedAlterCacheForSysTable`.
pub static ERR_UNSUPPORTED_ALTER_CACHE_FOR_SYS_TABLE: LazyLock<TerrorError> = LazyLock::new(|| {
    CLASS_DDL.new_plain_err(
        errcode::ErrUnsupportedDDLOperation,
        "ALTER table cache for tables in system database is currently unsupported",
    )
});
/// Source `dbterror.ErrUnsupportedAddPartialIndex`.
pub static ERR_UNSUPPORTED_ADD_PARTIAL_INDEX: LazyLock<TerrorError> = LazyLock::new(|| {
    CLASS_DDL.new_plain_err(
        errcode::ErrUnsupportedDDLOperation,
        "Unsupported add partial index: %s",
    )
});
/// Source `dbterror.ErrUnsupportedAddPartition`.
pub static ERR_UNSUPPORTED_ADD_PARTITION: LazyLock<TerrorError> = LazyLock::new(|| {
    CLASS_DDL.new_plain_err(
        errcode::ErrUnsupportedDDLOperation,
        "Unsupported add partitions",
    )
});
/// Source `dbterror.ErrUnsupportedCoalescePartition`.
pub static ERR_UNSUPPORTED_COALESCE_PARTITION: LazyLock<TerrorError> = LazyLock::new(|| {
    CLASS_DDL.new_plain_err(
        errcode::ErrUnsupportedDDLOperation,
        "Unsupported coalesce partitions",
    )
});
/// Source `dbterror.ErrUnsupportedReorganizePartition`.
pub static ERR_UNSUPPORTED_REORGANIZE_PARTITION: LazyLock<TerrorError> = LazyLock::new(|| {
    CLASS_DDL.new_plain_err(
        errcode::ErrUnsupportedDDLOperation,
        "Unsupported reorganize partition",
    )
});
/// Source `dbterror.ErrUnsupportedCheckPartition`.
pub static ERR_UNSUPPORTED_CHECK_PARTITION: LazyLock<TerrorError> = LazyLock::new(|| {
    CLASS_DDL.new_plain_err(
        errcode::ErrUnsupportedDDLOperation,
        "Unsupported check partition",
    )
});
/// Source `dbterror.ErrUnsupportedOptimizePartition`.
pub static ERR_UNSUPPORTED_OPTIMIZE_PARTITION: LazyLock<TerrorError> = LazyLock::new(|| {
    CLASS_DDL.new_plain_err(
        errcode::ErrUnsupportedDDLOperation,
        "Unsupported optimize partition",
    )
});
/// Source `dbterror.ErrUnsupportedRebuildPartition`.
pub static ERR_UNSUPPORTED_REBUILD_PARTITION: LazyLock<TerrorError> = LazyLock::new(|| {
    CLASS_DDL.new_plain_err(
        errcode::ErrUnsupportedDDLOperation,
        "Unsupported rebuild partition",
    )
});
/// Source `dbterror.ErrUnsupportedRemovePartition`.
pub static ERR_UNSUPPORTED_REMOVE_PARTITION: LazyLock<TerrorError> = LazyLock::new(|| {
    CLASS_DDL.new_plain_err(
        errcode::ErrUnsupportedDDLOperation,
        "Unsupported remove partitioning",
    )
});
/// Source `dbterror.ErrUnsupportedRepairPartition`.
pub static ERR_UNSUPPORTED_REPAIR_PARTITION: LazyLock<TerrorError> = LazyLock::new(|| {
    CLASS_DDL.new_plain_err(
        errcode::ErrUnsupportedDDLOperation,
        "Unsupported repair partition",
    )
});
/// Source `dbterror.ErrUnsupportedPartitionByRangeColumns`.
pub static ERR_UNSUPPORTED_PARTITION_BY_RANGE_COLUMNS: LazyLock<TerrorError> =
    LazyLock::new(|| {
        CLASS_DDL.new_plain_err(
            errcode::ErrUnsupportedDDLOperation,
            "Unsupported partition by range columns",
        )
    });
/// Source `dbterror.ErrUnsupportedCreatePartition`.
pub static ERR_UNSUPPORTED_CREATE_PARTITION: LazyLock<TerrorError> = LazyLock::new(|| {
    CLASS_DDL.new_plain_err(
        errcode::ErrUnsupportedDDLOperation,
        "Unsupported partition type, treat as normal table",
    )
});
/// Source `dbterror.ErrUnsupportedIndexType`.
pub static ERR_UNSUPPORTED_INDEX_TYPE: LazyLock<TerrorError> = LazyLock::new(|| {
    CLASS_DDL.new_plain_err(
        errcode::ErrUnsupportedDDLOperation,
        "Unsupported index type",
    )
});
/// Source `dbterror.ErrInvalidDDLState`.
pub static ERR_INVALID_DDL_STATE: LazyLock<TerrorError> =
    LazyLock::new(|| CLASS_DDL.new_plain_err(errcode::ErrInvalidDDLState, "Invalid %s state: %v"));
/// Source `dbterror.ErrUnsupportedModifyPrimaryKey`.
pub static ERR_UNSUPPORTED_MODIFY_PRIMARY_KEY: LazyLock<TerrorError> = LazyLock::new(|| {
    CLASS_DDL.new_plain_err(
        errcode::ErrUnsupportedDDLOperation,
        "Unsupported %s primary key",
    )
});
/// Source `dbterror.ErrConflictingDeclarations`.
pub static ERR_CONFLICTING_DECLARATIONS: LazyLock<TerrorError> = LazyLock::new(|| {
    CLASS_DDL.new_plain_err(
        errcode::ErrConflictingDeclarations,
        "Conflicting declarations: 'CHARACTER SET %s' and 'CHARACTER SET %s'",
    )
});
/// Source `dbterror.ErrUnsupportedExpressionIndex`.
pub static ERR_UNSUPPORTED_EXPRESSION_INDEX: LazyLock<TerrorError> = LazyLock::new(|| {
    CLASS_DDL.new_plain_err(errcode::ErrUnsupportedDDLOperation, "Unsupported creating expression index containing unsafe functions without allow-expression-index in config")
});
/// Source `dbterror.ErrUnsupportedPartitionType`.
pub static ERR_UNSUPPORTED_PARTITION_TYPE: LazyLock<TerrorError> = LazyLock::new(|| {
    CLASS_DDL.new_plain_err(
        errcode::ErrUnsupportedDDLOperation,
        "Unsupported partition type of table %s when exchanging partition",
    )
});
/// Source `dbterror.ErrExchangePartitionDisabled`.
pub static ERR_EXCHANGE_PARTITION_DISABLED: LazyLock<TerrorError> = LazyLock::new(|| {
    CLASS_DDL.new_plain_err(errcode::ErrUnsupportedDDLOperation, "Exchange Partition is disabled, please set 'tidb_enable_exchange_partition' if you need to need to enable it")
});
/// Source `dbterror.ErrUnsupportedOnCommitPreserve`.
pub static ERR_UNSUPPORTED_ON_COMMIT_PRESERVE: LazyLock<TerrorError> = LazyLock::new(|| {
    CLASS_DDL.new_plain_err(
        errcode::ErrUnsupportedDDLOperation,
        "TiDB doesn't support ON COMMIT PRESERVE ROWS for now",
    )
});
/// Source `dbterror.ErrUnsupportedClusteredSecondaryKey`.
pub static ERR_UNSUPPORTED_CLUSTERED_SECONDARY_KEY: LazyLock<TerrorError> = LazyLock::new(|| {
    CLASS_DDL.new_plain_err(
        errcode::ErrUnsupportedDDLOperation,
        "CLUSTERED/NONCLUSTERED keyword is only supported for primary key",
    )
});
/// Source `dbterror.ErrUnsupportedLocalTempTableDDL`.
pub static ERR_UNSUPPORTED_LOCAL_TEMP_TABLE_DDL: LazyLock<TerrorError> = LazyLock::new(|| {
    CLASS_DDL.new_plain_err(
        errcode::ErrUnsupportedDDLOperation,
        "TiDB doesn't support %s for local temporary table",
    )
});
/// Source `dbterror.ErrUnsupportedAlterTableSpec`.
pub static ERR_UNSUPPORTED_ALTER_TABLE_SPEC: LazyLock<TerrorError> = LazyLock::new(|| {
    CLASS_DDL.new_plain_err(
        errcode::ErrUnsupportedDDLOperation,
        "Unsupported Unsupported/unknown ALTER TABLE specification",
    )
});
/// Source `dbterror.ErrGeneralUnsupportedDDL`.
pub static ERR_GENERAL_UNSUPPORTED_DDL: LazyLock<TerrorError> = LazyLock::new(|| {
    CLASS_DDL.new_plain_err(errcode::ErrUnsupportedDDLOperation, "Unsupported %s")
});
/// Source `dbterror.ErrAlterTiFlashModeForTableWithoutTiFlashReplica`.
pub static ERR_ALTER_TI_FLASH_MODE_FOR_TABLE_WITHOUT_TI_FLASH_REPLICA: LazyLock<TerrorError> =
    LazyLock::new(|| {
        CLASS_DDL.new_plain_err(
            0,
            "TiFlash mode will take effect after at least one TiFlash replica is set for the table",
        )
    });
/// Source `dbterror.ErrUnsupportedTiFlashOperationForSysOrMemTable`.
pub static ERR_UNSUPPORTED_TI_FLASH_OPERATION_FOR_SYS_OR_MEM_TABLE: LazyLock<TerrorError> =
    LazyLock::new(|| {
        CLASS_DDL.new_plain_err(
            errcode::ErrUnsupportedDDLOperation,
            "Unsupported `set TiFlash replica` settings for system table and memory table",
        )
    });
/// Source `dbterror.ErrUnsupportedTiFlashOperationForUnsupportedCharsetTable`.
pub static ERR_UNSUPPORTED_TI_FLASH_OPERATION_FOR_UNSUPPORTED_CHARSET_TABLE: LazyLock<TerrorError> =
    LazyLock::new(|| {
        CLASS_DDL.new_plain_err(
            errcode::ErrUnsupportedDDLOperation,
            "Unsupported `set TiFlash replica` settings for table contains %s charset",
        )
    });
/// Source `dbterror.ErrTiFlashBackfillIndex`.
pub static ERR_TI_FLASH_BACKFILL_INDEX: LazyLock<TerrorError> = LazyLock::new(|| {
    CLASS_DDL.new_plain_err(
        errcode::ErrTiFlashBackfillIndex,
        "TiFlash backfill index failed: %s",
    )
});
/// Source `dbterror.ErrWarnDeprecatedIntegerDisplayWidth`.
pub static ERR_WARN_DEPRECATED_INTEGER_DISPLAY_WIDTH: LazyLock<TerrorError> = LazyLock::new(|| {
    CLASS_DDL.new_plain_err(
        errcode::ErrWarnDeprecatedSyntaxNoReplacement,
        "Integer display width is deprecated and will be removed in a future release.",
    )
});
/// Source `dbterror.ErrWarnDeprecatedZerofill`.
pub static ERR_WARN_DEPRECATED_ZEROFILL: LazyLock<TerrorError> = LazyLock::new(|| {
    CLASS_DDL.new_plain_err(errcode::ErrWarnDeprecatedSyntaxNoReplacement, "The ZEROFILL attribute is deprecated and will be removed in a future release. Use the LPAD function to zero-pad numbers, or store the formatted numbers in a CHAR column.")
});
/// Source `dbterror.ErrUnsupportedDistTask`.
pub static ERR_UNSUPPORTED_DIST_TASK: LazyLock<TerrorError> = LazyLock::new(|| {
    CLASS_DDL.new_plain_err(errcode::ErrUnsupportedDDLOperation, "Unsupported tidb_enable_dist_task setting. To utilize distributed task execution, please enable tidb_ddl_enable_fast_reorg first.")
});
/// Source `dbterror.ErrCannotSetAffinityOnTable`.
pub static ERR_CANNOT_SET_AFFINITY_ON_TABLE: LazyLock<TerrorError> = LazyLock::new(|| {
    CLASS_DDL.new_plain_err(errcode::ErrInvalidAffinityOption, "Can not set %s on a %s.")
});

/// Every DDL error paired with its Go variable name, for the fixture test.
#[cfg(test)]
pub(crate) fn fixture_entries() -> Vec<(&'static str, &'static TerrorError)> {
    vec![
        ("ErrInvalidWorker", &*ERR_INVALID_WORKER),
        ("ErrNotOwner", &*ERR_NOT_OWNER),
        ("ErrCantDecodeRecord", &*ERR_CANT_DECODE_RECORD),
        ("ErrInvalidDDLJob", &*ERR_INVALID_DDL_JOB),
        ("ErrCancelledDDLJob", &*ERR_CANCELLED_DDL_JOB),
        ("ErrPausedDDLJob", &*ERR_PAUSED_DDL_JOB),
        ("ErrBDRRestrictedDDL", &*ERR_BDR_RESTRICTED_DDL),
        (
            "ErrDDLAutoPausedByKVDiskFull",
            &*ERR_DDL_AUTO_PAUSED_BY_KV_DISK_FULL,
        ),
        ("ErrInvalidStoreVer", &*ERR_INVALID_STORE_VER),
        ("ErrRepairTableFail", &*ERR_REPAIR_TABLE_FAIL),
        (
            "ErrCantDropColWithCheckConstraint",
            &*ERR_CANT_DROP_COL_WITH_CHECK_CONSTRAINT,
        ),
        (
            "ErrUnsupportedEngineAttribute",
            &*ERR_UNSUPPORTED_ENGINE_ATTRIBUTE,
        ),
        (
            "ErrModifyColumnReferencedByPartialCondition",
            &*ERR_MODIFY_COLUMN_REFERENCED_BY_PARTIAL_CONDITION,
        ),
        ("ErrBlobKeyWithoutLength", &*ERR_BLOB_KEY_WITHOUT_LENGTH),
        ("ErrKeyPart0", &*ERR_KEY_PART0),
        ("ErrIncorrectPrefixKey", &*ERR_INCORRECT_PREFIX_KEY),
        ("ErrTooLongKey", &*ERR_TOO_LONG_KEY),
        ("ErrKeyColumnDoesNotExits", &*ERR_KEY_COLUMN_DOES_NOT_EXITS),
        ("ErrInvalidDDLJobVersion", &*ERR_INVALID_DDL_JOB_VERSION),
        ("ErrInvalidUseOfNull", &*ERR_INVALID_USE_OF_NULL),
        ("ErrTooManyFields", &*ERR_TOO_MANY_FIELDS),
        ("ErrTooManyKeys", &*ERR_TOO_MANY_KEYS),
        (
            "ErrInvalidSplitRegionRanges",
            &*ERR_INVALID_SPLIT_REGION_RANGES,
        ),
        ("ErrReorgPanic", &*ERR_REORG_PANIC),
        ("ErrFkColumnCannotDrop", &*ERR_FK_COLUMN_CANNOT_DROP),
        (
            "ErrFkColumnCannotDropChild",
            &*ERR_FK_COLUMN_CANNOT_DROP_CHILD,
        ),
        ("ErrFKIncompatibleColumns", &*ERR_FK_INCOMPATIBLE_COLUMNS),
        (
            "ErrOnlyOnRangeListPartition",
            &*ERR_ONLY_ON_RANGE_LIST_PARTITION,
        ),
        ("ErrWrongKeyColumn", &*ERR_WRONG_KEY_COLUMN),
        (
            "ErrWrongKeyColumnFunctionalIndex",
            &*ERR_WRONG_KEY_COLUMN_FUNCTIONAL_INDEX,
        ),
        (
            "ErrWrongFKOptionForGeneratedColumn",
            &*ERR_WRONG_FK_OPTION_FOR_GENERATED_COLUMN,
        ),
        (
            "ErrUnsupportedOnGeneratedColumn",
            &*ERR_UNSUPPORTED_ON_GENERATED_COLUMN,
        ),
        (
            "ErrGeneratedColumnNonPrior",
            &*ERR_GENERATED_COLUMN_NON_PRIOR,
        ),
        (
            "ErrDependentByGeneratedColumn",
            &*ERR_DEPENDENT_BY_GENERATED_COLUMN,
        ),
        ("ErrJSONUsedAsKey", &*ERR_JSON_USED_AS_KEY),
        ("ErrBlobCantHaveDefault", &*ERR_BLOB_CANT_HAVE_DEFAULT),
        ("ErrTooLongIndexComment", &*ERR_TOO_LONG_INDEX_COMMENT),
        ("ErrTooLongTableComment", &*ERR_TOO_LONG_TABLE_COMMENT),
        ("ErrTooLongFieldComment", &*ERR_TOO_LONG_FIELD_COMMENT),
        (
            "ErrTooLongTablePartitionComment",
            &*ERR_TOO_LONG_TABLE_PARTITION_COMMENT,
        ),
        ("ErrInvalidDefaultValue", &*ERR_INVALID_DEFAULT_VALUE),
        (
            "ErrDefValGeneratedNamedFunctionIsNotAllowed",
            &*ERR_DEF_VAL_GENERATED_NAMED_FUNCTION_IS_NOT_ALLOWED,
        ),
        (
            "ErrGeneratedColumnRefAutoInc",
            &*ERR_GENERATED_COLUMN_REF_AUTO_INC,
        ),
        (
            "ErrExpressionIndexCanNotRefer",
            &*ERR_EXPRESSION_INDEX_CAN_NOT_REFER,
        ),
        (
            "ErrGeneratedColumnFunctionIsNotAllowed",
            &*ERR_GENERATED_COLUMN_FUNCTION_IS_NOT_ALLOWED,
        ),
        (
            "ErrGeneratedColumnRowValueIsNotAllowed",
            &*ERR_GENERATED_COLUMN_ROW_VALUE_IS_NOT_ALLOWED,
        ),
        (
            "ErrFunctionalIndexFunctionIsNotAllowed",
            &*ERR_FUNCTIONAL_INDEX_FUNCTION_IS_NOT_ALLOWED,
        ),
        (
            "ErrFunctionalIndexRowValueIsNotAllowed",
            &*ERR_FUNCTIONAL_INDEX_ROW_VALUE_IS_NOT_ALLOWED,
        ),
        (
            "ErrWindowInvalidWindowFuncUse",
            &*ERR_WINDOW_INVALID_WINDOW_FUNC_USE,
        ),
        ("ErrDupKeyName", &*ERR_DUP_KEY_NAME),
        ("ErrFkDupName", &*ERR_FK_DUP_NAME),
        (
            "ErrPKIndexCantBeInvisible",
            &*ERR_PK_INDEX_CANT_BE_INVISIBLE,
        ),
        ("ErrColumnBadNull", &*ERR_COLUMN_BAD_NULL),
        ("ErrBadField", &*ERR_BAD_FIELD),
        ("ErrCantRemoveAllFields", &*ERR_CANT_REMOVE_ALL_FIELDS),
        ("ErrCantDropFieldOrKey", &*ERR_CANT_DROP_FIELD_OR_KEY),
        ("ErrInvalidOnUpdate", &*ERR_INVALID_ON_UPDATE),
        ("ErrTooLongIdent", &*ERR_TOO_LONG_IDENT),
        ("ErrWrongDBName", &*ERR_WRONG_DB_NAME),
        ("ErrWrongTableName", &*ERR_WRONG_TABLE_NAME),
        ("ErrWrongColumnName", &*ERR_WRONG_COLUMN_NAME),
        ("ErrWrongPartitionName", &*ERR_WRONG_PARTITION_NAME),
        ("ErrWrongUsage", &*ERR_WRONG_USAGE),
        ("ErrInvalidGroupFuncUse", &*ERR_INVALID_GROUP_FUNC_USE),
        ("ErrTableMustHaveColumns", &*ERR_TABLE_MUST_HAVE_COLUMNS),
        ("ErrWrongNameForIndex", &*ERR_WRONG_NAME_FOR_INDEX),
        ("ErrUnknownCharacterSet", &*ERR_UNKNOWN_CHARACTER_SET),
        ("ErrUnknownCollation", &*ERR_UNKNOWN_COLLATION),
        (
            "ErrCollationCharsetMismatch",
            &*ERR_COLLATION_CHARSET_MISMATCH,
        ),
        ("ErrPrimaryCantHaveNull", &*ERR_PRIMARY_CANT_HAVE_NULL),
        ("ErrErrorOnRename", &*ERR_ERROR_ON_RENAME),
        ("ErrViewSelectClause", &*ERR_VIEW_SELECT_CLAUSE),
        ("ErrViewSelectVariable", &*ERR_VIEW_SELECT_VARIABLE),
        (
            "ErrNotAllowedTypeInPartition",
            &*ERR_NOT_ALLOWED_TYPE_IN_PARTITION,
        ),
        (
            "ErrPartitionMgmtOnNonpartitioned",
            &*ERR_PARTITION_MGMT_ON_NONPARTITIONED,
        ),
        (
            "ErrDropPartitionNonExistent",
            &*ERR_DROP_PARTITION_NON_EXISTENT,
        ),
        ("ErrSameNamePartition", &*ERR_SAME_NAME_PARTITION),
        ("ErrSameNamePartitionField", &*ERR_SAME_NAME_PARTITION_FIELD),
        ("ErrRangeNotIncreasing", &*ERR_RANGE_NOT_INCREASING),
        ("ErrPartitionMaxvalue", &*ERR_PARTITION_MAXVALUE),
        ("ErrMaxvalueInValuesIn", &*ERR_MAXVALUE_IN_VALUES_IN),
        ("ErrDropLastPartition", &*ERR_DROP_LAST_PARTITION),
        ("ErrTooManyPartitions", &*ERR_TOO_MANY_PARTITIONS),
        ("ErrPartitionConstDomain", &*ERR_PARTITION_CONST_DOMAIN),
        (
            "ErrPartitionFunctionIsNotAllowed",
            &*ERR_PARTITION_FUNCTION_IS_NOT_ALLOWED,
        ),
        (
            "ErrPartitionFuncNotAllowed",
            &*ERR_PARTITION_FUNC_NOT_ALLOWED,
        ),
        (
            "ErrUniqueKeyNeedAllFieldsInPf",
            &*ERR_UNIQUE_KEY_NEED_ALL_FIELDS_IN_PF,
        ),
        (
            "ErrWrongExprInPartitionFunc",
            &*ERR_WRONG_EXPR_IN_PARTITION_FUNC,
        ),
        ("ErrWarnDataTruncated", &*ERR_WARN_DATA_TRUNCATED),
        (
            "ErrCoalesceOnlyOnHashPartition",
            &*ERR_COALESCE_ONLY_ON_HASH_PARTITION,
        ),
        ("ErrViewWrongList", &*ERR_VIEW_WRONG_LIST),
        (
            "ErrAlterOperationNotSupported",
            &*ERR_ALTER_OPERATION_NOT_SUPPORTED,
        ),
        ("ErrWrongObject", &*ERR_WRONG_OBJECT),
        ("ErrTableCantHandleFt", &*ERR_TABLE_CANT_HANDLE_FT),
        ("ErrFieldNotFoundPart", &*ERR_FIELD_NOT_FOUND_PART),
        ("ErrWrongTypeColumnValue", &*ERR_WRONG_TYPE_COLUMN_VALUE),
        ("ErrValuesIsNotIntType", &*ERR_VALUES_IS_NOT_INT_TYPE),
        (
            "ErrFunctionalIndexPrimaryKey",
            &*ERR_FUNCTIONAL_INDEX_PRIMARY_KEY,
        ),
        ("ErrFunctionalIndexOnField", &*ERR_FUNCTIONAL_INDEX_ON_FIELD),
        ("ErrInvalidAutoRandom", &*ERR_INVALID_AUTO_RANDOM),
        (
            "ErrUnsupportedConstraintCheck",
            &*ERR_UNSUPPORTED_CONSTRAINT_CHECK,
        ),
        ("ErrDerivedMustHaveAlias", &*ERR_DERIVED_MUST_HAVE_ALIAS),
        ("ErrNullInValuesLessThan", &*ERR_NULL_IN_VALUES_LESS_THAN),
        ("ErrSequenceRunOut", &*ERR_SEQUENCE_RUN_OUT),
        ("ErrSequenceInvalidData", &*ERR_SEQUENCE_INVALID_DATA),
        ("ErrSequenceAccessFail", &*ERR_SEQUENCE_ACCESS_FAIL),
        ("ErrNotSequence", &*ERR_NOT_SEQUENCE),
        ("ErrUnknownSequence", &*ERR_UNKNOWN_SEQUENCE),
        (
            "ErrSequenceUnsupportedTableOption",
            &*ERR_SEQUENCE_UNSUPPORTED_TABLE_OPTION,
        ),
        (
            "ErrColumnTypeUnsupportedNextValue",
            &*ERR_COLUMN_TYPE_UNSUPPORTED_NEXT_VALUE,
        ),
        (
            "ErrAddColumnWithSequenceAsDefault",
            &*ERR_ADD_COLUMN_WITH_SEQUENCE_AS_DEFAULT,
        ),
        (
            "ErrPartitionExchangePartTable",
            &*ERR_PARTITION_EXCHANGE_PART_TABLE,
        ),
        (
            "ErrPartitionExchangeTempTable",
            &*ERR_PARTITION_EXCHANGE_TEMP_TABLE,
        ),
        (
            "ErrTablesDifferentMetadata",
            &*ERR_TABLES_DIFFERENT_METADATA,
        ),
        (
            "ErrRowDoesNotMatchPartition",
            &*ERR_ROW_DOES_NOT_MATCH_PARTITION,
        ),
        (
            "ErrPartitionExchangeForeignKey",
            &*ERR_PARTITION_EXCHANGE_FOREIGN_KEY,
        ),
        ("ErrCheckNoSuchTable", &*ERR_CHECK_NO_SUCH_TABLE),
        (
            "ErrPartitionExchangeDifferentOption",
            &*ERR_PARTITION_EXCHANGE_DIFFERENT_OPTION,
        ),
        (
            "ErrTableOptionUnionUnsupported",
            &*ERR_TABLE_OPTION_UNION_UNSUPPORTED,
        ),
        (
            "ErrTableOptionInsertMethodUnsupported",
            &*ERR_TABLE_OPTION_INSERT_METHOD_UNSUPPORTED,
        ),
        (
            "ErrInvalidPlacementPolicyCheck",
            &*ERR_INVALID_PLACEMENT_POLICY_CHECK,
        ),
        (
            "ErrPlacementPolicyWithDirectOption",
            &*ERR_PLACEMENT_POLICY_WITH_DIRECT_OPTION,
        ),
        ("ErrPlacementPolicyInUse", &*ERR_PLACEMENT_POLICY_IN_USE),
        (
            "ErrMultipleDefConstInListPart",
            &*ERR_MULTIPLE_DEF_CONST_IN_LIST_PART,
        ),
        ("ErrTruncatedWrongValue", &*ERR_TRUNCATED_WRONG_VALUE),
        ("ErrWarnDataOutOfRange", &*ERR_WARN_DATA_OUT_OF_RANGE),
        ("ErrTooLongValueForType", &*ERR_TOO_LONG_VALUE_FOR_TYPE),
        ("ErrUnknownEngine", &*ERR_UNKNOWN_ENGINE),
        ("ErrPartitionNoTemporary", &*ERR_PARTITION_NO_TEMPORARY),
        ("ErrOptOnTemporaryTable", &*ERR_OPT_ON_TEMPORARY_TABLE),
        ("ErrOptOnCacheTable", &*ERR_OPT_ON_CACHE_TABLE),
        ("ErrInvalidAttributesSpec", &*ERR_INVALID_ATTRIBUTES_SPEC),
        (
            "ErrFunctionalIndexOnJSONOrGeometryFunction",
            &*ERR_FUNCTIONAL_INDEX_ON_JSON_OR_GEOMETRY_FUNCTION,
        ),
        (
            "ErrDependentByFunctionalIndex",
            &*ERR_DEPENDENT_BY_FUNCTIONAL_INDEX,
        ),
        ("ErrFunctionalIndexOnBlob", &*ERR_FUNCTIONAL_INDEX_ON_BLOB),
        (
            "ErrDependentByPartitionFunctional",
            &*ERR_DEPENDENT_BY_PARTITION_FUNCTIONAL,
        ),
        ("ErrAutoConvert", &*ERR_AUTO_CONVERT),
        ("ErrWrongStringLength", &*ERR_WRONG_STRING_LENGTH),
        (
            "ErrBinlogUnsafeSystemFunction",
            &*ERR_BINLOG_UNSAFE_SYSTEM_FUNCTION,
        ),
        ("ErrDDLJobNotFound", &*ERR_DDL_JOB_NOT_FOUND),
        ("ErrCancelFinishedDDLJob", &*ERR_CANCEL_FINISHED_DDL_JOB),
        ("ErrCannotCancelDDLJob", &*ERR_CANNOT_CANCEL_DDL_JOB),
        ("ErrCannotPauseDDLJob", &*ERR_CANNOT_PAUSE_DDL_JOB),
        ("ErrCannotResumeDDLJob", &*ERR_CANNOT_RESUME_DDL_JOB),
        ("ErrDDLSetting", &*ERR_DDL_SETTING),
        ("ErrIngestFailed", &*ERR_INGEST_FAILED),
        ("ErrIngestCheckEnvFailed", &*ERR_INGEST_CHECK_ENV_FAILED),
        ("ErrColumnInChange", &*ERR_COLUMN_IN_CHANGE),
        (
            "ErrDropIndexNeededInForeignKey",
            &*ERR_DROP_INDEX_NEEDED_IN_FOREIGN_KEY,
        ),
        (
            "ErrForeignKeyCannotDropParent",
            &*ERR_FOREIGN_KEY_CANNOT_DROP_PARENT,
        ),
        (
            "ErrTruncateIllegalForeignKey",
            &*ERR_TRUNCATE_ILLEGAL_FOREIGN_KEY,
        ),
        (
            "ErrForeignKeyColumnCannotChange",
            &*ERR_FOREIGN_KEY_COLUMN_CANNOT_CHANGE,
        ),
        (
            "ErrForeignKeyColumnCannotChangeChild",
            &*ERR_FOREIGN_KEY_COLUMN_CANNOT_CHANGE_CHILD,
        ),
        ("ErrNoReferencedRow2", &*ERR_NO_REFERENCED_ROW2),
        (
            "ErrUnsupportedColumnInTTLConfig",
            &*ERR_UNSUPPORTED_COLUMN_IN_TTL_CONFIG,
        ),
        ("ErrTTLColumnCannotDrop", &*ERR_TTL_COLUMN_CANNOT_DROP),
        (
            "ErrSetTTLOptionForNonTTLTable",
            &*ERR_SET_TTL_OPTION_FOR_NON_TTL_TABLE,
        ),
        (
            "ErrTempTableNotAllowedWithTTL",
            &*ERR_TEMP_TABLE_NOT_ALLOWED_WITH_TTL,
        ),
        (
            "ErrUnsupportedTTLReferencedByFK",
            &*ERR_UNSUPPORTED_TTL_REFERENCED_BY_FK,
        ),
        (
            "ErrUnsupportedPrimaryKeyTypeWithTTL",
            &*ERR_UNSUPPORTED_PRIMARY_KEY_TYPE_WITH_TTL,
        ),
        ("ErrNotSupportedYet", &*ERR_NOT_SUPPORTED_YET),
        (
            "ErrColumnCheckConstraintReferOther",
            &*ERR_COLUMN_CHECK_CONSTRAINT_REFER_OTHER,
        ),
        (
            "ErrTableCheckConstraintReferUnknown",
            &*ERR_TABLE_CHECK_CONSTRAINT_REFER_UNKNOWN,
        ),
        ("ErrConstraintNotFound", &*ERR_CONSTRAINT_NOT_FOUND),
        (
            "ErrCheckConstraintIsViolated",
            &*ERR_CHECK_CONSTRAINT_IS_VIOLATED,
        ),
        (
            "ErrCheckConstraintNamedFuncIsNotAllowed",
            &*ERR_CHECK_CONSTRAINT_NAMED_FUNC_IS_NOT_ALLOWED,
        ),
        (
            "ErrCheckConstraintFuncIsNotAllowed",
            &*ERR_CHECK_CONSTRAINT_FUNC_IS_NOT_ALLOWED,
        ),
        (
            "ErrCheckConstraintVariables",
            &*ERR_CHECK_CONSTRAINT_VARIABLES,
        ),
        (
            "ErrCheckConstraintRefersAutoIncrementColumn",
            &*ERR_CHECK_CONSTRAINT_REFERS_AUTO_INCREMENT_COLUMN,
        ),
        (
            "ErrCheckConstraintUsingFKReferActionColumn",
            &*ERR_CHECK_CONSTRAINT_USING_FK_REFER_ACTION_COLUMN,
        ),
        (
            "ErrNonBooleanExprForCheckConstraint",
            &*ERR_NON_BOOLEAN_EXPR_FOR_CHECK_CONSTRAINT,
        ),
        ("ErrCheckConstraintDupName", &*ERR_CHECK_CONSTRAINT_DUP_NAME),
        (
            "ErrGlobalIndexNotExplicitlySet",
            &*ERR_GLOBAL_INDEX_NOT_EXPLICITLY_SET,
        ),
        (
            "ErrWarnGlobalIndexNeedManuallyAnalyze",
            &*ERR_WARN_GLOBAL_INDEX_NEED_MANUALLY_ANALYZE,
        ),
        (
            "ErrEngineAttributeInvalidFormat",
            &*ERR_ENGINE_ATTRIBUTE_INVALID_FORMAT,
        ),
        (
            "ErrStorageClassInvalidSpec",
            &*ERR_STORAGE_CLASS_INVALID_SPEC,
        ),
        ("ErrInvalidTableAffinity", &*ERR_INVALID_TABLE_AFFINITY),
        ("ErrForbiddenDDL", &*ERR_FORBIDDEN_DDL),
        ("ErrRunMultiSchemaChanges", &*ERR_RUN_MULTI_SCHEMA_CHANGES),
        ("ErrOperateSameColumn", &*ERR_OPERATE_SAME_COLUMN),
        ("ErrOperateSameIndex", &*ERR_OPERATE_SAME_INDEX),
        ("ErrWaitReorgTimeout", &*ERR_WAIT_REORG_TIMEOUT),
        (
            "ErrUnsupportedAddColumnarIndex",
            &*ERR_UNSUPPORTED_ADD_COLUMNAR_INDEX,
        ),
        (
            "ErrUnsupportedAddVectorIndex",
            &*ERR_UNSUPPORTED_ADD_VECTOR_INDEX,
        ),
        ("ErrCantDropColWithIndex", &*ERR_CANT_DROP_COL_WITH_INDEX),
        (
            "ErrCantDropColWithAutoInc",
            &*ERR_CANT_DROP_COL_WITH_AUTO_INC,
        ),
        ("ErrUnsupportedAddColumn", &*ERR_UNSUPPORTED_ADD_COLUMN),
        (
            "ErrUnsupportedModifyColumn",
            &*ERR_UNSUPPORTED_MODIFY_COLUMN,
        ),
        (
            "ErrUnsupportedModifyCharset",
            &*ERR_UNSUPPORTED_MODIFY_CHARSET,
        ),
        (
            "ErrUnsupportedModifyCollation",
            &*ERR_UNSUPPORTED_MODIFY_COLLATION,
        ),
        ("ErrUnsupportedPKHandle", &*ERR_UNSUPPORTED_PK_HANDLE),
        ("ErrUnsupportedCharset", &*ERR_UNSUPPORTED_CHARSET),
        (
            "ErrUnsupportedShardRowIDBits",
            &*ERR_UNSUPPORTED_SHARD_ROW_ID_BITS,
        ),
        (
            "ErrUnsupportedAlterTableWithValidation",
            &*ERR_UNSUPPORTED_ALTER_TABLE_WITH_VALIDATION,
        ),
        (
            "ErrUnsupportedAlterTableWithoutValidation",
            &*ERR_UNSUPPORTED_ALTER_TABLE_WITHOUT_VALIDATION,
        ),
        (
            "ErrUnsupportedAlterTableOption",
            &*ERR_UNSUPPORTED_ALTER_TABLE_OPTION,
        ),
        (
            "ErrUnsupportedAlterCacheForSysTable",
            &*ERR_UNSUPPORTED_ALTER_CACHE_FOR_SYS_TABLE,
        ),
        (
            "ErrUnsupportedAddPartialIndex",
            &*ERR_UNSUPPORTED_ADD_PARTIAL_INDEX,
        ),
        (
            "ErrUnsupportedAddPartition",
            &*ERR_UNSUPPORTED_ADD_PARTITION,
        ),
        (
            "ErrUnsupportedCoalescePartition",
            &*ERR_UNSUPPORTED_COALESCE_PARTITION,
        ),
        (
            "ErrUnsupportedReorganizePartition",
            &*ERR_UNSUPPORTED_REORGANIZE_PARTITION,
        ),
        (
            "ErrUnsupportedCheckPartition",
            &*ERR_UNSUPPORTED_CHECK_PARTITION,
        ),
        (
            "ErrUnsupportedOptimizePartition",
            &*ERR_UNSUPPORTED_OPTIMIZE_PARTITION,
        ),
        (
            "ErrUnsupportedRebuildPartition",
            &*ERR_UNSUPPORTED_REBUILD_PARTITION,
        ),
        (
            "ErrUnsupportedRemovePartition",
            &*ERR_UNSUPPORTED_REMOVE_PARTITION,
        ),
        (
            "ErrUnsupportedRepairPartition",
            &*ERR_UNSUPPORTED_REPAIR_PARTITION,
        ),
        (
            "ErrUnsupportedPartitionByRangeColumns",
            &*ERR_UNSUPPORTED_PARTITION_BY_RANGE_COLUMNS,
        ),
        (
            "ErrUnsupportedCreatePartition",
            &*ERR_UNSUPPORTED_CREATE_PARTITION,
        ),
        ("ErrUnsupportedIndexType", &*ERR_UNSUPPORTED_INDEX_TYPE),
        ("ErrInvalidDDLState", &*ERR_INVALID_DDL_STATE),
        (
            "ErrUnsupportedModifyPrimaryKey",
            &*ERR_UNSUPPORTED_MODIFY_PRIMARY_KEY,
        ),
        ("ErrConflictingDeclarations", &*ERR_CONFLICTING_DECLARATIONS),
        (
            "ErrUnsupportedExpressionIndex",
            &*ERR_UNSUPPORTED_EXPRESSION_INDEX,
        ),
        (
            "ErrUnsupportedPartitionType",
            &*ERR_UNSUPPORTED_PARTITION_TYPE,
        ),
        (
            "ErrExchangePartitionDisabled",
            &*ERR_EXCHANGE_PARTITION_DISABLED,
        ),
        (
            "ErrUnsupportedOnCommitPreserve",
            &*ERR_UNSUPPORTED_ON_COMMIT_PRESERVE,
        ),
        (
            "ErrUnsupportedClusteredSecondaryKey",
            &*ERR_UNSUPPORTED_CLUSTERED_SECONDARY_KEY,
        ),
        (
            "ErrUnsupportedLocalTempTableDDL",
            &*ERR_UNSUPPORTED_LOCAL_TEMP_TABLE_DDL,
        ),
        (
            "ErrUnsupportedAlterTableSpec",
            &*ERR_UNSUPPORTED_ALTER_TABLE_SPEC,
        ),
        ("ErrGeneralUnsupportedDDL", &*ERR_GENERAL_UNSUPPORTED_DDL),
        (
            "ErrAlterTiFlashModeForTableWithoutTiFlashReplica",
            &*ERR_ALTER_TI_FLASH_MODE_FOR_TABLE_WITHOUT_TI_FLASH_REPLICA,
        ),
        (
            "ErrUnsupportedTiFlashOperationForSysOrMemTable",
            &*ERR_UNSUPPORTED_TI_FLASH_OPERATION_FOR_SYS_OR_MEM_TABLE,
        ),
        (
            "ErrUnsupportedTiFlashOperationForUnsupportedCharsetTable",
            &*ERR_UNSUPPORTED_TI_FLASH_OPERATION_FOR_UNSUPPORTED_CHARSET_TABLE,
        ),
        ("ErrTiFlashBackfillIndex", &*ERR_TI_FLASH_BACKFILL_INDEX),
        (
            "ErrWarnDeprecatedIntegerDisplayWidth",
            &*ERR_WARN_DEPRECATED_INTEGER_DISPLAY_WIDTH,
        ),
        ("ErrWarnDeprecatedZerofill", &*ERR_WARN_DEPRECATED_ZEROFILL),
        ("ErrUnsupportedDistTask", &*ERR_UNSUPPORTED_DIST_TASK),
        (
            "ErrCannotSetAffinityOnTable",
            &*ERR_CANNOT_SET_AFFINITY_ON_TABLE,
        ),
    ]
}
