// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");

//! Message catalog entries, part 4 of 4 (see `errname/mod.rs`).

use super::consts_4::*;
use super::errcode;
use super::CatalogEntry;

pub(super) const CATALOG_4: &[CatalogEntry] = &[
    CatalogEntry {
        name: "ErrNotImplemented",
        code: errcode::ErrNotImplemented,
        message: ErrNotImplemented,
    },
    CatalogEntry {
        name: "ErrInfoSchemaExpired",
        code: errcode::ErrInfoSchemaExpired,
        message: ErrInfoSchemaExpired,
    },
    CatalogEntry {
        name: "ErrInfoSchemaChanged",
        code: errcode::ErrInfoSchemaChanged,
        message: ErrInfoSchemaChanged,
    },
    CatalogEntry {
        name: "ErrBadNumber",
        code: errcode::ErrBadNumber,
        message: ErrBadNumber,
    },
    CatalogEntry {
        name: "ErrCastAsSignedOverflow",
        code: errcode::ErrCastAsSignedOverflow,
        message: ErrCastAsSignedOverflow,
    },
    CatalogEntry {
        name: "ErrCastNegIntAsUnsigned",
        code: errcode::ErrCastNegIntAsUnsigned,
        message: ErrCastNegIntAsUnsigned,
    },
    CatalogEntry {
        name: "ErrInvalidYearFormat",
        code: errcode::ErrInvalidYearFormat,
        message: ErrInvalidYearFormat,
    },
    CatalogEntry {
        name: "ErrInvalidYear",
        code: errcode::ErrInvalidYear,
        message: ErrInvalidYear,
    },
    CatalogEntry {
        name: "ErrIncorrectDatetimeValue",
        code: errcode::ErrIncorrectDatetimeValue,
        message: ErrIncorrectDatetimeValue,
    },
    CatalogEntry {
        name: "ErrInvalidTimeFormat",
        code: errcode::ErrInvalidTimeFormat,
        message: ErrInvalidTimeFormat,
    },
    CatalogEntry {
        name: "ErrInvalidWeekModeFormat",
        code: errcode::ErrInvalidWeekModeFormat,
        message: ErrInvalidWeekModeFormat,
    },
    CatalogEntry {
        name: "ErrFieldGetDefaultFailed",
        code: errcode::ErrFieldGetDefaultFailed,
        message: ErrFieldGetDefaultFailed,
    },
    CatalogEntry {
        name: "ErrIndexOutBound",
        code: errcode::ErrIndexOutBound,
        message: ErrIndexOutBound,
    },
    CatalogEntry {
        name: "ErrUnsupportedOp",
        code: errcode::ErrUnsupportedOp,
        message: ErrUnsupportedOp,
    },
    CatalogEntry {
        name: "ErrRowNotFound",
        code: errcode::ErrRowNotFound,
        message: ErrRowNotFound,
    },
    CatalogEntry {
        name: "ErrTableStateCantNone",
        code: errcode::ErrTableStateCantNone,
        message: ErrTableStateCantNone,
    },
    CatalogEntry {
        name: "ErrColumnStateCantNone",
        code: errcode::ErrColumnStateCantNone,
        message: ErrColumnStateCantNone,
    },
    CatalogEntry {
        name: "ErrColumnStateNonPublic",
        code: errcode::ErrColumnStateNonPublic,
        message: ErrColumnStateNonPublic,
    },
    CatalogEntry {
        name: "ErrIndexStateCantNone",
        code: errcode::ErrIndexStateCantNone,
        message: ErrIndexStateCantNone,
    },
    CatalogEntry {
        name: "ErrInvalidRecordKey",
        code: errcode::ErrInvalidRecordKey,
        message: ErrInvalidRecordKey,
    },
    CatalogEntry {
        name: "ErrUnsupportedValueForVar",
        code: errcode::ErrUnsupportedValueForVar,
        message: ErrUnsupportedValueForVar,
    },
    CatalogEntry {
        name: "ErrUnsupportedIsolationLevel",
        code: errcode::ErrUnsupportedIsolationLevel,
        message: ErrUnsupportedIsolationLevel,
    },
    CatalogEntry {
        name: "ErrInvalidDDLWorker",
        code: errcode::ErrInvalidDDLWorker,
        message: ErrInvalidDDLWorker,
    },
    CatalogEntry {
        name: "ErrUnsupportedDDLOperation",
        code: errcode::ErrUnsupportedDDLOperation,
        message: ErrUnsupportedDDLOperation,
    },
    CatalogEntry {
        name: "ErrNotOwner",
        code: errcode::ErrNotOwner,
        message: ErrNotOwner,
    },
    CatalogEntry {
        name: "ErrCantDecodeRecord",
        code: errcode::ErrCantDecodeRecord,
        message: ErrCantDecodeRecord,
    },
    CatalogEntry {
        name: "ErrInvalidDDLJob",
        code: errcode::ErrInvalidDDLJob,
        message: ErrInvalidDDLJob,
    },
    CatalogEntry {
        name: "ErrInvalidDDLJobFlag",
        code: errcode::ErrInvalidDDLJobFlag,
        message: ErrInvalidDDLJobFlag,
    },
    CatalogEntry {
        name: "ErrWaitReorgTimeout",
        code: errcode::ErrWaitReorgTimeout,
        message: ErrWaitReorgTimeout,
    },
    CatalogEntry {
        name: "ErrInvalidStoreVersion",
        code: errcode::ErrInvalidStoreVersion,
        message: ErrInvalidStoreVersion,
    },
    CatalogEntry {
        name: "ErrUnknownTypeLength",
        code: errcode::ErrUnknownTypeLength,
        message: ErrUnknownTypeLength,
    },
    CatalogEntry {
        name: "ErrUnknownFractionLength",
        code: errcode::ErrUnknownFractionLength,
        message: ErrUnknownFractionLength,
    },
    CatalogEntry {
        name: "ErrInvalidDDLJobVersion",
        code: errcode::ErrInvalidDDLJobVersion,
        message: ErrInvalidDDLJobVersion,
    },
    CatalogEntry {
        name: "ErrInvalidSplitRegionRanges",
        code: errcode::ErrInvalidSplitRegionRanges,
        message: ErrInvalidSplitRegionRanges,
    },
    CatalogEntry {
        name: "ErrReorgPanic",
        code: errcode::ErrReorgPanic,
        message: ErrReorgPanic,
    },
    CatalogEntry {
        name: "ErrInvalidDDLState",
        code: errcode::ErrInvalidDDLState,
        message: ErrInvalidDDLState,
    },
    CatalogEntry {
        name: "ErrCancelledDDLJob",
        code: errcode::ErrCancelledDDLJob,
        message: ErrCancelledDDLJob,
    },
    CatalogEntry {
        name: "ErrRepairTable",
        code: errcode::ErrRepairTable,
        message: ErrRepairTable,
    },
    CatalogEntry {
        name: "ErrLoadPrivilege",
        code: errcode::ErrLoadPrivilege,
        message: ErrLoadPrivilege,
    },
    CatalogEntry {
        name: "ErrInvalidPrivilegeType",
        code: errcode::ErrInvalidPrivilegeType,
        message: ErrInvalidPrivilegeType,
    },
    CatalogEntry {
        name: "ErrUnknownFieldType",
        code: errcode::ErrUnknownFieldType,
        message: ErrUnknownFieldType,
    },
    CatalogEntry {
        name: "ErrInvalidSequence",
        code: errcode::ErrInvalidSequence,
        message: ErrInvalidSequence,
    },
    CatalogEntry {
        name: "ErrInvalidType",
        code: errcode::ErrInvalidType,
        message: ErrInvalidType,
    },
    CatalogEntry {
        name: "ErrCantGetValidID",
        code: errcode::ErrCantGetValidID,
        message: ErrCantGetValidID,
    },
    CatalogEntry {
        name: "ErrCantSetToNull",
        code: errcode::ErrCantSetToNull,
        message: ErrCantSetToNull,
    },
    CatalogEntry {
        name: "ErrSnapshotTooOld",
        code: errcode::ErrSnapshotTooOld,
        message: ErrSnapshotTooOld,
    },
    CatalogEntry {
        name: "ErrInvalidTableID",
        code: errcode::ErrInvalidTableID,
        message: ErrInvalidTableID,
    },
    CatalogEntry {
        name: "ErrInvalidAutoRandom",
        code: errcode::ErrInvalidAutoRandom,
        message: ErrInvalidAutoRandom,
    },
    CatalogEntry {
        name: "ErrInvalidHashKeyFlag",
        code: errcode::ErrInvalidHashKeyFlag,
        message: ErrInvalidHashKeyFlag,
    },
    CatalogEntry {
        name: "ErrInvalidListIndex",
        code: errcode::ErrInvalidListIndex,
        message: ErrInvalidListIndex,
    },
    CatalogEntry {
        name: "ErrInvalidListMetaData",
        code: errcode::ErrInvalidListMetaData,
        message: ErrInvalidListMetaData,
    },
    CatalogEntry {
        name: "ErrWriteOnSnapshot",
        code: errcode::ErrWriteOnSnapshot,
        message: ErrWriteOnSnapshot,
    },
    CatalogEntry {
        name: "ErrInvalidKey",
        code: errcode::ErrInvalidKey,
        message: ErrInvalidKey,
    },
    CatalogEntry {
        name: "ErrInvalidIndexKey",
        code: errcode::ErrInvalidIndexKey,
        message: ErrInvalidIndexKey,
    },
    CatalogEntry {
        name: "ErrDataInconsistent",
        code: errcode::ErrDataInconsistent,
        message: ErrDataInconsistent,
    },
    CatalogEntry {
        name: "ErrDDLReorgElementNotExist",
        code: errcode::ErrDDLReorgElementNotExist,
        message: ErrDDLReorgElementNotExist,
    },
    CatalogEntry {
        name: "ErrDDLJobNotFound",
        code: errcode::ErrDDLJobNotFound,
        message: ErrDDLJobNotFound,
    },
    CatalogEntry {
        name: "ErrCancelFinishedDDLJob",
        code: errcode::ErrCancelFinishedDDLJob,
        message: ErrCancelFinishedDDLJob,
    },
    CatalogEntry {
        name: "ErrCannotCancelDDLJob",
        code: errcode::ErrCannotCancelDDLJob,
        message: ErrCannotCancelDDLJob,
    },
    CatalogEntry {
        name: "ErrUnknownAllocatorType",
        code: errcode::ErrUnknownAllocatorType,
        message: ErrUnknownAllocatorType,
    },
    CatalogEntry {
        name: "ErrAutoRandReadFailed",
        code: errcode::ErrAutoRandReadFailed,
        message: ErrAutoRandReadFailed,
    },
    CatalogEntry {
        name: "ErrInvalidIncrementAndOffset",
        code: errcode::ErrInvalidIncrementAndOffset,
        message: ErrInvalidIncrementAndOffset,
    },
    CatalogEntry {
        name: "ErrDataInconsistentMismatchCount",
        code: errcode::ErrDataInconsistentMismatchCount,
        message: ErrDataInconsistentMismatchCount,
    },
    CatalogEntry {
        name: "ErrDataInconsistentMismatchIndex",
        code: errcode::ErrDataInconsistentMismatchIndex,
        message: ErrDataInconsistentMismatchIndex,
    },
    CatalogEntry {
        name: "ErrInconsistentRowValue",
        code: errcode::ErrInconsistentRowValue,
        message: ErrInconsistentRowValue,
    },
    CatalogEntry {
        name: "ErrInconsistentHandle",
        code: errcode::ErrInconsistentHandle,
        message: ErrInconsistentHandle,
    },
    CatalogEntry {
        name: "ErrInconsistentIndexedValue",
        code: errcode::ErrInconsistentIndexedValue,
        message: ErrInconsistentIndexedValue,
    },
    CatalogEntry {
        name: "ErrAssertionFailed",
        code: errcode::ErrAssertionFailed,
        message: ErrAssertionFailed,
    },
    CatalogEntry {
        name: "ErrInstanceScope",
        code: errcode::ErrInstanceScope,
        message: ErrInstanceScope,
    },
    CatalogEntry {
        name: "ErrNonTransactionalJobFailure",
        code: errcode::ErrNonTransactionalJobFailure,
        message: ErrNonTransactionalJobFailure,
    },
    CatalogEntry {
        name: "ErrSettingNoopVariable",
        code: errcode::ErrSettingNoopVariable,
        message: ErrSettingNoopVariable,
    },
    CatalogEntry {
        name: "ErrGettingNoopVariable",
        code: errcode::ErrGettingNoopVariable,
        message: ErrGettingNoopVariable,
    },
    CatalogEntry {
        name: "ErrCannotMigrateSession",
        code: errcode::ErrCannotMigrateSession,
        message: ErrCannotMigrateSession,
    },
    CatalogEntry {
        name: "ErrLazyUniquenessCheckFailure",
        code: errcode::ErrLazyUniquenessCheckFailure,
        message: ErrLazyUniquenessCheckFailure,
    },
    CatalogEntry {
        name: "ErrUnsupportedColumnInTTLConfig",
        code: errcode::ErrUnsupportedColumnInTTLConfig,
        message: ErrUnsupportedColumnInTTLConfig,
    },
    CatalogEntry {
        name: "ErrTTLColumnCannotDrop",
        code: errcode::ErrTTLColumnCannotDrop,
        message: ErrTTLColumnCannotDrop,
    },
    CatalogEntry {
        name: "ErrSetTTLOptionForNonTTLTable",
        code: errcode::ErrSetTTLOptionForNonTTLTable,
        message: ErrSetTTLOptionForNonTTLTable,
    },
    CatalogEntry {
        name: "ErrTempTableNotAllowedWithTTL",
        code: errcode::ErrTempTableNotAllowedWithTTL,
        message: ErrTempTableNotAllowedWithTTL,
    },
    CatalogEntry {
        name: "ErrUnsupportedTTLReferencedByFK",
        code: errcode::ErrUnsupportedTTLReferencedByFK,
        message: ErrUnsupportedTTLReferencedByFK,
    },
    CatalogEntry {
        name: "ErrUnsupportedPrimaryKeyTypeWithTTL",
        code: errcode::ErrUnsupportedPrimaryKeyTypeWithTTL,
        message: ErrUnsupportedPrimaryKeyTypeWithTTL,
    },
    CatalogEntry {
        name: "ErrLoadDataFromServerDisk",
        code: errcode::ErrLoadDataFromServerDisk,
        message: ErrLoadDataFromServerDisk,
    },
    CatalogEntry {
        name: "ErrLoadParquetFromLocal",
        code: errcode::ErrLoadParquetFromLocal,
        message: ErrLoadParquetFromLocal,
    },
    CatalogEntry {
        name: "ErrLoadDataEmptyPath",
        code: errcode::ErrLoadDataEmptyPath,
        message: ErrLoadDataEmptyPath,
    },
    CatalogEntry {
        name: "ErrLoadDataUnsupportedFormat",
        code: errcode::ErrLoadDataUnsupportedFormat,
        message: ErrLoadDataUnsupportedFormat,
    },
    CatalogEntry {
        name: "ErrLoadDataInvalidURI",
        code: errcode::ErrLoadDataInvalidURI,
        message: ErrLoadDataInvalidURI,
    },
    CatalogEntry {
        name: "ErrLoadDataCantAccess",
        code: errcode::ErrLoadDataCantAccess,
        message: ErrLoadDataCantAccess,
    },
    CatalogEntry {
        name: "ErrLoadDataCantRead",
        code: errcode::ErrLoadDataCantRead,
        message: ErrLoadDataCantRead,
    },
    CatalogEntry {
        name: "ErrLoadDataWrongFormatConfig",
        code: errcode::ErrLoadDataWrongFormatConfig,
        message: ErrLoadDataWrongFormatConfig,
    },
    CatalogEntry {
        name: "ErrUnknownOption",
        code: errcode::ErrUnknownOption,
        message: ErrUnknownOption,
    },
    CatalogEntry {
        name: "ErrInvalidOptionVal",
        code: errcode::ErrInvalidOptionVal,
        message: ErrInvalidOptionVal,
    },
    CatalogEntry {
        name: "ErrDuplicateOption",
        code: errcode::ErrDuplicateOption,
        message: ErrDuplicateOption,
    },
    CatalogEntry {
        name: "ErrLoadDataUnsupportedOption",
        code: errcode::ErrLoadDataUnsupportedOption,
        message: ErrLoadDataUnsupportedOption,
    },
    CatalogEntry {
        name: "ErrLoadDataDuplicateKeyConflict",
        code: errcode::ErrLoadDataDuplicateKeyConflict,
        message: ErrLoadDataDuplicateKeyConflict,
    },
    CatalogEntry {
        name: "ErrLoadDataJobNotFound",
        code: errcode::ErrLoadDataJobNotFound,
        message: ErrLoadDataJobNotFound,
    },
    CatalogEntry {
        name: "ErrLoadDataInvalidOperation",
        code: errcode::ErrLoadDataInvalidOperation,
        message: ErrLoadDataInvalidOperation,
    },
    CatalogEntry {
        name: "ErrLoadDataLocalUnsupportedOption",
        code: errcode::ErrLoadDataLocalUnsupportedOption,
        message: ErrLoadDataLocalUnsupportedOption,
    },
    CatalogEntry {
        name: "ErrLoadDataPreCheckFailed",
        code: errcode::ErrLoadDataPreCheckFailed,
        message: ErrLoadDataPreCheckFailed,
    },
    CatalogEntry {
        name: "ErrMemoryExceedForQuery",
        code: errcode::ErrMemoryExceedForQuery,
        message: ErrMemoryExceedForQuery,
    },
    CatalogEntry {
        name: "ErrMemoryExceedForInstance",
        code: errcode::ErrMemoryExceedForInstance,
        message: ErrMemoryExceedForInstance,
    },
    CatalogEntry {
        name: "ErrDeleteNotFoundColumn",
        code: errcode::ErrDeleteNotFoundColumn,
        message: ErrDeleteNotFoundColumn,
    },
    CatalogEntry {
        name: "ErrKeyTooLarge",
        code: errcode::ErrKeyTooLarge,
        message: ErrKeyTooLarge,
    },
    CatalogEntry {
        name: "ErrProtectedTableMode",
        code: errcode::ErrProtectedTableMode,
        message: ErrProtectedTableMode,
    },
    CatalogEntry {
        name: "ErrInvalidTableModeSet",
        code: errcode::ErrInvalidTableModeSet,
        message: ErrInvalidTableModeSet,
    },
    CatalogEntry {
        name: "ErrForbiddenDDL",
        code: errcode::ErrForbiddenDDL,
        message: ErrForbiddenDDL,
    },
    CatalogEntry {
        name: "ErrHTTPServiceError",
        code: errcode::ErrHTTPServiceError,
        message: ErrHTTPServiceError,
    },
    CatalogEntry {
        name: "ErrWarnOptimizerHintInvalidInteger",
        code: errcode::ErrWarnOptimizerHintInvalidInteger,
        message: ErrWarnOptimizerHintInvalidInteger,
    },
    CatalogEntry {
        name: "ErrWarnOptimizerHintUnsupportedHint",
        code: errcode::ErrWarnOptimizerHintUnsupportedHint,
        message: ErrWarnOptimizerHintUnsupportedHint,
    },
    CatalogEntry {
        name: "ErrWarnOptimizerHintInvalidToken",
        code: errcode::ErrWarnOptimizerHintInvalidToken,
        message: ErrWarnOptimizerHintInvalidToken,
    },
    CatalogEntry {
        name: "ErrWarnMemoryQuotaOverflow",
        code: errcode::ErrWarnMemoryQuotaOverflow,
        message: ErrWarnMemoryQuotaOverflow,
    },
    CatalogEntry {
        name: "ErrWarnOptimizerHintParseError",
        code: errcode::ErrWarnOptimizerHintParseError,
        message: ErrWarnOptimizerHintParseError,
    },
    CatalogEntry {
        name: "ErrWarnOptimizerHintWrongPos",
        code: errcode::ErrWarnOptimizerHintWrongPos,
        message: ErrWarnOptimizerHintWrongPos,
    },
    CatalogEntry {
        name: "ErrSequenceUnsupportedTableOption",
        code: errcode::ErrSequenceUnsupportedTableOption,
        message: ErrSequenceUnsupportedTableOption,
    },
    CatalogEntry {
        name: "ErrColumnTypeUnsupportedNextValue",
        code: errcode::ErrColumnTypeUnsupportedNextValue,
        message: ErrColumnTypeUnsupportedNextValue,
    },
    CatalogEntry {
        name: "ErrAddColumnWithSequenceAsDefault",
        code: errcode::ErrAddColumnWithSequenceAsDefault,
        message: ErrAddColumnWithSequenceAsDefault,
    },
    CatalogEntry {
        name: "ErrUnsupportedType",
        code: errcode::ErrUnsupportedType,
        message: ErrUnsupportedType,
    },
    CatalogEntry {
        name: "ErrAnalyzeMissIndex",
        code: errcode::ErrAnalyzeMissIndex,
        message: ErrAnalyzeMissIndex,
    },
    CatalogEntry {
        name: "ErrAnalyzeMissColumn",
        code: errcode::ErrAnalyzeMissColumn,
        message: ErrAnalyzeMissColumn,
    },
    CatalogEntry {
        name: "ErrCartesianProductUnsupported",
        code: errcode::ErrCartesianProductUnsupported,
        message: ErrCartesianProductUnsupported,
    },
    CatalogEntry {
        name: "ErrPreparedStmtNotFound",
        code: errcode::ErrPreparedStmtNotFound,
        message: ErrPreparedStmtNotFound,
    },
    CatalogEntry {
        name: "ErrWrongParamCount",
        code: errcode::ErrWrongParamCount,
        message: ErrWrongParamCount,
    },
    CatalogEntry {
        name: "ErrSchemaChanged",
        code: errcode::ErrSchemaChanged,
        message: ErrSchemaChanged,
    },
    CatalogEntry {
        name: "ErrUnknownPlan",
        code: errcode::ErrUnknownPlan,
        message: ErrUnknownPlan,
    },
    CatalogEntry {
        name: "ErrPrepareMulti",
        code: errcode::ErrPrepareMulti,
        message: ErrPrepareMulti,
    },
    CatalogEntry {
        name: "ErrPrepareDDL",
        code: errcode::ErrPrepareDDL,
        message: ErrPrepareDDL,
    },
    CatalogEntry {
        name: "ErrResultIsEmpty",
        code: errcode::ErrResultIsEmpty,
        message: ErrResultIsEmpty,
    },
    CatalogEntry {
        name: "ErrBuildExecutor",
        code: errcode::ErrBuildExecutor,
        message: ErrBuildExecutor,
    },
    CatalogEntry {
        name: "ErrBatchInsertFail",
        code: errcode::ErrBatchInsertFail,
        message: ErrBatchInsertFail,
    },
    CatalogEntry {
        name: "ErrGetStartTS",
        code: errcode::ErrGetStartTS,
        message: ErrGetStartTS,
    },
    CatalogEntry {
        name: "ErrPrivilegeCheckFail",
        code: errcode::ErrPrivilegeCheckFail,
        message: ErrPrivilegeCheckFail,
    },
    CatalogEntry {
        name: "ErrInvalidWildCard",
        code: errcode::ErrInvalidWildCard,
        message: ErrInvalidWildCard,
    },
    CatalogEntry {
        name: "ErrMixOfGroupFuncAndFieldsIncompatible",
        code: errcode::ErrMixOfGroupFuncAndFieldsIncompatible,
        message: ErrMixOfGroupFuncAndFieldsIncompatible,
    },
    CatalogEntry {
        name: "ErrUnsupportedSecondArgumentType",
        code: errcode::ErrUnsupportedSecondArgumentType,
        message: ErrUnsupportedSecondArgumentType,
    },
    CatalogEntry {
        name: "ErrColumnNotMatched",
        code: errcode::ErrColumnNotMatched,
        message: ErrColumnNotMatched,
    },
    CatalogEntry {
        name: "ErrLockExpire",
        code: errcode::ErrLockExpire,
        message: ErrLockExpire,
    },
    CatalogEntry {
        name: "ErrTableOptionUnionUnsupported",
        code: errcode::ErrTableOptionUnionUnsupported,
        message: ErrTableOptionUnionUnsupported,
    },
    CatalogEntry {
        name: "ErrTableOptionInsertMethodUnsupported",
        code: errcode::ErrTableOptionInsertMethodUnsupported,
        message: ErrTableOptionInsertMethodUnsupported,
    },
    CatalogEntry {
        name: "ErrUserLockDeadlock",
        code: errcode::ErrUserLockDeadlock,
        message: ErrUserLockDeadlock,
    },
    CatalogEntry {
        name: "ErrUserLockWrongName",
        code: errcode::ErrUserLockWrongName,
        message: ErrUserLockWrongName,
    },
    CatalogEntry {
        name: "ErrBRIEBackupFailed",
        code: errcode::ErrBRIEBackupFailed,
        message: ErrBRIEBackupFailed,
    },
    CatalogEntry {
        name: "ErrBRIERestoreFailed",
        code: errcode::ErrBRIERestoreFailed,
        message: ErrBRIERestoreFailed,
    },
    CatalogEntry {
        name: "ErrBRIEImportFailed",
        code: errcode::ErrBRIEImportFailed,
        message: ErrBRIEImportFailed,
    },
    CatalogEntry {
        name: "ErrBRIEExportFailed",
        code: errcode::ErrBRIEExportFailed,
        message: ErrBRIEExportFailed,
    },
    CatalogEntry {
        name: "ErrBRJobNotFound",
        code: errcode::ErrBRJobNotFound,
        message: ErrBRJobNotFound,
    },
    CatalogEntry {
        name: "ErrInvalidTableSample",
        code: errcode::ErrInvalidTableSample,
        message: ErrInvalidTableSample,
    },
    CatalogEntry {
        name: "ErrJSONObjectKeyTooLong",
        code: errcode::ErrJSONObjectKeyTooLong,
        message: ErrJSONObjectKeyTooLong,
    },
    CatalogEntry {
        name: "ErrPartitionStatsMissing",
        code: errcode::ErrPartitionStatsMissing,
        message: ErrPartitionStatsMissing,
    },
    CatalogEntry {
        name: "ErrPartitionColumnStatsMissing",
        code: errcode::ErrPartitionColumnStatsMissing,
        message: ErrPartitionColumnStatsMissing,
    },
    CatalogEntry {
        name: "ErrDDLSetting",
        code: errcode::ErrDDLSetting,
        message: ErrDDLSetting,
    },
    CatalogEntry {
        name: "ErrIngestFailed",
        code: errcode::ErrIngestFailed,
        message: ErrIngestFailed,
    },
    CatalogEntry {
        name: "ErrIngestCheckEnvFailed",
        code: errcode::ErrIngestCheckEnvFailed,
        message: ErrIngestCheckEnvFailed,
    },
    CatalogEntry {
        name: "ErrNotSupportedWithSem",
        code: errcode::ErrNotSupportedWithSem,
        message: ErrNotSupportedWithSem,
    },
    CatalogEntry {
        name: "ErrPlacementPolicyCheck",
        code: errcode::ErrPlacementPolicyCheck,
        message: ErrPlacementPolicyCheck,
    },
    CatalogEntry {
        name: "ErrMultiStatementDisabled",
        code: errcode::ErrMultiStatementDisabled,
        message: ErrMultiStatementDisabled,
    },
    CatalogEntry {
        name: "ErrAsOf",
        code: errcode::ErrAsOf,
        message: ErrAsOf,
    },
    CatalogEntry {
        name: "ErrVariableNoLongerSupported",
        code: errcode::ErrVariableNoLongerSupported,
        message: ErrVariableNoLongerSupported,
    },
    CatalogEntry {
        name: "ErrInvalidAttributesSpec",
        code: errcode::ErrInvalidAttributesSpec,
        message: ErrInvalidAttributesSpec,
    },
    CatalogEntry {
        name: "ErrPlacementPolicyExists",
        code: errcode::ErrPlacementPolicyExists,
        message: ErrPlacementPolicyExists,
    },
    CatalogEntry {
        name: "ErrPlacementPolicyNotExists",
        code: errcode::ErrPlacementPolicyNotExists,
        message: ErrPlacementPolicyNotExists,
    },
    CatalogEntry {
        name: "ErrPlacementPolicyWithDirectOption",
        code: errcode::ErrPlacementPolicyWithDirectOption,
        message: ErrPlacementPolicyWithDirectOption,
    },
    CatalogEntry {
        name: "ErrPlacementPolicyInUse",
        code: errcode::ErrPlacementPolicyInUse,
        message: ErrPlacementPolicyInUse,
    },
    CatalogEntry {
        name: "ErrMaskingPolicyExists",
        code: errcode::ErrMaskingPolicyExists,
        message: ErrMaskingPolicyExists,
    },
    CatalogEntry {
        name: "ErrMaskingPolicyNotExists",
        code: errcode::ErrMaskingPolicyNotExists,
        message: ErrMaskingPolicyNotExists,
    },
    CatalogEntry {
        name: "ErrMaskingPolicyExprInvalidColumn",
        code: errcode::ErrMaskingPolicyExprInvalidColumn,
        message: ErrMaskingPolicyExprInvalidColumn,
    },
    CatalogEntry {
        name: "ErrOptOnCacheTable",
        code: errcode::ErrOptOnCacheTable,
        message: ErrOptOnCacheTable,
    },
    CatalogEntry {
        name: "ErrResourceGroupExists",
        code: errcode::ErrResourceGroupExists,
        message: ErrResourceGroupExists,
    },
    CatalogEntry {
        name: "ErrResourceGroupNotExists",
        code: errcode::ErrResourceGroupNotExists,
        message: ErrResourceGroupNotExists,
    },
    CatalogEntry {
        name: "ErrResourceGroupInvalidForRole",
        code: errcode::ErrResourceGroupInvalidForRole,
        message: ErrResourceGroupInvalidForRole,
    },
    CatalogEntry {
        name: "ErrColumnInChange",
        code: errcode::ErrColumnInChange,
        message: ErrColumnInChange,
    },
    CatalogEntry {
        name: "ErrResourceGroupSupportDisabled",
        code: errcode::ErrResourceGroupSupportDisabled,
        message: ErrResourceGroupSupportDisabled,
    },
    CatalogEntry {
        name: "ErrResourceGroupConfigUnavailable",
        code: errcode::ErrResourceGroupConfigUnavailable,
        message: ErrResourceGroupConfigUnavailable,
    },
    CatalogEntry {
        name: "ErrResourceGroupThrottled",
        code: errcode::ErrResourceGroupThrottled,
        message: ErrResourceGroupThrottled,
    },
    CatalogEntry {
        name: "ErrResourceGroupQueryRunawayInterrupted",
        code: errcode::ErrResourceGroupQueryRunawayInterrupted,
        message: ErrResourceGroupQueryRunawayInterrupted,
    },
    CatalogEntry {
        name: "ErrResourceGroupQueryRunawayQuarantine",
        code: errcode::ErrResourceGroupQueryRunawayQuarantine,
        message: ErrResourceGroupQueryRunawayQuarantine,
    },
    CatalogEntry {
        name: "ErrResourceGroupInvalidBackgroundTaskName",
        code: errcode::ErrResourceGroupInvalidBackgroundTaskName,
        message: ErrResourceGroupInvalidBackgroundTaskName,
    },
    CatalogEntry {
        name: "ErrQueryExecStopped",
        code: errcode::ErrQueryExecStopped,
        message: ErrQueryExecStopped,
    },
    CatalogEntry {
        name: "ErrEngineAttributeInvalidFormat",
        code: errcode::ErrEngineAttributeInvalidFormat,
        message: ErrEngineAttributeInvalidFormat,
    },
    CatalogEntry {
        name: "ErrStorageClassInvalidSpec",
        code: errcode::ErrStorageClassInvalidSpec,
        message: ErrStorageClassInvalidSpec,
    },
    CatalogEntry {
        name: "ErrModifyColumnReferencedByPartialCondition",
        code: errcode::ErrModifyColumnReferencedByPartialCondition,
        message: ErrModifyColumnReferencedByPartialCondition,
    },
    CatalogEntry {
        name: "ErrCheckPartialIndexWithoutFastCheck",
        code: errcode::ErrCheckPartialIndexWithoutFastCheck,
        message: ErrCheckPartialIndexWithoutFastCheck,
    },
    CatalogEntry {
        name: "ErrMaxKeysReadExceeded",
        code: errcode::ErrMaxKeysReadExceeded,
        message: ErrMaxKeysReadExceeded,
    },
    CatalogEntry {
        name: "ErrPDServerTimeout",
        code: errcode::ErrPDServerTimeout,
        message: ErrPDServerTimeout,
    },
    CatalogEntry {
        name: "ErrTiKVServerTimeout",
        code: errcode::ErrTiKVServerTimeout,
        message: ErrTiKVServerTimeout,
    },
    CatalogEntry {
        name: "ErrTiKVServerBusy",
        code: errcode::ErrTiKVServerBusy,
        message: ErrTiKVServerBusy,
    },
    CatalogEntry {
        name: "ErrTiFlashServerTimeout",
        code: errcode::ErrTiFlashServerTimeout,
        message: ErrTiFlashServerTimeout,
    },
    CatalogEntry {
        name: "ErrTiFlashServerBusy",
        code: errcode::ErrTiFlashServerBusy,
        message: ErrTiFlashServerBusy,
    },
    CatalogEntry {
        name: "ErrTiFlashBackfillIndex",
        code: errcode::ErrTiFlashBackfillIndex,
        message: ErrTiFlashBackfillIndex,
    },
    CatalogEntry {
        name: "ErrResolveLockTimeout",
        code: errcode::ErrResolveLockTimeout,
        message: ErrResolveLockTimeout,
    },
    CatalogEntry {
        name: "ErrRegionUnavailable",
        code: errcode::ErrRegionUnavailable,
        message: ErrRegionUnavailable,
    },
    CatalogEntry {
        name: "ErrTxnAbortedByGC",
        code: errcode::ErrTxnAbortedByGC,
        message: ErrTxnAbortedByGC,
    },
    CatalogEntry {
        name: "ErrWriteConflict",
        code: errcode::ErrWriteConflict,
        message: ErrWriteConflict,
    },
    CatalogEntry {
        name: "ErrTiKVStoreLimit",
        code: errcode::ErrTiKVStoreLimit,
        message: ErrTiKVStoreLimit,
    },
    CatalogEntry {
        name: "ErrPrometheusAddrIsNotSet",
        code: errcode::ErrPrometheusAddrIsNotSet,
        message: ErrPrometheusAddrIsNotSet,
    },
    CatalogEntry {
        name: "ErrTiKVStaleCommand",
        code: errcode::ErrTiKVStaleCommand,
        message: ErrTiKVStaleCommand,
    },
    CatalogEntry {
        name: "ErrTiKVMaxTimestampNotSynced",
        code: errcode::ErrTiKVMaxTimestampNotSynced,
        message: ErrTiKVMaxTimestampNotSynced,
    },
    CatalogEntry {
        name: "ErrCannotPauseDDLJob",
        code: errcode::ErrCannotPauseDDLJob,
        message: ErrCannotPauseDDLJob,
    },
    CatalogEntry {
        name: "ErrCannotResumeDDLJob",
        code: errcode::ErrCannotResumeDDLJob,
        message: ErrCannotResumeDDLJob,
    },
    CatalogEntry {
        name: "ErrPausedDDLJob",
        code: errcode::ErrPausedDDLJob,
        message: ErrPausedDDLJob,
    },
    CatalogEntry {
        name: "ErrBDRRestrictedDDL",
        code: errcode::ErrBDRRestrictedDDL,
        message: ErrBDRRestrictedDDL,
    },
    CatalogEntry {
        name: "ErrDDLAutoPausedByKVDiskFull",
        code: errcode::ErrDDLAutoPausedByKVDiskFull,
        message: ErrDDLAutoPausedByKVDiskFull,
    },
    CatalogEntry {
        name: "ErrGlobalIndexNotExplicitlySet",
        code: errcode::ErrGlobalIndexNotExplicitlySet,
        message: ErrGlobalIndexNotExplicitlySet,
    },
    CatalogEntry {
        name: "ErrWarnGlobalIndexNeedManuallyAnalyze",
        code: errcode::ErrWarnGlobalIndexNeedManuallyAnalyze,
        message: ErrWarnGlobalIndexNeedManuallyAnalyze,
    },
    CatalogEntry {
        name: "ErrTimeStampInDSTTransition",
        code: errcode::ErrTimeStampInDSTTransition,
        message: ErrTimeStampInDSTTransition,
    },
    CatalogEntry {
        name: "ErrInvalidAffinityOption",
        code: errcode::ErrInvalidAffinityOption,
        message: ErrInvalidAffinityOption,
    },
    CatalogEntry {
        name: "ErrUserPrefixMismatch",
        code: errcode::ErrUserPrefixMismatch,
        message: ErrUserPrefixMismatch,
    },
];
