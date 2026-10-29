// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");

//! Message catalog entries, part 3 of 4 (see `errname/mod.rs`).

use super::consts_3::*;
use super::consts_4::*;
use super::errcode;
use super::CatalogEntry;

pub(super) const CATALOG_3: &[CatalogEntry] = &[
    CatalogEntry {
        name: "ErrBinlogUnsafeInsertDelayed",
        code: errcode::ErrBinlogUnsafeInsertDelayed,
        message: ErrBinlogUnsafeInsertDelayed,
    },
    CatalogEntry {
        name: "ErrBinlogUnsafeAutoincColumns",
        code: errcode::ErrBinlogUnsafeAutoincColumns,
        message: ErrBinlogUnsafeAutoincColumns,
    },
    CatalogEntry {
        name: "ErrBinlogUnsafeSystemFunction",
        code: errcode::ErrBinlogUnsafeSystemFunction,
        message: ErrBinlogUnsafeSystemFunction,
    },
    CatalogEntry {
        name: "ErrBinlogUnsafeNontransAfterTrans",
        code: errcode::ErrBinlogUnsafeNontransAfterTrans,
        message: ErrBinlogUnsafeNontransAfterTrans,
    },
    CatalogEntry {
        name: "ErrMessageAndStatement",
        code: errcode::ErrMessageAndStatement,
        message: ErrMessageAndStatement,
    },
    CatalogEntry {
        name: "ErrInsideTransactionPreventsSwitchBinlogFormat",
        code: errcode::ErrInsideTransactionPreventsSwitchBinlogFormat,
        message: ErrInsideTransactionPreventsSwitchBinlogFormat,
    },
    CatalogEntry {
        name: "ErrPathLength",
        code: errcode::ErrPathLength,
        message: ErrPathLength,
    },
    CatalogEntry {
        name: "ErrWarnDeprecatedSyntaxNoReplacement",
        code: errcode::ErrWarnDeprecatedSyntaxNoReplacement,
        message: ErrWarnDeprecatedSyntaxNoReplacement,
    },
    CatalogEntry {
        name: "ErrWrongNativeTableStructure",
        code: errcode::ErrWrongNativeTableStructure,
        message: ErrWrongNativeTableStructure,
    },
    CatalogEntry {
        name: "ErrWrongPerfSchemaUsage",
        code: errcode::ErrWrongPerfSchemaUsage,
        message: ErrWrongPerfSchemaUsage,
    },
    CatalogEntry {
        name: "ErrWarnISSkippedTable",
        code: errcode::ErrWarnISSkippedTable,
        message: ErrWarnISSkippedTable,
    },
    CatalogEntry {
        name: "ErrInsideTransactionPreventsSwitchBinlogDirect",
        code: errcode::ErrInsideTransactionPreventsSwitchBinlogDirect,
        message: ErrInsideTransactionPreventsSwitchBinlogDirect,
    },
    CatalogEntry {
        name: "ErrStoredFunctionPreventsSwitchBinlogDirect",
        code: errcode::ErrStoredFunctionPreventsSwitchBinlogDirect,
        message: ErrStoredFunctionPreventsSwitchBinlogDirect,
    },
    CatalogEntry {
        name: "ErrSpatialMustHaveGeomCol",
        code: errcode::ErrSpatialMustHaveGeomCol,
        message: ErrSpatialMustHaveGeomCol,
    },
    CatalogEntry {
        name: "ErrTooLongIndexComment",
        code: errcode::ErrTooLongIndexComment,
        message: ErrTooLongIndexComment,
    },
    CatalogEntry {
        name: "ErrLockAborted",
        code: errcode::ErrLockAborted,
        message: ErrLockAborted,
    },
    CatalogEntry {
        name: "ErrDataOutOfRange",
        code: errcode::ErrDataOutOfRange,
        message: ErrDataOutOfRange,
    },
    CatalogEntry {
        name: "ErrWrongSpvarTypeInLimit",
        code: errcode::ErrWrongSpvarTypeInLimit,
        message: ErrWrongSpvarTypeInLimit,
    },
    CatalogEntry {
        name: "ErrBinlogUnsafeMultipleEnginesAndSelfLoggingEngine",
        code: errcode::ErrBinlogUnsafeMultipleEnginesAndSelfLoggingEngine,
        message: ErrBinlogUnsafeMultipleEnginesAndSelfLoggingEngine,
    },
    CatalogEntry {
        name: "ErrBinlogUnsafeMixedStatement",
        code: errcode::ErrBinlogUnsafeMixedStatement,
        message: ErrBinlogUnsafeMixedStatement,
    },
    CatalogEntry {
        name: "ErrInsideTransactionPreventsSwitchSQLLogBin",
        code: errcode::ErrInsideTransactionPreventsSwitchSQLLogBin,
        message: ErrInsideTransactionPreventsSwitchSQLLogBin,
    },
    CatalogEntry {
        name: "ErrStoredFunctionPreventsSwitchSQLLogBin",
        code: errcode::ErrStoredFunctionPreventsSwitchSQLLogBin,
        message: ErrStoredFunctionPreventsSwitchSQLLogBin,
    },
    CatalogEntry {
        name: "ErrFailedReadFromParFile",
        code: errcode::ErrFailedReadFromParFile,
        message: ErrFailedReadFromParFile,
    },
    CatalogEntry {
        name: "ErrValuesIsNotIntType",
        code: errcode::ErrValuesIsNotIntType,
        message: ErrValuesIsNotIntType,
    },
    CatalogEntry {
        name: "ErrAccessDeniedNoPassword",
        code: errcode::ErrAccessDeniedNoPassword,
        message: ErrAccessDeniedNoPassword,
    },
    CatalogEntry {
        name: "ErrSetPasswordAuthPlugin",
        code: errcode::ErrSetPasswordAuthPlugin,
        message: ErrSetPasswordAuthPlugin,
    },
    CatalogEntry {
        name: "ErrGrantPluginUserExists",
        code: errcode::ErrGrantPluginUserExists,
        message: ErrGrantPluginUserExists,
    },
    CatalogEntry {
        name: "ErrTruncateIllegalForeignKey",
        code: errcode::ErrTruncateIllegalForeignKey,
        message: ErrTruncateIllegalForeignKey,
    },
    CatalogEntry {
        name: "ErrPluginIsPermanent",
        code: errcode::ErrPluginIsPermanent,
        message: ErrPluginIsPermanent,
    },
    CatalogEntry {
        name: "ErrStmtCacheFull",
        code: errcode::ErrStmtCacheFull,
        message: ErrStmtCacheFull,
    },
    CatalogEntry {
        name: "ErrMultiUpdateKeyConflict",
        code: errcode::ErrMultiUpdateKeyConflict,
        message: ErrMultiUpdateKeyConflict,
    },
    CatalogEntry {
        name: "ErrTableNeedsRebuild",
        code: errcode::ErrTableNeedsRebuild,
        message: ErrTableNeedsRebuild,
    },
    CatalogEntry {
        name: "WarnOptionBelowLimit",
        code: errcode::WarnOptionBelowLimit,
        message: WarnOptionBelowLimit,
    },
    CatalogEntry {
        name: "ErrIndexColumnTooLong",
        code: errcode::ErrIndexColumnTooLong,
        message: ErrIndexColumnTooLong,
    },
    CatalogEntry {
        name: "ErrErrorInTriggerBody",
        code: errcode::ErrErrorInTriggerBody,
        message: ErrErrorInTriggerBody,
    },
    CatalogEntry {
        name: "ErrErrorInUnknownTriggerBody",
        code: errcode::ErrErrorInUnknownTriggerBody,
        message: ErrErrorInUnknownTriggerBody,
    },
    CatalogEntry {
        name: "ErrIndexCorrupt",
        code: errcode::ErrIndexCorrupt,
        message: ErrIndexCorrupt,
    },
    CatalogEntry {
        name: "ErrUndoRecordTooBig",
        code: errcode::ErrUndoRecordTooBig,
        message: ErrUndoRecordTooBig,
    },
    CatalogEntry {
        name: "ErrPluginNoUninstall",
        code: errcode::ErrPluginNoUninstall,
        message: ErrPluginNoUninstall,
    },
    CatalogEntry {
        name: "ErrPluginNoInstall",
        code: errcode::ErrPluginNoInstall,
        message: ErrPluginNoInstall,
    },
    CatalogEntry {
        name: "ErrBinlogUnsafeInsertTwoKeys",
        code: errcode::ErrBinlogUnsafeInsertTwoKeys,
        message: ErrBinlogUnsafeInsertTwoKeys,
    },
    CatalogEntry {
        name: "ErrTableInFkCheck",
        code: errcode::ErrTableInFkCheck,
        message: ErrTableInFkCheck,
    },
    CatalogEntry {
        name: "ErrUnsupportedEngine",
        code: errcode::ErrUnsupportedEngine,
        message: ErrUnsupportedEngine,
    },
    CatalogEntry {
        name: "ErrBinlogUnsafeAutoincNotFirst",
        code: errcode::ErrBinlogUnsafeAutoincNotFirst,
        message: ErrBinlogUnsafeAutoincNotFirst,
    },
    CatalogEntry {
        name: "ErrCannotLoadFromTableV2",
        code: errcode::ErrCannotLoadFromTableV2,
        message: ErrCannotLoadFromTableV2,
    },
    CatalogEntry {
        name: "ErrOnlyFdAndRbrEventsAllowedInBinlogStatement",
        code: errcode::ErrOnlyFdAndRbrEventsAllowedInBinlogStatement,
        message: ErrOnlyFdAndRbrEventsAllowedInBinlogStatement,
    },
    CatalogEntry {
        name: "ErrPartitionExchangeDifferentOption",
        code: errcode::ErrPartitionExchangeDifferentOption,
        message: ErrPartitionExchangeDifferentOption,
    },
    CatalogEntry {
        name: "ErrPartitionExchangePartTable",
        code: errcode::ErrPartitionExchangePartTable,
        message: ErrPartitionExchangePartTable,
    },
    CatalogEntry {
        name: "ErrPartitionExchangeTempTable",
        code: errcode::ErrPartitionExchangeTempTable,
        message: ErrPartitionExchangeTempTable,
    },
    CatalogEntry {
        name: "ErrPartitionInsteadOfSubpartition",
        code: errcode::ErrPartitionInsteadOfSubpartition,
        message: ErrPartitionInsteadOfSubpartition,
    },
    CatalogEntry {
        name: "ErrUnknownPartition",
        code: errcode::ErrUnknownPartition,
        message: ErrUnknownPartition,
    },
    CatalogEntry {
        name: "ErrTablesDifferentMetadata",
        code: errcode::ErrTablesDifferentMetadata,
        message: ErrTablesDifferentMetadata,
    },
    CatalogEntry {
        name: "ErrRowDoesNotMatchPartition",
        code: errcode::ErrRowDoesNotMatchPartition,
        message: ErrRowDoesNotMatchPartition,
    },
    CatalogEntry {
        name: "ErrBinlogCacheSizeGreaterThanMax",
        code: errcode::ErrBinlogCacheSizeGreaterThanMax,
        message: ErrBinlogCacheSizeGreaterThanMax,
    },
    CatalogEntry {
        name: "ErrWarnIndexNotApplicable",
        code: errcode::ErrWarnIndexNotApplicable,
        message: ErrWarnIndexNotApplicable,
    },
    CatalogEntry {
        name: "ErrPartitionExchangeForeignKey",
        code: errcode::ErrPartitionExchangeForeignKey,
        message: ErrPartitionExchangeForeignKey,
    },
    CatalogEntry {
        name: "ErrNoSuchKeyValue",
        code: errcode::ErrNoSuchKeyValue,
        message: ErrNoSuchKeyValue,
    },
    CatalogEntry {
        name: "ErrRplInfoDataTooLong",
        code: errcode::ErrRplInfoDataTooLong,
        message: ErrRplInfoDataTooLong,
    },
    CatalogEntry {
        name: "ErrNetworkReadEventChecksumFailure",
        code: errcode::ErrNetworkReadEventChecksumFailure,
        message: ErrNetworkReadEventChecksumFailure,
    },
    CatalogEntry {
        name: "ErrBinlogReadEventChecksumFailure",
        code: errcode::ErrBinlogReadEventChecksumFailure,
        message: ErrBinlogReadEventChecksumFailure,
    },
    CatalogEntry {
        name: "ErrBinlogStmtCacheSizeGreaterThanMax",
        code: errcode::ErrBinlogStmtCacheSizeGreaterThanMax,
        message: ErrBinlogStmtCacheSizeGreaterThanMax,
    },
    CatalogEntry {
        name: "ErrCantUpdateTableInCreateTableSelect",
        code: errcode::ErrCantUpdateTableInCreateTableSelect,
        message: ErrCantUpdateTableInCreateTableSelect,
    },
    CatalogEntry {
        name: "ErrPartitionClauseOnNonpartitioned",
        code: errcode::ErrPartitionClauseOnNonpartitioned,
        message: ErrPartitionClauseOnNonpartitioned,
    },
    CatalogEntry {
        name: "ErrRowDoesNotMatchGivenPartitionSet",
        code: errcode::ErrRowDoesNotMatchGivenPartitionSet,
        message: ErrRowDoesNotMatchGivenPartitionSet,
    },
    CatalogEntry {
        name: "ErrNoSuchPartitionunused",
        code: errcode::ErrNoSuchPartitionunused,
        message: ErrNoSuchPartitionunused,
    },
    CatalogEntry {
        name: "ErrChangeRplInfoRepositoryFailure",
        code: errcode::ErrChangeRplInfoRepositoryFailure,
        message: ErrChangeRplInfoRepositoryFailure,
    },
    CatalogEntry {
        name: "ErrWarningNotCompleteRollbackWithCreatedTempTable",
        code: errcode::ErrWarningNotCompleteRollbackWithCreatedTempTable,
        message: ErrWarningNotCompleteRollbackWithCreatedTempTable,
    },
    CatalogEntry {
        name: "ErrWarningNotCompleteRollbackWithDroppedTempTable",
        code: errcode::ErrWarningNotCompleteRollbackWithDroppedTempTable,
        message: ErrWarningNotCompleteRollbackWithDroppedTempTable,
    },
    CatalogEntry {
        name: "ErrMtsUpdatedDBsGreaterMax",
        code: errcode::ErrMtsUpdatedDBsGreaterMax,
        message: ErrMtsUpdatedDBsGreaterMax,
    },
    CatalogEntry {
        name: "ErrMtsCantParallel",
        code: errcode::ErrMtsCantParallel,
        message: ErrMtsCantParallel,
    },
    CatalogEntry {
        name: "ErrMtsInconsistentData",
        code: errcode::ErrMtsInconsistentData,
        message: ErrMtsInconsistentData,
    },
    CatalogEntry {
        name: "ErrFulltextNotSupportedWithPartitioning",
        code: errcode::ErrFulltextNotSupportedWithPartitioning,
        message: ErrFulltextNotSupportedWithPartitioning,
    },
    CatalogEntry {
        name: "ErrDaInvalidConditionNumber",
        code: errcode::ErrDaInvalidConditionNumber,
        message: ErrDaInvalidConditionNumber,
    },
    CatalogEntry {
        name: "ErrInsecurePlainText",
        code: errcode::ErrInsecurePlainText,
        message: ErrInsecurePlainText,
    },
    CatalogEntry {
        name: "ErrForeignDuplicateKeyWithChildInfo",
        code: errcode::ErrForeignDuplicateKeyWithChildInfo,
        message: ErrForeignDuplicateKeyWithChildInfo,
    },
    CatalogEntry {
        name: "ErrForeignDuplicateKeyWithoutChildInfo",
        code: errcode::ErrForeignDuplicateKeyWithoutChildInfo,
        message: ErrForeignDuplicateKeyWithoutChildInfo,
    },
    CatalogEntry {
        name: "ErrTableHasNoFt",
        code: errcode::ErrTableHasNoFt,
        message: ErrTableHasNoFt,
    },
    CatalogEntry {
        name: "ErrVariableNotSettableInSfOrTrigger",
        code: errcode::ErrVariableNotSettableInSfOrTrigger,
        message: ErrVariableNotSettableInSfOrTrigger,
    },
    CatalogEntry {
        name: "ErrVariableNotSettableInTransaction",
        code: errcode::ErrVariableNotSettableInTransaction,
        message: ErrVariableNotSettableInTransaction,
    },
    CatalogEntry {
        name: "ErrGtidNextIsNotInGtidNextList",
        code: errcode::ErrGtidNextIsNotInGtidNextList,
        message: ErrGtidNextIsNotInGtidNextList,
    },
    CatalogEntry {
        name: "ErrCantChangeGtidNextInTransactionWhenGtidNextListIsNull",
        code: errcode::ErrCantChangeGtidNextInTransactionWhenGtidNextListIsNull,
        message: ErrCantChangeGtidNextInTransactionWhenGtidNextListIsNull,
    },
    CatalogEntry {
        name: "ErrSetStatementCannotInvokeFunction",
        code: errcode::ErrSetStatementCannotInvokeFunction,
        message: ErrSetStatementCannotInvokeFunction,
    },
    CatalogEntry {
        name: "ErrGtidNextCantBeAutomaticIfGtidNextListIsNonNull",
        code: errcode::ErrGtidNextCantBeAutomaticIfGtidNextListIsNonNull,
        message: ErrGtidNextCantBeAutomaticIfGtidNextListIsNonNull,
    },
    CatalogEntry {
        name: "ErrSkippingLoggedTransaction",
        code: errcode::ErrSkippingLoggedTransaction,
        message: ErrSkippingLoggedTransaction,
    },
    CatalogEntry {
        name: "ErrMalformedGtidSetSpecification",
        code: errcode::ErrMalformedGtidSetSpecification,
        message: ErrMalformedGtidSetSpecification,
    },
    CatalogEntry {
        name: "ErrMalformedGtidSetEncoding",
        code: errcode::ErrMalformedGtidSetEncoding,
        message: ErrMalformedGtidSetEncoding,
    },
    CatalogEntry {
        name: "ErrMalformedGtidSpecification",
        code: errcode::ErrMalformedGtidSpecification,
        message: ErrMalformedGtidSpecification,
    },
    CatalogEntry {
        name: "ErrGnoExhausted",
        code: errcode::ErrGnoExhausted,
        message: ErrGnoExhausted,
    },
    CatalogEntry {
        name: "ErrCantDoImplicitCommitInTrxWhenGtidNextIsSet",
        code: errcode::ErrCantDoImplicitCommitInTrxWhenGtidNextIsSet,
        message: ErrCantDoImplicitCommitInTrxWhenGtidNextIsSet,
    },
    CatalogEntry {
        name: "ErrGtidMode2Or3RequiresEnforceGtidConsistencyOn",
        code: errcode::ErrGtidMode2Or3RequiresEnforceGtidConsistencyOn,
        message: ErrGtidMode2Or3RequiresEnforceGtidConsistencyOn,
    },
    CatalogEntry {
        name: "ErrCantSetGtidNextToGtidWhenGtidModeIsOff",
        code: errcode::ErrCantSetGtidNextToGtidWhenGtidModeIsOff,
        message: ErrCantSetGtidNextToGtidWhenGtidModeIsOff,
    },
    CatalogEntry {
        name: "ErrCantSetGtidNextToAnonymousWhenGtidModeIsOn",
        code: errcode::ErrCantSetGtidNextToAnonymousWhenGtidModeIsOn,
        message: ErrCantSetGtidNextToAnonymousWhenGtidModeIsOn,
    },
    CatalogEntry {
        name: "ErrCantSetGtidNextListToNonNullWhenGtidModeIsOff",
        code: errcode::ErrCantSetGtidNextListToNonNullWhenGtidModeIsOff,
        message: ErrCantSetGtidNextListToNonNullWhenGtidModeIsOff,
    },
    CatalogEntry {
        name: "ErrFoundGtidEventWhenGtidModeIsOff",
        code: errcode::ErrFoundGtidEventWhenGtidModeIsOff,
        message: ErrFoundGtidEventWhenGtidModeIsOff,
    },
    CatalogEntry {
        name: "ErrGtidUnsafeNonTransactionalTable",
        code: errcode::ErrGtidUnsafeNonTransactionalTable,
        message: ErrGtidUnsafeNonTransactionalTable,
    },
    CatalogEntry {
        name: "ErrGtidUnsafeCreateSelect",
        code: errcode::ErrGtidUnsafeCreateSelect,
        message: ErrGtidUnsafeCreateSelect,
    },
    CatalogEntry {
        name: "ErrGtidUnsafeCreateDropTemporaryTableInTransaction",
        code: errcode::ErrGtidUnsafeCreateDropTemporaryTableInTransaction,
        message: ErrGtidUnsafeCreateDropTemporaryTableInTransaction,
    },
    CatalogEntry {
        name: "ErrGtidModeCanOnlyChangeOneStepAtATime",
        code: errcode::ErrGtidModeCanOnlyChangeOneStepAtATime,
        message: ErrGtidModeCanOnlyChangeOneStepAtATime,
    },
    CatalogEntry {
        name: "ErrCantSetGtidNextWhenOwningGtid",
        code: errcode::ErrCantSetGtidNextWhenOwningGtid,
        message: ErrCantSetGtidNextWhenOwningGtid,
    },
    CatalogEntry {
        name: "ErrUnknownExplainFormat",
        code: errcode::ErrUnknownExplainFormat,
        message: ErrUnknownExplainFormat,
    },
    CatalogEntry {
        name: "ErrCantExecuteInReadOnlyTransaction",
        code: errcode::ErrCantExecuteInReadOnlyTransaction,
        message: ErrCantExecuteInReadOnlyTransaction,
    },
    CatalogEntry {
        name: "ErrTooLongTablePartitionComment",
        code: errcode::ErrTooLongTablePartitionComment,
        message: ErrTooLongTablePartitionComment,
    },
    CatalogEntry {
        name: "ErrInnodbFtLimit",
        code: errcode::ErrInnodbFtLimit,
        message: ErrInnodbFtLimit,
    },
    CatalogEntry {
        name: "ErrInnodbNoFtTempTable",
        code: errcode::ErrInnodbNoFtTempTable,
        message: ErrInnodbNoFtTempTable,
    },
    CatalogEntry {
        name: "ErrInnodbFtWrongDocidColumn",
        code: errcode::ErrInnodbFtWrongDocidColumn,
        message: ErrInnodbFtWrongDocidColumn,
    },
    CatalogEntry {
        name: "ErrInnodbFtWrongDocidIndex",
        code: errcode::ErrInnodbFtWrongDocidIndex,
        message: ErrInnodbFtWrongDocidIndex,
    },
    CatalogEntry {
        name: "ErrInnodbOnlineLogTooBig",
        code: errcode::ErrInnodbOnlineLogTooBig,
        message: ErrInnodbOnlineLogTooBig,
    },
    CatalogEntry {
        name: "ErrUnknownAlterAlgorithm",
        code: errcode::ErrUnknownAlterAlgorithm,
        message: ErrUnknownAlterAlgorithm,
    },
    CatalogEntry {
        name: "ErrUnknownAlterLock",
        code: errcode::ErrUnknownAlterLock,
        message: ErrUnknownAlterLock,
    },
    CatalogEntry {
        name: "ErrMtsResetWorkers",
        code: errcode::ErrMtsResetWorkers,
        message: ErrMtsResetWorkers,
    },
    CatalogEntry {
        name: "ErrColCountDoesntMatchCorruptedV2",
        code: errcode::ErrColCountDoesntMatchCorruptedV2,
        message: ErrColCountDoesntMatchCorruptedV2,
    },
    CatalogEntry {
        name: "ErrDiscardFkChecksRunning",
        code: errcode::ErrDiscardFkChecksRunning,
        message: ErrDiscardFkChecksRunning,
    },
    CatalogEntry {
        name: "ErrTableSchemaMismatch",
        code: errcode::ErrTableSchemaMismatch,
        message: ErrTableSchemaMismatch,
    },
    CatalogEntry {
        name: "ErrTableInSystemTablespace",
        code: errcode::ErrTableInSystemTablespace,
        message: ErrTableInSystemTablespace,
    },
    CatalogEntry {
        name: "ErrIoRead",
        code: errcode::ErrIoRead,
        message: ErrIoRead,
    },
    CatalogEntry {
        name: "ErrIoWrite",
        code: errcode::ErrIoWrite,
        message: ErrIoWrite,
    },
    CatalogEntry {
        name: "ErrTablespaceMissing",
        code: errcode::ErrTablespaceMissing,
        message: ErrTablespaceMissing,
    },
    CatalogEntry {
        name: "ErrTablespaceExists",
        code: errcode::ErrTablespaceExists,
        message: ErrTablespaceExists,
    },
    CatalogEntry {
        name: "ErrTablespaceDiscarded",
        code: errcode::ErrTablespaceDiscarded,
        message: ErrTablespaceDiscarded,
    },
    CatalogEntry {
        name: "ErrInternal",
        code: errcode::ErrInternal,
        message: ErrInternal,
    },
    CatalogEntry {
        name: "ErrInnodbImport",
        code: errcode::ErrInnodbImport,
        message: ErrInnodbImport,
    },
    CatalogEntry {
        name: "ErrInnodbIndexCorrupt",
        code: errcode::ErrInnodbIndexCorrupt,
        message: ErrInnodbIndexCorrupt,
    },
    CatalogEntry {
        name: "ErrInvalidYearColumnLength",
        code: errcode::ErrInvalidYearColumnLength,
        message: ErrInvalidYearColumnLength,
    },
    CatalogEntry {
        name: "ErrNotValidPassword",
        code: errcode::ErrNotValidPassword,
        message: ErrNotValidPassword,
    },
    CatalogEntry {
        name: "ErrMustChangePassword",
        code: errcode::ErrMustChangePassword,
        message: ErrMustChangePassword,
    },
    CatalogEntry {
        name: "ErrFkNoIndexChild",
        code: errcode::ErrFkNoIndexChild,
        message: ErrFkNoIndexChild,
    },
    CatalogEntry {
        name: "ErrForeignKeyNoIndexInParent",
        code: errcode::ErrForeignKeyNoIndexInParent,
        message: ErrForeignKeyNoIndexInParent,
    },
    CatalogEntry {
        name: "ErrFkFailAddSystem",
        code: errcode::ErrFkFailAddSystem,
        message: ErrFkFailAddSystem,
    },
    CatalogEntry {
        name: "ErrForeignKeyCannotOpenParent",
        code: errcode::ErrForeignKeyCannotOpenParent,
        message: ErrForeignKeyCannotOpenParent,
    },
    CatalogEntry {
        name: "ErrFkIncorrectOption",
        code: errcode::ErrFkIncorrectOption,
        message: ErrFkIncorrectOption,
    },
    CatalogEntry {
        name: "ErrFkDupName",
        code: errcode::ErrFkDupName,
        message: ErrFkDupName,
    },
    CatalogEntry {
        name: "ErrPasswordFormat",
        code: errcode::ErrPasswordFormat,
        message: ErrPasswordFormat,
    },
    CatalogEntry {
        name: "ErrFkColumnCannotDrop",
        code: errcode::ErrFkColumnCannotDrop,
        message: ErrFkColumnCannotDrop,
    },
    CatalogEntry {
        name: "ErrFkColumnCannotDropChild",
        code: errcode::ErrFkColumnCannotDropChild,
        message: ErrFkColumnCannotDropChild,
    },
    CatalogEntry {
        name: "ErrForeignKeyColumnNotNull",
        code: errcode::ErrForeignKeyColumnNotNull,
        message: ErrForeignKeyColumnNotNull,
    },
    CatalogEntry {
        name: "ErrDupIndex",
        code: errcode::ErrDupIndex,
        message: ErrDupIndex,
    },
    CatalogEntry {
        name: "ErrForeignKeyColumnCannotChange",
        code: errcode::ErrForeignKeyColumnCannotChange,
        message: ErrForeignKeyColumnCannotChange,
    },
    CatalogEntry {
        name: "ErrForeignKeyColumnCannotChangeChild",
        code: errcode::ErrForeignKeyColumnCannotChangeChild,
        message: ErrForeignKeyColumnCannotChangeChild,
    },
    CatalogEntry {
        name: "ErrFkCannotDeleteParent",
        code: errcode::ErrFkCannotDeleteParent,
        message: ErrFkCannotDeleteParent,
    },
    CatalogEntry {
        name: "ErrMalformedPacket",
        code: errcode::ErrMalformedPacket,
        message: ErrMalformedPacket,
    },
    CatalogEntry {
        name: "ErrReadOnlyMode",
        code: errcode::ErrReadOnlyMode,
        message: ErrReadOnlyMode,
    },
    CatalogEntry {
        name: "ErrVariableNotSettableInSp",
        code: errcode::ErrVariableNotSettableInSp,
        message: ErrVariableNotSettableInSp,
    },
    CatalogEntry {
        name: "ErrCantSetGtidPurgedWhenGtidModeIsOff",
        code: errcode::ErrCantSetGtidPurgedWhenGtidModeIsOff,
        message: ErrCantSetGtidPurgedWhenGtidModeIsOff,
    },
    CatalogEntry {
        name: "ErrCantSetGtidPurgedWhenGtidExecutedIsNotEmpty",
        code: errcode::ErrCantSetGtidPurgedWhenGtidExecutedIsNotEmpty,
        message: ErrCantSetGtidPurgedWhenGtidExecutedIsNotEmpty,
    },
    CatalogEntry {
        name: "ErrCantSetGtidPurgedWhenOwnedGtidsIsNotEmpty",
        code: errcode::ErrCantSetGtidPurgedWhenOwnedGtidsIsNotEmpty,
        message: ErrCantSetGtidPurgedWhenOwnedGtidsIsNotEmpty,
    },
    CatalogEntry {
        name: "ErrGtidPurgedWasChanged",
        code: errcode::ErrGtidPurgedWasChanged,
        message: ErrGtidPurgedWasChanged,
    },
    CatalogEntry {
        name: "ErrGtidExecutedWasChanged",
        code: errcode::ErrGtidExecutedWasChanged,
        message: ErrGtidExecutedWasChanged,
    },
    CatalogEntry {
        name: "ErrBinlogStmtModeAndNoReplTables",
        code: errcode::ErrBinlogStmtModeAndNoReplTables,
        message: ErrBinlogStmtModeAndNoReplTables,
    },
    CatalogEntry {
        name: "ErrAlterOperationNotSupported",
        code: errcode::ErrAlterOperationNotSupported,
        message: ErrAlterOperationNotSupported,
    },
    CatalogEntry {
        name: "ErrAlterOperationNotSupportedReason",
        code: errcode::ErrAlterOperationNotSupportedReason,
        message: ErrAlterOperationNotSupportedReason,
    },
    CatalogEntry {
        name: "ErrAlterOperationNotSupportedReasonCopy",
        code: errcode::ErrAlterOperationNotSupportedReasonCopy,
        message: ErrAlterOperationNotSupportedReasonCopy,
    },
    CatalogEntry {
        name: "ErrAlterOperationNotSupportedReasonPartition",
        code: errcode::ErrAlterOperationNotSupportedReasonPartition,
        message: ErrAlterOperationNotSupportedReasonPartition,
    },
    CatalogEntry {
        name: "ErrAlterOperationNotSupportedReasonFkRename",
        code: errcode::ErrAlterOperationNotSupportedReasonFkRename,
        message: ErrAlterOperationNotSupportedReasonFkRename,
    },
    CatalogEntry {
        name: "ErrAlterOperationNotSupportedReasonColumnType",
        code: errcode::ErrAlterOperationNotSupportedReasonColumnType,
        message: ErrAlterOperationNotSupportedReasonColumnType,
    },
    CatalogEntry {
        name: "ErrAlterOperationNotSupportedReasonFkCheck",
        code: errcode::ErrAlterOperationNotSupportedReasonFkCheck,
        message: ErrAlterOperationNotSupportedReasonFkCheck,
    },
    CatalogEntry {
        name: "ErrAlterOperationNotSupportedReasonIgnore",
        code: errcode::ErrAlterOperationNotSupportedReasonIgnore,
        message: ErrAlterOperationNotSupportedReasonIgnore,
    },
    CatalogEntry {
        name: "ErrAlterOperationNotSupportedReasonNopk",
        code: errcode::ErrAlterOperationNotSupportedReasonNopk,
        message: ErrAlterOperationNotSupportedReasonNopk,
    },
    CatalogEntry {
        name: "ErrAlterOperationNotSupportedReasonAutoinc",
        code: errcode::ErrAlterOperationNotSupportedReasonAutoinc,
        message: ErrAlterOperationNotSupportedReasonAutoinc,
    },
    CatalogEntry {
        name: "ErrAlterOperationNotSupportedReasonHiddenFts",
        code: errcode::ErrAlterOperationNotSupportedReasonHiddenFts,
        message: ErrAlterOperationNotSupportedReasonHiddenFts,
    },
    CatalogEntry {
        name: "ErrAlterOperationNotSupportedReasonChangeFts",
        code: errcode::ErrAlterOperationNotSupportedReasonChangeFts,
        message: ErrAlterOperationNotSupportedReasonChangeFts,
    },
    CatalogEntry {
        name: "ErrAlterOperationNotSupportedReasonFts",
        code: errcode::ErrAlterOperationNotSupportedReasonFts,
        message: ErrAlterOperationNotSupportedReasonFts,
    },
    CatalogEntry {
        name: "ErrDupUnknownInIndex",
        code: errcode::ErrDupUnknownInIndex,
        message: ErrDupUnknownInIndex,
    },
    CatalogEntry {
        name: "ErrIdentCausesTooLongPath",
        code: errcode::ErrIdentCausesTooLongPath,
        message: ErrIdentCausesTooLongPath,
    },
    CatalogEntry {
        name: "ErrAlterOperationNotSupportedReasonNotNull",
        code: errcode::ErrAlterOperationNotSupportedReasonNotNull,
        message: ErrAlterOperationNotSupportedReasonNotNull,
    },
    CatalogEntry {
        name: "ErrMustChangePasswordLogin",
        code: errcode::ErrMustChangePasswordLogin,
        message: ErrMustChangePasswordLogin,
    },
    CatalogEntry {
        name: "ErrRowInWrongPartition",
        code: errcode::ErrRowInWrongPartition,
        message: ErrRowInWrongPartition,
    },
    CatalogEntry {
        name: "ErrGeneratedColumnFunctionIsNotAllowed",
        code: errcode::ErrGeneratedColumnFunctionIsNotAllowed,
        message: ErrGeneratedColumnFunctionIsNotAllowed,
    },
    CatalogEntry {
        name: "ErrGeneratedColumnRowValueIsNotAllowed",
        code: errcode::ErrGeneratedColumnRowValueIsNotAllowed,
        message: ErrGeneratedColumnRowValueIsNotAllowed,
    },
    CatalogEntry {
        name: "ErrDefValGeneratedNamedFunctionIsNotAllowed",
        code: errcode::ErrDefValGeneratedNamedFunctionIsNotAllowed,
        message: ErrDefValGeneratedNamedFunctionIsNotAllowed,
    },
    CatalogEntry {
        name: "ErrUnsupportedAlterInplaceOnVirtualColumn",
        code: errcode::ErrUnsupportedAlterInplaceOnVirtualColumn,
        message: ErrUnsupportedAlterInplaceOnVirtualColumn,
    },
    CatalogEntry {
        name: "ErrWrongFKOptionForGeneratedColumn",
        code: errcode::ErrWrongFKOptionForGeneratedColumn,
        message: ErrWrongFKOptionForGeneratedColumn,
    },
    CatalogEntry {
        name: "ErrBadGeneratedColumn",
        code: errcode::ErrBadGeneratedColumn,
        message: ErrBadGeneratedColumn,
    },
    CatalogEntry {
        name: "ErrUnsupportedOnGeneratedColumn",
        code: errcode::ErrUnsupportedOnGeneratedColumn,
        message: ErrUnsupportedOnGeneratedColumn,
    },
    CatalogEntry {
        name: "ErrGeneratedColumnNonPrior",
        code: errcode::ErrGeneratedColumnNonPrior,
        message: ErrGeneratedColumnNonPrior,
    },
    CatalogEntry {
        name: "ErrDependentByGeneratedColumn",
        code: errcode::ErrDependentByGeneratedColumn,
        message: ErrDependentByGeneratedColumn,
    },
    CatalogEntry {
        name: "ErrGeneratedColumnRefAutoInc",
        code: errcode::ErrGeneratedColumnRefAutoInc,
        message: ErrGeneratedColumnRefAutoInc,
    },
    CatalogEntry {
        name: "ErrAccountHasBeenLocked",
        code: errcode::ErrAccountHasBeenLocked,
        message: ErrAccountHasBeenLocked,
    },
    CatalogEntry {
        name: "ErUserAccessDeniedForUserAccountBlockedByPasswordLock",
        code: errcode::ErUserAccessDeniedForUserAccountBlockedByPasswordLock,
        message: ErUserAccessDeniedForUserAccountBlockedByPasswordLock,
    },
    CatalogEntry {
        name: "ErrWarnConflictingHint",
        code: errcode::ErrWarnConflictingHint,
        message: ErrWarnConflictingHint,
    },
    CatalogEntry {
        name: "ErrUnresolvedHintName",
        code: errcode::ErrUnresolvedHintName,
        message: ErrUnresolvedHintName,
    },
    CatalogEntry {
        name: "ErrForeignKeyCascadeDepthExceeded",
        code: errcode::ErrForeignKeyCascadeDepthExceeded,
        message: ErrForeignKeyCascadeDepthExceeded,
    },
    CatalogEntry {
        name: "ErrInvalidFieldSize",
        code: errcode::ErrInvalidFieldSize,
        message: ErrInvalidFieldSize,
    },
    CatalogEntry {
        name: "ErrPasswordExpireAnonymousUser",
        code: errcode::ErrPasswordExpireAnonymousUser,
        message: ErrPasswordExpireAnonymousUser,
    },
    CatalogEntry {
        name: "ErrInvalidArgumentForLogarithm",
        code: errcode::ErrInvalidArgumentForLogarithm,
        message: ErrInvalidArgumentForLogarithm,
    },
    CatalogEntry {
        name: "ErrAggregateOrderNonAggQuery",
        code: errcode::ErrAggregateOrderNonAggQuery,
        message: ErrAggregateOrderNonAggQuery,
    },
    CatalogEntry {
        name: "ErrIncorrectType",
        code: errcode::ErrIncorrectType,
        message: ErrIncorrectType,
    },
    CatalogEntry {
        name: "ErrFieldInOrderNotSelect",
        code: errcode::ErrFieldInOrderNotSelect,
        message: ErrFieldInOrderNotSelect,
    },
    CatalogEntry {
        name: "ErrAggregateInOrderNotSelect",
        code: errcode::ErrAggregateInOrderNotSelect,
        message: ErrAggregateInOrderNotSelect,
    },
    CatalogEntry {
        name: "ErrInvalidJSONData",
        code: errcode::ErrInvalidJSONData,
        message: ErrInvalidJSONData,
    },
    CatalogEntry {
        name: "ErrInvalidJSONText",
        code: errcode::ErrInvalidJSONText,
        message: ErrInvalidJSONText,
    },
    CatalogEntry {
        name: "ErrInvalidJSONTextInParam",
        code: errcode::ErrInvalidJSONTextInParam,
        message: ErrInvalidJSONTextInParam,
    },
    CatalogEntry {
        name: "ErrInvalidJSONPath",
        code: errcode::ErrInvalidJSONPath,
        message: ErrInvalidJSONPath,
    },
    CatalogEntry {
        name: "ErrInvalidJSONCharset",
        code: errcode::ErrInvalidJSONCharset,
        message: ErrInvalidJSONCharset,
    },
    CatalogEntry {
        name: "ErrInvalidTypeForJSON",
        code: errcode::ErrInvalidTypeForJSON,
        message: ErrInvalidTypeForJSON,
    },
    CatalogEntry {
        name: "ErrInvalidJSONPathMultipleSelection",
        code: errcode::ErrInvalidJSONPathMultipleSelection,
        message: ErrInvalidJSONPathMultipleSelection,
    },
    CatalogEntry {
        name: "ErrInvalidJSONContainsPathType",
        code: errcode::ErrInvalidJSONContainsPathType,
        message: ErrInvalidJSONContainsPathType,
    },
    CatalogEntry {
        name: "ErrJSONUsedAsKey",
        code: errcode::ErrJSONUsedAsKey,
        message: ErrJSONUsedAsKey,
    },
    CatalogEntry {
        name: "ErrJSONDocumentTooDeep",
        code: errcode::ErrJSONDocumentTooDeep,
        message: ErrJSONDocumentTooDeep,
    },
    CatalogEntry {
        name: "ErrJSONDocumentNULLKey",
        code: errcode::ErrJSONDocumentNULLKey,
        message: ErrJSONDocumentNULLKey,
    },
    CatalogEntry {
        name: "ErrSecureTransportRequired",
        code: errcode::ErrSecureTransportRequired,
        message: ErrSecureTransportRequired,
    },
    CatalogEntry {
        name: "ErrBadUser",
        code: errcode::ErrBadUser,
        message: ErrBadUser,
    },
    CatalogEntry {
        name: "ErrUserAlreadyExists",
        code: errcode::ErrUserAlreadyExists,
        message: ErrUserAlreadyExists,
    },
    CatalogEntry {
        name: "ErrInvalidJSONPathArrayCell",
        code: errcode::ErrInvalidJSONPathArrayCell,
        message: ErrInvalidJSONPathArrayCell,
    },
    CatalogEntry {
        name: "ErrInvalidEncryptionOption",
        code: errcode::ErrInvalidEncryptionOption,
        message: ErrInvalidEncryptionOption,
    },
    CatalogEntry {
        name: "ErrTooLongValueForType",
        code: errcode::ErrTooLongValueForType,
        message: ErrTooLongValueForType,
    },
    CatalogEntry {
        name: "ErrPKIndexCantBeInvisible",
        code: errcode::ErrPKIndexCantBeInvisible,
        message: ErrPKIndexCantBeInvisible,
    },
    CatalogEntry {
        name: "ErrWindowNoSuchWindow",
        code: errcode::ErrWindowNoSuchWindow,
        message: ErrWindowNoSuchWindow,
    },
    CatalogEntry {
        name: "ErrWindowCircularityInWindowGraph",
        code: errcode::ErrWindowCircularityInWindowGraph,
        message: ErrWindowCircularityInWindowGraph,
    },
    CatalogEntry {
        name: "ErrWindowNoChildPartitioning",
        code: errcode::ErrWindowNoChildPartitioning,
        message: ErrWindowNoChildPartitioning,
    },
    CatalogEntry {
        name: "ErrWindowNoInherentFrame",
        code: errcode::ErrWindowNoInherentFrame,
        message: ErrWindowNoInherentFrame,
    },
    CatalogEntry {
        name: "ErrWindowNoRedefineOrderBy",
        code: errcode::ErrWindowNoRedefineOrderBy,
        message: ErrWindowNoRedefineOrderBy,
    },
    CatalogEntry {
        name: "ErrWindowFrameStartIllegal",
        code: errcode::ErrWindowFrameStartIllegal,
        message: ErrWindowFrameStartIllegal,
    },
    CatalogEntry {
        name: "ErrWindowFrameEndIllegal",
        code: errcode::ErrWindowFrameEndIllegal,
        message: ErrWindowFrameEndIllegal,
    },
    CatalogEntry {
        name: "ErrWindowFrameIllegal",
        code: errcode::ErrWindowFrameIllegal,
        message: ErrWindowFrameIllegal,
    },
    CatalogEntry {
        name: "ErrWindowRangeFrameOrderType",
        code: errcode::ErrWindowRangeFrameOrderType,
        message: ErrWindowRangeFrameOrderType,
    },
    CatalogEntry {
        name: "ErrWindowRangeFrameTemporalType",
        code: errcode::ErrWindowRangeFrameTemporalType,
        message: ErrWindowRangeFrameTemporalType,
    },
    CatalogEntry {
        name: "ErrWindowRangeFrameNumericType",
        code: errcode::ErrWindowRangeFrameNumericType,
        message: ErrWindowRangeFrameNumericType,
    },
    CatalogEntry {
        name: "ErrWindowRangeBoundNotConstant",
        code: errcode::ErrWindowRangeBoundNotConstant,
        message: ErrWindowRangeBoundNotConstant,
    },
    CatalogEntry {
        name: "ErrWindowDuplicateName",
        code: errcode::ErrWindowDuplicateName,
        message: ErrWindowDuplicateName,
    },
    CatalogEntry {
        name: "ErrWindowIllegalOrderBy",
        code: errcode::ErrWindowIllegalOrderBy,
        message: ErrWindowIllegalOrderBy,
    },
    CatalogEntry {
        name: "ErrWindowInvalidWindowFuncUse",
        code: errcode::ErrWindowInvalidWindowFuncUse,
        message: ErrWindowInvalidWindowFuncUse,
    },
    CatalogEntry {
        name: "ErrWindowInvalidWindowFuncAliasUse",
        code: errcode::ErrWindowInvalidWindowFuncAliasUse,
        message: ErrWindowInvalidWindowFuncAliasUse,
    },
    CatalogEntry {
        name: "ErrWindowNestedWindowFuncUseInWindowSpec",
        code: errcode::ErrWindowNestedWindowFuncUseInWindowSpec,
        message: ErrWindowNestedWindowFuncUseInWindowSpec,
    },
    CatalogEntry {
        name: "ErrWindowRowsIntervalUse",
        code: errcode::ErrWindowRowsIntervalUse,
        message: ErrWindowRowsIntervalUse,
    },
    CatalogEntry {
        name: "ErrWindowNoGroupOrderUnused",
        code: errcode::ErrWindowNoGroupOrderUnused,
        message: ErrWindowNoGroupOrderUnused,
    },
    CatalogEntry {
        name: "ErrWindowExplainJSON",
        code: errcode::ErrWindowExplainJSON,
        message: ErrWindowExplainJSON,
    },
    CatalogEntry {
        name: "ErrWindowFunctionIgnoresFrame",
        code: errcode::ErrWindowFunctionIgnoresFrame,
        message: ErrWindowFunctionIgnoresFrame,
    },
    CatalogEntry {
        name: "ErrInvalidNumberOfArgs",
        code: errcode::ErrInvalidNumberOfArgs,
        message: ErrInvalidNumberOfArgs,
    },
    CatalogEntry {
        name: "ErrFieldInGroupingNotGroupBy",
        code: errcode::ErrFieldInGroupingNotGroupBy,
        message: ErrFieldInGroupingNotGroupBy,
    },
    CatalogEntry {
        name: "ErrRoleNotGranted",
        code: errcode::ErrRoleNotGranted,
        message: ErrRoleNotGranted,
    },
    CatalogEntry {
        name: "ErrMaxExecTimeExceeded",
        code: errcode::ErrMaxExecTimeExceeded,
        message: ErrMaxExecTimeExceeded,
    },
    CatalogEntry {
        name: "ErrLockAcquireFailAndNoWaitSet",
        code: errcode::ErrLockAcquireFailAndNoWaitSet,
        message: ErrLockAcquireFailAndNoWaitSet,
    },
    CatalogEntry {
        name: "ErrNotHintUpdatable",
        code: errcode::ErrNotHintUpdatable,
        message: ErrNotHintUpdatable,
    },
    CatalogEntry {
        name: "ErrExistsInHistoryPassword",
        code: errcode::ErrExistsInHistoryPassword,
        message: ErrExistsInHistoryPassword,
    },
    CatalogEntry {
        name: "ErrInvalidDefaultUTF8MB4Collation",
        code: errcode::ErrInvalidDefaultUTF8MB4Collation,
        message: ErrInvalidDefaultUTF8MB4Collation,
    },
    CatalogEntry {
        name: "ErrForeignKeyCannotDropParent",
        code: errcode::ErrForeignKeyCannotDropParent,
        message: ErrForeignKeyCannotDropParent,
    },
    CatalogEntry {
        name: "ErrForeignKeyCannotUseVirtualColumn",
        code: errcode::ErrForeignKeyCannotUseVirtualColumn,
        message: ErrForeignKeyCannotUseVirtualColumn,
    },
    CatalogEntry {
        name: "ErrForeignKeyNoColumnInParent",
        code: errcode::ErrForeignKeyNoColumnInParent,
        message: ErrForeignKeyNoColumnInParent,
    },
    CatalogEntry {
        name: "ErrDataTruncatedFunctionalIndex",
        code: errcode::ErrDataTruncatedFunctionalIndex,
        message: ErrDataTruncatedFunctionalIndex,
    },
    CatalogEntry {
        name: "ErrDataOutOfRangeFunctionalIndex",
        code: errcode::ErrDataOutOfRangeFunctionalIndex,
        message: ErrDataOutOfRangeFunctionalIndex,
    },
    CatalogEntry {
        name: "ErrFunctionalIndexOnJSONOrGeometryFunction",
        code: errcode::ErrFunctionalIndexOnJSONOrGeometryFunction,
        message: ErrFunctionalIndexOnJSONOrGeometryFunction,
    },
    CatalogEntry {
        name: "ErrFunctionalIndexRefAutoIncrement",
        code: errcode::ErrFunctionalIndexRefAutoIncrement,
        message: ErrFunctionalIndexRefAutoIncrement,
    },
    CatalogEntry {
        name: "ErrCannotDropColumnFunctionalIndex",
        code: errcode::ErrCannotDropColumnFunctionalIndex,
        message: ErrCannotDropColumnFunctionalIndex,
    },
    CatalogEntry {
        name: "ErrFunctionalIndexPrimaryKey",
        code: errcode::ErrFunctionalIndexPrimaryKey,
        message: ErrFunctionalIndexPrimaryKey,
    },
    CatalogEntry {
        name: "ErrFunctionalIndexOnBlob",
        code: errcode::ErrFunctionalIndexOnBlob,
        message: ErrFunctionalIndexOnBlob,
    },
    CatalogEntry {
        name: "ErrFunctionalIndexFunctionIsNotAllowed",
        code: errcode::ErrFunctionalIndexFunctionIsNotAllowed,
        message: ErrFunctionalIndexFunctionIsNotAllowed,
    },
    CatalogEntry {
        name: "ErrFulltextFunctionalIndex",
        code: errcode::ErrFulltextFunctionalIndex,
        message: ErrFulltextFunctionalIndex,
    },
    CatalogEntry {
        name: "ErrSpatialFunctionalIndex",
        code: errcode::ErrSpatialFunctionalIndex,
        message: ErrSpatialFunctionalIndex,
    },
    CatalogEntry {
        name: "ErrWrongKeyColumnFunctionalIndex",
        code: errcode::ErrWrongKeyColumnFunctionalIndex,
        message: ErrWrongKeyColumnFunctionalIndex,
    },
    CatalogEntry {
        name: "ErrFunctionalIndexOnField",
        code: errcode::ErrFunctionalIndexOnField,
        message: ErrFunctionalIndexOnField,
    },
    CatalogEntry {
        name: "ErrFKIncompatibleColumns",
        code: errcode::ErrFKIncompatibleColumns,
        message: ErrFKIncompatibleColumns,
    },
    CatalogEntry {
        name: "ErrFunctionalIndexRowValueIsNotAllowed",
        code: errcode::ErrFunctionalIndexRowValueIsNotAllowed,
        message: ErrFunctionalIndexRowValueIsNotAllowed,
    },
    CatalogEntry {
        name: "ErrInvalidLateralJoin",
        code: errcode::ErrInvalidLateralJoin,
        message: ErrInvalidLateralJoin,
    },
    CatalogEntry {
        name: "ErrNonBooleanExprForCheckConstraint",
        code: errcode::ErrNonBooleanExprForCheckConstraint,
        message: ErrNonBooleanExprForCheckConstraint,
    },
    CatalogEntry {
        name: "ErrColumnCheckConstraintReferencesOtherColumn",
        code: errcode::ErrColumnCheckConstraintReferencesOtherColumn,
        message: ErrColumnCheckConstraintReferencesOtherColumn,
    },
    CatalogEntry {
        name: "ErrCheckConstraintNamedFunctionIsNotAllowed",
        code: errcode::ErrCheckConstraintNamedFunctionIsNotAllowed,
        message: ErrCheckConstraintNamedFunctionIsNotAllowed,
    },
    CatalogEntry {
        name: "ErrCheckConstraintFunctionIsNotAllowed",
        code: errcode::ErrCheckConstraintFunctionIsNotAllowed,
        message: ErrCheckConstraintFunctionIsNotAllowed,
    },
    CatalogEntry {
        name: "ErrCheckConstraintVariables",
        code: errcode::ErrCheckConstraintVariables,
        message: ErrCheckConstraintVariables,
    },
    CatalogEntry {
        name: "ErrCheckConstraintRefersAutoIncrementColumn",
        code: errcode::ErrCheckConstraintRefersAutoIncrementColumn,
        message: ErrCheckConstraintRefersAutoIncrementColumn,
    },
    CatalogEntry {
        name: "ErrCheckConstraintViolated",
        code: errcode::ErrCheckConstraintViolated,
        message: ErrCheckConstraintViolated,
    },
    CatalogEntry {
        name: "ErrTableCheckConstraintReferUnknown",
        code: errcode::ErrTableCheckConstraintReferUnknown,
        message: ErrTableCheckConstraintReferUnknown,
    },
    CatalogEntry {
        name: "ErrCheckConstraintDupName",
        code: errcode::ErrCheckConstraintDupName,
        message: ErrCheckConstraintDupName,
    },
    CatalogEntry {
        name: "ErrCheckConstraintClauseUsingFKReferActionColumn",
        code: errcode::ErrCheckConstraintClauseUsingFKReferActionColumn,
        message: ErrCheckConstraintClauseUsingFKReferActionColumn,
    },
    CatalogEntry {
        name: "ErrDependentByFunctionalIndex",
        code: errcode::ErrDependentByFunctionalIndex,
        message: ErrDependentByFunctionalIndex,
    },
    CatalogEntry {
        name: "ErrDependentByPartitionFunctional",
        code: errcode::ErrDependentByPartitionFunctional,
        message: ErrDependentByPartitionFunctional,
    },
    CatalogEntry {
        name: "ErrCannotConvertString",
        code: errcode::ErrCannotConvertString,
        message: ErrCannotConvertString,
    },
    CatalogEntry {
        name: "ErrInvalidJSONType",
        code: errcode::ErrInvalidJSONType,
        message: ErrInvalidJSONType,
    },
    CatalogEntry {
        name: "ErrInvalidJSONValueForFuncIndex",
        code: errcode::ErrInvalidJSONValueForFuncIndex,
        message: ErrInvalidJSONValueForFuncIndex,
    },
    CatalogEntry {
        name: "ErrJSONValueOutOfRangeForFuncIndex",
        code: errcode::ErrJSONValueOutOfRangeForFuncIndex,
        message: ErrJSONValueOutOfRangeForFuncIndex,
    },
    CatalogEntry {
        name: "ErrFunctionalIndexDataIsTooLong",
        code: errcode::ErrFunctionalIndexDataIsTooLong,
        message: ErrFunctionalIndexDataIsTooLong,
    },
    CatalogEntry {
        name: "ErrFunctionalIndexNotApplicable",
        code: errcode::ErrFunctionalIndexNotApplicable,
        message: ErrFunctionalIndexNotApplicable,
    },
    CatalogEntry {
        name: "ErrUnsupportedConstraintCheck",
        code: errcode::ErrUnsupportedConstraintCheck,
        message: ErrUnsupportedConstraintCheck,
    },
    CatalogEntry {
        name: "ErrDynamicPrivilegeNotRegistered",
        code: errcode::ErrDynamicPrivilegeNotRegistered,
        message: ErrDynamicPrivilegeNotRegistered,
    },
    CatalogEntry {
        name: "ErrIllegalPrivilegeLevel",
        code: errcode::ErrIllegalPrivilegeLevel,
        message: ErrIllegalPrivilegeLevel,
    },
    CatalogEntry {
        name: "ErrCTERecursiveRequiresUnion",
        code: errcode::ErrCTERecursiveRequiresUnion,
        message: ErrCTERecursiveRequiresUnion,
    },
    CatalogEntry {
        name: "ErrCTERecursiveRequiresNonRecursiveFirst",
        code: errcode::ErrCTERecursiveRequiresNonRecursiveFirst,
        message: ErrCTERecursiveRequiresNonRecursiveFirst,
    },
    CatalogEntry {
        name: "ErrCTERecursiveForbidsAggregation",
        code: errcode::ErrCTERecursiveForbidsAggregation,
        message: ErrCTERecursiveForbidsAggregation,
    },
    CatalogEntry {
        name: "ErrCTERecursiveForbiddenJoinOrder",
        code: errcode::ErrCTERecursiveForbiddenJoinOrder,
        message: ErrCTERecursiveForbiddenJoinOrder,
    },
    CatalogEntry {
        name: "ErrInvalidRequiresSingleReference",
        code: errcode::ErrInvalidRequiresSingleReference,
        message: ErrInvalidRequiresSingleReference,
    },
    CatalogEntry {
        name: "ErrCTEMaxRecursionDepth",
        code: errcode::ErrCTEMaxRecursionDepth,
        message: ErrCTEMaxRecursionDepth,
    },
    CatalogEntry {
        name: "ErrTableWithoutPrimaryKey",
        code: errcode::ErrTableWithoutPrimaryKey,
        message: ErrTableWithoutPrimaryKey,
    },
    CatalogEntry {
        name: "ErrConstraintNotFound",
        code: errcode::ErrConstraintNotFound,
        message: ErrConstraintNotFound,
    },
    CatalogEntry {
        name: "ErrDependentByCheckConstraint",
        code: errcode::ErrDependentByCheckConstraint,
        message: ErrDependentByCheckConstraint,
    },
    CatalogEntry {
        name: "ErrEngineAttributeNotSupported",
        code: errcode::ErrEngineAttributeNotSupported,
        message: ErrEngineAttributeNotSupported,
    },
    CatalogEntry {
        name: "ErrJSONInBooleanContext",
        code: errcode::ErrJSONInBooleanContext,
        message: ErrJSONInBooleanContext,
    },
    CatalogEntry {
        name: "ErrSecondPasswordCannotBeEmpty",
        code: errcode::ErrSecondPasswordCannotBeEmpty,
        message: ErrSecondPasswordCannotBeEmpty,
    },
    CatalogEntry {
        name: "ErrPasswordCannotBeRetainedOnPluginChange",
        code: errcode::ErrPasswordCannotBeRetainedOnPluginChange,
        message: ErrPasswordCannotBeRetainedOnPluginChange,
    },
    CatalogEntry {
        name: "ErrCurrentPasswordCannotBeRetained",
        code: errcode::ErrCurrentPasswordCannotBeRetained,
        message: ErrCurrentPasswordCannotBeRetained,
    },
    CatalogEntry {
        name: "ErrOnlyOneDefaultPartionAllowed",
        code: errcode::ErrOnlyOneDefaultPartionAllowed,
        message: ErrOnlyOneDefaultPartionAllowed,
    },
    CatalogEntry {
        name: "ErrWrongPartitionTypeExpectedSystemTime",
        code: errcode::ErrWrongPartitionTypeExpectedSystemTime,
        message: ErrWrongPartitionTypeExpectedSystemTime,
    },
    CatalogEntry {
        name: "ErrSystemVersioningWrongPartitions",
        code: errcode::ErrSystemVersioningWrongPartitions,
        message: ErrSystemVersioningWrongPartitions,
    },
    CatalogEntry {
        name: "ErrSequenceRunOut",
        code: errcode::ErrSequenceRunOut,
        message: ErrSequenceRunOut,
    },
    CatalogEntry {
        name: "ErrSequenceInvalidData",
        code: errcode::ErrSequenceInvalidData,
        message: ErrSequenceInvalidData,
    },
    CatalogEntry {
        name: "ErrSequenceAccessFail",
        code: errcode::ErrSequenceAccessFail,
        message: ErrSequenceAccessFail,
    },
    CatalogEntry {
        name: "ErrNotSequence",
        code: errcode::ErrNotSequence,
        message: ErrNotSequence,
    },
    CatalogEntry {
        name: "ErrUnknownSequence",
        code: errcode::ErrUnknownSequence,
        message: ErrUnknownSequence,
    },
    CatalogEntry {
        name: "ErrWrongInsertIntoSequence",
        code: errcode::ErrWrongInsertIntoSequence,
        message: ErrWrongInsertIntoSequence,
    },
    CatalogEntry {
        name: "ErrSequenceInvalidTableStructure",
        code: errcode::ErrSequenceInvalidTableStructure,
        message: ErrSequenceInvalidTableStructure,
    },
    CatalogEntry {
        name: "ErrMemExceedThreshold",
        code: errcode::ErrMemExceedThreshold,
        message: ErrMemExceedThreshold,
    },
    CatalogEntry {
        name: "ErrForUpdateCantRetry",
        code: errcode::ErrForUpdateCantRetry,
        message: ErrForUpdateCantRetry,
    },
    CatalogEntry {
        name: "ErrAdminCheckTable",
        code: errcode::ErrAdminCheckTable,
        message: ErrAdminCheckTable,
    },
    CatalogEntry {
        name: "ErrOptOnTemporaryTable",
        code: errcode::ErrOptOnTemporaryTable,
        message: ErrOptOnTemporaryTable,
    },
    CatalogEntry {
        name: "ErrDropTableOnTemporaryTable",
        code: errcode::ErrDropTableOnTemporaryTable,
        message: ErrDropTableOnTemporaryTable,
    },
    CatalogEntry {
        name: "ErrTxnTooLarge",
        code: errcode::ErrTxnTooLarge,
        message: ErrTxnTooLarge,
    },
    CatalogEntry {
        name: "ErrWriteConflictInTiDB",
        code: errcode::ErrWriteConflictInTiDB,
        message: ErrWriteConflictInTiDB,
    },
    CatalogEntry {
        name: "ErrInvalidPluginID",
        code: errcode::ErrInvalidPluginID,
        message: ErrInvalidPluginID,
    },
    CatalogEntry {
        name: "ErrInvalidPluginManifest",
        code: errcode::ErrInvalidPluginManifest,
        message: ErrInvalidPluginManifest,
    },
    CatalogEntry {
        name: "ErrInvalidPluginName",
        code: errcode::ErrInvalidPluginName,
        message: ErrInvalidPluginName,
    },
    CatalogEntry {
        name: "ErrInvalidPluginVersion",
        code: errcode::ErrInvalidPluginVersion,
        message: ErrInvalidPluginVersion,
    },
    CatalogEntry {
        name: "ErrDuplicatePlugin",
        code: errcode::ErrDuplicatePlugin,
        message: ErrDuplicatePlugin,
    },
    CatalogEntry {
        name: "ErrInvalidPluginSysVarName",
        code: errcode::ErrInvalidPluginSysVarName,
        message: ErrInvalidPluginSysVarName,
    },
    CatalogEntry {
        name: "ErrRequireVersionCheckFail",
        code: errcode::ErrRequireVersionCheckFail,
        message: ErrRequireVersionCheckFail,
    },
    CatalogEntry {
        name: "ErrUnsupportedReloadPlugin",
        code: errcode::ErrUnsupportedReloadPlugin,
        message: ErrUnsupportedReloadPlugin,
    },
    CatalogEntry {
        name: "ErrUnsupportedReloadPluginVar",
        code: errcode::ErrUnsupportedReloadPluginVar,
        message: ErrUnsupportedReloadPluginVar,
    },
    CatalogEntry {
        name: "ErrTableLocked",
        code: errcode::ErrTableLocked,
        message: ErrTableLocked,
    },
    CatalogEntry {
        name: "ErrNotExist",
        code: errcode::ErrNotExist,
        message: ErrNotExist,
    },
    CatalogEntry {
        name: "ErrTxnRetryable",
        code: errcode::ErrTxnRetryable,
        message: ErrTxnRetryable,
    },
    CatalogEntry {
        name: "ErrCannotSetNilValue",
        code: errcode::ErrCannotSetNilValue,
        message: ErrCannotSetNilValue,
    },
    CatalogEntry {
        name: "ErrInvalidTxn",
        code: errcode::ErrInvalidTxn,
        message: ErrInvalidTxn,
    },
    CatalogEntry {
        name: "ErrEntryTooLarge",
        code: errcode::ErrEntryTooLarge,
        message: ErrEntryTooLarge,
    },
];
