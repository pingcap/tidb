// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");

//! Message catalog entries, part 2 of 4 (see `errname/mod.rs`).

use super::consts_2::*;
use super::consts_3::*;
use super::errcode;
use super::CatalogEntry;

pub(super) const CATALOG_2: &[CatalogEntry] = &[
    CatalogEntry {
        name: "ErrStmtNotAllowedInSfOrTrg",
        code: errcode::ErrStmtNotAllowedInSfOrTrg,
        message: ErrStmtNotAllowedInSfOrTrg,
    },
    CatalogEntry {
        name: "ErrSpVarcondAfterCurshndlr",
        code: errcode::ErrSpVarcondAfterCurshndlr,
        message: ErrSpVarcondAfterCurshndlr,
    },
    CatalogEntry {
        name: "ErrSpCursorAfterHandler",
        code: errcode::ErrSpCursorAfterHandler,
        message: ErrSpCursorAfterHandler,
    },
    CatalogEntry {
        name: "ErrSpCaseNotFound",
        code: errcode::ErrSpCaseNotFound,
        message: ErrSpCaseNotFound,
    },
    CatalogEntry {
        name: "ErrFparserTooBigFile",
        code: errcode::ErrFparserTooBigFile,
        message: ErrFparserTooBigFile,
    },
    CatalogEntry {
        name: "ErrFparserBadHeader",
        code: errcode::ErrFparserBadHeader,
        message: ErrFparserBadHeader,
    },
    CatalogEntry {
        name: "ErrFparserEOFInComment",
        code: errcode::ErrFparserEOFInComment,
        message: ErrFparserEOFInComment,
    },
    CatalogEntry {
        name: "ErrFparserErrorInParameter",
        code: errcode::ErrFparserErrorInParameter,
        message: ErrFparserErrorInParameter,
    },
    CatalogEntry {
        name: "ErrFparserEOFInUnknownParameter",
        code: errcode::ErrFparserEOFInUnknownParameter,
        message: ErrFparserEOFInUnknownParameter,
    },
    CatalogEntry {
        name: "ErrViewNoExplain",
        code: errcode::ErrViewNoExplain,
        message: ErrViewNoExplain,
    },
    CatalogEntry {
        name: "ErrFrmUnknownType",
        code: errcode::ErrFrmUnknownType,
        message: ErrFrmUnknownType,
    },
    CatalogEntry {
        name: "ErrWrongObject",
        code: errcode::ErrWrongObject,
        message: ErrWrongObject,
    },
    CatalogEntry {
        name: "ErrNonupdateableColumn",
        code: errcode::ErrNonupdateableColumn,
        message: ErrNonupdateableColumn,
    },
    CatalogEntry {
        name: "ErrViewSelectDerived",
        code: errcode::ErrViewSelectDerived,
        message: ErrViewSelectDerived,
    },
    CatalogEntry {
        name: "ErrViewSelectClause",
        code: errcode::ErrViewSelectClause,
        message: ErrViewSelectClause,
    },
    CatalogEntry {
        name: "ErrViewSelectVariable",
        code: errcode::ErrViewSelectVariable,
        message: ErrViewSelectVariable,
    },
    CatalogEntry {
        name: "ErrViewSelectTmptable",
        code: errcode::ErrViewSelectTmptable,
        message: ErrViewSelectTmptable,
    },
    CatalogEntry {
        name: "ErrViewWrongList",
        code: errcode::ErrViewWrongList,
        message: ErrViewWrongList,
    },
    CatalogEntry {
        name: "ErrWarnViewMerge",
        code: errcode::ErrWarnViewMerge,
        message: ErrWarnViewMerge,
    },
    CatalogEntry {
        name: "ErrWarnViewWithoutKey",
        code: errcode::ErrWarnViewWithoutKey,
        message: ErrWarnViewWithoutKey,
    },
    CatalogEntry {
        name: "ErrViewInvalid",
        code: errcode::ErrViewInvalid,
        message: ErrViewInvalid,
    },
    CatalogEntry {
        name: "ErrSpNoDropSp",
        code: errcode::ErrSpNoDropSp,
        message: ErrSpNoDropSp,
    },
    CatalogEntry {
        name: "ErrSpGotoInHndlr",
        code: errcode::ErrSpGotoInHndlr,
        message: ErrSpGotoInHndlr,
    },
    CatalogEntry {
        name: "ErrTrgAlreadyExists",
        code: errcode::ErrTrgAlreadyExists,
        message: ErrTrgAlreadyExists,
    },
    CatalogEntry {
        name: "ErrTrgDoesNotExist",
        code: errcode::ErrTrgDoesNotExist,
        message: ErrTrgDoesNotExist,
    },
    CatalogEntry {
        name: "ErrTrgOnViewOrTempTable",
        code: errcode::ErrTrgOnViewOrTempTable,
        message: ErrTrgOnViewOrTempTable,
    },
    CatalogEntry {
        name: "ErrTrgCantChangeRow",
        code: errcode::ErrTrgCantChangeRow,
        message: ErrTrgCantChangeRow,
    },
    CatalogEntry {
        name: "ErrTrgNoSuchRowInTrg",
        code: errcode::ErrTrgNoSuchRowInTrg,
        message: ErrTrgNoSuchRowInTrg,
    },
    CatalogEntry {
        name: "ErrNoDefaultForField",
        code: errcode::ErrNoDefaultForField,
        message: ErrNoDefaultForField,
    },
    CatalogEntry {
        name: "ErrDivisionByZero",
        code: errcode::ErrDivisionByZero,
        message: ErrDivisionByZero,
    },
    CatalogEntry {
        name: "ErrTruncatedWrongValueForField",
        code: errcode::ErrTruncatedWrongValueForField,
        message: ErrTruncatedWrongValueForField,
    },
    CatalogEntry {
        name: "ErrIllegalValueForType",
        code: errcode::ErrIllegalValueForType,
        message: ErrIllegalValueForType,
    },
    CatalogEntry {
        name: "ErrViewNonupdCheck",
        code: errcode::ErrViewNonupdCheck,
        message: ErrViewNonupdCheck,
    },
    CatalogEntry {
        name: "ErrViewCheckFailed",
        code: errcode::ErrViewCheckFailed,
        message: ErrViewCheckFailed,
    },
    CatalogEntry {
        name: "ErrProcaccessDenied",
        code: errcode::ErrProcaccessDenied,
        message: ErrProcaccessDenied,
    },
    CatalogEntry {
        name: "ErrRelayLogFail",
        code: errcode::ErrRelayLogFail,
        message: ErrRelayLogFail,
    },
    CatalogEntry {
        name: "ErrPasswdLength",
        code: errcode::ErrPasswdLength,
        message: ErrPasswdLength,
    },
    CatalogEntry {
        name: "ErrUnknownTargetBinlog",
        code: errcode::ErrUnknownTargetBinlog,
        message: ErrUnknownTargetBinlog,
    },
    CatalogEntry {
        name: "ErrIoErrLogIndexRead",
        code: errcode::ErrIoErrLogIndexRead,
        message: ErrIoErrLogIndexRead,
    },
    CatalogEntry {
        name: "ErrBinlogPurgeProhibited",
        code: errcode::ErrBinlogPurgeProhibited,
        message: ErrBinlogPurgeProhibited,
    },
    CatalogEntry {
        name: "ErrFseekFail",
        code: errcode::ErrFseekFail,
        message: ErrFseekFail,
    },
    CatalogEntry {
        name: "ErrBinlogPurgeFatalErr",
        code: errcode::ErrBinlogPurgeFatalErr,
        message: ErrBinlogPurgeFatalErr,
    },
    CatalogEntry {
        name: "ErrLogInUse",
        code: errcode::ErrLogInUse,
        message: ErrLogInUse,
    },
    CatalogEntry {
        name: "ErrLogPurgeUnknownErr",
        code: errcode::ErrLogPurgeUnknownErr,
        message: ErrLogPurgeUnknownErr,
    },
    CatalogEntry {
        name: "ErrRelayLogInit",
        code: errcode::ErrRelayLogInit,
        message: ErrRelayLogInit,
    },
    CatalogEntry {
        name: "ErrNoBinaryLogging",
        code: errcode::ErrNoBinaryLogging,
        message: ErrNoBinaryLogging,
    },
    CatalogEntry {
        name: "ErrReservedSyntax",
        code: errcode::ErrReservedSyntax,
        message: ErrReservedSyntax,
    },
    CatalogEntry {
        name: "ErrWsasFailed",
        code: errcode::ErrWsasFailed,
        message: ErrWsasFailed,
    },
    CatalogEntry {
        name: "ErrDiffGroupsProc",
        code: errcode::ErrDiffGroupsProc,
        message: ErrDiffGroupsProc,
    },
    CatalogEntry {
        name: "ErrNoGroupForProc",
        code: errcode::ErrNoGroupForProc,
        message: ErrNoGroupForProc,
    },
    CatalogEntry {
        name: "ErrOrderWithProc",
        code: errcode::ErrOrderWithProc,
        message: ErrOrderWithProc,
    },
    CatalogEntry {
        name: "ErrLoggingProhibitChangingOf",
        code: errcode::ErrLoggingProhibitChangingOf,
        message: ErrLoggingProhibitChangingOf,
    },
    CatalogEntry {
        name: "ErrNoFileMapping",
        code: errcode::ErrNoFileMapping,
        message: ErrNoFileMapping,
    },
    CatalogEntry {
        name: "ErrWrongMagic",
        code: errcode::ErrWrongMagic,
        message: ErrWrongMagic,
    },
    CatalogEntry {
        name: "ErrPsManyParam",
        code: errcode::ErrPsManyParam,
        message: ErrPsManyParam,
    },
    CatalogEntry {
        name: "ErrKeyPart0",
        code: errcode::ErrKeyPart0,
        message: ErrKeyPart0,
    },
    CatalogEntry {
        name: "ErrViewChecksum",
        code: errcode::ErrViewChecksum,
        message: ErrViewChecksum,
    },
    CatalogEntry {
        name: "ErrViewMultiupdate",
        code: errcode::ErrViewMultiupdate,
        message: ErrViewMultiupdate,
    },
    CatalogEntry {
        name: "ErrViewNoInsertFieldList",
        code: errcode::ErrViewNoInsertFieldList,
        message: ErrViewNoInsertFieldList,
    },
    CatalogEntry {
        name: "ErrViewDeleteMergeView",
        code: errcode::ErrViewDeleteMergeView,
        message: ErrViewDeleteMergeView,
    },
    CatalogEntry {
        name: "ErrCannotUser",
        code: errcode::ErrCannotUser,
        message: ErrCannotUser,
    },
    CatalogEntry {
        name: "ErrGrantRole",
        code: errcode::ErrGrantRole,
        message: ErrGrantRole,
    },
    CatalogEntry {
        name: "ErrXaerNota",
        code: errcode::ErrXaerNota,
        message: ErrXaerNota,
    },
    CatalogEntry {
        name: "ErrXaerInval",
        code: errcode::ErrXaerInval,
        message: ErrXaerInval,
    },
    CatalogEntry {
        name: "ErrXaerRmfail",
        code: errcode::ErrXaerRmfail,
        message: ErrXaerRmfail,
    },
    CatalogEntry {
        name: "ErrXaerOutside",
        code: errcode::ErrXaerOutside,
        message: ErrXaerOutside,
    },
    CatalogEntry {
        name: "ErrXaerRmerr",
        code: errcode::ErrXaerRmerr,
        message: ErrXaerRmerr,
    },
    CatalogEntry {
        name: "ErrXaRbrollback",
        code: errcode::ErrXaRbrollback,
        message: ErrXaRbrollback,
    },
    CatalogEntry {
        name: "ErrNonexistingProcGrant",
        code: errcode::ErrNonexistingProcGrant,
        message: ErrNonexistingProcGrant,
    },
    CatalogEntry {
        name: "ErrProcAutoGrantFail",
        code: errcode::ErrProcAutoGrantFail,
        message: ErrProcAutoGrantFail,
    },
    CatalogEntry {
        name: "ErrProcAutoRevokeFail",
        code: errcode::ErrProcAutoRevokeFail,
        message: ErrProcAutoRevokeFail,
    },
    CatalogEntry {
        name: "ErrDataTooLong",
        code: errcode::ErrDataTooLong,
        message: ErrDataTooLong,
    },
    CatalogEntry {
        name: "ErrSpBadSQLstate",
        code: errcode::ErrSpBadSQLstate,
        message: ErrSpBadSQLstate,
    },
    CatalogEntry {
        name: "ErrStartup",
        code: errcode::ErrStartup,
        message: ErrStartup,
    },
    CatalogEntry {
        name: "ErrLoadFromFixedSizeRowsToVar",
        code: errcode::ErrLoadFromFixedSizeRowsToVar,
        message: ErrLoadFromFixedSizeRowsToVar,
    },
    CatalogEntry {
        name: "ErrCantCreateUserWithGrant",
        code: errcode::ErrCantCreateUserWithGrant,
        message: ErrCantCreateUserWithGrant,
    },
    CatalogEntry {
        name: "ErrWrongValueForType",
        code: errcode::ErrWrongValueForType,
        message: ErrWrongValueForType,
    },
    CatalogEntry {
        name: "ErrTableDefChanged",
        code: errcode::ErrTableDefChanged,
        message: ErrTableDefChanged,
    },
    CatalogEntry {
        name: "ErrSpDupHandler",
        code: errcode::ErrSpDupHandler,
        message: ErrSpDupHandler,
    },
    CatalogEntry {
        name: "ErrSpNotVarArg",
        code: errcode::ErrSpNotVarArg,
        message: ErrSpNotVarArg,
    },
    CatalogEntry {
        name: "ErrSpNoRetset",
        code: errcode::ErrSpNoRetset,
        message: ErrSpNoRetset,
    },
    CatalogEntry {
        name: "ErrCantCreateGeometryObject",
        code: errcode::ErrCantCreateGeometryObject,
        message: ErrCantCreateGeometryObject,
    },
    CatalogEntry {
        name: "ErrFailedRoutineBreakBinlog",
        code: errcode::ErrFailedRoutineBreakBinlog,
        message: ErrFailedRoutineBreakBinlog,
    },
    CatalogEntry {
        name: "ErrBinlogUnsafeRoutine",
        code: errcode::ErrBinlogUnsafeRoutine,
        message: ErrBinlogUnsafeRoutine,
    },
    CatalogEntry {
        name: "ErrBinlogCreateRoutineNeedSuper",
        code: errcode::ErrBinlogCreateRoutineNeedSuper,
        message: ErrBinlogCreateRoutineNeedSuper,
    },
    CatalogEntry {
        name: "ErrExecStmtWithOpenCursor",
        code: errcode::ErrExecStmtWithOpenCursor,
        message: ErrExecStmtWithOpenCursor,
    },
    CatalogEntry {
        name: "ErrStmtHasNoOpenCursor",
        code: errcode::ErrStmtHasNoOpenCursor,
        message: ErrStmtHasNoOpenCursor,
    },
    CatalogEntry {
        name: "ErrCommitNotAllowedInSfOrTrg",
        code: errcode::ErrCommitNotAllowedInSfOrTrg,
        message: ErrCommitNotAllowedInSfOrTrg,
    },
    CatalogEntry {
        name: "ErrNoDefaultForViewField",
        code: errcode::ErrNoDefaultForViewField,
        message: ErrNoDefaultForViewField,
    },
    CatalogEntry {
        name: "ErrSpNoRecursion",
        code: errcode::ErrSpNoRecursion,
        message: ErrSpNoRecursion,
    },
    CatalogEntry {
        name: "ErrTooBigScale",
        code: errcode::ErrTooBigScale,
        message: ErrTooBigScale,
    },
    CatalogEntry {
        name: "ErrTooBigPrecision",
        code: errcode::ErrTooBigPrecision,
        message: ErrTooBigPrecision,
    },
    CatalogEntry {
        name: "ErrMBiggerThanD",
        code: errcode::ErrMBiggerThanD,
        message: ErrMBiggerThanD,
    },
    CatalogEntry {
        name: "ErrWrongLockOfSystemTable",
        code: errcode::ErrWrongLockOfSystemTable,
        message: ErrWrongLockOfSystemTable,
    },
    CatalogEntry {
        name: "ErrConnectToForeignDataSource",
        code: errcode::ErrConnectToForeignDataSource,
        message: ErrConnectToForeignDataSource,
    },
    CatalogEntry {
        name: "ErrQueryOnForeignDataSource",
        code: errcode::ErrQueryOnForeignDataSource,
        message: ErrQueryOnForeignDataSource,
    },
    CatalogEntry {
        name: "ErrForeignDataSourceDoesntExist",
        code: errcode::ErrForeignDataSourceDoesntExist,
        message: ErrForeignDataSourceDoesntExist,
    },
    CatalogEntry {
        name: "ErrForeignDataStringInvalidCantCreate",
        code: errcode::ErrForeignDataStringInvalidCantCreate,
        message: ErrForeignDataStringInvalidCantCreate,
    },
    CatalogEntry {
        name: "ErrForeignDataStringInvalid",
        code: errcode::ErrForeignDataStringInvalid,
        message: ErrForeignDataStringInvalid,
    },
    CatalogEntry {
        name: "ErrCantCreateFederatedTable",
        code: errcode::ErrCantCreateFederatedTable,
        message: ErrCantCreateFederatedTable,
    },
    CatalogEntry {
        name: "ErrTrgInWrongSchema",
        code: errcode::ErrTrgInWrongSchema,
        message: ErrTrgInWrongSchema,
    },
    CatalogEntry {
        name: "ErrStackOverrunNeedMore",
        code: errcode::ErrStackOverrunNeedMore,
        message: ErrStackOverrunNeedMore,
    },
    CatalogEntry {
        name: "ErrTooLongBody",
        code: errcode::ErrTooLongBody,
        message: ErrTooLongBody,
    },
    CatalogEntry {
        name: "ErrWarnCantDropDefaultKeycache",
        code: errcode::ErrWarnCantDropDefaultKeycache,
        message: ErrWarnCantDropDefaultKeycache,
    },
    CatalogEntry {
        name: "ErrTooBigDisplaywidth",
        code: errcode::ErrTooBigDisplaywidth,
        message: ErrTooBigDisplaywidth,
    },
    CatalogEntry {
        name: "ErrXaerDupid",
        code: errcode::ErrXaerDupid,
        message: ErrXaerDupid,
    },
    CatalogEntry {
        name: "ErrDatetimeFunctionOverflow",
        code: errcode::ErrDatetimeFunctionOverflow,
        message: ErrDatetimeFunctionOverflow,
    },
    CatalogEntry {
        name: "ErrCantUpdateUsedTableInSfOrTrg",
        code: errcode::ErrCantUpdateUsedTableInSfOrTrg,
        message: ErrCantUpdateUsedTableInSfOrTrg,
    },
    CatalogEntry {
        name: "ErrViewPreventUpdate",
        code: errcode::ErrViewPreventUpdate,
        message: ErrViewPreventUpdate,
    },
    CatalogEntry {
        name: "ErrPsNoRecursion",
        code: errcode::ErrPsNoRecursion,
        message: ErrPsNoRecursion,
    },
    CatalogEntry {
        name: "ErrSpCantSetAutocommit",
        code: errcode::ErrSpCantSetAutocommit,
        message: ErrSpCantSetAutocommit,
    },
    CatalogEntry {
        name: "ErrMalformedDefiner",
        code: errcode::ErrMalformedDefiner,
        message: ErrMalformedDefiner,
    },
    CatalogEntry {
        name: "ErrViewFrmNoUser",
        code: errcode::ErrViewFrmNoUser,
        message: ErrViewFrmNoUser,
    },
    CatalogEntry {
        name: "ErrViewOtherUser",
        code: errcode::ErrViewOtherUser,
        message: ErrViewOtherUser,
    },
    CatalogEntry {
        name: "ErrNoSuchUser",
        code: errcode::ErrNoSuchUser,
        message: ErrNoSuchUser,
    },
    CatalogEntry {
        name: "ErrForbidSchemaChange",
        code: errcode::ErrForbidSchemaChange,
        message: ErrForbidSchemaChange,
    },
    CatalogEntry {
        name: "ErrRowIsReferenced2",
        code: errcode::ErrRowIsReferenced2,
        message: ErrRowIsReferenced2,
    },
    CatalogEntry {
        name: "ErrNoReferencedRow2",
        code: errcode::ErrNoReferencedRow2,
        message: ErrNoReferencedRow2,
    },
    CatalogEntry {
        name: "ErrSpBadVarShadow",
        code: errcode::ErrSpBadVarShadow,
        message: ErrSpBadVarShadow,
    },
    CatalogEntry {
        name: "ErrTrgNoDefiner",
        code: errcode::ErrTrgNoDefiner,
        message: ErrTrgNoDefiner,
    },
    CatalogEntry {
        name: "ErrOldFileFormat",
        code: errcode::ErrOldFileFormat,
        message: ErrOldFileFormat,
    },
    CatalogEntry {
        name: "ErrSpRecursionLimit",
        code: errcode::ErrSpRecursionLimit,
        message: ErrSpRecursionLimit,
    },
    CatalogEntry {
        name: "ErrSpProcTableCorrupt",
        code: errcode::ErrSpProcTableCorrupt,
        message: ErrSpProcTableCorrupt,
    },
    CatalogEntry {
        name: "ErrSpWrongName",
        code: errcode::ErrSpWrongName,
        message: ErrSpWrongName,
    },
    CatalogEntry {
        name: "ErrTableNeedsUpgrade",
        code: errcode::ErrTableNeedsUpgrade,
        message: ErrTableNeedsUpgrade,
    },
    CatalogEntry {
        name: "ErrSpNoAggregate",
        code: errcode::ErrSpNoAggregate,
        message: ErrSpNoAggregate,
    },
    CatalogEntry {
        name: "ErrMaxPreparedStmtCountReached",
        code: errcode::ErrMaxPreparedStmtCountReached,
        message: ErrMaxPreparedStmtCountReached,
    },
    CatalogEntry {
        name: "ErrViewRecursive",
        code: errcode::ErrViewRecursive,
        message: ErrViewRecursive,
    },
    CatalogEntry {
        name: "ErrNonGroupingFieldUsed",
        code: errcode::ErrNonGroupingFieldUsed,
        message: ErrNonGroupingFieldUsed,
    },
    CatalogEntry {
        name: "ErrTableCantHandleSpkeys",
        code: errcode::ErrTableCantHandleSpkeys,
        message: ErrTableCantHandleSpkeys,
    },
    CatalogEntry {
        name: "ErrNoTriggersOnSystemSchema",
        code: errcode::ErrNoTriggersOnSystemSchema,
        message: ErrNoTriggersOnSystemSchema,
    },
    CatalogEntry {
        name: "ErrRemovedSpaces",
        code: errcode::ErrRemovedSpaces,
        message: ErrRemovedSpaces,
    },
    CatalogEntry {
        name: "ErrAutoincReadFailed",
        code: errcode::ErrAutoincReadFailed,
        message: ErrAutoincReadFailed,
    },
    CatalogEntry {
        name: "ErrUsername",
        code: errcode::ErrUsername,
        message: ErrUsername,
    },
    CatalogEntry {
        name: "ErrHostname",
        code: errcode::ErrHostname,
        message: ErrHostname,
    },
    CatalogEntry {
        name: "ErrWrongStringLength",
        code: errcode::ErrWrongStringLength,
        message: ErrWrongStringLength,
    },
    CatalogEntry {
        name: "ErrNonInsertableTable",
        code: errcode::ErrNonInsertableTable,
        message: ErrNonInsertableTable,
    },
    CatalogEntry {
        name: "ErrAdminWrongMrgTable",
        code: errcode::ErrAdminWrongMrgTable,
        message: ErrAdminWrongMrgTable,
    },
    CatalogEntry {
        name: "ErrTooHighLevelOfNestingForSelect",
        code: errcode::ErrTooHighLevelOfNestingForSelect,
        message: ErrTooHighLevelOfNestingForSelect,
    },
    CatalogEntry {
        name: "ErrNameBecomesEmpty",
        code: errcode::ErrNameBecomesEmpty,
        message: ErrNameBecomesEmpty,
    },
    CatalogEntry {
        name: "ErrAmbiguousFieldTerm",
        code: errcode::ErrAmbiguousFieldTerm,
        message: ErrAmbiguousFieldTerm,
    },
    CatalogEntry {
        name: "ErrForeignServerExists",
        code: errcode::ErrForeignServerExists,
        message: ErrForeignServerExists,
    },
    CatalogEntry {
        name: "ErrForeignServerDoesntExist",
        code: errcode::ErrForeignServerDoesntExist,
        message: ErrForeignServerDoesntExist,
    },
    CatalogEntry {
        name: "ErrIllegalHaCreateOption",
        code: errcode::ErrIllegalHaCreateOption,
        message: ErrIllegalHaCreateOption,
    },
    CatalogEntry {
        name: "ErrPartitionRequiresValues",
        code: errcode::ErrPartitionRequiresValues,
        message: ErrPartitionRequiresValues,
    },
    CatalogEntry {
        name: "ErrPartitionWrongValues",
        code: errcode::ErrPartitionWrongValues,
        message: ErrPartitionWrongValues,
    },
    CatalogEntry {
        name: "ErrPartitionMaxvalue",
        code: errcode::ErrPartitionMaxvalue,
        message: ErrPartitionMaxvalue,
    },
    CatalogEntry {
        name: "ErrPartitionSubpartition",
        code: errcode::ErrPartitionSubpartition,
        message: ErrPartitionSubpartition,
    },
    CatalogEntry {
        name: "ErrPartitionSubpartMix",
        code: errcode::ErrPartitionSubpartMix,
        message: ErrPartitionSubpartMix,
    },
    CatalogEntry {
        name: "ErrPartitionWrongNoPart",
        code: errcode::ErrPartitionWrongNoPart,
        message: ErrPartitionWrongNoPart,
    },
    CatalogEntry {
        name: "ErrPartitionWrongNoSubpart",
        code: errcode::ErrPartitionWrongNoSubpart,
        message: ErrPartitionWrongNoSubpart,
    },
    CatalogEntry {
        name: "ErrWrongExprInPartitionFunc",
        code: errcode::ErrWrongExprInPartitionFunc,
        message: ErrWrongExprInPartitionFunc,
    },
    CatalogEntry {
        name: "ErrNoConstExprInRangeOrList",
        code: errcode::ErrNoConstExprInRangeOrList,
        message: ErrNoConstExprInRangeOrList,
    },
    CatalogEntry {
        name: "ErrFieldNotFoundPart",
        code: errcode::ErrFieldNotFoundPart,
        message: ErrFieldNotFoundPart,
    },
    CatalogEntry {
        name: "ErrListOfFieldsOnlyInHash",
        code: errcode::ErrListOfFieldsOnlyInHash,
        message: ErrListOfFieldsOnlyInHash,
    },
    CatalogEntry {
        name: "ErrInconsistentPartitionInfo",
        code: errcode::ErrInconsistentPartitionInfo,
        message: ErrInconsistentPartitionInfo,
    },
    CatalogEntry {
        name: "ErrPartitionFuncNotAllowed",
        code: errcode::ErrPartitionFuncNotAllowed,
        message: ErrPartitionFuncNotAllowed,
    },
    CatalogEntry {
        name: "ErrPartitionsMustBeDefined",
        code: errcode::ErrPartitionsMustBeDefined,
        message: ErrPartitionsMustBeDefined,
    },
    CatalogEntry {
        name: "ErrRangeNotIncreasing",
        code: errcode::ErrRangeNotIncreasing,
        message: ErrRangeNotIncreasing,
    },
    CatalogEntry {
        name: "ErrInconsistentTypeOfFunctions",
        code: errcode::ErrInconsistentTypeOfFunctions,
        message: ErrInconsistentTypeOfFunctions,
    },
    CatalogEntry {
        name: "ErrMultipleDefConstInListPart",
        code: errcode::ErrMultipleDefConstInListPart,
        message: ErrMultipleDefConstInListPart,
    },
    CatalogEntry {
        name: "ErrPartitionEntry",
        code: errcode::ErrPartitionEntry,
        message: ErrPartitionEntry,
    },
    CatalogEntry {
        name: "ErrMixHandler",
        code: errcode::ErrMixHandler,
        message: ErrMixHandler,
    },
    CatalogEntry {
        name: "ErrPartitionNotDefined",
        code: errcode::ErrPartitionNotDefined,
        message: ErrPartitionNotDefined,
    },
    CatalogEntry {
        name: "ErrTooManyPartitions",
        code: errcode::ErrTooManyPartitions,
        message: ErrTooManyPartitions,
    },
    CatalogEntry {
        name: "ErrSubpartition",
        code: errcode::ErrSubpartition,
        message: ErrSubpartition,
    },
    CatalogEntry {
        name: "ErrCantCreateHandlerFile",
        code: errcode::ErrCantCreateHandlerFile,
        message: ErrCantCreateHandlerFile,
    },
    CatalogEntry {
        name: "ErrBlobFieldInPartFunc",
        code: errcode::ErrBlobFieldInPartFunc,
        message: ErrBlobFieldInPartFunc,
    },
    CatalogEntry {
        name: "ErrUniqueKeyNeedAllFieldsInPf",
        code: errcode::ErrUniqueKeyNeedAllFieldsInPf,
        message: ErrUniqueKeyNeedAllFieldsInPf,
    },
    CatalogEntry {
        name: "ErrNoParts",
        code: errcode::ErrNoParts,
        message: ErrNoParts,
    },
    CatalogEntry {
        name: "ErrPartitionMgmtOnNonpartitioned",
        code: errcode::ErrPartitionMgmtOnNonpartitioned,
        message: ErrPartitionMgmtOnNonpartitioned,
    },
    CatalogEntry {
        name: "ErrForeignKeyOnPartitioned",
        code: errcode::ErrForeignKeyOnPartitioned,
        message: ErrForeignKeyOnPartitioned,
    },
    CatalogEntry {
        name: "ErrDropPartitionNonExistent",
        code: errcode::ErrDropPartitionNonExistent,
        message: ErrDropPartitionNonExistent,
    },
    CatalogEntry {
        name: "ErrDropLastPartition",
        code: errcode::ErrDropLastPartition,
        message: ErrDropLastPartition,
    },
    CatalogEntry {
        name: "ErrCoalesceOnlyOnHashPartition",
        code: errcode::ErrCoalesceOnlyOnHashPartition,
        message: ErrCoalesceOnlyOnHashPartition,
    },
    CatalogEntry {
        name: "ErrReorgHashOnlyOnSameNo",
        code: errcode::ErrReorgHashOnlyOnSameNo,
        message: ErrReorgHashOnlyOnSameNo,
    },
    CatalogEntry {
        name: "ErrReorgNoParam",
        code: errcode::ErrReorgNoParam,
        message: ErrReorgNoParam,
    },
    CatalogEntry {
        name: "ErrOnlyOnRangeListPartition",
        code: errcode::ErrOnlyOnRangeListPartition,
        message: ErrOnlyOnRangeListPartition,
    },
    CatalogEntry {
        name: "ErrAddPartitionSubpart",
        code: errcode::ErrAddPartitionSubpart,
        message: ErrAddPartitionSubpart,
    },
    CatalogEntry {
        name: "ErrAddPartitionNoNewPartition",
        code: errcode::ErrAddPartitionNoNewPartition,
        message: ErrAddPartitionNoNewPartition,
    },
    CatalogEntry {
        name: "ErrCoalescePartitionNoPartition",
        code: errcode::ErrCoalescePartitionNoPartition,
        message: ErrCoalescePartitionNoPartition,
    },
    CatalogEntry {
        name: "ErrReorgPartitionNotExist",
        code: errcode::ErrReorgPartitionNotExist,
        message: ErrReorgPartitionNotExist,
    },
    CatalogEntry {
        name: "ErrSameNamePartition",
        code: errcode::ErrSameNamePartition,
        message: ErrSameNamePartition,
    },
    CatalogEntry {
        name: "ErrNoBinlog",
        code: errcode::ErrNoBinlog,
        message: ErrNoBinlog,
    },
    CatalogEntry {
        name: "ErrConsecutiveReorgPartitions",
        code: errcode::ErrConsecutiveReorgPartitions,
        message: ErrConsecutiveReorgPartitions,
    },
    CatalogEntry {
        name: "ErrReorgOutsideRange",
        code: errcode::ErrReorgOutsideRange,
        message: ErrReorgOutsideRange,
    },
    CatalogEntry {
        name: "ErrPartitionFunctionFailure",
        code: errcode::ErrPartitionFunctionFailure,
        message: ErrPartitionFunctionFailure,
    },
    CatalogEntry {
        name: "ErrPartState",
        code: errcode::ErrPartState,
        message: ErrPartState,
    },
    CatalogEntry {
        name: "ErrLimitedPartRange",
        code: errcode::ErrLimitedPartRange,
        message: ErrLimitedPartRange,
    },
    CatalogEntry {
        name: "ErrPluginIsNotLoaded",
        code: errcode::ErrPluginIsNotLoaded,
        message: ErrPluginIsNotLoaded,
    },
    CatalogEntry {
        name: "ErrWrongValue",
        code: errcode::ErrWrongValue,
        message: ErrWrongValue,
    },
    CatalogEntry {
        name: "ErrNoPartitionForGivenValue",
        code: errcode::ErrNoPartitionForGivenValue,
        message: ErrNoPartitionForGivenValue,
    },
    CatalogEntry {
        name: "ErrFilegroupOptionOnlyOnce",
        code: errcode::ErrFilegroupOptionOnlyOnce,
        message: ErrFilegroupOptionOnlyOnce,
    },
    CatalogEntry {
        name: "ErrCreateFilegroupFailed",
        code: errcode::ErrCreateFilegroupFailed,
        message: ErrCreateFilegroupFailed,
    },
    CatalogEntry {
        name: "ErrDropFilegroupFailed",
        code: errcode::ErrDropFilegroupFailed,
        message: ErrDropFilegroupFailed,
    },
    CatalogEntry {
        name: "ErrTablespaceAutoExtend",
        code: errcode::ErrTablespaceAutoExtend,
        message: ErrTablespaceAutoExtend,
    },
    CatalogEntry {
        name: "ErrWrongSizeNumber",
        code: errcode::ErrWrongSizeNumber,
        message: ErrWrongSizeNumber,
    },
    CatalogEntry {
        name: "ErrSizeOverflow",
        code: errcode::ErrSizeOverflow,
        message: ErrSizeOverflow,
    },
    CatalogEntry {
        name: "ErrAlterFilegroupFailed",
        code: errcode::ErrAlterFilegroupFailed,
        message: ErrAlterFilegroupFailed,
    },
    CatalogEntry {
        name: "ErrBinlogRowLoggingFailed",
        code: errcode::ErrBinlogRowLoggingFailed,
        message: ErrBinlogRowLoggingFailed,
    },
    CatalogEntry {
        name: "ErrEventAlreadyExists",
        code: errcode::ErrEventAlreadyExists,
        message: ErrEventAlreadyExists,
    },
    CatalogEntry {
        name: "ErrEventStoreFailed",
        code: errcode::ErrEventStoreFailed,
        message: ErrEventStoreFailed,
    },
    CatalogEntry {
        name: "ErrEventDoesNotExist",
        code: errcode::ErrEventDoesNotExist,
        message: ErrEventDoesNotExist,
    },
    CatalogEntry {
        name: "ErrEventCantAlter",
        code: errcode::ErrEventCantAlter,
        message: ErrEventCantAlter,
    },
    CatalogEntry {
        name: "ErrEventDropFailed",
        code: errcode::ErrEventDropFailed,
        message: ErrEventDropFailed,
    },
    CatalogEntry {
        name: "ErrEventIntervalNotPositiveOrTooBig",
        code: errcode::ErrEventIntervalNotPositiveOrTooBig,
        message: ErrEventIntervalNotPositiveOrTooBig,
    },
    CatalogEntry {
        name: "ErrEventEndsBeforeStarts",
        code: errcode::ErrEventEndsBeforeStarts,
        message: ErrEventEndsBeforeStarts,
    },
    CatalogEntry {
        name: "ErrEventExecTimeInThePast",
        code: errcode::ErrEventExecTimeInThePast,
        message: ErrEventExecTimeInThePast,
    },
    CatalogEntry {
        name: "ErrEventOpenTableFailed",
        code: errcode::ErrEventOpenTableFailed,
        message: ErrEventOpenTableFailed,
    },
    CatalogEntry {
        name: "ErrEventNeitherMExprNorMAt",
        code: errcode::ErrEventNeitherMExprNorMAt,
        message: ErrEventNeitherMExprNorMAt,
    },
    CatalogEntry {
        name: "ErrObsoleteColCountDoesntMatchCorrupted",
        code: errcode::ErrObsoleteColCountDoesntMatchCorrupted,
        message: ErrObsoleteColCountDoesntMatchCorrupted,
    },
    CatalogEntry {
        name: "ErrObsoleteCannotLoadFromTable",
        code: errcode::ErrObsoleteCannotLoadFromTable,
        message: ErrObsoleteCannotLoadFromTable,
    },
    CatalogEntry {
        name: "ErrEventCannotDelete",
        code: errcode::ErrEventCannotDelete,
        message: ErrEventCannotDelete,
    },
    CatalogEntry {
        name: "ErrEventCompile",
        code: errcode::ErrEventCompile,
        message: ErrEventCompile,
    },
    CatalogEntry {
        name: "ErrEventSameName",
        code: errcode::ErrEventSameName,
        message: ErrEventSameName,
    },
    CatalogEntry {
        name: "ErrEventDataTooLong",
        code: errcode::ErrEventDataTooLong,
        message: ErrEventDataTooLong,
    },
    CatalogEntry {
        name: "ErrDropIndexNeededInForeignKey",
        code: errcode::ErrDropIndexNeededInForeignKey,
        message: ErrDropIndexNeededInForeignKey,
    },
    CatalogEntry {
        name: "ErrWarnDeprecatedSyntaxWithVer",
        code: errcode::ErrWarnDeprecatedSyntaxWithVer,
        message: ErrWarnDeprecatedSyntaxWithVer,
    },
    CatalogEntry {
        name: "ErrCantWriteLockLogTable",
        code: errcode::ErrCantWriteLockLogTable,
        message: ErrCantWriteLockLogTable,
    },
    CatalogEntry {
        name: "ErrCantLockLogTable",
        code: errcode::ErrCantLockLogTable,
        message: ErrCantLockLogTable,
    },
    CatalogEntry {
        name: "ErrForeignDuplicateKeyOldUnused",
        code: errcode::ErrForeignDuplicateKeyOldUnused,
        message: ErrForeignDuplicateKeyOldUnused,
    },
    CatalogEntry {
        name: "ErrColCountDoesntMatchPleaseUpdate",
        code: errcode::ErrColCountDoesntMatchPleaseUpdate,
        message: ErrColCountDoesntMatchPleaseUpdate,
    },
    CatalogEntry {
        name: "ErrTempTablePreventsSwitchOutOfRbr",
        code: errcode::ErrTempTablePreventsSwitchOutOfRbr,
        message: ErrTempTablePreventsSwitchOutOfRbr,
    },
    CatalogEntry {
        name: "ErrStoredFunctionPreventsSwitchBinlogFormat",
        code: errcode::ErrStoredFunctionPreventsSwitchBinlogFormat,
        message: ErrStoredFunctionPreventsSwitchBinlogFormat,
    },
    CatalogEntry {
        name: "ErrNdbCantSwitchBinlogFormat",
        code: errcode::ErrNdbCantSwitchBinlogFormat,
        message: ErrNdbCantSwitchBinlogFormat,
    },
    CatalogEntry {
        name: "ErrPartitionNoTemporary",
        code: errcode::ErrPartitionNoTemporary,
        message: ErrPartitionNoTemporary,
    },
    CatalogEntry {
        name: "ErrPartitionConstDomain",
        code: errcode::ErrPartitionConstDomain,
        message: ErrPartitionConstDomain,
    },
    CatalogEntry {
        name: "ErrPartitionFunctionIsNotAllowed",
        code: errcode::ErrPartitionFunctionIsNotAllowed,
        message: ErrPartitionFunctionIsNotAllowed,
    },
    CatalogEntry {
        name: "ErrDdlLog",
        code: errcode::ErrDdlLog,
        message: ErrDdlLog,
    },
    CatalogEntry {
        name: "ErrNullInValuesLessThan",
        code: errcode::ErrNullInValuesLessThan,
        message: ErrNullInValuesLessThan,
    },
    CatalogEntry {
        name: "ErrWrongPartitionName",
        code: errcode::ErrWrongPartitionName,
        message: ErrWrongPartitionName,
    },
    CatalogEntry {
        name: "ErrCantChangeTxCharacteristics",
        code: errcode::ErrCantChangeTxCharacteristics,
        message: ErrCantChangeTxCharacteristics,
    },
    CatalogEntry {
        name: "ErrDupEntryAutoincrementCase",
        code: errcode::ErrDupEntryAutoincrementCase,
        message: ErrDupEntryAutoincrementCase,
    },
    CatalogEntry {
        name: "ErrEventModifyQueue",
        code: errcode::ErrEventModifyQueue,
        message: ErrEventModifyQueue,
    },
    CatalogEntry {
        name: "ErrEventSetVar",
        code: errcode::ErrEventSetVar,
        message: ErrEventSetVar,
    },
    CatalogEntry {
        name: "ErrPartitionMerge",
        code: errcode::ErrPartitionMerge,
        message: ErrPartitionMerge,
    },
    CatalogEntry {
        name: "ErrCantActivateLog",
        code: errcode::ErrCantActivateLog,
        message: ErrCantActivateLog,
    },
    CatalogEntry {
        name: "ErrRbrNotAvailable",
        code: errcode::ErrRbrNotAvailable,
        message: ErrRbrNotAvailable,
    },
    CatalogEntry {
        name: "ErrBase64Decode",
        code: errcode::ErrBase64Decode,
        message: ErrBase64Decode,
    },
    CatalogEntry {
        name: "ErrEventRecursionForbidden",
        code: errcode::ErrEventRecursionForbidden,
        message: ErrEventRecursionForbidden,
    },
    CatalogEntry {
        name: "ErrEventsDB",
        code: errcode::ErrEventsDB,
        message: ErrEventsDB,
    },
    CatalogEntry {
        name: "ErrOnlyIntegersAllowed",
        code: errcode::ErrOnlyIntegersAllowed,
        message: ErrOnlyIntegersAllowed,
    },
    CatalogEntry {
        name: "ErrUnsuportedLogEngine",
        code: errcode::ErrUnsuportedLogEngine,
        message: ErrUnsuportedLogEngine,
    },
    CatalogEntry {
        name: "ErrBadLogStatement",
        code: errcode::ErrBadLogStatement,
        message: ErrBadLogStatement,
    },
    CatalogEntry {
        name: "ErrCantRenameLogTable",
        code: errcode::ErrCantRenameLogTable,
        message: ErrCantRenameLogTable,
    },
    CatalogEntry {
        name: "ErrWrongParamcountToNativeFct",
        code: errcode::ErrWrongParamcountToNativeFct,
        message: ErrWrongParamcountToNativeFct,
    },
    CatalogEntry {
        name: "ErrWrongParametersToNativeFct",
        code: errcode::ErrWrongParametersToNativeFct,
        message: ErrWrongParametersToNativeFct,
    },
    CatalogEntry {
        name: "ErrWrongParametersToStoredFct",
        code: errcode::ErrWrongParametersToStoredFct,
        message: ErrWrongParametersToStoredFct,
    },
    CatalogEntry {
        name: "ErrNativeFctNameCollision",
        code: errcode::ErrNativeFctNameCollision,
        message: ErrNativeFctNameCollision,
    },
    CatalogEntry {
        name: "ErrDupEntryWithKeyName",
        code: errcode::ErrDupEntryWithKeyName,
        message: ErrDupEntryWithKeyName,
    },
    CatalogEntry {
        name: "ErrBinlogPurgeEmFile",
        code: errcode::ErrBinlogPurgeEmFile,
        message: ErrBinlogPurgeEmFile,
    },
    CatalogEntry {
        name: "ErrEventCannotCreateInThePast",
        code: errcode::ErrEventCannotCreateInThePast,
        message: ErrEventCannotCreateInThePast,
    },
    CatalogEntry {
        name: "ErrEventCannotAlterInThePast",
        code: errcode::ErrEventCannotAlterInThePast,
        message: ErrEventCannotAlterInThePast,
    },
    CatalogEntry {
        name: "ErrNoPartitionForGivenValueSilent",
        code: errcode::ErrNoPartitionForGivenValueSilent,
        message: ErrNoPartitionForGivenValueSilent,
    },
    CatalogEntry {
        name: "ErrBinlogUnsafeStatement",
        code: errcode::ErrBinlogUnsafeStatement,
        message: ErrBinlogUnsafeStatement,
    },
    CatalogEntry {
        name: "ErrBinlogLoggingImpossible",
        code: errcode::ErrBinlogLoggingImpossible,
        message: ErrBinlogLoggingImpossible,
    },
    CatalogEntry {
        name: "ErrViewNoCreationCtx",
        code: errcode::ErrViewNoCreationCtx,
        message: ErrViewNoCreationCtx,
    },
    CatalogEntry {
        name: "ErrViewInvalidCreationCtx",
        code: errcode::ErrViewInvalidCreationCtx,
        message: ErrViewInvalidCreationCtx,
    },
    CatalogEntry {
        name: "ErrSrInvalidCreationCtx",
        code: errcode::ErrSrInvalidCreationCtx,
        message: ErrSrInvalidCreationCtx,
    },
    CatalogEntry {
        name: "ErrTrgCorruptedFile",
        code: errcode::ErrTrgCorruptedFile,
        message: ErrTrgCorruptedFile,
    },
    CatalogEntry {
        name: "ErrTrgNoCreationCtx",
        code: errcode::ErrTrgNoCreationCtx,
        message: ErrTrgNoCreationCtx,
    },
    CatalogEntry {
        name: "ErrTrgInvalidCreationCtx",
        code: errcode::ErrTrgInvalidCreationCtx,
        message: ErrTrgInvalidCreationCtx,
    },
    CatalogEntry {
        name: "ErrEventInvalidCreationCtx",
        code: errcode::ErrEventInvalidCreationCtx,
        message: ErrEventInvalidCreationCtx,
    },
    CatalogEntry {
        name: "ErrTrgCantOpenTable",
        code: errcode::ErrTrgCantOpenTable,
        message: ErrTrgCantOpenTable,
    },
    CatalogEntry {
        name: "ErrCantCreateSroutine",
        code: errcode::ErrCantCreateSroutine,
        message: ErrCantCreateSroutine,
    },
    CatalogEntry {
        name: "ErrNoFormatDescriptionEventBeforeBinlogStatement",
        code: errcode::ErrNoFormatDescriptionEventBeforeBinlogStatement,
        message: ErrNoFormatDescriptionEventBeforeBinlogStatement,
    },
    CatalogEntry {
        name: "ErrLoadDataInvalidColumn",
        code: errcode::ErrLoadDataInvalidColumn,
        message: ErrLoadDataInvalidColumn,
    },
    CatalogEntry {
        name: "ErrLogPurgeNoFile",
        code: errcode::ErrLogPurgeNoFile,
        message: ErrLogPurgeNoFile,
    },
    CatalogEntry {
        name: "ErrXaRbtimeout",
        code: errcode::ErrXaRbtimeout,
        message: ErrXaRbtimeout,
    },
    CatalogEntry {
        name: "ErrXaRbdeadlock",
        code: errcode::ErrXaRbdeadlock,
        message: ErrXaRbdeadlock,
    },
    CatalogEntry {
        name: "ErrNeedReprepare",
        code: errcode::ErrNeedReprepare,
        message: ErrNeedReprepare,
    },
    CatalogEntry {
        name: "ErrDelayedNotSupported",
        code: errcode::ErrDelayedNotSupported,
        message: ErrDelayedNotSupported,
    },
    CatalogEntry {
        name: "WarnOptionIgnored",
        code: errcode::WarnOptionIgnored,
        message: WarnOptionIgnored,
    },
    CatalogEntry {
        name: "WarnPluginDeleteBuiltin",
        code: errcode::WarnPluginDeleteBuiltin,
        message: WarnPluginDeleteBuiltin,
    },
    CatalogEntry {
        name: "WarnPluginBusy",
        code: errcode::WarnPluginBusy,
        message: WarnPluginBusy,
    },
    CatalogEntry {
        name: "ErrVariableIsReadonly",
        code: errcode::ErrVariableIsReadonly,
        message: ErrVariableIsReadonly,
    },
    CatalogEntry {
        name: "ErrWarnEngineTransactionRollback",
        code: errcode::ErrWarnEngineTransactionRollback,
        message: ErrWarnEngineTransactionRollback,
    },
    CatalogEntry {
        name: "ErrNdbReplicationSchema",
        code: errcode::ErrNdbReplicationSchema,
        message: ErrNdbReplicationSchema,
    },
    CatalogEntry {
        name: "ErrConflictFnParse",
        code: errcode::ErrConflictFnParse,
        message: ErrConflictFnParse,
    },
    CatalogEntry {
        name: "ErrExceptionsWrite",
        code: errcode::ErrExceptionsWrite,
        message: ErrExceptionsWrite,
    },
    CatalogEntry {
        name: "ErrTooLongTableComment",
        code: errcode::ErrTooLongTableComment,
        message: ErrTooLongTableComment,
    },
    CatalogEntry {
        name: "ErrTooLongFieldComment",
        code: errcode::ErrTooLongFieldComment,
        message: ErrTooLongFieldComment,
    },
    CatalogEntry {
        name: "ErrFuncInexistentNameCollision",
        code: errcode::ErrFuncInexistentNameCollision,
        message: ErrFuncInexistentNameCollision,
    },
    CatalogEntry {
        name: "ErrDatabaseName",
        code: errcode::ErrDatabaseName,
        message: ErrDatabaseName,
    },
    CatalogEntry {
        name: "ErrTableName",
        code: errcode::ErrTableName,
        message: ErrTableName,
    },
    CatalogEntry {
        name: "ErrPartitionName",
        code: errcode::ErrPartitionName,
        message: ErrPartitionName,
    },
    CatalogEntry {
        name: "ErrSubpartitionName",
        code: errcode::ErrSubpartitionName,
        message: ErrSubpartitionName,
    },
    CatalogEntry {
        name: "ErrTemporaryName",
        code: errcode::ErrTemporaryName,
        message: ErrTemporaryName,
    },
    CatalogEntry {
        name: "ErrRenamedName",
        code: errcode::ErrRenamedName,
        message: ErrRenamedName,
    },
    CatalogEntry {
        name: "ErrTooManyConcurrentTrxs",
        code: errcode::ErrTooManyConcurrentTrxs,
        message: ErrTooManyConcurrentTrxs,
    },
    CatalogEntry {
        name: "WarnNonASCIISeparatorNotImplemented",
        code: errcode::WarnNonASCIISeparatorNotImplemented,
        message: WarnNonASCIISeparatorNotImplemented,
    },
    CatalogEntry {
        name: "ErrDebugSyncTimeout",
        code: errcode::ErrDebugSyncTimeout,
        message: ErrDebugSyncTimeout,
    },
    CatalogEntry {
        name: "ErrDebugSyncHitLimit",
        code: errcode::ErrDebugSyncHitLimit,
        message: ErrDebugSyncHitLimit,
    },
    CatalogEntry {
        name: "ErrDupSignalSet",
        code: errcode::ErrDupSignalSet,
        message: ErrDupSignalSet,
    },
    CatalogEntry {
        name: "ErrSignalWarn",
        code: errcode::ErrSignalWarn,
        message: ErrSignalWarn,
    },
    CatalogEntry {
        name: "ErrSignalNotFound",
        code: errcode::ErrSignalNotFound,
        message: ErrSignalNotFound,
    },
    CatalogEntry {
        name: "ErrSignalException",
        code: errcode::ErrSignalException,
        message: ErrSignalException,
    },
    CatalogEntry {
        name: "ErrResignalWithoutActiveHandler",
        code: errcode::ErrResignalWithoutActiveHandler,
        message: ErrResignalWithoutActiveHandler,
    },
    CatalogEntry {
        name: "ErrSignalBadConditionType",
        code: errcode::ErrSignalBadConditionType,
        message: ErrSignalBadConditionType,
    },
    CatalogEntry {
        name: "WarnCondItemTruncated",
        code: errcode::WarnCondItemTruncated,
        message: WarnCondItemTruncated,
    },
    CatalogEntry {
        name: "ErrCondItemTooLong",
        code: errcode::ErrCondItemTooLong,
        message: ErrCondItemTooLong,
    },
    CatalogEntry {
        name: "ErrUnknownLocale",
        code: errcode::ErrUnknownLocale,
        message: ErrUnknownLocale,
    },
    CatalogEntry {
        name: "ErrQueryCacheDisabled",
        code: errcode::ErrQueryCacheDisabled,
        message: ErrQueryCacheDisabled,
    },
    CatalogEntry {
        name: "ErrSameNamePartitionField",
        code: errcode::ErrSameNamePartitionField,
        message: ErrSameNamePartitionField,
    },
    CatalogEntry {
        name: "ErrPartitionColumnList",
        code: errcode::ErrPartitionColumnList,
        message: ErrPartitionColumnList,
    },
    CatalogEntry {
        name: "ErrWrongTypeColumnValue",
        code: errcode::ErrWrongTypeColumnValue,
        message: ErrWrongTypeColumnValue,
    },
    CatalogEntry {
        name: "ErrTooManyPartitionFuncFields",
        code: errcode::ErrTooManyPartitionFuncFields,
        message: ErrTooManyPartitionFuncFields,
    },
    CatalogEntry {
        name: "ErrMaxvalueInValuesIn",
        code: errcode::ErrMaxvalueInValuesIn,
        message: ErrMaxvalueInValuesIn,
    },
    CatalogEntry {
        name: "ErrTooManyValues",
        code: errcode::ErrTooManyValues,
        message: ErrTooManyValues,
    },
    CatalogEntry {
        name: "ErrRowSinglePartitionField",
        code: errcode::ErrRowSinglePartitionField,
        message: ErrRowSinglePartitionField,
    },
    CatalogEntry {
        name: "ErrFieldTypeNotAllowedAsPartitionField",
        code: errcode::ErrFieldTypeNotAllowedAsPartitionField,
        message: ErrFieldTypeNotAllowedAsPartitionField,
    },
    CatalogEntry {
        name: "ErrPartitionFieldsTooLong",
        code: errcode::ErrPartitionFieldsTooLong,
        message: ErrPartitionFieldsTooLong,
    },
    CatalogEntry {
        name: "ErrBinlogRowEngineAndStmtEngine",
        code: errcode::ErrBinlogRowEngineAndStmtEngine,
        message: ErrBinlogRowEngineAndStmtEngine,
    },
    CatalogEntry {
        name: "ErrBinlogRowModeAndStmtEngine",
        code: errcode::ErrBinlogRowModeAndStmtEngine,
        message: ErrBinlogRowModeAndStmtEngine,
    },
    CatalogEntry {
        name: "ErrBinlogUnsafeAndStmtEngine",
        code: errcode::ErrBinlogUnsafeAndStmtEngine,
        message: ErrBinlogUnsafeAndStmtEngine,
    },
    CatalogEntry {
        name: "ErrBinlogRowInjectionAndStmtEngine",
        code: errcode::ErrBinlogRowInjectionAndStmtEngine,
        message: ErrBinlogRowInjectionAndStmtEngine,
    },
    CatalogEntry {
        name: "ErrBinlogStmtModeAndRowEngine",
        code: errcode::ErrBinlogStmtModeAndRowEngine,
        message: ErrBinlogStmtModeAndRowEngine,
    },
    CatalogEntry {
        name: "ErrBinlogRowInjectionAndStmtMode",
        code: errcode::ErrBinlogRowInjectionAndStmtMode,
        message: ErrBinlogRowInjectionAndStmtMode,
    },
    CatalogEntry {
        name: "ErrBinlogMultipleEnginesAndSelfLoggingEngine",
        code: errcode::ErrBinlogMultipleEnginesAndSelfLoggingEngine,
        message: ErrBinlogMultipleEnginesAndSelfLoggingEngine,
    },
    CatalogEntry {
        name: "ErrBinlogUnsafeLimit",
        code: errcode::ErrBinlogUnsafeLimit,
        message: ErrBinlogUnsafeLimit,
    },
];
