// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");

//! Message catalog entries, part 1 of 3 (see `errname/mod.rs`).

use super::consts_1::*;
use super::consts_2::*;
use super::errcode;
use super::CatalogEntry;

pub(super) const CATALOG_1: &[CatalogEntry] = &[
    CatalogEntry {
        name: "ErrHashchk",
        code: errcode::ErrHashchk,
        message: ErrHashchk,
    },
    CatalogEntry {
        name: "ErrNisamchk",
        code: errcode::ErrNisamchk,
        message: ErrNisamchk,
    },
    CatalogEntry {
        name: "ErrNo",
        code: errcode::ErrNo,
        message: ErrNo,
    },
    CatalogEntry {
        name: "ErrYes",
        code: errcode::ErrYes,
        message: ErrYes,
    },
    CatalogEntry {
        name: "ErrCantCreateFile",
        code: errcode::ErrCantCreateFile,
        message: ErrCantCreateFile,
    },
    CatalogEntry {
        name: "ErrCantCreateTable",
        code: errcode::ErrCantCreateTable,
        message: ErrCantCreateTable,
    },
    CatalogEntry {
        name: "ErrCantCreateDB",
        code: errcode::ErrCantCreateDB,
        message: ErrCantCreateDB,
    },
    CatalogEntry {
        name: "ErrDBCreateExists",
        code: errcode::ErrDBCreateExists,
        message: ErrDBCreateExists,
    },
    CatalogEntry {
        name: "ErrDBDropExists",
        code: errcode::ErrDBDropExists,
        message: ErrDBDropExists,
    },
    CatalogEntry {
        name: "ErrDBDropDelete",
        code: errcode::ErrDBDropDelete,
        message: ErrDBDropDelete,
    },
    CatalogEntry {
        name: "ErrDBDropRmdir",
        code: errcode::ErrDBDropRmdir,
        message: ErrDBDropRmdir,
    },
    CatalogEntry {
        name: "ErrCantDeleteFile",
        code: errcode::ErrCantDeleteFile,
        message: ErrCantDeleteFile,
    },
    CatalogEntry {
        name: "ErrCantFindSystemRec",
        code: errcode::ErrCantFindSystemRec,
        message: ErrCantFindSystemRec,
    },
    CatalogEntry {
        name: "ErrCantGetStat",
        code: errcode::ErrCantGetStat,
        message: ErrCantGetStat,
    },
    CatalogEntry {
        name: "ErrCantGetWd",
        code: errcode::ErrCantGetWd,
        message: ErrCantGetWd,
    },
    CatalogEntry {
        name: "ErrCantLock",
        code: errcode::ErrCantLock,
        message: ErrCantLock,
    },
    CatalogEntry {
        name: "ErrCantOpenFile",
        code: errcode::ErrCantOpenFile,
        message: ErrCantOpenFile,
    },
    CatalogEntry {
        name: "ErrFileNotFound",
        code: errcode::ErrFileNotFound,
        message: ErrFileNotFound,
    },
    CatalogEntry {
        name: "ErrCantReadDir",
        code: errcode::ErrCantReadDir,
        message: ErrCantReadDir,
    },
    CatalogEntry {
        name: "ErrCantSetWd",
        code: errcode::ErrCantSetWd,
        message: ErrCantSetWd,
    },
    CatalogEntry {
        name: "ErrCheckread",
        code: errcode::ErrCheckread,
        message: ErrCheckread,
    },
    CatalogEntry {
        name: "ErrDiskFull",
        code: errcode::ErrDiskFull,
        message: ErrDiskFull,
    },
    CatalogEntry {
        name: "ErrDupKey",
        code: errcode::ErrDupKey,
        message: ErrDupKey,
    },
    CatalogEntry {
        name: "ErrErrorOnClose",
        code: errcode::ErrErrorOnClose,
        message: ErrErrorOnClose,
    },
    CatalogEntry {
        name: "ErrErrorOnRead",
        code: errcode::ErrErrorOnRead,
        message: ErrErrorOnRead,
    },
    CatalogEntry {
        name: "ErrErrorOnRename",
        code: errcode::ErrErrorOnRename,
        message: ErrErrorOnRename,
    },
    CatalogEntry {
        name: "ErrErrorOnWrite",
        code: errcode::ErrErrorOnWrite,
        message: ErrErrorOnWrite,
    },
    CatalogEntry {
        name: "ErrFileUsed",
        code: errcode::ErrFileUsed,
        message: ErrFileUsed,
    },
    CatalogEntry {
        name: "ErrFilsortAbort",
        code: errcode::ErrFilsortAbort,
        message: ErrFilsortAbort,
    },
    CatalogEntry {
        name: "ErrFormNotFound",
        code: errcode::ErrFormNotFound,
        message: ErrFormNotFound,
    },
    CatalogEntry {
        name: "ErrGetErrno",
        code: errcode::ErrGetErrno,
        message: ErrGetErrno,
    },
    CatalogEntry {
        name: "ErrIllegalHa",
        code: errcode::ErrIllegalHa,
        message: ErrIllegalHa,
    },
    CatalogEntry {
        name: "ErrKeyNotFound",
        code: errcode::ErrKeyNotFound,
        message: ErrKeyNotFound,
    },
    CatalogEntry {
        name: "ErrNotFormFile",
        code: errcode::ErrNotFormFile,
        message: ErrNotFormFile,
    },
    CatalogEntry {
        name: "ErrNotKeyFile",
        code: errcode::ErrNotKeyFile,
        message: ErrNotKeyFile,
    },
    CatalogEntry {
        name: "ErrOldKeyFile",
        code: errcode::ErrOldKeyFile,
        message: ErrOldKeyFile,
    },
    CatalogEntry {
        name: "ErrOpenAsReadonly",
        code: errcode::ErrOpenAsReadonly,
        message: ErrOpenAsReadonly,
    },
    CatalogEntry {
        name: "ErrOutofMemory",
        code: errcode::ErrOutofMemory,
        message: ErrOutofMemory,
    },
    CatalogEntry {
        name: "ErrOutOfSortMemory",
        code: errcode::ErrOutOfSortMemory,
        message: ErrOutOfSortMemory,
    },
    CatalogEntry {
        name: "ErrUnexpectedEOF",
        code: errcode::ErrUnexpectedEOF,
        message: ErrUnexpectedEOF,
    },
    CatalogEntry {
        name: "ErrConCount",
        code: errcode::ErrConCount,
        message: ErrConCount,
    },
    CatalogEntry {
        name: "ErrOutOfResources",
        code: errcode::ErrOutOfResources,
        message: ErrOutOfResources,
    },
    CatalogEntry {
        name: "ErrBadHost",
        code: errcode::ErrBadHost,
        message: ErrBadHost,
    },
    CatalogEntry {
        name: "ErrHandshake",
        code: errcode::ErrHandshake,
        message: ErrHandshake,
    },
    CatalogEntry {
        name: "ErrDBaccessDenied",
        code: errcode::ErrDBaccessDenied,
        message: ErrDBaccessDenied,
    },
    CatalogEntry {
        name: "ErrAccessDenied",
        code: errcode::ErrAccessDenied,
        message: ErrAccessDenied,
    },
    CatalogEntry {
        name: "ErrNoDB",
        code: errcode::ErrNoDB,
        message: ErrNoDB,
    },
    CatalogEntry {
        name: "ErrUnknownCom",
        code: errcode::ErrUnknownCom,
        message: ErrUnknownCom,
    },
    CatalogEntry {
        name: "ErrBadNull",
        code: errcode::ErrBadNull,
        message: ErrBadNull,
    },
    CatalogEntry {
        name: "ErrBadDB",
        code: errcode::ErrBadDB,
        message: ErrBadDB,
    },
    CatalogEntry {
        name: "ErrTableExists",
        code: errcode::ErrTableExists,
        message: ErrTableExists,
    },
    CatalogEntry {
        name: "ErrBadTable",
        code: errcode::ErrBadTable,
        message: ErrBadTable,
    },
    CatalogEntry {
        name: "ErrNonUniq",
        code: errcode::ErrNonUniq,
        message: ErrNonUniq,
    },
    CatalogEntry {
        name: "ErrServerShutdown",
        code: errcode::ErrServerShutdown,
        message: ErrServerShutdown,
    },
    CatalogEntry {
        name: "ErrBadField",
        code: errcode::ErrBadField,
        message: ErrBadField,
    },
    CatalogEntry {
        name: "ErrFieldNotInGroupBy",
        code: errcode::ErrFieldNotInGroupBy,
        message: ErrFieldNotInGroupBy,
    },
    CatalogEntry {
        name: "ErrWrongGroupField",
        code: errcode::ErrWrongGroupField,
        message: ErrWrongGroupField,
    },
    CatalogEntry {
        name: "ErrWrongSumSelect",
        code: errcode::ErrWrongSumSelect,
        message: ErrWrongSumSelect,
    },
    CatalogEntry {
        name: "ErrWrongValueCount",
        code: errcode::ErrWrongValueCount,
        message: ErrWrongValueCount,
    },
    CatalogEntry {
        name: "ErrTooLongIdent",
        code: errcode::ErrTooLongIdent,
        message: ErrTooLongIdent,
    },
    CatalogEntry {
        name: "ErrDupFieldName",
        code: errcode::ErrDupFieldName,
        message: ErrDupFieldName,
    },
    CatalogEntry {
        name: "ErrDupKeyName",
        code: errcode::ErrDupKeyName,
        message: ErrDupKeyName,
    },
    CatalogEntry {
        name: "ErrDupEntry",
        code: errcode::ErrDupEntry,
        message: ErrDupEntry,
    },
    CatalogEntry {
        name: "ErrWrongFieldSpec",
        code: errcode::ErrWrongFieldSpec,
        message: ErrWrongFieldSpec,
    },
    CatalogEntry {
        name: "ErrParse",
        code: errcode::ErrParse,
        message: ErrParse,
    },
    CatalogEntry {
        name: "ErrEmptyQuery",
        code: errcode::ErrEmptyQuery,
        message: ErrEmptyQuery,
    },
    CatalogEntry {
        name: "ErrNonuniqTable",
        code: errcode::ErrNonuniqTable,
        message: ErrNonuniqTable,
    },
    CatalogEntry {
        name: "ErrInvalidDefault",
        code: errcode::ErrInvalidDefault,
        message: ErrInvalidDefault,
    },
    CatalogEntry {
        name: "ErrMultiplePriKey",
        code: errcode::ErrMultiplePriKey,
        message: ErrMultiplePriKey,
    },
    CatalogEntry {
        name: "ErrTooManyKeys",
        code: errcode::ErrTooManyKeys,
        message: ErrTooManyKeys,
    },
    CatalogEntry {
        name: "ErrTooManyKeyParts",
        code: errcode::ErrTooManyKeyParts,
        message: ErrTooManyKeyParts,
    },
    CatalogEntry {
        name: "ErrTooLongKey",
        code: errcode::ErrTooLongKey,
        message: ErrTooLongKey,
    },
    CatalogEntry {
        name: "ErrKeyColumnDoesNotExits",
        code: errcode::ErrKeyColumnDoesNotExits,
        message: ErrKeyColumnDoesNotExits,
    },
    CatalogEntry {
        name: "ErrBlobUsedAsKey",
        code: errcode::ErrBlobUsedAsKey,
        message: ErrBlobUsedAsKey,
    },
    CatalogEntry {
        name: "ErrJSONVacuousPath",
        code: errcode::ErrJSONVacuousPath,
        message: ErrJSONVacuousPath,
    },
    CatalogEntry {
        name: "ErrJSONBadOneOrAllArg",
        code: errcode::ErrJSONBadOneOrAllArg,
        message: ErrJSONBadOneOrAllArg,
    },
    CatalogEntry {
        name: "ErrTooBigFieldlength",
        code: errcode::ErrTooBigFieldlength,
        message: ErrTooBigFieldlength,
    },
    CatalogEntry {
        name: "ErrWrongAutoKey",
        code: errcode::ErrWrongAutoKey,
        message: ErrWrongAutoKey,
    },
    CatalogEntry {
        name: "ErrReady",
        code: errcode::ErrReady,
        message: ErrReady,
    },
    CatalogEntry {
        name: "ErrNormalShutdown",
        code: errcode::ErrNormalShutdown,
        message: ErrNormalShutdown,
    },
    CatalogEntry {
        name: "ErrGotSignal",
        code: errcode::ErrGotSignal,
        message: ErrGotSignal,
    },
    CatalogEntry {
        name: "ErrShutdownComplete",
        code: errcode::ErrShutdownComplete,
        message: ErrShutdownComplete,
    },
    CatalogEntry {
        name: "ErrForcingClose",
        code: errcode::ErrForcingClose,
        message: ErrForcingClose,
    },
    CatalogEntry {
        name: "ErrIpsock",
        code: errcode::ErrIpsock,
        message: ErrIpsock,
    },
    CatalogEntry {
        name: "ErrNoSuchIndex",
        code: errcode::ErrNoSuchIndex,
        message: ErrNoSuchIndex,
    },
    CatalogEntry {
        name: "ErrWrongFieldTerminators",
        code: errcode::ErrWrongFieldTerminators,
        message: ErrWrongFieldTerminators,
    },
    CatalogEntry {
        name: "ErrBlobsAndNoTerminated",
        code: errcode::ErrBlobsAndNoTerminated,
        message: ErrBlobsAndNoTerminated,
    },
    CatalogEntry {
        name: "ErrTextFileNotReadable",
        code: errcode::ErrTextFileNotReadable,
        message: ErrTextFileNotReadable,
    },
    CatalogEntry {
        name: "ErrFileExists",
        code: errcode::ErrFileExists,
        message: ErrFileExists,
    },
    CatalogEntry {
        name: "ErrLoadInfo",
        code: errcode::ErrLoadInfo,
        message: ErrLoadInfo,
    },
    CatalogEntry {
        name: "ErrAlterInfo",
        code: errcode::ErrAlterInfo,
        message: ErrAlterInfo,
    },
    CatalogEntry {
        name: "ErrWrongSubKey",
        code: errcode::ErrWrongSubKey,
        message: ErrWrongSubKey,
    },
    CatalogEntry {
        name: "ErrCantRemoveAllFields",
        code: errcode::ErrCantRemoveAllFields,
        message: ErrCantRemoveAllFields,
    },
    CatalogEntry {
        name: "ErrCantDropFieldOrKey",
        code: errcode::ErrCantDropFieldOrKey,
        message: ErrCantDropFieldOrKey,
    },
    CatalogEntry {
        name: "ErrInsertInfo",
        code: errcode::ErrInsertInfo,
        message: ErrInsertInfo,
    },
    CatalogEntry {
        name: "ErrUpdateTableUsed",
        code: errcode::ErrUpdateTableUsed,
        message: ErrUpdateTableUsed,
    },
    CatalogEntry {
        name: "ErrNoSuchThread",
        code: errcode::ErrNoSuchThread,
        message: ErrNoSuchThread,
    },
    CatalogEntry {
        name: "ErrKillDenied",
        code: errcode::ErrKillDenied,
        message: ErrKillDenied,
    },
    CatalogEntry {
        name: "ErrNoTablesUsed",
        code: errcode::ErrNoTablesUsed,
        message: ErrNoTablesUsed,
    },
    CatalogEntry {
        name: "ErrTooBigSet",
        code: errcode::ErrTooBigSet,
        message: ErrTooBigSet,
    },
    CatalogEntry {
        name: "ErrNoUniqueLogFile",
        code: errcode::ErrNoUniqueLogFile,
        message: ErrNoUniqueLogFile,
    },
    CatalogEntry {
        name: "ErrTableNotLockedForWrite",
        code: errcode::ErrTableNotLockedForWrite,
        message: ErrTableNotLockedForWrite,
    },
    CatalogEntry {
        name: "ErrTableNotLocked",
        code: errcode::ErrTableNotLocked,
        message: ErrTableNotLocked,
    },
    CatalogEntry {
        name: "ErrBlobCantHaveDefault",
        code: errcode::ErrBlobCantHaveDefault,
        message: ErrBlobCantHaveDefault,
    },
    CatalogEntry {
        name: "ErrWrongDBName",
        code: errcode::ErrWrongDBName,
        message: ErrWrongDBName,
    },
    CatalogEntry {
        name: "ErrWrongTableName",
        code: errcode::ErrWrongTableName,
        message: ErrWrongTableName,
    },
    CatalogEntry {
        name: "ErrTooBigSelect",
        code: errcode::ErrTooBigSelect,
        message: ErrTooBigSelect,
    },
    CatalogEntry {
        name: "ErrUnknown",
        code: errcode::ErrUnknown,
        message: ErrUnknown,
    },
    CatalogEntry {
        name: "ErrUnknownProcedure",
        code: errcode::ErrUnknownProcedure,
        message: ErrUnknownProcedure,
    },
    CatalogEntry {
        name: "ErrWrongParamcountToProcedure",
        code: errcode::ErrWrongParamcountToProcedure,
        message: ErrWrongParamcountToProcedure,
    },
    CatalogEntry {
        name: "ErrWrongParametersToProcedure",
        code: errcode::ErrWrongParametersToProcedure,
        message: ErrWrongParametersToProcedure,
    },
    CatalogEntry {
        name: "ErrUnknownTable",
        code: errcode::ErrUnknownTable,
        message: ErrUnknownTable,
    },
    CatalogEntry {
        name: "ErrFieldSpecifiedTwice",
        code: errcode::ErrFieldSpecifiedTwice,
        message: ErrFieldSpecifiedTwice,
    },
    CatalogEntry {
        name: "ErrInvalidGroupFuncUse",
        code: errcode::ErrInvalidGroupFuncUse,
        message: ErrInvalidGroupFuncUse,
    },
    CatalogEntry {
        name: "ErrUnsupportedExtension",
        code: errcode::ErrUnsupportedExtension,
        message: ErrUnsupportedExtension,
    },
    CatalogEntry {
        name: "ErrTableMustHaveColumns",
        code: errcode::ErrTableMustHaveColumns,
        message: ErrTableMustHaveColumns,
    },
    CatalogEntry {
        name: "ErrRecordFileFull",
        code: errcode::ErrRecordFileFull,
        message: ErrRecordFileFull,
    },
    CatalogEntry {
        name: "ErrUnknownCharacterSet",
        code: errcode::ErrUnknownCharacterSet,
        message: ErrUnknownCharacterSet,
    },
    CatalogEntry {
        name: "ErrTooManyTables",
        code: errcode::ErrTooManyTables,
        message: ErrTooManyTables,
    },
    CatalogEntry {
        name: "ErrTooManyFields",
        code: errcode::ErrTooManyFields,
        message: ErrTooManyFields,
    },
    CatalogEntry {
        name: "ErrTooBigRowsize",
        code: errcode::ErrTooBigRowsize,
        message: ErrTooBigRowsize,
    },
    CatalogEntry {
        name: "ErrStackOverrun",
        code: errcode::ErrStackOverrun,
        message: ErrStackOverrun,
    },
    CatalogEntry {
        name: "ErrWrongOuterJoin",
        code: errcode::ErrWrongOuterJoin,
        message: ErrWrongOuterJoin,
    },
    CatalogEntry {
        name: "ErrNullColumnInIndex",
        code: errcode::ErrNullColumnInIndex,
        message: ErrNullColumnInIndex,
    },
    CatalogEntry {
        name: "ErrCantFindUdf",
        code: errcode::ErrCantFindUdf,
        message: ErrCantFindUdf,
    },
    CatalogEntry {
        name: "ErrCantInitializeUdf",
        code: errcode::ErrCantInitializeUdf,
        message: ErrCantInitializeUdf,
    },
    CatalogEntry {
        name: "ErrUdfNoPaths",
        code: errcode::ErrUdfNoPaths,
        message: ErrUdfNoPaths,
    },
    CatalogEntry {
        name: "ErrUdfExists",
        code: errcode::ErrUdfExists,
        message: ErrUdfExists,
    },
    CatalogEntry {
        name: "ErrCantOpenLibrary",
        code: errcode::ErrCantOpenLibrary,
        message: ErrCantOpenLibrary,
    },
    CatalogEntry {
        name: "ErrCantFindDlEntry",
        code: errcode::ErrCantFindDlEntry,
        message: ErrCantFindDlEntry,
    },
    CatalogEntry {
        name: "ErrFunctionNotDefined",
        code: errcode::ErrFunctionNotDefined,
        message: ErrFunctionNotDefined,
    },
    CatalogEntry {
        name: "ErrHostIsBlocked",
        code: errcode::ErrHostIsBlocked,
        message: ErrHostIsBlocked,
    },
    CatalogEntry {
        name: "ErrHostNotPrivileged",
        code: errcode::ErrHostNotPrivileged,
        message: ErrHostNotPrivileged,
    },
    CatalogEntry {
        name: "ErrPasswordAnonymousUser",
        code: errcode::ErrPasswordAnonymousUser,
        message: ErrPasswordAnonymousUser,
    },
    CatalogEntry {
        name: "ErrPasswordNotAllowed",
        code: errcode::ErrPasswordNotAllowed,
        message: ErrPasswordNotAllowed,
    },
    CatalogEntry {
        name: "ErrPasswordNoMatch",
        code: errcode::ErrPasswordNoMatch,
        message: ErrPasswordNoMatch,
    },
    CatalogEntry {
        name: "ErrUpdateInfo",
        code: errcode::ErrUpdateInfo,
        message: ErrUpdateInfo,
    },
    CatalogEntry {
        name: "ErrCantCreateThread",
        code: errcode::ErrCantCreateThread,
        message: ErrCantCreateThread,
    },
    CatalogEntry {
        name: "ErrWrongValueCountOnRow",
        code: errcode::ErrWrongValueCountOnRow,
        message: ErrWrongValueCountOnRow,
    },
    CatalogEntry {
        name: "ErrCantReopenTable",
        code: errcode::ErrCantReopenTable,
        message: ErrCantReopenTable,
    },
    CatalogEntry {
        name: "ErrInvalidUseOfNull",
        code: errcode::ErrInvalidUseOfNull,
        message: ErrInvalidUseOfNull,
    },
    CatalogEntry {
        name: "ErrRegexp",
        code: errcode::ErrRegexp,
        message: ErrRegexp,
    },
    CatalogEntry {
        name: "ErrMixOfGroupFuncAndFields",
        code: errcode::ErrMixOfGroupFuncAndFields,
        message: ErrMixOfGroupFuncAndFields,
    },
    CatalogEntry {
        name: "ErrNonexistingGrant",
        code: errcode::ErrNonexistingGrant,
        message: ErrNonexistingGrant,
    },
    CatalogEntry {
        name: "ErrTableaccessDenied",
        code: errcode::ErrTableaccessDenied,
        message: ErrTableaccessDenied,
    },
    CatalogEntry {
        name: "ErrColumnaccessDenied",
        code: errcode::ErrColumnaccessDenied,
        message: ErrColumnaccessDenied,
    },
    CatalogEntry {
        name: "ErrIllegalGrantForTable",
        code: errcode::ErrIllegalGrantForTable,
        message: ErrIllegalGrantForTable,
    },
    CatalogEntry {
        name: "ErrGrantWrongHostOrUser",
        code: errcode::ErrGrantWrongHostOrUser,
        message: ErrGrantWrongHostOrUser,
    },
    CatalogEntry {
        name: "ErrNoSuchTable",
        code: errcode::ErrNoSuchTable,
        message: ErrNoSuchTable,
    },
    CatalogEntry {
        name: "ErrNonexistingTableGrant",
        code: errcode::ErrNonexistingTableGrant,
        message: ErrNonexistingTableGrant,
    },
    CatalogEntry {
        name: "ErrNotAllowedCommand",
        code: errcode::ErrNotAllowedCommand,
        message: ErrNotAllowedCommand,
    },
    CatalogEntry {
        name: "ErrSyntax",
        code: errcode::ErrSyntax,
        message: ErrSyntax,
    },
    CatalogEntry {
        name: "ErrDelayedCantChangeLock",
        code: errcode::ErrDelayedCantChangeLock,
        message: ErrDelayedCantChangeLock,
    },
    CatalogEntry {
        name: "ErrTooManyDelayedThreads",
        code: errcode::ErrTooManyDelayedThreads,
        message: ErrTooManyDelayedThreads,
    },
    CatalogEntry {
        name: "ErrAbortingConnection",
        code: errcode::ErrAbortingConnection,
        message: ErrAbortingConnection,
    },
    CatalogEntry {
        name: "ErrNetPacketTooLarge",
        code: errcode::ErrNetPacketTooLarge,
        message: ErrNetPacketTooLarge,
    },
    CatalogEntry {
        name: "ErrNetReadErrorFromPipe",
        code: errcode::ErrNetReadErrorFromPipe,
        message: ErrNetReadErrorFromPipe,
    },
    CatalogEntry {
        name: "ErrNetFcntl",
        code: errcode::ErrNetFcntl,
        message: ErrNetFcntl,
    },
    CatalogEntry {
        name: "ErrNetPacketsOutOfOrder",
        code: errcode::ErrNetPacketsOutOfOrder,
        message: ErrNetPacketsOutOfOrder,
    },
    CatalogEntry {
        name: "ErrNetUncompress",
        code: errcode::ErrNetUncompress,
        message: ErrNetUncompress,
    },
    CatalogEntry {
        name: "ErrNetRead",
        code: errcode::ErrNetRead,
        message: ErrNetRead,
    },
    CatalogEntry {
        name: "ErrNetReadInterrupted",
        code: errcode::ErrNetReadInterrupted,
        message: ErrNetReadInterrupted,
    },
    CatalogEntry {
        name: "ErrNetErrorOnWrite",
        code: errcode::ErrNetErrorOnWrite,
        message: ErrNetErrorOnWrite,
    },
    CatalogEntry {
        name: "ErrNetWriteInterrupted",
        code: errcode::ErrNetWriteInterrupted,
        message: ErrNetWriteInterrupted,
    },
    CatalogEntry {
        name: "ErrTooLongString",
        code: errcode::ErrTooLongString,
        message: ErrTooLongString,
    },
    CatalogEntry {
        name: "ErrTableCantHandleBlob",
        code: errcode::ErrTableCantHandleBlob,
        message: ErrTableCantHandleBlob,
    },
    CatalogEntry {
        name: "ErrTableCantHandleAutoIncrement",
        code: errcode::ErrTableCantHandleAutoIncrement,
        message: ErrTableCantHandleAutoIncrement,
    },
    CatalogEntry {
        name: "ErrDelayedInsertTableLocked",
        code: errcode::ErrDelayedInsertTableLocked,
        message: ErrDelayedInsertTableLocked,
    },
    CatalogEntry {
        name: "ErrWrongColumnName",
        code: errcode::ErrWrongColumnName,
        message: ErrWrongColumnName,
    },
    CatalogEntry {
        name: "ErrWrongKeyColumn",
        code: errcode::ErrWrongKeyColumn,
        message: ErrWrongKeyColumn,
    },
    CatalogEntry {
        name: "ErrWrongMrgTable",
        code: errcode::ErrWrongMrgTable,
        message: ErrWrongMrgTable,
    },
    CatalogEntry {
        name: "ErrDupUnique",
        code: errcode::ErrDupUnique,
        message: ErrDupUnique,
    },
    CatalogEntry {
        name: "ErrBlobKeyWithoutLength",
        code: errcode::ErrBlobKeyWithoutLength,
        message: ErrBlobKeyWithoutLength,
    },
    CatalogEntry {
        name: "ErrPrimaryCantHaveNull",
        code: errcode::ErrPrimaryCantHaveNull,
        message: ErrPrimaryCantHaveNull,
    },
    CatalogEntry {
        name: "ErrTooManyRows",
        code: errcode::ErrTooManyRows,
        message: ErrTooManyRows,
    },
    CatalogEntry {
        name: "ErrRequiresPrimaryKey",
        code: errcode::ErrRequiresPrimaryKey,
        message: ErrRequiresPrimaryKey,
    },
    CatalogEntry {
        name: "ErrNoRaidCompiled",
        code: errcode::ErrNoRaidCompiled,
        message: ErrNoRaidCompiled,
    },
    CatalogEntry {
        name: "ErrUpdateWithoutKeyInSafeMode",
        code: errcode::ErrUpdateWithoutKeyInSafeMode,
        message: ErrUpdateWithoutKeyInSafeMode,
    },
    CatalogEntry {
        name: "ErrKeyDoesNotExist",
        code: errcode::ErrKeyDoesNotExist,
        message: ErrKeyDoesNotExist,
    },
    CatalogEntry {
        name: "ErrCheckNoSuchTable",
        code: errcode::ErrCheckNoSuchTable,
        message: ErrCheckNoSuchTable,
    },
    CatalogEntry {
        name: "ErrCheckNotImplemented",
        code: errcode::ErrCheckNotImplemented,
        message: ErrCheckNotImplemented,
    },
    CatalogEntry {
        name: "ErrCantDoThisDuringAnTransaction",
        code: errcode::ErrCantDoThisDuringAnTransaction,
        message: ErrCantDoThisDuringAnTransaction,
    },
    CatalogEntry {
        name: "ErrErrorDuringCommit",
        code: errcode::ErrErrorDuringCommit,
        message: ErrErrorDuringCommit,
    },
    CatalogEntry {
        name: "ErrErrorDuringRollback",
        code: errcode::ErrErrorDuringRollback,
        message: ErrErrorDuringRollback,
    },
    CatalogEntry {
        name: "ErrErrorDuringFlushLogs",
        code: errcode::ErrErrorDuringFlushLogs,
        message: ErrErrorDuringFlushLogs,
    },
    CatalogEntry {
        name: "ErrErrorDuringCheckpoint",
        code: errcode::ErrErrorDuringCheckpoint,
        message: ErrErrorDuringCheckpoint,
    },
    CatalogEntry {
        name: "ErrNewAbortingConnection",
        code: errcode::ErrNewAbortingConnection,
        message: ErrNewAbortingConnection,
    },
    CatalogEntry {
        name: "ErrDumpNotImplemented",
        code: errcode::ErrDumpNotImplemented,
        message: ErrDumpNotImplemented,
    },
    CatalogEntry {
        name: "ErrFlushMasterBinlogClosed",
        code: errcode::ErrFlushMasterBinlogClosed,
        message: ErrFlushMasterBinlogClosed,
    },
    CatalogEntry {
        name: "ErrIndexRebuild",
        code: errcode::ErrIndexRebuild,
        message: ErrIndexRebuild,
    },
    CatalogEntry {
        name: "ErrMaster",
        code: errcode::ErrMaster,
        message: ErrMaster,
    },
    CatalogEntry {
        name: "ErrMasterNetRead",
        code: errcode::ErrMasterNetRead,
        message: ErrMasterNetRead,
    },
    CatalogEntry {
        name: "ErrMasterNetWrite",
        code: errcode::ErrMasterNetWrite,
        message: ErrMasterNetWrite,
    },
    CatalogEntry {
        name: "ErrFtMatchingKeyNotFound",
        code: errcode::ErrFtMatchingKeyNotFound,
        message: ErrFtMatchingKeyNotFound,
    },
    CatalogEntry {
        name: "ErrLockOrActiveTransaction",
        code: errcode::ErrLockOrActiveTransaction,
        message: ErrLockOrActiveTransaction,
    },
    CatalogEntry {
        name: "ErrUnknownSystemVariable",
        code: errcode::ErrUnknownSystemVariable,
        message: ErrUnknownSystemVariable,
    },
    CatalogEntry {
        name: "ErrCrashedOnUsage",
        code: errcode::ErrCrashedOnUsage,
        message: ErrCrashedOnUsage,
    },
    CatalogEntry {
        name: "ErrCrashedOnRepair",
        code: errcode::ErrCrashedOnRepair,
        message: ErrCrashedOnRepair,
    },
    CatalogEntry {
        name: "ErrWarningNotCompleteRollback",
        code: errcode::ErrWarningNotCompleteRollback,
        message: ErrWarningNotCompleteRollback,
    },
    CatalogEntry {
        name: "ErrTransCacheFull",
        code: errcode::ErrTransCacheFull,
        message: ErrTransCacheFull,
    },
    CatalogEntry {
        name: "ErrSlaveMustStop",
        code: errcode::ErrSlaveMustStop,
        message: ErrSlaveMustStop,
    },
    CatalogEntry {
        name: "ErrSlaveNotRunning",
        code: errcode::ErrSlaveNotRunning,
        message: ErrSlaveNotRunning,
    },
    CatalogEntry {
        name: "ErrBadSlave",
        code: errcode::ErrBadSlave,
        message: ErrBadSlave,
    },
    CatalogEntry {
        name: "ErrMasterInfo",
        code: errcode::ErrMasterInfo,
        message: ErrMasterInfo,
    },
    CatalogEntry {
        name: "ErrSlaveThread",
        code: errcode::ErrSlaveThread,
        message: ErrSlaveThread,
    },
    CatalogEntry {
        name: "ErrTooManyUserConnections",
        code: errcode::ErrTooManyUserConnections,
        message: ErrTooManyUserConnections,
    },
    CatalogEntry {
        name: "ErrSetConstantsOnly",
        code: errcode::ErrSetConstantsOnly,
        message: ErrSetConstantsOnly,
    },
    CatalogEntry {
        name: "ErrLockWaitTimeout",
        code: errcode::ErrLockWaitTimeout,
        message: ErrLockWaitTimeout,
    },
    CatalogEntry {
        name: "ErrLockTableFull",
        code: errcode::ErrLockTableFull,
        message: ErrLockTableFull,
    },
    CatalogEntry {
        name: "ErrReadOnlyTransaction",
        code: errcode::ErrReadOnlyTransaction,
        message: ErrReadOnlyTransaction,
    },
    CatalogEntry {
        name: "ErrDropDBWithReadLock",
        code: errcode::ErrDropDBWithReadLock,
        message: ErrDropDBWithReadLock,
    },
    CatalogEntry {
        name: "ErrCreateDBWithReadLock",
        code: errcode::ErrCreateDBWithReadLock,
        message: ErrCreateDBWithReadLock,
    },
    CatalogEntry {
        name: "ErrWrongArguments",
        code: errcode::ErrWrongArguments,
        message: ErrWrongArguments,
    },
    CatalogEntry {
        name: "ErrNoPermissionToCreateUser",
        code: errcode::ErrNoPermissionToCreateUser,
        message: ErrNoPermissionToCreateUser,
    },
    CatalogEntry {
        name: "ErrUnionTablesInDifferentDir",
        code: errcode::ErrUnionTablesInDifferentDir,
        message: ErrUnionTablesInDifferentDir,
    },
    CatalogEntry {
        name: "ErrLockDeadlock",
        code: errcode::ErrLockDeadlock,
        message: ErrLockDeadlock,
    },
    CatalogEntry {
        name: "ErrTableCantHandleFt",
        code: errcode::ErrTableCantHandleFt,
        message: ErrTableCantHandleFt,
    },
    CatalogEntry {
        name: "ErrCannotAddForeign",
        code: errcode::ErrCannotAddForeign,
        message: ErrCannotAddForeign,
    },
    CatalogEntry {
        name: "ErrNoReferencedRow",
        code: errcode::ErrNoReferencedRow,
        message: ErrNoReferencedRow,
    },
    CatalogEntry {
        name: "ErrRowIsReferenced",
        code: errcode::ErrRowIsReferenced,
        message: ErrRowIsReferenced,
    },
    CatalogEntry {
        name: "ErrConnectToMaster",
        code: errcode::ErrConnectToMaster,
        message: ErrConnectToMaster,
    },
    CatalogEntry {
        name: "ErrQueryOnMaster",
        code: errcode::ErrQueryOnMaster,
        message: ErrQueryOnMaster,
    },
    CatalogEntry {
        name: "ErrErrorWhenExecutingCommand",
        code: errcode::ErrErrorWhenExecutingCommand,
        message: ErrErrorWhenExecutingCommand,
    },
    CatalogEntry {
        name: "ErrWrongUsage",
        code: errcode::ErrWrongUsage,
        message: ErrWrongUsage,
    },
    CatalogEntry {
        name: "ErrWrongNumberOfColumnsInSelect",
        code: errcode::ErrWrongNumberOfColumnsInSelect,
        message: ErrWrongNumberOfColumnsInSelect,
    },
    CatalogEntry {
        name: "ErrCantUpdateWithReadlock",
        code: errcode::ErrCantUpdateWithReadlock,
        message: ErrCantUpdateWithReadlock,
    },
    CatalogEntry {
        name: "ErrMixingNotAllowed",
        code: errcode::ErrMixingNotAllowed,
        message: ErrMixingNotAllowed,
    },
    CatalogEntry {
        name: "ErrDupArgument",
        code: errcode::ErrDupArgument,
        message: ErrDupArgument,
    },
    CatalogEntry {
        name: "ErrUserLimitReached",
        code: errcode::ErrUserLimitReached,
        message: ErrUserLimitReached,
    },
    CatalogEntry {
        name: "ErrSpecificAccessDenied",
        code: errcode::ErrSpecificAccessDenied,
        message: ErrSpecificAccessDenied,
    },
    CatalogEntry {
        name: "ErrLocalVariable",
        code: errcode::ErrLocalVariable,
        message: ErrLocalVariable,
    },
    CatalogEntry {
        name: "ErrGlobalVariable",
        code: errcode::ErrGlobalVariable,
        message: ErrGlobalVariable,
    },
    CatalogEntry {
        name: "ErrNoDefault",
        code: errcode::ErrNoDefault,
        message: ErrNoDefault,
    },
    CatalogEntry {
        name: "ErrWrongValueForVar",
        code: errcode::ErrWrongValueForVar,
        message: ErrWrongValueForVar,
    },
    CatalogEntry {
        name: "ErrWrongTypeForVar",
        code: errcode::ErrWrongTypeForVar,
        message: ErrWrongTypeForVar,
    },
    CatalogEntry {
        name: "ErrVarCantBeRead",
        code: errcode::ErrVarCantBeRead,
        message: ErrVarCantBeRead,
    },
    CatalogEntry {
        name: "ErrCantUseOptionHere",
        code: errcode::ErrCantUseOptionHere,
        message: ErrCantUseOptionHere,
    },
    CatalogEntry {
        name: "ErrNotSupportedYet",
        code: errcode::ErrNotSupportedYet,
        message: ErrNotSupportedYet,
    },
    CatalogEntry {
        name: "ErrMasterFatalErrorReadingBinlog",
        code: errcode::ErrMasterFatalErrorReadingBinlog,
        message: ErrMasterFatalErrorReadingBinlog,
    },
    CatalogEntry {
        name: "ErrSlaveIgnoredTable",
        code: errcode::ErrSlaveIgnoredTable,
        message: ErrSlaveIgnoredTable,
    },
    CatalogEntry {
        name: "ErrIncorrectGlobalLocalVar",
        code: errcode::ErrIncorrectGlobalLocalVar,
        message: ErrIncorrectGlobalLocalVar,
    },
    CatalogEntry {
        name: "ErrWrongFkDef",
        code: errcode::ErrWrongFkDef,
        message: ErrWrongFkDef,
    },
    CatalogEntry {
        name: "ErrKeyRefDoNotMatchTableRef",
        code: errcode::ErrKeyRefDoNotMatchTableRef,
        message: ErrKeyRefDoNotMatchTableRef,
    },
    CatalogEntry {
        name: "ErrOperandColumns",
        code: errcode::ErrOperandColumns,
        message: ErrOperandColumns,
    },
    CatalogEntry {
        name: "ErrSubqueryNo1Row",
        code: errcode::ErrSubqueryNo1Row,
        message: ErrSubqueryNo1Row,
    },
    CatalogEntry {
        name: "ErrUnknownStmtHandler",
        code: errcode::ErrUnknownStmtHandler,
        message: ErrUnknownStmtHandler,
    },
    CatalogEntry {
        name: "ErrCorruptHelpDB",
        code: errcode::ErrCorruptHelpDB,
        message: ErrCorruptHelpDB,
    },
    CatalogEntry {
        name: "ErrCyclicReference",
        code: errcode::ErrCyclicReference,
        message: ErrCyclicReference,
    },
    CatalogEntry {
        name: "ErrAutoConvert",
        code: errcode::ErrAutoConvert,
        message: ErrAutoConvert,
    },
    CatalogEntry {
        name: "ErrIllegalReference",
        code: errcode::ErrIllegalReference,
        message: ErrIllegalReference,
    },
    CatalogEntry {
        name: "ErrDerivedMustHaveAlias",
        code: errcode::ErrDerivedMustHaveAlias,
        message: ErrDerivedMustHaveAlias,
    },
    CatalogEntry {
        name: "ErrSelectReduced",
        code: errcode::ErrSelectReduced,
        message: ErrSelectReduced,
    },
    CatalogEntry {
        name: "ErrTablenameNotAllowedHere",
        code: errcode::ErrTablenameNotAllowedHere,
        message: ErrTablenameNotAllowedHere,
    },
    CatalogEntry {
        name: "ErrNotSupportedAuthMode",
        code: errcode::ErrNotSupportedAuthMode,
        message: ErrNotSupportedAuthMode,
    },
    CatalogEntry {
        name: "ErrSpatialCantHaveNull",
        code: errcode::ErrSpatialCantHaveNull,
        message: ErrSpatialCantHaveNull,
    },
    CatalogEntry {
        name: "ErrCollationCharsetMismatch",
        code: errcode::ErrCollationCharsetMismatch,
        message: ErrCollationCharsetMismatch,
    },
    CatalogEntry {
        name: "ErrSlaveWasRunning",
        code: errcode::ErrSlaveWasRunning,
        message: ErrSlaveWasRunning,
    },
    CatalogEntry {
        name: "ErrSlaveWasNotRunning",
        code: errcode::ErrSlaveWasNotRunning,
        message: ErrSlaveWasNotRunning,
    },
    CatalogEntry {
        name: "ErrTooBigForUncompress",
        code: errcode::ErrTooBigForUncompress,
        message: ErrTooBigForUncompress,
    },
    CatalogEntry {
        name: "ErrZlibZMem",
        code: errcode::ErrZlibZMem,
        message: ErrZlibZMem,
    },
    CatalogEntry {
        name: "ErrZlibZBuf",
        code: errcode::ErrZlibZBuf,
        message: ErrZlibZBuf,
    },
    CatalogEntry {
        name: "ErrZlibZData",
        code: errcode::ErrZlibZData,
        message: ErrZlibZData,
    },
    CatalogEntry {
        name: "ErrCutValueGroupConcat",
        code: errcode::ErrCutValueGroupConcat,
        message: ErrCutValueGroupConcat,
    },
    CatalogEntry {
        name: "ErrWarnTooFewRecords",
        code: errcode::ErrWarnTooFewRecords,
        message: ErrWarnTooFewRecords,
    },
    CatalogEntry {
        name: "ErrWarnTooManyRecords",
        code: errcode::ErrWarnTooManyRecords,
        message: ErrWarnTooManyRecords,
    },
    CatalogEntry {
        name: "ErrWarnNullToNotnull",
        code: errcode::ErrWarnNullToNotnull,
        message: ErrWarnNullToNotnull,
    },
    CatalogEntry {
        name: "ErrWarnDataOutOfRange",
        code: errcode::ErrWarnDataOutOfRange,
        message: ErrWarnDataOutOfRange,
    },
    CatalogEntry {
        name: "WarnDataTruncated",
        code: errcode::WarnDataTruncated,
        message: WarnDataTruncated,
    },
    CatalogEntry {
        name: "ErrWarnUsingOtherHandler",
        code: errcode::ErrWarnUsingOtherHandler,
        message: ErrWarnUsingOtherHandler,
    },
    CatalogEntry {
        name: "ErrCantAggregate2collations",
        code: errcode::ErrCantAggregate2collations,
        message: ErrCantAggregate2collations,
    },
    CatalogEntry {
        name: "ErrDropUser",
        code: errcode::ErrDropUser,
        message: ErrDropUser,
    },
    CatalogEntry {
        name: "ErrRevokeGrants",
        code: errcode::ErrRevokeGrants,
        message: ErrRevokeGrants,
    },
    CatalogEntry {
        name: "ErrCantAggregate3collations",
        code: errcode::ErrCantAggregate3collations,
        message: ErrCantAggregate3collations,
    },
    CatalogEntry {
        name: "ErrCantAggregateNcollations",
        code: errcode::ErrCantAggregateNcollations,
        message: ErrCantAggregateNcollations,
    },
    CatalogEntry {
        name: "ErrVariableIsNotStruct",
        code: errcode::ErrVariableIsNotStruct,
        message: ErrVariableIsNotStruct,
    },
    CatalogEntry {
        name: "ErrUnknownCollation",
        code: errcode::ErrUnknownCollation,
        message: ErrUnknownCollation,
    },
    CatalogEntry {
        name: "ErrSlaveIgnoredSslParams",
        code: errcode::ErrSlaveIgnoredSslParams,
        message: ErrSlaveIgnoredSslParams,
    },
    CatalogEntry {
        name: "ErrServerIsInSecureAuthMode",
        code: errcode::ErrServerIsInSecureAuthMode,
        message: ErrServerIsInSecureAuthMode,
    },
    CatalogEntry {
        name: "ErrWarnFieldResolved",
        code: errcode::ErrWarnFieldResolved,
        message: ErrWarnFieldResolved,
    },
    CatalogEntry {
        name: "ErrBadSlaveUntilCond",
        code: errcode::ErrBadSlaveUntilCond,
        message: ErrBadSlaveUntilCond,
    },
    CatalogEntry {
        name: "ErrMissingSkipSlave",
        code: errcode::ErrMissingSkipSlave,
        message: ErrMissingSkipSlave,
    },
    CatalogEntry {
        name: "ErrUntilCondIgnored",
        code: errcode::ErrUntilCondIgnored,
        message: ErrUntilCondIgnored,
    },
    CatalogEntry {
        name: "ErrWrongNameForIndex",
        code: errcode::ErrWrongNameForIndex,
        message: ErrWrongNameForIndex,
    },
    CatalogEntry {
        name: "ErrWrongNameForCatalog",
        code: errcode::ErrWrongNameForCatalog,
        message: ErrWrongNameForCatalog,
    },
    CatalogEntry {
        name: "ErrWarnQcResize",
        code: errcode::ErrWarnQcResize,
        message: ErrWarnQcResize,
    },
    CatalogEntry {
        name: "ErrBadFtColumn",
        code: errcode::ErrBadFtColumn,
        message: ErrBadFtColumn,
    },
    CatalogEntry {
        name: "ErrUnknownKeyCache",
        code: errcode::ErrUnknownKeyCache,
        message: ErrUnknownKeyCache,
    },
    CatalogEntry {
        name: "ErrWarnHostnameWontWork",
        code: errcode::ErrWarnHostnameWontWork,
        message: ErrWarnHostnameWontWork,
    },
    CatalogEntry {
        name: "ErrUnknownStorageEngine",
        code: errcode::ErrUnknownStorageEngine,
        message: ErrUnknownStorageEngine,
    },
    CatalogEntry {
        name: "ErrWarnDeprecatedSyntax",
        code: errcode::ErrWarnDeprecatedSyntax,
        message: ErrWarnDeprecatedSyntax,
    },
    CatalogEntry {
        name: "ErrNonUpdatableTable",
        code: errcode::ErrNonUpdatableTable,
        message: ErrNonUpdatableTable,
    },
    CatalogEntry {
        name: "ErrFeatureDisabled",
        code: errcode::ErrFeatureDisabled,
        message: ErrFeatureDisabled,
    },
    CatalogEntry {
        name: "ErrOptionPreventsStatement",
        code: errcode::ErrOptionPreventsStatement,
        message: ErrOptionPreventsStatement,
    },
    CatalogEntry {
        name: "ErrDuplicatedValueInType",
        code: errcode::ErrDuplicatedValueInType,
        message: ErrDuplicatedValueInType,
    },
    CatalogEntry {
        name: "ErrTruncatedWrongValue",
        code: errcode::ErrTruncatedWrongValue,
        message: ErrTruncatedWrongValue,
    },
    CatalogEntry {
        name: "ErrTooMuchAutoTimestampCols",
        code: errcode::ErrTooMuchAutoTimestampCols,
        message: ErrTooMuchAutoTimestampCols,
    },
    CatalogEntry {
        name: "ErrInvalidOnUpdate",
        code: errcode::ErrInvalidOnUpdate,
        message: ErrInvalidOnUpdate,
    },
    CatalogEntry {
        name: "ErrUnsupportedPs",
        code: errcode::ErrUnsupportedPs,
        message: ErrUnsupportedPs,
    },
    CatalogEntry {
        name: "ErrGetErrmsg",
        code: errcode::ErrGetErrmsg,
        message: ErrGetErrmsg,
    },
    CatalogEntry {
        name: "ErrGetTemporaryErrmsg",
        code: errcode::ErrGetTemporaryErrmsg,
        message: ErrGetTemporaryErrmsg,
    },
    CatalogEntry {
        name: "ErrUnknownTimeZone",
        code: errcode::ErrUnknownTimeZone,
        message: ErrUnknownTimeZone,
    },
    CatalogEntry {
        name: "ErrWarnInvalidTimestamp",
        code: errcode::ErrWarnInvalidTimestamp,
        message: ErrWarnInvalidTimestamp,
    },
    CatalogEntry {
        name: "ErrInvalidCharacterString",
        code: errcode::ErrInvalidCharacterString,
        message: ErrInvalidCharacterString,
    },
    CatalogEntry {
        name: "ErrWarnAllowedPacketOverflowed",
        code: errcode::ErrWarnAllowedPacketOverflowed,
        message: ErrWarnAllowedPacketOverflowed,
    },
    CatalogEntry {
        name: "ErrConflictingDeclarations",
        code: errcode::ErrConflictingDeclarations,
        message: ErrConflictingDeclarations,
    },
    CatalogEntry {
        name: "ErrSpNoRecursiveCreate",
        code: errcode::ErrSpNoRecursiveCreate,
        message: ErrSpNoRecursiveCreate,
    },
    CatalogEntry {
        name: "ErrSpAlreadyExists",
        code: errcode::ErrSpAlreadyExists,
        message: ErrSpAlreadyExists,
    },
    CatalogEntry {
        name: "ErrSpDoesNotExist",
        code: errcode::ErrSpDoesNotExist,
        message: ErrSpDoesNotExist,
    },
    CatalogEntry {
        name: "ErrSpDropFailed",
        code: errcode::ErrSpDropFailed,
        message: ErrSpDropFailed,
    },
    CatalogEntry {
        name: "ErrSpStoreFailed",
        code: errcode::ErrSpStoreFailed,
        message: ErrSpStoreFailed,
    },
    CatalogEntry {
        name: "ErrSpLilabelMismatch",
        code: errcode::ErrSpLilabelMismatch,
        message: ErrSpLilabelMismatch,
    },
    CatalogEntry {
        name: "ErrSpLabelRedefine",
        code: errcode::ErrSpLabelRedefine,
        message: ErrSpLabelRedefine,
    },
    CatalogEntry {
        name: "ErrSpLabelMismatch",
        code: errcode::ErrSpLabelMismatch,
        message: ErrSpLabelMismatch,
    },
    CatalogEntry {
        name: "ErrSpUninitVar",
        code: errcode::ErrSpUninitVar,
        message: ErrSpUninitVar,
    },
    CatalogEntry {
        name: "ErrSpBadselect",
        code: errcode::ErrSpBadselect,
        message: ErrSpBadselect,
    },
    CatalogEntry {
        name: "ErrSpBadreturn",
        code: errcode::ErrSpBadreturn,
        message: ErrSpBadreturn,
    },
    CatalogEntry {
        name: "ErrSpBadstatement",
        code: errcode::ErrSpBadstatement,
        message: ErrSpBadstatement,
    },
    CatalogEntry {
        name: "ErrUpdateLogDeprecatedIgnored",
        code: errcode::ErrUpdateLogDeprecatedIgnored,
        message: ErrUpdateLogDeprecatedIgnored,
    },
    CatalogEntry {
        name: "ErrUpdateLogDeprecatedTranslated",
        code: errcode::ErrUpdateLogDeprecatedTranslated,
        message: ErrUpdateLogDeprecatedTranslated,
    },
    CatalogEntry {
        name: "ErrQueryInterrupted",
        code: errcode::ErrQueryInterrupted,
        message: ErrQueryInterrupted,
    },
];
