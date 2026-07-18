// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");

//! Direct message and redaction catalog from `pkg/errno/errname.go`.

#![allow(non_upper_case_globals)]

use super::errcode;
use crate::{CatalogEntry, ErrMessage};

/// Message metadata for `ErrHashchk`.
pub const ErrHashchk: ErrMessage = ErrMessage {
    raw: "hashchk",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrNisamchk`.
pub const ErrNisamchk: ErrMessage = ErrMessage {
    raw: "isamchk",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrNo`.
pub const ErrNo: ErrMessage = ErrMessage {
    raw: "NO",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrYes`.
pub const ErrYes: ErrMessage = ErrMessage {
    raw: "YES",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCantCreateFile`.
pub const ErrCantCreateFile: ErrMessage = ErrMessage {
    raw: "Can't create file '%-.200s' (errno: %d - %s)",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCantCreateTable`.
pub const ErrCantCreateTable: ErrMessage = ErrMessage {
    raw: "Can't create table '%-.200s' (errno: %d)",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCantCreateDB`.
pub const ErrCantCreateDB: ErrMessage = ErrMessage {
    raw: "Can't create database '%-.192s' (errno: %d)",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrDBCreateExists`.
pub const ErrDBCreateExists: ErrMessage = ErrMessage {
    raw: "Can't create database '%-.192s'; database exists",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrDBDropExists`.
pub const ErrDBDropExists: ErrMessage = ErrMessage {
    raw: "Can't drop database '%-.192s'; database doesn't exist",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrDBDropDelete`.
pub const ErrDBDropDelete: ErrMessage = ErrMessage {
    raw: "Error dropping database (can't delete '%-.192s', errno: %d)",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrDBDropRmdir`.
pub const ErrDBDropRmdir: ErrMessage = ErrMessage {
    raw: "Error dropping database (can't rmdir '%-.192s', errno: %d)",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCantDeleteFile`.
pub const ErrCantDeleteFile: ErrMessage = ErrMessage {
    raw: "Error on delete of '%-.192s' (errno: %d - %s)",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCantFindSystemRec`.
pub const ErrCantFindSystemRec: ErrMessage = ErrMessage {
    raw: "Can't read record in system table",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCantGetStat`.
pub const ErrCantGetStat: ErrMessage = ErrMessage {
    raw: "Can't get status of '%-.200s' (errno: %d - %s)",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCantGetWd`.
pub const ErrCantGetWd: ErrMessage = ErrMessage {
    raw: "Can't get working directory (errno: %d - %s)",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCantLock`.
pub const ErrCantLock: ErrMessage = ErrMessage {
    raw: "Can't lock file (errno: %d - %s)",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCantOpenFile`.
pub const ErrCantOpenFile: ErrMessage = ErrMessage {
    raw: "Can't open file: '%-.200s' (errno: %d - %s)",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrFileNotFound`.
pub const ErrFileNotFound: ErrMessage = ErrMessage {
    raw: "Can't find file: '%-.200s' (errno: %d - %s)",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCantReadDir`.
pub const ErrCantReadDir: ErrMessage = ErrMessage {
    raw: "Can't read dir of '%-.192s' (errno: %d - %s)",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCantSetWd`.
pub const ErrCantSetWd: ErrMessage = ErrMessage {
    raw: "Can't change dir to '%-.192s' (errno: %d - %s)",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCheckread`.
pub const ErrCheckread: ErrMessage = ErrMessage {
    raw: "Record has changed since last read in table '%-.192s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrDiskFull`.
pub const ErrDiskFull: ErrMessage = ErrMessage {
    raw: "Disk full (%s); waiting for someone to free some space... (errno: %d - %s)",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrDupKey`.
pub const ErrDupKey: ErrMessage = ErrMessage {
    raw: "Can't write; duplicate key in table '%-.192s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrErrorOnClose`.
pub const ErrErrorOnClose: ErrMessage = ErrMessage {
    raw: "Error on close of '%-.192s' (errno: %d - %s)",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrErrorOnRead`.
pub const ErrErrorOnRead: ErrMessage = ErrMessage {
    raw: "Error reading file '%-.200s' (errno: %d - %s)",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrErrorOnRename`.
pub const ErrErrorOnRename: ErrMessage = ErrMessage {
    raw: "Error on rename of '%-.210s' to '%-.210s' (errno: %d - %s)",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrErrorOnWrite`.
pub const ErrErrorOnWrite: ErrMessage = ErrMessage {
    raw: "Error writing file '%-.200s' (errno: %d - %s)",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrFileUsed`.
pub const ErrFileUsed: ErrMessage = ErrMessage {
    raw: "'%-.192s' is locked against change",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrFilsortAbort`.
pub const ErrFilsortAbort: ErrMessage = ErrMessage {
    raw: "Sort aborted",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrFormNotFound`.
pub const ErrFormNotFound: ErrMessage = ErrMessage {
    raw: "View '%-.192s' doesn't exist for '%-.192s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrGetErrno`.
pub const ErrGetErrno: ErrMessage = ErrMessage {
    raw: "Got error %d from storage engine",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrIllegalHa`.
pub const ErrIllegalHa: ErrMessage = ErrMessage {
    raw: "Table storage engine for '%-.192s' doesn't have this option",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrKeyNotFound`.
pub const ErrKeyNotFound: ErrMessage = ErrMessage {
    raw: "Can't find record in '%-.192s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrNotFormFile`.
pub const ErrNotFormFile: ErrMessage = ErrMessage {
    raw: "Incorrect information in file: '%-.200s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrNotKeyFile`.
pub const ErrNotKeyFile: ErrMessage = ErrMessage {
    raw: "Incorrect key file for table '%-.200s'; try to repair it",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrOldKeyFile`.
pub const ErrOldKeyFile: ErrMessage = ErrMessage {
    raw: "Old key file for table '%-.192s'; repair it!",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrOpenAsReadonly`.
pub const ErrOpenAsReadonly: ErrMessage = ErrMessage {
    raw: "Table '%-.192s' is read only",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrOutofMemory`.
pub const ErrOutofMemory: ErrMessage = ErrMessage {
    raw: "Out of memory; restart server and try again (needed %d bytes)",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrOutOfSortMemory`.
pub const ErrOutOfSortMemory: ErrMessage = ErrMessage {
    raw: "Out of sort memory, consider increasing server sort buffer size",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrUnexpectedEOF`.
pub const ErrUnexpectedEOF: ErrMessage = ErrMessage {
    raw: "Unexpected EOF found when reading file '%-.192s' (errno: %d - %s)",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrConCount`.
pub const ErrConCount: ErrMessage = ErrMessage {
    raw: "Too many connections",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrOutOfResources`.
pub const ErrOutOfResources: ErrMessage = ErrMessage {
    raw: "Out of memory; check if mysqld or some other process uses all available memory; if not, you may have to use 'ulimit' to allow mysqld to use more memory or you can add more swap space",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrBadHost`.
pub const ErrBadHost: ErrMessage = ErrMessage {
    raw: "Can't get hostname for your address",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrHandshake`.
pub const ErrHandshake: ErrMessage = ErrMessage {
    raw: "Bad handshake",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrDBaccessDenied`.
pub const ErrDBaccessDenied: ErrMessage = ErrMessage {
    raw: "Access denied for user '%-.48s'@'%-.255s' to database '%-.192s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrAccessDenied`.
pub const ErrAccessDenied: ErrMessage = ErrMessage {
    raw: "Access denied for user '%-.48s'@'%-.255s' (using password: %s)",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrNoDB`.
pub const ErrNoDB: ErrMessage = ErrMessage {
    raw: "No database selected",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrUnknownCom`.
pub const ErrUnknownCom: ErrMessage = ErrMessage {
    raw: "Unknown command",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrBadNull`.
pub const ErrBadNull: ErrMessage = ErrMessage {
    raw: "Column '%-.192s' cannot be null",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrBadDB`.
pub const ErrBadDB: ErrMessage = ErrMessage {
    raw: "Unknown database '%-.192s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTableExists`.
pub const ErrTableExists: ErrMessage = ErrMessage {
    raw: "Table '%-.192s' already exists",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrBadTable`.
pub const ErrBadTable: ErrMessage = ErrMessage {
    raw: "Unknown table '%-.100s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrNonUniq`.
pub const ErrNonUniq: ErrMessage = ErrMessage {
    raw: "Column '%-.192s' in %-.192s is ambiguous",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrServerShutdown`.
pub const ErrServerShutdown: ErrMessage = ErrMessage {
    raw: "Server shutdown in progress",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrBadField`.
pub const ErrBadField: ErrMessage = ErrMessage {
    raw: "Unknown column '%-.192s' in '%-.192s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrFieldNotInGroupBy`.
pub const ErrFieldNotInGroupBy: ErrMessage = ErrMessage {
    raw: "Expression #%d of %s is not in GROUP BY clause and contains nonaggregated column '%s' which is not functionally dependent on columns in GROUP BY clause; this is incompatible with sql_mode=only_full_group_by",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWrongGroupField`.
pub const ErrWrongGroupField: ErrMessage = ErrMessage {
    raw: "Can't group on '%-.192s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWrongSumSelect`.
pub const ErrWrongSumSelect: ErrMessage = ErrMessage {
    raw: "Statement has sum functions and columns in same statement",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWrongValueCount`.
pub const ErrWrongValueCount: ErrMessage = ErrMessage {
    raw: "Column count doesn't match value count",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTooLongIdent`.
pub const ErrTooLongIdent: ErrMessage = ErrMessage {
    raw: "Identifier name '%-.100s' is too long",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrDupFieldName`.
pub const ErrDupFieldName: ErrMessage = ErrMessage {
    raw: "Duplicate column name '%-.192s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrDupKeyName`.
pub const ErrDupKeyName: ErrMessage = ErrMessage {
    raw: "Duplicate key name '%-.192s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrDupEntry`.
pub const ErrDupEntry: ErrMessage = ErrMessage {
    raw: "Duplicate entry '%-.64s' for key '%-.192s'",
    redact_arg_pos: &[0],
};
/// Message metadata for `ErrWrongFieldSpec`.
pub const ErrWrongFieldSpec: ErrMessage = ErrMessage {
    raw: "Incorrect column specifier for column '%-.192s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrParse`.
pub const ErrParse: ErrMessage = ErrMessage {
    raw: "%s %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrEmptyQuery`.
pub const ErrEmptyQuery: ErrMessage = ErrMessage {
    raw: "Query was empty",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrNonuniqTable`.
pub const ErrNonuniqTable: ErrMessage = ErrMessage {
    raw: "Not unique table/alias: '%-.192s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInvalidDefault`.
pub const ErrInvalidDefault: ErrMessage = ErrMessage {
    raw: "Invalid default value for '%-.192s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrMultiplePriKey`.
pub const ErrMultiplePriKey: ErrMessage = ErrMessage {
    raw: "Multiple primary key defined",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTooManyKeys`.
pub const ErrTooManyKeys: ErrMessage = ErrMessage {
    raw: "Too many keys specified; max %d keys allowed",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTooManyKeyParts`.
pub const ErrTooManyKeyParts: ErrMessage = ErrMessage {
    raw: "Too many key parts specified; max %d parts allowed",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTooLongKey`.
pub const ErrTooLongKey: ErrMessage = ErrMessage {
    raw: "Specified key was too long (%d bytes); max key length is %d bytes",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrKeyColumnDoesNotExits`.
pub const ErrKeyColumnDoesNotExits: ErrMessage = ErrMessage {
    raw: "Key column '%-.192s' doesn't exist in table",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrBlobUsedAsKey`.
pub const ErrBlobUsedAsKey: ErrMessage = ErrMessage {
    raw: "BLOB column '%-.192s' can't be used in key specification with the used table type",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrJSONVacuousPath`.
pub const ErrJSONVacuousPath: ErrMessage = ErrMessage {
    raw: "The path expression '$' is not allowed in this context.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrJSONBadOneOrAllArg`.
pub const ErrJSONBadOneOrAllArg: ErrMessage = ErrMessage {
    raw: "The oneOrAll argument to %s may take these values: 'one' or 'all'.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTooBigFieldlength`.
pub const ErrTooBigFieldlength: ErrMessage = ErrMessage {
    raw: "Column length too big for column '%-.192s' (max = %d); use BLOB or TEXT instead",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWrongAutoKey`.
pub const ErrWrongAutoKey: ErrMessage = ErrMessage {
    raw: "Incorrect table definition; there can be only one auto column and it must be defined as a key",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrReady`.
pub const ErrReady: ErrMessage = ErrMessage {
    raw: "%s: ready for connections.\nVersion: '%s'  socket: '%s'  port: %d",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrNormalShutdown`.
pub const ErrNormalShutdown: ErrMessage = ErrMessage {
    raw: "%s: Normal shutdown\n",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrGotSignal`.
pub const ErrGotSignal: ErrMessage = ErrMessage {
    raw: "%s: Got signal %d. Aborting!\n",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrShutdownComplete`.
pub const ErrShutdownComplete: ErrMessage = ErrMessage {
    raw: "%s: Shutdown complete\n",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrForcingClose`.
pub const ErrForcingClose: ErrMessage = ErrMessage {
    raw: "%s: Forcing close of thread %d  user: '%-.48s'\n",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrIpsock`.
pub const ErrIpsock: ErrMessage = ErrMessage {
    raw: "Can't create IP socket",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrNoSuchIndex`.
pub const ErrNoSuchIndex: ErrMessage = ErrMessage {
    raw: "Table '%-.192s' has no index like the one used in CREATE INDEX; recreate the table",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWrongFieldTerminators`.
pub const ErrWrongFieldTerminators: ErrMessage = ErrMessage {
    raw: "Field separator argument is not what is expected; check the manual",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrBlobsAndNoTerminated`.
pub const ErrBlobsAndNoTerminated: ErrMessage = ErrMessage {
    raw: "You can't use fixed rowlength with BLOBs; please use 'fields terminated by'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTextFileNotReadable`.
pub const ErrTextFileNotReadable: ErrMessage = ErrMessage {
    raw: "The file '%-.128s' must be in the database directory or be readable by all",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrFileExists`.
pub const ErrFileExists: ErrMessage = ErrMessage {
    raw: "File '%-.200s' already exists",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrLoadInfo`.
pub const ErrLoadInfo: ErrMessage = ErrMessage {
    raw: "Records: %d  Deleted: %d  Skipped: %d  Warnings: %d",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrAlterInfo`.
pub const ErrAlterInfo: ErrMessage = ErrMessage {
    raw: "Records: %d  Duplicates: %d",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWrongSubKey`.
pub const ErrWrongSubKey: ErrMessage = ErrMessage {
    raw: "Incorrect prefix key; the used key part isn't a string, the used length is longer than the key part, or the storage engine doesn't support unique prefix keys",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCantRemoveAllFields`.
pub const ErrCantRemoveAllFields: ErrMessage = ErrMessage {
    raw: "You can't delete all columns with ALTER TABLE; use DROP TABLE instead",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCantDropFieldOrKey`.
pub const ErrCantDropFieldOrKey: ErrMessage = ErrMessage {
    raw: "Can't DROP '%-.192s'; check that column/key exists",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInsertInfo`.
pub const ErrInsertInfo: ErrMessage = ErrMessage {
    raw: "Records: %d  Duplicates: %d  Warnings: %d",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrUpdateTableUsed`.
pub const ErrUpdateTableUsed: ErrMessage = ErrMessage {
    raw: "You can't specify target table '%-.192s' for update in FROM clause",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrNoSuchThread`.
pub const ErrNoSuchThread: ErrMessage = ErrMessage {
    raw: "Unknown thread id: %d",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrKillDenied`.
pub const ErrKillDenied: ErrMessage = ErrMessage {
    raw: "You are not owner of thread %d",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrNoTablesUsed`.
pub const ErrNoTablesUsed: ErrMessage = ErrMessage {
    raw: "No tables used",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTooBigSet`.
pub const ErrTooBigSet: ErrMessage = ErrMessage {
    raw: "Too many strings for column %-.192s and SET",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrNoUniqueLogFile`.
pub const ErrNoUniqueLogFile: ErrMessage = ErrMessage {
    raw: "Can't generate a unique log-filename %-.200s.(1-999)\n",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTableNotLockedForWrite`.
pub const ErrTableNotLockedForWrite: ErrMessage = ErrMessage {
    raw: "Table '%-.192s' was locked with a READ lock and can't be updated",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTableNotLocked`.
pub const ErrTableNotLocked: ErrMessage = ErrMessage {
    raw: "Table '%-.192s' was not locked with LOCK TABLES",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrBlobCantHaveDefault`.
pub const ErrBlobCantHaveDefault: ErrMessage = ErrMessage {
    raw: "BLOB/TEXT/JSON column '%-.192s' can't have a default value",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWrongDBName`.
pub const ErrWrongDBName: ErrMessage = ErrMessage {
    raw: "Incorrect database name '%-.100s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWrongTableName`.
pub const ErrWrongTableName: ErrMessage = ErrMessage {
    raw: "Incorrect table name '%-.100s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTooBigSelect`.
pub const ErrTooBigSelect: ErrMessage = ErrMessage {
    raw: "The SELECT would examine more than MAXJOINSIZE rows; check your WHERE and use SET SQLBIGSELECTS=1 or SET MAXJOINSIZE=# if the SELECT is okay",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrUnknown`.
pub const ErrUnknown: ErrMessage = ErrMessage {
    raw: "Unknown error",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrUnknownProcedure`.
pub const ErrUnknownProcedure: ErrMessage = ErrMessage {
    raw: "Unknown procedure '%-.192s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWrongParamcountToProcedure`.
pub const ErrWrongParamcountToProcedure: ErrMessage = ErrMessage {
    raw: "Incorrect parameter count to procedure '%-.192s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWrongParametersToProcedure`.
pub const ErrWrongParametersToProcedure: ErrMessage = ErrMessage {
    raw: "Incorrect parameters to procedure '%-.192s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrUnknownTable`.
pub const ErrUnknownTable: ErrMessage = ErrMessage {
    raw: "Unknown table '%-.192s' in %-.32s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrFieldSpecifiedTwice`.
pub const ErrFieldSpecifiedTwice: ErrMessage = ErrMessage {
    raw: "Column '%-.192s' specified twice",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInvalidGroupFuncUse`.
pub const ErrInvalidGroupFuncUse: ErrMessage = ErrMessage {
    raw: "Invalid use of group function",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrUnsupportedExtension`.
pub const ErrUnsupportedExtension: ErrMessage = ErrMessage {
    raw: "Table '%-.192s' uses an extension that doesn't exist in this MySQL version",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTableMustHaveColumns`.
pub const ErrTableMustHaveColumns: ErrMessage = ErrMessage {
    raw: "A table must have at least 1 column",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrRecordFileFull`.
pub const ErrRecordFileFull: ErrMessage = ErrMessage {
    raw: "The table '%-.192s' is full",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrUnknownCharacterSet`.
pub const ErrUnknownCharacterSet: ErrMessage = ErrMessage {
    raw: "Unknown character set: '%-.64s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTooManyTables`.
pub const ErrTooManyTables: ErrMessage = ErrMessage {
    raw: "Too many tables; MySQL can only use %d tables in a join",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTooManyFields`.
pub const ErrTooManyFields: ErrMessage = ErrMessage {
    raw: "Too many columns",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTooBigRowsize`.
pub const ErrTooBigRowsize: ErrMessage = ErrMessage {
    raw: "Row size too large. The maximum row size for the used table type, not counting BLOBs, is %d. This includes storage overhead, check the manual. You have to change some columns to TEXT or BLOBs",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrStackOverrun`.
pub const ErrStackOverrun: ErrMessage = ErrMessage {
    raw: "Thread stack overrun:  Used: %d of a %d stack.  Use 'mysqld --threadStack=#' to specify a bigger stack if needed",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWrongOuterJoin`.
pub const ErrWrongOuterJoin: ErrMessage = ErrMessage {
    raw: "Cross dependency found in OUTER JOIN; examine your ON conditions",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrNullColumnInIndex`.
pub const ErrNullColumnInIndex: ErrMessage = ErrMessage {
    raw: "Table handler doesn't support NULL in given index. Please change column '%-.192s' to be NOT NULL or use another handler",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCantFindUdf`.
pub const ErrCantFindUdf: ErrMessage = ErrMessage {
    raw: "Can't load function '%-.192s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCantInitializeUdf`.
pub const ErrCantInitializeUdf: ErrMessage = ErrMessage {
    raw: "Can't initialize function '%-.192s'; %-.80s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrUdfNoPaths`.
pub const ErrUdfNoPaths: ErrMessage = ErrMessage {
    raw: "No paths allowed for shared library",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrUdfExists`.
pub const ErrUdfExists: ErrMessage = ErrMessage {
    raw: "Function '%-.192s' already exists",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCantOpenLibrary`.
pub const ErrCantOpenLibrary: ErrMessage = ErrMessage {
    raw: "Can't open shared library '%-.192s' (errno: %d %-.128s)",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCantFindDlEntry`.
pub const ErrCantFindDlEntry: ErrMessage = ErrMessage {
    raw: "Can't find symbol '%-.128s' in library",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrFunctionNotDefined`.
pub const ErrFunctionNotDefined: ErrMessage = ErrMessage {
    raw: "Function '%-.192s' is not defined",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrHostIsBlocked`.
pub const ErrHostIsBlocked: ErrMessage = ErrMessage {
    raw: "Host '%-.255s' is blocked because of many connection errors; unblock with 'mysqladmin flush-hosts'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrHostNotPrivileged`.
pub const ErrHostNotPrivileged: ErrMessage = ErrMessage {
    raw: "Host '%-.255s' is not allowed to connect to this MySQL server",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrPasswordAnonymousUser`.
pub const ErrPasswordAnonymousUser: ErrMessage = ErrMessage {
    raw: "You are using MySQL as an anonymous user and anonymous users are not allowed to change passwords",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrPasswordNotAllowed`.
pub const ErrPasswordNotAllowed: ErrMessage = ErrMessage {
    raw: "You must have privileges to update tables in the mysql database to be able to change passwords for others",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrPasswordNoMatch`.
pub const ErrPasswordNoMatch: ErrMessage = ErrMessage {
    raw: "Can't find any matching row in the user table",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrUpdateInfo`.
pub const ErrUpdateInfo: ErrMessage = ErrMessage {
    raw: "Rows matched: %d  Changed: %d  Warnings: %d",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCantCreateThread`.
pub const ErrCantCreateThread: ErrMessage = ErrMessage {
    raw: "Can't create a new thread (errno %d); if you are not out of available memory, you can consult the manual for a possible OS-dependent bug",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWrongValueCountOnRow`.
pub const ErrWrongValueCountOnRow: ErrMessage = ErrMessage {
    raw: "Column count doesn't match value count at row %d",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCantReopenTable`.
pub const ErrCantReopenTable: ErrMessage = ErrMessage {
    raw: "Can't reopen table: '%-.192s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInvalidUseOfNull`.
pub const ErrInvalidUseOfNull: ErrMessage = ErrMessage {
    raw: "Invalid use of NULL value",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrRegexp`.
pub const ErrRegexp: ErrMessage = ErrMessage {
    raw: "Got error '%-.64s' from regexp",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrMixOfGroupFuncAndFields`.
pub const ErrMixOfGroupFuncAndFields: ErrMessage = ErrMessage {
    raw: "Mixing of GROUP columns (MIN(),MAX(),COUNT(),...) with no GROUP columns is illegal if there is no GROUP BY clause",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrNonexistingGrant`.
pub const ErrNonexistingGrant: ErrMessage = ErrMessage {
    raw: "There is no such grant defined for user '%-.48s' on host '%-.255s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTableaccessDenied`.
pub const ErrTableaccessDenied: ErrMessage = ErrMessage {
    raw: "%-.128s command denied to user '%-.48s'@'%-.255s' for table '%-.64s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrColumnaccessDenied`.
pub const ErrColumnaccessDenied: ErrMessage = ErrMessage {
    raw: "%-.16s command denied to user '%-.48s'@'%-.255s' for column '%-.192s' in table '%-.192s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrIllegalGrantForTable`.
pub const ErrIllegalGrantForTable: ErrMessage = ErrMessage {
    raw: "Illegal GRANT/REVOKE command; please consult the manual to see which privileges can be used",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrGrantWrongHostOrUser`.
pub const ErrGrantWrongHostOrUser: ErrMessage = ErrMessage {
    raw: "The host or user argument to GRANT is too long",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrNoSuchTable`.
pub const ErrNoSuchTable: ErrMessage = ErrMessage {
    raw: "Table '%-.192s.%-.192s' doesn't exist",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrNonexistingTableGrant`.
pub const ErrNonexistingTableGrant: ErrMessage = ErrMessage {
    raw: "There is no such grant defined for user '%-.48s' on host '%-.255s' on table '%-.192s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrNotAllowedCommand`.
pub const ErrNotAllowedCommand: ErrMessage = ErrMessage {
    raw: "The used command is not allowed with this MySQL version",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSyntax`.
pub const ErrSyntax: ErrMessage = ErrMessage {
    raw: "You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrDelayedCantChangeLock`.
pub const ErrDelayedCantChangeLock: ErrMessage = ErrMessage {
    raw: "Delayed insert thread couldn't get requested lock for table %-.192s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTooManyDelayedThreads`.
pub const ErrTooManyDelayedThreads: ErrMessage = ErrMessage {
    raw: "Too many delayed threads in use",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrAbortingConnection`.
pub const ErrAbortingConnection: ErrMessage = ErrMessage {
    raw: "Aborted connection %d to db: '%-.192s' user: '%-.48s' (%-.64s)",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrNetPacketTooLarge`.
pub const ErrNetPacketTooLarge: ErrMessage = ErrMessage {
    raw: "Got a packet bigger than 'max_allowed_packet' bytes",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrNetReadErrorFromPipe`.
pub const ErrNetReadErrorFromPipe: ErrMessage = ErrMessage {
    raw: "Got a read error from the connection pipe",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrNetFcntl`.
pub const ErrNetFcntl: ErrMessage = ErrMessage {
    raw: "Got an error from fcntl()",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrNetPacketsOutOfOrder`.
pub const ErrNetPacketsOutOfOrder: ErrMessage = ErrMessage {
    raw: "Got packets out of order",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrNetUncompress`.
pub const ErrNetUncompress: ErrMessage = ErrMessage {
    raw: "Couldn't uncompress communication packet",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrNetRead`.
pub const ErrNetRead: ErrMessage = ErrMessage {
    raw: "Got an error reading communication packets",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrNetReadInterrupted`.
pub const ErrNetReadInterrupted: ErrMessage = ErrMessage {
    raw: "Got timeout reading communication packets",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrNetErrorOnWrite`.
pub const ErrNetErrorOnWrite: ErrMessage = ErrMessage {
    raw: "Got an error writing communication packets",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrNetWriteInterrupted`.
pub const ErrNetWriteInterrupted: ErrMessage = ErrMessage {
    raw: "Got timeout writing communication packets",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTooLongString`.
pub const ErrTooLongString: ErrMessage = ErrMessage {
    raw: "Result string is longer than 'maxAllowedPacket' bytes",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTableCantHandleBlob`.
pub const ErrTableCantHandleBlob: ErrMessage = ErrMessage {
    raw: "The used table type doesn't support BLOB/TEXT columns",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTableCantHandleAutoIncrement`.
pub const ErrTableCantHandleAutoIncrement: ErrMessage = ErrMessage {
    raw: "The used table type doesn't support AUTOINCREMENT columns",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrDelayedInsertTableLocked`.
pub const ErrDelayedInsertTableLocked: ErrMessage = ErrMessage {
    raw: "INSERT DELAYED can't be used with table '%-.192s' because it is locked with LOCK TABLES",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWrongColumnName`.
pub const ErrWrongColumnName: ErrMessage = ErrMessage {
    raw: "Incorrect column name '%-.100s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWrongKeyColumn`.
pub const ErrWrongKeyColumn: ErrMessage = ErrMessage {
    raw: "The used storage engine can't index column '%-.192s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWrongMrgTable`.
pub const ErrWrongMrgTable: ErrMessage = ErrMessage {
    raw: "Unable to open underlying table which is differently defined or of non-MyISAM type or doesn't exist",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrDupUnique`.
pub const ErrDupUnique: ErrMessage = ErrMessage {
    raw: "Can't write, because of unique constraint, to table '%-.192s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrBlobKeyWithoutLength`.
pub const ErrBlobKeyWithoutLength: ErrMessage = ErrMessage {
    raw: "BLOB/TEXT column '%-.192s' used in key specification without a key length",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrPrimaryCantHaveNull`.
pub const ErrPrimaryCantHaveNull: ErrMessage = ErrMessage {
    raw:
        "All parts of a PRIMARY KEY must be NOT NULL; if you need NULL in a key, use UNIQUE instead",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTooManyRows`.
pub const ErrTooManyRows: ErrMessage = ErrMessage {
    raw: "Result consisted of more than one row",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrRequiresPrimaryKey`.
pub const ErrRequiresPrimaryKey: ErrMessage = ErrMessage {
    raw: "This table type requires a primary key",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrNoRaidCompiled`.
pub const ErrNoRaidCompiled: ErrMessage = ErrMessage {
    raw: "This version of MySQL is not compiled with RAID support",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrUpdateWithoutKeyInSafeMode`.
pub const ErrUpdateWithoutKeyInSafeMode: ErrMessage = ErrMessage {
    raw: "You are using safe update mode and you tried to update a table without a WHERE that uses a KEY column",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrKeyDoesNotExist`.
pub const ErrKeyDoesNotExist: ErrMessage = ErrMessage {
    raw: "Key '%-.192s' doesn't exist in table '%-.192s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCheckNoSuchTable`.
pub const ErrCheckNoSuchTable: ErrMessage = ErrMessage {
    raw: "Can't open table",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCheckNotImplemented`.
pub const ErrCheckNotImplemented: ErrMessage = ErrMessage {
    raw: "The storage engine for the table doesn't support %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCantDoThisDuringAnTransaction`.
pub const ErrCantDoThisDuringAnTransaction: ErrMessage = ErrMessage {
    raw: "You are not allowed to execute this command in a transaction",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrErrorDuringCommit`.
pub const ErrErrorDuringCommit: ErrMessage = ErrMessage {
    raw: "Got error %d during COMMIT",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrErrorDuringRollback`.
pub const ErrErrorDuringRollback: ErrMessage = ErrMessage {
    raw: "Got error %d during ROLLBACK",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrErrorDuringFlushLogs`.
pub const ErrErrorDuringFlushLogs: ErrMessage = ErrMessage {
    raw: "Got error %d during FLUSHLOGS",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrErrorDuringCheckpoint`.
pub const ErrErrorDuringCheckpoint: ErrMessage = ErrMessage {
    raw: "Got error %d during CHECKPOINT",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrNewAbortingConnection`.
pub const ErrNewAbortingConnection: ErrMessage = ErrMessage {
    raw: "Aborted connection %d to db: '%-.192s' user: '%-.48s' host: '%-.255s' (%-.64s)",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrDumpNotImplemented`.
pub const ErrDumpNotImplemented: ErrMessage = ErrMessage {
    raw: "The storage engine for the table does not support binary table dump",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrIndexRebuild`.
pub const ErrIndexRebuild: ErrMessage = ErrMessage {
    raw: "Failed rebuilding the index of  dumped table '%-.192s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrFtMatchingKeyNotFound`.
pub const ErrFtMatchingKeyNotFound: ErrMessage = ErrMessage {
    raw: "Can't find FULLTEXT index matching the column list",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrLockOrActiveTransaction`.
pub const ErrLockOrActiveTransaction: ErrMessage = ErrMessage {
    raw: "Can't execute the given command because you have active locked tables or an active transaction",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrUnknownSystemVariable`.
pub const ErrUnknownSystemVariable: ErrMessage = ErrMessage {
    raw: "Unknown system variable '%-.64s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCrashedOnUsage`.
pub const ErrCrashedOnUsage: ErrMessage = ErrMessage {
    raw: "Table '%-.192s' is marked as crashed and should be repaired",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCrashedOnRepair`.
pub const ErrCrashedOnRepair: ErrMessage = ErrMessage {
    raw: "Table '%-.192s' is marked as crashed and last (automatic?) repair failed",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWarningNotCompleteRollback`.
pub const ErrWarningNotCompleteRollback: ErrMessage = ErrMessage {
    raw: "Some non-transactional changed tables couldn't be rolled back",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTransCacheFull`.
pub const ErrTransCacheFull: ErrMessage = ErrMessage {
    raw: "Multi-statement transaction required more than 'maxBinlogCacheSize' bytes of storage; increase this mysqld variable and try again",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTooManyUserConnections`.
pub const ErrTooManyUserConnections: ErrMessage = ErrMessage {
    raw: "User %-.64s has exceeded the 'max_user_connections' resource",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSetConstantsOnly`.
pub const ErrSetConstantsOnly: ErrMessage = ErrMessage {
    raw: "You may only use constant expressions with SET",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrLockWaitTimeout`.
pub const ErrLockWaitTimeout: ErrMessage = ErrMessage {
    raw: "Lock wait timeout exceeded; try restarting transaction",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrLockTableFull`.
pub const ErrLockTableFull: ErrMessage = ErrMessage {
    raw: "The total number of locks exceeds the lock table size",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrReadOnlyTransaction`.
pub const ErrReadOnlyTransaction: ErrMessage = ErrMessage {
    raw: "Update locks cannot be acquired during a READ UNCOMMITTED transaction",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrDropDBWithReadLock`.
pub const ErrDropDBWithReadLock: ErrMessage = ErrMessage {
    raw: "DROP DATABASE not allowed while thread is holding global read lock",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCreateDBWithReadLock`.
pub const ErrCreateDBWithReadLock: ErrMessage = ErrMessage {
    raw: "CREATE DATABASE not allowed while thread is holding global read lock",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWrongArguments`.
pub const ErrWrongArguments: ErrMessage = ErrMessage {
    raw: "Incorrect arguments to %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrNoPermissionToCreateUser`.
pub const ErrNoPermissionToCreateUser: ErrMessage = ErrMessage {
    raw: "'%-.48s'@'%-.255s' is not allowed to create new users",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrUnionTablesInDifferentDir`.
pub const ErrUnionTablesInDifferentDir: ErrMessage = ErrMessage {
    raw: "Incorrect table definition; all MERGE tables must be in the same database",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrLockDeadlock`.
pub const ErrLockDeadlock: ErrMessage = ErrMessage {
    raw: "Deadlock found when trying to get lock; try restarting transaction",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTableCantHandleFt`.
pub const ErrTableCantHandleFt: ErrMessage = ErrMessage {
    raw: "The used table type doesn't support FULLTEXT indexes",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCannotAddForeign`.
pub const ErrCannotAddForeign: ErrMessage = ErrMessage {
    raw: "Cannot add foreign key constraint",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrNoReferencedRow`.
pub const ErrNoReferencedRow: ErrMessage = ErrMessage {
    raw: "Cannot add or update a child row: a foreign key constraint fails",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrRowIsReferenced`.
pub const ErrRowIsReferenced: ErrMessage = ErrMessage {
    raw: "Cannot delete or update a parent row: a foreign key constraint fails",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrErrorWhenExecutingCommand`.
pub const ErrErrorWhenExecutingCommand: ErrMessage = ErrMessage {
    raw: "Error when executing command %s: %-.128s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWrongUsage`.
pub const ErrWrongUsage: ErrMessage = ErrMessage {
    raw: "Incorrect usage of %s and %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWrongNumberOfColumnsInSelect`.
pub const ErrWrongNumberOfColumnsInSelect: ErrMessage = ErrMessage {
    raw: "The used SELECT statements have a different number of columns",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCantUpdateWithReadlock`.
pub const ErrCantUpdateWithReadlock: ErrMessage = ErrMessage {
    raw: "Can't execute the query because you have a conflicting read lock",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrMixingNotAllowed`.
pub const ErrMixingNotAllowed: ErrMessage = ErrMessage {
    raw: "Mixing of transactional and non-transactional tables is disabled",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrDupArgument`.
pub const ErrDupArgument: ErrMessage = ErrMessage {
    raw: "Option '%s' used twice in statement",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrUserLimitReached`.
pub const ErrUserLimitReached: ErrMessage = ErrMessage {
    raw: "User '%-.64s' has exceeded the '%s' resource (current value: %d)",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSpecificAccessDenied`.
pub const ErrSpecificAccessDenied: ErrMessage = ErrMessage {
    raw: "Access denied; you need (at least one of) the %-.128s privilege(s) for this operation",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrLocalVariable`.
pub const ErrLocalVariable: ErrMessage = ErrMessage {
    raw: "Variable '%-.64s' is a SESSION variable and can't be used with SET GLOBAL",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrGlobalVariable`.
pub const ErrGlobalVariable: ErrMessage = ErrMessage {
    raw: "Variable '%-.64s' is a GLOBAL variable and should be set with SET GLOBAL",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrNoDefault`.
pub const ErrNoDefault: ErrMessage = ErrMessage {
    raw: "Variable '%-.64s' doesn't have a default value",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWrongValueForVar`.
pub const ErrWrongValueForVar: ErrMessage = ErrMessage {
    raw: "Variable '%-.64s' can't be set to the value of '%-.200s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWrongTypeForVar`.
pub const ErrWrongTypeForVar: ErrMessage = ErrMessage {
    raw: "Incorrect argument type to variable '%-.64s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrVarCantBeRead`.
pub const ErrVarCantBeRead: ErrMessage = ErrMessage {
    raw: "Variable '%-.64s' can only be set, not read",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCantUseOptionHere`.
pub const ErrCantUseOptionHere: ErrMessage = ErrMessage {
    raw: "Incorrect usage/placement of '%s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrNotSupportedYet`.
pub const ErrNotSupportedYet: ErrMessage = ErrMessage {
    raw: "This version of TiDB doesn't yet support '%s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrIncorrectGlobalLocalVar`.
pub const ErrIncorrectGlobalLocalVar: ErrMessage = ErrMessage {
    raw: "Variable '%-.192s' is a %s variable",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWrongFkDef`.
pub const ErrWrongFkDef: ErrMessage = ErrMessage {
    raw: "Incorrect foreign key definition for '%-.192s': %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrKeyRefDoNotMatchTableRef`.
pub const ErrKeyRefDoNotMatchTableRef: ErrMessage = ErrMessage {
    raw: "Key reference and table reference don't match",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrOperandColumns`.
pub const ErrOperandColumns: ErrMessage = ErrMessage {
    raw: "Operand should contain %d column(s)",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSubqueryNo1Row`.
pub const ErrSubqueryNo1Row: ErrMessage = ErrMessage {
    raw: "Subquery returns more than 1 row",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrUnknownStmtHandler`.
pub const ErrUnknownStmtHandler: ErrMessage = ErrMessage {
    raw: "Unknown prepared statement handler (%.*s) given to %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCorruptHelpDB`.
pub const ErrCorruptHelpDB: ErrMessage = ErrMessage {
    raw: "Help database is corrupt or does not exist",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCyclicReference`.
pub const ErrCyclicReference: ErrMessage = ErrMessage {
    raw: "Cyclic reference on subqueries",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrAutoConvert`.
pub const ErrAutoConvert: ErrMessage = ErrMessage {
    raw: "Converting column '%s' from %s to %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrIllegalReference`.
pub const ErrIllegalReference: ErrMessage = ErrMessage {
    raw: "Reference '%-.64s' not supported (%s)",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrDerivedMustHaveAlias`.
pub const ErrDerivedMustHaveAlias: ErrMessage = ErrMessage {
    raw: "Every derived table must have its own alias",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSelectReduced`.
pub const ErrSelectReduced: ErrMessage = ErrMessage {
    raw: "Select %d was reduced during optimization",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTablenameNotAllowedHere`.
pub const ErrTablenameNotAllowedHere: ErrMessage = ErrMessage {
    raw: "Table '%s' from one of the %ss cannot be used in %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrNotSupportedAuthMode`.
pub const ErrNotSupportedAuthMode: ErrMessage = ErrMessage {
    raw: "Client does not support authentication protocol requested by server; consider upgrading MySQL client",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSpatialCantHaveNull`.
pub const ErrSpatialCantHaveNull: ErrMessage = ErrMessage {
    raw: "All parts of a SPATIAL index must be NOT NULL",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCollationCharsetMismatch`.
pub const ErrCollationCharsetMismatch: ErrMessage = ErrMessage {
    raw: "COLLATION '%s' is not valid for CHARACTER SET '%s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTooBigForUncompress`.
pub const ErrTooBigForUncompress: ErrMessage = ErrMessage {
    raw: "Uncompressed data size too large; the maximum size is %d (probably, length of uncompressed data was corrupted)",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrZlibZMem`.
pub const ErrZlibZMem: ErrMessage = ErrMessage {
    raw: "ZLIB: Not enough memory",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrZlibZBuf`.
pub const ErrZlibZBuf: ErrMessage = ErrMessage {
    raw: "ZLIB: Not enough room in the output buffer (probably, length of uncompressed data was corrupted)",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrZlibZData`.
pub const ErrZlibZData: ErrMessage = ErrMessage {
    raw: "ZLIB: Input data corrupted",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCutValueGroupConcat`.
pub const ErrCutValueGroupConcat: ErrMessage = ErrMessage {
    raw: "Some rows were cut by GROUPCONCAT(%s)",
    redact_arg_pos: &[0],
};
/// Message metadata for `ErrWarnTooFewRecords`.
pub const ErrWarnTooFewRecords: ErrMessage = ErrMessage {
    raw: "Row %d doesn't contain data for all columns",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWarnTooManyRecords`.
pub const ErrWarnTooManyRecords: ErrMessage = ErrMessage {
    raw: "Row %d was truncated; it contained more data than there were input columns",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWarnNullToNotnull`.
pub const ErrWarnNullToNotnull: ErrMessage = ErrMessage {
    raw: "Column set to default value; NULL supplied to NOT NULL column '%s' at row %d",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWarnDataOutOfRange`.
pub const ErrWarnDataOutOfRange: ErrMessage = ErrMessage {
    raw: "Out of range value for column '%s' at row %d",
    redact_arg_pos: &[],
};
/// Message metadata for `WarnDataTruncated`.
pub const WarnDataTruncated: ErrMessage = ErrMessage {
    raw: "Data truncated for column '%s' at row %d",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWarnUsingOtherHandler`.
pub const ErrWarnUsingOtherHandler: ErrMessage = ErrMessage {
    raw: "Using storage engine %s for table '%s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCantAggregate2collations`.
pub const ErrCantAggregate2collations: ErrMessage = ErrMessage {
    raw: "Illegal mix of collations (%s,%s) and (%s,%s) for operation '%s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrDropUser`.
pub const ErrDropUser: ErrMessage = ErrMessage {
    raw: "Cannot drop one or more of the requested users",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrRevokeGrants`.
pub const ErrRevokeGrants: ErrMessage = ErrMessage {
    raw: "Can't revoke all privileges for one or more of the requested users",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCantAggregate3collations`.
pub const ErrCantAggregate3collations: ErrMessage = ErrMessage {
    raw: "Illegal mix of collations (%s,%s), (%s,%s), (%s,%s) for operation '%s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCantAggregateNcollations`.
pub const ErrCantAggregateNcollations: ErrMessage = ErrMessage {
    raw: "Illegal mix of collations for operation '%s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrVariableIsNotStruct`.
pub const ErrVariableIsNotStruct: ErrMessage = ErrMessage {
    raw: "Variable '%-.64s' is not a variable component (can't be used as XXXX.variableName)",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrUnknownCollation`.
pub const ErrUnknownCollation: ErrMessage = ErrMessage {
    raw: "Unknown collation: '%-.64s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrServerIsInSecureAuthMode`.
pub const ErrServerIsInSecureAuthMode: ErrMessage = ErrMessage {
    raw: "Server is running in --secure-auth mode, but '%s'@'%s' has a password in the old format; please change the password to the new format",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWarnFieldResolved`.
pub const ErrWarnFieldResolved: ErrMessage = ErrMessage {
    raw: "Field or reference '%-.192s%s%-.192s%s%-.192s' of SELECT #%d was resolved in SELECT #%d",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrUntilCondIgnored`.
pub const ErrUntilCondIgnored: ErrMessage = ErrMessage {
    raw: "SQL thread is not to be started so UNTIL options are ignored",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWrongNameForIndex`.
pub const ErrWrongNameForIndex: ErrMessage = ErrMessage {
    raw: "Incorrect index name '%-.100s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWrongNameForCatalog`.
pub const ErrWrongNameForCatalog: ErrMessage = ErrMessage {
    raw: "Incorrect catalog name '%-.100s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWarnQcResize`.
pub const ErrWarnQcResize: ErrMessage = ErrMessage {
    raw: "Query cache failed to set size %d; new query cache size is %d",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrBadFtColumn`.
pub const ErrBadFtColumn: ErrMessage = ErrMessage {
    raw: "Column '%-.192s' cannot be part of FULLTEXT index",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrUnknownKeyCache`.
pub const ErrUnknownKeyCache: ErrMessage = ErrMessage {
    raw: "Unknown key cache '%-.100s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWarnHostnameWontWork`.
pub const ErrWarnHostnameWontWork: ErrMessage = ErrMessage {
    raw: "MySQL is started in --skip-name-resolve mode; you must restart it without this switch for this grant to work",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrUnknownStorageEngine`.
pub const ErrUnknownStorageEngine: ErrMessage = ErrMessage {
    raw: "Unknown storage engine '%s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWarnDeprecatedSyntax`.
pub const ErrWarnDeprecatedSyntax: ErrMessage = ErrMessage {
    raw: "'%s' is deprecated and will be removed in a future release. Please use %s instead",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrNonUpdatableTable`.
pub const ErrNonUpdatableTable: ErrMessage = ErrMessage {
    raw: "The target table %-.100s of the %s is not updatable",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrFeatureDisabled`.
pub const ErrFeatureDisabled: ErrMessage = ErrMessage {
    raw: "The '%s' feature is disabled; you need MySQL built with '%s' to have it working",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrOptionPreventsStatement`.
pub const ErrOptionPreventsStatement: ErrMessage = ErrMessage {
    raw: "The MySQL server is running with the %s option so it cannot execute this statement",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrDuplicatedValueInType`.
pub const ErrDuplicatedValueInType: ErrMessage = ErrMessage {
    raw: "Column '%-.100s' has duplicated value '%-.64s' in %s",
    redact_arg_pos: &[1],
};
/// Message metadata for `ErrTruncatedWrongValue`.
pub const ErrTruncatedWrongValue: ErrMessage = ErrMessage {
    raw: "Truncated incorrect %-.64s value: '%-.128s'",
    redact_arg_pos: &[1],
};
/// Message metadata for `ErrTooMuchAutoTimestampCols`.
pub const ErrTooMuchAutoTimestampCols: ErrMessage = ErrMessage {
    raw: "Incorrect table definition; there can be only one TIMESTAMP column with CURRENTTIMESTAMP in DEFAULT or ON UPDATE clause",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInvalidOnUpdate`.
pub const ErrInvalidOnUpdate: ErrMessage = ErrMessage {
    raw: "Invalid ON UPDATE clause for '%-.192s' column",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrUnsupportedPs`.
pub const ErrUnsupportedPs: ErrMessage = ErrMessage {
    raw: "This command is not supported in the prepared statement protocol yet",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrGetErrmsg`.
pub const ErrGetErrmsg: ErrMessage = ErrMessage {
    raw: "Got error %d '%-.100s' from %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrGetTemporaryErrmsg`.
pub const ErrGetTemporaryErrmsg: ErrMessage = ErrMessage {
    raw: "Got temporary error %d '%-.100s' from %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrUnknownTimeZone`.
pub const ErrUnknownTimeZone: ErrMessage = ErrMessage {
    raw: "Unknown or incorrect time zone: '%-.64s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWarnInvalidTimestamp`.
pub const ErrWarnInvalidTimestamp: ErrMessage = ErrMessage {
    raw: "Invalid TIMESTAMP value in column '%s' at row %d",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInvalidCharacterString`.
pub const ErrInvalidCharacterString: ErrMessage = ErrMessage {
    raw: "Invalid %s character string: '%.64s'",
    redact_arg_pos: &[1],
};
/// Message metadata for `ErrWarnAllowedPacketOverflowed`.
pub const ErrWarnAllowedPacketOverflowed: ErrMessage = ErrMessage {
    raw: "Result of %s() was larger than max_allowed_packet (%d) - truncated",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrConflictingDeclarations`.
pub const ErrConflictingDeclarations: ErrMessage = ErrMessage {
    raw: "Conflicting declarations: '%s%s' and '%s%s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSpNoRecursiveCreate`.
pub const ErrSpNoRecursiveCreate: ErrMessage = ErrMessage {
    raw: "Can't create a %s from within another stored routine",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSpAlreadyExists`.
pub const ErrSpAlreadyExists: ErrMessage = ErrMessage {
    raw: "%s %s already exists",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSpDoesNotExist`.
pub const ErrSpDoesNotExist: ErrMessage = ErrMessage {
    raw: "%s %s does not exist",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSpDropFailed`.
pub const ErrSpDropFailed: ErrMessage = ErrMessage {
    raw: "Failed to DROP %s %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSpStoreFailed`.
pub const ErrSpStoreFailed: ErrMessage = ErrMessage {
    raw: "Failed to CREATE %s %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSpLilabelMismatch`.
pub const ErrSpLilabelMismatch: ErrMessage = ErrMessage {
    raw: "%s with no matching label: %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSpLabelRedefine`.
pub const ErrSpLabelRedefine: ErrMessage = ErrMessage {
    raw: "Redefining label %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSpLabelMismatch`.
pub const ErrSpLabelMismatch: ErrMessage = ErrMessage {
    raw: "End-label %s without match",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSpUninitVar`.
pub const ErrSpUninitVar: ErrMessage = ErrMessage {
    raw: "Referring to uninitialized variable %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSpBadselect`.
pub const ErrSpBadselect: ErrMessage = ErrMessage {
    raw: "PROCEDURE %s can't return a result set in the given context",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSpBadreturn`.
pub const ErrSpBadreturn: ErrMessage = ErrMessage {
    raw: "RETURN is only allowed in a FUNCTION",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSpBadstatement`.
pub const ErrSpBadstatement: ErrMessage = ErrMessage {
    raw: "%s is not allowed in stored procedures",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrUpdateLogDeprecatedIgnored`.
pub const ErrUpdateLogDeprecatedIgnored: ErrMessage = ErrMessage {
    raw: "The update log is deprecated and replaced by the binary log; SET SQLLOGUPDATE has been ignored.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrUpdateLogDeprecatedTranslated`.
pub const ErrUpdateLogDeprecatedTranslated: ErrMessage = ErrMessage {
    raw: "The update log is deprecated and replaced by the binary log; SET SQLLOGUPDATE has been translated to SET SQLLOGBIN.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrQueryInterrupted`.
pub const ErrQueryInterrupted: ErrMessage = ErrMessage {
    raw: "Query execution was interrupted",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSpWrongNoOfArgs`.
pub const ErrSpWrongNoOfArgs: ErrMessage = ErrMessage {
    raw: "Incorrect number of arguments for %s %s; expected %d, got %d",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSpCondMismatch`.
pub const ErrSpCondMismatch: ErrMessage = ErrMessage {
    raw: "Undefined CONDITION: %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSpNoreturn`.
pub const ErrSpNoreturn: ErrMessage = ErrMessage {
    raw: "No RETURN found in FUNCTION %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSpNoreturnend`.
pub const ErrSpNoreturnend: ErrMessage = ErrMessage {
    raw: "FUNCTION %s ended without RETURN",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSpBadCursorQuery`.
pub const ErrSpBadCursorQuery: ErrMessage = ErrMessage {
    raw: "Cursor statement must be a SELECT",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSpBadCursorSelect`.
pub const ErrSpBadCursorSelect: ErrMessage = ErrMessage {
    raw: "Cursor SELECT must not have INTO",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSpCursorMismatch`.
pub const ErrSpCursorMismatch: ErrMessage = ErrMessage {
    raw: "Undefined CURSOR: %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSpCursorAlreadyOpen`.
pub const ErrSpCursorAlreadyOpen: ErrMessage = ErrMessage {
    raw: "Cursor is already open",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSpCursorNotOpen`.
pub const ErrSpCursorNotOpen: ErrMessage = ErrMessage {
    raw: "Cursor is not open",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSpUndeclaredVar`.
pub const ErrSpUndeclaredVar: ErrMessage = ErrMessage {
    raw: "Undeclared variable: %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSpWrongNoOfFetchArgs`.
pub const ErrSpWrongNoOfFetchArgs: ErrMessage = ErrMessage {
    raw: "Incorrect number of FETCH variables",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSpFetchNoData`.
pub const ErrSpFetchNoData: ErrMessage = ErrMessage {
    raw: "No data - zero rows fetched, selected, or processed",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSpDupParam`.
pub const ErrSpDupParam: ErrMessage = ErrMessage {
    raw: "Duplicate parameter: %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSpDupVar`.
pub const ErrSpDupVar: ErrMessage = ErrMessage {
    raw: "Duplicate variable: %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSpDupCond`.
pub const ErrSpDupCond: ErrMessage = ErrMessage {
    raw: "Duplicate condition: %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSpDupCurs`.
pub const ErrSpDupCurs: ErrMessage = ErrMessage {
    raw: "Duplicate cursor: %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSpCantAlter`.
pub const ErrSpCantAlter: ErrMessage = ErrMessage {
    raw: "Failed to ALTER %s %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSpSubselectNyi`.
pub const ErrSpSubselectNyi: ErrMessage = ErrMessage {
    raw: "Subquery value not supported",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrStmtNotAllowedInSfOrTrg`.
pub const ErrStmtNotAllowedInSfOrTrg: ErrMessage = ErrMessage {
    raw: "%s is not allowed in stored function or trigger",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSpVarcondAfterCurshndlr`.
pub const ErrSpVarcondAfterCurshndlr: ErrMessage = ErrMessage {
    raw: "Variable or condition declaration after cursor or handler declaration",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSpCursorAfterHandler`.
pub const ErrSpCursorAfterHandler: ErrMessage = ErrMessage {
    raw: "Cursor declaration after handler declaration",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSpCaseNotFound`.
pub const ErrSpCaseNotFound: ErrMessage = ErrMessage {
    raw: "Case not found for CASE statement",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrFparserTooBigFile`.
pub const ErrFparserTooBigFile: ErrMessage = ErrMessage {
    raw: "Configuration file '%-.192s' is too big",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrFparserBadHeader`.
pub const ErrFparserBadHeader: ErrMessage = ErrMessage {
    raw: "Malformed file type header in file '%-.192s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrFparserEOFInComment`.
pub const ErrFparserEOFInComment: ErrMessage = ErrMessage {
    raw: "Unexpected end of file while parsing comment '%-.200s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrFparserErrorInParameter`.
pub const ErrFparserErrorInParameter: ErrMessage = ErrMessage {
    raw: "Error while parsing parameter '%-.192s' (line: '%-.192s')",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrFparserEOFInUnknownParameter`.
pub const ErrFparserEOFInUnknownParameter: ErrMessage = ErrMessage {
    raw: "Unexpected end of file while skipping unknown parameter '%-.192s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrViewNoExplain`.
pub const ErrViewNoExplain: ErrMessage = ErrMessage {
    raw: "EXPLAIN/SHOW can not be issued; lacking privileges for underlying table",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrFrmUnknownType`.
pub const ErrFrmUnknownType: ErrMessage = ErrMessage {
    raw: "File '%-.192s' has unknown type '%-.64s' in its header",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWrongObject`.
pub const ErrWrongObject: ErrMessage = ErrMessage {
    raw: "'%-.192s.%-.192s' is not %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrNonupdateableColumn`.
pub const ErrNonupdateableColumn: ErrMessage = ErrMessage {
    raw: "Column '%-.192s' is not updatable",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrViewSelectDerived`.
pub const ErrViewSelectDerived: ErrMessage = ErrMessage {
    raw: "View's SELECT contains a subquery in the FROM clause",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrViewSelectClause`.
pub const ErrViewSelectClause: ErrMessage = ErrMessage {
    raw: "View's SELECT contains a '%s' clause",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrViewSelectVariable`.
pub const ErrViewSelectVariable: ErrMessage = ErrMessage {
    raw: "View's SELECT contains a variable or parameter",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrViewSelectTmptable`.
pub const ErrViewSelectTmptable: ErrMessage = ErrMessage {
    raw: "View's SELECT refers to a temporary table '%-.192s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrViewWrongList`.
pub const ErrViewWrongList: ErrMessage = ErrMessage {
    raw: "In definition of view, derived table or common table expression, SELECT list and column names list have different column counts",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWarnViewMerge`.
pub const ErrWarnViewMerge: ErrMessage = ErrMessage {
    raw: "View merge algorithm can't be used here for now (assumed undefined algorithm)",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWarnViewWithoutKey`.
pub const ErrWarnViewWithoutKey: ErrMessage = ErrMessage {
    raw: "View being updated does not have complete key of underlying table in it",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrViewInvalid`.
pub const ErrViewInvalid: ErrMessage = ErrMessage {
    raw: "View '%-.192s.%-.192s' references invalid table(s) or column(s) or function(s) or definer/invoker of view lack rights to use them",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSpNoDropSp`.
pub const ErrSpNoDropSp: ErrMessage = ErrMessage {
    raw: "Can't drop or alter a %s from within another stored routine",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSpGotoInHndlr`.
pub const ErrSpGotoInHndlr: ErrMessage = ErrMessage {
    raw: "GOTO is not allowed in a stored procedure handler",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTrgAlreadyExists`.
pub const ErrTrgAlreadyExists: ErrMessage = ErrMessage {
    raw: "Trigger already exists",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTrgDoesNotExist`.
pub const ErrTrgDoesNotExist: ErrMessage = ErrMessage {
    raw: "Trigger does not exist",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTrgOnViewOrTempTable`.
pub const ErrTrgOnViewOrTempTable: ErrMessage = ErrMessage {
    raw: "Trigger's '%-.192s' is view or temporary table",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTrgCantChangeRow`.
pub const ErrTrgCantChangeRow: ErrMessage = ErrMessage {
    raw: "Updating of %s row is not allowed in %strigger",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTrgNoSuchRowInTrg`.
pub const ErrTrgNoSuchRowInTrg: ErrMessage = ErrMessage {
    raw: "There is no %s row in %s trigger",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrNoDefaultForField`.
pub const ErrNoDefaultForField: ErrMessage = ErrMessage {
    raw: "Field '%-.192s' doesn't have a default value",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrDivisionByZero`.
pub const ErrDivisionByZero: ErrMessage = ErrMessage {
    raw: "Division by 0",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTruncatedWrongValueForField`.
pub const ErrTruncatedWrongValueForField: ErrMessage = ErrMessage {
    raw: "Incorrect %-.32s value: '%-.128s' for column '%.192s' at row %d",
    redact_arg_pos: &[0, 1],
};
/// Message metadata for `ErrIllegalValueForType`.
pub const ErrIllegalValueForType: ErrMessage = ErrMessage {
    raw: "Illegal %s '%-.192s' value found during parsing",
    redact_arg_pos: &[1],
};
/// Message metadata for `ErrViewNonupdCheck`.
pub const ErrViewNonupdCheck: ErrMessage = ErrMessage {
    raw: "CHECK OPTION on non-updatable view '%-.192s.%-.192s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrViewCheckFailed`.
pub const ErrViewCheckFailed: ErrMessage = ErrMessage {
    raw: "CHECK OPTION failed '%-.192s.%-.192s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrProcaccessDenied`.
pub const ErrProcaccessDenied: ErrMessage = ErrMessage {
    raw: "%-.16s command denied to user '%-.48s'@'%-.255s' for routine '%-.192s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrRelayLogFail`.
pub const ErrRelayLogFail: ErrMessage = ErrMessage {
    raw: "Failed purging old relay logs: %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrPasswdLength`.
pub const ErrPasswdLength: ErrMessage = ErrMessage {
    raw: "Password hash should be a %d-digit hexadecimal number",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrUnknownTargetBinlog`.
pub const ErrUnknownTargetBinlog: ErrMessage = ErrMessage {
    raw: "Target log not found in binlog index",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrIoErrLogIndexRead`.
pub const ErrIoErrLogIndexRead: ErrMessage = ErrMessage {
    raw: "I/O error reading log index file",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrBinlogPurgeProhibited`.
pub const ErrBinlogPurgeProhibited: ErrMessage = ErrMessage {
    raw: "Server configuration does not permit binlog purge",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrFseekFail`.
pub const ErrFseekFail: ErrMessage = ErrMessage {
    raw: "Failed on fseek()",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrBinlogPurgeFatalErr`.
pub const ErrBinlogPurgeFatalErr: ErrMessage = ErrMessage {
    raw: "Fatal error during log purge",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrLogInUse`.
pub const ErrLogInUse: ErrMessage = ErrMessage {
    raw: "A purgeable log is in use, will not purge",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrLogPurgeUnknownErr`.
pub const ErrLogPurgeUnknownErr: ErrMessage = ErrMessage {
    raw: "Unknown error during log purge",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrRelayLogInit`.
pub const ErrRelayLogInit: ErrMessage = ErrMessage {
    raw: "Failed initializing relay log position: %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrNoBinaryLogging`.
pub const ErrNoBinaryLogging: ErrMessage = ErrMessage {
    raw: "You are not using binary logging",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrReservedSyntax`.
pub const ErrReservedSyntax: ErrMessage = ErrMessage {
    raw: "The '%-.64s' syntax is reserved for purposes internal to the MySQL server",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWsasFailed`.
pub const ErrWsasFailed: ErrMessage = ErrMessage {
    raw: "WSAStartup Failed",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrDiffGroupsProc`.
pub const ErrDiffGroupsProc: ErrMessage = ErrMessage {
    raw: "Can't handle procedures with different groups yet",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrNoGroupForProc`.
pub const ErrNoGroupForProc: ErrMessage = ErrMessage {
    raw: "Select must have a group with this procedure",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrOrderWithProc`.
pub const ErrOrderWithProc: ErrMessage = ErrMessage {
    raw: "Can't use ORDER clause with this procedure",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrLoggingProhibitChangingOf`.
pub const ErrLoggingProhibitChangingOf: ErrMessage = ErrMessage {
    raw: "Binary logging and replication forbid changing the global server %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrNoFileMapping`.
pub const ErrNoFileMapping: ErrMessage = ErrMessage {
    raw: "Can't map file: %-.200s, errno: %d",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWrongMagic`.
pub const ErrWrongMagic: ErrMessage = ErrMessage {
    raw: "Wrong magic in %-.64s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrPsManyParam`.
pub const ErrPsManyParam: ErrMessage = ErrMessage {
    raw: "Prepared statement contains too many placeholders",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrKeyPart0`.
pub const ErrKeyPart0: ErrMessage = ErrMessage {
    raw: "Key part '%-.192s' length cannot be 0",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrViewChecksum`.
pub const ErrViewChecksum: ErrMessage = ErrMessage {
    raw: "View text checksum failed",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrViewMultiupdate`.
pub const ErrViewMultiupdate: ErrMessage = ErrMessage {
    raw: "Can not modify more than one base table through a join view '%-.192s.%-.192s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrViewNoInsertFieldList`.
pub const ErrViewNoInsertFieldList: ErrMessage = ErrMessage {
    raw: "Can not insert into join view '%-.192s.%-.192s' without fields list",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrViewDeleteMergeView`.
pub const ErrViewDeleteMergeView: ErrMessage = ErrMessage {
    raw: "Can not delete from join view '%-.192s.%-.192s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCannotUser`.
pub const ErrCannotUser: ErrMessage = ErrMessage {
    raw: "Operation %s failed for %.256s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrGrantRole`.
pub const ErrGrantRole: ErrMessage = ErrMessage {
    raw: "Unknown authorization ID %.256s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrXaerNota`.
pub const ErrXaerNota: ErrMessage = ErrMessage {
    raw: "XAERNOTA: Unknown XID",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrXaerInval`.
pub const ErrXaerInval: ErrMessage = ErrMessage {
    raw: "XAERINVAL: Invalid arguments (or unsupported command)",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrXaerRmfail`.
pub const ErrXaerRmfail: ErrMessage = ErrMessage {
    raw:
        "XAERRMFAIL: The command cannot be executed when global transaction is in the  %.64s state",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrXaerOutside`.
pub const ErrXaerOutside: ErrMessage = ErrMessage {
    raw: "XAEROUTSIDE: Some work is done outside global transaction",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrXaerRmerr`.
pub const ErrXaerRmerr: ErrMessage = ErrMessage {
    raw: "XAERRMERR: Fatal error occurred in the transaction branch - check your data for consistency",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrXaRbrollback`.
pub const ErrXaRbrollback: ErrMessage = ErrMessage {
    raw: "XARBROLLBACK: Transaction branch was rolled back",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrNonexistingProcGrant`.
pub const ErrNonexistingProcGrant: ErrMessage = ErrMessage {
    raw: "There is no such grant defined for user '%-.48s' on host '%-.255s' on routine '%-.192s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrProcAutoGrantFail`.
pub const ErrProcAutoGrantFail: ErrMessage = ErrMessage {
    raw: "Failed to grant EXECUTE and ALTER ROUTINE privileges",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrProcAutoRevokeFail`.
pub const ErrProcAutoRevokeFail: ErrMessage = ErrMessage {
    raw: "Failed to revoke all privileges to dropped routine",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrDataTooLong`.
pub const ErrDataTooLong: ErrMessage = ErrMessage {
    raw: "Data too long for column '%s' at row %d",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSpBadSQLstate`.
pub const ErrSpBadSQLstate: ErrMessage = ErrMessage {
    raw: "Bad SQLSTATE: '%s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrStartup`.
pub const ErrStartup: ErrMessage = ErrMessage {
    raw: "%s: ready for connections.\nVersion: '%s'  socket: '%s'  port: %d  %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrLoadFromFixedSizeRowsToVar`.
pub const ErrLoadFromFixedSizeRowsToVar: ErrMessage = ErrMessage {
    raw: "Can't load value from file with fixed size rows to variable",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCantCreateUserWithGrant`.
pub const ErrCantCreateUserWithGrant: ErrMessage = ErrMessage {
    raw: "You are not allowed to create a user with GRANT",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWrongValueForType`.
pub const ErrWrongValueForType: ErrMessage = ErrMessage {
    raw: "Incorrect %-.32s value: '%-.128s' for function %-.32s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTableDefChanged`.
pub const ErrTableDefChanged: ErrMessage = ErrMessage {
    raw: "Table definition has changed, please retry transaction",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSpDupHandler`.
pub const ErrSpDupHandler: ErrMessage = ErrMessage {
    raw: "Duplicate handler declared in the same block",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSpNotVarArg`.
pub const ErrSpNotVarArg: ErrMessage = ErrMessage {
    raw: "OUT or INOUT argument %d for routine %s is not a variable or NEW pseudo-variable in BEFORE trigger",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSpNoRetset`.
pub const ErrSpNoRetset: ErrMessage = ErrMessage {
    raw: "Not allowed to return a result set from a %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCantCreateGeometryObject`.
pub const ErrCantCreateGeometryObject: ErrMessage = ErrMessage {
    raw: "Cannot get geometry object from data you send to the GEOMETRY field",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrFailedRoutineBreakBinlog`.
pub const ErrFailedRoutineBreakBinlog: ErrMessage = ErrMessage {
    raw: "A routine failed and has neither NO SQL nor READS SQL DATA in its declaration and binary logging is enabled; if non-transactional tables were updated, the binary log will miss their changes",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrBinlogUnsafeRoutine`.
pub const ErrBinlogUnsafeRoutine: ErrMessage = ErrMessage {
    raw: "This function has none of DETERMINISTIC, NO SQL, or READS SQL DATA in its declaration and binary logging is enabled (you *might* want to use the less safe logBinTrustFunctionCreators variable)",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrBinlogCreateRoutineNeedSuper`.
pub const ErrBinlogCreateRoutineNeedSuper: ErrMessage = ErrMessage {
    raw: "You do not have the SUPER privilege and binary logging is enabled (you *might* want to use the less safe logBinTrustFunctionCreators variable)",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrExecStmtWithOpenCursor`.
pub const ErrExecStmtWithOpenCursor: ErrMessage = ErrMessage {
    raw: "You can't execute a prepared statement which has an open cursor associated with it. Reset the statement to re-execute it.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrStmtHasNoOpenCursor`.
pub const ErrStmtHasNoOpenCursor: ErrMessage = ErrMessage {
    raw: "The statement (%d) has no open cursor.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCommitNotAllowedInSfOrTrg`.
pub const ErrCommitNotAllowedInSfOrTrg: ErrMessage = ErrMessage {
    raw: "Explicit or implicit commit is not allowed in stored function or trigger.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrNoDefaultForViewField`.
pub const ErrNoDefaultForViewField: ErrMessage = ErrMessage {
    raw: "Field of view '%-.192s.%-.192s' underlying table doesn't have a default value",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSpNoRecursion`.
pub const ErrSpNoRecursion: ErrMessage = ErrMessage {
    raw: "Recursive stored functions and triggers are not allowed.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTooBigScale`.
pub const ErrTooBigScale: ErrMessage = ErrMessage {
    raw: "Too big scale %d specified for column '%-.192s'. Maximum is %d.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTooBigPrecision`.
pub const ErrTooBigPrecision: ErrMessage = ErrMessage {
    raw: "Too-big precision %d specified for '%-.192s'. Maximum is %d.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrMBiggerThanD`.
pub const ErrMBiggerThanD: ErrMessage = ErrMessage {
    raw: "For float(M,D), double(M,D) or decimal(M,D), M must be >= D (column '%-.192s').",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWrongLockOfSystemTable`.
pub const ErrWrongLockOfSystemTable: ErrMessage = ErrMessage {
    raw: "You can't combine write-locking of system tables with other tables or lock types",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrConnectToForeignDataSource`.
pub const ErrConnectToForeignDataSource: ErrMessage = ErrMessage {
    raw: "Unable to connect to foreign data source: %.64s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrQueryOnForeignDataSource`.
pub const ErrQueryOnForeignDataSource: ErrMessage = ErrMessage {
    raw:
        "There was a problem processing the query on the foreign data source. Data source : %-.64s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrForeignDataSourceDoesntExist`.
pub const ErrForeignDataSourceDoesntExist: ErrMessage = ErrMessage {
    raw:
        "The foreign data source you are trying to reference does not exist. Data source :  %-.64s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrForeignDataStringInvalidCantCreate`.
pub const ErrForeignDataStringInvalidCantCreate: ErrMessage = ErrMessage {
    raw: "Can't create federated table. The data source connection string '%-.64s' is not in the correct format",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrForeignDataStringInvalid`.
pub const ErrForeignDataStringInvalid: ErrMessage = ErrMessage {
    raw: "The data source connection string '%-.64s' is not in the correct format",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCantCreateFederatedTable`.
pub const ErrCantCreateFederatedTable: ErrMessage = ErrMessage {
    raw: "Can't create federated table. Foreign data src :  %-.64s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTrgInWrongSchema`.
pub const ErrTrgInWrongSchema: ErrMessage = ErrMessage {
    raw: "Trigger in wrong schema",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrStackOverrunNeedMore`.
pub const ErrStackOverrunNeedMore: ErrMessage = ErrMessage {
    raw: "Thread stack overrun:  %d bytes used of a %d byte stack, and %d bytes needed.  Use 'mysqld --threadStack=#' to specify a bigger stack.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTooLongBody`.
pub const ErrTooLongBody: ErrMessage = ErrMessage {
    raw: "Routine body for '%-.100s' is too long",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWarnCantDropDefaultKeycache`.
pub const ErrWarnCantDropDefaultKeycache: ErrMessage = ErrMessage {
    raw: "Cannot drop default keycache",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTooBigDisplaywidth`.
pub const ErrTooBigDisplaywidth: ErrMessage = ErrMessage {
    raw: "Display width out of range for column '%-.192s' (max = %d)",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrXaerDupid`.
pub const ErrXaerDupid: ErrMessage = ErrMessage {
    raw: "XAERDUPID: The XID already exists",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrDatetimeFunctionOverflow`.
pub const ErrDatetimeFunctionOverflow: ErrMessage = ErrMessage {
    raw: "Datetime function: %-.32s field overflow",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCantUpdateUsedTableInSfOrTrg`.
pub const ErrCantUpdateUsedTableInSfOrTrg: ErrMessage = ErrMessage {
    raw: "Can't update table '%-.192s' in stored function/trigger because it is already used by statement which invoked this stored function/trigger.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrViewPreventUpdate`.
pub const ErrViewPreventUpdate: ErrMessage = ErrMessage {
    raw: "The definition of table '%-.192s' prevents operation %.192s on table '%-.192s'.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrPsNoRecursion`.
pub const ErrPsNoRecursion: ErrMessage = ErrMessage {
    raw: "The prepared statement contains a stored routine call that refers to that same statement. It's not allowed to execute a prepared statement in such a recursive manner",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSpCantSetAutocommit`.
pub const ErrSpCantSetAutocommit: ErrMessage = ErrMessage {
    raw: "Not allowed to set autocommit from a stored function or trigger",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrMalformedDefiner`.
pub const ErrMalformedDefiner: ErrMessage = ErrMessage {
    raw: "Definer is not fully qualified",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrViewFrmNoUser`.
pub const ErrViewFrmNoUser: ErrMessage = ErrMessage {
    raw: "View '%-.192s'.'%-.192s' has no definer information (old table format). Current user is used as definer. Please recreate the view!",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrViewOtherUser`.
pub const ErrViewOtherUser: ErrMessage = ErrMessage {
    raw: "You need the SUPER privilege for creation view with '%-.192s'@'%-.255s' definer",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrNoSuchUser`.
pub const ErrNoSuchUser: ErrMessage = ErrMessage {
    raw: "The user specified as a definer ('%-.64s'@'%-.255s') does not exist",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrForbidSchemaChange`.
pub const ErrForbidSchemaChange: ErrMessage = ErrMessage {
    raw: "Changing schema from '%-.192s' to '%-.192s' is not allowed.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrRowIsReferenced2`.
pub const ErrRowIsReferenced2: ErrMessage = ErrMessage {
    raw: "Cannot delete or update a parent row: a foreign key constraint fails (%.192s)",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrNoReferencedRow2`.
pub const ErrNoReferencedRow2: ErrMessage = ErrMessage {
    raw: "Cannot add or update a child row: a foreign key constraint fails (%.192s)",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSpBadVarShadow`.
pub const ErrSpBadVarShadow: ErrMessage = ErrMessage {
    raw: "Variable '%-.64s' must be quoted with `...`, or renamed",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTrgNoDefiner`.
pub const ErrTrgNoDefiner: ErrMessage = ErrMessage {
    raw: "No definer attribute for trigger '%-.192s'.'%-.192s'. The trigger will be activated under the authorization of the invoker, which may have insufficient privileges. Please recreate the trigger.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrOldFileFormat`.
pub const ErrOldFileFormat: ErrMessage = ErrMessage {
    raw: "'%-.192s' has an old format, you should re-create the '%s' object(s)",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSpRecursionLimit`.
pub const ErrSpRecursionLimit: ErrMessage = ErrMessage {
    raw: "Recursive limit %d (as set by the maxSpRecursionDepth variable) was exceeded for routine %.192s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSpProcTableCorrupt`.
pub const ErrSpProcTableCorrupt: ErrMessage = ErrMessage {
    raw: "Failed to load routine %-.192s. The table mysql.proc is missing, corrupt, or contains bad data (internal code %d)",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSpWrongName`.
pub const ErrSpWrongName: ErrMessage = ErrMessage {
    raw: "Incorrect routine name '%-.192s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTableNeedsUpgrade`.
pub const ErrTableNeedsUpgrade: ErrMessage = ErrMessage {
    raw: "Table upgrade required. Please do \"REPAIR TABLE `%-.32s`\"",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSpNoAggregate`.
pub const ErrSpNoAggregate: ErrMessage = ErrMessage {
    raw: "AGGREGATE is not supported for stored functions",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrMaxPreparedStmtCountReached`.
pub const ErrMaxPreparedStmtCountReached: ErrMessage = ErrMessage {
    raw: "Can't create more than maxPreparedStmtCount statements (current value: %d)",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrViewRecursive`.
pub const ErrViewRecursive: ErrMessage = ErrMessage {
    raw: "`%-.192s`.`%-.192s` contains view recursion",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrNonGroupingFieldUsed`.
pub const ErrNonGroupingFieldUsed: ErrMessage = ErrMessage {
    raw: "Non-grouping field '%-.192s' is used in %-.64s clause",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTableCantHandleSpkeys`.
pub const ErrTableCantHandleSpkeys: ErrMessage = ErrMessage {
    raw: "The used table type doesn't support SPATIAL indexes",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrNoTriggersOnSystemSchema`.
pub const ErrNoTriggersOnSystemSchema: ErrMessage = ErrMessage {
    raw: "Triggers can not be created on system tables",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrRemovedSpaces`.
pub const ErrRemovedSpaces: ErrMessage = ErrMessage {
    raw: "Leading spaces are removed from name '%s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrAutoincReadFailed`.
pub const ErrAutoincReadFailed: ErrMessage = ErrMessage {
    raw: "Failed to read auto-increment value from storage engine",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrUsername`.
pub const ErrUsername: ErrMessage = ErrMessage {
    raw: "user name",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrHostname`.
pub const ErrHostname: ErrMessage = ErrMessage {
    raw: "host name",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWrongStringLength`.
pub const ErrWrongStringLength: ErrMessage = ErrMessage {
    raw: "String '%-.70s' is too long for %s (should be no longer than %d)",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrNonInsertableTable`.
pub const ErrNonInsertableTable: ErrMessage = ErrMessage {
    raw: "The target table %-.100s of the %s is not insertable-into",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrAdminWrongMrgTable`.
pub const ErrAdminWrongMrgTable: ErrMessage = ErrMessage {
    raw: "Table '%-.64s' is differently defined or of non-MyISAM type or doesn't exist",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTooHighLevelOfNestingForSelect`.
pub const ErrTooHighLevelOfNestingForSelect: ErrMessage = ErrMessage {
    raw: "Too high level of nesting for select",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrNameBecomesEmpty`.
pub const ErrNameBecomesEmpty: ErrMessage = ErrMessage {
    raw: "Name '%-.64s' has become ''",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrAmbiguousFieldTerm`.
pub const ErrAmbiguousFieldTerm: ErrMessage = ErrMessage {
    raw: "First character of the FIELDS TERMINATED string is ambiguous; please use non-optional and non-empty FIELDS ENCLOSED BY",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrForeignServerExists`.
pub const ErrForeignServerExists: ErrMessage = ErrMessage {
    raw: "The foreign server, %s, you are trying to create already exists.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrForeignServerDoesntExist`.
pub const ErrForeignServerDoesntExist: ErrMessage = ErrMessage {
    raw:
        "The foreign server name you are trying to reference does not exist. Data source :  %-.64s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrIllegalHaCreateOption`.
pub const ErrIllegalHaCreateOption: ErrMessage = ErrMessage {
    raw: "Table storage engine '%-.64s' does not support the create option '%.64s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrPartitionRequiresValues`.
pub const ErrPartitionRequiresValues: ErrMessage = ErrMessage {
    raw: "Syntax : %-.64s PARTITIONING requires definition of VALUES %-.64s for each partition",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrPartitionWrongValues`.
pub const ErrPartitionWrongValues: ErrMessage = ErrMessage {
    raw: "Only %-.64s PARTITIONING can use VALUES %-.64s in partition definition",
    redact_arg_pos: &[1],
};
/// Message metadata for `ErrPartitionMaxvalue`.
pub const ErrPartitionMaxvalue: ErrMessage = ErrMessage {
    raw: "MAXVALUE can only be used in last partition definition",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrPartitionSubpartition`.
pub const ErrPartitionSubpartition: ErrMessage = ErrMessage {
    raw: "Subpartitions can only be hash partitions and by key",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrPartitionSubpartMix`.
pub const ErrPartitionSubpartMix: ErrMessage = ErrMessage {
    raw: "Must define subpartitions on all partitions if on one partition",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrPartitionWrongNoPart`.
pub const ErrPartitionWrongNoPart: ErrMessage = ErrMessage {
    raw: "Wrong number of partitions defined, mismatch with previous setting",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrPartitionWrongNoSubpart`.
pub const ErrPartitionWrongNoSubpart: ErrMessage = ErrMessage {
    raw: "Wrong number of subpartitions defined, mismatch with previous setting",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWrongExprInPartitionFunc`.
pub const ErrWrongExprInPartitionFunc: ErrMessage = ErrMessage {
    raw: "Constant, random or timezone-dependent expressions in (sub)partitioning function are not allowed",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrNoConstExprInRangeOrList`.
pub const ErrNoConstExprInRangeOrList: ErrMessage = ErrMessage {
    raw: "Expression in RANGE/LIST VALUES must be constant",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrFieldNotFoundPart`.
pub const ErrFieldNotFoundPart: ErrMessage = ErrMessage {
    raw: "Field in list of fields for partition function not found in table",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrListOfFieldsOnlyInHash`.
pub const ErrListOfFieldsOnlyInHash: ErrMessage = ErrMessage {
    raw: "List of fields is only allowed in KEY partitions",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInconsistentPartitionInfo`.
pub const ErrInconsistentPartitionInfo: ErrMessage = ErrMessage {
    raw: "The partition info in the frm file is not consistent with what can be written into the frm file",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrPartitionFuncNotAllowed`.
pub const ErrPartitionFuncNotAllowed: ErrMessage = ErrMessage {
    raw: "The %-.192s function returns the wrong type",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrPartitionsMustBeDefined`.
pub const ErrPartitionsMustBeDefined: ErrMessage = ErrMessage {
    raw: "For %-.64s partitions each partition must be defined",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrRangeNotIncreasing`.
pub const ErrRangeNotIncreasing: ErrMessage = ErrMessage {
    raw: "VALUES LESS THAN value must be strictly increasing for each partition",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInconsistentTypeOfFunctions`.
pub const ErrInconsistentTypeOfFunctions: ErrMessage = ErrMessage {
    raw: "VALUES value must be of same type as partition function",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrMultipleDefConstInListPart`.
pub const ErrMultipleDefConstInListPart: ErrMessage = ErrMessage {
    raw: "Multiple definition of same constant in list partitioning",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrPartitionEntry`.
pub const ErrPartitionEntry: ErrMessage = ErrMessage {
    raw: "Partitioning can not be used stand-alone in query",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrMixHandler`.
pub const ErrMixHandler: ErrMessage = ErrMessage {
    raw: "The mix of handlers in the partitions is not allowed in this version of MySQL",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrPartitionNotDefined`.
pub const ErrPartitionNotDefined: ErrMessage = ErrMessage {
    raw: "For the partitioned engine it is necessary to define all %-.64s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTooManyPartitions`.
pub const ErrTooManyPartitions: ErrMessage = ErrMessage {
    raw: "Too many partitions (including subpartitions) were defined",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSubpartition`.
pub const ErrSubpartition: ErrMessage = ErrMessage {
    raw: "It is only possible to mix RANGE/LIST partitioning with HASH/KEY partitioning for subpartitioning",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCantCreateHandlerFile`.
pub const ErrCantCreateHandlerFile: ErrMessage = ErrMessage {
    raw: "Failed to create specific handler file",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrBlobFieldInPartFunc`.
pub const ErrBlobFieldInPartFunc: ErrMessage = ErrMessage {
    raw: "A BLOB field is not allowed in partition function",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrUniqueKeyNeedAllFieldsInPf`.
pub const ErrUniqueKeyNeedAllFieldsInPf: ErrMessage = ErrMessage {
    raw: "A %-.192s must include all columns in the table's partitioning function",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrNoParts`.
pub const ErrNoParts: ErrMessage = ErrMessage {
    raw: "Number of %-.64s = 0 is not an allowed value",
    redact_arg_pos: &[0],
};
/// Message metadata for `ErrPartitionMgmtOnNonpartitioned`.
pub const ErrPartitionMgmtOnNonpartitioned: ErrMessage = ErrMessage {
    raw: "Partition management on a not partitioned table is not possible",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrForeignKeyOnPartitioned`.
pub const ErrForeignKeyOnPartitioned: ErrMessage = ErrMessage {
    raw: "Foreign key clause is not yet supported in conjunction with partitioning",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrDropPartitionNonExistent`.
pub const ErrDropPartitionNonExistent: ErrMessage = ErrMessage {
    raw: "Error in list of partitions to %-.64s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrDropLastPartition`.
pub const ErrDropLastPartition: ErrMessage = ErrMessage {
    raw: "Cannot remove all partitions, use DROP TABLE instead",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCoalesceOnlyOnHashPartition`.
pub const ErrCoalesceOnlyOnHashPartition: ErrMessage = ErrMessage {
    raw: "COALESCE PARTITION can only be used on HASH/KEY partitions",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrReorgHashOnlyOnSameNo`.
pub const ErrReorgHashOnlyOnSameNo: ErrMessage = ErrMessage {
    raw:
        "REORGANIZE PARTITION can only be used to reorganize partitions not to change their numbers",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrReorgNoParam`.
pub const ErrReorgNoParam: ErrMessage = ErrMessage {
    raw: "REORGANIZE PARTITION without parameters can only be used on auto-partitioned tables using HASH PARTITIONs",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrOnlyOnRangeListPartition`.
pub const ErrOnlyOnRangeListPartition: ErrMessage = ErrMessage {
    raw: "%-.64s PARTITION can only be used on RANGE/LIST partitions",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrAddPartitionSubpart`.
pub const ErrAddPartitionSubpart: ErrMessage = ErrMessage {
    raw: "Trying to Add partition(s) with wrong number of subpartitions",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrAddPartitionNoNewPartition`.
pub const ErrAddPartitionNoNewPartition: ErrMessage = ErrMessage {
    raw: "At least one partition must be added",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCoalescePartitionNoPartition`.
pub const ErrCoalescePartitionNoPartition: ErrMessage = ErrMessage {
    raw: "At least one partition must be coalesced",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrReorgPartitionNotExist`.
pub const ErrReorgPartitionNotExist: ErrMessage = ErrMessage {
    raw: "More partitions to reorganize than there are partitions",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSameNamePartition`.
pub const ErrSameNamePartition: ErrMessage = ErrMessage {
    raw: "Duplicate partition name %-.192s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrNoBinlog`.
pub const ErrNoBinlog: ErrMessage = ErrMessage {
    raw: "It is not allowed to shut off binlog on this command",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrConsecutiveReorgPartitions`.
pub const ErrConsecutiveReorgPartitions: ErrMessage = ErrMessage {
    raw: "When reorganizing a set of partitions they must be in consecutive order",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrReorgOutsideRange`.
pub const ErrReorgOutsideRange: ErrMessage = ErrMessage {
    raw: "Reorganize of range partitions cannot change total ranges except for last partition where it can extend the range",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrPartitionFunctionFailure`.
pub const ErrPartitionFunctionFailure: ErrMessage = ErrMessage {
    raw: "Partition function not supported in this version for this handler",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrPartState`.
pub const ErrPartState: ErrMessage = ErrMessage {
    raw: "Partition state cannot be defined from CREATE/ALTER TABLE",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrLimitedPartRange`.
pub const ErrLimitedPartRange: ErrMessage = ErrMessage {
    raw: "The %-.64s handler only supports 32 bit integers in VALUES",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrPluginIsNotLoaded`.
pub const ErrPluginIsNotLoaded: ErrMessage = ErrMessage {
    raw: "Plugin '%-.192s' is not loaded",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWrongValue`.
pub const ErrWrongValue: ErrMessage = ErrMessage {
    raw: "Incorrect %-.32s value: '%-.128s'",
    redact_arg_pos: &[1],
};
/// Message metadata for `ErrNoPartitionForGivenValue`.
pub const ErrNoPartitionForGivenValue: ErrMessage = ErrMessage {
    raw: "Table has no partition for value %-.64s",
    redact_arg_pos: &[0],
};
/// Message metadata for `ErrFilegroupOptionOnlyOnce`.
pub const ErrFilegroupOptionOnlyOnce: ErrMessage = ErrMessage {
    raw: "It is not allowed to specify %s more than once",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCreateFilegroupFailed`.
pub const ErrCreateFilegroupFailed: ErrMessage = ErrMessage {
    raw: "Failed to create %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrDropFilegroupFailed`.
pub const ErrDropFilegroupFailed: ErrMessage = ErrMessage {
    raw: "Failed to drop %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTablespaceAutoExtend`.
pub const ErrTablespaceAutoExtend: ErrMessage = ErrMessage {
    raw: "The handler doesn't support autoextend of tablespaces",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWrongSizeNumber`.
pub const ErrWrongSizeNumber: ErrMessage = ErrMessage {
    raw: "A size parameter was incorrectly specified, either number or on the form 10M",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSizeOverflow`.
pub const ErrSizeOverflow: ErrMessage = ErrMessage {
    raw: "The size number was correct but we don't allow the digit part to be more than 2 billion",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrAlterFilegroupFailed`.
pub const ErrAlterFilegroupFailed: ErrMessage = ErrMessage {
    raw: "Failed to alter: %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrBinlogRowLoggingFailed`.
pub const ErrBinlogRowLoggingFailed: ErrMessage = ErrMessage {
    raw: "Writing one row to the row-based binary log failed",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrEventAlreadyExists`.
pub const ErrEventAlreadyExists: ErrMessage = ErrMessage {
    raw: "Event '%-.192s' already exists",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrEventStoreFailed`.
pub const ErrEventStoreFailed: ErrMessage = ErrMessage {
    raw: "Failed to store event %s. Error code %d from storage engine.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrEventDoesNotExist`.
pub const ErrEventDoesNotExist: ErrMessage = ErrMessage {
    raw: "Unknown event '%-.192s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrEventCantAlter`.
pub const ErrEventCantAlter: ErrMessage = ErrMessage {
    raw: "Failed to alter event '%-.192s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrEventDropFailed`.
pub const ErrEventDropFailed: ErrMessage = ErrMessage {
    raw: "Failed to drop %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrEventIntervalNotPositiveOrTooBig`.
pub const ErrEventIntervalNotPositiveOrTooBig: ErrMessage = ErrMessage {
    raw: "INTERVAL is either not positive or too big",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrEventEndsBeforeStarts`.
pub const ErrEventEndsBeforeStarts: ErrMessage = ErrMessage {
    raw: "ENDS is either invalid or before STARTS",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrEventExecTimeInThePast`.
pub const ErrEventExecTimeInThePast: ErrMessage = ErrMessage {
    raw: "Event execution time is in the past. Event has been disabled",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrEventOpenTableFailed`.
pub const ErrEventOpenTableFailed: ErrMessage = ErrMessage {
    raw: "Failed to open mysql.event",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrEventNeitherMExprNorMAt`.
pub const ErrEventNeitherMExprNorMAt: ErrMessage = ErrMessage {
    raw: "No datetime expression provided",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrObsoleteColCountDoesntMatchCorrupted`.
pub const ErrObsoleteColCountDoesntMatchCorrupted: ErrMessage = ErrMessage {
    raw:
        "Column count of mysql.%s is wrong. Expected %d, found %d. The table is probably corrupted",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrObsoleteCannotLoadFromTable`.
pub const ErrObsoleteCannotLoadFromTable: ErrMessage = ErrMessage {
    raw: "Cannot load from mysql.%s. The table is probably corrupted",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrEventCannotDelete`.
pub const ErrEventCannotDelete: ErrMessage = ErrMessage {
    raw: "Failed to delete the event from mysql.event",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrEventCompile`.
pub const ErrEventCompile: ErrMessage = ErrMessage {
    raw: "Error during compilation of event's body",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrEventSameName`.
pub const ErrEventSameName: ErrMessage = ErrMessage {
    raw: "Same old and new event name",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrEventDataTooLong`.
pub const ErrEventDataTooLong: ErrMessage = ErrMessage {
    raw: "Data for column '%s' too long",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrDropIndexNeededInForeignKey`.
pub const ErrDropIndexNeededInForeignKey: ErrMessage = ErrMessage {
    raw: "Cannot drop index '%-.192s': needed in a foreign key constraint",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWarnDeprecatedSyntaxWithVer`.
pub const ErrWarnDeprecatedSyntaxWithVer: ErrMessage = ErrMessage {
    raw: "The syntax '%s' is deprecated and will be removed in MySQL %s. Please use %s instead",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCantWriteLockLogTable`.
pub const ErrCantWriteLockLogTable: ErrMessage = ErrMessage {
    raw: "You can't write-lock a log table. Only read access is possible",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCantLockLogTable`.
pub const ErrCantLockLogTable: ErrMessage = ErrMessage {
    raw: "You can't use locks with log tables.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrForeignDuplicateKeyOldUnused`.
pub const ErrForeignDuplicateKeyOldUnused: ErrMessage = ErrMessage {
    raw: "Upholding foreign key constraints for table '%.192s', entry '%-.192s', key %d would lead to a duplicate entry",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrColCountDoesntMatchPleaseUpdate`.
pub const ErrColCountDoesntMatchPleaseUpdate: ErrMessage = ErrMessage {
    raw: "Column count of mysql.%s is wrong. Expected %d, found %d. Created with MySQL %d, now running %d. Please use mysqlUpgrade to fix this error.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTempTablePreventsSwitchOutOfRbr`.
pub const ErrTempTablePreventsSwitchOutOfRbr: ErrMessage = ErrMessage {
    raw: "Cannot switch out of the row-based binary log format when the session has open temporary tables",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrStoredFunctionPreventsSwitchBinlogFormat`.
pub const ErrStoredFunctionPreventsSwitchBinlogFormat: ErrMessage = ErrMessage {
    raw: "Cannot change the binary logging format inside a stored function or trigger",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrNdbCantSwitchBinlogFormat`.
pub const ErrNdbCantSwitchBinlogFormat: ErrMessage = ErrMessage {
    raw: "The NDB cluster engine does not support changing the binlog format on the fly yet",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrPartitionNoTemporary`.
pub const ErrPartitionNoTemporary: ErrMessage = ErrMessage {
    raw: "Cannot create temporary table with partitions",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrPartitionConstDomain`.
pub const ErrPartitionConstDomain: ErrMessage = ErrMessage {
    raw: "Partition constant is out of partition function domain",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrPartitionFunctionIsNotAllowed`.
pub const ErrPartitionFunctionIsNotAllowed: ErrMessage = ErrMessage {
    raw: "This partition function is not allowed",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrDdlLog`.
pub const ErrDdlLog: ErrMessage = ErrMessage {
    raw: "Error in DDL log",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrNullInValuesLessThan`.
pub const ErrNullInValuesLessThan: ErrMessage = ErrMessage {
    raw: "Not allowed to use NULL value in VALUES LESS THAN",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWrongPartitionName`.
pub const ErrWrongPartitionName: ErrMessage = ErrMessage {
    raw: "Incorrect partition name",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCantChangeTxCharacteristics`.
pub const ErrCantChangeTxCharacteristics: ErrMessage = ErrMessage {
    raw: "Transaction characteristics can't be changed while a transaction is in progress",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrDupEntryAutoincrementCase`.
pub const ErrDupEntryAutoincrementCase: ErrMessage = ErrMessage {
    raw: "ALTER TABLE causes autoIncrement resequencing, resulting in duplicate entry '%-.192s' for key '%-.192s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrEventModifyQueue`.
pub const ErrEventModifyQueue: ErrMessage = ErrMessage {
    raw: "Internal scheduler error %d",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrEventSetVar`.
pub const ErrEventSetVar: ErrMessage = ErrMessage {
    raw: "Error during starting/stopping of the scheduler. Error code %d",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrPartitionMerge`.
pub const ErrPartitionMerge: ErrMessage = ErrMessage {
    raw: "Engine cannot be used in partitioned tables",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCantActivateLog`.
pub const ErrCantActivateLog: ErrMessage = ErrMessage {
    raw: "Cannot activate '%-.64s' log",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrRbrNotAvailable`.
pub const ErrRbrNotAvailable: ErrMessage = ErrMessage {
    raw: "The server was not built with row-based replication",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrBase64Decode`.
pub const ErrBase64Decode: ErrMessage = ErrMessage {
    raw: "Decoding of base64 string failed",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrEventRecursionForbidden`.
pub const ErrEventRecursionForbidden: ErrMessage = ErrMessage {
    raw: "Recursion of EVENT DDL statements is forbidden when body is present",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrEventsDB`.
pub const ErrEventsDB: ErrMessage = ErrMessage {
    raw: "Cannot proceed because system tables used by Event Scheduler were found damaged at server start",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrOnlyIntegersAllowed`.
pub const ErrOnlyIntegersAllowed: ErrMessage = ErrMessage {
    raw: "Only integers allowed as number here",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrUnsuportedLogEngine`.
pub const ErrUnsuportedLogEngine: ErrMessage = ErrMessage {
    raw: "This storage engine cannot be used for log tables\"",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrBadLogStatement`.
pub const ErrBadLogStatement: ErrMessage = ErrMessage {
    raw: "You cannot '%s' a log table if logging is enabled",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCantRenameLogTable`.
pub const ErrCantRenameLogTable: ErrMessage = ErrMessage {
    raw: "Cannot rename '%s'. When logging enabled, rename to/from log table must rename two tables: the log table to an archive table and another table back to '%s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWrongParamcountToNativeFct`.
pub const ErrWrongParamcountToNativeFct: ErrMessage = ErrMessage {
    raw: "Incorrect parameter count in the call to native function '%-.192s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWrongParametersToNativeFct`.
pub const ErrWrongParametersToNativeFct: ErrMessage = ErrMessage {
    raw: "Incorrect parameters in the call to native function '%-.192s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWrongParametersToStoredFct`.
pub const ErrWrongParametersToStoredFct: ErrMessage = ErrMessage {
    raw: "Incorrect parameters in the call to stored function '%-.192s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrNativeFctNameCollision`.
pub const ErrNativeFctNameCollision: ErrMessage = ErrMessage {
    raw: "This function '%-.192s' has the same name as a native function",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrDupEntryWithKeyName`.
pub const ErrDupEntryWithKeyName: ErrMessage = ErrMessage {
    raw: "Duplicate entry '%-.64s' for key '%-.192s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrBinlogPurgeEmFile`.
pub const ErrBinlogPurgeEmFile: ErrMessage = ErrMessage {
    raw: "Too many files opened, please execute the command again",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrEventCannotCreateInThePast`.
pub const ErrEventCannotCreateInThePast: ErrMessage = ErrMessage {
    raw: "Event execution time is in the past and ON COMPLETION NOT PRESERVE is set. The event was dropped immediately after creation.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrEventCannotAlterInThePast`.
pub const ErrEventCannotAlterInThePast: ErrMessage = ErrMessage {
    raw: "Event execution time is in the past and ON COMPLETION NOT PRESERVE is set. The event was not changed. Specify a time in the future.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrNoPartitionForGivenValueSilent`.
pub const ErrNoPartitionForGivenValueSilent: ErrMessage = ErrMessage {
    raw: "Table has no partition for some existing values",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrBinlogUnsafeStatement`.
pub const ErrBinlogUnsafeStatement: ErrMessage = ErrMessage {
    raw: "Unsafe statement written to the binary log using statement format since BINLOGFORMAT = STATEMENT. %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrBinlogLoggingImpossible`.
pub const ErrBinlogLoggingImpossible: ErrMessage = ErrMessage {
    raw: "Binary logging not possible. Message: %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrViewNoCreationCtx`.
pub const ErrViewNoCreationCtx: ErrMessage = ErrMessage {
    raw: "View `%-.64s`.`%-.64s` has no creation context",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrViewInvalidCreationCtx`.
pub const ErrViewInvalidCreationCtx: ErrMessage = ErrMessage {
    raw: "Creation context of view `%-.64s`.`%-.64s' is invalid",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSrInvalidCreationCtx`.
pub const ErrSrInvalidCreationCtx: ErrMessage = ErrMessage {
    raw: "Creation context of stored routine `%-.64s`.`%-.64s` is invalid",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTrgCorruptedFile`.
pub const ErrTrgCorruptedFile: ErrMessage = ErrMessage {
    raw: "Corrupted TRG file for table `%-.64s`.`%-.64s`",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTrgNoCreationCtx`.
pub const ErrTrgNoCreationCtx: ErrMessage = ErrMessage {
    raw: "Triggers for table `%-.64s`.`%-.64s` have no creation context",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTrgInvalidCreationCtx`.
pub const ErrTrgInvalidCreationCtx: ErrMessage = ErrMessage {
    raw: "Trigger creation context of table `%-.64s`.`%-.64s` is invalid",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrEventInvalidCreationCtx`.
pub const ErrEventInvalidCreationCtx: ErrMessage = ErrMessage {
    raw: "Creation context of event `%-.64s`.`%-.64s` is invalid",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTrgCantOpenTable`.
pub const ErrTrgCantOpenTable: ErrMessage = ErrMessage {
    raw: "Cannot open table for trigger `%-.64s`.`%-.64s`",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCantCreateSroutine`.
pub const ErrCantCreateSroutine: ErrMessage = ErrMessage {
    raw: "Cannot create stored routine `%-.64s`. Check warnings",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrNoFormatDescriptionEventBeforeBinlogStatement`.
pub const ErrNoFormatDescriptionEventBeforeBinlogStatement: ErrMessage = ErrMessage {
    raw: "The BINLOG statement of type `%s` was not preceded by a format description BINLOG statement.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrLoadDataInvalidColumn`.
pub const ErrLoadDataInvalidColumn: ErrMessage = ErrMessage {
    raw: "Invalid column reference (%-.64s) in LOAD DATA",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrLogPurgeNoFile`.
pub const ErrLogPurgeNoFile: ErrMessage = ErrMessage {
    raw: "Being purged log %s was not found",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrXaRbtimeout`.
pub const ErrXaRbtimeout: ErrMessage = ErrMessage {
    raw: "XARBTIMEOUT: Transaction branch was rolled back: took too long",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrXaRbdeadlock`.
pub const ErrXaRbdeadlock: ErrMessage = ErrMessage {
    raw: "XARBDEADLOCK: Transaction branch was rolled back: deadlock was detected",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrNeedReprepare`.
pub const ErrNeedReprepare: ErrMessage = ErrMessage {
    raw: "Prepared statement needs to be re-prepared",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrDelayedNotSupported`.
pub const ErrDelayedNotSupported: ErrMessage = ErrMessage {
    raw: "DELAYED option not supported for table '%-.192s'",
    redact_arg_pos: &[],
};
/// Message metadata for `WarnOptionIgnored`.
pub const WarnOptionIgnored: ErrMessage = ErrMessage {
    raw: "<%-.64s> option ignored",
    redact_arg_pos: &[],
};
/// Message metadata for `WarnPluginDeleteBuiltin`.
pub const WarnPluginDeleteBuiltin: ErrMessage = ErrMessage {
    raw: "Built-in plugins cannot be deleted",
    redact_arg_pos: &[],
};
/// Message metadata for `WarnPluginBusy`.
pub const WarnPluginBusy: ErrMessage = ErrMessage {
    raw: "Plugin is busy and will be uninstalled on shutdown",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrVariableIsReadonly`.
pub const ErrVariableIsReadonly: ErrMessage = ErrMessage {
    raw: "%s variable '%s' is read-only. Use SET %s to assign the value",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWarnEngineTransactionRollback`.
pub const ErrWarnEngineTransactionRollback: ErrMessage = ErrMessage {
    raw: "Storage engine %s does not support rollback for this statement. Transaction rolled back and must be restarted",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrNdbReplicationSchema`.
pub const ErrNdbReplicationSchema: ErrMessage = ErrMessage {
    raw: "Bad schema for mysql.ndbReplication table. Message: %-.64s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrConflictFnParse`.
pub const ErrConflictFnParse: ErrMessage = ErrMessage {
    raw: "Error in parsing conflict function. Message: %-.64s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrExceptionsWrite`.
pub const ErrExceptionsWrite: ErrMessage = ErrMessage {
    raw: "Write to exceptions table failed. Message: %-.128s\"",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTooLongTableComment`.
pub const ErrTooLongTableComment: ErrMessage = ErrMessage {
    raw: "Comment for table '%-.64s' is too long (max = %d)",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTooLongFieldComment`.
pub const ErrTooLongFieldComment: ErrMessage = ErrMessage {
    raw: "Comment for field '%-.64s' is too long (max = %d)",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrFuncInexistentNameCollision`.
pub const ErrFuncInexistentNameCollision: ErrMessage = ErrMessage {
    raw: "FUNCTION %s does not exist. Check the 'Function Name Parsing and Resolution' section in the Reference Manual",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrDatabaseName`.
pub const ErrDatabaseName: ErrMessage = ErrMessage {
    raw: "Database",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTableName`.
pub const ErrTableName: ErrMessage = ErrMessage {
    raw: "Table",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrPartitionName`.
pub const ErrPartitionName: ErrMessage = ErrMessage {
    raw: "Partition",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSubpartitionName`.
pub const ErrSubpartitionName: ErrMessage = ErrMessage {
    raw: "Subpartition",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTemporaryName`.
pub const ErrTemporaryName: ErrMessage = ErrMessage {
    raw: "Temporary",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrRenamedName`.
pub const ErrRenamedName: ErrMessage = ErrMessage {
    raw: "Renamed",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTooManyConcurrentTrxs`.
pub const ErrTooManyConcurrentTrxs: ErrMessage = ErrMessage {
    raw: "Too many active concurrent transactions",
    redact_arg_pos: &[],
};
/// Message metadata for `WarnNonASCIISeparatorNotImplemented`.
pub const WarnNonASCIISeparatorNotImplemented: ErrMessage = ErrMessage {
    raw: "Non-ASCII separator arguments are not fully supported",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrDebugSyncTimeout`.
pub const ErrDebugSyncTimeout: ErrMessage = ErrMessage {
    raw: "debug sync point wait timed out",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrDebugSyncHitLimit`.
pub const ErrDebugSyncHitLimit: ErrMessage = ErrMessage {
    raw: "debug sync point hit limit reached",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrDupSignalSet`.
pub const ErrDupSignalSet: ErrMessage = ErrMessage {
    raw: "Duplicate condition information item '%s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSignalWarn`.
pub const ErrSignalWarn: ErrMessage = ErrMessage {
    raw: "Unhandled user-defined warning condition",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSignalNotFound`.
pub const ErrSignalNotFound: ErrMessage = ErrMessage {
    raw: "Unhandled user-defined not found condition",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSignalException`.
pub const ErrSignalException: ErrMessage = ErrMessage {
    raw: "Unhandled user-defined exception condition",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrResignalWithoutActiveHandler`.
pub const ErrResignalWithoutActiveHandler: ErrMessage = ErrMessage {
    raw: "RESIGNAL when handler not active",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSignalBadConditionType`.
pub const ErrSignalBadConditionType: ErrMessage = ErrMessage {
    raw: "SIGNAL/RESIGNAL can only use a CONDITION defined with SQLSTATE",
    redact_arg_pos: &[],
};
/// Message metadata for `WarnCondItemTruncated`.
pub const WarnCondItemTruncated: ErrMessage = ErrMessage {
    raw: "Data truncated for condition item '%s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCondItemTooLong`.
pub const ErrCondItemTooLong: ErrMessage = ErrMessage {
    raw: "Data too long for condition item '%s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrUnknownLocale`.
pub const ErrUnknownLocale: ErrMessage = ErrMessage {
    raw: "Unknown locale: '%-.64s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrQueryCacheDisabled`.
pub const ErrQueryCacheDisabled: ErrMessage = ErrMessage {
    raw: "Query cache is disabled; restart the server with queryCacheType=1 to enable it",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSameNamePartitionField`.
pub const ErrSameNamePartitionField: ErrMessage = ErrMessage {
    raw: "Duplicate partition field name '%-.192s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrPartitionColumnList`.
pub const ErrPartitionColumnList: ErrMessage = ErrMessage {
    raw: "Inconsistency in usage of column lists for partitioning",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWrongTypeColumnValue`.
pub const ErrWrongTypeColumnValue: ErrMessage = ErrMessage {
    raw: "Partition column values of incorrect type",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTooManyPartitionFuncFields`.
pub const ErrTooManyPartitionFuncFields: ErrMessage = ErrMessage {
    raw: "Too many fields in '%-.192s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrMaxvalueInValuesIn`.
pub const ErrMaxvalueInValuesIn: ErrMessage = ErrMessage {
    raw: "Cannot use MAXVALUE as value in VALUES IN",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTooManyValues`.
pub const ErrTooManyValues: ErrMessage = ErrMessage {
    raw: "Cannot have more than one value for this type of %-.64s partitioning",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrRowSinglePartitionField`.
pub const ErrRowSinglePartitionField: ErrMessage = ErrMessage {
    raw: "Row expressions in VALUES IN only allowed for multi-field column partitioning",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrFieldTypeNotAllowedAsPartitionField`.
pub const ErrFieldTypeNotAllowedAsPartitionField: ErrMessage = ErrMessage {
    raw: "Field '%-.192s' is of a not allowed type for this type of partitioning",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrPartitionFieldsTooLong`.
pub const ErrPartitionFieldsTooLong: ErrMessage = ErrMessage {
    raw: "The total length of the partitioning fields is too large",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrBinlogRowEngineAndStmtEngine`.
pub const ErrBinlogRowEngineAndStmtEngine: ErrMessage = ErrMessage {
    raw: "Cannot execute statement: impossible to write to binary log since both row-incapable engines and statement-incapable engines are involved.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrBinlogRowModeAndStmtEngine`.
pub const ErrBinlogRowModeAndStmtEngine: ErrMessage = ErrMessage {
    raw: "Cannot execute statement: impossible to write to binary log since BINLOGFORMAT = ROW and at least one table uses a storage engine limited to statement-based logging.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrBinlogUnsafeAndStmtEngine`.
pub const ErrBinlogUnsafeAndStmtEngine: ErrMessage = ErrMessage {
    raw: "Cannot execute statement: impossible to write to binary log since statement is unsafe, storage engine is limited to statement-based logging, and BINLOGFORMAT = MIXED. %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrBinlogRowInjectionAndStmtEngine`.
pub const ErrBinlogRowInjectionAndStmtEngine: ErrMessage = ErrMessage {
    raw: "Cannot execute statement: impossible to write to binary log since statement is in row format and at least one table uses a storage engine limited to statement-based logging.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrBinlogStmtModeAndRowEngine`.
pub const ErrBinlogStmtModeAndRowEngine: ErrMessage = ErrMessage {
    raw: "Cannot execute statement: impossible to write to binary log since BINLOGFORMAT = STATEMENT and at least one table uses a storage engine limited to row-based logging.%s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrBinlogRowInjectionAndStmtMode`.
pub const ErrBinlogRowInjectionAndStmtMode: ErrMessage = ErrMessage {
    raw: "Cannot execute statement: impossible to write to binary log since statement is in row format and BINLOGFORMAT = STATEMENT.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrBinlogMultipleEnginesAndSelfLoggingEngine`.
pub const ErrBinlogMultipleEnginesAndSelfLoggingEngine: ErrMessage = ErrMessage {
    raw: "Cannot execute statement: impossible to write to binary log since more than one engine is involved and at least one engine is self-logging.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrBinlogUnsafeLimit`.
pub const ErrBinlogUnsafeLimit: ErrMessage = ErrMessage {
    raw: "The statement is unsafe because it uses a LIMIT clause. This is unsafe because the set of rows included cannot be predicted.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrBinlogUnsafeInsertDelayed`.
pub const ErrBinlogUnsafeInsertDelayed: ErrMessage = ErrMessage {
    raw: "The statement is unsafe because it uses INSERT DELAYED. This is unsafe because the times when rows are inserted cannot be predicted.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrBinlogUnsafeAutoincColumns`.
pub const ErrBinlogUnsafeAutoincColumns: ErrMessage = ErrMessage {
    raw: "Statement is unsafe because it invokes a trigger or a stored function that inserts into an AUTOINCREMENT column. Inserted values cannot be logged correctly.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrBinlogUnsafeSystemFunction`.
pub const ErrBinlogUnsafeSystemFunction: ErrMessage = ErrMessage {
    raw: "Statement is unsafe because it uses a system function that may return a different value on the slave",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrBinlogUnsafeNontransAfterTrans`.
pub const ErrBinlogUnsafeNontransAfterTrans: ErrMessage = ErrMessage {
    raw: "Statement is unsafe because it accesses a non-transactional table after accessing a transactional table within the same transaction.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrMessageAndStatement`.
pub const ErrMessageAndStatement: ErrMessage = ErrMessage {
    raw: "%s Statement: %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInsideTransactionPreventsSwitchBinlogFormat`.
pub const ErrInsideTransactionPreventsSwitchBinlogFormat: ErrMessage = ErrMessage {
    raw: "Cannot modify @@session.binlogFormat inside a transaction",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrPathLength`.
pub const ErrPathLength: ErrMessage = ErrMessage {
    raw: "The path specified for %.64s is too long.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWarnDeprecatedSyntaxNoReplacement`.
pub const ErrWarnDeprecatedSyntaxNoReplacement: ErrMessage = ErrMessage {
    raw: "%s is deprecated and will be removed in a future release.%s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWrongNativeTableStructure`.
pub const ErrWrongNativeTableStructure: ErrMessage = ErrMessage {
    raw: "Native table '%-.64s'.'%-.64s' has the wrong structure",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWrongPerfSchemaUsage`.
pub const ErrWrongPerfSchemaUsage: ErrMessage = ErrMessage {
    raw: "Invalid performanceSchema usage.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWarnISSkippedTable`.
pub const ErrWarnISSkippedTable: ErrMessage = ErrMessage {
    raw: "Table '%s'.'%s' was skipped since its definition is being modified by concurrent DDL statement",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInsideTransactionPreventsSwitchBinlogDirect`.
pub const ErrInsideTransactionPreventsSwitchBinlogDirect: ErrMessage = ErrMessage {
    raw: "Cannot modify @@session.binlogDirectNonTransactionalUpdates inside a transaction",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrStoredFunctionPreventsSwitchBinlogDirect`.
pub const ErrStoredFunctionPreventsSwitchBinlogDirect: ErrMessage = ErrMessage {
    raw: "Cannot change the binlog direct flag inside a stored function or trigger",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSpatialMustHaveGeomCol`.
pub const ErrSpatialMustHaveGeomCol: ErrMessage = ErrMessage {
    raw: "A SPATIAL index may only contain a geometrical type column",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTooLongIndexComment`.
pub const ErrTooLongIndexComment: ErrMessage = ErrMessage {
    raw: "Comment for index '%-.64s' is too long (max = %d)",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrLockAborted`.
pub const ErrLockAborted: ErrMessage = ErrMessage {
    raw: "Wait on a lock was aborted due to a pending exclusive lock",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrDataOutOfRange`.
pub const ErrDataOutOfRange: ErrMessage = ErrMessage {
    raw: "%s value is out of range in '%s'",
    redact_arg_pos: &[1],
};
/// Message metadata for `ErrWrongSpvarTypeInLimit`.
pub const ErrWrongSpvarTypeInLimit: ErrMessage = ErrMessage {
    raw: "A variable of a non-integer based type in LIMIT clause",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrBinlogUnsafeMultipleEnginesAndSelfLoggingEngine`.
pub const ErrBinlogUnsafeMultipleEnginesAndSelfLoggingEngine: ErrMessage = ErrMessage {
    raw: "Mixing self-logging and non-self-logging engines in a statement is unsafe.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrBinlogUnsafeMixedStatement`.
pub const ErrBinlogUnsafeMixedStatement: ErrMessage = ErrMessage {
    raw: "Statement accesses nontransactional table as well as transactional or temporary table, and writes to any of them.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInsideTransactionPreventsSwitchSQLLogBin`.
pub const ErrInsideTransactionPreventsSwitchSQLLogBin: ErrMessage = ErrMessage {
    raw: "Cannot modify @@session.sqlLogBin inside a transaction",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrStoredFunctionPreventsSwitchSQLLogBin`.
pub const ErrStoredFunctionPreventsSwitchSQLLogBin: ErrMessage = ErrMessage {
    raw: "Cannot change the sqlLogBin inside a stored function or trigger",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrFailedReadFromParFile`.
pub const ErrFailedReadFromParFile: ErrMessage = ErrMessage {
    raw: "Failed to read from the .par file",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrValuesIsNotIntType`.
pub const ErrValuesIsNotIntType: ErrMessage = ErrMessage {
    raw: "VALUES value for partition '%-.64s' must have type INT",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrAccessDeniedNoPassword`.
pub const ErrAccessDeniedNoPassword: ErrMessage = ErrMessage {
    raw: "Access denied for user '%-.48s'@'%-.255s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSetPasswordAuthPlugin`.
pub const ErrSetPasswordAuthPlugin: ErrMessage = ErrMessage {
    raw: "SET PASSWORD has no significance for user '%-.48s'@'%-.255s' as authentication plugin does not support it.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrGrantPluginUserExists`.
pub const ErrGrantPluginUserExists: ErrMessage = ErrMessage {
    raw: "GRANT with IDENTIFIED WITH is illegal because the user %-.*s already exists",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTruncateIllegalForeignKey`.
pub const ErrTruncateIllegalForeignKey: ErrMessage = ErrMessage {
    raw: "Cannot truncate a table referenced in a foreign key constraint (%.192s)",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrPluginIsPermanent`.
pub const ErrPluginIsPermanent: ErrMessage = ErrMessage {
    raw: "Plugin '%s' is forcePlusPermanent and can not be unloaded",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrStmtCacheFull`.
pub const ErrStmtCacheFull: ErrMessage = ErrMessage {
    raw: "Multi-row statements required more than 'maxBinlogStmtCacheSize' bytes of storage; increase this mysqld variable and try again",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrMultiUpdateKeyConflict`.
pub const ErrMultiUpdateKeyConflict: ErrMessage = ErrMessage {
    raw: "Primary key/partition key update is not allowed since the table is updated both as '%-.192s' and '%-.192s'.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTableNeedsRebuild`.
pub const ErrTableNeedsRebuild: ErrMessage = ErrMessage {
    raw:
        "Table rebuild required. Please do \"ALTER TABLE `%-.32s` FORCE\" or dump/reload to fix it!",
    redact_arg_pos: &[],
};
/// Message metadata for `WarnOptionBelowLimit`.
pub const WarnOptionBelowLimit: ErrMessage = ErrMessage {
    raw: "The value of '%s' should be no less than the value of '%s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrIndexColumnTooLong`.
pub const ErrIndexColumnTooLong: ErrMessage = ErrMessage {
    raw: "Index column size too large. The maximum column size is %d bytes.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrErrorInTriggerBody`.
pub const ErrErrorInTriggerBody: ErrMessage = ErrMessage {
    raw: "Trigger '%-.64s' has an error in its body: '%-.256s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrErrorInUnknownTriggerBody`.
pub const ErrErrorInUnknownTriggerBody: ErrMessage = ErrMessage {
    raw: "Unknown trigger has an error in its body: '%-.256s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrIndexCorrupt`.
pub const ErrIndexCorrupt: ErrMessage = ErrMessage {
    raw: "Index %s is corrupted",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrUndoRecordTooBig`.
pub const ErrUndoRecordTooBig: ErrMessage = ErrMessage {
    raw: "Undo log record is too big.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrPluginNoUninstall`.
pub const ErrPluginNoUninstall: ErrMessage = ErrMessage {
    raw: "Plugin '%s' is marked as not dynamically uninstallable. You have to stop the server to uninstall it.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrPluginNoInstall`.
pub const ErrPluginNoInstall: ErrMessage = ErrMessage {
    raw: "Plugin '%s' is marked as not dynamically installable. You have to stop the server to install it.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrBinlogUnsafeInsertTwoKeys`.
pub const ErrBinlogUnsafeInsertTwoKeys: ErrMessage = ErrMessage {
    raw: "INSERT... ON DUPLICATE KEY UPDATE  on a table with more than one UNIQUE KEY is unsafe",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTableInFkCheck`.
pub const ErrTableInFkCheck: ErrMessage = ErrMessage {
    raw: "Table is being used in foreign key check.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrUnsupportedEngine`.
pub const ErrUnsupportedEngine: ErrMessage = ErrMessage {
    raw: "Storage engine '%s' does not support system tables. [%s.%s]",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrBinlogUnsafeAutoincNotFirst`.
pub const ErrBinlogUnsafeAutoincNotFirst: ErrMessage = ErrMessage {
    raw: "INSERT into autoincrement field which is not the first part in the composed primary key is unsafe.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCannotLoadFromTableV2`.
pub const ErrCannotLoadFromTableV2: ErrMessage = ErrMessage {
    raw: "Cannot load from %s.%s. The table is probably corrupted",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrOnlyFdAndRbrEventsAllowedInBinlogStatement`.
pub const ErrOnlyFdAndRbrEventsAllowedInBinlogStatement: ErrMessage = ErrMessage {
    raw: "Only FormatDescriptionLogEvent and row events are allowed in BINLOG statements (but %s was provided)",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrPartitionExchangeDifferentOption`.
pub const ErrPartitionExchangeDifferentOption: ErrMessage = ErrMessage {
    raw: "Non matching attribute '%-.64s' between partition and table",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrPartitionExchangePartTable`.
pub const ErrPartitionExchangePartTable: ErrMessage = ErrMessage {
    raw: "Table to exchange with partition is partitioned: '%-.64s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrPartitionExchangeTempTable`.
pub const ErrPartitionExchangeTempTable: ErrMessage = ErrMessage {
    raw: "Table to exchange with partition is temporary: '%-.64s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrPartitionInsteadOfSubpartition`.
pub const ErrPartitionInsteadOfSubpartition: ErrMessage = ErrMessage {
    raw: "Subpartitioned table, use subpartition instead of partition",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrUnknownPartition`.
pub const ErrUnknownPartition: ErrMessage = ErrMessage {
    raw: "Unknown partition '%-.64s' in table '%-.64s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTablesDifferentMetadata`.
pub const ErrTablesDifferentMetadata: ErrMessage = ErrMessage {
    raw: "Tables have different definitions",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrRowDoesNotMatchPartition`.
pub const ErrRowDoesNotMatchPartition: ErrMessage = ErrMessage {
    raw: "Found a row that does not match the partition",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrBinlogCacheSizeGreaterThanMax`.
pub const ErrBinlogCacheSizeGreaterThanMax: ErrMessage = ErrMessage {
    raw: "Option binlogCacheSize (%d) is greater than maxBinlogCacheSize (%d); setting binlogCacheSize equal to maxBinlogCacheSize.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWarnIndexNotApplicable`.
pub const ErrWarnIndexNotApplicable: ErrMessage = ErrMessage {
    raw: "Cannot use %-.64s access on index '%-.64s' due to type or collation conversion on field '%-.64s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrPartitionExchangeForeignKey`.
pub const ErrPartitionExchangeForeignKey: ErrMessage = ErrMessage {
    raw: "Table to exchange with partition has foreign key references: '%-.64s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrNoSuchKeyValue`.
pub const ErrNoSuchKeyValue: ErrMessage = ErrMessage {
    raw: "Key value '%-.192s' was not found in table '%-.192s.%-.192s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrRplInfoDataTooLong`.
pub const ErrRplInfoDataTooLong: ErrMessage = ErrMessage {
    raw: "Data for column '%s' too long",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrNetworkReadEventChecksumFailure`.
pub const ErrNetworkReadEventChecksumFailure: ErrMessage = ErrMessage {
    raw: "Replication event checksum verification failed while reading from network.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrBinlogReadEventChecksumFailure`.
pub const ErrBinlogReadEventChecksumFailure: ErrMessage = ErrMessage {
    raw: "Replication event checksum verification failed while reading from a log file.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrBinlogStmtCacheSizeGreaterThanMax`.
pub const ErrBinlogStmtCacheSizeGreaterThanMax: ErrMessage = ErrMessage {
    raw: "Option binlogStmtCacheSize (%d) is greater than maxBinlogStmtCacheSize (%d); setting binlogStmtCacheSize equal to maxBinlogStmtCacheSize.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCantUpdateTableInCreateTableSelect`.
pub const ErrCantUpdateTableInCreateTableSelect: ErrMessage = ErrMessage {
    raw: "Can't update table '%-.192s' while '%-.192s' is being created.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrPartitionClauseOnNonpartitioned`.
pub const ErrPartitionClauseOnNonpartitioned: ErrMessage = ErrMessage {
    raw: "PARTITION () clause on non partitioned table",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrRowDoesNotMatchGivenPartitionSet`.
pub const ErrRowDoesNotMatchGivenPartitionSet: ErrMessage = ErrMessage {
    raw: "Found a row not matching the given partition set",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrNoSuchPartitionunused`.
pub const ErrNoSuchPartitionunused: ErrMessage = ErrMessage {
    raw: "partition '%-.64s' doesn't exist",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrChangeRplInfoRepositoryFailure`.
pub const ErrChangeRplInfoRepositoryFailure: ErrMessage = ErrMessage {
    raw: "Failure while changing the type of replication repository: %s.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWarningNotCompleteRollbackWithCreatedTempTable`.
pub const ErrWarningNotCompleteRollbackWithCreatedTempTable: ErrMessage = ErrMessage {
    raw: "The creation of some temporary tables could not be rolled back.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWarningNotCompleteRollbackWithDroppedTempTable`.
pub const ErrWarningNotCompleteRollbackWithDroppedTempTable: ErrMessage = ErrMessage {
    raw: "Some temporary tables were dropped, but these operations could not be rolled back.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrMtsUpdatedDBsGreaterMax`.
pub const ErrMtsUpdatedDBsGreaterMax: ErrMessage = ErrMessage {
    raw: "The number of modified databases exceeds the maximum %d; the database names will not be included in the replication event metadata.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrMtsCantParallel`.
pub const ErrMtsCantParallel: ErrMessage = ErrMessage {
    raw: "Cannot execute the current event group in the parallel mode. Encountered event %s, relay-log name %s, position %s which prevents execution of this event group in parallel mode. Reason: %s.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrMtsInconsistentData`.
pub const ErrMtsInconsistentData: ErrMessage = ErrMessage {
    raw: "%s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrFulltextNotSupportedWithPartitioning`.
pub const ErrFulltextNotSupportedWithPartitioning: ErrMessage = ErrMessage {
    raw: "FULLTEXT index is not supported for partitioned tables.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrDaInvalidConditionNumber`.
pub const ErrDaInvalidConditionNumber: ErrMessage = ErrMessage {
    raw: "Invalid condition number",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInsecurePlainText`.
pub const ErrInsecurePlainText: ErrMessage = ErrMessage {
    raw: "Sending passwords in plain text without SSL/TLS is extremely insecure.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrForeignDuplicateKeyWithChildInfo`.
pub const ErrForeignDuplicateKeyWithChildInfo: ErrMessage = ErrMessage {
    raw: "Foreign key constraint for table '%.192s', record '%-.192s' would lead to a duplicate entry in table '%.192s', key '%.192s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrForeignDuplicateKeyWithoutChildInfo`.
pub const ErrForeignDuplicateKeyWithoutChildInfo: ErrMessage = ErrMessage {
    raw: "Foreign key constraint for table '%.192s', record '%-.192s' would lead to a duplicate entry in a child table",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTableHasNoFt`.
pub const ErrTableHasNoFt: ErrMessage = ErrMessage {
    raw: "The table does not have FULLTEXT index to support this query",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrVariableNotSettableInSfOrTrigger`.
pub const ErrVariableNotSettableInSfOrTrigger: ErrMessage = ErrMessage {
    raw: "The system variable %.200s cannot be set in stored functions or triggers.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrVariableNotSettableInTransaction`.
pub const ErrVariableNotSettableInTransaction: ErrMessage = ErrMessage {
    raw: "The system variable %.200s cannot be set when there is an ongoing transaction.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrGtidNextIsNotInGtidNextList`.
pub const ErrGtidNextIsNotInGtidNextList: ErrMessage = ErrMessage {
    raw: "The system variable @@SESSION.GTIDNEXT has the value %.200s, which is not listed in @@SESSION.GTIDNEXTLIST.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCantChangeGtidNextInTransactionWhenGtidNextListIsNull`.
pub const ErrCantChangeGtidNextInTransactionWhenGtidNextListIsNull: ErrMessage = ErrMessage {
    raw: "When @@SESSION.GTIDNEXTLIST == NULL, the system variable @@SESSION.GTIDNEXT cannot change inside a transaction.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSetStatementCannotInvokeFunction`.
pub const ErrSetStatementCannotInvokeFunction: ErrMessage = ErrMessage {
    raw: "The statement 'SET %.200s' cannot invoke a stored function.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrGtidNextCantBeAutomaticIfGtidNextListIsNonNull`.
pub const ErrGtidNextCantBeAutomaticIfGtidNextListIsNonNull: ErrMessage = ErrMessage {
    raw: "The system variable @@SESSION.GTIDNEXT cannot be 'AUTOMATIC' when @@SESSION.GTIDNEXTLIST is non-NULL.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSkippingLoggedTransaction`.
pub const ErrSkippingLoggedTransaction: ErrMessage = ErrMessage {
    raw: "Skipping transaction %.200s because it has already been executed and logged.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrMalformedGtidSetSpecification`.
pub const ErrMalformedGtidSetSpecification: ErrMessage = ErrMessage {
    raw: "Malformed GTID set specification '%.200s'.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrMalformedGtidSetEncoding`.
pub const ErrMalformedGtidSetEncoding: ErrMessage = ErrMessage {
    raw: "Malformed GTID set encoding.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrMalformedGtidSpecification`.
pub const ErrMalformedGtidSpecification: ErrMessage = ErrMessage {
    raw: "Malformed GTID specification '%.200s'.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrGnoExhausted`.
pub const ErrGnoExhausted: ErrMessage = ErrMessage {
    raw: "Impossible to generate Global Transaction Identifier: the integer component reached the maximal value. Restart the server with a new serverUuid.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCantDoImplicitCommitInTrxWhenGtidNextIsSet`.
pub const ErrCantDoImplicitCommitInTrxWhenGtidNextIsSet: ErrMessage = ErrMessage {
    raw: "Cannot execute statements with implicit commit inside a transaction when @@SESSION.GTIDNEXT != AUTOMATIC or @@SESSION.GTIDNEXTLIST != NULL.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrGtidMode2Or3RequiresEnforceGtidConsistencyOn`.
pub const ErrGtidMode2Or3RequiresEnforceGtidConsistencyOn: ErrMessage = ErrMessage {
    raw: "@@GLOBAL.GTIDMODE = ON or UPGRADESTEP2 requires @@GLOBAL.ENFORCEGTIDCONSISTENCY = 1.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCantSetGtidNextToGtidWhenGtidModeIsOff`.
pub const ErrCantSetGtidNextToGtidWhenGtidModeIsOff: ErrMessage = ErrMessage {
    raw: "@@SESSION.GTIDNEXT cannot be set to UUID:NUMBER when @@GLOBAL.GTIDMODE = OFF.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCantSetGtidNextToAnonymousWhenGtidModeIsOn`.
pub const ErrCantSetGtidNextToAnonymousWhenGtidModeIsOn: ErrMessage = ErrMessage {
    raw: "@@SESSION.GTIDNEXT cannot be set to ANONYMOUS when @@GLOBAL.GTIDMODE = ON.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCantSetGtidNextListToNonNullWhenGtidModeIsOff`.
pub const ErrCantSetGtidNextListToNonNullWhenGtidModeIsOff: ErrMessage = ErrMessage {
    raw: "@@SESSION.GTIDNEXTLIST cannot be set to a non-NULL value when @@GLOBAL.GTIDMODE = OFF.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrFoundGtidEventWhenGtidModeIsOff`.
pub const ErrFoundGtidEventWhenGtidModeIsOff: ErrMessage = ErrMessage {
    raw: "Found a GtidLogEvent or PreviousGtidsLogEvent when @@GLOBAL.GTIDMODE = OFF.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrGtidUnsafeNonTransactionalTable`.
pub const ErrGtidUnsafeNonTransactionalTable: ErrMessage = ErrMessage {
    raw: "When @@GLOBAL.ENFORCEGTIDCONSISTENCY = 1, updates to non-transactional tables can only be done in either autocommitted statements or single-statement transactions, and never in the same statement as updates to transactional tables.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrGtidUnsafeCreateSelect`.
pub const ErrGtidUnsafeCreateSelect: ErrMessage = ErrMessage {
    raw: "CREATE TABLE ... SELECT is forbidden when @@GLOBAL.ENFORCEGTIDCONSISTENCY = 1.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrGtidUnsafeCreateDropTemporaryTableInTransaction`.
pub const ErrGtidUnsafeCreateDropTemporaryTableInTransaction: ErrMessage = ErrMessage {
    raw: "When @@GLOBAL.ENFORCEGTIDCONSISTENCY = 1, the statements CREATE TEMPORARY TABLE and DROP TEMPORARY TABLE can be executed in a non-transactional context only, and require that AUTOCOMMIT = 1.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrGtidModeCanOnlyChangeOneStepAtATime`.
pub const ErrGtidModeCanOnlyChangeOneStepAtATime: ErrMessage = ErrMessage {
    raw: "The value of @@GLOBAL.GTIDMODE can only change one step at a time: OFF <-> UPGRADESTEP1 <-> UPGRADESTEP2 <-> ON. Also note that this value must be stepped up or down simultaneously on all servers; see the Manual for instructions.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCantSetGtidNextWhenOwningGtid`.
pub const ErrCantSetGtidNextWhenOwningGtid: ErrMessage = ErrMessage {
    raw: "@@SESSION.GTIDNEXT cannot be changed by a client that owns a GTID. The client owns %s. Ownership is released on COMMIT or ROLLBACK.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrUnknownExplainFormat`.
pub const ErrUnknownExplainFormat: ErrMessage = ErrMessage {
    raw: "Unknown EXPLAIN format name: '%s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCantExecuteInReadOnlyTransaction`.
pub const ErrCantExecuteInReadOnlyTransaction: ErrMessage = ErrMessage {
    raw: "Cannot execute statement in a READ ONLY transaction.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTooLongTablePartitionComment`.
pub const ErrTooLongTablePartitionComment: ErrMessage = ErrMessage {
    raw: "Comment for table partition '%-.64s' is too long (max = %d)",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInnodbFtLimit`.
pub const ErrInnodbFtLimit: ErrMessage = ErrMessage {
    raw: "InnoDB presently supports one FULLTEXT index creation at a time",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInnodbNoFtTempTable`.
pub const ErrInnodbNoFtTempTable: ErrMessage = ErrMessage {
    raw: "Cannot create FULLTEXT index on temporary InnoDB table",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInnodbFtWrongDocidColumn`.
pub const ErrInnodbFtWrongDocidColumn: ErrMessage = ErrMessage {
    raw: "Column '%-.192s' is of wrong type for an InnoDB FULLTEXT index",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInnodbFtWrongDocidIndex`.
pub const ErrInnodbFtWrongDocidIndex: ErrMessage = ErrMessage {
    raw: "Index '%-.192s' is of wrong type for an InnoDB FULLTEXT index",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInnodbOnlineLogTooBig`.
pub const ErrInnodbOnlineLogTooBig: ErrMessage = ErrMessage {
    raw: "Creating index '%-.192s' required more than 'innodbOnlineAlterLogMaxSize' bytes of modification log. Please try again.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrUnknownAlterAlgorithm`.
pub const ErrUnknownAlterAlgorithm: ErrMessage = ErrMessage {
    raw: "Unknown ALGORITHM '%s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrUnknownAlterLock`.
pub const ErrUnknownAlterLock: ErrMessage = ErrMessage {
    raw: "Unknown LOCK type '%s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrMtsResetWorkers`.
pub const ErrMtsResetWorkers: ErrMessage = ErrMessage {
    raw: "Cannot clean up worker info tables. Additional error messages can be found in the MySQL error log.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrColCountDoesntMatchCorruptedV2`.
pub const ErrColCountDoesntMatchCorruptedV2: ErrMessage = ErrMessage {
    raw: "Column count of %s.%s is wrong. Expected %d, found %d. The table is probably corrupted",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrDiscardFkChecksRunning`.
pub const ErrDiscardFkChecksRunning: ErrMessage = ErrMessage {
    raw: "There is a foreign key check running on table '%-.192s'. Cannot discard the table.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTableSchemaMismatch`.
pub const ErrTableSchemaMismatch: ErrMessage = ErrMessage {
    raw: "Schema mismatch (%s)",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTableInSystemTablespace`.
pub const ErrTableInSystemTablespace: ErrMessage = ErrMessage {
    raw: "Table '%-.192s' in system tablespace",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrIoRead`.
pub const ErrIoRead: ErrMessage = ErrMessage {
    raw: "IO Read : (%d, %s) %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrIoWrite`.
pub const ErrIoWrite: ErrMessage = ErrMessage {
    raw: "IO Write : (%d, %s) %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTablespaceMissing`.
pub const ErrTablespaceMissing: ErrMessage = ErrMessage {
    raw: "Tablespace is missing for table '%-.192s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTablespaceExists`.
pub const ErrTablespaceExists: ErrMessage = ErrMessage {
    raw: "Tablespace for table '%-.192s' exists. Please DISCARD the tablespace before IMPORT.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTablespaceDiscarded`.
pub const ErrTablespaceDiscarded: ErrMessage = ErrMessage {
    raw: "Tablespace has been discarded for table '%-.192s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInternal`.
pub const ErrInternal: ErrMessage = ErrMessage {
    raw: "Internal : %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInnodbImport`.
pub const ErrInnodbImport: ErrMessage = ErrMessage {
    raw: "ALTER TABLE '%-.192s' IMPORT TABLESPACE failed with error %d : '%s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInnodbIndexCorrupt`.
pub const ErrInnodbIndexCorrupt: ErrMessage = ErrMessage {
    raw: "Index corrupt: %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInvalidYearColumnLength`.
pub const ErrInvalidYearColumnLength: ErrMessage = ErrMessage {
    raw: "Supports only YEAR or YEAR(4) column",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrNotValidPassword`.
pub const ErrNotValidPassword: ErrMessage = ErrMessage {
    raw: "Your password does not satisfy the current policy requirements (%s)",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrMustChangePassword`.
pub const ErrMustChangePassword: ErrMessage = ErrMessage {
    raw: "You must reset your password using ALTER USER statement before executing this statement",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrFkNoIndexChild`.
pub const ErrFkNoIndexChild: ErrMessage = ErrMessage {
    raw: "Failed to add the foreign key constraint. Missing index for constraint '%s' in the foreign table '%s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrForeignKeyNoIndexInParent`.
pub const ErrForeignKeyNoIndexInParent: ErrMessage = ErrMessage {
    raw: "Failed to add the foreign key constraint. Missing index for constraint '%s' in the referenced table '%s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrFkFailAddSystem`.
pub const ErrFkFailAddSystem: ErrMessage = ErrMessage {
    raw: "Failed to add the foreign key constraint '%s' to system tables",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrForeignKeyCannotOpenParent`.
pub const ErrForeignKeyCannotOpenParent: ErrMessage = ErrMessage {
    raw: "Failed to open the referenced table '%s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrFkIncorrectOption`.
pub const ErrFkIncorrectOption: ErrMessage = ErrMessage {
    raw: "Failed to add the foreign key constraint on table '%s'. Incorrect options in FOREIGN KEY constraint '%s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrFkDupName`.
pub const ErrFkDupName: ErrMessage = ErrMessage {
    raw: "Duplicate foreign key constraint name '%s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrPasswordFormat`.
pub const ErrPasswordFormat: ErrMessage = ErrMessage {
    raw: "The password hash doesn't have the expected format. Check if the correct password algorithm is being used with the PASSWORD() function.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrFkColumnCannotDrop`.
pub const ErrFkColumnCannotDrop: ErrMessage = ErrMessage {
    raw: "Cannot drop column '%-.192s': needed in a foreign key constraint '%-.192s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrFkColumnCannotDropChild`.
pub const ErrFkColumnCannotDropChild: ErrMessage = ErrMessage {
    raw: "Cannot drop column '%-.192s': needed in a foreign key constraint '%-.192s' of table '%-.192s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrForeignKeyColumnNotNull`.
pub const ErrForeignKeyColumnNotNull: ErrMessage = ErrMessage {
    raw:
        "Column '%-.192s' cannot be NOT NULL: needed in a foreign key constraint '%-.192s' SET NULL",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrDupIndex`.
pub const ErrDupIndex: ErrMessage = ErrMessage {
    raw: "Duplicate index '%-.64s' defined on the table '%-.64s.%-.64s'. This is deprecated and will be disallowed in a future release.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrForeignKeyColumnCannotChange`.
pub const ErrForeignKeyColumnCannotChange: ErrMessage = ErrMessage {
    raw: "Cannot change column '%-.192s': used in a foreign key constraint '%-.192s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrForeignKeyColumnCannotChangeChild`.
pub const ErrForeignKeyColumnCannotChangeChild: ErrMessage = ErrMessage {
    raw: "Cannot change column '%-.192s': used in a foreign key constraint '%-.192s' of table '%-.192s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrFkCannotDeleteParent`.
pub const ErrFkCannotDeleteParent: ErrMessage = ErrMessage {
    raw: "Cannot delete rows from table which is parent in a foreign key constraint '%-.192s' of table '%-.192s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrMalformedPacket`.
pub const ErrMalformedPacket: ErrMessage = ErrMessage {
    raw: "Malformed communication packet.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrReadOnlyMode`.
pub const ErrReadOnlyMode: ErrMessage = ErrMessage {
    raw: "Running in read-only mode",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrVariableNotSettableInSp`.
pub const ErrVariableNotSettableInSp: ErrMessage = ErrMessage {
    raw: "The system variable %.200s cannot be set in stored procedures.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCantSetGtidPurgedWhenGtidModeIsOff`.
pub const ErrCantSetGtidPurgedWhenGtidModeIsOff: ErrMessage = ErrMessage {
    raw: "@@GLOBAL.GTIDPURGED can only be set when @@GLOBAL.GTIDMODE = ON.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCantSetGtidPurgedWhenGtidExecutedIsNotEmpty`.
pub const ErrCantSetGtidPurgedWhenGtidExecutedIsNotEmpty: ErrMessage = ErrMessage {
    raw: "@@GLOBAL.GTIDPURGED can only be set when @@GLOBAL.GTIDEXECUTED is empty.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCantSetGtidPurgedWhenOwnedGtidsIsNotEmpty`.
pub const ErrCantSetGtidPurgedWhenOwnedGtidsIsNotEmpty: ErrMessage = ErrMessage {
    raw: "@@GLOBAL.GTIDPURGED can only be set when there are no ongoing transactions (not even in other clients).",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrGtidPurgedWasChanged`.
pub const ErrGtidPurgedWasChanged: ErrMessage = ErrMessage {
    raw: "@@GLOBAL.GTIDPURGED was changed from '%s' to '%s'.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrGtidExecutedWasChanged`.
pub const ErrGtidExecutedWasChanged: ErrMessage = ErrMessage {
    raw: "@@GLOBAL.GTIDEXECUTED was changed from '%s' to '%s'.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrBinlogStmtModeAndNoReplTables`.
pub const ErrBinlogStmtModeAndNoReplTables: ErrMessage = ErrMessage {
    raw: "Cannot execute statement: impossible to write to binary log since BINLOGFORMAT = STATEMENT, and both replicated and non replicated tables are written to.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrAlterOperationNotSupported`.
pub const ErrAlterOperationNotSupported: ErrMessage = ErrMessage {
    raw: "%s is not supported for this operation. Try %s.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrAlterOperationNotSupportedReason`.
pub const ErrAlterOperationNotSupportedReason: ErrMessage = ErrMessage {
    raw: "%s is not supported. Reason: %s. Try %s.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrAlterOperationNotSupportedReasonCopy`.
pub const ErrAlterOperationNotSupportedReasonCopy: ErrMessage = ErrMessage {
    raw: "COPY algorithm requires a lock",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrAlterOperationNotSupportedReasonPartition`.
pub const ErrAlterOperationNotSupportedReasonPartition: ErrMessage = ErrMessage {
    raw: "Partition specific operations do not yet support LOCK/ALGORITHM",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrAlterOperationNotSupportedReasonFkRename`.
pub const ErrAlterOperationNotSupportedReasonFkRename: ErrMessage = ErrMessage {
    raw: "Columns participating in a foreign key are renamed",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrAlterOperationNotSupportedReasonColumnType`.
pub const ErrAlterOperationNotSupportedReasonColumnType: ErrMessage = ErrMessage {
    raw: "Cannot change column type INPLACE",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrAlterOperationNotSupportedReasonFkCheck`.
pub const ErrAlterOperationNotSupportedReasonFkCheck: ErrMessage = ErrMessage {
    raw: "Adding foreign keys needs foreignKeyChecks=OFF",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrAlterOperationNotSupportedReasonIgnore`.
pub const ErrAlterOperationNotSupportedReasonIgnore: ErrMessage = ErrMessage {
    raw: "Creating unique indexes with IGNORE requires COPY algorithm to remove duplicate rows",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrAlterOperationNotSupportedReasonNopk`.
pub const ErrAlterOperationNotSupportedReasonNopk: ErrMessage = ErrMessage {
    raw: "Dropping a primary key is not allowed without also adding a new primary key",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrAlterOperationNotSupportedReasonAutoinc`.
pub const ErrAlterOperationNotSupportedReasonAutoinc: ErrMessage = ErrMessage {
    raw: "Adding an auto-increment column requires a lock",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrAlterOperationNotSupportedReasonHiddenFts`.
pub const ErrAlterOperationNotSupportedReasonHiddenFts: ErrMessage = ErrMessage {
    raw: "Cannot replace hidden FTSDOCID with a user-visible one",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrAlterOperationNotSupportedReasonChangeFts`.
pub const ErrAlterOperationNotSupportedReasonChangeFts: ErrMessage = ErrMessage {
    raw: "Cannot drop or rename FTSDOCID",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrAlterOperationNotSupportedReasonFts`.
pub const ErrAlterOperationNotSupportedReasonFts: ErrMessage = ErrMessage {
    raw: "Fulltext index creation requires a lock",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrDupUnknownInIndex`.
pub const ErrDupUnknownInIndex: ErrMessage = ErrMessage {
    raw: "Duplicate entry for key '%-.192s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrIdentCausesTooLongPath`.
pub const ErrIdentCausesTooLongPath: ErrMessage = ErrMessage {
    raw: "Long database name and identifier for object resulted in path length exceeding %d characters. Path: '%s'.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrAlterOperationNotSupportedReasonNotNull`.
pub const ErrAlterOperationNotSupportedReasonNotNull: ErrMessage = ErrMessage {
    raw: "cannot silently convert NULL values, as required in this SQLMODE",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrMustChangePasswordLogin`.
pub const ErrMustChangePasswordLogin: ErrMessage = ErrMessage {
    raw: "Your password has expired. To log in you must change it using a client that supports expired passwords.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrRowInWrongPartition`.
pub const ErrRowInWrongPartition: ErrMessage = ErrMessage {
    raw: "Found a row in wrong partition %s",
    redact_arg_pos: &[0],
};
/// Message metadata for `ErrGeneratedColumnFunctionIsNotAllowed`.
pub const ErrGeneratedColumnFunctionIsNotAllowed: ErrMessage = ErrMessage {
    raw: "Expression of generated column '%s' contains a disallowed function.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrGeneratedColumnRowValueIsNotAllowed`.
pub const ErrGeneratedColumnRowValueIsNotAllowed: ErrMessage = ErrMessage {
    raw: "Expression of generated column '%s' cannot refer to a row value",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrDefValGeneratedNamedFunctionIsNotAllowed`.
pub const ErrDefValGeneratedNamedFunctionIsNotAllowed: ErrMessage = ErrMessage {
    raw: "Default value expression of column '%s' contains a disallowed function: `%s`.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrUnsupportedAlterInplaceOnVirtualColumn`.
pub const ErrUnsupportedAlterInplaceOnVirtualColumn: ErrMessage = ErrMessage {
    raw:
        "INPLACE ADD or DROP of virtual columns cannot be combined with other ALTER TABLE actions.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWrongFKOptionForGeneratedColumn`.
pub const ErrWrongFKOptionForGeneratedColumn: ErrMessage = ErrMessage {
    raw: "Cannot define foreign key with %s clause on a generated column.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrBadGeneratedColumn`.
pub const ErrBadGeneratedColumn: ErrMessage = ErrMessage {
    raw: "The value specified for generated column '%s' in table '%s' is not allowed.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrUnsupportedOnGeneratedColumn`.
pub const ErrUnsupportedOnGeneratedColumn: ErrMessage = ErrMessage {
    raw: "'%s' is not supported for generated columns.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrGeneratedColumnNonPrior`.
pub const ErrGeneratedColumnNonPrior: ErrMessage = ErrMessage {
    raw: "Generated column can refer only to generated columns defined prior to it.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrDependentByGeneratedColumn`.
pub const ErrDependentByGeneratedColumn: ErrMessage = ErrMessage {
    raw: "Column '%s' has a generated column dependency.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrGeneratedColumnRefAutoInc`.
pub const ErrGeneratedColumnRefAutoInc: ErrMessage = ErrMessage {
    raw: "Generated column '%s' cannot refer to auto-increment column.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrAccountHasBeenLocked`.
pub const ErrAccountHasBeenLocked: ErrMessage = ErrMessage {
    raw: "Access denied for user '%s'@'%s'. Account is locked.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErUserAccessDeniedForUserAccountBlockedByPasswordLock`.
pub const ErUserAccessDeniedForUserAccountBlockedByPasswordLock: ErrMessage = ErrMessage {
    raw: "Access denied for user '%s'@'%s'. Account is blocked for %s day(s) (%s day(s) remaining) due to %d consecutive failed logins.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWarnConflictingHint`.
pub const ErrWarnConflictingHint: ErrMessage = ErrMessage {
    raw: "Hint %s is ignored as conflicting/duplicated.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrUnresolvedHintName`.
pub const ErrUnresolvedHintName: ErrMessage = ErrMessage {
    raw: "Unresolved name '%s' for %s hint",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrForeignKeyCascadeDepthExceeded`.
pub const ErrForeignKeyCascadeDepthExceeded: ErrMessage = ErrMessage {
    raw: "Foreign key cascade delete/update exceeds max depth of %v.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInvalidFieldSize`.
pub const ErrInvalidFieldSize: ErrMessage = ErrMessage {
    raw: "Invalid size for column '%s'.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrPasswordExpireAnonymousUser`.
pub const ErrPasswordExpireAnonymousUser: ErrMessage = ErrMessage {
    raw: "The password for anonymous user cannot be expired.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInvalidArgumentForLogarithm`.
pub const ErrInvalidArgumentForLogarithm: ErrMessage = ErrMessage {
    raw: "Invalid argument for logarithm",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrAggregateOrderNonAggQuery`.
pub const ErrAggregateOrderNonAggQuery: ErrMessage = ErrMessage {
    raw: "Expression #%d of ORDER BY contains aggregate function and applies to the result of a non-aggregated query",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrIncorrectType`.
pub const ErrIncorrectType: ErrMessage = ErrMessage {
    raw: "Incorrect type for argument %s in function %s.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrFieldInOrderNotSelect`.
pub const ErrFieldInOrderNotSelect: ErrMessage = ErrMessage {
    raw: "Expression #%d of ORDER BY clause is not in SELECT list, references column '%s' which is not in SELECT list; this is incompatible with %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrAggregateInOrderNotSelect`.
pub const ErrAggregateInOrderNotSelect: ErrMessage = ErrMessage {
    raw: "Expression #%d of ORDER BY clause is not in SELECT list, contains aggregate function; this is incompatible with %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInvalidJSONData`.
pub const ErrInvalidJSONData: ErrMessage = ErrMessage {
    raw: "Invalid JSON data provided to function %s: %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInvalidJSONText`.
pub const ErrInvalidJSONText: ErrMessage = ErrMessage {
    raw: "Invalid JSON text: %-.192s",
    redact_arg_pos: &[0],
};
/// Message metadata for `ErrInvalidJSONTextInParam`.
pub const ErrInvalidJSONTextInParam: ErrMessage = ErrMessage {
    raw: "Invalid JSON text in argument %d to function %s: \"%s\" at position %d.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInvalidJSONPath`.
pub const ErrInvalidJSONPath: ErrMessage = ErrMessage {
    raw: "Invalid JSON path expression. The error is around character position %d.",
    redact_arg_pos: &[0],
};
/// Message metadata for `ErrInvalidJSONCharset`.
pub const ErrInvalidJSONCharset: ErrMessage = ErrMessage {
    raw: "Cannot create a JSON value from a string with CHARACTER SET '%s'.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInvalidTypeForJSON`.
pub const ErrInvalidTypeForJSON: ErrMessage = ErrMessage {
    raw: "Invalid data type for JSON data in argument %d to function %s; a JSON string or JSON type is required.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInvalidJSONPathMultipleSelection`.
pub const ErrInvalidJSONPathMultipleSelection: ErrMessage = ErrMessage {
    raw:
        "In this situation, path expressions may not contain the * and ** tokens or an array range.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInvalidJSONContainsPathType`.
pub const ErrInvalidJSONContainsPathType: ErrMessage = ErrMessage {
    raw: "The second argument can only be either 'one' or 'all'.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrJSONUsedAsKey`.
pub const ErrJSONUsedAsKey: ErrMessage = ErrMessage {
    raw: "JSON column '%-.192s' cannot be used in key specification.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrJSONDocumentTooDeep`.
pub const ErrJSONDocumentTooDeep: ErrMessage = ErrMessage {
    raw: "The JSON document exceeds the maximum depth.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrJSONDocumentNULLKey`.
pub const ErrJSONDocumentNULLKey: ErrMessage = ErrMessage {
    raw: "JSON documents may not contain NULL member names.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSecureTransportRequired`.
pub const ErrSecureTransportRequired: ErrMessage = ErrMessage {
    raw: "Connections using insecure transport are prohibited while --require_secure_transport=ON.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrBadUser`.
pub const ErrBadUser: ErrMessage = ErrMessage {
    raw: "User %s does not exist.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrUserAlreadyExists`.
pub const ErrUserAlreadyExists: ErrMessage = ErrMessage {
    raw: "User %s already exists.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInvalidJSONPathArrayCell`.
pub const ErrInvalidJSONPathArrayCell: ErrMessage = ErrMessage {
    raw: "A path expression is not a path to a cell in an array.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInvalidEncryptionOption`.
pub const ErrInvalidEncryptionOption: ErrMessage = ErrMessage {
    raw: "Invalid encryption option.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTooLongValueForType`.
pub const ErrTooLongValueForType: ErrMessage = ErrMessage {
    raw: "Too long enumeration/set value for column %s.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrPKIndexCantBeInvisible`.
pub const ErrPKIndexCantBeInvisible: ErrMessage = ErrMessage {
    raw: "A primary key index cannot be invisible",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWindowNoSuchWindow`.
pub const ErrWindowNoSuchWindow: ErrMessage = ErrMessage {
    raw: "Window name '%s' is not defined.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWindowCircularityInWindowGraph`.
pub const ErrWindowCircularityInWindowGraph: ErrMessage = ErrMessage {
    raw: "There is a circularity in the window dependency graph.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWindowNoChildPartitioning`.
pub const ErrWindowNoChildPartitioning: ErrMessage = ErrMessage {
    raw: "A window which depends on another cannot define partitioning.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWindowNoInherentFrame`.
pub const ErrWindowNoInherentFrame: ErrMessage = ErrMessage {
    raw: "Window '%s' has a frame definition, so cannot be referenced by another window.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWindowNoRedefineOrderBy`.
pub const ErrWindowNoRedefineOrderBy: ErrMessage = ErrMessage {
    raw: "Window '%s' cannot inherit '%s' since both contain an ORDER BY clause.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWindowFrameStartIllegal`.
pub const ErrWindowFrameStartIllegal: ErrMessage = ErrMessage {
    raw: "Window '%s': frame start cannot be UNBOUNDED FOLLOWING.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWindowFrameEndIllegal`.
pub const ErrWindowFrameEndIllegal: ErrMessage = ErrMessage {
    raw: "Window '%s': frame end cannot be UNBOUNDED PRECEDING.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWindowFrameIllegal`.
pub const ErrWindowFrameIllegal: ErrMessage = ErrMessage {
    raw: "Window '%s': frame start or end is negative, NULL or of non-integral type",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWindowRangeFrameOrderType`.
pub const ErrWindowRangeFrameOrderType: ErrMessage = ErrMessage {
    raw: "Window '%s' with RANGE N PRECEDING/FOLLOWING frame requires exactly one ORDER BY expression, of numeric or temporal type",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWindowRangeFrameTemporalType`.
pub const ErrWindowRangeFrameTemporalType: ErrMessage = ErrMessage {
    raw: "Window '%s' with RANGE frame has ORDER BY expression of datetime type. Only INTERVAL bound value allowed.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWindowRangeFrameNumericType`.
pub const ErrWindowRangeFrameNumericType: ErrMessage = ErrMessage {
    raw: "Window '%s' with RANGE frame has ORDER BY expression of numeric type, INTERVAL bound value not allowed.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWindowRangeBoundNotConstant`.
pub const ErrWindowRangeBoundNotConstant: ErrMessage = ErrMessage {
    raw: "Window '%s' has a non-constant frame bound.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWindowDuplicateName`.
pub const ErrWindowDuplicateName: ErrMessage = ErrMessage {
    raw: "Window '%s' is defined twice.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWindowIllegalOrderBy`.
pub const ErrWindowIllegalOrderBy: ErrMessage = ErrMessage {
    raw: "Window '%s': ORDER BY or PARTITION BY uses legacy position indication which is not supported, use expression.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWindowInvalidWindowFuncUse`.
pub const ErrWindowInvalidWindowFuncUse: ErrMessage = ErrMessage {
    raw: "You cannot use the window function '%s' in this context.'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWindowInvalidWindowFuncAliasUse`.
pub const ErrWindowInvalidWindowFuncAliasUse: ErrMessage = ErrMessage {
    raw: "You cannot use the alias '%s' of an expression containing a window function in this context.'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWindowNestedWindowFuncUseInWindowSpec`.
pub const ErrWindowNestedWindowFuncUseInWindowSpec: ErrMessage = ErrMessage {
    raw: "You cannot nest a window function in the specification of window '%s'.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWindowRowsIntervalUse`.
pub const ErrWindowRowsIntervalUse: ErrMessage = ErrMessage {
    raw: "Window '%s': INTERVAL can only be used with RANGE frames.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWindowNoGroupOrderUnused`.
pub const ErrWindowNoGroupOrderUnused: ErrMessage = ErrMessage {
    raw:
        "ASC or DESC with GROUP BY isn't allowed with window functions; put ASC or DESC in ORDER BY",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWindowExplainJSON`.
pub const ErrWindowExplainJSON: ErrMessage = ErrMessage {
    raw: "To get information about window functions use EXPLAIN FORMAT=JSON",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWindowFunctionIgnoresFrame`.
pub const ErrWindowFunctionIgnoresFrame: ErrMessage = ErrMessage {
    raw: "Window function '%s' ignores the frame clause of window '%s' and aggregates over the whole partition",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInvalidNumberOfArgs`.
pub const ErrInvalidNumberOfArgs: ErrMessage = ErrMessage {
    raw: "Too many arguments for function %s; maximum allowed is %d",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrFieldInGroupingNotGroupBy`.
pub const ErrFieldInGroupingNotGroupBy: ErrMessage = ErrMessage {
    raw: "Argument %s of GROUPING function is not in GROUP BY",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrRoleNotGranted`.
pub const ErrRoleNotGranted: ErrMessage = ErrMessage {
    raw: "%s is not granted to %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrMaxExecTimeExceeded`.
pub const ErrMaxExecTimeExceeded: ErrMessage = ErrMessage {
    raw: "Query execution was interrupted, maximum statement execution time exceeded",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrLockAcquireFailAndNoWaitSet`.
pub const ErrLockAcquireFailAndNoWaitSet: ErrMessage = ErrMessage {
    raw: "Statement aborted because lock(s) could not be acquired immediately and NOWAIT is set.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrNotHintUpdatable`.
pub const ErrNotHintUpdatable: ErrMessage = ErrMessage {
    raw: "Variable '%s' might not be affected by SET_VAR hint.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrExistsInHistoryPassword`.
pub const ErrExistsInHistoryPassword: ErrMessage = ErrMessage {
    raw: "Cannot use these credentials for '%s@%s' because they contradict the password history policy.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInvalidDefaultUTF8MB4Collation`.
pub const ErrInvalidDefaultUTF8MB4Collation: ErrMessage = ErrMessage {
    raw: "Invalid default collation %s: utf8mb4_0900_ai_ci or utf8mb4_general_ci or utf8mb4_bin expected",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrForeignKeyCannotDropParent`.
pub const ErrForeignKeyCannotDropParent: ErrMessage = ErrMessage {
    raw: "Cannot drop table '%s' referenced by a foreign key constraint '%s' on table '%s'.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrForeignKeyCannotUseVirtualColumn`.
pub const ErrForeignKeyCannotUseVirtualColumn: ErrMessage = ErrMessage {
    raw: "Foreign key '%s' uses virtual column '%s' which is not supported.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrForeignKeyNoColumnInParent`.
pub const ErrForeignKeyNoColumnInParent: ErrMessage = ErrMessage {
    raw: "Failed to add the foreign key constraint. Missing column '%s' for constraint '%s' in the referenced table '%s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrDataTruncatedFunctionalIndex`.
pub const ErrDataTruncatedFunctionalIndex: ErrMessage = ErrMessage {
    raw: "Data truncated for expression index '%s' at row %d",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrDataOutOfRangeFunctionalIndex`.
pub const ErrDataOutOfRangeFunctionalIndex: ErrMessage = ErrMessage {
    raw: "Value is out of range for expression index '%s' at row %d",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrFunctionalIndexOnJSONOrGeometryFunction`.
pub const ErrFunctionalIndexOnJSONOrGeometryFunction: ErrMessage = ErrMessage {
    raw: "Cannot create an expression index on a function that returns a JSON or GEOMETRY value",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrFunctionalIndexRefAutoIncrement`.
pub const ErrFunctionalIndexRefAutoIncrement: ErrMessage = ErrMessage {
    raw: "Expression index '%s' cannot refer to an auto-increment column",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCannotDropColumnFunctionalIndex`.
pub const ErrCannotDropColumnFunctionalIndex: ErrMessage = ErrMessage {
    raw: "Cannot drop column '%s' because it is used by an expression index. In order to drop the column, you must remove the expression index",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrFunctionalIndexPrimaryKey`.
pub const ErrFunctionalIndexPrimaryKey: ErrMessage = ErrMessage {
    raw: "The primary key cannot be an expression index",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrFunctionalIndexOnBlob`.
pub const ErrFunctionalIndexOnBlob: ErrMessage = ErrMessage {
    raw: "Cannot create an expression index on an expression that returns a BLOB or TEXT. Please consider using CAST",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrFunctionalIndexFunctionIsNotAllowed`.
pub const ErrFunctionalIndexFunctionIsNotAllowed: ErrMessage = ErrMessage {
    raw: "Expression of expression index '%s' contains a disallowed function",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrFulltextFunctionalIndex`.
pub const ErrFulltextFunctionalIndex: ErrMessage = ErrMessage {
    raw: "Fulltext expression index is not supported",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSpatialFunctionalIndex`.
pub const ErrSpatialFunctionalIndex: ErrMessage = ErrMessage {
    raw: "Spatial expression index is not supported",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWrongKeyColumnFunctionalIndex`.
pub const ErrWrongKeyColumnFunctionalIndex: ErrMessage = ErrMessage {
    raw: "The used storage engine cannot index the expression '%s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrFunctionalIndexOnField`.
pub const ErrFunctionalIndexOnField: ErrMessage = ErrMessage {
    raw: "Expression index on a column is not supported. Consider using a regular index instead",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrFKIncompatibleColumns`.
pub const ErrFKIncompatibleColumns: ErrMessage = ErrMessage {
    raw: "Referencing column '%s' and referenced column '%s' in foreign key constraint '%s' are incompatible.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrFunctionalIndexRowValueIsNotAllowed`.
pub const ErrFunctionalIndexRowValueIsNotAllowed: ErrMessage = ErrMessage {
    raw: "Expression of expression index '%s' cannot refer to a row value",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInvalidLateralJoin`.
pub const ErrInvalidLateralJoin: ErrMessage = ErrMessage {
    raw: "Invalid use of LATERAL: %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrNonBooleanExprForCheckConstraint`.
pub const ErrNonBooleanExprForCheckConstraint: ErrMessage = ErrMessage {
    raw: "An expression of non-boolean type specified to a check constraint '%s'.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrColumnCheckConstraintReferencesOtherColumn`.
pub const ErrColumnCheckConstraintReferencesOtherColumn: ErrMessage = ErrMessage {
    raw: "Column check constraint '%s' references other column.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCheckConstraintNamedFunctionIsNotAllowed`.
pub const ErrCheckConstraintNamedFunctionIsNotAllowed: ErrMessage = ErrMessage {
    raw: "An expression of a check constraint '%s' contains disallowed function: %s.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCheckConstraintFunctionIsNotAllowed`.
pub const ErrCheckConstraintFunctionIsNotAllowed: ErrMessage = ErrMessage {
    raw: "An expression of a check constraint '%s' contains disallowed function.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCheckConstraintVariables`.
pub const ErrCheckConstraintVariables: ErrMessage = ErrMessage {
    raw: "An expression of a check constraint '%s' cannot refer to a user or system variable.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCheckConstraintRefersAutoIncrementColumn`.
pub const ErrCheckConstraintRefersAutoIncrementColumn: ErrMessage = ErrMessage {
    raw: "Check constraint '%s' cannot refer to an auto-increment column.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCheckConstraintViolated`.
pub const ErrCheckConstraintViolated: ErrMessage = ErrMessage {
    raw: "Check constraint '%s' is violated.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTableCheckConstraintReferUnknown`.
pub const ErrTableCheckConstraintReferUnknown: ErrMessage = ErrMessage {
    raw: "Check constraint '%s' refers to non-existing column '%s'.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCheckConstraintDupName`.
pub const ErrCheckConstraintDupName: ErrMessage = ErrMessage {
    raw: "Duplicate check constraint name '%s'.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCheckConstraintClauseUsingFKReferActionColumn`.
pub const ErrCheckConstraintClauseUsingFKReferActionColumn: ErrMessage = ErrMessage {
    raw: "Column '%s' cannot be used in a check constraint '%s': needed in a foreign key constraint referential action.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrDependentByFunctionalIndex`.
pub const ErrDependentByFunctionalIndex: ErrMessage = ErrMessage {
    raw: "Column '%s' has an expression index dependency and cannot be dropped or renamed",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrDependentByPartitionFunctional`.
pub const ErrDependentByPartitionFunctional: ErrMessage = ErrMessage {
    raw: "Column '%s' has a partitioning function dependency and cannot be dropped or renamed",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCannotConvertString`.
pub const ErrCannotConvertString: ErrMessage = ErrMessage {
    raw: "Cannot convert string '%.64s' from %s to %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInvalidJSONType`.
pub const ErrInvalidJSONType: ErrMessage = ErrMessage {
    raw: "Invalid JSON type in argument %d to function %s; an %s is required.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInvalidJSONValueForFuncIndex`.
pub const ErrInvalidJSONValueForFuncIndex: ErrMessage = ErrMessage {
    raw: "Invalid JSON value for CAST for expression index '%s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrJSONValueOutOfRangeForFuncIndex`.
pub const ErrJSONValueOutOfRangeForFuncIndex: ErrMessage = ErrMessage {
    raw: "Out of range JSON value for CAST for expression index '%s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrFunctionalIndexDataIsTooLong`.
pub const ErrFunctionalIndexDataIsTooLong: ErrMessage = ErrMessage {
    raw: "Data too long for expression index '%s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrFunctionalIndexNotApplicable`.
pub const ErrFunctionalIndexNotApplicable: ErrMessage = ErrMessage {
    raw: "Cannot use expression index '%s' due to type or collation conversion",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrUnsupportedConstraintCheck`.
pub const ErrUnsupportedConstraintCheck: ErrMessage = ErrMessage {
    raw: "%s is not supported",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrDynamicPrivilegeNotRegistered`.
pub const ErrDynamicPrivilegeNotRegistered: ErrMessage = ErrMessage {
    raw: "Dynamic privilege '%s' is not registered with the server.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrIllegalPrivilegeLevel`.
pub const ErrIllegalPrivilegeLevel: ErrMessage = ErrMessage {
    raw: "Illegal privilege level specified for %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCTERecursiveRequiresUnion`.
pub const ErrCTERecursiveRequiresUnion: ErrMessage = ErrMessage {
    raw: "Recursive Common Table Expression '%s' should contain a UNION",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCTERecursiveRequiresNonRecursiveFirst`.
pub const ErrCTERecursiveRequiresNonRecursiveFirst: ErrMessage = ErrMessage {
    raw: "Recursive Common Table Expression '%s' should have one or more non-recursive query blocks followed by one or more recursive ones",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCTERecursiveForbidsAggregation`.
pub const ErrCTERecursiveForbidsAggregation: ErrMessage = ErrMessage {
    raw: "Recursive Common Table Expression '%s' can contain neither aggregation nor window functions in recursive query block",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCTERecursiveForbiddenJoinOrder`.
pub const ErrCTERecursiveForbiddenJoinOrder: ErrMessage = ErrMessage {
    raw: "In recursive query block of Recursive Common Table Expression '%s', the recursive table must neither be in the right argument of a LEFT JOIN, nor be forced to be non-first with join order hints",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInvalidRequiresSingleReference`.
pub const ErrInvalidRequiresSingleReference: ErrMessage = ErrMessage {
    raw: "In recursive query block of Recursive Common Table Expression '%s', the recursive table must be referenced only once, and not in any subquery",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCTEMaxRecursionDepth`.
pub const ErrCTEMaxRecursionDepth: ErrMessage = ErrMessage {
    raw: "Recursive query aborted after %d iterations. Try increasing @@cte_max_recursion_depth to a larger value",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTableWithoutPrimaryKey`.
pub const ErrTableWithoutPrimaryKey: ErrMessage = ErrMessage {
    raw: "Unable to create or change a table without a primary key, when the system variable 'sql_require_primary_key' is set. Add a primary key to the table or unset this variable to avoid this message. Note that tables without a primary key can cause performance problems in row-based replication, so please consult your DBA before changing this setting.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrConstraintNotFound`.
pub const ErrConstraintNotFound: ErrMessage = ErrMessage {
    raw: "Constraint '%s' does not exist.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrDependentByCheckConstraint`.
pub const ErrDependentByCheckConstraint: ErrMessage = ErrMessage {
    raw: "Check constraint '%s' uses column '%s', hence column cannot be dropped or renamed.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrEngineAttributeNotSupported`.
pub const ErrEngineAttributeNotSupported: ErrMessage = ErrMessage {
    raw: "Storage engine does not support ENGINE_ATTRIBUTE.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrJSONInBooleanContext`.
pub const ErrJSONInBooleanContext: ErrMessage = ErrMessage {
    raw: "Evaluating a JSON value in SQL boolean context does an implicit comparison against JSON integer 0; if this is not what you want, consider converting JSON to a SQL numeric type with JSON_VALUE RETURNING",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSecondPasswordCannotBeEmpty`.
pub const ErrSecondPasswordCannotBeEmpty: ErrMessage = ErrMessage {
    raw: "Empty password can not be retained as second password for user '%-.64s'@'%-.64s'.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrPasswordCannotBeRetainedOnPluginChange`.
pub const ErrPasswordCannotBeRetainedOnPluginChange: ErrMessage = ErrMessage {
    raw: "Current password can not be retained for user '%-.64s'@'%-.64s' because authentication plugin is being changed.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCurrentPasswordCannotBeRetained`.
pub const ErrCurrentPasswordCannotBeRetained: ErrMessage = ErrMessage {
    raw: "Current password can not be retained for user '%-.64s'@'%-.64s' because new password is empty.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrOnlyOneDefaultPartionAllowed`.
pub const ErrOnlyOneDefaultPartionAllowed: ErrMessage = ErrMessage {
    raw: "Only one DEFAULT partition allowed",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWrongPartitionTypeExpectedSystemTime`.
pub const ErrWrongPartitionTypeExpectedSystemTime: ErrMessage = ErrMessage {
    raw: "Wrong partitioning type, expected type: `SYSTEM_TIME`",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSystemVersioningWrongPartitions`.
pub const ErrSystemVersioningWrongPartitions: ErrMessage = ErrMessage {
    raw: "Wrong Partitions: must have at least one HISTORY and exactly one last CURRENT",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSequenceRunOut`.
pub const ErrSequenceRunOut: ErrMessage = ErrMessage {
    raw: "Sequence '%-.64s.%-.64s' has run out",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSequenceInvalidData`.
pub const ErrSequenceInvalidData: ErrMessage = ErrMessage {
    raw: "Sequence '%-.64s.%-.64s' values are conflicting",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSequenceAccessFail`.
pub const ErrSequenceAccessFail: ErrMessage = ErrMessage {
    raw: "Sequence '%-.64s.%-.64s' access error",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrNotSequence`.
pub const ErrNotSequence: ErrMessage = ErrMessage {
    raw: "'%-.64s.%-.64s' is not a SEQUENCE",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrUnknownSequence`.
pub const ErrUnknownSequence: ErrMessage = ErrMessage {
    raw: "Unknown SEQUENCE: '%-.300s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWrongInsertIntoSequence`.
pub const ErrWrongInsertIntoSequence: ErrMessage = ErrMessage {
    raw: "Wrong INSERT into a SEQUENCE. One can only do single table INSERT into a sequence object (like with mysqldump).  If you want to change the SEQUENCE, use ALTER SEQUENCE instead.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSequenceInvalidTableStructure`.
pub const ErrSequenceInvalidTableStructure: ErrMessage = ErrMessage {
    raw: "Sequence '%-.64s.%-.64s' table structure is invalid (%s)",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrMemExceedThreshold`.
pub const ErrMemExceedThreshold: ErrMessage = ErrMessage {
    raw: "%s holds %dB memory, exceeds threshold %dB.%s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrForUpdateCantRetry`.
pub const ErrForUpdateCantRetry: ErrMessage = ErrMessage {
    raw: "[%d] can not retry select for update statement",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrAdminCheckTable`.
pub const ErrAdminCheckTable: ErrMessage = ErrMessage {
    raw: "TiDB admin check table failed.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrOptOnTemporaryTable`.
pub const ErrOptOnTemporaryTable: ErrMessage = ErrMessage {
    raw: "`%s` is unsupported on temporary tables.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrDropTableOnTemporaryTable`.
pub const ErrDropTableOnTemporaryTable: ErrMessage = ErrMessage {
    raw: "`drop global temporary table` can only drop global temporary table",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTxnTooLarge`.
pub const ErrTxnTooLarge: ErrMessage = ErrMessage {
    raw: "Transaction is too large, size: %d",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWriteConflictInTiDB`.
pub const ErrWriteConflictInTiDB: ErrMessage = ErrMessage {
    raw: "Write conflict, txnStartTS %d is stale",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInvalidPluginID`.
pub const ErrInvalidPluginID: ErrMessage = ErrMessage {
    raw: "Wrong plugin id: %s, valid plugin id is [name]-[version], and version should not contain '-'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInvalidPluginManifest`.
pub const ErrInvalidPluginManifest: ErrMessage = ErrMessage {
    raw: "Cannot read plugin %s's manifest",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInvalidPluginName`.
pub const ErrInvalidPluginName: ErrMessage = ErrMessage {
    raw: "Plugin load with %s but got wrong name %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInvalidPluginVersion`.
pub const ErrInvalidPluginVersion: ErrMessage = ErrMessage {
    raw: "Plugin load with %s but got %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrDuplicatePlugin`.
pub const ErrDuplicatePlugin: ErrMessage = ErrMessage {
    raw: "Plugin [%s] is redeclared",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInvalidPluginSysVarName`.
pub const ErrInvalidPluginSysVarName: ErrMessage = ErrMessage {
    raw: "Plugin %s's sysVar %s must start with its plugin name %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrRequireVersionCheckFail`.
pub const ErrRequireVersionCheckFail: ErrMessage = ErrMessage {
    raw: "Plugin %s require %s be %v but got %v",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrUnsupportedReloadPlugin`.
pub const ErrUnsupportedReloadPlugin: ErrMessage = ErrMessage {
    raw: "Plugin %s isn't loaded so cannot be reloaded",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrUnsupportedReloadPluginVar`.
pub const ErrUnsupportedReloadPluginVar: ErrMessage = ErrMessage {
    raw: "Reload plugin with different sysVar is unsupported %v",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTableLocked`.
pub const ErrTableLocked: ErrMessage = ErrMessage {
    raw: "Table '%s' was locked in %s by %v",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrNotExist`.
pub const ErrNotExist: ErrMessage = ErrMessage {
    raw: "Error: key not exist",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTxnRetryable`.
pub const ErrTxnRetryable: ErrMessage = ErrMessage {
    raw: "Error: KV error safe to retry %s ",
    redact_arg_pos: &[0],
};
/// Message metadata for `ErrCannotSetNilValue`.
pub const ErrCannotSetNilValue: ErrMessage = ErrMessage {
    raw: "can not set nil value",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInvalidTxn`.
pub const ErrInvalidTxn: ErrMessage = ErrMessage {
    raw: "invalid transaction",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrEntryTooLarge`.
pub const ErrEntryTooLarge: ErrMessage = ErrMessage {
    raw: "entry too large, the max entry size is %d, the size of data is %d",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrNotImplemented`.
pub const ErrNotImplemented: ErrMessage = ErrMessage {
    raw: "not implemented",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInfoSchemaExpired`.
pub const ErrInfoSchemaExpired: ErrMessage = ErrMessage {
    raw: "Information schema is out of date: schema failed to update in 1 lease, please make sure TiDB can connect to TiKV",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInfoSchemaChanged`.
pub const ErrInfoSchemaChanged: ErrMessage = ErrMessage {
    raw: "Information schema is changed during the execution of the statement(for example, table definition may be updated by other DDL ran in parallel). If you see this error often, try increasing `tidb_max_delta_schema_count`",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrBadNumber`.
pub const ErrBadNumber: ErrMessage = ErrMessage {
    raw: "Bad Number",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCastAsSignedOverflow`.
pub const ErrCastAsSignedOverflow: ErrMessage = ErrMessage {
    raw: "Cast to signed converted positive out-of-range integer to its negative complement",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCastNegIntAsUnsigned`.
pub const ErrCastNegIntAsUnsigned: ErrMessage = ErrMessage {
    raw: "Cast to unsigned converted negative integer to it's positive complement",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInvalidYearFormat`.
pub const ErrInvalidYearFormat: ErrMessage = ErrMessage {
    raw: "invalid year format",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInvalidYear`.
pub const ErrInvalidYear: ErrMessage = ErrMessage {
    raw: "invalid year",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrIncorrectDatetimeValue`.
pub const ErrIncorrectDatetimeValue: ErrMessage = ErrMessage {
    raw: "Incorrect datetime value: '%s'",
    redact_arg_pos: &[0],
};
/// Message metadata for `ErrInvalidTimeFormat`.
pub const ErrInvalidTimeFormat: ErrMessage = ErrMessage {
    raw: "invalid time format: '%v'",
    redact_arg_pos: &[0],
};
/// Message metadata for `ErrInvalidWeekModeFormat`.
pub const ErrInvalidWeekModeFormat: ErrMessage = ErrMessage {
    raw: "invalid week mode format: '%v'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrFieldGetDefaultFailed`.
pub const ErrFieldGetDefaultFailed: ErrMessage = ErrMessage {
    raw: "Field '%s' get default value fail",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrIndexOutBound`.
pub const ErrIndexOutBound: ErrMessage = ErrMessage {
    raw: "Index column %s offset out of bound, offset: %d, row: %v",
    redact_arg_pos: &[2],
};
/// Message metadata for `ErrUnsupportedOp`.
pub const ErrUnsupportedOp: ErrMessage = ErrMessage {
    raw: "operation not supported",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrRowNotFound`.
pub const ErrRowNotFound: ErrMessage = ErrMessage {
    raw: "can not find the row: %s",
    redact_arg_pos: &[0],
};
/// Message metadata for `ErrTableStateCantNone`.
pub const ErrTableStateCantNone: ErrMessage = ErrMessage {
    raw: "table %s can't be in none state",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrColumnStateCantNone`.
pub const ErrColumnStateCantNone: ErrMessage = ErrMessage {
    raw: "column %s can't be in none state",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrColumnStateNonPublic`.
pub const ErrColumnStateNonPublic: ErrMessage = ErrMessage {
    raw: "can not use non-public column",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrIndexStateCantNone`.
pub const ErrIndexStateCantNone: ErrMessage = ErrMessage {
    raw: "index %s can't be in none state",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInvalidRecordKey`.
pub const ErrInvalidRecordKey: ErrMessage = ErrMessage {
    raw: "invalid record key",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrUnsupportedValueForVar`.
pub const ErrUnsupportedValueForVar: ErrMessage = ErrMessage {
    raw: "variable '%s' does not yet support value: %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrUnsupportedIsolationLevel`.
pub const ErrUnsupportedIsolationLevel: ErrMessage = ErrMessage {
    raw: "The isolation level '%s' is not supported. Set tidb_skip_isolation_level_check=1 to skip this error",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInvalidDDLWorker`.
pub const ErrInvalidDDLWorker: ErrMessage = ErrMessage {
    raw: "Invalid DDL worker",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrUnsupportedDDLOperation`.
pub const ErrUnsupportedDDLOperation: ErrMessage = ErrMessage {
    raw: "Unsupported %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrNotOwner`.
pub const ErrNotOwner: ErrMessage = ErrMessage {
    raw: "TiDB server is not a DDL owner",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCantDecodeRecord`.
pub const ErrCantDecodeRecord: ErrMessage = ErrMessage {
    raw: "Cannot decode %s value, because %v",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInvalidDDLJob`.
pub const ErrInvalidDDLJob: ErrMessage = ErrMessage {
    raw: "Invalid DDL job",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInvalidDDLJobFlag`.
pub const ErrInvalidDDLJobFlag: ErrMessage = ErrMessage {
    raw: "Invalid DDL job flag",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWaitReorgTimeout`.
pub const ErrWaitReorgTimeout: ErrMessage = ErrMessage {
    raw: "Timeout waiting for data reorganization",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInvalidStoreVersion`.
pub const ErrInvalidStoreVersion: ErrMessage = ErrMessage {
    raw: "Invalid storage current version: %d",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrUnknownTypeLength`.
pub const ErrUnknownTypeLength: ErrMessage = ErrMessage {
    raw: "Unknown length for type %d",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrUnknownFractionLength`.
pub const ErrUnknownFractionLength: ErrMessage = ErrMessage {
    raw: "Unknown length for type %d and fraction %d",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInvalidDDLJobVersion`.
pub const ErrInvalidDDLJobVersion: ErrMessage = ErrMessage {
    raw: "Version %d of DDL job is greater than current one: %d",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInvalidSplitRegionRanges`.
pub const ErrInvalidSplitRegionRanges: ErrMessage = ErrMessage {
    raw: "Failed to split region ranges: %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrReorgPanic`.
pub const ErrReorgPanic: ErrMessage = ErrMessage {
    raw: "Reorg worker panic",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInvalidDDLState`.
pub const ErrInvalidDDLState: ErrMessage = ErrMessage {
    raw: "Invalid %s state: %v",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCancelledDDLJob`.
pub const ErrCancelledDDLJob: ErrMessage = ErrMessage {
    raw: "Cancelled DDL job",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrRepairTable`.
pub const ErrRepairTable: ErrMessage = ErrMessage {
    raw: "Failed to repair table: %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrLoadPrivilege`.
pub const ErrLoadPrivilege: ErrMessage = ErrMessage {
    raw: "Load privilege table fail: %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInvalidPrivilegeType`.
pub const ErrInvalidPrivilegeType: ErrMessage = ErrMessage {
    raw: "unknown privilege type %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrUnknownFieldType`.
pub const ErrUnknownFieldType: ErrMessage = ErrMessage {
    raw: "unknown field type",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInvalidSequence`.
pub const ErrInvalidSequence: ErrMessage = ErrMessage {
    raw: "invalid sequence",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInvalidType`.
pub const ErrInvalidType: ErrMessage = ErrMessage {
    raw: "invalid type",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCantGetValidID`.
pub const ErrCantGetValidID: ErrMessage = ErrMessage {
    raw: "Cannot get a valid auto-ID when retrying the statement",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCantSetToNull`.
pub const ErrCantSetToNull: ErrMessage = ErrMessage {
    raw: "cannot set variable to null",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSnapshotTooOld`.
pub const ErrSnapshotTooOld: ErrMessage = ErrMessage {
    raw: "snapshot is older than GC safe point %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInvalidTableID`.
pub const ErrInvalidTableID: ErrMessage = ErrMessage {
    raw: "invalid TableID",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInvalidAutoRandom`.
pub const ErrInvalidAutoRandom: ErrMessage = ErrMessage {
    raw: "Invalid auto random: %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInvalidHashKeyFlag`.
pub const ErrInvalidHashKeyFlag: ErrMessage = ErrMessage {
    raw: "invalid encoded hash key flag",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInvalidListIndex`.
pub const ErrInvalidListIndex: ErrMessage = ErrMessage {
    raw: "invalid list index",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInvalidListMetaData`.
pub const ErrInvalidListMetaData: ErrMessage = ErrMessage {
    raw: "invalid list meta data",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWriteOnSnapshot`.
pub const ErrWriteOnSnapshot: ErrMessage = ErrMessage {
    raw: "write on snapshot",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInvalidKey`.
pub const ErrInvalidKey: ErrMessage = ErrMessage {
    raw: "invalid key",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInvalidIndexKey`.
pub const ErrInvalidIndexKey: ErrMessage = ErrMessage {
    raw: "invalid index key",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrDataInconsistent`.
pub const ErrDataInconsistent: ErrMessage = ErrMessage {
    raw: "data inconsistency in table: %s, index: %s, handle: %s, index-values:%#v != record-values:%#v",
    redact_arg_pos: &[2, 3, 4],
};
/// Message metadata for `ErrDDLReorgElementNotExist`.
pub const ErrDDLReorgElementNotExist: ErrMessage = ErrMessage {
    raw: "DDL reorg element does not exist",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrDDLJobNotFound`.
pub const ErrDDLJobNotFound: ErrMessage = ErrMessage {
    raw: "DDL Job:%v not found",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCancelFinishedDDLJob`.
pub const ErrCancelFinishedDDLJob: ErrMessage = ErrMessage {
    raw: "This job:%v is finished, so can't be cancelled",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCannotCancelDDLJob`.
pub const ErrCannotCancelDDLJob: ErrMessage = ErrMessage {
    raw: "This job:%v is almost finished, can't be cancelled now",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrUnknownAllocatorType`.
pub const ErrUnknownAllocatorType: ErrMessage = ErrMessage {
    raw: "Invalid allocator type",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrAutoRandReadFailed`.
pub const ErrAutoRandReadFailed: ErrMessage = ErrMessage {
    raw: "Failed to read auto-random value from storage engine",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInvalidIncrementAndOffset`.
pub const ErrInvalidIncrementAndOffset: ErrMessage = ErrMessage {
    raw: "Invalid auto_increment settings: auto_increment_increment: %d, auto_increment_offset: %d, both of them must be in range [1..65535]",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrDataInconsistentMismatchCount`.
pub const ErrDataInconsistentMismatchCount: ErrMessage = ErrMessage {
    raw: "data inconsistency in table: %s, index: %s, index-count:%d != record-count:%d",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrDataInconsistentMismatchIndex`.
pub const ErrDataInconsistentMismatchIndex: ErrMessage = ErrMessage {
    raw: "data inconsistency in table: %s, index: %s, col: %s, handle: %#v, index-values:%#v != record-values:%#v, compare err:%#v",
    redact_arg_pos: &[3, 4, 5, 6],
};
/// Message metadata for `ErrInconsistentRowValue`.
pub const ErrInconsistentRowValue: ErrMessage = ErrMessage {
    raw: "writing inconsistent data in table: %s, expected-values:{%s} != record-values:{%s}",
    redact_arg_pos: &[1, 2],
};
/// Message metadata for `ErrInconsistentHandle`.
pub const ErrInconsistentHandle: ErrMessage = ErrMessage {
    raw: "writing inconsistent data in table: %s, index: %s, index-handle:%#v != record-handle:%#v, index: %#v, record: %#v",
    redact_arg_pos: &[2, 3, 4, 5],
};
/// Message metadata for `ErrInconsistentIndexedValue`.
pub const ErrInconsistentIndexedValue: ErrMessage = ErrMessage {
    raw: "writing inconsistent data in table: %s, index: %s, col: %s, indexed-value:{%s} != record-value:{%s}",
    redact_arg_pos: &[3, 4],
};
/// Message metadata for `ErrAssertionFailed`.
pub const ErrAssertionFailed: ErrMessage = ErrMessage {
    raw: "assertion failed: key: %s, assertion: %s, start_ts: %v, existing start ts: %v, existing commit ts: %v",
    redact_arg_pos: &[0],
};
/// Message metadata for `ErrInstanceScope`.
pub const ErrInstanceScope: ErrMessage = ErrMessage {
    raw: "modifying %s will require SET GLOBAL in a future version of TiDB",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrNonTransactionalJobFailure`.
pub const ErrNonTransactionalJobFailure: ErrMessage = ErrMessage {
    raw: "non-transactional job failed, job id: %d, total jobs: %d. job range: [%s, %s], job sql: %s, err: %v",
    redact_arg_pos: &[2, 3, 4],
};
/// Message metadata for `ErrSettingNoopVariable`.
pub const ErrSettingNoopVariable: ErrMessage = ErrMessage {
    raw: "setting %s has no effect in TiDB",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrGettingNoopVariable`.
pub const ErrGettingNoopVariable: ErrMessage = ErrMessage {
    raw: "variable %s has no effect in TiDB",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCannotMigrateSession`.
pub const ErrCannotMigrateSession: ErrMessage = ErrMessage {
    raw: "cannot migrate the current session: %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrLazyUniquenessCheckFailure`.
pub const ErrLazyUniquenessCheckFailure: ErrMessage = ErrMessage {
    raw: "transaction aborted because lazy uniqueness check is enabled and an error occurred: %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrUnsupportedColumnInTTLConfig`.
pub const ErrUnsupportedColumnInTTLConfig: ErrMessage = ErrMessage {
    raw: "Field '%-.192s' is of a not supported type for TTL config, expect DATETIME, DATE or TIMESTAMP",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTTLColumnCannotDrop`.
pub const ErrTTLColumnCannotDrop: ErrMessage = ErrMessage {
    raw: "Cannot drop column '%-.192s': needed in TTL config",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSetTTLOptionForNonTTLTable`.
pub const ErrSetTTLOptionForNonTTLTable: ErrMessage = ErrMessage {
    raw: "Cannot set %s on a table without TTL config",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTempTableNotAllowedWithTTL`.
pub const ErrTempTableNotAllowedWithTTL: ErrMessage = ErrMessage {
    raw: "Set TTL for temporary table is not allowed",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrUnsupportedTTLReferencedByFK`.
pub const ErrUnsupportedTTLReferencedByFK: ErrMessage = ErrMessage {
    raw: "Set TTL for a table referenced by foreign key is not allowed",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrUnsupportedPrimaryKeyTypeWithTTL`.
pub const ErrUnsupportedPrimaryKeyTypeWithTTL: ErrMessage = ErrMessage {
    raw: "Unsupported clustered primary key type FLOAT/DOUBLE for TTL",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrLoadDataFromServerDisk`.
pub const ErrLoadDataFromServerDisk: ErrMessage = ErrMessage {
    raw: "Don't support load data from tidb-server's disk. Or if you want to load local data via client, the path of INFILE '%s' needs to specify the clause of LOCAL first",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrLoadParquetFromLocal`.
pub const ErrLoadParquetFromLocal: ErrMessage = ErrMessage {
    raw: "Do not support loading parquet files from local. Please try to load the parquet files from the cloud storage",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrLoadDataEmptyPath`.
pub const ErrLoadDataEmptyPath: ErrMessage = ErrMessage {
    raw: "The value of INFILE must not be empty when LOAD DATA from LOCAL",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrLoadDataUnsupportedFormat`.
pub const ErrLoadDataUnsupportedFormat: ErrMessage = ErrMessage {
    raw: "The FORMAT '%s' is not supported",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrLoadDataInvalidURI`.
pub const ErrLoadDataInvalidURI: ErrMessage = ErrMessage {
    raw: "The URI of %s is invalid. Reason: %s. Please provide a valid URI, such as 's3://import/test.csv?access-key={your_access_key_id ID}&secret-access-key={your_secret_access_key}&session-token={your_session_token}'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrLoadDataCantAccess`.
pub const ErrLoadDataCantAccess: ErrMessage = ErrMessage {
    raw: "Access to the %s has been denied. Reason: %s. Please check the URI, access key and secret access key are correct",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrLoadDataCantRead`.
pub const ErrLoadDataCantRead: ErrMessage = ErrMessage {
    raw: "Failed to read source files. Reason: %s. %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrLoadDataWrongFormatConfig`.
pub const ErrLoadDataWrongFormatConfig: ErrMessage = ErrMessage {
    raw: "",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrUnknownOption`.
pub const ErrUnknownOption: ErrMessage = ErrMessage {
    raw: "Unknown option %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInvalidOptionVal`.
pub const ErrInvalidOptionVal: ErrMessage = ErrMessage {
    raw: "Invalid option value for %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrDuplicateOption`.
pub const ErrDuplicateOption: ErrMessage = ErrMessage {
    raw: "Option %s specified more than once",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrLoadDataUnsupportedOption`.
pub const ErrLoadDataUnsupportedOption: ErrMessage = ErrMessage {
    raw: "Unsupported option %s for %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrLoadDataDuplicateKeyConflict`.
pub const ErrLoadDataDuplicateKeyConflict: ErrMessage = ErrMessage {
    raw: "Duplicate key conflict found. Please resolve conflicts in the input dataset, or set on_duplicate_key to a strategy that can handle conflicts, for example 'capture'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrLoadDataJobNotFound`.
pub const ErrLoadDataJobNotFound: ErrMessage = ErrMessage {
    raw: "Job ID %d doesn't exist",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrLoadDataInvalidOperation`.
pub const ErrLoadDataInvalidOperation: ErrMessage = ErrMessage {
    raw: "The current job status cannot perform the operation. %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrLoadDataLocalUnsupportedOption`.
pub const ErrLoadDataLocalUnsupportedOption: ErrMessage = ErrMessage {
    raw: "Unsupported option for LOAD DATA LOCAL INFILE: %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrLoadDataPreCheckFailed`.
pub const ErrLoadDataPreCheckFailed: ErrMessage = ErrMessage {
    raw: "PreCheck failed: %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrMemoryExceedForQuery`.
pub const ErrMemoryExceedForQuery: ErrMessage = ErrMessage {
    raw: "Your query has been cancelled due to exceeding the allowed memory limit for a single SQL query. Please try narrowing your query scope or increase the tidb_mem_quota_query limit and try again.[conn=%d]",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrMemoryExceedForInstance`.
pub const ErrMemoryExceedForInstance: ErrMessage = ErrMessage {
    raw: "Your query has been cancelled due to exceeding the allowed memory limit for the tidb-server instance and this query is currently using the most memory. Please try narrowing your query scope or increase the tidb_server_memory_limit and try again.[conn=%d]",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrDeleteNotFoundColumn`.
pub const ErrDeleteNotFoundColumn: ErrMessage = ErrMessage {
    raw: "Delete can not find column %s for table %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrKeyTooLarge`.
pub const ErrKeyTooLarge: ErrMessage = ErrMessage {
    raw: "key is too large, the size of given key is %d",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrProtectedTableMode`.
pub const ErrProtectedTableMode: ErrMessage = ErrMessage {
    raw: "Table %s is in mode %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInvalidTableModeSet`.
pub const ErrInvalidTableModeSet: ErrMessage = ErrMessage {
    raw: "Invalid mode set from (or by default) %s to %s for table %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrForbiddenDDL`.
pub const ErrForbiddenDDL: ErrMessage = ErrMessage {
    raw: "%s is forbidden",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrHTTPServiceError`.
pub const ErrHTTPServiceError: ErrMessage = ErrMessage {
    raw: "HTTP request failed with status %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWarnOptimizerHintInvalidInteger`.
pub const ErrWarnOptimizerHintInvalidInteger: ErrMessage = ErrMessage {
    raw: "integer value is out of range in '%s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWarnOptimizerHintUnsupportedHint`.
pub const ErrWarnOptimizerHintUnsupportedHint: ErrMessage = ErrMessage {
    raw: "Optimizer hint %s is not supported by TiDB and is ignored",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWarnOptimizerHintInvalidToken`.
pub const ErrWarnOptimizerHintInvalidToken: ErrMessage = ErrMessage {
    raw: "Cannot use %s '%s' (tok = %d) in an optimizer hint",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWarnMemoryQuotaOverflow`.
pub const ErrWarnMemoryQuotaOverflow: ErrMessage = ErrMessage {
    raw: "Max value of MEMORY_QUOTA is %d bytes, ignore this invalid limit",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWarnOptimizerHintParseError`.
pub const ErrWarnOptimizerHintParseError: ErrMessage = ErrMessage {
    raw: "Optimizer hint syntax error at %v",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWarnOptimizerHintWrongPos`.
pub const ErrWarnOptimizerHintWrongPos: ErrMessage = ErrMessage {
    raw: "Optimizer hint can only be followed by certain keywords like SELECT, INSERT, etc.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSequenceUnsupportedTableOption`.
pub const ErrSequenceUnsupportedTableOption: ErrMessage = ErrMessage {
    raw: "Unsupported sequence table-option %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrColumnTypeUnsupportedNextValue`.
pub const ErrColumnTypeUnsupportedNextValue: ErrMessage = ErrMessage {
    raw: "Unsupported sequence default value for column type '%s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrAddColumnWithSequenceAsDefault`.
pub const ErrAddColumnWithSequenceAsDefault: ErrMessage = ErrMessage {
    raw: "Unsupported using sequence as default value in add column '%s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrUnsupportedType`.
pub const ErrUnsupportedType: ErrMessage = ErrMessage {
    raw: "Unsupported type %T",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrAnalyzeMissIndex`.
pub const ErrAnalyzeMissIndex: ErrMessage = ErrMessage {
    raw: "Index '%s' in field list does not exist in table '%s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrAnalyzeMissColumn`.
pub const ErrAnalyzeMissColumn: ErrMessage = ErrMessage {
    raw: "Column '%s' in ANALYZE column option does not exist in table '%s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCartesianProductUnsupported`.
pub const ErrCartesianProductUnsupported: ErrMessage = ErrMessage {
    raw: "Cartesian product is unsupported",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrPreparedStmtNotFound`.
pub const ErrPreparedStmtNotFound: ErrMessage = ErrMessage {
    raw: "Prepared statement not found",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWrongParamCount`.
pub const ErrWrongParamCount: ErrMessage = ErrMessage {
    raw: "Wrong parameter count",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSchemaChanged`.
pub const ErrSchemaChanged: ErrMessage = ErrMessage {
    raw: "Schema has changed",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrUnknownPlan`.
pub const ErrUnknownPlan: ErrMessage = ErrMessage {
    raw: "Unknown plan",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrPrepareMulti`.
pub const ErrPrepareMulti: ErrMessage = ErrMessage {
    raw: "Can not prepare multiple statements",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrPrepareDDL`.
pub const ErrPrepareDDL: ErrMessage = ErrMessage {
    raw: "Can not prepare DDL statements with parameters",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrResultIsEmpty`.
pub const ErrResultIsEmpty: ErrMessage = ErrMessage {
    raw: "Result is empty",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrBuildExecutor`.
pub const ErrBuildExecutor: ErrMessage = ErrMessage {
    raw: "Failed to build executor",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrBatchInsertFail`.
pub const ErrBatchInsertFail: ErrMessage = ErrMessage {
    raw: "Batch insert failed, please clean the table and try again.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrGetStartTS`.
pub const ErrGetStartTS: ErrMessage = ErrMessage {
    raw: "Can not get start ts",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrPrivilegeCheckFail`.
pub const ErrPrivilegeCheckFail: ErrMessage = ErrMessage {
    raw: "privilege check for '%s' fail",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInvalidWildCard`.
pub const ErrInvalidWildCard: ErrMessage = ErrMessage {
    raw: "Wildcard fields without any table name appears in wrong place",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrMixOfGroupFuncAndFieldsIncompatible`.
pub const ErrMixOfGroupFuncAndFieldsIncompatible: ErrMessage = ErrMessage {
    raw: "In aggregated query without GROUP BY, expression #%d of SELECT list contains nonaggregated column '%s'; this is incompatible with sql_mode=only_full_group_by",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrUnsupportedSecondArgumentType`.
pub const ErrUnsupportedSecondArgumentType: ErrMessage = ErrMessage {
    raw: "JSON_OBJECTAGG: unsupported second argument type %v",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrColumnNotMatched`.
pub const ErrColumnNotMatched: ErrMessage = ErrMessage {
    raw: "Load data: unmatched columns",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrLockExpire`.
pub const ErrLockExpire: ErrMessage = ErrMessage {
    raw: "TTL manager has timed out, pessimistic locks may expire, please commit or rollback this transaction",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTableOptionUnionUnsupported`.
pub const ErrTableOptionUnionUnsupported: ErrMessage = ErrMessage {
    raw: "CREATE/ALTER table with union option is not supported",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTableOptionInsertMethodUnsupported`.
pub const ErrTableOptionInsertMethodUnsupported: ErrMessage = ErrMessage {
    raw: "CREATE/ALTER table with insert method option is not supported",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrUserLockDeadlock`.
pub const ErrUserLockDeadlock: ErrMessage = ErrMessage {
    raw: "Deadlock found when trying to get user-level lock; try rolling back transaction/releasing locks and restarting lock acquisition.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrUserLockWrongName`.
pub const ErrUserLockWrongName: ErrMessage = ErrMessage {
    raw: "Incorrect user-level lock name '%s'.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrBRIEBackupFailed`.
pub const ErrBRIEBackupFailed: ErrMessage = ErrMessage {
    raw: "Backup failed: %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrBRIERestoreFailed`.
pub const ErrBRIERestoreFailed: ErrMessage = ErrMessage {
    raw: "Restore failed: %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrBRIEImportFailed`.
pub const ErrBRIEImportFailed: ErrMessage = ErrMessage {
    raw: "Import failed: %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrBRIEExportFailed`.
pub const ErrBRIEExportFailed: ErrMessage = ErrMessage {
    raw: "Export failed: %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrBRJobNotFound`.
pub const ErrBRJobNotFound: ErrMessage = ErrMessage {
    raw: "BRIE Job %d not found",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInvalidTableSample`.
pub const ErrInvalidTableSample: ErrMessage = ErrMessage {
    raw: "Invalid TABLESAMPLE: %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrJSONObjectKeyTooLong`.
pub const ErrJSONObjectKeyTooLong: ErrMessage = ErrMessage {
    raw: "TiDB does not yet support JSON objects with the key length >= 65536",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrPartitionStatsMissing`.
pub const ErrPartitionStatsMissing: ErrMessage = ErrMessage {
    raw: "Build global-level stats failed due to missing partition-level stats: %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrPartitionColumnStatsMissing`.
pub const ErrPartitionColumnStatsMissing: ErrMessage = ErrMessage {
    raw: "Build global-level stats failed due to missing partition-level column stats: %s, please run analyze table to refresh columns of all partitions",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrDDLSetting`.
pub const ErrDDLSetting: ErrMessage = ErrMessage {
    raw: "Error happened when %s DDL: %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrIngestFailed`.
pub const ErrIngestFailed: ErrMessage = ErrMessage {
    raw: "Ingest failed: %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrIngestCheckEnvFailed`.
pub const ErrIngestCheckEnvFailed: ErrMessage = ErrMessage {
    raw: "Check ingest environment failed: %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrNotSupportedWithSem`.
pub const ErrNotSupportedWithSem: ErrMessage = ErrMessage {
    raw: "Feature '%s' is not supported when security enhanced mode is enabled",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrPlacementPolicyCheck`.
pub const ErrPlacementPolicyCheck: ErrMessage = ErrMessage {
    raw: "Placement policy didn't meet the constraint, reason: %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrMultiStatementDisabled`.
pub const ErrMultiStatementDisabled: ErrMessage = ErrMessage {
    raw: "client has multi-statement capability disabled. Run SET GLOBAL tidb_multi_statement_mode='ON' after you understand the security risk",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrAsOf`.
pub const ErrAsOf: ErrMessage = ErrMessage {
    raw: "invalid as of timestamp: %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrVariableNoLongerSupported`.
pub const ErrVariableNoLongerSupported: ErrMessage = ErrMessage {
    raw: "option '%s' is no longer supported. Reason: %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInvalidAttributesSpec`.
pub const ErrInvalidAttributesSpec: ErrMessage = ErrMessage {
    raw: "Invalid attributes: %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrPlacementPolicyExists`.
pub const ErrPlacementPolicyExists: ErrMessage = ErrMessage {
    raw: "Placement policy '%-.192s' already exists",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrPlacementPolicyNotExists`.
pub const ErrPlacementPolicyNotExists: ErrMessage = ErrMessage {
    raw: "Unknown placement policy '%-.192s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrPlacementPolicyWithDirectOption`.
pub const ErrPlacementPolicyWithDirectOption: ErrMessage = ErrMessage {
    raw: "Placement policy '%s' can't co-exist with direct placement options",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrPlacementPolicyInUse`.
pub const ErrPlacementPolicyInUse: ErrMessage = ErrMessage {
    raw: "Placement policy '%-.192s' is still in use",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrMaskingPolicyExists`.
pub const ErrMaskingPolicyExists: ErrMessage = ErrMessage {
    raw: "masking policy already exists",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrMaskingPolicyNotExists`.
pub const ErrMaskingPolicyNotExists: ErrMessage = ErrMessage {
    raw: "masking policy doesn't exist",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrMaskingPolicyExprInvalidColumn`.
pub const ErrMaskingPolicyExprInvalidColumn: ErrMessage = ErrMessage {
    raw: "masking policy expression can only reference the target column '%-.64s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrOptOnCacheTable`.
pub const ErrOptOnCacheTable: ErrMessage = ErrMessage {
    raw: "'%s' is unsupported on cache tables.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrResourceGroupExists`.
pub const ErrResourceGroupExists: ErrMessage = ErrMessage {
    raw: "Resource group '%-.192s' already exists",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrResourceGroupNotExists`.
pub const ErrResourceGroupNotExists: ErrMessage = ErrMessage {
    raw: "Unknown resource group '%-.192s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrResourceGroupInvalidForRole`.
pub const ErrResourceGroupInvalidForRole: ErrMessage = ErrMessage {
    raw: "Cannot set resource group for a role",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrColumnInChange`.
pub const ErrColumnInChange: ErrMessage = ErrMessage {
    raw: "column %s id %d does not exist, this column may have been updated by other DDL ran in parallel",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrResourceGroupSupportDisabled`.
pub const ErrResourceGroupSupportDisabled: ErrMessage = ErrMessage {
    raw: "Resource control feature is disabled. Run `SET GLOBAL tidb_enable_resource_control='on'` to enable the feature",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrResourceGroupConfigUnavailable`.
pub const ErrResourceGroupConfigUnavailable: ErrMessage = ErrMessage {
    raw: "Resource group configuration is unavailable",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrResourceGroupThrottled`.
pub const ErrResourceGroupThrottled: ErrMessage = ErrMessage {
    raw: "Exceeded resource group quota limitation",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrResourceGroupQueryRunawayInterrupted`.
pub const ErrResourceGroupQueryRunawayInterrupted: ErrMessage = ErrMessage {
    raw: "Query execution was interrupted, identified as runaway query [%s]",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrResourceGroupQueryRunawayQuarantine`.
pub const ErrResourceGroupQueryRunawayQuarantine: ErrMessage = ErrMessage {
    raw: "Quarantined and interrupted because of being in runaway watch list",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrResourceGroupInvalidBackgroundTaskName`.
pub const ErrResourceGroupInvalidBackgroundTaskName: ErrMessage = ErrMessage {
    raw: "Unknown background task name '%-.192s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrQueryExecStopped`.
pub const ErrQueryExecStopped: ErrMessage = ErrMessage {
    raw: "Query execution was stopped by the global memory arbitrator [reason=%s] [conn=%d]",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrEngineAttributeInvalidFormat`.
pub const ErrEngineAttributeInvalidFormat: ErrMessage = ErrMessage {
    raw: "Invalid engine attribute format: %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrStorageClassInvalidSpec`.
pub const ErrStorageClassInvalidSpec: ErrMessage = ErrMessage {
    raw: "Invalid storage class: %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrModifyColumnReferencedByPartialCondition`.
pub const ErrModifyColumnReferencedByPartialCondition: ErrMessage = ErrMessage {
    raw: "Cannot drop, change or modify column '%s': it is referenced in partial index '%s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCheckPartialIndexWithoutFastCheck`.
pub const ErrCheckPartialIndexWithoutFastCheck: ErrMessage = ErrMessage {
    raw: "Validation of partial indexes requires tidb_enable_fast_table_check=ON",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrMaxKeysReadExceeded`.
pub const ErrMaxKeysReadExceeded: ErrMessage = ErrMessage {
    raw: "tidb_max_keys_read limit exceeded",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrPDServerTimeout`.
pub const ErrPDServerTimeout: ErrMessage = ErrMessage {
    raw: "PD server timeout: %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTiKVServerTimeout`.
pub const ErrTiKVServerTimeout: ErrMessage = ErrMessage {
    raw: "TiKV server timeout",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTiKVServerBusy`.
pub const ErrTiKVServerBusy: ErrMessage = ErrMessage {
    raw: "TiKV server is busy",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTiFlashServerTimeout`.
pub const ErrTiFlashServerTimeout: ErrMessage = ErrMessage {
    raw: "TiFlash server timeout",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTiFlashServerBusy`.
pub const ErrTiFlashServerBusy: ErrMessage = ErrMessage {
    raw: "TiFlash server is busy",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTiFlashBackfillIndex`.
pub const ErrTiFlashBackfillIndex: ErrMessage = ErrMessage {
    raw: "TiFlash backfill index failed: %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrResolveLockTimeout`.
pub const ErrResolveLockTimeout: ErrMessage = ErrMessage {
    raw: "Resolve lock timeout",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrRegionUnavailable`.
pub const ErrRegionUnavailable: ErrMessage = ErrMessage {
    raw: "Region is unavailable",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTxnAbortedByGC`.
pub const ErrTxnAbortedByGC: ErrMessage = ErrMessage {
    raw: "GC life time is shorter than transaction duration, transaction start ts is %v (%v), txn safe point is %v (%v)",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWriteConflict`.
pub const ErrWriteConflict: ErrMessage = ErrMessage {
    raw: "Write conflict, txnStartTS=%d, conflictStartTS=%d, conflictCommitTS=%d, key=%s%s%s%s, reason=%s",
    redact_arg_pos: &[3, 4, 5, 6],
};
/// Message metadata for `ErrTiKVStoreLimit`.
pub const ErrTiKVStoreLimit: ErrMessage = ErrMessage {
    raw: "Store token is up to the limit, store id = %d",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrPrometheusAddrIsNotSet`.
pub const ErrPrometheusAddrIsNotSet: ErrMessage = ErrMessage {
    raw: "Prometheus address is not set in PD and etcd",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTiKVStaleCommand`.
pub const ErrTiKVStaleCommand: ErrMessage = ErrMessage {
    raw: "TiKV server reports stale command",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTiKVMaxTimestampNotSynced`.
pub const ErrTiKVMaxTimestampNotSynced: ErrMessage = ErrMessage {
    raw: "TiKV max timestamp is not synced",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCannotPauseDDLJob`.
pub const ErrCannotPauseDDLJob: ErrMessage = ErrMessage {
    raw: "Job [%v] can't be paused: %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCannotResumeDDLJob`.
pub const ErrCannotResumeDDLJob: ErrMessage = ErrMessage {
    raw: "Job [%v] can't be resumed: %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrPausedDDLJob`.
pub const ErrPausedDDLJob: ErrMessage = ErrMessage {
    raw: "Job [%v] has already been paused",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrBDRRestrictedDDL`.
pub const ErrBDRRestrictedDDL: ErrMessage = ErrMessage {
    raw: "The operation is not allowed while the bdr role of this cluster is set to %s.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrDDLAutoPausedByKVDiskFull`.
pub const ErrDDLAutoPausedByKVDiskFull: ErrMessage = ErrMessage {
    raw: "Job [%v] has been paused by TiDB because a storage node does not have enough disk space: %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrGlobalIndexNotExplicitlySet`.
pub const ErrGlobalIndexNotExplicitlySet: ErrMessage = ErrMessage {
    raw: "Global Index is needed for index '%-.192s', since the unique index is not including all partitioning columns, and GLOBAL is not given as IndexOption",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWarnGlobalIndexNeedManuallyAnalyze`.
pub const ErrWarnGlobalIndexNeedManuallyAnalyze: ErrMessage = ErrMessage {
    raw: "Auto analyze is not effective for index '%-.192s', need analyze manually",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrTimeStampInDSTTransition`.
pub const ErrTimeStampInDSTTransition: ErrMessage = ErrMessage {
    raw: "Timestamp is not valid, since it is in Daylight Saving Time transition '%s' for time zone '%s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInvalidAffinityOption`.
pub const ErrInvalidAffinityOption: ErrMessage = ErrMessage {
    raw: "Invalid AFFINITY %s",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrUserPrefixMismatch`.
pub const ErrUserPrefixMismatch: ErrMessage = ErrMessage {
    raw: "User name prefix does not match the assigned keyspace.",
    redact_arg_pos: &[],
};

/// Complete TiDB message catalog in source order.
pub const CATALOG: &[CatalogEntry] = &[
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
        name: "ErrIndexRebuild",
        code: errcode::ErrIndexRebuild,
        message: ErrIndexRebuild,
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
    CatalogEntry {
        name: "ErrSpWrongNoOfArgs",
        code: errcode::ErrSpWrongNoOfArgs,
        message: ErrSpWrongNoOfArgs,
    },
    CatalogEntry {
        name: "ErrSpCondMismatch",
        code: errcode::ErrSpCondMismatch,
        message: ErrSpCondMismatch,
    },
    CatalogEntry {
        name: "ErrSpNoreturn",
        code: errcode::ErrSpNoreturn,
        message: ErrSpNoreturn,
    },
    CatalogEntry {
        name: "ErrSpNoreturnend",
        code: errcode::ErrSpNoreturnend,
        message: ErrSpNoreturnend,
    },
    CatalogEntry {
        name: "ErrSpBadCursorQuery",
        code: errcode::ErrSpBadCursorQuery,
        message: ErrSpBadCursorQuery,
    },
    CatalogEntry {
        name: "ErrSpBadCursorSelect",
        code: errcode::ErrSpBadCursorSelect,
        message: ErrSpBadCursorSelect,
    },
    CatalogEntry {
        name: "ErrSpCursorMismatch",
        code: errcode::ErrSpCursorMismatch,
        message: ErrSpCursorMismatch,
    },
    CatalogEntry {
        name: "ErrSpCursorAlreadyOpen",
        code: errcode::ErrSpCursorAlreadyOpen,
        message: ErrSpCursorAlreadyOpen,
    },
    CatalogEntry {
        name: "ErrSpCursorNotOpen",
        code: errcode::ErrSpCursorNotOpen,
        message: ErrSpCursorNotOpen,
    },
    CatalogEntry {
        name: "ErrSpUndeclaredVar",
        code: errcode::ErrSpUndeclaredVar,
        message: ErrSpUndeclaredVar,
    },
    CatalogEntry {
        name: "ErrSpWrongNoOfFetchArgs",
        code: errcode::ErrSpWrongNoOfFetchArgs,
        message: ErrSpWrongNoOfFetchArgs,
    },
    CatalogEntry {
        name: "ErrSpFetchNoData",
        code: errcode::ErrSpFetchNoData,
        message: ErrSpFetchNoData,
    },
    CatalogEntry {
        name: "ErrSpDupParam",
        code: errcode::ErrSpDupParam,
        message: ErrSpDupParam,
    },
    CatalogEntry {
        name: "ErrSpDupVar",
        code: errcode::ErrSpDupVar,
        message: ErrSpDupVar,
    },
    CatalogEntry {
        name: "ErrSpDupCond",
        code: errcode::ErrSpDupCond,
        message: ErrSpDupCond,
    },
    CatalogEntry {
        name: "ErrSpDupCurs",
        code: errcode::ErrSpDupCurs,
        message: ErrSpDupCurs,
    },
    CatalogEntry {
        name: "ErrSpCantAlter",
        code: errcode::ErrSpCantAlter,
        message: ErrSpCantAlter,
    },
    CatalogEntry {
        name: "ErrSpSubselectNyi",
        code: errcode::ErrSpSubselectNyi,
        message: ErrSpSubselectNyi,
    },
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

/// Finds the source entry registered for `code`.
#[must_use]
pub fn entry_by_code(code: u16) -> Option<&'static CatalogEntry> {
    CATALOG.iter().find(|entry| entry.code == code)
}

/// Finds the source message registered for `code`.
#[must_use]
pub fn message_by_code(code: u16) -> Option<&'static ErrMessage> {
    entry_by_code(code).map(|entry| &entry.message)
}
