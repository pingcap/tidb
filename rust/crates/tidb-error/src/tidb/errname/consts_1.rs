// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");

//! Message catalog constants, part 1 of 4 (see `errname/mod.rs`).

use crate::ErrMessage;

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
