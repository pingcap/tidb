// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");

//! Message catalog constants, part 2 of 4 (see `errname/mod.rs`).

use crate::ErrMessage;

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
