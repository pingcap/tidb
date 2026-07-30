// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");

//! Message catalog constants, part 3 of 3 (see `errname/mod.rs`).

use crate::ErrMessage;

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
/// Message metadata for `ErrSlaveIgnoreServerIDs`.
pub const ErrSlaveIgnoreServerIDs: ErrMessage = ErrMessage {
    raw: "The requested server id %d clashes with the slave startup option --replicate-same-server-id",
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
/// Message metadata for `ErrBinlogUnsafeSystemTable`.
pub const ErrBinlogUnsafeSystemTable: ErrMessage = ErrMessage {
    raw: "The statement is unsafe because it uses the general log, slow query log, or performanceSchema table(s). This is unsafe because system tables may differ on slaves.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrBinlogUnsafeAutoincColumns`.
pub const ErrBinlogUnsafeAutoincColumns: ErrMessage = ErrMessage {
    raw: "Statement is unsafe because it invokes a trigger or a stored function that inserts into an AUTOINCREMENT column. Inserted values cannot be logged correctly.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrBinlogUnsafeUdf`.
pub const ErrBinlogUnsafeUdf: ErrMessage = ErrMessage {
    raw: "Statement is unsafe because it uses a UDF which may not return the same value on the slave.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrBinlogUnsafeSystemVariable`.
pub const ErrBinlogUnsafeSystemVariable: ErrMessage = ErrMessage {
    raw: "Statement is unsafe because it uses a system variable that may have a different value on the slave.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrBinlogUnsafeSystemFunction`.
pub const ErrBinlogUnsafeSystemFunction: ErrMessage = ErrMessage {
    raw: "Statement is unsafe because it uses a system function that may return a different value on the slave.",
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
/// Message metadata for `ErrSlaveConversionFailed`.
pub const ErrSlaveConversionFailed: ErrMessage = ErrMessage {
    raw: "Column %d of table '%-.192s.%-.192s' cannot be converted from type '%-.32s' to type '%-.32s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSlaveCantCreateConversion`.
pub const ErrSlaveCantCreateConversion: ErrMessage = ErrMessage {
    raw: "Can't create conversion table for table '%-.192s.%-.192s'",
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
    redact_arg_pos: &[],
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
    raw: "Access denied for user '%-.48s'@'%-.64s'",
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
/// Message metadata for `ErrSlaveHeartbeatValueOutOfRangeMin`.
pub const ErrSlaveHeartbeatValueOutOfRangeMin: ErrMessage = ErrMessage {
    raw: "The requested value for the heartbeat period is less than 1 millisecond. The value is reset to 0, meaning that heartbeating will effectively be disabled.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSlaveHeartbeatValueOutOfRangeMax`.
pub const ErrSlaveHeartbeatValueOutOfRangeMax: ErrMessage = ErrMessage {
    raw: "The requested value for the heartbeat period exceeds the value of `slaveNetTimeout' seconds. A sensible value for the period should be less than the timeout.",
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
/// Message metadata for `ErrBinlogUnsafeInsertIgnoreSelect`.
pub const ErrBinlogUnsafeInsertIgnoreSelect: ErrMessage = ErrMessage {
    raw: "INSERT IGNORE... SELECT is unsafe because the order in which rows are retrieved by the SELECT determines which (if any) rows are ignored. This order cannot be predicted and may differ on master and the slave.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrBinlogUnsafeInsertSelectUpdate`.
pub const ErrBinlogUnsafeInsertSelectUpdate: ErrMessage = ErrMessage {
    raw: "INSERT... SELECT... ON DUPLICATE KEY UPDATE is unsafe because the order in which rows are retrieved by the SELECT determines which (if any) rows are updated. This order cannot be predicted and may differ on master and the slave.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrBinlogUnsafeReplaceSelect`.
pub const ErrBinlogUnsafeReplaceSelect: ErrMessage = ErrMessage {
    raw: "REPLACE... SELECT is unsafe because the order in which rows are retrieved by the SELECT determines which (if any) rows are replaced. This order cannot be predicted and may differ on master and the slave.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrBinlogUnsafeCreateIgnoreSelect`.
pub const ErrBinlogUnsafeCreateIgnoreSelect: ErrMessage = ErrMessage {
    raw: "CREATE... IGNORE SELECT is unsafe because the order in which rows are retrieved by the SELECT determines which (if any) rows are ignored. This order cannot be predicted and may differ on master and the slave.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrBinlogUnsafeCreateReplaceSelect`.
pub const ErrBinlogUnsafeCreateReplaceSelect: ErrMessage = ErrMessage {
    raw: "CREATE... REPLACE SELECT is unsafe because the order in which rows are retrieved by the SELECT determines which (if any) rows are replaced. This order cannot be predicted and may differ on master and the slave.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrBinlogUnsafeUpdateIgnore`.
pub const ErrBinlogUnsafeUpdateIgnore: ErrMessage = ErrMessage {
    raw: "UPDATE IGNORE is unsafe because the order in which rows are updated determines which (if any) rows are ignored. This order cannot be predicted and may differ on master and the slave.",
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
/// Message metadata for `ErrBinlogUnsafeWriteAutoincSelect`.
pub const ErrBinlogUnsafeWriteAutoincSelect: ErrMessage = ErrMessage {
    raw: "Statements writing to a table with an auto-increment column after selecting from another table are unsafe because the order in which rows are retrieved determines what (if any) rows will be written. This order cannot be predicted and may differ on master and the slave.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrBinlogUnsafeCreateSelectAutoinc`.
pub const ErrBinlogUnsafeCreateSelectAutoinc: ErrMessage = ErrMessage {
    raw: "CREATE TABLE... SELECT...  on a table with an auto-increment column is unsafe because the order in which rows are retrieved by the SELECT determines which (if any) rows are inserted. This order cannot be predicted and may differ on master and the slave.",
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
/// Message metadata for `ErrMasterDelayValueOutOfRange`.
pub const ErrMasterDelayValueOutOfRange: ErrMessage = ErrMessage {
    raw: "The requested value %d for the master delay exceeds the maximum %d",
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
/// Message metadata for `ErrMtsFeatureIsNotSupported`.
pub const ErrMtsFeatureIsNotSupported: ErrMessage = ErrMessage {
    raw: "%s is not supported in multi-threaded slave mode. %s",
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
/// Message metadata for `ErrInsecureChangeMaster`.
pub const ErrInsecureChangeMaster: ErrMessage = ErrMessage {
    raw: "Storing MySQL user name or password information in the master.info repository is not secure and is therefore not recommended. Please see the MySQL Manual for more about this issue and possible alternatives.",
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
/// Message metadata for `ErrSQLthreadWithSecureSlave`.
pub const ErrSQLthreadWithSecureSlave: ErrMessage = ErrMessage {
    raw: "Setting authentication options is not possible when only the Slave SQL Thread is being started.",
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
/// Message metadata for `ErrBadSlaveAutoPosition`.
pub const ErrBadSlaveAutoPosition: ErrMessage = ErrMessage {
    raw: "Parameters MASTERLOGFILE, MASTERLOGPOS, RELAYLOGFILE and RELAYLOGPOS cannot be set when MASTERAUTOPOSITION is active.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrAutoPositionRequiresGtidModeOn`.
pub const ErrAutoPositionRequiresGtidModeOn: ErrMessage = ErrMessage {
    raw:
        "CHANGE MASTER TO MASTERAUTOPOSITION = 1 can only be executed when @@GLOBAL.GTIDMODE = ON.",
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
/// Message metadata for `ErrGtidModeRequiresBinlog`.
pub const ErrGtidModeRequiresBinlog: ErrMessage = ErrMessage {
    raw: "@@GLOBAL.GTIDMODE = ON or UPGRADESTEP1 or UPGRADESTEP2 requires --log-bin and --log-slave-updates.",
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
/// Message metadata for `ErrMasterHasPurgedRequiredGtids`.
pub const ErrMasterHasPurgedRequiredGtids: ErrMessage = ErrMessage {
    raw: "The slave is connecting using CHANGE MASTER TO MASTERAUTOPOSITION = 1, but the master has purged binary logs containing GTIDs that the slave requires.",
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
/// Message metadata for `ErrSlaveConfiguration`.
pub const ErrSlaveConfiguration: ErrMessage = ErrMessage {
    raw: "Slave is not configured or failed to initialize properly. You must at least set --server-id to enable either a master or a slave. Additional error messages can be found in the MySQL error log.",
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
/// Message metadata for `ErrMtsChangeMasterCantRunWithGaps`.
pub const ErrMtsChangeMasterCantRunWithGaps: ErrMessage = ErrMessage {
    raw: "CHANGE MASTER cannot be executed when the slave was stopped with an error or killed in MTS mode. Consider using RESET SLAVE or START SLAVE UNTIL.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrMtsRecoveryFailure`.
pub const ErrMtsRecoveryFailure: ErrMessage = ErrMessage {
    raw: "Cannot recover after SLAVE errored out in parallel execution mode. Additional error messages can be found in the MySQL error log.",
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
/// Message metadata for `ErrSlaveSilentRetryTransaction`.
pub const ErrSlaveSilentRetryTransaction: ErrMessage = ErrMessage {
    raw: "Slave must silently retry current transaction",
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
    raw: "You must SET PASSWORD before executing this statement",
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
/// Message metadata for `ErrGtidNextTypeUndefinedGroup`.
pub const ErrGtidNextTypeUndefinedGroup: ErrMessage = ErrMessage {
    raw: "When @@SESSION.GTIDNEXT is set to a GTID, you must explicitly set it again after a COMMIT or ROLLBACK. If you see this error message in the slave SQL thread, it means that a table in the current transaction is transactional on the master and non-transactional on the slave. In a client connection, it means that you executed SET @@SESSION.GTIDNEXT before a transaction and forgot to set @@SESSION.GTIDNEXT to a different identifier or to 'AUTOMATIC' after COMMIT or ROLLBACK. Current @@SESSION.GTIDNEXT is '%s'.",
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
/// Message metadata for `ErrSQLSlaveSkipCounterNotSettableInGtidMode`.
pub const ErrSQLSlaveSkipCounterNotSettableInGtidMode: ErrMessage = ErrMessage {
    raw: "sqlSlaveSkipCounter can not be set when the server is running with @@GLOBAL.GTIDMODE = ON. Instead, for each transaction that you want to skip, generate an empty transaction with the same GTID as the transaction",
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
    redact_arg_pos: &[],
};
/// Message metadata for `ErrGeneratedColumnFunctionIsNotAllowed`.
pub const ErrGeneratedColumnFunctionIsNotAllowed: ErrMessage = ErrMessage {
    raw: "Expression of generated column '%s' contains a disallowed function.",
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
/// Message metadata for `ErrIncorrectType`.
pub const ErrIncorrectType: ErrMessage = ErrMessage {
    raw: "Incorrect type for argument %s in function %s.",
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
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInvalidJSONTextInParam`.
pub const ErrInvalidJSONTextInParam: ErrMessage = ErrMessage {
    raw: "Invalid JSON text in argument %d to function %s: \"%s\" at position %d.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInvalidJSONPath`.
pub const ErrInvalidJSONPath: ErrMessage = ErrMessage {
    raw: "Invalid JSON path expression %s.",
    redact_arg_pos: &[],
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
/// Message metadata for `ErrInvalidJSONPathWildcard`.
pub const ErrInvalidJSONPathWildcard: ErrMessage = ErrMessage {
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
/// Message metadata for `ErrWindowExplainJson`.
pub const ErrWindowExplainJson: ErrMessage = ErrMessage {
    raw: "To get information about window functions use EXPLAIN FORMAT=JSON",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWindowFunctionIgnoresFrame`.
pub const ErrWindowFunctionIgnoresFrame: ErrMessage = ErrMessage {
    raw: "Window function '%s' ignores the frame clause of window '%s' and aggregates over the whole partition",
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
/// Message metadata for `ErrDataTruncatedFunctionalIndex`.
pub const ErrDataTruncatedFunctionalIndex: ErrMessage = ErrMessage {
    raw: "Data truncated for functional index '%s' at row %d",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrDataOutOfRangeFunctionalIndex`.
pub const ErrDataOutOfRangeFunctionalIndex: ErrMessage = ErrMessage {
    raw: "Value is out of range for functional index '%s' at row %d",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrFunctionalIndexOnJsonOrGeometryFunction`.
pub const ErrFunctionalIndexOnJsonOrGeometryFunction: ErrMessage = ErrMessage {
    raw: "Cannot create a functional index on a function that returns a JSON or GEOMETRY value",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrFunctionalIndexRefAutoIncrement`.
pub const ErrFunctionalIndexRefAutoIncrement: ErrMessage = ErrMessage {
    raw: "Functional index '%s' cannot refer to an auto-increment column",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrCannotDropColumnFunctionalIndex`.
pub const ErrCannotDropColumnFunctionalIndex: ErrMessage = ErrMessage {
    raw: "Cannot drop column '%s' because it is used by a functional index. In order to drop the column, you must remove the functional index",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrFunctionalIndexPrimaryKey`.
pub const ErrFunctionalIndexPrimaryKey: ErrMessage = ErrMessage {
    raw: "The primary key cannot be a functional index",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrFunctionalIndexOnLob`.
pub const ErrFunctionalIndexOnLob: ErrMessage = ErrMessage {
    raw: "Cannot create a functional index on an expression that returns a BLOB or TEXT. Please consider using CAST",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrFunctionalIndexFunctionIsNotAllowed`.
pub const ErrFunctionalIndexFunctionIsNotAllowed: ErrMessage = ErrMessage {
    raw: "Expression of functional index '%s' contains a disallowed function",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrFulltextFunctionalIndex`.
pub const ErrFulltextFunctionalIndex: ErrMessage = ErrMessage {
    raw: "Fulltext functional index is not supported",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrSpatialFunctionalIndex`.
pub const ErrSpatialFunctionalIndex: ErrMessage = ErrMessage {
    raw: "Spatial functional index is not supported",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrWrongKeyColumnFunctionalIndex`.
pub const ErrWrongKeyColumnFunctionalIndex: ErrMessage = ErrMessage {
    raw: "The used storage engine cannot index the expression '%s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrFunctionalIndexOnField`.
pub const ErrFunctionalIndexOnField: ErrMessage = ErrMessage {
    raw: "Functional index on a column is not supported. Consider using a regular index instead",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrFKIncompatibleColumns`.
pub const ErrFKIncompatibleColumns: ErrMessage = ErrMessage {
    raw: "Referencing column '%s' and referenced column '%s' in foreign key constraint '%s' are incompatible.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrFunctionalIndexRowValueIsNotAllowed`.
pub const ErrFunctionalIndexRowValueIsNotAllowed: ErrMessage = ErrMessage {
    raw: "Expression of functional index '%s' cannot refer to a row value",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrDependentByFunctionalIndex`.
pub const ErrDependentByFunctionalIndex: ErrMessage = ErrMessage {
    raw: "Column '%s' has a functional index dependency and cannot be dropped or renamed",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInvalidJSONType`.
pub const ErrInvalidJSONType: ErrMessage = ErrMessage {
    raw: "Invalid JSON type in argument %d to function %s; an %s is required.",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrInvalidJsonValueForFuncIndex`.
pub const ErrInvalidJsonValueForFuncIndex: ErrMessage = ErrMessage {
    raw: "Invalid JSON value for CAST for functional index '%s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrJsonValueOutOfRangeForFuncIndex`.
pub const ErrJsonValueOutOfRangeForFuncIndex: ErrMessage = ErrMessage {
    raw: "Out of range JSON value for CAST for functional index '%s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrFunctionalIndexDataIsTooLong`.
pub const ErrFunctionalIndexDataIsTooLong: ErrMessage = ErrMessage {
    raw: "Data too long for functional index '%s'",
    redact_arg_pos: &[],
};
/// Message metadata for `ErrFunctionalIndexNotApplicable`.
pub const ErrFunctionalIndexNotApplicable: ErrMessage = ErrMessage {
    raw: "Cannot use functional index '%s' due to type or collation conversion",
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
