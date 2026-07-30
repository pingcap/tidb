// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");

//! Message catalog constants, part 4 of 4 (see `errname/mod.rs`).

use crate::ErrMessage;

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
