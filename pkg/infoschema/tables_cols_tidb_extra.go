// Copyright 2016 PingCAP, Inc.
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

package infoschema

import (
	"github.com/pingcap/tidb/pkg/ddl/resourcegroup"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/session/txninfo"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/deadlockhistory"
)

var tableStorageStatsCols = []columnInfo{
	{name: "TABLE_SCHEMA", tp: mysql.TypeVarchar, size: 64},
	{name: "TABLE_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "TABLE_ID", tp: mysql.TypeLonglong, size: 21},
	{name: "PEER_COUNT", tp: mysql.TypeLonglong, size: 21},
	{name: "REGION_COUNT", tp: mysql.TypeLonglong, size: 21, comment: "The region count of single replica of the table"},
	{name: "EMPTY_REGION_COUNT", tp: mysql.TypeLonglong, size: 21, comment: "The region count of single replica of the table"},
	{name: "TABLE_SIZE", tp: mysql.TypeLonglong, size: 21, comment: "The disk usage(MB) of single replica of the table, if the table size is empty or less than 1MB, it would show 1MB "},
	{name: "TABLE_KEYS", tp: mysql.TypeLonglong, size: 21, comment: "The count of keys of single replica of the table"},
}

var tableTableTiFlashTablesCols = []columnInfo{
	// TiFlash DB and Table Name contains the internal KeyspaceID,
	// which is not suitable for presenting to users. Commented out.
	// {name: "DATABASE", tp: mysql.TypeVarchar, size: 64},
	// {name: "TABLE", tp: mysql.TypeVarchar, size: 64},
	{name: "TIDB_DATABASE", tp: mysql.TypeVarchar, size: 64},
	{name: "TIDB_TABLE", tp: mysql.TypeVarchar, size: 64},
	{name: "TABLE_ID", tp: mysql.TypeLonglong, size: 21},
	{name: "IS_TOMBSTONE", tp: mysql.TypeLonglong, size: 21},
	{name: "COLUMN_COUNT", tp: mysql.TypeLonglong, size: 21},
	{name: "SEGMENT_COUNT", tp: mysql.TypeLonglong, size: 21},
	{name: "TOTAL_ROWS", tp: mysql.TypeLonglong, size: 21},
	{name: "TOTAL_SIZE", tp: mysql.TypeLonglong, size: 21},
	{name: "TOTAL_DELETE_RANGES", tp: mysql.TypeLonglong, size: 21},
	{name: "DELTA_RATE_ROWS", tp: mysql.TypeDouble, size: 64},
	{name: "DELTA_RATE_SEGMENTS", tp: mysql.TypeDouble, size: 64},
	{name: "DELTA_PLACED_RATE", tp: mysql.TypeDouble, size: 64},
	{name: "DELTA_CACHE_SIZE", tp: mysql.TypeLonglong, size: 21},
	{name: "DELTA_CACHE_ALLOC_SIZE", tp: mysql.TypeLonglong, size: 21},
	{name: "DELTA_CACHE_RATE", tp: mysql.TypeDouble, size: 64},
	{name: "DELTA_CACHE_WASTED_RATE", tp: mysql.TypeDouble, size: 64},
	{name: "DELTA_INDEX_SIZE", tp: mysql.TypeLonglong, size: 21},
	{name: "AVG_SEGMENT_ROWS", tp: mysql.TypeDouble, size: 64},
	{name: "AVG_SEGMENT_SIZE", tp: mysql.TypeDouble, size: 64},
	{name: "DELTA_COUNT", tp: mysql.TypeLonglong, size: 21},
	{name: "TOTAL_DELTA_ROWS", tp: mysql.TypeLonglong, size: 21},
	{name: "TOTAL_DELTA_SIZE", tp: mysql.TypeLonglong, size: 21},
	{name: "AVG_DELTA_ROWS", tp: mysql.TypeDouble, size: 64},
	{name: "AVG_DELTA_SIZE", tp: mysql.TypeDouble, size: 64},
	{name: "AVG_DELTA_DELETE_RANGES", tp: mysql.TypeDouble, size: 64},
	{name: "STABLE_COUNT", tp: mysql.TypeLonglong, size: 21},
	{name: "TOTAL_STABLE_ROWS", tp: mysql.TypeLonglong, size: 21},
	{name: "TOTAL_STABLE_SIZE", tp: mysql.TypeLonglong, size: 21},
	{name: "TOTAL_STABLE_SIZE_ON_DISK", tp: mysql.TypeLonglong, size: 21},
	{name: "AVG_STABLE_ROWS", tp: mysql.TypeDouble, size: 64},
	{name: "AVG_STABLE_SIZE", tp: mysql.TypeDouble, size: 64},
	{name: "TOTAL_PACK_COUNT_IN_DELTA", tp: mysql.TypeLonglong, size: 21},
	{name: "MAX_PACK_COUNT_IN_DELTA", tp: mysql.TypeLonglong, size: 21},
	{name: "AVG_PACK_COUNT_IN_DELTA", tp: mysql.TypeDouble, size: 64},
	{name: "AVG_PACK_ROWS_IN_DELTA", tp: mysql.TypeDouble, size: 64},
	{name: "AVG_PACK_SIZE_IN_DELTA", tp: mysql.TypeDouble, size: 64},
	{name: "TOTAL_PACK_COUNT_IN_STABLE", tp: mysql.TypeLonglong, size: 21},
	{name: "AVG_PACK_COUNT_IN_STABLE", tp: mysql.TypeDouble, size: 64},
	{name: "AVG_PACK_ROWS_IN_STABLE", tp: mysql.TypeDouble, size: 64},
	{name: "AVG_PACK_SIZE_IN_STABLE", tp: mysql.TypeDouble, size: 64},
	{name: "STORAGE_STABLE_NUM_SNAPSHOTS", tp: mysql.TypeLonglong, size: 21},
	{name: "STORAGE_STABLE_OLDEST_SNAPSHOT_LIFETIME", tp: mysql.TypeDouble, size: 64},
	{name: "STORAGE_STABLE_OLDEST_SNAPSHOT_THREAD_ID", tp: mysql.TypeLonglong, size: 21},
	{name: "STORAGE_STABLE_OLDEST_SNAPSHOT_TRACING_ID", tp: mysql.TypeVarchar, size: 128},
	{name: "STORAGE_DELTA_NUM_SNAPSHOTS", tp: mysql.TypeLonglong, size: 21},
	{name: "STORAGE_DELTA_OLDEST_SNAPSHOT_LIFETIME", tp: mysql.TypeDouble, size: 64},
	{name: "STORAGE_DELTA_OLDEST_SNAPSHOT_THREAD_ID", tp: mysql.TypeLonglong, size: 21},
	{name: "STORAGE_DELTA_OLDEST_SNAPSHOT_TRACING_ID", tp: mysql.TypeVarchar, size: 128},
	{name: "STORAGE_META_NUM_SNAPSHOTS", tp: mysql.TypeLonglong, size: 21},
	{name: "STORAGE_META_OLDEST_SNAPSHOT_LIFETIME", tp: mysql.TypeDouble, size: 64},
	{name: "STORAGE_META_OLDEST_SNAPSHOT_THREAD_ID", tp: mysql.TypeLonglong, size: 21},
	{name: "STORAGE_META_OLDEST_SNAPSHOT_TRACING_ID", tp: mysql.TypeVarchar, size: 128},
	{name: "BACKGROUND_TASKS_LENGTH", tp: mysql.TypeLonglong, size: 21},
	{name: "TIFLASH_INSTANCE", tp: mysql.TypeVarchar, size: 64},
}

var tableTableTiFlashSegmentsCols = []columnInfo{
	// TiFlash DB and Table Name contains the internal KeyspaceID,
	// which is not suitable for presenting to users. Commented out.
	// {name: "DATABASE", tp: mysql.TypeVarchar, size: 64},
	// {name: "TABLE", tp: mysql.TypeVarchar, size: 64},
	{name: "TIDB_DATABASE", tp: mysql.TypeVarchar, size: 64},
	{name: "TIDB_TABLE", tp: mysql.TypeVarchar, size: 64},
	{name: "TABLE_ID", tp: mysql.TypeLonglong, size: 21},
	{name: "IS_TOMBSTONE", tp: mysql.TypeLonglong, size: 21},
	{name: "SEGMENT_ID", tp: mysql.TypeLonglong, size: 21},
	{name: "RANGE", tp: mysql.TypeVarchar, size: 64},
	{name: "EPOCH", tp: mysql.TypeLonglong, size: 21},
	{name: "ROWS", tp: mysql.TypeLonglong, size: 21},
	{name: "SIZE", tp: mysql.TypeLonglong, size: 21},
	{name: "DELTA_RATE", tp: mysql.TypeDouble, size: 64},
	{name: "DELTA_MEMTABLE_ROWS", tp: mysql.TypeLonglong, size: 21},
	{name: "DELTA_MEMTABLE_SIZE", tp: mysql.TypeLonglong, size: 21},
	{name: "DELTA_MEMTABLE_COLUMN_FILES", tp: mysql.TypeLonglong, size: 21},
	{name: "DELTA_MEMTABLE_DELETE_RANGES", tp: mysql.TypeLonglong, size: 21},
	{name: "DELTA_PERSISTED_PAGE_ID", tp: mysql.TypeLonglong, size: 21},
	{name: "DELTA_PERSISTED_ROWS", tp: mysql.TypeLonglong, size: 21},
	{name: "DELTA_PERSISTED_SIZE", tp: mysql.TypeLonglong, size: 21},
	{name: "DELTA_PERSISTED_COLUMN_FILES", tp: mysql.TypeLonglong, size: 21},
	{name: "DELTA_PERSISTED_DELETE_RANGES", tp: mysql.TypeLonglong, size: 21},
	{name: "DELTA_CACHE_SIZE", tp: mysql.TypeLonglong, size: 21},
	{name: "DELTA_CACHE_ALLOC_SIZE", tp: mysql.TypeLonglong, size: 21},
	{name: "DELTA_INDEX_SIZE", tp: mysql.TypeLonglong, size: 21},
	{name: "STABLE_PAGE_ID", tp: mysql.TypeLonglong, size: 21},
	{name: "STABLE_ROWS", tp: mysql.TypeLonglong, size: 21},
	{name: "STABLE_SIZE", tp: mysql.TypeLonglong, size: 21},
	{name: "STABLE_DMFILES", tp: mysql.TypeLonglong, size: 21},
	{name: "STABLE_DMFILES_ID_0", tp: mysql.TypeLonglong, size: 21},
	{name: "STABLE_DMFILES_ROWS", tp: mysql.TypeLonglong, size: 21},
	{name: "STABLE_DMFILES_SIZE", tp: mysql.TypeLonglong, size: 21},
	{name: "STABLE_DMFILES_SIZE_ON_DISK", tp: mysql.TypeLonglong, size: 21},
	{name: "STABLE_DMFILES_PACKS", tp: mysql.TypeLonglong, size: 21},
	{name: "TIFLASH_INSTANCE", tp: mysql.TypeVarchar, size: 64},
}

var tableTiFlashIndexesCols = []columnInfo{
	{name: "TIDB_DATABASE", tp: mysql.TypeVarchar, size: 64},
	{name: "TIDB_TABLE", tp: mysql.TypeVarchar, size: 64},
	{name: "TABLE_ID", tp: mysql.TypeLonglong, size: 21},
	{name: "COLUMN_NAME", tp: mysql.TypeVarchar, size: 64}, // Supplied by TiDB
	{name: "INDEX_NAME", tp: mysql.TypeVarchar, size: 64},  // Supplied by TiDB
	{name: "COLUMN_ID", tp: mysql.TypeLonglong, size: 64},
	{name: "INDEX_ID", tp: mysql.TypeLonglong, size: 21},
	{name: "INDEX_KIND", tp: mysql.TypeVarchar, size: 64},
	{name: "ROWS_STABLE_INDEXED", tp: mysql.TypeLonglong, size: 64},
	{name: "ROWS_STABLE_NOT_INDEXED", tp: mysql.TypeLonglong, size: 64},
	{name: "ROWS_DELTA_INDEXED", tp: mysql.TypeLonglong, size: 64},
	{name: "ROWS_DELTA_NOT_INDEXED", tp: mysql.TypeLonglong, size: 64},
	{name: "ERROR_MESSAGE", tp: mysql.TypeVarchar, size: 1024},
	{name: "TIFLASH_INSTANCE", tp: mysql.TypeVarchar, size: 64},
}

var tableClientErrorsSummaryGlobalCols = []columnInfo{
	{name: "ERROR_NUMBER", tp: mysql.TypeLonglong, size: 21, flag: mysql.NotNullFlag},
	{name: "ERROR_MESSAGE", tp: mysql.TypeVarchar, size: 1024, flag: mysql.NotNullFlag},
	{name: "ERROR_COUNT", tp: mysql.TypeLonglong, size: 21, flag: mysql.NotNullFlag},
	{name: "WARNING_COUNT", tp: mysql.TypeLonglong, size: 21, flag: mysql.NotNullFlag},
	{name: "FIRST_SEEN", tp: mysql.TypeTimestamp, size: 26},
	{name: "LAST_SEEN", tp: mysql.TypeTimestamp, size: 26},
}

var tableClientErrorsSummaryByUserCols = []columnInfo{
	{name: "USER", tp: mysql.TypeVarchar, size: 64, flag: mysql.NotNullFlag},
	{name: "ERROR_NUMBER", tp: mysql.TypeLonglong, size: 21, flag: mysql.NotNullFlag},
	{name: "ERROR_MESSAGE", tp: mysql.TypeVarchar, size: 1024, flag: mysql.NotNullFlag},
	{name: "ERROR_COUNT", tp: mysql.TypeLonglong, size: 21, flag: mysql.NotNullFlag},
	{name: "WARNING_COUNT", tp: mysql.TypeLonglong, size: 21, flag: mysql.NotNullFlag},
	{name: "FIRST_SEEN", tp: mysql.TypeTimestamp, size: 26},
	{name: "LAST_SEEN", tp: mysql.TypeTimestamp, size: 26},
}

var tableClientErrorsSummaryByHostCols = []columnInfo{
	{name: "HOST", tp: mysql.TypeVarchar, size: 255, flag: mysql.NotNullFlag},
	{name: "ERROR_NUMBER", tp: mysql.TypeLonglong, size: 21, flag: mysql.NotNullFlag},
	{name: "ERROR_MESSAGE", tp: mysql.TypeVarchar, size: 1024, flag: mysql.NotNullFlag},
	{name: "ERROR_COUNT", tp: mysql.TypeLonglong, size: 21, flag: mysql.NotNullFlag},
	{name: "WARNING_COUNT", tp: mysql.TypeLonglong, size: 21, flag: mysql.NotNullFlag},
	{name: "FIRST_SEEN", tp: mysql.TypeTimestamp, size: 26},
	{name: "LAST_SEEN", tp: mysql.TypeTimestamp, size: 26},
}

var tableTiDBTrxCols = []columnInfo{
	{name: txninfo.IDStr, tp: mysql.TypeLonglong, size: 21, flag: mysql.PriKeyFlag | mysql.NotNullFlag | mysql.UnsignedFlag},
	{name: txninfo.StartTimeStr, tp: mysql.TypeTimestamp, decimal: 6, size: 26, comment: "Start time of the transaction"},
	{name: txninfo.CurrentSQLDigestStr, tp: mysql.TypeVarchar, size: 64, comment: "Digest of the sql the transaction are currently running"},
	{name: txninfo.CurrentSQLDigestTextStr, tp: mysql.TypeBlob, size: types.UnspecifiedLength, comment: "The normalized sql the transaction are currently running"},
	{name: txninfo.StateStr, tp: mysql.TypeEnum, size: 16, enumElems: txninfo.TxnRunningStateStrs, comment: "Current running state of the transaction"},
	{name: txninfo.WaitingStartTimeStr, tp: mysql.TypeTimestamp, decimal: 6, size: 26, comment: "Current lock waiting's start time"},
	{name: txninfo.MemBufferKeysStr, tp: mysql.TypeLonglong, size: 21, comment: "How many entries are in MemDB"},
	{name: txninfo.MemBufferBytesStr, tp: mysql.TypeLonglong, size: 21, comment: "MemDB used memory"},
	{name: txninfo.SessionIDStr, tp: mysql.TypeLonglong, size: 21, flag: mysql.UnsignedFlag, comment: "Which session this transaction belongs to"},
	{name: txninfo.UserStr, tp: mysql.TypeVarchar, size: 16, comment: "The user who open this session"},
	{name: txninfo.DBStr, tp: mysql.TypeVarchar, size: 64, comment: "The schema this transaction works on"},
	{name: txninfo.AllSQLDigestsStr, tp: mysql.TypeBlob, size: types.UnspecifiedLength, comment: "A list of the digests of SQL statements that the transaction has executed"},
	{name: txninfo.RelatedTableIDsStr, tp: mysql.TypeBlob, size: types.UnspecifiedLength, comment: "A list of the table IDs that the transaction has accessed"},
	{name: txninfo.WaitingTimeStr, tp: mysql.TypeDouble, size: 22, comment: "Current lock waiting time"},
}

var tableDeadlocksCols = []columnInfo{
	{name: deadlockhistory.ColDeadlockIDStr, tp: mysql.TypeLonglong, size: 21, flag: mysql.NotNullFlag, comment: "The ID to distinguish different deadlock events"},
	{name: deadlockhistory.ColOccurTimeStr, tp: mysql.TypeTimestamp, decimal: 6, size: 26, comment: "The physical time when the deadlock occurs"},
	{name: deadlockhistory.ColRetryableStr, tp: mysql.TypeTiny, size: 1, flag: mysql.NotNullFlag, comment: "Whether the deadlock is retryable. Retryable deadlocks are usually not reported to the client"},
	{name: deadlockhistory.ColTryLockTrxIDStr, tp: mysql.TypeLonglong, size: 21, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "The transaction ID (start ts) of the transaction that's trying to acquire the lock"},
	{name: deadlockhistory.ColCurrentSQLDigestStr, tp: mysql.TypeVarchar, size: 64, comment: "The digest of the SQL that's being blocked"},
	{name: deadlockhistory.ColCurrentSQLDigestTextStr, tp: mysql.TypeBlob, size: types.UnspecifiedLength, comment: "The normalized SQL that's being blocked"},
	{name: deadlockhistory.ColKeyStr, tp: mysql.TypeBlob, size: types.UnspecifiedLength, comment: "The key on which a transaction is waiting for another"},
	{name: deadlockhistory.ColKeyInfoStr, tp: mysql.TypeBlob, size: types.UnspecifiedLength, comment: "Information of the key"},
	{name: deadlockhistory.ColTrxHoldingLockStr, tp: mysql.TypeLonglong, size: 21, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "The transaction ID (start ts) of the transaction that's currently holding the lock"},
}

var tableDataLockWaitsCols = []columnInfo{
	{name: DataLockWaitsColumnKey, tp: mysql.TypeBlob, size: types.UnspecifiedLength, flag: mysql.NotNullFlag, comment: "The key that's being waiting on"},
	{name: DataLockWaitsColumnKeyInfo, tp: mysql.TypeBlob, size: types.UnspecifiedLength, comment: "Information of the key"},
	{name: DataLockWaitsColumnTrxID, tp: mysql.TypeLonglong, size: 21, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Current transaction that's waiting for the lock"},
	{name: DataLockWaitsColumnCurrentHoldingTrxID, tp: mysql.TypeLonglong, size: 21, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "The transaction that's holding the lock and blocks the current transaction"},
	{name: DataLockWaitsColumnSQLDigest, tp: mysql.TypeVarchar, size: 64, comment: "Digest of the SQL that's trying to acquire the lock"},
	{name: DataLockWaitsColumnSQLDigestText, tp: mysql.TypeBlob, size: types.UnspecifiedLength, comment: "Digest of the SQL that's trying to acquire the lock"},
}

var tableStatementsSummaryEvictedCols = []columnInfo{
	{name: "BEGIN_TIME", tp: mysql.TypeTimestamp, size: 26},
	{name: "END_TIME", tp: mysql.TypeTimestamp, size: 26},
	{name: "EVICTED_COUNT", tp: mysql.TypeLonglong, size: 21, flag: mysql.NotNullFlag},
}

var tableAttributesCols = []columnInfo{
	{name: "ID", tp: mysql.TypeVarchar, size: types.UnspecifiedLength, flag: mysql.NotNullFlag},
	{name: "TYPE", tp: mysql.TypeVarchar, size: 16, flag: mysql.NotNullFlag},
	{name: "ATTRIBUTES", tp: mysql.TypeVarchar, size: types.UnspecifiedLength},
	{name: "RANGES", tp: mysql.TypeBlob, size: types.UnspecifiedLength},
}

var tableTrxSummaryCols = []columnInfo{
	{name: "DIGEST", tp: mysql.TypeVarchar, size: 16, flag: mysql.NotNullFlag, comment: "Digest of a transaction"},
	{name: txninfo.AllSQLDigestsStr, tp: mysql.TypeBlob, size: types.UnspecifiedLength, comment: "A list of the digests of SQL statements that the transaction has executed"},
}

var tablePlacementPoliciesCols = []columnInfo{
	{name: "POLICY_ID", tp: mysql.TypeLonglong, size: 21, flag: mysql.NotNullFlag},
	{name: "CATALOG_NAME", tp: mysql.TypeVarchar, size: 512, flag: mysql.NotNullFlag},
	{name: "POLICY_NAME", tp: mysql.TypeVarchar, size: 64, flag: mysql.NotNullFlag}, // Catalog wide policy
	{name: "PRIMARY_REGION", tp: mysql.TypeVarchar, size: 1024},
	{name: "REGIONS", tp: mysql.TypeVarchar, size: 1024},
	{name: "CONSTRAINTS", tp: mysql.TypeVarchar, size: 1024},
	{name: "LEADER_CONSTRAINTS", tp: mysql.TypeVarchar, size: 1024},
	{name: "FOLLOWER_CONSTRAINTS", tp: mysql.TypeVarchar, size: 1024},
	{name: "LEARNER_CONSTRAINTS", tp: mysql.TypeVarchar, size: 1024},
	{name: "SCHEDULE", tp: mysql.TypeVarchar, size: 20}, // EVEN or MAJORITY_IN_PRIMARY
	{name: "FOLLOWERS", tp: mysql.TypeLonglong, size: 21},
	{name: "LEARNERS", tp: mysql.TypeLonglong, size: 21},
}

var tableVariablesInfoCols = []columnInfo{
	{name: "VARIABLE_NAME", tp: mysql.TypeVarchar, size: 64, flag: mysql.NotNullFlag},
	{name: "VARIABLE_SCOPE", tp: mysql.TypeVarchar, size: 64, flag: mysql.NotNullFlag},
	{name: "DEFAULT_VALUE", tp: mysql.TypeVarchar, size: 64, flag: mysql.NotNullFlag},
	{name: "CURRENT_VALUE", tp: mysql.TypeVarchar, size: 64, flag: mysql.NotNullFlag},
	{name: "MIN_VALUE", tp: mysql.TypeLonglong, size: 21},
	{name: "MAX_VALUE", tp: mysql.TypeLonglong, size: 21, flag: mysql.UnsignedFlag},
	{name: "POSSIBLE_VALUES", tp: mysql.TypeVarchar, size: 256},
	{name: "IS_NOOP", tp: mysql.TypeVarchar, size: 64, flag: mysql.NotNullFlag},
}

var tableUserAttributesCols = []columnInfo{
	{name: "USER", tp: mysql.TypeVarchar, size: 32, flag: mysql.NotNullFlag},
	{name: "HOST", tp: mysql.TypeVarchar, size: 255, flag: mysql.NotNullFlag},
	{name: "ATTRIBUTE", tp: mysql.TypeLongBlob, size: types.UnspecifiedLength},
}

var tableMemoryUsageCols = []columnInfo{
	{name: "MEMORY_TOTAL", tp: mysql.TypeLonglong, size: 21, flag: mysql.NotNullFlag},
	{name: "MEMORY_LIMIT", tp: mysql.TypeLonglong, size: 21, flag: mysql.NotNullFlag},
	{name: "MEMORY_CURRENT", tp: mysql.TypeLonglong, size: 21, flag: mysql.NotNullFlag},
	{name: "MEMORY_MAX_USED", tp: mysql.TypeLonglong, size: 21, flag: mysql.NotNullFlag},
	{name: "CURRENT_OPS", tp: mysql.TypeVarchar, size: 50},
	{name: "SESSION_KILL_LAST", tp: mysql.TypeDatetime},
	{name: "SESSION_KILL_TOTAL", tp: mysql.TypeLonglong, size: 21, flag: mysql.NotNullFlag},
	{name: "GC_LAST", tp: mysql.TypeDatetime},
	{name: "GC_TOTAL", tp: mysql.TypeLonglong, size: 21, flag: mysql.NotNullFlag},
	{name: "DISK_USAGE", tp: mysql.TypeLonglong, size: 21, flag: mysql.NotNullFlag},
	{name: "QUERY_FORCE_DISK", tp: mysql.TypeLonglong, size: 21, flag: mysql.NotNullFlag},
}

var tableMemoryUsageOpsHistoryCols = []columnInfo{
	{name: "TIME", tp: mysql.TypeDatetime, size: 64, flag: mysql.NotNullFlag},
	{name: "OPS", tp: mysql.TypeVarchar, size: 20, flag: mysql.NotNullFlag},
	{name: "MEMORY_LIMIT", tp: mysql.TypeLonglong, size: 21, flag: mysql.NotNullFlag},
	{name: "MEMORY_CURRENT", tp: mysql.TypeLonglong, size: 21, flag: mysql.NotNullFlag},
	{name: "PROCESSID", tp: mysql.TypeLonglong, size: 21, flag: mysql.UnsignedFlag},
	{name: "MEM", tp: mysql.TypeLonglong, size: 21, flag: mysql.UnsignedFlag},
	{name: "DISK", tp: mysql.TypeLonglong, size: 21, flag: mysql.UnsignedFlag},
	{name: "CLIENT", tp: mysql.TypeVarchar, size: 64},
	{name: "DB", tp: mysql.TypeVarchar, size: 64},
	{name: "USER", tp: mysql.TypeVarchar, size: 16},
	{name: "SQL_DIGEST", tp: mysql.TypeVarchar, size: 64},
	{name: "SQL_TEXT", tp: mysql.TypeVarchar, size: 256},
}

var tableResourceGroupsCols = []columnInfo{
	{name: "NAME", tp: mysql.TypeVarchar, size: resourcegroup.MaxGroupNameLength, flag: mysql.NotNullFlag},
	{name: "RU_PER_SEC", tp: mysql.TypeVarchar, size: 21},
	{name: "PRIORITY", tp: mysql.TypeVarchar, size: 6},
	{name: "BURSTABLE", tp: mysql.TypeVarchar, size: 3},
	{name: "QUERY_LIMIT", tp: mysql.TypeVarchar, size: 256},
	{name: "BACKGROUND", tp: mysql.TypeVarchar, size: 256},
}

var tableRunawayWatchListCols = []columnInfo{
	{name: "ID", tp: mysql.TypeLonglong, size: 21, flag: mysql.NotNullFlag},
	{name: "RESOURCE_GROUP_NAME", tp: mysql.TypeVarchar, size: resourcegroup.MaxGroupNameLength, flag: mysql.NotNullFlag},
	{name: "START_TIME", tp: mysql.TypeVarchar, size: 32, flag: mysql.NotNullFlag},
	{name: "END_TIME", tp: mysql.TypeVarchar, size: 32},
	{name: "WATCH", tp: mysql.TypeVarchar, size: 12, flag: mysql.NotNullFlag},
	{name: "WATCH_TEXT", tp: mysql.TypeBlob, size: types.UnspecifiedLength, flag: mysql.NotNullFlag},
	{name: "SOURCE", tp: mysql.TypeVarchar, size: 128, flag: mysql.NotNullFlag},
	{name: "ACTION", tp: mysql.TypeVarchar, size: 12, flag: mysql.NotNullFlag},
	{name: "RULE", tp: mysql.TypeVarchar, size: 128, flag: mysql.NotNullFlag},
}

// information_schema.CHECK_CONSTRAINTS
var tableCheckConstraintsCols = []columnInfo{
	{name: "CONSTRAINT_CATALOG", tp: mysql.TypeVarchar, size: 64, flag: mysql.NotNullFlag},
	{name: "CONSTRAINT_SCHEMA", tp: mysql.TypeVarchar, size: 64, flag: mysql.NotNullFlag},
	{name: "CONSTRAINT_NAME", tp: mysql.TypeVarchar, size: 64, flag: mysql.NotNullFlag},
	{name: "CHECK_CLAUSE", tp: mysql.TypeLongBlob, size: types.UnspecifiedLength, flag: mysql.NotNullFlag},
}

// information_schema.TIDB_CHECK_CONSTRAINTS
var tableTiDBCheckConstraintsCols = []columnInfo{
	{name: "CONSTRAINT_CATALOG", tp: mysql.TypeVarchar, size: 64, flag: mysql.NotNullFlag},
	{name: "CONSTRAINT_SCHEMA", tp: mysql.TypeVarchar, size: 64, flag: mysql.NotNullFlag},
	{name: "CONSTRAINT_NAME", tp: mysql.TypeVarchar, size: 64, flag: mysql.NotNullFlag},
	{name: "CHECK_CLAUSE", tp: mysql.TypeLongBlob, size: types.UnspecifiedLength, flag: mysql.NotNullFlag},
	{name: "TABLE_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "TABLE_ID", tp: mysql.TypeLonglong, size: 21},
}

var tableKeywords = []columnInfo{
	{name: "WORD", tp: mysql.TypeVarchar, size: 128},
	{name: "RESERVED", tp: mysql.TypeLong, size: 11},
}

var tableTiDBIndexUsage = []columnInfo{
	{name: "TABLE_SCHEMA", tp: mysql.TypeVarchar, size: 64},
	{name: "TABLE_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "INDEX_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "QUERY_TOTAL", tp: mysql.TypeLonglong, size: 21},
	{name: "KV_REQ_TOTAL", tp: mysql.TypeLonglong, size: 21},
	{name: "ROWS_ACCESS_TOTAL", tp: mysql.TypeLonglong, size: 21},
	{name: "PERCENTAGE_ACCESS_0", tp: mysql.TypeLonglong, size: 21},
	{name: "PERCENTAGE_ACCESS_0_1", tp: mysql.TypeLonglong, size: 21},
	{name: "PERCENTAGE_ACCESS_1_10", tp: mysql.TypeLonglong, size: 21},
	{name: "PERCENTAGE_ACCESS_10_20", tp: mysql.TypeLonglong, size: 21},
	{name: "PERCENTAGE_ACCESS_20_50", tp: mysql.TypeLonglong, size: 21},
	{name: "PERCENTAGE_ACCESS_50_100", tp: mysql.TypeLonglong, size: 21},
	{name: "PERCENTAGE_ACCESS_100", tp: mysql.TypeLonglong, size: 21},
	{name: "LAST_ACCESS_TIME", tp: mysql.TypeDatetime, size: 21},
}

var tablePlanCache = []columnInfo{
	{name: "SQL_DIGEST", tp: mysql.TypeVarchar, size: 64},
	{name: "SQL_TEXT", tp: mysql.TypeLongBlob, size: types.UnspecifiedLength},
	{name: "STMT_TYPE", tp: mysql.TypeVarchar, size: 64},
	{name: "PARSE_USER", tp: mysql.TypeVarchar, size: 64},
	{name: "PLAN_DIGEST", tp: mysql.TypeVarchar, size: 64},
	{name: "BINARY_PLAN", tp: mysql.TypeLongBlob, size: types.UnspecifiedLength},
	{name: "BINDING", tp: mysql.TypeLongBlob, size: types.UnspecifiedLength},
	{name: "OPT_ENV", tp: mysql.TypeVarchar, size: 64},
	{name: "PARSE_VALUES", tp: mysql.TypeLongBlob, size: types.UnspecifiedLength},
	{name: "MEM_SIZE", tp: mysql.TypeLonglong, size: 21},
	{name: "EXECUTIONS", tp: mysql.TypeLonglong, size: 21},
	{name: "PROCESSED_KEYS", tp: mysql.TypeLonglong, size: 21},
	{name: "TOTAL_KEYS", tp: mysql.TypeLonglong, size: 21},
	{name: "SUM_LATENCY", tp: mysql.TypeLonglong, size: 21},
	{name: "LOAD_TIME", tp: mysql.TypeDatetime, size: 19},
	{name: "LAST_ACTIVE_TIME", tp: mysql.TypeDatetime, size: 19},
}

var tableKeyspaceMetaCols = []columnInfo{
	{name: "KEYSPACE_NAME", tp: mysql.TypeVarchar, size: 128},
	{name: "KEYSPACE_ID", tp: mysql.TypeVarchar, size: 64},
	{name: "CONFIG", tp: mysql.TypeJSON, size: types.UnspecifiedLength},
}
