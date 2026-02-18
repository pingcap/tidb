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
	"github.com/pingcap/tidb/pkg/util/stmtsummary"
)


var tableTiDBServersInfoCols = []columnInfo{
	{name: "DDL_ID", tp: mysql.TypeVarchar, size: 64},
	{name: "IP", tp: mysql.TypeVarchar, size: 64},
	{name: "PORT", tp: mysql.TypeLonglong, size: 21},
	{name: "STATUS_PORT", tp: mysql.TypeLonglong, size: 21},
	{name: "LEASE", tp: mysql.TypeVarchar, size: 64},
	{name: "VERSION", tp: mysql.TypeVarchar, size: 64},
	{name: "GIT_HASH", tp: mysql.TypeVarchar, size: 64},
	{name: "LABELS", tp: mysql.TypeVarchar, size: 128},
}

var tableClusterConfigCols = []columnInfo{
	{name: "TYPE", tp: mysql.TypeVarchar, size: 64},
	{name: "INSTANCE", tp: mysql.TypeVarchar, size: 64},
	{name: "KEY", tp: mysql.TypeVarchar, size: 256},
	{name: "VALUE", tp: mysql.TypeLongBlob, size: types.UnspecifiedLength},
}

var tableClusterLogCols = []columnInfo{
	{name: "TIME", tp: mysql.TypeVarchar, size: 32},
	{name: "TYPE", tp: mysql.TypeVarchar, size: 64},
	{name: "INSTANCE", tp: mysql.TypeVarchar, size: 64},
	{name: "LEVEL", tp: mysql.TypeVarchar, size: 8},
	{name: "MESSAGE", tp: mysql.TypeLongBlob, size: types.UnspecifiedLength},
}

var tableClusterLoadCols = []columnInfo{
	{name: "TYPE", tp: mysql.TypeVarchar, size: 64},
	{name: "INSTANCE", tp: mysql.TypeVarchar, size: 64},
	{name: "DEVICE_TYPE", tp: mysql.TypeVarchar, size: 64},
	{name: "DEVICE_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "NAME", tp: mysql.TypeVarchar, size: 256},
	{name: "VALUE", tp: mysql.TypeVarchar, size: 128},
}

var tableClusterHardwareCols = []columnInfo{
	{name: "TYPE", tp: mysql.TypeVarchar, size: 64},
	{name: "INSTANCE", tp: mysql.TypeVarchar, size: 64},
	{name: "DEVICE_TYPE", tp: mysql.TypeVarchar, size: 64},
	{name: "DEVICE_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "NAME", tp: mysql.TypeVarchar, size: 256},
	{name: "VALUE", tp: mysql.TypeVarchar, size: 128},
}

var tableClusterSystemInfoCols = []columnInfo{
	{name: "TYPE", tp: mysql.TypeVarchar, size: 64},
	{name: "INSTANCE", tp: mysql.TypeVarchar, size: 64},
	{name: "SYSTEM_TYPE", tp: mysql.TypeVarchar, size: 64},
	{name: "SYSTEM_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "NAME", tp: mysql.TypeVarchar, size: 256},
	{name: "VALUE", tp: mysql.TypeVarchar, size: 128},
}

var filesCols = []columnInfo{
	{name: "FILE_ID", tp: mysql.TypeLonglong, size: 4},
	{name: "FILE_NAME", tp: mysql.TypeVarchar, size: 4000},
	{name: "FILE_TYPE", tp: mysql.TypeVarchar, size: 20},
	{name: "TABLESPACE_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "TABLE_CATALOG", tp: mysql.TypeVarchar, size: 64},
	{name: "TABLE_SCHEMA", tp: mysql.TypeVarchar, size: 64},
	{name: "TABLE_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "LOGFILE_GROUP_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "LOGFILE_GROUP_NUMBER", tp: mysql.TypeLonglong, size: 32},
	{name: "ENGINE", tp: mysql.TypeVarchar, size: 64},
	{name: "FULLTEXT_KEYS", tp: mysql.TypeVarchar, size: 64},
	{name: "DELETED_ROWS", tp: mysql.TypeLonglong, size: 4},
	{name: "UPDATE_COUNT", tp: mysql.TypeLonglong, size: 4},
	{name: "FREE_EXTENTS", tp: mysql.TypeLonglong, size: 4},
	{name: "TOTAL_EXTENTS", tp: mysql.TypeLonglong, size: 4},
	{name: "EXTENT_SIZE", tp: mysql.TypeLonglong, size: 4},
	{name: "INITIAL_SIZE", tp: mysql.TypeLonglong, size: 21},
	{name: "MAXIMUM_SIZE", tp: mysql.TypeLonglong, size: 21},
	{name: "AUTOEXTEND_SIZE", tp: mysql.TypeLonglong, size: 21},
	{name: "CREATION_TIME", tp: mysql.TypeDatetime, size: -1},
	{name: "LAST_UPDATE_TIME", tp: mysql.TypeDatetime, size: -1},
	{name: "LAST_ACCESS_TIME", tp: mysql.TypeDatetime, size: -1},
	{name: "RECOVER_TIME", tp: mysql.TypeLonglong, size: 4},
	{name: "TRANSACTION_COUNTER", tp: mysql.TypeLonglong, size: 4},
	{name: "VERSION", tp: mysql.TypeLonglong, size: 21},
	{name: "ROW_FORMAT", tp: mysql.TypeVarchar, size: 10},
	{name: "TABLE_ROWS", tp: mysql.TypeLonglong, size: 21},
	{name: "AVG_ROW_LENGTH", tp: mysql.TypeLonglong, size: 21},
	{name: "DATA_LENGTH", tp: mysql.TypeLonglong, size: 21},
	{name: "MAX_DATA_LENGTH", tp: mysql.TypeLonglong, size: 21},
	{name: "INDEX_LENGTH", tp: mysql.TypeLonglong, size: 21},
	{name: "DATA_FREE", tp: mysql.TypeLonglong, size: 21},
	{name: "CREATE_TIME", tp: mysql.TypeDatetime, size: -1},
	{name: "UPDATE_TIME", tp: mysql.TypeDatetime, size: -1},
	{name: "CHECK_TIME", tp: mysql.TypeDatetime, size: -1},
	{name: "CHECKSUM", tp: mysql.TypeLonglong, size: 21},
	{name: "STATUS", tp: mysql.TypeVarchar, size: 20},
	{name: "EXTRA", tp: mysql.TypeVarchar, size: 255},
}

var tableClusterInfoCols = []columnInfo{
	{name: "TYPE", tp: mysql.TypeVarchar, size: 64},
	{name: "INSTANCE", tp: mysql.TypeVarchar, size: 64},
	{name: "STATUS_ADDRESS", tp: mysql.TypeVarchar, size: 64},
	{name: "VERSION", tp: mysql.TypeVarchar, size: 64},
	{name: "GIT_HASH", tp: mysql.TypeVarchar, size: 64},
	{name: "START_TIME", tp: mysql.TypeDatetime, size: 19},
	{name: "UPTIME", tp: mysql.TypeVarchar, size: 32},
	{name: "SERVER_ID", tp: mysql.TypeLonglong, size: 21, comment: "invalid if the configuration item `enable-global-kill` is set to FALSE"},
}

var tableTableTiFlashReplicaCols = []columnInfo{
	{name: "TABLE_SCHEMA", tp: mysql.TypeVarchar, size: 64},
	{name: "TABLE_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "TABLE_ID", tp: mysql.TypeLonglong, size: 21},
	{name: "REPLICA_COUNT", tp: mysql.TypeLonglong, size: 21},
	{name: "LOCATION_LABELS", tp: mysql.TypeVarchar, size: 64},
	{name: "AVAILABLE", tp: mysql.TypeTiny, size: 1},
	{name: "PROGRESS", tp: mysql.TypeDouble, size: 22},
}

var tableInspectionResultCols = []columnInfo{
	{name: "RULE", tp: mysql.TypeVarchar, size: 64},
	{name: "ITEM", tp: mysql.TypeVarchar, size: 64},
	{name: "TYPE", tp: mysql.TypeVarchar, size: 64},
	{name: "INSTANCE", tp: mysql.TypeVarchar, size: 64},
	{name: "STATUS_ADDRESS", tp: mysql.TypeVarchar, size: 64},
	{name: "VALUE", tp: mysql.TypeVarchar, size: 64},
	{name: "REFERENCE", tp: mysql.TypeVarchar, size: 64},
	{name: "SEVERITY", tp: mysql.TypeVarchar, size: 64},
	{name: "DETAILS", tp: mysql.TypeVarchar, size: 256},
}

var tableInspectionSummaryCols = []columnInfo{
	{name: "RULE", tp: mysql.TypeVarchar, size: 64},
	{name: "INSTANCE", tp: mysql.TypeVarchar, size: 64},
	{name: "METRICS_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "LABEL", tp: mysql.TypeVarchar, size: 64},
	{name: "QUANTILE", tp: mysql.TypeDouble, size: 22},
	{name: "AVG_VALUE", tp: mysql.TypeDouble, size: 22, decimal: 6},
	{name: "MIN_VALUE", tp: mysql.TypeDouble, size: 22, decimal: 6},
	{name: "MAX_VALUE", tp: mysql.TypeDouble, size: 22, decimal: 6},
	{name: "COMMENT", tp: mysql.TypeVarchar, size: 256},
}

var tableInspectionRulesCols = []columnInfo{
	{name: "NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "TYPE", tp: mysql.TypeVarchar, size: 64},
	{name: "COMMENT", tp: mysql.TypeVarchar, size: 256},
}

var tableMetricTablesCols = []columnInfo{
	{name: "TABLE_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "PROMQL", tp: mysql.TypeVarchar, size: 64},
	{name: "LABELS", tp: mysql.TypeVarchar, size: 64},
	{name: "QUANTILE", tp: mysql.TypeDouble, size: 22},
	{name: "COMMENT", tp: mysql.TypeVarchar, size: 256},
}

var tableMetricSummaryCols = []columnInfo{
	{name: "METRICS_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "QUANTILE", tp: mysql.TypeDouble, size: 22},
	{name: "SUM_VALUE", tp: mysql.TypeDouble, size: 22, decimal: 6},
	{name: "AVG_VALUE", tp: mysql.TypeDouble, size: 22, decimal: 6},
	{name: "MIN_VALUE", tp: mysql.TypeDouble, size: 22, decimal: 6},
	{name: "MAX_VALUE", tp: mysql.TypeDouble, size: 22, decimal: 6},
	{name: "COMMENT", tp: mysql.TypeVarchar, size: 256},
}

var tableMetricSummaryByLabelCols = []columnInfo{
	{name: "INSTANCE", tp: mysql.TypeVarchar, size: 64},
	{name: "METRICS_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "LABEL", tp: mysql.TypeVarchar, size: 64},
	{name: "QUANTILE", tp: mysql.TypeDouble, size: 22},
	{name: "SUM_VALUE", tp: mysql.TypeDouble, size: 22, decimal: 6},
	{name: "AVG_VALUE", tp: mysql.TypeDouble, size: 22, decimal: 6},
	{name: "MIN_VALUE", tp: mysql.TypeDouble, size: 22, decimal: 6},
	{name: "MAX_VALUE", tp: mysql.TypeDouble, size: 22, decimal: 6},
	{name: "COMMENT", tp: mysql.TypeVarchar, size: 256},
}

var tableDDLJobsCols = []columnInfo{
	{name: "JOB_ID", tp: mysql.TypeLonglong, size: 21},
	{name: "DB_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "TABLE_NAME", tp: mysql.TypeVarchar, size: 64},
	{name: "JOB_TYPE", tp: mysql.TypeVarchar, size: 64},
	{name: "SCHEMA_STATE", tp: mysql.TypeVarchar, size: 64},
	{name: "SCHEMA_ID", tp: mysql.TypeLonglong, size: 21},
	{name: "TABLE_ID", tp: mysql.TypeLonglong, size: 21},
	{name: "ROW_COUNT", tp: mysql.TypeLonglong, size: 21},
	{name: "CREATE_TIME", tp: mysql.TypeDatetime, size: 26, decimal: 6},
	{name: "START_TIME", tp: mysql.TypeDatetime, size: 26, decimal: 6},
	{name: "END_TIME", tp: mysql.TypeDatetime, size: 26, decimal: 6},
	{name: "STATE", tp: mysql.TypeVarchar, size: 64},
	{name: "QUERY", tp: mysql.TypeBlob, size: types.UnspecifiedLength},
}

var tableSequencesCols = []columnInfo{
	{name: "TABLE_CATALOG", tp: mysql.TypeVarchar, size: 512, flag: mysql.NotNullFlag},
	{name: "SEQUENCE_SCHEMA", tp: mysql.TypeVarchar, size: 64, flag: mysql.NotNullFlag},
	{name: "SEQUENCE_NAME", tp: mysql.TypeVarchar, size: 64, flag: mysql.NotNullFlag},
	{name: "CACHE", tp: mysql.TypeTiny, flag: mysql.NotNullFlag},
	{name: "CACHE_VALUE", tp: mysql.TypeLonglong, size: 21},
	{name: "CYCLE", tp: mysql.TypeTiny, flag: mysql.NotNullFlag},
	{name: "INCREMENT", tp: mysql.TypeLonglong, size: 21, flag: mysql.NotNullFlag},
	{name: "MAX_VALUE", tp: mysql.TypeLonglong, size: 21},
	{name: "MIN_VALUE", tp: mysql.TypeLonglong, size: 21},
	{name: "START", tp: mysql.TypeLonglong, size: 21},
	{name: "COMMENT", tp: mysql.TypeVarchar, size: 64},
}

var tableStatementsSummaryCols = []columnInfo{
	{name: stmtsummary.SummaryBeginTimeStr, tp: mysql.TypeTimestamp, size: 26, flag: mysql.NotNullFlag, comment: "Begin time of this summary"},
	{name: stmtsummary.SummaryEndTimeStr, tp: mysql.TypeTimestamp, size: 26, flag: mysql.NotNullFlag, comment: "End time of this summary"},
	{name: stmtsummary.StmtTypeStr, tp: mysql.TypeVarchar, size: 64, flag: mysql.NotNullFlag, comment: "Statement type"},
	{name: stmtsummary.SchemaNameStr, tp: mysql.TypeVarchar, size: 64, comment: "Current schema"},
	{name: stmtsummary.DigestStr, tp: mysql.TypeVarchar, size: 64},
	{name: stmtsummary.DigestTextStr, tp: mysql.TypeBlob, size: types.UnspecifiedLength, flag: mysql.NotNullFlag, comment: "Normalized statement"},
	{name: stmtsummary.TableNamesStr, tp: mysql.TypeBlob, size: types.UnspecifiedLength, comment: "Involved tables"},
	{name: stmtsummary.IndexNamesStr, tp: mysql.TypeBlob, size: types.UnspecifiedLength, comment: "Used indices"},
	{name: stmtsummary.SampleUserStr, tp: mysql.TypeVarchar, size: 64, comment: "Sampled user who executed these statements"},
	{name: stmtsummary.ExecCountStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Count of executions"},
	{name: stmtsummary.SumErrorsStr, tp: mysql.TypeLong, size: 11, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Sum of errors"},
	{name: stmtsummary.SumWarningsStr, tp: mysql.TypeLong, size: 11, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Sum of warnings"},
	{name: stmtsummary.SumLatencyStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Sum latency of these statements"},
	{name: stmtsummary.MaxLatencyStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Max latency of these statements"},
	{name: stmtsummary.MinLatencyStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Min latency of these statements"},
	{name: stmtsummary.AvgLatencyStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Average latency of these statements"},
	{name: stmtsummary.AvgParseLatencyStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Average latency of parsing"},
	{name: stmtsummary.MaxParseLatencyStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Max latency of parsing"},
	{name: stmtsummary.AvgCompileLatencyStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Average latency of compiling"},
	{name: stmtsummary.MaxCompileLatencyStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Max latency of compiling"},
	{name: stmtsummary.SumCopTaskNumStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Total number of CopTasks"},
	{name: stmtsummary.MaxCopProcessTimeStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Max processing time of CopTasks"},
	{name: stmtsummary.MaxCopProcessAddressStr, tp: mysql.TypeVarchar, size: 256, comment: "Address of the CopTask with max processing time"},
	{name: stmtsummary.MaxCopWaitTimeStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Max waiting time of CopTasks"},
	{name: stmtsummary.MaxCopWaitAddressStr, tp: mysql.TypeVarchar, size: 256, comment: "Address of the CopTask with max waiting time"},
	{name: stmtsummary.AvgProcessTimeStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Average processing time in TiKV"},
	{name: stmtsummary.MaxProcessTimeStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Max processing time in TiKV"},
	{name: stmtsummary.AvgWaitTimeStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Average waiting time in TiKV"},
	{name: stmtsummary.MaxWaitTimeStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Max waiting time in TiKV"},
	{name: stmtsummary.AvgBackoffTimeStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Average waiting time before retry"},
	{name: stmtsummary.MaxBackoffTimeStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Max waiting time before retry"},
	{name: stmtsummary.AvgTotalKeysStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Average number of scanned keys"},
	{name: stmtsummary.MaxTotalKeysStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Max number of scanned keys"},
	{name: stmtsummary.AvgProcessedKeysStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Average number of processed keys"},
	{name: stmtsummary.MaxProcessedKeysStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Max number of processed keys"},
	{name: stmtsummary.AvgRocksdbDeleteSkippedCountStr, tp: mysql.TypeDouble, size: 22, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Average number of rocksdb delete skipped count"},
	{name: stmtsummary.MaxRocksdbDeleteSkippedCountStr, tp: mysql.TypeLong, size: 11, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Max number of rocksdb delete skipped count"},
	{name: stmtsummary.AvgRocksdbKeySkippedCountStr, tp: mysql.TypeDouble, size: 22, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Average number of rocksdb key skipped count"},
	{name: stmtsummary.MaxRocksdbKeySkippedCountStr, tp: mysql.TypeLong, size: 11, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Max number of rocksdb key skipped count"},
	{name: stmtsummary.AvgRocksdbBlockCacheHitCountStr, tp: mysql.TypeDouble, size: 22, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Average number of rocksdb block cache hit count"},
	{name: stmtsummary.MaxRocksdbBlockCacheHitCountStr, tp: mysql.TypeLong, size: 11, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Max number of rocksdb block cache hit count"},
	{name: stmtsummary.AvgRocksdbBlockReadCountStr, tp: mysql.TypeDouble, size: 22, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Average number of rocksdb block read count"},
	{name: stmtsummary.MaxRocksdbBlockReadCountStr, tp: mysql.TypeLong, size: 11, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Max number of rocksdb block read count"},
	{name: stmtsummary.AvgRocksdbBlockReadByteStr, tp: mysql.TypeDouble, size: 22, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Average number of rocksdb block read byte"},
	{name: stmtsummary.MaxRocksdbBlockReadByteStr, tp: mysql.TypeLong, size: 11, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Max number of rocksdb block read byte"},
	{name: stmtsummary.AvgPrewriteTimeStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Average time of prewrite phase"},
	{name: stmtsummary.MaxPrewriteTimeStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Max time of prewrite phase"},
	{name: stmtsummary.AvgCommitTimeStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Average time of commit phase"},
	{name: stmtsummary.MaxCommitTimeStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Max time of commit phase"},
	{name: stmtsummary.AvgGetCommitTsTimeStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Average time of getting commit_ts"},
	{name: stmtsummary.MaxGetCommitTsTimeStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Max time of getting commit_ts"},
	{name: stmtsummary.AvgCommitBackoffTimeStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Average time before retry during commit phase"},
	{name: stmtsummary.MaxCommitBackoffTimeStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Max time before retry during commit phase"},
	{name: stmtsummary.AvgResolveLockTimeStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Average time for resolving locks"},
	{name: stmtsummary.MaxResolveLockTimeStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Max time for resolving locks"},
	{name: stmtsummary.AvgLocalLatchWaitTimeStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Average waiting time of local transaction"},
	{name: stmtsummary.MaxLocalLatchWaitTimeStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Max waiting time of local transaction"},
	{name: stmtsummary.AvgWriteKeysStr, tp: mysql.TypeDouble, size: 22, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Average count of written keys"},
	{name: stmtsummary.MaxWriteKeysStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Max count of written keys"},
	{name: stmtsummary.AvgWriteSizeStr, tp: mysql.TypeDouble, size: 22, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Average amount of written bytes"},
	{name: stmtsummary.MaxWriteSizeStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Max amount of written bytes"},
	{name: stmtsummary.AvgPrewriteRegionsStr, tp: mysql.TypeDouble, size: 22, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Average number of involved regions in prewrite phase"},
	{name: stmtsummary.MaxPrewriteRegionsStr, tp: mysql.TypeLong, size: 11, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Max number of involved regions in prewrite phase"},
	{name: stmtsummary.AvgTxnRetryStr, tp: mysql.TypeDouble, size: 22, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Average number of transaction retries"},
	{name: stmtsummary.MaxTxnRetryStr, tp: mysql.TypeLong, size: 11, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Max number of transaction retries"},
	{name: stmtsummary.SumExecRetryStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Sum number of execution retries in pessimistic transactions"},
	{name: stmtsummary.SumExecRetryTimeStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Sum time of execution retries in pessimistic transactions"},
	{name: stmtsummary.SumBackoffTimesStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Sum of retries"},
	{name: stmtsummary.BackoffTypesStr, tp: mysql.TypeVarchar, size: 1024, comment: "Types of errors and the number of retries for each type"},
	{name: stmtsummary.AvgMemStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Average memory(byte) used"},
	{name: stmtsummary.MaxMemStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Max memory(byte) used"},
	{name: stmtsummary.AvgMemArbitrationStr, tp: mysql.TypeDouble, size: 22, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Average time of memory arbitration"},
	{name: stmtsummary.MaxMemArbitrationStr, tp: mysql.TypeDouble, size: 22, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Max time of memory arbitration"},
	{name: stmtsummary.AvgDiskStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Average disk space(byte) used"},
	{name: stmtsummary.MaxDiskStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Max disk space(byte) used"},
	{name: stmtsummary.AvgKvTimeStr, tp: mysql.TypeLonglong, size: 22, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Average time of TiKV used"},
	{name: stmtsummary.AvgPdTimeStr, tp: mysql.TypeLonglong, size: 22, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Average time of PD used"},
	{name: stmtsummary.AvgBackoffTotalTimeStr, tp: mysql.TypeLonglong, size: 22, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Average time of Backoff used"},
	{name: stmtsummary.AvgWriteSQLRespTimeStr, tp: mysql.TypeLonglong, size: 22, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Average time of write sql resp used"},
	{name: stmtsummary.AvgTidbCPUTimeStr, tp: mysql.TypeLonglong, size: 22, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Average cpu time tidb used"},
	{name: stmtsummary.AvgTikvCPUTimeStr, tp: mysql.TypeLonglong, size: 22, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Average cpu time tikv used"},
	{name: stmtsummary.MaxResultRowsStr, tp: mysql.TypeLonglong, size: 22, flag: mysql.NotNullFlag, comment: "Max count of sql result rows"},
	{name: stmtsummary.MinResultRowsStr, tp: mysql.TypeLonglong, size: 22, flag: mysql.NotNullFlag, comment: "Min count of sql result rows"},
	{name: stmtsummary.AvgResultRowsStr, tp: mysql.TypeLonglong, size: 22, flag: mysql.NotNullFlag, comment: "Average count of sql result rows"},
	{name: stmtsummary.PreparedStr, tp: mysql.TypeTiny, size: 1, flag: mysql.NotNullFlag, comment: "Whether prepared"},
	{name: stmtsummary.AvgAffectedRowsStr, tp: mysql.TypeDouble, size: 22, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Average number of rows affected"},
	{name: stmtsummary.FirstSeenStr, tp: mysql.TypeTimestamp, size: 26, flag: mysql.NotNullFlag, comment: "The time these statements are seen for the first time"},
	{name: stmtsummary.LastSeenStr, tp: mysql.TypeTimestamp, size: 26, flag: mysql.NotNullFlag, comment: "The time these statements are seen for the last time"},
	{name: stmtsummary.PlanInCacheStr, tp: mysql.TypeTiny, size: 1, flag: mysql.NotNullFlag, comment: "Whether the last statement hit plan cache"},
	{name: stmtsummary.PlanCacheHitsStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag, comment: "The number of times these statements hit plan cache"},
	{name: stmtsummary.PlanInBindingStr, tp: mysql.TypeTiny, size: 1, flag: mysql.NotNullFlag, comment: "Whether the last statement is matched with the hints in the binding"},
	{name: stmtsummary.QuerySampleTextStr, tp: mysql.TypeBlob, size: types.UnspecifiedLength, comment: "Sampled original statement"},
	{name: stmtsummary.PrevSampleTextStr, tp: mysql.TypeBlob, size: types.UnspecifiedLength, comment: "The previous statement before commit"},
	{name: stmtsummary.PlanDigestStr, tp: mysql.TypeVarchar, size: 64, comment: "Digest of its execution plan"},
	{name: stmtsummary.PlanStr, tp: mysql.TypeBlob, size: types.UnspecifiedLength, comment: "Sampled execution plan"},
	{name: stmtsummary.BinaryPlan, tp: mysql.TypeBlob, size: types.UnspecifiedLength, comment: "Sampled binary plan"},
	{name: stmtsummary.BindingDigestStr, tp: mysql.TypeVarchar, size: 64, comment: "Digest of normalized statement for bindings"},
	{name: stmtsummary.BindingDigestTextStr, tp: mysql.TypeBlob, size: types.UnspecifiedLength, flag: mysql.NotNullFlag, comment: "Normalized statement for bindings"},
	{name: stmtsummary.Charset, tp: mysql.TypeVarchar, size: 64, comment: "Sampled charset"},
	{name: stmtsummary.Collation, tp: mysql.TypeVarchar, size: 64, comment: "Sampled collation"},
	{name: stmtsummary.PlanHint, tp: mysql.TypeBlob, size: types.UnspecifiedLength, comment: "Sampled plan hint"},
	{name: stmtsummary.MaxRequestUnitReadStr, tp: mysql.TypeDouble, flag: mysql.NotNullFlag | mysql.UnsignedFlag, size: 22, comment: "Max read request-unit cost of these statements"},
	{name: stmtsummary.AvgRequestUnitReadStr, tp: mysql.TypeDouble, flag: mysql.NotNullFlag | mysql.UnsignedFlag, size: 22, comment: "Average read request-unit cost of these statements"},
	{name: stmtsummary.MaxRequestUnitWriteStr, tp: mysql.TypeDouble, flag: mysql.NotNullFlag | mysql.UnsignedFlag, size: 22, comment: "Max write request-unit cost of these statements"},
	{name: stmtsummary.AvgRequestUnitWriteStr, tp: mysql.TypeDouble, flag: mysql.NotNullFlag | mysql.UnsignedFlag, size: 22, comment: "Average write request-unit cost of these statements"},
	{name: stmtsummary.MaxQueuedRcTimeStr, tp: mysql.TypeLonglong, size: 22, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Max time of waiting for available request-units"},
	{name: stmtsummary.AvgQueuedRcTimeStr, tp: mysql.TypeLonglong, size: 22, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Average time of waiting for available request-units"},
	{name: stmtsummary.ResourceGroupName, tp: mysql.TypeVarchar, size: 64, comment: "Bind resource group name"},
	{name: stmtsummary.PlanCacheUnqualifiedStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag, comment: "The number of times that these statements are not supported by the plan cache"},
	{name: stmtsummary.PlanCacheUnqualifiedLastReasonStr, tp: mysql.TypeBlob, size: types.UnspecifiedLength, comment: "The last reason why the statement is not supported by the plan cache"},
	{name: stmtsummary.SumUnpackedBytesSentTiKVTotalStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Total bytes sent to tikv"},
	{name: stmtsummary.SumUnpackedBytesReceivedTiKVTotalStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Total bytes received from tikv"},
	{name: stmtsummary.SumUnpackedBytesSentTiKVCrossZoneStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Total bytes sent to tikv cross zone"},
	{name: stmtsummary.SumUnpackedBytesReceivedTiKVCrossZoneStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Total bytes received from tikv cross zone"},
	{name: stmtsummary.SumUnpackedBytesSentTiFlashTotalStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Total bytes sent to tiflash"},
	{name: stmtsummary.SumUnpackedBytesReceivedTiFlashTotalStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Total bytes received from tiflash"},
	{name: stmtsummary.SumUnpackedBytesSentTiFlashCrossZoneStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Total bytes sent to tiflash cross zone"},
	{name: stmtsummary.SumUnpackedBytesReceiveTiFlashCrossZoneStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Total bytes received from tiflash cross zone"},
	{name: stmtsummary.StorageKVStr, tp: mysql.TypeTiny, size: 1, flag: mysql.NotNullFlag, comment: "Whether the last statement read data from TiKV"},
	{name: stmtsummary.StorageMPPStr, tp: mysql.TypeTiny, size: 1, flag: mysql.NotNullFlag, comment: "Whether the last statement read data from TiFlash"},
}

var tableTiDBStatementsStatsCols = []columnInfo{
	{name: stmtsummary.StmtTypeStr, tp: mysql.TypeVarchar, size: 64, flag: mysql.NotNullFlag, comment: "Statement type"},
	{name: stmtsummary.SchemaNameStr, tp: mysql.TypeVarchar, size: 64, comment: "Current schema"},
	{name: stmtsummary.DigestStr, tp: mysql.TypeVarchar, size: 64},
	{name: stmtsummary.DigestTextStr, tp: mysql.TypeBlob, size: types.UnspecifiedLength, flag: mysql.NotNullFlag, comment: "Normalized statement"},
	{name: stmtsummary.TableNamesStr, tp: mysql.TypeBlob, size: types.UnspecifiedLength, comment: "Involved tables"},
	{name: stmtsummary.IndexNamesStr, tp: mysql.TypeBlob, size: types.UnspecifiedLength, comment: "Used indices"},
	{name: stmtsummary.SampleUserStr, tp: mysql.TypeVarchar, size: 64, comment: "Sampled user who executed these statements"},
	{name: stmtsummary.ExecCountStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Count of executions"},
	{name: stmtsummary.ErrorsStr, tp: mysql.TypeLong, size: 11, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Sum of errors"},
	{name: stmtsummary.WarningsStr, tp: mysql.TypeLong, size: 11, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Sum of warnings"},
	{name: stmtsummary.MemStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Total memory(byte) used"},
	{name: stmtsummary.MemArbitrationStr, tp: mysql.TypeDouble, size: 22, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Total time of memory arbitration"},
	{name: stmtsummary.DiskStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Total disk space(byte) used"},
	{name: stmtsummary.TotalTimeStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Sum latency of these statements"},
	{name: stmtsummary.ParseTimeStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Total latency of parsing"},
	{name: stmtsummary.CompileTimeStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Total latency of compiling"},
	{name: stmtsummary.CopTaskNumStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Total number of CopTasks"},
	{name: stmtsummary.CopProcessTimeStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Total processing time of CopTasks"},
	{name: stmtsummary.MaxCopProcessAddressStr, tp: mysql.TypeVarchar, size: 256, comment: "Address of the CopTask with max processing time"},
	{name: stmtsummary.CopWaitTimeStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Total waiting time of CopTasks"},
	{name: stmtsummary.MaxCopWaitAddressStr, tp: mysql.TypeVarchar, size: 256, comment: "Address of the CopTask with max waiting time"},
	{name: stmtsummary.PdTimeStr, tp: mysql.TypeLonglong, size: 22, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Total time of PD used"},
	{name: stmtsummary.KvTimeStr, tp: mysql.TypeLonglong, size: 22, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Total time of TiKV used"},
	{name: stmtsummary.ProcessTimeStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Total processing time in TiKV"},
	{name: stmtsummary.WaitTimeStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Total waiting time in TiKV"},
	{name: stmtsummary.BackoffTimeStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Total waiting time before retry"},
	{name: stmtsummary.TotalKeysStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Total number of scanned keys"},
	{name: stmtsummary.ProcessedKeysStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Total number of processed keys"},
	{name: stmtsummary.RocksdbDeleteSkippedCountStr, tp: mysql.TypeDouble, size: 22, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Total RocksDB delete skipped count"},
	{name: stmtsummary.RocksdbKeySkippedCountStr, tp: mysql.TypeDouble, size: 22, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Total RocksDB key skipped count"},
	{name: stmtsummary.RocksdbBlockCacheHitCountStr, tp: mysql.TypeDouble, size: 22, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Total RocksDB block cache hit count"},
	{name: stmtsummary.RocksdbBlockReadCountStr, tp: mysql.TypeDouble, size: 22, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Total RocksDB block read count"},
	{name: stmtsummary.RocksdbBlockReadByteStr, tp: mysql.TypeDouble, size: 22, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Total RocksDB block read byte"},
	{name: stmtsummary.PrewriteTimeStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Total time of prewrite phase"},
	{name: stmtsummary.CommitTimeStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Total time of commit phase"},
	{name: stmtsummary.CommitTsTimeStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Total time of getting commit_ts"},
	{name: stmtsummary.CommitBackoffTimeStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Total time before retry during commit phase"},
	{name: stmtsummary.ResolveLockTimeStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Total time for resolving locks"},
	{name: stmtsummary.LocalLatchWaitTimeStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Total waiting time of local transaction"},
	{name: stmtsummary.WriteKeysStr, tp: mysql.TypeDouble, size: 22, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Total count of written keys"},
	{name: stmtsummary.WriteSizeStr, tp: mysql.TypeDouble, size: 22, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Total amount of written bytes"},
	{name: stmtsummary.PrewriteRegionsStr, tp: mysql.TypeDouble, size: 22, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Total number of involved regions in prewrite phase"},
	{name: stmtsummary.TxnRetryStr, tp: mysql.TypeDouble, size: 22, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Total number of transaction retries"},
	{name: stmtsummary.ExecRetryStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Sum number of execution retries in pessimistic transactions"},
	{name: stmtsummary.ExecRetryTimeStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Sum time of execution retries in pessimistic transactions"},
	{name: stmtsummary.BackoffTimesStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Sum of retries"},
	{name: stmtsummary.BackoffTypesStr, tp: mysql.TypeVarchar, size: 1024, comment: "Types of errors and the number of retries for each type"},
	{name: stmtsummary.BackoffTotalTimeStr, tp: mysql.TypeLonglong, size: 22, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Total time spent in backoff and retry"},
	{name: stmtsummary.WriteSQLRespTimeStr, tp: mysql.TypeLonglong, size: 22, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Total time used to write a response to the client."},
	{name: stmtsummary.ResultRowsStr, tp: mysql.TypeLonglong, size: 22, flag: mysql.NotNullFlag, comment: "Total count of SQL result rows"},
	{name: stmtsummary.AffectedRowsStr, tp: mysql.TypeDouble, size: 22, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Total number of rows affected"},
	{name: stmtsummary.PreparedStr, tp: mysql.TypeTiny, size: 1, flag: mysql.NotNullFlag, comment: "Whether prepared"},
	{name: stmtsummary.FirstSeenStr, tp: mysql.TypeTimestamp, size: 26, flag: mysql.NotNullFlag, comment: "The time these statements are seen for the first time"},
	{name: stmtsummary.LastSeenStr, tp: mysql.TypeTimestamp, size: 26, flag: mysql.NotNullFlag, comment: "The time these statements are seen for the last time"},
	{name: stmtsummary.PlanInCacheStr, tp: mysql.TypeTiny, size: 1, flag: mysql.NotNullFlag, comment: "Whether the last statement hit the plan cache"},
	{name: stmtsummary.PlanCacheHitsStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag, comment: "The number of times these statements hit the plan cache"},
	{name: stmtsummary.PlanInBindingStr, tp: mysql.TypeTiny, size: 1, flag: mysql.NotNullFlag, comment: "Whether the last statement is matched with the hints in the binding"},
	{name: stmtsummary.QuerySampleTextStr, tp: mysql.TypeBlob, size: types.UnspecifiedLength, comment: "Sampled original statement"},
	{name: stmtsummary.PrevSampleTextStr, tp: mysql.TypeBlob, size: types.UnspecifiedLength, comment: "The previous statement before commit"},
	{name: stmtsummary.PlanDigestStr, tp: mysql.TypeVarchar, size: 64, comment: "Digest of its execution plan"},
	{name: stmtsummary.PlanStr, tp: mysql.TypeBlob, size: types.UnspecifiedLength, comment: "Sampled execution plan"},
	{name: stmtsummary.BinaryPlan, tp: mysql.TypeBlob, size: types.UnspecifiedLength, comment: "Sampled binary plan"},
	{name: stmtsummary.BindingDigestStr, tp: mysql.TypeVarchar, size: 64, comment: "Digest of normalized statement for bindings"},
	{name: stmtsummary.BindingDigestTextStr, tp: mysql.TypeBlob, size: types.UnspecifiedLength, flag: mysql.NotNullFlag, comment: "Normalized statement for bindings"},
	{name: stmtsummary.Charset, tp: mysql.TypeVarchar, size: 64, comment: "Sampled charset"},
	{name: stmtsummary.Collation, tp: mysql.TypeVarchar, size: 64, comment: "Sampled collation"},
	{name: stmtsummary.PlanHint, tp: mysql.TypeBlob, size: types.UnspecifiedLength, comment: "Sampled plan hint"},
	{name: stmtsummary.RequestUnitReadStr, tp: mysql.TypeDouble, flag: mysql.NotNullFlag | mysql.UnsignedFlag, size: 22, comment: "Total read request-unit cost of these statements"},
	{name: stmtsummary.RequestUnitWriteStr, tp: mysql.TypeDouble, flag: mysql.NotNullFlag | mysql.UnsignedFlag, size: 22, comment: "Total write request-unit cost of these statements"},
	{name: stmtsummary.QueuedRcTimeStr, tp: mysql.TypeLonglong, size: 22, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Total time waiting for available request-units"},
	{name: stmtsummary.ResourceGroupName, tp: mysql.TypeVarchar, size: 64, comment: "Bind resource group name"},
	{name: stmtsummary.UnpackedBytesSentTiKVTotalStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Total bytes sent to tikv"},
	{name: stmtsummary.UnpackedBytesReceivedTiKVTotalStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Total bytes received from tikv"},
	{name: stmtsummary.UnpackedBytesSentTiKVCrossZoneStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Total bytes sent to tikv cross zone"},
	{name: stmtsummary.UnpackedBytesReceivedTiKVCrossZoneStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Total bytes received from tikv cross zone"},
	{name: stmtsummary.UnpackedBytesSentTiFlashTotalStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Total bytes sent to tiflash"},
	{name: stmtsummary.UnpackedBytesReceivedTiFlashTotalStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Total bytes received from tiflash"},
	{name: stmtsummary.UnpackedBytesSentTiFlashCrossZoneStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Total bytes sent to tiflash cross zone"},
	{name: stmtsummary.UnpackedBytesReceiveTiFlashCrossZoneStr, tp: mysql.TypeLonglong, size: 20, flag: mysql.NotNullFlag | mysql.UnsignedFlag, comment: "Total bytes received from tiflash cross zone"},
	{name: stmtsummary.StorageKVStr, tp: mysql.TypeTiny, size: 1, flag: mysql.NotNullFlag, comment: "Whether the last statement read data from TiKV"},
	{name: stmtsummary.StorageMPPStr, tp: mysql.TypeTiny, size: 1, flag: mysql.NotNullFlag, comment: "Whether the last statement read data from TiFlash"},
}

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
