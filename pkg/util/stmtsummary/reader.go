// Copyright 2021 PingCAP, Inc.
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

package stmtsummary

import (
	"fmt"
	"time"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/set"
)

// stmtSummaryReader uses to read the statement summaries data and convert to []datum row.
type stmtSummaryReader struct {
	user *auth.UserIdentity
	// If the user has the 'PROCESS' privilege, he can read all the statements.
	hasProcessPriv       bool
	columns              []*model.ColumnInfo
	instanceAddr         string
	ssMap                *stmtSummaryByDigestMap
	columnValueFactories []columnValueFactory
	checker              *stmtSummaryChecker
	tz                   *time.Location
}

// NewStmtSummaryReader return a new statement summaries reader.
func NewStmtSummaryReader(user *auth.UserIdentity, hasProcessPriv bool, cols []*model.ColumnInfo, instanceAddr string, tz *time.Location) *stmtSummaryReader {
	reader := &stmtSummaryReader{
		user:           user,
		hasProcessPriv: hasProcessPriv,
		columns:        cols,
		instanceAddr:   instanceAddr,
		ssMap:          StmtSummaryByDigestMap,
		tz:             tz,
	}
	// initialize column value factories.
	reader.columnValueFactories = make([]columnValueFactory, len(reader.columns))
	for i, col := range reader.columns {
		factory, ok := columnValueFactoryMap[col.Name.O]
		if !ok {
			panic(fmt.Sprintf("should never happen, should register new column %v into columnValueFactoryMap", col.Name.O))
		}
		reader.columnValueFactories[i] = factory
	}
	return reader
}

// GetStmtSummaryCumulativeRows gets statement summary rows with cumulative metrics.
func (ssr *stmtSummaryReader) GetStmtSummaryCumulativeRows() [][]types.Datum {
	ssMap := ssr.ssMap
	ssMap.Lock()
	values := ssMap.summaryMap.Values()
	ssMap.Unlock()

	rows := make([][]types.Datum, 0, len(values))
	for _, value := range values {
		ssbd := value.(*stmtSummaryByDigest)
		if ssr.checker != nil && !ssr.checker.isDigestValid(ssbd.digest) {
			continue
		}
		record := ssr.getStmtByDigestCumulativeRow(ssbd)
		if record != nil {
			rows = append(rows, record)
		}
	}
	return rows
}

// GetStmtSummaryCurrentRows gets all current statement summaries rows.
func (ssr *stmtSummaryReader) GetStmtSummaryCurrentRows() [][]types.Datum {
	ssMap := ssr.ssMap
	ssMap.Lock()
	values := ssMap.summaryMap.Values()
	beginTime := ssMap.beginTimeForCurInterval
	other := ssMap.other
	ssMap.Unlock()

	rows := make([][]types.Datum, 0, len(values))
	for _, value := range values {
		ssbd := value.(*stmtSummaryByDigest)
		if ssr.checker != nil && !ssr.checker.isDigestValid(ssbd.digest) {
			continue
		}
		record := ssr.getStmtByDigestRow(ssbd, beginTime)
		if record != nil {
			rows = append(rows, record)
		}
	}
	if ssr.checker == nil {
		if otherDatum := ssr.getStmtEvictedOtherRow(other); otherDatum != nil {
			rows = append(rows, otherDatum)
		}
	}
	return rows
}

// GetStmtSummaryHistoryRows gets all history statement summaries rows.
func (ssr *stmtSummaryReader) GetStmtSummaryHistoryRows() [][]types.Datum {
	ssMap := ssr.ssMap
	ssMap.Lock()
	values := ssMap.summaryMap.Values()
	other := ssMap.other
	ssMap.Unlock()

	historySize := ssMap.historySize()
	rows := make([][]types.Datum, 0, len(values)*historySize)
	for _, value := range values {
		records := ssr.getStmtByDigestHistoryRow(value.(*stmtSummaryByDigest), historySize)
		rows = append(rows, records...)
	}

	if ssr.checker == nil {
		otherDatum := ssr.getStmtEvictedOtherHistoryRow(other, historySize)
		rows = append(rows, otherDatum...)
	}
	return rows
}

func (ssr *stmtSummaryReader) SetChecker(checker *stmtSummaryChecker) {
	ssr.checker = checker
}

func (ssr *stmtSummaryReader) isAuthed(ssStats *stmtSummaryStats) bool {
	isAuthed := true
	if ssr.user != nil && !ssr.hasProcessPriv {
		_, isAuthed = ssStats.authUsers[ssr.user.Username]
	}
	return isAuthed
}

func (ssr *stmtSummaryReader) getStmtByDigestCumulativeRow(ssbd *stmtSummaryByDigest) []types.Datum {
	ssbd.Lock()
	defer ssbd.Unlock()
	if !ssr.isAuthed(&ssbd.cumulative) {
		return nil
	}

	datums := make([]types.Datum, len(ssr.columnValueFactories))
	for i, factory := range ssr.columnValueFactories {
		datums[i] = types.NewDatum(factory(ssr, nil, ssbd, &ssbd.cumulative))
	}
	return datums
}

func (ssr *stmtSummaryReader) getStmtByDigestRow(ssbd *stmtSummaryByDigest, beginTimeForCurInterval int64) []types.Datum {
	var ssElement *stmtSummaryByDigestElement

	ssbd.Lock()
	if ssbd.initialized && ssbd.history.Len() > 0 {
		ssElement = ssbd.history.Back().Value.(*stmtSummaryByDigestElement)
	}
	ssbd.Unlock()

	// `ssElement` is lazy expired, so expired elements could also be read.
	// `beginTime` won't change since `ssElement` is created, so locking is not needed here.
	if ssElement == nil || ssElement.beginTime < beginTimeForCurInterval {
		return nil
	}
	return ssr.getStmtByDigestElementRow(ssElement, ssbd)
}

func (ssr *stmtSummaryReader) getStmtByDigestElementRow(ssElement *stmtSummaryByDigestElement, ssbd *stmtSummaryByDigest) []types.Datum {
	ssElement.Lock()
	defer ssElement.Unlock()
	if !ssr.isAuthed(&ssElement.stmtSummaryStats) {
		return nil
	}

	datums := make([]types.Datum, len(ssr.columnValueFactories))
	for i, factory := range ssr.columnValueFactories {
		datums[i] = types.NewDatum(factory(ssr, ssElement, ssbd, &ssElement.stmtSummaryStats))
	}
	return datums
}

func (ssr *stmtSummaryReader) getStmtByDigestHistoryRow(ssbd *stmtSummaryByDigest, historySize int) [][]types.Datum {
	// Collect all history summaries to an array.
	ssElements := ssbd.collectHistorySummaries(ssr.checker, historySize)

	rows := make([][]types.Datum, 0, len(ssElements))
	for _, ssElement := range ssElements {
		record := ssr.getStmtByDigestElementRow(ssElement, ssbd)
		if record != nil {
			rows = append(rows, record)
		}
	}
	return rows
}

func (ssr *stmtSummaryReader) getStmtEvictedOtherRow(ssbde *stmtSummaryByDigestEvicted) []types.Datum {
	var seElement *stmtSummaryByDigestEvictedElement

	ssbde.Lock()
	if ssbde.history.Len() > 0 {
		seElement = ssbde.history.Back().Value.(*stmtSummaryByDigestEvictedElement)
	}
	ssbde.Unlock()

	if seElement == nil {
		return nil
	}

	return ssr.getStmtByDigestElementRow(seElement.otherSummary, new(stmtSummaryByDigest))
}

func (ssr *stmtSummaryReader) getStmtEvictedOtherHistoryRow(ssbde *stmtSummaryByDigestEvicted, historySize int) [][]types.Datum {
	// Collect all history summaries to an array.
	ssbde.Lock()
	seElements := ssbde.collectHistorySummaries(historySize)
	ssbde.Unlock()
	rows := make([][]types.Datum, 0, len(seElements))

	ssbd := new(stmtSummaryByDigest)
	for _, seElement := range seElements {
		record := ssr.getStmtByDigestElementRow(seElement.otherSummary, ssbd)
		if record != nil {
			rows = append(rows, record)
		}
	}
	return rows
}

type stmtSummaryChecker struct {
	digests set.StringSet
}

// NewStmtSummaryChecker return a new statement summaries checker.
func NewStmtSummaryChecker(digests set.StringSet) *stmtSummaryChecker {
	return &stmtSummaryChecker{
		digests: digests,
	}
}

func (ssc *stmtSummaryChecker) isDigestValid(digest string) bool {
	return ssc.digests.Exist(digest)
}

// Statements summary table column name.
const (
	ClusterTableInstanceColumnNameStr          = "INSTANCE"
	SummaryBeginTimeStr                        = "SUMMARY_BEGIN_TIME"
	SummaryEndTimeStr                          = "SUMMARY_END_TIME"
	StmtTypeStr                                = "STMT_TYPE"
	SchemaNameStr                              = "SCHEMA_NAME"
	DigestStr                                  = "DIGEST"
	DigestTextStr                              = "DIGEST_TEXT"
	TableNamesStr                              = "TABLE_NAMES"
	IndexNamesStr                              = "INDEX_NAMES"
	SampleUserStr                              = "SAMPLE_USER"
	ExecCountStr                               = "EXEC_COUNT"
	SumErrorsStr                               = "SUM_ERRORS"
	SumWarningsStr                             = "SUM_WARNINGS"
	SumLatencyStr                              = "SUM_LATENCY"
	MaxLatencyStr                              = "MAX_LATENCY"
	MinLatencyStr                              = "MIN_LATENCY"
	AvgLatencyStr                              = "AVG_LATENCY"
	AvgParseLatencyStr                         = "AVG_PARSE_LATENCY"
	MaxParseLatencyStr                         = "MAX_PARSE_LATENCY"
	AvgCompileLatencyStr                       = "AVG_COMPILE_LATENCY"
	MaxCompileLatencyStr                       = "MAX_COMPILE_LATENCY"
	SumCopTaskNumStr                           = "SUM_COP_TASK_NUM"
	MaxCopProcessTimeStr                       = "MAX_COP_PROCESS_TIME"
	MaxCopProcessAddressStr                    = "MAX_COP_PROCESS_ADDRESS"
	MaxCopWaitTimeStr                          = "MAX_COP_WAIT_TIME"    // #nosec G101
	MaxCopWaitAddressStr                       = "MAX_COP_WAIT_ADDRESS" // #nosec G101
	AvgProcessTimeStr                          = "AVG_PROCESS_TIME"
	MaxProcessTimeStr                          = "MAX_PROCESS_TIME"
	AvgWaitTimeStr                             = "AVG_WAIT_TIME"
	MaxWaitTimeStr                             = "MAX_WAIT_TIME"
	AvgBackoffTimeStr                          = "AVG_BACKOFF_TIME"
	MaxBackoffTimeStr                          = "MAX_BACKOFF_TIME"
	AvgTotalKeysStr                            = "AVG_TOTAL_KEYS"
	MaxTotalKeysStr                            = "MAX_TOTAL_KEYS"
	AvgProcessedKeysStr                        = "AVG_PROCESSED_KEYS"
	MaxProcessedKeysStr                        = "MAX_PROCESSED_KEYS"
	AvgRocksdbDeleteSkippedCountStr            = "AVG_ROCKSDB_DELETE_SKIPPED_COUNT"
	MaxRocksdbDeleteSkippedCountStr            = "MAX_ROCKSDB_DELETE_SKIPPED_COUNT"
	AvgRocksdbKeySkippedCountStr               = "AVG_ROCKSDB_KEY_SKIPPED_COUNT"
	MaxRocksdbKeySkippedCountStr               = "MAX_ROCKSDB_KEY_SKIPPED_COUNT"
	AvgRocksdbBlockCacheHitCountStr            = "AVG_ROCKSDB_BLOCK_CACHE_HIT_COUNT"
	MaxRocksdbBlockCacheHitCountStr            = "MAX_ROCKSDB_BLOCK_CACHE_HIT_COUNT"
	AvgRocksdbBlockReadCountStr                = "AVG_ROCKSDB_BLOCK_READ_COUNT"
	MaxRocksdbBlockReadCountStr                = "MAX_ROCKSDB_BLOCK_READ_COUNT"
	AvgRocksdbBlockReadByteStr                 = "AVG_ROCKSDB_BLOCK_READ_BYTE"
	MaxRocksdbBlockReadByteStr                 = "MAX_ROCKSDB_BLOCK_READ_BYTE"
	AvgPrewriteTimeStr                         = "AVG_PREWRITE_TIME"
	MaxPrewriteTimeStr                         = "MAX_PREWRITE_TIME"
	AvgCommitTimeStr                           = "AVG_COMMIT_TIME"
	MaxCommitTimeStr                           = "MAX_COMMIT_TIME"
	AvgGetCommitTsTimeStr                      = "AVG_GET_COMMIT_TS_TIME"
	MaxGetCommitTsTimeStr                      = "MAX_GET_COMMIT_TS_TIME"
	AvgCommitBackoffTimeStr                    = "AVG_COMMIT_BACKOFF_TIME"
	MaxCommitBackoffTimeStr                    = "MAX_COMMIT_BACKOFF_TIME"
	AvgResolveLockTimeStr                      = "AVG_RESOLVE_LOCK_TIME"
	MaxResolveLockTimeStr                      = "MAX_RESOLVE_LOCK_TIME"
	AvgLocalLatchWaitTimeStr                   = "AVG_LOCAL_LATCH_WAIT_TIME"
	MaxLocalLatchWaitTimeStr                   = "MAX_LOCAL_LATCH_WAIT_TIME"
	AvgWriteKeysStr                            = "AVG_WRITE_KEYS"
	MaxWriteKeysStr                            = "MAX_WRITE_KEYS"
	AvgWriteSizeStr                            = "AVG_WRITE_SIZE"
	MaxWriteSizeStr                            = "MAX_WRITE_SIZE"
	AvgPrewriteRegionsStr                      = "AVG_PREWRITE_REGIONS"
	MaxPrewriteRegionsStr                      = "MAX_PREWRITE_REGIONS"
	AvgTxnRetryStr                             = "AVG_TXN_RETRY"
	MaxTxnRetryStr                             = "MAX_TXN_RETRY"
	SumExecRetryStr                            = "SUM_EXEC_RETRY"
	SumExecRetryTimeStr                        = "SUM_EXEC_RETRY_TIME"
	SumBackoffTimesStr                         = "SUM_BACKOFF_TIMES"
	BackoffTypesStr                            = "BACKOFF_TYPES"
	AvgMemStr                                  = "AVG_MEM"
	MaxMemStr                                  = "MAX_MEM"
	AvgMemArbitrationStr                       = "AVG_MEM_ARBITRATION"
	MaxMemArbitrationStr                       = "MAX_MEM_ARBITRATION"
	AvgDiskStr                                 = "AVG_DISK"
	MaxDiskStr                                 = "MAX_DISK"
	AvgKvTimeStr                               = "AVG_KV_TIME"
	AvgPdTimeStr                               = "AVG_PD_TIME"
	AvgBackoffTotalTimeStr                     = "AVG_BACKOFF_TOTAL_TIME"
	AvgWriteSQLRespTimeStr                     = "AVG_WRITE_SQL_RESP_TIME"
	AvgTidbCPUTimeStr                          = "AVG_TIDB_CPU_TIME"
	AvgTikvCPUTimeStr                          = "AVG_TIKV_CPU_TIME"
	MaxResultRowsStr                           = "MAX_RESULT_ROWS"
	MinResultRowsStr                           = "MIN_RESULT_ROWS"
	AvgResultRowsStr                           = "AVG_RESULT_ROWS"
	PreparedStr                                = "PREPARED"
	AvgAffectedRowsStr                         = "AVG_AFFECTED_ROWS"
	FirstSeenStr                               = "FIRST_SEEN"
	LastSeenStr                                = "LAST_SEEN"
	PlanInCacheStr                             = "PLAN_IN_CACHE"
	PlanCacheHitsStr                           = "PLAN_CACHE_HITS"
	PlanCacheUnqualifiedStr                    = "PLAN_CACHE_UNQUALIFIED"
	PlanCacheUnqualifiedLastReasonStr          = "PLAN_CACHE_UNQUALIFIED_LAST_REASON"
	PlanInBindingStr                           = "PLAN_IN_BINDING"
	QuerySampleTextStr                         = "QUERY_SAMPLE_TEXT"
	PrevSampleTextStr                          = "PREV_SAMPLE_TEXT"
	PlanDigestStr                              = "PLAN_DIGEST"
	PlanStr                                    = "PLAN"
	BinaryPlan                                 = "BINARY_PLAN"
	BindingDigestStr                           = "BINDING_DIGEST"
	BindingDigestTextStr                       = "BINDING_DIGEST_TEXT"
	Charset                                    = "CHARSET"
	Collation                                  = "COLLATION"
	PlanHint                                   = "PLAN_HINT"
	AvgRequestUnitReadStr                      = "AVG_REQUEST_UNIT_READ"
	MaxRequestUnitReadStr                      = "MAX_REQUEST_UNIT_READ"
	AvgRequestUnitWriteStr                     = "AVG_REQUEST_UNIT_WRITE"
	MaxRequestUnitWriteStr                     = "MAX_REQUEST_UNIT_WRITE"
	AvgQueuedRcTimeStr                         = "AVG_QUEUED_RC_TIME"
	MaxQueuedRcTimeStr                         = "MAX_QUEUED_RC_TIME"
	ResourceGroupName                          = "RESOURCE_GROUP"
	SumUnpackedBytesSentTiKVTotalStr           = "SUM_UNPACKED_BYTES_SENT_TIKV_TOTAL"
	SumUnpackedBytesReceivedTiKVTotalStr       = "SUM_UNPACKED_BYTES_RECEIVED_TIKV_TOTAL"
	SumUnpackedBytesSentTiKVCrossZoneStr       = "SUM_UNPACKED_BYTES_SENT_TIKV_CROSS_ZONE"
	SumUnpackedBytesReceivedTiKVCrossZoneStr   = "SUM_UNPACKED_BYTES_RECEIVED_TIKV_CROSS_ZONE"
	SumUnpackedBytesSentTiFlashTotalStr        = "SUM_UNPACKED_BYTES_SENT_TIFLASH_TOTAL"
	SumUnpackedBytesReceivedTiFlashTotalStr    = "SUM_UNPACKED_BYTES_RECEIVED_TIFLASH_TOTAL"
	SumUnpackedBytesSentTiFlashCrossZoneStr    = "SUM_UNPACKED_BYTES_SENT_TIFLASH_CROSS_ZONE"
	SumUnpackedBytesReceiveTiFlashCrossZoneStr = "SUM_UNPACKED_BYTES_RECEIVED_TIFLASH_CROSS_ZONE"
	StorageKVStr                               = "STORAGE_KV"
	StorageMPPStr                              = "STORAGE_MPP"
)

// Column names for the statement stats table, including columns that have been
// renamed from their equivalent columns in the statement summary table.
const (
	ErrorsStr                               = "ERRORS"
	WarningsStr                             = "WARNINGS"
	MemStr                                  = "MEM"
	MemArbitrationStr                       = "MEM_ARBITRATION"
	DiskStr                                 = "DISK"
	TotalTimeStr                            = "TOTAL_TIME"
	ParseTimeStr                            = "PARSE_TIME"
	CompileTimeStr                          = "COMPILE_TIME"
	CopTaskNumStr                           = "COP_TASK_NUM"
	CopProcessTimeStr                       = "COP_PROCESS_TIME"
	CopWaitTimeStr                          = "COP_WAIT_TIME"
	PdTimeStr                               = "PD_TIME"
	KvTimeStr                               = "KV_TIME"
	ProcessTimeStr                          = "PROCESS_TIME"
	WaitTimeStr                             = "WAIT_TIME"
	BackoffTimeStr                          = "BACKOFF_TIME"
	TotalKeysStr                            = "TOTAL_KEYS"
	ProcessedKeysStr                        = "PROCESSED_KEYS"
	RocksdbDeleteSkippedCountStr            = "ROCKSDB_DELETE_SKIPPED_COUNT"
	RocksdbKeySkippedCountStr               = "ROCKSDB_KEY_SKIPPED_COUNT"
	RocksdbBlockCacheHitCountStr            = "ROCKSDB_BLOCK_CACHE_HIT_COUNT"
	RocksdbBlockReadCountStr                = "ROCKSDB_BLOCK_READ_COUNT"
	RocksdbBlockReadByteStr                 = "ROCKSDB_BLOCK_READ_BYTE"
	PrewriteTimeStr                         = "PREWRITE_TIME"
	CommitTimeStr                           = "COMMIT_TIME"
	CommitTsTimeStr                         = "COMMIT_TS_TIME"
	CommitBackoffTimeStr                    = "COMMIT_BACKOFF_TIME"
	ResolveLockTimeStr                      = "RESOLVE_LOCK_TIME"
	LocalLatchWaitTimeStr                   = "LOCAL_LATCH_WAIT_TIME"
	WriteKeysStr                            = "WRITE_KEYS"
	WriteSizeStr                            = "WRITE_SIZE"
	PrewriteRegionsStr                      = "PREWRITE_REGIONS"
	TxnRetryStr                             = "TXN_RETRY"
	ExecRetryStr                            = "EXEC_RETRY"
	ExecRetryTimeStr                        = "EXEC_RETRY_TIME"
	BackoffTimesStr                         = "BACKOFF_TIMES"
	BackoffTotalTimeStr                     = "BACKOFF_TOTAL_TIME"
	WriteSQLRespTimeStr                     = "WRITE_SQL_RESP_TIME"
	ResultRowsStr                           = "RESULT_ROWS"
	AffectedRowsStr                         = "AFFECTED_ROWS"
	RequestUnitReadStr                      = "REQUEST_UNIT_READ"
	RequestUnitWriteStr                     = "REQUEST_UNIT_WRITE"
	QueuedRcTimeStr                         = "QUEUED_RC_TIME"
	UnpackedBytesSentTiKVTotalStr           = "UNPACKED_BYTES_SENT_TIKV_TOTAL"
	UnpackedBytesReceivedTiKVTotalStr       = "UNPACKED_BYTES_RECEIVED_TIKV_TOTAL"
	UnpackedBytesSentTiKVCrossZoneStr       = "UNPACKED_BYTES_SENT_TIKV_CROSS_ZONE"
	UnpackedBytesReceivedTiKVCrossZoneStr   = "UNPACKED_BYTES_RECEIVED_TIKV_CROSS_ZONE"
	UnpackedBytesSentTiFlashTotalStr        = "UNPACKED_BYTES_SENT_TIFLASH_TOTAL"
	UnpackedBytesReceivedTiFlashTotalStr    = "UNPACKED_BYTES_RECEIVED_TIFLASH_TOTAL"
	UnpackedBytesSentTiFlashCrossZoneStr    = "UNPACKED_BYTES_SENT_TIFLASH_CROSS_ZONE"
	UnpackedBytesReceiveTiFlashCrossZoneStr = "UNPACKED_BYTES_RECEIVED_TIFLASH_CROSS_ZONE"
)

