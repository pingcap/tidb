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
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/set"
	"go.uber.org/zap"
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
	ClusterTableInstanceColumnNameStr      = "INSTANCE"
	SummaryBeginTimeStr                    = "SUMMARY_BEGIN_TIME"
	SummaryEndTimeStr                      = "SUMMARY_END_TIME"
	StmtTypeStr                            = "STMT_TYPE"
	SchemaNameStr                          = "SCHEMA_NAME"
	DigestStr                              = "DIGEST"
	DigestTextStr                          = "DIGEST_TEXT"
	TableNamesStr                          = "TABLE_NAMES"
	IndexNamesStr                          = "INDEX_NAMES"
	SampleUserStr                          = "SAMPLE_USER"
	ExecCountStr                           = "EXEC_COUNT"
	SumErrorsStr                           = "SUM_ERRORS"
	SumWarningsStr                         = "SUM_WARNINGS"
	SumLatencyStr                          = "SUM_LATENCY"
	MaxLatencyStr                          = "MAX_LATENCY"
	MinLatencyStr                          = "MIN_LATENCY"
	AvgLatencyStr                          = "AVG_LATENCY"
	AvgParseLatencyStr                     = "AVG_PARSE_LATENCY"
	MaxParseLatencyStr                     = "MAX_PARSE_LATENCY"
	AvgCompileLatencyStr                   = "AVG_COMPILE_LATENCY"
	MaxCompileLatencyStr                   = "MAX_COMPILE_LATENCY"
	SumCopTaskNumStr                       = "SUM_COP_TASK_NUM"
	MaxCopProcessTimeStr                   = "MAX_COP_PROCESS_TIME"
	MaxCopProcessAddressStr                = "MAX_COP_PROCESS_ADDRESS"
	MaxCopWaitTimeStr                      = "MAX_COP_WAIT_TIME"    // #nosec G101
	MaxCopWaitAddressStr                   = "MAX_COP_WAIT_ADDRESS" // #nosec G101
	AvgProcessTimeStr                      = "AVG_PROCESS_TIME"
	MaxProcessTimeStr                      = "MAX_PROCESS_TIME"
	AvgWaitTimeStr                         = "AVG_WAIT_TIME"
	MaxWaitTimeStr                         = "MAX_WAIT_TIME"
	AvgBackoffTimeStr                      = "AVG_BACKOFF_TIME"
	MaxBackoffTimeStr                      = "MAX_BACKOFF_TIME"
	AvgTotalKeysStr                        = "AVG_TOTAL_KEYS"
	MaxTotalKeysStr                        = "MAX_TOTAL_KEYS"
	AvgProcessedKeysStr                    = "AVG_PROCESSED_KEYS"
	MaxProcessedKeysStr                    = "MAX_PROCESSED_KEYS"
	AvgRocksdbDeleteSkippedCountStr        = "AVG_ROCKSDB_DELETE_SKIPPED_COUNT"
	MaxRocksdbDeleteSkippedCountStr        = "MAX_ROCKSDB_DELETE_SKIPPED_COUNT"
	AvgRocksdbKeySkippedCountStr           = "AVG_ROCKSDB_KEY_SKIPPED_COUNT"
	MaxRocksdbKeySkippedCountStr           = "MAX_ROCKSDB_KEY_SKIPPED_COUNT"
	AvgRocksdbBlockCacheHitCountStr        = "AVG_ROCKSDB_BLOCK_CACHE_HIT_COUNT"
	MaxRocksdbBlockCacheHitCountStr        = "MAX_ROCKSDB_BLOCK_CACHE_HIT_COUNT"
	AvgRocksdbBlockReadCountStr            = "AVG_ROCKSDB_BLOCK_READ_COUNT"
	MaxRocksdbBlockReadCountStr            = "MAX_ROCKSDB_BLOCK_READ_COUNT"
	AvgRocksdbBlockReadByteStr             = "AVG_ROCKSDB_BLOCK_READ_BYTE"
	MaxRocksdbBlockReadByteStr             = "MAX_ROCKSDB_BLOCK_READ_BYTE"
	AvgPrewriteTimeStr                     = "AVG_PREWRITE_TIME"
	MaxPrewriteTimeStr                     = "MAX_PREWRITE_TIME"
	AvgCommitTimeStr                       = "AVG_COMMIT_TIME"
	MaxCommitTimeStr                       = "MAX_COMMIT_TIME"
	AvgGetCommitTsTimeStr                  = "AVG_GET_COMMIT_TS_TIME"
	MaxGetCommitTsTimeStr                  = "MAX_GET_COMMIT_TS_TIME"
	AvgCommitBackoffTimeStr                = "AVG_COMMIT_BACKOFF_TIME"
	MaxCommitBackoffTimeStr                = "MAX_COMMIT_BACKOFF_TIME"
	AvgResolveLockTimeStr                  = "AVG_RESOLVE_LOCK_TIME"
	MaxResolveLockTimeStr                  = "MAX_RESOLVE_LOCK_TIME"
	AvgLocalLatchWaitTimeStr               = "AVG_LOCAL_LATCH_WAIT_TIME"
	MaxLocalLatchWaitTimeStr               = "MAX_LOCAL_LATCH_WAIT_TIME"
	AvgWriteKeysStr                        = "AVG_WRITE_KEYS"
	MaxWriteKeysStr                        = "MAX_WRITE_KEYS"
	AvgWriteSizeStr                        = "AVG_WRITE_SIZE"
	MaxWriteSizeStr                        = "MAX_WRITE_SIZE"
	AvgPrewriteRegionsStr                  = "AVG_PREWRITE_REGIONS"
	MaxPrewriteRegionsStr                  = "MAX_PREWRITE_REGIONS"
	AvgTxnRetryStr                         = "AVG_TXN_RETRY"
	MaxTxnRetryStr                         = "MAX_TXN_RETRY"
	SumExecRetryStr                        = "SUM_EXEC_RETRY"
	SumExecRetryTimeStr                    = "SUM_EXEC_RETRY_TIME"
	SumBackoffTimesStr                     = "SUM_BACKOFF_TIMES"
	BackoffTypesStr                        = "BACKOFF_TYPES"
	AvgMemStr                              = "AVG_MEM"
	MaxMemStr                              = "MAX_MEM"
	AvgDiskStr                             = "AVG_DISK"
	MaxDiskStr                             = "MAX_DISK"
	AvgKvTimeStr                           = "AVG_KV_TIME"
	AvgPdTimeStr                           = "AVG_PD_TIME"
	AvgBackoffTotalTimeStr                 = "AVG_BACKOFF_TOTAL_TIME"
	AvgWriteSQLRespTimeStr                 = "AVG_WRITE_SQL_RESP_TIME"
	AvgTidbCPUTimeStr                      = "AVG_TIDB_CPU_TIME"
	AvgTikvCPUTimeStr                      = "AVG_TIKV_CPU_TIME"
	MaxResultRowsStr                       = "MAX_RESULT_ROWS"
	MinResultRowsStr                       = "MIN_RESULT_ROWS"
	AvgResultRowsStr                       = "AVG_RESULT_ROWS"
	PreparedStr                            = "PREPARED"
	AvgAffectedRowsStr                     = "AVG_AFFECTED_ROWS"
	FirstSeenStr                           = "FIRST_SEEN"
	LastSeenStr                            = "LAST_SEEN"
	PlanInCacheStr                         = "PLAN_IN_CACHE"
	PlanCacheHitsStr                       = "PLAN_CACHE_HITS"
	PlanCacheUnqualifiedStr                = "PLAN_CACHE_UNQUALIFIED"
	PlanCacheUnqualifiedLastReasonStr      = "PLAN_CACHE_UNQUALIFIED_LAST_REASON"
	PlanInBindingStr                       = "PLAN_IN_BINDING"
	QuerySampleTextStr                     = "QUERY_SAMPLE_TEXT"
	PrevSampleTextStr                      = "PREV_SAMPLE_TEXT"
	PlanDigestStr                          = "PLAN_DIGEST"
	PlanStr                                = "PLAN"
	BinaryPlan                             = "BINARY_PLAN"
	Charset                                = "CHARSET"
	Collation                              = "COLLATION"
	PlanHint                               = "PLAN_HINT"
	AvgRequestUnitReadStr                  = "AVG_REQUEST_UNIT_READ"
	MaxRequestUnitReadStr                  = "MAX_REQUEST_UNIT_READ"
	AvgRequestUnitWriteStr                 = "AVG_REQUEST_UNIT_WRITE"
	MaxRequestUnitWriteStr                 = "MAX_REQUEST_UNIT_WRITE"
	AvgQueuedRcTimeStr                     = "AVG_QUEUED_RC_TIME"
	MaxQueuedRcTimeStr                     = "MAX_QUEUED_RC_TIME"
	ResourceGroupName                      = "RESOURCE_GROUP"
	SumUnpackedBytesSentKVTotalStr         = "SUM_UNPACKED_BYTES_SENT_KV_TOTAL"
	SumUnpackedBytesReceivedKVTotalStr     = "SUM_UNPACKED_BYTES_RECEIVED_KV_TOTAL"
	SumUnpackedBytesSentKVCrossZoneStr     = "SUM_UNPACKED_BYTES_SENT_KV_CROSS_ZONE"
	SumUnpackedBytesReceivedKVCrossZoneStr = "SUM_UNPACKED_BYTES_RECEIVED_KV_CROSS_ZONE"
	SumUnpackedBytesSentMPPTotalStr        = "SUM_UNPACKED_BYTES_SENT_MPP_TOTAL"
	SumUnpackedBytesReceivedMPPTotalStr    = "SUM_UNPACKED_BYTES_RECEIVED_MPP_TOTAL"
	SumUnpackedBytesSentMPPCrossZoneStr    = "SUM_UNPACKED_BYTES_SENT_MPP_CROSS_ZONE"
	SumUnpackedBytesReceiveMPPCrossZoneStr = "SUM_UNPACKED_BYTES_RECEIVED_MPP_CROSS_ZONE"
)

// Column names for the statement stats table, including columns that have been
// renamed from their equivalent columns in the statement summary table.
const (
	ErrorsStr                    = "ERRORS"
	WarningsStr                  = "WARNINGS"
	MemStr                       = "MEM"
	DiskStr                      = "DISK"
	TotalTimeStr                 = "TOTAL_TIME"
	ParseTimeStr                 = "PARSE_TIME"
	CompileTimeStr               = "COMPILE_TIME"
	CopTaskNumStr                = "COP_TASK_NUM"
	CopProcessTimeStr            = "COP_PROCESS_TIME"
	CopWaitTimeStr               = "COP_WAIT_TIME"
	PdTimeStr                    = "PD_TIME"
	KvTimeStr                    = "KV_TIME"
	ProcessTimeStr               = "PROCESS_TIME"
	WaitTimeStr                  = "WAIT_TIME"
	BackoffTimeStr               = "BACKOFF_TIME"
	TotalKeysStr                 = "TOTAL_KEYS"
	ProcessedKeysStr             = "PROCESSED_KEYS"
	RocksdbDeleteSkippedCountStr = "ROCKSDB_DELETE_SKIPPED_COUNT"
	RocksdbKeySkippedCountStr    = "ROCKSDB_KEY_SKIPPED_COUNT"
	RocksdbBlockCacheHitCountStr = "ROCKSDB_BLOCK_CACHE_HIT_COUNT"
	RocksdbBlockReadCountStr     = "ROCKSDB_BLOCK_READ_COUNT"
	RocksdbBlockReadByteStr      = "ROCKSDB_BLOCK_READ_BYTE"
	PrewriteTimeStr              = "PREWRITE_TIME"
	CommitTimeStr                = "COMMIT_TIME"
	CommitTsTimeStr              = "COMMIT_TS_TIME"
	CommitBackoffTimeStr         = "COMMIT_BACKOFF_TIME"
	ResolveLockTimeStr           = "RESOLVE_LOCK_TIME"
	LocalLatchWaitTimeStr        = "LOCAL_LATCH_WAIT_TIME"
	WriteKeysStr                 = "WRITE_KEYS"
	WriteSizeStr                 = "WRITE_SIZE"
	PrewriteRegionsStr           = "PREWRITE_REGIONS"
	TxnRetryStr                  = "TXN_RETRY"
	ExecRetryStr                 = "EXEC_RETRY"
	ExecRetryTimeStr             = "EXEC_RETRY_TIME"
	BackoffTimesStr              = "BACKOFF_TIMES"
	BackoffTotalTimeStr          = "BACKOFF_TOTAL_TIME"
	WriteSQLRespTimeStr          = "WRITE_SQL_RESP_TIME"
	ResultRowsStr                = "RESULT_ROWS"
	AffectedRowsStr              = "AFFECTED_ROWS"
	RequestUnitReadStr           = "REQUEST_UNIT_READ"
	RequestUnitWriteStr          = "REQUEST_UNIT_WRITE"
	QueuedRcTimeStr              = "QUEUED_RC_TIME"
)

type columnValueFactory func(reader *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, ssbd *stmtSummaryByDigest, ssStats *stmtSummaryStats) any

var columnValueFactoryMap = map[string]columnValueFactory{
	ClusterTableInstanceColumnNameStr: func(reader *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, _ *stmtSummaryStats) any {
		return reader.instanceAddr
	},
	SummaryBeginTimeStr: func(reader *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, _ *stmtSummaryStats) any {
		beginTime := time.Unix(ssElement.beginTime, 0)
		if beginTime.Location() != reader.tz {
			beginTime = beginTime.In(reader.tz)
		}
		return types.NewTime(types.FromGoTime(beginTime), mysql.TypeTimestamp, 0)
	},
	SummaryEndTimeStr: func(reader *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, _ *stmtSummaryStats) any {
		endTime := time.Unix(ssElement.endTime, 0)
		if endTime.Location() != reader.tz {
			endTime = endTime.In(reader.tz)
		}
		return types.NewTime(types.FromGoTime(endTime), mysql.TypeTimestamp, 0)
	},
	StmtTypeStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, ssbd *stmtSummaryByDigest, _ *stmtSummaryStats) any {
		return ssbd.stmtType
	},
	SchemaNameStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, ssbd *stmtSummaryByDigest, _ *stmtSummaryStats) any {
		return convertEmptyToNil(ssbd.schemaName)
	},
	DigestStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, ssbd *stmtSummaryByDigest, _ *stmtSummaryStats) any {
		return convertEmptyToNil(ssbd.digest)
	},
	DigestTextStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, ssbd *stmtSummaryByDigest, _ *stmtSummaryStats) any {
		return ssbd.normalizedSQL
	},
	TableNamesStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, ssbd *stmtSummaryByDigest, _ *stmtSummaryStats) any {
		return convertEmptyToNil(ssbd.tableNames)
	},
	IndexNamesStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return convertEmptyToNil(strings.Join(ssStats.indexNames, ","))
	},
	SampleUserStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		sampleUser := ""
		for key := range ssStats.authUsers {
			sampleUser = key
			break
		}
		return convertEmptyToNil(sampleUser)
	},
	ExecCountStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.execCount
	},
	ErrorsStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.sumErrors
	},
	SumErrorsStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.sumErrors
	},
	WarningsStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.sumWarnings
	},
	SumWarningsStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.sumWarnings
	},
	TotalTimeStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return int64(ssStats.sumLatency)
	},
	SumLatencyStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return int64(ssStats.sumLatency)
	},
	MaxLatencyStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return int64(ssStats.maxLatency)
	},
	MinLatencyStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return int64(ssStats.minLatency)
	},
	AvgLatencyStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return avgInt(int64(ssStats.sumLatency), ssStats.execCount)
	},
	ParseTimeStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return int64(ssStats.sumParseLatency)
	},
	AvgParseLatencyStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return avgInt(int64(ssStats.sumParseLatency), ssStats.execCount)
	},
	MaxParseLatencyStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return int64(ssStats.maxParseLatency)
	},
	CompileTimeStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return int64(ssStats.sumCompileLatency)
	},
	AvgCompileLatencyStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return avgInt(int64(ssStats.sumCompileLatency), ssStats.execCount)
	},
	MaxCompileLatencyStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return int64(ssStats.maxCompileLatency)
	},
	CopTaskNumStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.sumNumCopTasks
	},
	SumCopTaskNumStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.sumNumCopTasks
	},
	CopProcessTimeStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return int64(ssStats.sumCopProcessTime)
	},
	MaxCopProcessTimeStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return int64(ssStats.maxCopProcessTime)
	},
	MaxCopProcessAddressStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return convertEmptyToNil(ssStats.maxCopProcessAddress)
	},
	CopWaitTimeStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return int64(ssStats.sumCopWaitTime)
	},
	MaxCopWaitTimeStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return int64(ssStats.maxCopWaitTime)
	},
	MaxCopWaitAddressStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return convertEmptyToNil(ssStats.maxCopWaitAddress)
	},
	ProcessTimeStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return int64(ssStats.sumProcessTime)
	},
	AvgProcessTimeStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return avgInt(int64(ssStats.sumProcessTime), ssStats.execCount)
	},
	MaxProcessTimeStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return int64(ssStats.maxProcessTime)
	},
	WaitTimeStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return int64(ssStats.sumWaitTime)
	},
	AvgWaitTimeStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return avgInt(int64(ssStats.sumWaitTime), ssStats.execCount)
	},
	MaxWaitTimeStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return int64(ssStats.maxWaitTime)
	},
	BackoffTimeStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return int64(ssStats.sumBackoffTime)
	},
	AvgBackoffTimeStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return avgInt(int64(ssStats.sumBackoffTime), ssStats.execCount)
	},
	MaxBackoffTimeStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return int64(ssStats.maxBackoffTime)
	},
	TotalKeysStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.sumTotalKeys
	},
	AvgTotalKeysStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return avgInt(ssStats.sumTotalKeys, ssStats.execCount)
	},
	MaxTotalKeysStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.maxTotalKeys
	},
	ProcessedKeysStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.sumProcessedKeys
	},
	AvgProcessedKeysStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return avgInt(ssStats.sumProcessedKeys, ssStats.execCount)
	},
	MaxProcessedKeysStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.maxProcessedKeys
	},
	RocksdbDeleteSkippedCountStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.sumRocksdbDeleteSkippedCount
	},
	AvgRocksdbDeleteSkippedCountStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return avgInt(int64(ssStats.sumRocksdbDeleteSkippedCount), ssStats.execCount)
	},
	MaxRocksdbDeleteSkippedCountStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.maxRocksdbDeleteSkippedCount
	},
	RocksdbKeySkippedCountStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.sumRocksdbKeySkippedCount
	},
	AvgRocksdbKeySkippedCountStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return avgInt(int64(ssStats.sumRocksdbKeySkippedCount), ssStats.execCount)
	},
	MaxRocksdbKeySkippedCountStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.maxRocksdbKeySkippedCount
	},
	RocksdbBlockCacheHitCountStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.sumRocksdbBlockCacheHitCount
	},
	AvgRocksdbBlockCacheHitCountStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return avgInt(int64(ssStats.sumRocksdbBlockCacheHitCount), ssStats.execCount)
	},
	MaxRocksdbBlockCacheHitCountStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.maxRocksdbBlockCacheHitCount
	},
	RocksdbBlockReadCountStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.sumRocksdbBlockReadCount
	},
	AvgRocksdbBlockReadCountStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return avgInt(int64(ssStats.sumRocksdbBlockReadCount), ssStats.execCount)
	},
	MaxRocksdbBlockReadCountStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.maxRocksdbBlockReadCount
	},
	RocksdbBlockReadByteStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.sumRocksdbBlockReadByte
	},
	AvgRocksdbBlockReadByteStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return avgInt(int64(ssStats.sumRocksdbBlockReadByte), ssStats.execCount)
	},
	MaxRocksdbBlockReadByteStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.maxRocksdbBlockReadByte
	},
	PrewriteTimeStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return int64(ssStats.sumPrewriteTime)
	},
	AvgPrewriteTimeStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return avgInt(int64(ssStats.sumPrewriteTime), ssStats.commitCount)
	},
	MaxPrewriteTimeStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return int64(ssStats.maxPrewriteTime)
	},
	CommitTimeStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return int64(ssStats.sumCommitTime)
	},
	AvgCommitTimeStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return avgInt(int64(ssStats.sumCommitTime), ssStats.commitCount)
	},
	MaxCommitTimeStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return int64(ssStats.maxCommitTime)
	},
	CommitTsTimeStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return int64(ssStats.sumGetCommitTsTime)
	},
	AvgGetCommitTsTimeStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return avgInt(int64(ssStats.sumGetCommitTsTime), ssStats.commitCount)
	},
	MaxGetCommitTsTimeStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return int64(ssStats.maxGetCommitTsTime)
	},
	CommitBackoffTimeStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.sumCommitBackoffTime
	},
	AvgCommitBackoffTimeStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return avgInt(ssStats.sumCommitBackoffTime, ssStats.commitCount)
	},
	MaxCommitBackoffTimeStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.maxCommitBackoffTime
	},
	ResolveLockTimeStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.sumResolveLockTime
	},
	AvgResolveLockTimeStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return avgInt(ssStats.sumResolveLockTime, ssStats.commitCount)
	},
	MaxResolveLockTimeStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.maxResolveLockTime
	},
	LocalLatchWaitTimeStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return int64(ssStats.sumLocalLatchTime)
	},
	AvgLocalLatchWaitTimeStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return avgInt(int64(ssStats.sumLocalLatchTime), ssStats.commitCount)
	},
	MaxLocalLatchWaitTimeStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return int64(ssStats.maxLocalLatchTime)
	},
	WriteKeysStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.sumWriteKeys
	},
	AvgWriteKeysStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return avgFloat(ssStats.sumWriteKeys, ssStats.commitCount)
	},
	MaxWriteKeysStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.maxWriteKeys
	},
	WriteSizeStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.sumWriteSize
	},
	AvgWriteSizeStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return avgFloat(ssStats.sumWriteSize, ssStats.commitCount)
	},
	MaxWriteSizeStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.maxWriteSize
	},
	PrewriteRegionsStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.sumPrewriteRegionNum
	},
	AvgPrewriteRegionsStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return avgFloat(ssStats.sumPrewriteRegionNum, ssStats.commitCount)
	},
	MaxPrewriteRegionsStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return int(ssStats.maxPrewriteRegionNum)
	},
	TxnRetryStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.sumTxnRetry
	},
	AvgTxnRetryStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return avgFloat(ssStats.sumTxnRetry, ssStats.commitCount)
	},
	MaxTxnRetryStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.maxTxnRetry
	},
	ExecRetryStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return int(ssStats.execRetryCount)
	},
	SumExecRetryStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return int(ssStats.execRetryCount)
	},
	ExecRetryTimeStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return int64(ssStats.execRetryTime)
	},
	SumExecRetryTimeStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return int64(ssStats.execRetryTime)
	},
	BackoffTimesStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.sumBackoffTimes
	},
	SumBackoffTimesStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.sumBackoffTimes
	},
	BackoffTypesStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return formatBackoffTypes(ssStats.backoffTypes)
	},
	MemStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.sumMem
	},
	AvgMemStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return avgInt(ssStats.sumMem, ssStats.execCount)
	},
	MaxMemStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.maxMem
	},
	DiskStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.sumDisk
	},
	AvgDiskStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return avgInt(ssStats.sumDisk, ssStats.execCount)
	},
	MaxDiskStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.maxDisk
	},
	KvTimeStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return int64(ssStats.sumKVTotal)
	},
	AvgKvTimeStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return avgInt(int64(ssStats.sumKVTotal), ssStats.commitCount)
	},
	PdTimeStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return int64(ssStats.sumPDTotal)
	},
	AvgPdTimeStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return avgInt(int64(ssStats.sumPDTotal), ssStats.commitCount)
	},
	BackoffTotalTimeStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return int64(ssStats.sumBackoffTotal)
	},
	AvgBackoffTotalTimeStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return avgInt(int64(ssStats.sumBackoffTotal), ssStats.commitCount)
	},
	WriteSQLRespTimeStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return int64(ssStats.sumWriteSQLRespTotal)
	},
	AvgWriteSQLRespTimeStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return avgInt(int64(ssStats.sumWriteSQLRespTotal), ssStats.commitCount)
	},
	AvgTidbCPUTimeStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return avgInt(int64(ssStats.sumTidbCPU), ssStats.execCount)
	},
	AvgTikvCPUTimeStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return avgInt(int64(ssStats.sumTikvCPU), ssStats.execCount)
	},
	ResultRowsStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.sumResultRows
	},
	MaxResultRowsStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.maxResultRows
	},
	MinResultRowsStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.minResultRows
	},
	AffectedRowsStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.sumAffectedRows
	},
	AvgResultRowsStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return avgInt(ssStats.sumResultRows, ssStats.execCount)
	},
	PreparedStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.prepared
	},
	AvgAffectedRowsStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return avgFloat(int64(ssStats.sumAffectedRows), ssStats.execCount)
	},
	FirstSeenStr: func(reader *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		firstSeen := ssStats.firstSeen
		if firstSeen.Location() != reader.tz {
			firstSeen = firstSeen.In(reader.tz)
		}
		return types.NewTime(types.FromGoTime(firstSeen), mysql.TypeTimestamp, 0)
	},
	LastSeenStr: func(reader *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		lastSeen := ssStats.lastSeen
		if lastSeen.Location() != reader.tz {
			lastSeen = lastSeen.In(reader.tz)
		}
		return types.NewTime(types.FromGoTime(lastSeen), mysql.TypeTimestamp, 0)
	},
	PlanInCacheStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.planInCache
	},
	PlanCacheHitsStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.planCacheHits
	},
	PlanInBindingStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.planInBinding
	},
	QuerySampleTextStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.sampleSQL
	},
	PrevSampleTextStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.prevSQL
	},
	PlanDigestStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, ssbd *stmtSummaryByDigest, _ *stmtSummaryStats) any {
		return ssbd.planDigest
	},
	PlanStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		plan, err := plancodec.DecodePlan(ssStats.samplePlan)
		if err != nil {
			logutil.BgLogger().Error("decode plan in statement summary failed", zap.String("plan", ssStats.samplePlan), zap.String("query", ssStats.sampleSQL), zap.Error(err))
			plan = ""
		}
		return plan
	},
	BinaryPlan: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.sampleBinaryPlan
	},
	Charset: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.charset
	},
	Collation: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.collation
	},
	PlanHint: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.planHint
	},
	RequestUnitReadStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.SumRRU
	},
	AvgRequestUnitReadStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return avgSumFloat(ssStats.SumRRU, ssStats.execCount)
	},
	MaxRequestUnitReadStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.MaxRRU
	},
	RequestUnitWriteStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.SumWRU
	},
	AvgRequestUnitWriteStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return avgSumFloat(ssStats.SumWRU, ssStats.execCount)
	},
	MaxRequestUnitWriteStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.MaxWRU
	},
	QueuedRcTimeStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return int64(ssStats.SumRUWaitDuration)
	},
	AvgQueuedRcTimeStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return avgInt(int64(ssStats.SumRUWaitDuration), ssStats.execCount)
	},
	MaxQueuedRcTimeStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return int64(ssStats.MaxRUWaitDuration)
	},
	ResourceGroupName: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.resourceGroupName
	},
	PlanCacheUnqualifiedStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.planCacheUnqualifiedCount
	},
	PlanCacheUnqualifiedLastReasonStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.lastPlanCacheUnqualified
	},
	SumUnpackedBytesSentKVTotalStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.UnpackedBytesSentKVTotal
	},

	SumUnpackedBytesReceivedKVTotalStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.UnpackedBytesReceivedKVTotal
	},
	SumUnpackedBytesSentKVCrossZoneStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.UnpackedBytesSentKVCrossZone
	},
	SumUnpackedBytesReceivedKVCrossZoneStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.UnpackedBytesReceivedKVCrossZone
	},
	SumUnpackedBytesSentMPPTotalStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.UnpackedBytesSentMPPTotal
	},
	SumUnpackedBytesReceivedMPPTotalStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.UnpackedBytesReceivedMPPTotal
	},
	SumUnpackedBytesSentMPPCrossZoneStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.UnpackedBytesSentMPPCrossZone
	},
	SumUnpackedBytesReceiveMPPCrossZoneStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.UnpackedBytesReceivedMPPCrossZone
	},
}
