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

	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/plancodec"
	"github.com/pingcap/tidb/util/set"
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
		ssbd := value.(*stmtSummaryByDigest)
		if ssr.checker != nil && !ssr.checker.isDigestValid(ssbd.digest) {
			continue
		}
		records := ssr.getStmtByDigestHistoryRow(ssbd, historySize)
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

func (ssr *stmtSummaryReader) getStmtByDigestRow(ssbd *stmtSummaryByDigest, beginTimeForCurInterval int64) []types.Datum {
	var ssElement *stmtSummaryByDigestElement

	ssbd.Lock()
	if ssbd.initialized && ssbd.history.Len() > 0 {
		ssElement = ssbd.history.Back().Value.(*stmtSummaryByDigestElement)
	}
	ssbd.Unlock()

	// `ssElement` is lazy expired, so expired elements could also be read.
	// `beginTime` won't change since `ssElement` is created, so locking is not needed here.
	isAuthed := true
	if ssr.user != nil && !ssr.hasProcessPriv && ssElement != nil {
		_, isAuthed = ssElement.authUsers[ssr.user.Username]
	}
	if ssElement == nil || ssElement.beginTime < beginTimeForCurInterval || !isAuthed {
		return nil
	}
	return ssr.getStmtByDigestElementRow(ssElement, ssbd)
}

func (ssr *stmtSummaryReader) getStmtByDigestElementRow(ssElement *stmtSummaryByDigestElement, ssbd *stmtSummaryByDigest) []types.Datum {
	ssElement.Lock()
	defer ssElement.Unlock()
	datums := make([]types.Datum, len(ssr.columnValueFactories))
	for i, factory := range ssr.columnValueFactories {
		datums[i] = types.NewDatum(factory(ssr, ssElement, ssbd))
	}
	return datums
}

func (ssr *stmtSummaryReader) getStmtByDigestHistoryRow(ssbd *stmtSummaryByDigest, historySize int) [][]types.Datum {
	// Collect all history summaries to an array.
	ssElements := ssbd.collectHistorySummaries(historySize)

	rows := make([][]types.Datum, 0, len(ssElements))
	for _, ssElement := range ssElements {
		isAuthed := true
		if ssr.user != nil && !ssr.hasProcessPriv {
			_, isAuthed = ssElement.authUsers[ssr.user.Username]
		}
		if isAuthed {
			rows = append(rows, ssr.getStmtByDigestElementRow(ssElement, ssbd))
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
		rows = append(rows, ssr.getStmtByDigestElementRow(seElement.otherSummary, ssbd))
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
	ClusterTableInstanceColumnNameStr = "INSTANCE"
	SummaryBeginTimeStr               = "SUMMARY_BEGIN_TIME"
	SummaryEndTimeStr                 = "SUMMARY_END_TIME"
	StmtTypeStr                       = "STMT_TYPE"
	SchemaNameStr                     = "SCHEMA_NAME"
	DigestStr                         = "DIGEST"
	DigestTextStr                     = "DIGEST_TEXT"
	TableNamesStr                     = "TABLE_NAMES"
	IndexNamesStr                     = "INDEX_NAMES"
	SampleUserStr                     = "SAMPLE_USER"
	ExecCountStr                      = "EXEC_COUNT"
	SumErrorsStr                      = "SUM_ERRORS"
	SumWarningsStr                    = "SUM_WARNINGS"
	SumLatencyStr                     = "SUM_LATENCY"
	MaxLatencyStr                     = "MAX_LATENCY"
	MinLatencyStr                     = "MIN_LATENCY"
	AvgLatencyStr                     = "AVG_LATENCY"
	AvgParseLatencyStr                = "AVG_PARSE_LATENCY"
	MaxParseLatencyStr                = "MAX_PARSE_LATENCY"
	AvgCompileLatencyStr              = "AVG_COMPILE_LATENCY"
	MaxCompileLatencyStr              = "MAX_COMPILE_LATENCY"
	SumCopTaskNumStr                  = "SUM_COP_TASK_NUM"
	MaxCopProcessTimeStr              = "MAX_COP_PROCESS_TIME"
	MaxCopProcessAddressStr           = "MAX_COP_PROCESS_ADDRESS"
	MaxCopWaitTimeStr                 = "MAX_COP_WAIT_TIME"    // #nosec G101
	MaxCopWaitAddressStr              = "MAX_COP_WAIT_ADDRESS" // #nosec G101
	AvgProcessTimeStr                 = "AVG_PROCESS_TIME"
	MaxProcessTimeStr                 = "MAX_PROCESS_TIME"
	AvgWaitTimeStr                    = "AVG_WAIT_TIME"
	MaxWaitTimeStr                    = "MAX_WAIT_TIME"
	AvgBackoffTimeStr                 = "AVG_BACKOFF_TIME"
	MaxBackoffTimeStr                 = "MAX_BACKOFF_TIME"
	AvgTotalKeysStr                   = "AVG_TOTAL_KEYS"
	MaxTotalKeysStr                   = "MAX_TOTAL_KEYS"
	AvgProcessedKeysStr               = "AVG_PROCESSED_KEYS"
	MaxProcessedKeysStr               = "MAX_PROCESSED_KEYS"
	AvgRocksdbDeleteSkippedCountStr   = "AVG_ROCKSDB_DELETE_SKIPPED_COUNT"
	MaxRocksdbDeleteSkippedCountStr   = "MAX_ROCKSDB_DELETE_SKIPPED_COUNT"
	AvgRocksdbKeySkippedCountStr      = "AVG_ROCKSDB_KEY_SKIPPED_COUNT"
	MaxRocksdbKeySkippedCountStr      = "MAX_ROCKSDB_KEY_SKIPPED_COUNT"
	AvgRocksdbBlockCacheHitCountStr   = "AVG_ROCKSDB_BLOCK_CACHE_HIT_COUNT"
	MaxRocksdbBlockCacheHitCountStr   = "MAX_ROCKSDB_BLOCK_CACHE_HIT_COUNT"
	AvgRocksdbBlockReadCountStr       = "AVG_ROCKSDB_BLOCK_READ_COUNT"
	MaxRocksdbBlockReadCountStr       = "MAX_ROCKSDB_BLOCK_READ_COUNT"
	AvgRocksdbBlockReadByteStr        = "AVG_ROCKSDB_BLOCK_READ_BYTE"
	MaxRocksdbBlockReadByteStr        = "MAX_ROCKSDB_BLOCK_READ_BYTE"
	AvgPrewriteTimeStr                = "AVG_PREWRITE_TIME"
	MaxPrewriteTimeStr                = "MAX_PREWRITE_TIME"
	AvgCommitTimeStr                  = "AVG_COMMIT_TIME"
	MaxCommitTimeStr                  = "MAX_COMMIT_TIME"
	AvgGetCommitTsTimeStr             = "AVG_GET_COMMIT_TS_TIME"
	MaxGetCommitTsTimeStr             = "MAX_GET_COMMIT_TS_TIME"
	AvgCommitBackoffTimeStr           = "AVG_COMMIT_BACKOFF_TIME"
	MaxCommitBackoffTimeStr           = "MAX_COMMIT_BACKOFF_TIME"
	AvgResolveLockTimeStr             = "AVG_RESOLVE_LOCK_TIME"
	MaxResolveLockTimeStr             = "MAX_RESOLVE_LOCK_TIME"
	AvgLocalLatchWaitTimeStr          = "AVG_LOCAL_LATCH_WAIT_TIME"
	MaxLocalLatchWaitTimeStr          = "MAX_LOCAL_LATCH_WAIT_TIME"
	AvgWriteKeysStr                   = "AVG_WRITE_KEYS"
	MaxWriteKeysStr                   = "MAX_WRITE_KEYS"
	AvgWriteSizeStr                   = "AVG_WRITE_SIZE"
	MaxWriteSizeStr                   = "MAX_WRITE_SIZE"
	AvgPrewriteRegionsStr             = "AVG_PREWRITE_REGIONS"
	MaxPrewriteRegionsStr             = "MAX_PREWRITE_REGIONS"
	AvgTxnRetryStr                    = "AVG_TXN_RETRY"
	MaxTxnRetryStr                    = "MAX_TXN_RETRY"
	SumExecRetryStr                   = "SUM_EXEC_RETRY"
	SumExecRetryTimeStr               = "SUM_EXEC_RETRY_TIME"
	SumBackoffTimesStr                = "SUM_BACKOFF_TIMES"
	BackoffTypesStr                   = "BACKOFF_TYPES"
	AvgMemStr                         = "AVG_MEM"
	MaxMemStr                         = "MAX_MEM"
	AvgDiskStr                        = "AVG_DISK"
	MaxDiskStr                        = "MAX_DISK"
	AvgKvTimeStr                      = "AVG_KV_TIME"
	AvgPdTimeStr                      = "AVG_PD_TIME"
	AvgBackoffTotalTimeStr            = "AVG_BACKOFF_TOTAL_TIME"
	AvgWriteSQLRespTimeStr            = "AVG_WRITE_SQL_RESP_TIME"
	MaxResultRowsStr                  = "MAX_RESULT_ROWS"
	MinResultRowsStr                  = "MIN_RESULT_ROWS"
	AvgResultRowsStr                  = "AVG_RESULT_ROWS"
	PreparedStr                       = "PREPARED"
	AvgAffectedRowsStr                = "AVG_AFFECTED_ROWS"
	FirstSeenStr                      = "FIRST_SEEN"
	LastSeenStr                       = "LAST_SEEN"
	PlanInCacheStr                    = "PLAN_IN_CACHE"
	PlanCacheHitsStr                  = "PLAN_CACHE_HITS"
	PlanInBindingStr                  = "PLAN_IN_BINDING"
	QuerySampleTextStr                = "QUERY_SAMPLE_TEXT"
	PrevSampleTextStr                 = "PREV_SAMPLE_TEXT"
	PlanDigestStr                     = "PLAN_DIGEST"
	PlanStr                           = "PLAN"
)

type columnValueFactory func(reader *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, ssbd *stmtSummaryByDigest) interface{}

var columnValueFactoryMap = map[string]columnValueFactory{
	ClusterTableInstanceColumnNameStr: func(reader *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, ssbd *stmtSummaryByDigest) interface{} {
		return reader.instanceAddr
	},
	SummaryBeginTimeStr: func(reader *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		beginTime := time.Unix(ssElement.beginTime, 0)
		if beginTime.Location() != reader.tz {
			beginTime = beginTime.In(reader.tz)
		}
		return types.NewTime(types.FromGoTime(beginTime), mysql.TypeTimestamp, 0)
	},
	SummaryEndTimeStr: func(reader *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		endTime := time.Unix(ssElement.beginTime, 0)
		if endTime.Location() != reader.tz {
			endTime = endTime.In(reader.tz)
		}
		return types.NewTime(types.FromGoTime(endTime), mysql.TypeTimestamp, 0)
	},
	StmtTypeStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, ssbd *stmtSummaryByDigest) interface{} {
		return ssbd.stmtType
	},
	SchemaNameStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, ssbd *stmtSummaryByDigest) interface{} {
		return convertEmptyToNil(ssbd.schemaName)
	},
	DigestStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, ssbd *stmtSummaryByDigest) interface{} {
		return convertEmptyToNil(ssbd.digest)
	},
	DigestTextStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, ssbd *stmtSummaryByDigest) interface{} {
		return ssbd.normalizedSQL
	},
	TableNamesStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, ssbd *stmtSummaryByDigest) interface{} {
		return convertEmptyToNil(ssbd.tableNames)
	},
	IndexNamesStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return convertEmptyToNil(strings.Join(ssElement.indexNames, ","))
	},
	SampleUserStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		sampleUser := ""
		for key := range ssElement.authUsers {
			sampleUser = key
			break
		}
		return convertEmptyToNil(sampleUser)
	},
	ExecCountStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return ssElement.execCount
	},
	SumErrorsStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return ssElement.sumErrors
	},
	SumWarningsStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return ssElement.sumWarnings
	},
	SumLatencyStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return int64(ssElement.sumLatency)
	},
	MaxLatencyStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return int64(ssElement.maxLatency)
	},
	MinLatencyStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return int64(ssElement.minLatency)
	},
	AvgLatencyStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return avgInt(int64(ssElement.sumLatency), ssElement.execCount)
	},
	AvgParseLatencyStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return avgInt(int64(ssElement.sumParseLatency), ssElement.execCount)
	},
	MaxParseLatencyStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return int64(ssElement.maxParseLatency)
	},
	AvgCompileLatencyStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return avgInt(int64(ssElement.sumCompileLatency), ssElement.execCount)
	},
	MaxCompileLatencyStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return int64(ssElement.maxCompileLatency)
	},
	SumCopTaskNumStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return ssElement.sumNumCopTasks
	},
	MaxCopProcessTimeStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return int64(ssElement.maxCopProcessTime)
	},
	MaxCopProcessAddressStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return convertEmptyToNil(ssElement.maxCopProcessAddress)
	},
	MaxCopWaitTimeStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return int64(ssElement.maxCopWaitTime)
	},
	MaxCopWaitAddressStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return convertEmptyToNil(ssElement.maxCopWaitAddress)
	},
	AvgProcessTimeStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return avgInt(int64(ssElement.sumProcessTime), ssElement.execCount)
	},
	MaxProcessTimeStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return int64(ssElement.maxProcessTime)
	},
	AvgWaitTimeStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return avgInt(int64(ssElement.sumWaitTime), ssElement.execCount)
	},
	MaxWaitTimeStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return int64(ssElement.maxWaitTime)
	},
	AvgBackoffTimeStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return avgInt(int64(ssElement.sumBackoffTime), ssElement.execCount)
	},
	MaxBackoffTimeStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return int64(ssElement.maxBackoffTime)
	},
	AvgTotalKeysStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return avgInt(ssElement.sumTotalKeys, ssElement.execCount)
	},
	MaxTotalKeysStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return ssElement.maxTotalKeys
	},
	AvgProcessedKeysStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return avgInt(ssElement.sumProcessedKeys, ssElement.execCount)
	},
	MaxProcessedKeysStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return ssElement.maxProcessedKeys
	},
	AvgRocksdbDeleteSkippedCountStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return avgInt(int64(ssElement.sumRocksdbDeleteSkippedCount), ssElement.execCount)
	},
	MaxRocksdbDeleteSkippedCountStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return ssElement.maxRocksdbDeleteSkippedCount
	},
	AvgRocksdbKeySkippedCountStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return avgInt(int64(ssElement.sumRocksdbKeySkippedCount), ssElement.execCount)
	},
	MaxRocksdbKeySkippedCountStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return ssElement.maxRocksdbKeySkippedCount
	},
	AvgRocksdbBlockCacheHitCountStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return avgInt(int64(ssElement.sumRocksdbBlockCacheHitCount), ssElement.execCount)
	},
	MaxRocksdbBlockCacheHitCountStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return ssElement.maxRocksdbBlockCacheHitCount
	},
	AvgRocksdbBlockReadCountStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return avgInt(int64(ssElement.sumRocksdbBlockReadCount), ssElement.execCount)
	},
	MaxRocksdbBlockReadCountStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return ssElement.maxRocksdbBlockReadCount
	},
	AvgRocksdbBlockReadByteStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return avgInt(int64(ssElement.sumRocksdbBlockReadByte), ssElement.execCount)
	},
	MaxRocksdbBlockReadByteStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return ssElement.maxRocksdbBlockReadByte
	},
	AvgPrewriteTimeStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return avgInt(int64(ssElement.sumPrewriteTime), ssElement.commitCount)
	},
	MaxPrewriteTimeStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return int64(ssElement.maxPrewriteTime)
	},
	AvgCommitTimeStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return avgInt(int64(ssElement.sumCommitTime), ssElement.commitCount)
	},
	MaxCommitTimeStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return int64(ssElement.maxCommitTime)
	},
	AvgGetCommitTsTimeStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return avgInt(int64(ssElement.sumGetCommitTsTime), ssElement.commitCount)
	},
	MaxGetCommitTsTimeStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return int64(ssElement.maxGetCommitTsTime)
	},
	AvgCommitBackoffTimeStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return avgInt(ssElement.sumCommitBackoffTime, ssElement.commitCount)
	},
	MaxCommitBackoffTimeStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return ssElement.maxCommitBackoffTime
	},
	AvgResolveLockTimeStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return avgInt(ssElement.sumResolveLockTime, ssElement.commitCount)
	},
	MaxResolveLockTimeStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return ssElement.maxResolveLockTime
	},
	AvgLocalLatchWaitTimeStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return avgInt(int64(ssElement.sumLocalLatchTime), ssElement.commitCount)
	},
	MaxLocalLatchWaitTimeStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return int64(ssElement.maxLocalLatchTime)
	},
	AvgWriteKeysStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return avgFloat(ssElement.sumWriteKeys, ssElement.commitCount)
	},
	MaxWriteKeysStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return ssElement.maxWriteKeys
	},
	AvgWriteSizeStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return avgFloat(ssElement.sumWriteSize, ssElement.commitCount)
	},
	MaxWriteSizeStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return ssElement.maxWriteSize
	},
	AvgPrewriteRegionsStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return avgFloat(ssElement.sumPrewriteRegionNum, ssElement.commitCount)
	},
	MaxPrewriteRegionsStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return int(ssElement.maxPrewriteRegionNum)
	},
	AvgTxnRetryStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return avgFloat(ssElement.sumTxnRetry, ssElement.commitCount)
	},
	MaxTxnRetryStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return ssElement.maxTxnRetry
	},
	SumExecRetryStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return int(ssElement.execRetryCount)
	},
	SumExecRetryTimeStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return int64(ssElement.execRetryTime)
	},
	SumBackoffTimesStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return ssElement.sumBackoffTimes
	},
	BackoffTypesStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return formatBackoffTypes(ssElement.backoffTypes)
	},
	AvgMemStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return avgInt(ssElement.sumMem, ssElement.execCount)
	},
	MaxMemStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return ssElement.maxMem
	},
	AvgDiskStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return avgInt(ssElement.sumDisk, ssElement.execCount)
	},
	MaxDiskStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return ssElement.maxDisk
	},
	AvgKvTimeStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return avgInt(int64(ssElement.sumKVTotal), ssElement.commitCount)
	},
	AvgPdTimeStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return avgInt(int64(ssElement.sumPDTotal), ssElement.commitCount)
	},
	AvgBackoffTotalTimeStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return avgInt(int64(ssElement.sumBackoffTotal), ssElement.commitCount)
	},
	AvgWriteSQLRespTimeStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return avgInt(int64(ssElement.sumWriteSQLRespTotal), ssElement.commitCount)
	},
	MaxResultRowsStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return ssElement.maxResultRows
	},
	MinResultRowsStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return ssElement.minResultRows
	},
	AvgResultRowsStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return avgInt(ssElement.sumResultRows, ssElement.execCount)
	},
	PreparedStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return ssElement.prepared
	},
	AvgAffectedRowsStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return avgFloat(int64(ssElement.sumAffectedRows), ssElement.execCount)
	},
	FirstSeenStr: func(reader *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		firstSeen := ssElement.firstSeen
		if firstSeen.Location() != reader.tz {
			firstSeen = firstSeen.In(reader.tz)
		}
		return types.NewTime(types.FromGoTime(firstSeen), mysql.TypeTimestamp, 0)
	},
	LastSeenStr: func(reader *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		lastSeen := ssElement.lastSeen
		if lastSeen.Location() != reader.tz {
			lastSeen = lastSeen.In(reader.tz)
		}
		return types.NewTime(types.FromGoTime(lastSeen), mysql.TypeTimestamp, 0)
	},
	PlanInCacheStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return ssElement.planInCache
	},
	PlanCacheHitsStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return ssElement.planCacheHits
	},
	PlanInBindingStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return ssElement.planInBinding
	},
	QuerySampleTextStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return ssElement.sampleSQL
	},
	PrevSampleTextStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		return ssElement.prevSQL
	},
	PlanDigestStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, ssbd *stmtSummaryByDigest) interface{} {
		return ssbd.planDigest
	},
	PlanStr: func(_ *stmtSummaryReader, ssElement *stmtSummaryByDigestElement, _ *stmtSummaryByDigest) interface{} {
		plan, err := plancodec.DecodePlan(ssElement.samplePlan)
		if err != nil {
			logutil.BgLogger().Error("decode plan in statement summary failed", zap.String("plan", ssElement.samplePlan), zap.String("query", ssElement.sampleSQL), zap.Error(err))
			plan = ""
		}
		return plan
	},
}
