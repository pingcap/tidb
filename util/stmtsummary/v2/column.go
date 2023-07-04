// Copyright 2023 PingCAP, Inc.
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
	"bytes"
	"fmt"
	"strings"
	"time"

	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/plancodec"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
)

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
	BinaryPlan                        = "BINARY_PLAN"
	Charset                           = "CHARSET"
	Collation                         = "COLLATION"
	PlanHint                          = "PLAN_HINT"
)

type columnInfo interface {
	getInstanceAddr() string
	getTimeLocation() *time.Location
}

type columnFactory func(info columnInfo, record *StmtRecord) interface{}

var columnFactoryMap = map[string]columnFactory{
	ClusterTableInstanceColumnNameStr: func(info columnInfo, record *StmtRecord) interface{} {
		return info.getInstanceAddr()
	},
	SummaryBeginTimeStr: func(info columnInfo, record *StmtRecord) interface{} {
		beginTime := time.Unix(record.Begin, 0)
		if beginTime.Location() != info.getTimeLocation() {
			beginTime = beginTime.In(info.getTimeLocation())
		}
		return types.NewTime(types.FromGoTime(beginTime), mysql.TypeTimestamp, 0)
	},
	SummaryEndTimeStr: func(info columnInfo, record *StmtRecord) interface{} {
		endTime := time.Unix(record.End, 0)
		if endTime.Location() != info.getTimeLocation() {
			endTime = endTime.In(info.getTimeLocation())
		}
		return types.NewTime(types.FromGoTime(endTime), mysql.TypeTimestamp, 0)
	},
	StmtTypeStr: func(info columnInfo, record *StmtRecord) interface{} {
		return record.StmtType
	},
	SchemaNameStr: func(info columnInfo, record *StmtRecord) interface{} {
		return convertEmptyToNil(record.SchemaName)
	},
	DigestStr: func(info columnInfo, record *StmtRecord) interface{} {
		return convertEmptyToNil(record.Digest)
	},
	DigestTextStr: func(info columnInfo, record *StmtRecord) interface{} {
		return record.NormalizedSQL
	},
	TableNamesStr: func(info columnInfo, record *StmtRecord) interface{} {
		return convertEmptyToNil(record.TableNames)
	},
	IndexNamesStr: func(info columnInfo, record *StmtRecord) interface{} {
		return convertEmptyToNil(strings.Join(record.IndexNames, ","))
	},
	SampleUserStr: func(info columnInfo, record *StmtRecord) interface{} {
		sampleUser := ""
		for key := range record.AuthUsers {
			sampleUser = key
			break
		}
		return convertEmptyToNil(sampleUser)
	},
	ExecCountStr: func(info columnInfo, record *StmtRecord) interface{} {
		return record.ExecCount
	},
	SumErrorsStr: func(info columnInfo, record *StmtRecord) interface{} {
		return record.SumErrors
	},
	SumWarningsStr: func(info columnInfo, record *StmtRecord) interface{} {
		return record.SumWarnings
	},
	SumLatencyStr: func(info columnInfo, record *StmtRecord) interface{} {
		return int64(record.SumLatency)
	},
	MaxLatencyStr: func(info columnInfo, record *StmtRecord) interface{} {
		return int64(record.MaxLatency)
	},
	MinLatencyStr: func(info columnInfo, record *StmtRecord) interface{} {
		return int64(record.MinLatency)
	},
	AvgLatencyStr: func(info columnInfo, record *StmtRecord) interface{} {
		return avgInt(int64(record.SumLatency), record.ExecCount)
	},
	AvgParseLatencyStr: func(info columnInfo, record *StmtRecord) interface{} {
		return avgInt(int64(record.SumParseLatency), record.ExecCount)
	},
	MaxParseLatencyStr: func(info columnInfo, record *StmtRecord) interface{} {
		return int64(record.MaxParseLatency)
	},
	AvgCompileLatencyStr: func(info columnInfo, record *StmtRecord) interface{} {
		return avgInt(int64(record.SumCompileLatency), record.ExecCount)
	},
	MaxCompileLatencyStr: func(info columnInfo, record *StmtRecord) interface{} {
		return int64(record.MaxCompileLatency)
	},
	SumCopTaskNumStr: func(info columnInfo, record *StmtRecord) interface{} {
		return record.SumNumCopTasks
	},
	MaxCopProcessTimeStr: func(info columnInfo, record *StmtRecord) interface{} {
		return int64(record.MaxCopProcessTime)
	},
	MaxCopProcessAddressStr: func(info columnInfo, record *StmtRecord) interface{} {
		return convertEmptyToNil(record.MaxCopProcessAddress)
	},
	MaxCopWaitTimeStr: func(info columnInfo, record *StmtRecord) interface{} {
		return int64(record.MaxCopWaitTime)
	},
	MaxCopWaitAddressStr: func(info columnInfo, record *StmtRecord) interface{} {
		return convertEmptyToNil(record.MaxCopWaitAddress)
	},
	AvgProcessTimeStr: func(info columnInfo, record *StmtRecord) interface{} {
		return avgInt(int64(record.SumProcessTime), record.ExecCount)
	},
	MaxProcessTimeStr: func(info columnInfo, record *StmtRecord) interface{} {
		return int64(record.MaxProcessTime)
	},
	AvgWaitTimeStr: func(info columnInfo, record *StmtRecord) interface{} {
		return avgInt(int64(record.SumWaitTime), record.ExecCount)
	},
	MaxWaitTimeStr: func(info columnInfo, record *StmtRecord) interface{} {
		return int64(record.MaxWaitTime)
	},
	AvgBackoffTimeStr: func(info columnInfo, record *StmtRecord) interface{} {
		return avgInt(int64(record.SumBackoffTime), record.ExecCount)
	},
	MaxBackoffTimeStr: func(info columnInfo, record *StmtRecord) interface{} {
		return int64(record.MaxBackoffTime)
	},
	AvgTotalKeysStr: func(info columnInfo, record *StmtRecord) interface{} {
		return avgInt(record.SumTotalKeys, record.ExecCount)
	},
	MaxTotalKeysStr: func(info columnInfo, record *StmtRecord) interface{} {
		return record.MaxTotalKeys
	},
	AvgProcessedKeysStr: func(info columnInfo, record *StmtRecord) interface{} {
		return avgInt(record.SumProcessedKeys, record.ExecCount)
	},
	MaxProcessedKeysStr: func(info columnInfo, record *StmtRecord) interface{} {
		return record.MaxProcessedKeys
	},
	AvgRocksdbDeleteSkippedCountStr: func(info columnInfo, record *StmtRecord) interface{} {
		return avgInt(int64(record.SumRocksdbDeleteSkippedCount), record.ExecCount)
	},
	MaxRocksdbDeleteSkippedCountStr: func(info columnInfo, record *StmtRecord) interface{} {
		return record.MaxRocksdbDeleteSkippedCount
	},
	AvgRocksdbKeySkippedCountStr: func(info columnInfo, record *StmtRecord) interface{} {
		return avgInt(int64(record.SumRocksdbKeySkippedCount), record.ExecCount)
	},
	MaxRocksdbKeySkippedCountStr: func(info columnInfo, record *StmtRecord) interface{} {
		return record.MaxRocksdbKeySkippedCount
	},
	AvgRocksdbBlockCacheHitCountStr: func(info columnInfo, record *StmtRecord) interface{} {
		return avgInt(int64(record.SumRocksdbBlockCacheHitCount), record.ExecCount)
	},
	MaxRocksdbBlockCacheHitCountStr: func(info columnInfo, record *StmtRecord) interface{} {
		return record.MaxRocksdbBlockCacheHitCount
	},
	AvgRocksdbBlockReadCountStr: func(info columnInfo, record *StmtRecord) interface{} {
		return avgInt(int64(record.SumRocksdbBlockReadCount), record.ExecCount)
	},
	MaxRocksdbBlockReadCountStr: func(info columnInfo, record *StmtRecord) interface{} {
		return record.MaxRocksdbBlockReadCount
	},
	AvgRocksdbBlockReadByteStr: func(info columnInfo, record *StmtRecord) interface{} {
		return avgInt(int64(record.SumRocksdbBlockReadByte), record.ExecCount)
	},
	MaxRocksdbBlockReadByteStr: func(info columnInfo, record *StmtRecord) interface{} {
		return record.MaxRocksdbBlockReadByte
	},
	AvgPrewriteTimeStr: func(info columnInfo, record *StmtRecord) interface{} {
		return avgInt(int64(record.SumPrewriteTime), record.CommitCount)
	},
	MaxPrewriteTimeStr: func(info columnInfo, record *StmtRecord) interface{} {
		return int64(record.MaxPrewriteTime)
	},
	AvgCommitTimeStr: func(info columnInfo, record *StmtRecord) interface{} {
		return avgInt(int64(record.SumCommitTime), record.CommitCount)
	},
	MaxCommitTimeStr: func(info columnInfo, record *StmtRecord) interface{} {
		return int64(record.MaxCommitTime)
	},
	AvgGetCommitTsTimeStr: func(info columnInfo, record *StmtRecord) interface{} {
		return avgInt(int64(record.SumGetCommitTsTime), record.CommitCount)
	},
	MaxGetCommitTsTimeStr: func(info columnInfo, record *StmtRecord) interface{} {
		return int64(record.MaxGetCommitTsTime)
	},
	AvgCommitBackoffTimeStr: func(info columnInfo, record *StmtRecord) interface{} {
		return avgInt(record.SumCommitBackoffTime, record.CommitCount)
	},
	MaxCommitBackoffTimeStr: func(info columnInfo, record *StmtRecord) interface{} {
		return record.MaxCommitBackoffTime
	},
	AvgResolveLockTimeStr: func(info columnInfo, record *StmtRecord) interface{} {
		return avgInt(record.SumResolveLockTime, record.CommitCount)
	},
	MaxResolveLockTimeStr: func(info columnInfo, record *StmtRecord) interface{} {
		return record.MaxResolveLockTime
	},
	AvgLocalLatchWaitTimeStr: func(info columnInfo, record *StmtRecord) interface{} {
		return avgInt(int64(record.SumLocalLatchTime), record.CommitCount)
	},
	MaxLocalLatchWaitTimeStr: func(info columnInfo, record *StmtRecord) interface{} {
		return int64(record.MaxLocalLatchTime)
	},
	AvgWriteKeysStr: func(info columnInfo, record *StmtRecord) interface{} {
		return avgFloat(record.SumWriteKeys, record.CommitCount)
	},
	MaxWriteKeysStr: func(info columnInfo, record *StmtRecord) interface{} {
		return record.MaxWriteKeys
	},
	AvgWriteSizeStr: func(info columnInfo, record *StmtRecord) interface{} {
		return avgFloat(record.SumWriteSize, record.CommitCount)
	},
	MaxWriteSizeStr: func(info columnInfo, record *StmtRecord) interface{} {
		return record.MaxWriteSize
	},
	AvgPrewriteRegionsStr: func(info columnInfo, record *StmtRecord) interface{} {
		return avgFloat(record.SumPrewriteRegionNum, record.CommitCount)
	},
	MaxPrewriteRegionsStr: func(info columnInfo, record *StmtRecord) interface{} {
		return int(record.MaxPrewriteRegionNum)
	},
	AvgTxnRetryStr: func(info columnInfo, record *StmtRecord) interface{} {
		return avgFloat(record.SumTxnRetry, record.CommitCount)
	},
	MaxTxnRetryStr: func(info columnInfo, record *StmtRecord) interface{} {
		return record.MaxTxnRetry
	},
	SumExecRetryStr: func(info columnInfo, record *StmtRecord) interface{} {
		return int(record.ExecRetryCount)
	},
	SumExecRetryTimeStr: func(info columnInfo, record *StmtRecord) interface{} {
		return int64(record.ExecRetryTime)
	},
	SumBackoffTimesStr: func(info columnInfo, record *StmtRecord) interface{} {
		return record.SumBackoffTimes
	},
	BackoffTypesStr: func(info columnInfo, record *StmtRecord) interface{} {
		return formatBackoffTypes(record.BackoffTypes)
	},
	AvgMemStr: func(info columnInfo, record *StmtRecord) interface{} {
		return avgInt(record.SumMem, record.ExecCount)
	},
	MaxMemStr: func(info columnInfo, record *StmtRecord) interface{} {
		return record.MaxMem
	},
	AvgDiskStr: func(info columnInfo, record *StmtRecord) interface{} {
		return avgInt(record.SumDisk, record.ExecCount)
	},
	MaxDiskStr: func(info columnInfo, record *StmtRecord) interface{} {
		return record.MaxDisk
	},
	AvgKvTimeStr: func(info columnInfo, record *StmtRecord) interface{} {
		return avgInt(int64(record.SumKVTotal), record.CommitCount)
	},
	AvgPdTimeStr: func(info columnInfo, record *StmtRecord) interface{} {
		return avgInt(int64(record.SumPDTotal), record.CommitCount)
	},
	AvgBackoffTotalTimeStr: func(info columnInfo, record *StmtRecord) interface{} {
		return avgInt(int64(record.SumBackoffTotal), record.CommitCount)
	},
	AvgWriteSQLRespTimeStr: func(info columnInfo, record *StmtRecord) interface{} {
		return avgInt(int64(record.SumWriteSQLRespTotal), record.CommitCount)
	},
	MaxResultRowsStr: func(info columnInfo, record *StmtRecord) interface{} {
		return record.MaxResultRows
	},
	MinResultRowsStr: func(info columnInfo, record *StmtRecord) interface{} {
		return record.MinResultRows
	},
	AvgResultRowsStr: func(info columnInfo, record *StmtRecord) interface{} {
		return avgInt(record.SumResultRows, record.ExecCount)
	},
	PreparedStr: func(info columnInfo, record *StmtRecord) interface{} {
		return record.Prepared
	},
	AvgAffectedRowsStr: func(info columnInfo, record *StmtRecord) interface{} {
		return avgFloat(int64(record.SumAffectedRows), record.ExecCount)
	},
	FirstSeenStr: func(info columnInfo, record *StmtRecord) interface{} {
		firstSeen := record.FirstSeen
		if firstSeen.Location() != info.getTimeLocation() {
			firstSeen = firstSeen.In(info.getTimeLocation())
		}
		return types.NewTime(types.FromGoTime(firstSeen), mysql.TypeTimestamp, 0)
	},
	LastSeenStr: func(info columnInfo, record *StmtRecord) interface{} {
		lastSeen := record.LastSeen
		if lastSeen.Location() != info.getTimeLocation() {
			lastSeen = lastSeen.In(info.getTimeLocation())
		}
		return types.NewTime(types.FromGoTime(lastSeen), mysql.TypeTimestamp, 0)
	},
	PlanInCacheStr: func(info columnInfo, record *StmtRecord) interface{} {
		return record.PlanInCache
	},
	PlanCacheHitsStr: func(info columnInfo, record *StmtRecord) interface{} {
		return record.PlanCacheHits
	},
	PlanInBindingStr: func(info columnInfo, record *StmtRecord) interface{} {
		return record.PlanInBinding
	},
	QuerySampleTextStr: func(info columnInfo, record *StmtRecord) interface{} {
		return record.SampleSQL
	},
	PrevSampleTextStr: func(info columnInfo, record *StmtRecord) interface{} {
		return record.PrevSQL
	},
	PlanDigestStr: func(info columnInfo, record *StmtRecord) interface{} {
		return record.PlanDigest
	},
	PlanStr: func(info columnInfo, record *StmtRecord) interface{} {
		plan, err := plancodec.DecodePlan(record.SamplePlan)
		if err != nil {
			logutil.BgLogger().Error("decode plan in statement summary failed",
				zap.String("plan", record.SamplePlan),
				zap.String("query", record.SampleSQL), zap.Error(err))
			plan = ""
		}
		return plan
	},
	BinaryPlan: func(info columnInfo, record *StmtRecord) interface{} {
		return record.SampleBinaryPlan
	},
	Charset: func(info columnInfo, record *StmtRecord) interface{} {
		return record.Charset
	},
	Collation: func(info columnInfo, record *StmtRecord) interface{} {
		return record.Collation
	},
	PlanHint: func(info columnInfo, record *StmtRecord) interface{} {
		return record.PlanHint
	},
}

func makeColumnFactories(columns []*model.ColumnInfo) []columnFactory {
	columnFactories := make([]columnFactory, len(columns))
	for i, col := range columns {
		factory, ok := columnFactoryMap[col.Name.O]
		if !ok {
			panic(fmt.Sprintf("should never happen, should register new column %v into columnValueFactoryMap", col.Name.O))
		}
		columnFactories[i] = factory
	}
	return columnFactories
}

// Format the backoffType map to a string or nil.
func formatBackoffTypes(backoffMap map[string]int) interface{} {
	type backoffStat struct {
		backoffType string
		count       int
	}

	size := len(backoffMap)
	if size == 0 {
		return nil
	}

	backoffArray := make([]backoffStat, 0, len(backoffMap))
	for backoffType, count := range backoffMap {
		backoffArray = append(backoffArray, backoffStat{backoffType, count})
	}
	slices.SortFunc(backoffArray, func(i, j backoffStat) bool {
		return i.count > j.count
	})

	var buffer bytes.Buffer
	for index, stat := range backoffArray {
		if _, err := fmt.Fprintf(&buffer, "%v:%d", stat.backoffType, stat.count); err != nil {
			return "FORMAT ERROR"
		}
		if index < len(backoffArray)-1 {
			buffer.WriteString(",")
		}
	}
	return buffer.String()
}

func avgInt(sum int64, count int64) int64 {
	if count > 0 {
		return sum / count
	}
	return 0
}

func avgFloat(sum int64, count int64) float64 {
	if count > 0 {
		return float64(sum) / float64(count)
	}
	return 0
}

func convertEmptyToNil(str string) interface{} {
	if str == "" {
		return nil
	}
	return str
}
