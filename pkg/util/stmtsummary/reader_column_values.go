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
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"go.uber.org/zap"
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
	BindingDigestStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, ssbd *stmtSummaryByDigest, _ *stmtSummaryStats) any {
		return convertEmptyToNil(ssbd.bindingDigest)
	},
	BindingDigestTextStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, ssbd *stmtSummaryByDigest, _ *stmtSummaryStats) any {
		return ssbd.bindingSQL
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
	MemArbitrationStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.sumMemArbitration
	},
	AvgMemArbitrationStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return avgSumFloat(ssStats.sumMemArbitration, ssStats.execCount)
	},
	MaxMemArbitrationStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.maxMemArbitration
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
	SumUnpackedBytesSentTiKVTotalStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.UnpackedBytesSentTiKVTotal
	},
	SumUnpackedBytesReceivedTiKVTotalStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.UnpackedBytesReceivedTiKVTotal
	},
	SumUnpackedBytesSentTiKVCrossZoneStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.UnpackedBytesSentTiKVCrossZone
	},
	SumUnpackedBytesReceivedTiKVCrossZoneStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.UnpackedBytesReceivedTiKVCrossZone
	},
	SumUnpackedBytesSentTiFlashTotalStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.UnpackedBytesSentTiFlashTotal
	},
	SumUnpackedBytesReceivedTiFlashTotalStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.UnpackedBytesReceivedTiFlashTotal
	},
	SumUnpackedBytesSentTiFlashCrossZoneStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.UnpackedBytesSentTiFlashCrossZone
	},
	SumUnpackedBytesReceiveTiFlashCrossZoneStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.UnpackedBytesReceivedTiFlashCrossZone
	},
	UnpackedBytesSentTiKVTotalStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.UnpackedBytesSentTiKVTotal
	},
	UnpackedBytesReceivedTiKVTotalStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.UnpackedBytesReceivedTiKVTotal
	},
	UnpackedBytesSentTiKVCrossZoneStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.UnpackedBytesSentTiKVCrossZone
	},
	UnpackedBytesReceivedTiKVCrossZoneStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.UnpackedBytesReceivedTiKVCrossZone
	},
	UnpackedBytesSentTiFlashTotalStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.UnpackedBytesSentTiFlashTotal
	},
	UnpackedBytesReceivedTiFlashTotalStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.UnpackedBytesReceivedTiFlashTotal
	},
	UnpackedBytesSentTiFlashCrossZoneStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.UnpackedBytesSentTiFlashCrossZone
	},
	UnpackedBytesReceiveTiFlashCrossZoneStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.UnpackedBytesReceivedTiFlashCrossZone
	},
	StorageKVStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.storageKV
	},
	StorageMPPStr: func(_ *stmtSummaryReader, _ *stmtSummaryByDigestElement, _ *stmtSummaryByDigest, ssStats *stmtSummaryStats) any {
		return ssStats.storageMPP
	},
}
