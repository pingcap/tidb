// Copyright 2019 PingCAP, Inc.
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
	"container/list"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/plancodec"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
)

func emptyPlanGenerator() (string, string) {
	return "", ""
}

func fakePlanDigestGenerator() string {
	return "point_get"
}

func TestSetUp(t *testing.T) {
	ssMap := newStmtSummaryByDigestMap()
	err := ssMap.SetEnabled(true)
	require.NoError(t, err)
	err = ssMap.SetRefreshInterval(1800)
	require.NoError(t, err)
	err = ssMap.SetHistorySize(24)
	require.NoError(t, err)
}

const (
	boTxnLockName = "txnlock"
)

// Test stmtSummaryByDigest.AddStatement.
func TestAddStatement(t *testing.T) {
	ssMap := newStmtSummaryByDigestMap()
	now := time.Now().Unix()
	ssMap.beginTimeForCurInterval = now + 60

	tables := []stmtctx.TableEntry{{DB: "db1", Table: "tb1"}, {DB: "db2", Table: "tb2"}}
	indexes := []string{"a", "b"}

	// first statement
	stmtExecInfo1 := generateAnyExecInfo()
	stmtExecInfo1.ExecDetail.CommitDetail.Mu.BackoffTypes = make([]string, 0)
	key := &stmtSummaryByDigestKey{
		schemaName: stmtExecInfo1.SchemaName,
		digest:     stmtExecInfo1.Digest,
		planDigest: stmtExecInfo1.PlanDigest,
	}
	samplePlan, _ := stmtExecInfo1.PlanGenerator()
	stmtExecInfo1.ExecDetail.CommitDetail.Mu.Lock()
	expectedSummaryElement := stmtSummaryByDigestElement{
		beginTime:            now + 60,
		endTime:              now + 1860,
		sampleSQL:            stmtExecInfo1.OriginalSQL,
		samplePlan:           samplePlan,
		indexNames:           stmtExecInfo1.StmtCtx.IndexNames,
		execCount:            1,
		sumLatency:           stmtExecInfo1.TotalLatency,
		maxLatency:           stmtExecInfo1.TotalLatency,
		minLatency:           stmtExecInfo1.TotalLatency,
		sumParseLatency:      stmtExecInfo1.ParseLatency,
		maxParseLatency:      stmtExecInfo1.ParseLatency,
		sumCompileLatency:    stmtExecInfo1.CompileLatency,
		maxCompileLatency:    stmtExecInfo1.CompileLatency,
		sumNumCopTasks:       int64(stmtExecInfo1.CopTasks.NumCopTasks),
		maxCopProcessTime:    stmtExecInfo1.CopTasks.MaxProcessTime,
		maxCopProcessAddress: stmtExecInfo1.CopTasks.MaxProcessAddress,
		maxCopWaitTime:       stmtExecInfo1.CopTasks.MaxWaitTime,
		maxCopWaitAddress:    stmtExecInfo1.CopTasks.MaxWaitAddress,
		sumProcessTime:       stmtExecInfo1.ExecDetail.TimeDetail.ProcessTime,
		maxProcessTime:       stmtExecInfo1.ExecDetail.TimeDetail.ProcessTime,
		sumWaitTime:          stmtExecInfo1.ExecDetail.TimeDetail.WaitTime,
		maxWaitTime:          stmtExecInfo1.ExecDetail.TimeDetail.WaitTime,
		sumBackoffTime:       stmtExecInfo1.ExecDetail.BackoffTime,
		maxBackoffTime:       stmtExecInfo1.ExecDetail.BackoffTime,
		sumTotalKeys:         stmtExecInfo1.ExecDetail.ScanDetail.TotalKeys,
		maxTotalKeys:         stmtExecInfo1.ExecDetail.ScanDetail.TotalKeys,
		sumProcessedKeys:     stmtExecInfo1.ExecDetail.ScanDetail.ProcessedKeys,
		maxProcessedKeys:     stmtExecInfo1.ExecDetail.ScanDetail.ProcessedKeys,
		sumGetCommitTsTime:   stmtExecInfo1.ExecDetail.CommitDetail.GetCommitTsTime,
		maxGetCommitTsTime:   stmtExecInfo1.ExecDetail.CommitDetail.GetCommitTsTime,
		sumPrewriteTime:      stmtExecInfo1.ExecDetail.CommitDetail.PrewriteTime,
		maxPrewriteTime:      stmtExecInfo1.ExecDetail.CommitDetail.PrewriteTime,
		sumCommitTime:        stmtExecInfo1.ExecDetail.CommitDetail.CommitTime,
		maxCommitTime:        stmtExecInfo1.ExecDetail.CommitDetail.CommitTime,
		sumLocalLatchTime:    stmtExecInfo1.ExecDetail.CommitDetail.LocalLatchTime,
		maxLocalLatchTime:    stmtExecInfo1.ExecDetail.CommitDetail.LocalLatchTime,
		sumCommitBackoffTime: stmtExecInfo1.ExecDetail.CommitDetail.Mu.CommitBackoffTime,
		maxCommitBackoffTime: stmtExecInfo1.ExecDetail.CommitDetail.Mu.CommitBackoffTime,
		sumResolveLockTime:   stmtExecInfo1.ExecDetail.CommitDetail.ResolveLockTime,
		maxResolveLockTime:   stmtExecInfo1.ExecDetail.CommitDetail.ResolveLockTime,
		sumWriteKeys:         int64(stmtExecInfo1.ExecDetail.CommitDetail.WriteKeys),
		maxWriteKeys:         stmtExecInfo1.ExecDetail.CommitDetail.WriteKeys,
		sumWriteSize:         int64(stmtExecInfo1.ExecDetail.CommitDetail.WriteSize),
		maxWriteSize:         stmtExecInfo1.ExecDetail.CommitDetail.WriteSize,
		sumPrewriteRegionNum: int64(stmtExecInfo1.ExecDetail.CommitDetail.PrewriteRegionNum),
		maxPrewriteRegionNum: stmtExecInfo1.ExecDetail.CommitDetail.PrewriteRegionNum,
		sumTxnRetry:          int64(stmtExecInfo1.ExecDetail.CommitDetail.TxnRetry),
		maxTxnRetry:          stmtExecInfo1.ExecDetail.CommitDetail.TxnRetry,
		backoffTypes:         make(map[string]int),
		sumMem:               stmtExecInfo1.MemMax,
		maxMem:               stmtExecInfo1.MemMax,
		sumDisk:              stmtExecInfo1.DiskMax,
		maxDisk:              stmtExecInfo1.DiskMax,
		sumAffectedRows:      stmtExecInfo1.StmtCtx.AffectedRows(),
		firstSeen:            stmtExecInfo1.StartTime,
		lastSeen:             stmtExecInfo1.StartTime,
	}
	stmtExecInfo1.ExecDetail.CommitDetail.Mu.Unlock()
	history := list.New()
	history.PushBack(&expectedSummaryElement)
	expectedSummary := stmtSummaryByDigest{
		schemaName:    stmtExecInfo1.SchemaName,
		stmtType:      stmtExecInfo1.StmtCtx.StmtType,
		digest:        stmtExecInfo1.Digest,
		normalizedSQL: stmtExecInfo1.NormalizedSQL,
		planDigest:    stmtExecInfo1.PlanDigest,
		tableNames:    "db1.tb1,db2.tb2",
		history:       history,
	}
	ssMap.AddStatement(stmtExecInfo1)
	summary, ok := ssMap.summaryMap.Get(key)
	require.True(t, ok)
	require.True(t, matchStmtSummaryByDigest(summary.(*stmtSummaryByDigest), &expectedSummary))

	// Second statement is similar with the first statement, and its values are
	// greater than that of the first statement.
	stmtExecInfo2 := &StmtExecInfo{
		SchemaName:     "schema_name",
		OriginalSQL:    "original_sql2",
		NormalizedSQL:  "normalized_sql",
		Digest:         "digest",
		PlanDigest:     "plan_digest",
		PlanGenerator:  emptyPlanGenerator,
		User:           "user2",
		TotalLatency:   20000,
		ParseLatency:   200,
		CompileLatency: 2000,
		CopTasks: &stmtctx.CopTasksDetails{
			NumCopTasks:       20,
			AvgProcessTime:    2000,
			P90ProcessTime:    20000,
			MaxProcessAddress: "200",
			MaxProcessTime:    25000,
			AvgWaitTime:       200,
			P90WaitTime:       2000,
			MaxWaitAddress:    "201",
			MaxWaitTime:       2500,
		},
		ExecDetail: &execdetails.ExecDetails{
			CalleeAddress: "202",
			BackoffTime:   180,
			RequestCount:  20,
			CommitDetail: &util.CommitDetails{
				GetCommitTsTime: 500,
				PrewriteTime:    50000,
				CommitTime:      5000,
				LocalLatchTime:  50,
				Mu: struct {
					sync.Mutex
					CommitBackoffTime int64
					BackoffTypes      []string
				}{
					CommitBackoffTime: 1000,
					BackoffTypes:      []string{boTxnLockName},
				},
				ResolveLockTime:   10000,
				WriteKeys:         100000,
				WriteSize:         1000000,
				PrewriteRegionNum: 100,
				TxnRetry:          10,
			},
			ScanDetail: &util.ScanDetail{
				TotalKeys:                 6000,
				ProcessedKeys:             1500,
				RocksdbDeleteSkippedCount: 100,
				RocksdbKeySkippedCount:    10,
				RocksdbBlockCacheHitCount: 10,
				RocksdbBlockReadCount:     10,
				RocksdbBlockReadByte:      1000,
			},
			TimeDetail: util.TimeDetail{
				ProcessTime: 1500,
				WaitTime:    150,
			},
		},
		StmtCtx: &stmtctx.StatementContext{
			StmtType:   "Select",
			Tables:     tables,
			IndexNames: indexes,
		},
		MemMax:    20000,
		DiskMax:   20000,
		StartTime: time.Date(2019, 1, 1, 10, 10, 20, 10, time.UTC),
		Succeed:   true,
	}
	stmtExecInfo2.StmtCtx.AddAffectedRows(200)
	expectedSummaryElement.execCount++
	expectedSummaryElement.sumLatency += stmtExecInfo2.TotalLatency
	expectedSummaryElement.maxLatency = stmtExecInfo2.TotalLatency
	expectedSummaryElement.sumParseLatency += stmtExecInfo2.ParseLatency
	expectedSummaryElement.maxParseLatency = stmtExecInfo2.ParseLatency
	expectedSummaryElement.sumCompileLatency += stmtExecInfo2.CompileLatency
	expectedSummaryElement.maxCompileLatency = stmtExecInfo2.CompileLatency
	expectedSummaryElement.sumNumCopTasks += int64(stmtExecInfo2.CopTasks.NumCopTasks)
	expectedSummaryElement.maxCopProcessTime = stmtExecInfo2.CopTasks.MaxProcessTime
	expectedSummaryElement.maxCopProcessAddress = stmtExecInfo2.CopTasks.MaxProcessAddress
	expectedSummaryElement.maxCopWaitTime = stmtExecInfo2.CopTasks.MaxWaitTime
	expectedSummaryElement.maxCopWaitAddress = stmtExecInfo2.CopTasks.MaxWaitAddress
	expectedSummaryElement.sumProcessTime += stmtExecInfo2.ExecDetail.TimeDetail.ProcessTime
	expectedSummaryElement.maxProcessTime = stmtExecInfo2.ExecDetail.TimeDetail.ProcessTime
	expectedSummaryElement.sumWaitTime += stmtExecInfo2.ExecDetail.TimeDetail.WaitTime
	expectedSummaryElement.maxWaitTime = stmtExecInfo2.ExecDetail.TimeDetail.WaitTime
	expectedSummaryElement.sumBackoffTime += stmtExecInfo2.ExecDetail.BackoffTime
	expectedSummaryElement.maxBackoffTime = stmtExecInfo2.ExecDetail.BackoffTime
	expectedSummaryElement.sumTotalKeys += stmtExecInfo2.ExecDetail.ScanDetail.TotalKeys
	expectedSummaryElement.maxTotalKeys = stmtExecInfo2.ExecDetail.ScanDetail.TotalKeys
	expectedSummaryElement.sumProcessedKeys += stmtExecInfo2.ExecDetail.ScanDetail.ProcessedKeys
	expectedSummaryElement.maxProcessedKeys = stmtExecInfo2.ExecDetail.ScanDetail.ProcessedKeys
	expectedSummaryElement.sumGetCommitTsTime += stmtExecInfo2.ExecDetail.CommitDetail.GetCommitTsTime
	expectedSummaryElement.maxGetCommitTsTime = stmtExecInfo2.ExecDetail.CommitDetail.GetCommitTsTime
	expectedSummaryElement.sumPrewriteTime += stmtExecInfo2.ExecDetail.CommitDetail.PrewriteTime
	expectedSummaryElement.maxPrewriteTime = stmtExecInfo2.ExecDetail.CommitDetail.PrewriteTime
	expectedSummaryElement.sumCommitTime += stmtExecInfo2.ExecDetail.CommitDetail.CommitTime
	expectedSummaryElement.maxCommitTime = stmtExecInfo2.ExecDetail.CommitDetail.CommitTime
	expectedSummaryElement.sumLocalLatchTime += stmtExecInfo2.ExecDetail.CommitDetail.LocalLatchTime
	expectedSummaryElement.maxLocalLatchTime = stmtExecInfo2.ExecDetail.CommitDetail.LocalLatchTime
	stmtExecInfo2.ExecDetail.CommitDetail.Mu.Lock()
	expectedSummaryElement.sumCommitBackoffTime += stmtExecInfo2.ExecDetail.CommitDetail.Mu.CommitBackoffTime
	expectedSummaryElement.maxCommitBackoffTime = stmtExecInfo2.ExecDetail.CommitDetail.Mu.CommitBackoffTime
	stmtExecInfo2.ExecDetail.CommitDetail.Mu.Unlock()
	expectedSummaryElement.sumResolveLockTime += stmtExecInfo2.ExecDetail.CommitDetail.ResolveLockTime
	expectedSummaryElement.maxResolveLockTime = stmtExecInfo2.ExecDetail.CommitDetail.ResolveLockTime
	expectedSummaryElement.sumWriteKeys += int64(stmtExecInfo2.ExecDetail.CommitDetail.WriteKeys)
	expectedSummaryElement.maxWriteKeys = stmtExecInfo2.ExecDetail.CommitDetail.WriteKeys
	expectedSummaryElement.sumWriteSize += int64(stmtExecInfo2.ExecDetail.CommitDetail.WriteSize)
	expectedSummaryElement.maxWriteSize = stmtExecInfo2.ExecDetail.CommitDetail.WriteSize
	expectedSummaryElement.sumPrewriteRegionNum += int64(stmtExecInfo2.ExecDetail.CommitDetail.PrewriteRegionNum)
	expectedSummaryElement.maxPrewriteRegionNum = stmtExecInfo2.ExecDetail.CommitDetail.PrewriteRegionNum
	expectedSummaryElement.sumTxnRetry += int64(stmtExecInfo2.ExecDetail.CommitDetail.TxnRetry)
	expectedSummaryElement.maxTxnRetry = stmtExecInfo2.ExecDetail.CommitDetail.TxnRetry
	expectedSummaryElement.sumBackoffTimes += 1
	expectedSummaryElement.backoffTypes[boTxnLockName] = 1
	expectedSummaryElement.sumMem += stmtExecInfo2.MemMax
	expectedSummaryElement.maxMem = stmtExecInfo2.MemMax
	expectedSummaryElement.sumDisk += stmtExecInfo2.DiskMax
	expectedSummaryElement.maxDisk = stmtExecInfo2.DiskMax
	expectedSummaryElement.sumAffectedRows += stmtExecInfo2.StmtCtx.AffectedRows()
	expectedSummaryElement.lastSeen = stmtExecInfo2.StartTime

	ssMap.AddStatement(stmtExecInfo2)
	summary, ok = ssMap.summaryMap.Get(key)
	require.True(t, ok)
	require.True(t, matchStmtSummaryByDigest(summary.(*stmtSummaryByDigest), &expectedSummary))

	// Third statement is similar with the first statement, and its values are
	// less than that of the first statement.
	stmtExecInfo3 := &StmtExecInfo{
		SchemaName:     "schema_name",
		OriginalSQL:    "original_sql3",
		NormalizedSQL:  "normalized_sql",
		Digest:         "digest",
		PlanDigest:     "plan_digest",
		PlanGenerator:  emptyPlanGenerator,
		User:           "user3",
		TotalLatency:   1000,
		ParseLatency:   50,
		CompileLatency: 500,
		CopTasks: &stmtctx.CopTasksDetails{
			NumCopTasks:       2,
			AvgProcessTime:    100,
			P90ProcessTime:    300,
			MaxProcessAddress: "300",
			MaxProcessTime:    350,
			AvgWaitTime:       20,
			P90WaitTime:       200,
			MaxWaitAddress:    "301",
			MaxWaitTime:       250,
		},
		ExecDetail: &execdetails.ExecDetails{
			CalleeAddress: "302",
			BackoffTime:   18,
			RequestCount:  2,
			CommitDetail: &util.CommitDetails{
				GetCommitTsTime: 50,
				PrewriteTime:    5000,
				CommitTime:      500,
				LocalLatchTime:  5,
				Mu: struct {
					sync.Mutex
					CommitBackoffTime int64
					BackoffTypes      []string
				}{
					CommitBackoffTime: 100,
					BackoffTypes:      []string{boTxnLockName},
				},
				ResolveLockTime:   1000,
				WriteKeys:         10000,
				WriteSize:         100000,
				PrewriteRegionNum: 10,
				TxnRetry:          1,
			},
			ScanDetail: &util.ScanDetail{
				TotalKeys:                 600,
				ProcessedKeys:             150,
				RocksdbDeleteSkippedCount: 100,
				RocksdbKeySkippedCount:    10,
				RocksdbBlockCacheHitCount: 10,
				RocksdbBlockReadCount:     10,
				RocksdbBlockReadByte:      1000,
			},
			TimeDetail: util.TimeDetail{
				ProcessTime: 150,
				WaitTime:    15,
			},
		},
		StmtCtx: &stmtctx.StatementContext{
			StmtType:   "Select",
			Tables:     tables,
			IndexNames: indexes,
		},
		MemMax:    200,
		DiskMax:   200,
		StartTime: time.Date(2019, 1, 1, 10, 10, 0, 10, time.UTC),
		Succeed:   true,
	}
	stmtExecInfo3.StmtCtx.AddAffectedRows(20000)
	expectedSummaryElement.execCount++
	expectedSummaryElement.sumLatency += stmtExecInfo3.TotalLatency
	expectedSummaryElement.minLatency = stmtExecInfo3.TotalLatency
	expectedSummaryElement.sumParseLatency += stmtExecInfo3.ParseLatency
	expectedSummaryElement.sumCompileLatency += stmtExecInfo3.CompileLatency
	expectedSummaryElement.sumNumCopTasks += int64(stmtExecInfo3.CopTasks.NumCopTasks)
	expectedSummaryElement.sumProcessTime += stmtExecInfo3.ExecDetail.TimeDetail.ProcessTime
	expectedSummaryElement.sumWaitTime += stmtExecInfo3.ExecDetail.TimeDetail.WaitTime
	expectedSummaryElement.sumBackoffTime += stmtExecInfo3.ExecDetail.BackoffTime
	expectedSummaryElement.sumTotalKeys += stmtExecInfo3.ExecDetail.ScanDetail.TotalKeys
	expectedSummaryElement.sumProcessedKeys += stmtExecInfo3.ExecDetail.ScanDetail.ProcessedKeys
	expectedSummaryElement.sumGetCommitTsTime += stmtExecInfo3.ExecDetail.CommitDetail.GetCommitTsTime
	expectedSummaryElement.sumPrewriteTime += stmtExecInfo3.ExecDetail.CommitDetail.PrewriteTime
	expectedSummaryElement.sumCommitTime += stmtExecInfo3.ExecDetail.CommitDetail.CommitTime
	expectedSummaryElement.sumLocalLatchTime += stmtExecInfo3.ExecDetail.CommitDetail.LocalLatchTime
	stmtExecInfo3.ExecDetail.CommitDetail.Mu.Lock()
	expectedSummaryElement.sumCommitBackoffTime += stmtExecInfo3.ExecDetail.CommitDetail.Mu.CommitBackoffTime
	stmtExecInfo3.ExecDetail.CommitDetail.Mu.Unlock()
	expectedSummaryElement.sumResolveLockTime += stmtExecInfo3.ExecDetail.CommitDetail.ResolveLockTime
	expectedSummaryElement.sumWriteKeys += int64(stmtExecInfo3.ExecDetail.CommitDetail.WriteKeys)
	expectedSummaryElement.sumWriteSize += int64(stmtExecInfo3.ExecDetail.CommitDetail.WriteSize)
	expectedSummaryElement.sumPrewriteRegionNum += int64(stmtExecInfo3.ExecDetail.CommitDetail.PrewriteRegionNum)
	expectedSummaryElement.sumTxnRetry += int64(stmtExecInfo3.ExecDetail.CommitDetail.TxnRetry)
	expectedSummaryElement.sumBackoffTimes += 1
	expectedSummaryElement.backoffTypes[boTxnLockName] = 2
	expectedSummaryElement.sumMem += stmtExecInfo3.MemMax
	expectedSummaryElement.sumDisk += stmtExecInfo3.DiskMax
	expectedSummaryElement.sumAffectedRows += stmtExecInfo3.StmtCtx.AffectedRows()
	expectedSummaryElement.firstSeen = stmtExecInfo3.StartTime

	ssMap.AddStatement(stmtExecInfo3)
	summary, ok = ssMap.summaryMap.Get(key)
	require.True(t, ok)
	require.True(t, matchStmtSummaryByDigest(summary.(*stmtSummaryByDigest), &expectedSummary))

	// Fourth statement is in a different schema.
	stmtExecInfo4 := stmtExecInfo1
	stmtExecInfo4.SchemaName = "schema2"
	stmtExecInfo4.ExecDetail.CommitDetail = nil
	key = &stmtSummaryByDigestKey{
		schemaName: stmtExecInfo4.SchemaName,
		digest:     stmtExecInfo4.Digest,
		planDigest: stmtExecInfo4.PlanDigest,
	}
	ssMap.AddStatement(stmtExecInfo4)
	require.Equal(t, 2, ssMap.summaryMap.Size())
	_, ok = ssMap.summaryMap.Get(key)
	require.True(t, ok)

	// Fifth statement has a different digest.
	stmtExecInfo5 := stmtExecInfo1
	stmtExecInfo5.Digest = "digest2"
	key = &stmtSummaryByDigestKey{
		schemaName: stmtExecInfo5.SchemaName,
		digest:     stmtExecInfo5.Digest,
		planDigest: stmtExecInfo4.PlanDigest,
	}
	ssMap.AddStatement(stmtExecInfo5)
	require.Equal(t, 3, ssMap.summaryMap.Size())
	_, ok = ssMap.summaryMap.Get(key)
	require.True(t, ok)

	// Sixth statement has a different plan digest.
	stmtExecInfo6 := stmtExecInfo1
	stmtExecInfo6.PlanDigest = "plan_digest2"
	key = &stmtSummaryByDigestKey{
		schemaName: stmtExecInfo6.SchemaName,
		digest:     stmtExecInfo6.Digest,
		planDigest: stmtExecInfo6.PlanDigest,
	}
	ssMap.AddStatement(stmtExecInfo6)
	require.Equal(t, 4, ssMap.summaryMap.Size())
	_, ok = ssMap.summaryMap.Get(key)
	require.True(t, ok)

	// Test for plan too large
	stmtExecInfo7 := stmtExecInfo1
	stmtExecInfo7.PlanDigest = "plan_digest7"
	stmtExecInfo7.PlanGenerator = func() (string, string) {
		buf := make([]byte, maxEncodedPlanSizeInBytes+1)
		for i := range buf {
			buf[i] = 'a'
		}
		return string(buf), ""
	}
	key = &stmtSummaryByDigestKey{
		schemaName: stmtExecInfo7.SchemaName,
		digest:     stmtExecInfo7.Digest,
		planDigest: stmtExecInfo7.PlanDigest,
	}
	ssMap.AddStatement(stmtExecInfo7)
	require.Equal(t, 5, ssMap.summaryMap.Size())
	v, ok := ssMap.summaryMap.Get(key)
	require.True(t, ok)
	stmt := v.(*stmtSummaryByDigest)
	require.Equal(t, key.digest, stmt.digest)
	e := stmt.history.Back()
	ssElement := e.Value.(*stmtSummaryByDigestElement)
	require.Equal(t, plancodec.PlanDiscardedEncoded, ssElement.samplePlan)
}

func matchStmtSummaryByDigest(first, second *stmtSummaryByDigest) bool {
	if first.schemaName != second.schemaName ||
		first.digest != second.digest ||
		first.normalizedSQL != second.normalizedSQL ||
		first.planDigest != second.planDigest ||
		first.tableNames != second.tableNames ||
		!strings.EqualFold(first.stmtType, second.stmtType) {
		return false
	}
	if first.history.Len() != second.history.Len() {
		return false
	}
	ele1 := first.history.Front()
	ele2 := second.history.Front()
	for {
		if ele1 == nil {
			break
		}
		ssElement1 := ele1.Value.(*stmtSummaryByDigestElement)
		ssElement2 := ele2.Value.(*stmtSummaryByDigestElement)
		if ssElement1.beginTime != ssElement2.beginTime ||
			ssElement1.endTime != ssElement2.endTime ||
			ssElement1.sampleSQL != ssElement2.sampleSQL ||
			ssElement1.samplePlan != ssElement2.samplePlan ||
			ssElement1.prevSQL != ssElement2.prevSQL ||
			ssElement1.execCount != ssElement2.execCount ||
			ssElement1.sumErrors != ssElement2.sumErrors ||
			ssElement1.sumWarnings != ssElement2.sumWarnings ||
			ssElement1.sumLatency != ssElement2.sumLatency ||
			ssElement1.maxLatency != ssElement2.maxLatency ||
			ssElement1.minLatency != ssElement2.minLatency ||
			ssElement1.sumParseLatency != ssElement2.sumParseLatency ||
			ssElement1.maxParseLatency != ssElement2.maxParseLatency ||
			ssElement1.sumCompileLatency != ssElement2.sumCompileLatency ||
			ssElement1.maxCompileLatency != ssElement2.maxCompileLatency ||
			ssElement1.sumNumCopTasks != ssElement2.sumNumCopTasks ||
			ssElement1.maxCopProcessTime != ssElement2.maxCopProcessTime ||
			ssElement1.maxCopProcessAddress != ssElement2.maxCopProcessAddress ||
			ssElement1.maxCopWaitTime != ssElement2.maxCopWaitTime ||
			ssElement1.maxCopWaitAddress != ssElement2.maxCopWaitAddress ||
			ssElement1.sumProcessTime != ssElement2.sumProcessTime ||
			ssElement1.maxProcessTime != ssElement2.maxProcessTime ||
			ssElement1.sumWaitTime != ssElement2.sumWaitTime ||
			ssElement1.maxWaitTime != ssElement2.maxWaitTime ||
			ssElement1.sumBackoffTime != ssElement2.sumBackoffTime ||
			ssElement1.maxBackoffTime != ssElement2.maxBackoffTime ||
			ssElement1.sumTotalKeys != ssElement2.sumTotalKeys ||
			ssElement1.maxTotalKeys != ssElement2.maxTotalKeys ||
			ssElement1.sumProcessedKeys != ssElement2.sumProcessedKeys ||
			ssElement1.maxProcessedKeys != ssElement2.maxProcessedKeys ||
			ssElement1.sumGetCommitTsTime != ssElement2.sumGetCommitTsTime ||
			ssElement1.maxGetCommitTsTime != ssElement2.maxGetCommitTsTime ||
			ssElement1.sumPrewriteTime != ssElement2.sumPrewriteTime ||
			ssElement1.maxPrewriteTime != ssElement2.maxPrewriteTime ||
			ssElement1.sumCommitTime != ssElement2.sumCommitTime ||
			ssElement1.maxCommitTime != ssElement2.maxCommitTime ||
			ssElement1.sumLocalLatchTime != ssElement2.sumLocalLatchTime ||
			ssElement1.maxLocalLatchTime != ssElement2.maxLocalLatchTime ||
			ssElement1.sumCommitBackoffTime != ssElement2.sumCommitBackoffTime ||
			ssElement1.maxCommitBackoffTime != ssElement2.maxCommitBackoffTime ||
			ssElement1.sumResolveLockTime != ssElement2.sumResolveLockTime ||
			ssElement1.maxResolveLockTime != ssElement2.maxResolveLockTime ||
			ssElement1.sumWriteKeys != ssElement2.sumWriteKeys ||
			ssElement1.maxWriteKeys != ssElement2.maxWriteKeys ||
			ssElement1.sumWriteSize != ssElement2.sumWriteSize ||
			ssElement1.maxWriteSize != ssElement2.maxWriteSize ||
			ssElement1.sumPrewriteRegionNum != ssElement2.sumPrewriteRegionNum ||
			ssElement1.maxPrewriteRegionNum != ssElement2.maxPrewriteRegionNum ||
			ssElement1.sumTxnRetry != ssElement2.sumTxnRetry ||
			ssElement1.maxTxnRetry != ssElement2.maxTxnRetry ||
			ssElement1.sumBackoffTimes != ssElement2.sumBackoffTimes ||
			ssElement1.sumMem != ssElement2.sumMem ||
			ssElement1.maxMem != ssElement2.maxMem ||
			ssElement1.sumAffectedRows != ssElement2.sumAffectedRows ||
			ssElement1.firstSeen != ssElement2.firstSeen ||
			ssElement1.lastSeen != ssElement2.lastSeen {
			return false
		}
		if len(ssElement1.backoffTypes) != len(ssElement2.backoffTypes) {
			return false
		}
		for key, value1 := range ssElement1.backoffTypes {
			value2, ok := ssElement2.backoffTypes[key]
			if !ok || value1 != value2 {
				return false
			}
		}
		if len(ssElement1.indexNames) != len(ssElement2.indexNames) {
			return false
		}
		for key, value1 := range ssElement1.indexNames {
			if value1 != ssElement2.indexNames[key] {
				return false
			}
		}
		ele1 = ele1.Next()
		ele2 = ele2.Next()
	}
	return true
}

func match(t *testing.T, row []types.Datum, expected ...interface{}) {
	require.Equal(t, len(expected), len(row))
	for i := range row {
		got := fmt.Sprintf("%v", row[i].GetValue())
		need := fmt.Sprintf("%v", expected[i])
		require.Equal(t, need, got)
	}
}

func generateAnyExecInfo() *StmtExecInfo {
	tables := []stmtctx.TableEntry{{DB: "db1", Table: "tb1"}, {DB: "db2", Table: "tb2"}}
	indexes := []string{"a"}
	stmtExecInfo := &StmtExecInfo{
		SchemaName:     "schema_name",
		OriginalSQL:    "original_sql1",
		NormalizedSQL:  "normalized_sql",
		Digest:         "digest",
		PlanDigest:     "plan_digest",
		PlanGenerator:  emptyPlanGenerator,
		User:           "user",
		TotalLatency:   10000,
		ParseLatency:   100,
		CompileLatency: 1000,
		CopTasks: &stmtctx.CopTasksDetails{
			NumCopTasks:       10,
			AvgProcessTime:    1000,
			P90ProcessTime:    10000,
			MaxProcessAddress: "127",
			MaxProcessTime:    15000,
			AvgWaitTime:       100,
			P90WaitTime:       1000,
			MaxWaitAddress:    "128",
			MaxWaitTime:       1500,
		},
		ExecDetail: &execdetails.ExecDetails{
			CalleeAddress: "129",
			BackoffTime:   80,
			RequestCount:  10,
			CommitDetail: &util.CommitDetails{
				GetCommitTsTime: 100,
				PrewriteTime:    10000,
				CommitTime:      1000,
				LocalLatchTime:  10,
				Mu: struct {
					sync.Mutex
					CommitBackoffTime int64
					BackoffTypes      []string
				}{
					CommitBackoffTime: 200,
					BackoffTypes:      []string{boTxnLockName},
				},
				ResolveLockTime:   2000,
				WriteKeys:         20000,
				WriteSize:         200000,
				PrewriteRegionNum: 20,
				TxnRetry:          2,
			},
			ScanDetail: &util.ScanDetail{
				TotalKeys:                 1000,
				ProcessedKeys:             500,
				RocksdbDeleteSkippedCount: 100,
				RocksdbKeySkippedCount:    10,
				RocksdbBlockCacheHitCount: 10,
				RocksdbBlockReadCount:     10,
				RocksdbBlockReadByte:      1000,
			},
			TimeDetail: util.TimeDetail{
				ProcessTime: 500,
				WaitTime:    50,
			},
		},
		StmtCtx: &stmtctx.StatementContext{
			StmtType:   "Select",
			Tables:     tables,
			IndexNames: indexes,
		},
		MemMax:    10000,
		DiskMax:   10000,
		StartTime: time.Date(2019, 1, 1, 10, 10, 10, 10, time.UTC),
		Succeed:   true,
	}
	stmtExecInfo.StmtCtx.AddAffectedRows(10000)
	return stmtExecInfo
}

func newStmtSummaryReaderForTest(ssMap *stmtSummaryByDigestMap) *stmtSummaryReader {
	columnNames := []string{
		SummaryBeginTimeStr,
		SummaryEndTimeStr,
		StmtTypeStr,
		SchemaNameStr,
		DigestStr,
		DigestTextStr,
		TableNamesStr,
		IndexNamesStr,
		SampleUserStr,
		ExecCountStr,
		SumErrorsStr,
		SumWarningsStr,
		SumLatencyStr,
		MaxLatencyStr,
		MinLatencyStr,
		AvgLatencyStr,
		AvgParseLatencyStr,
		MaxParseLatencyStr,
		AvgCompileLatencyStr,
		MaxCompileLatencyStr,
		SumCopTaskNumStr,
		MaxCopProcessTimeStr,
		MaxCopProcessAddressStr,
		MaxCopWaitTimeStr,
		MaxCopWaitAddressStr,
		AvgProcessTimeStr,
		MaxProcessTimeStr,
		AvgWaitTimeStr,
		MaxWaitTimeStr,
		AvgBackoffTimeStr,
		MaxBackoffTimeStr,
		AvgTotalKeysStr,
		MaxTotalKeysStr,
		AvgProcessedKeysStr,
		MaxProcessedKeysStr,
		AvgRocksdbDeleteSkippedCountStr,
		MaxRocksdbDeleteSkippedCountStr,
		AvgRocksdbKeySkippedCountStr,
		MaxRocksdbKeySkippedCountStr,
		AvgRocksdbBlockCacheHitCountStr,
		MaxRocksdbBlockCacheHitCountStr,
		AvgRocksdbBlockReadCountStr,
		MaxRocksdbBlockReadCountStr,
		AvgRocksdbBlockReadByteStr,
		MaxRocksdbBlockReadByteStr,
		AvgPrewriteTimeStr,
		MaxPrewriteTimeStr,
		AvgCommitTimeStr,
		MaxCommitTimeStr,
		AvgGetCommitTsTimeStr,
		MaxGetCommitTsTimeStr,
		AvgCommitBackoffTimeStr,
		MaxCommitBackoffTimeStr,
		AvgResolveLockTimeStr,
		MaxResolveLockTimeStr,
		AvgLocalLatchWaitTimeStr,
		MaxLocalLatchWaitTimeStr,
		AvgWriteKeysStr,
		MaxWriteKeysStr,
		AvgWriteSizeStr,
		MaxWriteSizeStr,
		AvgPrewriteRegionsStr,
		MaxPrewriteRegionsStr,
		AvgTxnRetryStr,
		MaxTxnRetryStr,
		SumExecRetryStr,
		SumExecRetryTimeStr,
		SumBackoffTimesStr,
		BackoffTypesStr,
		AvgMemStr,
		MaxMemStr,
		AvgDiskStr,
		MaxDiskStr,
		AvgKvTimeStr,
		AvgPdTimeStr,
		AvgBackoffTotalTimeStr,
		AvgWriteSQLRespTimeStr,
		MaxResultRowsStr,
		MinResultRowsStr,
		AvgResultRowsStr,
		PreparedStr,
		AvgAffectedRowsStr,
		FirstSeenStr,
		LastSeenStr,
		PlanInCacheStr,
		PlanCacheHitsStr,
		PlanInBindingStr,
		QuerySampleTextStr,
		PrevSampleTextStr,
		PlanDigestStr,
		PlanStr,
	}
	cols := make([]*model.ColumnInfo, len(columnNames))
	for i := range columnNames {
		cols[i] = &model.ColumnInfo{
			ID:     int64(i),
			Name:   model.NewCIStr(columnNames[i]),
			Offset: i,
		}
	}
	reader := NewStmtSummaryReader(nil, true, cols, "", time.UTC)
	reader.ssMap = ssMap
	return reader
}

// Test stmtSummaryByDigest.ToDatum.
func TestToDatum(t *testing.T) {
	ssMap := newStmtSummaryByDigestMap()
	now := time.Now().Unix()
	// to disable expiration
	ssMap.beginTimeForCurInterval = now + 60

	stmtExecInfo1 := generateAnyExecInfo()
	ssMap.AddStatement(stmtExecInfo1)
	reader := newStmtSummaryReaderForTest(ssMap)
	datums := reader.GetStmtSummaryCurrentRows()
	require.Equal(t, 1, len(datums))
	n := types.NewTime(types.FromGoTime(time.Unix(ssMap.beginTimeForCurInterval, 0)), mysql.TypeTimestamp, types.DefaultFsp)
	e := types.NewTime(types.FromGoTime(time.Unix(ssMap.beginTimeForCurInterval+1800, 0)), mysql.TypeTimestamp, types.DefaultFsp)
	f := types.NewTime(types.FromGoTime(stmtExecInfo1.StartTime), mysql.TypeTimestamp, types.DefaultFsp)
	stmtExecInfo1.ExecDetail.CommitDetail.Mu.Lock()
	expectedDatum := []interface{}{n, e, "Select", stmtExecInfo1.SchemaName, stmtExecInfo1.Digest, stmtExecInfo1.NormalizedSQL,
		"db1.tb1,db2.tb2", "a", stmtExecInfo1.User, 1, 0, 0, int64(stmtExecInfo1.TotalLatency),
		int64(stmtExecInfo1.TotalLatency), int64(stmtExecInfo1.TotalLatency), int64(stmtExecInfo1.TotalLatency),
		int64(stmtExecInfo1.ParseLatency), int64(stmtExecInfo1.ParseLatency), int64(stmtExecInfo1.CompileLatency),
		int64(stmtExecInfo1.CompileLatency), stmtExecInfo1.CopTasks.NumCopTasks, int64(stmtExecInfo1.CopTasks.MaxProcessTime),
		stmtExecInfo1.CopTasks.MaxProcessAddress, int64(stmtExecInfo1.CopTasks.MaxWaitTime),
		stmtExecInfo1.CopTasks.MaxWaitAddress, int64(stmtExecInfo1.ExecDetail.TimeDetail.ProcessTime), int64(stmtExecInfo1.ExecDetail.TimeDetail.ProcessTime),
		int64(stmtExecInfo1.ExecDetail.TimeDetail.WaitTime), int64(stmtExecInfo1.ExecDetail.TimeDetail.WaitTime), int64(stmtExecInfo1.ExecDetail.BackoffTime),
		int64(stmtExecInfo1.ExecDetail.BackoffTime), stmtExecInfo1.ExecDetail.ScanDetail.TotalKeys, stmtExecInfo1.ExecDetail.ScanDetail.TotalKeys,
		stmtExecInfo1.ExecDetail.ScanDetail.ProcessedKeys, stmtExecInfo1.ExecDetail.ScanDetail.ProcessedKeys,
		int64(stmtExecInfo1.ExecDetail.ScanDetail.RocksdbDeleteSkippedCount), int64(stmtExecInfo1.ExecDetail.ScanDetail.RocksdbDeleteSkippedCount),
		int64(stmtExecInfo1.ExecDetail.ScanDetail.RocksdbKeySkippedCount), int64(stmtExecInfo1.ExecDetail.ScanDetail.RocksdbKeySkippedCount),
		int64(stmtExecInfo1.ExecDetail.ScanDetail.RocksdbBlockCacheHitCount), int64(stmtExecInfo1.ExecDetail.ScanDetail.RocksdbBlockCacheHitCount),
		int64(stmtExecInfo1.ExecDetail.ScanDetail.RocksdbBlockReadCount), int64(stmtExecInfo1.ExecDetail.ScanDetail.RocksdbBlockReadCount),
		int64(stmtExecInfo1.ExecDetail.ScanDetail.RocksdbBlockReadByte), int64(stmtExecInfo1.ExecDetail.ScanDetail.RocksdbBlockReadByte),
		int64(stmtExecInfo1.ExecDetail.CommitDetail.PrewriteTime), int64(stmtExecInfo1.ExecDetail.CommitDetail.PrewriteTime),
		int64(stmtExecInfo1.ExecDetail.CommitDetail.CommitTime), int64(stmtExecInfo1.ExecDetail.CommitDetail.CommitTime),
		int64(stmtExecInfo1.ExecDetail.CommitDetail.GetCommitTsTime), int64(stmtExecInfo1.ExecDetail.CommitDetail.GetCommitTsTime),
		stmtExecInfo1.ExecDetail.CommitDetail.Mu.CommitBackoffTime, stmtExecInfo1.ExecDetail.CommitDetail.Mu.CommitBackoffTime,
		stmtExecInfo1.ExecDetail.CommitDetail.ResolveLockTime, stmtExecInfo1.ExecDetail.CommitDetail.ResolveLockTime,
		int64(stmtExecInfo1.ExecDetail.CommitDetail.LocalLatchTime), int64(stmtExecInfo1.ExecDetail.CommitDetail.LocalLatchTime),
		stmtExecInfo1.ExecDetail.CommitDetail.WriteKeys, stmtExecInfo1.ExecDetail.CommitDetail.WriteKeys,
		stmtExecInfo1.ExecDetail.CommitDetail.WriteSize, stmtExecInfo1.ExecDetail.CommitDetail.WriteSize,
		stmtExecInfo1.ExecDetail.CommitDetail.PrewriteRegionNum, stmtExecInfo1.ExecDetail.CommitDetail.PrewriteRegionNum,
		stmtExecInfo1.ExecDetail.CommitDetail.TxnRetry, stmtExecInfo1.ExecDetail.CommitDetail.TxnRetry, 0, 0, 1,
		fmt.Sprintf("%s:1", boTxnLockName), stmtExecInfo1.MemMax, stmtExecInfo1.MemMax, stmtExecInfo1.DiskMax, stmtExecInfo1.DiskMax,
		0, 0, 0, 0, 0, 0, 0, 0, stmtExecInfo1.StmtCtx.AffectedRows(),
		f, f, 0, 0, 0, stmtExecInfo1.OriginalSQL, stmtExecInfo1.PrevSQL, "plan_digest", ""}
	stmtExecInfo1.ExecDetail.CommitDetail.Mu.Unlock()
	match(t, datums[0], expectedDatum...)
	datums = reader.GetStmtSummaryHistoryRows()
	require.Equal(t, 1, len(datums))
	match(t, datums[0], expectedDatum...)

	// test evict
	err := ssMap.SetMaxStmtCount(1)
	defer func() {
		// clean up
		err = ssMap.SetMaxStmtCount(24)
		require.NoError(t, err)
	}()

	require.NoError(t, err)
	stmtExecInfo2 := stmtExecInfo1
	stmtExecInfo2.Digest = "bandit sei"
	ssMap.AddStatement(stmtExecInfo2)
	require.Equal(t, 1, ssMap.summaryMap.Size())
	datums = reader.GetStmtSummaryCurrentRows()
	expectedEvictedDatum := []interface{}{n, e, "", "<nil>", "<nil>", "",
		"<nil>", "<nil>", stmtExecInfo1.User, 1, 0, 0, int64(stmtExecInfo1.TotalLatency),
		int64(stmtExecInfo1.TotalLatency), int64(stmtExecInfo1.TotalLatency), int64(stmtExecInfo1.TotalLatency),
		int64(stmtExecInfo1.ParseLatency), int64(stmtExecInfo1.ParseLatency), int64(stmtExecInfo1.CompileLatency),
		int64(stmtExecInfo1.CompileLatency), stmtExecInfo1.CopTasks.NumCopTasks, int64(stmtExecInfo1.CopTasks.MaxProcessTime),
		stmtExecInfo1.CopTasks.MaxProcessAddress, int64(stmtExecInfo1.CopTasks.MaxWaitTime),
		stmtExecInfo1.CopTasks.MaxWaitAddress, int64(stmtExecInfo1.ExecDetail.TimeDetail.ProcessTime), int64(stmtExecInfo1.ExecDetail.TimeDetail.ProcessTime),
		int64(stmtExecInfo1.ExecDetail.TimeDetail.WaitTime), int64(stmtExecInfo1.ExecDetail.TimeDetail.WaitTime), int64(stmtExecInfo1.ExecDetail.BackoffTime),
		int64(stmtExecInfo1.ExecDetail.BackoffTime), stmtExecInfo1.ExecDetail.ScanDetail.TotalKeys, stmtExecInfo1.ExecDetail.ScanDetail.TotalKeys,
		stmtExecInfo1.ExecDetail.ScanDetail.ProcessedKeys, stmtExecInfo1.ExecDetail.ScanDetail.ProcessedKeys,
		int64(stmtExecInfo1.ExecDetail.ScanDetail.RocksdbDeleteSkippedCount), int64(stmtExecInfo1.ExecDetail.ScanDetail.RocksdbDeleteSkippedCount),
		int64(stmtExecInfo1.ExecDetail.ScanDetail.RocksdbKeySkippedCount), int64(stmtExecInfo1.ExecDetail.ScanDetail.RocksdbKeySkippedCount),
		int64(stmtExecInfo1.ExecDetail.ScanDetail.RocksdbBlockCacheHitCount), int64(stmtExecInfo1.ExecDetail.ScanDetail.RocksdbBlockCacheHitCount),
		int64(stmtExecInfo1.ExecDetail.ScanDetail.RocksdbBlockReadCount), int64(stmtExecInfo1.ExecDetail.ScanDetail.RocksdbBlockReadCount),
		int64(stmtExecInfo1.ExecDetail.ScanDetail.RocksdbBlockReadByte), int64(stmtExecInfo1.ExecDetail.ScanDetail.RocksdbBlockReadByte),
		int64(stmtExecInfo1.ExecDetail.CommitDetail.PrewriteTime), int64(stmtExecInfo1.ExecDetail.CommitDetail.PrewriteTime),
		int64(stmtExecInfo1.ExecDetail.CommitDetail.CommitTime), int64(stmtExecInfo1.ExecDetail.CommitDetail.CommitTime),
		int64(stmtExecInfo1.ExecDetail.CommitDetail.GetCommitTsTime), int64(stmtExecInfo1.ExecDetail.CommitDetail.GetCommitTsTime),
		stmtExecInfo1.ExecDetail.CommitDetail.Mu.CommitBackoffTime, stmtExecInfo1.ExecDetail.CommitDetail.Mu.CommitBackoffTime,
		stmtExecInfo1.ExecDetail.CommitDetail.ResolveLockTime, stmtExecInfo1.ExecDetail.CommitDetail.ResolveLockTime,
		int64(stmtExecInfo1.ExecDetail.CommitDetail.LocalLatchTime), int64(stmtExecInfo1.ExecDetail.CommitDetail.LocalLatchTime),
		stmtExecInfo1.ExecDetail.CommitDetail.WriteKeys, stmtExecInfo1.ExecDetail.CommitDetail.WriteKeys,
		stmtExecInfo1.ExecDetail.CommitDetail.WriteSize, stmtExecInfo1.ExecDetail.CommitDetail.WriteSize,
		stmtExecInfo1.ExecDetail.CommitDetail.PrewriteRegionNum, stmtExecInfo1.ExecDetail.CommitDetail.PrewriteRegionNum,
		stmtExecInfo1.ExecDetail.CommitDetail.TxnRetry, stmtExecInfo1.ExecDetail.CommitDetail.TxnRetry, 0, 0, 1,
		fmt.Sprintf("%s:1", boTxnLockName), stmtExecInfo1.MemMax, stmtExecInfo1.MemMax, stmtExecInfo1.DiskMax, stmtExecInfo1.DiskMax,
		0, 0, 0, 0, 0, 0, 0, 0, stmtExecInfo1.StmtCtx.AffectedRows(),
		f, f, 0, 0, 0, "", "", "", ""}
	expectedDatum[4] = stmtExecInfo2.Digest
	match(t, datums[0], expectedDatum...)
	match(t, datums[1], expectedEvictedDatum...)
}

// Test AddStatement and ToDatum parallel.
func TestAddStatementParallel(t *testing.T) {
	ssMap := newStmtSummaryByDigestMap()
	now := time.Now().Unix()
	// to disable expiration
	ssMap.beginTimeForCurInterval = now + 60

	threads := 8
	loops := 32
	wg := sync.WaitGroup{}
	wg.Add(threads)

	reader := newStmtSummaryReaderForTest(ssMap)
	addStmtFunc := func() {
		defer wg.Done()
		stmtExecInfo1 := generateAnyExecInfo()

		// Add 32 times with different digest.
		for i := 0; i < loops; i++ {
			stmtExecInfo1.Digest = fmt.Sprintf("digest%d", i)
			ssMap.AddStatement(stmtExecInfo1)
		}

		// There would be 32 summaries.
		datums := reader.GetStmtSummaryCurrentRows()
		require.Len(t, datums, loops)
	}

	for i := 0; i < threads; i++ {
		go addStmtFunc()
	}
	wg.Wait()

	datums := reader.GetStmtSummaryCurrentRows()
	require.Len(t, datums, loops)
}

// Test max number of statement count.
func TestMaxStmtCount(t *testing.T) {
	ssMap := newStmtSummaryByDigestMap()
	now := time.Now().Unix()
	// to disable expiration
	ssMap.beginTimeForCurInterval = now + 60

	// Test the original value and modify it.
	maxStmtCount := ssMap.maxStmtCount()
	require.Equal(t, 3000, maxStmtCount)
	require.Nil(t, ssMap.SetMaxStmtCount(10))
	require.Equal(t, 10, ssMap.maxStmtCount())
	defer func() {
		require.Nil(t, ssMap.SetMaxStmtCount(3000))
		require.Equal(t, 3000, maxStmtCount)
	}()

	// 100 digests
	stmtExecInfo1 := generateAnyExecInfo()
	loops := 100
	for i := 0; i < loops; i++ {
		stmtExecInfo1.Digest = fmt.Sprintf("digest%d", i)
		ssMap.AddStatement(stmtExecInfo1)
	}

	// Summary count should be MaxStmtCount.
	sm := ssMap.summaryMap
	require.Equal(t, 10, sm.Size())

	// LRU cache should work.
	for i := loops - 10; i < loops; i++ {
		key := &stmtSummaryByDigestKey{
			schemaName: stmtExecInfo1.SchemaName,
			digest:     fmt.Sprintf("digest%d", i),
			planDigest: stmtExecInfo1.PlanDigest,
		}
		_, ok := sm.Get(key)
		require.True(t, ok)
	}

	// Change to a bigger value.
	require.Nil(t, ssMap.SetMaxStmtCount(50))
	for i := 0; i < loops; i++ {
		stmtExecInfo1.Digest = fmt.Sprintf("digest%d", i)
		ssMap.AddStatement(stmtExecInfo1)
	}
	require.Equal(t, 50, sm.Size())

	// Change to a smaller value.
	require.Nil(t, ssMap.SetMaxStmtCount(10))
	for i := 0; i < loops; i++ {
		stmtExecInfo1.Digest = fmt.Sprintf("digest%d", i)
		ssMap.AddStatement(stmtExecInfo1)
	}
	require.Equal(t, 10, sm.Size())
}

// Test max length of normalized and sample SQL.
func TestMaxSQLLength(t *testing.T) {
	ssMap := newStmtSummaryByDigestMap()
	now := time.Now().Unix()
	// to disable expiration
	ssMap.beginTimeForCurInterval = now + 60

	// Test the original value and modify it.
	maxSQLLength := ssMap.maxSQLLength()
	require.Equal(t, 4096, maxSQLLength)

	// Create a long SQL
	length := maxSQLLength * 10
	str := strings.Repeat("a", length)

	stmtExecInfo1 := generateAnyExecInfo()
	stmtExecInfo1.OriginalSQL = str
	stmtExecInfo1.NormalizedSQL = str
	ssMap.AddStatement(stmtExecInfo1)

	key := &stmtSummaryByDigestKey{
		schemaName: stmtExecInfo1.SchemaName,
		digest:     stmtExecInfo1.Digest,
		planDigest: stmtExecInfo1.PlanDigest,
		prevDigest: stmtExecInfo1.PrevSQLDigest,
	}
	value, ok := ssMap.summaryMap.Get(key)
	require.True(t, ok)

	expectedSQL := fmt.Sprintf("%s(len:%d)", strings.Repeat("a", maxSQLLength), length)
	summary := value.(*stmtSummaryByDigest)
	require.Equal(t, expectedSQL, summary.normalizedSQL)
	ssElement := summary.history.Back().Value.(*stmtSummaryByDigestElement)
	require.Equal(t, expectedSQL, ssElement.sampleSQL)

	require.Nil(t, ssMap.SetMaxSQLLength(100))
	require.Equal(t, 100, ssMap.maxSQLLength())
	require.Nil(t, ssMap.SetMaxSQLLength(10))
	require.Equal(t, 10, ssMap.maxSQLLength())
	require.Nil(t, ssMap.SetMaxSQLLength(4096))
	require.Equal(t, 4096, ssMap.maxSQLLength())
}

// Test AddStatement and SetMaxStmtCount parallel.
func TestSetMaxStmtCountParallel(t *testing.T) {
	ssMap := newStmtSummaryByDigestMap()
	now := time.Now().Unix()
	// to disable expiration
	ssMap.beginTimeForCurInterval = now + 60

	threads := 8
	loops := 20
	wg := sync.WaitGroup{}
	wg.Add(threads + 1)

	addStmtFunc := func() {
		defer wg.Done()
		stmtExecInfo1 := generateAnyExecInfo()

		// Add 32 times with different digest.
		for i := 0; i < loops; i++ {
			stmtExecInfo1.Digest = fmt.Sprintf("digest%d", i)
			ssMap.AddStatement(stmtExecInfo1)
		}
	}
	for i := 0; i < threads; i++ {
		go addStmtFunc()
	}

	defer func() {
		require.Nil(t, ssMap.SetMaxStmtCount(3000))
	}()

	setStmtCountFunc := func() {
		defer wg.Done()
		// Turn down MaxStmtCount one by one.
		for i := 10; i > 0; i-- {
			require.Nil(t, ssMap.SetMaxStmtCount(uint(i)))
		}
	}
	go setStmtCountFunc()

	wg.Wait()

	// add stmt again to make sure evict occurs after SetMaxStmtCount.
	wg.Add(1)
	addStmtFunc()

	reader := newStmtSummaryReaderForTest(ssMap)
	datums := reader.GetStmtSummaryCurrentRows()
	// due to evictions happened in cache, an additional record will be appended to the table.
	require.Equal(t, 2, len(datums))
}

// Test setting EnableStmtSummary to 0.
func TestDisableStmtSummary(t *testing.T) {
	ssMap := newStmtSummaryByDigestMap()
	now := time.Now().Unix()

	err := ssMap.SetEnabled(false)
	require.NoError(t, err)
	ssMap.beginTimeForCurInterval = now + 60

	stmtExecInfo1 := generateAnyExecInfo()
	ssMap.AddStatement(stmtExecInfo1)
	reader := newStmtSummaryReaderForTest(ssMap)
	datums := reader.GetStmtSummaryCurrentRows()
	require.Len(t, datums, 0)

	err = ssMap.SetEnabled(true)
	require.NoError(t, err)

	ssMap.AddStatement(stmtExecInfo1)
	datums = reader.GetStmtSummaryCurrentRows()
	require.Equal(t, 1, len(datums))

	ssMap.beginTimeForCurInterval = now + 60

	stmtExecInfo2 := stmtExecInfo1
	stmtExecInfo2.OriginalSQL = "original_sql2"
	stmtExecInfo2.NormalizedSQL = "normalized_sql2"
	stmtExecInfo2.Digest = "digest2"
	ssMap.AddStatement(stmtExecInfo2)
	datums = reader.GetStmtSummaryCurrentRows()
	require.Equal(t, 2, len(datums))

	// Unset
	err = ssMap.SetEnabled(false)
	require.NoError(t, err)
	ssMap.beginTimeForCurInterval = now + 60
	ssMap.AddStatement(stmtExecInfo2)
	datums = reader.GetStmtSummaryCurrentRows()
	require.Len(t, datums, 0)

	// Unset
	err = ssMap.SetEnabled(false)
	require.NoError(t, err)

	err = ssMap.SetEnabled(true)
	require.NoError(t, err)

	ssMap.beginTimeForCurInterval = now + 60
	ssMap.AddStatement(stmtExecInfo1)
	datums = reader.GetStmtSummaryCurrentRows()
	require.Equal(t, 1, len(datums))

	// Set back.
	err = ssMap.SetEnabled(true)
	require.NoError(t, err)
}

// Test disable and enable statement summary concurrently with adding statements.
func TestEnableSummaryParallel(t *testing.T) {
	ssMap := newStmtSummaryByDigestMap()

	threads := 8
	loops := 32
	wg := sync.WaitGroup{}
	wg.Add(threads)

	reader := newStmtSummaryReaderForTest(ssMap)
	addStmtFunc := func() {
		defer wg.Done()
		stmtExecInfo1 := generateAnyExecInfo()

		// Add 32 times with same digest.
		for i := 0; i < loops; i++ {
			// Sometimes enable it and sometimes disable it.
			err := ssMap.SetEnabled(i%2 == 0)
			require.NoError(t, err)
			ssMap.AddStatement(stmtExecInfo1)
			// Try to read it.
			reader.GetStmtSummaryHistoryRows()
		}
		err := ssMap.SetEnabled(true)
		require.NoError(t, err)
	}

	for i := 0; i < threads; i++ {
		go addStmtFunc()
	}
	// Ensure that there's no deadlocks.
	wg.Wait()

	// Ensure that it's enabled at last.
	require.True(t, ssMap.Enabled())
}

// Test GetMoreThanCntBindableStmt.
func TestGetMoreThanCntBindableStmt(t *testing.T) {
	ssMap := newStmtSummaryByDigestMap()

	stmtExecInfo1 := generateAnyExecInfo()
	stmtExecInfo1.OriginalSQL = "insert 1"
	stmtExecInfo1.NormalizedSQL = "insert ?"
	stmtExecInfo1.StmtCtx.StmtType = "Insert"
	ssMap.AddStatement(stmtExecInfo1)
	stmts := ssMap.GetMoreThanCntBindableStmt(1)
	require.Equal(t, 0, len(stmts))

	stmtExecInfo1.NormalizedSQL = "select ?"
	stmtExecInfo1.Digest = "digest1"
	stmtExecInfo1.StmtCtx.StmtType = "Select"
	ssMap.AddStatement(stmtExecInfo1)
	stmts = ssMap.GetMoreThanCntBindableStmt(1)
	require.Equal(t, 0, len(stmts))

	ssMap.AddStatement(stmtExecInfo1)
	stmts = ssMap.GetMoreThanCntBindableStmt(1)
	require.Equal(t, 1, len(stmts))
}

// Test `formatBackoffTypes`.
func TestFormatBackoffTypes(t *testing.T) {
	backoffMap := make(map[string]int)
	require.Nil(t, formatBackoffTypes(backoffMap))
	bo1 := "pdrpc"
	backoffMap[bo1] = 1
	require.Equal(t, "pdrpc:1", formatBackoffTypes(backoffMap))
	bo2 := "txnlock"
	backoffMap[bo2] = 2

	require.Equal(t, "txnlock:2,pdrpc:1", formatBackoffTypes(backoffMap))
}

// Test refreshing current statement summary periodically.
func TestRefreshCurrentSummary(t *testing.T) {
	ssMap := newStmtSummaryByDigestMap()
	now := time.Now().Unix()

	ssMap.beginTimeForCurInterval = now + 10
	stmtExecInfo1 := generateAnyExecInfo()
	key := &stmtSummaryByDigestKey{
		schemaName: stmtExecInfo1.SchemaName,
		digest:     stmtExecInfo1.Digest,
		planDigest: stmtExecInfo1.PlanDigest,
	}
	ssMap.AddStatement(stmtExecInfo1)
	require.Equal(t, 1, ssMap.summaryMap.Size())
	value, ok := ssMap.summaryMap.Get(key)
	require.True(t, ok)
	ssElement := value.(*stmtSummaryByDigest).history.Back().Value.(*stmtSummaryByDigestElement)
	require.Equal(t, ssMap.beginTimeForCurInterval, ssElement.beginTime)
	require.Equal(t, int64(1), ssElement.execCount)

	ssMap.beginTimeForCurInterval = now - 1900
	ssElement.beginTime = now - 1900
	ssMap.AddStatement(stmtExecInfo1)
	require.Equal(t, 1, ssMap.summaryMap.Size())
	value, ok = ssMap.summaryMap.Get(key)
	require.True(t, ok)
	require.Equal(t, 2, value.(*stmtSummaryByDigest).history.Len())
	ssElement = value.(*stmtSummaryByDigest).history.Back().Value.(*stmtSummaryByDigestElement)
	require.Greater(t, ssElement.beginTime, now-1900)
	require.Equal(t, int64(1), ssElement.execCount)

	err := ssMap.SetRefreshInterval(10)
	require.NoError(t, err)
	ssMap.beginTimeForCurInterval = now - 20
	ssElement.beginTime = now - 20
	ssMap.AddStatement(stmtExecInfo1)
	require.Equal(t, 3, value.(*stmtSummaryByDigest).history.Len())
}

// Test expiring statement summary to history.
func TestSummaryHistory(t *testing.T) {
	ssMap := newStmtSummaryByDigestMap()
	now := time.Now().Unix()
	err := ssMap.SetRefreshInterval(10)
	require.NoError(t, err)
	err = ssMap.SetHistorySize(10)
	require.NoError(t, err)
	defer func() {
		err := ssMap.SetRefreshInterval(1800)
		require.NoError(t, err)
	}()
	defer func() {
		err := ssMap.SetHistorySize(24)
		require.NoError(t, err)
	}()

	stmtExecInfo1 := generateAnyExecInfo()
	key := &stmtSummaryByDigestKey{
		schemaName: stmtExecInfo1.SchemaName,
		digest:     stmtExecInfo1.Digest,
		planDigest: stmtExecInfo1.PlanDigest,
	}
	for i := 0; i < 11; i++ {
		ssMap.beginTimeForCurInterval = now + int64(i+1)*10
		ssMap.AddStatement(stmtExecInfo1)
		require.Equal(t, 1, ssMap.summaryMap.Size())
		value, ok := ssMap.summaryMap.Get(key)
		require.True(t, ok)
		ssbd := value.(*stmtSummaryByDigest)
		if i < 10 {
			require.Equal(t, i+1, ssbd.history.Len())
			ssElement := ssbd.history.Back().Value.(*stmtSummaryByDigestElement)
			require.Equal(t, ssMap.beginTimeForCurInterval, ssElement.beginTime)
			require.Equal(t, int64(1), ssElement.execCount)
		} else {
			require.Equal(t, 10, ssbd.history.Len())
			ssElement := ssbd.history.Back().Value.(*stmtSummaryByDigestElement)
			require.Equal(t, ssMap.beginTimeForCurInterval, ssElement.beginTime)
			ssElement = ssbd.history.Front().Value.(*stmtSummaryByDigestElement)
			require.Equal(t, now+20, ssElement.beginTime)
		}
	}
	reader := newStmtSummaryReaderForTest(ssMap)
	datum := reader.GetStmtSummaryHistoryRows()
	require.Equal(t, 10, len(datum))

	err = ssMap.SetHistorySize(5)
	require.NoError(t, err)
	datum = reader.GetStmtSummaryHistoryRows()
	require.Equal(t, 5, len(datum))

	// test eviction
	ssMap.Clear()
	err = ssMap.SetMaxStmtCount(1)
	require.NoError(t, err)
	defer func() {
		err := ssMap.SetMaxStmtCount(3000)
		require.NoError(t, err)
	}()
	// insert first digest
	for i := 0; i < 6; i++ {
		ssMap.beginTimeForCurInterval = now + int64(i)*10
		ssMap.AddStatement(stmtExecInfo1)
		require.Equal(t, 1, ssMap.summaryMap.Size())
		require.Equal(t, 0, ssMap.other.history.Len())
	}
	// insert another digest to evict it
	stmtExecInfo2 := stmtExecInfo1
	stmtExecInfo2.Digest = "bandit digest"
	ssMap.AddStatement(stmtExecInfo2)
	require.Equal(t, 1, ssMap.summaryMap.Size())
	// length of `other` should not longer than historySize.
	require.Equal(t, 5, ssMap.other.history.Len())
	datum = reader.GetStmtSummaryHistoryRows()
	// length of STATEMENT_SUMMARY_HISTORY == (history in cache) + (history evicted)
	require.Equal(t, 6, len(datum))
}

// Test summary when PrevSQL is not empty.
func TestPrevSQL(t *testing.T) {
	ssMap := newStmtSummaryByDigestMap()
	now := time.Now().Unix()
	// to disable expiration
	ssMap.beginTimeForCurInterval = now + 60

	stmtExecInfo1 := generateAnyExecInfo()
	stmtExecInfo1.PrevSQL = "prevSQL"
	stmtExecInfo1.PrevSQLDigest = "prevSQLDigest"
	ssMap.AddStatement(stmtExecInfo1)
	key := &stmtSummaryByDigestKey{
		schemaName: stmtExecInfo1.SchemaName,
		digest:     stmtExecInfo1.Digest,
		planDigest: stmtExecInfo1.PlanDigest,
		prevDigest: stmtExecInfo1.PrevSQLDigest,
	}
	require.Equal(t, 1, ssMap.summaryMap.Size())
	_, ok := ssMap.summaryMap.Get(key)
	require.True(t, ok)

	// same prevSQL
	ssMap.AddStatement(stmtExecInfo1)
	require.Equal(t, 1, ssMap.summaryMap.Size())

	// different prevSQL
	stmtExecInfo2 := stmtExecInfo1
	stmtExecInfo2.PrevSQL = "prevSQL1"
	stmtExecInfo2.PrevSQLDigest = "prevSQLDigest1"
	key.prevDigest = stmtExecInfo2.PrevSQLDigest
	ssMap.AddStatement(stmtExecInfo2)
	require.Equal(t, 2, ssMap.summaryMap.Size())
	_, ok = ssMap.summaryMap.Get(key)
	require.True(t, ok)
}

func TestEndTime(t *testing.T) {
	ssMap := newStmtSummaryByDigestMap()
	now := time.Now().Unix()
	ssMap.beginTimeForCurInterval = now - 100

	stmtExecInfo1 := generateAnyExecInfo()
	ssMap.AddStatement(stmtExecInfo1)
	key := &stmtSummaryByDigestKey{
		schemaName: stmtExecInfo1.SchemaName,
		digest:     stmtExecInfo1.Digest,
		planDigest: stmtExecInfo1.PlanDigest,
	}
	require.Equal(t, 1, ssMap.summaryMap.Size())
	value, ok := ssMap.summaryMap.Get(key)
	require.True(t, ok)
	ssbd := value.(*stmtSummaryByDigest)
	ssElement := ssbd.history.Back().Value.(*stmtSummaryByDigestElement)
	require.Equal(t, now-100, ssElement.beginTime)
	require.Equal(t, now+1700, ssElement.endTime)

	err := ssMap.SetRefreshInterval(3600)
	require.NoError(t, err)
	defer func() {
		err := ssMap.SetRefreshInterval(1800)
		require.NoError(t, err)
	}()
	ssMap.AddStatement(stmtExecInfo1)
	require.Equal(t, 1, ssbd.history.Len())
	ssElement = ssbd.history.Back().Value.(*stmtSummaryByDigestElement)
	require.Equal(t, now-100, ssElement.beginTime)
	require.Equal(t, now+3500, ssElement.endTime)

	err = ssMap.SetRefreshInterval(60)
	require.NoError(t, err)
	ssMap.AddStatement(stmtExecInfo1)
	require.Equal(t, 2, ssbd.history.Len())
	now2 := time.Now().Unix()
	ssElement = ssbd.history.Front().Value.(*stmtSummaryByDigestElement)
	require.Equal(t, now-100, ssElement.beginTime)
	require.GreaterOrEqual(t, ssElement.endTime, now)
	require.LessOrEqual(t, ssElement.endTime, now2)
	ssElement = ssbd.history.Back().Value.(*stmtSummaryByDigestElement)
	require.GreaterOrEqual(t, ssElement.beginTime, now-60)
	require.LessOrEqual(t, ssElement.beginTime, now2)
	require.Equal(t, int64(60), ssElement.endTime-ssElement.beginTime)
}

func TestPointGet(t *testing.T) {
	ssMap := newStmtSummaryByDigestMap()
	now := time.Now().Unix()
	ssMap.beginTimeForCurInterval = now - 100

	stmtExecInfo1 := generateAnyExecInfo()
	stmtExecInfo1.PlanDigest = ""
	stmtExecInfo1.PlanDigestGen = fakePlanDigestGenerator
	ssMap.AddStatement(stmtExecInfo1)
	key := &stmtSummaryByDigestKey{
		schemaName: stmtExecInfo1.SchemaName,
		digest:     stmtExecInfo1.Digest,
		planDigest: "",
	}
	require.Equal(t, 1, ssMap.summaryMap.Size())
	value, ok := ssMap.summaryMap.Get(key)
	require.True(t, ok)
	ssbd := value.(*stmtSummaryByDigest)
	ssElement := ssbd.history.Back().Value.(*stmtSummaryByDigestElement)
	require.Equal(t, int64(1), ssElement.execCount)

	ssMap.AddStatement(stmtExecInfo1)
	require.Equal(t, int64(2), ssElement.execCount)
}

func TestAccessPrivilege(t *testing.T) {
	ssMap := newStmtSummaryByDigestMap()

	loops := 32
	stmtExecInfo1 := generateAnyExecInfo()

	for i := 0; i < loops; i++ {
		stmtExecInfo1.Digest = fmt.Sprintf("digest%d", i)
		ssMap.AddStatement(stmtExecInfo1)
	}

	user := &auth.UserIdentity{Username: "user"}
	badUser := &auth.UserIdentity{Username: "bad_user"}

	reader := newStmtSummaryReaderForTest(ssMap)
	reader.user = user
	reader.hasProcessPriv = false
	datums := reader.GetStmtSummaryCurrentRows()
	require.Len(t, datums, loops)
	reader.user = badUser
	reader.hasProcessPriv = false
	datums = reader.GetStmtSummaryCurrentRows()
	require.Len(t, datums, 0)
	reader.hasProcessPriv = true
	datums = reader.GetStmtSummaryCurrentRows()
	require.Len(t, datums, loops)

	reader.user = user
	reader.hasProcessPriv = false
	datums = reader.GetStmtSummaryHistoryRows()
	require.Len(t, datums, loops)
	reader.user = badUser
	reader.hasProcessPriv = false
	datums = reader.GetStmtSummaryHistoryRows()
	require.Len(t, datums, 0)
	reader.hasProcessPriv = true
	datums = reader.GetStmtSummaryHistoryRows()
	require.Len(t, datums, loops)
}
