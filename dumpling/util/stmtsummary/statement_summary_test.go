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
// See the License for the specific language governing permissions and
// limitations under the License.

package stmtsummary

import (
	"container/list"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/execdetails"
)

var _ = Suite(&testStmtSummarySuite{})

type testStmtSummarySuite struct {
	ssMap *stmtSummaryByDigestMap
}

func emptyPlanGenerator() string {
	return ""
}

func fakePlanDigestGenerator() string {
	return "point_get"
}

func (s *testStmtSummarySuite) SetUpSuite(c *C) {
	s.ssMap = newStmtSummaryByDigestMap()
	s.ssMap.SetEnabled("1", false)
	s.ssMap.SetRefreshInterval("1800", false)
	s.ssMap.SetHistorySize("24", false)
}

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

// Test stmtSummaryByDigest.AddStatement.
func (s *testStmtSummarySuite) TestAddStatement(c *C) {
	s.ssMap.Clear()
	now := time.Now().Unix()
	s.ssMap.beginTimeForCurInterval = now + 60

	tables := []stmtctx.TableEntry{{DB: "db1", Table: "tb1"}, {DB: "db2", Table: "tb2"}}
	indexes := []string{"a", "b"}

	// first statement
	stmtExecInfo1 := generateAnyExecInfo()
	stmtExecInfo1.ExecDetail.CommitDetail.Mu.BackoffTypes = make([]fmt.Stringer, 0)
	key := &stmtSummaryByDigestKey{
		schemaName: stmtExecInfo1.SchemaName,
		digest:     stmtExecInfo1.Digest,
		planDigest: stmtExecInfo1.PlanDigest,
	}
	expectedSummaryElement := stmtSummaryByDigestElement{
		beginTime:            now + 60,
		endTime:              now + 1860,
		sampleSQL:            stmtExecInfo1.OriginalSQL,
		samplePlan:           stmtExecInfo1.PlanGenerator(),
		indexNames:           stmtExecInfo1.StmtCtx.IndexNames,
		execCount:            1,
		sumLatency:           stmtExecInfo1.TotalLatency,
		maxLatency:           stmtExecInfo1.TotalLatency,
		minLatency:           stmtExecInfo1.TotalLatency,
		sumParseLatency:      stmtExecInfo1.ParseLatency,
		maxParseLatency:      stmtExecInfo1.ParseLatency,
		sumCompileLatency:    stmtExecInfo1.CompileLatency,
		maxCompileLatency:    stmtExecInfo1.CompileLatency,
		numCopTasks:          int64(stmtExecInfo1.CopTasks.NumCopTasks),
		sumCopProcessTime:    int64(stmtExecInfo1.CopTasks.AvgProcessTime) * int64(stmtExecInfo1.CopTasks.NumCopTasks),
		maxCopProcessTime:    stmtExecInfo1.CopTasks.MaxProcessTime,
		maxCopProcessAddress: stmtExecInfo1.CopTasks.MaxProcessAddress,
		sumCopWaitTime:       int64(stmtExecInfo1.CopTasks.AvgWaitTime) * int64(stmtExecInfo1.CopTasks.NumCopTasks),
		maxCopWaitTime:       stmtExecInfo1.CopTasks.MaxWaitTime,
		maxCopWaitAddress:    stmtExecInfo1.CopTasks.MaxWaitAddress,
		sumProcessTime:       stmtExecInfo1.ExecDetail.ProcessTime,
		maxProcessTime:       stmtExecInfo1.ExecDetail.ProcessTime,
		sumWaitTime:          stmtExecInfo1.ExecDetail.WaitTime,
		maxWaitTime:          stmtExecInfo1.ExecDetail.WaitTime,
		sumBackoffTime:       stmtExecInfo1.ExecDetail.BackoffTime,
		maxBackoffTime:       stmtExecInfo1.ExecDetail.BackoffTime,
		sumTotalKeys:         stmtExecInfo1.ExecDetail.TotalKeys,
		maxTotalKeys:         stmtExecInfo1.ExecDetail.TotalKeys,
		sumProcessedKeys:     stmtExecInfo1.ExecDetail.ProcessedKeys,
		maxProcessedKeys:     stmtExecInfo1.ExecDetail.ProcessedKeys,
		sumGetCommitTsTime:   stmtExecInfo1.ExecDetail.CommitDetail.GetCommitTsTime,
		maxGetCommitTsTime:   stmtExecInfo1.ExecDetail.CommitDetail.GetCommitTsTime,
		sumPrewriteTime:      stmtExecInfo1.ExecDetail.CommitDetail.PrewriteTime,
		maxPrewriteTime:      stmtExecInfo1.ExecDetail.CommitDetail.PrewriteTime,
		sumCommitTime:        stmtExecInfo1.ExecDetail.CommitDetail.CommitTime,
		maxCommitTime:        stmtExecInfo1.ExecDetail.CommitDetail.CommitTime,
		sumLocalLatchTime:    stmtExecInfo1.ExecDetail.CommitDetail.LocalLatchTime,
		maxLocalLatchTime:    stmtExecInfo1.ExecDetail.CommitDetail.LocalLatchTime,
		sumCommitBackoffTime: stmtExecInfo1.ExecDetail.CommitDetail.CommitBackoffTime,
		maxCommitBackoffTime: stmtExecInfo1.ExecDetail.CommitDetail.CommitBackoffTime,
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
		backoffTypes:         make(map[fmt.Stringer]int),
		sumMem:               stmtExecInfo1.MemMax,
		maxMem:               stmtExecInfo1.MemMax,
		sumAffectedRows:      stmtExecInfo1.StmtCtx.AffectedRows(),
		firstSeen:            stmtExecInfo1.StartTime,
		lastSeen:             stmtExecInfo1.StartTime,
	}
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
	s.ssMap.AddStatement(stmtExecInfo1)
	summary, ok := s.ssMap.summaryMap.Get(key)
	c.Assert(ok, IsTrue)
	c.Assert(matchStmtSummaryByDigest(summary.(*stmtSummaryByDigest), &expectedSummary), IsTrue)

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
			ProcessTime:   1500,
			WaitTime:      150,
			BackoffTime:   180,
			RequestCount:  20,
			TotalKeys:     6000,
			ProcessedKeys: 1500,
			CommitDetail: &execdetails.CommitDetails{
				GetCommitTsTime:   500,
				PrewriteTime:      50000,
				CommitTime:        5000,
				LocalLatchTime:    50,
				CommitBackoffTime: 1000,
				Mu: struct {
					sync.Mutex
					BackoffTypes []fmt.Stringer
				}{
					BackoffTypes: []fmt.Stringer{tikv.BoTxnLock},
				},
				ResolveLockTime:   10000,
				WriteKeys:         100000,
				WriteSize:         1000000,
				PrewriteRegionNum: 100,
				TxnRetry:          10,
			},
		},
		StmtCtx: &stmtctx.StatementContext{
			StmtType:   "Select",
			Tables:     tables,
			IndexNames: indexes,
		},
		MemMax:    20000,
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
	expectedSummaryElement.numCopTasks += int64(stmtExecInfo2.CopTasks.NumCopTasks)
	expectedSummaryElement.sumCopProcessTime += int64(stmtExecInfo2.CopTasks.AvgProcessTime) * int64(stmtExecInfo2.CopTasks.NumCopTasks)
	expectedSummaryElement.maxCopProcessTime = stmtExecInfo2.CopTasks.MaxProcessTime
	expectedSummaryElement.maxCopProcessAddress = stmtExecInfo2.CopTasks.MaxProcessAddress
	expectedSummaryElement.sumCopWaitTime += int64(stmtExecInfo2.CopTasks.AvgWaitTime) * int64(stmtExecInfo2.CopTasks.NumCopTasks)
	expectedSummaryElement.maxCopWaitTime = stmtExecInfo2.CopTasks.MaxWaitTime
	expectedSummaryElement.maxCopWaitAddress = stmtExecInfo2.CopTasks.MaxWaitAddress
	expectedSummaryElement.sumProcessTime += stmtExecInfo2.ExecDetail.ProcessTime
	expectedSummaryElement.maxProcessTime = stmtExecInfo2.ExecDetail.ProcessTime
	expectedSummaryElement.sumWaitTime += stmtExecInfo2.ExecDetail.WaitTime
	expectedSummaryElement.maxWaitTime = stmtExecInfo2.ExecDetail.WaitTime
	expectedSummaryElement.sumBackoffTime += stmtExecInfo2.ExecDetail.BackoffTime
	expectedSummaryElement.maxBackoffTime = stmtExecInfo2.ExecDetail.BackoffTime
	expectedSummaryElement.sumTotalKeys += stmtExecInfo2.ExecDetail.TotalKeys
	expectedSummaryElement.maxTotalKeys = stmtExecInfo2.ExecDetail.TotalKeys
	expectedSummaryElement.sumProcessedKeys += stmtExecInfo2.ExecDetail.ProcessedKeys
	expectedSummaryElement.maxProcessedKeys = stmtExecInfo2.ExecDetail.ProcessedKeys
	expectedSummaryElement.sumGetCommitTsTime += stmtExecInfo2.ExecDetail.CommitDetail.GetCommitTsTime
	expectedSummaryElement.maxGetCommitTsTime = stmtExecInfo2.ExecDetail.CommitDetail.GetCommitTsTime
	expectedSummaryElement.sumPrewriteTime += stmtExecInfo2.ExecDetail.CommitDetail.PrewriteTime
	expectedSummaryElement.maxPrewriteTime = stmtExecInfo2.ExecDetail.CommitDetail.PrewriteTime
	expectedSummaryElement.sumCommitTime += stmtExecInfo2.ExecDetail.CommitDetail.CommitTime
	expectedSummaryElement.maxCommitTime = stmtExecInfo2.ExecDetail.CommitDetail.CommitTime
	expectedSummaryElement.sumLocalLatchTime += stmtExecInfo2.ExecDetail.CommitDetail.LocalLatchTime
	expectedSummaryElement.maxLocalLatchTime = stmtExecInfo2.ExecDetail.CommitDetail.LocalLatchTime
	expectedSummaryElement.sumCommitBackoffTime += stmtExecInfo2.ExecDetail.CommitDetail.CommitBackoffTime
	expectedSummaryElement.maxCommitBackoffTime = stmtExecInfo2.ExecDetail.CommitDetail.CommitBackoffTime
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
	expectedSummaryElement.backoffTypes[tikv.BoTxnLock] = 1
	expectedSummaryElement.sumMem += stmtExecInfo2.MemMax
	expectedSummaryElement.maxMem = stmtExecInfo2.MemMax
	expectedSummaryElement.sumAffectedRows += stmtExecInfo2.StmtCtx.AffectedRows()
	expectedSummaryElement.lastSeen = stmtExecInfo2.StartTime

	s.ssMap.AddStatement(stmtExecInfo2)
	summary, ok = s.ssMap.summaryMap.Get(key)
	c.Assert(ok, IsTrue)
	c.Assert(matchStmtSummaryByDigest(summary.(*stmtSummaryByDigest), &expectedSummary), IsTrue)

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
			ProcessTime:   150,
			WaitTime:      15,
			BackoffTime:   18,
			RequestCount:  2,
			TotalKeys:     600,
			ProcessedKeys: 150,
			CommitDetail: &execdetails.CommitDetails{
				GetCommitTsTime:   50,
				PrewriteTime:      5000,
				CommitTime:        500,
				LocalLatchTime:    5,
				CommitBackoffTime: 100,
				Mu: struct {
					sync.Mutex
					BackoffTypes []fmt.Stringer
				}{
					BackoffTypes: []fmt.Stringer{tikv.BoTxnLock},
				},
				ResolveLockTime:   1000,
				WriteKeys:         10000,
				WriteSize:         100000,
				PrewriteRegionNum: 10,
				TxnRetry:          1,
			},
		},
		StmtCtx: &stmtctx.StatementContext{
			StmtType:   "Select",
			Tables:     tables,
			IndexNames: indexes,
		},
		MemMax:    200,
		StartTime: time.Date(2019, 1, 1, 10, 10, 0, 10, time.UTC),
		Succeed:   true,
	}
	stmtExecInfo3.StmtCtx.AddAffectedRows(20000)
	expectedSummaryElement.execCount++
	expectedSummaryElement.sumLatency += stmtExecInfo3.TotalLatency
	expectedSummaryElement.minLatency = stmtExecInfo3.TotalLatency
	expectedSummaryElement.sumParseLatency += stmtExecInfo3.ParseLatency
	expectedSummaryElement.sumCompileLatency += stmtExecInfo3.CompileLatency
	expectedSummaryElement.numCopTasks += int64(stmtExecInfo3.CopTasks.NumCopTasks)
	expectedSummaryElement.sumCopProcessTime += int64(stmtExecInfo3.CopTasks.AvgProcessTime) * int64(stmtExecInfo3.CopTasks.NumCopTasks)
	expectedSummaryElement.sumCopWaitTime += int64(stmtExecInfo3.CopTasks.AvgWaitTime) * int64(stmtExecInfo3.CopTasks.NumCopTasks)
	expectedSummaryElement.sumProcessTime += stmtExecInfo3.ExecDetail.ProcessTime
	expectedSummaryElement.sumWaitTime += stmtExecInfo3.ExecDetail.WaitTime
	expectedSummaryElement.sumBackoffTime += stmtExecInfo3.ExecDetail.BackoffTime
	expectedSummaryElement.sumTotalKeys += stmtExecInfo3.ExecDetail.TotalKeys
	expectedSummaryElement.sumProcessedKeys += stmtExecInfo3.ExecDetail.ProcessedKeys
	expectedSummaryElement.sumGetCommitTsTime += stmtExecInfo3.ExecDetail.CommitDetail.GetCommitTsTime
	expectedSummaryElement.sumPrewriteTime += stmtExecInfo3.ExecDetail.CommitDetail.PrewriteTime
	expectedSummaryElement.sumCommitTime += stmtExecInfo3.ExecDetail.CommitDetail.CommitTime
	expectedSummaryElement.sumLocalLatchTime += stmtExecInfo3.ExecDetail.CommitDetail.LocalLatchTime
	expectedSummaryElement.sumCommitBackoffTime += stmtExecInfo3.ExecDetail.CommitDetail.CommitBackoffTime
	expectedSummaryElement.sumResolveLockTime += stmtExecInfo3.ExecDetail.CommitDetail.ResolveLockTime
	expectedSummaryElement.sumWriteKeys += int64(stmtExecInfo3.ExecDetail.CommitDetail.WriteKeys)
	expectedSummaryElement.sumWriteSize += int64(stmtExecInfo3.ExecDetail.CommitDetail.WriteSize)
	expectedSummaryElement.sumPrewriteRegionNum += int64(stmtExecInfo3.ExecDetail.CommitDetail.PrewriteRegionNum)
	expectedSummaryElement.sumTxnRetry += int64(stmtExecInfo3.ExecDetail.CommitDetail.TxnRetry)
	expectedSummaryElement.sumBackoffTimes += 1
	expectedSummaryElement.backoffTypes[tikv.BoTxnLock] = 2
	expectedSummaryElement.sumMem += stmtExecInfo3.MemMax
	expectedSummaryElement.sumAffectedRows += stmtExecInfo3.StmtCtx.AffectedRows()
	expectedSummaryElement.firstSeen = stmtExecInfo3.StartTime

	s.ssMap.AddStatement(stmtExecInfo3)
	summary, ok = s.ssMap.summaryMap.Get(key)
	c.Assert(ok, IsTrue)
	c.Assert(matchStmtSummaryByDigest(summary.(*stmtSummaryByDigest), &expectedSummary), IsTrue)

	// Fourth statement is in a different schema.
	stmtExecInfo4 := stmtExecInfo1
	stmtExecInfo4.SchemaName = "schema2"
	stmtExecInfo4.ExecDetail.CommitDetail = nil
	key = &stmtSummaryByDigestKey{
		schemaName: stmtExecInfo4.SchemaName,
		digest:     stmtExecInfo4.Digest,
		planDigest: stmtExecInfo4.PlanDigest,
	}
	s.ssMap.AddStatement(stmtExecInfo4)
	c.Assert(s.ssMap.summaryMap.Size(), Equals, 2)
	_, ok = s.ssMap.summaryMap.Get(key)
	c.Assert(ok, IsTrue)

	// Fifth statement has a different digest.
	stmtExecInfo5 := stmtExecInfo1
	stmtExecInfo5.Digest = "digest2"
	key = &stmtSummaryByDigestKey{
		schemaName: stmtExecInfo5.SchemaName,
		digest:     stmtExecInfo5.Digest,
		planDigest: stmtExecInfo4.PlanDigest,
	}
	s.ssMap.AddStatement(stmtExecInfo5)
	c.Assert(s.ssMap.summaryMap.Size(), Equals, 3)
	_, ok = s.ssMap.summaryMap.Get(key)
	c.Assert(ok, IsTrue)

	// Sixth statement has a different plan digest.
	stmtExecInfo6 := stmtExecInfo1
	stmtExecInfo6.PlanDigest = "plan_digest2"
	key = &stmtSummaryByDigestKey{
		schemaName: stmtExecInfo6.SchemaName,
		digest:     stmtExecInfo6.Digest,
		planDigest: stmtExecInfo6.PlanDigest,
	}
	s.ssMap.AddStatement(stmtExecInfo6)
	c.Assert(s.ssMap.summaryMap.Size(), Equals, 4)
	_, ok = s.ssMap.summaryMap.Get(key)
	c.Assert(ok, IsTrue)
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
			ssElement1.numCopTasks != ssElement2.numCopTasks ||
			ssElement1.sumCopProcessTime != ssElement2.sumCopProcessTime ||
			ssElement1.maxCopProcessTime != ssElement2.maxCopProcessTime ||
			ssElement1.maxCopProcessAddress != ssElement2.maxCopProcessAddress ||
			ssElement1.sumCopWaitTime != ssElement2.sumCopWaitTime ||
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

func match(c *C, row []types.Datum, expected ...interface{}) {
	c.Assert(len(row), Equals, len(expected))
	for i := range row {
		got := fmt.Sprintf("%v", row[i].GetValue())
		need := fmt.Sprintf("%v", expected[i])
		c.Assert(got, Equals, need)
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
			ProcessTime:   500,
			WaitTime:      50,
			BackoffTime:   80,
			RequestCount:  10,
			TotalKeys:     1000,
			ProcessedKeys: 500,
			CommitDetail: &execdetails.CommitDetails{
				GetCommitTsTime:   100,
				PrewriteTime:      10000,
				CommitTime:        1000,
				LocalLatchTime:    10,
				CommitBackoffTime: 200,
				Mu: struct {
					sync.Mutex
					BackoffTypes []fmt.Stringer
				}{
					BackoffTypes: []fmt.Stringer{tikv.BoTxnLock},
				},
				ResolveLockTime:   2000,
				WriteKeys:         20000,
				WriteSize:         200000,
				PrewriteRegionNum: 20,
				TxnRetry:          2,
			},
		},
		StmtCtx: &stmtctx.StatementContext{
			StmtType:   "Select",
			Tables:     tables,
			IndexNames: indexes,
		},
		MemMax:    10000,
		StartTime: time.Date(2019, 1, 1, 10, 10, 10, 10, time.UTC),
		Succeed:   true,
	}
	stmtExecInfo.StmtCtx.AddAffectedRows(10000)
	return stmtExecInfo
}

// Test stmtSummaryByDigest.ToDatum.
func (s *testStmtSummarySuite) TestToDatum(c *C) {
	s.ssMap.Clear()
	now := time.Now().Unix()
	// to disable expiration
	s.ssMap.beginTimeForCurInterval = now + 60

	stmtExecInfo1 := generateAnyExecInfo()
	s.ssMap.AddStatement(stmtExecInfo1)
	datums := s.ssMap.ToCurrentDatum(nil, true)
	c.Assert(len(datums), Equals, 1)
	n := types.NewTime(types.FromGoTime(time.Unix(s.ssMap.beginTimeForCurInterval, 0)), mysql.TypeTimestamp, types.DefaultFsp)
	e := types.NewTime(types.FromGoTime(time.Unix(s.ssMap.beginTimeForCurInterval+1800, 0)), mysql.TypeTimestamp, types.DefaultFsp)
	t := types.NewTime(types.FromGoTime(stmtExecInfo1.StartTime), mysql.TypeTimestamp, types.DefaultFsp)
	expectedDatum := []interface{}{n, e, "select", stmtExecInfo1.SchemaName, stmtExecInfo1.Digest, stmtExecInfo1.NormalizedSQL,
		"db1.tb1,db2.tb2", "a", stmtExecInfo1.User, 1, 0, 0, int64(stmtExecInfo1.TotalLatency),
		int64(stmtExecInfo1.TotalLatency), int64(stmtExecInfo1.TotalLatency), int64(stmtExecInfo1.TotalLatency),
		int64(stmtExecInfo1.ParseLatency), int64(stmtExecInfo1.ParseLatency), int64(stmtExecInfo1.CompileLatency),
		int64(stmtExecInfo1.CompileLatency), stmtExecInfo1.CopTasks.NumCopTasks, int64(stmtExecInfo1.CopTasks.AvgProcessTime),
		int64(stmtExecInfo1.CopTasks.MaxProcessTime), stmtExecInfo1.CopTasks.MaxProcessAddress,
		int64(stmtExecInfo1.CopTasks.AvgWaitTime), int64(stmtExecInfo1.CopTasks.MaxWaitTime),
		stmtExecInfo1.CopTasks.MaxWaitAddress, int64(stmtExecInfo1.ExecDetail.ProcessTime), int64(stmtExecInfo1.ExecDetail.ProcessTime),
		int64(stmtExecInfo1.ExecDetail.WaitTime), int64(stmtExecInfo1.ExecDetail.WaitTime), int64(stmtExecInfo1.ExecDetail.BackoffTime),
		int64(stmtExecInfo1.ExecDetail.BackoffTime), stmtExecInfo1.ExecDetail.TotalKeys, stmtExecInfo1.ExecDetail.TotalKeys,
		stmtExecInfo1.ExecDetail.ProcessedKeys, stmtExecInfo1.ExecDetail.ProcessedKeys,
		int64(stmtExecInfo1.ExecDetail.CommitDetail.PrewriteTime), int64(stmtExecInfo1.ExecDetail.CommitDetail.PrewriteTime),
		int64(stmtExecInfo1.ExecDetail.CommitDetail.CommitTime), int64(stmtExecInfo1.ExecDetail.CommitDetail.CommitTime),
		int64(stmtExecInfo1.ExecDetail.CommitDetail.GetCommitTsTime), int64(stmtExecInfo1.ExecDetail.CommitDetail.GetCommitTsTime),
		stmtExecInfo1.ExecDetail.CommitDetail.CommitBackoffTime, stmtExecInfo1.ExecDetail.CommitDetail.CommitBackoffTime,
		stmtExecInfo1.ExecDetail.CommitDetail.ResolveLockTime, stmtExecInfo1.ExecDetail.CommitDetail.ResolveLockTime,
		int64(stmtExecInfo1.ExecDetail.CommitDetail.LocalLatchTime), int64(stmtExecInfo1.ExecDetail.CommitDetail.LocalLatchTime),
		stmtExecInfo1.ExecDetail.CommitDetail.WriteKeys, stmtExecInfo1.ExecDetail.CommitDetail.WriteKeys,
		stmtExecInfo1.ExecDetail.CommitDetail.WriteSize, stmtExecInfo1.ExecDetail.CommitDetail.WriteSize,
		stmtExecInfo1.ExecDetail.CommitDetail.PrewriteRegionNum, stmtExecInfo1.ExecDetail.CommitDetail.PrewriteRegionNum,
		stmtExecInfo1.ExecDetail.CommitDetail.TxnRetry, stmtExecInfo1.ExecDetail.CommitDetail.TxnRetry, 1,
		"txnLock:1", stmtExecInfo1.MemMax, stmtExecInfo1.MemMax, stmtExecInfo1.StmtCtx.AffectedRows(),
		t, t, stmtExecInfo1.OriginalSQL, stmtExecInfo1.PrevSQL, "plan_digest", ""}
	match(c, datums[0], expectedDatum...)

	datums = s.ssMap.ToHistoryDatum(nil, true)
	c.Assert(len(datums), Equals, 1)
	match(c, datums[0], expectedDatum...)
}

// Test AddStatement and ToDatum parallel.
func (s *testStmtSummarySuite) TestAddStatementParallel(c *C) {
	s.ssMap.Clear()
	now := time.Now().Unix()
	// to disable expiration
	s.ssMap.beginTimeForCurInterval = now + 60

	threads := 8
	loops := 32
	wg := sync.WaitGroup{}
	wg.Add(threads)

	addStmtFunc := func() {
		defer wg.Done()
		stmtExecInfo1 := generateAnyExecInfo()

		// Add 32 times with different digest.
		for i := 0; i < loops; i++ {
			stmtExecInfo1.Digest = fmt.Sprintf("digest%d", i)
			s.ssMap.AddStatement(stmtExecInfo1)
		}

		// There would be 32 summaries.
		datums := s.ssMap.ToCurrentDatum(nil, true)
		c.Assert(len(datums), Equals, loops)
	}

	for i := 0; i < threads; i++ {
		go addStmtFunc()
	}
	wg.Wait()

	datums := s.ssMap.ToCurrentDatum(nil, true)
	c.Assert(len(datums), Equals, loops)
}

// Test max number of statement count.
func (s *testStmtSummarySuite) TestMaxStmtCount(c *C) {
	s.ssMap.Clear()
	now := time.Now().Unix()
	// to disable expiration
	s.ssMap.beginTimeForCurInterval = now + 60

	// Test the original value and modify it.
	maxStmtCount := s.ssMap.maxStmtCount()
	c.Assert(maxStmtCount, Equals, int(config.GetGlobalConfig().StmtSummary.MaxStmtCount))
	c.Assert(s.ssMap.SetMaxStmtCount("10", false), IsNil)
	c.Assert(s.ssMap.maxStmtCount(), Equals, 10)
	defer func() {
		c.Assert(s.ssMap.SetMaxStmtCount("", false), IsNil)
		c.Assert(s.ssMap.SetMaxStmtCount("", true), IsNil)
		c.Assert(maxStmtCount, Equals, int(config.GetGlobalConfig().StmtSummary.MaxStmtCount))
	}()

	// 100 digests
	stmtExecInfo1 := generateAnyExecInfo()
	loops := 100
	for i := 0; i < loops; i++ {
		stmtExecInfo1.Digest = fmt.Sprintf("digest%d", i)
		s.ssMap.AddStatement(stmtExecInfo1)
	}

	// Summary count should be MaxStmtCount.
	sm := s.ssMap.summaryMap
	c.Assert(sm.Size(), Equals, 10)

	// LRU cache should work.
	for i := loops - 10; i < loops; i++ {
		key := &stmtSummaryByDigestKey{
			schemaName: stmtExecInfo1.SchemaName,
			digest:     fmt.Sprintf("digest%d", i),
			planDigest: stmtExecInfo1.PlanDigest,
		}
		_, ok := sm.Get(key)
		c.Assert(ok, IsTrue)
	}

	// Change to a bigger value.
	c.Assert(s.ssMap.SetMaxStmtCount("50", true), IsNil)
	for i := 0; i < loops; i++ {
		stmtExecInfo1.Digest = fmt.Sprintf("digest%d", i)
		s.ssMap.AddStatement(stmtExecInfo1)
	}
	c.Assert(sm.Size(), Equals, 50)

	// Change to a smaller value.
	c.Assert(s.ssMap.SetMaxStmtCount("10", true), IsNil)
	for i := 0; i < loops; i++ {
		stmtExecInfo1.Digest = fmt.Sprintf("digest%d", i)
		s.ssMap.AddStatement(stmtExecInfo1)
	}
	c.Assert(sm.Size(), Equals, 10)
}

// Test max length of normalized and sample SQL.
func (s *testStmtSummarySuite) TestMaxSQLLength(c *C) {
	s.ssMap.Clear()
	now := time.Now().Unix()
	// to disable expiration
	s.ssMap.beginTimeForCurInterval = now + 60

	// Test the original value and modify it.
	maxSQLLength := s.ssMap.maxSQLLength()
	c.Assert(maxSQLLength, Equals, int(config.GetGlobalConfig().StmtSummary.MaxSQLLength))

	// Create a long SQL
	length := maxSQLLength * 10
	str := strings.Repeat("a", length)

	stmtExecInfo1 := generateAnyExecInfo()
	stmtExecInfo1.OriginalSQL = str
	stmtExecInfo1.NormalizedSQL = str
	s.ssMap.AddStatement(stmtExecInfo1)

	key := &stmtSummaryByDigestKey{
		schemaName: stmtExecInfo1.SchemaName,
		digest:     stmtExecInfo1.Digest,
		planDigest: stmtExecInfo1.PlanDigest,
		prevDigest: stmtExecInfo1.PrevSQLDigest,
	}
	value, ok := s.ssMap.summaryMap.Get(key)
	c.Assert(ok, IsTrue)

	expectedSQL := fmt.Sprintf("%s(len:%d)", strings.Repeat("a", maxSQLLength), length)
	summary := value.(*stmtSummaryByDigest)
	c.Assert(summary.normalizedSQL, Equals, expectedSQL)
	ssElement := summary.history.Back().Value.(*stmtSummaryByDigestElement)
	c.Assert(ssElement.sampleSQL, Equals, expectedSQL)

	c.Assert(s.ssMap.SetMaxSQLLength("100", false), IsNil)
	c.Assert(s.ssMap.maxSQLLength(), Equals, 100)
	c.Assert(s.ssMap.SetMaxSQLLength("10", true), IsNil)
	c.Assert(s.ssMap.maxSQLLength(), Equals, 10)
	c.Assert(s.ssMap.SetMaxSQLLength("", true), IsNil)
	c.Assert(s.ssMap.maxSQLLength(), Equals, 100)
}

// Test AddStatement and SetMaxStmtCount parallel.
func (s *testStmtSummarySuite) TestSetMaxStmtCountParallel(c *C) {
	s.ssMap.Clear()
	now := time.Now().Unix()
	// to disable expiration
	s.ssMap.beginTimeForCurInterval = now + 60

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
			s.ssMap.AddStatement(stmtExecInfo1)
		}
	}
	for i := 0; i < threads; i++ {
		go addStmtFunc()
	}

	defer c.Assert(s.ssMap.SetMaxStmtCount("", true), IsNil)
	setStmtCountFunc := func() {
		defer wg.Done()
		// Turn down MaxStmtCount one by one.
		for i := 10; i > 0; i-- {
			c.Assert(s.ssMap.SetMaxStmtCount(strconv.Itoa(i), true), IsNil)
		}
	}
	go setStmtCountFunc()

	wg.Wait()

	datums := s.ssMap.ToCurrentDatum(nil, true)
	c.Assert(len(datums), Equals, 1)
}

// Test setting EnableStmtSummary to 0.
func (s *testStmtSummarySuite) TestDisableStmtSummary(c *C) {
	s.ssMap.Clear()
	now := time.Now().Unix()

	// Set false in global scope, it should work.
	s.ssMap.SetEnabled("0", false)
	s.ssMap.beginTimeForCurInterval = now + 60

	stmtExecInfo1 := generateAnyExecInfo()
	s.ssMap.AddStatement(stmtExecInfo1)
	datums := s.ssMap.ToCurrentDatum(nil, true)
	c.Assert(len(datums), Equals, 0)

	// Set true in session scope, it will overwrite global scope.
	s.ssMap.SetEnabled("1", true)

	s.ssMap.AddStatement(stmtExecInfo1)
	datums = s.ssMap.ToCurrentDatum(nil, true)
	c.Assert(len(datums), Equals, 1)

	// Set false in global scope, it shouldn't work.
	s.ssMap.SetEnabled("0", false)
	s.ssMap.beginTimeForCurInterval = now + 60

	stmtExecInfo2 := stmtExecInfo1
	stmtExecInfo2.OriginalSQL = "original_sql2"
	stmtExecInfo2.NormalizedSQL = "normalized_sql2"
	stmtExecInfo2.Digest = "digest2"
	s.ssMap.AddStatement(stmtExecInfo2)
	datums = s.ssMap.ToCurrentDatum(nil, true)
	c.Assert(len(datums), Equals, 2)

	// Unset in session scope.
	s.ssMap.SetEnabled("", true)
	s.ssMap.beginTimeForCurInterval = now + 60
	s.ssMap.AddStatement(stmtExecInfo2)
	datums = s.ssMap.ToCurrentDatum(nil, true)
	c.Assert(len(datums), Equals, 0)

	// Unset in global scope.
	s.ssMap.SetEnabled("", false)
	s.ssMap.beginTimeForCurInterval = now + 60
	s.ssMap.AddStatement(stmtExecInfo1)
	datums = s.ssMap.ToCurrentDatum(nil, true)
	c.Assert(len(datums), Equals, 1)

	// Set back.
	s.ssMap.SetEnabled("1", false)
}

// Test disable and enable statement summary concurrently with adding statements.
func (s *testStmtSummarySuite) TestEnableSummaryParallel(c *C) {
	s.ssMap.Clear()

	threads := 8
	loops := 32
	wg := sync.WaitGroup{}
	wg.Add(threads)

	addStmtFunc := func() {
		defer wg.Done()
		stmtExecInfo1 := generateAnyExecInfo()

		// Add 32 times with same digest.
		for i := 0; i < loops; i++ {
			// Sometimes enable it and sometimes disable it.
			s.ssMap.SetEnabled(fmt.Sprintf("%d", i%2), false)
			s.ssMap.AddStatement(stmtExecInfo1)
			// Try to read it.
			s.ssMap.ToHistoryDatum(nil, true)
		}
		s.ssMap.SetEnabled("1", false)
	}

	for i := 0; i < threads; i++ {
		go addStmtFunc()
	}
	// Ensure that there's no deadlocks.
	wg.Wait()

	// Ensure that it's enabled at last.
	c.Assert(s.ssMap.Enabled(), IsTrue)
}

// Test GetMoreThanOnceSelect.
func (s *testStmtSummarySuite) TestGetMoreThanOnceSelect(c *C) {
	s.ssMap.Clear()

	stmtExecInfo1 := generateAnyExecInfo()
	stmtExecInfo1.OriginalSQL = "insert 1"
	stmtExecInfo1.NormalizedSQL = "insert ?"
	stmtExecInfo1.StmtCtx.StmtType = "insert"
	s.ssMap.AddStatement(stmtExecInfo1)
	schemas, sqls := s.ssMap.GetMoreThanOnceSelect()
	c.Assert(len(schemas), Equals, 0)
	c.Assert(len(sqls), Equals, 0)

	stmtExecInfo1.NormalizedSQL = "select ?"
	stmtExecInfo1.Digest = "digest1"
	stmtExecInfo1.StmtCtx.StmtType = "select"
	s.ssMap.AddStatement(stmtExecInfo1)
	schemas, sqls = s.ssMap.GetMoreThanOnceSelect()
	c.Assert(len(schemas), Equals, 0)
	c.Assert(len(sqls), Equals, 0)

	s.ssMap.AddStatement(stmtExecInfo1)
	schemas, sqls = s.ssMap.GetMoreThanOnceSelect()
	c.Assert(len(schemas), Equals, 1)
	c.Assert(len(sqls), Equals, 1)
}

// Test `formatBackoffTypes`.
func (s *testStmtSummarySuite) TestFormatBackoffTypes(c *C) {
	backoffMap := make(map[fmt.Stringer]int)
	c.Assert(formatBackoffTypes(backoffMap), IsNil)

	backoffMap[tikv.BoPDRPC] = 1
	c.Assert(formatBackoffTypes(backoffMap), Equals, "pdRPC:1")

	backoffMap[tikv.BoTxnLock] = 2
	c.Assert(formatBackoffTypes(backoffMap), Equals, "txnLock:2,pdRPC:1")
}

// Test refreshing current statement summary periodically.
func (s *testStmtSummarySuite) TestRefreshCurrentSummary(c *C) {
	s.ssMap.Clear()
	now := time.Now().Unix()

	s.ssMap.beginTimeForCurInterval = now + 10
	stmtExecInfo1 := generateAnyExecInfo()
	key := &stmtSummaryByDigestKey{
		schemaName: stmtExecInfo1.SchemaName,
		digest:     stmtExecInfo1.Digest,
		planDigest: stmtExecInfo1.PlanDigest,
	}
	s.ssMap.AddStatement(stmtExecInfo1)
	c.Assert(s.ssMap.summaryMap.Size(), Equals, 1)
	value, ok := s.ssMap.summaryMap.Get(key)
	c.Assert(ok, IsTrue)
	ssElement := value.(*stmtSummaryByDigest).history.Back().Value.(*stmtSummaryByDigestElement)
	c.Assert(ssElement.beginTime, Equals, s.ssMap.beginTimeForCurInterval)
	c.Assert(ssElement.execCount, Equals, int64(1))

	s.ssMap.beginTimeForCurInterval = now - 1900
	ssElement.beginTime = now - 1900
	s.ssMap.AddStatement(stmtExecInfo1)
	c.Assert(s.ssMap.summaryMap.Size(), Equals, 1)
	value, ok = s.ssMap.summaryMap.Get(key)
	c.Assert(ok, IsTrue)
	c.Assert(value.(*stmtSummaryByDigest).history.Len(), Equals, 2)
	ssElement = value.(*stmtSummaryByDigest).history.Back().Value.(*stmtSummaryByDigestElement)
	c.Assert(ssElement.beginTime, Greater, now-1900)
	c.Assert(ssElement.execCount, Equals, int64(1))

	s.ssMap.SetRefreshInterval("10", false)
	s.ssMap.beginTimeForCurInterval = now - 20
	ssElement.beginTime = now - 20
	s.ssMap.AddStatement(stmtExecInfo1)
	c.Assert(value.(*stmtSummaryByDigest).history.Len(), Equals, 3)
}

// Test expiring statement summary to history.
func (s *testStmtSummarySuite) TestSummaryHistory(c *C) {
	s.ssMap.Clear()
	now := time.Now().Unix()
	s.ssMap.SetRefreshInterval("10", false)
	s.ssMap.SetHistorySize("10", false)
	defer s.ssMap.SetRefreshInterval("1800", false)
	defer s.ssMap.SetHistorySize("24", false)

	stmtExecInfo1 := generateAnyExecInfo()
	key := &stmtSummaryByDigestKey{
		schemaName: stmtExecInfo1.SchemaName,
		digest:     stmtExecInfo1.Digest,
		planDigest: stmtExecInfo1.PlanDigest,
	}
	for i := 0; i < 11; i++ {
		s.ssMap.beginTimeForCurInterval = now + int64(i+1)*10
		s.ssMap.AddStatement(stmtExecInfo1)
		c.Assert(s.ssMap.summaryMap.Size(), Equals, 1)
		value, ok := s.ssMap.summaryMap.Get(key)
		c.Assert(ok, IsTrue)
		ssbd := value.(*stmtSummaryByDigest)
		if i < 10 {
			c.Assert(ssbd.history.Len(), Equals, i+1)
			ssElement := ssbd.history.Back().Value.(*stmtSummaryByDigestElement)
			c.Assert(ssElement.beginTime, Equals, s.ssMap.beginTimeForCurInterval)
			c.Assert(ssElement.execCount, Equals, int64(1))
		} else {
			c.Assert(ssbd.history.Len(), Equals, 10)
			ssElement := ssbd.history.Back().Value.(*stmtSummaryByDigestElement)
			c.Assert(ssElement.beginTime, Equals, s.ssMap.beginTimeForCurInterval)
			ssElement = ssbd.history.Front().Value.(*stmtSummaryByDigestElement)
			c.Assert(ssElement.beginTime, Equals, now+20)
		}
	}
	datum := s.ssMap.ToHistoryDatum(nil, true)
	c.Assert(len(datum), Equals, 10)

	s.ssMap.SetHistorySize("5", false)
	datum = s.ssMap.ToHistoryDatum(nil, true)
	c.Assert(len(datum), Equals, 5)
}

// Test summary when PrevSQL is not empty.
func (s *testStmtSummarySuite) TestPrevSQL(c *C) {
	s.ssMap.Clear()
	now := time.Now().Unix()
	// to disable expiration
	s.ssMap.beginTimeForCurInterval = now + 60

	stmtExecInfo1 := generateAnyExecInfo()
	stmtExecInfo1.PrevSQL = "prevSQL"
	stmtExecInfo1.PrevSQLDigest = "prevSQLDigest"
	s.ssMap.AddStatement(stmtExecInfo1)
	key := &stmtSummaryByDigestKey{
		schemaName: stmtExecInfo1.SchemaName,
		digest:     stmtExecInfo1.Digest,
		planDigest: stmtExecInfo1.PlanDigest,
		prevDigest: stmtExecInfo1.PrevSQLDigest,
	}
	c.Assert(s.ssMap.summaryMap.Size(), Equals, 1)
	_, ok := s.ssMap.summaryMap.Get(key)
	c.Assert(ok, IsTrue)

	// same prevSQL
	s.ssMap.AddStatement(stmtExecInfo1)
	c.Assert(s.ssMap.summaryMap.Size(), Equals, 1)

	// different prevSQL
	stmtExecInfo2 := stmtExecInfo1
	stmtExecInfo2.PrevSQL = "prevSQL1"
	stmtExecInfo2.PrevSQLDigest = "prevSQLDigest1"
	key.prevDigest = stmtExecInfo2.PrevSQLDigest
	s.ssMap.AddStatement(stmtExecInfo2)
	c.Assert(s.ssMap.summaryMap.Size(), Equals, 2)
	_, ok = s.ssMap.summaryMap.Get(key)
	c.Assert(ok, IsTrue)
}

func (s *testStmtSummarySuite) TestEndTime(c *C) {
	s.ssMap.Clear()
	now := time.Now().Unix()
	s.ssMap.beginTimeForCurInterval = now - 100

	stmtExecInfo1 := generateAnyExecInfo()
	s.ssMap.AddStatement(stmtExecInfo1)
	key := &stmtSummaryByDigestKey{
		schemaName: stmtExecInfo1.SchemaName,
		digest:     stmtExecInfo1.Digest,
		planDigest: stmtExecInfo1.PlanDigest,
	}
	c.Assert(s.ssMap.summaryMap.Size(), Equals, 1)
	value, ok := s.ssMap.summaryMap.Get(key)
	c.Assert(ok, IsTrue)
	ssbd := value.(*stmtSummaryByDigest)
	ssElement := ssbd.history.Back().Value.(*stmtSummaryByDigestElement)
	c.Assert(ssElement.beginTime, Equals, now-100)
	c.Assert(ssElement.endTime, Equals, now+1700)

	s.ssMap.SetRefreshInterval("3600", false)
	defer s.ssMap.SetRefreshInterval("1800", false)
	s.ssMap.AddStatement(stmtExecInfo1)
	c.Assert(ssbd.history.Len(), Equals, 1)
	ssElement = ssbd.history.Back().Value.(*stmtSummaryByDigestElement)
	c.Assert(ssElement.beginTime, Equals, now-100)
	c.Assert(ssElement.endTime, Equals, now+3500)

	s.ssMap.SetRefreshInterval("60", false)
	s.ssMap.AddStatement(stmtExecInfo1)
	c.Assert(ssbd.history.Len(), Equals, 2)
	now2 := time.Now().Unix()
	ssElement = ssbd.history.Front().Value.(*stmtSummaryByDigestElement)
	c.Assert(ssElement.beginTime, Equals, now-100)
	c.Assert(ssElement.endTime, GreaterEqual, now)
	c.Assert(ssElement.endTime, LessEqual, now2)
	ssElement = ssbd.history.Back().Value.(*stmtSummaryByDigestElement)
	c.Assert(ssElement.beginTime, GreaterEqual, now-60)
	c.Assert(ssElement.beginTime, LessEqual, now2)
	c.Assert(ssElement.endTime-ssElement.beginTime, Equals, int64(60))
}

func (s *testStmtSummarySuite) TestPointGet(c *C) {
	s.ssMap.Clear()
	now := time.Now().Unix()
	s.ssMap.beginTimeForCurInterval = now - 100

	stmtExecInfo1 := generateAnyExecInfo()
	stmtExecInfo1.PlanDigest = ""
	stmtExecInfo1.PlanDigestGen = fakePlanDigestGenerator
	s.ssMap.AddStatement(stmtExecInfo1)
	key := &stmtSummaryByDigestKey{
		schemaName: stmtExecInfo1.SchemaName,
		digest:     stmtExecInfo1.Digest,
		planDigest: "",
	}
	c.Assert(s.ssMap.summaryMap.Size(), Equals, 1)
	value, ok := s.ssMap.summaryMap.Get(key)
	c.Assert(ok, IsTrue)
	ssbd := value.(*stmtSummaryByDigest)
	ssElement := ssbd.history.Back().Value.(*stmtSummaryByDigestElement)
	c.Assert(ssElement.execCount, Equals, int64(1))

	s.ssMap.AddStatement(stmtExecInfo1)
	c.Assert(ssElement.execCount, Equals, int64(2))
}

func (s *testStmtSummarySuite) TestAccessPrivilege(c *C) {
	s.ssMap.Clear()

	loops := 32
	stmtExecInfo1 := generateAnyExecInfo()

	for i := 0; i < loops; i++ {
		stmtExecInfo1.Digest = fmt.Sprintf("digest%d", i)
		s.ssMap.AddStatement(stmtExecInfo1)
	}

	user := &auth.UserIdentity{Username: "user"}
	badUser := &auth.UserIdentity{Username: "bad_user"}

	datums := s.ssMap.ToCurrentDatum(user, false)
	c.Assert(len(datums), Equals, loops)
	datums = s.ssMap.ToCurrentDatum(badUser, false)
	c.Assert(len(datums), Equals, 0)
	datums = s.ssMap.ToCurrentDatum(badUser, true)
	c.Assert(len(datums), Equals, loops)

	datums = s.ssMap.ToHistoryDatum(user, false)
	c.Assert(len(datums), Equals, loops)
	datums = s.ssMap.ToHistoryDatum(badUser, false)
	c.Assert(len(datums), Equals, 0)
	datums = s.ssMap.ToHistoryDatum(badUser, true)
	c.Assert(len(datums), Equals, loops)
}
