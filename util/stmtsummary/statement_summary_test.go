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
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	. "github.com/pingcap/check"
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

func (s *testStmtSummarySuite) SetUpSuite(c *C) {
	s.ssMap = newStmtSummaryByDigestMap()
	s.ssMap.SetEnabled("1", false)
	s.ssMap.SetRefreshInterval("1800", false)
}

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

// Test stmtSummaryByDigest.AddStatement.
func (s *testStmtSummarySuite) TestAddStatement(c *C) {
	s.ssMap.Clear()
	now := time.Now().Unix()
	s.ssMap.beginTimeForCurInterval = now
	// to disable expiring
	s.ssMap.lastCheckExpireTime = now + 60

	tables := []stmtctx.TableEntry{{DB: "db1", Table: "tb1"}, {DB: "db2", Table: "tb2"}}
	indexes := []string{"a"}

	// first statement
	stmtExecInfo1 := generateAnyExecInfo()
	stmtExecInfo1.ExecDetail.CommitDetail.Mu.BackoffTypes = make([]fmt.Stringer, 0)
	key := &stmtSummaryByDigestKey{
		schemaName: stmtExecInfo1.SchemaName,
		digest:     stmtExecInfo1.Digest,
	}
	expectedSummary := stmtSummaryByDigest{
		beginTime:            now,
		schemaName:           stmtExecInfo1.SchemaName,
		stmtType:             stmtExecInfo1.StmtCtx.StmtType,
		digest:               stmtExecInfo1.Digest,
		normalizedSQL:        stmtExecInfo1.NormalizedSQL,
		sampleSQL:            stmtExecInfo1.OriginalSQL,
		tableNames:           "db1.tb1,db2.tb2",
		indexNames:           "a",
		sampleUser:           stmtExecInfo1.User,
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
	}
	stmtExecInfo2.StmtCtx.AddAffectedRows(200)
	expectedSummary.execCount++
	expectedSummary.sampleSQL = stmtExecInfo2.OriginalSQL
	expectedSummary.sampleUser = stmtExecInfo2.User
	expectedSummary.sumLatency += stmtExecInfo2.TotalLatency
	expectedSummary.maxLatency = stmtExecInfo2.TotalLatency
	expectedSummary.sumParseLatency += stmtExecInfo2.ParseLatency
	expectedSummary.maxParseLatency = stmtExecInfo2.ParseLatency
	expectedSummary.sumCompileLatency += stmtExecInfo2.CompileLatency
	expectedSummary.maxCompileLatency = stmtExecInfo2.CompileLatency
	expectedSummary.numCopTasks += int64(stmtExecInfo2.CopTasks.NumCopTasks)
	expectedSummary.sumCopProcessTime += int64(stmtExecInfo2.CopTasks.AvgProcessTime) * int64(stmtExecInfo2.CopTasks.NumCopTasks)
	expectedSummary.maxCopProcessTime = stmtExecInfo2.CopTasks.MaxProcessTime
	expectedSummary.maxCopProcessAddress = stmtExecInfo2.CopTasks.MaxProcessAddress
	expectedSummary.sumCopWaitTime += int64(stmtExecInfo2.CopTasks.AvgWaitTime) * int64(stmtExecInfo2.CopTasks.NumCopTasks)
	expectedSummary.maxCopWaitTime = stmtExecInfo2.CopTasks.MaxWaitTime
	expectedSummary.maxCopWaitAddress = stmtExecInfo2.CopTasks.MaxWaitAddress
	expectedSummary.sumProcessTime += stmtExecInfo2.ExecDetail.ProcessTime
	expectedSummary.maxProcessTime = stmtExecInfo2.ExecDetail.ProcessTime
	expectedSummary.sumWaitTime += stmtExecInfo2.ExecDetail.WaitTime
	expectedSummary.maxWaitTime = stmtExecInfo2.ExecDetail.WaitTime
	expectedSummary.sumBackoffTime += stmtExecInfo2.ExecDetail.BackoffTime
	expectedSummary.maxBackoffTime = stmtExecInfo2.ExecDetail.BackoffTime
	expectedSummary.sumTotalKeys += stmtExecInfo2.ExecDetail.TotalKeys
	expectedSummary.maxTotalKeys = stmtExecInfo2.ExecDetail.TotalKeys
	expectedSummary.sumProcessedKeys += stmtExecInfo2.ExecDetail.ProcessedKeys
	expectedSummary.maxProcessedKeys = stmtExecInfo2.ExecDetail.ProcessedKeys
	expectedSummary.sumGetCommitTsTime += stmtExecInfo2.ExecDetail.CommitDetail.GetCommitTsTime
	expectedSummary.maxGetCommitTsTime = stmtExecInfo2.ExecDetail.CommitDetail.GetCommitTsTime
	expectedSummary.sumPrewriteTime += stmtExecInfo2.ExecDetail.CommitDetail.PrewriteTime
	expectedSummary.maxPrewriteTime = stmtExecInfo2.ExecDetail.CommitDetail.PrewriteTime
	expectedSummary.sumCommitTime += stmtExecInfo2.ExecDetail.CommitDetail.CommitTime
	expectedSummary.maxCommitTime = stmtExecInfo2.ExecDetail.CommitDetail.CommitTime
	expectedSummary.sumLocalLatchTime += stmtExecInfo2.ExecDetail.CommitDetail.LocalLatchTime
	expectedSummary.maxLocalLatchTime = stmtExecInfo2.ExecDetail.CommitDetail.LocalLatchTime
	expectedSummary.sumCommitBackoffTime += stmtExecInfo2.ExecDetail.CommitDetail.CommitBackoffTime
	expectedSummary.maxCommitBackoffTime = stmtExecInfo2.ExecDetail.CommitDetail.CommitBackoffTime
	expectedSummary.sumResolveLockTime += stmtExecInfo2.ExecDetail.CommitDetail.ResolveLockTime
	expectedSummary.maxResolveLockTime = stmtExecInfo2.ExecDetail.CommitDetail.ResolveLockTime
	expectedSummary.sumWriteKeys += int64(stmtExecInfo2.ExecDetail.CommitDetail.WriteKeys)
	expectedSummary.maxWriteKeys = stmtExecInfo2.ExecDetail.CommitDetail.WriteKeys
	expectedSummary.sumWriteSize += int64(stmtExecInfo2.ExecDetail.CommitDetail.WriteSize)
	expectedSummary.maxWriteSize = stmtExecInfo2.ExecDetail.CommitDetail.WriteSize
	expectedSummary.sumPrewriteRegionNum += int64(stmtExecInfo2.ExecDetail.CommitDetail.PrewriteRegionNum)
	expectedSummary.maxPrewriteRegionNum = stmtExecInfo2.ExecDetail.CommitDetail.PrewriteRegionNum
	expectedSummary.sumTxnRetry += int64(stmtExecInfo2.ExecDetail.CommitDetail.TxnRetry)
	expectedSummary.maxTxnRetry = stmtExecInfo2.ExecDetail.CommitDetail.TxnRetry
	expectedSummary.backoffTypes[tikv.BoTxnLock] = 1
	expectedSummary.sumMem += stmtExecInfo2.MemMax
	expectedSummary.maxMem = stmtExecInfo2.MemMax
	expectedSummary.sumAffectedRows += stmtExecInfo2.StmtCtx.AffectedRows()
	expectedSummary.lastSeen = stmtExecInfo2.StartTime

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
	}
	stmtExecInfo3.StmtCtx.AddAffectedRows(20000)
	expectedSummary.execCount++
	expectedSummary.sampleUser = stmtExecInfo3.User
	expectedSummary.sampleSQL = stmtExecInfo3.OriginalSQL
	expectedSummary.sumLatency += stmtExecInfo3.TotalLatency
	expectedSummary.minLatency = stmtExecInfo3.TotalLatency
	expectedSummary.sumParseLatency += stmtExecInfo3.ParseLatency
	expectedSummary.sumCompileLatency += stmtExecInfo3.CompileLatency
	expectedSummary.numCopTasks += int64(stmtExecInfo3.CopTasks.NumCopTasks)
	expectedSummary.sumCopProcessTime += int64(stmtExecInfo3.CopTasks.AvgProcessTime) * int64(stmtExecInfo3.CopTasks.NumCopTasks)
	expectedSummary.sumCopWaitTime += int64(stmtExecInfo3.CopTasks.AvgWaitTime) * int64(stmtExecInfo3.CopTasks.NumCopTasks)
	expectedSummary.sumProcessTime += stmtExecInfo3.ExecDetail.ProcessTime
	expectedSummary.sumWaitTime += stmtExecInfo3.ExecDetail.WaitTime
	expectedSummary.sumBackoffTime += stmtExecInfo3.ExecDetail.BackoffTime
	expectedSummary.sumTotalKeys += stmtExecInfo3.ExecDetail.TotalKeys
	expectedSummary.sumProcessedKeys += stmtExecInfo3.ExecDetail.ProcessedKeys
	expectedSummary.sumGetCommitTsTime += stmtExecInfo3.ExecDetail.CommitDetail.GetCommitTsTime
	expectedSummary.sumPrewriteTime += stmtExecInfo3.ExecDetail.CommitDetail.PrewriteTime
	expectedSummary.sumCommitTime += stmtExecInfo3.ExecDetail.CommitDetail.CommitTime
	expectedSummary.sumLocalLatchTime += stmtExecInfo3.ExecDetail.CommitDetail.LocalLatchTime
	expectedSummary.sumCommitBackoffTime += stmtExecInfo3.ExecDetail.CommitDetail.CommitBackoffTime
	expectedSummary.sumResolveLockTime += stmtExecInfo3.ExecDetail.CommitDetail.ResolveLockTime
	expectedSummary.sumWriteKeys += int64(stmtExecInfo3.ExecDetail.CommitDetail.WriteKeys)
	expectedSummary.sumWriteSize += int64(stmtExecInfo3.ExecDetail.CommitDetail.WriteSize)
	expectedSummary.sumPrewriteRegionNum += int64(stmtExecInfo3.ExecDetail.CommitDetail.PrewriteRegionNum)
	expectedSummary.sumTxnRetry += int64(stmtExecInfo3.ExecDetail.CommitDetail.TxnRetry)
	expectedSummary.backoffTypes[tikv.BoTxnLock] = 2
	expectedSummary.sumMem += stmtExecInfo3.MemMax
	expectedSummary.sumAffectedRows += stmtExecInfo3.StmtCtx.AffectedRows()
	expectedSummary.firstSeen = stmtExecInfo3.StartTime

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
	}
	s.ssMap.AddStatement(stmtExecInfo5)
	c.Assert(s.ssMap.summaryMap.Size(), Equals, 3)
	_, ok = s.ssMap.summaryMap.Get(key)
	c.Assert(ok, IsTrue)
}

func matchStmtSummaryByDigest(first, second *stmtSummaryByDigest) bool {
	if first.beginTime != second.beginTime ||
		first.schemaName != second.schemaName ||
		!strings.EqualFold(first.stmtType, second.stmtType) ||
		first.digest != second.digest ||
		first.normalizedSQL != second.normalizedSQL ||
		first.sampleSQL != second.sampleSQL ||
		first.sampleUser != second.sampleUser ||
		first.tableNames != second.tableNames ||
		first.indexNames != second.indexNames ||
		first.execCount != second.execCount ||
		first.sumLatency != second.sumLatency ||
		first.maxLatency != second.maxLatency ||
		first.minLatency != second.minLatency ||
		first.sumParseLatency != second.sumParseLatency ||
		first.maxParseLatency != second.maxParseLatency ||
		first.sumCompileLatency != second.sumCompileLatency ||
		first.maxCompileLatency != second.maxCompileLatency ||
		first.numCopTasks != second.numCopTasks ||
		first.sumCopProcessTime != second.sumCopProcessTime ||
		first.maxCopProcessTime != second.maxCopProcessTime ||
		first.maxCopProcessAddress != second.maxCopProcessAddress ||
		first.sumCopWaitTime != second.sumCopWaitTime ||
		first.maxCopWaitTime != second.maxCopWaitTime ||
		first.maxCopWaitAddress != second.maxCopWaitAddress ||
		first.sumProcessTime != second.sumProcessTime ||
		first.maxProcessTime != second.maxProcessTime ||
		first.sumWaitTime != second.sumWaitTime ||
		first.maxWaitTime != second.maxWaitTime ||
		first.sumBackoffTime != second.sumBackoffTime ||
		first.maxBackoffTime != second.maxBackoffTime ||
		first.sumTotalKeys != second.sumTotalKeys ||
		first.maxTotalKeys != second.maxTotalKeys ||
		first.sumProcessedKeys != second.sumProcessedKeys ||
		first.maxProcessedKeys != second.maxProcessedKeys ||
		first.sumGetCommitTsTime != second.sumGetCommitTsTime ||
		first.maxGetCommitTsTime != second.maxGetCommitTsTime ||
		first.sumPrewriteTime != second.sumPrewriteTime ||
		first.maxPrewriteTime != second.maxPrewriteTime ||
		first.sumCommitTime != second.sumCommitTime ||
		first.maxCommitTime != second.maxCommitTime ||
		first.sumLocalLatchTime != second.sumLocalLatchTime ||
		first.maxLocalLatchTime != second.maxLocalLatchTime ||
		first.sumCommitBackoffTime != second.sumCommitBackoffTime ||
		first.maxCommitBackoffTime != second.maxCommitBackoffTime ||
		first.sumResolveLockTime != second.sumResolveLockTime ||
		first.maxResolveLockTime != second.maxResolveLockTime ||
		first.sumWriteKeys != second.sumWriteKeys ||
		first.maxWriteKeys != second.maxWriteKeys ||
		first.sumWriteSize != second.sumWriteSize ||
		first.maxWriteSize != second.maxWriteSize ||
		first.sumPrewriteRegionNum != second.sumPrewriteRegionNum ||
		first.maxPrewriteRegionNum != second.maxPrewriteRegionNum ||
		first.sumTxnRetry != second.sumTxnRetry ||
		first.maxTxnRetry != second.maxTxnRetry ||
		first.sumMem != second.sumMem ||
		first.maxMem != second.maxMem ||
		first.sumAffectedRows != second.sumAffectedRows ||
		first.firstSeen != second.firstSeen ||
		first.lastSeen != second.lastSeen {
		return false
	}
	if len(first.backoffTypes) != len(second.backoffTypes) {
		return false
	}
	for key, value1 := range first.backoffTypes {
		value2, ok := second.backoffTypes[key]
		if ok != true {
			return false
		}
		if value1 != value2 {
			return false
		}
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
	datums := s.ssMap.ToDatum()
	c.Assert(len(datums), Equals, 1)
	n := types.Time{Time: types.FromGoTime(time.Unix(s.ssMap.beginTimeForCurInterval, 0)), Type: mysql.TypeTimestamp}
	t := types.Time{Time: types.FromGoTime(stmtExecInfo1.StartTime), Type: mysql.TypeTimestamp}
	match(c, datums[0], n, "select", stmtExecInfo1.SchemaName, stmtExecInfo1.Digest, stmtExecInfo1.NormalizedSQL,
		"db1.tb1,db2.tb2", "a", stmtExecInfo1.User, 1, int64(stmtExecInfo1.TotalLatency),
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
		stmtExecInfo1.ExecDetail.CommitDetail.TxnRetry, stmtExecInfo1.ExecDetail.CommitDetail.TxnRetry,
		"txnLock:1", stmtExecInfo1.MemMax, stmtExecInfo1.MemMax, stmtExecInfo1.StmtCtx.AffectedRows(),
		t, t, stmtExecInfo1.OriginalSQL)
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
		datums := s.ssMap.ToDatum()
		c.Assert(len(datums), Equals, loops)
	}

	for i := 0; i < threads; i++ {
		go addStmtFunc()
	}
	wg.Wait()

	datums := s.ssMap.ToDatum()
	c.Assert(len(datums), Equals, loops)
}

// Test max number of statement count.
func (s *testStmtSummarySuite) TestMaxStmtCount(c *C) {
	s.ssMap.Clear()
	now := time.Now().Unix()
	// to disable expiration
	s.ssMap.beginTimeForCurInterval = now + 60

	stmtExecInfo1 := generateAnyExecInfo()
	maxStmtCount := config.GetGlobalConfig().StmtSummary.MaxStmtCount

	// 1000 digests
	loops := int(maxStmtCount) * 10
	for i := 0; i < loops; i++ {
		stmtExecInfo1.Digest = fmt.Sprintf("digest%d", i)
		s.ssMap.AddStatement(stmtExecInfo1)
	}

	// Summary count should be MaxStmtCount.
	sm := s.ssMap.summaryMap
	c.Assert(sm.Size(), Equals, int(maxStmtCount))

	// LRU cache should work.
	for i := loops - int(maxStmtCount); i < loops; i++ {
		key := &stmtSummaryByDigestKey{
			schemaName: stmtExecInfo1.SchemaName,
			digest:     fmt.Sprintf("digest%d", i),
		}
		_, ok := sm.Get(key)
		c.Assert(ok, IsTrue)
	}
}

// Test max length of normalized and sample SQL.
func (s *testStmtSummarySuite) TestMaxSQLLength(c *C) {
	s.ssMap.Clear()
	now := time.Now().Unix()
	// to disable expiration
	s.ssMap.beginTimeForCurInterval = now + 60

	// Create a long SQL
	maxSQLLength := config.GetGlobalConfig().StmtSummary.MaxSQLLength
	length := int(maxSQLLength) * 10
	str := strings.Repeat("a", length)

	stmtExecInfo1 := generateAnyExecInfo()
	stmtExecInfo1.OriginalSQL = str
	stmtExecInfo1.NormalizedSQL = str
	s.ssMap.AddStatement(stmtExecInfo1)

	key := &stmtSummaryByDigestKey{
		schemaName: stmtExecInfo1.SchemaName,
		digest:     stmtExecInfo1.Digest,
	}
	value, ok := s.ssMap.summaryMap.Get(key)
	c.Assert(ok, IsTrue)
	// Length of normalizedSQL and sampleSQL should be maxSQLLength.
	summary := value.(*stmtSummaryByDigest)
	c.Assert(len(summary.normalizedSQL), Equals, int(maxSQLLength))
	c.Assert(len(summary.sampleSQL), Equals, int(maxSQLLength))
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
	datums := s.ssMap.ToDatum()
	c.Assert(len(datums), Equals, 0)

	// Set true in session scope, it will overwrite global scope.
	s.ssMap.SetEnabled("1", true)

	s.ssMap.AddStatement(stmtExecInfo1)
	datums = s.ssMap.ToDatum()
	c.Assert(len(datums), Equals, 1)

	// Set false in global scope, it shouldn't work.
	s.ssMap.SetEnabled("0", false)
	s.ssMap.beginTimeForCurInterval = now + 60

	stmtExecInfo2 := stmtExecInfo1
	stmtExecInfo2.OriginalSQL = "original_sql2"
	stmtExecInfo2.NormalizedSQL = "normalized_sql2"
	stmtExecInfo2.Digest = "digest2"
	s.ssMap.AddStatement(stmtExecInfo2)
	datums = s.ssMap.ToDatum()
	c.Assert(len(datums), Equals, 2)

	// Unset in session scope.
	s.ssMap.SetEnabled("", true)
	s.ssMap.beginTimeForCurInterval = now + 60
	s.ssMap.AddStatement(stmtExecInfo2)
	datums = s.ssMap.ToDatum()
	c.Assert(len(datums), Equals, 0)

	// Unset in global scope.
	s.ssMap.SetEnabled("", false)
	s.ssMap.beginTimeForCurInterval = now + 60
	s.ssMap.AddStatement(stmtExecInfo1)
	datums = s.ssMap.ToDatum()
	c.Assert(len(datums), Equals, 0)

	// Set back.
	s.ssMap.SetEnabled("1", false)
}

// Test GetMoreThanOnceSelect.
func (s *testStmtSummarySuite) TestGetMoreThenOnceSelect(c *C) {
	s.ssMap.Clear()
	now := time.Now().Unix()
	// to disable expiration
	s.ssMap.beginTimeForCurInterval = now + 60

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
	}
	s.ssMap.AddStatement(stmtExecInfo1)
	c.Assert(s.ssMap.summaryMap.Size(), Equals, 1)
	value, ok := s.ssMap.summaryMap.Get(key)
	c.Assert(ok, IsTrue)
	c.Assert(value.(*stmtSummaryByDigest).beginTime, Equals, s.ssMap.beginTimeForCurInterval)
	c.Assert(value.(*stmtSummaryByDigest).execCount, Equals, int64(1))

	s.ssMap.beginTimeForCurInterval = now - 1900
	value.(*stmtSummaryByDigest).beginTime = now - 1900
	s.ssMap.lastCheckExpireTime = now - 10
	s.ssMap.AddStatement(stmtExecInfo1)
	c.Assert(s.ssMap.summaryMap.Size(), Equals, 1)
	value, ok = s.ssMap.summaryMap.Get(key)
	c.Assert(ok, IsTrue)
	c.Assert(value.(*stmtSummaryByDigest).beginTime, Greater, now-1900)
	c.Assert(value.(*stmtSummaryByDigest).execCount, Equals, int64(1))
}
