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
	"hash/fnv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/hack"
)

var _ = Suite(&testStmtSummarySuite{})

type testStmtSummarySuite struct {
	ssMap *stmtSummaryByDigestMap
}

func (s *testStmtSummarySuite) SetUpSuite(c *C) {
	s.ssMap = newStmtSummaryByDigestMap()
	s.ssMap.SetEnabled("1", false)
	s.ssMap.SetIntervalMinutes("30", false)
	s.ssMap.SetHistoryMaxHours("12", false)
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

	// first statement
	stmtExecInfo1 := &StmtExecInfo{
		SchemaName:     "schema_name",
		OriginalSQL:    "original_sql1",
		NormalizedSQL:  "normalized_sql",
		Digest:         "digest",
		User:           "user",
		TotalLatency:   10000,
		ParseLatency:   100,
		CompileLatency: 1000,
		TableIDs:       "1,2",
		IndexNames:     "1,2",
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
		},
		MemMax:       10000,
		Plan:         "plan",
		AffectedRows: 100,
		StartTime:    time.Date(2019, 1, 1, 10, 10, 10, 10, time.UTC),
	}
	hash := fnv.New64()
	_, err := hash.Write(hack.Slice(stmtExecInfo1.Plan))
	c.Assert(err, IsNil)
	key := &stmtSummaryByDigestKey{
		schemaName: stmtExecInfo1.SchemaName,
		digest:     stmtExecInfo1.Digest,
		planHash:   hash.Sum64(),
	}
	expectedSummaryElement := stmtSummaryByDigestElement{
		beginTime:            now,
		user:                 stmtExecInfo1.User,
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
		sumRequestCount:      int64(stmtExecInfo1.ExecDetail.RequestCount),
		maxRequestCount:      int64(stmtExecInfo1.ExecDetail.RequestCount),
		sumTotalKeys:         stmtExecInfo1.ExecDetail.TotalKeys,
		maxTotalKeys:         stmtExecInfo1.ExecDetail.TotalKeys,
		sumProcessedKeys:     stmtExecInfo1.ExecDetail.ProcessedKeys,
		maxProcessedKeys:     stmtExecInfo1.ExecDetail.ProcessedKeys,
		sumMem:               stmtExecInfo1.MemMax,
		maxMem:               stmtExecInfo1.MemMax,
		sumAffectedRows:      stmtExecInfo1.AffectedRows,
		firstSeen:            stmtExecInfo1.StartTime,
		lastSeen:             stmtExecInfo1.StartTime,
	}
	history := list.New()
	history.PushBack(&expectedSummaryElement)
	expectedSummary := stmtSummaryByDigest{
		history:       history,
		schemaName:    stmtExecInfo1.SchemaName,
		digest:        stmtExecInfo1.Digest,
		plan:          stmtExecInfo1.Plan,
		normalizedSQL: stmtExecInfo1.NormalizedSQL,
		sampleSQL:     stmtExecInfo1.OriginalSQL,
		tableIDs:      stmtExecInfo1.TableIDs,
		indexNames:    stmtExecInfo1.IndexNames,
	}

	s.ssMap.AddStatement(stmtExecInfo1)
	summary, ok := s.ssMap.summaryMap.Get(key)
	c.Assert(ok, IsTrue)
	expectedSummaryElement.beginTime = summary.(*stmtSummaryByDigest).history.Front().Value.(*stmtSummaryByDigestElement).beginTime
	c.Assert(matchStmtSummaryByDigest(summary.(*stmtSummaryByDigest), &expectedSummary), IsTrue)

	// Second statement is similar with the first statement, and its values are
	// greater than that of the first statement.
	stmtExecInfo2 := &StmtExecInfo{
		SchemaName:     "schema_name",
		OriginalSQL:    "original_sql2",
		NormalizedSQL:  "normalized_sql",
		Digest:         "digest",
		User:           "user",
		TotalLatency:   20000,
		ParseLatency:   200,
		CompileLatency: 2000,
		TableIDs:       "1,2",
		IndexNames:     "1,2",
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
		},
		MemMax:       20000,
		Plan:         "plan",
		AffectedRows: 200,
		StartTime:    time.Date(2019, 1, 1, 10, 10, 20, 10, time.UTC),
	}
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
	expectedSummaryElement.sumRequestCount += int64(stmtExecInfo2.ExecDetail.RequestCount)
	expectedSummaryElement.maxRequestCount = int64(stmtExecInfo2.ExecDetail.RequestCount)
	expectedSummaryElement.sumTotalKeys += stmtExecInfo2.ExecDetail.TotalKeys
	expectedSummaryElement.maxTotalKeys = stmtExecInfo2.ExecDetail.TotalKeys
	expectedSummaryElement.sumProcessedKeys += stmtExecInfo2.ExecDetail.ProcessedKeys
	expectedSummaryElement.maxProcessedKeys = stmtExecInfo2.ExecDetail.ProcessedKeys
	expectedSummaryElement.sumMem += stmtExecInfo2.MemMax
	expectedSummaryElement.maxMem = stmtExecInfo2.MemMax
	expectedSummaryElement.sumAffectedRows += stmtExecInfo2.AffectedRows
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
		User:           "user",
		TotalLatency:   1000,
		ParseLatency:   50,
		CompileLatency: 500,
		TableIDs:       "1,2",
		IndexNames:     "1,2",
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
		},
		MemMax:       200,
		Plan:         "plan",
		AffectedRows: 0,
		StartTime:    time.Date(2019, 1, 1, 10, 10, 0, 10, time.UTC),
	}
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
	expectedSummaryElement.sumRequestCount += int64(stmtExecInfo3.ExecDetail.RequestCount)
	expectedSummaryElement.sumTotalKeys += stmtExecInfo3.ExecDetail.TotalKeys
	expectedSummaryElement.sumProcessedKeys += stmtExecInfo3.ExecDetail.ProcessedKeys
	expectedSummaryElement.sumMem += stmtExecInfo3.MemMax
	expectedSummaryElement.sumAffectedRows += stmtExecInfo3.AffectedRows
	expectedSummaryElement.firstSeen = stmtExecInfo3.StartTime

	s.ssMap.AddStatement(stmtExecInfo3)
	summary, ok = s.ssMap.summaryMap.Get(key)
	c.Assert(ok, IsTrue)
	c.Assert(matchStmtSummaryByDigest(summary.(*stmtSummaryByDigest), &expectedSummary), IsTrue)

	// Fourth statement is in a different schema.
	stmtExecInfo4 := &StmtExecInfo{
		SchemaName:     "schema_name2",
		OriginalSQL:    "original_sql1",
		NormalizedSQL:  "normalized_sql",
		Digest:         "digest",
		User:           "user",
		TotalLatency:   1000,
		ParseLatency:   50,
		CompileLatency: 500,
		TableIDs:       "1,2",
		IndexNames:     "1,2",
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
		},
		MemMax:       200,
		Plan:         "plan",
		AffectedRows: 10,
		StartTime:    time.Date(2019, 1, 1, 10, 10, 0, 10, time.UTC),
	}
	key = &stmtSummaryByDigestKey{
		schemaName: stmtExecInfo4.SchemaName,
		digest:     stmtExecInfo4.Digest,
		planHash:   hash.Sum64(),
	}

	s.ssMap.AddStatement(stmtExecInfo4)
	c.Assert(s.ssMap.summaryMap.Size(), Equals, 2)
	_, ok = s.ssMap.summaryMap.Get(key)
	c.Assert(ok, IsTrue)

	// Fifth statement has a different digest.
	stmtExecInfo5 := &StmtExecInfo{
		SchemaName:     "schema_name",
		OriginalSQL:    "original_sql1",
		NormalizedSQL:  "normalized_sql2",
		Digest:         "digest2",
		User:           "user",
		TotalLatency:   1000,
		ParseLatency:   50,
		CompileLatency: 500,
		TableIDs:       "1,2",
		IndexNames:     "1,2",
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
		},
		MemMax:       200,
		Plan:         "plan",
		AffectedRows: 10,
		StartTime:    time.Date(2019, 1, 1, 10, 10, 0, 10, time.UTC),
	}
	key = &stmtSummaryByDigestKey{
		schemaName: stmtExecInfo5.SchemaName,
		digest:     stmtExecInfo5.Digest,
		planHash:   hash.Sum64(),
	}

	s.ssMap.AddStatement(stmtExecInfo5)
	c.Assert(s.ssMap.summaryMap.Size(), Equals, 3)
	_, ok = s.ssMap.summaryMap.Get(key)
	c.Assert(ok, IsTrue)

	// Sixth statement has a different plan.
	stmtExecInfo6 := &StmtExecInfo{
		SchemaName:     "schema_name",
		OriginalSQL:    "original_sql1",
		NormalizedSQL:  "normalized_sql2",
		Digest:         "digest",
		User:           "user",
		TotalLatency:   1000,
		ParseLatency:   50,
		CompileLatency: 500,
		TableIDs:       "1,2",
		IndexNames:     "1,2",
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
		},
		MemMax:       200,
		Plan:         "plan1",
		AffectedRows: 10,
		StartTime:    time.Date(2019, 1, 1, 10, 10, 0, 10, time.UTC),
	}
	hash = fnv.New64()
	_, err = hash.Write(hack.Slice(stmtExecInfo1.Plan))
	c.Assert(err, IsNil)
	key = &stmtSummaryByDigestKey{
		schemaName: stmtExecInfo6.SchemaName,
		digest:     stmtExecInfo5.Digest,
		planHash:   hash.Sum64(),
	}

	s.ssMap.AddStatement(stmtExecInfo6)
	c.Assert(s.ssMap.summaryMap.Size(), Equals, 3)
	_, ok = s.ssMap.summaryMap.Get(key)
	c.Assert(ok, IsTrue)
}

func matchStmtSummaryByDigest(first *stmtSummaryByDigest, second *stmtSummaryByDigest) bool {
	if first.schemaName != second.schemaName ||
		first.digest != second.digest ||
		first.plan != second.plan ||
		first.normalizedSQL != second.normalizedSQL ||
		first.sampleSQL != second.sampleSQL ||
		first.tableIDs != second.tableIDs ||
		first.indexNames != second.indexNames {
		return false
	}
	ele1 := first.history.Front()
	ele2 := second.history.Front()
	for {
		if ele1 == nil && ele2 == nil {
			break
		}
		if ele1 != nil && ele2 != nil {
			if *ele1.Value.(*stmtSummaryByDigestElement) != *ele2.Value.(*stmtSummaryByDigestElement) {
				return false
			}
			ele1 = ele1.Next()
			ele2 = ele2.Next()
		} else {
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

// Test stmtSummaryByDigest.ToDatum
func (s *testStmtSummarySuite) TestToDatum(c *C) {
	s.ssMap.Clear()
	now := time.Now().Unix()
	s.ssMap.beginTimeForCurInterval = now

	stmtExecInfo1 := &StmtExecInfo{
		SchemaName:     "schema_name",
		OriginalSQL:    "original_sql1",
		NormalizedSQL:  "normalized_sql",
		Digest:         "digest",
		User:           "user",
		TotalLatency:   10000,
		ParseLatency:   100,
		CompileLatency: 1000,
		TableIDs:       "1,2",
		IndexNames:     "1,2",
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
		},
		MemMax:       10000,
		Plan:         "plan",
		AffectedRows: 100,
		StartTime:    time.Date(2019, 1, 1, 10, 10, 10, 10, time.UTC),
	}
	s.ssMap.AddStatement(stmtExecInfo1)
	datums := s.ssMap.ToCurrentDatum()
	c.Assert(len(datums), Equals, 1)
	n := types.Time{Time: types.FromGoTime(time.Unix(now, 0)), Type: mysql.TypeTimestamp}
	t := types.Time{Time: types.FromGoTime(stmtExecInfo1.StartTime), Type: mysql.TypeTimestamp}
	match(c, datums[0], n, stmtExecInfo1.SchemaName, stmtExecInfo1.Digest, stmtExecInfo1.NormalizedSQL, stmtExecInfo1.Plan,
		stmtExecInfo1.TableIDs, stmtExecInfo1.IndexNames, stmtExecInfo1.User,
		1, int64(stmtExecInfo1.TotalLatency), int64(stmtExecInfo1.TotalLatency), int64(stmtExecInfo1.TotalLatency), int64(stmtExecInfo1.TotalLatency),
		int64(stmtExecInfo1.ParseLatency), int64(stmtExecInfo1.ParseLatency), int64(stmtExecInfo1.CompileLatency), int64(stmtExecInfo1.CompileLatency),
		stmtExecInfo1.CopTasks.NumCopTasks, int64(stmtExecInfo1.CopTasks.AvgProcessTime), int64(stmtExecInfo1.CopTasks.MaxProcessTime),
		stmtExecInfo1.CopTasks.MaxProcessAddress, int64(stmtExecInfo1.CopTasks.AvgWaitTime), int64(stmtExecInfo1.CopTasks.MaxWaitTime),
		stmtExecInfo1.CopTasks.MaxWaitAddress, int64(stmtExecInfo1.ExecDetail.ProcessTime), int64(stmtExecInfo1.ExecDetail.ProcessTime),
		int64(stmtExecInfo1.ExecDetail.WaitTime), int64(stmtExecInfo1.ExecDetail.WaitTime), int64(stmtExecInfo1.ExecDetail.BackoffTime),
		int64(stmtExecInfo1.ExecDetail.BackoffTime), stmtExecInfo1.ExecDetail.RequestCount, stmtExecInfo1.ExecDetail.RequestCount,
		stmtExecInfo1.ExecDetail.TotalKeys, stmtExecInfo1.ExecDetail.TotalKeys, stmtExecInfo1.ExecDetail.ProcessedKeys,
		stmtExecInfo1.ExecDetail.ProcessedKeys, stmtExecInfo1.MemMax, stmtExecInfo1.MemMax,
		stmtExecInfo1.AffectedRows, t, t, stmtExecInfo1.OriginalSQL)
}

// Test AddStatement and ToDatum parallel
func (s *testStmtSummarySuite) TestAddStatementParallel(c *C) {
	s.ssMap.Clear()
	now := time.Now().Unix()
	s.ssMap.beginTimeForCurInterval = now
	// to disable expiring
	s.ssMap.lastCheckExpireTime = now + 60

	threads := 8
	loops := 32
	wg := sync.WaitGroup{}
	wg.Add(threads)

	addStmtFunc := func() {
		defer wg.Done()
		stmtExecInfo1 := &StmtExecInfo{
			SchemaName:     "schema_name",
			OriginalSQL:    "original_sql1",
			NormalizedSQL:  "normalized_sql",
			Digest:         "digest",
			User:           "user",
			TotalLatency:   10000,
			ParseLatency:   100,
			CompileLatency: 1000,
			TableIDs:       "1,2",
			IndexNames:     "1,2",
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
			},
			MemMax:       10000,
			Plan:         "plan",
			AffectedRows: 100,
			StartTime:    time.Date(2019, 1, 1, 10, 10, 10, 10, time.UTC),
		}

		// Add 32 times with different digest
		for i := 0; i < loops; i++ {
			stmtExecInfo1.Digest = fmt.Sprintf("digest%d", i)
			s.ssMap.AddStatement(stmtExecInfo1)
		}

		// There would be 32 summaries
		datums := s.ssMap.ToCurrentDatum()
		c.Assert(len(datums), Equals, loops)
	}

	for i := 0; i < threads; i++ {
		go addStmtFunc()
	}
	wg.Wait()

	datums := s.ssMap.ToCurrentDatum()
	c.Assert(len(datums), Equals, loops)
}

func (s *testStmtSummarySuite) TestExpireToHistory(c *C) {
	s.ssMap.Clear()
	s.ssMap.SetIntervalMinutes("30", true)
	s.ssMap.SetHistoryMaxHours("12", true)

	now := time.Now().Unix() / 1800 * 1800
	s.ssMap.beginTimeForCurInterval = now
	// to disable expiring
	s.ssMap.lastCheckExpireTime = now + 60

	// first statement
	stmtExecInfo1 := &StmtExecInfo{
		SchemaName:     "schema_name",
		OriginalSQL:    "original_sql1",
		NormalizedSQL:  "normalized_sql",
		Digest:         "digest",
		User:           "user",
		TotalLatency:   10000,
		ParseLatency:   100,
		CompileLatency: 1000,
		TableIDs:       "1,2",
		IndexNames:     "1,2",
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
		},
		MemMax:       10000,
		Plan:         "plan",
		AffectedRows: 100,
		StartTime:    time.Date(2019, 1, 1, 10, 10, 10, 10, time.UTC),
	}
	s.ssMap.AddStatement(stmtExecInfo1)
	datums := s.ssMap.ToCurrentDatum()
	c.Assert(len(datums), Equals, 1)

	// Expire the current summary to history.
	s.ssMap.beginTimeForCurInterval = now - 1800
	s.ssMap.lastCheckExpireTime = now - 60
	hash := fnv.New64()
	_, err := hash.Write(hack.Slice(stmtExecInfo1.Plan))
	c.Assert(err, IsNil)
	key := &stmtSummaryByDigestKey{
		schemaName: stmtExecInfo1.SchemaName,
		digest:     stmtExecInfo1.Digest,
		planHash:   hash.Sum64(),
	}
	value, ok := s.ssMap.summaryMap.Get(key)
	c.Assert(ok, IsTrue)
	ssElement := value.(*stmtSummaryByDigest).history.Back().Value.(*stmtSummaryByDigestElement)
	ssElement.beginTime = now - 1800

	s.ssMap.AddStatement(stmtExecInfo1)
	datums = s.ssMap.ToCurrentDatum()
	c.Assert(len(datums), Equals, 1)
	datums = s.ssMap.ToHistoryDatum()
	c.Assert(len(datums), Equals, 2)

	// Expire the old summaries out of the history.
	s.ssMap.beginTimeForCurInterval = now - 13*3600
	s.ssMap.lastCheckExpireTime = now - 60
	value, ok = s.ssMap.summaryMap.Get(key)
	c.Assert(ok, IsTrue)
	ssElement = value.(*stmtSummaryByDigest).history.Back().Value.(*stmtSummaryByDigestElement)
	ssElement.beginTime = now - 13*3600
	s.ssMap.AddStatement(stmtExecInfo1)
	datums = s.ssMap.ToHistoryDatum()
	c.Assert(len(datums), Equals, 2)
}

// Test max number of statement count.
func (s *testStmtSummarySuite) TestMaxStmtCount(c *C) {
	s.ssMap.Clear()

	stmtExecInfo1 := &StmtExecInfo{
		SchemaName:     "schema_name",
		OriginalSQL:    "original_sql1",
		NormalizedSQL:  "normalized_sql",
		Digest:         "digest",
		User:           "user",
		TotalLatency:   10000,
		ParseLatency:   100,
		CompileLatency: 1000,
		TableIDs:       "1,2",
		IndexNames:     "1,2",
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
		},
		MemMax:       10000,
		Plan:         "plan",
		AffectedRows: 100,
		StartTime:    time.Date(2019, 1, 1, 10, 10, 10, 10, time.UTC),
	}

	maxStmtCount := config.GetGlobalConfig().StmtSummary.MaxStmtCount

	// 1000 digests
	loops := int(maxStmtCount) * 10
	for i := 0; i < loops; i++ {
		stmtExecInfo1.Digest = fmt.Sprintf("digest%d", i)
		s.ssMap.AddStatement(stmtExecInfo1)
	}

	// Summary count should be MaxStmtCount
	sm := s.ssMap.summaryMap
	c.Assert(sm.Size(), Equals, int(maxStmtCount))

	hash := fnv.New64()
	_, err := hash.Write(hack.Slice(stmtExecInfo1.Plan))
	c.Assert(err, IsNil)
	// LRU cache should work
	for i := loops - int(maxStmtCount); i < loops; i++ {
		key := &stmtSummaryByDigestKey{
			schemaName: stmtExecInfo1.SchemaName,
			digest:     fmt.Sprintf("digest%d", i),
			planHash:   hash.Sum64(),
		}
		_, ok := sm.Get(key)
		c.Assert(ok, IsTrue)
	}
}

// Test max length of normalized and sample SQL.
func (s *testStmtSummarySuite) TestMaxSQLLength(c *C) {
	s.ssMap.Clear()

	// Create a long SQL
	maxSQLLength := config.GetGlobalConfig().StmtSummary.MaxSQLLength
	length := int(maxSQLLength) * 10
	str := strings.Repeat("a", length)

	stmtExecInfo1 := &StmtExecInfo{
		SchemaName:     "schema_name",
		OriginalSQL:    str,
		NormalizedSQL:  str,
		Digest:         "digest",
		User:           "user",
		TotalLatency:   10000,
		ParseLatency:   100,
		CompileLatency: 1000,
		TableIDs:       "1,2",
		IndexNames:     "1,2",
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
		},
		MemMax:       10000,
		Plan:         str,
		AffectedRows: 100,
		StartTime:    time.Date(2019, 1, 1, 10, 10, 10, 10, time.UTC),
	}

	s.ssMap.AddStatement(stmtExecInfo1)

	hash := fnv.New64()
	_, err := hash.Write(hack.Slice(stmtExecInfo1.Plan))
	c.Assert(err, IsNil)
	key := &stmtSummaryByDigestKey{
		schemaName: stmtExecInfo1.SchemaName,
		digest:     stmtExecInfo1.Digest,
		planHash:   hash.Sum64(),
	}
	value, ok := s.ssMap.summaryMap.Get(key)
	c.Assert(ok, IsTrue)
	// Length of normalizedSQL and sampleSQL should be maxSQLLength
	summary := value.(*stmtSummaryByDigest)
	c.Assert(len(summary.normalizedSQL), Equals, int(maxSQLLength))
	c.Assert(len(summary.sampleSQL), Equals, int(maxSQLLength))
	c.Assert(len(summary.plan), Equals, int(maxSQLLength))
}

// Test setting EnableStmtSummary to 0
func (s *testStmtSummarySuite) TestDisableStmtSummary(c *C) {
	s.ssMap.Clear()
	now := time.Now().Unix()
	s.ssMap.beginTimeForCurInterval = now
	// to disable expiring
	s.ssMap.lastCheckExpireTime = now + 60

	// Set false in global scope, it should work.
	s.ssMap.SetEnabled("0", false)

	stmtExecInfo1 := &StmtExecInfo{
		SchemaName:     "schema_name",
		OriginalSQL:    "original_sql1",
		NormalizedSQL:  "normalized_sql",
		Digest:         "digest",
		User:           "user",
		TotalLatency:   10000,
		ParseLatency:   100,
		CompileLatency: 1000,
		TableIDs:       "1,2",
		IndexNames:     "1,2",
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
		},
		MemMax:       10000,
		Plan:         "plan",
		AffectedRows: 100,
		StartTime:    time.Date(2019, 1, 1, 10, 10, 10, 10, time.UTC),
	}

	s.ssMap.AddStatement(stmtExecInfo1)
	datums := s.ssMap.ToCurrentDatum()
	c.Assert(len(datums), Equals, 0)

	// Set true in session scope, it will overwrite global scope.
	s.ssMap.SetEnabled("1", true)

	s.ssMap.AddStatement(stmtExecInfo1)
	datums = s.ssMap.ToCurrentDatum()
	c.Assert(len(datums), Equals, 1)

	// Set false in global scope, it shouldn't work.
	s.ssMap.SetEnabled("0", false)

	stmtExecInfo2 := &StmtExecInfo{
		SchemaName:     "schema_name",
		OriginalSQL:    "original_sql2",
		NormalizedSQL:  "normalized_sql2",
		Digest:         "digest2",
		User:           "user",
		TotalLatency:   10000,
		ParseLatency:   100,
		CompileLatency: 1000,
		TableIDs:       "1,2",
		IndexNames:     "1,2",
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
		},
		MemMax:       10000,
		Plan:         "plan",
		AffectedRows: 100,
		StartTime:    time.Date(2019, 1, 1, 10, 10, 10, 10, time.UTC),
	}
	s.ssMap.AddStatement(stmtExecInfo2)
	datums = s.ssMap.ToCurrentDatum()
	c.Assert(len(datums), Equals, 2)

	// Unset in session scope
	s.ssMap.SetEnabled("", true)
	s.ssMap.AddStatement(stmtExecInfo2)
	datums = s.ssMap.ToCurrentDatum()
	c.Assert(len(datums), Equals, 0)

	// Unset in global scope
	s.ssMap.SetEnabled("", false)
	s.ssMap.AddStatement(stmtExecInfo1)
	datums = s.ssMap.ToCurrentDatum()
	c.Assert(len(datums), Equals, 0)

	// Set back
	s.ssMap.SetEnabled("1", false)
}

// Set setting interval minutes and history hours.
func (s *testStmtSummarySuite) TestSetSysVars(c *C) {
	s.ssMap.SetIntervalMinutes("", true)
	s.ssMap.SetIntervalMinutes("60", false)
	c.Assert(atomic.LoadInt32(&s.ssMap.sysVars.intervalMinutes), Equals, int32(60))
	s.ssMap.SetIntervalMinutes("30", true)
	c.Assert(atomic.LoadInt32(&s.ssMap.sysVars.intervalMinutes), Equals, int32(30))
	s.ssMap.SetIntervalMinutes("", true)
	c.Assert(atomic.LoadInt32(&s.ssMap.sysVars.intervalMinutes), Equals, int32(60))

	s.ssMap.SetHistoryMaxHours("", true)
	s.ssMap.SetHistoryMaxHours("24", false)
	c.Assert(atomic.LoadInt32(&s.ssMap.sysVars.historyHours), Equals, int32(24))
	s.ssMap.SetHistoryMaxHours("12", true)
	c.Assert(atomic.LoadInt32(&s.ssMap.sysVars.historyHours), Equals, int32(12))
	s.ssMap.SetHistoryMaxHours("", true)
	c.Assert(atomic.LoadInt32(&s.ssMap.sysVars.historyHours), Equals, int32(24))
}
