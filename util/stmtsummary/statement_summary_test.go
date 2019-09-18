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
	"github.com/pingcap/tidb/types"
)

var _ = Suite(&testStmtSummarySuite{})

type testStmtSummarySuite struct {
	ssMap *stmtSummaryByDigestMap
}

func (s *testStmtSummarySuite) SetUpSuite(c *C) {
	s.ssMap = newStmtSummaryByDigestMap()
	s.ssMap.SetEnabled("1", false)
}

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

// Test stmtSummaryByDigest.AddStatement
func (s *testStmtSummarySuite) TestAddStatement(c *C) {
	s.ssMap.Clear()

	// First statement
	stmtExecInfo1 := &StmtExecInfo{
		SchemaName:    "schema_name",
		OriginalSQL:   "original_sql1",
		NormalizedSQL: "normalized_sql",
		Digest:        "digest",
		TotalLatency:  10000,
		AffectedRows:  100,
		SentRows:      100,
		StartTime:     time.Date(2019, 1, 1, 10, 10, 10, 10, time.UTC),
	}
	key := &stmtSummaryByDigestKey{
		schemaName: stmtExecInfo1.SchemaName,
		digest:     stmtExecInfo1.Digest,
	}
	expectedSummary := stmtSummaryByDigest{
		schemaName:      stmtExecInfo1.SchemaName,
		digest:          stmtExecInfo1.Digest,
		normalizedSQL:   stmtExecInfo1.NormalizedSQL,
		sampleSQL:       stmtExecInfo1.OriginalSQL,
		execCount:       1,
		sumLatency:      stmtExecInfo1.TotalLatency,
		maxLatency:      stmtExecInfo1.TotalLatency,
		minLatency:      stmtExecInfo1.TotalLatency,
		sumAffectedRows: stmtExecInfo1.AffectedRows,
		sumSentRows:     stmtExecInfo1.SentRows,
		firstSeen:       stmtExecInfo1.StartTime,
		lastSeen:        stmtExecInfo1.StartTime,
	}

	s.ssMap.AddStatement(stmtExecInfo1)
	summary, ok := s.ssMap.summaryMap.Get(key)
	c.Assert(ok, IsTrue)
	c.Assert(*summary.(*stmtSummaryByDigest) == expectedSummary, IsTrue)

	// Second statement
	stmtExecInfo2 := &StmtExecInfo{
		SchemaName:    "schema_name",
		OriginalSQL:   "original_sql2",
		NormalizedSQL: "normalized_sql",
		Digest:        "digest",
		TotalLatency:  50000,
		AffectedRows:  500,
		SentRows:      500,
		StartTime:     time.Date(2019, 1, 1, 10, 10, 20, 10, time.UTC),
	}
	expectedSummary.execCount++
	expectedSummary.sumLatency += stmtExecInfo2.TotalLatency
	expectedSummary.maxLatency = stmtExecInfo2.TotalLatency
	expectedSummary.sumAffectedRows += stmtExecInfo2.AffectedRows
	expectedSummary.sumSentRows += stmtExecInfo2.SentRows
	expectedSummary.lastSeen = stmtExecInfo2.StartTime

	s.ssMap.AddStatement(stmtExecInfo2)
	summary, ok = s.ssMap.summaryMap.Get(key)
	c.Assert(ok, IsTrue)
	c.Assert(*summary.(*stmtSummaryByDigest) == expectedSummary, IsTrue)

	// Third statement
	stmtExecInfo3 := &StmtExecInfo{
		SchemaName:    "schema_name",
		OriginalSQL:   "original_sql3",
		NormalizedSQL: "normalized_sql",
		Digest:        "digest",
		TotalLatency:  1000,
		AffectedRows:  10,
		SentRows:      10,
		StartTime:     time.Date(2019, 1, 1, 10, 10, 0, 10, time.UTC),
	}
	expectedSummary.execCount++
	expectedSummary.sumLatency += stmtExecInfo3.TotalLatency
	expectedSummary.minLatency = stmtExecInfo3.TotalLatency
	expectedSummary.sumAffectedRows += stmtExecInfo3.AffectedRows
	expectedSummary.sumSentRows += stmtExecInfo3.SentRows
	expectedSummary.firstSeen = stmtExecInfo3.StartTime

	s.ssMap.AddStatement(stmtExecInfo3)
	summary, ok = s.ssMap.summaryMap.Get(key)
	c.Assert(ok, IsTrue)
	c.Assert(*summary.(*stmtSummaryByDigest) == expectedSummary, IsTrue)

	// Fourth statement that in a different schema
	stmtExecInfo4 := &StmtExecInfo{
		SchemaName:    "schema_name2",
		OriginalSQL:   "original_sql1",
		NormalizedSQL: "normalized_sql",
		Digest:        "digest",
		TotalLatency:  1000,
		AffectedRows:  10,
		SentRows:      10,
		StartTime:     time.Date(2019, 1, 1, 10, 10, 0, 10, time.UTC),
	}
	key = &stmtSummaryByDigestKey{
		schemaName: stmtExecInfo4.SchemaName,
		digest:     stmtExecInfo4.Digest,
	}

	s.ssMap.AddStatement(stmtExecInfo4)
	c.Assert(s.ssMap.summaryMap.Size(), Equals, 2)
	_, ok = s.ssMap.summaryMap.Get(key)
	c.Assert(ok, IsTrue)

	// Fifth statement that has a different digest
	stmtExecInfo5 := &StmtExecInfo{
		SchemaName:    "schema_name",
		OriginalSQL:   "original_sql1",
		NormalizedSQL: "normalized_sql2",
		Digest:        "digest2",
		TotalLatency:  1000,
		AffectedRows:  10,
		SentRows:      10,
		StartTime:     time.Date(2019, 1, 1, 10, 10, 0, 10, time.UTC),
	}
	key = &stmtSummaryByDigestKey{
		schemaName: stmtExecInfo5.SchemaName,
		digest:     stmtExecInfo5.Digest,
	}

	s.ssMap.AddStatement(stmtExecInfo5)
	c.Assert(s.ssMap.summaryMap.Size(), Equals, 3)
	_, ok = s.ssMap.summaryMap.Get(key)
	c.Assert(ok, IsTrue)
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

	stmtExecInfo1 := &StmtExecInfo{
		SchemaName:    "schema_name",
		OriginalSQL:   "original_sql1",
		NormalizedSQL: "normalized_sql",
		Digest:        "digest",
		TotalLatency:  10000,
		AffectedRows:  100,
		SentRows:      100,
		StartTime:     time.Date(2019, 1, 1, 10, 10, 10, 10, time.UTC),
	}
	s.ssMap.AddStatement(stmtExecInfo1)
	datums := s.ssMap.ToDatum()
	c.Assert(len(datums), Equals, 1)
	t := types.Time{Time: types.FromGoTime(stmtExecInfo1.StartTime), Type: mysql.TypeTimestamp}
	match(c, datums[0], stmtExecInfo1.SchemaName, stmtExecInfo1.Digest, stmtExecInfo1.NormalizedSQL,
		1, stmtExecInfo1.TotalLatency, stmtExecInfo1.TotalLatency, stmtExecInfo1.TotalLatency, stmtExecInfo1.TotalLatency,
		stmtExecInfo1.AffectedRows, t, t, stmtExecInfo1.OriginalSQL)
}

// Test AddStatement and ToDatum parallel
func (s *testStmtSummarySuite) TestAddStatementParallel(c *C) {
	s.ssMap.Clear()

	threads := 8
	loops := 32
	wg := sync.WaitGroup{}
	wg.Add(threads)

	addStmtFunc := func() {
		defer wg.Done()
		stmtExecInfo1 := &StmtExecInfo{
			SchemaName:    "schema_name",
			OriginalSQL:   "original_sql1",
			NormalizedSQL: "normalized_sql",
			Digest:        "digest",
			TotalLatency:  10000,
			AffectedRows:  100,
			SentRows:      100,
			StartTime:     time.Date(2019, 1, 1, 10, 10, 10, 10, time.UTC),
		}

		// Add 32 times with different digest
		for i := 0; i < loops; i++ {
			stmtExecInfo1.Digest = fmt.Sprintf("digest%d", i)
			s.ssMap.AddStatement(stmtExecInfo1)
		}

		// There would be 32 summaries
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

	stmtExecInfo1 := &StmtExecInfo{
		SchemaName:    "schema_name",
		OriginalSQL:   "original_sql1",
		NormalizedSQL: "normalized_sql",
		Digest:        "digest",
		TotalLatency:  10000,
		AffectedRows:  100,
		SentRows:      100,
		StartTime:     time.Date(2019, 1, 1, 10, 10, 10, 10, time.UTC),
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

	// LRU cache should work
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

	// Create a long SQL
	maxSQLLength := config.GetGlobalConfig().StmtSummary.MaxSQLLength
	length := int(maxSQLLength) * 10
	str := strings.Repeat("a", length)

	stmtExecInfo1 := &StmtExecInfo{
		SchemaName:    "schema_name",
		OriginalSQL:   str,
		NormalizedSQL: str,
		Digest:        "digest",
		TotalLatency:  10000,
		AffectedRows:  100,
		SentRows:      100,
		StartTime:     time.Date(2019, 1, 1, 10, 10, 10, 10, time.UTC),
	}

	s.ssMap.AddStatement(stmtExecInfo1)
	key := &stmtSummaryByDigestKey{
		schemaName: stmtExecInfo1.SchemaName,
		digest:     stmtExecInfo1.Digest,
	}
	value, ok := s.ssMap.summaryMap.Get(key)
	c.Assert(ok, IsTrue)
	// Length of normalizedSQL and sampleSQL should be maxSQLLength
	summary := value.(*stmtSummaryByDigest)
	c.Assert(len(summary.normalizedSQL), Equals, int(maxSQLLength))
	c.Assert(len(summary.sampleSQL), Equals, int(maxSQLLength))
}

// Test setting EnableStmtSummary to 0
func (s *testStmtSummarySuite) TestDisableStmtSummary(c *C) {
	s.ssMap.Clear()
	// Set false in global scope, it should work.
	s.ssMap.SetEnabled("0", false)

	stmtExecInfo1 := &StmtExecInfo{
		SchemaName:    "schema_name",
		OriginalSQL:   "original_sql1",
		NormalizedSQL: "normalized_sql",
		Digest:        "digest",
		TotalLatency:  10000,
		AffectedRows:  100,
		SentRows:      100,
		StartTime:     time.Date(2019, 1, 1, 10, 10, 10, 10, time.UTC),
	}

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

	stmtExecInfo2 := &StmtExecInfo{
		SchemaName:    "schema_name",
		OriginalSQL:   "original_sql2",
		NormalizedSQL: "normalized_sql2",
		Digest:        "digest2",
		TotalLatency:  50000,
		AffectedRows:  500,
		SentRows:      500,
		StartTime:     time.Date(2019, 1, 1, 10, 10, 20, 10, time.UTC),
	}
	s.ssMap.AddStatement(stmtExecInfo2)
	datums = s.ssMap.ToDatum()
	c.Assert(len(datums), Equals, 2)

	// Unset in session scope
	s.ssMap.SetEnabled("", true)
	s.ssMap.AddStatement(stmtExecInfo2)
	datums = s.ssMap.ToDatum()
	c.Assert(len(datums), Equals, 0)

	// Unset in global scope
	s.ssMap.SetEnabled("", false)
	s.ssMap.AddStatement(stmtExecInfo1)
	datums = s.ssMap.ToDatum()
	c.Assert(len(datums), Equals, 0)

	// Set back
	s.ssMap.SetEnabled("1", false)
}
