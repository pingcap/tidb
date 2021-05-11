package stmtsummary

import (
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
	"time"

	. "github.com/pingcap/check"
)

// Test STATEMENTS_SUMMARY_EVICTED
func (s *testStmtSummarySuite) TestEvictedCountDatum(c *C) {
	s.ssMap.Clear()
	s.ssMap.refreshInterval()

	// set summaryMap capacity to 1.
	err := s.ssMap.summaryMap.SetCapacity(1)
	if err != nil {
		c.Assert(err.Error(), IsNil)
	}
	s.ssMap.Clear()

	sei0 := generateAnyExecInfo()
	sei0.SchemaName = "schema_00"
	s.ssMap.AddStatement(sei0)
	n := s.ssMap.beginTimeForCurInterval
	intervalSeconds := s.ssMap.refreshInterval()
	s.ssMap.AddStatement(generateAnyExecInfo())

	expectedEvictedCount := []interface{}{
		types.NewTime(types.FromGoTime(time.Unix(n, 0)), mysql.TypeTimestamp, types.DefaultFsp),
		types.NewTime(types.FromGoTime(time.Unix(n+intervalSeconds, 0)), mysql.TypeTimestamp, types.DefaultFsp),
		int64(1),
	}
	match(c, s.ssMap.other.ToEvictedCountDatum()[0], expectedEvictedCount...)
}
