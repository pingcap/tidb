package stmtsummary

import (
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
)

// Test stmtSummaryByDigestEvictedElement.ToEvictedCountDatum
// Test stmtSummaryByDigestMap.ToEvictedCountDatum
func (s *testStmtSummarySuite) TestToEvictedCountDatum(c *C) {
	s.ssMap.Clear()
	now := time.Now().Unix()
	s.ssMap.beginTimeForCurInterval = now + 60

	// set summaryMap capacity to 1.
	err := s.ssMap.summaryMap.SetCapacity(1)
	if err != nil {
		c.Assert(err.Error(), IsNil)
	}
	s.ssMap.Clear()

	sei0 := generateAnyExecInfo()
	sei1 := generateAnyExecInfo()

	sei0.SchemaName = "I'll occupy this cache! :("
	s.ssMap.AddStatement(sei0)
	n := s.ssMap.beginTimeForCurInterval
	intervalSeconds := s.ssMap.refreshInterval()
	sei1.SchemaName = "sorry, it's mine now. =)"
	s.ssMap.AddStatement(sei1)

	expectedEvictedCount := []interface{}{
		types.NewTime(types.FromGoTime(time.Unix(n, 0)), mysql.TypeTimestamp, types.DefaultFsp),
		types.NewTime(types.FromGoTime(time.Unix(n+intervalSeconds, 0)), mysql.TypeTimestamp, types.DefaultFsp),
		int64(1),
	}

	// test stmtSummaryByDigestEvictedElement.toEvictedCountDatum()
	element := s.ssMap.other.history.Front().Value.(*stmtSummaryByDigestEvictedElement)
	match(c, element.toEvictedCountDatum(), expectedEvictedCount...)

	// test stmtSummaryByDigestMap.toEvictedCountDatum
	match(c, s.ssMap.ToEvictedCountDatum()[0], expectedEvictedCount...)
}
