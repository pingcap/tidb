package stmtsummary

import (
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/log"
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
		log.Fatal(err.Error())
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

// test stmtSummaryByDigestEvictedElement.matchAndAdd
// test stmtSummaryByDigestEvictedElement.addEvicted
func (s *testStmtSummarySuite) TestEvictedElementAdd(c *C) {
	s.ssMap.Clear()
	now := time.Now().Unix()
	s.ssMap.beginTimeForCurInterval = now + 60
	// set capacity to 1
	err := s.ssMap.summaryMap.SetCapacity(1)
	c.Assert(err, IsNil)

	s.ssMap.AddStatement(generateAnyExecInfo())
	digestKeys := make([]*stmtSummaryByDigestKey, 0)
	digestValues := make([]*stmtSummaryByDigest, 0)
	for _, k := range s.ssMap.summaryMap.Keys() {
		digestKeys = append(digestKeys, k.(*stmtSummaryByDigestKey))
	}
	for _, v := range s.ssMap.summaryMap.Values() {
		digestValues = append(digestValues, v.(*stmtSummaryByDigest))
	}
	c.Assert(digestKeys[0], NotNil)
	c.Assert(digestValues[0], NotNil)
	c.Assert(digestValues[0].history, NotNil)
	c.Assert(digestValues[0].history.Back(), NotNil)

	stmtEvictedElement := newStmtSummaryByDigestEvictedElement(now, now+60)
	digestKey := digestKeys[0]
	digestElement := digestValues[0].history.Back().Value.(*stmtSummaryByDigestElement)

	// test add NULL values
	stmtEvictedElement.matchAndAdd(nil, nil)
	c.Assert(len(stmtEvictedElement.digestKeyMap), Equals, 0)
	stmtEvictedElement.matchAndAdd(digestKey, nil)
	c.Assert(len(stmtEvictedElement.digestKeyMap), Equals, 0)
	stmtEvictedElement.matchAndAdd(nil, digestElement)
	c.Assert(len(stmtEvictedElement.digestKeyMap), Equals, 0)

	// test matchAndAdd
	c.Assert(stmtEvictedElement.matchAndAdd(digestKey, digestElement), Equals, isTooYoung)
	digestElement.beginTime, digestElement.endTime = now-60, now
	c.Assert(stmtEvictedElement.matchAndAdd(digestKey, digestElement), Equals, isTooOld)
	digestElement.beginTime, digestElement.endTime = now, now+60
	c.Assert(stmtEvictedElement.matchAndAdd(digestKey, digestElement), Equals, isMatch)
	c.Assert(len(stmtEvictedElement.digestKeyMap), Equals, 1)

	// test clear
	s.ssMap.Clear()
	c.Assert(s.ssMap.other.history.Len(), Equals, 0)
}

func (s *testStmtSummarySuite) TestNewStmtSummaryByDigestEvictedElement(c *C) {
	now := time.Now().Unix()
	end := now + 60
	stmtEvictedElement := newStmtSummaryByDigestEvictedElement(now, end)
	c.Assert(stmtEvictedElement.beginTime, Equals, now)
	c.Assert(stmtEvictedElement.endTime, Equals, end)
	c.Assert(len(stmtEvictedElement.digestKeyMap), Equals, 0)
}

func (s *testStmtSummarySuite) TestStmtSummaryByDigestEvicted(c *C) {
	stmtEvicted := newStmtSummaryByDigestEvicted()
	c.Assert(stmtEvicted.history.Len(), Equals, 0)
}
