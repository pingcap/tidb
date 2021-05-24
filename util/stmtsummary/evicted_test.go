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
	ssMap := newStmtSummaryByDigestMap()
	ssMap.Clear()
	now := time.Now().Unix()
	ssMap.beginTimeForCurInterval = now + 60

	// set summaryMap capacity to 1.
	err := ssMap.summaryMap.SetCapacity(1)
	if err != nil {
		log.Fatal(err.Error())
	}
	ssMap.Clear()

	sei0 := generateAnyExecInfo()
	sei1 := generateAnyExecInfo()

	sei0.SchemaName = "I'll occupy this cache! :("
	ssMap.AddStatement(sei0)
	n := ssMap.beginTimeForCurInterval
	intervalSeconds := ssMap.refreshInterval()
	sei1.SchemaName = "sorry, it's mine now. =)"
	ssMap.AddStatement(sei1)

	expectedEvictedCount := []interface{}{
		types.NewTime(types.FromGoTime(time.Unix(n, 0)), mysql.TypeTimestamp, types.DefaultFsp),
		types.NewTime(types.FromGoTime(time.Unix(n+intervalSeconds, 0)), mysql.TypeTimestamp, types.DefaultFsp),
		int64(1),
	}

	// test stmtSummaryByDigestEvictedElement.toEvictedCountDatum()
	element := ssMap.other.history.Front().Value.(*stmtSummaryByDigestEvictedElement)
	match(c, element.toEvictedCountDatum(), expectedEvictedCount...)

	// test stmtSummaryByDigestMap.toEvictedCountDatum
	match(c, ssMap.ToEvictedCountDatum()[0], expectedEvictedCount...)
}

// test stmtSummaryByDigestEvicted.addEvicted
// test evicted count's detail
func (s *testStmtSummarySuite) TestEvictedCountDetailed(c *C) {
	ssMap := newStmtSummaryByDigestMap()
	ssMap.Clear()
	err := ssMap.SetRefreshInterval("60", false)
	c.Assert(err, IsNil)
	err = ssMap.SetHistorySize("100", false)
	c.Assert(err, IsNil)
	now := time.Now().Unix()
	interval := int64(60)
	ssMap.beginTimeForCurInterval = now + interval
	// set capacity to 1
	err = ssMap.summaryMap.SetCapacity(1)
	c.Assert(err, IsNil)

	// test stmtSummaryByDigest's history length
	for i := 0; i < 100; i++ {
		if i == 0 {
			c.Assert(ssMap.summaryMap.Size(), Equals, 0)
		} else {
			c.Assert(ssMap.summaryMap.Size(), Equals, 1)
			val := ssMap.summaryMap.Values()[0]
			c.Assert(val, NotNil)
			digest := val.(*stmtSummaryByDigest)
			c.Assert(digest.history.Len(), Equals, i)
		}
		ssMap.AddStatement(generateAnyExecInfo())
		ssMap.beginTimeForCurInterval += interval
	}
	ssMap.beginTimeForCurInterval -= interval

	banditSei := generateAnyExecInfo()
	banditSei.SchemaName = "kick you out >:("
	ssMap.AddStatement(banditSei)
	evictedCountDatums := ssMap.ToEvictedCountDatum()
	n := ssMap.beginTimeForCurInterval
	for _, evictedCountDatum := range evictedCountDatums {
		expectedDatum := []interface{}{
			types.NewTime(types.FromGoTime(time.Unix(n, 0)), mysql.TypeTimestamp, types.DefaultFsp),
			types.NewTime(types.FromGoTime(time.Unix(n+60, 0)), mysql.TypeTimestamp, types.DefaultFsp),
			int64(1),
		}
		match(c, evictedCountDatum, expectedDatum...)
		n -= 60
	}

	// test more than one eviction in single interval
	banditSei.SchemaName = "Yet another kicker"
	n = ssMap.beginTimeForCurInterval
	expectedDatum := []interface{}{
		types.NewTime(types.FromGoTime(time.Unix(n, 0)), mysql.TypeTimestamp, types.DefaultFsp),
		types.NewTime(types.FromGoTime(time.Unix(n+60, 0)), mysql.TypeTimestamp, types.DefaultFsp),
		int64(2),
	}
	ssMap.AddStatement(banditSei)
	evictedCountDatums = ssMap.ToEvictedCountDatum()
	match(c, evictedCountDatums[0], expectedDatum...)

	ssMap.Clear()
	other := ssMap.other
	// test poisoning with empty-history digestValue
	other.AddEvicted(new(stmtSummaryByDigestKey), new(stmtSummaryByDigest), 100)
	c.Assert(other.history.Len(), Equals, 0)
	return
}

// test stmtSummaryByDigestEvictedElement.addEvicted
func (s *testStmtSummarySuite) TestEvictCountMultiple(c *C) {
	ssMap := newStmtSummaryByDigestMap()
	err := ssMap.SetRefreshInterval("60", false)
	c.Assert(err, IsNil)
	err = ssMap.SetMaxStmtCount("1", false)
	c.Assert(err, IsNil)
	err = ssMap.SetHistorySize("100", false)
	c.Assert(err, IsNil)

	now := time.Now().Unix()
	interval := int64(60)
	ssMap.beginTimeForCurInterval = now + interval
	// insert one statement every other interval.
	for i := 0; i < 50; i++ {
		ssMap.AddStatement(generateAnyExecInfo())
		ssMap.beginTimeForCurInterval += interval * 2
	}
	c.Assert(ssMap.summaryMap.Size(), Equals, 1)
	val := ssMap.summaryMap.Values()[0]
	c.Assert(val, NotNil)
	digest := val.(*stmtSummaryByDigest)
	c.Assert(digest.history.Len(), Equals, 50)

	err = ssMap.SetHistorySize("100", false)
	c.Assert(err, IsNil)
	// update begin time
	ssMap.beginTimeForCurInterval += interval * 2
	banditSei := generateAnyExecInfo()
	banditSei.SchemaName = "Kick you out >:("
	ssMap.AddStatement(banditSei)

	evictedCountDatums := ssMap.ToEvictedCountDatum()
	c.Assert(len(evictedCountDatums), Equals, 50)

	// update begin time
	ssMap.beginTimeForCurInterval += interval * 2
	banditSei.SchemaName = "Yet another kicker"
	ssMap.AddStatement(banditSei)

	evictedCountDatums = ssMap.ToEvictedCountDatum()
	c.Assert(len(evictedCountDatums), Equals, 51)
}

// test stmtSummaryByDigestEvictedElement.matchAndAdd
// test stmtSummaryByDigestEvictedElement.addEvicted
func (s *testStmtSummarySuite) TestEvictedElementAdd(c *C) {
	ssMap := newStmtSummaryByDigestMap()
	ssMap.Clear()
	now := time.Now().Unix()
	ssMap.beginTimeForCurInterval = now + 60
	// set capacity to 1
	err := ssMap.summaryMap.SetCapacity(1)
	c.Assert(err, IsNil)

	ssMap.AddStatement(generateAnyExecInfo())
	digestKeys := make([]*stmtSummaryByDigestKey, 0)
	digestValues := make([]*stmtSummaryByDigest, 0)
	for _, k := range ssMap.summaryMap.Keys() {
		digestKeys = append(digestKeys, k.(*stmtSummaryByDigestKey))
	}
	for _, v := range ssMap.summaryMap.Values() {
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
