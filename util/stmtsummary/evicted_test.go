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
// See the License for the specific language governing permissions and
// limitations under the License.

package stmtsummary

import (
	"bytes"
	"container/list"
	"fmt"
	"reflect"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
)

// fake a stmtSummaryByDigest
func newInduceSsbd(beginTime int64, endTime int64) *stmtSummaryByDigest {
	newSsbd := &stmtSummaryByDigest{
		history: list.New(),
	}
	newSsbd.history.PushBack(newInduceSsbde(beginTime, endTime))
	return newSsbd
}

// fake a stmtSummaryByDigestElement
func newInduceSsbde(beginTime int64, endTime int64) *stmtSummaryByDigestElement {
	newSsbde := &stmtSummaryByDigestElement{
		beginTime:  beginTime,
		endTime:    endTime,
		minLatency: time.Duration.Round(1<<63-1, time.Nanosecond),
	}
	return newSsbde
}

// generate new stmtSummaryByDigestKey and stmtSummaryByDigest
func (s *testStmtSummarySuite) generateStmtSummaryByDigestKeyValue(schema string, beginTime int64, endTime int64) (*stmtSummaryByDigestKey, *stmtSummaryByDigest) {
	key := &stmtSummaryByDigestKey{
		schemaName: schema,
	}
	value := newInduceSsbd(beginTime, endTime)
	return key, value
}

// Test stmtSummaryByDigestMap.ToEvictedCountDatum
func (s *testStmtSummarySuite) TestMapToEvictedCountDatum(c *C) {
	ssMap := newStmtSummaryByDigestMap()
	ssMap.Clear()
	now := time.Now().Unix()
	interval := ssMap.refreshInterval()
	ssMap.beginTimeForCurInterval = now + interval

	// set summaryMap's capacity to 1.
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
	sei1.SchemaName = "sorry, it's mine now. =)"
	ssMap.AddStatement(sei1)

	expectedEvictedCount := []interface{}{
		types.NewTime(types.FromGoTime(time.Unix(n, 0)), mysql.TypeTimestamp, types.DefaultFsp),
		types.NewTime(types.FromGoTime(time.Unix(n+interval, 0)), mysql.TypeTimestamp, types.DefaultFsp),
		int64(1),
	}

	// test stmtSummaryByDigestMap.toEvictedCountDatum
	match(c, ssMap.ToEvictedCountDatum()[0], expectedEvictedCount...)

	// test multiple intervals
	ssMap.Clear()
	err = ssMap.SetRefreshInterval("60", false)
	interval = ssMap.refreshInterval()
	c.Assert(err, IsNil)
	err = ssMap.SetMaxStmtCount("1", false)
	c.Assert(err, IsNil)
	err = ssMap.SetHistorySize("100", false)
	c.Assert(err, IsNil)

	ssMap.beginTimeForCurInterval = now + interval
	// insert one statement per interval.
	for i := 0; i < 50; i++ {
		ssMap.AddStatement(generateAnyExecInfo())
		ssMap.beginTimeForCurInterval += interval * 2
	}
	c.Assert(ssMap.summaryMap.Size(), Equals, 1)
	val := ssMap.summaryMap.Values()[0]
	c.Assert(val, NotNil)
	digest := val.(*stmtSummaryByDigest)
	c.Assert(digest.history.Len(), Equals, 50)

	err = ssMap.SetHistorySize("25", false)
	c.Assert(err, IsNil)
	// update begin time
	ssMap.beginTimeForCurInterval += interval * 2
	banditSei := generateAnyExecInfo()
	banditSei.SchemaName = "Kick you out >:("
	ssMap.AddStatement(banditSei)

	evictedCountDatums := ssMap.ToEvictedCountDatum()
	c.Assert(len(evictedCountDatums), Equals, 25)

	// update begin time
	banditSei.SchemaName = "Yet another kicker"
	ssMap.AddStatement(banditSei)

	evictedCountDatums = ssMap.ToEvictedCountDatum()
	// test young digest
	c.Assert(len(evictedCountDatums), Equals, 25)
	n = ssMap.beginTimeForCurInterval
	newlyEvicted := evictedCountDatums[0]
	expectedEvictedCount = []interface{}{
		types.NewTime(types.FromGoTime(time.Unix(n, 0)), mysql.TypeTimestamp, types.DefaultFsp),
		types.NewTime(types.FromGoTime(time.Unix(n+interval, 0)), mysql.TypeTimestamp, types.DefaultFsp),
		int64(1),
	}
	match(c, newlyEvicted, expectedEvictedCount...)
}

// Test stmtSummaryByDigestEvicted
func (s *testStmtSummarySuite) TestSimpleStmtSummaryByDigestEvicted(c *C) {
	ssbde := newStmtSummaryByDigestEvicted()
	evictedKey, evictedValue := s.generateStmtSummaryByDigestKeyValue("a", 1, 2)

	// test NULL
	ssbde.AddEvicted(nil, nil, 10)
	c.Assert(ssbde.history.Len(), Equals, 0)
	ssbde.Clear()
	// passing NULL key is used as *refresh*.
	ssbde.AddEvicted(nil, evictedValue, 10)
	c.Assert(ssbde.history.Len(), Equals, 1)
	ssbde.Clear()
	ssbde.AddEvicted(evictedKey, nil, 10)
	c.Assert(ssbde.history.Len(), Equals, 0)
	ssbde.Clear()

	// test zero historySize
	ssbde.AddEvicted(evictedKey, evictedValue, 0)
	c.Assert(ssbde.history.Len(), Equals, 0)

	ssbde = newStmtSummaryByDigestEvicted()
	ssbde.AddEvicted(evictedKey, evictedValue, 1)
	c.Assert(getAllEvicted(ssbde), Equals, "{begin: 1, end: 2, count: 1}")
	// test insert same *kind* of digest
	ssbde.AddEvicted(evictedKey, evictedValue, 1)
	c.Assert(getAllEvicted(ssbde), Equals, "{begin: 1, end: 2, count: 1}")

	evictedKey, evictedValue = s.generateStmtSummaryByDigestKeyValue("b", 1, 2)
	ssbde.AddEvicted(evictedKey, evictedValue, 1)
	c.Assert(getAllEvicted(ssbde), Equals, "{begin: 1, end: 2, count: 2}")

	evictedKey, evictedValue = s.generateStmtSummaryByDigestKeyValue("b", 5, 6)
	ssbde.AddEvicted(evictedKey, evictedValue, 2)
	c.Assert(getAllEvicted(ssbde), Equals, "{begin: 5, end: 6, count: 1}, {begin: 1, end: 2, count: 2}")

	evictedKey, evictedValue = s.generateStmtSummaryByDigestKeyValue("b", 3, 4)
	ssbde.AddEvicted(evictedKey, evictedValue, 3)
	c.Assert(getAllEvicted(ssbde), Equals, "{begin: 5, end: 6, count: 1}, {begin: 3, end: 4, count: 1}, {begin: 1, end: 2, count: 2}")

	// test evicted element with multi-time range value.
	ssbde = newStmtSummaryByDigestEvicted()
	evictedKey, evictedValue = s.generateStmtSummaryByDigestKeyValue("a", 1, 2)
	evictedValue.history.PushBack(newInduceSsbde(2, 3))
	evictedValue.history.PushBack(newInduceSsbde(5, 6))
	evictedValue.history.PushBack(newInduceSsbde(8, 9))
	ssbde.AddEvicted(evictedKey, evictedValue, 3)
	c.Assert(getAllEvicted(ssbde), Equals, "{begin: 8, end: 9, count: 1}, {begin: 5, end: 6, count: 1}, {begin: 2, end: 3, count: 1}")

	evictedKey = &stmtSummaryByDigestKey{schemaName: "b"}
	ssbde.AddEvicted(evictedKey, evictedValue, 4)
	c.Assert(getAllEvicted(ssbde), Equals, "{begin: 8, end: 9, count: 2}, {begin: 5, end: 6, count: 2}, {begin: 2, end: 3, count: 2}, {begin: 1, end: 2, count: 1}")

	evictedKey, evictedValue = s.generateStmtSummaryByDigestKeyValue("c", 4, 5)
	evictedValue.history.PushBack(newInduceSsbde(5, 6))
	evictedValue.history.PushBack(newInduceSsbde(7, 8))
	ssbde.AddEvicted(evictedKey, evictedValue, 4)
	c.Assert(getAllEvicted(ssbde), Equals, "{begin: 8, end: 9, count: 2}, {begin: 7, end: 8, count: 1}, {begin: 5, end: 6, count: 3}, {begin: 4, end: 5, count: 1}")

	evictedKey, evictedValue = s.generateStmtSummaryByDigestKeyValue("d", 7, 8)
	ssbde.AddEvicted(evictedKey, evictedValue, 4)
	c.Assert(getAllEvicted(ssbde), Equals, "{begin: 8, end: 9, count: 2}, {begin: 7, end: 8, count: 2}, {begin: 5, end: 6, count: 3}, {begin: 4, end: 5, count: 1}")

	// test for too old
	evictedKey, evictedValue = s.generateStmtSummaryByDigestKeyValue("d", 0, 1)
	evictedValue.history.PushBack(newInduceSsbde(1, 2))
	evictedValue.history.PushBack(newInduceSsbde(2, 3))
	evictedValue.history.PushBack(newInduceSsbde(4, 5))
	ssbde.AddEvicted(evictedKey, evictedValue, 4)
	c.Assert(getAllEvicted(ssbde), Equals, "{begin: 8, end: 9, count: 2}, {begin: 7, end: 8, count: 2}, {begin: 5, end: 6, count: 3}, {begin: 4, end: 5, count: 2}")

	// test for too young
	evictedKey, evictedValue = s.generateStmtSummaryByDigestKeyValue("d", 1, 2)
	evictedValue.history.PushBack(newInduceSsbde(9, 10))
	ssbde.AddEvicted(evictedKey, evictedValue, 4)
	c.Assert(getAllEvicted(ssbde), Equals, "{begin: 9, end: 10, count: 1}, {begin: 8, end: 9, count: 2}, {begin: 7, end: 8, count: 2}, {begin: 5, end: 6, count: 3}")
}

// Test stmtSummaryByDigestEvictedElement.ToEvictedCountDatum
func (s *testStmtSummarySuite) TestStmtSummaryByDigestEvictedElement(c *C) {
	record := newStmtSummaryByDigestEvictedElement(0, 1)
	evictedKey, evictedValue := s.generateStmtSummaryByDigestKeyValue("alpha", 0, 1)
	digestValue := evictedValue.history.Back().Value.(*stmtSummaryByDigestElement)

	// test poisoning will NULL key.
	record.addEvicted(nil, nil)
	c.Assert(getEvicted(record), Equals, "{begin: 0, end: 1, count: 0}")
	record.addEvicted(nil, digestValue)
	c.Assert(getEvicted(record), Equals, "{begin: 0, end: 1, count: 0}")

	// test add evictedKey and evicted stmtSummaryByDigestElement
	record.addEvicted(evictedKey, digestValue)
	c.Assert(getEvicted(record), Equals, "{begin: 0, end: 1, count: 1}")

	// test add same *kind* of values.
	record.addEvicted(evictedKey, digestValue)
	c.Assert(getEvicted(record), Equals, "{begin: 0, end: 1, count: 1}")

	// test add different *kind* of values.
	evictedKey, evictedValue = s.generateStmtSummaryByDigestKeyValue("bravo", 0, 1)
	digestValue = evictedValue.history.Back().Value.(*stmtSummaryByDigestElement)
	record.addEvicted(evictedKey, digestValue)
	c.Assert(getEvicted(record), Equals, "{begin: 0, end: 1, count: 2}")
}

// test stmtSummaryByDigestEvicted.addEvicted
// test stmtSummaryByDigestEvicted.toEvictedCountDatum (single and multiple intervals)
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

// test addInfo function
func (s *testStmtSummarySuite) TestAddInfo(c *C) {
	now := time.Now().Unix()
	addTo := stmtSummaryByDigestElement{
		// user
		authUsers: map[string]struct{}{"a": {}},

		// execCount and sumWarnings
		execCount:   3,
		sumWarnings: 8,

		// latency
		sumLatency:        8,
		maxLatency:        5,
		minLatency:        1,
		sumParseLatency:   3,
		maxParseLatency:   2,
		sumCompileLatency: 3,
		maxCompileLatency: 2,

		// coprocessor
		sumNumCopTasks:       4,
		maxCopProcessTime:    4,
		maxCopProcessAddress: "19.19.8.10",
		maxCopWaitTime:       4,
		maxCopWaitAddress:    "19.19.8.10",

		// TiKV
		sumProcessTime: 1,
		maxProcessTime: 1,
		sumWaitTime:    2,
		maxWaitTime:    1,
		sumBackoffTime: 2,
		maxBackoffTime: 2,

		sumTotalKeys:                 3,
		maxTotalKeys:                 2,
		sumProcessedKeys:             8,
		maxProcessedKeys:             4,
		sumRocksdbDeleteSkippedCount: 8,
		maxRocksdbDeleteSkippedCount: 2,

		sumRocksdbKeySkippedCount:    8,
		maxRocksdbKeySkippedCount:    3,
		sumRocksdbBlockCacheHitCount: 8,
		maxRocksdbBlockCacheHitCount: 3,
		sumRocksdbBlockReadCount:     3,
		maxRocksdbBlockReadCount:     3,
		sumRocksdbBlockReadByte:      4,
		maxRocksdbBlockReadByte:      4,

		// txn
		commitCount:          8,
		sumPrewriteTime:      3,
		maxPrewriteTime:      3,
		sumCommitTime:        8,
		maxCommitTime:        5,
		sumGetCommitTsTime:   8,
		maxGetCommitTsTime:   8,
		sumCommitBackoffTime: 8,
		maxCommitBackoffTime: 8,

		sumResolveLockTime:   8,
		maxResolveLockTime:   8,
		sumLocalLatchTime:    8,
		maxLocalLatchTime:    8,
		sumWriteKeys:         8,
		maxWriteKeys:         8,
		sumWriteSize:         8,
		maxWriteSize:         8,
		sumPrewriteRegionNum: 8,
		maxPrewriteRegionNum: 8,
		sumTxnRetry:          8,
		maxTxnRetry:          8,
		sumBackoffTimes:      8,
		backoffTypes:         map[string]int{},

		// plan cache
		planCacheHits: 8,

		// other
		sumAffectedRows:      8,
		sumMem:               8,
		maxMem:               8,
		sumDisk:              8,
		maxDisk:              8,
		firstSeen:            time.Unix(now-10, 0),
		lastSeen:             time.Unix(now-8, 0),
		execRetryCount:       8,
		execRetryTime:        8,
		sumKVTotal:           2,
		sumPDTotal:           2,
		sumBackoffTotal:      2,
		sumWriteSQLRespTotal: 100,
		sumErrors:            8,
	}

	addWith := stmtSummaryByDigestElement{
		// user
		authUsers: map[string]struct{}{"a": {}},

		// execCount and sumWarnings
		execCount:   3,
		sumWarnings: 8,

		// latency
		sumLatency:        8,
		maxLatency:        5,
		minLatency:        1,
		sumParseLatency:   3,
		maxParseLatency:   2,
		sumCompileLatency: 3,
		maxCompileLatency: 2,

		// coprocessor
		sumNumCopTasks:       4,
		maxCopProcessTime:    4,
		maxCopProcessAddress: "19.19.8.10",
		maxCopWaitTime:       4,
		maxCopWaitAddress:    "19.19.8.10",

		// TiKV
		sumProcessTime: 1,
		maxProcessTime: 1,
		sumWaitTime:    2,
		maxWaitTime:    1,
		sumBackoffTime: 2,
		maxBackoffTime: 2,

		sumTotalKeys:                 3,
		maxTotalKeys:                 2,
		sumProcessedKeys:             8,
		maxProcessedKeys:             4,
		sumRocksdbDeleteSkippedCount: 8,
		maxRocksdbDeleteSkippedCount: 2,

		sumRocksdbKeySkippedCount:    8,
		maxRocksdbKeySkippedCount:    3,
		sumRocksdbBlockCacheHitCount: 8,
		maxRocksdbBlockCacheHitCount: 3,
		sumRocksdbBlockReadCount:     3,
		maxRocksdbBlockReadCount:     3,
		sumRocksdbBlockReadByte:      4,
		maxRocksdbBlockReadByte:      4,

		// txn
		commitCount:          8,
		sumPrewriteTime:      3,
		maxPrewriteTime:      3,
		sumCommitTime:        8,
		maxCommitTime:        5,
		sumGetCommitTsTime:   8,
		maxGetCommitTsTime:   8,
		sumCommitBackoffTime: 8,
		maxCommitBackoffTime: 8,

		sumResolveLockTime:   8,
		maxResolveLockTime:   8,
		sumLocalLatchTime:    8,
		maxLocalLatchTime:    8,
		sumWriteKeys:         8,
		maxWriteKeys:         8,
		sumWriteSize:         8,
		maxWriteSize:         8,
		sumPrewriteRegionNum: 8,
		maxPrewriteRegionNum: 8,
		sumTxnRetry:          8,
		maxTxnRetry:          8,
		sumBackoffTimes:      8,
		backoffTypes:         map[string]int{},

		// plan cache
		planCacheHits: 8,

		// other
		sumAffectedRows:      8,
		sumMem:               8,
		maxMem:               8,
		sumDisk:              8,
		maxDisk:              8,
		firstSeen:            time.Unix(now-10, 0),
		lastSeen:             time.Unix(now-8, 0),
		execRetryCount:       8,
		execRetryTime:        8,
		sumKVTotal:           2,
		sumPDTotal:           2,
		sumBackoffTotal:      2,
		sumWriteSQLRespTotal: 100,
		sumErrors:            8,
	}
	addWith.authUsers["b"] = struct{}{}
	addWith.maxCopProcessTime = 15
	addWith.maxCopProcessAddress = "1.14.5.14"
	addWith.firstSeen = time.Unix(now-20, 0)
	addWith.lastSeen = time.Unix(now, 0)

	addInfo(&addTo, &addWith)

	expectedSum := stmtSummaryByDigestElement{
		// user
		authUsers: map[string]struct{}{"a": {}, "b": {}},

		// execCount and sumWarnings
		execCount:   6,
		sumWarnings: 16,

		// latency
		sumLatency:        16,
		maxLatency:        5,
		minLatency:        1,
		sumParseLatency:   6,
		maxParseLatency:   2,
		sumCompileLatency: 6,
		maxCompileLatency: 2,

		// coprocessor
		sumNumCopTasks:       8,
		maxCopProcessTime:    15,
		maxCopProcessAddress: "1.14.5.14",
		maxCopWaitTime:       4,
		maxCopWaitAddress:    "19.19.8.10",

		// TiKV
		sumProcessTime: 2,
		maxProcessTime: 1,
		sumWaitTime:    4,
		maxWaitTime:    1,
		sumBackoffTime: 4,
		maxBackoffTime: 2,

		sumTotalKeys:                 6,
		maxTotalKeys:                 2,
		sumProcessedKeys:             16,
		maxProcessedKeys:             4,
		sumRocksdbDeleteSkippedCount: 16,
		maxRocksdbDeleteSkippedCount: 2,

		sumRocksdbKeySkippedCount:    16,
		maxRocksdbKeySkippedCount:    3,
		sumRocksdbBlockCacheHitCount: 16,
		maxRocksdbBlockCacheHitCount: 3,
		sumRocksdbBlockReadCount:     6,
		maxRocksdbBlockReadCount:     3,
		sumRocksdbBlockReadByte:      8,
		maxRocksdbBlockReadByte:      4,

		// txn
		commitCount:          16,
		sumPrewriteTime:      6,
		maxPrewriteTime:      3,
		sumCommitTime:        16,
		maxCommitTime:        5,
		sumGetCommitTsTime:   16,
		maxGetCommitTsTime:   8,
		sumCommitBackoffTime: 16,
		maxCommitBackoffTime: 8,

		sumResolveLockTime:   16,
		maxResolveLockTime:   8,
		sumLocalLatchTime:    16,
		maxLocalLatchTime:    8,
		sumWriteKeys:         16,
		maxWriteKeys:         8,
		sumWriteSize:         16,
		maxWriteSize:         8,
		sumPrewriteRegionNum: 16,
		maxPrewriteRegionNum: 8,
		sumTxnRetry:          16,
		maxTxnRetry:          8,
		sumBackoffTimes:      16,
		backoffTypes:         map[string]int{},

		// plan cache
		planCacheHits: 16,

		// other
		sumAffectedRows:      16,
		sumMem:               16,
		maxMem:               8,
		sumDisk:              16,
		maxDisk:              8,
		firstSeen:            time.Unix(now-20, 0),
		lastSeen:             time.Unix(now, 0),
		execRetryCount:       16,
		execRetryTime:        16,
		sumKVTotal:           4,
		sumPDTotal:           4,
		sumBackoffTotal:      4,
		sumWriteSQLRespTotal: 200,
		sumErrors:            16,
	}
	c.Assert(reflect.DeepEqual(&addTo, &expectedSum), Equals, true)
}

func getAllEvicted(ssdbe *stmtSummaryByDigestEvicted) string {
	buf := bytes.NewBuffer(nil)
	for e := ssdbe.history.Back(); e != nil; e = e.Prev() {
		if buf.Len() != 0 {
			buf.WriteString(", ")
		}
		val := e.Value.(*stmtSummaryByDigestEvictedElement)
		buf.WriteString(fmt.Sprintf("{begin: %v, end: %v, count: %v}", val.beginTime, val.endTime, len(val.digestKeyMap)))
	}
	return buf.String()
}

func getEvicted(ssbdee *stmtSummaryByDigestEvictedElement) string {
	buf := bytes.NewBuffer(nil)
	buf.WriteString(fmt.Sprintf("{begin: %v, end: %v, count: %v}", ssbdee.beginTime, ssbdee.endTime, len(ssbdee.digestKeyMap)))
	return buf.String()
}
