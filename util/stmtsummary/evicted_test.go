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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package stmtsummary

import (
	"bytes"
	"container/list"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/stretchr/testify/require"
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
func generateStmtSummaryByDigestKeyValue(schema string, beginTime int64, endTime int64) (*stmtSummaryByDigestKey, *stmtSummaryByDigest) {
	key := &stmtSummaryByDigestKey{
		schemaName: schema,
	}
	value := newInduceSsbd(beginTime, endTime)
	return key, value
}

// Test stmtSummaryByDigestMap.ToEvictedCountDatum
func TestMapToEvictedCountDatum(t *testing.T) {
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
	match(t, ssMap.ToEvictedCountDatum()[0], expectedEvictedCount...)

	// test multiple intervals
	ssMap.Clear()
	err = ssMap.SetRefreshInterval("60", false)
	interval = ssMap.refreshInterval()
	require.NoError(t, err)
	err = ssMap.SetMaxStmtCount("1", false)
	require.NoError(t, err)

	err = ssMap.SetHistorySize("100", false)
	require.NoError(t, err)

	ssMap.beginTimeForCurInterval = now + interval
	// insert one statement per interval.
	for i := 0; i < 50; i++ {
		ssMap.AddStatement(generateAnyExecInfo())
		ssMap.beginTimeForCurInterval += interval * 2
	}
	require.Equal(t, 1, ssMap.summaryMap.Size())
	val := ssMap.summaryMap.Values()[0]
	require.NotNil(t, val)
	digest := val.(*stmtSummaryByDigest)
	require.Equal(t, 50, digest.history.Len())

	err = ssMap.SetHistorySize("25", false)
	require.NoError(t, err)
	// update begin time
	ssMap.beginTimeForCurInterval += interval * 2
	banditSei := generateAnyExecInfo()
	banditSei.SchemaName = "Kick you out >:("
	ssMap.AddStatement(banditSei)

	evictedCountDatums := ssMap.ToEvictedCountDatum()
	require.Equal(t, 25, len(evictedCountDatums))

	// update begin time
	banditSei.SchemaName = "Yet another kicker"
	ssMap.AddStatement(banditSei)

	evictedCountDatums = ssMap.ToEvictedCountDatum()
	// test young digest
	require.Equal(t, 25, len(evictedCountDatums))
	n = ssMap.beginTimeForCurInterval
	newlyEvicted := evictedCountDatums[0]
	expectedEvictedCount = []interface{}{
		types.NewTime(types.FromGoTime(time.Unix(n, 0)), mysql.TypeTimestamp, types.DefaultFsp),
		types.NewTime(types.FromGoTime(time.Unix(n+interval, 0)), mysql.TypeTimestamp, types.DefaultFsp),
		int64(1),
	}
	match(t, newlyEvicted, expectedEvictedCount...)
}

// Test stmtSummaryByDigestEvicted
func TestSimpleStmtSummaryByDigestEvicted(t *testing.T) {
	ssbde := newStmtSummaryByDigestEvicted()
	evictedKey, evictedValue := generateStmtSummaryByDigestKeyValue("a", 1, 2)

	// test NULL
	ssbde.AddEvicted(nil, nil, 10)
	require.Equal(t, 0, ssbde.history.Len())
	ssbde.Clear()
	// passing NULL key is used as *refresh*.
	ssbde.AddEvicted(nil, evictedValue, 10)
	require.Equal(t, 1, ssbde.history.Len())
	ssbde.Clear()
	ssbde.AddEvicted(evictedKey, nil, 10)
	require.Equal(t, 0, ssbde.history.Len())
	ssbde.Clear()

	// test zero historySize
	ssbde.AddEvicted(evictedKey, evictedValue, 0)
	require.Equal(t, 0, ssbde.history.Len())

	ssbde = newStmtSummaryByDigestEvicted()
	ssbde.AddEvicted(evictedKey, evictedValue, 1)
	require.Equal(t, "{begin: 1, end: 2, count: 1}", getAllEvicted(ssbde))
	// test insert same *kind* of digest
	ssbde.AddEvicted(evictedKey, evictedValue, 1)
	require.Equal(t, "{begin: 1, end: 2, count: 1}", getAllEvicted(ssbde))

	evictedKey, evictedValue = generateStmtSummaryByDigestKeyValue("b", 1, 2)
	ssbde.AddEvicted(evictedKey, evictedValue, 1)
	require.Equal(t, "{begin: 1, end: 2, count: 2}", getAllEvicted(ssbde))

	evictedKey, evictedValue = generateStmtSummaryByDigestKeyValue("b", 5, 6)
	ssbde.AddEvicted(evictedKey, evictedValue, 2)
	require.Equal(t, "{begin: 5, end: 6, count: 1}, {begin: 1, end: 2, count: 2}", getAllEvicted(ssbde))

	evictedKey, evictedValue = generateStmtSummaryByDigestKeyValue("b", 3, 4)
	ssbde.AddEvicted(evictedKey, evictedValue, 3)
	require.Equal(t, "{begin: 5, end: 6, count: 1}, {begin: 3, end: 4, count: 1}, {begin: 1, end: 2, count: 2}", getAllEvicted(ssbde))

	// test evicted element with multi-time range value.
	ssbde = newStmtSummaryByDigestEvicted()
	evictedKey, evictedValue = generateStmtSummaryByDigestKeyValue("a", 1, 2)
	evictedValue.history.PushBack(newInduceSsbde(2, 3))
	evictedValue.history.PushBack(newInduceSsbde(5, 6))
	evictedValue.history.PushBack(newInduceSsbde(8, 9))
	ssbde.AddEvicted(evictedKey, evictedValue, 3)
	require.Equal(t, "{begin: 8, end: 9, count: 1}, {begin: 5, end: 6, count: 1}, {begin: 2, end: 3, count: 1}", getAllEvicted(ssbde))

	evictedKey = &stmtSummaryByDigestKey{schemaName: "b"}
	ssbde.AddEvicted(evictedKey, evictedValue, 4)
	require.Equal(t, "{begin: 8, end: 9, count: 2}, {begin: 5, end: 6, count: 2}, {begin: 2, end: 3, count: 2}, {begin: 1, end: 2, count: 1}", getAllEvicted(ssbde))

	evictedKey, evictedValue = generateStmtSummaryByDigestKeyValue("c", 4, 5)
	evictedValue.history.PushBack(newInduceSsbde(5, 6))
	evictedValue.history.PushBack(newInduceSsbde(7, 8))
	ssbde.AddEvicted(evictedKey, evictedValue, 4)
	require.Equal(t, "{begin: 8, end: 9, count: 2}, {begin: 7, end: 8, count: 1}, {begin: 5, end: 6, count: 3}, {begin: 4, end: 5, count: 1}", getAllEvicted(ssbde))
	evictedKey, evictedValue = generateStmtSummaryByDigestKeyValue("d", 7, 8)
	ssbde.AddEvicted(evictedKey, evictedValue, 4)
	require.Equal(t, "{begin: 8, end: 9, count: 2}, {begin: 7, end: 8, count: 2}, {begin: 5, end: 6, count: 3}, {begin: 4, end: 5, count: 1}", getAllEvicted(ssbde))

	// test for too old
	evictedKey, evictedValue = generateStmtSummaryByDigestKeyValue("d", 0, 1)
	evictedValue.history.PushBack(newInduceSsbde(1, 2))
	evictedValue.history.PushBack(newInduceSsbde(2, 3))
	evictedValue.history.PushBack(newInduceSsbde(4, 5))
	ssbde.AddEvicted(evictedKey, evictedValue, 4)
	require.Equal(t, "{begin: 8, end: 9, count: 2}, {begin: 7, end: 8, count: 2}, {begin: 5, end: 6, count: 3}, {begin: 4, end: 5, count: 2}", getAllEvicted(ssbde))

	// test for too young
	evictedKey, evictedValue = generateStmtSummaryByDigestKeyValue("d", 1, 2)
	evictedValue.history.PushBack(newInduceSsbde(9, 10))
	ssbde.AddEvicted(evictedKey, evictedValue, 4)
	require.Equal(t, "{begin: 9, end: 10, count: 1}, {begin: 8, end: 9, count: 2}, {begin: 7, end: 8, count: 2}, {begin: 5, end: 6, count: 3}", getAllEvicted(ssbde))
}

// Test stmtSummaryByDigestEvictedElement.ToEvictedCountDatum
func TestStmtSummaryByDigestEvictedElement(t *testing.T) {
	record := newStmtSummaryByDigestEvictedElement(0, 1)
	evictedKey, evictedValue := generateStmtSummaryByDigestKeyValue("alpha", 0, 1)
	digestValue := evictedValue.history.Back().Value.(*stmtSummaryByDigestElement)

	// test poisoning will NULL key.
	record.addEvicted(nil, nil)
	require.Equal(t, "{begin: 0, end: 1, count: 0}", getEvicted(record))
	record.addEvicted(nil, digestValue)
	require.Equal(t, "{begin: 0, end: 1, count: 0}", getEvicted(record))

	// test add evictedKey and evicted stmtSummaryByDigestElement
	record.addEvicted(evictedKey, digestValue)
	require.Equal(t, "{begin: 0, end: 1, count: 1}", getEvicted(record))

	// test add same *kind* of values.
	record.addEvicted(evictedKey, digestValue)
	require.Equal(t, "{begin: 0, end: 1, count: 1}", getEvicted(record))

	// test add different *kind* of values.
	evictedKey, evictedValue = generateStmtSummaryByDigestKeyValue("bravo", 0, 1)
	digestValue = evictedValue.history.Back().Value.(*stmtSummaryByDigestElement)
	record.addEvicted(evictedKey, digestValue)
	require.Equal(t, "{begin: 0, end: 1, count: 2}", getEvicted(record))
}

// test stmtSummaryByDigestEvicted.addEvicted
// test stmtSummaryByDigestEvicted.toEvictedCountDatum (single and multiple intervals)
func TestEvictedCountDetailed(t *testing.T) {
	ssMap := newStmtSummaryByDigestMap()
	ssMap.Clear()
	err := ssMap.SetRefreshInterval("60", false)
	require.NoError(t, err)
	err = ssMap.SetHistorySize("100", false)
	require.NoError(t, err)
	now := time.Now().Unix()
	interval := int64(60)
	ssMap.beginTimeForCurInterval = now + interval
	// set capacity to 1
	err = ssMap.summaryMap.SetCapacity(1)
	require.NoError(t, err)

	// test stmtSummaryByDigest's history length
	for i := 0; i < 100; i++ {
		if i == 0 {
			require.Equal(t, 0, ssMap.summaryMap.Size())
		} else {
			require.Equal(t, 1, ssMap.summaryMap.Size())
			val := ssMap.summaryMap.Values()[0]
			require.NotNil(t, val)
			digest := val.(*stmtSummaryByDigest)
			require.Equal(t, i, digest.history.Len())
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
		match(t, evictedCountDatum, expectedDatum...)
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
	match(t, evictedCountDatums[0], expectedDatum...)
	ssMap.Clear()
	other := ssMap.other
	// test poisoning with empty-history digestValue
	other.AddEvicted(new(stmtSummaryByDigestKey), new(stmtSummaryByDigest), 100)
	require.Equal(t, 0, other.history.Len())
}

func TestNewStmtSummaryByDigestEvictedElement(t *testing.T) {
	now := time.Now().Unix()
	end := now + 60
	stmtEvictedElement := newStmtSummaryByDigestEvictedElement(now, end)
	require.Equal(t, now, stmtEvictedElement.beginTime)
	require.Equal(t, end, stmtEvictedElement.endTime)
	require.Equal(t, 0, len(stmtEvictedElement.digestKeyMap))
}

func TestStmtSummaryByDigestEvicted(t *testing.T) {
	stmtEvicted := newStmtSummaryByDigestEvicted()
	require.Equal(t, 0, stmtEvicted.history.Len())
}

// test addInfo function
func TestAddInfo(t *testing.T) {
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
	require.Equal(t, true, reflect.DeepEqual(&addTo, &expectedSum))
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
