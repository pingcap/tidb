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
	"container/list"
	"math"
	"sync"
	"time"

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
)

// stmtSummaryByDigestEvicted contents digests evicted from stmtSummaryByDigestMap
type stmtSummaryByDigestEvicted struct {
	sync.Mutex
	// record evicted data in intervals
	// latest history data is Back()
	history *list.List
}

// element being stored in stmtSummaryByDigestEvicted
type stmtSummaryByDigestEvictedElement struct {
	// beginTime is the begin time of current interval
	beginTime int64
	// endTime is the end time of current interval
	endTime int64
	// digestKeyMap contains *Kinds* of digest being evicted
	digestKeyMap map[string]struct{}
	// otherSummary contains summed up information of evicted elements
	otherSummary *stmtSummaryByDigestElement
}

// spawn a new pointer to stmtSummaryByDigestEvicted
func newStmtSummaryByDigestEvicted() *stmtSummaryByDigestEvicted {
	return &stmtSummaryByDigestEvicted{
		history: list.New(),
	}
}

// spawn a new pointer to stmtSummaryByDigestEvictedElement
func newStmtSummaryByDigestEvictedElement(beginTime int64, endTime int64) *stmtSummaryByDigestEvictedElement {
	return &stmtSummaryByDigestEvictedElement{
		beginTime:    beginTime,
		endTime:      endTime,
		digestKeyMap: make(map[string]struct{}),
		otherSummary: &stmtSummaryByDigestElement{
			beginTime:    beginTime,
			endTime:      endTime,
			authUsers:    make(map[string]struct{}),
			minLatency:   time.Duration(math.MaxInt64),
			backoffTypes: make(map[string]int),
			firstSeen:    time.Unix(endTime, 0),
		},
	}
}

// AddEvicted is used add an evicted record to stmtSummaryByDigestEvicted
func (ssbde *stmtSummaryByDigestEvicted) AddEvicted(evictedKey *stmtSummaryByDigestKey, evictedValue *stmtSummaryByDigest, historySize int) {
	if evictedValue == nil {
		return
	}

	evictedValue.Lock()
	defer evictedValue.Unlock()

	ssbde.Lock()
	defer ssbde.Unlock()

	if evictedValue.history == nil {
		return
	}
	for e, h := evictedValue.history.Back(), ssbde.history.Back(); e != nil; e = e.Prev() {
		evictedElement := e.Value.(*stmtSummaryByDigestElement)

		// use closure to minimize time holding lock
		func() {
			evictedElement.Lock()
			defer evictedElement.Unlock()
			// no record in ssbde.history, direct insert
			if ssbde.history.Len() == 0 && historySize != 0 {

				eBeginTime := evictedElement.beginTime
				eEndTime := evictedElement.endTime
				record := newStmtSummaryByDigestEvictedElement(eBeginTime, eEndTime)
				record.addEvicted(evictedKey, evictedElement)
				ssbde.history.PushFront(record)

				h = ssbde.history.Back()
				return
			}

			// look for matching history interval
		MATCHING:
			for ; h != nil; h = h.Prev() {
				historyElement := h.Value.(*stmtSummaryByDigestEvictedElement)

				switch historyElement.matchAndAdd(evictedKey, evictedElement) {
				case isMatch:
					// automatically added
					break MATCHING
				// not matching, create a new record and insert
				case isTooYoung:
					{
						eBeginTime := evictedElement.beginTime
						eEndTime := evictedElement.endTime
						record := newStmtSummaryByDigestEvictedElement(eBeginTime, eEndTime)
						record.addEvicted(evictedKey, evictedElement)
						ssbde.history.InsertAfter(record, h)
						break MATCHING
					}
				default: // isTooOld
					{
						if h == ssbde.history.Front() {
							// if digest older than all records in ssbde.history.
							eBeginTime := evictedElement.beginTime
							eEndTime := evictedElement.endTime
							record := newStmtSummaryByDigestEvictedElement(eBeginTime, eEndTime)
							record.addEvicted(evictedKey, evictedElement)
							ssbde.history.PushFront(record)
							break MATCHING
						}
					}
				}
			}
		}()

		// prevent exceeding history size
		for ssbde.history.Len() > historySize && ssbde.history.Len() > 0 {
			ssbde.history.Remove(ssbde.history.Front())
		}
	}
}

// Clear up all records in stmtSummaryByDigestEvicted
func (ssbde *stmtSummaryByDigestEvicted) Clear() {
	ssbde.Lock()
	defer ssbde.Unlock()
	ssbde.history.Init()
}

// add an evicted record to stmtSummaryByDigestEvictedElement
func (seElement *stmtSummaryByDigestEvictedElement) addEvicted(digestKey *stmtSummaryByDigestKey, digestValue *stmtSummaryByDigestElement) {
	if digestKey != nil {
		seElement.digestKeyMap[string(digestKey.Hash())] = struct{}{}
		addInfo(seElement.otherSummary, digestValue)
	}
}

const (
	isMatch    = 0
	isTooOld   = 1
	isTooYoung = 2
)

// matchAndAdd check time interval of seElement and digestValue.
// if matches, it will add the digest and return enum match
// if digest too old, it will return enum tooOld and do nothing
// if digest too young, it will return enum tooYoung and do nothing
func (seElement *stmtSummaryByDigestEvictedElement) matchAndAdd(digestKey *stmtSummaryByDigestKey, digestValue *stmtSummaryByDigestElement) (statement int) {
	if seElement == nil || digestValue == nil {
		return isTooYoung
	}
	sBeginTime, sEndTime := seElement.beginTime, seElement.endTime
	eBeginTime, eEndTime := digestValue.beginTime, digestValue.endTime
	if sBeginTime <= eBeginTime && eEndTime <= sEndTime {
		seElement.addEvicted(digestKey, digestValue)
		return isMatch
	} else if eEndTime <= sBeginTime {
		return isTooOld
	} else {
		return isTooYoung
	}
}

// ToEvictedCountDatum converts history evicted record to `evicted count` record's datum
func (ssbde *stmtSummaryByDigestEvicted) ToEvictedCountDatum() [][]types.Datum {
	records := make([][]types.Datum, 0, ssbde.history.Len())
	for e := ssbde.history.Back(); e != nil; e = e.Prev() {
		if record := e.Value.(*stmtSummaryByDigestEvictedElement).toEvictedCountDatum(); record != nil {
			records = append(records, record)
		}
	}
	return records
}

// toEvictedCountDatum converts evicted record to `EvictedCount` record's datum
func (seElement *stmtSummaryByDigestEvictedElement) toEvictedCountDatum() []types.Datum {
	datum := types.MakeDatums(
		types.NewTime(types.FromGoTime(time.Unix(seElement.beginTime, 0)), mysql.TypeTimestamp, 0),
		types.NewTime(types.FromGoTime(time.Unix(seElement.endTime, 0)), mysql.TypeTimestamp, 0),
		int64(len(seElement.digestKeyMap)),
	)
	return datum
}

func (ssMap *stmtSummaryByDigestMap) ToEvictedCountDatum() [][]types.Datum {
	return ssMap.other.ToEvictedCountDatum()
}

func (ssbde *stmtSummaryByDigestEvicted) collectHistorySummaries(historySize int) []*stmtSummaryByDigestEvictedElement {
	lst := make([]*stmtSummaryByDigestEvictedElement, 0, ssbde.history.Len())
	for element := ssbde.history.Front(); element != nil && len(lst) < historySize; element = element.Next() {
		seElement := element.Value.(*stmtSummaryByDigestEvictedElement)
		lst = append(lst, seElement)
	}
	return lst
}

// addInfo adds information in addWith into addTo.
func addInfo(addTo *stmtSummaryByDigestElement, addWith *stmtSummaryByDigestElement) {
	addTo.Lock()
	defer addTo.Unlock()

	// user
	for user := range addWith.authUsers {
		addTo.authUsers[user] = struct{}{}
	}

	// execCount and sumWarnings
	addTo.execCount += addWith.execCount
	addTo.sumWarnings += addWith.sumWarnings

	// latency
	addTo.sumLatency += addWith.sumLatency
	if addTo.maxLatency < addWith.maxLatency {
		addTo.maxLatency = addWith.maxLatency
	}
	if addTo.minLatency > addWith.minLatency {
		addTo.minLatency = addWith.minLatency
	}
	addTo.sumParseLatency += addWith.sumParseLatency
	if addTo.maxParseLatency < addWith.maxParseLatency {
		addTo.maxParseLatency = addWith.maxParseLatency
	}
	addTo.sumCompileLatency += addWith.sumCompileLatency
	if addTo.maxCompileLatency < addWith.maxCompileLatency {
		addTo.maxCompileLatency = addWith.maxCompileLatency
	}

	// coprocessor
	addTo.sumNumCopTasks += addWith.sumNumCopTasks
	if addTo.maxCopProcessTime < addWith.maxCopProcessTime {
		addTo.maxCopProcessTime = addWith.maxCopProcessTime
		addTo.maxCopProcessAddress = addWith.maxCopProcessAddress
	}
	if addTo.maxCopWaitTime < addWith.maxCopWaitTime {
		addTo.maxCopWaitTime = addWith.maxCopWaitTime
		addTo.maxCopWaitAddress = addWith.maxCopWaitAddress
	}

	// TiKV
	addTo.sumProcessTime += addWith.sumProcessTime
	if addTo.maxProcessTime < addWith.maxProcessTime {
		addTo.maxProcessTime = addWith.maxProcessTime
	}
	addTo.sumWaitTime += addWith.sumWaitTime
	if addTo.maxWaitTime < addWith.maxWaitTime {
		addTo.maxWaitTime = addWith.maxWaitTime
	}
	addTo.sumBackoffTime += addWith.sumBackoffTime
	if addTo.maxBackoffTime < addWith.maxBackoffTime {
		addTo.maxBackoffTime = addWith.maxBackoffTime
	}

	addTo.sumTotalKeys += addWith.sumTotalKeys
	if addTo.maxTotalKeys < addWith.maxTotalKeys {
		addTo.maxTotalKeys = addWith.maxTotalKeys
	}
	addTo.sumProcessedKeys += addWith.sumProcessedKeys
	if addTo.maxProcessedKeys < addWith.maxProcessedKeys {
		addTo.maxProcessedKeys = addWith.maxProcessedKeys
	}
	addTo.sumRocksdbDeleteSkippedCount += addWith.sumRocksdbDeleteSkippedCount
	if addTo.maxRocksdbDeleteSkippedCount < addWith.maxRocksdbDeleteSkippedCount {
		addTo.maxRocksdbDeleteSkippedCount = addWith.maxRocksdbDeleteSkippedCount
	}
	addTo.sumRocksdbKeySkippedCount += addWith.sumRocksdbKeySkippedCount
	if addTo.maxRocksdbKeySkippedCount < addWith.maxRocksdbKeySkippedCount {
		addTo.maxRocksdbKeySkippedCount = addWith.maxRocksdbKeySkippedCount
	}
	addTo.sumRocksdbBlockCacheHitCount += addWith.sumRocksdbBlockCacheHitCount
	if addTo.maxRocksdbBlockCacheHitCount < addWith.maxRocksdbBlockCacheHitCount {
		addTo.maxRocksdbBlockCacheHitCount = addWith.maxRocksdbBlockCacheHitCount
	}
	addTo.sumRocksdbBlockReadCount += addWith.sumRocksdbBlockReadCount
	if addTo.maxRocksdbBlockReadCount < addWith.maxRocksdbBlockReadCount {
		addTo.maxRocksdbBlockReadCount = addWith.maxRocksdbBlockReadCount
	}
	addTo.sumRocksdbBlockReadByte += addWith.sumRocksdbBlockReadByte
	if addTo.maxRocksdbBlockReadByte < addWith.maxRocksdbBlockReadByte {
		addTo.maxRocksdbBlockReadByte = addWith.maxRocksdbBlockReadByte
	}

	// txn
	addTo.commitCount += addWith.commitCount
	addTo.sumPrewriteTime += addWith.sumPrewriteTime
	if addTo.maxPrewriteTime < addWith.maxPrewriteTime {
		addTo.maxPrewriteTime = addWith.maxPrewriteTime
	}
	addTo.sumCommitTime += addWith.sumCommitTime
	if addTo.maxCommitTime < addWith.maxCommitTime {
		addTo.maxCommitTime = addWith.maxCommitTime
	}
	addTo.sumGetCommitTsTime += addWith.sumGetCommitTsTime
	if addTo.maxGetCommitTsTime < addWith.maxGetCommitTsTime {
		addTo.maxGetCommitTsTime = addWith.maxGetCommitTsTime
	}
	addTo.sumCommitBackoffTime += addWith.sumCommitBackoffTime
	if addTo.maxCommitBackoffTime < addWith.maxCommitBackoffTime {
		addTo.maxCommitBackoffTime = addWith.maxCommitBackoffTime
	}
	addTo.sumResolveLockTime += addWith.sumResolveLockTime
	if addTo.maxResolveLockTime < addWith.maxResolveLockTime {
		addTo.maxResolveLockTime = addWith.maxResolveLockTime
	}
	addTo.sumLocalLatchTime += addWith.sumLocalLatchTime
	if addTo.maxLocalLatchTime < addWith.maxLocalLatchTime {
		addTo.maxLocalLatchTime = addWith.maxLocalLatchTime
	}
	addTo.sumWriteKeys += addWith.sumWriteKeys
	if addTo.maxWriteKeys < addWith.maxWriteKeys {
		addTo.maxWriteKeys = addWith.maxWriteKeys
	}
	addTo.sumWriteSize += addWith.sumWriteSize
	if addTo.maxWriteSize < addWith.maxWriteSize {
		addTo.maxWriteSize = addWith.maxWriteSize
	}
	addTo.sumPrewriteRegionNum += addWith.sumPrewriteRegionNum
	if addTo.maxPrewriteRegionNum < addWith.maxPrewriteRegionNum {
		addTo.maxPrewriteRegionNum = addWith.maxPrewriteRegionNum
	}
	addTo.sumTxnRetry += addWith.sumTxnRetry
	if addTo.maxTxnRetry < addWith.maxTxnRetry {
		addTo.maxTxnRetry = addWith.maxTxnRetry
	}
	addTo.sumBackoffTimes += addWith.sumBackoffTimes
	for backoffType, backoffValue := range addWith.backoffTypes {
		_, ok := addTo.backoffTypes[backoffType]
		if ok {
			addTo.backoffTypes[backoffType] += backoffValue
		} else {
			addTo.backoffTypes[backoffType] = backoffValue
		}
	}

	// plan cache
	addTo.planCacheHits += addWith.planCacheHits

	// other
	addTo.sumAffectedRows += addWith.sumAffectedRows
	addTo.sumMem += addWith.sumMem
	if addTo.maxMem < addWith.maxMem {
		addTo.maxMem = addWith.maxMem
	}
	addTo.sumDisk += addWith.sumDisk
	if addTo.maxDisk < addWith.maxDisk {
		addTo.maxDisk = addWith.maxDisk
	}
	if addTo.firstSeen.After(addWith.firstSeen) {
		addTo.firstSeen = addWith.firstSeen
	}
	if addTo.lastSeen.Before(addWith.lastSeen) {
		addTo.lastSeen = addWith.lastSeen
	}
	addTo.execRetryCount += addWith.execRetryCount
	addTo.execRetryTime += addWith.execRetryTime
	addTo.sumKVTotal += addWith.sumKVTotal
	addTo.sumPDTotal += addWith.sumPDTotal
	addTo.sumBackoffTotal += addWith.sumBackoffTotal
	addTo.sumWriteSQLRespTotal += addWith.sumWriteSQLRespTotal

	addTo.sumErrors += addWith.sumErrors
}
