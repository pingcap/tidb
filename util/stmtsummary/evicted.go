package stmtsummary

import (
	"container/list"
	"time"

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
)

// stmtSummaryByDigestEvicted contents digests evicted from stmtSummaryByDigestMap
type stmtSummaryByDigestEvicted struct {
	// record evicted data in intervals
	// latest history data is Back()
	history *list.List
}

// element being stored in stmtSummaryByDigestEvicted
type stmtSummaryByDigestEvictedElement struct {
	// *Kinds* of digest being evicted
	digestKeyMap map[string]struct{}

	// summary of digest being evicted
	sum *stmtSummaryByDigestElement
}

// spawn a new pointer to stmtSummaryByDigestEvicted
func newStmtSummaryByDigestEvicted() *stmtSummaryByDigestEvicted {
	return &stmtSummaryByDigestEvicted{
		history: list.New(),
	}
}

// spawn a new pointer to stmtSummaryByDigestEvictedElement
func newStmtSummaryByDigestEvictedElement(beginTimeForCurrentInterval int64, intervalSeconds int64) *stmtSummaryByDigestEvictedElement {
	ssElement := new(stmtSummaryByDigestElement)
	ssElement.beginTime = beginTimeForCurrentInterval
	ssElement.endTime = beginTimeForCurrentInterval + intervalSeconds
	return &stmtSummaryByDigestEvictedElement{
		digestKeyMap: make(map[string]struct{}),
		sum:          ssElement,
	}
}

// AddEvicted is used add an evicted record to stmtSummaryByDigestEvicted
func (ssbde *stmtSummaryByDigestEvicted) AddEvicted(evictedKey *stmtSummaryByDigestKey, evictedValue *stmtSummaryByDigest, historySize int) {

	// *need to get optimized*!!
	evictedValue.Lock()
	defer evictedValue.Unlock()
	for e := evictedValue.history.Back(); e != nil; e = e.Prev() {
		eBeginTime := e.Value.(*stmtSummaryByDigestElement).beginTime
		eEndTime := e.Value.(*stmtSummaryByDigestElement).endTime

		// prevent exceeding history size
		for ssbde.history.Len() >= historySize && ssbde.history.Len() > 1 {
			ssbde.history.Remove(ssbde.history.Front())
		}

		// look for matching history interval
		if ssbde.history.Len() == 0 && historySize > 0 {
			// no record in history
			beginTime := eBeginTime
			intervalSeconds := eEndTime - eBeginTime
			record := newStmtSummaryByDigestEvictedElement(beginTime, intervalSeconds)
			record.addEvicted(evictedKey, e.Value.(*stmtSummaryByDigestElement))
			ssbde.history.PushBack(record)

			if evictedKey == nil {
				// passing a empty Key is used as `refresh`.
				continue
			}
		}

		for h := ssbde.history.Back(); h != nil; h = h.Prev() {
			sBeginTime := h.Value.(*stmtSummaryByDigestEvictedElement).sum.beginTime
			sEndTime := h.Value.(*stmtSummaryByDigestEvictedElement).sum.endTime

			if sBeginTime <= eBeginTime &&
				sEndTime >= eEndTime {
				// is in this history interval
				h.Value.(*stmtSummaryByDigestEvictedElement).addEvicted(evictedKey, e.Value.(*stmtSummaryByDigestElement))
				break
			}

			if sEndTime <= eBeginTime {
				// digest is young, insert into new interval after this history interval
				beginTime := eBeginTime
				intervalSeconds := eEndTime - eBeginTime
				record := newStmtSummaryByDigestEvictedElement(beginTime, intervalSeconds)
				record.addEvicted(evictedKey, e.Value.(*stmtSummaryByDigestElement))
				ssbde.history.InsertAfter(record, h)
				break
			}

			if sBeginTime > eEndTime {
				// digestElement is old
				if h != ssbde.history.Front() {
					// check older history digestEvictedElement
					continue
				} else if ssbde.history.Len() >= historySize {
					// out of history size, abandon
					break
				} else {
					// is oldest digest
					// creat a digestEvictedElement and PushFront!
					beginTime := eBeginTime
					intervalSeconds := eEndTime - eBeginTime
					record := newStmtSummaryByDigestEvictedElement(beginTime, intervalSeconds)
					record.addEvicted(evictedKey, e.Value.(*stmtSummaryByDigestElement))
					ssbde.history.PushFront(record)
					break
				}
			}
		}
	}
}

// Clear up all records in stmtSummaryByDigestEvicted
func (ssbde *stmtSummaryByDigestEvicted) Clear() {
	ssbde.history.Init()
}

// add an evicted record to stmtSummaryByDigestEvictedElement
func (seElement *stmtSummaryByDigestEvictedElement) addEvicted(digestKey *stmtSummaryByDigestKey, digestValue *stmtSummaryByDigestElement) {
	if digestKey != nil {
		seElement.digestKeyMap[string(digestKey.Hash())] = struct{}{}
	}
	sumEvicted(seElement.sum, digestValue)
}

// ToCurrentOtherDatum converts current evicted record to `other` record's datum
func (ssbde *stmtSummaryByDigestEvicted) ToCurrentOtherDatum() []types.Datum {
	induceSsbd := new(stmtSummaryByDigest)
	return ssbde.history.Back().Value.(*stmtSummaryByDigestEvictedElement).toOtherDatum(induceSsbd)
}

// ToHistoryOtherDatum converts history evicted record to `other` record's datum
func (ssbde *stmtSummaryByDigestEvicted) ToHistoryOtherDatum() [][]types.Datum {
	induceSsbd := new(stmtSummaryByDigest)

	var records [][]types.Datum
	for e := ssbde.history.Front(); e != nil; e = e.Next() {
		if record := e.Value.(*stmtSummaryByDigestEvictedElement).toOtherDatum(induceSsbd); record != nil {
			records = append(records, record)
		}
	}
	return records
}

// ToEvictedCountDatum converts history evicted record to `evicted count` record's datum
func (ssbde *stmtSummaryByDigestEvicted) ToEvictedCountDatum() [][]types.Datum {
	records := make([][]types.Datum, 0, ssbde.history.Len())
	for e := ssbde.history.Front(); e != nil; e = e.Next() {
		if record := e.Value.(*stmtSummaryByDigestEvictedElement).toEvictedCountDatum(); record != nil {
			records = append(records, record)
		}
	}
	return records
}

// toOtherDatum converts evicted record to `other` record's datum
func (seElement *stmtSummaryByDigestEvictedElement) toOtherDatum(ssbd *stmtSummaryByDigest) []types.Datum {
	return seElement.sum.toDatum(ssbd)
}

// toEvictedCountDatum converts evicted record to `EvictedCount` record's datum
func (seElement *stmtSummaryByDigestEvictedElement) toEvictedCountDatum() []types.Datum {
	datum := types.MakeDatums(
		types.NewTime(types.FromGoTime(time.Unix(seElement.sum.beginTime, 0)), mysql.TypeTimestamp, 0),
		types.NewTime(types.FromGoTime(time.Unix(seElement.sum.endTime, 0)), mysql.TypeTimestamp, 0),
		int64(len(seElement.digestKeyMap)),
	)
	return datum
}

func (ssMap *stmtSummaryByDigestMap) ToEvictedCountDatum() [][]types.Datum {
	return ssMap.other.ToEvictedCountDatum()
}

// sumEvicted sum addWith into addTo
func sumEvicted(sumTo *stmtSummaryByDigestElement, addWith *stmtSummaryByDigestElement) {
	// Time duration relation: addWith ⊆ sumTo
	if sumTo.beginTime < addWith.beginTime {
		sumTo.beginTime = addWith.beginTime
	}
	if sumTo.endTime > addWith.beginTime {
		sumTo.endTime = addWith.endTime
	}
	// basic
	sumTo.execCount += addWith.execCount
	sumTo.sumErrors += addWith.sumErrors
	sumTo.sumWarnings += addWith.sumWarnings
	// latency
	sumTo.sumLatency += addWith.sumLatency
	sumTo.sumParseLatency += addWith.sumParseLatency
	sumTo.sumCompileLatency += addWith.sumCompileLatency
	if sumTo.maxLatency < addWith.maxLatency {
		sumTo.maxLatency = addWith.maxLatency
	}
	if sumTo.maxCompileLatency < addWith.maxCompileLatency {
		sumTo.maxCompileLatency = addWith.maxCompileLatency
	}
	if sumTo.minLatency > addWith.minLatency {
		sumTo.minLatency = addWith.minLatency
	}
	// coprocessor
	sumTo.sumNumCopTasks += addWith.sumNumCopTasks
	if sumTo.maxCopProcessTime < addWith.maxCopProcessTime {
		sumTo.maxCopProcessTime = addWith.maxCopProcessTime
	}
	if sumTo.maxCopWaitTime < addWith.maxCopWaitTime {
		sumTo.maxCopWaitTime = addWith.maxCopWaitTime
	}
	// TiKV
	sumTo.sumProcessTime += addWith.sumProcessTime
	sumTo.sumWaitTime += addWith.sumWaitTime
	sumTo.sumBackoffTime += addWith.sumBackoffTime
	sumTo.sumTotalKeys += addWith.sumTotalKeys
	sumTo.sumProcessedKeys += addWith.sumProcessedKeys
	sumTo.sumRocksdbDeleteSkippedCount += addWith.sumRocksdbDeleteSkippedCount
	sumTo.sumRocksdbKeySkippedCount += addWith.sumRocksdbKeySkippedCount
	sumTo.sumRocksdbBlockCacheHitCount += addWith.sumRocksdbBlockCacheHitCount
	sumTo.sumRocksdbBlockReadCount += addWith.sumRocksdbBlockReadCount
	sumTo.sumRocksdbBlockReadByte += addWith.sumRocksdbBlockReadByte
	if sumTo.maxProcessTime < addWith.maxProcessTime {
		sumTo.maxProcessTime = addWith.maxProcessTime
	}
	if sumTo.maxWaitTime < addWith.maxWaitTime {
		sumTo.maxWaitTime = addWith.maxWaitTime
	}
	if sumTo.maxBackoffTime < addWith.maxBackoffTime {
		sumTo.maxBackoffTime = addWith.maxBackoffTime
	}
	if sumTo.maxTotalKeys < addWith.maxTotalKeys {
		sumTo.maxTotalKeys = addWith.maxTotalKeys
	}
	if sumTo.maxProcessedKeys < addWith.maxProcessedKeys {
		sumTo.maxProcessedKeys = addWith.maxProcessedKeys
	}
	if sumTo.maxRocksdbBlockReadByte < addWith.maxRocksdbBlockReadByte {
		sumTo.maxRocksdbBlockReadByte = addWith.maxRocksdbBlockReadByte
	}
	if sumTo.maxRocksdbBlockCacheHitCount < addWith.maxRocksdbBlockCacheHitCount {
		sumTo.maxRocksdbBlockCacheHitCount = addWith.maxRocksdbBlockCacheHitCount
	}
	if sumTo.maxRocksdbBlockReadCount < addWith.maxRocksdbBlockReadCount {
		sumTo.maxRocksdbBlockReadCount = addWith.maxRocksdbBlockReadCount
	}
	if sumTo.maxRocksdbDeleteSkippedCount < addWith.maxRocksdbDeleteSkippedCount {
		sumTo.maxRocksdbDeleteSkippedCount = addWith.maxRocksdbDeleteSkippedCount
	}
	if sumTo.maxRocksdbKeySkippedCount < addWith.maxRocksdbKeySkippedCount {
		sumTo.maxRocksdbKeySkippedCount = addWith.maxRocksdbKeySkippedCount
	}
	// txn
	sumTo.commitCount += addWith.commitCount
	sumTo.sumGetCommitTsTime += addWith.sumGetCommitTsTime
	sumTo.sumPrewriteTime += addWith.sumPrewriteTime
	sumTo.sumCommitTime += addWith.sumCommitTime
	sumTo.sumLocalLatchTime += addWith.sumLocalLatchTime
	sumTo.sumCommitBackoffTime += addWith.sumCommitBackoffTime
	sumTo.sumResolveLockTime += addWith.sumResolveLockTime
	sumTo.sumWriteKeys += addWith.sumWriteKeys
	sumTo.sumWriteSize += addWith.sumWriteSize
	sumTo.sumPrewriteRegionNum += addWith.sumPrewriteRegionNum
	sumTo.sumTxnRetry += addWith.sumTxnRetry
	sumTo.sumBackoffTimes += sumTo.sumBackoffTimes
	if sumTo.maxGetCommitTsTime < addWith.maxGetCommitTsTime {
		sumTo.maxGetCommitTsTime = addWith.maxGetCommitTsTime
	}
	if sumTo.maxPrewriteTime < addWith.maxPrewriteTime {
		sumTo.maxPrewriteTime = addWith.maxPrewriteTime
	}
	if sumTo.maxCommitTime < addWith.maxCommitTime {
		sumTo.maxCommitTime = addWith.maxCommitTime
	}
	if sumTo.maxLocalLatchTime < addWith.maxLocalLatchTime {
		sumTo.maxLocalLatchTime = addWith.maxLocalLatchTime
	}
	if sumTo.maxCommitBackoffTime < addWith.maxCommitBackoffTime {
		sumTo.maxCommitBackoffTime = addWith.maxCommitBackoffTime
	}
	if sumTo.maxResolveLockTime < addWith.maxResolveLockTime {
		sumTo.maxResolveLockTime = addWith.maxResolveLockTime
	}
	if sumTo.maxWriteKeys < addWith.maxWriteKeys {
		sumTo.maxWriteKeys = addWith.maxWriteKeys
	}
	if sumTo.maxWriteSize < addWith.maxWriteSize {
		sumTo.maxWriteSize = addWith.maxWriteSize
	}
	if sumTo.maxPrewriteRegionNum < sumTo.maxPrewriteRegionNum {
		sumTo.maxPrewriteRegionNum = addWith.maxPrewriteRegionNum
	}
	if sumTo.maxTxnRetry < addWith.maxTxnRetry {
		sumTo.maxTxnRetry = addWith.maxTxnRetry
	}
	// other
	sumTo.sumMem += addWith.sumMem
	sumTo.sumDisk += addWith.sumDisk
	sumTo.sumAffectedRows += addWith.sumAffectedRows
	sumTo.sumKVTotal += addWith.sumKVTotal
	sumTo.sumPDTotal += addWith.sumPDTotal
	sumTo.sumBackoffTotal += addWith.sumBackoffTotal
	sumTo.sumWriteSQLRespTotal += addWith.sumWriteSQLRespTotal
	if sumTo.maxMem < addWith.maxMem {
		sumTo.maxMem = addWith.maxMem
	}
	if sumTo.maxDisk < addWith.maxDisk {
		sumTo.maxDisk = addWith.maxDisk
	}
	// plan cache
	sumTo.planCacheHits += addWith.planCacheHits
	// pessimistic execution retry information
	sumTo.execRetryCount += addWith.execRetryCount
	sumTo.execRetryTime += addWith.execRetryTime
}
