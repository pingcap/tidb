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
