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
	// beginTime is the begin time of current interval
	beginTime int64
	// endTime is the end time of current interval
	endTime int64
	// *Kinds* of digest being evicted
	digestKeyMap map[string]struct{}
}

// spawn a new pointer to stmtSummaryByDigestEvicted
func newStmtSummaryByDigestEvicted() *stmtSummaryByDigestEvicted {
	return &stmtSummaryByDigestEvicted{
		history: list.New(),
	}
}

// spawn a new pointer to stmtSummaryByDigestEvictedElement
func newStmtSummaryByDigestEvictedElement(beginTimeForCurrentInterval int64, intervalSeconds int64) *stmtSummaryByDigestEvictedElement {
	return &stmtSummaryByDigestEvictedElement{
		beginTime:    beginTimeForCurrentInterval,
		endTime:      beginTimeForCurrentInterval + intervalSeconds,
		digestKeyMap: make(map[string]struct{}),
	}
}

// AddEvicted is used add an evicted record to stmtSummaryByDigestEvicted
func (ssbde *stmtSummaryByDigestEvicted) AddEvicted(evictedKey *stmtSummaryByDigestKey, evictedValue *stmtSummaryByDigest, historySize int) {
	evictedValue.Lock()
	defer evictedValue.Unlock()

	for e, h := evictedValue.history.Back(), ssbde.history.Back(); e != nil; e = e.Prev() {
		evictedElement := e.Value.(*stmtSummaryByDigestElement)
		eBeginTime := evictedElement.beginTime
		eEndTime := evictedElement.endTime

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
			record.addEvicted(evictedKey, evictedElement)
			ssbde.history.PushBack(record)

			if evictedKey == nil {
				// passing an empty Key is used as `refresh`.
				h = ssbde.history.Back()
				continue
			}
		}

		// because 2 lists are both exactly time-ordered
		// begin matching from the latest matched history element.
		for ; h != nil; h = h.Prev() {
			historyElement := h.Value.(*stmtSummaryByDigestEvictedElement)
			sBeginTime := historyElement.beginTime
			sEndTime := historyElement.endTime

			if sBeginTime <= eBeginTime &&
				sEndTime >= eEndTime {
				// is in this history interval
				historyElement.addEvicted(evictedKey, evictedElement)
				break
			}

			if sBeginTime > eEndTime &&
				ssbde.history.Len() < historySize &&
				h == ssbde.history.Front(){
					// digestElement is older than all history elements.
					// creat a digestEvictedElement and PushFront
					beginTime := eBeginTime
					intervalSeconds := eEndTime - eBeginTime
					record := newStmtSummaryByDigestEvictedElement(beginTime, intervalSeconds)
					record.addEvicted(evictedKey, evictedElement)
					ssbde.history.PushFront(record)
					break
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
		types.NewTime(types.FromGoTime(time.Unix(seElement.beginTime, 0)), mysql.TypeTimestamp, 0),
		types.NewTime(types.FromGoTime(time.Unix(seElement.endTime, 0)), mysql.TypeTimestamp, 0),
		int64(len(seElement.digestKeyMap)),
	)
	return datum
}

func (ssMap *stmtSummaryByDigestMap) ToEvictedCountDatum() [][]types.Datum {
	return ssMap.other.ToEvictedCountDatum()
}
