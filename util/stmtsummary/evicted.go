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
func newStmtSummaryByDigestEvictedElement(beginTime int64, endTime int64) *stmtSummaryByDigestEvictedElement {
	return &stmtSummaryByDigestEvictedElement{
		beginTime:    beginTime,
		endTime:      endTime,
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

		// no record in ssbde.history, direct insert
		if ssbde.history.Len() == 0 {
			record := newStmtSummaryByDigestEvictedElement(eBeginTime, eEndTime)
			record.addEvicted(evictedKey, evictedElement)
			ssbde.history.PushFront(record)
			continue
		}

		// prevent exceeding history size
		for ssbde.history.Len() >= historySize && ssbde.history.Len() > 1 {
			ssbde.history.Remove(ssbde.history.Front())
		}

		// look for matching history interval
		// if there are no records in ssbde.history, following code will not be executed. which will probably lead to a bug.
	MATCHING:
		for ; h != nil; h = h.Prev() {
			historyElement := h.Value.(*stmtSummaryByDigestEvictedElement)

			switch historyElement.matchAndAdd(evictedKey, evictedElement) {
			case isMatch:
				break MATCHING
			case isTooYoung:
				{
					record := newStmtSummaryByDigestEvictedElement(eBeginTime, eEndTime)
					record.addEvicted(evictedKey, evictedElement)
					ssbde.history.InsertAfter(record, h)
					break MATCHING
				}
			default:
				{
					if h == ssbde.history.Front() {
						record := newStmtSummaryByDigestEvictedElement(eBeginTime, eEndTime)
						record.addEvicted(evictedKey, evictedElement)
						ssbde.history.PushFront(record)
						break MATCHING
					}
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
	if seElement == nil {
		return isTooYoung
	}
	sBeginTime, sEndTime := seElement.beginTime, seElement.endTime
	eBeginTime, eEndTime := digestValue.beginTime, digestValue.endTime
	if sBeginTime <= eBeginTime && eEndTime <= sEndTime {
		seElement.addEvicted(digestKey, digestValue)
		return isMatch
	} else if eEndTime < sBeginTime {
		return isTooOld
	} else {
		return isTooYoung
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
