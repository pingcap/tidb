package stmtsummary

import (
	"container/list"
	"strconv"
	"sync"
	"time"

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/kvcache"
)

// StmtEvictedMap is a global map containing all statement summaries evicted.
var StmtEvictedMap = newstmtEvictedMap()

type stmtSummaryEvictedMap struct {
	// It's rare to read concurrently, so RWMutex is not needed.
	sync.Mutex
	summaryMap *kvcache.SimpleLRUCache
	// beginTimeForCurInterval is the begin time for current summary.
	beginTimeForCurInterval int64
	sysVars                 *systemVars
}

type stmtSummaryEvictedKey struct {
	hash          []byte
	beginTimeHash string
}

type stmtSummaryEvictedInfo struct {
	sync.Mutex
	beginTime  int64
	endTime    int64
	isInit     bool
	history    *list.List
	Count      int64
	historyMax int
}

func newstmtEvictedMap() *stmtSummaryEvictedMap {
	sysVars := newSysVars()
	maxCount := uint(sysVars.getVariable(typeMaxStmtCount))
	return &stmtSummaryEvictedMap{
		summaryMap: kvcache.NewSimpleLRUCache(maxCount, 0, 0),
		sysVars:    sysVars,
	}
}

func (key *stmtSummaryEvictedKey) Hash() []byte {
	key.hash = make([]byte, 0, len(key.beginTimeHash))
	key.hash = append(key.hash, hack.Slice(key.beginTimeHash)...)
	return key.hash
}

func (sseMap *stmtSummaryEvictedMap) AddEvictedRecord(now int64, value kvcache.Value) {
	intervalSeconds := sseMap.refreshInterval()
	historySize := sseMap.historySize()
	if sseMap.beginTimeForCurInterval+intervalSeconds <= now {
		sseMap.beginTimeForCurInterval = now / intervalSeconds * intervalSeconds
	}
	begintime := sseMap.beginTimeForCurInterval
	key := &stmtSummaryEvictedKey{
		beginTimeHash: strconv.FormatInt(begintime, 10),
	}
	key.Hash()
	evicted := func() *stmtSummaryEvictedInfo {
		sseMap.Lock()
		defer sseMap.Unlock()
		value, ok := sseMap.summaryMap.Get(key)
		var evicted *stmtSummaryEvictedInfo
		if !ok {
			evicted = new(stmtSummaryEvictedInfo)
			sseMap.summaryMap.Put(key, evicted)
		} else {
			evicted = value.(*stmtSummaryEvictedInfo)
		}
		return evicted
	}()
	evicted.add(value, begintime, intervalSeconds, historySize)
}

func (sseMap *stmtSummaryEvictedMap) ToCurrentDatum() [][]types.Datum {
	sseMap.Lock()
	values := sseMap.summaryMap.Values()
	sseMap.Unlock()
	rows := make([][]types.Datum, 0, len(values))
	for _, v := range values {
		record := v.(*stmtSummaryEvictedInfo)
		rows = append(rows, record.toDatum())
	}
	return rows
}

func (eb *stmtSummaryEvictedInfo) add(value kvcache.Value, begintime int64, intervalSeconds int64, historysize int) {
	eb.Lock()
	defer eb.Unlock()
	if !eb.isInit {
		eb.init(begintime, begintime+intervalSeconds, historysize)
	}
	EvictedCol := value.(*stmtSummaryByDigest)
	if EvictedCol.history != nil {
		for i := EvictedCol.history.Front(); i != nil; i = i.Next() {
			if i.Value.(*stmtSummaryByDigestElement).endTime > begintime {
				if eb.history.Len() >= eb.historyMax {
					eb.history.Remove(eb.history.Back())
				}
				eb.history.PushFront(i.Value.(*stmtSummaryByDigestElement))
				eb.Count++
			}
		}
	}
}

func (eb *stmtSummaryEvictedInfo) init(begintime int64, endtime int64, historysize int) {
	eb.isInit = true
	eb.history = list.New()
	eb.beginTime = begintime
	eb.endTime = endtime
	eb.historyMax = historysize
	eb.Count = 0
}

func (eb *stmtSummaryEvictedInfo) toDatum() []types.Datum {
	eb.Lock()
	defer eb.Unlock()
	return types.MakeDatums(
		types.NewTime(types.FromGoTime(time.Unix(eb.beginTime, 0)), mysql.TypeTimestamp, 0),
		types.NewTime(types.FromGoTime(time.Unix(eb.endTime, 0)), mysql.TypeTimestamp, 0),
		eb.Count,
	)
}

func (sseMap *stmtSummaryEvictedMap) refreshInterval() int64 {
	return sseMap.sysVars.getVariable(typeRefreshInterval)
}
func (sseMap *stmtSummaryEvictedMap) historySize() int {
	return int(sseMap.sysVars.getVariable(typeHistorySize))
}
