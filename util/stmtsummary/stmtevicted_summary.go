package stmtsummary

import (
	"container/list"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/kvcache"
	"strconv"
	"sync"
	"time"
)

var StmtEvictedMap = newstmtEvictedMap()

type stmtEvictedByDigestMap struct {
	// It's rare to read concurrently, so RWMutex is not needed.
	sync.Mutex
	summaryMap *kvcache.SimpleLRUCache
	// beginTimeForCurInterval is the begin time for current summary.
	beginTimeForCurInterval int64
}

type stmEvictedByDigestKey struct {
	hash          []byte
	beginTimeHash string
}

type stmEvictedByDigest struct {
	sync.Mutex
	beginTime int64
	endTime int64
	isInit	bool
	history	*list.List
	Count	int64
	historyMax int
}

func newstmtEvictedMap() *stmtEvictedByDigestMap {
	return &stmtEvictedByDigestMap{
		//Only record 96 time window(48H)
		//maybe we can modify this later
		summaryMap:              		kvcache.NewSimpleLRUCache(96,0,0),
		beginTimeForCurInterval:		900,
	}
}

func (key *stmEvictedByDigestKey) Hash() []byte {
	key.hash=make([]byte,0,len(key.beginTimeHash))
	key.hash=append(key.hash,hack.Slice(key.beginTimeHash)...)
	return key.hash
}

func (sseMap *stmtEvictedByDigestMap) AddEvictedRecord(now int64,value kvcache.Value)   {
	if sseMap.beginTimeForCurInterval+900<=now{
		sseMap.beginTimeForCurInterval = now / 900 * 900
	}
	begintime:=sseMap.beginTimeForCurInterval
	key:=&stmEvictedByDigestKey{
		beginTimeHash: strconv.FormatInt(begintime,10),
	}
	key.Hash()
	evicted:= func() *stmEvictedByDigest {
		sseMap.Lock()
		defer sseMap.Unlock()
		value,ok:=sseMap.summaryMap.Get(key)
		var evicted *stmEvictedByDigest
		if !ok {
			evicted = new(stmEvictedByDigest)
			sseMap.summaryMap.Put(key, evicted)
		} else {
			evicted = value.(*stmEvictedByDigest)
		}
		return evicted
	}()
	evicted.add(value,begintime)
}

func (sseMap *stmtEvictedByDigestMap) ToCurrentDatum() [][]types.Datum {
	sseMap.Lock()
	values := sseMap.summaryMap.Values()
	sseMap.Unlock()
	rows := make([][]types.Datum, 0, len(values))
	for _,v:=range values{
		record:=v.(*stmEvictedByDigest)
		rows=append(rows,record.toCurrentDatum())
	}
	return rows
}

func (eb *stmEvictedByDigest) add(value kvcache.Value,begintime int64)  {
	eb.Lock()
	defer eb.Unlock()
	if !eb.isInit{
		eb.init(begintime,begintime+900)
	}
	EvictedCol:=value.(*stmtSummaryByDigest)
	for i:=EvictedCol.history.Front();i!=nil;i=i.Next(){
		if i.Value.(*stmtSummaryByDigestElement).endTime>begintime {
			eb.history.PushBack(i.Value.(*stmtSummaryByDigestElement))
			//Add Count
			eb.Count++
		}
	}
}

func (eb *stmEvictedByDigest) init(begintime int64,endtime int64) {
	eb.isInit=  true
	eb.history= list.New()
	eb.beginTime= begintime
	eb.endTime= endtime
	eb.Count= 0
}

func (eb *stmEvictedByDigest) toCurrentDatum() []types.Datum {
	eb.Lock()
	defer eb.Unlock()
	return types.MakeDatums(
		types.NewTime(types.FromGoTime(time.Unix(eb.beginTime, 0)), mysql.TypeTimestamp, 0),
		types.NewTime(types.FromGoTime(time.Unix(eb.endTime, 0)), mysql.TypeTimestamp, 0),
		eb.Count,
	)
}
