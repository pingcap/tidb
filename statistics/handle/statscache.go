package handle

import (
	"github.com/pingcap/tidb/statistics"
)

type tableLRUHandle struct {
	*statistics.Table
	prev *tableLRUHandle
	next *tableLRUHandle
}

//MemoryUsage of tableLRUHandle returns the Table MemoryUsage
func (tbr *tableLRUHandle) MemoryUsage() int64 {
	return tbr.Table.MemoryUsage()
}

// StatsCache caches Regions loaded from PD.
type StatsCache struct {
	tables   map[int64]*tableLRUHandle
	version  uint64
	memUsage int64
	//after insert a table, append it to the end of the list
	//lruList.next is the next table to remove
	lruList *tableLRUHandle
}

//NewStatsCache creates a new StatsCache.
func NewStatsCache() *StatsCache {
	sc := &StatsCache{
		tables:   make(map[int64]*tableLRUHandle),
		lruList:  &tableLRUHandle{},
		memUsage: 0,
	}
	sc.lruList.next = sc.lruList
	sc.lruList.prev = sc.lruList

	return sc
}

//Lookup remove a tableLRUHandle from lruList
func (sc *StatsCache) setLRUlist() {
	if sc.lruList != nil {
		return
	}
	sc.lruList = &tableLRUHandle{}

	sc.lruList.next = sc.lruList
	sc.lruList.prev = sc.lruList
}

//Lookup remove a tableLRUHandle from lruList
func (sc *StatsCache) Lookup(id int64) (*statistics.Table, bool) {
	sc.setLRUlist()

	htbl, ok := sc.tables[id]
	if !ok {
		return nil, false
	}
	sc.LRURemove(htbl)
	sc.LRUAppend(htbl)
	return htbl.Table, true
}

//Insert insert a new table to tables and append to lrulist.
func (sc *StatsCache) Insert(table *statistics.Table) (memUsage int64) {
	sc.setLRUlist()

	tb := &tableLRUHandle{Table: table}

	id := table.PhysicalID
	if ptbl, ok := sc.tables[id]; ok {
		sc.LRURemove(ptbl)
		memUsage -= ptbl.MemoryUsage()
	}
	memUsage += table.MemoryUsage()
	sc.memUsage += memUsage
	sc.LRUAppend(tb)
	sc.tables[id] = tb

	return
}

//LRUAppend remove a tableLRUHandle from lruList
func (sc *StatsCache) LRUAppend(e *tableLRUHandle) {
	e.next = sc.lruList
	e.prev = sc.lruList.prev
	e.prev.next = e
	e.next.prev = e
}

//LRURemove remove a tableLRUHandle from lruList
func (sc *StatsCache) LRURemove(e *tableLRUHandle) {

	e.next.prev = e.prev
	e.prev.next = e.next
}

//Erase Erase a stateCache with physical id
//return erased memUsage
func (sc *StatsCache) Erase(deletedID int64) (memUsage int64) {
	sc.setLRUlist()
	e := sc.tables[deletedID]
	sc.LRURemove(e)
	if ptbl, ok := sc.tables[deletedID]; ok {
		memUsage -= ptbl.MemoryUsage()
	}
	sc.memUsage += memUsage

	delete(sc.tables, deletedID)
	return
}

//EraseLast erase last table statistic data in cache
//and table meta data not erased only used in Handle.updateStatsCache
//returns erased memUsage
func (sc *StatsCache) EraseLast() int64 {
	ID := sc.lruList.next.PhysicalID

	return sc.Insert(sc.tables[ID].CopyMeta())
}

//isEmpty returns if sc is empty
func (sc StatsCache) isEmpty() bool {
	return sc.lruList.next == sc.lruList
}
