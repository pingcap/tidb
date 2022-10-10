package autoid

import "sync/atomic"

type persist interface {
	syncID(dbID, tblID int64, val uint64, addr *int64, done <-chan struct{})
	loadID(dbID, tblID int64) uint64
}

type mockPersist struct {
	data map[autoIDKey]uint64
}

func (p *mockPersist) syncID(dbID, tblID int64, val uint64, addr *int64, done <-chan struct{}) {
	key := autoIDKey{dbID: dbID, tblID: tblID}
	p.data[key] = val
	atomic.StoreInt64(addr, int64(val))
	<-done
}

func (p *mockPersist) loadID(dbID, tblID int64) uint64 {
	key := autoIDKey{dbID: dbID, tblID: tblID}
	return p.data[key]
}
