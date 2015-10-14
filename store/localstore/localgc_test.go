package localstore

import (
	"time"

	"github.com/ngaut/log"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/localstore/engine"
)

var _ = Suite(&localstoreGCTestSuite{})

type localstoreGCTestSuite struct {
}

func count(db engine.DB) int {
	it, _ := db.Seek([]byte{0})
	defer it.Release()
	totalCnt := 0
	for it.Next() {
		log.Error(it.Key())
		totalCnt++
	}
	return totalCnt
}

func (s *localstoreGCTestSuite) TestGC(c *C) {
	store := createMemStore()
	db := store.(*dbStore).db

	gc := newLocalGC(kv.GCPolicy{2, 100 * time.Millisecond}, db)
	gc.Start()

	txn, _ := store.Begin()
	txn.Set([]byte("a"), []byte("1"))
	txn.Commit()

	txn, _ = store.Begin()
	txn.Set([]byte("a"), []byte("2"))
	txn.Commit()

	txn, _ = store.Begin()
	txn.Set([]byte("a"), []byte("3"))
	txn.Commit()

	txn, _ = store.Begin()
	txn.Set([]byte("a"), []byte("3"))
	txn.Commit()

	txn, _ = store.Begin()
	txn.Set([]byte("a"), []byte("4"))
	txn.Commit()

	txn, _ = store.Begin()
	txn.Set([]byte("a"), []byte("5"))
	txn.Commit()

	t := count(db)
	c.Assert(t, Equals, 7)
	// Do background GC
	time.Sleep(5 * time.Second)
	t = count(db)
	c.Assert(t, Equals, 2)
}
