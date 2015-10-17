package localstore

import (
	"time"

	"github.com/ngaut/log"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/localstore/engine"
)

var _ = Suite(&localstoreCompactorTestSuite{})

type localstoreCompactorTestSuite struct {
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

func (s *localstoreCompactorTestSuite) TestCompactor(c *C) {
	store := createMemStore()
	db := store.(*dbStore).db
	store.(*dbStore).compactor.Stop()

	policy := kv.CompactorPolicy{
		SafeTime:        500,
		BatchDeleteSize: 1,
		TriggerInterval: 100 * time.Millisecond,
	}
	compactor := newLocalCompactor(policy, db)
	store.(*dbStore).compactor = compactor

	compactor.Start()

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

	// Simulating timeout
	time.Sleep(1 * time.Second)
	// Touch a, tigger GC
	txn, _ = store.Begin()
	txn.Set([]byte("a"), []byte("b"))
	txn.Commit()
	time.Sleep(1 * time.Second)
	// Do background GC
	t = count(db)
	c.Assert(t, Equals, 2)
}
