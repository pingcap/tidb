// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package localstore

import (
	"time"

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
		totalCnt++
	}
	return totalCnt
}

func (s *localstoreCompactorTestSuite) TestCompactor(c *C) {
	store := createMemStore()
	db := store.(*dbStore).db
	store.(*dbStore).compactor.Stop()

	policy := kv.CompactPolicy{
		SafePoint:       500,
		BatchDeleteCnt:  1,
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
	c.Assert(t, Equals, 3)

	compactor.Stop()
}
