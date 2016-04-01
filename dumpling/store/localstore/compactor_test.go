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
	var k kv.Key
	totalCnt := 0
	for {
		var err error
		k, _, err = db.Seek(k)
		if err != nil {
			break
		}
		k = k.Next()
		totalCnt++
	}
	return totalCnt
}

func (s *localstoreCompactorTestSuite) TestCompactor(c *C) {
	store := createMemStore(time.Now().Nanosecond())
	db := store.(*dbStore).db
	store.(*dbStore).compactor.Stop()

	policy := compactPolicy{
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
	c.Assert(t, Equals, 6)

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

	compactor.Stop()
}

func (s *localstoreCompactorTestSuite) TestGetAllVersions(c *C) {
	store := createMemStore(time.Now().Nanosecond())
	compactor := store.(*dbStore).compactor
	txn, _ := store.Begin()
	txn.Set([]byte("a"), []byte("1"))
	txn.Commit()
	txn, _ = store.Begin()
	txn.Set([]byte("a"), []byte("2"))
	txn.Commit()
	txn, _ = store.Begin()
	txn.Set([]byte("b"), []byte("1"))
	txn.Commit()
	txn, _ = store.Begin()
	txn.Set([]byte("b"), []byte("2"))
	txn.Commit()

	keys, err := compactor.getAllVersions([]byte("a"))
	c.Assert(err, IsNil)
	c.Assert(keys, HasLen, 2)
}

// TestStartStop is to test `Panic: sync: WaitGroup is reused before previous Wait has returned`
// in Stop function.
func (s *localstoreCompactorTestSuite) TestStartStop(c *C) {
	store := createMemStore(time.Now().Nanosecond())
	db := store.(*dbStore).db

	for i := 0; i < 10000; i++ {
		policy := compactPolicy{
			SafePoint:       500,
			BatchDeleteCnt:  1,
			TriggerInterval: 100 * time.Millisecond,
		}
		compactor := newLocalCompactor(policy, db)
		compactor.Start()
		compactor.Stop()
		c.Logf("Test compactor stop and start %d times", i)
	}
}
