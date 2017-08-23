// Copyright 2016 PingCAP, Inc.
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

package tikv

import (
	"fmt"
	"time"

	"github.com/ngaut/log"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/kv"
)

type testSafePointSuite struct {
	store   *tikvStore
	oracle   *mockOracle
	gcWorker *GCWorker
	prefix  string
}

var _ = Suite(&testSafePointSuite{})

func (s *testSafePointSuite) SetUpSuite(c *C) {
	s.store = newTestStore(c)
	s.oracle = &mockOracle{}
	s.store.oracle = s.oracle
	_, err := tidb.BootstrapSession(s.store)
	c.Assert(err, IsNil)
	gcWorker, err := NewGCWorker(s.store)
	c.Assert(err, IsNil)
	s.gcWorker = gcWorker
	s.prefix = fmt.Sprintf("seek_%d", time.Now().Unix())
	log.Error("start SetupSuite!\n")
}

func (s *testSafePointSuite) TearDownSuite(c *C) {
	err := s.store.Close()
	c.Assert(err, IsNil)
}

func (s *testSafePointSuite) beginTxn(c *C) *tikvTxn {
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	return txn.(*tikvTxn)
}

func mymakeKeys(rowNum int, prefix string) []kv.Key {
	keys := make([]kv.Key, 0, rowNum)
	for i := 0; i < rowNum; i++ {
		k := encodeKey(prefix, s08d("key", i))
		keys = append(keys, k)
	}
	return keys
}

func (s *testSafePointSuite) TestSafePoint(c *C) {
	log.Error("Start TestSafePoint!\n")
	txn := s.beginTxn(c)
	for i := 0; i < 10; i++ {
		set_err := txn.Set(encodeKey(s.prefix, s08d("key", i)), valueBytes(i))
		c.Assert(set_err, IsNil)
	}
	commit_err := txn.Commit()
	c.Assert(commit_err, IsNil)

	// for txn get
	txn2 := s.beginTxn(c)
	_, get_err := txn2.Get(encodeKey(s.prefix, s08d("key", 0)))
	c.Assert(get_err, IsNil)

	for {
		log.Error("Enter For!\n")
		log.Error("startTS:%v, write safePoint:%v", txn2.startTS, txn2.startTS + 10)
		s.store.saveUint64(gcSavedSafePoint, txn2.startTS + 10)

		log.Error("Start Fetch SafePoint\n")
		newSafePoint, load_err := s.store.loadUint64(gcSavedSafePoint)
		if load_err == nil {
			s.store.spMutex.Lock()
			s.store.safePoint = newSafePoint
			s.store.spTime = time.Now()
			s.store.spMutex.Unlock()
			log.Error("[safepoint load OK:%v]\n", s.store.safePoint)
			log.Error("Break For!\n")
			break
		} else {
			log.Error("TestSafePoint Read Error: %v", load_err)
			time.Sleep(5 * time.Second)
		}
	}

	log.Error("Break For Success!\n")
	_, get_err2 := txn2.Get(encodeKey(s.prefix, s08d("key", 0)))
	c.Assert(get_err2, NotNil)
	
	// for txn seek
	txn3 := s.beginTxn(c)
	for {
		log.Error("Enter For!\n")
		log.Error("startTS:%v, write safePoint:%v", txn3.startTS, txn3.startTS + 10)
		s.store.saveUint64(gcSavedSafePoint, txn3.startTS + 10)

		log.Error("Start Fetch SafePoint\n")
		newSafePoint, load_err := s.store.loadUint64(gcSavedSafePoint)
		if load_err == nil {
			s.store.spMutex.Lock()
			s.store.safePoint = newSafePoint
			s.store.spTime = time.Now()
			s.store.spMutex.Unlock()
			log.Error("[safepoint load OK:%v]\n", s.store.safePoint)
			log.Error("Break For!\n")
			break
		} else {
			log.Error("TestSafePoint Read Error: %v", load_err)
			time.Sleep(5 * time.Second)
		}
	}

	_, seek_err := txn3.Seek(encodeKey(s.prefix, ""))
	c.Assert(seek_err, NotNil)

	// for snapshot batchGet
	keys := mymakeKeys(10, s.prefix)
	txn4 := s.beginTxn(c)
	for {
		log.Error("Enter For!\n")
		log.Error("startTS:%v, write safePoint:%v", txn4.startTS, txn4.startTS + 10)
		s.store.saveUint64(gcSavedSafePoint, txn4.startTS + 10)

		log.Error("Start Fetch SafePoint\n")
		newSafePoint, load_err := s.store.loadUint64(gcSavedSafePoint)
		if load_err == nil {
			s.store.spMutex.Lock()
			s.store.safePoint = newSafePoint
			s.store.spTime = time.Now()
			s.store.spMutex.Unlock()
			log.Error("[safepoint load OK:%v]\n", s.store.safePoint)
			log.Error("Break For!\n")
			break
		} else {
			log.Error("TestSafePoint Read Error: %v", load_err)
			time.Sleep(5 * time.Second)
		}
	}

	snapshot := newTiKVSnapshot(s.store, kv.Version{Ver: txn4.StartTS()})
	_, batchget_err := snapshot.BatchGet(keys)
	c.Assert(batchget_err, NotNil)
}
