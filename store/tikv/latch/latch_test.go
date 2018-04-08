// Copyright 2018 PingCAP, Inc.
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

package latch

import (
	. "github.com/pingcap/check"
	"sync"
	"sync/atomic"
)

var _ = Suite(&testLatchSuite{})
var baseTso uint64

type testLatchSuite struct {
}

func (s *testLatchSuite) SetUpSuite(c *C) {
}

func (s *testLatchSuite) TearDownSuite(c *C) {
}

func getTso() uint64 {
	return atomic.AddUint64(&baseTso, uint64(1))
}

func (s *testLatchSuite) TestWakeUp(c *C) {
	latches := NewLatches(256)
	keysA := [][]byte{
		[]byte("a"), []byte("a"), []byte("b"), []byte("c")}
	startTsA := getTso()
	lockA := latches.GenLock(startTsA, keysA)

	keysB := [][]byte{[]byte("d"), []byte("e"), []byte("a"), []byte("c")}
	startTsB := getTso()
	lockB := latches.GenLock(startTsB, keysB)

	//A acquire lock success
	acquired, timeout := latches.Acquire(&lockA)
	c.Assert(timeout, IsFalse)
	c.Assert(acquired, IsTrue)

	// B acquire lock failed
	acquired, timeout = latches.Acquire(&lockB)
	c.Assert(timeout, IsFalse)
	c.Assert(acquired, IsFalse)

	// A release lock, and get wakeup list
	commitTSA := getTso()
	wakeupList := latches.Release(&lockA, commitTSA)
	c.Assert(wakeupList[0], Equals, startTsB)

	// B acquire failed since startTSB has timeout for some keys
	acquired, timeout = latches.Acquire(&lockB)
	c.Assert(timeout, IsTrue)
	c.Assert(acquired, IsFalse)

	// B release lock since it received a timeout
	wakeupList = latches.Release(&lockB, 0)
	c.Assert(len(wakeupList), Equals, 0)

	// B restart:get a new startTso
	startTsB = getTso()
	lockB = latches.GenLock(startTsB, keysB)
	acquired, timeout = latches.Acquire(&lockB)
	c.Assert(acquired, IsTrue)
	c.Assert(timeout, IsFalse)
}

type txn struct {
	keys    [][]byte
	startTs uint64
	lock    Lock
}

func newTxn(keys [][]byte, startTs uint64, lock Lock) txn {
	return txn{
		keys:    keys,
		startTs: startTs,
		lock:    lock,
	}
}

type txnStore struct {
	txns    map[uint64]*txn
	latches Latches
	lock    sync.Mutex
	wait    *sync.WaitGroup
}

func newTxnStore(wait *sync.WaitGroup) *txnStore {
	txnsMap := make(map[uint64]*txn)
	return &txnStore{
		txns:    txnsMap,
		latches: NewLatches(256),
		lock:    sync.Mutex{},
		wait:    wait,
	}
}

func (store *txnStore) runTxn(startTs uint64) {
	store.lock.Lock()
	txn, ok := store.txns[startTs]
	store.lock.Unlock()
	if !ok {
		panic(startTs)
	}
	acquired, timeout := store.latches.Acquire(&txn.lock)

	if !timeout && !acquired {
		return
	}
	commitTs := uint64(0)
	if !timeout {
		// DO 2pc
		commitTs = getTso()
	}
	wakeupList := store.latches.Release(&txn.lock, commitTs)
	for _, s := range wakeupList {
		go store.runTxn(s)
	}
	store.lock.Lock()
	delete(store.txns, startTs)
	store.lock.Unlock()
	if timeout {
		// restart Txn
		go store.newTxn(txn.keys)
	} else {
		store.wait.Done()
	}
}

func (store *txnStore) newTxn(keys [][]byte) {
	startTs := getTso()
	lock := store.latches.GenLock(startTs, keys)
	t := newTxn(keys, startTs, lock)
	store.lock.Lock()
	defer store.lock.Unlock()
	store.txns[t.startTs] = &t
	go store.runTxn(t.startTs)
}

func (s *testLatchSuite) TestWithConcurrency(c *C) {
	waitGroup := sync.WaitGroup{}
	txn1 := [][]byte{
		[]byte("a"), []byte("a"), []byte("b"), []byte("c")}
	txn2 := [][]byte{
		[]byte("a"), []byte("d"), []byte("e"), []byte("f")}
	txn3 := [][]byte{
		[]byte("e"), []byte("f"), []byte("g"), []byte("h")}
	waitGroup.Add(3)
	store := newTxnStore(&waitGroup)
	go store.newTxn(txn1)
	go store.newTxn(txn2)
	go store.newTxn(txn3)
	waitGroup.Wait()
}
