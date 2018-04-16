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
	"sync"
	"sync/atomic"
	"testing"

	. "github.com/pingcap/check"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testLatchSuite{})

var baseTso uint64

type testLatchSuite struct {
	latches *Latches
}

func (s *testLatchSuite) SetUpTest(c *C) {
	s.latches = NewLatches(256)
}

func (s *testLatchSuite) newLock(keys [][]byte) (startTS uint64, lock Lock) {
	startTS = getTso()
	lock = s.latches.GenLock(startTS, keys)
	return
}

func getTso() uint64 {
	return atomic.AddUint64(&baseTso, uint64(1))
}

func (s *testLatchSuite) TestWakeUp(c *C) {
	keysA := [][]byte{
		[]byte("a"), []byte("b"), []byte("c"), []byte("c")}
	_, lockA := s.newLock(keysA)

	keysB := [][]byte{[]byte("d"), []byte("e"), []byte("a"), []byte("c")}
	startTSB, lockB := s.newLock(keysB)

	// A acquire lock success.
	acquired, stale := s.latches.Acquire(&lockA)
	c.Assert(stale, IsFalse)
	c.Assert(acquired, IsTrue)

	// B acquire lock failed.
	acquired, stale = s.latches.Acquire(&lockB)
	c.Assert(stale, IsFalse)
	c.Assert(acquired, IsFalse)

	// A release lock, and get wakeup list.
	commitTSA := getTso()
	wakeupList := s.latches.Release(&lockA, commitTSA)
	c.Assert(wakeupList[0], Equals, startTSB)

	// B acquire failed since startTSB has stale for some keys.
	acquired, stale = s.latches.Acquire(&lockB)
	c.Assert(stale, IsTrue)
	c.Assert(acquired, IsFalse)

	// B release lock since it received a stale.
	wakeupList = s.latches.Release(&lockB, 0)
	c.Assert(len(wakeupList), Equals, 0)

	// B restart:get a new startTS.
	startTSB = getTso()
	lockB = s.latches.GenLock(startTSB, keysB)
	acquired, stale = s.latches.Acquire(&lockB)
	c.Assert(acquired, IsTrue)
	c.Assert(stale, IsFalse)
}

type txn struct {
	keys    [][]byte
	startTS uint64
	lock    Lock
}

func newTxn(keys [][]byte, startTS uint64, lock Lock) txn {
	return txn{
		keys:    keys,
		startTS: startTS,
		lock:    lock,
	}
}

type txnScheduler struct {
	txns    map[uint64]*txn
	latches *Latches
	lock    sync.Mutex
	wait    *sync.WaitGroup
}

func newTxnScheduler(wait *sync.WaitGroup, latches *Latches) *txnScheduler {
	return &txnScheduler{
		txns:    make(map[uint64]*txn),
		latches: latches,
		wait:    wait,
	}
}

func (store *txnScheduler) runTxn(startTS uint64) {
	store.lock.Lock()
	txn, ok := store.txns[startTS]
	store.lock.Unlock()
	if !ok {
		panic(startTS)
	}
	acquired, stale := store.latches.Acquire(&txn.lock)

	if !stale && !acquired {
		return
	}
	commitTs := uint64(0)
	if stale {
		// restart Txn
		go store.newTxn(txn.keys)
	} else {
		// DO commit
		commitTs = getTso()
		store.wait.Done()
	}
	wakeupList := store.latches.Release(&txn.lock, commitTs)
	for _, s := range wakeupList {
		go store.runTxn(s)
	}
	store.lock.Lock()
	delete(store.txns, startTS)
	store.lock.Unlock()
}

func (store *txnScheduler) newTxn(keys [][]byte) {
	startTS := getTso()
	lock := store.latches.GenLock(startTS, keys)
	t := newTxn(keys, startTS, lock)
	store.lock.Lock()
	store.txns[t.startTS] = &t
	store.lock.Unlock()
	go store.runTxn(t.startTS)
}

func (s *testLatchSuite) TestWithConcurrency(c *C) {
	waitGroup := sync.WaitGroup{}
	txns := [][][]byte{
		{[]byte("a"), []byte("a"), []byte("b"), []byte("c")},
		{[]byte("a"), []byte("d"), []byte("e"), []byte("f")},
		{[]byte("e"), []byte("f"), []byte("g"), []byte("h")},
	}

	store := newTxnScheduler(&waitGroup, s.latches)
	waitGroup.Add(len(txns))
	for _, txn := range txns {
		go store.newTxn(txn)
	}
	waitGroup.Wait()
}
