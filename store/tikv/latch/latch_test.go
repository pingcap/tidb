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
	result := s.latches.Acquire(&lockA)
	c.Assert(result, Equals, AcquireSuccess)

	// B acquire lock failed.
	result = s.latches.Acquire(&lockB)
	c.Assert(result, Equals, AcquireLocked)

	// A release lock, and get wakeup list.
	commitTSA := getTso()
	wakeupList := s.latches.Release(&lockA, commitTSA)
	c.Assert(wakeupList[0].startTS, Equals, startTSB)

	// B acquire failed since startTSB has stale for some keys.
	result = s.latches.Acquire(&lockB)
	c.Assert(result, Equals, AcquireStale)

	// B release lock since it received a stale.
	wakeupList = s.latches.Release(&lockB, 0)
	c.Assert(len(wakeupList), Equals, 0)

	// B restart:get a new startTS.
	startTSB = getTso()
	lockB = s.latches.GenLock(startTSB, keysB)
	result = s.latches.Acquire(&lockB)
	c.Assert(result, Equals, AcquireSuccess)
}

// txnLock wraps Lock to provide a real lock, when Lock() fail, the goroutine will block.
type txnLock struct {
	lock     Lock
	commitTS uint64
	sched    *txnScheduler
}

func (l *txnLock) Lock() (stale bool) {
	l.lock.wg.Add(1)
	result := l.sched.latches.Acquire(&l.lock)
	if result == AcquireLocked {
		l.lock.wg.Wait()
		result = l.lock.result
	}

	if result == AcquireSuccess {
		return false
	} else if result == AcquireStale {
		return true
	}
	panic("should never run here")
}

func (l *txnLock) Unlock(commitTS uint64) {
	l.commitTS = commitTS
	l.sched.msgCh <- l
}

// Design Note:
// Each txn corresponds to their own goroutine, and the global scheduler runs in a goroutine.
// If the txn is blocked by the lock, the goroutine will sleep, until the scheduler wake up it.
// When a txn releases lock, it sends a message to scheduler, then the scheduler may wakeup
// runnable txns.

type txnScheduler struct {
	latches *Latches
	msgCh   chan *txnLock
}

func newTxnScheduler(latches *Latches) *txnScheduler {
	return &txnScheduler{
		latches: latches,
		msgCh:   make(chan *txnLock, 100),
	}
}

// Loop runs in txnScheduler's goroutine.
func (sched *txnScheduler) Loop() {
	for txnLock := range sched.msgCh {
		lock := &txnLock.lock
		wakeupList := sched.latches.Release(lock, txnLock.commitTS)
		if len(wakeupList) > 0 {
			sched.handleWakeupList(wakeupList)
		}
	}
}

func (sched *txnScheduler) handleWakeupList(wakeupList []*Lock) {
	for _, lock := range wakeupList {
		result := sched.latches.Acquire(lock)
		switch result {
		case AcquireSuccess:
			lock.result = AcquireSuccess
			lock.wg.Done()
		case AcquireStale:
			lock.result = AcquireStale
			lock.wg.Done()
		case AcquireLocked:
		}
	}
}

func (sched *txnScheduler) newLock(startTS uint64, keys [][]byte) *txnLock {
	return &txnLock{
		lock:  sched.latches.GenLock(startTS, keys),
		sched: sched,
	}
}

func (sched *txnScheduler) Close() {
	close(sched.msgCh)
}

func (s *testLatchSuite) TestWithConcurrency(c *C) {
	txns := [][][]byte{
		{[]byte("a"), []byte("a"), []byte("b"), []byte("c")},
		{[]byte("a"), []byte("d"), []byte("e"), []byte("f")},
		{[]byte("e"), []byte("f"), []byte("g"), []byte("h")},
	}
	sched := newTxnScheduler(s.latches)
	go sched.Loop()
	defer sched.Close()

	var wg sync.WaitGroup
	wg.Add(len(txns))
	for i := 0; i < len(txns); i++ {
		txn := txns[i]
		go func(txn [][]byte, wg *sync.WaitGroup) {
			lock := sched.newLock(getTso(), txn)
			lock.Lock()
			lock.Unlock(getTso())
			wg.Done()
		}(txn, &wg)
	}
	wg.Wait()
}
