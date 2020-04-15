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
	"sync/atomic"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/v4/store/tikv/oracle"
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

func (s *testLatchSuite) newLock(keys [][]byte) (startTS uint64, lock *Lock) {
	startTS = getTso()
	lock = s.latches.genLock(startTS, keys)
	return
}

func getTso() uint64 {
	return atomic.AddUint64(&baseTso, uint64(1))
}

func (s *testLatchSuite) TestWakeUp(c *C) {
	keysA := [][]byte{
		[]byte("a"), []byte("b"), []byte("c")}
	_, lockA := s.newLock(keysA)

	keysB := [][]byte{[]byte("d"), []byte("e"), []byte("a"), []byte("c")}
	startTSB, lockB := s.newLock(keysB)

	// A acquire lock success.
	result := s.latches.acquire(lockA)
	c.Assert(result, Equals, acquireSuccess)

	// B acquire lock failed.
	result = s.latches.acquire(lockB)
	c.Assert(result, Equals, acquireLocked)

	// A release lock, and get wakeup list.
	commitTSA := getTso()
	wakeupList := make([]*Lock, 0)
	lockA.SetCommitTS(commitTSA)
	wakeupList = s.latches.release(lockA, wakeupList)
	c.Assert(wakeupList[0].startTS, Equals, startTSB)

	// B acquire failed since startTSB has stale for some keys.
	result = s.latches.acquire(lockB)
	c.Assert(result, Equals, acquireStale)

	// B release lock since it received a stale.
	wakeupList = s.latches.release(lockB, wakeupList)
	c.Assert(wakeupList, HasLen, 0)

	// B restart:get a new startTS.
	startTSB = getTso()
	lockB = s.latches.genLock(startTSB, keysB)
	result = s.latches.acquire(lockB)
	c.Assert(result, Equals, acquireSuccess)
}

func (s *testLatchSuite) TestFirstAcquireFailedWithStale(c *C) {
	keys := [][]byte{
		[]byte("a"), []byte("b"), []byte("c")}
	_, lockA := s.newLock(keys)
	startTSB, lockB := s.newLock(keys)
	// acquire lockA success
	result := s.latches.acquire(lockA)
	c.Assert(result, Equals, acquireSuccess)
	// release lockA
	commitTSA := getTso()
	wakeupList := make([]*Lock, 0)
	lockA.SetCommitTS(commitTSA)
	s.latches.release(lockA, wakeupList)

	c.Assert(commitTSA, Greater, startTSB)
	// acquire lockB first time, should be failed with stale since commitTSA > startTSB
	result = s.latches.acquire(lockB)
	c.Assert(result, Equals, acquireStale)
	s.latches.release(lockB, wakeupList)
}

func (s *testLatchSuite) TestRecycle(c *C) {
	latches := NewLatches(8)
	now := time.Now()
	startTS := oracle.ComposeTS(oracle.GetPhysical(now), 0)
	lock := latches.genLock(startTS, [][]byte{
		[]byte("a"), []byte("b"),
	})
	lock1 := latches.genLock(startTS, [][]byte{
		[]byte("b"), []byte("c"),
	})
	c.Assert(latches.acquire(lock), Equals, acquireSuccess)
	c.Assert(latches.acquire(lock1), Equals, acquireLocked)
	lock.SetCommitTS(startTS + 1)
	var wakeupList []*Lock
	latches.release(lock, wakeupList)
	// Release lock will grant latch to lock1 automatically,
	// so release lock1 is called here.
	latches.release(lock1, wakeupList)

	lock2 := latches.genLock(startTS+3, [][]byte{
		[]byte("b"), []byte("c"),
	})
	c.Assert(latches.acquire(lock2), Equals, acquireSuccess)
	wakeupList = wakeupList[:0]
	latches.release(lock2, wakeupList)

	allEmpty := true
	for i := 0; i < len(latches.slots); i++ {
		latch := &latches.slots[i]
		if latch.queue != nil {
			allEmpty = false
		}
	}
	c.Assert(allEmpty, IsFalse)

	currentTS := oracle.ComposeTS(oracle.GetPhysical(now.Add(expireDuration)), 3)
	latches.recycle(currentTS)

	for i := 0; i < len(latches.slots); i++ {
		latch := &latches.slots[i]
		c.Assert(latch.queue, IsNil)
	}
}
