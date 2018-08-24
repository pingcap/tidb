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
		[]byte("a"), []byte("b"), []byte("c"), []byte("c")}
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
	wakeupList = s.latches.release(lockA, commitTSA, wakeupList)
	c.Assert(wakeupList[0].startTS, Equals, startTSB)

	// B acquire failed since startTSB has stale for some keys.
	result = s.latches.acquire(lockB)
	c.Assert(result, Equals, acquireStale)

	// B release lock since it received a stale.
	wakeupList = s.latches.release(lockB, 0, wakeupList)
	c.Assert(wakeupList, HasLen, 0)

	// B restart:get a new startTS.
	startTSB = getTso()
	lockB = s.latches.genLock(startTSB, keysB)
	result = s.latches.acquire(lockB)
	c.Assert(result, Equals, acquireSuccess)
}

func (s *testLatchSuite) TestFirstAcquireFailedWithStale(c *C) {
	keys := [][]byte{
		[]byte("a"), []byte("b"), []byte("c"), []byte("c")}
	_, lockA := s.newLock(keys)
	startTSB, lockB := s.newLock(keys)
	// acquire lockA success
	result := s.latches.acquire(lockA)
	c.Assert(result, Equals, acquireSuccess)
	// release lockA
	commitTSA := getTso()
	wakeupList := make([]*Lock, 0)
	s.latches.release(lockA, commitTSA, wakeupList)

	c.Assert(commitTSA, Greater, startTSB)
	// acquire lockB first time, should be failed with stale since commitTSA > startTSB
	result = s.latches.acquire(lockB)
	c.Assert(result, Equals, acquireStale)
	s.latches.release(lockB, 0, wakeupList)
}
