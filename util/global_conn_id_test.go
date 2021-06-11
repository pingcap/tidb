// Copyright 2021 PingCAP, Inc.
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

package util_test

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cznic/mathutil"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util"
)

var _ = Suite(&testGlobalConnIDSuite{})

type testGlobalConnIDSuite struct {
}

func (s *testGlobalConnIDSuite) SetUpSuite(c *C) {
}

func (s *testGlobalConnIDSuite) TearDownSuite(c *C) {
}

func (s *testGlobalConnIDSuite) TestParse(c *C) {
	connID := util.GlobalConnID{
		Is64bits:    true,
		ServerID:    1001,
		LocalConnID: 123,
	}
	c.Assert(connID.ID(), Equals, (uint64(1001)<<41)|(uint64(123)<<1)|1)

	var (
		err         error
		isTruncated bool
	)

	// exceeds int64
	_, _, err = util.ParseGlobalConnID(0x80000000_00000321)
	c.Assert(err, NotNil)

	// 64bits truncated
	_, isTruncated, err = util.ParseGlobalConnID(101)
	c.Assert(err, IsNil)
	c.Assert(isTruncated, IsTrue)

	// 64bits
	id1 := (uint64(1001) << 41) | (uint64(123) << 1) | 1
	connID1, isTruncated, err := util.ParseGlobalConnID(id1)
	c.Assert(err, IsNil)
	c.Assert(isTruncated, IsFalse)
	c.Assert(connID1.ServerID, Equals, uint64(1001))
	c.Assert(connID1.LocalConnID, Equals, uint64(123))
	c.Assert(connID1.Is64bits, IsTrue)

	// exceeds uint32
	_, _, err = util.ParseGlobalConnID(0x1_00000320)
	c.Assert(err, NotNil)

	// 32bits
	id2 := (uint64(2002) << 21) | (uint64(321) << 1)
	connID2, isTruncated, err := util.ParseGlobalConnID(id2)
	c.Assert(err, IsNil)
	c.Assert(isTruncated, IsFalse)
	c.Assert(connID2.ServerID, Equals, uint64(2002))
	c.Assert(connID2.LocalConnID, Equals, uint64(321))
	c.Assert(connID2.Is64bits, IsFalse)
}

func (s *testGlobalConnIDSuite) TestPoolBasic(c *C) {
	const SizeInBits uint32 = 8
	const Size uint32 = 1<<SizeInBits - 1

	var (
		pool util.Pool
		val  uint32
		ok   bool
		i    uint32
	)

	pool.Init(SizeInBits, 0xffffffff)
	c.Assert(pool.Len(), Equals, Size)

	// get all.
	for i = 1; i <= Size; i++ {
		val, ok = pool.Get()
		c.Assert(ok, IsTrue)
		c.Assert(val, Equals, i)
	}
	_, ok = pool.Get()
	c.Assert(ok, IsFalse)
	c.Assert(pool.Len(), Equals, uint32(0))

	// put to full.
	for i = 1; i <= Size; i++ {
		ok = pool.Put(i)
		c.Assert(ok, IsTrue)
	}
	ok = pool.Put(0)
	c.Assert(ok, IsFalse)
	c.Assert(pool.Len(), Equals, Size)

	// get all.
	for i = 1; i <= Size; i++ {
		val, ok = pool.Get()
		c.Assert(ok, IsTrue)
		c.Assert(val, Equals, i)
	}
	_, ok = pool.Get()
	c.Assert(ok, IsFalse)
	c.Assert(pool.Len(), Equals, uint32(0))
}

func (s *testGlobalConnIDSuite) TestPoolInitEmpty(c *C) {
	const SizeInBits uint32 = 8
	const Size uint32 = 1<<SizeInBits - 1

	var (
		pool util.Pool
		val  uint32
		ok   bool
		i    uint32
	)

	pool.Init(SizeInBits, 0)
	c.Assert(pool.Len(), Equals, uint32(0))

	// put to full.
	for i = 1; i <= Size; i++ {
		ok = pool.Put(i)
		c.Assert(ok, IsTrue)
	}
	ok = pool.Put(0)
	c.Assert(ok, IsFalse)
	c.Assert(pool.Len(), Equals, Size)

	// get all.
	for i = 1; i <= Size; i++ {
		val, ok = pool.Get()
		c.Assert(ok, IsTrue)
		c.Assert(val, Equals, i)
	}
	_, ok = pool.Get()
	c.Assert(ok, IsFalse)
	c.Assert(pool.Len(), Equals, uint32(0))
}

type queue interface {
	fmt.Stringer
	Len() uint32
	Put(val uint32) (ok bool)
	Get() (val uint32, ok bool)
}

var (
	_ queue = &util.Pool{}
	_ queue = &lockPool{}
)

type lockPool struct {
	_align    uint64
	head      uint32 // first available slot
	_padding1 uint32 // padding to avoid false sharing
	tail      uint32 // first empty slot. `head==tail` means empty.
	_padding2 uint32
	cap       uint32

	mu    sync.Mutex
	slots []uint32
}

func (p *lockPool) Init(sizeInBits uint32) {
	p.cap = 1 << sizeInBits
	p.slots = make([]uint32, p.cap)
}

func (p *lockPool) Len() uint32 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.tail - p.head
}

func (p lockPool) String() string {
	head := p.head
	tail := p.tail
	headVal := p.slots[head&(p.cap-1)]
	tailVal := p.slots[tail&(p.cap-1)]
	len := tail - head

	return fmt.Sprintf("cap:%v, len:%v; head:%x, slot:{%x}; tail:%x, slot:{%x}",
		p.cap, len, head, headVal, tail, tailVal)
}

func (p *lockPool) Put(val uint32) (ok bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.tail-p.head == p.cap-1 { // full
		return false
	}

	p.slots[p.tail&(p.cap-1)] = val
	p.tail++
	return true
}

func (p *lockPool) Get() (val uint32, ok bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.head == p.tail { // empty
		return util.PoolInvalidValue, false
	}

	val = p.slots[p.head&(p.cap-1)]
	p.head++
	return val, true
}

func prepareLockPool(poolSizeInBits uint32) queue {
	var pool lockPool
	pool.Init(poolSizeInBits)
	return &pool
}

func preparePool(poolSizeInBits uint32, fillCount uint32, headPos uint32) queue {
	var pool util.Pool
	pool.Init(poolSizeInBits, fillCount)
	if headPos > 0 {
		pool.InitForTest(headPos, fillCount)
	}

	return &pool
}

func prepareConcurrencyTest(q queue, producers int, consumers int, requests int, total *int64) (ready chan struct{}, done chan struct{}, wgProducer *sync.WaitGroup, wgConsumer *sync.WaitGroup) {
	ready = make(chan struct{})
	done = make(chan struct{})

	wgProducer = &sync.WaitGroup{}
	if producers > 0 {
		reqsPerProducer := (requests + producers - 1) / producers
		wgProducer.Add(producers)
		for p := 0; p < producers; p++ {
			go func(p int) {
				defer wgProducer.Done()
				<-ready

				for i := p * reqsPerProducer; i < (p+1)*reqsPerProducer && i < requests; i++ {
					for !q.Put(uint32(i)) {
						runtime.Gosched()
					}
				}
			}(p)
		}
	}

	wgConsumer = &sync.WaitGroup{}
	if consumers > 0 {
		wgConsumer.Add(consumers)
		for c := 0; c < consumers; c++ {
			go func(c int) {
				defer wgConsumer.Done()
				<-ready

				var sum int64
			L:
				for {
					val, ok := q.Get()
					if ok {
						sum += int64(val)
						continue
					}
					select {
					case <-done:
						break L
					default:
						runtime.Gosched()
					}
				}
				atomic.AddInt64(total, sum)
			}(c)
		}
	}

	return ready, done, wgProducer, wgConsumer
}

func testPoolConcurrency(poolSizeInBits uint32, fillCount uint32, producers int, consumers int, requests int, headPos uint32) (expected, actual int64) {
	var total int64
	q := preparePool(poolSizeInBits, fillCount, headPos)
	ready, done, wgProducer, wgConsumer := prepareConcurrencyTest(q, producers, consumers, requests, &total)

	// logutil.BgLogger().Info("Init", zap.Stringer("pool", pool))
	close(ready)
	wgProducer.Wait()
	// logutil.BgLogger().Info("Snapshot on producing done", zap.Stringer("pool", pool))
	close(done)
	wgConsumer.Wait()
	// logutil.BgLogger().Info("Finally", zap.Stringer("pool", pool))

	if producers > 0 && consumers > 0 {
		expected += (int64(requests) - 1) * int64(requests) / 2
	}
	if fillCount > 0 {
		fillCount = mathutil.MinUint32(1<<poolSizeInBits-1, fillCount)
		expected += (1 + int64(fillCount)) * int64(fillCount) / 2
	}

	return expected, atomic.LoadInt64(&total)
}

func testLockPoolConcurrency(poolSizeInBits uint32, producers int, consumers int, requests int) (expected, actual int64) {
	var total int64
	q := prepareLockPool(poolSizeInBits)
	ready, done, wgProducer, wgConsumer := prepareConcurrencyTest(q, producers, consumers, requests, &total)

	// logutil.BgLogger().Info("Init", zap.Stringer("pool", pool))
	close(ready)
	wgProducer.Wait()
	// logutil.BgLogger().Info("Snapshot on producing done", zap.Stringer("pool", pool))
	close(done)
	wgConsumer.Wait()
	// logutil.BgLogger().Info("Finally", zap.Stringer("pool", pool))

	if producers > 0 && consumers > 0 {
		expected += (int64(requests) - 1) * int64(requests) / 2
	}

	return expected, atomic.LoadInt64(&total)
}

func (s *testGlobalConnIDSuite) TestPoolBasicConcurrencySafety(c *C) {
	var (
		expected int64
		actual   int64
	)

	const (
		sizeInBits = 8
		fillCount  = 0
		producers  = 20
		consumers  = 20
		requests   = 1 << 20
		headPos    = uint32(0x1_0000_0000 - (1 << (sizeInBits + 8)))
	)

	expected, actual = testPoolConcurrency(sizeInBits, fillCount, producers, consumers, requests, 0)
	c.Assert(actual, Equals, expected)

	// test overflow of head & tail
	expected, actual = testPoolConcurrency(sizeInBits, fillCount, producers, consumers, requests, headPos)
	c.Assert(actual, Equals, expected)
}

func (s *testGlobalConnIDSuite) TestLockPoolConcurrencySafety(c *C) {
	var (
		expected int64
		actual   int64
	)

	const (
		sizeInBits = 8
		producers  = 20
		consumers  = 20
		requests   = 1 << 20
	)

	expected, actual = testLockPoolConcurrency(sizeInBits, producers, consumers, requests)
	c.Assert(actual, Equals, expected)
}

type poolConcurrencyTestCase struct {
	sizeInBits uint32
	fillCount  uint32
	producers  int
	consumers  int
}

func (ta poolConcurrencyTestCase) String() string {
	return fmt.Sprintf("sizeInBits:%v, fillCount:%v, producers:%v, consumers:%v",
		ta.sizeInBits, ta.fillCount, ta.producers, ta.consumers)
}

func (s *testGlobalConnIDSuite) TestPoolConcurrencySafety(c *C) {
	const (
		requests = 1 << 20
	)

	// Test cases from "C++ Concurrency in Action", 11.2.2 "Locating concurrency-related bugs by testing":
	cases := []poolConcurrencyTestCase{
		// #1 Multiple threads calling pop() on a partially full queue with insufficient items for all threads
		{sizeInBits: 4, fillCount: 8, producers: 0, consumers: 32},
		// #2 Multiple threads calling push() while one thread calls pop() on an empty queue
		{sizeInBits: 12, fillCount: 0, producers: 20, consumers: 1},
		// #3 Multiple threads calling push() while one thread calls pop() on a full queue
		{sizeInBits: 12, fillCount: 0xffff_ffff, producers: 20, consumers: 1},
		// #4 Multiple threads calling push() while multiple threads call pop() on an empty queue
		{sizeInBits: 12, fillCount: 0, producers: 20, consumers: 20},
		// #5 Multiple threads calling push() while multiple threads call pop() on a full queue
		{sizeInBits: 12, fillCount: 0xffff_ffff, producers: 20, consumers: 20},
	}

	for i, ca := range cases {
		expected, actual := testPoolConcurrency(ca.sizeInBits, ca.fillCount, ca.producers, ca.consumers, requests, 0)
		c.Assert(actual, Equals, expected, Commentf("case #%v: %v", i+1, ca))
	}
}

func BenchmarkPoolConcurrency(b *testing.B) {
	b.ReportAllocs()

	const (
		poolSizeInBits = 12
	)

	requests := []int{1 << 20}
	producers := []int{2}
	consumers := []int{2}

	for _, req := range requests {
		for idx := range producers {
			producer := producers[idx]
			consumer := consumers[idx]

			b.Run("LockPool", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					b.StopTimer()
					var total int64
					q := prepareLockPool(poolSizeInBits)
					ready, done, wgProducer, wgConsumer := prepareConcurrencyTest(q, producer, consumer, req, &total)

					b.StartTimer()
					close(ready)
					wgProducer.Wait()
					close(done)
					wgConsumer.Wait()

					b.StopTimer()
				}
			})

			b.Run("LockFreePool", func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					b.StopTimer()
					var total int64
					q := preparePool(poolSizeInBits, 0, 0)
					ready, done, wgProducer, wgConsumer := prepareConcurrencyTest(q, producer, consumer, req, &total)

					b.StartTimer()
					close(ready)
					wgProducer.Wait()
					close(done)
					wgConsumer.Wait()

					b.StopTimer()
				}
			})
		}
	}
}
