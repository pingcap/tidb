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
	"math"
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

func (s *testGlobalConnIDSuite) TestLockFreePoolBasic(c *C) {
	const SizeInBits uint32 = 8
	const Size uint32 = 1<<SizeInBits - 1

	var (
		pool util.LockFreePool
		val  uint32
		ok   bool
		i    uint32
	)

	pool.Init(SizeInBits, math.MaxUint32)
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

func (s *testGlobalConnIDSuite) TestLockFreePoolInitEmpty(c *C) {
	const SizeInBits uint32 = 8
	const Size uint32 = 1<<SizeInBits - 1

	var (
		pool util.LockFreePool
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

var _ util.LocalConnIDPool = (*LockBasedPool)(nil)

// LockBasedPool implements LocalConnIDPool by lock-based manner.
// For benchmark purpose.
type LockBasedPool struct {
	_    uint64 // align to 64bits
	head uint32 // first available slot
	_    uint32 // padding to avoid false sharing
	tail uint32 // first empty slot. `head==tail` means empty.
	_    uint32 // padding to avoid false sharing
	cap  uint32

	mu    *sync.Mutex
	slots []uint32
}

func (p *LockBasedPool) Init(sizeInBits uint32, fillCount uint32) {
	p.mu = &sync.Mutex{}

	p.cap = 1 << sizeInBits
	p.slots = make([]uint32, p.cap)

	fillCount = mathutil.MinUint32(p.cap-1, fillCount)
	var i uint32
	for i = 0; i < fillCount; i++ {
		p.slots[i] = i + 1
	}
	for ; i < p.cap; i++ {
		p.slots[i] = util.PoolInvalidValue
	}

	p.head = 0
	p.tail = fillCount
}

func (p *LockBasedPool) Len() uint32 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.tail - p.head
}

func (p LockBasedPool) String() string {
	head := p.head
	tail := p.tail
	headVal := p.slots[head&(p.cap-1)]
	tailVal := p.slots[tail&(p.cap-1)]
	len := tail - head

	return fmt.Sprintf("cap:%v, len:%v; head:%x, slot:{%x}; tail:%x, slot:{%x}",
		p.cap, len, head, headVal, tail, tailVal)
}

func (p *LockBasedPool) Put(val uint32) (ok bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.tail-p.head == p.cap-1 { // full
		return false
	}

	p.slots[p.tail&(p.cap-1)] = val
	p.tail++
	return true
}

func (p *LockBasedPool) Get() (val uint32, ok bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.head == p.tail { // empty
		return util.PoolInvalidValue, false
	}

	val = p.slots[p.head&(p.cap-1)]
	p.head++
	return val, true
}

func prepareLockBasedPool(sizeInBits uint32, fillCount uint32) util.LocalConnIDPool {
	var pool LockBasedPool
	pool.Init(sizeInBits, fillCount)
	return &pool
}

func prepareLockFreePool(sizeInBits uint32, fillCount uint32, headPos uint32) util.LocalConnIDPool {
	var pool util.LockFreePool
	pool.Init(sizeInBits, fillCount)
	if headPos > 0 {
		pool.InitForTest(headPos, fillCount)
	}

	return &pool
}

func prepareConcurrencyTest(pool util.LocalConnIDPool, producers int, consumers int, requests int, total *int64) (ready chan struct{}, done chan struct{}, wgProducer *sync.WaitGroup, wgConsumer *sync.WaitGroup) {
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
					for !pool.Put(uint32(i)) {
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
			Loop:
				for {
					val, ok := pool.Get()
					if ok {
						sum += int64(val)
						continue
					}
					select {
					case <-done:
						break Loop
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

func doConcurrencyTest(ready chan struct{}, done chan struct{}, wgProducer *sync.WaitGroup, wgConsumer *sync.WaitGroup) {
	// logutil.BgLogger().Info("Init", zap.Stringer("pool", q))
	close(ready)
	wgProducer.Wait()
	// logutil.BgLogger().Info("Snapshot on producing done", zap.Stringer("pool", q))
	close(done)
	wgConsumer.Wait()
	// logutil.BgLogger().Info("Finally", zap.Stringer("pool", q))
}

func expectedConcurrencyTestResult(poolSizeInBits uint32, fillCount uint32, producers int, consumers int, requests int) (expected int64) {
	if producers > 0 && consumers > 0 {
		expected += (int64(requests) - 1) * int64(requests) / 2
	}
	if fillCount > 0 {
		fillCount = mathutil.MinUint32(1<<poolSizeInBits-1, fillCount)
		expected += (1 + int64(fillCount)) * int64(fillCount) / 2
	}
	return expected
}

func testLockFreePoolConcurrency(poolSizeInBits uint32, fillCount uint32, producers int, consumers int, requests int, headPos uint32) (expected, actual int64) {
	var total int64
	pool := prepareLockFreePool(poolSizeInBits, fillCount, headPos)
	ready, done, wgProducer, wgConsumer := prepareConcurrencyTest(pool, producers, consumers, requests, &total)

	doConcurrencyTest(ready, done, wgProducer, wgConsumer)

	expected = expectedConcurrencyTestResult(poolSizeInBits, fillCount, producers, consumers, requests)
	return expected, atomic.LoadInt64(&total)
}

func testLockBasedPoolConcurrency(poolSizeInBits uint32, producers int, consumers int, requests int) (expected, actual int64) {
	var total int64
	pool := prepareLockBasedPool(poolSizeInBits, 0)
	ready, done, wgProducer, wgConsumer := prepareConcurrencyTest(pool, producers, consumers, requests, &total)

	doConcurrencyTest(ready, done, wgProducer, wgConsumer)

	expected = expectedConcurrencyTestResult(poolSizeInBits, 0, producers, consumers, requests)
	return expected, atomic.LoadInt64(&total)
}

func (s *testGlobalConnIDSuite) TestLockFreePoolBasicConcurrencySafety(c *C) {
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

	expected, actual = testLockFreePoolConcurrency(sizeInBits, fillCount, producers, consumers, requests, 0)
	c.Assert(actual, Equals, expected)

	// test overflow of head & tail
	expected, actual = testLockFreePoolConcurrency(sizeInBits, fillCount, producers, consumers, requests, headPos)
	c.Assert(actual, Equals, expected)
}

func (s *testGlobalConnIDSuite) TestLockBasedPoolConcurrencySafety(c *C) {
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

	expected, actual = testLockBasedPoolConcurrency(sizeInBits, producers, consumers, requests)
	c.Assert(actual, Equals, expected)
}

type poolConcurrencyTestCase struct {
	sizeInBits uint32
	fillCount  uint32
	producers  int
	consumers  int
	requests   int64
}

func (ta poolConcurrencyTestCase) String() string {
	return fmt.Sprintf("size:%v, fillCount:%v, producers:%v, consumers:%v, requests:%v",
		1<<ta.sizeInBits, ta.fillCount, ta.producers, ta.consumers, ta.requests)
}

func (s *testGlobalConnIDSuite) TestLockFreePoolConcurrencySafety(c *C) {
	const (
		poolSizeInBits = 16
		requests       = 1 << 20
		concurrency    = 1000
	)

	// Test cases from Anthony Williams, "C++ Concurrency in Action, 2nd", 11.2.2 "Locating concurrency-related bugs by testing":
	cases := []poolConcurrencyTestCase{
		// #1 Multiple threads calling pop() on a partially full queue with insufficient items for all threads
		{sizeInBits: 4, fillCount: 1 << 3, producers: 0, consumers: 32, requests: requests},
		// #2 Multiple threads calling push() while one thread calls pop() on an empty queue
		{sizeInBits: poolSizeInBits, fillCount: 0, producers: concurrency, consumers: 1, requests: requests},
		// #3 Multiple threads calling push() while one thread calls pop() on a full queue
		{sizeInBits: poolSizeInBits, fillCount: 0xffff_ffff, producers: concurrency, consumers: 1, requests: requests},
		// #4 Multiple threads calling push() while multiple threads call pop() on an empty queue
		{sizeInBits: poolSizeInBits, fillCount: 0, producers: concurrency, consumers: concurrency, requests: requests},
		// #5 Multiple threads calling push() while multiple threads call pop() on a full queue
		{sizeInBits: poolSizeInBits, fillCount: 0xffff_ffff, producers: concurrency, consumers: concurrency, requests: requests},
	}

	for i, ca := range cases {
		expected, actual := testLockFreePoolConcurrency(ca.sizeInBits, ca.fillCount, ca.producers, ca.consumers, requests, 0)
		c.Assert(actual, Equals, expected, Commentf("case #%v: %v", i+1, ca))
	}
}

func BenchmarkPoolConcurrency(b *testing.B) {
	b.ReportAllocs()

	const (
		poolSizeInBits = 16
		requests       = 1 << 18
	)

	cases := []poolConcurrencyTestCase{
		{producers: 1, consumers: 1},
		{producers: 3, consumers: 3},
		{producers: 10, consumers: 10},
		{producers: 100, consumers: 100},
		{producers: 1000, consumers: 1000},
		{producers: 10000, consumers: 10000},
	}

	for _, ta := range cases {
		b.Run(fmt.Sprintf("LockBasedPool: P:C: %v:%v", ta.producers, ta.consumers), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				var total int64
				pool := prepareLockBasedPool(poolSizeInBits, 0)
				ready, done, wgProducer, wgConsumer := prepareConcurrencyTest(pool, ta.producers, ta.consumers, requests, &total)

				b.StartTimer()
				doConcurrencyTest(ready, done, wgProducer, wgConsumer)
				b.StopTimer()

				expected := expectedConcurrencyTestResult(poolSizeInBits, 0, ta.producers, ta.consumers, requests)
				actual := atomic.LoadInt64(&total)
				if expected != actual {
					b.Fatalf("concurrency safety fail, expected:%v, actual:%v", expected, actual)
				}
			}
		})

		b.Run(fmt.Sprintf("LockFreePool: P:C: %v:%v", ta.producers, ta.consumers), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				var total int64
				pool := prepareLockFreePool(poolSizeInBits, 0, 0)
				ready, done, wgProducer, wgConsumer := prepareConcurrencyTest(pool, ta.producers, ta.consumers, requests, &total)

				b.StartTimer()
				doConcurrencyTest(ready, done, wgProducer, wgConsumer)
				b.StopTimer()

				expected := expectedConcurrencyTestResult(poolSizeInBits, 0, ta.producers, ta.consumers, requests)
				actual := atomic.LoadInt64(&total)
				if expected != actual {
					b.Fatalf("concurrency safety fail, expected:%v, actual:%v", expected, actual)
				}
			}
		})
	}
}

func benchmarkLocalConnIDAllocator32(b *testing.B, a *util.LocalConnIDAllocator32) {
	var id uint64

	// allocate local conn ID.
	for {
		var ok bool
		if id, ok = a.Allocate(); ok {
			break
		}
		runtime.Gosched()
	}

	// deallocate local conn ID.
	if err := a.Deallocate(id); err != nil {
		b.Fatal("pool unexpected full")
	}
}

func BenchmarkLocalConnIDAllocator(b *testing.B) {
	b.ReportAllocs()

	concurrencyCases := []int{1, 3, 10, 100, 1000, 10000}
	for _, concurrency := range concurrencyCases {
		b.Run(fmt.Sprintf("Allocator 64 x%v", concurrency), func(b *testing.B) {
			const ServerID = 42
			clients := make(map[uint64]struct{})
			mu := sync.RWMutex{}
			existedChecker := func(id uint64) bool {
				mu.RLock()
				_, ok := clients[id]
				mu.RUnlock()
				return ok
			}

			a := util.LocalConnIDAllocator64{}
			a.Init(existedChecker)

			b.SetParallelism(concurrency)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					id := a.Allocate(ServerID)
					a.Deallocate(id)
				}
			})
		})

		b.Run(fmt.Sprintf("Allocator 32(LockBased) x%v", concurrency), func(b *testing.B) {
			a := util.LocalConnIDAllocator32{}
			a.Init(&LockBasedPool{})

			b.SetParallelism(concurrency)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					benchmarkLocalConnIDAllocator32(b, &a)
				}
			})
		})

		b.Run(fmt.Sprintf("LockFreePool x%v", concurrency), func(b *testing.B) {
			a := util.LocalConnIDAllocator32{}
			a.Init(nil)

			b.SetParallelism(concurrency)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					benchmarkLocalConnIDAllocator32(b, &a)
				}
			})
		})
	}
}
