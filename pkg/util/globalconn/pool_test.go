// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package globalconn_test

import (
	"fmt"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cznic/mathutil"
	"github.com/pingcap/tidb/pkg/util/globalconn"
	"github.com/stretchr/testify/assert"
)

func TestAutoIncPool(t *testing.T) {
	assert := assert.New(t)

	const SizeInBits uint32 = 8
	const Size uint64 = 1 << SizeInBits
	const TryCnt = 4

	var (
		pool globalconn.AutoIncPool
		val  uint64
		ok   bool
		i    uint64
	)

	pool.InitExt(Size, true, TryCnt)
	assert.Equal(int(Size), pool.Cap())
	assert.Equal(0, pool.Len())

	// get all.
	for i = 1; i < Size; i++ {
		val, ok = pool.Get()
		assert.True(ok)
		assert.Equal(i, val)
	}
	val, ok = pool.Get()
	assert.True(ok)
	assert.Equal(uint64(0), val) // wrap around to 0
	assert.Equal(int(Size), pool.Len())

	_, ok = pool.Get() // exhausted. try TryCnt times, lastID is added to 0+TryCnt.
	assert.False(ok)

	nextVal := uint64(TryCnt + 1)
	pool.Put(nextVal)
	val, ok = pool.Get()
	assert.True(ok)
	assert.Equal(nextVal, val)

	nextVal += TryCnt - 1
	pool.Put(nextVal)
	val, ok = pool.Get()
	assert.True(ok)
	assert.Equal(nextVal, val)

	nextVal += TryCnt + 1
	pool.Put(nextVal)
	_, ok = pool.Get()
	assert.False(ok)
}

func TestLockFreePoolBasic(t *testing.T) {
	assert := assert.New(t)

	const SizeInBits uint32 = 8
	const Size uint64 = 1<<SizeInBits - 1

	var (
		pool globalconn.LockFreeCircularPool
		val  uint64
		ok   bool
		i    uint64
	)

	pool.InitExt(uint32(1<<SizeInBits), math.MaxUint32)
	assert.Equal(int(Size), pool.Cap())
	assert.Equal(int(Size), pool.Len())

	// get all.
	for i = 1; i <= Size; i++ {
		val, ok = pool.Get()
		assert.True(ok)
		assert.Equal(i, val)
	}
	_, ok = pool.Get()
	assert.False(ok)
	assert.Equal(0, pool.Len())

	// put to full.
	for i = 1; i <= Size; i++ {
		ok = pool.Put(i)
		assert.True(ok)
	}
	ok = pool.Put(0)
	assert.False(ok)
	assert.Equal(int(Size), pool.Len())

	// get all.
	for i = 1; i <= Size; i++ {
		val, ok = pool.Get()
		assert.True(ok)
		assert.Equal(i, val)
	}
	_, ok = pool.Get()
	assert.False(ok)
	assert.Equal(0, pool.Len())
}

func TestLockFreePoolInitEmpty(t *testing.T) {
	assert := assert.New(t)

	const SizeInBits uint32 = 8
	const Size uint64 = 1<<SizeInBits - 1

	var (
		pool globalconn.LockFreeCircularPool
		val  uint64
		ok   bool
		i    uint64
	)

	pool.InitExt(uint32(1<<SizeInBits), 0)
	assert.Equal(int(Size), pool.Cap())
	assert.Equal(0, pool.Len())

	// put to full.
	for i = 1; i <= Size; i++ {
		ok = pool.Put(i)
		assert.True(ok)
	}
	ok = pool.Put(0)
	assert.False(ok)
	assert.Equal(int(Size), pool.Len())

	// get all.
	for i = 1; i <= Size; i++ {
		val, ok = pool.Get()
		assert.True(ok)
		assert.Equal(i, val)
	}
	_, ok = pool.Get()
	assert.False(ok)
	assert.Equal(0, pool.Len())
}

var _ globalconn.IDPool = (*LockBasedCircularPool)(nil)

// LockBasedCircularPool implements IDPool by lock-based manner.
// For benchmark purpose.
type LockBasedCircularPool struct {
	_    uint64 // align to 64bits
	head uint32 // first available slot
	_    uint32 // padding to avoid false sharing
	tail uint32 // first empty slot. `head==tail` means empty.
	_    uint32 // padding to avoid false sharing
	cap  uint32

	mu    *sync.Mutex
	slots []uint32
}

func (p *LockBasedCircularPool) Init(size uint64) {
	p.InitExt(uint32(size), 0)
}

func (p *LockBasedCircularPool) InitExt(size uint32, fillCount uint32) {
	p.mu = &sync.Mutex{}

	p.cap = size
	p.slots = make([]uint32, p.cap)

	fillCount = mathutil.MinUint32(p.cap-1, fillCount)
	var i uint32
	for i = 0; i < fillCount; i++ {
		p.slots[i] = i + 1
	}
	for ; i < p.cap; i++ {
		p.slots[i] = math.MaxUint32
	}

	p.head = 0
	p.tail = fillCount
}

func (p *LockBasedCircularPool) Len() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return int(p.tail - p.head)
}

func (p *LockBasedCircularPool) Cap() int {
	return int(p.cap - 1)
}

func (p LockBasedCircularPool) String() string {
	head := p.head
	tail := p.tail
	headVal := p.slots[head&(p.cap-1)]
	tailVal := p.slots[tail&(p.cap-1)]
	length := tail - head

	return fmt.Sprintf("cap:%v, len:%v; head:%x, slot:{%x}; tail:%x, slot:{%x}",
		p.cap, length, head, headVal, tail, tailVal)
}

func (p *LockBasedCircularPool) Put(val uint64) (ok bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.tail-p.head == p.cap-1 { // full
		return false
	}

	p.slots[p.tail&(p.cap-1)] = uint32(val)
	p.tail++
	return true
}

func (p *LockBasedCircularPool) Get() (val uint64, ok bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.head == p.tail { // empty
		return globalconn.IDPoolInvalidValue, false
	}

	val = uint64(p.slots[p.head&(p.cap-1)])
	p.head++
	return val, true
}

func prepareLockBasedPool(sizeInBits uint32, fillCount uint32) globalconn.IDPool {
	var pool LockBasedCircularPool
	pool.InitExt(1<<sizeInBits, fillCount)
	return &pool
}

func prepareLockFreePool(sizeInBits uint32, fillCount uint32, headPos uint32) globalconn.IDPool {
	var pool globalconn.LockFreeCircularPool
	pool.InitExt(1<<sizeInBits, fillCount)
	if headPos > 0 {
		pool.InitForTest(headPos, fillCount)
	}

	return &pool
}

func prepareConcurrencyTest(pool globalconn.IDPool, producers int, consumers int, requests int, total *int64) (ready chan struct{}, done chan struct{}, wgProducer *sync.WaitGroup, wgConsumer *sync.WaitGroup) {
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
					for !pool.Put(uint64(i)) {
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

func TestLockFreePoolBasicConcurrencySafety(t *testing.T) {
	assert := assert.New(t)

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
	assert.Equal(expected, actual)

	// test overflow of head & tail
	expected, actual = testLockFreePoolConcurrency(sizeInBits, fillCount, producers, consumers, requests, headPos)
	assert.Equal(expected, actual)
}

func TestLockBasedPoolConcurrencySafety(t *testing.T) {
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
	assert.Equal(t, expected, actual)
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

func TestLockFreePoolConcurrencySafety(t *testing.T) {
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
		assert.Equalf(t, expected, actual, "case #%v: %v", i+1, ca)
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
		{producers: 20, consumers: 20},
		{producers: 100, consumers: 100},
	}

	for _, ta := range cases {
		b.Run(fmt.Sprintf("LockBasedCircularPool: P:C: %v:%v", ta.producers, ta.consumers), func(b *testing.B) {
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

		b.Run(fmt.Sprintf("LockFreeCircularPool: P:C: %v:%v", ta.producers, ta.consumers), func(b *testing.B) {
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
