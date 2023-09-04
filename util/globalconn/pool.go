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

package globalconn

import (
	"fmt"
	"math"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/cznic/mathutil"
)

const (
	// IDPoolInvalidValue indicates invalid value from IDPool.
	IDPoolInvalidValue = math.MaxUint64
)

// IDPool is the pool allocating & deallocating IDs.
type IDPool interface {
	fmt.Stringer
	// Init initiates pool.
	Init(size uint64)
	// Len returns length of available id's in pool.
	// Note that Len() would return -1 when this method is NOT supported.
	Len() int
	// Cap returns the capacity of pool.
	Cap() int
	// Put puts value to pool. "ok" is false when pool is full.
	Put(val uint64) (ok bool)
	// Get gets value from pool. "ok" is false when pool is empty.
	Get() (val uint64, ok bool)
}

var (
	_ IDPool = (*AutoIncPool)(nil)
	_ IDPool = (*LockFreeCircularPool)(nil)
)

// AutoIncPool simply do auto-increment to allocate ID. Wrapping will happen.
type AutoIncPool struct {
	lastID uint64
	cap    uint64
	tryCnt int

	mu      *sync.Mutex
	existed map[uint64]struct{}
}

// Init initiates AutoIncPool.
func (p *AutoIncPool) Init(size uint64) {
	p.InitExt(size, false, 1)
}

// InitExt initiates AutoIncPool with more parameters.
func (p *AutoIncPool) InitExt(size uint64, checkExisted bool, tryCnt int) {
	p.cap = size
	if checkExisted {
		p.existed = make(map[uint64]struct{})
		p.mu = &sync.Mutex{}
	}
	p.tryCnt = tryCnt
}

// Get id by auto-increment.
func (p *AutoIncPool) Get() (id uint64, ok bool) {
	for i := 0; i < p.tryCnt; i++ {
		id := atomic.AddUint64(&p.lastID, 1)
		if p.cap < math.MaxUint64 {
			id = id % p.cap
		}
		if p.existed != nil {
			p.mu.Lock()
			_, occupied := p.existed[id]
			if occupied {
				p.mu.Unlock()
				continue
			}
			p.existed[id] = struct{}{}
			p.mu.Unlock()
		}
		return id, true
	}
	return 0, false
}

// Put id back to pool.
func (p *AutoIncPool) Put(id uint64) (ok bool) {
	if p.existed != nil {
		p.mu.Lock()
		delete(p.existed, id)
		p.mu.Unlock()
	}
	return true
}

// Len implements IDPool interface.
func (p *AutoIncPool) Len() int {
	if p.existed != nil {
		p.mu.Lock()
		length := len(p.existed)
		p.mu.Unlock()
		return length
	}
	return -1
}

// Cap implements IDPool interface.
func (p *AutoIncPool) Cap() int {
	return int(p.cap)
}

// String implements IDPool interface.
func (p AutoIncPool) String() string {
	return fmt.Sprintf("lastID: %v", p.lastID)
}

// LockFreeCircularPool is a lock-free circular implementation of IDPool.
// Note that to reduce memory usage, LockFreeCircularPool supports 32bits IDs ONLY.
type LockFreeCircularPool struct {
	_    uint64        // align to 64bits
	head atomic.Uint32 // first available slot
	_    uint32        // padding to avoid false sharing
	tail atomic.Uint32 // first empty slot. `head==tail` means empty.
	_    uint32        // padding to avoid false sharing

	cap   uint32
	slots []lockFreePoolItem
}

type lockFreePoolItem struct {
	value uint32

	// seq indicates read/write status
	// Sequence:
	//   seq==tail: writable ---> doWrite,seq:=tail+1 ---> seq==head+1:written/readable ---> doRead,seq:=head+size
	//         ^                                                                                        |
	//         +----------------------------------------------------------------------------------------+
	//   slot[i].seq: i(writable) ---> i+1(readable) ---> i+cap(writable) ---> i+cap+1(readable) ---> i+2*cap ---> ...
	seq uint32
}

// Init implements IDPool interface.
func (p *LockFreeCircularPool) Init(size uint64) {
	p.InitExt(uint32(size), 0)
}

// InitExt initializes LockFreeCircularPool with more parameters.
// fillCount: fills pool with [1, min(fillCount, 1<<(sizeInBits-1)]. Pass "math.MaxUint32" to fulfill the pool.
func (p *LockFreeCircularPool) InitExt(size uint32, fillCount uint32) {
	p.cap = size
	p.slots = make([]lockFreePoolItem, p.cap)

	fillCount = mathutil.MinUint32(p.cap-1, fillCount)
	var i uint32
	for i = 0; i < fillCount; i++ {
		p.slots[i] = lockFreePoolItem{value: i + 1, seq: i + 1}
	}
	for ; i < p.cap; i++ {
		p.slots[i] = lockFreePoolItem{value: math.MaxUint32, seq: i}
	}

	p.head.Store(0)
	p.tail.Store(fillCount)
}

// InitForTest used to unit test overflow of head & tail.
func (p *LockFreeCircularPool) InitForTest(head uint32, fillCount uint32) {
	fillCount = mathutil.MinUint32(p.cap-1, fillCount)
	var i uint32
	for i = 0; i < fillCount; i++ {
		p.slots[i] = lockFreePoolItem{value: i + 1, seq: head + i + 1}
	}
	for ; i < p.cap; i++ {
		p.slots[i] = lockFreePoolItem{value: math.MaxUint32, seq: head + i}
	}

	p.head.Store(head)
	p.tail.Store(head + fillCount)
}

// Len implements IDPool interface.
func (p *LockFreeCircularPool) Len() int {
	return int(p.tail.Load() - p.head.Load())
}

// Cap implements IDPool interface.
func (p *LockFreeCircularPool) Cap() int {
	return int(p.cap - 1)
}

// String implements IDPool interface.
// Notice: NOT thread safe.
func (p *LockFreeCircularPool) String() string {
	head := p.head.Load()
	tail := p.tail.Load()
	headSlot := &p.slots[head&(p.cap-1)]
	tailSlot := &p.slots[tail&(p.cap-1)]
	length := tail - head

	return fmt.Sprintf("cap:%v, length:%v; head:%x, slot:{%x,%x}; tail:%x, slot:{%x,%x}",
		p.cap, length, head, headSlot.value, headSlot.seq, tail, tailSlot.value, tailSlot.seq)
}

// Put implements IDPool interface.
func (p *LockFreeCircularPool) Put(val uint64) (ok bool) {
	for {
		tail := p.tail.Load() // `tail` should be loaded before `head`, to avoid "false full".
		head := p.head.Load()

		if tail-head == p.cap-1 { // full
			return false
		}

		if !p.tail.CompareAndSwap(tail, tail+1) {
			continue
		}

		slot := &p.slots[tail&(p.cap-1)]
		for {
			seq := atomic.LoadUint32(&slot.seq)

			if seq == tail { // writable
				slot.value = uint32(val)
				atomic.StoreUint32(&slot.seq, tail+1)
				return true
			}

			runtime.Gosched()
		}
	}
}

// Get implements IDPool interface.
func (p *LockFreeCircularPool) Get() (val uint64, ok bool) {
	for {
		head := p.head.Load()
		tail := p.tail.Load()
		if head == tail { // empty
			return IDPoolInvalidValue, false
		}

		if !p.head.CompareAndSwap(head, head+1) {
			continue
		}

		slot := &p.slots[head&(p.cap-1)]
		for {
			seq := atomic.LoadUint32(&slot.seq)

			if seq == head+1 { // readable
				val = uint64(slot.value)
				slot.value = math.MaxUint32
				atomic.StoreUint32(&slot.seq, head+p.cap)
				return val, true
			}

			runtime.Gosched()
		}
	}
}
