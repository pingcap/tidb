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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package util

import (
	"errors"
	"fmt"
	"math"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/cznic/mathutil"
	"github.com/ngaut/sync2"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

//////////////////////////////////// Class Diagram //////////////////////////////////////
//                                                                                     //
//  +----------+      +-----------------------+         +-----------------------+      //
//  |  Server  | ---> | ConnectionIDAllocator | <<--+-- | GlobalConnIDAllocator | --+  //
//  +----------+      +-----------------------+     |   +-----------------------+   |  //
//                                                  +-- | SimpleConnIDAllocator |   |  //
//                                                      +----------+------------+   |  //
//                                                                 |                |  //
//                                                                 V                |  //
//                                  +--------+          +----------------------+    |  //
//                                  | IDPool | <<--+--  |     AutoIncPool      | <--+  //
//                                  +--------+     |    +----------------------+    |  //
//                                                 +--  | LockFreeCircularPool | <--+  //
//                                                      +----------------------+       //
//                                                                                     //
/////////////////////////////////////////////////////////////////////////////////////////

type serverIDGetterFn func() uint64

// ConnectionIDAllocator allocates connection IDs.
type ConnectionIDAllocator interface {
	// Init initiates the allocator.
	Init()
	// SetServerIDGetter set serverIDGetter to allocator.
	SetServerIDGetter(serverIDGetter serverIDGetterFn)
	// NextID returns next connection ID.
	NextID() uint64
	// Release releases connection ID to allocator.
	Release(connectionID uint64)
}

var (
	_ ConnectionIDAllocator = (*SimpleConnIDAllocator)(nil)
	_ ConnectionIDAllocator = (*GlobalConnIDAllocator)(nil)
)

// SimpleConnIDAllocator is a simple connection id allocator used when GlobalKill feature is disable.
type SimpleConnIDAllocator struct {
	pool AutoIncPool
}

// Init implements ConnectionIDAllocator interface.
func (a *SimpleConnIDAllocator) Init() {
	a.pool.Init(64)
}

// SetServerIDGetter implements ConnectionIDAllocator interface.
func (a *SimpleConnIDAllocator) SetServerIDGetter(_ serverIDGetterFn) {
	// do nothing
}

// NextID implements ConnectionIDAllocator interface.
func (a *SimpleConnIDAllocator) NextID() uint64 {
	id, _ := a.pool.Get()
	return id
}

// Release implements ConnectionIDAllocator interface.
func (a *SimpleConnIDAllocator) Release(id uint64) {
	a.pool.Put(id)
}

// GlobalConnID is the global connection ID, providing UNIQUE connection IDs across the whole TiDB cluster.
// Used when GlobalKill feature is enable.
// See https://github.com/pingcap/tidb/blob/master/docs/design/2020-06-01-global-kill.md
// 32 bits version:
//   31    21 20               1    0
//  +--------+------------------+------+
//  |serverID|   local connID   |markup|
//  | (11b)  |       (20b)      |  =0  |
//  +--------+------------------+------+
// 64 bits version:
//   63 62                 41 40                                   1   0
//  +--+---------------------+--------------------------------------+------+
//  |  |      serverId       |             local connId             |markup|
//  |=0|       (22b)         |                 (40b)                |  =1  |
//  +--+---------------------+--------------------------------------+------+
type GlobalConnID struct {
	ServerID    uint64
	LocalConnID uint64
	Is64bits    bool
}

const (
	// MaxServerID32 is maximum serverID for 32bits global connection ID.
	MaxServerID32 = 1<<11 - 1
	// LocalConnIDBits32 is the number of bits of localConnID for 32bits global connection ID.
	LocalConnIDBits32 = 20
	// MaxLocalConnID32 is maximum localConnID for 32bits global connection ID.
	MaxLocalConnID32 = 1<<LocalConnIDBits32 - 1

	// MaxServerID64 is maximum serverID for 64bits global connection ID.
	MaxServerID64 = 1<<22 - 1
	// LocalConnIDBits64 is the number of bits of localConnID for 64bits global connection ID.
	LocalConnIDBits64 = 40
	// MaxLocalConnID64 is maximum localConnID for 64bits global connection ID.
	MaxLocalConnID64 = 1<<LocalConnIDBits64 - 1
)

// makeGlobalConnID composes GlobalConnID.
func makeGlobalConnID(is64bits bool, serverID uint64, localConnID uint64) uint64 {
	var id uint64
	if is64bits {
		id |= 0x1
		id |= localConnID & MaxLocalConnID64 << 1 // 40 bits local connID.
		id |= serverID & MaxServerID64 << 41      // 22 bits serverID.
	} else {
		id |= localConnID & MaxLocalConnID32 << 1 // 20 bits local connID.
		id |= serverID & MaxServerID32 << 21      // 11 bits serverID.
	}
	return id
}

// ID returns the connection id
func (g *GlobalConnID) ID() uint64 {
	return makeGlobalConnID(g.Is64bits, g.ServerID, g.LocalConnID)
}

// ParseGlobalConnID parses an uint64 to GlobalConnID.
//   `isTruncated` indicates that older versions of the client truncated the 64-bit GlobalConnID to 32-bit.
func ParseGlobalConnID(id uint64) (g GlobalConnID, isTruncated bool, err error) {
	if id&0x80000000_00000000 > 0 {
		return GlobalConnID{}, false, errors.New("Unexpected connectionID exceeds int64")
	}
	if id&0x1 > 0 { // 64bits
		if id&0xffffffff_00000000 == 0 {
			return GlobalConnID{}, true, nil
		}
		return GlobalConnID{
			Is64bits:    true,
			LocalConnID: (id >> 1) & MaxLocalConnID64,
			ServerID:    (id >> 41) & MaxServerID64,
		}, false, nil
	}

	// 32bits
	if id&0xffffffff_00000000 > 0 {
		return GlobalConnID{}, false, errors.New("Unexpected connectionID exceeds uint32")
	}
	return GlobalConnID{
		Is64bits:    false,
		LocalConnID: (id >> 1) & MaxLocalConnID32,
		ServerID:    (id >> 21) & MaxServerID32,
	}, false, nil
}

// GlobalConnIDAllocator is global connection ID allocator.
type GlobalConnIDAllocator struct {
	is64bits       sync2.AtomicInt32 // !0: true, 0: false
	serverIDGetter func() uint64

	local32 LockFreeCircularPool
	local64 AutoIncPool
}

// Is64 indicates allocate 64bits global connection ID or not.
func (g *GlobalConnIDAllocator) Is64() bool {
	return g.is64bits.Get() != 0
}

// UpgradeTo64 upgrade allocator to 64bits.
func (g *GlobalConnIDAllocator) UpgradeTo64() {
	g.is64bits.Set(1)
}

// LocalConnIDAllocator64TryCount is the try count of 64bits local connID allocation.
const LocalConnIDAllocator64TryCount = 10

// Init initiate members.
func (g *GlobalConnIDAllocator) Init() {
	g.local32.InitExt(LocalConnIDBits32, math.MaxUint32)
	g.local64.InitExt(LocalConnIDBits64, true, LocalConnIDAllocator64TryCount)

	g.is64bits.Set(1) // TODO: set 32bits as default, after 32bits logics is fully implemented and tested.
}

// SetServerIDGetter set serverIDGetter member.
func (g *GlobalConnIDAllocator) SetServerIDGetter(serverIDGetter serverIDGetterFn) {
	g.serverIDGetter = serverIDGetter
}

// NextID returns next connection ID.
func (g *GlobalConnIDAllocator) NextID() uint64 {
	globalConnID := g.Allocate()
	return globalConnID.ID()
}

// Allocate allocates a new global connID.
func (g *GlobalConnIDAllocator) Allocate() GlobalConnID {
	serverID := g.serverIDGetter()

	// 32bits.
	if !g.Is64() {
		localConnID, ok := g.local32.Get()
		if ok {
			return GlobalConnID{
				ServerID:    serverID,
				LocalConnID: localConnID,
				Is64bits:    false,
			}
		}
		g.UpgradeTo64() // go on to 64bits.
	}

	// 64bits.
	localConnID, ok := g.local64.Get()
	if !ok {
		// local connID with 40bits pool size is big enough and should not be exhausted, as `MaxServerConnections` is no more than math.MaxUint32.
		panic(fmt.Sprintf("Failed to allocate 64bits local connID after try %v times. Should never happen", LocalConnIDAllocator64TryCount))
	}
	return GlobalConnID{
		ServerID:    serverID,
		LocalConnID: localConnID,
		Is64bits:    true,
	}
}

// Release releases connectionID to pool.
func (g *GlobalConnIDAllocator) Release(connectionID uint64) {
	globalConnID, isTruncated, err := ParseGlobalConnID(connectionID)
	if err != nil || isTruncated {
		logutil.BgLogger().Error("failed to ParseGlobalConnID", zap.Error(err), zap.Uint64("connectionID", connectionID), zap.Bool("isTruncated", isTruncated))
		return
	}

	if globalConnID.Is64bits {
		g.local64.Put(globalConnID.LocalConnID)
	} else {
		if ok := g.local32.Put(globalConnID.LocalConnID); !ok {
			logutil.BgLogger().Error("failed to release 32bits connection ID", zap.Uint64("connectionID", connectionID), zap.Uint64("localConnID", globalConnID.LocalConnID))
		}
	}
}

const (
	// IDPoolInvalidValue indicates invalid value from IDPool.
	IDPoolInvalidValue = math.MaxUint64
)

// IDPool is the pool allocating & deallocating IDs.
type IDPool interface {
	fmt.Stringer
	// Init initiates pool.
	Init(sizeInBits uint32)
	// Len returns length of available id's in pool.
	// Note that Len() would return -1 when this method is NOT supported.
	Len() int
	// Put puts value to pool. "ok" is false when pool is full.
	Put(val uint64) (ok bool)
	// Get gets value from pool. "ok" is false when pool is empty.
	Get() (val uint64, ok bool)
}

var _ IDPool = (*AutoIncPool)(nil)
var _ IDPool = (*LockFreeCircularPool)(nil)

// AutoIncPool simply do auto-increment to allocate ID. Wrapping will happen.
type AutoIncPool struct {
	lastID uint64
	idMask uint64
	tryCnt int

	mu      *sync.Mutex
	existed map[uint64]struct{}
}

// Init initiates AutoIncPool.
func (p *AutoIncPool) Init(sizeInBits uint32) {
	p.InitExt(sizeInBits, false, 1)
}

// InitExt initiates AutoIncPool with more parameters.
func (p *AutoIncPool) InitExt(sizeInBits uint32, checkExisted bool, tryCnt int) {
	p.idMask = 1<<sizeInBits - 1
	if checkExisted {
		p.existed = make(map[uint64]struct{})
		p.mu = &sync.Mutex{}
	}
	p.tryCnt = tryCnt
}

// Get id by auto-increment.
func (p *AutoIncPool) Get() (id uint64, ok bool) {
	for i := 0; i < p.tryCnt; i++ {
		id := atomic.AddUint64(&p.lastID, 1) & p.idMask
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
		len := len(p.existed)
		p.mu.Unlock()
		return len
	}
	return -1
}

// String implements IDPool interface.
func (p AutoIncPool) String() string {
	return fmt.Sprintf("lastID: %v", p.lastID)
}

// LockFreeCircularPool is a lock-free circular implementation of IDPool.
// Note that to reduce memory usage, LockFreeCircularPool supports 32bits IDs ONLY.
type LockFreeCircularPool struct {
	_    uint64             // align to 64bits
	head sync2.AtomicUint32 // first available slot
	_    uint32             // padding to avoid false sharing
	tail sync2.AtomicUint32 // first empty slot. `head==tail` means empty.
	_    uint32             // padding to avoid false sharing

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
func (p *LockFreeCircularPool) Init(sizeInBits uint32) {
	p.InitExt(sizeInBits, 0)
}

// InitExt initializes LockFreeCircularPool with more parameters.
// fillCount: fills pool with [1, min(fillCount, 1<<(sizeInBits-1)]. Pass "math.MaxUint32" to fulfill the pool.
func (p *LockFreeCircularPool) InitExt(sizeInBits uint32, fillCount uint32) {
	p.cap = 1 << sizeInBits
	p.slots = make([]lockFreePoolItem, p.cap)

	fillCount = mathutil.MinUint32(p.cap-1, fillCount)
	var i uint32
	for i = 0; i < fillCount; i++ {
		p.slots[i] = lockFreePoolItem{value: i + 1, seq: i + 1}
	}
	for ; i < p.cap; i++ {
		p.slots[i] = lockFreePoolItem{value: math.MaxUint32, seq: i}
	}

	p.head.Set(0)
	p.tail.Set(fillCount)
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

	p.head.Set(head)
	p.tail.Set(head + fillCount)
}

// Len implements IDPool interface.
func (p *LockFreeCircularPool) Len() int {
	return int(p.tail.Get() - p.head.Get())
}

// String implements IDPool interface.
// Notice: NOT thread safe.
func (p LockFreeCircularPool) String() string {
	head := p.head.Get()
	tail := p.tail.Get()
	headSlot := &p.slots[head&(p.cap-1)]
	tailSlot := &p.slots[tail&(p.cap-1)]
	len := tail - head

	return fmt.Sprintf("cap:%v, len:%v; head:%x, slot:{%x,%x}; tail:%x, slot:{%x,%x}",
		p.cap, len, head, headSlot.value, headSlot.seq, tail, tailSlot.value, tailSlot.seq)
}

// Put implements IDPool interface.
func (p *LockFreeCircularPool) Put(val uint64) (ok bool) {
	for {
		tail := p.tail.Get() // `tail` should be loaded before `head`, to avoid "false full".
		head := p.head.Get()

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
		head := p.head.Get()
		tail := p.tail.Get()
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
