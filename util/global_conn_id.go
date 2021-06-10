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

package util

import (
	"errors"
	"sync/atomic"

	"github.com/cznic/mathutil"
	"github.com/ngaut/sync2"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// GlobalConnID is the global connection ID, providing UNIQUE connection IDs across the whole TiDB cluster.
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
// TODO: move to global_conn_id.go
type GlobalConnID struct {
	ServerID    uint64
	LocalConnID uint64
	Is64bits    bool
}

const (
	// MaxServerID32 is maximum serverID for 32bits global connection ID.
	MaxServerID32 = 1<<11 - 1
	// MaxLocalConnID32 is maximum localConnID for 32bits global connection ID.
	MaxLocalConnID32 = 1<<20 - 1

	// MaxServerID64 is maximum serverID for 64bits global connection ID.
	MaxServerID64 = 1<<22 - 1
	// MaxLocalConnID64 is maximum localConnID for 64bits global connection ID.
	MaxLocalConnID64 = 1<<40 - 1
)

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

	localAllocator32 LocalConnIDAllocator32
	localAllocator64 LocalConnIDAllocator64
}

// Is64 indicates allocate 64bits global connection ID or not.
func (g *GlobalConnIDAllocator) Is64() bool {
	return g.is64bits.Get() != 0
}

// UpgradeTo64 upgrade allocator to 64bits.
func (g *GlobalConnIDAllocator) UpgradeTo64() {
	g.is64bits.Set(1)
}

type serverIDGetterFn func() uint64
type globalConnIDExistCheckerFn func(globalConnID uint64) bool

// Init initiate members.
func (g *GlobalConnIDAllocator) Init(serverIDGetter serverIDGetterFn, existedChecker globalConnIDExistCheckerFn) {
	g.serverIDGetter = serverIDGetter

	g.localAllocator32.Init()
	g.localAllocator64.Init(existedChecker)

	g.is64bits.Set(1) // TODO: set 32bits as default, after 32bits logics is fully implemented and tested.
}

// SetServerIDGetter set serverIDGetter member.
func (g *GlobalConnIDAllocator) SetServerIDGetter(serverIDGetter serverIDGetterFn) {
	g.serverIDGetter = serverIDGetter
}

// NextID returns next connection ID.
func (g *GlobalConnIDAllocator) NextID() GlobalConnID {
	serverID := g.serverIDGetter()

	// 32bits.
	if !g.Is64() {
		localConnID, isExhausted := g.localAllocator32.Allocate()
		if isExhausted {
			g.UpgradeTo64() // go on to 64bits.
		}
		return GlobalConnID{
			ServerID:    serverID,
			LocalConnID: localConnID,
			Is64bits:    false,
		}
	}

	// 64bits.
	return GlobalConnID{
		ServerID:    serverID,
		LocalConnID: g.localAllocator64.Allocate(serverID),
		Is64bits:    true,
	}
}

// Release releases connectionID to pool.
func (g *GlobalConnIDAllocator) Release(connectionID uint64) {
	globalConnID, isTruncated, err := ParseGlobalConnID(connectionID)
	if err != nil || isTruncated {
		logutil.BgLogger().Error("failed to ParseGlobalConnID", zap.Uint64("connectionID", connectionID), zap.Error(err), zap.Bool("isTruncated", isTruncated))
		return
	}

	if globalConnID.Is64bits {
		g.localAllocator64.Deallocate(globalConnID.LocalConnID)
	} else {
		if err = g.localAllocator32.Deallocate(globalConnID.LocalConnID); err != nil {
			logutil.BgLogger().Error("failed to release connection ID", zap.Uint64("connectionID", connectionID), zap.Error(err), zap.Uint64("localConnID", globalConnID.LocalConnID))
		}
	}
}

// LocalConnIDAllocator64 is local connID allocator for 64bits global connection ID.
type LocalConnIDAllocator64 struct {
	existedChecker globalConnIDExistCheckerFn
	lastID         uint64
}

// Init initiates LocalConnIDAllocator64
func (a *LocalConnIDAllocator64) Init(existedChecker globalConnIDExistCheckerFn) {
	a.existedChecker = existedChecker
}

const (
	// LocalConnIDAllocator64RetryCnt is retry count for localConnIDAllocator64
	LocalConnIDAllocator64RetryCnt = 10
)

// Allocate local connID for 64bits global connID.
// local connID has 40bits pool size and should not be exhausted, as `MaxServerConnections` is a uint32.
func (a *LocalConnIDAllocator64) Allocate(serverID uint64) (localConnID uint64) {
	for {
		localConnID := atomic.AddUint64(&a.lastID, 1) & MaxLocalConnID64
		if !a.existedChecker(makeGlobalConnID(true, serverID, localConnID)) {
			return localConnID
		}
	}
}

// Deallocate local connID to pool.
func (a *LocalConnIDAllocator64) Deallocate(localConnID uint64) {
	// Do nothing. 64bits connection IDs are maintained by `Server.clients`.
}

// LocalConnIDAllocator32 is local connID allocator for 32bits global connection ID.
type LocalConnIDAllocator32 struct {
	lastID uint32
}

// Init initiates LocalConnIDAllocator32
func (a *LocalConnIDAllocator32) Init() {
}

// Allocate local connID.
func (a *LocalConnIDAllocator32) Allocate() (localConnID uint64, isExhausted bool) {
	return (uint64)(atomic.AddUint32(&a.lastID, 1) & MaxLocalConnID32), false
}

// Deallocate local connID to pool.
func (a *LocalConnIDAllocator32) Deallocate(localConnID uint64) error {
	return nil
}

const (
	PoolInvalidValue = 0xffff_ffff
)

type poolItem struct {
	value uint32

	// seq indicates read/write status
	// Sequence:
	//   seq==tail: writable ---> doWrite,seq:=tail+1 ---> seq==head+1:written/readable ---> doRead,seq:=head+size
	//       ^                                                                                          |
	//       +------------------------------------------------------------------------------------------+
	seq uint32
}

type Pool struct {
	head sync2.AtomicUint32 // first available slot
	tail sync2.AtomicUint32 // first empty slot. `head==tail` means empty.
	cap  uint32

	slots []poolItem
}

func (p *Pool) Init(sizeInBits uint32, fillCount uint32) {
	p.cap = 1 << sizeInBits
	p.slots = make([]poolItem, p.cap)
	fillCount = mathutil.MinUint32(p.cap-1, fillCount)
	var i uint32
	for i = 0; i < fillCount; i++ {
		p.slots[i] = poolItem{value: i + 1, seq: i + p.cap}
	}
	for ; i < p.cap; i++ {
		p.slots[i] = poolItem{value: PoolInvalidValue, seq: i}
	}

	p.head.Set(0)
	p.tail.Set(fillCount)
}

func (p *Pool) Len() uint32 {
	return p.tail.Get() - p.head.Get()
}

func (p *Pool) Put(val uint32) (ok bool) {
	for {
		head := p.head.Get()
		tail := p.tail.Get()

		if tail-head == p.cap-1 { // full
			return false
		}

		slot := &p.slots[tail&(p.cap-1)]
		seq := atomic.LoadUint32(&slot.seq)

		if seq == tail { // writable
			if p.tail.CompareAndSwap(tail, tail+1) {
				slot.value = val
				atomic.StoreUint32(&slot.seq, tail+1)
				return true
			}
		} else if seq < tail {
			panic("Pool in a corrupted state during a Put operation.")
		}
		// else: preempted by another thread, try again.
	}
}

func (p *Pool) Get() (val uint32, ok bool) {
	for {
		head := p.head.Get()
		tail := p.tail.Get()
		if head == tail { // empty
			return PoolInvalidValue, false
		}

		slot := &p.slots[head&(p.cap-1)]
		seq := atomic.LoadUint32(&slot.seq)

		if seq == head+1 { // readable
			if p.head.CompareAndSwap(head, head+1) {
				val = slot.value
				slot.value = PoolInvalidValue
				atomic.StoreUint32(&slot.seq, head+p.cap)
				return val, true
			}
		} else if seq < head+1 { // corrupted, should not happen
			panic("Pool in a corrupted state during a Get operation.")
		}
		// else: preempted by another thread, try again.
	}
}
