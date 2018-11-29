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
	"bytes"
	"math/bits"
	"sort"
	"sync"
	"time"

	"github.com/cznic/mathutil"
	log "github.com/sirupsen/logrus"
	"github.com/spaolacci/murmur3"
)

type node struct {
	slotID      int
	key         []byte
	maxCommitTS uint64
	value       *Lock

	next nodePtr
}

// latch stores a key's waiting transactions information.
type latch struct {
	queue   nodePtr
	count   int
	waiting []*Lock
	sync.Mutex
}

// Lock is the locks' information required for a transaction.
type Lock struct {
	keys [][]byte
	// The slot IDs of the latches(keys) that a startTS must acquire before being able to processed.
	requiredSlots []int
	// The number of latches that the transaction has acquired. For status is stale, it include the
	// latch whose front is current lock already.
	acquiredCount int
	// Current transaction's startTS.
	startTS uint64
	// Current transaction's commitTS.
	commitTS uint64

	wg      sync.WaitGroup
	isStale bool
}

// acquireResult is the result type for acquire()
type acquireResult int32

const (
	// acquireSuccess is a type constant for acquireResult.
	// which means acquired success
	acquireSuccess acquireResult = iota
	// acquireLocked is a type constant for acquireResult
	// which means still locked by other Lock.
	acquireLocked
	// acquireStale is a type constant for acquireResult
	// which means current Lock's startTS is stale.
	acquireStale
)

// IsStale returns whether the status is stale.
func (l *Lock) IsStale() bool {
	return l.isStale
}

func (l *Lock) isLocked() bool {
	return !l.isStale && l.acquiredCount != len(l.requiredSlots)
}

// SetCommitTS sets the lock's commitTS.
func (l *Lock) SetCommitTS(commitTS uint64) {
	l.commitTS = commitTS
}

// Latches which are used for concurrency control.
// Each latch is indexed by a slot's ID, hence the term latch and slot are used in interchangeable,
// but conceptually a latch is a queue, and a slot is an index to the queue
type Latches struct {
	slots []latch
	nodeAlloc
}

type bytesSlice [][]byte

func (s bytesSlice) Len() int {
	return len(s)
}

func (s bytesSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s bytesSlice) Less(i, j int) bool {
	return bytes.Compare(s[i], s[j]) < 0
}

// NewLatches create a Latches with fixed length,
// the size will be rounded up to the power of 2.
func NewLatches(size uint) *Latches {
	powerOfTwoSize := 1 << uint32(bits.Len32(uint32(size-1)))
	slots := make([]latch, powerOfTwoSize)
	return &Latches{
		slots: slots,
	}
}

// genLock generates Lock for the transaction with startTS and keys.
func (latches *Latches) genLock(startTS uint64, keys [][]byte) *Lock {
	sort.Sort(bytesSlice(keys))
	return &Lock{
		keys:          keys,
		requiredSlots: latches.genSlotIDs(keys),
		acquiredCount: 0,
		startTS:       startTS,
	}
}

func (latches *Latches) genSlotIDs(keys [][]byte) []int {
	slots := make([]int, 0, len(keys))
	for _, key := range keys {
		slots = append(slots, latches.slotID(key))
	}
	return slots
}

// slotID return slotID for current key.
func (latches *Latches) slotID(key []byte) int {
	return int(murmur3.Sum32(key)) & (len(latches.slots) - 1)
}

// acquire tries to acquire the lock for a transaction.
func (latches *Latches) acquire(lock *Lock) acquireResult {
	if lock.IsStale() {
		return acquireStale
	}
	for lock.acquiredCount < len(lock.requiredSlots) {
		status := latches.acquireSlot(lock)
		if status != acquireSuccess {
			return status
		}
	}
	return acquireSuccess
}

// release releases all latches owned by the `lock` and returns the wakeup list.
// Preconditions: the caller must ensure the transaction's status is not locked.
func (latches *Latches) release(lock *Lock, wakeupList []*Lock) []*Lock {
	wakeupList = wakeupList[:0]
	for lock.acquiredCount > 0 {
		if nextLock := latches.releaseSlot(lock); nextLock != nil {
			wakeupList = append(wakeupList, nextLock)
		}
	}
	return wakeupList
}

func (latches *Latches) releaseSlot(lock *Lock) (nextLock *Lock) {
	key := lock.keys[lock.acquiredCount-1]
	slotID := lock.requiredSlots[lock.acquiredCount-1]
	latch := &latches.slots[slotID]
	lock.acquiredCount--
	latch.Lock()
	defer latch.Unlock()

	find := findNode(&latches.nodeAlloc, latch.queue, key)
	if find.value != lock {
		panic("releaseSlot wrong")
	}
	find.maxCommitTS = mathutil.MaxUint64(find.maxCommitTS, lock.commitTS)
	find.value = nil
	// Make a copy of the key, so latch does not reference the transaction's memory.
	// If we do not do it, transaction memory can't be recycle by GC and there will
	// be a leak.
	copyKey := make([]byte, len(find.key))
	copy(copyKey, find.key)
	find.key = copyKey
	if len(latch.waiting) == 0 {
		return nil
	}

	var idx int
	for idx = 0; idx < len(latch.waiting); idx++ {
		waiting := latch.waiting[idx]
		if bytes.Compare(waiting.keys[waiting.acquiredCount], key) == 0 {
			break
		}
	}
	// Wake up the first one in waiting queue.
	if idx < len(latch.waiting) {
		nextLock = latch.waiting[idx]
		// Delete element latch.waiting[idx] from the array.
		copy(latch.waiting[idx:], latch.waiting[idx+1:])
		latch.waiting[len(latch.waiting)-1] = nil
		latch.waiting = latch.waiting[:len(latch.waiting)-1]

		if find.maxCommitTS > nextLock.startTS {
			find.value = nextLock
			nextLock.acquiredCount++
			nextLock.isStale = true
		}
	}

	return
}

func (latches *Latches) acquireSlot(lock *Lock) acquireResult {
	key := lock.keys[lock.acquiredCount]
	slotID := lock.requiredSlots[lock.acquiredCount]
	latch := &latches.slots[slotID]
	latch.Lock()
	defer latch.Unlock()

	// Try to recycle to limit the memory usage.
	if latch.count >= latchListCount {
		latch.recycle(lock.startTS)
	}

	find := findNode(&latches.nodeAlloc, latch.queue, key)
	if find == nil {
		ptr := latches.nodeAlloc.New()
		tmp := ptr.Value(&latches.nodeAlloc)
		tmp.slotID = slotID
		tmp.key = key
		tmp.value = lock
		// tmp := &node{
		// 	slotID: slotID,
		// 	key:    key,
		// 	value:  lock,
		// }
		tmp.next = latch.queue
		latch.queue = ptr
		latch.count++

		lock.acquiredCount++
		return acquireSuccess
	}

	if find.maxCommitTS > lock.startTS {
		lock.isStale = true
		return acquireStale
	}

	if find.value == nil {
		find.value = lock
		lock.acquiredCount++
		return acquireSuccess
	}

	// Push the current transaction into waitingQueue.
	latch.waiting = append(latch.waiting, lock)
	return acquireLocked
}

// recycle is not thread safe, the latch should acquire its lock before executing this function.
func (l *latch) recycle(currentTS uint64) int {
	total := 0
	fakeHead := node{next: l.queue}
	prev := &fakeHead
	for curr := prev.next; curr != nil; curr = curr.next {
		if tsoSub(currentTS, curr.maxCommitTS) >= expireDuration && curr.value == nil {
			l.count--
			prev.next = curr.next
			total++
		} else {
			prev = curr
		}
	}
	l.queue = fakeHead.next
	return total
}

func (latches *Latches) recycle(currentTS uint64) {
	total := 0
	for i := 0; i < len(latches.slots); i++ {
		latch := &latches.slots[i]
		latch.Lock()
		total += latch.recycle(currentTS)
		latch.Unlock()
	}
	log.Debugf("recycle run at %v, recycle count = %d...\n", time.Now(), total)
}

func findNode(alloc *nodeAlloc, list nodePtr, key []byte) *node {
	for ptr := list; !ptr.IsNil(); ptr = ptr.Next(alloc) {
		n := ptr.Value(alloc)
		if bytes.Compare(n.key, key) == 0 {
			return n
		}
	}
	return nil
}

const nodeBlockSize = 1024

type nodeBlock [nodeBlockSize]node

func (b *nodeBlock) init(start int) {
	for i := 0; i < nodeBlockSize; i++ {
		(*b)[i].next = nodePtr(start + i + 1)
	}
}

type nodeAlloc struct {
	// Note that physically, all nodeBlocks are not in a continuous memory,
	// but logically they are conotinuous:
	// data[0] contains node[0-1023]
	// data[1] contains node[1024-2047] ...
	data     []*nodeBlock
	freeList nodePtr
}

func (a *nodeAlloc) New() nodePtr {
	if a.freeList.IsNil() {
		block := new(nodeBlock)
		start := len(a.data) * nodeBlockSize
		block.init(start)
		a.data = append(a.data, block)
		a.freeList = nodePtr(start)
		if a.freeList == 0 {
			// nodePtr == 0 means IsNil, so don't use the data[0] node.
			a.freeList++
		}
	}
	ret := a.freeList
	n := ret.Value(a)
	a.freeList = n.next
	*n = node{}
	return ret
}

type nodePtr int

func (p nodePtr) IsNil() bool {
	return p == 0
}

func (p nodePtr) Next(alloc *nodeAlloc) nodePtr {
	return p.Value(alloc).next
}

func (p nodePtr) Value(alloc *nodeAlloc) *node {
	b := alloc.data[p/nodeBlockSize]
	return &(*b)[p%nodeBlockSize]
}
