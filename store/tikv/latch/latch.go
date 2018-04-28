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
	"math/bits"
	"sort"
	"sync"

	"github.com/cznic/mathutil"
	"github.com/spaolacci/murmur3"
)

// latch stores a key's waiting transactions information.
type latch struct {
	// Whether there is any transaction in waitingQueue except head.
	hasMoreWaiting bool
	// The startTS of the transaction which is the head of waiting transactions.
	waitingQueueHead uint64
	maxCommitTS      uint64
	sync.Mutex
}

func (l *latch) isEmpty() bool {
	return l.waitingQueueHead == 0 && !l.hasMoreWaiting
}

func (l *latch) free() {
	l.waitingQueueHead = 0
}

func (l *latch) refreshCommitTS(commitTS uint64) {
	l.Lock()
	defer l.Unlock()
	l.maxCommitTS = mathutil.MaxUint64(commitTS, l.maxCommitTS)
}

// Lock is the locks' information required for a transaction.
type Lock struct {
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
	// The waiting queue for each slot(slotID => slice of Lock).
	waitingQueues map[int][]*Lock
	sync.RWMutex
}

// NewLatches create a Latches with fixed length,
// the size will be rounded up to the power of 2.
func NewLatches(size uint) *Latches {
	powerOfTwoSize := 1 << uint32(bits.Len32(uint32(size-1)))
	slots := make([]latch, powerOfTwoSize)
	return &Latches{
		slots:         slots,
		waitingQueues: make(map[int][]*Lock),
	}
}

// genLock generates Lock for the transaction with startTS and keys.
func (latches *Latches) genLock(startTS uint64, keys [][]byte) *Lock {
	return &Lock{
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
	sort.Ints(slots)
	if len(slots) <= 1 {
		return slots
	}
	dedup := slots[:1]
	for i := 1; i < len(slots); i++ {
		if slots[i] != slots[i-1] {
			dedup = append(dedup, slots[i])
		}
	}
	return dedup
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
		slotID := lock.requiredSlots[lock.acquiredCount]
		status := latches.acquireSlot(slotID, lock)
		if status != acquireSuccess {
			return status
		}
	}
	return acquireSuccess
}

// release releases all latches owned by the `lock` and returns the wakeup list.
// Preconditions: the caller must ensure the transaction's status is not locked.
func (latches *Latches) release(lock *Lock, commitTS uint64, wakeupList []*Lock) []*Lock {
	wakeupList = wakeupList[:0]
	for i := 0; i < lock.acquiredCount; i++ {
		slotID := lock.requiredSlots[i]
		if nextLock := latches.releaseSlot(slotID, commitTS); nextLock != nil {
			wakeupList = append(wakeupList, nextLock)
		}
	}
	return wakeupList
}

// refreshCommitTS refreshes commitTS for keys.
func (latches *Latches) refreshCommitTS(keys [][]byte, commitTS uint64) {
	slotIDs := latches.genSlotIDs(keys)
	for _, slotID := range slotIDs {
		latches.slots[slotID].refreshCommitTS(commitTS)
	}
}

func (latches *Latches) releaseSlot(slotID int, commitTS uint64) (nextLock *Lock) {
	latch := &latches.slots[slotID]
	latch.Lock()
	defer latch.Unlock()
	latch.maxCommitTS = mathutil.MaxUint64(latch.maxCommitTS, commitTS)
	if !latch.hasMoreWaiting {
		latch.free()
		return nil
	}
	nextLock, latch.hasMoreWaiting = latches.popFromWaitingQueue(slotID)
	latch.waitingQueueHead = nextLock.startTS
	nextLock.acquiredCount++
	if latch.maxCommitTS > nextLock.startTS {
		nextLock.isStale = true
	}
	return nextLock
}

func (latches *Latches) popFromWaitingQueue(slotID int) (front *Lock, hasMoreWaiting bool) {
	latches.Lock()
	defer latches.Unlock()
	waiting := latches.waitingQueues[slotID]
	front = waiting[0]
	if len(waiting) == 1 {
		delete(latches.waitingQueues, slotID)
	} else {
		latches.waitingQueues[slotID] = waiting[1:]
		hasMoreWaiting = true
	}
	return
}

func (latches *Latches) acquireSlot(slotID int, lock *Lock) acquireResult {
	latch := &latches.slots[slotID]
	latch.Lock()
	defer latch.Unlock()
	if latch.maxCommitTS > lock.startTS {
		lock.isStale = true
		return acquireStale
	}

	if latch.isEmpty() {
		latch.waitingQueueHead = lock.startTS
		lock.acquiredCount++
		return acquireSuccess
	}
	// Push the current transaction into waitingQueue.
	latch.hasMoreWaiting = true
	latches.Lock()
	defer latches.Unlock()
	latches.waitingQueues[slotID] = append(latches.waitingQueues[slotID], lock)
	return acquireLocked
}
