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
	"fmt"
	"math/bits"
	"sort"
	"sync"

	"github.com/cznic/mathutil"
	"github.com/spaolacci/murmur3"
)

// latch stores a key's waiting transactions information.
type latch struct {
	// Whether there is any transaction in waitingQueue.
	hasWaiting bool
	// The startTS of the transaction which has acquired current latch.
	acquiredTxn uint64
	maxCommitTS uint64
	sync.Mutex
}

func (l *latch) isFree() bool {
	return l.acquiredTxn == 0 && l.hasWaiting == false
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

	wg     sync.WaitGroup
	status acquireResult
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
	return l.status == acquireStale
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
func NewLatches(size int) *Latches {
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

// retryAcquire retries tries to acquire the lock for a transaction.
// Preconditions: the caller must ensure the lock's last call of
// acquire or retryAcquire is locked.
func (latches *Latches) retryAcquire(lock *Lock) acquireResult {
	retrySlotID := lock.requiredSlots[lock.acquiredCount]
	latches.retryAcquireSlot(retrySlotID, lock)
	if lock.status != acquireSuccess {
		return lock.status
	}
	return latches.acquire(lock)
}

// acquire tries to acquire the lock for a transaction
// Preconditions: the caller must ensure lock's last slot was acquired
// successfully.
func (latches *Latches) acquire(lock *Lock) acquireResult {
	for lock.acquiredCount < len(lock.requiredSlots) {
		slotID := lock.requiredSlots[lock.acquiredCount]
		latches.acquireSlot(slotID, lock)
		if lock.status != acquireSuccess {
			return lock.status
		}
	}
	return lock.status
}

// release releases all latches owned by the `lock` and returns the wakeup list.
// Preconditions: the caller must ensure the transaction is at the front of the latches.
func (latches *Latches) release(lock *Lock, commitTS uint64) (wakeupList []*Lock) {
	releaseCount := lock.acquiredCount
	// for status is locked, the number of latches to release is
	// acquiredCount + 1 since the last latch is locked.
	if lock.status == acquireLocked {
		releaseCount++
	}
	wakeupList = make([]*Lock, 0, releaseCount)
	for i := 0; i < releaseCount; i++ {
		slotID := lock.requiredSlots[i]

		if nextLock := latches.releaseSlot(slotID, lock.startTS, commitTS); nextLock != nil {
			wakeupList = append(wakeupList, nextLock)
		}
	}
	return
}

// refreshCommitTS refreshes commitTS for keys.
func (latches *Latches) refreshCommitTS(keys [][]byte, commitTS uint64) {
	slotIDs := latches.genSlotIDs(keys)
	for _, slotID := range slotIDs {
		latches.slots[slotID].refreshCommitTS(commitTS)
	}
}

func (latches *Latches) releaseSlot(slotID int, startTS, commitTS uint64) (nextLock *Lock) {
	latch := &latches.slots[slotID]
	latch.Lock()
	defer latch.Unlock()
	if startTS != latch.acquiredTxn {
		panic(fmt.Sprintf("invalid front ts %d, latch:%#v", startTS, latch))
	}
	latch.maxCommitTS = mathutil.MaxUint64(latch.maxCommitTS, commitTS)
	latch.acquiredTxn = 0
	if !latch.hasWaiting {
		return
	}

	return latches.frontOfWaitingQueue(slotID)
}

func (latches *Latches) popFromWaitingQueue(slotID int) (front *Lock, hasWaiting bool) {
	latches.Lock()
	defer latches.Unlock()
	waiting := latches.waitingQueues[slotID]
	front = waiting[0]
	if len(waiting) == 1 {
		delete(latches.waitingQueues, slotID)
	} else {
		latches.waitingQueues[slotID] = waiting[1:]
		hasWaiting = true
	}
	return
}

func (latches *Latches) frontOfWaitingQueue(slotID int) *Lock {
	latches.RLock()
	defer latches.RUnlock()
	return latches.waitingQueues[slotID][0]
}

// acquireSlot tries to acquire a slot first time.
func (latches *Latches) acquireSlot(slotID int, lock *Lock) {
	latch := &latches.slots[slotID]
	latch.Lock()
	defer latch.Unlock()
	if latch.isFree() {
		latch.acquiredTxn = lock.startTS
		lock.acquiredCount++
		if latch.maxCommitTS > lock.startTS {
			lock.status = acquireStale
		} else {
			lock.status = acquireSuccess
		}
		return
	}
	if latch.maxCommitTS > lock.startTS {
		lock.status = acquireStale
		return
	}
	lock.status = acquireLocked
	// Push the current transaction into waitingQueue.
	latch.hasWaiting = true
	latches.Lock()
	defer latches.Unlock()
	latches.waitingQueues[slotID] = append(latches.waitingQueues[slotID], lock)
}

// retryAcquireSlot retries to acquire slot for current lock.
// Preconditions: the caller must ensure the last call of acquireSlot
// is failed.
func (latches *Latches) retryAcquireSlot(slotID int, lock *Lock) {
	latch := &latches.slots[slotID]
	latch.Lock()
	defer latch.Unlock()
	if latch.isFree() {
		panic(fmt.Sprintf("Invalid latch:%#v", latch))
	}
	var front *Lock
	front, latch.hasWaiting = latches.popFromWaitingQueue(slotID)
	if front.startTS != lock.startTS {
		panic(fmt.Sprintf("invalid front ts %d, lock:%#v", front.startTS, lock))
	}

	lock.acquiredCount++
	latch.acquiredTxn = lock.startTS
	if latch.maxCommitTS > lock.startTS {
		lock.status = acquireStale
	} else {
		lock.status = acquireSuccess
	}
}
