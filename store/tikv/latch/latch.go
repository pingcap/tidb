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

	"github.com/spaolacci/murmur3"
)

// Latch stores a key's waiting transactions information.
type Latch struct {
	// Whether there is any transaction in waitingQueue except head.
	hasWaiting bool
	// The startTS of the transaction which is the head of waiting transactions.
	head        uint64
	maxCommitTS uint64
	sync.Mutex
}

// acquire tries to get current key's lock for the transaction with startTS.
// success is true when success.
// stale is true when the startTS is already stale.
func (l *Latch) acquire(startTS uint64) (success, stale bool) {
	l.Lock()
	defer l.Unlock()

	if stale = startTS <= l.maxCommitTS; stale {
		return
	}

	if l.head == 0 {
		l.head = startTS
	}
	success = l.head == startTS
	if !success {
		l.hasWaiting = true
	}
	return
}

// release releases the transaction with startTS and commitTS from current latch,
// and set the next transaction to head if hasNext is true.
func (l *Latch) release(startTS, commitTS uint64, hasNext bool, nextStartTS uint64) {
	l.Lock()
	defer l.Unlock()
	if startTS != l.head {
		panic(fmt.Sprintf("invalid front ts %d, latch:%#v", startTS, l))
	}
	if commitTS > l.maxCommitTS {
		l.maxCommitTS = commitTS
	}
	l.hasWaiting = hasNext
	l.head = nextStartTS
}

// Lock is the locks' information required for a transaction.
type Lock struct {
	// The slot IDs of the latches(keys) that a startTS must acquire before being able to processed.
	requiredSlots []int
	// The number of latches that the transaction has acquired.
	acquiredCount int
	// Whether current transaction is waiting
	waiting bool
	// Current transaction's startTS.
	startTS uint64
}

// NewLock creates a new lock.
func NewLock(startTS uint64, requiredSlots []int) Lock {
	return Lock{
		requiredSlots: requiredSlots,
		acquiredCount: 0,
		waiting:       false,
		startTS:       startTS,
	}
}

// Latches which are used for concurrency control.
// Each latch is indexed by a slot's ID, hence the term latch and slot are used in interchangeable,
// but conceptually a latch is a queue, and a slot is an index to the queue
type Latches struct {
	slots []Latch
	// The waiting queue for each slot(slotID => slice of startTS).
	waitingQueue map[int][]uint64
	sync.RWMutex
}

// NewLatches create a Latches with fixed length,
// the size will be rounded up to the power of 2.
func NewLatches(size int) *Latches {
	powerOfTwoSize := 1 << uint32(bits.Len32(uint32(size-1)))
	slots := make([]Latch, powerOfTwoSize)
	return &Latches{
		slots:        slots,
		waitingQueue: make(map[int][]uint64),
	}
}

// GenLock generates Lock for the transaction with startTS and keys.
func (latches *Latches) GenLock(startTS uint64, keys [][]byte) Lock {
	slots := make([]int, 0, len(keys))
	for _, key := range keys {
		slots = append(slots, latches.hash(key))
	}
	sort.Ints(slots)
	if len(slots) == 0 {
		return NewLock(startTS, slots)
	}
	dedup := slots[:1]
	for i := 1; i < len(slots); i++ {
		if slots[i] != slots[i-1] {
			dedup = append(dedup, slots[i])
		}
	}
	return NewLock(startTS, dedup)
}

// hash return hash int for current key.
func (latches *Latches) hash(key []byte) int {
	return int(murmur3.Sum32(key)) & (len(latches.slots) - 1)
}

// Acquire tries to acquire the lock for a transaction.
// It returns with stale = true when the transaction is stale(
// when the lock.startTS is smaller than any key's last commitTS).
func (latches *Latches) Acquire(lock *Lock) (success, stale bool) {
	for lock.acquiredCount < len(lock.requiredSlots) {
		slotID := lock.requiredSlots[lock.acquiredCount]
		success, stale = latches.acquireSlot(slotID, lock.startTS)
		if success {
			lock.acquiredCount++
			lock.waiting = false
			continue
		}
		if !stale {
			lock.waiting = true
		}
		return
	}
	return
}

// Release releases all latches owned by the `lock` and returns the wakeup list.
// Preconditions: the caller must ensure the transaction is at the front of the latches.
func (latches *Latches) Release(lock *Lock, commitTS uint64) (wakeupList []uint64) {
	wakeupCount := lock.acquiredCount
	if lock.waiting {
		wakeupCount++
	}
	wakeupList = make([]uint64, 0, wakeupCount)
	for id := 0; id < wakeupCount; id++ {
		slotID := lock.requiredSlots[id]

		if hasNext, nextStartTS := latches.releaseSlot(slotID, lock.startTS, commitTS); hasNext {
			wakeupList = append(wakeupList, nextStartTS)
		}
	}
	return
}

func (latches *Latches) releaseSlot(slotID int, startTS, commitTS uint64) (hasNext bool, nextStartTS uint64) {
	latch := &latches.slots[slotID]
	latch.Lock()
	defer latch.Unlock()
	if latch.maxCommitTS < commitTS {
		latch.maxCommitTS = commitTS
	}
	if !latch.hasWaiting {
		latch.head = 0
		return
	}

	latches.Lock()
	if waiting, ok := latches.waitingQueue[slotID]; ok {
		hasNext = true
		nextStartTS = waiting[0]
		if len(waiting) == 1 {
			delete(latches.waitingQueue, slotID)
			latch.hasWaiting = false
		} else {
			latches.waitingQueue[slotID] = waiting[1:]
		}
	}
	latches.Unlock()
	latch.head = nextStartTS
	return
}

func (latches *Latches) acquireSlot(slotID int, startTS uint64) (success, stale bool) {
	success, stale = latches.slots[slotID].acquire(startTS)
	if success || stale {
		return
	}
	latches.Lock()
	defer latches.Unlock()
	if waitingQueue, ok := latches.waitingQueue[slotID]; ok {
		latches.waitingQueue[slotID] = append(waitingQueue, startTS)
	} else {
		latches.waitingQueue[slotID] = []uint64{startTS}
	}
	return
}
