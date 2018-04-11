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
	"math"
	"sort"
	"sync"

	"github.com/spaolacci/murmur3"
)

// Latch stores a key's waiting transactions.
type Latch struct {
	// A queue by startTs of those waiting transactions.
	waiting      []uint64
	lastCommitTs uint64
	sync.Mutex
}

// acquire tries to get current key's lock for the transaction with startTS.
// acquire is true when success
// timeout is true when the startTS is already timeout
// newWait is true when current transaction is new for the current latch
func (l *Latch) acquire(startTS uint64) (acquire, timeout, newWait bool) {
	l.Lock()
	defer l.Unlock()
	timeout = startTS <= l.lastCommitTs
	if timeout {
		return
	}
	if len(l.waiting) == 0 || l.waiting[0] != startTS {
		l.waiting = append(l.waiting, startTS)
		newWait = true
	}

	acquire = l.waiting[0] == startTS
	return
}

// release releases the transaction with startTS and commitTS from current latch.
// isEmpty is true when the waiting queue is empty after release current transaction,
// otherwise return the front transaction in queue.
func (l *Latch) release(startTS uint64, commitTS uint64) (isEmpty bool, front uint64) {
	l.Lock()
	defer l.Unlock()
	if startTS != l.waiting[0] {
		panic(fmt.Sprintf("invalid front ts %d, latch:%+v", startTS, l))
	}
	if commitTS > l.lastCommitTs {
		l.lastCommitTs = commitTS
	}
	l.waiting = l.waiting[1:]
	if len(l.waiting) == 0 {
		isEmpty = true
	} else {
		front = l.waiting[0]
		isEmpty = false
	}
	return
}

// Lock is the locks' information required for a transaction.
type Lock struct {
	// The slot IDs of the latches(keys) that a startTS must acquire before being able to processed.
	requiredSlots []int
	// The number of latches that the transaction has acquired.
	acquiredCount int
	// The number of latches whose waiting queue contains current transaction.
	waitedCount int
	// Current transaction's startTS.
	startTS uint64
}

// NewLock creates a new lock.
func NewLock(startTS uint64, requiredSlots []int) Lock {
	return Lock{
		requiredSlots: requiredSlots,
		acquiredCount: 0,
		waitedCount:   0,
		startTS:       startTS,
	}
}

// Latches which are used for concurrency control.
// Each latch is indexed by a slot's ID, hence the term latch and slot are used in interchangeable,
// but conceptually a latch is a queue, and a slot is an index to the queue
type Latches []Latch

// NewLatches create a Latches with fixed length,
// the size will be rounded up to the power of 2.
func NewLatches(size int) Latches {
	powerOfTwoSize := 1 << uint(math.Ceil(math.Log2(float64(size))))
	latches := make([]Latch, powerOfTwoSize, powerOfTwoSize)
	return latches
}

// GenLock generates Lock for the transaction with startTS and keys.
func (latches Latches) GenLock(startTS uint64, keys [][]byte) Lock {
	hashes := make(map[int]bool)
	for _, key := range keys {
		hashes[latches.hash(key)] = true
	}
	slots := make([]int, 0, len(hashes))
	for key := range hashes {
		slots = append(slots, key)
	}
	sort.Ints(slots)
	return NewLock(startTS, slots)
}

// hash return hash int for current key.
func (latches Latches) hash(key []byte) int {
	return int(murmur3.Sum32(key)) & (len(latches) - 1)
}

// Acquire tries to acquire the lock for a transaction.
// It returns with timeout = true when the transaction is timeout(
// when the lock.startTS is smaller than any key's last commitTS).
// It returns with acquired = true when acquire success and the transaction
// is ready to commit.
func (latches Latches) Acquire(lock *Lock) (acquired, timeout bool) {
	var newWait bool
	for lock.acquiredCount < len(lock.requiredSlots) {
		slotID := lock.requiredSlots[lock.acquiredCount]
		acquired, timeout, newWait = latches[slotID].acquire(lock.startTS)
		if newWait {
			lock.waitedCount++
		}
		if timeout || !acquired {
			return
		}
		lock.acquiredCount++
	}
	return
}

// Release releases all latches owned by the `lock` and returns the wakeup list.
// Preconditions: the caller must ensure the transaction is at the front of the latches.
func (latches Latches) Release(lock *Lock, commitTS uint64) (wakeupList []uint64) {
	wakeupList = make([]uint64, 0, lock.waitedCount)
	for id := 0; id < lock.waitedCount; id++ {
		slotID := lock.requiredSlots[id]
		isEmpty, front := latches[slotID].release(lock.startTS, commitTS)
		if !isEmpty {
			wakeupList = append(wakeupList, front)
		}
	}
	return
}
