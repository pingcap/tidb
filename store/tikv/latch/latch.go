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

	log "github.com/sirupsen/logrus"
	"github.com/spaolacci/murmur3"
)

// Latch stores a key's waiting transactions
type Latch struct {
	// A queue by startTs of those waiting transactions
	waiting      []uint64
	lastCommitTs uint64
	sync.Mutex
}

// acquire tries to get current key's lock for the transaction with startTs
// acquire is true when success
// timeout is true when the startTs is already timeout
// newWait is true when current transaction is new for the current latch
func (l *Latch) acquire(startTs uint64) (acquire, timeout, newWait bool) {
	l.Lock()
	defer l.Unlock()
	timeout = startTs <= l.lastCommitTs
	if timeout {
		return
	}
	if len(l.waiting) == 0 || l.waiting[0] != startTs {
		l.waiting = append(l.waiting, startTs)
		newWait = true
	}

	acquire = l.waiting[0] == startTs
	return
}

// release releases the transaction with startTs and commitTs from current latch.
// isEmpty is true when the waiting queue is empty after release current transaction,
// otherwise return the front transaction in queue.
func (l *Latch) release(startTs uint64, commitTs uint64) (isEmpty bool, front uint64) {
	l.Lock()
	defer l.Unlock()
	if startTs != l.waiting[0] {
		panic(fmt.Sprintf("invalid front ts %d, latch:%+v", startTs, l))
	}
	// for rollback or timeout, the commitTs maybe zero
	if commitTs > l.lastCommitTs {
		l.lastCommitTs = commitTs
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

// Lock is the locks' information required for a transaction
type Lock struct {
	// the slot IDs of the latches(keys) that a startTs must acquire before being able to processed
	requiredSlots []int
	/// The number of latches that the transaction has acquired.
	acquiredCount int
	/// The number of latches that the waiting queue contains current transaction
	waitedCount int
	// Current transaction's startTs
	startTs uint64
}

// NewLock creates a new lock
func NewLock(startTs uint64, requiredSlots []int) Lock {
	return Lock{
		requiredSlots: requiredSlots,
		acquiredCount: 0,
		waitedCount:   0,
		startTs:       startTs,
	}
}

// Latches which are used for concurrency control
// Each latch is indexed by a slit ID, hence the term latch and slot are used in interchangeable,
// but conceptually a latch is a queue, and a slot is an index to the queue
type Latches []Latch

// NewLatches the size will be rounded up to the power of 2
func NewLatches(size int) Latches {
	powerOfTwoSize := 1 << uint(math.Ceil(math.Log2(float64(size))))
	latches := make([]Latch, powerOfTwoSize, powerOfTwoSize)
	return latches
}

// GenLock generates Lock for the transaction with startTs and keys
func (latches Latches) GenLock(startTs uint64, keys [][]byte) Lock {
	hashes := make(map[int]bool)
	for _, key := range keys {
		hashes[latches.hash(key)] = true
	}
	slots := make([]int, 0, len(hashes))
	for key := range hashes {
		slots = append(slots, key)
	}
	sort.Ints(slots)
	return NewLock(startTs, slots)
}

// return hash int for current key
func (latches Latches) hash(key []byte) int {
	h := murmur3.New32()
	_, err := h.Write(key)
	if err != nil {
		log.Warn("hash key %v failed with err:%+v", key, err)
	}
	return int(h.Sum32()) & (len(latches) - 1)
}

// Acquire tries to acquire the lock for a transaction
// It returns with timeout = true when the transaction is timeout(
// when the lock.startTs is smaller than any key's last commitTs).
// It returns with acquired = true when acquire success and the transaction
// is ready to 2PC
func (latches Latches) Acquire(lock *Lock) (acquired, timeout bool) {
	var newWait bool
	for lock.acquiredCount < len(lock.requiredSlots) {
		slotID := lock.requiredSlots[lock.acquiredCount]
		acquired, timeout, newWait = latches[slotID].acquire(lock.startTs)
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
func (latches Latches) Release(lock *Lock, commitTs uint64) (wakeupList []uint64) {
	wakeupList = make([]uint64, 0, lock.waitedCount)
	for id := 0; id < lock.waitedCount; id++ {
		slotID := lock.requiredSlots[id]
		isEmpty, front := latches[slotID].release(lock.startTs, commitTs)
		if !isEmpty {
			wakeupList = append(wakeupList, front)
		}
	}
	return
}
