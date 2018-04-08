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

package tikv

import (
	"fmt"
	"hash/fnv"
	"math"
	"sort"
	"sync"

	log "github.com/sirupsen/logrus"
)

// Latch stores a key's waiting transactions
type Latch struct {
	waiting      []uint64
	lastCommitTs uint64
	lock         sync.Mutex
}

// acquire tries to get current key's lock for the transaction with startTs
// acquire is true when success
// timeout is true when the startTs is already timeout
// newWait is true when current transaction is new for the current latch
func (l *Latch) acquire(startTs uint64) (acquire, timeout, newWait bool) {
	l.lock.Lock()
	defer l.lock.Unlock()
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
	l.lock.Lock()
	defer l.lock.Unlock()
	if startTs != l.waiting[0] {
		panic(fmt.Sprintf("invalid front ts %v, latch:%+v", startTs, l))
	}
	// for rollback or timeout, the commitTs maybe zero
	if commitTs > l.lastCommitTs {
		l.lastCommitTs = commitTs
	}
	l.waiting = l.waiting[1:]
	if len(l.waiting) == 0 {
		// Should we reset the lastCommitTs since next key
		// may different from current key?
		// l.lastCommitTs = 0
		isEmpty = true
	} else {
		front = l.waiting[0]
		isEmpty = false
	}
	return
}

// try append startTs into latch, return false
// when startTs is already timeout
func (l *Latch) append(startTs uint64) bool {
	if startTs <= l.lastCommitTs {
		return false
	}
	l.waiting = append(l.waiting, startTs)
	return true
}

// TLock is the locks' information required for a transaction
type TLock struct {
	// the slot IDs of the latches(keys) that a startTs must acquire before being able to processed
	requiredSlots []int
	/// The number of latches that the transaction has acquired.
	acquiredCount int
	/// The number of latches that the waiting queue contains current transaction
	waitedCount int
	// Current transaction's startTs
	startTs uint64
}

// NewTLock creates a new lock
func NewTLock(startTs uint64, requiredSlots []int) TLock {
	return TLock{
		requiredSlots: requiredSlots,
		acquiredCount: 0,
		waitedCount:   0,
		startTs:       startTs,
	}
}

// Latches which are used for concurrency control
// Each latch is indexed by a slit ID, hence the term latch and slot are used in interchangeable,
// but conceptually a latch is a queue, and a slot is an index to the queue
type Latches struct {
	slots []Latch
	size  int
}

// NewLatches the size will be rounded up to the power of 2
func NewLatches(size int) Latches {
	powerOfTwoSize := int(math.Pow(2, math.Ceil(math.Log2(float64(size)))))
	slots := make([]Latch, powerOfTwoSize, powerOfTwoSize)
	return Latches{
		slots: slots,
		size:  powerOfTwoSize,
	}
}

// GenTLock generates TLock for the transaction with startTs and keys
func (latches *Latches) GenTLock(startTs uint64, keys [][]byte) TLock {
	hashes := make(map[int]bool)
	for _, key := range keys {
		hashes[latches.hash(key)] = true
	}
	slots := make([]int, 0, len(hashes))
	for key := range hashes {
		slots = append(slots, key)
	}
	sort.Ints(slots)
	return NewTLock(startTs, slots)
}

// return hash int for current key
func (latches *Latches) hash(key []byte) int {
	h := fnv.New32a()
	_, err := h.Write(key)
	if err != nil {
		log.Warn("hash key %v failed with err:%+v", key, err)
	}
	return int(h.Sum32()) & (latches.size - 1)
}

// Acquire tries to acquire the lock for a transaction
// It returns with timeout = true when the transaction is timeout(
// when the lock.startTs is smaller than any key's last commitTs).
// It returns with acquired = true when acquire success and the transaction
// is ready to 2PC
func (latches *Latches) Acquire(lock *TLock) (acquired, timeout bool) {
	var newWait bool
	for lock.acquiredCount < len(lock.requiredSlots) {
		slotID := lock.requiredSlots[lock.acquiredCount]
		acquired, timeout, newWait = latches.slots[slotID].acquire(lock.startTs)
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
func (latches *Latches) Release(lock *TLock, commitTs uint64) (wakeupList []uint64) {
	wakeupList = make([]uint64, 0, lock.waitedCount)
	for id := 0; id < lock.waitedCount; id++ {
		slotID := lock.requiredSlots[id]
		isEmpty, front := latches.slots[slotID].release(lock.startTs, commitTs)
		if !isEmpty {
			wakeupList = append(wakeupList, front)
		}
	}
	return
}
