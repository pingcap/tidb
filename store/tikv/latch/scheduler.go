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

const lockChanSize = 100

// LatchesScheduler is used to schedule latches for transactions.
type LatchesScheduler struct {
	latches *Latches
	lockCh  chan *Lock
}

//NewLatchesScheduler create the LatchesScheduler.
func NewLatchesScheduler(size int) *LatchesScheduler {
	latches := NewLatches(size)
	lockCh := make(chan *Lock, lockChanSize)
	scheduler := &LatchesScheduler{
		latches: latches,
		lockCh:  lockCh,
	}
	go scheduler.run()
	return scheduler
}

func (scheduler *LatchesScheduler) run() {
	for lock := range scheduler.lockCh {
		wakeupList := scheduler.latches.release(lock, lock.commitTS)
		if len(wakeupList) > 0 {
			scheduler.wakeup(wakeupList)
		}
	}
}

func (scheduler *LatchesScheduler) wakeup(wakeupList []*Lock) {
	for _, lock := range wakeupList {
		if scheduler.latches.acquire(lock) != acquireLocked {
			lock.wg.Done()
		}
	}
}

// Close closes LatchesScheduler.
func (scheduler *LatchesScheduler) Close() {
	close(scheduler.lockCh)
}

// Lock acquire the lock for transaction with startTS and keys.
func (scheduler *LatchesScheduler) Lock(startTS uint64, keys [][]byte) *Lock {
	lock := scheduler.latches.GenLock(startTS, keys)
	lock.wg.Add(1)
	if scheduler.latches.acquire(lock) == acquireLocked {
		lock.wg.Wait()
	}
	if lock.status == acquireLocked {
		panic("should never run here")
	}
	return lock
}

// UnLock unlocks a lock with commitTS.
func (scheduler *LatchesScheduler) UnLock(lock *Lock, commitTS uint64) {
	lock.commitTS = commitTS
	scheduler.lockCh <- lock
}

// RefreshCommitTS refresh commitTS for keys.
func (scheduler *LatchesScheduler) RefreshCommitTS(keys [][]byte, commitTS uint64) {
	scheduler.latches.refreshCommitTS(keys, commitTS)
}
