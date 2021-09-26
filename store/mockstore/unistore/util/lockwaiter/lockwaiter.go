// Copyright 2019-present PingCAP, Inc.
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

package lockwaiter

import (
	"sort"
	"sync"
	"time"

	"github.com/pingcap/kvproto/pkg/deadlock"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/store/mockstore/unistore/config"
	"go.uber.org/zap"
)

// LockNoWait is used for pessimistic lock wait time
// these two constants are special for lock protocol with tikv
// -1 means nowait, others meaning lock wait in milliseconds
var LockNoWait = int64(-1)

// Manager represents a waiters manager.
type Manager struct {
	mu                  sync.Mutex
	waitingQueues       map[uint64]*queue
	wakeUpDelayDuration int64
}

// NewManager returns a new manager.
func NewManager(conf *config.Config) *Manager {
	return &Manager{
		waitingQueues:       map[uint64]*queue{},
		wakeUpDelayDuration: conf.PessimisticTxn.WakeUpDelayDuration,
	}
}

type queue struct {
	waiters []*Waiter
}

func (q *queue) getOldestWaiter() (*Waiter, []*Waiter) {
	// make the waiters in start ts order
	sort.Slice(q.waiters, func(i, j int) bool {
		return q.waiters[i].startTS < q.waiters[j].startTS
	})
	oldestWaiter := q.waiters[0]
	remainWaiter := q.waiters[1:]
	// the remain waiters still exist in the wait queue
	q.waiters = remainWaiter
	return oldestWaiter, remainWaiter
}

// removeWaiter removes the correspond waiter from pending array
// it should be used under map lock protection
func (q *queue) removeWaiter(w *Waiter) {
	for i, waiter := range q.waiters {
		if waiter == w {
			q.waiters = append(q.waiters[:i], q.waiters[i+1:]...)
			break
		}
	}
}

// Waiter represents a waiter.
type Waiter struct {
	deadlineTime        time.Time
	timer               *time.Timer
	ch                  chan WaitResult
	wakeUpDelayDuration int64
	startTS             uint64
	LockTS              uint64
	KeyHash             uint64
	CommitTs            uint64
	wakeupDelayed       bool
}

// WakeupWaitTime is the implementation of variable "wake-up-delay-duration"
type WakeupWaitTime int

// WaitResult represents a wait result.
type WaitResult struct {
	// WakeupSleepTime, -1 means the wait is already timeout, 0 means the lock will be granted to this waiter
	// others are the wake-up-delay-duration sleep time, in milliseconds
	WakeupSleepTime WakeupWaitTime
	CommitTS        uint64
	DeadlockResp    *deadlock.DeadlockResponse
}

// WakeupWaitTime
const (
	WaitTimeout        WakeupWaitTime = -1
	WakeUpThisWaiter   WakeupWaitTime = 0
	WakeupDelayTimeout WakeupWaitTime = 1
)

// Wait waits on a lock until waked by others or timeout.
func (w *Waiter) Wait() WaitResult {
	for {
		select {
		case <-w.timer.C:
			if w.wakeupDelayed {
				return WaitResult{WakeupSleepTime: WakeupDelayTimeout, CommitTS: w.CommitTs}
			}
			return WaitResult{WakeupSleepTime: WaitTimeout}
		case result := <-w.ch:
			if result.WakeupSleepTime == WakeupDelayTimeout {
				w.CommitTs = result.CommitTS
				w.wakeupDelayed = true
				delaySleepDuration := time.Duration(w.wakeUpDelayDuration) * time.Millisecond
				if time.Now().Add(delaySleepDuration).Before(w.deadlineTime) {
					if w.timer.Stop() {
						w.timer.Reset(delaySleepDuration)
					}
				}
				continue
			}
			return result
		}
	}
}

// DrainCh drains channel.
func (w *Waiter) DrainCh() {
	for len(w.ch) > 0 {
		<-w.ch
	}
}

// NewWaiter returns a new waiter.
func (lw *Manager) NewWaiter(startTS, lockTS, keyHash uint64, timeout time.Duration) *Waiter {
	// allocate memory before hold the lock.
	q := new(queue)
	q.waiters = make([]*Waiter, 0, 8)
	waiter := &Waiter{
		deadlineTime:        time.Now().Add(timeout),
		wakeUpDelayDuration: lw.wakeUpDelayDuration,
		timer:               time.NewTimer(timeout),
		ch:                  make(chan WaitResult, 32),
		startTS:             startTS,
		LockTS:              lockTS,
		KeyHash:             keyHash,
	}
	q.waiters = append(q.waiters, waiter)
	lw.mu.Lock()
	if old, ok := lw.waitingQueues[keyHash]; ok {
		old.waiters = append(old.waiters, waiter)
	} else {
		lw.waitingQueues[keyHash] = q
	}
	lw.mu.Unlock()
	return waiter
}

// WakeUp wakes up waiters that waiting on the transaction.
func (lw *Manager) WakeUp(txn, commitTS uint64, keyHashes []uint64) {
	waiters := make([]*Waiter, 0, 8)
	wakeUpDelayWaiters := make([]*Waiter, 0, 8)
	lw.mu.Lock()
	for _, keyHash := range keyHashes {
		q := lw.waitingQueues[keyHash]
		if q != nil {
			waiter, remainWaiters := q.getOldestWaiter()
			waiters = append(waiters, waiter)
			if len(remainWaiters) == 0 {
				delete(lw.waitingQueues, keyHash)
			} else {
				wakeUpDelayWaiters = append(wakeUpDelayWaiters, remainWaiters...)
			}
		}
	}
	lw.mu.Unlock()

	// wake up waiters
	if len(waiters) > 0 {
		for _, w := range waiters {
			select {
			case w.ch <- WaitResult{WakeupSleepTime: WakeUpThisWaiter, CommitTS: commitTS}:
			default:
			}
		}
		log.S().Debug("wakeup", len(waiters), "txns blocked by txn", txn, " keyHashes=", keyHashes)
	}
	// wake up delay waiters, this will not remove waiter from queue
	if len(wakeUpDelayWaiters) > 0 {
		for _, w := range wakeUpDelayWaiters {
			select {
			case w.ch <- WaitResult{WakeupSleepTime: WakeupDelayTimeout, CommitTS: commitTS}:
			default:
			}
		}
	}
}

// CleanUp removes a waiter from waitingQueues when wait timeout.
func (lw *Manager) CleanUp(w *Waiter) {
	lw.mu.Lock()
	q := lw.waitingQueues[w.KeyHash]
	if q != nil {
		q.removeWaiter(w)
		if len(q.waiters) == 0 {
			delete(lw.waitingQueues, w.KeyHash)
		}
	}
	lw.mu.Unlock()
	w.DrainCh()
}

// WakeUpForDeadlock wakes up waiters waiting for deadlock detection results
func (lw *Manager) WakeUpForDeadlock(resp *deadlock.DeadlockResponse) {
	var (
		waiter         *Waiter
		waitForKeyHash uint64
	)
	waitForKeyHash = resp.Entry.KeyHash
	lw.mu.Lock()
	q := lw.waitingQueues[waitForKeyHash]
	if q != nil {
		for i, curWaiter := range q.waiters {
			// there should be no duplicated waiters
			if curWaiter.startTS == resp.Entry.Txn && curWaiter.KeyHash == resp.Entry.KeyHash {
				log.Info("deadlock detection response got", zap.Stringer("entry", &resp.Entry))
				waiter = curWaiter
				q.waiters = append(q.waiters[:i], q.waiters[i+1:]...)
				break
			}
		}
		if len(q.waiters) == 0 {
			delete(lw.waitingQueues, waitForKeyHash)
		}
	}
	lw.mu.Unlock()
	if waiter != nil {
		waiter.ch <- WaitResult{DeadlockResp: resp}
		log.S().Infof("wakeup txn=%v blocked by txn=%v because of deadlock, keyHash=%v, deadlockKeyHash=%v",
			resp.Entry.Txn, resp.Entry.WaitForTxn, resp.Entry.KeyHash, resp.DeadlockKeyHash)
	}
}
