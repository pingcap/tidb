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
	"sync"
	"testing"
	"time"

	deadlockPb "github.com/pingcap/kvproto/pkg/deadlock"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/store/mockstore/unistore/config"
	"github.com/stretchr/testify/require"
)

func TestLockwaiterBasic(t *testing.T) {
	mgr := NewManager(&config.DefaultConf)

	keyHash := uint64(100)
	mgr.NewWaiter(1, 2, keyHash, 10)

	// basic check queue and waiter
	q := mgr.waitingQueues[keyHash]
	require.NotNil(t, q)
	waiter := q.waiters[0]
	require.Equal(t, uint64(1), waiter.startTS)
	require.Equal(t, uint64(2), waiter.LockTS)
	require.Equal(t, uint64(100), waiter.KeyHash)

	// check ready waiters
	keysHash := make([]uint64, 0, 10)
	keysHash = append(keysHash, keyHash)
	rdyWaiter, _ := q.getOldestWaiter()
	require.Equal(t, uint64(1), rdyWaiter.startTS)
	require.Equal(t, uint64(2), rdyWaiter.LockTS)
	require.Equal(t, uint64(100), rdyWaiter.KeyHash)

	// basic wake up test
	waiter = mgr.NewWaiter(3, 2, keyHash, 10)
	mgr.WakeUp(2, 222, keysHash)
	res := <-waiter.ch
	require.Equal(t, uint64(222), res.CommitTS)
	require.Len(t, q.waiters, 0)
	q = mgr.waitingQueues[keyHash]
	// verify queue deleted from map
	require.Nil(t, q)

	// basic wake up for deadlock test
	waiter = mgr.NewWaiter(3, 4, keyHash, 10)
	resp := &deadlockPb.DeadlockResponse{}
	resp.Entry.Txn = 3
	resp.Entry.WaitForTxn = 4
	resp.Entry.KeyHash = keyHash
	resp.DeadlockKeyHash = 30192
	mgr.WakeUpForDeadlock(resp)
	res = <-waiter.ch
	require.NotNil(t, res.DeadlockResp)
	require.Equal(t, uint64(3), res.DeadlockResp.Entry.Txn)
	require.Equal(t, uint64(4), res.DeadlockResp.Entry.WaitForTxn)
	require.Equal(t, keyHash, res.DeadlockResp.Entry.KeyHash)
	require.Equal(t, uint64(30192), res.DeadlockResp.DeadlockKeyHash)
	q = mgr.waitingQueues[4]
	// verify queue deleted from map
	require.Nil(t, q)
}

func TestLockwaiterConcurrent(t *testing.T) {
	mgr := NewManager(&config.DefaultConf)
	wg := &sync.WaitGroup{}
	endWg := &sync.WaitGroup{}
	waitForTxn := uint64(100)
	commitTs := uint64(199)
	deadlockKeyHash := uint64(299)
	numbers := uint64(10)
	lock := sync.RWMutex{}
	for i := uint64(0); i < numbers; i++ {
		wg.Add(1)
		endWg.Add(1)
		go func(num uint64) {
			defer endWg.Done()
			waiter := mgr.NewWaiter(num, waitForTxn, num*10, 100*time.Millisecond)
			// i == numbers - 1 use CleanUp Waiter and the results will be timeout
			if num == numbers-1 {
				mgr.CleanUp(waiter)
				wg.Done()
				res := waiter.Wait()
				require.Equal(t, WaitTimeout, res.WakeupSleepTime)
				require.Equal(t, uint64(0), res.CommitTS)
				require.Nil(t, res.DeadlockResp)
			} else {
				wg.Done()
				res := waiter.Wait()
				// even woken up by commit
				if num%2 == 0 {
					require.Equal(t, commitTs, res.CommitTS)
				} else {
					// odd woken up by deadlock
					require.NotNil(t, res.DeadlockResp)
					lock.RLock()
					require.Equal(t, deadlockKeyHash, res.DeadlockResp.DeadlockKeyHash)
					lock.RUnlock()
				}
			}
		}(i)
	}
	wg.Wait()
	keyHashes := make([]uint64, 0, 4)
	resp := &deadlockPb.DeadlockResponse{}
	for i := uint64(0); i < numbers; i++ {
		keyHashes = keyHashes[:0]
		if i%2 == 0 {
			log.S().Infof("wakeup i=%v", i)
			mgr.WakeUp(waitForTxn, commitTs, append(keyHashes, i*10))
		} else {
			log.S().Infof("deadlock wakeup i=%v", i)
			lock.Lock()
			resp.DeadlockKeyHash = deadlockKeyHash
			lock.Unlock()
			resp.Entry.Txn = i
			resp.Entry.WaitForTxn = waitForTxn
			resp.Entry.KeyHash = i * 10
			mgr.WakeUpForDeadlock(resp)
		}
	}
	endWg.Wait()
}
