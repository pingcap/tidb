// Copyright 2020 PingCAP, Inc.
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
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	pb "github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/prometheus/client_golang/prometheus"
)

type actionPessimisticLock struct {
	*kv.LockCtx
}
type actionPessimisticRollback struct{}

var (
	_ twoPhaseCommitAction = actionPessimisticLock{}
	_ twoPhaseCommitAction = actionPessimisticRollback{}

	tiKVTxnRegionsNumHistogramPessimisticLock     = metrics.TiKVTxnRegionsNumHistogram.WithLabelValues(metricsTag("pessimistic_lock"))
	tiKVTxnRegionsNumHistogramPessimisticRollback = metrics.TiKVTxnRegionsNumHistogram.WithLabelValues(metricsTag("pessimistic_rollback"))
)

func (actionPessimisticLock) String() string {
	return "pessimistic_lock"
}

func (actionPessimisticLock) tiKVTxnRegionsNumHistogram() prometheus.Observer {
	return tiKVTxnRegionsNumHistogramPessimisticLock
}

func (actionPessimisticRollback) String() string {
	return "pessimistic_rollback"
}

func (actionPessimisticRollback) tiKVTxnRegionsNumHistogram() prometheus.Observer {
	return tiKVTxnRegionsNumHistogramPessimisticRollback
}

func (action actionPessimisticLock) handleSingleBatch(c *twoPhaseCommitter, bo *Backoffer, batch *batchMutations) (bool, error) {
	m := &batch.mutations
	mutations := make([]*pb.Mutation, m.len())
	for i := range m.keys {
		mut := &pb.Mutation{
			Op:  pb.Op_PessimisticLock,
			Key: m.keys[i],
		}
		if c.txn.us.HasPresumeKeyNotExists(m.keys[i]) {
			mut.Assertion = pb.Assertion_NotExist
		}
		mutations[i] = mut
	}
	elapsed := uint64(time.Since(c.txn.startTime) / time.Millisecond)
	req := tikvrpc.NewRequest(tikvrpc.CmdPessimisticLock, &pb.PessimisticLockRequest{
		Mutations:    mutations,
		PrimaryLock:  c.primary(),
		StartVersion: c.startTS,
		ForUpdateTs:  c.forUpdateTS,
		LockTtl:      elapsed + atomic.LoadUint64(&ManagedLockTTL),
		IsFirstLock:  c.isFirstLock,
		WaitTimeout:  action.LockWaitTime,
		ReturnValues: action.ReturnValues,
		MinCommitTs:  c.forUpdateTS + 1,
	}, pb.Context{Priority: c.priority, SyncLog: c.syncLog})
	lockWaitStartTime := action.WaitStartTime
	for {
		// if lockWaitTime set, refine the request `WaitTimeout` field based on timeout limit
		if action.LockWaitTime > 0 {
			timeLeft := action.LockWaitTime - (time.Since(lockWaitStartTime)).Milliseconds()
			if timeLeft <= 0 {
				req.PessimisticLock().WaitTimeout = kv.LockNoWait
			} else {
				req.PessimisticLock().WaitTimeout = timeLeft
			}
		}
		failpoint.Inject("PessimisticLockErrWriteConflict", func() {
			time.Sleep(300 * time.Millisecond)
			failpoint.Return(false, kv.ErrWriteConflict)
		})
		startTime := time.Now()
		resp, err := c.store.SendReq(bo, req, batch.region, readTimeoutShort)
		if action.LockCtx.Stats != nil {
			atomic.AddInt64(&action.LockCtx.Stats.LockRPCTime, int64(time.Since(startTime)))
			atomic.AddInt64(&action.LockCtx.Stats.LockRPCCount, 1)
		}
		if err != nil {
			return false, errors.Trace(err)
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return false, errors.Trace(err)
		}
		if regionErr != nil {
			return true, errors.Trace(errors.New(regionErr.String()))
		}
		if resp.Resp == nil {
			return false, errors.Trace(ErrBodyMissing)
		}
		lockResp := resp.Resp.(*pb.PessimisticLockResponse)
		keyErrs := lockResp.GetErrors()
		if len(keyErrs) == 0 {
			if action.ReturnValues {
				action.ValuesLock.Lock()
				for i, mutation := range mutations {
					action.Values[string(mutation.Key)] = kv.ReturnedValue{Value: lockResp.Values[i]}
				}
				action.ValuesLock.Unlock()
			}
			return false, nil
		}
		var locks []*Lock
		for _, keyErr := range keyErrs {
			// Check already exists error
			if alreadyExist := keyErr.GetAlreadyExist(); alreadyExist != nil {
				key := alreadyExist.GetKey()
				return false, c.extractKeyExistsErr(key)
			}
			if deadlock := keyErr.Deadlock; deadlock != nil {
				return false, &ErrDeadlock{Deadlock: deadlock}
			}

			// Extract lock from key error
			lock, err1 := extractLockFromKeyErr(keyErr)
			if err1 != nil {
				return false, errors.Trace(err1)
			}
			locks = append(locks, lock)
		}
		// Because we already waited on tikv, no need to Backoff here.
		// tikv default will wait 3s(also the maximum wait value) when lock error occurs
		startTime = time.Now()
		msBeforeTxnExpired, _, err := c.store.lockResolver.ResolveLocks(bo, 0, locks)
		if err != nil {
			return false, errors.Trace(err)
		}
		if action.LockCtx.Stats != nil {
			atomic.AddInt64(&action.LockCtx.Stats.ResolveLockTime, int64(time.Since(startTime)))
		}

		// If msBeforeTxnExpired is not zero, it means there are still locks blocking us acquiring
		// the pessimistic lock. We should return acquire fail with nowait set or timeout error if necessary.
		if msBeforeTxnExpired > 0 {
			if action.LockWaitTime == kv.LockNoWait {
				return false, ErrLockAcquireFailAndNoWaitSet
			} else if action.LockWaitTime == kv.LockAlwaysWait {
				// do nothing but keep wait
			} else {
				// the lockWaitTime is set, we should return wait timeout if we are still blocked by a lock
				if time.Since(lockWaitStartTime).Milliseconds() >= action.LockWaitTime {
					return false, errors.Trace(ErrLockWaitTimeout)
				}
			}
			if action.LockCtx.PessimisticLockWaited != nil {
				atomic.StoreInt32(action.LockCtx.PessimisticLockWaited, 1)
			}
		}

		// Handle the killed flag when waiting for the pessimistic lock.
		// When a txn runs into LockKeys() and backoff here, it has no chance to call
		// executor.Next() and check the killed flag.
		if action.Killed != nil {
			// Do not reset the killed flag here!
			// actionPessimisticLock runs on each region parallelly, we have to consider that
			// the error may be dropped.
			if atomic.LoadUint32(action.Killed) == 1 {
				return false, errors.Trace(ErrQueryInterrupted)
			}
		}
	}
}

func (actionPessimisticRollback) handleSingleBatch(c *twoPhaseCommitter, bo *Backoffer, batch *batchMutations) (bool, error) {
	req := tikvrpc.NewRequest(tikvrpc.CmdPessimisticRollback, &pb.PessimisticRollbackRequest{
		StartVersion: c.startTS,
		ForUpdateTs:  c.forUpdateTS,
		Keys:         batch.mutations.keys,
	})
	resp, err := c.store.SendReq(bo, req, batch.region, readTimeoutShort)
	if err != nil {
		return false, errors.Trace(err)
	}
	regionErr, err := resp.GetRegionError()
	if err != nil {
		return false, errors.Trace(err)
	}
	if regionErr != nil {
		return true, errors.Trace(errors.New(regionErr.String()))
	}
	return false, nil
}

func (c *twoPhaseCommitter) tryPreSplitLockKeys(bo *Backoffer, keys [][]byte) error {
	if uint32(len(keys)) < preSplitDetectThreshold {
		return nil
	}

	var preSplitCal preSplitCalculator
	it := c.mapWithRegion(bo, lockKeysMutations{keys}.Iter(nil, nil))
	for {
		m, err := it.Next()
		if err != nil {
			return errors.Trace(err)
		}
		if m.key == nil {
			break
		}
		preSplitCal.Process(m)
	}
	splitKeys, splitRegions := preSplitCal.Finish()
	c.trySplitRegions(splitKeys, splitRegions)
	return nil
}

func (c *twoPhaseCommitter) pessimisticLockMutations(bo *Backoffer, lockCtx *kv.LockCtx, mutations mutations) error {
	exec := c.newExecController(mutations, actionPessimisticLock{LockCtx: lockCtx})
	return exec.run(bo)
}

func (c *twoPhaseCommitter) pessimisticRollbackMutations(bo *Backoffer, mutations mutations) error {
	exec := c.newExecController(mutations, actionPessimisticRollback{})
	return exec.run(bo)
}

func (c *twoPhaseCommitter) pessimisticLockKeys(bo *Backoffer, lockCtx *kv.LockCtx, keys [][]byte) error {
	if err := c.tryPreSplitLockKeys(bo, keys); err != nil {
		return err
	}
	return c.pessimisticLockMutations(bo, lockCtx, lockKeysMutations{keys})
}

func (c *twoPhaseCommitter) pessimisticRollbackKeys(bo *Backoffer, keys [][]byte) error {
	return c.pessimisticRollbackMutations(bo, lockKeysMutations{keys})
}
