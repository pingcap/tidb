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

func (action actionPessimisticLock) handleSingleBatch(c *twoPhaseCommitter, bo *Backoffer, batch batchMutations) error {
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
		failpoint.Inject("PessimisticLockErrWriteConflict", func() error {
			time.Sleep(300 * time.Millisecond)
			return kv.ErrWriteConflict
		})
		resp, err := c.store.SendReq(bo, req, batch.region, readTimeoutShort)
		if err != nil {
			return errors.Trace(err)
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return errors.Trace(err)
		}
		if regionErr != nil {
			err = bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return errors.Trace(err)
			}
			err = c.pessimisticLockMutations(bo, action.LockCtx, batch.mutations)
			return errors.Trace(err)
		}
		if resp.Resp == nil {
			return errors.Trace(ErrBodyMissing)
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
			return nil
		}
		var locks []*Lock
		for _, keyErr := range keyErrs {
			// Check already exists error
			if alreadyExist := keyErr.GetAlreadyExist(); alreadyExist != nil {
				key := alreadyExist.GetKey()
				return c.extractKeyExistsErr(key)
			}
			if deadlock := keyErr.Deadlock; deadlock != nil {
				return &ErrDeadlock{Deadlock: deadlock}
			}

			// Extract lock from key error
			lock, err1 := extractLockFromKeyErr(keyErr)
			if err1 != nil {
				return errors.Trace(err1)
			}
			locks = append(locks, lock)
		}
		// Because we already waited on tikv, no need to Backoff here.
		// tikv default will wait 3s(also the maximum wait value) when lock error occurs
		startTime := time.Now()
		msBeforeTxnExpired, _, err := c.store.lockResolver.ResolveLocks(bo, 0, locks)
		if err != nil {
			return errors.Trace(err)
		}
		if action.LockCtx.Stats != nil {
			atomic.AddInt64(&action.LockCtx.Stats.ResolveLockTime, int64(time.Since(startTime)))
		}

		// If msBeforeTxnExpired is not zero, it means there are still locks blocking us acquiring
		// the pessimistic lock. We should return acquire fail with nowait set or timeout error if necessary.
		if msBeforeTxnExpired > 0 {
			if action.LockWaitTime == kv.LockNoWait {
				return ErrLockAcquireFailAndNoWaitSet
			} else if action.LockWaitTime == kv.LockAlwaysWait {
				// do nothing but keep wait
			} else {
				// the lockWaitTime is set, we should return wait timeout if we are still blocked by a lock
				if time.Since(lockWaitStartTime).Milliseconds() >= action.LockWaitTime {
					return errors.Trace(ErrLockWaitTimeout)
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
				return errors.Trace(ErrQueryInterrupted)
			}
		}
	}
}

func (actionPessimisticRollback) handleSingleBatch(c *twoPhaseCommitter, bo *Backoffer, batch batchMutations) error {
	req := tikvrpc.NewRequest(tikvrpc.CmdPessimisticRollback, &pb.PessimisticRollbackRequest{
		StartVersion: c.startTS,
		ForUpdateTs:  c.forUpdateTS,
		Keys:         batch.mutations.keys,
	})
	resp, err := c.store.SendReq(bo, req, batch.region, readTimeoutShort)
	if err != nil {
		return errors.Trace(err)
	}
	regionErr, err := resp.GetRegionError()
	if err != nil {
		return errors.Trace(err)
	}
	if regionErr != nil {
		err = bo.Backoff(BoRegionMiss, errors.New(regionErr.String()))
		if err != nil {
			return errors.Trace(err)
		}
		err = c.pessimisticRollbackMutations(bo, batch.mutations)
		return errors.Trace(err)
	}
	return nil
}

func (c *twoPhaseCommitter) pessimisticLockMutations(bo *Backoffer, lockCtx *kv.LockCtx, mutations CommitterMutations) error {
	return c.doActionOnMutations(bo, actionPessimisticLock{lockCtx}, mutations)
}

func (c *twoPhaseCommitter) pessimisticRollbackMutations(bo *Backoffer, mutations CommitterMutations) error {
	return c.doActionOnMutations(bo, actionPessimisticRollback{}, mutations)
}
