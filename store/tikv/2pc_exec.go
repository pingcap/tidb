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
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	pb "github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

type execJob struct {
	batch  *batchMutations
	bo     *Backoffer
	cancel context.CancelFunc
}

type execController struct {
	sync.WaitGroup
	committer  *twoPhaseCommitter
	action     twoPhaseCommitAction
	filter     func([]byte) bool
	mutations  mutations
	numRegions int

	bo     *Backoffer
	cancel context.CancelFunc

	errCh       chan error
	jobCh       chan execJob
	workerLimit int
	workerCnt   int32
	stop        int32
}

func (c *twoPhaseCommitter) newExecController(mutations mutations, action twoPhaseCommitAction) *execController {
	return &execController{
		committer: c,
		action:    action,
		mutations: mutations,

		errCh:       make(chan error, 1),
		jobCh:       make(chan execJob),
		workerLimit: config.GetGlobalConfig().Performance.CommitterConcurrency,
	}
}

func (c *execController) run(bo *Backoffer) error {
	failpoint.Inject("pessimisticRollbackDoNth", func() {
		_, actionIsPessimisticRollback := c.action.(actionPessimisticRollback)
		if actionIsPessimisticRollback && c.committer.connID > 0 {
			logutil.Logger(c.bo.ctx).Warn("pessimisticRollbackDoNth failpoint")
			failpoint.Return(nil)
		}
	})

	_, actionIsPrewrite := c.action.(actionPrewrite)
	_, actionIsCommit := c.action.(actionCommit)
	_, actionIsCleanup := c.action.(actionCleanup)
	_, actionIsPessimisticLock := c.action.(actionPessimisticLock)
	if actionIsPessimisticLock {
		defer func() {
			if stats := c.action.(actionPessimisticLock).Stats; stats != nil {
				stats.RegionNum = int32(c.numRegions)
			}
		}()
	}

	var skipPrimary, skipSecondary bool
	failpoint.Inject("skipKeyReturnOK", func(val failpoint.Value) {
		valStr, ok := val.(string)
		if ok && c.committer.connID > 0 {
			if c.containsPrimaryKey() && actionIsPessimisticLock {
				logutil.Logger(c.bo.ctx).Warn("pessimisticLock failpoint", zap.String("valStr", valStr))
				switch valStr {
				case "pessimisticLockSkipPrimary":
					skipPrimary = true
				case "pessimisticLockSkipSecondary":
					skipSecondary = true
				}
			}
		}
	})

	var cancel context.CancelFunc
	c.bo, cancel = bo.Fork()
	if actionIsPrewrite {
		c.cancel = cancel
	}

	if c.containsPrimaryKey() && (actionIsCommit || actionIsCleanup || actionIsPessimisticLock) && !skipPrimary {
		c.numRegions++
		keys, err := c.handlePrimaryMutation(actionIsCommit)
		if err != nil || keys == c.mutations.Len() {
			return errors.Trace(err)
		}
	}

	if skipSecondary {
		return nil
	}

	if actionIsCommit {
		c.bo, cancel = NewBackofferWithVars(context.Background(), int(atomic.LoadUint64(&CommitMaxBackoff)), c.committer.txn.vars).Fork()
		go func() {
			err := c.handleMutations()
			cancel()
			if err != nil {
				logutil.BgLogger().Debug("2PC async doActionOnBatches",
					zap.Uint64("conn", c.committer.connID),
					zap.Stringer("action type", c.action),
					zap.Error(err))
				tikvSecondaryLockCleanupFailureCounterCommit.Inc()
			}
		}()
		return nil
	}

	err := c.handleMutations()
	cancel()
	return errors.Trace(err)
}

func (c *execController) handlePrimaryMutation(actionIsCommit bool) (int, error) {
	var (
		batch *batchMutations
		err   error
	)

	for {
		collector := c.getCollector(c.committer.primaryKey, nil)
		batch, err = collector.Collect()
		if err != nil {
			return 0, errors.Trace(err)
		}

		retry, err := c.action.handleSingleBatch(c.committer, c.bo, batch)
		if err == nil && !retry {
			break
		}
		if !retry {
			return 0, errors.Trace(err)
		}
		if err != nil {
			err = c.bo.Backoff(BoRegionMiss, err)
			if err != nil {
				return 0, errors.Trace(err)
			}
		}
	}

	knobs := &c.committer.testingKnobs
	if actionIsCommit && knobs.bkAfterCommitPrimary != nil && knobs.acAfterCommitPrimary != nil {
		knobs.acAfterCommitPrimary <- struct{}{}
		<-knobs.bkAfterCommitPrimary
	}

	start, end := batch.mutations.keys[0], batch.mutations.keys[batch.mutations.len()-1]
	c.filter = func(key []byte) bool {
		return bytes.Compare(key, start) < 0 || bytes.Compare(key, end) > 0
	}
	return batch.mutations.len(), nil
}

func (c *execController) handleMutations() (err error) {
	collector := c.getCollector(nil, nil)
	var prev RegionVerID
	for {
		var batch *batchMutations
		batch, err = collector.Collect()

		if err != nil {
			c.handleError(err)
			break
		}
		if batch == nil {
			break
		}
		if batch.region != prev {
			prev = batch.region
			c.numRegions++
		}

		if collector.Finished() {
			bo, cancel := c.bo.Fork()
			job := execJob{batch, bo, cancel}
			c.handleSingleBatch(&job)
			break
		}

		if err = c.dispatchBatch(batch); err != nil {
			break
		}
	}
	c.Wait()
	if err == nil {
		select {
		case err = <-c.errCh:
		default:
		}
	}
	close(c.jobCh)

	return errors.Trace(err)
}

func (c *execController) containsPrimaryKey() bool {
	it := c.mutations.Iter(c.committer.primaryKey, nil)
	m := it.Next()
	return bytes.Equal(m.key, c.committer.primaryKey)
}

func (c *execController) getCollector(start, end []byte) *mutationBatchCollector {
	_, actionIsPrewrite := c.action.(actionPrewrite)
	it := c.mutations.Iter(start, end)
	if c.filter != nil {
		it.WithFilter(c.filter)
	}

	return &mutationBatchCollector{
		src:            c.committer.mapWithRegion(c.bo, it),
		limit:          txnCommitBatchSize,
		primaryKey:     c.committer.primaryKey,
		onlyCollectKey: !actionIsPrewrite,
	}
}

func (c *execController) dispatchBatch(batch *batchMutations) error {
	bo, cancel := c.bo.Fork()
	job := execJob{batch, bo, cancel}
	c.Add(1)

	select {
	case c.jobCh <- job:
		return nil
	default:
		c.tryAddWorker()
	}

	if c.cancel == nil {
		c.jobCh <- job
		return nil
	}

	// We can cancel dispatcher by return error.
	select {
	case c.jobCh <- job:
		return nil
	case err := <-c.errCh:
		c.Done()
		return err
	}
}

func (c *execController) workerLoop() {
	for job := range c.jobCh {
		if !c.terminated() {
			c.handleSingleBatch(&job)
		}
		c.Done()
	}
}

func (c *execController) handleSingleBatch(job *execJob) {
	beforeSleep := job.bo.totalSleep
	defer c.updateSingleBatchDetail(job.bo, beforeSleep)

	retry, err := c.action.handleSingleBatch(c.committer, job.bo, job.batch)
	if err == nil && !retry {
		job.cancel()
		return
	}
	if !retry {
		c.handleError(err)
		job.cancel()
		return
	}
	c.handleRegionError(err, job)
}

func (c *execController) handleRegionError(retryErr error, retryJob *execJob) {
	keys := retryJob.batch.mutations.keys
	start := kv.Key(keys[0])
	end := kv.Key(keys[len(keys)-1]).Next()

	for {
		if retryErr != nil {
			err := retryJob.bo.Backoff(BoRegionMiss, retryErr)
			if err != nil {
				c.handleError(err)
				return
			}
		}

		collector := c.getCollector(start, end)
		for {
			batch, err := collector.Collect()
			if err != nil {
				c.handleError(err)
				return
			}

			if batch == nil {
				return
			}

			bo, cancel := retryJob.bo.Fork()
			job := execJob{batch, bo, cancel}

			c.Add(1)
			select {
			case c.jobCh <- job:
				continue
			default:
				c.Done()
			}

			if c.terminated() {
				cancel()
				return
			}

			retry, err := c.action.handleSingleBatch(c.committer, job.bo, job.batch)
			cancel()
			if err != nil && !retry {
				c.handleError(err)
				return
			}
			if retry {
				start = job.batch.mutations.keys[0]
				retryErr = err
				break
			}
		}
	}
}

func (c *execController) tryAddWorker() {
	limit := int32(c.workerLimit)
	for {
		cnt := atomic.LoadInt32(&c.workerCnt)
		if cnt >= limit {
			return
		}
		if atomic.CompareAndSwapInt32(&c.workerCnt, cnt, cnt+1) {
			go c.workerLoop()
			return
		}
	}
}

func (c *execController) handleError(err error) {
	logutil.BgLogger().Debug("2PC doActionOnBatch failed",
		zap.Uint64("conn", c.committer.connID),
		zap.Stringer("action type", c.action),
		zap.Error(err),
		zap.Uint64("txnStartTS", c.committer.startTS))

	if c.cancel != nil {
		old := atomic.SwapInt32(&c.stop, 1)
		if old == 0 {
			logutil.BgLogger().Debug("2PC doActionOnBatch to cancel other actions",
				zap.Uint64("conn", c.committer.connID),
				zap.Stringer("action type", c.action),
				zap.Uint64("txnStartTS", c.committer.startTS))
			c.cancel()
		}
	}

	select {
	case c.errCh <- err:
	default:
	}
}

func (c *execController) terminated() bool {
	return atomic.LoadInt32(&c.stop) == 1
}

func (c *execController) updateSingleBatchDetail(bo *Backoffer, beforeSleep int) {
	detail := c.committer.getDetail()
	if detail == nil {
		// lock operations of pessimistic-txn will let commitDetail be nil
		return
	}
	delta := bo.totalSleep - beforeSleep
	if delta <= 0 {
		return
	}
	atomic.AddInt64(&detail.CommitBackoffTime, int64(delta)*int64(time.Millisecond))
	detail.Mu.Lock()
	detail.Mu.BackoffTypes = append(detail.Mu.BackoffTypes, bo.types...)
	detail.Mu.Unlock()
}

type mutation struct {
	key               []byte
	value             []byte
	isPessimisticLock bool
	op                pb.Op
}

type mutations interface {
	Iter(start, end []byte) mutationsIter
	Len() int
}

type mutationsIter interface {
	Next() mutation
	WithFilter(func([]byte) bool)
}

type mutationWithRegion struct {
	mutation
	region RegionVerID
}

type mutationWithRegionIter struct {
	src mutationsIter

	rc  *RegionCache
	loc *KeyLocation
	bo  *Backoffer
}

func (c *twoPhaseCommitter) mapWithRegion(bo *Backoffer, src mutationsIter) *mutationWithRegionIter {
	return &mutationWithRegionIter{
		src: src,
		rc:  c.store.regionCache,
		bo:  bo,
	}
}

func (it *mutationWithRegionIter) Next() (mutationWithRegion, error) {
	m := it.src.Next()
	if m.key == nil {
		return mutationWithRegion{}, nil
	}

	var err error
	if it.loc == nil || !it.loc.Contains(m.key) {
		it.loc, err = it.rc.LocateKey(it.bo, m.key)
		if err != nil {
			return mutationWithRegion{}, errors.Trace(err)
		}
	}

	return mutationWithRegion{m, it.loc.Region}, nil
}

type mutationBatchCollector struct {
	src            *mutationWithRegionIter
	primaryKey     []byte
	limit          int
	init           bool
	done           bool
	onlyCollectKey bool

	curr    mutationWithRegion
	lenHint int
}

func (c *mutationBatchCollector) Collect() (*batchMutations, error) {
	if !c.init {
		m, err := c.src.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
		c.curr = m
		c.init = true
	}
	if c.done {
		return nil, nil
	}

	var (
		mutations = NewCommiterMutations(c.lenHint)
		region    = c.curr.region
		m         = c.curr
		isPrimary bool
		size      int
		err       error
	)

	for {
		if m.key == nil {
			c.done = true
			break
		}

		if c.onlyCollectKey {
			mutations.keys = append(mutations.keys, m.key)
			size += len(m.key)
		} else {
			mutations.Push(m.op, m.key, m.value, m.isPessimisticLock)
			size += len(m.key) + len(m.value)
		}

		if !isPrimary {
			isPrimary = bytes.Equal(m.key, c.primaryKey)
		}

		m, err = c.src.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}

		if size >= c.limit || m.region.id != region.id {
			c.curr = m
			break
		}
	}

	var ret *batchMutations
	if mutations.len() != 0 {
		c.lenHint = mutations.len()
		ret = &batchMutations{
			mutations: mutations,
			region:    region,
			isPrimary: isPrimary,
		}
	}

	return ret, nil
}

func (c *mutationBatchCollector) Finished() bool {
	return c.curr.key == nil
}
