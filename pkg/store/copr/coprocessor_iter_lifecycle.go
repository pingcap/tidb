// Copyright 2016 PingCAP, Inc.
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

package copr

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/resourcegroup"
	"github.com/pingcap/tidb/pkg/store/driver/backoff"
	derr "github.com/pingcap/tidb/pkg/store/driver/error"
	util2 "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv/txnsnapshot"
	"github.com/tikv/client-go/v2/util"
	atomic2 "go.uber.org/atomic"
	"go.uber.org/zap"
)

// SetLiteWorkerFallbackHookForTest installs a hook invoked when the lite worker falls back to the concurrent worker.
// Production code should not rely on this hook.
func SetLiteWorkerFallbackHookForTest(hook func()) {
	if hook != nil {
		liteWorkerFallbackHook.Store(&hook)
		return
	}
	liteWorkerFallbackHook.Store(nil)
}

func triggerLiteWorkerFallbackHook() {
	if hookPtr := liteWorkerFallbackHook.Load(); hookPtr != nil && *hookPtr != nil {
		(*hookPtr)()
	}
}

// run is a worker function that get a copTask from channel, handle it and
// send the result back.
func (worker *copIteratorWorker) run(ctx context.Context) {
	defer func() {
		failpoint.Inject("ticase-4169", func(val failpoint.Value) {
			if val.(bool) {
				worker.memTracker.Consume(10 * MockResponseSizeForTest)
				worker.memTracker.Consume(10 * MockResponseSizeForTest)
			}
		})
		worker.wg.Done()
	}()
	// 16KB ballast helps grow the stack to the requirement of copIteratorWorker.
	// This reduces the `morestack` call during the execution of `handleTask`, thus improvement the efficiency of TiDB.
	// TODO: remove ballast after global pool is applied.
	ballast := make([]byte, 16*size.KB)
	for task := range worker.taskCh {
		respCh := worker.respChan
		if respCh == nil {
			respCh = task.respChan
		}
		worker.handleTask(ctx, task, respCh)
		if worker.respChan != nil {
			// When a task is finished by the worker, send a finCopResp into channel to notify the copIterator that
			// there is a task finished.
			worker.sendToRespCh(finCopResp, worker.respChan)
		}
		if task.respChan != nil {
			close(task.respChan)
		}
		if worker.finished() {
			return
		}
	}
	runtime.KeepAlive(ballast)
}

// open starts workers and sender goroutines.
func (it *copIterator) open(ctx context.Context, tryCopLiteWorker *atomic2.Uint32) {
	if len(it.tasks) == 1 && tryCopLiteWorker != nil && tryCopLiteWorker.CompareAndSwap(0, 1) {
		// For a query, only one `copIterator` can use `liteWorker`, otherwise it will affect the performance of multiple cop iterators executed concurrently,
		// see more detail in TestQueryWithConcurrentSmallCop.
		it.liteWorker = &liteCopIteratorWorker{
			ctx:              ctx,
			worker:           newCopIteratorWorker(it, nil),
			tryCopLiteWorker: tryCopLiteWorker,
		}
		return
	}
	taskCh := make(chan *copTask, 1)
	it.wg.Add(it.concurrency + it.smallTaskConcurrency)
	var smallTaskCh chan *copTask
	if it.smallTaskConcurrency > 0 {
		smallTaskCh = make(chan *copTask, 1)
	}
	// Start it.concurrency number of workers to handle cop requests.
	for i := range it.concurrency + it.smallTaskConcurrency {
		ch := taskCh
		if i >= it.concurrency && smallTaskCh != nil {
			ch = smallTaskCh
		}
		worker := newCopIteratorWorker(it, ch)
		go worker.run(ctx)
	}
	taskSender := &copIteratorTaskSender{
		taskCh:      taskCh,
		smallTaskCh: smallTaskCh,
		wg:          &it.wg,
		tasks:       it.tasks,
		finishCh:    it.finishCh,
		sendRate:    it.sendRate,
	}
	taskSender.respChan = it.respChan
	failpoint.Inject("ticase-4171", func(val failpoint.Value) {
		if val.(bool) {
			it.memTracker.Consume(10 * MockResponseSizeForTest)
			it.memTracker.Consume(10 * MockResponseSizeForTest)
		}
	})
	go taskSender.run(it.req.ConnID, it.req.RunawayChecker)
}

func newCopIteratorWorker(it *copIterator, taskCh <-chan *copTask) *copIteratorWorker {
	return &copIteratorWorker{
		taskCh:                  taskCh,
		wg:                      &it.wg,
		store:                   it.store,
		req:                     it.req,
		respChan:                it.respChan,
		finishCh:                it.finishCh,
		vars:                    it.vars,
		kvclient:                txnsnapshot.NewClientHelper(it.store.store, &it.resolvedLocks, &it.committedLocks, false),
		memTracker:              it.memTracker,
		replicaReadSeed:         it.replicaReadSeed,
		pagingTaskIdx:           &it.pagingTaskIdx,
		storeBatchedNum:         &it.storeBatchedNum,
		storeBatchedFallbackNum: &it.storeBatchedFallbackNum,
		stats:                   it.stats,
	}
}

func (sender *copIteratorTaskSender) run(connID uint64, checker resourcegroup.RunawayChecker) {
	// Send tasks to feed the worker goroutines.
	for _, t := range sender.tasks {
		// we control the sending rate to prevent all tasks
		// being done (aka. all of the responses are buffered) by copIteratorWorker.
		// We keep the number of inflight tasks within the number of 2 * concurrency when Keep Order is true.
		// If KeepOrder is false, the number equals the concurrency.
		// It sends one more task if a task has been finished in copIterator.Next.
		exit := sender.sendRate.GetToken(sender.finishCh)
		if exit {
			break
		}
		var sendTo chan<- *copTask
		if isSmallTask(t) && sender.smallTaskCh != nil {
			sendTo = sender.smallTaskCh
		} else {
			sendTo = sender.taskCh
		}
		exit = sender.sendToTaskCh(t, sendTo)
		if exit {
			break
		}
		if connID > 0 {
			failpoint.Inject("pauseCopIterTaskSender", func() {})
		}
	}
	close(sender.taskCh)
	if sender.smallTaskCh != nil {
		close(sender.smallTaskCh)
	}

	// Wait for worker goroutines to exit.
	sender.wg.Wait()
	if sender.respChan != nil {
		close(sender.respChan)
	}
	if checker != nil {
		// runaway checker need to focus on the all processed keys of all tasks at a time.
		checker.ResetTotalProcessedKeys()
	}
}

func (it *copIterator) recvFromRespCh(ctx context.Context, respCh <-chan *copResponse) (resp *copResponse, ok bool, exit bool) {
	for {
		select {
		case resp, ok = <-respCh:
			memTrackerConsumeResp(it.memTracker, resp)
			return
		case <-it.finishCh:
			exit = true
			return
		case <-ctx.Done():
			// We select the ctx.Done() in the thread of `Next` instead of in the worker to avoid the cost of `WithCancel`.
			if atomic.CompareAndSwapUint32(&it.closed, 0, 1) {
				close(it.finishCh)
			}
			exit = true
			return
		}
	}
}

func memTrackerConsumeResp(memTracker *memory.Tracker, resp *copResponse) {
	if memTracker != nil && resp != nil {
		consumed := resp.MemSize()
		failpoint.Inject("testRateLimitActionMockConsumeAndAssert", func(val failpoint.Value) {
			if val.(bool) {
				if resp != finCopResp {
					consumed = MockResponseSizeForTest
				}
			}
		})
		memTracker.Consume(-consumed)
	}
}

// GetConcurrency returns the concurrency and small task concurrency.
func (it *copIterator) GetConcurrency() (int, int) {
	return it.concurrency, it.smallTaskConcurrency
}

// GetStoreBatchInfo returns the batched and fallback num.
func (it *copIterator) GetStoreBatchInfo() (uint64, uint64) {
	return it.storeBatchedNum.Load(), it.storeBatchedFallbackNum.Load()
}

// GetBuildTaskElapsed returns the duration of building task.
func (it *copIterator) GetBuildTaskElapsed() time.Duration {
	return it.buildTaskElapsed
}

// GetSendRate returns the rate-limit object.
func (it *copIterator) GetSendRate() *util.RateLimit {
	return it.sendRate
}

// GetTasks returns the built tasks.
func (it *copIterator) GetTasks() []*copTask {
	return it.tasks
}

func (sender *copIteratorTaskSender) sendToTaskCh(t *copTask, sendTo chan<- *copTask) (exit bool) {
	select {
	case sendTo <- t:
	case <-sender.finishCh:
		exit = true
	}
	return
}

func (worker *copIteratorWorker) sendToRespCh(resp *copResponse, respCh chan<- *copResponse) (exit bool) {
	select {
	case respCh <- resp:
	case <-worker.finishCh:
		exit = true
	}
	return
}

func (worker *copIteratorWorker) checkRespOOM(resp *copResponse) {
	if worker.memTracker != nil {
		consumed := resp.MemSize()
		failpoint.Inject("testRateLimitActionMockConsumeAndAssert", func(val failpoint.Value) {
			if val.(bool) {
				if resp != finCopResp {
					consumed = MockResponseSizeForTest
				}
			}
		})
		failpoint.Inject("ConsumeRandomPanic", nil)
		worker.memTracker.Consume(consumed)
	}
}

// MockResponseSizeForTest mock the response size
const MockResponseSizeForTest = 100 * 1024 * 1024

// Next returns next coprocessor result.
// NOTE: Use nil to indicate finish, so if the returned ResultSubset is not nil, reader should continue to call Next().
func (it *copIterator) Next(ctx context.Context) (kv.ResultSubset, error) {
	var (
		resp   *copResponse
		ok     bool
		closed bool
	)
	defer func() {
		if resp == nil {
			failpoint.Inject("ticase-4170", func(val failpoint.Value) {
				if val.(bool) {
					it.memTracker.Consume(10 * MockResponseSizeForTest)
					it.memTracker.Consume(10 * MockResponseSizeForTest)
				}
			})
		}
	}()
	// wait unit at least 5 copResponse received.
	failpoint.Inject("testRateLimitActionMockWaitMax", func(val failpoint.Value) {
		if val.(bool) {
			// we only need to trigger oom at least once.
			if len(it.tasks) > 9 {
				for it.memTracker.MaxConsumed() < 5*MockResponseSizeForTest {
					time.Sleep(10 * time.Millisecond)
				}
			}
		}
	})
	// If data order matters, response should be returned in the same order as copTask slice.
	// Otherwise all responses are returned from a single channel.

	failpoint.InjectCall("CtxCancelBeforeReceive", ctx)
	if it.liteWorker != nil {
		resp = it.liteWorker.liteSendReq(ctx, it)
		// after lite handle 1 task, reset tryCopLiteWorker to 0 to make future request can reuse copLiteWorker.
		it.liteWorker.tryCopLiteWorker.CompareAndSwap(1, 0)
		if resp == nil {
			it.actionOnExceed.close()
			return nil, nil
		}
		if len(it.tasks) > 0 && len(it.liteWorker.batchCopRespList) == 0 && resp.err == nil {
			// if there are remain tasks to be processed, we need to run worker concurrently to avoid blocking.
			// see more detail in https://github.com/pingcap/tidb/issues/58658 and TestDMLWithLiteCopWorker.
			it.liteWorker.runWorkerConcurrently(it)
			it.liteWorker = nil
		}
		it.actionOnExceed.destroyTokenIfNeeded(func() {})
		memTrackerConsumeResp(it.memTracker, resp)
	} else if it.respChan != nil {
		// Get next fetched resp from chan
		resp, ok, closed = it.recvFromRespCh(ctx, it.respChan)
		if !ok || closed {
			it.actionOnExceed.close()
			return nil, errors.Trace(ctx.Err())
		}
		if resp == finCopResp {
			it.actionOnExceed.destroyTokenIfNeeded(func() {
				it.sendRate.PutToken()
			})
			return it.Next(ctx)
		}
	} else {
		for {
			if it.curr >= len(it.tasks) {
				// Resp will be nil if iterator is finishCh.
				it.actionOnExceed.close()
				return nil, nil
			}
			task := it.tasks[it.curr]
			resp, ok, closed = it.recvFromRespCh(ctx, task.respChan)
			if closed {
				// Close() is called or context cancelled/timeout, so Next() is invalid.
				return nil, errors.Trace(ctx.Err())
			}
			if ok {
				break
			}
			it.actionOnExceed.destroyTokenIfNeeded(func() {
				it.sendRate.PutToken()
			})
			// Switch to next task.
			it.tasks[it.curr] = nil
			it.curr++
		}
	}

	if resp.err != nil {
		return nil, errors.Trace(resp.err)
	}

	err := it.store.CheckVisibility(it.req.StartTs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return resp, nil
}

func (w *liteCopIteratorWorker) liteSendReq(ctx context.Context, it *copIterator) (resp *copResponse) {
	defer func() {
		r := recover()
		if r != nil {
			logutil.Logger(ctx).Warn("copIteratorWork meet panic",
				zap.Any("r", r),
				zap.Stack("stack trace"))
			resp = &copResponse{err: util2.GetRecoverError(r)}
		}
	}()

	worker := w.worker
	if len(w.batchCopRespList) > 0 {
		resp = w.batchCopRespList[0]
		w.batchCopRespList = w.batchCopRespList[1:]
		return resp
	}
	backoffermap := make(map[uint64]*Backoffer)
	cancelFuncs := make([]context.CancelFunc, 0)
	defer func() {
		for _, cancel := range cancelFuncs {
			cancel()
		}
	}()
	for len(it.tasks) > 0 {
		curTask := it.tasks[0]
		bo, cancel := chooseBackoffer(w.ctx, backoffermap, curTask, worker)
		if cancel != nil {
			cancelFuncs = append(cancelFuncs, cancel)
		}
		result, err := worker.handleTaskOnce(bo, curTask)
		if err != nil {
			resp = &copResponse{err: errors.Trace(err)}
			worker.checkRespOOM(resp)
			return resp
		}

		if result != nil && len(result.remains) > 0 {
			it.tasks = append(result.remains, it.tasks[1:]...)
		} else {
			it.tasks = it.tasks[1:]
		}
		if result != nil {
			if result.resp != nil {
				w.batchCopRespList = result.batchRespList
				return result.resp
			}
			if len(result.batchRespList) > 0 {
				resp = result.batchRespList[0]
				w.batchCopRespList = result.batchRespList[1:]
				return resp
			}
		}
	}
	return nil
}

func (w *liteCopIteratorWorker) runWorkerConcurrently(it *copIterator) {
	triggerLiteWorkerFallbackHook()
	taskCh := make(chan *copTask, 1)
	worker := w.worker
	worker.taskCh = taskCh
	it.wg.Add(1)
	go worker.run(w.ctx)

	if it.respChan == nil {
		// If it.respChan is nil, we will read the response from task.respChan,
		// but task.respChan maybe nil when rebuilding cop task, so we need to create respChan for the task.
		for i := range it.tasks {
			if it.tasks[i].respChan == nil {
				it.tasks[i].respChan = make(chan *copResponse, 2)
			}
		}
	}

	taskSender := &copIteratorTaskSender{
		taskCh:   taskCh,
		wg:       &it.wg,
		tasks:    it.tasks,
		finishCh: it.finishCh,
		sendRate: it.sendRate,
		respChan: it.respChan,
	}
	go taskSender.run(it.req.ConnID, it.req.RunawayChecker)
}

// HasUnconsumedCopRuntimeStats indicate whether has unconsumed CopRuntimeStats.
type HasUnconsumedCopRuntimeStats interface {
	// CollectUnconsumedCopRuntimeStats returns unconsumed CopRuntimeStats.
	CollectUnconsumedCopRuntimeStats() []*CopRuntimeStats
}

func (it *copIterator) CollectUnconsumedCopRuntimeStats() []*CopRuntimeStats {
	if it == nil || it.stats == nil {
		return nil
	}
	it.stats.Lock()
	stats := make([]*CopRuntimeStats, 0, len(it.stats.stats))
	stats = append(stats, it.stats.stats...)
	it.stats.Unlock()
	return stats
}

// Associate each region with an independent backoffer. In this way, when multiple regions are
// unavailable, TiDB can execute very quickly without blocking, if the returned CancelFunc is not nil,
// the caller must call it to avoid context leak.
func chooseBackoffer(ctx context.Context, backoffermap map[uint64]*Backoffer, task *copTask, worker *copIteratorWorker) (*Backoffer, context.CancelFunc) {
	bo, ok := backoffermap[task.region.GetID()]
	if ok {
		return bo, nil
	}
	boMaxSleep := CopNextMaxBackoff
	failpoint.Inject("ReduceCopNextMaxBackoff", func(value failpoint.Value) {
		if value.(bool) {
			boMaxSleep = 2
		}
	})
	var cancel context.CancelFunc
	boCtx := ctx
	if worker.req.MaxExecutionTime > 0 {
		boCtx, cancel = context.WithTimeout(boCtx, time.Duration(worker.req.MaxExecutionTime)*time.Millisecond)
	}
	newbo := backoff.NewBackofferWithVars(boCtx, boMaxSleep, worker.vars)
	backoffermap[task.region.GetID()] = newbo
	return newbo, cancel
}



// CopRuntimeStats contains execution detail information.
type CopRuntimeStats struct {
	execdetails.CopExecDetails
	ReqStats *tikv.RegionRequestRuntimeStats

	CoprCacheHit bool
}

type copIteratorRuntimeStats struct {
	sync.Mutex
	stats []*CopRuntimeStats
}

func (worker *copIteratorWorker) handleTiDBSendReqErr(err error, task *copTask) (*copTaskResult, error) {
	errCode := errno.ErrUnknown
	errMsg := err.Error()
	if terror.ErrorEqual(err, derr.ErrTiKVServerTimeout) {
		errCode = errno.ErrTiKVServerTimeout
		errMsg = "TiDB server timeout, address is " + task.storeAddr
	}
	if terror.ErrorEqual(err, derr.ErrTiFlashServerTimeout) {
		errCode = errno.ErrTiFlashServerTimeout
		errMsg = "TiDB server timeout, address is " + task.storeAddr
	}
	selResp := tipb.SelectResponse{
		Warnings: []*tipb.Error{
			{
				Code: int32(errCode),
				Msg:  errMsg,
			},
		},
	}
	data, err := proto.Marshal(&selResp)
	if err != nil {
		return nil, errors.Trace(err)
	}
	resp := &copResponse{
		pbResp: &coprocessor.Response{
			Data: data,
		},
		detail: &CopRuntimeStats{},
	}
	worker.checkRespOOM(resp)
	return &copTaskResult{resp: resp}, nil
}

// calculateRetry splits the input ranges into two, and take one of them according to desc flag.
// It's used in paging API, to calculate which range is consumed and what needs to be retry.
// For example:
// ranges: [r1 --> r2) [r3 --> r4)
// split:      [s1   -->   s2)
// In normal scan order, all data before s1 is consumed, so the retry ranges should be [s1 --> r2) [r3 --> r4)
// In reverse scan order, all data after s2 is consumed, so the retry ranges should be [r1 --> r2) [r3 --> s2)
func (worker *copIteratorWorker) calculateRetry(ranges *KeyRanges, split *coprocessor.KeyRange, desc bool) *KeyRanges {
	if split == nil {
		return ranges
	}
	if desc {
		left, _ := ranges.Split(split.End)
		return left
	}
	_, right := ranges.Split(split.Start)
	return right
}

// calculateRemain calculates the remain ranges to be processed, it's used in paging API.
// For example:
// ranges: [r1 --> r2) [r3 --> r4)
// split:      [s1   -->   s2)
// In normal scan order, all data before s2 is consumed, so the remained ranges should be [s2 --> r4)
// In reverse scan order, all data after s1 is consumed, so the remained ranges should be [r1 --> s1)
func (worker *copIteratorWorker) calculateRemain(ranges *KeyRanges, split *coprocessor.KeyRange, desc bool) *KeyRanges {
	if split == nil {
		return ranges
	}
	if desc {
		left, _ := ranges.Split(split.Start)
		return left
	}
	_, right := ranges.Split(split.End)
	return right
}

// finished checks the flags and finished channel, it tells whether the worker is finished.
func (worker *copIteratorWorker) finished() bool {
	if worker.vars != nil && worker.vars.Killed != nil {
		killed := atomic.LoadUint32(worker.vars.Killed)
		if killed != 0 {
			logutil.BgLogger().Info(
				"a killed signal is received in copIteratorWorker",
				zap.Uint32("signal", killed),
			)
			return true
		}
	}
	select {
	case <-worker.finishCh:
		return true
	default:
		return false
	}
}

func (it *copIterator) Close() error {
	if atomic.CompareAndSwapUint32(&it.closed, 0, 1) {
		close(it.finishCh)
	}
	it.rpcCancel.CancelAll()
	it.actionOnExceed.close()
	it.wg.Wait()
	return nil
}
