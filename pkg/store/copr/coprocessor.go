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
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/tidb/pkg/kv"
	tidbmetrics "github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/store/driver/backoff"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc/interceptor"
	"github.com/tikv/client-go/v2/util"
)

// Maximum total sleep time(in ms) for kv/cop commands.
const (
	copBuildTaskMaxBackoff = 5000
	CopNextMaxBackoff      = 20000
	CopSmallTaskRow        = 32 // 32 is the initial batch size of TiKV
	smallTaskSigma         = 0.5
	smallConcPerCore       = 20
)

var liteWorkerFallbackHook atomic.Pointer[func()]

// CopClient is coprocessor client.
type CopClient struct {
	kv.RequestTypeSupportedChecker
	store           *Store
	replicaReadSeed uint32
}

// Send builds the request and gets the coprocessor iterator response.
func (c *CopClient) Send(ctx context.Context, req *kv.Request, variables any, option *kv.ClientSendOption) kv.Response {
	vars, ok := variables.(*tikv.Variables)
	if !ok {
		return copErrorResponse{errors.Errorf("unsupported variables:%+v", variables)}
	}
	if req.StoreType == kv.TiFlash && req.BatchCop {
		logutil.BgLogger().Debug("send batch requests")
		return c.sendBatch(ctx, req, vars, option)
	}
	ctx = context.WithValue(ctx, tikv.TxnStartKey(), req.StartTs)
	ctx = context.WithValue(ctx, util.RequestSourceKey, req.RequestSource)
	ctx = interceptor.WithRPCInterceptor(ctx, interceptor.GetRPCInterceptorFromCtx(ctx))
	it, errRes := c.BuildCopIterator(ctx, req, vars, option)
	if errRes != nil {
		return errRes
	}
	ctx = context.WithValue(ctx, tikv.RPCCancellerCtxKey{}, it.rpcCancel)
	if ctx.Value(util.RUDetailsCtxKey) == nil {
		ctx = context.WithValue(ctx, util.RUDetailsCtxKey, util.NewRUDetails())
	}
	it.open(ctx, option.TryCopLiteWorker)
	return it
}

// BuildCopIterator builds the iterator without calling `open`.
func (c *CopClient) BuildCopIterator(ctx context.Context, req *kv.Request, vars *tikv.Variables, option *kv.ClientSendOption) (*copIterator, kv.Response) {
	eventCb := option.EventCb
	failpoint.Inject("DisablePaging", func(_ failpoint.Value) {
		req.Paging.Enable = false
	})
	if req.StoreType == kv.TiDB {
		// coprocessor on TiDB doesn't support paging
		req.Paging.Enable = false
	}
	if req.Tp != kv.ReqTypeDAG {
		// coprocessor request but type is not DAG
		req.Paging.Enable = false
	}
	failpoint.Inject("checkKeyRangeSortedForPaging", func(_ failpoint.Value) {
		if req.Paging.Enable {
			if !req.KeyRanges.IsFullySorted() {
				logutil.BgLogger().Fatal("distsql request key range not sorted!")
			}
		}
	})
	if !checkStoreBatchCopr(req) {
		req.StoreBatchSize = 0
	}

	boCtx := ctx
	if req.MaxExecutionTime > 0 {
		// If the request has a MaxExecutionTime, we need to set the deadline of the context.
		var cancel context.CancelFunc
		boCtx, cancel = context.WithTimeout(boCtx, time.Duration(req.MaxExecutionTime)*time.Millisecond)
		defer func() {
			cancel()
		}()
	}
	bo := backoff.NewBackofferWithVars(boCtx, copBuildTaskMaxBackoff, vars)
	var (
		tasks []*copTask
		err   error
	)
	tryRowHint := optRowHint(req)
	elapsed := time.Duration(0)
	buildOpt := &buildCopTaskOpt{
		req:      req,
		cache:    c.store.GetRegionCache(),
		eventCb:  eventCb,
		respChan: req.KeepOrder,
		elapsed:  &elapsed,
	}
	buildTaskFunc := func(ranges []kv.KeyRange, hints []int) error {
		keyRanges := NewKeyRanges(ranges)
		if tryRowHint {
			buildOpt.rowHints = hints
		}
		tasksFromRanges, err := buildCopTasks(bo, keyRanges, buildOpt)
		if err != nil {
			return err
		}
		if len(tasks) == 0 {
			tasks = tasksFromRanges
			return nil
		}
		tasks = append(tasks, tasksFromRanges...)
		return nil
	}
	// Here we build the task by partition, not directly by region.
	// This is because it's possible that TiDB merge multiple small partition into one region which break some assumption.
	// Keep it split by partition would be more safe.
	err = req.KeyRanges.ForEachPartitionWithErr(buildTaskFunc)
	// only batch store requests in first build.
	req.StoreBatchSize = 0
	reqType := "null"
	if req.ClosestReplicaReadAdjuster != nil {
		reqType = "miss"
		if req.ClosestReplicaReadAdjuster(req, len(tasks)) {
			reqType = "hit"
		}
	}
	tidbmetrics.DistSQLCoprClosestReadCounter.WithLabelValues(reqType).Inc()
	if err != nil {
		return nil, copErrorResponse{err}
	}
	it := &copIterator{
		store:            c.store,
		req:              req,
		concurrency:      req.Concurrency,
		finishCh:         make(chan struct{}),
		vars:             vars,
		memTracker:       req.MemTracker,
		replicaReadSeed:  c.replicaReadSeed,
		rpcCancel:        tikv.NewRPCanceller(),
		buildTaskElapsed: *buildOpt.elapsed,
		runawayChecker:   req.RunawayChecker,
	}
	// Pipelined-dml can flush locks when it is still reading.
	// The coprocessor of the txn should not be blocked by itself.
	// It should be the only case where a coprocessor can read locks of the same ts.
	//
	// But when start_ts is not obtained from PD,
	// the start_ts could conflict with another pipelined-txn's start_ts.
	// in which case the locks of same ts cannot be ignored.
	// We rely on the assumption: start_ts is not from PD => this is a stale read.
	if !req.IsStaleness {
		it.resolvedLocks.Put(req.StartTs)
	}
	it.tasks = tasks
	if it.concurrency > len(tasks) {
		it.concurrency = len(tasks)
	}
	if tryRowHint {
		var smallTasks int
		smallTasks, it.smallTaskConcurrency = smallTaskConcurrency(tasks, c.store.numcpu)
		if len(tasks)-smallTasks < it.concurrency {
			it.concurrency = len(tasks) - smallTasks
		}
	}
	if it.concurrency < 1 {
		// Make sure that there is at least one worker.
		it.concurrency = 1
	}

	// issue56916 is about the cooldown of the runaway checker may block the SQL execution.
	failpoint.Inject("issue56916", func(_ failpoint.Value) {
		it.concurrency = 1
		it.smallTaskConcurrency = 0
	})

	// if the request is triggered cool down by the runaway checker, we need to adjust the concurrency, let the sql run slowly.
	if req.RunawayChecker != nil && req.RunawayChecker.CheckAction() == rmpb.RunawayAction_CoolDown {
		it.concurrency = 1
		it.smallTaskConcurrency = 0
	}

	if it.req.KeepOrder {
		if it.smallTaskConcurrency > 20 {
			it.smallTaskConcurrency = 20
		}
		it.sendRate = util.NewRateLimit(2 * (it.concurrency + it.smallTaskConcurrency))
		it.respChan = nil
	} else {
		it.respChan = make(chan *copResponse)
		it.sendRate = util.NewRateLimit(it.concurrency + it.smallTaskConcurrency)
	}
	if option.EnableCollectExecutionInfo {
		it.stats = &copIteratorRuntimeStats{}
	}
	it.actionOnExceed = newRateLimitAction(uint(it.sendRate.GetCapacity()))
	if option.SessionMemTracker != nil && option.EnabledRateLimitAction {
		option.SessionMemTracker.FallbackOldAndSetNewAction(it.actionOnExceed)
	}
	it.actionOnExceed.setEnabled(option.EnabledRateLimitAction)
	return it, nil
}

