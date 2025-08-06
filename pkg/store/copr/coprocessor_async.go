package copr

import (
	"context"
	"sync/atomic"

	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/util/async"
	"go.uber.org/zap"
)

type asyncCopTask struct {
	tasks    []*copTask
	resps    []*copResponse
	bos      map[uint64]*Backoffer
	fetching bool
}

func (t *asyncCopTask) onResult(result *copTaskResult, err error) {
	t.fetching = false
	if err != nil {
		t.resps = append(t.resps, &copResponse{err: err})
		return
	}
	if result != nil {
		if result.resp != nil {
			t.resps = append(t.resps, result.resp)
		}
		if len(result.batchRespList) > 0 {
			t.resps = append(t.resps, result.batchRespList...)
		}
		if len(result.remains) > 0 {
			t.tasks = append(result.remains, t.tasks[1:]...)
		} else {
			t.tasks = t.tasks[1:]
		}
	}
}

func (t *asyncCopTask) popNextResp() (*copResponse, bool) {
	if len(t.resps) == 0 {
		return nil, false
	}
	resp := t.resps[0]
	t.resps = t.resps[1:]
	return resp, true
}

type asyncCopIterator struct {
	base   *copIterator
	runner *async.RunLoop

	pending    []*asyncCopTask
	processing []*asyncCopTask
}

func newAsyncCopIterator(base *copIterator) *asyncCopIterator {
	it := &asyncCopIterator{
		base:       base,
		runner:     async.NewRunLoop(),
		pending:    make([]*asyncCopTask, 0, len(base.tasks)),
		processing: make([]*asyncCopTask, 0, base.concurrency+base.smallTaskConcurrency),
	}
	for _, task := range base.tasks {
		it.pending = append(it.pending, &asyncCopTask{tasks: []*copTask{task}})
	}
	return it
}

func (it *asyncCopIterator) open(ctx context.Context) {
	it.schedule(ctx)
}

func (it *asyncCopIterator) next(ctx context.Context) *copResponse {
	popNextResp := it.popNextUnordered
	if it.base.req.KeepOrder {
		popNextResp = it.popNextOrdered
	}
	for len(it.processing) > 0 || len(it.pending) > 0 {
		if len(it.processing) == 0 {
			if it.schedule(ctx) {
				return nil
			}
			continue
		}
		if resp, ok := popNextResp(); ok {
			if it.schedule(ctx) {
				return nil
			}
			return resp
		}
		if interrupted, err := it.waitForResps(ctx); interrupted || err != nil {
			return &copResponse{err: err}
		}
		if it.schedule(ctx) {
			return nil
		}
	}
	return nil
}

func (it *asyncCopIterator) popNextOrdered() (*copResponse, bool) {
	return it.processing[0].popNextResp()
}

func (it *asyncCopIterator) popNextUnordered() (*copResponse, bool) {
	for _, t := range it.processing {
		if resp, ok := t.popNextResp(); ok {
			return resp, ok
		}
	}
	return nil, false
}

func (it *asyncCopIterator) update() {
	count := 0
	for _, t := range it.processing {
		if t.fetching || len(t.resps) > 0 || len(t.tasks) > 0 {
			it.processing[count] = t
			count++
		}
	}
	it.processing = it.processing[:count]

	limit := it.base.concurrency + it.base.smallTaskConcurrency
	for len(it.pending) > 0 && count < limit {
		t := it.pending[0]
		t.bos = make(map[uint64]*Backoffer)
		it.processing = append(it.processing, t)
		it.pending[0] = nil
		it.pending = it.pending[1:]
		count++
	}
}

func (it *asyncCopIterator) interrupted() bool {
	if atomic.LoadUint32(&it.base.closed) != 0 {
		return true
	}
	if it.base.vars != nil && it.base.vars.Killed != nil && atomic.LoadUint32(it.base.vars.Killed) != 0 {
		return true
	}
	return false
}

func (it *asyncCopIterator) schedule(ctx context.Context) bool {
	it.update()

	maxBufferedResps := 2
	if it.base.req.Paging.Enable {
		maxBufferedResps = 18
	}
	for _, t := range it.processing {
		if t.fetching || len(t.tasks) == 0 || len(t.resps) >= maxBufferedResps {
			continue
		}
		if it.interrupted() {
			return true
		}
		it.startAsyncTask(ctx, t)
	}
	return false
}

func (it *asyncCopIterator) waitForResps(ctx context.Context) (bool, error) {
	for _, t := range it.processing {
		if t.fetching {
			if it.interrupted() {
				return true, nil
			}
			_, err := it.runner.Exec(ctx)
			return false, err
		}
	}
	return false, nil
}

func (it *asyncCopIterator) startAsyncTask(ctx context.Context, t *asyncCopTask) {
	defer func() {
		if r := recover(); r != nil {
			logutil.Logger(ctx).Error("panic on starting async cop task", zap.Any("r", r), zap.Stack("stack"))
			t.onResult(nil, util.GetRecoverError(r))
		}
	}()

	worker := newCopIteratorWorker(it.base, nil)
	task := t.tasks[0]
	bo := chooseBackoffer(ctx, t.bos, task, it.base.vars)

	in := worker.buildCopInput(bo, task)
	if in.runaway != nil {
		t.onResult(nil, in.runaway)
		return
	}

	t.fetching = true
	onResult := async.NewCallback(it.runner, t.onResult)
	onResponse := async.NewCallback(it.runner, func(resp *tikvrpc.ResponseExt, err error) {
		defer func() {
			if r := recover(); r != nil {
				logutil.Logger(ctx).Error("panic on handling cop response", zap.Any("r", r), zap.Stack("stack"))
				onResult.Invoke(nil, util.GetRecoverError(r))
			}
		}()
		out := copOutput{
			copResp:   resp.Resp.(*coprocessor.Response),
			rpcCtx:    &tikv.RPCContext{Addr: resp.Addr},
			storeAddr: resp.Addr,
			err:       err,
		}
		if isSyncCapable(out.copResp) {
			onResult.Invoke(worker.handleCopOutput(bo, task, in, out))
			return
		}
		it.runner.Go(func() {
			defer func() {
				if r := recover(); r != nil {
					logutil.Logger(ctx).Error("panic on handling cop response", zap.Any("r", r), zap.Stack("stack"))
					onResult.Schedule(nil, util.GetRecoverError(r))
				}
			}()
			onResult.Schedule(worker.handleCopOutput(bo, task, in, out))
		})
	})
	worker.kvclient.SendReqAsync(bo.TiKVBackoffer(), in.req, task.region, in.timeout, onResponse, in.ops...)
}

func isSyncCapable(resp *coprocessor.Response) bool {
	if resp.GetRegionError() != nil {
		return false
	}
	if resp.GetLocked() != nil {
		return false
	}
	batchResps := resp.GetBatchResponses()
	for _, batchResp := range batchResps {
		if batchResp.GetRegionError() != nil {
			return false
		}
		if batchResp.GetLocked() != nil {
			return false
		}
	}
	return true
}
