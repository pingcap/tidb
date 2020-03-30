package tikv

import (
	"context"
	"github.com/cznic/mathutil"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/memory"
	"go.uber.org/zap"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

type batchCopTask struct {
	respChan  chan *batchCopResponse
	storeAddr string
	cmdType   tikvrpc.CmdType

	regionTaskMap map[uint64]*copTask
	copTasks      []copTaskAndRPCContext
}

type batchCopResponse struct {
	pbResp   *coprocessor.BatchResponse
	detail   *execdetails.ExecDetails
	startKey kv.Key
	err      error
	respSize int64
	respTime time.Duration
}

// GetData implements the kv.ResultSubset GetData interface.
func (rs *batchCopResponse) GetData() []byte {
	return rs.pbResp.Data
}

// GetStartKey implements the kv.ResultSubset GetStartKey interface.
func (rs *batchCopResponse) GetStartKey() kv.Key {
	return rs.startKey
}

func (rs *batchCopResponse) GetExecDetails() *execdetails.ExecDetails {
	return &execdetails.ExecDetails{}
}

// MemSize returns how many bytes of memory this response use
func (rs *batchCopResponse) MemSize() int64 {
	if rs.respSize != 0 {
		return rs.respSize
	}

	// ignore rs.err
	rs.respSize += int64(cap(rs.startKey))
	if rs.detail != nil {
		rs.respSize += int64(sizeofExecDetails)
		if rs.detail.CommitDetail != nil {
			rs.respSize += int64(sizeofCommitDetails)
		}
	}
	if rs.pbResp != nil {
		// Using a approximate size since it's hard to get a accurate value.
		rs.respSize += int64(rs.pbResp.Size())
	}
	return rs.respSize
}

func (rs *batchCopResponse) RespTime() time.Duration {
	return rs.respTime
}

type copTaskAndRPCContext struct {
	task *copTask
	ctx  *RPCContext
}

func getFlashRPCContextWithRetry(bo *Backoffer, task *copTask, cache *RegionCache) (*RPCContext, error) {
	for {
		ctx, err := cache.GetTiFlashRPCContext(bo, task.region)
		if err != nil {
			return nil, err
		}
		if ctx != nil {
			return ctx, nil
		}

		if err = bo.Backoff(BoRegionMiss, errors.New("failed to get flash rpc context")); err != nil {
			return nil, errors.Trace(err)
		}
	}
}

func buildBatchCopTasks(bo *Backoffer, cache *RegionCache, ranges *copRanges, req *kv.Request) ([]*batchCopTask, error) {
	if req.StoreType != kv.TiFlash {
		return nil, errors.New("store type must be tiflash !")
	}

	start := time.Now()
	cmdType := tikvrpc.CmdBatchCop
	rangesLen := ranges.len()
	var tasks []*copTask
	appendTask := func(regionWithRangeInfo *KeyLocation, ranges *copRanges) {
		// TiKV will return gRPC error if the message is too large. So we need to limit the length of the ranges slice
		// to make sure the message can be sent successfully.
		rLen := ranges.len()
		for i := 0; i < rLen; {
			nextI := mathutil.Min(i+rangesPerTask, rLen)
			tasks = append(tasks, &copTask{
				region: regionWithRangeInfo.Region,
				ranges: ranges.slice(i, nextI),
				// Channel buffer is 2 for handling region split.
				// In a common case, two region split tasks will not be blocked.
				respChan:  make(chan *copResponse),
				cmdType:   cmdType,
				storeType: req.StoreType,
			})
			i = nextI
		}
	}

	err := splitRanges(bo, cache, ranges, appendTask)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var batchTasks []*batchCopTask

	storeTaskMap := make(map[string]*batchCopTask)

	for _, task := range tasks {
		rpcCtx, err := getFlashRPCContextWithRetry(bo, task, cache)
		if err != nil {
			return nil, err
		}
		if batchCop, ok := storeTaskMap[rpcCtx.Addr]; ok {
			batchCop.copTasks = append(batchCop.copTasks, copTaskAndRPCContext{task: task, ctx: rpcCtx})
			batchCop.regionTaskMap[task.region.id] = task
		} else {
			batchTask := &batchCopTask{
				respChan:      make(chan *batchCopResponse, 2048),
				storeAddr:     rpcCtx.Addr,
				cmdType:       task.cmdType,
				regionTaskMap: make(map[uint64]*copTask),
				copTasks:      []copTaskAndRPCContext{{task, rpcCtx}},
			}
			batchTask.regionTaskMap[task.region.id] = task
			storeTaskMap[rpcCtx.Addr] = batchTask
		}
	}

	for _, task := range storeTaskMap {
		batchTasks = append(batchTasks, task)
	}

	if elapsed := time.Since(start); elapsed > time.Millisecond*500 {
		logutil.BgLogger().Warn("buildCopTasks takes too much time",
			zap.Duration("elapsed", elapsed),
			zap.Int("range len", rangesLen),
			zap.Int("task len", len(tasks)))
	}
	tikvTxnRegionsNumHistogramWithCoprocessor.Observe(float64(len(tasks)))
	return batchTasks, nil

}

func (c *CopClient) SendBatch(ctx context.Context, req *kv.Request, vars *kv.Variables) kv.Response {
	ctx = context.WithValue(ctx, txnStartKey, req.StartTs)
	bo := NewBackoffer(ctx, copBuildTaskMaxBackoff).WithVars(vars)
	tasks, err := buildBatchCopTasks(bo, c.store.regionCache, &copRanges{mid: req.KeyRanges}, req)
	if err != nil {
		return copErrorResponse{err}
	}
	it := &batchCopIterator{
		store:           c.store,
		req:             req,
		concurrency:     req.Concurrency,
		finishCh:        make(chan struct{}),
		vars:            vars,
		memTracker:      req.MemTracker,
		replicaReadSeed: c.replicaReadSeed,
		clientHelper: clientHelper{
			LockResolver:      c.store.lockResolver,
			RegionCache:       c.store.regionCache,
			Client:            c.store.client,
			minCommitTSPushed: &minCommitTSPushed{data: make(map[uint64]struct{}, 5)},
		},
	}
	it.tasks = tasks
	if it.concurrency > len(tasks) {
		it.concurrency = len(tasks)
	}
	if it.concurrency < 1 {
		// Make sure that there is at least one worker.
		it.concurrency = 1
	}
	if it.req.KeepOrder {
		it.sendRate = newRateLimit(2 * it.concurrency)
	} else {
		it.respChan = make(chan *batchCopResponse, it.concurrency)
	}
	go it.run(ctx)
	return it
}

type batchCopIterator struct {
	clientHelper

	store       *tikvStore
	req         *kv.Request
	concurrency int
	finishCh    chan struct{}

	// If keepOrder, results are stored in copTask.respChan, read them out one by one.
	tasks []*batchCopTask
	curr  int
	// sendRate controls the sending rate of copIteratorTaskSender, if keepOrder,
	// to prevent all tasks being done (aka. all of the responses are buffered)
	sendRate *rateLimit

	// Otherwise, results are stored in respChan.
	respChan chan *batchCopResponse

	vars *kv.Variables

	memTracker *memory.Tracker

	replicaReadSeed uint32

	wg sync.WaitGroup
	// closed represents when the Close is called.
	// There are two cases we need to close the `finishCh` channel, one is when context is done, the other one is
	// when the Close is called. we use atomic.CompareAndSwap `closed` to to make sure the channel is not closed twice.
	closed uint32
}

func (b *batchCopIterator) run(ctx context.Context) {
	for _, task := range b.tasks {
		b.wg.Add(1)
		bo := NewBackoffer(ctx, copNextMaxBackoff).WithVars(b.vars)
		go b.handleTask(ctx, bo, task)
	}
	b.wg.Wait()
	close(b.respChan)
}

// Next returns next coprocessor result.
// NOTE: Use nil to indicate finish, so if the returned ResultSubset is not nil, reader should continue to call Next().
func (b *batchCopIterator) Next(ctx context.Context) (kv.ResultSubset, error) {
	var (
		resp   *batchCopResponse
		ok     bool
		closed bool
	)
	// If data order matters, response should be returned in the same order as copTask slice.
	// Otherwise all responses are returned from a single channel.

	// Get next fetched resp from chan
	resp, ok, closed = b.recvFromRespCh(ctx)
	if !ok || closed {
		return nil, nil
	}

	if resp.err != nil {
		return nil, errors.Trace(resp.err)
	}

	err := b.store.CheckVisibility(b.req.StartTs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return resp, nil
}

func (b *batchCopIterator) recvFromRespCh(ctx context.Context) (resp *batchCopResponse, ok bool, exit bool) {
	select {
	case resp, ok = <-b.respChan:
	case <-b.finishCh:
		exit = true
	case <-ctx.Done():
		// We select the ctx.Done() in the thread of `Next` instead of in the worker to avoid the cost of `WithCancel`.
		if atomic.CompareAndSwapUint32(&b.closed, 0, 1) {
			close(b.finishCh)
		}
		exit = true
	}
	return
}

func (it *batchCopIterator) Close() error {
	if atomic.CompareAndSwapUint32(&it.closed, 0, 1) {
		close(it.finishCh)
	}
	it.wg.Wait()
	return nil
}

func (b *batchCopIterator) handleTask(ctx context.Context, bo *Backoffer, task *batchCopTask) {
	logutil.BgLogger().Debug("handle batch task")
	tasks := []*batchCopTask{task}
	idx := 0
	for idx < len(tasks) {
		ret, err := b.handleTaskOnce(ctx, bo, task)
		if err != nil {
			resp := &batchCopResponse{err: errors.Trace(err)}
			b.sendToRespCh(resp)
		} else {
			tasks = append(tasks, ret...)
		}
		idx++
	}
	close(task.respChan)
	b.wg.Done()
}

func (b *batchCopIterator) handleTaskOnce(ctx context.Context, bo *Backoffer, task *batchCopTask) ([]*batchCopTask, error) {
	logutil.BgLogger().Debug("handle batch task once")
	sender := NewRegionBatchRequestSender(b.store.regionCache, b.store.client)
	var regionInfos []*coprocessor.RegionInfo
	for _, task := range task.copTasks {
		regionInfos = append(regionInfos, &coprocessor.RegionInfo{
			RegionId: task.task.region.id,
			RegionEpoch: &metapb.RegionEpoch{
				ConfVer: task.task.region.confVer,
				Version: task.task.region.ver,
			},
			Ranges: task.task.ranges.toPBRanges(),
		})
	}

	copReq := coprocessor.BatchRequest{
		Tp:        b.req.Tp,
		StartTs:   b.req.StartTs,
		Data:      b.req.Data,
		SchemaVer: b.req.SchemaVar,
		Regions:   regionInfos,
	}

	req := tikvrpc.NewReplicaReadRequest(task.cmdType, &copReq, b.req.ReplicaRead, b.replicaReadSeed, kvrpcpb.Context{
		IsolationLevel: pbIsolationLevel(b.req.IsolationLevel),
		Priority:       kvPriorityToCommandPri(b.req.Priority),
		NotFillCache:   b.req.NotFillCache,
		HandleTime:     true,
		ScanDetail:     true,
	})
	req.StoreTp = kv.TiFlash

	logutil.BgLogger().Debug("send batch to ", zap.String("req info", req.String()), zap.Int("cop task len", len(task.copTasks)))
	resp, err := sender.sendReqToAddr(bo, task.copTasks, req, ReadTimeoutMedium)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return b.handleStreamedBatchCopResponse(ctx, bo, resp.Resp.(*tikvrpc.BatchCopStreamResponse), task)
}
func (b *batchCopIterator) handleStreamedBatchCopResponse(ctx context.Context, bo *Backoffer, response *tikvrpc.BatchCopStreamResponse, task *batchCopTask) (totalRetTask []*batchCopTask, err error) {
	defer response.Close()
	resp := response.BatchResponse
	if resp == nil {
		// streaming request returns io.EOF, so the first Response is nil.
		return nil, nil
	}
	for {
		remainedTasks, err := b.handleBatchCopResponse(bo, resp, task)
		if err != nil || len(remainedTasks) != 0 {
			return remainedTasks, errors.Trace(err)
		}
		resp, err = response.Recv()
		if err != nil {
			if errors.Cause(err) == io.EOF {
				return nil, nil
			}

			if err1 := bo.Backoff(boTiKVRPC, errors.Errorf("recv stream response error: %v, task: %s", err, task)); err1 != nil {
				return nil, errors.Trace(err)
			}

			// No coprocessor.Response for network error, rebuild task based on the last success one.
			if errors.Cause(err) == context.Canceled {
				logutil.BgLogger().Info("stream recv timeout", zap.Error(err))
			} else {
				logutil.BgLogger().Info("stream unknown error", zap.Error(err))
			}
			return nil, errors.Trace(err)
		}
	}
}

func (b *batchCopIterator) handleBatchCopResponse(bo *Backoffer, response *coprocessor.BatchResponse, task *batchCopTask) (totalRetTask []*batchCopTask, err error) {
	for _, status := range response.RegionStatus {
		id := status.RegionId
		if status.RegionError != nil {

			if err := bo.Backoff(BoRegionMiss, errors.New(status.RegionError.String())); err != nil {
				return nil, errors.Trace(err)
			}

			copTask := task.regionTaskMap[id]

			// We may meet RegionError at the first packet, but not during visiting the stream.
			retTasks, err := buildBatchCopTasks(bo, b.store.regionCache, copTask.ranges, b.req)
			if err != nil {
				return nil, errors.Trace(err)
			}
			totalRetTask = append(totalRetTask, retTasks...)
		}
		if status.Locked != nil {
			msBeforeExpired, err1 := b.ResolveLocks(bo, b.req.StartTs, []*Lock{NewLock(status.Locked)})
			if err1 != nil {
				return nil, errors.Trace(err1)
			}
			if msBeforeExpired > 0 {
				if err := bo.BackoffWithMaxSleep(boTxnLockFast, int(msBeforeExpired), errors.New(status.Locked.String())); err != nil {
					return nil, errors.Trace(err)
				}
			}
			totalRetTask = append(totalRetTask, task)
		}
	}
	if otherErr := response.GetOtherError(); otherErr != "" {
		err := errors.Errorf("other error: %s", otherErr)
		logutil.BgLogger().Warn("other error",
			zap.Uint64("txnStartTS", b.req.StartTs),
			zap.String("storeAddr", task.storeAddr),
			zap.Error(err))
		return nil, errors.Trace(err)
	}

	b.sendToRespCh(&batchCopResponse{
		pbResp: response,
	})

	return totalRetTask, nil
}

func (b *batchCopIterator) sendToRespCh(resp *batchCopResponse) (exit bool) {
	select {
	case b.respChan <- resp:
	case <-b.finishCh:
		exit = true
	}
	return
}
