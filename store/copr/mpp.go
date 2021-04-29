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

package copr

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/mpp"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
	tikverr "github.com/pingcap/tidb/store/tikv/error"
	"github.com/pingcap/tidb/store/tikv/logutil"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"go.uber.org/zap"
)

// MPPClient servers MPP requests.
type MPPClient struct {
	store *tikv.KVStore
}

// GetAddress returns the network address.
func (c *batchCopTask) GetAddress() string {
	return c.storeAddr
}

func (c *MPPClient) selectAllTiFlashStore() []kv.MPPTaskMeta {
	resultTasks := make([]kv.MPPTaskMeta, 0)
	for _, addr := range c.store.GetRegionCache().GetTiFlashStoreAddrs() {
		task := &batchCopTask{storeAddr: addr, cmdType: tikvrpc.CmdMPPTask}
		resultTasks = append(resultTasks, task)
	}
	return resultTasks
}

// ConstructMPPTasks receives ScheduleRequest, which are actually collects of kv ranges. We allocates MPPTaskMeta for them and returns.
func (c *MPPClient) ConstructMPPTasks(ctx context.Context, req *kv.MPPBuildTasksRequest) ([]kv.MPPTaskMeta, error) {
	ctx = context.WithValue(ctx, tikv.TxnStartKey, req.StartTS)
	bo := tikv.NewBackofferWithVars(ctx, copBuildTaskMaxBackoff, nil)
	if req.KeyRanges == nil {
		return c.selectAllTiFlashStore(), nil
	}
	ranges := toTiKVKeyRanges(req.KeyRanges)
	tasks, err := buildBatchCopTasks(bo, c.store.GetRegionCache(), ranges, kv.TiFlash)
	if err != nil {
		return nil, errors.Trace(err)
	}
	mppTasks := make([]kv.MPPTaskMeta, 0, len(tasks))
	for _, copTask := range tasks {
		mppTasks = append(mppTasks, copTask)
	}
	return mppTasks, nil
}

// mppResponse wraps mpp data packet.
type mppResponse struct {
	pbResp   *mpp.MPPDataPacket
	detail   *CopRuntimeStats
	respTime time.Duration
	respSize int64

	err error
}

// GetData implements the kv.ResultSubset GetData interface.
func (m *mppResponse) GetData() []byte {
	return m.pbResp.Data
}

// GetStartKey implements the kv.ResultSubset GetStartKey interface.
func (m *mppResponse) GetStartKey() kv.Key {
	return nil
}

// GetExecDetails is unavailable currently.
func (m *mppResponse) GetCopRuntimeStats() *CopRuntimeStats {
	return m.detail
}

// MemSize returns how many bytes of memory this response use
func (m *mppResponse) MemSize() int64 {
	if m.respSize != 0 {
		return m.respSize
	}

	if m.detail != nil {
		m.respSize += int64(sizeofExecDetails)
	}
	if m.pbResp != nil {
		m.respSize += int64(m.pbResp.Size())
	}
	return m.respSize
}

func (m *mppResponse) RespTime() time.Duration {
	return m.respTime
}

type mppIterator struct {
	store *tikv.KVStore

	tasks    []*kv.MPPDispatchRequest
	finishCh chan struct{}

	startTs uint64

	respChan chan *mppResponse

	cancelFunc context.CancelFunc

	wg sync.WaitGroup

	closed uint32

	vars *tikv.Variables

	mu sync.Mutex
}

func (m *mppIterator) run(ctx context.Context) {
	for _, task := range m.tasks {
		if atomic.LoadUint32(&m.closed) == 1 {
			break
		}
		m.mu.Lock()
		switch task.State {
		case kv.MppTaskReady:
			task.State = kv.MppTaskRunning
			m.mu.Unlock()
		default:
			m.mu.Unlock()
			break
		}
		m.wg.Add(1)
		bo := tikv.NewBackoffer(ctx, copNextMaxBackoff)
		go m.handleDispatchReq(ctx, bo, task)
	}
	m.wg.Wait()
	close(m.respChan)
}

func (m *mppIterator) sendError(err error) {
	m.sendToRespCh(&mppResponse{err: err})
	m.cancelMppTasks()
}

func (m *mppIterator) sendToRespCh(resp *mppResponse) (exit bool) {
	select {
	case m.respChan <- resp:
	case <-m.finishCh:
		exit = true
	}
	return
}

// TODO:: Consider that which way is better:
// - dispatch all tasks at once, and connect tasks at second.
// - dispatch tasks and establish connection at the same time.
func (m *mppIterator) handleDispatchReq(ctx context.Context, bo *tikv.Backoffer, req *kv.MPPDispatchRequest) {
	defer func() {
		m.wg.Done()
	}()
	var regionInfos []*coprocessor.RegionInfo
	originalTask, ok := req.Meta.(*batchCopTask)
	if ok {
		for _, task := range originalTask.copTasks {
			regionInfos = append(regionInfos, &coprocessor.RegionInfo{
				RegionId: task.task.region.GetID(),
				RegionEpoch: &metapb.RegionEpoch{
					ConfVer: task.task.region.GetConfVer(),
					Version: task.task.region.GetVer(),
				},
				Ranges: task.task.ranges.ToPBRanges(),
			})
		}
	}

	// meta for current task.
	taskMeta := &mpp.TaskMeta{StartTs: req.StartTs, TaskId: req.ID, Address: req.Meta.GetAddress()}

	mppReq := &mpp.DispatchTaskRequest{
		Meta:        taskMeta,
		EncodedPlan: req.Data,
		// TODO: This is only an experience value. It's better to be configurable.
		Timeout:   60,
		SchemaVer: req.SchemaVar,
		Regions:   regionInfos,
	}

	wrappedReq := tikvrpc.NewRequest(tikvrpc.CmdMPPTask, mppReq, kvrpcpb.Context{})
	wrappedReq.StoreTp = tikvrpc.TiFlash

	// TODO: Handle dispatch task response correctly, including retry logic and cancel logic.
	var rpcResp *tikvrpc.Response
	var err error
	// If copTasks is not empty, we should send request according to region distribution.
	// Or else it's the task without region, which always happens in high layer task without table.
	// In that case
	if originalTask != nil {
		sender := NewRegionBatchRequestSender(m.store.GetRegionCache(), m.store.GetTiKVClient())
		rpcResp, _, _, err = sender.sendStreamReqToAddr(bo, originalTask.copTasks, wrappedReq, tikv.ReadTimeoutMedium)
		// No matter what the rpc error is, we won't retry the mpp dispatch tasks.
		// TODO: If we want to retry, we must redo the plan fragment cutting and task scheduling.
		// That's a hard job but we can try it in the future.
		if sender.GetRPCError() != nil {
			logutil.BgLogger().Error("mpp dispatch meet io error", zap.String("error", sender.GetRPCError().Error()))
			// we return timeout to trigger tikv's fallback
			m.sendError(tikverr.ErrTiFlashServerTimeout)
			return
		}
	} else {
		rpcResp, err = m.store.GetTiKVClient().SendRequest(ctx, req.Meta.GetAddress(), wrappedReq, tikv.ReadTimeoutMedium)
	}

	if err != nil {
		logutil.BgLogger().Error("mpp dispatch meet error", zap.String("error", err.Error()))
		// we return timeout to trigger tikv's fallback
		m.sendError(tikverr.ErrTiFlashServerTimeout)
		return
	}

	realResp := rpcResp.Resp.(*mpp.DispatchTaskResponse)

	if realResp.Error != nil {
		m.sendError(errors.New(realResp.Error.Msg))
		return
	}
	failpoint.Inject("mppNonRootTaskError", func(val failpoint.Value) {
		if val.(bool) && !req.IsRoot {
			time.Sleep(1 * time.Second)
			m.sendError(tikverr.ErrTiFlashServerTimeout)
			return
		}
	})
	if !req.IsRoot {
		return
	}

	m.establishMPPConns(bo, req, taskMeta)
}

// NOTE: We do not retry here, because retry is helpless when errors result from TiFlash or Network. If errors occur, the execution on TiFlash will finally stop after some minutes.
// This function is exclusively called, and only the first call succeeds sending tasks and setting all tasks as cancelled, while others will not work.
func (m *mppIterator) cancelMppTasks() {
	m.mu.Lock()
	defer m.mu.Unlock()
	killReq := &mpp.CancelTaskRequest{
		Meta: &mpp.TaskMeta{StartTs: m.startTs},
	}

	wrappedReq := tikvrpc.NewRequest(tikvrpc.CmdMPPCancel, killReq, kvrpcpb.Context{})
	wrappedReq.StoreTp = tikvrpc.TiFlash

	usedStoreAddrs := make(map[string]bool)
	for _, task := range m.tasks {
		// get the store address of running tasks
		if task.State == kv.MppTaskRunning && !usedStoreAddrs[task.Meta.GetAddress()] {
			usedStoreAddrs[task.Meta.GetAddress()] = true
		} else if task.State == kv.MppTaskCancelled {
			return
		}
		task.State = kv.MppTaskCancelled
	}

	// send cancel cmd to all stores where tasks run
	for addr := range usedStoreAddrs {
		_, err := m.store.GetTiKVClient().SendRequest(context.Background(), addr, wrappedReq, tikv.ReadTimeoutShort)
		logutil.BgLogger().Debug("cancel task ", zap.Uint64("query id ", m.startTs), zap.String(" on addr ", addr))
		if err != nil {
			logutil.BgLogger().Error("cancel task error: ", zap.Error(err), zap.Uint64(" for query id ", m.startTs), zap.String(" on addr ", addr))
		}
	}
}

func (m *mppIterator) establishMPPConns(bo *tikv.Backoffer, req *kv.MPPDispatchRequest, taskMeta *mpp.TaskMeta) {
	connReq := &mpp.EstablishMPPConnectionRequest{
		SenderMeta: taskMeta,
		ReceiverMeta: &mpp.TaskMeta{
			StartTs: req.StartTs,
			TaskId:  -1,
		},
	}

	wrappedReq := tikvrpc.NewRequest(tikvrpc.CmdMPPConn, connReq, kvrpcpb.Context{})
	wrappedReq.StoreTp = tikvrpc.TiFlash

	// Drain result from root task.
	// We don't need to process any special error. When we meet errors, just let it fail.
	rpcResp, err := m.store.GetTiKVClient().SendRequest(bo.GetCtx(), req.Meta.GetAddress(), wrappedReq, tikv.ReadTimeoutUltraLong)

	if err != nil {
		logutil.BgLogger().Error("establish mpp connection meet error", zap.String("error", err.Error()))
		// we return timeout to trigger tikv's fallback
		m.sendError(tikverr.ErrTiFlashServerTimeout)
		return
	}

	stream := rpcResp.Resp.(*tikvrpc.MPPStreamResponse)
	defer stream.Close()

	resp := stream.MPPDataPacket
	if resp == nil {
		return
	}

	for {
		err := m.handleMPPStreamResponse(bo, resp, req)
		if err != nil {
			m.sendError(err)
			return
		}

		resp, err = stream.Recv()
		if err != nil {
			if errors.Cause(err) == io.EOF {
				return
			}

			if err1 := bo.Backoff(tikv.BoTiKVRPC, errors.Errorf("recv stream response error: %v", err)); err1 != nil {
				if errors.Cause(err) == context.Canceled {
					logutil.BgLogger().Info("stream recv timeout", zap.Error(err))
				} else {
					logutil.BgLogger().Info("stream unknown error", zap.Error(err))
				}
			}
			m.sendError(tikverr.ErrTiFlashServerTimeout)
			return
		}
	}
}

// TODO: Test the case that user cancels the query.
func (m *mppIterator) Close() error {
	if atomic.CompareAndSwapUint32(&m.closed, 0, 1) {
		close(m.finishCh)
	}
	m.cancelFunc()
	m.wg.Wait()
	return nil
}

func (m *mppIterator) handleMPPStreamResponse(bo *tikv.Backoffer, response *mpp.MPPDataPacket, req *kv.MPPDispatchRequest) (err error) {
	if response.Error != nil {
		err = errors.Errorf("other error for mpp stream: %s", response.Error.Msg)
		logutil.BgLogger().Warn("other error",
			zap.Uint64("txnStartTS", req.StartTs),
			zap.String("storeAddr", req.Meta.GetAddress()),
			zap.Error(err))
		return err
	}

	resp := &mppResponse{
		pbResp: response,
		detail: new(CopRuntimeStats),
	}

	backoffTimes := bo.GetBackoffTimes()
	resp.detail.BackoffTime = time.Duration(bo.GetTotalSleep()) * time.Millisecond
	resp.detail.BackoffSleep = make(map[string]time.Duration, len(backoffTimes))
	resp.detail.BackoffTimes = make(map[string]int, len(backoffTimes))
	for backoff := range backoffTimes {
		backoffName := backoff.String()
		resp.detail.BackoffTimes[backoffName] = backoffTimes[backoff]
		resp.detail.BackoffSleep[backoffName] = time.Duration(bo.GetBackoffSleepMS()[backoff]) * time.Millisecond
	}
	resp.detail.CalleeAddress = req.Meta.GetAddress()

	m.sendToRespCh(resp)
	return
}

func (m *mppIterator) nextImpl(ctx context.Context) (resp *mppResponse, ok bool, exit bool, err error) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case resp, ok = <-m.respChan:
			return
		case <-ticker.C:
			if m.vars != nil && m.vars.Killed != nil && atomic.LoadUint32(m.vars.Killed) == 1 {
				err = tikverr.ErrQueryInterrupted
				exit = true
				return
			}
		case <-m.finishCh:
			exit = true
			return
		case <-ctx.Done():
			if atomic.CompareAndSwapUint32(&m.closed, 0, 1) {
				close(m.finishCh)
			}
			exit = true
			return
		}
	}
}

func (m *mppIterator) Next(ctx context.Context) (kv.ResultSubset, error) {
	resp, ok, closed, err := m.nextImpl(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !ok || closed {
		return nil, nil
	}

	if resp.err != nil {
		return nil, errors.Trace(resp.err)
	}

	err = m.store.CheckVisibility(m.startTs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return resp, nil
}

// DispatchMPPTasks dispatches all the mpp task and waits for the responses.
func (c *MPPClient) DispatchMPPTasks(ctx context.Context, variables interface{}, dispatchReqs []*kv.MPPDispatchRequest) kv.Response {
	vars := variables.(*tikv.Variables)
	ctxChild, cancelFunc := context.WithCancel(ctx)
	iter := &mppIterator{
		store:      c.store,
		tasks:      dispatchReqs,
		finishCh:   make(chan struct{}),
		cancelFunc: cancelFunc,
		respChan:   make(chan *mppResponse, 4096),
		startTs:    dispatchReqs[0].StartTs,
		vars:       vars,
	}
	go iter.run(ctxChild)
	return iter
}
