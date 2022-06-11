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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package copr

import (
	"context"
	"io"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/mpp"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/driver/backoff"
	derr "github.com/pingcap/tidb/store/driver/error"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mathutil"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// MPPClient servers MPP requests.
type MPPClient struct {
	store *kvStore
}

// GetAddress returns the network address.
func (c *batchCopTask) GetAddress() string {
	return c.storeAddr
}

func (c *MPPClient) selectAllTiFlashStore() []kv.MPPTaskMeta {
	resultTasks := make([]kv.MPPTaskMeta, 0)
	for _, s := range c.store.GetRegionCache().GetTiFlashStores() {
		task := &batchCopTask{storeAddr: s.GetAddr(), cmdType: tikvrpc.CmdMPPTask}
		resultTasks = append(resultTasks, task)
	}
	return resultTasks
}

// ConstructMPPTasks receives ScheduleRequest, which are actually collects of kv ranges. We allocates MPPTaskMeta for them and returns.
func (c *MPPClient) ConstructMPPTasks(ctx context.Context, req *kv.MPPBuildTasksRequest, mppStoreLastFailTime map[string]time.Time, ttl time.Duration) ([]kv.MPPTaskMeta, error) {
	ctx = context.WithValue(ctx, tikv.TxnStartKey(), req.StartTS)
	bo := backoff.NewBackofferWithVars(ctx, copBuildTaskMaxBackoff, nil)
	var tasks []*batchCopTask
	var err error
	if req.PartitionIDAndRanges != nil {
		rangesForEachPartition := make([]*KeyRanges, len(req.PartitionIDAndRanges))
		partitionIDs := make([]int64, len(req.PartitionIDAndRanges))
		for i, p := range req.PartitionIDAndRanges {
			rangesForEachPartition[i] = NewKeyRanges(p.KeyRanges)
			partitionIDs[i] = p.ID
		}
		tasks, err = buildBatchCopTasksForPartitionedTable(bo, c.store, rangesForEachPartition, kv.TiFlash, mppStoreLastFailTime, ttl, true, 20, partitionIDs)
	} else {
		if req.KeyRanges == nil {
			return c.selectAllTiFlashStore(), nil
		}
		ranges := NewKeyRanges(req.KeyRanges)
		tasks, err = buildBatchCopTasksForNonPartitionedTable(bo, c.store, ranges, kv.TiFlash, mppStoreLastFailTime, ttl, true, 20)
	}

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
	store *kvStore

	tasks    []*kv.MPPDispatchRequest
	finishCh chan struct{}

	startTs uint64

	respChan chan *mppResponse

	cancelFunc context.CancelFunc

	wg sync.WaitGroup

	closed uint32

	vars *tikv.Variables

	needTriggerFallback bool

	mu sync.Mutex

	enableCollectExecutionInfo bool
}

func (m *mppIterator) run(ctx context.Context) {
	for _, task := range m.tasks {
		if atomic.LoadUint32(&m.closed) == 1 {
			break
		}
		m.mu.Lock()
		if task.State == kv.MppTaskReady {
			task.State = kv.MppTaskRunning
		}
		m.mu.Unlock()
		m.wg.Add(1)
		boMaxSleep := copNextMaxBackoff
		failpoint.Inject("ReduceCopNextMaxBackoff", func(value failpoint.Value) {
			if value.(bool) {
				boMaxSleep = 2
			}
		})
		bo := backoff.NewBackoffer(ctx, boMaxSleep)
		go func(mppTask *kv.MPPDispatchRequest) {
			defer func() {
				m.wg.Done()
			}()
			m.handleDispatchReq(ctx, bo, mppTask)
		}(task)
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
func (m *mppIterator) handleDispatchReq(ctx context.Context, bo *Backoffer, req *kv.MPPDispatchRequest) {
	var regionInfos []*coprocessor.RegionInfo
	originalTask, ok := req.Meta.(*batchCopTask)
	if ok {
		for _, ri := range originalTask.regionInfos {
			regionInfos = append(regionInfos, ri.toCoprocessorRegionInfo())
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
	if originalTask != nil {
		mppReq.TableRegions = originalTask.PartitionTableRegions
		if mppReq.TableRegions != nil {
			mppReq.Regions = nil
		}
	}

	wrappedReq := tikvrpc.NewRequest(tikvrpc.CmdMPPTask, mppReq, kvrpcpb.Context{})
	wrappedReq.StoreTp = tikvrpc.TiFlash

	// TODO: Handle dispatch task response correctly, including retry logic and cancel logic.
	var rpcResp *tikvrpc.Response
	var err error
	var retry bool
	// If copTasks is not empty, we should send request according to region distribution.
	// Or else it's the task without region, which always happens in high layer task without table.
	// In that case
	if originalTask != nil {
		sender := NewRegionBatchRequestSender(m.store.GetRegionCache(), m.store.GetTiKVClient(), m.enableCollectExecutionInfo)
		rpcResp, retry, _, err = sender.SendReqToAddr(bo, originalTask.ctx, originalTask.regionInfos, wrappedReq, tikv.ReadTimeoutMedium)
		// No matter what the rpc error is, we won't retry the mpp dispatch tasks.
		// TODO: If we want to retry, we must redo the plan fragment cutting and task scheduling.
		// That's a hard job but we can try it in the future.
		if sender.GetRPCError() != nil {
			logutil.BgLogger().Warn("mpp dispatch meet io error", zap.String("error", sender.GetRPCError().Error()), zap.Uint64("timestamp", taskMeta.StartTs), zap.Int64("task", taskMeta.TaskId))
			// if needTriggerFallback is true, we return timeout to trigger tikv's fallback
			if m.needTriggerFallback {
				err = derr.ErrTiFlashServerTimeout
			} else {
				err = sender.GetRPCError()
			}
		}
	} else {
		rpcResp, err = m.store.GetTiKVClient().SendRequest(ctx, req.Meta.GetAddress(), wrappedReq, tikv.ReadTimeoutMedium)
		if errors.Cause(err) == context.Canceled || status.Code(errors.Cause(err)) == codes.Canceled {
			retry = false
		} else if err != nil {
			if bo.Backoff(tikv.BoTiFlashRPC(), err) == nil {
				retry = true
			}
		}
	}

	if retry {
		logutil.BgLogger().Warn("mpp dispatch meet error and retrying", zap.Error(err), zap.Uint64("timestamp", taskMeta.StartTs), zap.Int64("task", taskMeta.TaskId))
		m.handleDispatchReq(ctx, bo, req)
		return
	}

	if err != nil {
		logutil.BgLogger().Error("mpp dispatch meet error", zap.String("error", err.Error()), zap.Uint64("timestamp", taskMeta.StartTs), zap.Int64("task", taskMeta.TaskId))
		// if needTriggerFallback is true, we return timeout to trigger tikv's fallback
		if m.needTriggerFallback {
			err = derr.ErrTiFlashServerTimeout
		}
		m.sendError(err)
		return
	}

	realResp := rpcResp.Resp.(*mpp.DispatchTaskResponse)

	if realResp.Error != nil {
		logutil.BgLogger().Error("mpp dispatch response meet error", zap.String("error", realResp.Error.Msg), zap.Uint64("timestamp", taskMeta.StartTs), zap.Int64("task", taskMeta.TaskId))
		m.sendError(errors.New(realResp.Error.Msg))
		return
	}
	if len(realResp.RetryRegions) > 0 {
		logutil.BgLogger().Info("TiFlash found " + strconv.Itoa(len(realResp.RetryRegions)) + " stale regions. Only first " + strconv.Itoa(mathutil.Min(10, len(realResp.RetryRegions))) + " regions will be logged if the log level is higher than Debug")
		for index, retry := range realResp.RetryRegions {
			id := tikv.NewRegionVerID(retry.Id, retry.RegionEpoch.ConfVer, retry.RegionEpoch.Version)
			if index < 10 || log.GetLevel() <= zap.DebugLevel {
				logutil.BgLogger().Info("invalid region because tiflash detected stale region", zap.String("region id", id.String()))
			}
			m.store.GetRegionCache().InvalidateCachedRegionWithReason(id, tikv.EpochNotMatch)
		}
	}
	failpoint.Inject("mppNonRootTaskError", func(val failpoint.Value) {
		if val.(bool) && !req.IsRoot {
			time.Sleep(1 * time.Second)
			m.sendError(derr.ErrTiFlashServerTimeout)
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

func (m *mppIterator) establishMPPConns(bo *Backoffer, req *kv.MPPDispatchRequest, taskMeta *mpp.TaskMeta) {
	connReq := &mpp.EstablishMPPConnectionRequest{
		SenderMeta: taskMeta,
		ReceiverMeta: &mpp.TaskMeta{
			StartTs: req.StartTs,
			TaskId:  -1,
		},
	}

	wrappedReq := tikvrpc.NewRequest(tikvrpc.CmdMPPConn, connReq, kvrpcpb.Context{})
	wrappedReq.StoreTp = tikvrpc.TiFlash

	// Drain results from root task.
	// We don't need to process any special error. When we meet errors, just let it fail.
	rpcResp, err := m.store.GetTiKVClient().SendRequest(bo.GetCtx(), req.Meta.GetAddress(), wrappedReq, readTimeoutUltraLong)

	if err != nil {
		logutil.BgLogger().Warn("establish mpp connection meet error and cannot retry", zap.String("error", err.Error()), zap.Uint64("timestamp", taskMeta.StartTs), zap.Int64("task", taskMeta.TaskId))
		// if needTriggerFallback is true, we return timeout to trigger tikv's fallback
		if m.needTriggerFallback {
			m.sendError(derr.ErrTiFlashServerTimeout)
		} else {
			m.sendError(err)
		}
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

			if err1 := bo.Backoff(tikv.BoTiKVRPC(), errors.Errorf("recv stream response error: %v", err)); err1 != nil {
				if errors.Cause(err) == context.Canceled {
					logutil.BgLogger().Info("stream recv timeout", zap.Error(err), zap.Uint64("timestamp", taskMeta.StartTs), zap.Int64("task", taskMeta.TaskId))
				} else {
					logutil.BgLogger().Info("stream unknown error", zap.Error(err), zap.Uint64("timestamp", taskMeta.StartTs), zap.Int64("task", taskMeta.TaskId))
				}
			}
			// if needTriggerFallback is true, we return timeout to trigger tikv's fallback
			if m.needTriggerFallback {
				m.sendError(derr.ErrTiFlashServerTimeout)
			} else {
				m.sendError(err)
			}
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

func (m *mppIterator) handleMPPStreamResponse(bo *Backoffer, response *mpp.MPPDataPacket, req *kv.MPPDispatchRequest) (err error) {
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
		resp.detail.BackoffTimes[backoff] = backoffTimes[backoff]
		resp.detail.BackoffSleep[backoff] = time.Duration(bo.GetBackoffSleepMS()[backoff]) * time.Millisecond
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
				err = derr.ErrQueryInterrupted
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
func (c *MPPClient) DispatchMPPTasks(ctx context.Context, variables interface{}, dispatchReqs []*kv.MPPDispatchRequest, needTriggerFallback bool, startTs uint64) kv.Response {
	vars := variables.(*tikv.Variables)
	ctxChild, cancelFunc := context.WithCancel(ctx)
	iter := &mppIterator{
		store:                      c.store,
		tasks:                      dispatchReqs,
		finishCh:                   make(chan struct{}),
		cancelFunc:                 cancelFunc,
		respChan:                   make(chan *mppResponse, 4096),
		startTs:                    startTs,
		vars:                       vars,
		needTriggerFallback:        needTriggerFallback,
		enableCollectExecutionInfo: config.GetGlobalConfig().Instance.EnableCollectExecutionInfo,
	}
	go iter.run(ctxChild)
	return iter
}
