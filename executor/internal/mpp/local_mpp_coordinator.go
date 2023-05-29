// Copyright 2023 PingCAP, Inc.
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

package mpp

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/mpp"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/executor/internal/builder"
	"github.com/pingcap/tidb/executor/internal/util"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/copr"
	"github.com/pingcap/tidb/store/driver/backoff"
	derr "github.com/pingcap/tidb/store/driver/error"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
)

// mppResponse wraps mpp data packet.
type mppResponse struct {
	pbResp   *mpp.MPPDataPacket
	detail   *copr.CopRuntimeStats
	respTime time.Duration
	respSize int64

	err error
}

// GetData implements the kv.ResultSubset GetData interface.
func (m *mppResponse) GetData() []byte {
	return m.pbResp.Data
}

// GetStartKey implements the kv.ResultSubset GetStartKey interface.
func (*mppResponse) GetStartKey() kv.Key {
	return nil
}

// GetExecDetails is unavailable currently.
func (m *mppResponse) GetCopRuntimeStats() *copr.CopRuntimeStats {
	return m.detail
}

// MemSize returns how many bytes of memory this response use
func (m *mppResponse) MemSize() int64 {
	if m.respSize != 0 {
		return m.respSize
	}
	if m.detail != nil {
		m.respSize += int64(int(unsafe.Sizeof(execdetails.ExecDetails{})))
	}
	if m.pbResp != nil {
		m.respSize += int64(m.pbResp.Size())
	}
	return m.respSize
}

func (m *mppResponse) RespTime() time.Duration {
	return m.respTime
}

// localMppCoordinator stands for constructing and dispatching mpp tasks in local tidb server, since these work might be done remotely too
type localMppCoordinator struct {
	sessionCtx   sessionctx.Context
	is           infoschema.InfoSchema
	originalPlan plannercore.PhysicalPlan
	startTS      uint64
	mppQueryID   kv.MPPQueryID
	gatherID     uint64

	mppReqs []*kv.MPPDispatchRequest

	finishCh chan struct{}

	respChan chan *mppResponse

	cancelFunc context.CancelFunc

	wg         sync.WaitGroup
	wgDoneChan chan struct{}

	closed uint32

	vars *kv.Variables

	needTriggerFallback        bool
	enableCollectExecutionInfo bool
	firstErrMsg	string
	reqMap	map[int64]*kv.MPPDispatchRequest
	reportedReqCount int
	usedRRU	float64

	mu sync.Mutex

	memTracker *memory.Tracker
}

// NewLocalMPPCoordinator creates a new localMppCoordinator instance
func NewLocalMPPCoordinator(sctx sessionctx.Context, is infoschema.InfoSchema, plan plannercore.PhysicalPlan, startTS uint64, mppQueryID kv.MPPQueryID, gatherID uint64, memTracker *memory.Tracker) *localMppCoordinator {
	coord := &localMppCoordinator{
		sessionCtx:   sctx,
		is:           is,
		originalPlan: plan,
		startTS:      startTS,
		mppQueryID:   mppQueryID,
		gatherID:     gatherID,
		memTracker:   memTracker,
		finishCh:     make(chan struct{}),
		wgDoneChan:   make(chan struct{}),
		respChan:     make(chan *mppResponse),
		vars:         sctx.GetSessionVars().KVVars,
		reqMap: make(map[int64]*kv.MPPDispatchRequest),
		reportedReqCount: 0,
	}
	return coord
}

func (c *localMppCoordinator) appendMPPDispatchReq(pf *plannercore.Fragment) error {
	dagReq, err := builder.ConstructDAGReq(c.sessionCtx, []plannercore.PhysicalPlan{pf.ExchangeSender}, kv.TiFlash)
	if err != nil {
		return errors.Trace(err)
	}
	for i := range pf.ExchangeSender.Schema().Columns {
		dagReq.OutputOffsets = append(dagReq.OutputOffsets, uint32(i))
	}
	if !pf.IsRoot {
		dagReq.EncodeType = tipb.EncodeType_TypeCHBlock
	} else {
		dagReq.EncodeType = tipb.EncodeType_TypeChunk
	}
	for _, mppTask := range pf.ExchangeSender.Tasks {
		if mppTask.PartitionTableIDs != nil {
			err = util.UpdateExecutorTableID(context.Background(), dagReq.RootExecutor, true, mppTask.PartitionTableIDs)
		} else if !mppTask.IsDisaggregatedTiFlashStaticPrune {
			// If isDisaggregatedTiFlashStaticPrune is true, it means this TableScan is under PartitionUnoin,
			// tableID in TableScan is already the physical table id of this partition, no need to update again.
			err = util.UpdateExecutorTableID(context.Background(), dagReq.RootExecutor, true, []int64{mppTask.TableID})
		}
		if err != nil {
			return errors.Trace(err)
		}
		err = c.fixTaskForCTEStorageAndReader(dagReq.RootExecutor, mppTask.Meta)
		if err != nil {
			return err
		}
		pbData, err := dagReq.Marshal()
		if err != nil {
			return errors.Trace(err)
		}

		logutil.BgLogger().Info("Dispatch mpp task", zap.Uint64("timestamp", mppTask.StartTs),
			zap.Int64("ID", mppTask.ID), zap.Uint64("QueryTs", mppTask.MppQueryID.QueryTs), zap.Uint64("LocalQueryId", mppTask.MppQueryID.LocalQueryID),
			zap.Uint64("ServerID", mppTask.MppQueryID.ServerID), zap.String("address", mppTask.Meta.GetAddress()),
			zap.String("plan", plannercore.ToString(pf.ExchangeSender)),
			zap.Int64("mpp-version", mppTask.MppVersion.ToInt64()),
			zap.String("exchange-compression-mode", pf.ExchangeSender.CompressionMode.Name()),
		)
		req := &kv.MPPDispatchRequest{
			Data:       pbData,
			Meta:       mppTask.Meta,
			ID:         mppTask.ID,
			IsRoot:     pf.IsRoot,
			Timeout:    10,
			SchemaVar:  c.is.SchemaMetaVersion(),
			StartTs:    c.startTS,
			MppQueryID: mppTask.MppQueryID,
			GatherID:   c.gatherID,
			MppVersion: mppTask.MppVersion,
			State:      kv.MppTaskReady,
		}
		c.reqMap[req.ID] = req
		c.mppReqs = append(c.mppReqs, req)
	}
	return nil
}

// fixTaskForCTEStorageAndReader fixes the upstream/downstream tasks for the producers and consumers.
// After we split the fragments. A CTE producer in the fragment will holds all the task address of the consumers.
// For example, the producer has two task on node_1 and node_2. As we know that each consumer also has two task on the same nodes(node_1 and node_2)
// We need to prune address of node_2 for producer's task on node_1 since we just want the producer task on the node_1 only send to the consumer tasks on the node_1.
// And the same for the task on the node_2.
// And the same for the consumer task. We need to prune the unnecessary task address of its producer tasks(i.e. the downstream tasks).
func (c *localMppCoordinator) fixTaskForCTEStorageAndReader(exec *tipb.Executor, meta kv.MPPTaskMeta) error {
	children := make([]*tipb.Executor, 0, 2)
	switch exec.Tp {
	case tipb.ExecType_TypeTableScan, tipb.ExecType_TypePartitionTableScan, tipb.ExecType_TypeIndexScan:
	case tipb.ExecType_TypeSelection:
		children = append(children, exec.Selection.Child)
	case tipb.ExecType_TypeAggregation, tipb.ExecType_TypeStreamAgg:
		children = append(children, exec.Aggregation.Child)
	case tipb.ExecType_TypeTopN:
		children = append(children, exec.TopN.Child)
	case tipb.ExecType_TypeLimit:
		children = append(children, exec.Limit.Child)
	case tipb.ExecType_TypeExchangeSender:
		children = append(children, exec.ExchangeSender.Child)
		if len(exec.ExchangeSender.UpstreamCteTaskMeta) == 0 {
			break
		}
		actualUpStreamTasks := make([][]byte, 0, len(exec.ExchangeSender.UpstreamCteTaskMeta))
		actualTIDs := make([]int64, 0, len(exec.ExchangeSender.UpstreamCteTaskMeta))
		for _, tasksFromOneConsumer := range exec.ExchangeSender.UpstreamCteTaskMeta {
			for _, taskBytes := range tasksFromOneConsumer.EncodedTasks {
				taskMeta := &mpp.TaskMeta{}
				err := taskMeta.Unmarshal(taskBytes)
				if err != nil {
					return err
				}
				if taskMeta.Address != meta.GetAddress() {
					continue
				}
				actualUpStreamTasks = append(actualUpStreamTasks, taskBytes)
				actualTIDs = append(actualTIDs, taskMeta.TaskId)
			}
		}
		logutil.BgLogger().Warn("refine tunnel for cte producer task", zap.String("the final tunnel", fmt.Sprintf("up stream consumer tasks: %v", actualTIDs)))
		exec.ExchangeSender.EncodedTaskMeta = actualUpStreamTasks
	case tipb.ExecType_TypeExchangeReceiver:
		if len(exec.ExchangeReceiver.OriginalCtePrdocuerTaskMeta) == 0 {
			break
		}
		exec.ExchangeReceiver.EncodedTaskMeta = [][]byte{}
		actualTIDs := make([]int64, 0, 4)
		for _, taskBytes := range exec.ExchangeReceiver.OriginalCtePrdocuerTaskMeta {
			taskMeta := &mpp.TaskMeta{}
			err := taskMeta.Unmarshal(taskBytes)
			if err != nil {
				return err
			}
			if taskMeta.Address != meta.GetAddress() {
				continue
			}
			exec.ExchangeReceiver.EncodedTaskMeta = append(exec.ExchangeReceiver.EncodedTaskMeta, taskBytes)
			actualTIDs = append(actualTIDs, taskMeta.TaskId)
		}
		logutil.BgLogger().Warn("refine tunnel for cte consumer task", zap.String("the final tunnel", fmt.Sprintf("down stream producer task: %v", actualTIDs)))
	case tipb.ExecType_TypeJoin:
		children = append(children, exec.Join.Children...)
	case tipb.ExecType_TypeProjection:
		children = append(children, exec.Projection.Child)
	case tipb.ExecType_TypeWindow:
		children = append(children, exec.Window.Child)
	case tipb.ExecType_TypeSort:
		children = append(children, exec.Sort.Child)
	case tipb.ExecType_TypeExpand:
		children = append(children, exec.Expand.Child)
	default:
		return errors.Errorf("unknown new tipb protocol %d", exec.Tp)
	}
	for _, child := range children {
		err := c.fixTaskForCTEStorageAndReader(child, meta)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *localMppCoordinator) dispatchAll(ctx context.Context) {
	for _, task := range c.mppReqs {
		if atomic.LoadUint32(&c.closed) == 1 {
			break
		}
		c.mu.Lock()
		if task.State == kv.MppTaskReady {
			task.State = kv.MppTaskRunning
		}
		c.mu.Unlock()
		c.wg.Add(1)
		boMaxSleep := copr.CopNextMaxBackoff
		failpoint.Inject("ReduceCopNextMaxBackoff", func(value failpoint.Value) {
			if value.(bool) {
				boMaxSleep = 2
			}
		})
		bo := backoff.NewBackoffer(ctx, boMaxSleep)
		go func(mppTask *kv.MPPDispatchRequest) {
			defer func() {
				c.wg.Done()
			}()
			c.handleDispatchReq(ctx, bo, mppTask)
		}(task)
	}
	c.wg.Wait()
	close(c.wgDoneChan)
	close(c.respChan)
}

func (c *localMppCoordinator) sendError(err error) {
	c.sendToRespCh(&mppResponse{err: err})
	c.cancelMppTasks()
}

func (c *localMppCoordinator) sendToRespCh(resp *mppResponse) (exit bool) {
	defer func() {
		if r := recover(); r != nil {
			logutil.BgLogger().Error("localMppCoordinator panic", zap.Stack("stack"), zap.Any("recover", r))
			c.sendError(errors.New(fmt.Sprint(r)))
		}
	}()
	if c.memTracker != nil {
		respSize := resp.MemSize()
		failpoint.Inject("testMPPOOMPanic", func(val failpoint.Value) {
			if val.(bool) && respSize != 0 {
				respSize = 1 << 30
			}
		})
		c.memTracker.Consume(respSize)
		defer c.memTracker.Consume(-respSize)
	}
	select {
	case c.respChan <- resp:
	case <-c.finishCh:
		exit = true
	}
	return
}

// TODO:: Consider that which way is better:
// - dispatch all Tasks at once, and connect Tasks at second.
// - dispatch Tasks and establish connection at the same time.
func (c *localMppCoordinator) handleDispatchReq(ctx context.Context, bo *backoff.Backoffer, req *kv.MPPDispatchRequest) {
	var rpcResp *mpp.DispatchTaskResponse
	var err error
	var retry bool
	for {
		rpcResp, retry, err = c.sessionCtx.GetMPPClient().DispatchMPPTask(
			kv.DispatchMPPTaskParam{
				Ctx:                        ctx,
				Req:                        req,
				EnableCollectExecutionInfo: c.enableCollectExecutionInfo,
				Bo:                         bo.TiKVBackoffer(),
			})
		if retry {
			// TODO: If we want to retry, we might need to redo the plan fragment cutting and task scheduling. https://github.com/pingcap/tidb/issues/31015
			logutil.BgLogger().Warn("mpp dispatch meet error and retrying", zap.Error(err), zap.Uint64("timestamp", c.startTS), zap.Int64("task", req.ID), zap.Int64("mpp-version", req.MppVersion.ToInt64()))
			continue
		}
		break
	}

	if err != nil {
		logutil.BgLogger().Error("mpp dispatch meet error", zap.String("error", err.Error()), zap.Uint64("timestamp", req.StartTs), zap.Int64("task", req.ID), zap.Int64("mpp-version", req.MppVersion.ToInt64()))
		// if NeedTriggerFallback is true, we return timeout to trigger tikv's fallback
		if c.needTriggerFallback {
			err = derr.ErrTiFlashServerTimeout
		}
		c.sendError(err)
		return
	}

	if rpcResp.Error != nil {
		logutil.BgLogger().Error("mpp dispatch response meet error", zap.String("error", rpcResp.Error.Msg), zap.Uint64("timestamp", req.StartTs), zap.Int64("task", req.ID), zap.Int64("task-mpp-version", req.MppVersion.ToInt64()), zap.Int64("error-mpp-version", rpcResp.Error.GetMppVersion()))
		c.sendError(errors.New(rpcResp.Error.Msg))
		return
	}
	failpoint.Inject("mppNonRootTaskError", func(val failpoint.Value) {
		if val.(bool) && !req.IsRoot {
			time.Sleep(1 * time.Second)
			c.sendError(derr.ErrTiFlashServerTimeout)
			return
		}
	})
	if !req.IsRoot {
		return
	}
	// only root task should establish a stream conn with tiFlash to receive result.
	taskMeta := &mpp.TaskMeta{StartTs: req.StartTs, QueryTs: req.MppQueryID.QueryTs, LocalQueryId: req.MppQueryID.LocalQueryID, TaskId: req.ID, ServerId: req.MppQueryID.ServerID,
		Address:    req.Meta.GetAddress(),
		MppVersion: req.MppVersion.ToInt64(),
	}
	c.receiveResults(req, taskMeta, bo)
}

// NOTE: We do not retry here, because retry is helpless when errors result from TiFlash or Network. If errors occur, the execution on TiFlash will finally stop after some minutes.
// This function is exclusively called, and only the first call succeeds sending Tasks and setting all Tasks as cancelled, while others will not work.
func (c *localMppCoordinator) cancelMppTasks() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sessionCtx.GetMPPClient().CancelMPPTasks(kv.CancelMPPTasksParam{Reqs: c.mppReqs})
}

func (c *localMppCoordinator) receiveResults(req *kv.MPPDispatchRequest, taskMeta *mpp.TaskMeta, bo *backoff.Backoffer) {
	stream, err := c.sessionCtx.GetMPPClient().EstablishMPPConns(kv.EstablishMPPConnsParam{Ctx: bo.GetCtx(), Req: req, TaskMeta: taskMeta})
	if err != nil {
		// if NeedTriggerFallback is true, we return timeout to trigger tikv's fallback
		if c.needTriggerFallback {
			c.sendError(derr.ErrTiFlashServerTimeout)
		} else {
			c.sendError(err)
		}
		return
	}

	defer stream.Close()
	resp := stream.MPPDataPacket
	if resp == nil {
		return
	}

	for {
		err := c.handleMPPStreamResponse(bo, resp, req)
		if err != nil {
			c.sendError(err)
			return
		}

		resp, err = stream.Recv()
		if err != nil {
			if errors.Cause(err) == io.EOF {
				return
			}

			if err1 := bo.Backoff(tikv.BoTiKVRPC(), errors.Errorf("recv stream response error: %v", err)); err1 != nil {
				if errors.Cause(err) == context.Canceled {
					logutil.BgLogger().Info("stream recv timeout", zap.Error(err), zap.Uint64("timestamp", taskMeta.StartTs), zap.Int64("task", taskMeta.TaskId), zap.Int64("mpp-version", taskMeta.MppVersion))
				} else {
					logutil.BgLogger().Info("stream unknown error", zap.Error(err), zap.Uint64("timestamp", taskMeta.StartTs), zap.Int64("task", taskMeta.TaskId), zap.Int64("mpp-version", taskMeta.MppVersion))
				}
			}
			// if NeedTriggerFallback is true, we return timeout to trigger tikv's fallback
			if c.needTriggerFallback {
				c.sendError(derr.ErrTiFlashServerTimeout)
			} else {
				c.sendError(err)
			}
			return
		}
	}
}

func (c *localMppCoordinator) ReportStatus(info kv.MCStatusInfo) error {
	taskID := info.Request.Meta.TaskId
	var errMsg string
	if info.Request.Error != nil {
		errMsg = info.Request.Error.Msg
	}
	// TODO: check if add a new mutex
	c.mu.Lock()
	defer c.mu.Unlock()
	req, exists := c.reqMap[taskID]
	if !exists {
		return errors.Errorf("ReportStatus received missing taskID: %s", taskID)
	}
	if len(c.firstErrMsg) == 0 && len(errMsg) > 0 {
		c.firstErrMsg = errMsg
	}

	// update resource consumption and report counter
	if req.ReceivedReport {
		return errors.Errorf("ReportStatus already received taskID: %s", taskID)
	}
	c.reportedReqCount++
	rc := info.Request.ResourceConsumption
	c.usedRRU += rc.RRU
	return nil
}

// Close and release the used resources.
// TODO: Test the case that user cancels the query.
func (c *localMppCoordinator) Close() error {
	if atomic.CompareAndSwapUint32(&c.closed, 0, 1) {
		close(c.finishCh)
	}
	c.cancelFunc()
	<-c.wgDoneChan
	// wait until all ReportStatus received
	return nil
}

func (c *localMppCoordinator) handleMPPStreamResponse(bo *backoff.Backoffer, response *mpp.MPPDataPacket, req *kv.MPPDispatchRequest) (err error) {
	if response.Error != nil {
		c.mu.Lock()
		defer c.mu.Unlock()
		if len(c.firstErrMsg) > 0 {
			err = errors.Errorf("other error for mpp stream: %s", c.firstErrMsg)
		} else {
			err = errors.Errorf("other error for mpp stream: %s", response.Error.Msg)
		}
		logutil.BgLogger().Warn("other error",
			zap.Uint64("txnStartTS", req.StartTs),
			zap.String("storeAddr", req.Meta.GetAddress()),
			zap.Int64("mpp-version", req.MppVersion.ToInt64()),
			zap.Int64("task-id", req.ID),
			zap.Error(err))
		return err
	}

	resp := &mppResponse{
		pbResp: response,
		detail: new(copr.CopRuntimeStats),
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

	c.sendToRespCh(resp)
	return
}

func (c *localMppCoordinator) nextImpl(ctx context.Context) (resp *mppResponse, ok bool, exit bool, err error) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case resp, ok = <-c.respChan:
			return
		case <-ticker.C:
			if c.vars != nil && c.vars.Killed != nil && atomic.LoadUint32(c.vars.Killed) == 1 {
				err = derr.ErrQueryInterrupted
				exit = true
				return
			}
		case <-c.finishCh:
			exit = true
			return
		case <-ctx.Done():
			if atomic.CompareAndSwapUint32(&c.closed, 0, 1) {
				close(c.finishCh)
			}
			exit = true
			return
		}
	}
}

// Next returns next result subset
func (c *localMppCoordinator) Next(ctx context.Context) (kv.ResultSubset, error) {
	resp, ok, closed, err := c.nextImpl(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !ok || closed {
		return nil, nil
	}

	if resp.err != nil {
		return nil, errors.Trace(resp.err)
	}

	err = c.sessionCtx.GetMPPClient().CheckVisibility(c.startTS)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return resp, nil
}

// Execute executes physical plan and returns a response.
func (c *localMppCoordinator) Execute(ctx context.Context) (kv.Response, error) {
	// TODO: Move the construct tasks logic to planner, so we can see the explain results.
	sender := c.originalPlan.(*plannercore.PhysicalExchangeSender)
	sctx := c.sessionCtx
	frags, err := plannercore.GenerateRootMPPTasks(sctx, c.startTS, c.mppQueryID, sender, c.is)
	if err != nil {
		return nil, errors.Trace(err)
	}

	for _, frag := range frags {
		err = c.appendMPPDispatchReq(frag)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	failpoint.Inject("checkTotalMPPTasks", func(val failpoint.Value) {
		if val.(int) != len(c.mppReqs) {
			failpoint.Return(nil, errors.Errorf("The number of tasks is not right, expect %d tasks but actually there are %d tasks", val.(int), len(c.mppReqs)))
		}
	})

	ctx = distsql.WithSQLKvExecCounterInterceptor(ctx, sctx.GetSessionVars().StmtCtx)
	_, allowTiFlashFallback := sctx.GetSessionVars().AllowFallbackToTiKV[kv.TiFlash]
	ctx = distsql.SetTiFlashConfVarsInContext(ctx, sctx)
	c.needTriggerFallback = allowTiFlashFallback
	c.enableCollectExecutionInfo = config.GetGlobalConfig().Instance.EnableCollectExecutionInfo.Load()

	var ctxChild context.Context
	ctxChild, c.cancelFunc = context.WithCancel(ctx)
	go c.dispatchAll(ctxChild)

	return c, nil
}
