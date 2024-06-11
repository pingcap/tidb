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
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/distsql"
	"github.com/pingcap/tidb/pkg/executor/internal/builder"
	"github.com/pingcap/tidb/pkg/executor/internal/util"
	"github.com/pingcap/tidb/pkg/executor/metrics"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/store/copr"
	"github.com/pingcap/tidb/pkg/store/driver/backoff"
	derr "github.com/pingcap/tidb/pkg/store/driver/error"
	util2 "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tipb/go-tipb"
	clientutil "github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
)

const (
	receiveReportTimeout = 3 * time.Second
)

// mppResponse wraps mpp data packet.
type mppResponse struct {
	err      error
	pbResp   *mpp.MPPDataPacket
	detail   *copr.CopRuntimeStats
	respTime time.Duration
	respSize int64
}

// GetData implements the kv.ResultSubset GetData interface.
func (m *mppResponse) GetData() []byte {
	return m.pbResp.Data
}

// GetStartKey implements the kv.ResultSubset GetStartKey interface.
func (*mppResponse) GetStartKey() kv.Key {
	return nil
}

// GetCopRuntimeStats is unavailable currently.
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

type mppRequestReport struct {
	mppReq             *kv.MPPDispatchRequest
	errMsg             string
	executionSummaries []*tipb.ExecutorExecutionSummary
	receivedReport     bool // if received ReportStatus from mpp task
}

// localMppCoordinator stands for constructing and dispatching mpp tasks in local tidb server, since these work might be done remotely too
type localMppCoordinator struct {
	ctx          context.Context
	sessionCtx   sessionctx.Context
	is           infoschema.InfoSchema
	originalPlan base.PhysicalPlan
	reqMap       map[int64]*mppRequestReport

	cancelFunc context.CancelFunc

	wgDoneChan chan struct{}

	memTracker *memory.Tracker

	reportStatusCh chan struct{} // used to notify inside coordinator that all reports has been received

	vars *kv.Variables

	respChan chan *mppResponse

	finishCh chan struct{}

	coordinatorAddr string // empty if coordinator service not available
	firstErrMsg     string

	mppReqs []*kv.MPPDispatchRequest

	planIDs    []int
	mppQueryID kv.MPPQueryID

	wg               sync.WaitGroup
	gatherID         uint64
	reportedReqCount int
	startTS          uint64
	mu               sync.Mutex

	closed uint32

	dispatchFailed    uint32
	allReportsHandled uint32

	needTriggerFallback        bool
	enableCollectExecutionInfo bool
	reportExecutionInfo        bool // if each mpp task needs to report execution info directly to coordinator through ReportMPPTaskStatus

	// Record node cnt that involved in the mpp computation.
	nodeCnt int
}

// NewLocalMPPCoordinator creates a new localMppCoordinator instance
func NewLocalMPPCoordinator(ctx context.Context, sctx sessionctx.Context, is infoschema.InfoSchema, plan base.PhysicalPlan, planIDs []int, startTS uint64, mppQueryID kv.MPPQueryID, gatherID uint64, coordinatorAddr string, memTracker *memory.Tracker) *localMppCoordinator {
	if sctx.GetSessionVars().ChooseMppVersion() < kv.MppVersionV2 {
		coordinatorAddr = ""
	}
	coord := &localMppCoordinator{
		ctx:             ctx,
		sessionCtx:      sctx,
		is:              is,
		originalPlan:    plan,
		planIDs:         planIDs,
		startTS:         startTS,
		mppQueryID:      mppQueryID,
		gatherID:        gatherID,
		coordinatorAddr: coordinatorAddr,
		memTracker:      memTracker,
		finishCh:        make(chan struct{}),
		wgDoneChan:      make(chan struct{}),
		respChan:        make(chan *mppResponse),
		reportStatusCh:  make(chan struct{}),
		vars:            sctx.GetSessionVars().KVVars,
		reqMap:          make(map[int64]*mppRequestReport),
	}

	if len(coordinatorAddr) > 0 && needReportExecutionSummary(coord.originalPlan) {
		coord.reportExecutionInfo = true
	}
	return coord
}

func (c *localMppCoordinator) appendMPPDispatchReq(pf *plannercore.Fragment) error {
	dagReq, err := builder.ConstructDAGReq(c.sessionCtx, []base.PhysicalPlan{pf.ExchangeSender}, kv.TiFlash)
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
		} else if !mppTask.TiFlashStaticPrune {
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

		rgName := c.sessionCtx.GetSessionVars().StmtCtx.ResourceGroupName
		if !variable.EnableResourceControl.Load() {
			rgName = ""
		}
		logutil.BgLogger().Info("Dispatch mpp task", zap.Uint64("timestamp", mppTask.StartTs),
			zap.Int64("ID", mppTask.ID), zap.Uint64("QueryTs", mppTask.MppQueryID.QueryTs), zap.Uint64("LocalQueryId", mppTask.MppQueryID.LocalQueryID),
			zap.Uint64("ServerID", mppTask.MppQueryID.ServerID), zap.String("address", mppTask.Meta.GetAddress()),
			zap.String("plan", plannercore.ToString(pf.ExchangeSender)),
			zap.Int64("mpp-version", mppTask.MppVersion.ToInt64()),
			zap.String("exchange-compression-mode", pf.ExchangeSender.CompressionMode.Name()),
			zap.Uint64("GatherID", c.gatherID),
			zap.String("resource_group", rgName),
		)
		req := &kv.MPPDispatchRequest{
			Data:                   pbData,
			Meta:                   mppTask.Meta,
			ID:                     mppTask.ID,
			IsRoot:                 pf.IsRoot,
			Timeout:                10,
			SchemaVar:              c.is.SchemaMetaVersion(),
			StartTs:                c.startTS,
			MppQueryID:             mppTask.MppQueryID,
			GatherID:               c.gatherID,
			MppVersion:             mppTask.MppVersion,
			CoordinatorAddress:     c.coordinatorAddr,
			ReportExecutionSummary: c.reportExecutionInfo,
			State:                  kv.MppTaskReady,
			ResourceGroupName:      rgName,
			ConnectionID:           c.sessionCtx.GetSessionVars().ConnectionID,
			ConnectionAlias:        c.sessionCtx.ShowProcess().SessionAlias,
		}
		c.reqMap[req.ID] = &mppRequestReport{mppReq: req, receivedReport: false, errMsg: "", executionSummaries: nil}
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
	case tipb.ExecType_TypeExpand2:
		children = append(children, exec.Expand2.Child)
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

// DFS to check if plan needs report execution summary through ReportMPPTaskStatus mpp service
// Currently, return true if plan contains limit operator
func needReportExecutionSummary(plan base.PhysicalPlan) bool {
	switch x := plan.(type) {
	case *plannercore.PhysicalLimit:
		return true
	default:
		for _, child := range x.Children() {
			if needReportExecutionSummary(child) {
				return true
			}
		}
	}
	return false
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
			c.sendError(util2.GetRecoverError(r))
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
		atomic.CompareAndSwapUint32(&c.dispatchFailed, 0, 1)
		// if NeedTriggerFallback is true, we return timeout to trigger tikv's fallback
		if c.needTriggerFallback {
			err = derr.ErrTiFlashServerTimeout
		}
		c.sendError(err)
		return
	}

	if rpcResp.Error != nil {
		logutil.BgLogger().Error("mpp dispatch response meet error", zap.String("error", rpcResp.Error.Msg), zap.Uint64("timestamp", req.StartTs), zap.Int64("task", req.ID), zap.Int64("task-mpp-version", req.MppVersion.ToInt64()), zap.Int64("error-mpp-version", rpcResp.Error.GetMppVersion()))
		atomic.CompareAndSwapUint32(&c.dispatchFailed, 0, 1)
		c.sendError(errors.New(rpcResp.Error.Msg))
		return
	}
	failpoint.Inject("mppNonRootTaskError", func(val failpoint.Value) {
		if val.(bool) && !req.IsRoot {
			time.Sleep(1 * time.Second)
			atomic.CompareAndSwapUint32(&c.dispatchFailed, 0, 1)
			c.sendError(derr.ErrTiFlashServerTimeout)
			return
		}
	})
	if !req.IsRoot {
		return
	}
	// only root task should establish a stream conn with tiFlash to receive result.
	taskMeta := &mpp.TaskMeta{StartTs: req.StartTs, GatherId: c.gatherID, QueryTs: req.MppQueryID.QueryTs, LocalQueryId: req.MppQueryID.LocalQueryID, TaskId: req.ID, ServerId: req.MppQueryID.ServerID,
		Address:           req.Meta.GetAddress(),
		MppVersion:        req.MppVersion.ToInt64(),
		ResourceGroupName: req.ResourceGroupName,
	}
	c.receiveResults(req, taskMeta, bo)
}

// NOTE: We do not retry here, because retry is helpless when errors result from TiFlash or Network. If errors occur, the execution on TiFlash will finally stop after some minutes.
// This function is exclusively called, and only the first call succeeds sending Tasks and setting all Tasks as cancelled, while others will not work.
func (c *localMppCoordinator) cancelMppTasks() {
	if len(c.mppReqs) == 0 {
		return
	}
	usedStoreAddrs := make(map[string]bool)
	c.mu.Lock()
	// 1. One address will receive one cancel request, since cancel request cancels all mpp tasks within the same mpp gather
	// 2. Cancel process will set all mpp task requests' states, thus if one request's state is Cancelled already, just return
	if c.mppReqs[0].State == kv.MppTaskCancelled {
		c.mu.Unlock()
		return
	}
	for _, task := range c.mppReqs {
		// get the store address of running tasks,
		if task.State == kv.MppTaskRunning && !usedStoreAddrs[task.Meta.GetAddress()] {
			usedStoreAddrs[task.Meta.GetAddress()] = true
		}
		task.State = kv.MppTaskCancelled
	}
	c.mu.Unlock()
	c.sessionCtx.GetMPPClient().CancelMPPTasks(kv.CancelMPPTasksParam{StoreAddr: usedStoreAddrs, Reqs: c.mppReqs})
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

			logutil.BgLogger().Info("mpp stream recv got error", zap.Error(err), zap.Uint64("timestamp", taskMeta.StartTs),
				zap.Int64("task", taskMeta.TaskId), zap.Int64("mpp-version", taskMeta.MppVersion))

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

// ReportStatus implements MppCoordinator interface
func (c *localMppCoordinator) ReportStatus(info kv.ReportStatusRequest) error {
	taskID := info.Request.Meta.TaskId
	var errMsg string
	if info.Request.Error != nil {
		errMsg = info.Request.Error.Msg
	}
	executionInfo := new(tipb.TiFlashExecutionInfo)
	err := executionInfo.Unmarshal(info.Request.GetData())
	if err != nil {
		// since it is very corner case to reach here, and it won't cause forever hang due to not close reportStatusCh
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	req, exists := c.reqMap[taskID]
	if !exists {
		return errors.Errorf("ReportMPPTaskStatus task not exists taskID: %d", taskID)
	}
	if req.receivedReport {
		return errors.Errorf("ReportMPPTaskStatus task already received taskID: %d", taskID)
	}

	req.receivedReport = true
	if len(errMsg) > 0 {
		req.errMsg = errMsg
		if len(c.firstErrMsg) == 0 {
			c.firstErrMsg = errMsg
		}
	}

	c.reportedReqCount++
	req.executionSummaries = executionInfo.GetExecutionSummaries()
	if c.reportedReqCount == len(c.mppReqs) {
		close(c.reportStatusCh)
	}
	return nil
}

func (c *localMppCoordinator) handleAllReports() error {
	if c.reportExecutionInfo && atomic.LoadUint32(&c.dispatchFailed) == 0 && atomic.CompareAndSwapUint32(&c.allReportsHandled, 0, 1) {
		startTime := time.Now()
		select {
		case <-c.reportStatusCh:
			metrics.MppCoordinatorLatencyRcvReport.Observe(float64(time.Since(startTime).Milliseconds()))
			var recordedPlanIDs = make(map[int]int)
			for _, report := range c.reqMap {
				for _, detail := range report.executionSummaries {
					if detail != nil && detail.TimeProcessedNs != nil &&
						detail.NumProducedRows != nil && detail.NumIterations != nil {
						recordedPlanIDs[c.sessionCtx.GetSessionVars().StmtCtx.RuntimeStatsColl.
							RecordOneCopTask(-1, kv.TiFlash.Name(), report.mppReq.Meta.GetAddress(), detail)] = 0
					}
				}
				if ruDetailsRaw := c.ctx.Value(clientutil.RUDetailsCtxKey); ruDetailsRaw != nil {
					if err := execdetails.MergeTiFlashRUConsumption(report.executionSummaries, ruDetailsRaw.(*clientutil.RUDetails)); err != nil {
						return err
					}
				}
			}
			distsql.FillDummySummariesForTiFlashTasks(c.sessionCtx.GetSessionVars().StmtCtx.RuntimeStatsColl, "", kv.TiFlash.Name(), c.planIDs, recordedPlanIDs)
		case <-time.After(receiveReportTimeout):
			metrics.MppCoordinatorStatsReportNotReceived.Inc()
			logutil.BgLogger().Warn(fmt.Sprintf("Mpp coordinator not received all reports within %d seconds", int(receiveReportTimeout.Seconds())),
				zap.Uint64("txnStartTS", c.startTS),
				zap.Uint64("gatherID", c.gatherID),
				zap.Int("expectCount", len(c.mppReqs)),
				zap.Int("actualCount", c.reportedReqCount))
		}
	}
	return nil
}

// IsClosed implements MppCoordinator interface
func (c *localMppCoordinator) IsClosed() bool {
	return atomic.LoadUint32(&c.closed) == 1
}

// Close implements MppCoordinator interface
// TODO: Test the case that user cancels the query.
func (c *localMppCoordinator) Close() error {
	c.closeWithoutReport()
	return c.handleAllReports()
}

func (c *localMppCoordinator) closeWithoutReport() {
	if atomic.CompareAndSwapUint32(&c.closed, 0, 1) {
		close(c.finishCh)
	}
	c.cancelFunc()
	<-c.wgDoneChan
}

func (c *localMppCoordinator) handleMPPStreamResponse(bo *backoff.Backoffer, response *mpp.MPPDataPacket, req *kv.MPPDispatchRequest) (err error) {
	if response.Error != nil {
		c.mu.Lock()
		firstErrMsg := c.firstErrMsg
		c.mu.Unlock()
		// firstErrMsg is only used when already received error response from root tasks, avoid confusing error messages
		if len(firstErrMsg) > 0 {
			err = errors.Errorf("other error for mpp stream: %s", firstErrMsg)
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
			if c.vars != nil && c.vars.Killed != nil {
				killed := atomic.LoadUint32(c.vars.Killed)
				if killed != 0 {
					logutil.Logger(ctx).Info(
						"a killed signal is received",
						zap.Uint32("signal", killed),
					)
					err = derr.ErrQueryInterrupted
					exit = true
					return
				}
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

// Next implements MppCoordinator interface
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
		return nil, errors.Trace(derr.ErrQueryInterrupted)
	}
	return resp, nil
}

// Execute implements MppCoordinator interface
func (c *localMppCoordinator) Execute(ctx context.Context) (kv.Response, []kv.KeyRange, error) {
	// TODO: Move the construct tasks logic to planner, so we can see the explain results.
	sender := c.originalPlan.(*plannercore.PhysicalExchangeSender)
	sctx := c.sessionCtx
	frags, kvRanges, nodeInfo, err := plannercore.GenerateRootMPPTasks(sctx, c.startTS, c.gatherID, c.mppQueryID, sender, c.is)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	if nodeInfo == nil {
		return nil, nil, errors.New("node info should not be nil")
	}
	c.nodeCnt = len(nodeInfo)

	for _, frag := range frags {
		err = c.appendMPPDispatchReq(frag)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
	}
	failpoint.Inject("checkTotalMPPTasks", func(val failpoint.Value) {
		if val.(int) != len(c.mppReqs) {
			failpoint.Return(nil, nil, errors.Errorf("The number of tasks is not right, expect %d tasks but actually there are %d tasks", val.(int), len(c.mppReqs)))
		}
	})

	ctx = distsql.WithSQLKvExecCounterInterceptor(ctx, sctx.GetSessionVars().StmtCtx.KvExecCounter)
	_, allowTiFlashFallback := sctx.GetSessionVars().AllowFallbackToTiKV[kv.TiFlash]
	ctx = distsql.SetTiFlashConfVarsInContext(ctx, sctx.GetDistSQLCtx())
	c.needTriggerFallback = allowTiFlashFallback
	c.enableCollectExecutionInfo = config.GetGlobalConfig().Instance.EnableCollectExecutionInfo.Load()

	var ctxChild context.Context
	ctxChild, c.cancelFunc = context.WithCancel(ctx)
	go c.dispatchAll(ctxChild)

	return c, kvRanges, nil
}

// GetNodeCnt returns the node count that involved in the mpp computation.
func (c *localMppCoordinator) GetNodeCnt() int {
	return c.nodeCnt
}
