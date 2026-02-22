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
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/mpp"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl/placement"
	"github.com/pingcap/tidb/pkg/distsql"
	"github.com/pingcap/tidb/pkg/executor/metrics"
	"github.com/pingcap/tidb/pkg/kv"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/store/copr"
	"github.com/pingcap/tidb/pkg/store/driver/backoff"
	derr "github.com/pingcap/tidb/pkg/store/driver/error"
	util2 "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/tikv/client-go/v2/tikvrpc"
	clientutil "github.com/tikv/client-go/v2/util"
	"go.uber.org/zap"
)

// taskZoneInfoHelper used to help reset exchange executor's same zone flags
type taskZoneInfoHelper struct {
	allTiFlashZoneInfo map[string]string
	// exchangeZoneInfo is used to cache one mpp task's zone info:
	// key is executor id, value is zone info array
	// for ExchangeSender, it's target tiflash nodes' zone info; for ExchangeReceiver, it's source tiflash nodes' zone info
	exchangeZoneInfo map[string][]string
	tidbZone         string
	currentTaskZone  string
	isRoot           bool
}

func (h *taskZoneInfoHelper) init(allTiFlashZoneInfo map[string]string) {
	h.tidbZone = config.GetGlobalConfig().Labels[placement.DCLabelKey]
	h.allTiFlashZoneInfo = allTiFlashZoneInfo
	// initial capacity to 2, for one exchange sender and one exchange receiver
	h.exchangeZoneInfo = make(map[string][]string, 2)
}

func (h *taskZoneInfoHelper) tryQuickFillWithUncertainZones(exec *tipb.Executor, slots int, sameZoneFlags []bool) (bool, []bool) {
	if exec.ExecutorId == nil || len(h.currentTaskZone) == 0 {
		for range slots {
			sameZoneFlags = append(sameZoneFlags, true)
		}
		return true, sameZoneFlags
	}
	if h.isRoot && exec.Tp == tipb.ExecType_TypeExchangeSender {
		sameZoneFlags = append(sameZoneFlags, len(h.tidbZone) == 0 || h.currentTaskZone == h.tidbZone)
		return true, sameZoneFlags
	}

	// For CTE exchange nodes, data is passed locally, set all to true
	if (exec.Tp == tipb.ExecType_TypeExchangeSender && len(exec.ExchangeSender.UpstreamCteTaskMeta) != 0) ||
		(exec.Tp == tipb.ExecType_TypeExchangeReceiver && len(exec.ExchangeReceiver.OriginalCtePrdocuerTaskMeta) != 0) {
		for range slots {
			sameZoneFlags = append(sameZoneFlags, true)
		}
		return true, sameZoneFlags
	}

	return false, sameZoneFlags
}

func (h *taskZoneInfoHelper) collectExchangeZoneInfos(encodedTaskMeta [][]byte, slots int) []string {
	zoneInfos := make([]string, 0, slots)
	for _, taskBytes := range encodedTaskMeta {
		taskMeta := &mpp.TaskMeta{}
		err := taskMeta.Unmarshal(taskBytes)
		if err != nil {
			zoneInfos = append(zoneInfos, "")
			continue
		}
		zoneInfos = append(zoneInfos, h.allTiFlashZoneInfo[taskMeta.GetAddress()])
	}
	return zoneInfos
}

func (h *taskZoneInfoHelper) inferSameZoneFlag(exec *tipb.Executor, encodedTaskMeta [][]byte) []bool {
	slots := len(encodedTaskMeta)
	sameZoneFlags := make([]bool, 0, slots)
	filled := false
	if filled, sameZoneFlags = h.tryQuickFillWithUncertainZones(exec, slots, sameZoneFlags); filled {
		return sameZoneFlags
	}
	zoneInfos, exist := h.exchangeZoneInfo[*exec.ExecutorId]
	if !exist {
		zoneInfos = h.collectExchangeZoneInfos(encodedTaskMeta, slots)
		h.exchangeZoneInfo[*exec.ExecutorId] = zoneInfos
	}

	if len(zoneInfos) != slots {
		// This branch is for safety purpose, not expected
		for range slots {
			sameZoneFlags = append(sameZoneFlags, true)
		}
		return sameZoneFlags
	}

	for i := range slots {
		sameZoneFlags = append(sameZoneFlags, len(zoneInfos[i]) == 0 || h.currentTaskZone == zoneInfos[i])
	}
	return sameZoneFlags
}

func (h *taskZoneInfoHelper) fillSameZoneFlagForExchange(exec *tipb.Executor) {
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
		exec.ExchangeSender.SameZoneFlag = h.inferSameZoneFlag(exec, exec.ExchangeSender.EncodedTaskMeta)
	case tipb.ExecType_TypeExchangeReceiver:
		exec.ExchangeReceiver.SameZoneFlag = h.inferSameZoneFlag(exec, exec.ExchangeReceiver.EncodedTaskMeta)
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
		logutil.BgLogger().Warn(fmt.Sprintf("unknown new tipb protocol %d", exec.Tp))
	}
	for _, child := range children {
		h.fillSameZoneFlagForExchange(child)
	}
}

func getActualPhysicalPlan(plan base.Plan) base.PhysicalPlan {
	if plan == nil {
		return nil
	}
	if pp, ok := plan.(base.PhysicalPlan); ok {
		return pp
	}
	switch x := plan.(type) {
	case *plannercore.Explain:
		return getActualPhysicalPlan(x.TargetPlan)
	case *plannercore.SelectInto:
		return getActualPhysicalPlan(x.TargetPlan)
	case *physicalop.Insert:
		return x.SelectPlan
	case *plannercore.ImportInto:
		return x.SelectPlan
	case *physicalop.Update:
		return x.SelectPlan
	case *physicalop.Delete:
		return x.SelectPlan
	case *plannercore.Execute:
		return getActualPhysicalPlan(x.Plan)
	}
	return nil
}

// DFS to check if plan needs report execution summary through ReportMPPTaskStatus mpp service
// Currently, return true if there is a limit operator in the path from current TableReader to root
func needReportExecutionSummary(plan base.PhysicalPlan, destTablePlanID int, foundLimit bool) bool {
	switch x := plan.(type) {
	case *physicalop.PhysicalLimit:
		return needReportExecutionSummary(x.Children()[0], destTablePlanID, true)
	case *physicalop.PhysicalTableReader:
		if foundLimit {
			return x.GetTablePlan().ID() == destTablePlanID
		}
	case *physicalop.PhysicalShuffleReceiverStub:
		return needReportExecutionSummary(x.DataSource, destTablePlanID, foundLimit)
	case *physicalop.PhysicalCTE:
		if needReportExecutionSummary(x.SeedPlan, destTablePlanID, foundLimit) {
			return true
		}
		if x.RecurPlan != nil {
			return needReportExecutionSummary(x.RecurPlan, destTablePlanID, foundLimit)
		}
	default:
		for _, child := range x.Children() {
			if needReportExecutionSummary(child, destTablePlanID, foundLimit) {
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
			logutil.BgLogger().Warn("localMppCoordinator panic", zap.Stack("stack"), zap.Any("recover", r))
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
		logutil.BgLogger().Warn("mpp dispatch meet error", zap.String("error", err.Error()), zap.Uint64("timestamp", req.StartTs), zap.Int64("task", req.ID), zap.Int64("mpp-version", req.MppVersion.ToInt64()))
		atomic.CompareAndSwapUint32(&c.dispatchFailed, 0, 1)
		// if NeedTriggerFallback is true, we return timeout to trigger tikv's fallback
		if c.needTriggerFallback {
			err = derr.ErrTiFlashServerTimeout
		}
		c.sendError(err)
		return
	}

	if rpcResp.Error != nil {
		logutil.BgLogger().Warn("mpp dispatch response meet error", zap.String("error", rpcResp.Error.Msg), zap.Uint64("timestamp", req.StartTs), zap.Int64("task", req.ID), zap.Int64("task-mpp-version", req.MppVersion.ToInt64()), zap.Int64("error-mpp-version", rpcResp.Error.GetMppVersion()))
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
	var stream *tikvrpc.MPPStreamResponse
	var err error
	var retry bool
	for {
		stream, retry, err = c.sessionCtx.GetMPPClient().EstablishMPPConns(kv.EstablishMPPConnsParam{Ctx: bo.GetCtx(), Req: req, TaskMeta: taskMeta, Bo: bo.TiKVBackoffer()})
		if retry {
			logutil.BgLogger().Warn("establish mpp connection meet error and retrying", zap.Error(err), zap.Uint64("timestamp", c.startTS), zap.Int64("task", req.ID), zap.Int64("mpp-version", req.MppVersion.ToInt64()))
			continue
		}
		break
	}
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
							RecordOneCopTask(-1, kv.TiFlash, detail)] = 0
					}
				}
				if ruDetailsRaw := c.ctx.Value(clientutil.RUDetailsCtxKey); ruDetailsRaw != nil {
					if err := execdetails.MergeTiFlashRUConsumption(report.executionSummaries, ruDetailsRaw.(*clientutil.RUDetails)); err != nil {
						return err
					}
				}
			}
			distsql.FillDummySummariesForTiFlashTasks(c.sessionCtx.GetSessionVars().StmtCtx.RuntimeStatsColl, kv.TiFlash, c.planIDs, recordedPlanIDs)
		case <-time.After(receiveReportTimeout):
			metrics.MppCoordinatorStatsReportNotReceived.Inc()
			logutil.BgLogger().Info(fmt.Sprintf("Mpp coordinator not received all reports within %d ms", int(receiveReportTimeout.Milliseconds())),
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
	if c.cancelFunc != nil {
		c.cancelFunc()
		<-c.wgDoneChan
	}
}

