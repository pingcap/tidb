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
	"sync"
	"time"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/mpp"
	"github.com/pingcap/tidb/pkg/executor/internal/builder"
	"github.com/pingcap/tidb/pkg/executor/internal/util"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/store/copr"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

const (
	receiveReportTimeout = 100 * time.Millisecond
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

	value := sctx.GetSessionVars().StmtCtx.GetPlan()
	if value != nil {
		if p, ok := value.(base.Plan); ok {
			pp := getActualPhysicalPlan(p)
			if pp != nil {
				if len(coordinatorAddr) > 0 && needReportExecutionSummary(pp, coord.originalPlan.ID(), false) {
					coord.reportExecutionInfo = true
				}
			}
		}
	}
	return coord
}

func (c *localMppCoordinator) appendMPPDispatchReq(pf *physicalop.Fragment, allTiFlashZoneInfo map[string]string) error {
	dagReq, err := builder.ConstructDAGReq(c.sessionCtx, []base.PhysicalPlan{pf.Sink}, kv.TiFlash)
	if err != nil {
		return errors.Trace(err)
	}
	for i := range pf.Sink.Schema().Columns {
		dagReq.OutputOffsets = append(dagReq.OutputOffsets, uint32(i))
	}
	if !pf.IsRoot {
		dagReq.EncodeType = tipb.EncodeType_TypeCHBlock
	} else {
		dagReq.EncodeType = tipb.EncodeType_TypeChunk
	}
	zoneHelper := taskZoneInfoHelper{}
	zoneHelper.init(allTiFlashZoneInfo)
	for _, mppTask := range pf.Sink.GetSelfTasks() {
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
		zoneHelper.isRoot = pf.IsRoot
		zoneHelper.currentTaskZone = zoneHelper.allTiFlashZoneInfo[mppTask.Meta.GetAddress()]
		zoneHelper.fillSameZoneFlagForExchange(dagReq.RootExecutor)
		pbData, err := dagReq.Marshal()
		if err != nil {
			return errors.Trace(err)
		}

		rgName := c.sessionCtx.GetSessionVars().StmtCtx.ResourceGroupName
		if !vardef.EnableResourceControl.Load() {
			rgName = ""
		}
		logutil.BgLogger().Info("Dispatch mpp task", zap.Uint64("timestamp", mppTask.StartTs),
			zap.Int64("ID", mppTask.ID), zap.Uint64("QueryTs", mppTask.MppQueryID.QueryTs), zap.Uint64("LocalQueryId", mppTask.MppQueryID.LocalQueryID),
			zap.Uint64("ServerID", mppTask.MppQueryID.ServerID), zap.String("address", mppTask.Meta.GetAddress()),
			zap.String("plan", plannercore.ToString(pf.Sink)),
			zap.Int64("mpp-version", mppTask.MppVersion.ToInt64()),
			zap.String("exchange-compression-mode", pf.Sink.GetCompressionMode().Name()),
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

