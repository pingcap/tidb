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
	"strconv"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/mpp"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/store/driver/backoff"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/tiflash"
	"github.com/pingcap/tidb/pkg/util/tiflashcompute"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// MPPClient servers MPP requests.
type MPPClient struct {
	store *kvStore
}

type mppStoreCnt struct {
	cnt        int32
	lastUpdate int64
	initFlag   int32
}

// GetAddress returns the network address.
func (c *batchCopTask) GetAddress() string {
	return c.storeAddr
}

// ConstructMPPTasks receives ScheduleRequest, which are actually collects of kv ranges. We allocates MPPTaskMeta for them and returns.
func (c *MPPClient) ConstructMPPTasks(ctx context.Context, req *kv.MPPBuildTasksRequest, ttl time.Duration, dispatchPolicy tiflashcompute.DispatchPolicy, tiflashReplicaReadPolicy tiflash.ReplicaRead, appendWarning func(error)) ([]kv.MPPTaskMeta, error) {
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
		tasks, err = buildBatchCopTasksForPartitionedTable(ctx, bo, c.store, rangesForEachPartition, kv.TiFlash, true, ttl, true, 20, partitionIDs, dispatchPolicy, tiflashReplicaReadPolicy, appendWarning)
	} else {
		if req.KeyRanges == nil {
			return nil, errors.New("KeyRanges in MPPBuildTasksRequest is nil")
		}
		ranges := NewKeyRanges(req.KeyRanges)
		tasks, err = buildBatchCopTasksForNonPartitionedTable(ctx, bo, c.store, ranges, kv.TiFlash, true, ttl, true, 20, dispatchPolicy, tiflashReplicaReadPolicy, appendWarning)
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

// DispatchMPPTask dispatch mpp task, and returns valid response when retry = false and err is nil
func (c *MPPClient) DispatchMPPTask(param kv.DispatchMPPTaskParam) (resp *mpp.DispatchTaskResponse, retry bool, err error) {
	req := param.Req
	var regionInfos []*coprocessor.RegionInfo
	originalTask, ok := req.Meta.(*batchCopTask)
	if ok {
		for _, ri := range originalTask.regionInfos {
			regionInfos = append(regionInfos, ri.toCoprocessorRegionInfo())
		}
	}

	// meta for current task.
	taskMeta := &mpp.TaskMeta{StartTs: req.StartTs, QueryTs: req.MppQueryID.QueryTs, LocalQueryId: req.MppQueryID.LocalQueryID, TaskId: req.ID, ServerId: req.MppQueryID.ServerID,
		GatherId:               req.GatherID,
		Address:                req.Meta.GetAddress(),
		CoordinatorAddress:     req.CoordinatorAddress,
		ReportExecutionSummary: req.ReportExecutionSummary,
		MppVersion:             req.MppVersion.ToInt64(),
		ResourceGroupName:      req.ResourceGroupName,
		ConnectionId:           req.ConnectionID,
		ConnectionAlias:        req.ConnectionAlias,
	}

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
	wrappedReq.StoreTp = getEndPointType(kv.TiFlash)

	// TODO: Handle dispatch task response correctly, including retry logic and cancel logic.
	var rpcResp *tikvrpc.Response
	invalidPDCache := config.GetGlobalConfig().DisaggregatedTiFlash && !config.GetGlobalConfig().UseAutoScaler
	bo := backoff.NewBackofferWithTikvBo(param.Bo)

	// If copTasks is not empty, we should send request according to region distribution.
	// Or else it's the task without region, which always happens in high layer task without table.
	// In that case
	if originalTask != nil {
		sender := NewRegionBatchRequestSender(c.store.GetRegionCache(), c.store.GetTiKVClient(), param.EnableCollectExecutionInfo)
		rpcResp, retry, _, err = sender.SendReqToAddr(bo, originalTask.ctx, originalTask.regionInfos, wrappedReq, tikv.ReadTimeoutMedium)
		// No matter what the rpc error is, we won't retry the mpp dispatch tasks.
		// TODO: If we want to retry, we must redo the plan fragment cutting and task scheduling.
		// That's a hard job but we can try it in the future.
		if sender.GetRPCError() != nil {
			logutil.BgLogger().Warn("mpp dispatch meet io error", zap.String("error", sender.GetRPCError().Error()), zap.Uint64("timestamp", taskMeta.StartTs), zap.Int64("task", taskMeta.TaskId), zap.Int64("mpp-version", taskMeta.MppVersion))
			if invalidPDCache {
				c.store.GetRegionCache().InvalidateTiFlashComputeStores()
			}
			err = sender.GetRPCError()
		}
	} else {
		rpcResp, err = c.store.GetTiKVClient().SendRequest(param.Ctx, req.Meta.GetAddress(), wrappedReq, tikv.ReadTimeoutMedium)
		if errors.Cause(err) == context.Canceled || status.Code(errors.Cause(err)) == codes.Canceled {
			retry = false
		} else if err != nil {
			if invalidPDCache {
				c.store.GetRegionCache().InvalidateTiFlashComputeStores()
			}
			if bo.Backoff(tikv.BoTiFlashRPC(), err) == nil {
				retry = true
			}
		}
	}

	if err != nil || retry {
		return nil, retry, err
	}

	realResp := rpcResp.Resp.(*mpp.DispatchTaskResponse)
	if realResp.Error != nil {
		return realResp, false, nil
	}

	if len(realResp.RetryRegions) > 0 {
		logutil.BgLogger().Info("TiFlash found " + strconv.Itoa(len(realResp.RetryRegions)) + " stale regions. Only first " + strconv.Itoa(min(10, len(realResp.RetryRegions))) + " regions will be logged if the log level is higher than Debug")
		for index, retry := range realResp.RetryRegions {
			id := tikv.NewRegionVerID(retry.Id, retry.RegionEpoch.ConfVer, retry.RegionEpoch.Version)
			if index < 10 || log.GetLevel() <= zap.DebugLevel {
				logutil.BgLogger().Info("invalid region because tiflash detected stale region", zap.String("region id", id.String()))
			}
			c.store.GetRegionCache().InvalidateCachedRegionWithReason(id, tikv.EpochNotMatch)
		}
	}
	return realResp, retry, err
}

// CancelMPPTasks cancels mpp tasks
// NOTE: We do not retry here, because retry is helpless when errors result from TiFlash or Network. If errors occur, the execution on TiFlash will finally stop after some minutes.
// This function is exclusively called, and only the first call succeeds sending tasks and setting all tasks as cancelled, while others will not work.
func (c *MPPClient) CancelMPPTasks(param kv.CancelMPPTasksParam) {
	usedStoreAddrs := param.StoreAddr
	reqs := param.Reqs
	if len(usedStoreAddrs) == 0 || len(reqs) == 0 {
		return
	}

	firstReq := reqs[0]
	killReq := &mpp.CancelTaskRequest{
		Meta: &mpp.TaskMeta{StartTs: firstReq.StartTs, GatherId: firstReq.GatherID, QueryTs: firstReq.MppQueryID.QueryTs, LocalQueryId: firstReq.MppQueryID.LocalQueryID, ServerId: firstReq.MppQueryID.ServerID, MppVersion: firstReq.MppVersion.ToInt64(), ResourceGroupName: firstReq.ResourceGroupName},
	}

	wrappedReq := tikvrpc.NewRequest(tikvrpc.CmdMPPCancel, killReq, kvrpcpb.Context{})
	wrappedReq.StoreTp = getEndPointType(kv.TiFlash)

	// send cancel cmd to all stores where tasks run
	invalidPDCache := config.GetGlobalConfig().DisaggregatedTiFlash && !config.GetGlobalConfig().UseAutoScaler
	wg := util.WaitGroupWrapper{}
	gotErr := atomic.Bool{}
	for addr := range usedStoreAddrs {
		storeAddr := addr
		wg.Run(func() {
			_, err := c.store.GetTiKVClient().SendRequest(context.Background(), storeAddr, wrappedReq, tikv.ReadTimeoutShort)
			logutil.BgLogger().Debug("cancel task", zap.Uint64("query id ", firstReq.StartTs), zap.String("on addr", storeAddr), zap.Int64("mpp-version", firstReq.MppVersion.ToInt64()))
			if err != nil {
				logutil.BgLogger().Error("cancel task error", zap.Error(err), zap.Uint64("query id", firstReq.StartTs), zap.String("on addr", storeAddr), zap.Int64("mpp-version", firstReq.MppVersion.ToInt64()))
				if invalidPDCache {
					gotErr.CompareAndSwap(false, true)
				}
			}
		})
	}
	wg.Wait()
	if invalidPDCache && gotErr.Load() {
		c.store.GetRegionCache().InvalidateTiFlashComputeStores()
	}
}

// EstablishMPPConns build a mpp connection to receive data, return valid response when err is nil
func (c *MPPClient) EstablishMPPConns(param kv.EstablishMPPConnsParam) (*tikvrpc.MPPStreamResponse, error) {
	req := param.Req
	taskMeta := param.TaskMeta
	connReq := &mpp.EstablishMPPConnectionRequest{
		SenderMeta: taskMeta,
		ReceiverMeta: &mpp.TaskMeta{
			StartTs:           req.StartTs,
			GatherId:          req.GatherID,
			QueryTs:           req.MppQueryID.QueryTs,
			LocalQueryId:      req.MppQueryID.LocalQueryID,
			ServerId:          req.MppQueryID.ServerID,
			MppVersion:        req.MppVersion.ToInt64(),
			TaskId:            -1,
			ResourceGroupName: req.ResourceGroupName,
		},
	}

	var err error

	wrappedReq := tikvrpc.NewRequest(tikvrpc.CmdMPPConn, connReq, kvrpcpb.Context{})
	wrappedReq.StoreTp = getEndPointType(kv.TiFlash)

	// Drain results from root task.
	// We don't need to process any special error. When we meet errors, just let it fail.
	rpcResp, err := c.store.GetTiKVClient().SendRequest(param.Ctx, req.Meta.GetAddress(), wrappedReq, TiFlashReadTimeoutUltraLong)

	var stream *tikvrpc.MPPStreamResponse
	if rpcResp != nil && rpcResp.Resp != nil {
		stream = rpcResp.Resp.(*tikvrpc.MPPStreamResponse)
	}

	if err != nil {
		if stream != nil {
			stream.Close()
		}
		logutil.BgLogger().Warn("establish mpp connection meet error and cannot retry", zap.String("error", err.Error()), zap.Uint64("timestamp", taskMeta.StartTs), zap.Int64("task", taskMeta.TaskId), zap.Int64("mpp-version", taskMeta.MppVersion))
		if config.GetGlobalConfig().DisaggregatedTiFlash && !config.GetGlobalConfig().UseAutoScaler {
			c.store.GetRegionCache().InvalidateTiFlashComputeStores()
		}
		return nil, err
	}

	return stream, nil
}

// CheckVisibility checks if it is safe to read using given ts.
func (c *MPPClient) CheckVisibility(startTime uint64) error {
	return c.store.CheckVisibility(startTime)
}

func (c *mppStoreCnt) getMPPStoreCount(ctx context.Context, pdClient pd.Client, TTL int64) (int, error) {
	failpoint.Inject("mppStoreCountSetLastUpdateTime", func(value failpoint.Value) {
		v, _ := strconv.ParseInt(value.(string), 10, 0)
		c.lastUpdate = v
	})

	lastUpdate := atomic.LoadInt64(&c.lastUpdate)
	now := time.Now().UnixMicro()
	isInit := atomic.LoadInt32(&c.initFlag) != 0

	if now-lastUpdate < TTL {
		if isInit {
			return int(atomic.LoadInt32(&c.cnt)), nil
		}
	}

	failpoint.Inject("mppStoreCountSetLastUpdateTimeP2", func(value failpoint.Value) {
		v, _ := strconv.ParseInt(value.(string), 10, 0)
		c.lastUpdate = v
	})

	if !atomic.CompareAndSwapInt64(&c.lastUpdate, lastUpdate, now) {
		if isInit {
			return int(atomic.LoadInt32(&c.cnt)), nil
		}
		// if has't initialized, always fetch latest mpp store info
	}

	// update mpp store cache
	cnt := 0
	stores, err := pdClient.GetAllStores(ctx, pd.WithExcludeTombstone())

	failpoint.Inject("mppStoreCountPDError", func(value failpoint.Value) {
		if value.(bool) {
			err = errors.New("failed to get mpp store count")
		}
	})

	if err != nil {
		// always to update cache next time
		atomic.StoreInt32(&c.initFlag, 0)
		return 0, err
	}
	for _, s := range stores {
		if !tikv.LabelFilterNoTiFlashWriteNode(s.GetLabels()) {
			continue
		}
		cnt += 1
	}
	failpoint.Inject("mppStoreCountSetMPPCnt", func(value failpoint.Value) {
		cnt = value.(int)
	})

	if !isInit || atomic.LoadInt64(&c.lastUpdate) == now {
		atomic.StoreInt32(&c.cnt, int32(cnt))
		atomic.StoreInt32(&c.initFlag, 1)
	}

	return cnt, nil
}

// GetMPPStoreCount returns number of TiFlash stores
func (c *MPPClient) GetMPPStoreCount() (int, error) {
	return c.store.mppStoreCnt.getMPPStoreCount(c.store.store.Ctx(), c.store.store.GetPDClient(), 120*1e6 /* TTL 120sec */)
}
