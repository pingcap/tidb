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
	"bytes"
	"context"
	"fmt"
	"io"
	"slices"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/store/driver/backoff"
	derr "github.com/pingcap/tidb/pkg/store/driver/error"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/tiflashcompute"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"go.uber.org/zap"
)

func (b *batchCopIterator) handleTask(ctx context.Context, bo *Backoffer, task *batchCopTask) {
	tasks := []*batchCopTask{task}
	// Cannot change this to idx:=range tasks, since it changes the ranges within the for loop
	for idx := 0; idx < len(tasks); idx++ {
		ret, err := b.handleTaskOnce(ctx, bo, tasks[idx])
		if err != nil {
			resp := &batchCopResponse{err: errors.Trace(err), detail: new(CopRuntimeStats)}
			b.sendToRespCh(resp)
			break
		}
		tasks = append(tasks, ret...)
	}
	b.wg.Done()
}

// Merge all ranges and request again.
func (b *batchCopIterator) retryBatchCopTask(ctx context.Context, bo *backoff.Backoffer, batchTask *batchCopTask) ([]*batchCopTask, error) {
	if batchTask.regionInfos != nil {
		var ranges []kv.KeyRange
		for _, ri := range batchTask.regionInfos {
			ri.Ranges.Do(func(ran *kv.KeyRange) {
				ranges = append(ranges, *ran)
			})
		}
		// need to make sure the key ranges is sorted
		slices.SortFunc(ranges, func(i, j kv.KeyRange) int {
			return bytes.Compare(i.StartKey, j.StartKey)
		})
		ret, err := buildBatchCopTasksForNonPartitionedTable(ctx, bo, b.store, NewKeyRanges(ranges), b.req.StoreType, false, 0, false, 0, tiflashcompute.DispatchPolicyInvalid, b.tiflashReplicaReadPolicy, b.appendWarning)
		return ret, err
	}
	// Retry Partition Table Scan
	keyRanges := make([]*KeyRanges, 0, len(batchTask.PartitionTableRegions))
	pid := make([]int64, 0, len(batchTask.PartitionTableRegions))
	for _, trs := range batchTask.PartitionTableRegions {
		pid = append(pid, trs.PhysicalTableId)
		ranges := make([]kv.KeyRange, 0, len(trs.Regions))
		for _, ri := range trs.Regions {
			for _, ran := range ri.Ranges {
				ranges = append(ranges, kv.KeyRange{
					StartKey: ran.Start,
					EndKey:   ran.End,
				})
			}
		}
		// need to make sure the key ranges is sorted
		slices.SortFunc(ranges, func(i, j kv.KeyRange) int {
			return bytes.Compare(i.StartKey, j.StartKey)
		})
		keyRanges = append(keyRanges, NewKeyRanges(ranges))
	}
	ret, err := buildBatchCopTasksForPartitionedTable(ctx, bo, b.store, keyRanges, b.req.StoreType, false, 0, false, 0, pid, tiflashcompute.DispatchPolicyInvalid, b.tiflashReplicaReadPolicy, b.appendWarning)
	return ret, err
}

// TiFlashReadTimeoutUltraLong represents the max time that tiflash request may take, since it may scan many regions for tiflash.
const TiFlashReadTimeoutUltraLong = 3600 * time.Second

func (b *batchCopIterator) handleTaskOnce(ctx context.Context, bo *backoff.Backoffer, task *batchCopTask) ([]*batchCopTask, error) {
	sender := NewRegionBatchRequestSender(b.store.GetRegionCache(), b.store.GetTiKVClient(), b.store.store.GetOracle(), b.enableCollectExecutionInfo)
	var regionInfos = make([]*coprocessor.RegionInfo, 0, len(task.regionInfos))
	for _, ri := range task.regionInfos {
		regionInfos = append(regionInfos, ri.toCoprocessorRegionInfo())
	}

	copReq := coprocessor.BatchRequest{
		Tp:              b.req.Tp,
		StartTs:         b.req.StartTs,
		Data:            b.req.Data,
		SchemaVer:       b.req.SchemaVar,
		Regions:         regionInfos,
		TableRegions:    task.PartitionTableRegions,
		ConnectionId:    b.req.ConnID,
		ConnectionAlias: b.req.ConnAlias,
	}

	rgName := b.req.ResourceGroupName
	if !vardef.EnableResourceControl.Load() {
		rgName = ""
	}
	req := tikvrpc.NewRequest(task.cmdType, &copReq, kvrpcpb.Context{
		IsolationLevel: isolationLevelToPB(b.req.IsolationLevel),
		Priority:       priorityToPB(b.req.Priority),
		NotFillCache:   b.req.NotFillCache,
		RecordTimeStat: true,
		RecordScanStat: true,
		TaskId:         b.req.TaskID,
		ResourceControlContext: &kvrpcpb.ResourceControlContext{
			ResourceGroupName: rgName,
		},
	})
	if b.req.ResourceGroupTagger != nil {
		b.req.ResourceGroupTagger.Build(req)
	}
	req.StoreTp = getEndPointType(kv.TiFlash)

	logutil.BgLogger().Debug("send batch request to ", zap.String("req info", req.String()), zap.Int("cop task len", len(task.regionInfos)))
	resp, retry, cancel, err := sender.SendReqToAddr(bo, task.ctx, task.regionInfos, req, TiFlashReadTimeoutUltraLong)
	// If there are store errors, we should retry for all regions.
	if retry {
		return b.retryBatchCopTask(ctx, bo, task)
	}
	if err != nil {
		err = derr.ToTiDBErr(err)
		return nil, errors.Trace(err)
	}
	defer cancel()
	return nil, b.handleStreamedBatchCopResponse(bo, resp.Resp.(*tikvrpc.BatchCopStreamResponse), task)
}

func (b *batchCopIterator) handleStreamedBatchCopResponse(bo *Backoffer, response *tikvrpc.BatchCopStreamResponse, task *batchCopTask) (err error) {
	defer response.Close()
	resp := response.BatchResponse
	if resp == nil {
		// streaming request returns io.EOF, so the first Response is nil.
		return
	}
	for {
		err = b.handleBatchCopResponse(bo, resp, task)
		if err != nil {
			return errors.Trace(err)
		}
		resp, err = response.Recv()
		if err != nil {
			if errors.Cause(err) == io.EOF {
				return nil
			}

			if err1 := bo.Backoff(tikv.BoTiKVRPC(), errors.Errorf("recv stream response error: %v, task store addr: %s", err, task.storeAddr)); err1 != nil {
				return errors.Trace(err)
			}

			// No coprocessor.Response for network error, rebuild task based on the last success one.
			if errors.Cause(err) == context.Canceled {
				logutil.BgLogger().Info("stream recv timeout", zap.Error(err))
			} else {
				logutil.BgLogger().Info("stream unknown error", zap.Error(err))
			}
			return derr.ErrTiFlashServerTimeout
		}
	}
}

func (b *batchCopIterator) handleBatchCopResponse(bo *Backoffer, response *coprocessor.BatchResponse, task *batchCopTask) (err error) {
	if otherErr := response.GetOtherError(); otherErr != "" {
		err = errors.Errorf("other error: %s", otherErr)
		logutil.BgLogger().Warn("other error",
			zap.Uint64("txnStartTS", b.req.StartTs),
			zap.String("storeAddr", task.storeAddr),
			zap.Error(err))
		return errors.Trace(err)
	}

	if len(response.RetryRegions) > 0 {
		logutil.BgLogger().Info("multiple regions are stale and need to be refreshed", zap.Int("region size", len(response.RetryRegions)))
		for idx, retry := range response.RetryRegions {
			id := tikv.NewRegionVerID(retry.Id, retry.RegionEpoch.ConfVer, retry.RegionEpoch.Version)
			logutil.BgLogger().Info("invalid region because tiflash detected stale region", zap.String("region id", id.String()))
			b.store.GetRegionCache().InvalidateCachedRegionWithReason(id, tikv.EpochNotMatch)
			if idx >= 10 {
				logutil.BgLogger().Info("stale regions are too many, so we omit the rest ones")
				break
			}
		}
		return
	}

	resp := &batchCopResponse{
		pbResp: response,
		detail: new(CopRuntimeStats),
	}

	b.handleCollectExecutionInfo(bo, resp, task)
	b.sendToRespCh(resp)

	return
}

func (b *batchCopIterator) sendToRespCh(resp *batchCopResponse) (exit bool) {
	select {
	case b.respChan <- resp:
	case <-b.finishCh:
		exit = true
	}
	return
}

func (b *batchCopIterator) handleCollectExecutionInfo(bo *Backoffer, resp *batchCopResponse, task *batchCopTask) {
	if !b.enableCollectExecutionInfo {
		return
	}
	backoffTimes := bo.GetBackoffTimes()
	resp.detail.BackoffTime = time.Duration(bo.GetTotalSleep()) * time.Millisecond
	resp.detail.BackoffSleep = make(map[string]time.Duration, len(backoffTimes))
	resp.detail.BackoffTimes = make(map[string]int, len(backoffTimes))
	for backoff := range backoffTimes {
		resp.detail.BackoffTimes[backoff] = backoffTimes[backoff]
		resp.detail.BackoffSleep[backoff] = time.Duration(bo.GetBackoffSleepMS()[backoff]) * time.Millisecond
	}
	resp.detail.CalleeAddress = task.storeAddr
}

// Only called when UseAutoScaler is false.
func buildBatchCopTasksConsistentHashForPD(bo *backoff.Backoffer,
	kvStore *kvStore,
	rangesForEachPhysicalTable []*KeyRanges,
	storeType kv.StoreType,
	ttl time.Duration,
	dispatchPolicy tiflashcompute.DispatchPolicy) (res []*batchCopTask, err error) {
	failpointCheckWhichPolicy(dispatchPolicy)
	const cmdType = tikvrpc.CmdBatchCop
	var (
		retryNum        int
		rangesLen       int
		copTaskNum      int
		splitKeyElapsed time.Duration
		getStoreElapsed time.Duration
	)
	cache := kvStore.GetRegionCache()
	start := time.Now()

	for {
		retryNum++
		rangesLen = 0
		tasks := make([]*copTask, 0)
		regionIDs := make([]tikv.RegionVerID, 0)

		splitKeyStart := time.Now()
		for i, ranges := range rangesForEachPhysicalTable {
			rangesLen += ranges.Len()
			locations, err := cache.SplitKeyRangesByLocations(bo, ranges, UnspecifiedLimit, false, false)
			if err != nil {
				return nil, errors.Trace(err)
			}
			for _, lo := range locations {
				tasks = append(tasks, &copTask{
					region:         lo.Location.Region,
					ranges:         lo.Ranges,
					cmdType:        cmdType,
					storeType:      storeType,
					partitionIndex: int64(i),
				})
				regionIDs = append(regionIDs, lo.Location.Region)
			}
		}
		splitKeyElapsed += time.Since(splitKeyStart)

		getStoreStart := time.Now()
		stores, err := cache.GetTiFlashComputeStores(bo.TiKVBackoffer())
		if err != nil {
			return nil, err
		}
		stores = filterAliveStores(bo.GetCtx(), stores, ttl, kvStore)
		if len(stores) == 0 {
			if intest.InTest {
				// To avoid keep retrying, which makes CI slow.
				return nil, errors.New("tiflash_compute node is unavailable")
			}
			logutil.BgLogger().Info("buildBatchCopTasksConsistentHashForPD retry because no alive tiflash", zap.Int("retryNum", retryNum))
			cache.InvalidateTiFlashComputeStores()
			if err := bo.Backoff(tikv.BoTiFlashRPC(), errors.New("tiflash_compute node is unavailable")); err != nil {
				return nil, errors.Trace(err)
			}
			getStoreElapsed += time.Since(getStoreStart)
			continue
		}
		getStoreElapsed = time.Since(getStoreStart)

		storesStr := make([]string, 0, len(stores))
		for _, s := range stores {
			storesStr = append(storesStr, s.GetAddr())
		}
		var rpcCtxs []*tikv.RPCContext
		if dispatchPolicy == tiflashcompute.DispatchPolicyRR {
			rpcCtxs, err = getTiFlashComputeRPCContextByRoundRobin(regionIDs, storesStr)
		} else if dispatchPolicy == tiflashcompute.DispatchPolicyConsistentHash {
			rpcCtxs, err = getTiFlashComputeRPCContextByConsistentHash(regionIDs, storesStr)
		} else {
			err = errors.Errorf("unexpected dispatch policy %v", dispatchPolicy)
		}
		if err != nil {
			return nil, err
		}
		if rpcCtxs == nil {
			logutil.BgLogger().Info("buildBatchCopTasksConsistentHashForPD retry because rcpCtx is nil", zap.Int("retryNum", retryNum))
			if err := bo.Backoff(tikv.BoTiFlashRPC(), errors.New("Cannot find region with TiFlash peer")); err != nil {
				return nil, errors.Trace(err)
			}
			continue
		}
		if len(rpcCtxs) != len(tasks) {
			return nil, errors.Errorf("length should be equal, len(rpcCtxs): %d, len(tasks): %d", len(rpcCtxs), len(tasks))
		}
		copTaskNum = len(tasks)
		taskMap := make(map[string]*batchCopTask)
		for i, rpcCtx := range rpcCtxs {
			regionInfo := RegionInfo{
				// tasks and rpcCtxs are correspond to each other.
				Region:         tasks[i].region,
				Ranges:         tasks[i].ranges,
				PartitionIndex: tasks[i].partitionIndex,
			}
			if batchTask, ok := taskMap[rpcCtx.Addr]; ok {
				batchTask.regionInfos = append(batchTask.regionInfos, regionInfo)
			} else {
				batchTask := &batchCopTask{
					storeAddr:   rpcCtx.Addr,
					cmdType:     cmdType,
					ctx:         rpcCtx,
					regionInfos: []RegionInfo{regionInfo},
				}
				taskMap[rpcCtx.Addr] = batchTask
				res = append(res, batchTask)
			}
		}
		logutil.BgLogger().Info("buildBatchCopTasksConsistentHashForPD done",
			zap.Int("len(tasks)", len(taskMap)),
			zap.Int("len(tiflash_compute)", len(stores)),
			zap.String("dispatchPolicy", tiflashcompute.GetDispatchPolicy(dispatchPolicy)))
		if log.GetLevel() <= zap.DebugLevel {
			debugTaskMap := make(map[string]string, len(taskMap))
			for s, b := range taskMap {
				debugTaskMap[s] = fmt.Sprintf("addr: %s; regionInfos: %v", b.storeAddr, b.regionInfos)
			}
			logutil.BgLogger().Debug("detailed info buildBatchCopTasksConsistentHashForPD", zap.Any("taskMap", debugTaskMap), zap.Strings("allStores", storesStr))
		}
		break
	}

	if elapsed := time.Since(start); elapsed > time.Millisecond*500 {
		logutil.BgLogger().Warn("buildBatchCopTasksConsistentHashForPD takes too much time",
			zap.Duration("total elapsed", elapsed),
			zap.Int("retryNum", retryNum),
			zap.Duration("splitKeyElapsed", splitKeyElapsed),
			zap.Duration("getStoreElapsed", getStoreElapsed),
			zap.Int("range len", rangesLen),
			zap.Int("copTaskNum", copTaskNum),
			zap.Int("batchCopTaskNum", len(res)))
	}
	failpointCheckForConsistentHash(res)
	return res, nil
}
