// Copyright 2016 PingCAP, Inc.
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
	"time"
	"unsafe"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/kv"
	derr "github.com/pingcap/tidb/pkg/store/driver/error"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/paging"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/txnkv/txnlock"
	"go.uber.org/zap"
)

// handle the batched cop response.
// tasks will be changed, so the input tasks should not be used after calling this function.
func (worker *copIteratorWorker) handleBatchCopResponse(bo *Backoffer, rpcCtx *tikv.RPCContext, resp *coprocessor.Response,
	tasks map[uint64]*batchedCopTask) (batchRespList []*copResponse, remainTasks []*copTask, err error) {
	if len(tasks) == 0 {
		return nil, nil, nil
	}
	batchedNum := len(tasks)
	busyThresholdFallback := false
	defer func() {
		if err != nil {
			return
		}
		if !busyThresholdFallback {
			worker.storeBatchedNum.Add(uint64(batchedNum - len(remainTasks)))
			worker.storeBatchedFallbackNum.Add(uint64(len(remainTasks)))
		}
	}()
	appendRemainTasks := func(tasks ...*copTask) {
		if remainTasks == nil {
			// allocate size of remain length
			remainTasks = make([]*copTask, 0, len(tasks))
		}
		remainTasks = append(remainTasks, tasks...)
	}
	// need Addr for recording details.
	var dummyRPCCtx *tikv.RPCContext
	if rpcCtx != nil {
		dummyRPCCtx = &tikv.RPCContext{
			Addr: rpcCtx.Addr,
		}
	}
	batchResps := resp.GetBatchResponses()
	batchRespList = make([]*copResponse, 0, len(batchResps))
	for _, batchResp := range batchResps {
		taskID := batchResp.GetTaskId()
		batchedTask, ok := tasks[taskID]
		if !ok {
			return batchRespList, nil, errors.Errorf("task id %d not found", batchResp.GetTaskId())
		}
		delete(tasks, taskID)
		resp := &copResponse{
			pbResp: &coprocessor.Response{
				Data:          batchResp.Data,
				ExecDetailsV2: batchResp.ExecDetailsV2,
			},
		}
		task := batchedTask.task
		failpoint.Inject("batchCopRegionError", func() {
			batchResp.RegionError = &errorpb.Error{}
		})
		if regionErr := getRegionError(bo.GetCtx(), batchResp); regionErr != nil {
			errStr := fmt.Sprintf("region_id:%v, region_ver:%v, store_type:%s, peer_addr:%s, error:%s",
				task.region.GetID(), task.region.GetVer(), task.storeType.Name(), task.storeAddr, regionErr.String())
			if err := bo.Backoff(tikv.BoRegionMiss(), errors.New(errStr)); err != nil {
				return batchRespList, nil, errors.Trace(err)
			}
			remains, err := buildCopTasks(bo, task.ranges, &buildCopTaskOpt{
				req:                         worker.req,
				cache:                       worker.store.GetRegionCache(),
				respChan:                    false,
				eventCb:                     task.eventCb,
				ignoreTiKVClientReadTimeout: true,
				skipBuckets:                 task.skipBuckets,
				exceedsBoundRetry:           task.exceedsBoundRetry,
			})
			if err != nil {
				return batchRespList, nil, err
			}
			appendRemainTasks(remains...)
			continue
		}
		//TODO: handle locks in batch
		if lockErr := batchResp.GetLocked(); lockErr != nil {
			if err := worker.handleLockErr(bo, resp.pbResp.GetLocked(), task); err != nil {
				return batchRespList, nil, err
			}
			task.meetLockFallback = true
			appendRemainTasks(task)
			continue
		}
		if otherErr := batchResp.GetOtherError(); otherErr != "" {
			err := errors.Errorf("other error: %s", otherErr)

			firstRangeStartKey := task.ranges.At(0).StartKey
			lastRangeEndKey := task.ranges.At(task.ranges.Len() - 1).EndKey

			logutil.Logger(bo.GetCtx()).Warn("other error",
				zap.Uint64("txnStartTS", worker.req.StartTs),
				zap.Uint64("regionID", task.region.GetID()),
				zap.Uint64("regionVer", task.region.GetVer()),
				zap.Uint64("regionConfVer", task.region.GetConfVer()),
				zap.Uint64("bucketsVer", task.bucketsVer),
				// TODO: add bucket version in log
				//zap.Uint64("latestBucketsVer", batchResp.GetLatestBucketsVersion()),
				zap.Int("rangeNums", task.ranges.Len()),
				zap.ByteString("firstRangeStartKey", firstRangeStartKey),
				zap.ByteString("lastRangeEndKey", lastRangeEndKey),
				zap.String("storeAddr", task.storeAddr),
				zap.Error(err))
			if strings.Contains(err.Error(), "write conflict") {
				return batchRespList, nil, kv.ErrWriteConflict.FastGen("%s", otherErr)
			}
			return batchRespList, nil, errors.Trace(err)
		}
		if err := worker.handleCollectExecutionInfo(bo, dummyRPCCtx, resp); err != nil {
			return batchRespList, nil, err
		}
		worker.checkRespOOM(resp)
		batchRespList = append(batchRespList, resp)
	}
	for _, t := range tasks {
		task := t.task
		// when the error is generated by client or a load-based server busy,
		// response is empty by design, skip warning for this case.
		if len(batchResps) != 0 {
			firstRangeStartKey := task.ranges.At(0).StartKey
			lastRangeEndKey := task.ranges.At(task.ranges.Len() - 1).EndKey
			logutil.Logger(bo.GetCtx()).Error("response of batched task missing",
				zap.Uint64("id", task.taskID),
				zap.Uint64("txnStartTS", worker.req.StartTs),
				zap.Uint64("regionID", task.region.GetID()),
				zap.Uint64("regionVer", task.region.GetVer()),
				zap.Uint64("regionConfVer", task.region.GetConfVer()),
				zap.Uint64("bucketsVer", task.bucketsVer),
				zap.Int("rangeNums", task.ranges.Len()),
				zap.ByteString("firstRangeStartKey", firstRangeStartKey),
				zap.ByteString("lastRangeEndKey", lastRangeEndKey),
				zap.String("storeAddr", task.storeAddr))
		}
		appendRemainTasks(t.task)
	}
	if regionErr := getRegionError(bo.GetCtx(), resp); regionErr != nil && regionErr.ServerIsBusy != nil &&
		regionErr.ServerIsBusy.EstimatedWaitMs > 0 && len(remainTasks) != 0 {
		if len(batchResps) != 0 {
			return batchRespList, nil, errors.New("store batched coprocessor with server is busy error shouldn't contain responses")
		}
		busyThresholdFallback = true
		handler := newBatchTaskBuilder(bo, worker.req, worker.store.GetRegionCache(), kv.ReplicaReadFollower)
		for _, task := range remainTasks {
			// do not set busy threshold again.
			task.busyThreshold = 0
			if err = handler.handle(task); err != nil {
				return batchRespList, nil, err
			}
		}
		remainTasks = handler.build()
	}
	return batchRespList, remainTasks, nil
}

func (worker *copIteratorWorker) handleLockErr(bo *Backoffer, lockErr *kvrpcpb.LockInfo, task *copTask) error {
	if lockErr == nil {
		return nil
	}
	resolveLockDetail := worker.getLockResolverDetails()
	// Be care that we didn't redact the SQL statement because the log is DEBUG level.
	if task.eventCb != nil {
		task.eventCb(trxevents.WrapCopMeetLock(&trxevents.CopMeetLock{
			LockInfo: lockErr,
		}))
	} else {
		logutil.Logger(bo.GetCtx()).Debug("coprocessor encounters lock",
			zap.Stringer("lock", lockErr))
	}
	var locks []*txnlock.Lock
	if sharedLocks := lockErr.GetSharedLockInfos(); len(sharedLocks) > 0 {
		locks = make([]*txnlock.Lock, 0, len(sharedLocks))
		for _, l := range sharedLocks {
			locks = append(locks, txnlock.NewLock(l))
		}
	} else {
		locks = []*txnlock.Lock{txnlock.NewLock(lockErr)}
	}
	resolveLocksOpts := txnlock.ResolveLocksOptions{
		CallerStartTS: worker.req.StartTs,
		Locks:         locks,
		Detail:        resolveLockDetail,
	}
	resolveLocksRes, err1 := worker.kvclient.ResolveLocksWithOpts(bo.TiKVBackoffer(), resolveLocksOpts)
	err1 = derr.ToTiDBErr(err1)
	if err1 != nil {
		return errors.Trace(err1)
	}
	msBeforeExpired := resolveLocksRes.TTL
	if msBeforeExpired > 0 {
		if err := bo.BackoffWithMaxSleepTxnLockFast(int(msBeforeExpired), errors.New(lockErr.String())); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (worker *copIteratorWorker) buildCacheKey(task *copTask, copReq *coprocessor.Request) (cacheKey []byte, cacheValue *coprCacheValue) {
	// If there are many ranges, it is very likely to be a TableLookupRequest. They are not worth to cache since
	// computing is not the main cost. Ignore requests with many ranges directly to avoid slowly building the cache key.
	if task.cmdType == tikvrpc.CmdCop && worker.store.coprCache != nil && worker.req.Cacheable && worker.store.coprCache.CheckRequestAdmission(len(copReq.Ranges)) {
		cKey, err := coprCacheBuildKey(copReq)
		if err == nil {
			cacheKey = cKey
			cValue := worker.store.coprCache.Get(cKey)
			copReq.IsCacheEnabled = true

			if cValue != nil && cValue.RegionID == task.region.GetID() && cValue.TimeStamp <= worker.req.StartTs {
				// Append cache version to the request to skip Coprocessor computation if possible
				// when request result is cached
				copReq.CacheIfMatchVersion = cValue.RegionDataVersion
				cacheValue = cValue
			} else {
				copReq.CacheIfMatchVersion = 0
			}
		} else {
			logutil.BgLogger().Warn("Failed to build copr cache key", zap.Error(err))
		}
	}
	return
}

func (worker *copIteratorWorker) handleCopCache(task *copTask, resp *copResponse, cacheKey []byte, cacheValue *coprCacheValue) error {
	if resp.pbResp.IsCacheHit {
		if cacheValue == nil {
			return errors.New("Internal error: received illegal TiKV response")
		}
		copr_metrics.CoprCacheCounterHit.Add(1)
		// Cache hit and is valid: use cached data as response data and we don't update the cache.
		data := slices.Clone(cacheValue.Data)
		resp.pbResp.Data = data
		if worker.req.Paging.Enable {
			var start, end []byte
			if cacheValue.PageStart != nil {
				start = slices.Clone(cacheValue.PageStart)
			}
			if cacheValue.PageEnd != nil {
				end = slices.Clone(cacheValue.PageEnd)
			}
			// When paging protocol is used, the response key range is part of the cache data.
			if start != nil || end != nil {
				resp.pbResp.Range = &coprocessor.KeyRange{
					Start: start,
					End:   end,
				}
			} else {
				resp.pbResp.Range = nil
			}
		}
		// `worker.enableCollectExecutionInfo` is loaded from the instance's config. Because it's not related to the request,
		// the cache key can be same when `worker.enableCollectExecutionInfo` is true or false.
		// When `worker.enableCollectExecutionInfo` is false, the `resp.detail` is nil, and hit cache is still possible.
		// Check `resp.detail` to avoid panic.
		// Details: https://github.com/pingcap/tidb/issues/48212
		if resp.detail != nil {
			resp.detail.CoprCacheHit = true
		}
		return nil
	}
	copr_metrics.CoprCacheCounterMiss.Add(1)
	// Cache not hit or cache hit but not valid: update the cache if the response can be cached.
	if cacheKey != nil && resp.pbResp.CanBeCached && resp.pbResp.CacheLastVersion > 0 {
		if resp.detail != nil {
			if worker.store.coprCache.CheckResponseAdmission(resp.pbResp.Data.Size(), resp.detail.TimeDetail.ProcessTime, task.pagingTaskIdx) {
				data := slices.Clone(resp.pbResp.Data)

				newCacheValue := coprCacheValue{
					Data:              data,
					TimeStamp:         worker.req.StartTs,
					RegionID:          task.region.GetID(),
					RegionDataVersion: resp.pbResp.CacheLastVersion,
				}
				// When paging protocol is used, the response key range is part of the cache data.
				if r := resp.pbResp.GetRange(); r != nil {
					newCacheValue.PageStart = slices.Clone(r.GetStart())
					newCacheValue.PageEnd = slices.Clone(r.GetEnd())
				}
				worker.store.coprCache.Set(cacheKey, &newCacheValue)
			}
		}
	}
	return nil
}

func (worker *copIteratorWorker) getLockResolverDetails() *util.ResolveLockDetail {
	if worker.stats == nil {
		return nil
	}
	return &util.ResolveLockDetail{}
}

func (worker *copIteratorWorker) handleCollectExecutionInfo(bo *Backoffer, rpcCtx *tikv.RPCContext, resp *copResponse) error {
	if worker.stats == nil {
		return nil
	}
	failpoint.Inject("disable-collect-execution", func(val failpoint.Value) {
		if val.(bool) {
			panic("shouldn't reachable")
		}
	})
	if resp.detail == nil {
		resp.detail = new(CopRuntimeStats)
		resp.detail.ScanDetail = &util.ScanDetail{}
	}
	return worker.collectCopRuntimeStats(resp.detail, bo, rpcCtx, resp)
}

func (worker *copIteratorWorker) collectCopRuntimeStats(copStats *CopRuntimeStats, bo *Backoffer, rpcCtx *tikv.RPCContext, resp *copResponse) error {
	worker.collectKVClientRuntimeStats(copStats, bo, rpcCtx)
	if resp == nil {
		return nil
	}
	if pbDetails := resp.pbResp.ExecDetailsV2; pbDetails != nil {
		// Take values in `ExecDetailsV2` first.
		if pbDetails.TimeDetail != nil || pbDetails.TimeDetailV2 != nil {
			copStats.TimeDetail.MergeFromTimeDetail(pbDetails.TimeDetailV2, pbDetails.TimeDetail)
		}
		if scanDetailV2 := pbDetails.ScanDetailV2; scanDetailV2 != nil {
			copStats.ScanDetail.MergeFromScanDetailV2(scanDetailV2)
		}
	} else if pbDetails := resp.pbResp.ExecDetails; pbDetails != nil {
		if timeDetail := pbDetails.TimeDetail; timeDetail != nil {
			copStats.TimeDetail.MergeFromTimeDetail(nil, timeDetail)
		}
		if scanDetail := pbDetails.ScanDetail; scanDetail != nil {
			if scanDetail.Write != nil {
				copStats.ScanDetail.ProcessedKeys = scanDetail.Write.Processed
				copStats.ScanDetail.TotalKeys = scanDetail.Write.Total
			}
		}
	}

	if worker.req.RunawayChecker != nil {
		var ruDetail *util.RUDetails
		if ruDetailRaw := bo.GetCtx().Value(util.RUDetailsCtxKey); ruDetailRaw != nil {
			ruDetail = ruDetailRaw.(*util.RUDetails)
		}
		if err := worker.req.RunawayChecker.CheckThresholds(ruDetail, copStats.ScanDetail.ProcessedKeys, nil); err != nil {
			return err
		}
	}
	return nil
}

func (worker *copIteratorWorker) collectKVClientRuntimeStats(copStats *CopRuntimeStats, bo *Backoffer, rpcCtx *tikv.RPCContext) {
	if rpcCtx != nil {
		copStats.CalleeAddress = rpcCtx.Addr
	}
	if worker.kvclient.Stats == nil {
		return
	}
	defer func() {
		worker.kvclient.Stats = nil
	}()
	copStats.ReqStats = worker.kvclient.Stats
	backoffTimes := bo.GetBackoffTimes()
	if len(backoffTimes) > 0 {
		copStats.BackoffTime = time.Duration(bo.GetTotalSleep()) * time.Millisecond
		copStats.BackoffSleep = make(map[string]time.Duration, len(backoffTimes))
		copStats.BackoffTimes = make(map[string]int, len(backoffTimes))
		for backoff := range backoffTimes {
			copStats.BackoffTimes[backoff] = backoffTimes[backoff]
			copStats.BackoffSleep[backoff] = time.Duration(bo.GetBackoffSleepMS()[backoff]) * time.Millisecond
		}
	}
}

func (worker *copIteratorWorker) collectUnconsumedCopRuntimeStats(bo *Backoffer, rpcCtx *tikv.RPCContext) {
	if worker.kvclient.Stats != nil && worker.stats != nil {
		copStats := &CopRuntimeStats{}
		worker.collectKVClientRuntimeStats(copStats, bo, rpcCtx)
		worker.stats.Lock()
		worker.stats.stats = append(worker.stats.stats, copStats)
		worker.stats.Unlock()
	}
}
