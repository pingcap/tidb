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
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/kv"
	tidbmetrics "github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	derr "github.com/pingcap/tidb/pkg/store/driver/error"
	"github.com/pingcap/tidb/pkg/store/driver/options"
	util2 "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/paging"
	"github.com/pingcap/tidb/pkg/util/traceevent"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"go.uber.org/zap"
)

// handleTask handles single copTask, sends the result to channel, retry automatically on error.
func (worker *copIteratorWorker) handleTask(ctx context.Context, task *copTask, respCh chan<- *copResponse) {
	defer func() {
		r := recover()
		if r != nil {
			logutil.BgLogger().Warn("copIteratorWork meet panic",
				zap.Any("r", r),
				zap.Stack("stack trace"))
			resp := &copResponse{err: util2.GetRecoverError(r)}
			// if panic has happened, not checkRespOOM to avoid another panic.
			worker.sendToRespCh(resp, respCh)
		}
	}()
	remainTasks := []*copTask{task}
	backoffermap := make(map[uint64]*Backoffer)
	cancelFuncs := make([]context.CancelFunc, 0)
	defer func() {
		for _, cancel := range cancelFuncs {
			cancel()
		}
	}()
	for len(remainTasks) > 0 {
		curTask := remainTasks[0]
		bo, cancel := chooseBackoffer(ctx, backoffermap, curTask, worker)
		if cancel != nil {
			cancelFuncs = append(cancelFuncs, cancel)
		}
		result, err := worker.handleTaskOnce(bo, curTask)
		if err != nil {
			resp := &copResponse{err: errors.Trace(err)}
			worker.checkRespOOM(resp)
			worker.sendToRespCh(resp, respCh)
			return
		}
		if result != nil {
			if result.resp != nil {
				worker.sendToRespCh(result.resp, respCh)
			}
			for _, resp := range result.batchRespList {
				worker.sendToRespCh(resp, respCh)
			}
		}
		if worker.finished() {
			break
		}
		if result != nil && len(result.remains) > 0 {
			remainTasks = append(result.remains, remainTasks[1:]...)
		} else {
			remainTasks = remainTasks[1:]
		}
	}
}

// handleTaskOnce handles single copTask, successful results are send to channel.
// If error happened, returns error. If region split or meet lock, returns the remain tasks.
func (worker *copIteratorWorker) handleTaskOnce(bo *Backoffer, task *copTask) (*copTaskResult, error) {
	failpoint.Inject("handleTaskOnceError", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(nil, errors.New("mock handleTaskOnce error"))
		}
	})

	if task.paging {
		task.pagingTaskIdx = atomic.AddUint32(worker.pagingTaskIdx, 1)
	}

	copReq := coprocessor.Request{
		Tp:              worker.req.Tp,
		StartTs:         worker.req.StartTs,
		Data:            worker.req.Data,
		Ranges:          task.ranges.ToPBRanges(),
		SchemaVer:       worker.req.SchemaVar,
		PagingSize:      task.pagingSize,
		Tasks:           task.ToPBBatchTasks(),
		ConnectionId:    worker.req.ConnID,
		ConnectionAlias: worker.req.ConnAlias,
	}

	cacheKey, cacheValue := worker.buildCacheKey(task, &copReq)

	replicaRead := worker.req.ReplicaRead
	rgName := worker.req.ResourceGroupName
	if task.storeType == kv.TiFlash && !vardef.EnableResourceControl.Load() {
		// By calling variable.EnableGlobalResourceControlFunc() and setting global variables,
		// tikv/client-go can sense whether the rg function is enabled
		// But for tiflash, it check if rgName is empty to decide if resource control is enabled or not.
		rgName = ""
	}
	req := tikvrpc.NewReplicaReadRequest(task.cmdType, &copReq, options.GetTiKVReplicaReadType(replicaRead), &worker.replicaReadSeed, kvrpcpb.Context{
		IsolationLevel: isolationLevelToPB(worker.req.IsolationLevel),
		Priority:       priorityToPB(worker.req.Priority),
		NotFillCache:   worker.req.NotFillCache,
		RecordTimeStat: true,
		RecordScanStat: true,
		TaskId:         worker.req.TaskID,
		ResourceControlContext: &kvrpcpb.ResourceControlContext{
			ResourceGroupName: rgName,
		},
		BusyThresholdMs: uint32(task.busyThreshold.Milliseconds()),
		BucketsVersion:  task.bucketsVer,
	})
	req.InputRequestSource = task.requestSource.GetRequestSource()
	if task.firstReadType != "" {
		req.ReadType = task.firstReadType
		req.IsRetryRequest = true
	}
	if worker.req.ResourceGroupTagger != nil {
		worker.req.ResourceGroupTagger.Build(req)
	}
	timeout := config.GetGlobalConfig().TiKVClient.CoprReqTimeout
	if task.tikvClientReadTimeout > 0 {
		timeout = time.Duration(task.tikvClientReadTimeout) * time.Millisecond
	}
	failpoint.Inject("sleepCoprRequest", func(v failpoint.Value) {
		//nolint:durationcheck
		time.Sleep(time.Millisecond * time.Duration(v.(int)))
	})

	if worker.req.RunawayChecker != nil {
		if runawayErr := worker.req.RunawayChecker.BeforeCopRequest(req); runawayErr != nil {
			return nil, runawayErr
		}
	}
	req.StoreTp = getEndPointType(task.storeType)
	startTime := time.Now()
	if worker.stats != nil && worker.kvclient.Stats == nil {
		worker.kvclient.Stats = tikv.NewRegionRequestRuntimeStats()
	}
	// set ReadReplicaScope and TxnScope so that req.IsStaleRead will be true when it's a global scope stale read.
	req.ReadReplicaScope = worker.req.ReadReplicaScope
	req.TxnScope = worker.req.TxnScope
	if task.meetLockFallback {
		req.DisableStaleReadMeetLock()
	} else if worker.req.IsStaleness {
		req.EnableStaleWithMixedReplicaRead()
	}
	ops := make([]tikv.StoreSelectorOption, 0, 2)
	if len(worker.req.MatchStoreLabels) > 0 {
		ops = append(ops, tikv.WithMatchLabels(worker.req.MatchStoreLabels))
	}
	if task.redirect2Replica != nil {
		req.ReplicaRead = true
		req.ReplicaReadType = options.GetTiKVReplicaReadType(kv.ReplicaReadFollower)
		ops = append(ops, tikv.WithMatchStores([]uint64{*task.redirect2Replica}))
	}

	failpoint.InjectCall("onBeforeSendReqCtx", req)
	resp, rpcCtx, storeAddr, err := worker.kvclient.SendReqCtx(bo.TiKVBackoffer(), req, task.region,
		timeout, getEndPointType(task.storeType), task.storeAddr, ops...)
	err = derr.ToTiDBErr(err)
	if worker.req.RunawayChecker != nil {
		err = worker.req.RunawayChecker.CheckThresholds(nil, 0, err)
	}
	if err != nil {
		if task.storeType == kv.TiDB {
			return worker.handleTiDBSendReqErr(err, task)
		}
		worker.collectUnconsumedCopRuntimeStats(bo, rpcCtx)
		return nil, errors.Trace(err)
	}

	// Set task.storeAddr field so its task.String() method have the store address information.
	task.storeAddr = storeAddr

	costTime := time.Since(startTime)
	copResp := resp.Resp.(*coprocessor.Response)

	if costTime > minLogCopTaskTime {
		worker.logTimeCopTask(costTime, task, bo, copResp)
	}

	if copResp != nil {
		tidbmetrics.DistSQLCoprRespBodySize.WithLabelValues(storeAddr).Observe(float64(len(copResp.Data) / 1024))
	}

	var result *copTaskResult
	if worker.req.Paging.Enable ||
		copResp.GetRange() != nil { // For next-gen, the storage may return paging range even if paging is not enabled.
		result, err = worker.handleCopPagingResult(bo, rpcCtx, &copResponse{pbResp: copResp}, cacheKey, cacheValue, task, costTime)
	} else {
		// Handles the response for non-paging copTask.
		result, err = worker.handleCopResponse(bo, rpcCtx, &copResponse{pbResp: copResp}, cacheKey, cacheValue, task, costTime)
	}
	if req.ReadType != "" && result != nil {
		for _, remain := range result.remains {
			remain.firstReadType = req.ReadType
		}
	}
	return result, err
}

const (
	minLogBackoffTime   = 100
	minLogKVProcessTime = 100
)

func (worker *copIteratorWorker) logTimeCopTask(costTime time.Duration, task *copTask, bo *Backoffer, resp *coprocessor.Response) {
	logStr := fmt.Sprintf("[TIME_COP_PROCESS] resp_time:%s txnStartTS:%d region_id:%d store_addr:%s", costTime, worker.req.StartTs, task.region.GetID(), task.storeAddr)
	if worker.kvclient.Stats != nil {
		logStr += fmt.Sprintf(" stats:%s", worker.kvclient.Stats.String())
	}
	if bo.GetTotalSleep() > minLogBackoffTime {
		backoffTypes := strings.ReplaceAll(fmt.Sprintf("%v", bo.TiKVBackoffer().GetTypes()), " ", ",")
		logStr += fmt.Sprintf(" backoff_ms:%d backoff_types:%s", bo.GetTotalSleep(), backoffTypes)
	}
	if regionErr := getRegionError(bo.GetCtx(), resp); regionErr != nil {
		logStr += fmt.Sprintf(" region_err:%s", regionErr.String())
	}
	// resp might be nil, but it is safe to call resp.GetXXX here.
	detailV2 := resp.GetExecDetailsV2()
	detail := resp.GetExecDetails()
	var timeDetail *kvrpcpb.TimeDetail
	if detailV2 != nil && detailV2.TimeDetail != nil {
		timeDetail = detailV2.TimeDetail
	} else if detail != nil && detail.TimeDetail != nil {
		timeDetail = detail.TimeDetail
	}
	if timeDetail != nil {
		logStr += fmt.Sprintf(" kv_process_ms:%d", timeDetail.ProcessWallTimeMs)
		logStr += fmt.Sprintf(" kv_wait_ms:%d", timeDetail.WaitWallTimeMs)
		logStr += fmt.Sprintf(" kv_read_ms:%d", timeDetail.KvReadWallTimeMs)
		if timeDetail.ProcessWallTimeMs <= minLogKVProcessTime {
			logStr = strings.Replace(logStr, "TIME_COP_PROCESS", "TIME_COP_WAIT", 1)
		}
	}

	if detailV2 != nil && detailV2.ScanDetailV2 != nil {
		logStr += fmt.Sprintf(" processed_versions:%d", detailV2.ScanDetailV2.ProcessedVersions)
		logStr += fmt.Sprintf(" total_versions:%d", detailV2.ScanDetailV2.TotalVersions)
		logStr += fmt.Sprintf(" rocksdb_delete_skipped_count:%d", detailV2.ScanDetailV2.RocksdbDeleteSkippedCount)
		logStr += fmt.Sprintf(" rocksdb_key_skipped_count:%d", detailV2.ScanDetailV2.RocksdbKeySkippedCount)
		logStr += fmt.Sprintf(" rocksdb_cache_hit_count:%d", detailV2.ScanDetailV2.RocksdbBlockCacheHitCount)
		logStr += fmt.Sprintf(" rocksdb_read_count:%d", detailV2.ScanDetailV2.RocksdbBlockReadCount)
		logStr += fmt.Sprintf(" rocksdb_read_byte:%d", detailV2.ScanDetailV2.RocksdbBlockReadByte)
	} else if detail != nil && detail.ScanDetail != nil {
		logStr = appendScanDetail(logStr, "write", detail.ScanDetail.Write)
		logStr = appendScanDetail(logStr, "data", detail.ScanDetail.Data)
		logStr = appendScanDetail(logStr, "lock", detail.ScanDetail.Lock)
	}
	logutil.Logger(bo.GetCtx()).Info(logStr)
}

func appendScanDetail(logStr string, columnFamily string, scanInfo *kvrpcpb.ScanInfo) string {
	if scanInfo != nil {
		logStr += fmt.Sprintf(" scan_total_%s:%d", columnFamily, scanInfo.Total)
		logStr += fmt.Sprintf(" scan_processed_%s:%d", columnFamily, scanInfo.Processed)
	}
	return logStr
}

func (worker *copIteratorWorker) handleCopPagingResult(bo *Backoffer, rpcCtx *tikv.RPCContext, resp *copResponse, cacheKey []byte, cacheValue *coprCacheValue, task *copTask, costTime time.Duration) (*copTaskResult, error) {
	result, err := worker.handleCopResponse(bo, rpcCtx, resp, cacheKey, cacheValue, task, costTime)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if result != nil && len(result.remains) > 0 {
		// If there is region error or lock error, keep the paging size and retry.
		for _, remainedTask := range result.remains {
			remainedTask.pagingSize = task.pagingSize
		}
		return result, nil
	}
	pagingRange := resp.pbResp.Range
	// only paging requests need to calculate the next ranges
	if pagingRange == nil {
		// If the storage engine doesn't support paging protocol, it should have return all the region data.
		// So we finish here.
		return result, nil
	}

	// calculate next ranges and grow the paging size
	task.ranges = worker.calculateRemain(task.ranges, pagingRange, worker.req.Desc)
	if task.ranges.Len() == 0 {
		return result, nil
	}

	task.pagingSize = paging.GrowPagingSize(task.pagingSize, worker.req.Paging.MaxPagingSize)
	result.remains = []*copTask{task}
	return result, nil
}

// buildExceedsBoundDiagFields builds diagnostic log fields for "Request range exceeds bound" errors.
// Shared between the initial-retry path (retrying without buckets) and the persist-after-retry path
// (skipBuckets=true, bounded retries).
func buildExceedsBoundDiagFields(
	req *kv.Request,
	task *copTask,
	rpcCtx *tikv.RPCContext,
	regionCache *RegionCache,
	bo *Backoffer,
	otherErr string,
	latestBucketsVer uint64,
) []zap.Field {
	fields := []zap.Field{
		zap.Uint64("connID", req.ConnID),
		zap.String("connAlias", req.ConnAlias),
		zap.Uint64("txnStartTS", req.StartTs),
		zap.Uint64("regionID", task.region.GetID()),
		zap.Uint64("regionVer", task.region.GetVer()),
		zap.Uint64("regionConfVer", task.region.GetConfVer()),
		zap.Uint64("bucketsVer", task.bucketsVer),
		zap.Uint64("latestBucketsVer", latestBucketsVer),
		zap.String("storeType", task.storeType.Name()),
		zap.String("peerAddr", task.storeAddr),
		zap.Bool("skipBuckets", task.skipBuckets),
		zap.Int("exceedsBoundRetry", task.exceedsBoundRetry),
		zap.Int("maxExceedsBoundRetries", maxExceedsBoundRetries),
		zap.Int("rangeCount", task.ranges.Len()),
		zap.Any("rangeIssues", rangeIssuesForKeyRanges(task.ranges)),
		zap.String("error", otherErr),
	}

	if task.ranges.Len() > 0 {
		minStart, maxEnd := minStartAndMaxEndKeyOfKeyRanges(task.ranges)
		first := task.ranges.At(0)
		last := task.ranges.At(task.ranges.Len() - 1)
		fields = append(fields,
			keyField("minRangeStartKey", minStart),
			keyField("maxRangeEndKey", maxEnd),
			keyField("firstRangeStartKey", first.StartKey),
			keyField("firstRangeEndKey", first.EndKey),
			keyField("lastRangeStartKey", last.StartKey),
			keyField("lastRangeEndKey", last.EndKey),
		)
	}
	if len(task.buildLocStartKey) > 0 || len(task.buildLocEndKey) > 0 {
		fields = append(fields,
			keyField("buildLocationStartKey", task.buildLocStartKey),
			keyField("buildLocationEndKey", task.buildLocEndKey),
		)
	}

	var cachedStart, cachedEnd []byte
	var diagStart []byte
	if rpcCtx != nil && rpcCtx.Meta != nil {
		cachedStart = rpcCtx.Meta.GetStartKey()
		cachedEnd = rpcCtx.Meta.GetEndKey()
		fields = append(fields,
			keyField("cachedRegionStartKey", cachedStart),
			keyField("cachedRegionEndKey", cachedEnd),
		)
		badIdx, badRange, badReason := firstOutOfBoundKeyRangeInLocation(task.ranges, cachedStart, cachedEnd)
		if badIdx >= 0 {
			diagStart = badRange.StartKey
			fields = append(fields,
				zap.Int("outOfBoundRangeIndex", badIdx),
				zap.String("outOfBoundReason", badReason),
				keyField("outOfBoundRangeStartKey", badRange.StartKey),
				keyField("outOfBoundRangeEndKey", badRange.EndKey),
			)
		}
		if len(task.buildLocStartKey) > 0 || len(task.buildLocEndKey) > 0 {
			fields = append(fields,
				zap.Bool("buildBoundaryChangedVsCached",
					!bytes.Equal(task.buildLocStartKey, cachedStart) || !bytes.Equal(task.buildLocEndKey, cachedEnd)),
			)
		}
	} else {
		fields = append(fields, zap.Bool("cachedRegionMetaMissing", true))
	}
	if len(diagStart) == 0 && task.ranges.Len() > 0 {
		diagStart = task.ranges.At(0).StartKey
	}
	if len(diagStart) > 0 {
		cacheLoc := regionCache.TryLocateKey(diagStart)
		if cacheLoc == nil {
			fields = append(fields,
				keyField("diagStartKey", diagStart),
				zap.Bool("cacheLocateByDiagStartMissing", true),
			)
		} else {
			fields = append(fields,
				keyField("diagStartKey", diagStart),
				formatKeyLocation("cacheLocateByDiagStart", cacheLoc),
			)
		}
	}

	// Best-effort: query PD directly to compare region boundaries with cached info.
	pdLoc, pdErr := regionCache.LocateRegionByIDFromPD(bo.TiKVBackoffer(), task.region.GetID())
	if pdErr != nil {
		fields = append(fields, zap.Error(pdErr))
	} else {
		fields = append(fields,
			zap.Uint64("pdRegionVer", pdLoc.Region.GetVer()),
			zap.Uint64("pdRegionConfVer", pdLoc.Region.GetConfVer()),
			keyField("pdRegionStartKey", pdLoc.StartKey),
			keyField("pdRegionEndKey", pdLoc.EndKey),
		)
		if cachedStart != nil || cachedEnd != nil {
			fields = append(fields,
				zap.Bool("pdEpochChanged", pdLoc.Region.GetVer() != task.region.GetVer() || pdLoc.Region.GetConfVer() != task.region.GetConfVer()),
				zap.Bool("pdBoundaryChanged", !bytes.Equal(pdLoc.StartKey, cachedStart) || !bytes.Equal(pdLoc.EndKey, cachedEnd)),
			)
		}
		if len(task.buildLocStartKey) > 0 || len(task.buildLocEndKey) > 0 {
			fields = append(fields,
				zap.Bool("buildBoundaryChangedVsPD",
					!bytes.Equal(task.buildLocStartKey, pdLoc.StartKey) || !bytes.Equal(task.buildLocEndKey, pdLoc.EndKey)),
			)
		}
	}

	fields = append(fields,
		formatRanges(task.ranges),
		zap.Stack("stack"))
	return fields
}

// handleCopResponse checks coprocessor Response for region split and lock,
// returns more tasks when that happens, or handles the response if no error.
// if we're handling coprocessor paging response, lastRange is the range of last
// successful response, otherwise it's nil.
func (worker *copIteratorWorker) handleCopResponse(bo *Backoffer, rpcCtx *tikv.RPCContext, resp *copResponse, cacheKey []byte, cacheValue *coprCacheValue, task *copTask, costTime time.Duration) (*copTaskResult, error) {
	if ver := resp.pbResp.GetLatestBucketsVersion(); task.bucketsVer < ver {
		worker.store.GetRegionCache().UpdateBucketsIfNeeded(task.region, ver)
	}
	if regionErr := getRegionError(bo.GetCtx(), resp.pbResp); regionErr != nil {
		if rpcCtx != nil && task.storeType == kv.TiDB {
			resp.err = errors.Errorf("error: %v", regionErr)
			worker.checkRespOOM(resp)
			return &copTaskResult{resp: resp}, nil
		}
		errStr := fmt.Sprintf("region_id:%v, region_ver:%v, store_type:%s, peer_addr:%s, error:%s",
			task.region.GetID(), task.region.GetVer(), task.storeType.Name(), task.storeAddr, regionErr.String())
		if err := bo.Backoff(tikv.BoRegionMiss(), errors.New(errStr)); err != nil {
			return nil, errors.Trace(err)
		}
		// We may meet RegionError at the first packet, but not during visiting the stream.
		remains, err := buildCopTasks(bo, task.ranges, &buildCopTaskOpt{
			req:                         worker.req,
			cache:                       worker.store.GetRegionCache(),
			respChan:                    false,
			eventCb:                     task.eventCb,
			ignoreTiKVClientReadTimeout: true,
		})
		if err != nil {
			return nil, err
		}
		return worker.handleBatchRemainsOnErr(bo, rpcCtx, remains, resp.pbResp, task)
	}
	if lockErr := resp.pbResp.GetLocked(); lockErr != nil {
		if err := worker.handleLockErr(bo, lockErr, task); err != nil {
			return nil, err
		}
		task.meetLockFallback = true
		return worker.handleBatchRemainsOnErr(bo, rpcCtx, []*copTask{task}, resp.pbResp, task)
	}
	if otherErr := resp.pbResp.GetOtherError(); otherErr != "" {
		err := errors.Errorf("other error: %s", otherErr)

		// Handle "Request range exceeds bound" error from TiKV.
		// This can happen when bucket metadata is stale and causes TiDB to send
		// ranges outside the region boundary. Invalidate cache and retry.
		if strings.Contains(otherErr, "Request range exceeds bound") {
			// If this task was already built without bucket splitting and still got this error,
			// the problem isn't stale bucket metadata. We use bounded self-healing retries first,
			// then fail when retry budget is exhausted.
			if task.skipBuckets {
				fields := buildExceedsBoundDiagFields(worker.req, task, rpcCtx, worker.store.GetRegionCache(), bo, otherErr, resp.pbResp.GetLatestBucketsVersion())
				logutil.Logger(bo.GetCtx()).Error("Request range exceeds bound persists after bucket-less retry", fields...)
				if task.exceedsBoundRetry >= maxExceedsBoundRetries {
					return nil, errors.Errorf(
						"request range exceeds bound persists after bucket-less retry and exceeded retry budget, "+
							"region_id:%v, region_ver:%v, store_type:%s, peer_addr:%s, retry:%d/%d, error:%s",
						task.region.GetID(), task.region.GetVer(), task.storeType.Name(), task.storeAddr,
						task.exceedsBoundRetry, maxExceedsBoundRetries, otherErr)
				}

				logutil.Logger(bo.GetCtx()).Warn("Retrying persists-after-skipBuckets request range exceeds bound",
					zap.Uint64("connID", worker.req.ConnID),
					zap.String("connAlias", worker.req.ConnAlias),
					zap.Uint64("txnStartTS", worker.req.StartTs),
					zap.Uint64("regionID", task.region.GetID()),
					zap.Uint64("regionVer", task.region.GetVer()),
					zap.Uint64("regionConfVer", task.region.GetConfVer()),
					zap.Int("retry", task.exceedsBoundRetry+1),
					zap.Int("maxRetry", maxExceedsBoundRetries))

				// Self-healing retry: invalidate and rebuild in skip-buckets mode.
				worker.store.GetRegionCache().InvalidateCachedRegion(task.region)
				errStr := fmt.Sprintf("Request range exceeds bound persists after bucket-less retry: region_id:%v, region_ver:%v, store_type:%s, peer_addr:%s, retry:%d/%d, error:%s",
					task.region.GetID(), task.region.GetVer(), task.storeType.Name(), task.storeAddr,
					task.exceedsBoundRetry+1, maxExceedsBoundRetries, otherErr)
				if err := bo.Backoff(tikv.BoRegionMiss(), errors.New(errStr)); err != nil {
					return nil, errors.Trace(err)
				}
				remains, err := buildCopTasks(bo, task.ranges, &buildCopTaskOpt{
					req:                         worker.req,
					cache:                       worker.store.GetRegionCache(),
					respChan:                    false,
					eventCb:                     task.eventCb,
					ignoreTiKVClientReadTimeout: true,
					skipBuckets:                 true,
					exceedsBoundRetry:           task.exceedsBoundRetry + 1,
				})
				if err != nil {
					return nil, err
				}
				return worker.handleBatchRemainsOnErr(bo, rpcCtx, remains, resp.pbResp, task)
			}

			// Important: log enough context here even if the retry succeeds, so we can
			// still diagnose the root cause in production.
			fields := buildExceedsBoundDiagFields(worker.req, task, rpcCtx, worker.store.GetRegionCache(), bo, otherErr, resp.pbResp.GetLatestBucketsVersion())
			logutil.Logger(bo.GetCtx()).Warn("Request range exceeds bound - invalidating cache and retrying without buckets", fields...)

			// Invalidate the cached region to force refresh from PD
			worker.store.GetRegionCache().InvalidateCachedRegion(task.region)

			// Backoff before retry
			errStr := fmt.Sprintf("Request range exceeds bound: region_id:%v, region_ver:%v, store_type:%s, peer_addr:%s, error:%s",
				task.region.GetID(), task.region.GetVer(), task.storeType.Name(), task.storeAddr, otherErr)
			if err := bo.Backoff(tikv.BoRegionMiss(), errors.New(errStr)); err != nil {
				return nil, errors.Trace(err)
			}

			// Rebuild cop tasks with fresh region info, skipping bucket splitting
			// since buckets are suspected to be the cause of this error.
			// The new tasks will have skipBuckets=true set during construction.
			remains, err := buildCopTasks(bo, task.ranges, &buildCopTaskOpt{
				req:                         worker.req,
				cache:                       worker.store.GetRegionCache(),
				respChan:                    false,
				eventCb:                     task.eventCb,
				ignoreTiKVClientReadTimeout: true,
				skipBuckets:                 true,
				exceedsBoundRetry:           task.exceedsBoundRetry + 1,
			})
			if err != nil {
				return nil, err
			}
			return worker.handleBatchRemainsOnErr(bo, rpcCtx, remains, resp.pbResp, task)
		}

		otherErrFields := []zap.Field{
			zap.Uint64("connID", worker.req.ConnID),
			zap.String("connAlias", worker.req.ConnAlias),
			zap.Uint64("txnStartTS", worker.req.StartTs),
			zap.Uint64("regionID", task.region.GetID()),
			zap.Uint64("regionVer", task.region.GetVer()),
			zap.Uint64("regionConfVer", task.region.GetConfVer()),
			zap.Uint64("bucketsVer", task.bucketsVer),
			zap.Uint64("latestBucketsVer", resp.pbResp.GetLatestBucketsVersion()),
			zap.Int("rangeNums", task.ranges.Len()),
			zap.String("storeAddr", task.storeAddr),
			zap.String("error", otherErr),
		}
		if task.ranges.Len() > 0 {
			otherErrFields = append(otherErrFields,
				keyField("firstRangeStartKey", task.ranges.At(0).StartKey),
				keyField("lastRangeEndKey", task.ranges.At(task.ranges.Len()-1).EndKey),
			)
		}
		logutil.Logger(bo.GetCtx()).Warn("other error", otherErrFields...)

		if strings.Contains(err.Error(), "write conflict") {
			return nil, kv.ErrWriteConflict.FastGen("%s", otherErr)
		}
		return nil, errors.Trace(err)
	}
	// When the request is using paging API, the `Range` is not nil.
	if resp.pbResp.Range != nil {
		resp.startKey = resp.pbResp.Range.Start
	} else if task.ranges != nil && task.ranges.Len() > 0 {
		resp.startKey = task.ranges.At(0).StartKey
	}
	if err := worker.handleCollectExecutionInfo(bo, rpcCtx, resp); err != nil {
		return nil, err
	}
	resp.respTime = costTime

	if err := worker.handleCopCache(task, resp, cacheKey, cacheValue); err != nil {
		return nil, err
	}

	worker.checkRespOOM(resp)
	result := &copTaskResult{resp: resp}
	batchRespList, batchRemainTasks, err := worker.handleBatchCopResponse(bo, rpcCtx, resp.pbResp, task.batchTaskList)
	if err != nil {
		return result, err
	}
	result.batchRespList = batchRespList
	result.remains = batchRemainTasks
	return result, nil
}

func (worker *copIteratorWorker) handleBatchRemainsOnErr(bo *Backoffer, rpcCtx *tikv.RPCContext, remains []*copTask, resp *coprocessor.Response, task *copTask) (*copTaskResult, error) {
	if len(task.batchTaskList) == 0 {
		return &copTaskResult{remains: remains}, nil
	}
	batchedTasks := task.batchTaskList
	task.batchTaskList = nil
	batchRespList, remainTasks, err := worker.handleBatchCopResponse(bo, rpcCtx, resp, batchedTasks)
	if err != nil {
		return nil, err
	}
	return &copTaskResult{
		batchRespList: batchRespList,
		remains:       append(remains, remainTasks...),
	}, nil
}

func regionErrorDumpTriggerCheck(config *traceevent.DumpTriggerConfig) bool {
	return config.Event.Type == "region_error"
}

func getRegionError(ctx context.Context, resp interface{ GetRegionError() *errorpb.Error }) *errorpb.Error {
	err := resp.GetRegionError()
	if err != nil {
		traceevent.CheckFlightRecorderDumpTrigger(ctx, "dump_trigger.suspicious_event", regionErrorDumpTriggerCheck)
		return err
	}
	return nil
}
