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
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/mpp"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/logutil"
	"github.com/pingcap/tidb/store/tikv/metrics"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/store/tikv/util"
	"github.com/pingcap/tidb/util/memory"
	"go.uber.org/zap"
)

// batchCopTask comprises of multiple copTask that will send to same store.
type batchCopTask struct {
	storeAddr string
	cmdType   tikvrpc.CmdType
	ctx       *tikv.RPCContext

	regionInfos []tikv.RegionInfo
}

type batchCopResponse struct {
	pbResp *coprocessor.BatchResponse
	detail *CopRuntimeStats

	// batch Cop Response is yet to return startKey. So batchCop cannot retry partially.
	startKey kv.Key
	err      error
	respSize int64
	respTime time.Duration
}

// GetData implements the kv.ResultSubset GetData interface.
func (rs *batchCopResponse) GetData() []byte {
	return rs.pbResp.Data
}

// GetStartKey implements the kv.ResultSubset GetStartKey interface.
func (rs *batchCopResponse) GetStartKey() kv.Key {
	return rs.startKey
}

// GetExecDetails is unavailable currently, because TiFlash has not collected exec details for batch cop.
// TODO: Will fix in near future.
func (rs *batchCopResponse) GetCopRuntimeStats() *CopRuntimeStats {
	return rs.detail
}

// MemSize returns how many bytes of memory this response use
func (rs *batchCopResponse) MemSize() int64 {
	if rs.respSize != 0 {
		return rs.respSize
	}

	// ignore rs.err
	rs.respSize += int64(cap(rs.startKey))
	if rs.detail != nil {
		rs.respSize += int64(sizeofExecDetails)
	}
	if rs.pbResp != nil {
		// Using a approximate size since it's hard to get a accurate value.
		rs.respSize += int64(rs.pbResp.Size())
	}
	return rs.respSize
}

func (rs *batchCopResponse) RespTime() time.Duration {
	return rs.respTime
}

// balanceBatchCopTask balance the regions between available stores, the basic rule is
// 1. the first region of each original batch cop task belongs to its original store because some
//    meta data(like the rpc context) in batchCopTask is related to it
// 2. for the remaining regions:
//    if there is only 1 available store, then put the region to the related store
//    otherwise, use a greedy algorithm to put it into the store with highest weight
func balanceBatchCopTask(ctx context.Context, kvStore *tikv.KVStore, originalTasks []*batchCopTask, isMPP bool) []*batchCopTask {
	if len(originalTasks) <= 1 {
		return originalTasks
	}
	cache := kvStore.GetRegionCache()
	storeTaskMap := make(map[uint64]*batchCopTask)
	// storeCandidateRegionMap stores all the possible store->region map. Its content is
	// store id -> region signature -> region info. We can see it as store id -> region lists.
	storeCandidateRegionMap := make(map[uint64]map[string]tikv.RegionInfo)
	totalRegionCandidateNum := 0
	totalRemainingRegionNum := 0

	if !isMPP {
		for _, task := range originalTasks {
			taskStoreID := task.regionInfos[0].AllStores[0]
			batchTask := &batchCopTask{
				storeAddr:   task.storeAddr,
				cmdType:     task.cmdType,
				ctx:         task.ctx,
				regionInfos: []tikv.RegionInfo{task.regionInfos[0]},
			}
			storeTaskMap[taskStoreID] = batchTask
		}
	} else {
		logutil.BgLogger().Info("detecting available mpp stores")
		// decide the available stores
		stores := cache.GetTiFlashStores()
		var wg sync.WaitGroup
		var mu sync.Mutex
		wg.Add(len(stores))
		for i := range stores {
			go func(idx int) {
				defer wg.Done()
				s := stores[idx]
				aliveReq := tikvrpc.NewRequest(tikvrpc.CmdMPPAlive, &mpp.IsAliveRequest{}, kvrpcpb.Context{})
				aliveReq.StoreTp = kv.TiFlash
				alive := false
				resp, err := kvStore.GetTiKVClient().SendRequest(ctx, s.GetAddr(), aliveReq, 3*time.Second)
				if err != nil {
					logutil.BgLogger().Warn("Cannot detect store's availability", zap.String("store address", s.GetAddr()), zap.String("err message", err.Error()))
				} else {
					rpcResp := resp.Resp.(*mpp.IsAliveResponse)
					if rpcResp.Available {
						alive = true
					} else {
						logutil.BgLogger().Warn("Cannot detect store's availability", zap.String("store address", s.GetAddr()))
					}
				}
				if !alive {
					return
				}

				mu.Lock()
				defer mu.Unlock()
				storeTaskMap[s.StoreID()] = &batchCopTask{
					storeAddr: s.GetAddr(),
					cmdType:   originalTasks[0].cmdType,
					ctx:       &tikv.RPCContext{Addr: s.GetAddr(), Store: s},
				}
			}(i)
		}
		wg.Wait()
	}

	for _, task := range originalTasks {
		for index, ri := range task.regionInfos {
			// for each region, figure out the valid store num
			validStoreNum := 0
			if index == 0 && !isMPP {
				continue
			}
			var validStoreID uint64
			for _, storeID := range ri.AllStores {
				if _, ok := storeTaskMap[storeID]; ok {
					validStoreNum++
					// original store id might be invalid, so we have to set it again.
					validStoreID = storeID
				}
			}
			if validStoreNum == 0 {
				logutil.BgLogger().Warn("Meet regions that don't have an available store. Give up balancing")
				return originalTasks
			} else if validStoreNum == 1 {
				// if only one store is valid, just put it to storeTaskMap
				storeTaskMap[validStoreID].regionInfos = append(storeTaskMap[validStoreID].regionInfos, ri)
			} else {
				// if more than one store is valid, put the region
				// to store candidate map
				totalRegionCandidateNum += validStoreNum
				totalRemainingRegionNum += 1
				taskKey := ri.Region.String()
				for _, storeID := range ri.AllStores {
					if _, validStore := storeTaskMap[storeID]; !validStore {
						continue
					}
					if _, ok := storeCandidateRegionMap[storeID]; !ok {
						candidateMap := make(map[string]tikv.RegionInfo)
						storeCandidateRegionMap[storeID] = candidateMap
					}
					if _, duplicateRegion := storeCandidateRegionMap[storeID][taskKey]; duplicateRegion {
						// duplicated region, should not happen, just give up balance
						logutil.BgLogger().Warn("Meet duplicated region info during when trying to balance batch cop task, give up balancing")
						return originalTasks
					}
					storeCandidateRegionMap[storeID][taskKey] = ri
				}
			}
		}
	}
	if totalRemainingRegionNum == 0 {
		return originalTasks
	}

	avgStorePerRegion := float64(totalRegionCandidateNum) / float64(totalRemainingRegionNum)
	findNextStore := func(candidateStores []uint64) uint64 {
		store := uint64(math.MaxUint64)
		weightedRegionNum := math.MaxFloat64
		if candidateStores != nil {
			for _, storeID := range candidateStores {
				if _, validStore := storeCandidateRegionMap[storeID]; !validStore {
					continue
				}
				num := float64(len(storeCandidateRegionMap[storeID]))/avgStorePerRegion + float64(len(storeTaskMap[storeID].regionInfos))
				if num < weightedRegionNum {
					store = storeID
					weightedRegionNum = num
				}
			}
			if store != uint64(math.MaxUint64) {
				return store
			}
		}
		for storeID := range storeTaskMap {
			if _, validStore := storeCandidateRegionMap[storeID]; !validStore {
				continue
			}
			num := float64(len(storeCandidateRegionMap[storeID]))/avgStorePerRegion + float64(len(storeTaskMap[storeID].regionInfos))
			if num < weightedRegionNum {
				store = storeID
				weightedRegionNum = num
			}
		}
		return store
	}

	store := findNextStore(nil)
	for totalRemainingRegionNum > 0 {
		if store == uint64(math.MaxUint64) {
			break
		}
		var key string
		var ri tikv.RegionInfo
		for key, ri = range storeCandidateRegionMap[store] {
			// get the first region
			break
		}
		storeTaskMap[store].regionInfos = append(storeTaskMap[store].regionInfos, ri)
		totalRemainingRegionNum--
		for _, id := range ri.AllStores {
			if _, ok := storeCandidateRegionMap[id]; ok {
				delete(storeCandidateRegionMap[id], key)
				totalRegionCandidateNum--
				if len(storeCandidateRegionMap[id]) == 0 {
					delete(storeCandidateRegionMap, id)
				}
			}
		}
		if totalRemainingRegionNum > 0 {
			avgStorePerRegion = float64(totalRegionCandidateNum) / float64(totalRemainingRegionNum)
			// it is not optimal because we only check the stores that affected by this region, in fact in order
			// to find out the store with the lowest weightedRegionNum, all stores should be checked, but I think
			// check only the affected stores is more simple and will get a good enough result
			store = findNextStore(ri.AllStores)
		}
	}
	if totalRemainingRegionNum > 0 {
		logutil.BgLogger().Warn("Some regions are not used when trying to balance batch cop task, give up balancing")
		return originalTasks
	}

	var ret []*batchCopTask
	for _, task := range storeTaskMap {
		if len(task.regionInfos) > 0 {
			ret = append(ret, task)
		}
	}
	return ret
}

func buildBatchCopTasks(bo *tikv.Backoffer, store *tikv.KVStore, ranges *tikv.KeyRanges, storeType kv.StoreType, isMPP bool) ([]*batchCopTask, error) {
	cache := store.GetRegionCache()
	start := time.Now()
	const cmdType = tikvrpc.CmdBatchCop
	rangesLen := ranges.Len()
	for {
		var tasks []*copTask
		appendTask := func(regionWithRangeInfo *tikv.KeyLocation, ranges *tikv.KeyRanges) {
			tasks = append(tasks, &copTask{
				region:    regionWithRangeInfo.Region,
				ranges:    ranges,
				cmdType:   cmdType,
				storeType: storeType,
			})
		}

		err := tikv.SplitKeyRanges(bo, cache, ranges, appendTask)
		if err != nil {
			return nil, errors.Trace(err)
		}

		var batchTasks []*batchCopTask

		storeTaskMap := make(map[string]*batchCopTask)
		needRetry := false
		for _, task := range tasks {
			rpcCtx, err := cache.GetTiFlashRPCContext(bo, task.region, false)
			if err != nil {
				return nil, errors.Trace(err)
			}
			// When rpcCtx is nil, it's not only attributed to the miss region, but also
			// some TiFlash stores crash and can't be recovered.
			// That is not an error that can be easily recovered, so we regard this error
			// same as rpc error.
			if rpcCtx == nil {
				needRetry = true
				logutil.BgLogger().Info("retry for TiFlash peer with region missing", zap.Uint64("region id", task.region.GetID()))
				// Probably all the regions are invalid. Make the loop continue and mark all the regions invalid.
				// Then `splitRegion` will reloads these regions.
				continue
			}
			allStores := cache.GetAllValidTiFlashStores(task.region, rpcCtx.Store)
			if batchCop, ok := storeTaskMap[rpcCtx.Addr]; ok {
				batchCop.regionInfos = append(batchCop.regionInfos, tikv.RegionInfo{Region: task.region, Meta: rpcCtx.Meta, Ranges: task.ranges, AllStores: allStores})
			} else {
				batchTask := &batchCopTask{
					storeAddr:   rpcCtx.Addr,
					cmdType:     cmdType,
					ctx:         rpcCtx,
					regionInfos: []tikv.RegionInfo{{Region: task.region, Meta: rpcCtx.Meta, Ranges: task.ranges, AllStores: allStores}},
				}
				storeTaskMap[rpcCtx.Addr] = batchTask
			}
		}
		if needRetry {
			// As mentioned above, nil rpcCtx is always attributed to failed stores.
			// It's equal to long poll the store but get no response. Here we'd better use
			// TiFlash error to trigger the TiKV fallback mechanism.
			err = bo.Backoff(tikv.BoTiFlashRPC, errors.New("Cannot find region with TiFlash peer"))
			if err != nil {
				return nil, errors.Trace(err)
			}
			continue
		}

		for _, task := range storeTaskMap {
			batchTasks = append(batchTasks, task)
		}
		if log.GetLevel() <= zap.DebugLevel {
			msg := "Before region balance:"
			for _, task := range batchTasks {
				msg += " store " + task.storeAddr + ": " + strconv.Itoa(len(task.regionInfos)) + " regions,"
			}
			logutil.BgLogger().Debug(msg)
		}
		batchTasks = balanceBatchCopTask(bo.GetCtx(), store, batchTasks, isMPP)
		if log.GetLevel() <= zap.DebugLevel {
			msg := "After region balance:"
			for _, task := range batchTasks {
				msg += " store " + task.storeAddr + ": " + strconv.Itoa(len(task.regionInfos)) + " regions,"
			}
			logutil.BgLogger().Debug(msg)
		}

		if elapsed := time.Since(start); elapsed > time.Millisecond*500 {
			logutil.BgLogger().Warn("buildBatchCopTasks takes too much time",
				zap.Duration("elapsed", elapsed),
				zap.Int("range len", rangesLen),
				zap.Int("task len", len(batchTasks)))
		}
		metrics.TxnRegionsNumHistogramWithBatchCoprocessor.Observe(float64(len(batchTasks)))
		return batchTasks, nil
	}
}

func (c *CopClient) sendBatch(ctx context.Context, req *kv.Request, vars *kv.Variables) kv.Response {
	if req.KeepOrder || req.Desc {
		return copErrorResponse{errors.New("batch coprocessor cannot prove keep order or desc property")}
	}
	ctx = context.WithValue(ctx, tikv.TxnStartKey, req.StartTs)
	bo := tikv.NewBackofferWithVars(ctx, copBuildTaskMaxBackoff, vars)
	tasks, err := buildBatchCopTasks(bo, c.store.KVStore, tikv.NewKeyRanges(req.KeyRanges), req.StoreType, false)
	if err != nil {
		return copErrorResponse{err}
	}
	it := &batchCopIterator{
		store:        c.store.KVStore,
		req:          req,
		finishCh:     make(chan struct{}),
		vars:         vars,
		memTracker:   req.MemTracker,
		ClientHelper: tikv.NewClientHelper(c.store.KVStore, util.NewTSSet(5)),
		rpcCancel:    tikv.NewRPCanceller(),
	}
	ctx = context.WithValue(ctx, tikv.RPCCancellerCtxKey{}, it.rpcCancel)
	it.tasks = tasks
	it.respChan = make(chan *batchCopResponse, 2048)
	go it.run(ctx)
	return it
}

type batchCopIterator struct {
	*tikv.ClientHelper

	store    *tikv.KVStore
	req      *kv.Request
	finishCh chan struct{}

	tasks []*batchCopTask

	// Batch results are stored in respChan.
	respChan chan *batchCopResponse

	vars *kv.Variables

	memTracker *memory.Tracker

	rpcCancel *tikv.RPCCanceller

	wg sync.WaitGroup
	// closed represents when the Close is called.
	// There are two cases we need to close the `finishCh` channel, one is when context is done, the other one is
	// when the Close is called. we use atomic.CompareAndSwap `closed` to to make sure the channel is not closed twice.
	closed uint32
}

func (b *batchCopIterator) run(ctx context.Context) {
	// We run workers for every batch cop.
	for _, task := range b.tasks {
		b.wg.Add(1)
		bo := tikv.NewBackofferWithVars(ctx, copNextMaxBackoff, b.vars)
		go b.handleTask(ctx, bo, task)
	}
	b.wg.Wait()
	close(b.respChan)
}

// Next returns next coprocessor result.
// NOTE: Use nil to indicate finish, so if the returned ResultSubset is not nil, reader should continue to call Next().
func (b *batchCopIterator) Next(ctx context.Context) (kv.ResultSubset, error) {
	var (
		resp   *batchCopResponse
		ok     bool
		closed bool
	)

	// Get next fetched resp from chan
	resp, ok, closed = b.recvFromRespCh(ctx)
	if !ok || closed {
		return nil, nil
	}

	if resp.err != nil {
		return nil, errors.Trace(resp.err)
	}

	err := b.store.CheckVisibility(b.req.StartTs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return resp, nil
}

func (b *batchCopIterator) recvFromRespCh(ctx context.Context) (resp *batchCopResponse, ok bool, exit bool) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case resp, ok = <-b.respChan:
			return
		case <-ticker.C:
			if atomic.LoadUint32(b.vars.Killed) == 1 {
				resp = &batchCopResponse{err: tikv.ErrQueryInterrupted}
				ok = true
				return
			}
		case <-b.finishCh:
			exit = true
			return
		case <-ctx.Done():
			// We select the ctx.Done() in the thread of `Next` instead of in the worker to avoid the cost of `WithCancel`.
			if atomic.CompareAndSwapUint32(&b.closed, 0, 1) {
				close(b.finishCh)
			}
			exit = true
			return
		}
	}
}

// Close releases the resource.
func (b *batchCopIterator) Close() error {
	if atomic.CompareAndSwapUint32(&b.closed, 0, 1) {
		close(b.finishCh)
	}
	b.rpcCancel.CancelAll()
	b.wg.Wait()
	return nil
}

func (b *batchCopIterator) handleTask(ctx context.Context, bo *tikv.Backoffer, task *batchCopTask) {
	tasks := []*batchCopTask{task}
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
func (b *batchCopIterator) retryBatchCopTask(ctx context.Context, bo *tikv.Backoffer, batchTask *batchCopTask) ([]*batchCopTask, error) {
	var ranges []kv.KeyRange
	for _, ri := range batchTask.regionInfos {
		ri.Ranges.Do(func(ran *kv.KeyRange) {
			ranges = append(ranges, *ran)
		})
	}
	return buildBatchCopTasks(bo, b.store, tikv.NewKeyRanges(ranges), b.req.StoreType, false)
}

func (b *batchCopIterator) handleTaskOnce(ctx context.Context, bo *tikv.Backoffer, task *batchCopTask) ([]*batchCopTask, error) {
	sender := tikv.NewRegionBatchRequestSender(b.store.GetRegionCache(), b.store.GetTiKVClient())
	var regionInfos []*coprocessor.RegionInfo
	for _, ri := range task.regionInfos {
		regionInfos = append(regionInfos, &coprocessor.RegionInfo{
			RegionId: ri.Region.GetID(),
			RegionEpoch: &metapb.RegionEpoch{
				ConfVer: ri.Region.GetConfVer(),
				Version: ri.Region.GetVer(),
			},
			Ranges: ri.Ranges.ToPBRanges(),
		})
	}

	copReq := coprocessor.BatchRequest{
		Tp:        b.req.Tp,
		StartTs:   b.req.StartTs,
		Data:      b.req.Data,
		SchemaVer: b.req.SchemaVar,
		Regions:   regionInfos,
	}

	req := tikvrpc.NewRequest(task.cmdType, &copReq, kvrpcpb.Context{
		IsolationLevel: tikv.IsolationLevelToPB(b.req.IsolationLevel),
		Priority:       tikv.PriorityToPB(b.req.Priority),
		NotFillCache:   b.req.NotFillCache,
		RecordTimeStat: true,
		RecordScanStat: true,
		TaskId:         b.req.TaskID,
	})
	req.StoreTp = kv.TiFlash

	logutil.BgLogger().Debug("send batch request to ", zap.String("req info", req.String()), zap.Int("cop task len", len(task.regionInfos)))
	resp, retry, cancel, err := sender.SendReqToAddr(bo, task.ctx, task.regionInfos, req, tikv.ReadTimeoutUltraLong)
	// If there are store errors, we should retry for all regions.
	if retry {
		return b.retryBatchCopTask(ctx, bo, task)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer cancel()
	return nil, b.handleStreamedBatchCopResponse(ctx, bo, resp.Resp.(*tikvrpc.BatchCopStreamResponse), task)
}

func (b *batchCopIterator) handleStreamedBatchCopResponse(ctx context.Context, bo *tikv.Backoffer, response *tikvrpc.BatchCopStreamResponse, task *batchCopTask) (err error) {
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

			if err1 := bo.Backoff(tikv.BoTiKVRPC, errors.Errorf("recv stream response error: %v, task store addr: %s", err, task.storeAddr)); err1 != nil {
				return errors.Trace(err)
			}

			// No coprocessor.Response for network error, rebuild task based on the last success one.
			if errors.Cause(err) == context.Canceled {
				logutil.BgLogger().Info("stream recv timeout", zap.Error(err))
			} else {
				logutil.BgLogger().Info("stream unknown error", zap.Error(err))
			}
			return tikv.ErrTiFlashServerTimeout
		}
	}
}

func (b *batchCopIterator) handleBatchCopResponse(bo *tikv.Backoffer, response *coprocessor.BatchResponse, task *batchCopTask) (err error) {
	if otherErr := response.GetOtherError(); otherErr != "" {
		err = errors.Errorf("other error: %s", otherErr)
		logutil.BgLogger().Warn("other error",
			zap.Uint64("txnStartTS", b.req.StartTs),
			zap.String("storeAddr", task.storeAddr),
			zap.Error(err))
		return errors.Trace(err)
	}

	resp := batchCopResponse{
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
	resp.detail.CalleeAddress = task.storeAddr

	b.sendToRespCh(&resp)

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
