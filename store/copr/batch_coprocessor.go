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
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/driver/backoff"
	derr "github.com/pingcap/tidb/store/driver/error"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/tiflashcompute"
	"github.com/stathat/consistent"
	"github.com/tikv/client-go/v2/metrics"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
)

const fetchTopoMaxBackoff = 20000

// batchCopTask comprises of multiple copTask that will send to same store.
type batchCopTask struct {
	storeAddr string
	cmdType   tikvrpc.CmdType
	ctx       *tikv.RPCContext

	regionInfos []RegionInfo // region info for single physical table
	// PartitionTableRegions indicates region infos for each partition table, used by scanning partitions in batch.
	// Thus, one of `regionInfos` and `PartitionTableRegions` must be nil.
	PartitionTableRegions []*coprocessor.TableRegions
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

func deepCopyStoreTaskMap(storeTaskMap map[uint64]*batchCopTask) map[uint64]*batchCopTask {
	storeTasks := make(map[uint64]*batchCopTask)
	for storeID, task := range storeTaskMap {
		t := batchCopTask{
			storeAddr: task.storeAddr,
			cmdType:   task.cmdType,
			ctx:       task.ctx,
		}
		t.regionInfos = make([]RegionInfo, len(task.regionInfos))
		copy(t.regionInfos, task.regionInfos)
		storeTasks[storeID] = &t
	}
	return storeTasks
}

func regionTotalCount(storeTasks map[uint64]*batchCopTask, candidateRegionInfos []RegionInfo) int {
	count := len(candidateRegionInfos)
	for _, task := range storeTasks {
		count += len(task.regionInfos)
	}
	return count
}

const (
	maxBalanceScore       = 100
	balanceScoreThreshold = 85
)

// Select at most cnt RegionInfos from candidateRegionInfos that belong to storeID.
// If selected[i] is true, candidateRegionInfos[i] has been selected and should be skip.
// storeID2RegionIndex is a map that key is storeID and value is a region index slice.
// selectRegion use storeID2RegionIndex to find RegionInfos that belong to storeID efficiently.
func selectRegion(storeID uint64, candidateRegionInfos []RegionInfo, selected []bool, storeID2RegionIndex map[uint64][]int, cnt int64) []RegionInfo {
	regionIndexes, ok := storeID2RegionIndex[storeID]
	if !ok {
		logutil.BgLogger().Error("selectRegion: storeID2RegionIndex not found", zap.Uint64("storeID", storeID))
		return nil
	}
	var regionInfos []RegionInfo
	i := 0
	for ; i < len(regionIndexes) && len(regionInfos) < int(cnt); i++ {
		idx := regionIndexes[i]
		if selected[idx] {
			continue
		}
		selected[idx] = true
		regionInfos = append(regionInfos, candidateRegionInfos[idx])
	}
	// Remove regions that has beed selected.
	storeID2RegionIndex[storeID] = regionIndexes[i:]
	return regionInfos
}

// Higher scores mean more balance: (100 - unblance percentage)
func balanceScore(maxRegionCount, minRegionCount int, balanceContinuousRegionCount int64) int {
	if minRegionCount <= 0 {
		return math.MinInt32
	}
	unbalanceCount := maxRegionCount - minRegionCount
	if unbalanceCount <= int(balanceContinuousRegionCount) {
		return maxBalanceScore
	}
	return maxBalanceScore - unbalanceCount*100/minRegionCount
}

func isBalance(score int) bool {
	return score >= balanceScoreThreshold
}

func checkBatchCopTaskBalance(storeTasks map[uint64]*batchCopTask, balanceContinuousRegionCount int64) (int, []string) {
	if len(storeTasks) == 0 {
		return 0, []string{}
	}
	maxRegionCount := 0
	minRegionCount := math.MaxInt32
	balanceInfos := []string{}
	for storeID, task := range storeTasks {
		cnt := len(task.regionInfos)
		if cnt > maxRegionCount {
			maxRegionCount = cnt
		}
		if cnt < minRegionCount {
			minRegionCount = cnt
		}
		balanceInfos = append(balanceInfos, fmt.Sprintf("storeID %d storeAddr %s regionCount %d", storeID, task.storeAddr, cnt))
	}
	return balanceScore(maxRegionCount, minRegionCount, balanceContinuousRegionCount), balanceInfos
}

// balanceBatchCopTaskWithContinuity try to balance `continuous regions` between TiFlash Stores.
// In fact, not absolutely continuous is required, regions' range are closed to store in a TiFlash segment is enough for internal read optimization.
//
// First, sort candidateRegionInfos by their key ranges.
// Second, build a storeID2RegionIndex data structure to fastly locate regions of a store (avoid scanning candidateRegionInfos repeatly).
// Third, each store will take balanceContinuousRegionCount from the sorted candidateRegionInfos. These regions are stored very close to each other in TiFlash.
// Fourth, if the region count is not balance between TiFlash, it may fallback to the original balance logic.
func balanceBatchCopTaskWithContinuity(storeTaskMap map[uint64]*batchCopTask, candidateRegionInfos []RegionInfo, balanceContinuousRegionCount int64) ([]*batchCopTask, int) {
	if len(candidateRegionInfos) < 500 {
		return nil, 0
	}
	funcStart := time.Now()
	regionCount := regionTotalCount(storeTaskMap, candidateRegionInfos)
	storeTasks := deepCopyStoreTaskMap(storeTaskMap)

	// Sort regions by their key ranges.
	slices.SortFunc(candidateRegionInfos, func(i, j RegionInfo) bool {
		// Special case: Sort empty ranges to the end.
		if i.Ranges.Len() < 1 || j.Ranges.Len() < 1 {
			return i.Ranges.Len() > j.Ranges.Len()
		}
		// StartKey0 < StartKey1
		return bytes.Compare(i.Ranges.At(0).StartKey, j.Ranges.At(0).StartKey) == -1
	})

	balanceStart := time.Now()
	// Build storeID -> region index slice index and we can fastly locate regions of a store.
	storeID2RegionIndex := make(map[uint64][]int)
	for i, ri := range candidateRegionInfos {
		for _, storeID := range ri.AllStores {
			if val, ok := storeID2RegionIndex[storeID]; ok {
				storeID2RegionIndex[storeID] = append(val, i)
			} else {
				storeID2RegionIndex[storeID] = []int{i}
			}
		}
	}

	// If selected[i] is true, candidateRegionInfos[i] is selected by a store and should skip it in selectRegion.
	selected := make([]bool, len(candidateRegionInfos))
	for {
		totalCount := 0
		selectCountThisRound := 0
		for storeID, task := range storeTasks {
			// Each store select balanceContinuousRegionCount regions from candidateRegionInfos.
			// Since candidateRegionInfos is sorted, it is very likely that these regions are close to each other in TiFlash.
			regionInfo := selectRegion(storeID, candidateRegionInfos, selected, storeID2RegionIndex, balanceContinuousRegionCount)
			task.regionInfos = append(task.regionInfos, regionInfo...)
			totalCount += len(task.regionInfos)
			selectCountThisRound += len(regionInfo)
		}
		if totalCount >= regionCount {
			break
		}
		if selectCountThisRound == 0 {
			logutil.BgLogger().Error("selectCandidateRegionInfos fail: some region cannot find relevant store.", zap.Int("regionCount", regionCount), zap.Int("candidateCount", len(candidateRegionInfos)))
			return nil, 0
		}
	}
	balanceEnd := time.Now()

	score, balanceInfos := checkBatchCopTaskBalance(storeTasks, balanceContinuousRegionCount)
	if !isBalance(score) {
		logutil.BgLogger().Warn("balanceBatchCopTaskWithContinuity is not balance", zap.Int("score", score), zap.Strings("balanceInfos", balanceInfos))
	}

	totalCount := 0
	var res []*batchCopTask
	for _, task := range storeTasks {
		totalCount += len(task.regionInfos)
		if len(task.regionInfos) > 0 {
			res = append(res, task)
		}
	}
	if totalCount != regionCount {
		logutil.BgLogger().Error("balanceBatchCopTaskWithContinuity error", zap.Int("totalCount", totalCount), zap.Int("regionCount", regionCount))
		return nil, 0
	}

	logutil.BgLogger().Debug("balanceBatchCopTaskWithContinuity time",
		zap.Int("candidateRegionCount", len(candidateRegionInfos)),
		zap.Int64("balanceContinuousRegionCount", balanceContinuousRegionCount),
		zap.Int("balanceScore", score),
		zap.Duration("balanceTime", balanceEnd.Sub(balanceStart)),
		zap.Duration("totalTime", time.Since(funcStart)))

	return res, score
}

// balanceBatchCopTask balance the regions between available stores, the basic rule is
//  1. the first region of each original batch cop task belongs to its original store because some
//     meta data(like the rpc context) in batchCopTask is related to it
//  2. for the remaining regions:
//     if there is only 1 available store, then put the region to the related store
//     otherwise, these region will be balance between TiFlash stores.
//
// Currently, there are two balance strategies.
// The first balance strategy: use a greedy algorithm to put it into the store with highest weight. This strategy only consider the region count between TiFlash stores.
//
// The second balance strategy: Not only consider the region count between TiFlash stores, but also try to make the regions' range continuous(stored in TiFlash closely).
// If balanceWithContinuity is true, the second balance strategy is enable.
func balanceBatchCopTask(ctx context.Context, kvStore *kvStore, originalTasks []*batchCopTask, isMPP bool, ttl time.Duration, balanceWithContinuity bool, balanceContinuousRegionCount int64) []*batchCopTask {
	if len(originalTasks) == 0 {
		log.Info("Batch cop task balancer got an empty task set.")
		return originalTasks
	}
	// for mpp, we still need to detect the store availability
	if len(originalTasks) <= 1 && !isMPP {
		return originalTasks
	}
	cache := kvStore.GetRegionCache()
	storeTaskMap := make(map[uint64]*batchCopTask)
	// storeCandidateRegionMap stores all the possible store->region map. Its content is
	// store id -> region signature -> region info. We can see it as store id -> region lists.
	storeCandidateRegionMap := make(map[uint64]map[string]RegionInfo)
	totalRegionCandidateNum := 0
	totalRemainingRegionNum := 0

	if !isMPP {
		for _, task := range originalTasks {
			taskStoreID := task.regionInfos[0].AllStores[0]
			batchTask := &batchCopTask{
				storeAddr:   task.storeAddr,
				cmdType:     task.cmdType,
				ctx:         task.ctx,
				regionInfos: []RegionInfo{task.regionInfos[0]},
			}
			storeTaskMap[taskStoreID] = batchTask
		}
	} else {
		stores := cache.RegionCache.GetTiFlashStores()
		aliveStores := filterAliveStores(ctx, stores, ttl, kvStore)
		for _, s := range aliveStores {
			storeTaskMap[s.StoreID()] = &batchCopTask{
				storeAddr: s.GetAddr(),
				cmdType:   originalTasks[0].cmdType,
				ctx:       &tikv.RPCContext{Addr: s.GetAddr(), Store: s},
			}
		}
	}

	var candidateRegionInfos []RegionInfo
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
				totalRemainingRegionNum++
				candidateRegionInfos = append(candidateRegionInfos, ri)
				taskKey := ri.Region.String()
				for _, storeID := range ri.AllStores {
					if _, validStore := storeTaskMap[storeID]; !validStore {
						continue
					}
					if _, ok := storeCandidateRegionMap[storeID]; !ok {
						candidateMap := make(map[string]RegionInfo)
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

	// If balanceBatchCopTaskWithContinuity failed (not balance or return nil), it will fallback to the original balance logic.
	// So storeTaskMap should not be modify.
	var contiguousTasks []*batchCopTask = nil
	contiguousBalanceScore := 0
	if balanceWithContinuity {
		contiguousTasks, contiguousBalanceScore = balanceBatchCopTaskWithContinuity(storeTaskMap, candidateRegionInfos, balanceContinuousRegionCount)
		if isBalance(contiguousBalanceScore) && contiguousTasks != nil {
			return contiguousTasks
		}
	}

	if totalRemainingRegionNum > 0 {
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
			var ri RegionInfo
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
	}

	if contiguousTasks != nil {
		score, balanceInfos := checkBatchCopTaskBalance(storeTaskMap, balanceContinuousRegionCount)
		if !isBalance(score) {
			logutil.BgLogger().Warn("Region count is not balance and use contiguousTasks", zap.Int("contiguousBalanceScore", contiguousBalanceScore), zap.Int("score", score), zap.Strings("balanceInfos", balanceInfos))
			return contiguousTasks
		}
	}

	var ret []*batchCopTask
	for _, task := range storeTaskMap {
		if len(task.regionInfos) > 0 {
			ret = append(ret, task)
		}
	}
	return ret
}

func buildBatchCopTasksForNonPartitionedTable(
	ctx context.Context,
	bo *backoff.Backoffer,
	store *kvStore,
	ranges *KeyRanges,
	storeType kv.StoreType,
	isMPP bool,
	ttl time.Duration,
	balanceWithContinuity bool,
	balanceContinuousRegionCount int64) ([]*batchCopTask, error) {
	if config.GetGlobalConfig().DisaggregatedTiFlash {
		if config.GetGlobalConfig().UseAutoScaler {
			return buildBatchCopTasksConsistentHash(ctx, bo, store, []*KeyRanges{ranges}, storeType, ttl)
		}
		return buildBatchCopTasksConsistentHashForPD(bo, store, []*KeyRanges{ranges}, storeType, ttl)
	}
	return buildBatchCopTasksCore(bo, store, []*KeyRanges{ranges}, storeType, isMPP, ttl, balanceWithContinuity, balanceContinuousRegionCount)
}

func buildBatchCopTasksForPartitionedTable(
	ctx context.Context,
	bo *backoff.Backoffer,
	store *kvStore,
	rangesForEachPhysicalTable []*KeyRanges,
	storeType kv.StoreType,
	isMPP bool,
	ttl time.Duration,
	balanceWithContinuity bool,
	balanceContinuousRegionCount int64,
	partitionIDs []int64) (batchTasks []*batchCopTask, err error) {
	if config.GetGlobalConfig().DisaggregatedTiFlash {
		if config.GetGlobalConfig().UseAutoScaler {
			batchTasks, err = buildBatchCopTasksConsistentHash(ctx, bo, store, rangesForEachPhysicalTable, storeType, ttl)
		} else {
			// todo: remove this after AutoScaler is stable.
			batchTasks, err = buildBatchCopTasksConsistentHashForPD(bo, store, rangesForEachPhysicalTable, storeType, ttl)
		}
	} else {
		batchTasks, err = buildBatchCopTasksCore(bo, store, rangesForEachPhysicalTable, storeType, isMPP, ttl, balanceWithContinuity, balanceContinuousRegionCount)
	}
	if err != nil {
		return nil, err
	}
	// generate tableRegions for batchCopTasks
	convertRegionInfosToPartitionTableRegions(batchTasks, partitionIDs)
	return batchTasks, nil
}

func filterAliveStores(ctx context.Context, stores []*tikv.Store, ttl time.Duration, kvStore *kvStore) (aliveStores []*tikv.Store) {
	storesStr := make([]string, 0, len(stores))
	for _, s := range stores {
		storesStr = append(storesStr, s.GetAddr())
	}

	aliveIdx := filterAliveStoresHelper(ctx, storesStr, ttl, kvStore)
	for _, idx := range aliveIdx {
		aliveStores = append(aliveStores, stores[idx])
	}
	return aliveStores
}

func filterAliveStoresHelper(ctx context.Context, stores []string, ttl time.Duration, kvStore *kvStore) (aliveIdx []int) {
	var wg sync.WaitGroup
	var mu sync.Mutex
	wg.Add(len(stores))
	for i := range stores {
		go func(idx int) {
			defer wg.Done()
			s := stores[idx]

			// Check if store is failed already.
			if ok := GlobalMPPFailedStoreProber.IsRecovery(ctx, s, ttl); !ok {
				return
			}

			tikvClient := kvStore.GetTiKVClient()
			if ok := detectMPPStore(ctx, tikvClient, s, DetectTimeoutLimit); !ok {
				GlobalMPPFailedStoreProber.Add(ctx, s, tikvClient)
				return
			}

			mu.Lock()
			defer mu.Unlock()
			aliveIdx = append(aliveIdx, idx)
		}(i)
	}
	wg.Wait()

	logutil.BgLogger().Info("detecting available mpp stores", zap.Any("total", len(stores)), zap.Any("alive", len(aliveIdx)))
	return aliveIdx
}

func getTiFlashComputeRPCContextByConsistentHash(ids []tikv.RegionVerID, storesStr []string) (res []*tikv.RPCContext, err error) {
	hasher := consistent.New()
	for _, addr := range storesStr {
		hasher.Add(addr)
	}

	for _, id := range ids {
		addr, err := hasher.Get(strconv.Itoa(int(id.GetID())))
		if err != nil {
			return nil, err
		}

		rpcCtx := &tikv.RPCContext{
			Region: id,
			Addr:   addr,
		}

		res = append(res, rpcCtx)
	}
	return res, nil
}

// 1. Split range by region location to build copTasks.
// 2. For each copTask build its rpcCtx , the target tiflash_compute node will be chosen using consistent hash.
// 3. All copTasks that will be sent to one tiflash_compute node are put in one batchCopTask.
func buildBatchCopTasksConsistentHash(
	ctx context.Context,
	bo *backoff.Backoffer,
	kvStore *kvStore,
	rangesForEachPhysicalTable []*KeyRanges,
	storeType kv.StoreType,
	ttl time.Duration) (res []*batchCopTask, err error) {
	const cmdType = tikvrpc.CmdBatchCop
	cache := kvStore.GetRegionCache()
	fetchTopoBo := backoff.NewBackofferWithVars(ctx, fetchTopoMaxBackoff, nil)

	var retryNum int
	var rangesLen int
	var storesStr []string

	tasks := make([]*copTask, 0)
	regionIDs := make([]tikv.RegionVerID, 0)

	for i, ranges := range rangesForEachPhysicalTable {
		rangesLen += ranges.Len()
		locations, err := cache.SplitKeyRangesByLocations(bo, ranges, UnspecifiedLimit)
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

	for {
		retryNum++
		// todo: use AssureAndGetTopo() after SNS is done.
		storesStr, err = tiflashcompute.GetGlobalTopoFetcher().FetchAndGetTopo()
		if err != nil {
			return nil, err
		}
		if len(storesStr) == 0 {
			retErr := errors.New("Cannot find proper topo from AutoScaler")
			logutil.BgLogger().Info("buildBatchCopTasksConsistentHash retry because FetchAndGetTopo return empty topo", zap.Int("retryNum", retryNum))
			err := fetchTopoBo.Backoff(tikv.BoTiFlashRPC(), retErr)
			if err != nil {
				return nil, retErr
			}
			continue
		}
		break
	}

	rpcCtxs, err := getTiFlashComputeRPCContextByConsistentHash(regionIDs, storesStr)
	if err != nil {
		return nil, err
	}
	if len(rpcCtxs) != len(tasks) {
		return nil, errors.Errorf("length should be equal, len(rpcCtxs): %d, len(tasks): %d", len(rpcCtxs), len(tasks))
	}
	taskMap := make(map[string]*batchCopTask)
	for i, rpcCtx := range rpcCtxs {
		regionInfo := RegionInfo{
			// tasks and rpcCtxs are correspond to each other.
			Region:         tasks[i].region,
			Ranges:         tasks[i].ranges,
			PartitionIndex: tasks[i].partitionIndex,
			// No need to setup regionMeta and Store info.
			// Meta:           rpcCtx.Meta,
			// AllStores:      []uint64{rpcCtx.Store.StoreID()},
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
	logutil.BgLogger().Info("buildBatchCopTasksConsistentHash done", zap.Any("len(tasks)", len(taskMap)), zap.Any("len(tiflash_compute)", len(storesStr)))

	failpointCheckForConsistentHash(res)
	return res, nil
}

func failpointCheckForConsistentHash(tasks []*batchCopTask) {
	failpoint.Inject("checkOnlyDispatchToTiFlashComputeNodes", func(val failpoint.Value) {
		logutil.BgLogger().Debug("in checkOnlyDispatchToTiFlashComputeNodes")

		// This failpoint will be tested in test-infra case, because we needs setup a cluster.
		// All tiflash_compute nodes addrs are stored in val, separated by semicolon.
		str := val.(string)
		addrs := strings.Split(str, ";")
		if len(addrs) < 1 {
			err := fmt.Sprintf("unexpected length of tiflash_compute node addrs: %v, %s", len(addrs), str)
			panic(err)
		}
		addrMap := make(map[string]struct{})
		for _, addr := range addrs {
			addrMap[addr] = struct{}{}
		}
		for _, batchTask := range tasks {
			if _, ok := addrMap[batchTask.storeAddr]; !ok {
				err := errors.Errorf("batchCopTask send to node which is not tiflash_compute: %v(tiflash_compute nodes: %s)", batchTask.storeAddr, str)
				panic(err)
			}
		}
	})
}

// When `partitionIDs != nil`, it means that buildBatchCopTasksCore is constructing a batch cop tasks for PartitionTableScan.
// At this time, `len(rangesForEachPhysicalTable) == len(partitionIDs)` and `rangesForEachPhysicalTable[i]` is for partition `partitionIDs[i]`.
// Otherwise, `rangesForEachPhysicalTable[0]` indicates the range for the single physical table.
func buildBatchCopTasksCore(bo *backoff.Backoffer, store *kvStore, rangesForEachPhysicalTable []*KeyRanges, storeType kv.StoreType, isMPP bool, ttl time.Duration, balanceWithContinuity bool, balanceContinuousRegionCount int64) ([]*batchCopTask, error) {
	cache := store.GetRegionCache()
	start := time.Now()
	const cmdType = tikvrpc.CmdBatchCop
	rangesLen := 0

	for {
		var tasks []*copTask
		rangesLen = 0
		for i, ranges := range rangesForEachPhysicalTable {
			rangesLen += ranges.Len()
			locations, err := cache.SplitKeyRangesByLocations(bo, ranges, UnspecifiedLimit)
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
			}
		}

		var batchTasks []*batchCopTask

		storeTaskMap := make(map[string]*batchCopTask)
		needRetry := false
		for _, task := range tasks {
			rpcCtx, err := cache.GetTiFlashRPCContext(bo.TiKVBackoffer(), task.region, isMPP)
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
				batchCop.regionInfos = append(batchCop.regionInfos, RegionInfo{Region: task.region, Meta: rpcCtx.Meta, Ranges: task.ranges, AllStores: allStores, PartitionIndex: task.partitionIndex})
			} else {
				batchTask := &batchCopTask{
					storeAddr:   rpcCtx.Addr,
					cmdType:     cmdType,
					ctx:         rpcCtx,
					regionInfos: []RegionInfo{{Region: task.region, Meta: rpcCtx.Meta, Ranges: task.ranges, AllStores: allStores, PartitionIndex: task.partitionIndex}},
				}
				storeTaskMap[rpcCtx.Addr] = batchTask
			}
		}
		if needRetry {
			// As mentioned above, nil rpcCtx is always attributed to failed stores.
			// It's equal to long poll the store but get no response. Here we'd better use
			// TiFlash error to trigger the TiKV fallback mechanism.
			err := bo.Backoff(tikv.BoTiFlashRPC(), errors.New("Cannot find region with TiFlash peer"))
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
		balanceStart := time.Now()
		batchTasks = balanceBatchCopTask(bo.GetCtx(), store, batchTasks, isMPP, ttl, balanceWithContinuity, balanceContinuousRegionCount)
		balanceElapsed := time.Since(balanceStart)
		if log.GetLevel() <= zap.DebugLevel {
			msg := "After region balance:"
			for _, task := range batchTasks {
				msg += " store " + task.storeAddr + ": " + strconv.Itoa(len(task.regionInfos)) + " regions,"
			}
			logutil.BgLogger().Debug(msg)
		}

		if elapsed := time.Since(start); elapsed > time.Millisecond*500 {
			logutil.BgLogger().Warn("buildBatchCopTasksCore takes too much time",
				zap.Duration("elapsed", elapsed),
				zap.Duration("balanceElapsed", balanceElapsed),
				zap.Int("range len", rangesLen),
				zap.Int("task len", len(batchTasks)))
		}
		metrics.TxnRegionsNumHistogramWithBatchCoprocessor.Observe(float64(len(batchTasks)))
		return batchTasks, nil
	}
}

func convertRegionInfosToPartitionTableRegions(batchTasks []*batchCopTask, partitionIDs []int64) {
	for _, copTask := range batchTasks {
		tableRegions := make([]*coprocessor.TableRegions, len(partitionIDs))
		// init coprocessor.TableRegions
		for j, pid := range partitionIDs {
			tableRegions[j] = &coprocessor.TableRegions{
				PhysicalTableId: pid,
			}
		}
		// fill region infos
		for _, ri := range copTask.regionInfos {
			tableRegions[ri.PartitionIndex].Regions = append(tableRegions[ri.PartitionIndex].Regions,
				ri.toCoprocessorRegionInfo())
		}
		count := 0
		// clear empty table region
		for j := 0; j < len(tableRegions); j++ {
			if len(tableRegions[j].Regions) != 0 {
				tableRegions[count] = tableRegions[j]
				count++
			}
		}
		copTask.PartitionTableRegions = tableRegions[:count]
		copTask.regionInfos = nil
	}
}

func (c *CopClient) sendBatch(ctx context.Context, req *kv.Request, vars *tikv.Variables, option *kv.ClientSendOption) kv.Response {
	if req.KeepOrder || req.Desc {
		return copErrorResponse{errors.New("batch coprocessor cannot prove keep order or desc property")}
	}
	ctx = context.WithValue(ctx, tikv.TxnStartKey(), req.StartTs)
	bo := backoff.NewBackofferWithVars(ctx, copBuildTaskMaxBackoff, vars)

	var tasks []*batchCopTask
	var err error
	if req.PartitionIDAndRanges != nil {
		// For Partition Table Scan
		keyRanges := make([]*KeyRanges, 0, len(req.PartitionIDAndRanges))
		partitionIDs := make([]int64, 0, len(req.PartitionIDAndRanges))
		for _, pi := range req.PartitionIDAndRanges {
			keyRanges = append(keyRanges, NewKeyRanges(pi.KeyRanges))
			partitionIDs = append(partitionIDs, pi.ID)
		}
		tasks, err = buildBatchCopTasksForPartitionedTable(ctx, bo, c.store.kvStore, keyRanges, req.StoreType, false, 0, false, 0, partitionIDs)
	} else {
		// TODO: merge the if branch.
		ranges := NewKeyRanges(req.KeyRanges.FirstPartitionRange())
		tasks, err = buildBatchCopTasksForNonPartitionedTable(ctx, bo, c.store.kvStore, ranges, req.StoreType, false, 0, false, 0)
	}

	if err != nil {
		return copErrorResponse{err}
	}
	it := &batchCopIterator{
		store:                      c.store.kvStore,
		req:                        req,
		finishCh:                   make(chan struct{}),
		vars:                       vars,
		rpcCancel:                  tikv.NewRPCanceller(),
		enableCollectExecutionInfo: option.EnableCollectExecutionInfo,
	}
	ctx = context.WithValue(ctx, tikv.RPCCancellerCtxKey{}, it.rpcCancel)
	it.tasks = tasks
	it.respChan = make(chan *batchCopResponse, 2048)
	go it.run(ctx)
	return it
}

type batchCopIterator struct {
	store    *kvStore
	req      *kv.Request
	finishCh chan struct{}

	tasks []*batchCopTask

	// Batch results are stored in respChan.
	respChan chan *batchCopResponse

	vars *tikv.Variables

	rpcCancel *tikv.RPCCanceller

	wg sync.WaitGroup
	// closed represents when the Close is called.
	// There are two cases we need to close the `finishCh` channel, one is when context is done, the other one is
	// when the Close is called. we use atomic.CompareAndSwap `closed` to to make sure the channel is not closed twice.
	closed uint32

	enableCollectExecutionInfo bool
}

func (b *batchCopIterator) run(ctx context.Context) {
	// We run workers for every batch cop.
	for _, task := range b.tasks {
		b.wg.Add(1)
		boMaxSleep := copNextMaxBackoff
		failpoint.Inject("ReduceCopNextMaxBackoff", func(value failpoint.Value) {
			if value.(bool) {
				boMaxSleep = 2
			}
		})
		bo := backoff.NewBackofferWithVars(ctx, boMaxSleep, b.vars)
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
				resp = &batchCopResponse{err: derr.ErrQueryInterrupted}
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

func (b *batchCopIterator) handleTask(ctx context.Context, bo *Backoffer, task *batchCopTask) {
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
func (b *batchCopIterator) retryBatchCopTask(ctx context.Context, bo *backoff.Backoffer, batchTask *batchCopTask) ([]*batchCopTask, error) {
	if batchTask.regionInfos != nil {
		var ranges []kv.KeyRange
		for _, ri := range batchTask.regionInfos {
			ri.Ranges.Do(func(ran *kv.KeyRange) {
				ranges = append(ranges, *ran)
			})
		}
		ret, err := buildBatchCopTasksForNonPartitionedTable(ctx, bo, b.store, NewKeyRanges(ranges), b.req.StoreType, false, 0, false, 0)
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
		keyRanges = append(keyRanges, NewKeyRanges(ranges))
	}
	ret, err := buildBatchCopTasksForPartitionedTable(ctx, bo, b.store, keyRanges, b.req.StoreType, false, 0, false, 0, pid)
	return ret, err
}

const readTimeoutUltraLong = 3600 * time.Second // For requests that may scan many regions for tiflash.

func (b *batchCopIterator) handleTaskOnce(ctx context.Context, bo *backoff.Backoffer, task *batchCopTask) ([]*batchCopTask, error) {
	sender := NewRegionBatchRequestSender(b.store.GetRegionCache(), b.store.GetTiKVClient(), b.enableCollectExecutionInfo)
	var regionInfos = make([]*coprocessor.RegionInfo, 0, len(task.regionInfos))
	for _, ri := range task.regionInfos {
		regionInfos = append(regionInfos, ri.toCoprocessorRegionInfo())
	}

	copReq := coprocessor.BatchRequest{
		Tp:           b.req.Tp,
		StartTs:      b.req.StartTs,
		Data:         b.req.Data,
		SchemaVer:    b.req.SchemaVar,
		Regions:      regionInfos,
		TableRegions: task.PartitionTableRegions,
	}

	req := tikvrpc.NewRequest(task.cmdType, &copReq, kvrpcpb.Context{
		IsolationLevel: isolationLevelToPB(b.req.IsolationLevel),
		Priority:       priorityToPB(b.req.Priority),
		NotFillCache:   b.req.NotFillCache,
		RecordTimeStat: true,
		RecordScanStat: true,
		TaskId:         b.req.TaskID,
	})
	if b.req.ResourceGroupTagger != nil {
		b.req.ResourceGroupTagger(req)
	}
	req.StoreTp = getEndPointType(kv.TiFlash)

	logutil.BgLogger().Debug("send batch request to ", zap.String("req info", req.String()), zap.Int("cop task len", len(task.regionInfos)))
	resp, retry, cancel, err := sender.SendReqToAddr(bo, task.ctx, task.regionInfos, req, readTimeoutUltraLong)
	// If there are store errors, we should retry for all regions.
	if retry {
		return b.retryBatchCopTask(ctx, bo, task)
	}
	if err != nil {
		err = derr.ToTiDBErr(err)
		return nil, errors.Trace(err)
	}
	defer cancel()
	return nil, b.handleStreamedBatchCopResponse(ctx, bo, resp.Resp.(*tikvrpc.BatchCopStreamResponse), task)
}

func (b *batchCopIterator) handleStreamedBatchCopResponse(ctx context.Context, bo *Backoffer, response *tikvrpc.BatchCopStreamResponse, task *batchCopTask) (err error) {
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
	ttl time.Duration) (res []*batchCopTask, err error) {
	const cmdType = tikvrpc.CmdBatchCop
	var retryNum int
	cache := kvStore.GetRegionCache()

	for {
		retryNum++
		var rangesLen int
		tasks := make([]*copTask, 0)
		regionIDs := make([]tikv.RegionVerID, 0)

		for i, ranges := range rangesForEachPhysicalTable {
			rangesLen += ranges.Len()
			locations, err := cache.SplitKeyRangesByLocations(bo, ranges, UnspecifiedLimit)
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

		stores, err := cache.GetTiFlashComputeStores(bo.TiKVBackoffer())
		if err != nil {
			return nil, err
		}
		stores = filterAliveStores(bo.GetCtx(), stores, ttl, kvStore)
		if len(stores) == 0 {
			return nil, errors.New("tiflash_compute node is unavailable")
		}

		rpcCtxs, err := cache.GetTiFlashComputeRPCContextByConsistentHash(bo.TiKVBackoffer(), regionIDs, stores)
		if err != nil {
			return nil, err
		}
		if rpcCtxs == nil {
			logutil.BgLogger().Info("buildBatchCopTasksConsistentHash retry because rcpCtx is nil", zap.Int("retryNum", retryNum))
			err := bo.Backoff(tikv.BoTiFlashRPC(), errors.New("Cannot find region with TiFlash peer"))
			if err != nil {
				return nil, errors.Trace(err)
			}
			continue
		}
		if len(rpcCtxs) != len(tasks) {
			return nil, errors.Errorf("length should be equal, len(rpcCtxs): %d, len(tasks): %d", len(rpcCtxs), len(tasks))
		}
		taskMap := make(map[string]*batchCopTask)
		for i, rpcCtx := range rpcCtxs {
			regionInfo := RegionInfo{
				// tasks and rpcCtxs are correspond to each other.
				Region:         tasks[i].region,
				Meta:           rpcCtx.Meta,
				Ranges:         tasks[i].ranges,
				AllStores:      []uint64{rpcCtx.Store.StoreID()},
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
		logutil.BgLogger().Info("buildBatchCopTasksConsistentHash done", zap.Any("len(tasks)", len(taskMap)), zap.Any("len(tiflash_compute)", len(stores)))
		break
	}

	failpointCheckForConsistentHash(res)
	return res, nil
}
