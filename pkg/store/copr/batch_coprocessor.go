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
	"fmt"
	"math"
	"math/rand"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl/placement"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/store/driver/backoff"
	derr "github.com/pingcap/tidb/pkg/store/driver/error"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/tiflash"
	"github.com/pingcap/tidb/pkg/util/tiflashcompute"
	"github.com/tikv/client-go/v2/metrics"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/twmb/murmur3"
	"go.uber.org/zap"
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
func balanceBatchCopTask(aliveStores []*tikv.Store, originalTasks []*batchCopTask, balanceWithContinuity bool, balanceContinuousRegionCount int64, allRegionInfos []RegionInfo) []*batchCopTask {
	if len(originalTasks) == 0 {
		log.Info("Batch cop task balancer got an empty task set.")
		return originalTasks
	}
	storeTaskMap := make(map[uint64]*batchCopTask)
	// storeCandidateRegionMap stores all the possible store->region map. Its content is
	// store id -> region signature -> region info. We can see it as store id -> region lists.
	storeCandidateRegionMap := make(map[uint64]map[string]RegionInfo)
	totalRegionCandidateNum := 0
	totalRemainingRegionNum := 0

	for _, s := range aliveStores {
		storeTaskMap[s.StoreID()] = &batchCopTask{
			storeAddr: s.GetAddr(),
			cmdType:   originalTasks[0].cmdType,
			ctx:       &tikv.RPCContext{Addr: s.GetAddr(), Store: s},
		}
	}

	var candidateRegionInfos []RegionInfo = make([]RegionInfo, 0, len(allRegionInfos))
	for _, ri := range allRegionInfos {
		// for each region, figure out the valid store num
		validStoreNum := 0
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
	balanceContinuousRegionCount int64,
	dispatchPolicy tiflashcompute.DispatchPolicy,
	tiflashReplicaReadPolicy tiflash.ReplicaRead,
	appendWarning func(error)) ([]*batchCopTask, error) {
	if config.GetGlobalConfig().DisaggregatedTiFlash {
		if config.GetGlobalConfig().UseAutoScaler {
			return buildBatchCopTasksConsistentHash(ctx, bo, store, []*KeyRanges{ranges}, storeType, ttl, dispatchPolicy)
		}
		return buildBatchCopTasksConsistentHashForPD(bo, store, []*KeyRanges{ranges}, storeType, ttl, dispatchPolicy)
	}
	return buildBatchCopTasksCore(bo, store, []*KeyRanges{ranges}, storeType, isMPP, ttl, balanceWithContinuity, balanceContinuousRegionCount, tiflashReplicaReadPolicy, appendWarning)
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
	partitionIDs []int64,
	dispatchPolicy tiflashcompute.DispatchPolicy,
	tiflashReplicaReadPolicy tiflash.ReplicaRead,
	appendWarning func(error)) (batchTasks []*batchCopTask, err error) {
	if config.GetGlobalConfig().DisaggregatedTiFlash {
		if config.GetGlobalConfig().UseAutoScaler {
			batchTasks, err = buildBatchCopTasksConsistentHash(ctx, bo, store, rangesForEachPhysicalTable, storeType, ttl, dispatchPolicy)
		} else {
			// todo: remove this after AutoScaler is stable.
			batchTasks, err = buildBatchCopTasksConsistentHashForPD(bo, store, rangesForEachPhysicalTable, storeType, ttl, dispatchPolicy)
		}
	} else {
		batchTasks, err = buildBatchCopTasksCore(bo, store, rangesForEachPhysicalTable, storeType, isMPP, ttl, balanceWithContinuity, balanceContinuousRegionCount, tiflashReplicaReadPolicy, appendWarning)
	}
	if err != nil {
		return nil, err
	}
	// generate tableRegions for batchCopTasks
	convertRegionInfosToPartitionTableRegions(batchTasks, partitionIDs)
	return batchTasks, nil
}

func filterAliveStoresStr(ctx context.Context, storesStr []string, ttl time.Duration, kvStore *kvStore) (aliveStores []string) {
	aliveIdx := filterAliveStoresHelper(ctx, storesStr, ttl, kvStore)
	for _, idx := range aliveIdx {
		aliveStores = append(aliveStores, storesStr[idx])
	}
	return aliveStores
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

	if len(stores) != len(aliveIdx) {
		logutil.BgLogger().Info("detecting available mpp stores", zap.Int("total", len(stores)), zap.Int("alive", len(aliveIdx)))
	}
	return aliveIdx
}

func getTiFlashComputeRPCContextByConsistentHash(ids []tikv.RegionVerID, storesStr []string) (res []*tikv.RPCContext, err error) {
	// Use RendezvousHash
	for _, id := range ids {
		var maxHash uint32 = 0
		var maxHashStore string = ""
		for _, store := range storesStr {
			h := murmur3.StringSum32(fmt.Sprintf("%s-%d", store, id.GetID()))
			if h > maxHash {
				maxHash = h
				maxHashStore = store
			}
		}
		rpcCtx := &tikv.RPCContext{
			Region: id,
			Addr:   maxHashStore,
		}
		res = append(res, rpcCtx)
	}
	return res, nil
}

func getTiFlashComputeRPCContextByRoundRobin(ids []tikv.RegionVerID, storesStr []string) (res []*tikv.RPCContext, err error) {
	startIdx := rand.Intn(len(storesStr))
	for _, id := range ids {
		rpcCtx := &tikv.RPCContext{
			Region: id,
			Addr:   storesStr[startIdx%len(storesStr)],
		}

		startIdx++
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
	ttl time.Duration,
	dispatchPolicy tiflashcompute.DispatchPolicy) (res []*batchCopTask, err error) {
	failpointCheckWhichPolicy(dispatchPolicy)
	start := time.Now()
	const cmdType = tikvrpc.CmdBatchCop
	cache := kvStore.GetRegionCache()
	fetchTopoBo := backoff.NewBackofferWithVars(ctx, fetchTopoMaxBackoff, nil)

	var (
		retryNum  int
		rangesLen int
		storesStr []string
	)

	tasks := make([]*copTask, 0)
	regionIDs := make([]tikv.RegionVerID, 0)

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
	splitKeyElapsed := time.Since(start)

	fetchTopoStart := time.Now()
	for {
		retryNum++
		storesStr, err = tiflashcompute.GetGlobalTopoFetcher().FetchAndGetTopo()
		if err != nil {
			return nil, err
		}
		storesBefFilter := len(storesStr)
		storesStr = filterAliveStoresStr(ctx, storesStr, ttl, kvStore)
		logutil.BgLogger().Info("topo filter alive", zap.Strings("topo", storesStr))
		if len(storesStr) == 0 {
			errMsg := "Cannot find proper topo to dispatch MPPTask: "
			if storesBefFilter == 0 {
				errMsg += "topo from AutoScaler is empty"
			} else {
				errMsg += "detect aliveness failed, no alive ComputeNode"
			}
			retErr := errors.New(errMsg)
			logutil.BgLogger().Info("buildBatchCopTasksConsistentHash retry because FetchAndGetTopo return empty topo", zap.Int("retryNum", retryNum))
			if intest.InTest && retryNum > 3 {
				return nil, retErr
			}
			err := fetchTopoBo.Backoff(tikv.BoTiFlashRPC(), retErr)
			if err != nil {
				return nil, retErr
			}
			continue
		}
		break
	}
	fetchTopoElapsed := time.Since(fetchTopoStart)

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
	logutil.BgLogger().Info("buildBatchCopTasksConsistentHash done",
		zap.Int("len(tasks)", len(taskMap)),
		zap.Int("len(tiflash_compute)", len(storesStr)),
		zap.String("dispatchPolicy", tiflashcompute.GetDispatchPolicy(dispatchPolicy)))

	if log.GetLevel() <= zap.DebugLevel {
		debugTaskMap := make(map[string]string, len(taskMap))
		for s, b := range taskMap {
			debugTaskMap[s] = fmt.Sprintf("addr: %s; regionInfos: %v", b.storeAddr, b.regionInfos)
		}
		logutil.BgLogger().Debug("detailed info buildBatchCopTasksConsistentHash", zap.Any("taskMap", debugTaskMap), zap.Strings("allStores", storesStr))
	}

	if elapsed := time.Since(start); elapsed > time.Millisecond*500 {
		logutil.BgLogger().Warn("buildBatchCopTasksConsistentHash takes too much time",
			zap.Duration("total elapsed", elapsed),
			zap.Int("retryNum", retryNum),
			zap.Duration("splitKeyElapsed", splitKeyElapsed),
			zap.Duration("fetchTopoElapsed", fetchTopoElapsed),
			zap.Int("range len", rangesLen),
			zap.Int("copTaskNum", len(tasks)),
			zap.Int("batchCopTaskNum", len(res)))
	}
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

func failpointCheckWhichPolicy(act tiflashcompute.DispatchPolicy) {
	failpoint.Inject("testWhichDispatchPolicy", func(exp failpoint.Value) {
		expStr := exp.(string)
		actStr := tiflashcompute.GetDispatchPolicy(act)
		if actStr != expStr {
			err := errors.Errorf("tiflash_compute dispatch should be %v, but got %v", expStr, actStr)
			panic(err)
		}
	})
}

func filterAllStoresAccordingToTiFlashReplicaRead(allStores []uint64, aliveStores *aliveStoresBundle, policy tiflash.ReplicaRead) (storesMatchedPolicy []uint64, needsCrossZoneAccess bool) {
	if policy.IsAllReplicas() {
		for _, id := range allStores {
			if _, ok := aliveStores.storeIDsInAllZones[id]; ok {
				storesMatchedPolicy = append(storesMatchedPolicy, id)
			}
		}
		return
	}
	// Check whether exists available stores in TiDB zone. If so, we only need to access TiFlash stores in TiDB zone.
	for _, id := range allStores {
		if _, ok := aliveStores.storeIDsInTiDBZone[id]; ok {
			storesMatchedPolicy = append(storesMatchedPolicy, id)
		}
	}
	// If no available stores in TiDB zone, we need to access TiFlash stores in other zones.
	if len(storesMatchedPolicy) == 0 {
		// needsCrossZoneAccess indicates whether we need to access(directly read or remote read) TiFlash stores in other zones.
		needsCrossZoneAccess = true

		if policy == tiflash.ClosestAdaptive {
			// If the policy is `ClosestAdaptive`, we can dispatch tasks to the TiFlash stores in other zones.
			for _, id := range allStores {
				if _, ok := aliveStores.storeIDsInAllZones[id]; ok {
					storesMatchedPolicy = append(storesMatchedPolicy, id)
				}
			}
		} else if policy == tiflash.ClosestReplicas {
			// If the policy is `ClosestReplicas`, we dispatch tasks to the TiFlash stores in TiDB zone and remote read from other zones.
			for id := range aliveStores.storeIDsInTiDBZone {
				storesMatchedPolicy = append(storesMatchedPolicy, id)
			}
		}
	}
	return
}

func getAllUsedTiFlashStores(allTiFlashStores []*tikv.Store, allUsedTiFlashStoresMap map[uint64]struct{}) []*tikv.Store {
	allUsedTiFlashStores := make([]*tikv.Store, 0, len(allUsedTiFlashStoresMap))
	for _, store := range allTiFlashStores {
		_, ok := allUsedTiFlashStoresMap[store.StoreID()]
		if ok {
			allUsedTiFlashStores = append(allUsedTiFlashStores, store)
		}
	}
	return allUsedTiFlashStores
}

// getAliveStoresAndStoreIDs gets alive TiFlash stores and their IDs.
// If tiflashReplicaReadPolicy is not all_replicas, it will also return the IDs of the alive TiFlash stores in TiDB zone.
func getAliveStoresAndStoreIDs(ctx context.Context, cache *RegionCache, allUsedTiFlashStoresMap map[uint64]struct{}, ttl time.Duration, store *kvStore, tiflashReplicaReadPolicy tiflash.ReplicaRead, tidbZone string) (aliveStores *aliveStoresBundle) {
	aliveStores = new(aliveStoresBundle)
	allTiFlashStores := cache.RegionCache.GetTiFlashStores(tikv.LabelFilterNoTiFlashWriteNode)
	allUsedTiFlashStores := getAllUsedTiFlashStores(allTiFlashStores, allUsedTiFlashStoresMap)

	// Get storesInAllZones and storeIDsInAllZones for all policy.
	aliveStores.storesInAllZones = filterAliveStores(ctx, allUsedTiFlashStores, ttl, store)
	aliveStores.storeIDsInAllZones = make(map[uint64]struct{}, len(aliveStores.storesInAllZones))
	for _, as := range aliveStores.storesInAllZones {
		aliveStores.storeIDsInAllZones[as.StoreID()] = struct{}{}
	}

	// Only get storesInTiDBZone and storeIDsInTiDBZone for closest_replica and closest_adaptive.
	if !tiflashReplicaReadPolicy.IsAllReplicas() {
		aliveStores.storeIDsInTiDBZone = make(map[uint64]struct{}, len(aliveStores.storesInAllZones))
		for _, as := range aliveStores.storesInAllZones {
			// If the `zone` label of the TiFlash store is not set, we treat it as a TiFlash store in other zones.
			if tiflashZone, isSet := as.GetLabelValue(placement.DCLabelKey); isSet && tiflashZone == tidbZone {
				aliveStores.storeIDsInTiDBZone[as.StoreID()] = struct{}{}
				aliveStores.storesInTiDBZone = append(aliveStores.storesInTiDBZone, as)
			}
		}
	}
	return aliveStores
}

// filterAccessibleStoresAndBuildRegionInfo filters the stores that can be accessed according to:
// 1. tiflash_replica_read policy
// 2. whether the store is alive
// After filtering, it will build the RegionInfo.
func filterAccessibleStoresAndBuildRegionInfo(
	cache *RegionCache,
	allStores []uint64,
	bo *Backoffer,
	task *copTask,
	rpcCtx *tikv.RPCContext,
	aliveStores *aliveStoresBundle,
	tiflashReplicaReadPolicy tiflash.ReplicaRead,
	regionInfoNeedsReloadOnSendFail []RegionInfo,
	regionsInOtherZones []uint64,
	maxRemoteReadCountAllowed int,
	tidbZone string) (regionInfo RegionInfo, _ []RegionInfo, _ []uint64, err error) {
	needCrossZoneAccess := false
	allStores, needCrossZoneAccess = filterAllStoresAccordingToTiFlashReplicaRead(allStores, aliveStores, tiflashReplicaReadPolicy)

	regionInfo = RegionInfo{
		Region:         task.region,
		Meta:           rpcCtx.Meta,
		Ranges:         task.ranges,
		AllStores:      allStores,
		PartitionIndex: task.partitionIndex}

	if needCrossZoneAccess {
		regionsInOtherZones = append(regionsInOtherZones, task.region.GetID())
		regionInfoNeedsReloadOnSendFail = append(regionInfoNeedsReloadOnSendFail, regionInfo)
		if tiflashReplicaReadPolicy.IsClosestReplicas() && len(regionsInOtherZones) > maxRemoteReadCountAllowed {
			regionIDErrMsg := ""
			for i := 0; i < 3 && i < len(regionsInOtherZones); i++ {
				regionIDErrMsg += fmt.Sprintf("%d, ", regionsInOtherZones[i])
			}
			err = errors.Errorf(
				"no less than %d region(s) can not be accessed by TiFlash in the zone [%s]: %setc",
				len(regionsInOtherZones), tidbZone, regionIDErrMsg)
			// We need to reload the region cache here to avoid the failure throughout the region cache refresh TTL.
			cache.OnSendFailForBatchRegions(bo, rpcCtx.Store, regionInfoNeedsReloadOnSendFail, true, err)
			return regionInfo, nil, nil, err
		}
	}
	return regionInfo, regionInfoNeedsReloadOnSendFail, regionsInOtherZones, nil
}

type aliveStoresBundle struct {
	storesInAllZones   []*tikv.Store
	storeIDsInAllZones map[uint64]struct{}
	storesInTiDBZone   []*tikv.Store
	storeIDsInTiDBZone map[uint64]struct{}
}

// When `partitionIDs != nil`, it means that buildBatchCopTasksCore is constructing a batch cop tasks for PartitionTableScan.
// At this time, `len(rangesForEachPhysicalTable) == len(partitionIDs)` and `rangesForEachPhysicalTable[i]` is for partition `partitionIDs[i]`.
// Otherwise, `rangesForEachPhysicalTable[0]` indicates the range for the single physical table.
func buildBatchCopTasksCore(bo *backoff.Backoffer, store *kvStore, rangesForEachPhysicalTable []*KeyRanges, storeType kv.StoreType, isMPP bool, ttl time.Duration, balanceWithContinuity bool, balanceContinuousRegionCount int64, tiflashReplicaReadPolicy tiflash.ReplicaRead, appendWarning func(error)) ([]*batchCopTask, error) {
	cache := store.GetRegionCache()
	start := time.Now()
	const cmdType = tikvrpc.CmdBatchCop
	rangesLen := 0

	tidbZone, isTiDBLabelZoneSet := config.GetGlobalConfig().Labels[placement.DCLabelKey]
	var (
		aliveStores               *aliveStoresBundle
		maxRemoteReadCountAllowed int
		retryNum                  int
	)
	if !isTiDBLabelZoneSet {
		tiflashReplicaReadPolicy = tiflash.AllReplicas
	}

	for {
		retryNum++
		var tasks []*copTask
		var tasksForPartitions [][]*copTask = make([][]*copTask, len(rangesForEachPhysicalTable))
		rangesLen = 0
		for i, ranges := range rangesForEachPhysicalTable {
			rangesLen += ranges.Len()
			locations, err := cache.SplitKeyRangesByLocations(bo, ranges, UnspecifiedLimit, false, false)
			if err != nil {
				return nil, errors.Trace(err)
			}
			tasksForPartitions[i] = make([]*copTask, 0, len(locations))
			for _, lo := range locations {
				tasksForPartitions[i] = append(tasksForPartitions[i], &copTask{
					region:         lo.Location.Region,
					ranges:         lo.Ranges,
					cmdType:        cmdType,
					storeType:      storeType,
					partitionIndex: int64(i),
				})
			}
		}
		if len(tasksForPartitions) == 1 {
			tasks = tasksForPartitions[0]
		} else {
			slices.SortFunc(tasksForPartitions, func(a, b []*copTask) int {
				if len(a) == 0 {
					return -1
				}
				if len(b) == 0 {
					return 1
				}
				return a[0].ranges.RefAt(0).StartKey.Cmp(b[0].ranges.RefAt(0).StartKey)
			})
			// The ranges corresponding to each partiton do not intersect, so we can merge tasks directly
			for _, tasksForPartition := range tasksForPartitions {
				tasks = append(tasks, tasksForPartition...)
			}
		}

		rpcCtxs := make([]*tikv.RPCContext, 0, len(tasks))
		usedTiFlashStores := make([][]uint64, 0, len(tasks))
		usedTiFlashStoresMap := make(map[uint64]struct{}, 0)
		var needRetry bool
		minReplicaNum := uint64(math.MaxUint64)
		for _, task := range tasks {
			rpcCtx, err := cache.GetTiFlashRPCContext(bo.TiKVBackoffer(), task.region, isMPP, tikv.LabelFilterNoTiFlashWriteNode)
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

			allStores, _ := cache.GetAllValidTiFlashStores(task.region, rpcCtx.Store, tikv.LabelFilterNoTiFlashWriteNode)
			for _, storeID := range allStores {
				usedTiFlashStoresMap[storeID] = struct{}{}
			}
			rpcCtxs = append(rpcCtxs, rpcCtx)
			usedTiFlashStores = append(usedTiFlashStores, allStores)
			minReplicaNum = min(minReplicaNum, uint64(len(allStores)))
		}

		if !needRetry {
			aliveStores = getAliveStoresAndStoreIDs(bo.GetCtx(), cache, usedTiFlashStoresMap, ttl, store, tiflashReplicaReadPolicy, tidbZone)
			maxRemoteReadCountAllowed = len(aliveStores.storeIDsInTiDBZone) * tiflash.MaxRemoteReadCountPerNodeForClosestReplicas
			needRetry, _ = checkAliveStore(aliveStores, usedTiFlashStores, usedTiFlashStoresMap, cache, tiflashReplicaReadPolicy, retryNum, tasks, minReplicaNum, maxRemoteReadCountAllowed)
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

		var batchTasks []*batchCopTask
		var regionIDsInOtherZones []uint64
		var regionInfosNeedReloadOnSendFail []RegionInfo
		var allRegionInfos []RegionInfo
		storeTaskMap := make(map[string]*batchCopTask)
		storeIDsUnionSetForAllTasks := make(map[uint64]struct{})
		for idx, task := range tasks {
			var err error
			var regionInfo RegionInfo
			regionInfo, regionInfosNeedReloadOnSendFail, regionIDsInOtherZones, err = filterAccessibleStoresAndBuildRegionInfo(cache, usedTiFlashStores[idx], bo, task, rpcCtxs[idx], aliveStores, tiflashReplicaReadPolicy, regionInfosNeedReloadOnSendFail, regionIDsInOtherZones, maxRemoteReadCountAllowed, tidbZone)
			if err != nil {
				return nil, err
			}
			if batchCop, ok := storeTaskMap[rpcCtxs[idx].Addr]; ok {
				batchCop.regionInfos = append(batchCop.regionInfos, regionInfo)
			} else {
				batchTask := &batchCopTask{
					storeAddr:   rpcCtxs[idx].Addr,
					cmdType:     cmdType,
					ctx:         rpcCtxs[idx],
					regionInfos: []RegionInfo{regionInfo},
				}
				storeTaskMap[rpcCtxs[idx].Addr] = batchTask
			}
			for _, storeID := range regionInfo.AllStores {
				storeIDsUnionSetForAllTasks[storeID] = struct{}{}
			}
			allRegionInfos = append(allRegionInfos, regionInfo)
		}

		if len(regionIDsInOtherZones) != 0 {
			warningMsg := fmt.Sprintf("total %d region(s) can not be accessed by TiFlash in the zone [%s]:", len(regionIDsInOtherZones), tidbZone)
			regionIDErrMsg := ""
			for i := 0; i < 3 && i < len(regionIDsInOtherZones); i++ {
				regionIDErrMsg += fmt.Sprintf("%d, ", regionIDsInOtherZones[i])
			}
			warningMsg += regionIDErrMsg + "etc"
			appendWarning(errors.NewNoStackError(warningMsg))
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
		storesUnionSetForAllTasks := make([]*tikv.Store, 0, len(storeIDsUnionSetForAllTasks))
		for _, store := range aliveStores.storesInAllZones {
			if _, ok := storeIDsUnionSetForAllTasks[store.StoreID()]; ok {
				storesUnionSetForAllTasks = append(storesUnionSetForAllTasks, store)
			}
		}
		batchTasks = balanceBatchCopTask(storesUnionSetForAllTasks, batchTasks, balanceWithContinuity, balanceContinuousRegionCount, allRegionInfos)
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
				zap.Int("batchCopTask len", len(batchTasks)),
				zap.Int("copTask len", len(tasks)),
				zap.Int("retry num", retryNum))
		}
		metrics.TxnRegionsNumHistogramWithBatchCoprocessor.Observe(float64(len(batchTasks)))
		return batchTasks, nil
	}
}

// Check if all stores of one specific region has at least on alive store.
// If not, invalid region cache and return needRetry as true.
func checkAliveStore(aliveStores *aliveStoresBundle, usedTiFlashStores [][]uint64,
	usedTiFlashStoresMap map[uint64]struct{}, cache *RegionCache,
	tiflashReplicaReadPolicy tiflash.ReplicaRead, retryNum int,
	tasks []*copTask, minReplicaNum uint64, maxRemoteReadCountAllowed int) (needRetry bool, invalidRegions []tikv.RegionVerID) {
	if canSkipCheckAliveStores(aliveStores, usedTiFlashStores, usedTiFlashStoresMap, tiflashReplicaReadPolicy, retryNum, minReplicaNum) {
		return
	}

	if len(aliveStores.storeIDsInAllZones) == 0 {
		for i := range usedTiFlashStores {
			invalidRegions = append(invalidRegions, tasks[i].region)
		}
		needRetry = true
	} else if tiflashReplicaReadPolicy.IsClosestReplicas() {
		var remoteRegions []tikv.RegionVerID
		for i, allStoresPerRegion := range usedTiFlashStores {
			var storeOk bool
			var remoteStoreOk bool
			for _, storeID := range allStoresPerRegion {
				if _, ok := aliveStores.storeIDsInTiDBZone[storeID]; ok {
					storeOk = true
					break
				}
				if _, ok := aliveStores.storeIDsInAllZones[storeID]; ok {
					remoteStoreOk = true
				}
			}
			if !storeOk {
				if remoteStoreOk {
					remoteRegions = append(remoteRegions, tasks[i].region)
				} else {
					invalidRegions = append(invalidRegions, tasks[i].region)
				}
			}
		}
		if len(remoteRegions) > maxRemoteReadCountAllowed {
			invalidRegions = append(invalidRegions, remoteRegions...)
		}
	} else {
		for i, allStoresPerRegion := range usedTiFlashStores {
			var storeOk bool
			for _, storeID := range allStoresPerRegion {
				if _, ok := aliveStores.storeIDsInAllZones[storeID]; ok {
					storeOk = true
					break
				}
			}
			if !storeOk {
				invalidRegions = append(invalidRegions, tasks[i].region)
			}
		}
	}

	if len(invalidRegions) > 0 {
		needRetry = true
		if !intest.InTest {
			handleInvalidRegions(invalidRegions, cache)
		}
		// To avoid too many logs.
		if log.GetLevel() > zap.DebugLevel && len(invalidRegions) > 10 {
			invalidRegions = invalidRegions[:10]
		}
		var logStrs []string
		for _, region := range invalidRegions {
			logStrs = append(logStrs, region.String())
		}
		logutil.BgLogger().Info("need retry because region has no alive tiflash store", zap.Any("invalid regions", logStrs))
	}
	return
}

// Fast path for checkAliveStore(): If minReplicaNum > deadStoreNum, it means
// there is at least one alive store for each regions, we can skip check.
func canSkipCheckAliveStores(aliveStores *aliveStoresBundle, usedTiFlashStores [][]uint64,
	usedTiFlashStoresMap map[uint64]struct{}, tiflashReplicaReadPolicy tiflash.ReplicaRead,
	retryNum int, minReplicaNum uint64) bool {
	// Skip check because there is no real tiflash in most testcases.
	skipCheck := intest.InTest
	failpoint.Inject("mockNoAliveTiFlash", func(val failpoint.Value) {
		// This test will setup tiflash store properly, so detecting alive will success.
		skipCheck = false
		if val.(bool) && retryNum <= 1 {
			aliveStores.storesInAllZones = []*tikv.Store{}
		}
	})
	if skipCheck {
		return true
	}

	if !tiflashReplicaReadPolicy.IsClosestReplicas() {
		deadStoreNum := len(usedTiFlashStoresMap) - len(aliveStores.storeIDsInAllZones)
		return minReplicaNum > uint64(deadStoreNum)
	}

	// For closest_replica, need to recompute minReplicaNum and other infos,
	// which is duplicated with the logic in checkAliveStore(), so return false directly.
	return false
}

func handleInvalidRegions(invalidRegions []tikv.RegionVerID, cache *RegionCache) {
	for _, region := range invalidRegions {
		cache.InvalidateCachedRegion(region)
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
		for j := range tableRegions {
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

	if req.MaxExecutionTime > 0 {
		// If MaxExecutionTime is set, we need to set the deadline for the whole batch coprocessor request context.
		ctxWithTimeout, cancel := context.WithTimeout(bo.GetCtx(), time.Duration(req.MaxExecutionTime)*time.Millisecond)
		defer cancel()
		bo.TiKVBackoffer().SetCtx(ctxWithTimeout)
	}

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
		tasks, err = buildBatchCopTasksForPartitionedTable(ctx, bo, c.store.kvStore, keyRanges, req.StoreType, false, 0, false, 0, partitionIDs, tiflashcompute.DispatchPolicyInvalid, option.TiFlashReplicaRead, option.AppendWarning)
	} else {
		// TODO: merge the if branch.
		ranges := NewKeyRanges(req.KeyRanges.FirstPartitionRange())
		tasks, err = buildBatchCopTasksForNonPartitionedTable(ctx, bo, c.store.kvStore, ranges, req.StoreType, false, 0, false, 0, tiflashcompute.DispatchPolicyInvalid, option.TiFlashReplicaRead, option.AppendWarning)
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
		tiflashReplicaReadPolicy:   option.TiFlashReplicaRead,
		appendWarning:              option.AppendWarning,
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
	tiflashReplicaReadPolicy   tiflash.ReplicaRead
	appendWarning              func(error)
}

func (b *batchCopIterator) run(ctx context.Context) {
	// We run workers for every batch cop.
	for _, task := range b.tasks {
		b.wg.Add(1)
		boMaxSleep := CopNextMaxBackoff
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
			killed := atomic.LoadUint32(b.vars.Killed)
			if killed != 0 {
				logutil.Logger(ctx).Info(
					"a killed signal is received",
					zap.Uint32("signal", killed),
				)
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

