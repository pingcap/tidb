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
	"fmt"
	"math"
	"time"

	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

func deepCopyStoreTaskMap(storeTaskMap map[uint64]*batchCopTask, avgRegionsNum int) map[uint64]*batchCopTask {
	storeTasks := make(map[uint64]*batchCopTask)
	for storeID, task := range storeTaskMap {
		t := batchCopTask{
			storeAddr: task.storeAddr,
			cmdType:   task.cmdType,
			ctx:       task.ctx,
		}
		t.regionInfos = make([]RegionInfo, len(task.regionInfos), len(task.regionInfos)+avgRegionsNum)
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
func selectRegion(storeID uint64, candidateRegionInfos []RegionInfo, selected []bool, storeID2RegionIndex map[uint64][]int, cnt int64, regionInfosBuffer []RegionInfo) []RegionInfo {
	regionIndexes, ok := storeID2RegionIndex[storeID]
	if !ok {
		logutil.BgLogger().Error("selectRegion: storeID2RegionIndex not found", zap.Uint64("storeID", storeID))
		return nil
	}
	i := 0
	for ; i < len(regionIndexes) && len(regionInfosBuffer) < int(cnt); i++ {
		idx := regionIndexes[i]
		if selected[idx] {
			continue
		}
		selected[idx] = true
		regionInfosBuffer = append(regionInfosBuffer, candidateRegionInfos[idx])
	}
	// Remove regions that has been selected.
	storeID2RegionIndex[storeID] = regionIndexes[i:]
	return regionInfosBuffer
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
// First, the caller should guarantee the candidateRegionInfos is ordered.
// Second, build a storeID2RegionIndex data structure to fastly locate regions of a store (avoid scanning candidateRegionInfos repeatedly).
// Third, each store will take balanceContinuousRegionCount from the sorted candidateRegionInfos. These regions are stored very close to each other in TiFlash.
// Fourth, if the region count is not balance between TiFlash, it may fallback to the original balance logic.
func balanceBatchCopTaskWithContinuity(storeTaskMap map[uint64]*batchCopTask, candidateRegionInfos []RegionInfo, balanceContinuousRegionCount int64) ([]*batchCopTask, int) {
	if len(candidateRegionInfos) < 500 {
		return nil, 0
	}
	funcStart := time.Now()
	regionCount := regionTotalCount(storeTaskMap, candidateRegionInfos)
	storeTasks := deepCopyStoreTaskMap(storeTaskMap, len(candidateRegionInfos)/len(storeTaskMap))

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
	regionInfosBuffer := make([]RegionInfo, 0, balanceContinuousRegionCount)
	for {
		totalCount := 0
		selectCountThisRound := 0
		for storeID, task := range storeTasks {
			// Each store select balanceContinuousRegionCount regions from candidateRegionInfos.
			// Since candidateRegionInfos is sorted, it is very likely that these regions are close to each other in TiFlash.
			regionInfosBuffer = selectRegion(storeID, candidateRegionInfos, selected, storeID2RegionIndex, balanceContinuousRegionCount, regionInfosBuffer[:0])
			task.regionInfos = append(task.regionInfos, regionInfosBuffer...)
			totalCount += len(task.regionInfos)
			selectCountThisRound += len(regionInfosBuffer)
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
