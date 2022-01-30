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
	"math/rand"
	"sort"
	"strconv"
	"testing"

	"github.com/pingcap/tidb/kv"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
)

// StoreID: [1, storeCount]
func buildStoreTaskMap(storeCount int) map[uint64]*batchCopTask {
	storeTasks := make(map[uint64]*batchCopTask)
	for i := 0; i < storeCount; i++ {
		storeTasks[uint64(i+1)] = &batchCopTask{}
	}
	return storeTasks
}

func buildRegionInfos(storeCount, regionCount, replicaNum int) []RegionInfo {
	var ss []string
	for i := 0; i < regionCount; i++ {
		s := strconv.Itoa(i)
		ss = append(ss, s)
	}
	sort.Strings(ss)

	storeIDExist := func(storeID uint64, storeIDs []uint64) bool {
		for _, i := range storeIDs {
			if i == storeID {
				return true
			}
		}
		return false
	}

	randomStores := func(storeCount, replicaNum int) []uint64 {
		var storeIDs []uint64
		for len(storeIDs) < replicaNum {
			t := uint64(rand.Intn(storeCount) + 1)
			if storeIDExist(t, storeIDs) {
				continue
			}
			storeIDs = append(storeIDs, t)
		}
		return storeIDs
	}

	var regionInfos []RegionInfo
	var startKey string
	for i, s := range ss {
		var ri RegionInfo
		ri.Region = tikv.NewRegionVerID(uint64(i), 1, 1)
		ri.Meta = nil
		ri.AllStores = randomStores(storeCount, replicaNum)

		var keyRange kv.KeyRange
		if len(startKey) == 0 {
			keyRange.StartKey = nil
		} else {
			keyRange.StartKey = kv.Key(startKey)
		}
		keyRange.EndKey = kv.Key(s)
		ri.Ranges = NewKeyRanges([]kv.KeyRange{keyRange})
		regionInfos = append(regionInfos, ri)
		startKey = s
	}
	return regionInfos
}

func calcReginCount(tasks []*batchCopTask) int {
	count := 0
	for _, task := range tasks {
		count += len(task.regionInfos)
	}
	return count
}

func TestBalanceBatchCopTaskWithContinuity(t *testing.T) {
	for replicaNum := 1; replicaNum < 6; replicaNum++ {
		storeCount := 10
		regionCount := 100000
		storeTasks := buildStoreTaskMap(storeCount)
		regionInfos := buildRegionInfos(storeCount, regionCount, replicaNum)
		tasks, score := balanceBatchCopTaskWithContinuity(storeTasks, regionInfos, 20)
		require.True(t, isBalance(score))
		require.Equal(t, regionCount, calcReginCount(tasks))
	}

	{
		storeCount := 10
		regionCount := 100
		replicaNum := 2
		storeTasks := buildStoreTaskMap(storeCount)
		regionInfos := buildRegionInfos(storeCount, regionCount, replicaNum)
		tasks, _ := balanceBatchCopTaskWithContinuity(storeTasks, regionInfos, 20)
		require.True(t, tasks == nil)
	}
}

func TestDeepCopyStoreTaskMap(t *testing.T) {
	storeTasks1 := buildStoreTaskMap(10)
	for _, task := range storeTasks1 {
		task.regionInfos = append(task.regionInfos, RegionInfo{})
	}

	storeTasks2 := deepCopyStoreTaskMap(storeTasks1)
	for _, task := range storeTasks2 {
		task.regionInfos = append(task.regionInfos, RegionInfo{})
	}

	for _, task := range storeTasks1 {
		require.Equal(t, 1, len(task.regionInfos))
	}

	for _, task := range storeTasks2 {
		require.Equal(t, 2, len(task.regionInfos))
	}
}
