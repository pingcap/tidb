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
	"context"
	"math/rand"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/store/driver/backoff"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/stathat/consistent"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/testutils"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"go.uber.org/zap"
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

	var startKey string
	regionInfos := make([]RegionInfo, 0, len(ss))
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

func TestBalanceBatchCopTaskWithEmptyTaskSet(t *testing.T) {
	{
		var nilTaskSet []*batchCopTask
		nilResult := balanceBatchCopTask(nil, nilTaskSet, false, 0)
		require.True(t, nilResult == nil)
	}

	{
		emptyTaskSet := make([]*batchCopTask, 0)
		emptyResult := balanceBatchCopTask(nil, emptyTaskSet, false, 0)
		require.True(t, emptyResult != nil)
		require.True(t, len(emptyResult) == 0)
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

// Make sure no duplicated ip:addr.
func generateOneAddr() string {
	var ip string
	for i := 0; i < 4; i++ {
		if i != 0 {
			ip += "."
		}
		ip += strconv.Itoa(rand.Intn(255))
	}
	return ip + ":" + strconv.Itoa(rand.Intn(65535))
}

func generateDifferentAddrs(num int) (res []string) {
	addrMap := make(map[string]struct{})
	for len(addrMap) < num {
		addr := generateOneAddr()
		if _, ok := addrMap[addr]; !ok {
			addrMap[addr] = struct{}{}
		}
	}
	for addr := range addrMap {
		res = append(res, addr)
	}
	return
}

func TestConsistentHash(t *testing.T) {
	allAddrs := generateDifferentAddrs(100)

	computeNodes := allAddrs[:30]
	storageNodes := allAddrs[30:]
	firstRoundMap := make(map[string]string)
	for round := 0; round < 100; round++ {
		hasher := consistent.New()
		rand.Shuffle(len(computeNodes), func(i, j int) {
			computeNodes[i], computeNodes[j] = computeNodes[j], computeNodes[i]
		})
		for _, computeNode := range computeNodes {
			hasher.Add(computeNode)
		}
		for _, storageNode := range storageNodes {
			computeNode, err := hasher.Get(storageNode)
			require.NoError(t, err)
			if round == 0 {
				firstRoundMap[storageNode] = computeNode
			} else {
				firstRoundAddr, ok := firstRoundMap[storageNode]
				require.True(t, ok)
				require.Equal(t, firstRoundAddr, computeNode)
			}
		}
	}
}

func TestDispatchPolicyRR(t *testing.T) {
	allAddrs := generateDifferentAddrs(100)
	for i := 0; i < 100; i++ {
		regCnt := rand.Intn(10000)
		regIDs := make([]tikv.RegionVerID, 0, regCnt)
		for i := 0; i < regCnt; i++ {
			regIDs = append(regIDs, tikv.NewRegionVerID(uint64(i), 0, 0))
		}

		rpcCtxs, err := getTiFlashComputeRPCContextByRoundRobin(regIDs, allAddrs)
		require.NoError(t, err)
		require.Equal(t, len(rpcCtxs), len(regIDs))
		checkMap := make(map[string]int, len(rpcCtxs))
		for _, c := range rpcCtxs {
			if v, ok := checkMap[c.Addr]; !ok {
				checkMap[c.Addr] = 1
			} else {
				checkMap[c.Addr] = v + 1
			}
		}
		actCnt := 0
		for _, v := range checkMap {
			actCnt += v
		}
		require.Equal(t, regCnt, actCnt)
		if len(regIDs) < len(allAddrs) {
			require.Equal(t, len(regIDs), len(checkMap))
			exp := -1
			for _, v := range checkMap {
				if exp == -1 {
					exp = v
				} else {
					require.Equal(t, exp, v)
				}
			}
		} else {
			// Using RR, it means region cnt for each tiflash_compute node should be almost same.
			minV := regCnt
			for _, v := range checkMap {
				if v < minV {
					minV = v
				}
			}
			for k, v := range checkMap {
				checkMap[k] = v - minV
			}
			for _, v := range checkMap {
				require.True(t, v == 0 || v == 1)
			}
		}
	}
}

func TestTopoFetcherBackoff(t *testing.T) {
	fetchTopoBo := backoff.NewBackofferWithVars(context.Background(), fetchTopoMaxBackoff, nil)
	expectErr := errors.New("Cannot find proper topo from AutoScaler")
	var retryNum int
	start := time.Now()
	for {
		retryNum++
		if err := fetchTopoBo.Backoff(tikv.BoTiFlashRPC(), expectErr); err != nil {
			break
		}
		logutil.BgLogger().Info("TestTopoFetcherBackoff", zap.Any("retryNum", retryNum))
	}
	dura := time.Since(start)
	// fetchTopoMaxBackoff is milliseconds.
	require.GreaterOrEqual(t, dura, time.Duration(fetchTopoMaxBackoff*1000))
	require.GreaterOrEqual(t, dura, 30*time.Second)
	require.LessOrEqual(t, dura, 50*time.Second)
}

func TestGetAllUsedTiFlashStores(t *testing.T) {
	mockClient, _, pdClient, err := testutils.NewMockTiKV("", nil)
	require.NoError(t, err)
	defer func() {
		pdClient.Close()
		err = mockClient.Close()
		require.NoError(t, err)
	}()

	pdCli := tikv.NewCodecPDClient(tikv.ModeTxn, pdClient)
	defer pdCli.Close()

	cache := NewRegionCache(tikv.NewRegionCache(pdCli))
	defer cache.Close()

	label1 := metapb.StoreLabel{Key: tikvrpc.EngineLabelKey, Value: tikvrpc.EngineLabelTiFlash}
	label2 := metapb.StoreLabel{Key: tikvrpc.EngineRoleLabelKey, Value: tikvrpc.EngineLabelTiFlashCompute}

	cache.SetRegionCacheStore(1, "192.168.1.1", "", tikvrpc.TiFlash, 1, []*metapb.StoreLabel{&label1, &label2})
	cache.SetRegionCacheStore(2, "192.168.1.2", "192.168.1.3", tikvrpc.TiFlash, 1, []*metapb.StoreLabel{&label1, &label2})
	cache.SetRegionCacheStore(3, "192.168.1.3", "192.168.1.2", tikvrpc.TiFlash, 1, []*metapb.StoreLabel{&label1, &label2})

	allUsedTiFlashStoresMap := make(map[uint64]struct{})
	allUsedTiFlashStoresMap[2] = struct{}{}
	allUsedTiFlashStoresMap[3] = struct{}{}
	allTiFlashStores := cache.RegionCache.GetTiFlashStores(tikv.LabelFilterNoTiFlashWriteNode)
	require.Equal(t, 3, len(allTiFlashStores))
	allUsedTiFlashStores := getAllUsedTiFlashStores(allTiFlashStores, allUsedTiFlashStoresMap)
	require.Equal(t, len(allUsedTiFlashStoresMap), len(allUsedTiFlashStores))
	for _, store := range allUsedTiFlashStores {
		_, ok := allUsedTiFlashStoresMap[store.StoreID()]
		require.True(t, ok)
	}
}
