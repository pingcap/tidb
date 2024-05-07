// Copyright 2019 PingCAP, Inc.
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

package local

import (
	"bytes"
	"context"
	"math"
	"math/rand"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	"github.com/pingcap/tidb/pkg/store/pdtypes"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

type testSplitClient struct {
	split.SplitClient
	mu           sync.RWMutex
	stores       map[uint64]*metapb.Store
	regions      map[uint64]*split.RegionInfo
	regionsInfo  *pdtypes.RegionTree // For now it's only used in ScanRegions
	nextRegionID uint64
	splitCount   atomic.Int32
	hook         clientHook
}

func newTestSplitClient(
	stores map[uint64]*metapb.Store,
	regions map[uint64]*split.RegionInfo,
	nextRegionID uint64,
	hook clientHook,
) *testSplitClient {
	regionsInfo := &pdtypes.RegionTree{}
	for _, regionInfo := range regions {
		regionsInfo.SetRegion(pdtypes.NewRegionInfo(regionInfo.Region, regionInfo.Leader))
	}
	return &testSplitClient{
		stores:       stores,
		regions:      regions,
		regionsInfo:  regionsInfo,
		nextRegionID: nextRegionID,
		hook:         hook,
	}
}

func (c *testSplitClient) GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	store, ok := c.stores[storeID]
	if !ok {
		return nil, errors.Errorf("store not found")
	}
	return store, nil
}

func (c *testSplitClient) GetRegion(ctx context.Context, key []byte) (*split.RegionInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, region := range c.regions {
		if bytes.Compare(key, region.Region.StartKey) >= 0 && beforeEnd(key, region.Region.EndKey) {
			return region, nil
		}
	}
	return nil, errors.Errorf("region not found: key=%s", string(key))
}

func (c *testSplitClient) GetRegionByID(ctx context.Context, regionID uint64) (*split.RegionInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	region, ok := c.regions[regionID]
	if !ok {
		return nil, errors.Errorf("region not found: id=%d", regionID)
	}
	return region, nil
}

func (c *testSplitClient) SplitWaitAndScatter(ctx context.Context, region *split.RegionInfo, keys [][]byte) ([]*split.RegionInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.splitCount.Inc()

	if c.hook != nil {
		region, keys = c.hook.BeforeSplitRegion(ctx, region, keys)
	}
	if len(keys) == 0 {
		return nil, errors.New("no valid key")
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	newRegions := make([]*split.RegionInfo, 0)
	target, ok := c.regions[region.Region.Id]
	if !ok {
		return nil, errors.New("region not found")
	}
	if target.Region.RegionEpoch.Version != region.Region.RegionEpoch.Version ||
		target.Region.RegionEpoch.ConfVer != region.Region.RegionEpoch.ConfVer {
		return nil, errors.New("epoch not match")
	}
	splitKeys := make([][]byte, 0, len(keys))
	for _, k := range keys {
		splitKey := codec.EncodeBytes([]byte{}, k)
		splitKeys = append(splitKeys, splitKey)
	}
	sort.Slice(splitKeys, func(i, j int) bool {
		return bytes.Compare(splitKeys[i], splitKeys[j]) < 0
	})

	startKey := target.Region.StartKey
	for _, key := range splitKeys {
		if bytes.Compare(key, startKey) <= 0 || bytes.Compare(key, target.Region.EndKey) >= 0 {
			continue
		}
		newRegion := &split.RegionInfo{
			Region: &metapb.Region{
				Peers:    target.Region.Peers,
				Id:       c.nextRegionID,
				StartKey: startKey,
				EndKey:   key,
			},
		}
		c.regions[c.nextRegionID] = newRegion
		c.regionsInfo.SetRegion(pdtypes.NewRegionInfo(newRegion.Region, newRegion.Leader))
		c.nextRegionID++
		startKey = key
		newRegions = append(newRegions, newRegion)
	}
	if !bytes.Equal(target.Region.StartKey, startKey) {
		target.Region.StartKey = startKey
		c.regions[target.Region.Id] = target
		c.regionsInfo.SetRegion(pdtypes.NewRegionInfo(target.Region, target.Leader))
	}

	if len(newRegions) == 0 {
		return nil, errors.New("no valid key")
	}

	var err error
	if c.hook != nil {
		newRegions, err = c.hook.AfterSplitRegion(ctx, target, keys, newRegions, nil)
	}

	return newRegions, err
}

func (c *testSplitClient) ScanRegions(ctx context.Context, key, endKey []byte, limit int) ([]*split.RegionInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if c.hook != nil {
		key, endKey, limit = c.hook.BeforeScanRegions(ctx, key, endKey, limit)
	}

	infos := c.regionsInfo.ScanRange(key, endKey, limit)
	regions := make([]*split.RegionInfo, 0, len(infos))
	for _, info := range infos {
		regions = append(regions, &split.RegionInfo{
			Region: info.Meta,
			Leader: info.Leader,
		})
	}

	var err error
	if c.hook != nil {
		regions, err = c.hook.AfterScanRegions(regions, nil)
	}
	return regions, err
}

func (c *testSplitClient) WaitRegionsScattered(context.Context, []*split.RegionInfo) (int, error) {
	return 0, nil
}

// For keys ["", "aay", "bba", "bbh", "cca", ""], the key ranges of
// regions are [, aay), [aay, bba), [bba, bbh), [bbh, cca), [cca, ).
func initTestSplitClient(keys [][]byte, hook clientHook) *testSplitClient {
	peers := make([]*metapb.Peer, 1)
	peers[0] = &metapb.Peer{
		Id:      1,
		StoreId: 1,
	}
	regions := make(map[uint64]*split.RegionInfo)
	for i := uint64(1); i < uint64(len(keys)); i++ {
		startKey := keys[i-1]
		if len(startKey) != 0 {
			startKey = codec.EncodeBytes([]byte{}, startKey)
		}
		endKey := keys[i]
		if len(endKey) != 0 {
			endKey = codec.EncodeBytes([]byte{}, endKey)
		}
		regions[i] = &split.RegionInfo{
			Region: &metapb.Region{
				Id:          i,
				Peers:       peers,
				StartKey:    startKey,
				EndKey:      endKey,
				RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
			},
			Leader: peers[0],
		}
	}
	stores := make(map[uint64]*metapb.Store)
	stores[1] = &metapb.Store{
		Id: 1,
	}
	return newTestSplitClient(stores, regions, uint64(len(keys)), hook)
}

// initTestSplitClient3Replica will create a client that each region has 3 replicas, and their IDs and StoreIDs are
// (1, 2, 3), (11, 12, 13), ...
// For keys ["", "aay", "bba", "bbh", "cca", ""], the key ranges of
// region ranges are [, aay), [aay, bba), [bba, bbh), [bbh, cca), [cca, ).
func initTestSplitClient3Replica(keys [][]byte, hook clientHook) *testSplitClient {
	regions := make(map[uint64]*split.RegionInfo)
	stores := make(map[uint64]*metapb.Store)
	for i := uint64(1); i < uint64(len(keys)); i++ {
		startKey := keys[i-1]
		if len(startKey) != 0 {
			startKey = codec.EncodeBytes([]byte{}, startKey)
		}
		endKey := keys[i]
		if len(endKey) != 0 {
			endKey = codec.EncodeBytes([]byte{}, endKey)
		}
		baseID := (i-1)*10 + 1
		peers := make([]*metapb.Peer, 3)
		for j := 0; j < 3; j++ {
			peers[j] = &metapb.Peer{
				Id:      baseID + uint64(j),
				StoreId: baseID + uint64(j),
			}
		}

		regions[baseID] = &split.RegionInfo{
			Region: &metapb.Region{
				Id:          baseID,
				Peers:       peers,
				StartKey:    startKey,
				EndKey:      endKey,
				RegionEpoch: &metapb.RegionEpoch{ConfVer: 1, Version: 1},
			},
			Leader: peers[0],
		}
		stores[baseID] = &metapb.Store{
			Id: baseID,
		}
	}
	return newTestSplitClient(stores, regions, uint64(len(keys)), hook)
}

type clientHook interface {
	BeforeSplitRegion(ctx context.Context, regionInfo *split.RegionInfo, keys [][]byte) (*split.RegionInfo, [][]byte)
	AfterSplitRegion(context.Context, *split.RegionInfo, [][]byte, []*split.RegionInfo, error) ([]*split.RegionInfo, error)
	BeforeScanRegions(ctx context.Context, key, endKey []byte, limit int) ([]byte, []byte, int)
	AfterScanRegions([]*split.RegionInfo, error) ([]*split.RegionInfo, error)
}

func TestStoreWriteLimiter(t *testing.T) {
	// Test create store write limiter with limit math.MaxInt.
	limiter := newStoreWriteLimiter(math.MaxInt)
	err := limiter.WaitN(context.Background(), 1, 1024)
	require.NoError(t, err)

	// Test WaitN exceeds the burst.
	limiter = newStoreWriteLimiter(100)
	start := time.Now()
	// 120 is the initial burst, 150 is the number of new tokens.
	err = limiter.WaitN(context.Background(), 1, 120+120)
	require.NoError(t, err)
	require.Greater(t, time.Since(start), time.Second)

	// Test WaitN with different store id.
	limiter = newStoreWriteLimiter(100)
	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(storeID uint64) {
			defer wg.Done()
			start := time.Now()
			var gotTokens int
			for {
				n := rand.Intn(50)
				if limiter.WaitN(ctx, storeID, n) != nil {
					break
				}
				gotTokens += n
			}
			elapsed := time.Since(start)
			maxTokens := 120 + int(float64(elapsed)/float64(time.Second)*100)
			// In theory, gotTokens should be less than or equal to maxTokens.
			// But we allow a little of error to avoid the test being flaky.
			require.LessOrEqual(t, gotTokens, maxTokens+1)
		}(uint64(i))
	}
	wg.Wait()
}
