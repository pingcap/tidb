// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mockstore

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/helper"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
)

func newMockRegion(regionID uint64, startKey []byte, endKey []byte) *pd.Region {
	leader := &metapb.Peer{
		Id:      regionID,
		StoreId: 1,
		Role:    metapb.PeerRole_Voter,
	}

	return &pd.Region{
		Meta: &metapb.Region{
			Id:       regionID,
			StartKey: startKey,
			EndKey:   endKey,
			Peers:    []*metapb.Peer{leader},
		},
		Leader: leader,
	}
}

type mockPDClient struct {
	t *testing.T
	pd.Client
	regions       []*pd.Region
	regionsSorted bool
}

// ScanRegion gets a list of regions, starts from the region that contains key.
func (c *mockPDClient) ScanRegions(_ context.Context, key, endKey []byte, limit int) ([]*pd.Region, error) {
	if len(c.regions) == 0 {
		return []*pd.Region{newMockRegion(1, []byte{}, []byte{0xFF, 0xFF})}, nil
	}

	if !c.regionsSorted {
		sort.Slice(c.regions, func(i, j int) bool {
			return kv.Key(c.regions[i].Meta.StartKey).Cmp(c.regions[j].Meta.StartKey) < 0
		})
		c.regionsSorted = true
	}

	regions := []*pd.Region{newMockRegion(1, []byte{}, c.regions[0].Meta.StartKey)}
	regions = append(regions, c.regions...)
	regions = append(regions, newMockRegion(2, c.regions[len(c.regions)-1].Meta.EndKey, []byte{0xFF, 0xFF, 0xFF}))

	result := make([]*pd.Region, 0)
	for _, r := range regions {
		if kv.Key(r.Meta.StartKey).Cmp(endKey) >= 0 {
			continue
		}

		if kv.Key(r.Meta.EndKey).Cmp(key) <= 0 {
			continue
		}

		if len(result) >= limit {
			break
		}

		result = append(result, r)
	}
	return result, nil
}

// GetStore gets a store from PD by store id.
func (c *mockPDClient) GetStore(_ context.Context, storeID uint64) (*metapb.Store, error) {
	return &metapb.Store{
		Id:      storeID,
		Address: fmt.Sprintf("127.0.0.%d", storeID),
	}, nil
}

// MockTiKVStore mocks helper.Storage interface.
type MockTiKVStore struct {
	t *testing.T
	helper.Storage
	pdClient     *mockPDClient
	cache        *tikv.RegionCache
	nextRegionID uint64
}

// NewMockTiKVStore init the MockTiKVStore.
func NewMockTiKVStore(t *testing.T) *MockTiKVStore {
	pdClient := &mockPDClient{t: t}
	s := &MockTiKVStore{
		t:            t,
		pdClient:     pdClient,
		cache:        tikv.NewRegionCache(pdClient),
		nextRegionID: 1000,
	}
	s.refreshCache()
	t.Cleanup(func() {
		s.cache.Close()
	})
	return s
}

// AddRegionEndWithTablePrefix add region begin with table prefix.
func (s *MockTiKVStore) AddRegionBeginWithTablePrefix(tableID int64, handle kv.Handle) *MockTiKVStore {
	start := tablecodec.GenTablePrefix(tableID)
	end := tablecodec.EncodeRowKeyWithHandle(tableID, handle)
	return s.AddRegion(start, end)
}

// AddRegionEndWithTablePrefix add region end with table prefix.
func (s *MockTiKVStore) AddRegionEndWithTablePrefix(handle kv.Handle, tableID int64) *MockTiKVStore {
	start := tablecodec.EncodeRowKeyWithHandle(tableID, handle)
	end := tablecodec.GenTablePrefix(tableID + 1)
	return s.AddRegion(start, end)
}

// AddRegionWithTablePrefix add region with table prefix.
func (s *MockTiKVStore) AddRegionWithTablePrefix(tableID int64, start kv.Handle, end kv.Handle) *MockTiKVStore {
	startKey := tablecodec.EncodeRowKeyWithHandle(tableID, start)
	endKey := tablecodec.EncodeRowKeyWithHandle(tableID, end)
	return s.AddRegion(startKey, endKey)
}

// AddRegion add region to mock pd.
func (s *MockTiKVStore) AddRegion(key, endKey []byte) *MockTiKVStore {
	require.True(s.t, kv.Key(endKey).Cmp(key) > 0)
	if len(s.pdClient.regions) > 0 {
		lastRegion := s.pdClient.regions[len(s.pdClient.regions)-1]
		require.True(s.t, kv.Key(endKey).Cmp(lastRegion.Meta.EndKey) >= 0)
	}

	regionID := s.nextRegionID
	s.nextRegionID++
	leader := &metapb.Peer{
		Id:      regionID,
		StoreId: 1,
		Role:    metapb.PeerRole_Voter,
	}

	s.pdClient.regions = append(s.pdClient.regions, &pd.Region{
		Meta: &metapb.Region{
			Id:       regionID,
			StartKey: key,
			EndKey:   endKey,
			Peers:    []*metapb.Peer{leader},
		},
		Leader: leader,
	})

	s.pdClient.regionsSorted = false
	s.refreshCache()
	return s
}

func (s *MockTiKVStore) refreshCache() {
	_, err := s.cache.LoadRegionsInKeyRange(
		tikv.NewBackofferWithVars(context.Background(), 1000, nil),
		[]byte{},
		[]byte{0xFF},
	)
	require.NoError(s.t, err)
}

// BatchAddIntHandleRegions add int handle regions.
func (s *MockTiKVStore) BatchAddIntHandleRegions(tblID int64, regionCnt, regionSize int,
	offset int64) (end kv.IntHandle) {
	for i := 0; i < regionCnt; i++ {
		start := kv.IntHandle(offset + int64(i*regionSize))
		end = kv.IntHandle(start.IntValue() + int64(regionSize))
		s.AddRegionWithTablePrefix(tblID, start, end)
	}
	return
}

// ClearRegions clears regions in pdClient and cache.
func (s *MockTiKVStore) ClearRegions() {
	s.pdClient.regions = nil
	s.cache.Close()
	s.cache = tikv.NewRegionCache(s.pdClient)
	s.refreshCache()
}

// GetRegionCache returns the region cache instance.
func (s *MockTiKVStore) GetRegionCache() *tikv.RegionCache {
	return s.cache
}
