// Copyright 2022 PingCAP, Inc.
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

package cache_test

import (
	"context"
	"fmt"
	"math"
	"sort"
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/store/helper"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/ttl/cache"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
)

type mockPDClient struct {
	t *testing.T
	pd.Client
	regions       []*pd.Region
	regionsSorted bool
}

func (c *mockPDClient) ScanRegions(_ context.Context, key, endKey []byte, limit int) ([]*pd.Region, error) {
	if !c.regionsSorted {
		sort.Slice(c.regions, func(i, j int) bool {
			return kv.Key(c.regions[i].Meta.StartKey).Cmp(c.regions[j].Meta.StartKey) < 0
		})
		c.regionsSorted = true
	}

	result := make([]*pd.Region, 0)
	for _, r := range c.regions {
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

func (c *mockPDClient) GetStore(_ context.Context, storeID uint64) (*metapb.Store, error) {
	return &metapb.Store{
		Id:      storeID,
		Address: fmt.Sprintf("127.0.0.%d", storeID),
	}, nil
}

type mockTiKVStore struct {
	t *testing.T
	helper.Storage
	pdClient *mockPDClient
	cache    *tikv.RegionCache
}

func newMockTiKVStore(t *testing.T) *mockTiKVStore {
	pdClient := &mockPDClient{t: t}
	regionCache := tikv.NewRegionCache(pdClient)
	t.Cleanup(regionCache.Close)
	return &mockTiKVStore{
		t:        t,
		pdClient: pdClient,
		cache:    regionCache,
	}
}

func (s *mockTiKVStore) addFullTableRegion(regionID uint64, tableID ...int64) *mockTiKVStore {
	prefix1 := tablecodec.GenTablePrefix(tableID[0])
	prefix2 := tablecodec.GenTablePrefix(tableID[len(tableID)-1])
	return s.addRegion(regionID, prefix1, prefix2.PrefixNext())
}

func (s *mockTiKVStore) addRegionBeginWithTablePrefix(regionID uint64, tableID int64, handle kv.Handle) *mockTiKVStore {
	start := tablecodec.GenTablePrefix(tableID)
	end := tablecodec.EncodeRowKeyWithHandle(tableID, handle)
	return s.addRegion(regionID, start, end)
}

func (s *mockTiKVStore) addRegionEndWithTablePrefix(regionID uint64, handle kv.Handle, tableID int64) *mockTiKVStore {
	start := tablecodec.EncodeRowKeyWithHandle(tableID, handle)
	end := tablecodec.GenTablePrefix(tableID + 1)
	return s.addRegion(regionID, start, end)
}

func (s *mockTiKVStore) addRegionWithTablePrefix(regionID uint64, tableID int64, start kv.Handle, end kv.Handle) *mockTiKVStore {
	startKey := tablecodec.EncodeRowKeyWithHandle(tableID, start)
	endKey := tablecodec.EncodeRowKeyWithHandle(tableID, end)
	return s.addRegion(regionID, startKey, endKey)
}

func (s *mockTiKVStore) addRegion(regionID uint64, key, endKey []byte) *mockTiKVStore {
	require.True(s.t, kv.Key(endKey).Cmp(key) > 0)
	if len(s.pdClient.regions) > 0 {
		lastRegion := s.pdClient.regions[len(s.pdClient.regions)-1]
		require.True(s.t, kv.Key(endKey).Cmp(lastRegion.Meta.EndKey) >= 0)
	}

	leader := &metapb.Peer{
		Id:      regionID*1000 + 1,
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
	return s
}

func (s *mockTiKVStore) prepareIntHandleRegions(tblID int64, regionCnt int, regionSize int) {
	for i := 0; i < regionCnt; i++ {
		if i == 0 {
			s.addRegionBeginWithTablePrefix(uint64(i), tblID, kv.IntHandle((i+1)*regionSize))
			continue
		}

		if i == regionCnt-1 {
			s.addRegionEndWithTablePrefix(uint64(i), kv.IntHandle(i*regionSize), tblID)
			continue
		}

		s.addRegionWithTablePrefix(uint64(i), tblID, kv.IntHandle(i*regionSize), kv.IntHandle((i+1)*regionSize))
	}
}

func (s *mockTiKVStore) newCommonHandle(ds ...types.Datum) *kv.CommonHandle {
	encoded, err := codec.EncodeKey(nil, nil, ds...)
	require.NoError(s.t, err)
	h, err := kv.NewCommonHandle(encoded)
	require.NoError(s.t, err)
	return h
}

func (s *mockTiKVStore) GetRegionCache() *tikv.RegionCache {
	return s.cache
}

func createTTLTable(t *testing.T, tk *testkit.TestKit, do *domain.Domain, name string, option string) *cache.PhysicalTable {
	tk.MustExec(fmt.Sprintf("create table test.%s %s", name, option))
	tbl, err := do.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr(name))
	require.NoError(t, err)
	ttlTbl, err := cache.NewPhysicalTable(model.NewCIStr("test"), tbl.Meta(), model.NewCIStr(""))
	require.NoError(t, err)
	return ttlTbl
}

func TestSplitTTLScanRangesWithSignedInt(t *testing.T) {
	parser.TTLFeatureGate = true
	defer func() {
		parser.TTLFeatureGate = false
	}()

	store, do := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tbl1 := createTTLTable(t, tk, do, "t1", "(id int primary key, t timestamp) TTL = `t` + interval 1 day")

	tikvStore := newMockTiKVStore(t)
	tikvStore.prepareIntHandleRegions(tbl1.ID, 10, 100)

	ranges, err := tbl1.SplitScanRanges(context.TODO(), tikvStore, 4)
	require.NoError(t, err)
	require.Equal(t, 4, len(ranges))
}

func TestGetNextBytesHandleDatum(t *testing.T) {
	tblID := int64(7)
	buildHandleBytes := func(data []byte) []byte {
		handleBytes, err := codec.EncodeKey(nil, nil, types.NewBytesDatum(data))
		require.NoError(t, err)
		return handleBytes
	}

	buildRowKey := func(handleBytes []byte) kv.Key {
		return tablecodec.EncodeRowKey(tblID, handleBytes)
	}

	buildBytesRowKey := func(data []byte) kv.Key {
		return buildRowKey(buildHandleBytes(data))
	}

	binaryDataStartPos := len(tablecodec.GenTableRecordPrefix(tblID)) + 1
	cases := []struct {
		key    interface{}
		result []byte
		isNull bool
	}{
		{
			key:    buildBytesRowKey([]byte{}),
			result: []byte{},
		},
		{
			key:    buildBytesRowKey([]byte{1, 2, 3}),
			result: []byte{1, 2, 3},
		},
		{
			key:    buildBytesRowKey([]byte{1, 2, 3, 0}),
			result: []byte{1, 2, 3, 0},
		},
		{
			key:    buildBytesRowKey([]byte{1, 2, 3, 4, 5, 6, 7, 8}),
			result: []byte{1, 2, 3, 4, 5, 6, 7, 8},
		},
		{
			key:    buildBytesRowKey([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9}),
			result: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			key:    buildBytesRowKey([]byte{1, 2, 3, 4, 5, 6, 7, 8, 0}),
			result: []byte{1, 2, 3, 4, 5, 6, 7, 8, 0},
		},
		{
			key:    []byte{},
			result: []byte{},
		},
		{
			key:    tablecodec.GenTableRecordPrefix(tblID),
			result: []byte{},
		},
		{
			key:    tablecodec.GenTableRecordPrefix(tblID - 1),
			result: []byte{},
		},
		{
			key:    tablecodec.GenTablePrefix(tblID).PrefixNext(),
			isNull: true,
		},
		{
			key:    buildRowKey([]byte{0}),
			result: []byte{},
		},
		{
			key:    buildRowKey([]byte{1}),
			result: []byte{},
		},
		{
			key:    buildRowKey([]byte{2}),
			isNull: true,
		},
		{
			// recordPrefix + bytesFlag + [0]
			key:    buildBytesRowKey([]byte{})[:binaryDataStartPos+1],
			result: []byte{},
		},
		{
			// recordPrefix + bytesFlag + [0, 0, 0, 0, 0, 0, 0, 0]
			key:    buildBytesRowKey([]byte{})[:binaryDataStartPos+8],
			result: []byte{},
		},
		{
			// recordPrefix + bytesFlag + [1]
			key:    buildBytesRowKey([]byte{1, 2, 3})[:binaryDataStartPos+1],
			result: []byte{1},
		},
		{
			// recordPrefix + bytesFlag + [1, 2, 3]
			key:    buildBytesRowKey([]byte{1, 2, 3})[:binaryDataStartPos+3],
			result: []byte{1, 2, 3},
		},
		{
			// recordPrefix + bytesFlag + [1, 2, 3, 0]
			key:    buildBytesRowKey([]byte{1, 2, 3})[:binaryDataStartPos+4],
			result: []byte{1, 2, 3},
		},
		{
			// recordPrefix + bytesFlag + [1, 2, 3, 0, 0, 0, 0, 0, 247]
			key: func() []byte {
				bs := buildBytesRowKey([]byte{1, 2, 3})
				bs[len(bs)-1] = 247
				return bs
			},
			result: []byte{1, 2, 3},
		},
		{
			// recordPrefix + bytesFlag + [1, 2, 3, 0, 0, 0, 0, 0, 0]
			key: func() []byte {
				bs := buildBytesRowKey([]byte{1, 2, 3})
				bs[len(bs)-1] = 0
				return bs
			},
			result: []byte{1, 2, 3},
		},
		{
			// recordPrefix + bytesFlag + [1, 2, 3, 4, 5, 6, 7, 8, 254, 9, 0, 0, 0, 0, 0, 0, 0, 248]
			key: func() []byte {
				bs := buildBytesRowKey([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9})
				bs[len(bs)-10] = 254
				return bs
			},
			result: []byte{1, 2, 3, 4, 5, 6, 7, 8},
		},
		{
			// recordPrefix + bytesFlag + [1, 2, 3, 4, 5, 6, 7, 0, 254, 9, 0, 0, 0, 0, 0, 0, 0, 248]
			key: func() []byte {
				bs := buildBytesRowKey([]byte{1, 2, 3, 4, 5, 6, 7, 0, 9})
				bs[len(bs)-10] = 254
				return bs
			},
			result: []byte{1, 2, 3, 4, 5, 6, 7},
		},
		{
			// recordPrefix + bytesFlag + [1, 2, 3, 4, 5, 6, 7, 0, 253, 9, 0, 0, 0, 0, 0, 0, 0, 248]
			key: func() []byte {
				bs := buildBytesRowKey([]byte{1, 2, 3, 4, 5, 6, 7, 0, 9})
				bs[len(bs)-10] = 253
				return bs
			},
			result: []byte{1, 2, 3, 4, 5, 6, 7},
		},
		{
			// recordPrefix + bytesFlag + [1, 2, 3, 4, 5, 6, 7, 8, 255, 9, 0, 0, 0, 0, 0, 0, 0, 247]
			key: func() []byte {
				bs := buildBytesRowKey([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9})
				bs[len(bs)-1] = 247
				return bs
			},
			result: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			// recordPrefix + bytesFlag + [1, 2, 3, 4, 5, 6, 7, 8, 255, 9, 0, 0, 0, 0, 0, 0, 0, 0]
			key: func() []byte {
				bs := buildBytesRowKey([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9})
				bs[len(bs)-1] = 0
				return bs
			},
			result: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			// recordPrefix + bytesFlag + [1, 2, 3, 4, 5, 6, 7, 8, 255, 9, 0, 0, 0, 0, 0, 0, 0]
			key: func() []byte {
				bs := buildBytesRowKey([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9})
				bs = bs[:len(bs)-1]
				return bs
			},
			result: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		{
			// recordPrefix + bytesFlag + [1, 2, 3, 4, 5, 6, 7, 8, 255, 0, 0, 0, 0, 0, 0, 0, 0, 0]
			key: func() []byte {
				bs := buildBytesRowKey([]byte{1, 2, 3, 4, 5, 6, 7, 8})
				bs = bs[:len(bs)-1]
				return bs
			},
			result: []byte{1, 2, 3, 4, 5, 6, 7, 8},
		},
		{
			// recordPrefix + bytesFlag + [1, 2, 3, 4, 5, 6, 7, 8, 255, 0, 0, 0, 0, 0, 0, 0, 0, 246]
			key: func() []byte {
				bs := buildBytesRowKey([]byte{1, 2, 3, 4, 5, 6, 7, 8})
				bs = bs[:len(bs)-1]
				return bs
			},
			result: []byte{1, 2, 3, 4, 5, 6, 7, 8},
		},
	}

	for i, c := range cases {
		var key kv.Key
		switch k := c.key.(type) {
		case kv.Key:
			key = k
		case []byte:
			key = k
		case func() []byte:
			key = k()
		case func() kv.Key:
			key = k()
		default:
			require.FailNow(t, "%d", i)
		}

		d := cache.GetNextBytesHandleDatum(key, tablecodec.GenTableRecordPrefix(tblID))
		if c.isNull {
			require.True(t, d.IsNull(), i)
		} else {
			require.Equal(t, types.KindBytes, d.Kind(), i)
			require.Equal(t, c.result, d.GetBytes(), i)
		}
	}
}
func TestGetNextIntHandle(t *testing.T) {
	tblID := int64(7)
	cases := []struct {
		key    interface{}
		result int64
		isNull bool
	}{
		{
			key:    tablecodec.EncodeRowKeyWithHandle(tblID, kv.IntHandle(0)),
			result: 0,
		},
		{
			key:    tablecodec.EncodeRowKeyWithHandle(tblID, kv.IntHandle(3)),
			result: 3,
		},
		{
			key:    tablecodec.EncodeRowKeyWithHandle(tblID, kv.IntHandle(math.MaxInt64)),
			result: math.MaxInt64,
		},
		{
			key:    tablecodec.EncodeRowKeyWithHandle(tblID, kv.IntHandle(math.MinInt64)),
			result: math.MinInt64,
		},
		{
			key:    []byte{},
			result: math.MinInt64,
		},
		{
			key:    tablecodec.GenTableRecordPrefix(tblID),
			result: math.MinInt64,
		},
		{
			key:    tablecodec.GenTableRecordPrefix(tblID - 1),
			result: math.MinInt64,
		},
		{
			key:    tablecodec.GenTablePrefix(tblID).PrefixNext(),
			isNull: true,
		},
		{
			key:    tablecodec.EncodeRowKey(tblID, []byte{0}),
			result: codec.DecodeCmpUintToInt(0),
		},
		{
			key:    tablecodec.EncodeRowKey(tblID, []byte{0, 1, 2, 3}),
			result: codec.DecodeCmpUintToInt(0x0001020300000000),
		},
		{
			key:    tablecodec.EncodeRowKey(tblID, []byte{8, 1, 2, 3}),
			result: codec.DecodeCmpUintToInt(0x0801020300000000),
		},
		{
			key:    tablecodec.EncodeRowKey(tblID, []byte{0, 1, 2, 3, 4, 5, 6, 7, 0}),
			result: codec.DecodeCmpUintToInt(0x0001020304050607) + 1,
		},
		{
			key:    tablecodec.EncodeRowKey(tblID, []byte{8, 1, 2, 3, 4, 5, 6, 7, 0}),
			result: codec.DecodeCmpUintToInt(0x0801020304050607) + 1,
		},
		{
			key:    tablecodec.EncodeRowKey(tblID, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}),
			result: math.MaxInt64,
		},
		{
			key:    tablecodec.EncodeRowKey(tblID, []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0}),
			isNull: true,
		},
	}

	for i, c := range cases {
		var key kv.Key
		switch k := c.key.(type) {
		case kv.Key:
			key = k
		case []byte:
			key = k
		case func() []byte:
			key = k()
		case func() kv.Key:
			key = k()
		default:
			require.FailNow(t, "%d", i)
		}

		v := cache.GetNextIntHandle(key, tablecodec.GenTableRecordPrefix(tblID))
		if c.isNull {
			require.Nil(t, v, i)
		} else {
			require.IsType(t, kv.IntHandle(0), v, i)
			require.Equal(t, c.result, v.IntValue())
		}
	}
}
