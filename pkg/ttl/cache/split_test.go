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
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/ttl/cache"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
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

func (c *mockPDClient) ScanRegions(_ context.Context, key, endKey []byte, limit int, _ ...pd.GetRegionOption) ([]*pd.Region, error) {
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

func (c *mockPDClient) GetStore(_ context.Context, storeID uint64) (*metapb.Store, error) {
	return &metapb.Store{
		Id:      storeID,
		Address: fmt.Sprintf("127.0.0.%d", storeID),
	}, nil
}

func (c *mockPDClient) GetClusterID(_ context.Context) uint64 {
	return 1
}

type mockTiKVStore struct {
	t *testing.T
	helper.Storage
	pdClient     *mockPDClient
	cache        *tikv.RegionCache
	nextRegionID uint64
}

func newMockTiKVStore(t *testing.T) *mockTiKVStore {
	pdClient := &mockPDClient{t: t}
	s := &mockTiKVStore{
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

func (s *mockTiKVStore) addRegionBeginWithTablePrefix(tableID int64, handle kv.Handle) *mockTiKVStore {
	start := tablecodec.GenTablePrefix(tableID)
	end := tablecodec.EncodeRowKeyWithHandle(tableID, handle)
	return s.addRegion(start, end)
}

func (s *mockTiKVStore) addRegionEndWithTablePrefix(handle kv.Handle, tableID int64) *mockTiKVStore {
	start := tablecodec.EncodeRowKeyWithHandle(tableID, handle)
	end := tablecodec.GenTablePrefix(tableID + 1)
	return s.addRegion(start, end)
}

func (s *mockTiKVStore) addRegionWithTablePrefix(tableID int64, start kv.Handle, end kv.Handle) *mockTiKVStore {
	startKey := tablecodec.EncodeRowKeyWithHandle(tableID, start)
	endKey := tablecodec.EncodeRowKeyWithHandle(tableID, end)
	return s.addRegion(startKey, endKey)
}

func (s *mockTiKVStore) addRegion(key, endKey []byte) *mockTiKVStore {
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

func (s *mockTiKVStore) refreshCache() {
	_, err := s.cache.LoadRegionsInKeyRange(
		tikv.NewBackofferWithVars(context.Background(), 1000, nil),
		[]byte{},
		[]byte{0xFF},
	)
	require.NoError(s.t, err)
}

func (s *mockTiKVStore) batchAddIntHandleRegions(tblID int64, regionCnt, regionSize int,
	offset int64) (end kv.IntHandle) {
	for i := 0; i < regionCnt; i++ {
		start := kv.IntHandle(offset + int64(i*regionSize))
		end = kv.IntHandle(start.IntValue() + int64(regionSize))
		s.addRegionWithTablePrefix(tblID, start, end)
	}
	return
}

func (s *mockTiKVStore) clearRegions() {
	s.pdClient.regions = nil
	s.cache.Close()
	s.cache = tikv.NewRegionCache(s.pdClient)
	s.refreshCache()
}

func (s *mockTiKVStore) GetRegionCache() *tikv.RegionCache {
	return s.cache
}

func bytesHandle(t *testing.T, data []byte) kv.Handle {
	return commonHandle(t, types.NewBytesDatum(data))
}

func commonHandle(t *testing.T, d ...types.Datum) kv.Handle {
	encoded, err := codec.EncodeKey(time.UTC, nil, d...)
	require.NoError(t, err)
	h, err := kv.NewCommonHandle(encoded)
	require.NoError(t, err)
	return h
}

func createTTLTable(t *testing.T, tk *testkit.TestKit, name string, option string) *cache.PhysicalTable {
	if option == "" {
		return createTTLTableWithSQL(t, tk, name,
			fmt.Sprintf("create table test.%s(t timestamp) TTL = `t` + interval 1 day", name))
	}

	return createTTLTableWithSQL(t, tk, name,
		fmt.Sprintf("create table test.%s(id %s primary key, t timestamp) TTL = `t` + interval 1 day",
			name, option))
}

func create2PKTTLTable(t *testing.T, tk *testkit.TestKit, name string, option string) *cache.PhysicalTable {
	return createTTLTableWithSQL(t, tk, name,
		fmt.Sprintf(
			"create table test.%s(id %s, id2 int, t timestamp, primary key(id, id2)) TTL = `t` + interval 1 day",
			name, option))
}

func createTTLTableWithSQL(t *testing.T, tk *testkit.TestKit, name string, sql string) *cache.PhysicalTable {
	tk.MustExec(sql)
	is, ok := tk.Session().GetDomainInfoSchema().(infoschema.InfoSchema)
	require.True(t, ok)
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr(name))
	require.NoError(t, err)
	ttlTbl, err := cache.NewPhysicalTable(model.NewCIStr("test"), tbl.Meta(), model.NewCIStr(""))
	require.NoError(t, err)
	return ttlTbl
}

func checkRange(t *testing.T, r cache.ScanRange, start, end types.Datum) {
	if start.IsNull() {
		require.Nil(t, r.Start)
	} else {
		require.Equal(t, 1, len(r.Start))
		require.Equal(t, start.Kind(), r.Start[0].Kind())
		require.Equal(t, start.GetValue(), r.Start[0].GetValue())
	}

	if end.IsNull() {
		require.Nil(t, r.End)
	} else {
		require.Equal(t, 1, len(r.End))
		require.Equal(t, end.Kind(), r.End[0].Kind())
		require.Equal(t, end.GetValue(), r.End[0].GetValue())
	}
}

func TestSplitTTLScanRangesWithSignedInt(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tbls := []*cache.PhysicalTable{
		createTTLTable(t, tk, "t1", "tinyint"),
		createTTLTable(t, tk, "t2", "smallint"),
		createTTLTable(t, tk, "t3", "mediumint"),
		createTTLTable(t, tk, "t4", "int"),
		createTTLTable(t, tk, "t5", "bigint"),
		createTTLTable(t, tk, "t6", ""), // no clustered
	}

	tikvStore := newMockTiKVStore(t)
	for _, tbl := range tbls {
		// test only one region
		tikvStore.clearRegions()
		ranges, err := tbl.SplitScanRanges(context.TODO(), tikvStore, 4)
		require.NoError(t, err)
		require.Equal(t, 1, len(ranges))
		checkRange(t, ranges[0], types.Datum{}, types.Datum{})

		// test share regions with other table
		tikvStore.clearRegions()
		tikvStore.addRegion(
			tablecodec.GenTablePrefix(tbl.ID-1),
			tablecodec.GenTablePrefix(tbl.ID+1),
		)
		ranges, err = tbl.SplitScanRanges(context.TODO(), tikvStore, 4)
		require.NoError(t, err)
		require.Equal(t, 1, len(ranges))
		checkRange(t, ranges[0], types.Datum{}, types.Datum{})

		// test one table has multiple regions
		tikvStore.clearRegions()
		tikvStore.addRegionBeginWithTablePrefix(tbl.ID, kv.IntHandle(0))
		end := tikvStore.batchAddIntHandleRegions(tbl.ID, 8, 100, 0)
		tikvStore.addRegionEndWithTablePrefix(end, tbl.ID)
		ranges, err = tbl.SplitScanRanges(context.TODO(), tikvStore, 4)
		require.NoError(t, err)
		require.Equal(t, 4, len(ranges))
		checkRange(t, ranges[0], types.Datum{}, types.NewIntDatum(200))
		checkRange(t, ranges[1], types.NewIntDatum(200), types.NewIntDatum(500))
		checkRange(t, ranges[2], types.NewIntDatum(500), types.NewIntDatum(700))
		checkRange(t, ranges[3], types.NewIntDatum(700), types.Datum{})

		// test one table has multiple regions and one table region across 0
		tikvStore.clearRegions()
		tikvStore.addRegionBeginWithTablePrefix(tbl.ID, kv.IntHandle(-350))
		end = tikvStore.batchAddIntHandleRegions(tbl.ID, 8, 100, -350)
		tikvStore.addRegionEndWithTablePrefix(end, tbl.ID)
		ranges, err = tbl.SplitScanRanges(context.TODO(), tikvStore, 5)
		require.NoError(t, err)
		require.Equal(t, 5, len(ranges))
		checkRange(t, ranges[0], types.Datum{}, types.NewIntDatum(-250))
		checkRange(t, ranges[1], types.NewIntDatum(-250), types.NewIntDatum(-50))
		checkRange(t, ranges[2], types.NewIntDatum(-50), types.NewIntDatum(150))
		checkRange(t, ranges[3], types.NewIntDatum(150), types.NewIntDatum(350))
		checkRange(t, ranges[4], types.NewIntDatum(350), types.Datum{})
	}
}

func TestSplitTTLScanRangesWithUnsignedInt(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tbls := []*cache.PhysicalTable{
		createTTLTable(t, tk, "t1", "tinyint unsigned"),
		createTTLTable(t, tk, "t2", "smallint unsigned"),
		createTTLTable(t, tk, "t3", "mediumint unsigned"),
		createTTLTable(t, tk, "t4", "int unsigned"),
		createTTLTable(t, tk, "t5", "bigint unsigned"),
	}

	tikvStore := newMockTiKVStore(t)
	for _, tbl := range tbls {
		// test only one region
		tikvStore.clearRegions()
		ranges, err := tbl.SplitScanRanges(context.TODO(), tikvStore, 4)
		require.NoError(t, err)
		require.Equal(t, 1, len(ranges))
		checkRange(t, ranges[0], types.Datum{}, types.Datum{})

		// test share regions with other table
		tikvStore.clearRegions()
		tikvStore.addRegion(
			tablecodec.GenTablePrefix(tbl.ID-1),
			tablecodec.GenTablePrefix(tbl.ID+1),
		)
		ranges, err = tbl.SplitScanRanges(context.TODO(), tikvStore, 4)
		require.NoError(t, err)
		require.Equal(t, 1, len(ranges))
		checkRange(t, ranges[0], types.Datum{}, types.Datum{})

		// test one table has multiple regions: [MinInt64, a) [a, b) [b, 0) [0, c) [c, d) [d, MaxInt64]
		tikvStore.clearRegions()
		tikvStore.addRegionBeginWithTablePrefix(tbl.ID, kv.IntHandle(-200))
		end := tikvStore.batchAddIntHandleRegions(tbl.ID, 4, 100, -200)
		tikvStore.addRegionEndWithTablePrefix(end, tbl.ID)
		ranges, err = tbl.SplitScanRanges(context.TODO(), tikvStore, 6)
		require.NoError(t, err)
		require.Equal(t, 6, len(ranges))
		checkRange(t, ranges[0],
			types.NewUintDatum(uint64(math.MaxInt64)+1), types.NewUintDatum(uint64(math.MaxUint64)-199))
		checkRange(t, ranges[1],
			types.NewUintDatum(uint64(math.MaxUint64)-199), types.NewUintDatum(uint64(math.MaxUint64)-99))
		checkRange(t, ranges[2],
			types.NewUintDatum(uint64(math.MaxUint64)-99), types.Datum{})
		checkRange(t, ranges[3],
			types.Datum{}, types.NewUintDatum(100))
		checkRange(t, ranges[4],
			types.NewUintDatum(100), types.NewUintDatum(200))
		checkRange(t, ranges[5],
			types.NewUintDatum(200), types.NewUintDatum(uint64(math.MaxInt64)+1))

		// test one table has multiple regions: [MinInt64, a) [a, b) [b, c) [c, d) [d, MaxInt64], b < 0 < c
		tikvStore.clearRegions()
		tikvStore.addRegionBeginWithTablePrefix(tbl.ID, kv.IntHandle(-150))
		end = tikvStore.batchAddIntHandleRegions(tbl.ID, 3, 100, -150)
		tikvStore.addRegionEndWithTablePrefix(end, tbl.ID)
		ranges, err = tbl.SplitScanRanges(context.TODO(), tikvStore, 5)
		require.NoError(t, err)
		require.Equal(t, 6, len(ranges))
		checkRange(t, ranges[0],
			types.NewUintDatum(uint64(math.MaxInt64)+1), types.NewUintDatum(uint64(math.MaxUint64)-149))
		checkRange(t, ranges[1],
			types.NewUintDatum(uint64(math.MaxUint64)-149), types.NewUintDatum(uint64(math.MaxUint64)-49))
		checkRange(t, ranges[2],
			types.NewUintDatum(uint64(math.MaxUint64)-49), types.Datum{})
		checkRange(t, ranges[3],
			types.Datum{}, types.NewUintDatum(50))
		checkRange(t, ranges[4],
			types.NewUintDatum(50), types.NewUintDatum(150))
		checkRange(t, ranges[5],
			types.NewUintDatum(150), types.NewUintDatum(uint64(math.MaxInt64)+1))
	}
}

func TestSplitTTLScanRangesCommonHandleSignedInt(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tbls := []*cache.PhysicalTable{
		create2PKTTLTable(t, tk, "t1", "bigint"),
		create2PKTTLTable(t, tk, "t2", "int"),
	}

	tikvStore := newMockTiKVStore(t)
	for _, tbl := range tbls {
		// test only one region
		tikvStore.clearRegions()
		ranges, err := tbl.SplitScanRanges(context.TODO(), tikvStore, 4)
		require.NoError(t, err)
		require.Equal(t, 1, len(ranges))
		checkRange(t, ranges[0], types.Datum{}, types.Datum{})

		// test share regions with other table
		tikvStore.clearRegions()
		tikvStore.addRegion(
			tablecodec.GenTablePrefix(tbl.ID-1),
			tablecodec.GenTablePrefix(tbl.ID+1),
		)
		ranges, err = tbl.SplitScanRanges(context.TODO(), tikvStore, 4)
		require.NoError(t, err)
		require.Equal(t, 1, len(ranges))
		checkRange(t, ranges[0], types.Datum{}, types.Datum{})

		// test one table has multiple regions
		tikvStore.clearRegions()
		tikvStore.addRegionBeginWithTablePrefix(tbl.ID, commonHandle(t, types.NewIntDatum(-21)))
		tikvStore.addRegionWithTablePrefix(tbl.ID,
			commonHandle(t, types.NewIntDatum(-21)),
			commonHandle(t, types.NewIntDatum(-19), types.NewIntDatum(0)),
		)
		tikvStore.addRegionWithTablePrefix(tbl.ID,
			commonHandle(t, types.NewIntDatum(-19), types.NewIntDatum(0)),
			commonHandle(t, types.NewIntDatum(2)),
		)
		tikvStore.addRegionEndWithTablePrefix(commonHandle(t, types.NewIntDatum(2)), tbl.ID)
		ranges, err = tbl.SplitScanRanges(context.TODO(), tikvStore, 4)
		require.NoError(t, err)
		require.Equal(t, 4, len(ranges))
		checkRange(t, ranges[0], types.Datum{}, types.NewIntDatum(-21))
		checkRange(t, ranges[1], types.NewIntDatum(-21), types.NewIntDatum(-18))
		checkRange(t, ranges[2], types.NewIntDatum(-18), types.NewIntDatum(2))
		checkRange(t, ranges[3], types.NewIntDatum(2), types.Datum{})
	}
}

func TestSplitTTLScanRangesCommonHandleUnsignedInt(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tbls := []*cache.PhysicalTable{
		create2PKTTLTable(t, tk, "t1", "bigint unsigned"),
		create2PKTTLTable(t, tk, "t2", "int unsigned"),
	}

	tikvStore := newMockTiKVStore(t)
	for _, tbl := range tbls {
		// test only one region
		tikvStore.clearRegions()
		ranges, err := tbl.SplitScanRanges(context.TODO(), tikvStore, 4)
		require.NoError(t, err)
		require.Equal(t, 1, len(ranges))
		checkRange(t, ranges[0], types.Datum{}, types.Datum{})

		// test share regions with other table
		tikvStore.clearRegions()
		tikvStore.addRegion(
			tablecodec.GenTablePrefix(tbl.ID-1),
			tablecodec.GenTablePrefix(tbl.ID+1),
		)
		ranges, err = tbl.SplitScanRanges(context.TODO(), tikvStore, 4)
		require.NoError(t, err)
		require.Equal(t, 1, len(ranges))
		checkRange(t, ranges[0], types.Datum{}, types.Datum{})

		// test one table has multiple regions
		tikvStore.clearRegions()
		tikvStore.addRegionBeginWithTablePrefix(tbl.ID, commonHandle(t, types.NewUintDatum(9)))
		tikvStore.addRegionWithTablePrefix(tbl.ID,
			commonHandle(t, types.NewUintDatum(9)),
			commonHandle(t, types.NewUintDatum(23), types.NewUintDatum(0)),
		)
		tikvStore.addRegionWithTablePrefix(tbl.ID,
			commonHandle(t, types.NewUintDatum(23), types.NewUintDatum(0)),
			commonHandle(t, types.NewUintDatum(math.MaxInt64+9)),
		)
		tikvStore.addRegionEndWithTablePrefix(commonHandle(t, types.NewUintDatum(math.MaxInt64+9)), tbl.ID)
		ranges, err = tbl.SplitScanRanges(context.TODO(), tikvStore, 4)
		require.NoError(t, err)
		require.Equal(t, 4, len(ranges))
		checkRange(t, ranges[0], types.Datum{}, types.NewUintDatum(9))
		checkRange(t, ranges[1], types.NewUintDatum(9), types.NewUintDatum(24))
		checkRange(t, ranges[2], types.NewUintDatum(24), types.NewUintDatum(math.MaxInt64+9))
		checkRange(t, ranges[3], types.NewUintDatum(math.MaxInt64+9), types.Datum{})
	}
}

func TestSplitTTLScanRangesWithBytes(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tbls := []*cache.PhysicalTable{
		createTTLTable(t, tk, "t1", "binary(32)"),
		createTTLTable(t, tk, "t2", "char(32) CHARACTER SET BINARY"),
		createTTLTable(t, tk, "t3", "varchar(32) CHARACTER SET BINARY"),
		createTTLTable(t, tk, "t4", "bit(32)"),
		create2PKTTLTable(t, tk, "t5", "binary(32)"),
	}

	tikvStore := newMockTiKVStore(t)
	for _, tbl := range tbls {
		// test only one region
		tikvStore.clearRegions()
		ranges, err := tbl.SplitScanRanges(context.TODO(), tikvStore, 4)
		require.NoError(t, err)
		require.Equal(t, 1, len(ranges))
		checkRange(t, ranges[0], types.Datum{}, types.Datum{})

		// test share regions with other table
		tikvStore.clearRegions()
		tikvStore.addRegion(
			tablecodec.GenTablePrefix(tbl.ID-1),
			tablecodec.GenTablePrefix(tbl.ID+1),
		)
		ranges, err = tbl.SplitScanRanges(context.TODO(), tikvStore, 4)
		require.NoError(t, err)
		require.Equal(t, 1, len(ranges))
		checkRange(t, ranges[0], types.Datum{}, types.Datum{})

		// test one table has multiple regions
		tikvStore.clearRegions()
		tikvStore.addRegionBeginWithTablePrefix(tbl.ID, bytesHandle(t, []byte{1, 2, 3}))
		tikvStore.addRegionWithTablePrefix(
			tbl.ID, bytesHandle(t, []byte{1, 2, 3}), bytesHandle(t, []byte{1, 2, 3, 4}))
		tikvStore.addRegionWithTablePrefix(
			tbl.ID, bytesHandle(t, []byte{1, 2, 3, 4}), bytesHandle(t, []byte{1, 2, 3, 4, 5}))
		tikvStore.addRegionWithTablePrefix(
			tbl.ID, bytesHandle(t, []byte{1, 2, 3, 4, 5}), bytesHandle(t, []byte{1, 2, 4}))
		tikvStore.addRegionWithTablePrefix(
			tbl.ID, bytesHandle(t, []byte{1, 2, 4}), bytesHandle(t, []byte{1, 2, 5}))
		tikvStore.addRegionEndWithTablePrefix(bytesHandle(t, []byte{1, 2, 5}), tbl.ID)
		ranges, err = tbl.SplitScanRanges(context.TODO(), tikvStore, 4)
		require.NoError(t, err)
		require.Equal(t, 4, len(ranges))
		checkRange(t, ranges[0], types.Datum{}, types.NewBytesDatum([]byte{1, 2, 3, 4}))
		checkRange(t, ranges[1], types.NewBytesDatum([]byte{1, 2, 3, 4}), types.NewBytesDatum([]byte{1, 2, 4}))
		checkRange(t, ranges[2], types.NewBytesDatum([]byte{1, 2, 4}), types.NewBytesDatum([]byte{1, 2, 5}))
		checkRange(t, ranges[3], types.NewBytesDatum([]byte{1, 2, 5}), types.Datum{})
	}
}

func TestNoTTLSplitSupportTables(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tbls := []*cache.PhysicalTable{
		createTTLTable(t, tk, "t1", "char(32)  CHARACTER SET UTF8MB4"),
		createTTLTable(t, tk, "t2", "varchar(32) CHARACTER SET UTF8MB4"),
		createTTLTable(t, tk, "t4", "decimal(32, 2)"),
		create2PKTTLTable(t, tk, "t5", "char(32)  CHARACTER SET UTF8MB4"),
	}

	tikvStore := newMockTiKVStore(t)
	for _, tbl := range tbls {
		// test only one region
		tikvStore.clearRegions()
		ranges, err := tbl.SplitScanRanges(context.TODO(), tikvStore, 4)
		require.NoError(t, err)
		require.Equal(t, 1, len(ranges))
		checkRange(t, ranges[0], types.Datum{}, types.Datum{})

		// test share regions with other table
		tikvStore.clearRegions()
		tikvStore.addRegion(
			tablecodec.GenTablePrefix(tbl.ID-1),
			tablecodec.GenTablePrefix(tbl.ID+1),
		)
		ranges, err = tbl.SplitScanRanges(context.TODO(), tikvStore, 4)
		require.NoError(t, err)
		require.Equal(t, 1, len(ranges))
		checkRange(t, ranges[0], types.Datum{}, types.Datum{})

		// test one table has multiple regions
		tikvStore.clearRegions()
		tikvStore.addRegionBeginWithTablePrefix(tbl.ID, bytesHandle(t, []byte{1, 2, 3}))
		tikvStore.addRegionWithTablePrefix(tbl.ID, bytesHandle(t, []byte{1, 2, 3}), bytesHandle(t, []byte{1, 2, 3, 4}))
		tikvStore.addRegionEndWithTablePrefix(bytesHandle(t, []byte{1, 2, 3, 4}), tbl.ID)
		ranges, err = tbl.SplitScanRanges(context.TODO(), tikvStore, 3)
		require.NoError(t, err)
		require.Equal(t, 1, len(ranges))
		checkRange(t, ranges[0], types.Datum{}, types.Datum{})
	}
}

func TestGetNextBytesHandleDatum(t *testing.T) {
	tblID := int64(7)
	buildHandleBytes := func(data []byte) []byte {
		handleBytes, err := codec.EncodeKey(time.UTC, nil, types.NewBytesDatum(data))
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
		key    any
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
			key:    append(buildBytesRowKey([]byte{1, 2, 3, 4, 5, 6, 7, 8, 0}), 0),
			result: []byte{1, 2, 3, 4, 5, 6, 7, 8, 0, 0},
		},
		{
			key:    append(buildBytesRowKey([]byte{1, 2, 3, 4, 5, 6, 7, 8, 0}), 1),
			result: []byte{1, 2, 3, 4, 5, 6, 7, 8, 0, 0},
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
			result: []byte{1, 2, 3, 4, 5, 6, 7, 0},
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
		key    any
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
			key:    append(tablecodec.EncodeRowKeyWithHandle(tblID, kv.IntHandle(7)), 0),
			result: 8,
		},
		{
			key:    append(tablecodec.EncodeRowKeyWithHandle(tblID, kv.IntHandle(math.MaxInt64)), 0),
			isNull: true,
		},
		{
			key:    append(tablecodec.EncodeRowKeyWithHandle(tblID, kv.IntHandle(math.MinInt64)), 0),
			result: math.MinInt64 + 1,
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

func TestGetNextIntDatumFromCommonHandle(t *testing.T) {
	encode := func(tblID int64, d ...types.Datum) kv.Key {
		encoded, err := codec.EncodeKey(time.UTC, nil, d...)
		require.NoError(t, err)
		h, err := kv.NewCommonHandle(encoded)
		require.NoError(t, err)
		return tablecodec.EncodeRowKey(tblID, h.Encoded())
	}

	var nullDatum types.Datum
	nullDatum.SetNull()
	tblID := int64(7)
	fixedLen := len(encode(tblID, types.NewIntDatum(0)))

	cases := []struct {
		key      kv.Key
		d        types.Datum
		unsigned bool
	}{
		{
			key: encode(tblID, types.NewIntDatum(0)),
			d:   types.NewIntDatum(0),
		},
		{
			key: encode(tblID, types.NewIntDatum(1)),
			d:   types.NewIntDatum(1),
		},
		{
			key: encode(tblID, types.NewIntDatum(1024)),
			d:   types.NewIntDatum(1024),
		},
		{
			key: encode(tblID, types.NewIntDatum(math.MaxInt64)),
			d:   types.NewIntDatum(math.MaxInt64),
		},
		{
			key: encode(tblID, types.NewIntDatum(math.MaxInt64/2)),
			d:   types.NewIntDatum(math.MaxInt64 / 2),
		},
		{
			key: encode(tblID, types.NewIntDatum(-1)),
			d:   types.NewIntDatum(-1),
		},
		{
			key: encode(tblID, types.NewIntDatum(-1024)),
			d:   types.NewIntDatum(-1024),
		},
		{
			key: encode(tblID, types.NewIntDatum(math.MinInt64)),
			d:   types.NewIntDatum(math.MinInt64),
		},
		{
			key: encode(tblID, types.NewIntDatum(math.MinInt64/2)),
			d:   types.NewIntDatum(math.MinInt64 / 2),
		},
		{
			key: encode(tblID, types.NewIntDatum(math.MaxInt64))[:fixedLen-1],
			d:   types.NewIntDatum(math.MaxInt64 - 0xFF),
		},
		{
			key: encode(tblID, types.NewIntDatum(math.MaxInt64), types.NewIntDatum(0)),
			d:   nullDatum,
		},
		{
			key: encode(tblID, types.NewIntDatum(math.MaxInt64-1), types.NewIntDatum(0)),
			d:   types.NewIntDatum(math.MaxInt64),
		},
		{
			key: encode(tblID, types.NewIntDatum(123), types.NewIntDatum(0)),
			d:   types.NewIntDatum(124),
		},
		{
			key: encode(tblID, types.NewIntDatum(-123), types.NewIntDatum(0)),
			d:   types.NewIntDatum(-122),
		},
		{
			key: encode(tblID, types.NewIntDatum(math.MinInt64), types.NewIntDatum(0)),
			d:   types.NewIntDatum(math.MinInt64 + 1),
		},
		{
			key:      encode(tblID, types.NewUintDatum(0)),
			d:        types.NewUintDatum(0),
			unsigned: true,
		},
		{
			key:      encode(tblID, types.NewUintDatum(1)),
			d:        types.NewUintDatum(1),
			unsigned: true,
		},
		{
			key:      encode(tblID, types.NewUintDatum(1024)),
			d:        types.NewUintDatum(1024),
			unsigned: true,
		},
		{
			key:      encode(tblID, types.NewUintDatum(math.MaxInt64)),
			d:        types.NewUintDatum(math.MaxInt64),
			unsigned: true,
		},
		{
			key:      encode(tblID, types.NewUintDatum(math.MaxInt64+1)),
			d:        types.NewUintDatum(math.MaxInt64 + 1),
			unsigned: true,
		},
		{
			key:      encode(tblID, types.NewUintDatum(math.MaxUint64)),
			d:        types.NewUintDatum(math.MaxUint64),
			unsigned: true,
		},
		{
			key:      encode(tblID, types.NewUintDatum(math.MaxUint64))[:fixedLen-1],
			d:        types.NewUintDatum(math.MaxUint64 - 0xFF),
			unsigned: true,
		},

		{
			key: encode(tblID, types.NewUintDatum(math.MaxUint64), types.NewIntDatum(0)),
			d:   nullDatum,
		},
		{
			key:      encode(tblID, types.NewUintDatum(math.MaxUint64-1), types.NewIntDatum(0)),
			d:        types.NewUintDatum(math.MaxUint64),
			unsigned: true,
		},
		{
			key:      encode(tblID, types.NewUintDatum(123), types.NewIntDatum(0)),
			d:        types.NewUintDatum(124),
			unsigned: true,
		},
		{
			key:      encode(tblID, types.NewUintDatum(0), types.NewIntDatum(0)),
			d:        types.NewUintDatum(1),
			unsigned: true,
		},
		{
			key: []byte{},
			d:   types.NewIntDatum(math.MinInt64),
		},
		{
			key:      []byte{},
			d:        types.NewUintDatum(0),
			unsigned: true,
		},
		{
			key: tablecodec.GenTableRecordPrefix(tblID),
			d:   types.NewIntDatum(math.MinInt64),
		},
		{
			key:      tablecodec.GenTableRecordPrefix(tblID),
			d:        types.NewUintDatum(0),
			unsigned: true,
		},
		{
			// 3 is encoded intFlag
			key: append(tablecodec.GenTableRecordPrefix(tblID), []byte{3}...),
			d:   types.NewIntDatum(math.MinInt64),
		},
		{
			// 3 is encoded intFlag
			key:      append(tablecodec.GenTableRecordPrefix(tblID), []byte{3}...),
			d:        types.NewUintDatum(0),
			unsigned: true,
		},
		{
			// 4 is encoded uintFlag
			key: append(tablecodec.GenTableRecordPrefix(tblID), []byte{4}...),
			d:   nullDatum,
		},
		{
			// 4 is encoded uintFlag
			key:      append(tablecodec.GenTableRecordPrefix(tblID), []byte{4}...),
			d:        types.NewUintDatum(0),
			unsigned: true,
		},
		{
			// 5
			key: append(tablecodec.GenTableRecordPrefix(tblID), []byte{5}...),
			d:   nullDatum,
		},
		{
			// 5
			key:      append(tablecodec.GenTableRecordPrefix(tblID), []byte{5}...),
			d:        nullDatum,
			unsigned: true,
		},
	}

	for _, c := range cases {
		if !c.d.IsNull() {
			if c.unsigned {
				require.Equal(t, types.KindUint64, c.d.Kind())
			} else {
				require.Equal(t, types.KindInt64, c.d.Kind())
			}
		}

		d := cache.GetNextIntDatumFromCommonHandle(c.key, tablecodec.GenTableRecordPrefix(tblID), c.unsigned)
		require.Equal(t, c.d, d)
	}
}
