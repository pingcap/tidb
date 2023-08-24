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
	"testing"

	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/ttl/cache"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/stretchr/testify/require"
)

func bytesHandle(t *testing.T, data []byte) kv.Handle {
	encoded, err := codec.EncodeKey(nil, nil, types.NewBytesDatum(data))
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
		create2PKTTLTable(t, tk, "t7", "tinyint"),
	}

	tikvStore := mockstore.NewMockTiKVStore(t)
	for _, tbl := range tbls {
		// test only one region
		tikvStore.ClearRegions()
		ranges, err := tbl.SplitScanRanges(context.TODO(), tikvStore, 4)
		require.NoError(t, err)
		require.Equal(t, 1, len(ranges))
		checkRange(t, ranges[0], types.Datum{}, types.Datum{})

		// test share regions with other table
		tikvStore.ClearRegions()
		tikvStore.AddRegion(
			tablecodec.GenTablePrefix(tbl.ID-1),
			tablecodec.GenTablePrefix(tbl.ID+1),
		)
		ranges, err = tbl.SplitScanRanges(context.TODO(), tikvStore, 4)
		require.NoError(t, err)
		require.Equal(t, 1, len(ranges))
		checkRange(t, ranges[0], types.Datum{}, types.Datum{})

		// test one table has multiple regions
		tikvStore.ClearRegions()
		tikvStore.AddRegionBeginWithTablePrefix(tbl.ID, kv.IntHandle(0))
		end := tikvStore.BatchAddIntHandleRegions(tbl.ID, 8, 100, 0)
		tikvStore.AddRegionEndWithTablePrefix(end, tbl.ID)
		ranges, err = tbl.SplitScanRanges(context.TODO(), tikvStore, 4)
		require.NoError(t, err)
		require.Equal(t, 4, len(ranges))
		checkRange(t, ranges[0], types.Datum{}, types.NewIntDatum(200))
		checkRange(t, ranges[1], types.NewIntDatum(200), types.NewIntDatum(500))
		checkRange(t, ranges[2], types.NewIntDatum(500), types.NewIntDatum(700))
		checkRange(t, ranges[3], types.NewIntDatum(700), types.Datum{})

		// test one table has multiple regions and one table region across 0
		tikvStore.ClearRegions()
		tikvStore.AddRegionBeginWithTablePrefix(tbl.ID, kv.IntHandle(-350))
		end = tikvStore.BatchAddIntHandleRegions(tbl.ID, 8, 100, -350)
		tikvStore.AddRegionEndWithTablePrefix(end, tbl.ID)
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
		create2PKTTLTable(t, tk, "t6", "tinyint unsigned"),
	}

	tikvStore := mockstore.NewMockTiKVStore(t)
	for _, tbl := range tbls {
		// test only one region
		tikvStore.ClearRegions()
		ranges, err := tbl.SplitScanRanges(context.TODO(), tikvStore, 4)
		require.NoError(t, err)
		require.Equal(t, 1, len(ranges))
		checkRange(t, ranges[0], types.Datum{}, types.Datum{})

		// test share regions with other table
		tikvStore.ClearRegions()
		tikvStore.AddRegion(
			tablecodec.GenTablePrefix(tbl.ID-1),
			tablecodec.GenTablePrefix(tbl.ID+1),
		)
		ranges, err = tbl.SplitScanRanges(context.TODO(), tikvStore, 4)
		require.NoError(t, err)
		require.Equal(t, 1, len(ranges))
		checkRange(t, ranges[0], types.Datum{}, types.Datum{})

		// test one table has multiple regions: [MinInt64, a) [a, b) [b, 0) [0, c) [c, d) [d, MaxInt64]
		tikvStore.ClearRegions()
		tikvStore.AddRegionBeginWithTablePrefix(tbl.ID, kv.IntHandle(-200))
		end := tikvStore.BatchAddIntHandleRegions(tbl.ID, 4, 100, -200)
		tikvStore.AddRegionEndWithTablePrefix(end, tbl.ID)
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
		tikvStore.ClearRegions()
		tikvStore.AddRegionBeginWithTablePrefix(tbl.ID, kv.IntHandle(-150))
		end = tikvStore.BatchAddIntHandleRegions(tbl.ID, 3, 100, -150)
		tikvStore.AddRegionEndWithTablePrefix(end, tbl.ID)
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

	tikvStore := mockstore.NewMockTiKVStore(t)
	for _, tbl := range tbls {
		// test only one region
		tikvStore.ClearRegions()
		ranges, err := tbl.SplitScanRanges(context.TODO(), tikvStore, 4)
		require.NoError(t, err)
		require.Equal(t, 1, len(ranges))
		checkRange(t, ranges[0], types.Datum{}, types.Datum{})

		// test share regions with other table
		tikvStore.ClearRegions()
		tikvStore.AddRegion(
			tablecodec.GenTablePrefix(tbl.ID-1),
			tablecodec.GenTablePrefix(tbl.ID+1),
		)
		ranges, err = tbl.SplitScanRanges(context.TODO(), tikvStore, 4)
		require.NoError(t, err)
		require.Equal(t, 1, len(ranges))
		checkRange(t, ranges[0], types.Datum{}, types.Datum{})

		// test one table has multiple regions
		tikvStore.ClearRegions()
		tikvStore.AddRegionBeginWithTablePrefix(tbl.ID, bytesHandle(t, []byte{1, 2, 3}))
		tikvStore.AddRegionWithTablePrefix(
			tbl.ID, bytesHandle(t, []byte{1, 2, 3}), bytesHandle(t, []byte{1, 2, 3, 4}))
		tikvStore.AddRegionWithTablePrefix(
			tbl.ID, bytesHandle(t, []byte{1, 2, 3, 4}), bytesHandle(t, []byte{1, 2, 3, 4, 5}))
		tikvStore.AddRegionWithTablePrefix(
			tbl.ID, bytesHandle(t, []byte{1, 2, 3, 4, 5}), bytesHandle(t, []byte{1, 2, 4}))
		tikvStore.AddRegionWithTablePrefix(
			tbl.ID, bytesHandle(t, []byte{1, 2, 4}), bytesHandle(t, []byte{1, 2, 5}))
		tikvStore.AddRegionEndWithTablePrefix(bytesHandle(t, []byte{1, 2, 5}), tbl.ID)
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

	tikvStore := mockstore.NewMockTiKVStore(t)
	for _, tbl := range tbls {
		// test only one region
		tikvStore.ClearRegions()
		ranges, err := tbl.SplitScanRanges(context.TODO(), tikvStore, 4)
		require.NoError(t, err)
		require.Equal(t, 1, len(ranges))
		checkRange(t, ranges[0], types.Datum{}, types.Datum{})

		// test share regions with other table
		tikvStore.ClearRegions()
		tikvStore.AddRegion(
			tablecodec.GenTablePrefix(tbl.ID-1),
			tablecodec.GenTablePrefix(tbl.ID+1),
		)
		ranges, err = tbl.SplitScanRanges(context.TODO(), tikvStore, 4)
		require.NoError(t, err)
		require.Equal(t, 1, len(ranges))
		checkRange(t, ranges[0], types.Datum{}, types.Datum{})

		// test one table has multiple regions
		tikvStore.ClearRegions()
		tikvStore.AddRegionBeginWithTablePrefix(tbl.ID, bytesHandle(t, []byte{1, 2, 3}))
		tikvStore.AddRegionWithTablePrefix(tbl.ID, bytesHandle(t, []byte{1, 2, 3}), bytesHandle(t, []byte{1, 2, 3, 4}))
		tikvStore.AddRegionEndWithTablePrefix(bytesHandle(t, []byte{1, 2, 3, 4}), tbl.ID)
		ranges, err = tbl.SplitScanRanges(context.TODO(), tikvStore, 3)
		require.NoError(t, err)
		require.Equal(t, 1, len(ranges))
		checkRange(t, ranges[0], types.Datum{}, types.Datum{})
	}
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
