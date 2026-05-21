// Copyright 2026 PingCAP, Inc.
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

package ddl

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/docker/go-units"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/util/rowsize"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
	tikv "github.com/tikv/client-go/v2/tikv"
	pdhttp "github.com/tikv/pd/client/http"
)

type mockCodec struct {
	tikv.Codec
}

func (mockCodec) EncodeRegionRange(start, end []byte) ([]byte, []byte) {
	return append([]byte("k:"), start...), append([]byte("k:"), end...)
}

type mockHelperStorage struct {
	helper.Storage
	codec tikv.Codec
	pdCli pdhttp.Client
}

func (s mockHelperStorage) GetCodec() tikv.Codec {
	return s.codec
}

func (s mockHelperStorage) GetPDHTTPClient() pdhttp.Client {
	return s.pdCli
}

func (mockHelperStorage) GetRegionCache() *tikv.RegionCache {
	return nil
}

type mockPDHTTPClient struct {
	pdhttp.Client
	regionInfos []*pdhttp.RegionsInfo
	storesInfo  *pdhttp.StoresInfo
	callCount   int
	firstRange  *pdhttp.KeyRange
	firstLimit  int
}

func (c *mockPDHTTPClient) WithCallerID(string) pdhttp.Client {
	return c
}

func (c *mockPDHTTPClient) GetRegionsByKeyRange(_ context.Context, keyRange *pdhttp.KeyRange, limit int) (*pdhttp.RegionsInfo, error) {
	if c.callCount == 0 {
		c.firstRange = keyRange
		c.firstLimit = limit
	}
	if c.callCount >= len(c.regionInfos) {
		return &pdhttp.RegionsInfo{}, nil
	}
	info := c.regionInfos[c.callCount]
	c.callCount++
	return info, nil
}

func (c *mockPDHTTPClient) GetStores(context.Context) (*pdhttp.StoresInfo, error) {
	if c.storesInfo == nil {
		return &pdhttp.StoresInfo{}, nil
	}
	return c.storesInfo, nil
}

func expectedRegionRange(tableID int64) ([]byte, []byte) {
	tableStart, tableEnd := tablecodec.GetTableHandleKeyRange(tableID)
	return mockCodec{}.EncodeRegionRange(tableStart, tableEnd)
}

func TestEstimateTableSizeByIDUsesMaxApproximateSizes(t *testing.T) {
	t.Run("TiKVSpacePrecheckCapability", func(t *testing.T) {
		ok, err := canRunTiKVSpacePrecheck(mockHelperStorage{codec: mockCodec{}})
		require.NoError(t, err)
		require.False(t, ok)

		ok, err = canRunTiKVSpacePrecheck(mockHelperStorage{
			codec: mockCodec{},
			pdCli: &mockPDHTTPClient{},
		})
		require.NoError(t, err)
		require.True(t, ok)
	})

	pdCli := &mockPDHTTPClient{
		regionInfos: []*pdhttp.RegionsInfo{
			{
				Count: 3,
				Regions: []pdhttp.RegionInfo{
					// kv > size -> use kv
					{ID: 1, ApproximateSize: 5, ApproximateKvSize: 64},
					// size > kv -> use size
					{ID: 2, ApproximateSize: 16, ApproximateKvSize: 7},
					// zero still follows max()
					{ID: 3, ApproximateSize: 0, ApproximateKvSize: 9},
				},
			},
			{},
		},
	}

	size, err := estimateTableSizeByID(context.Background(), pdCli, mockHelperStorage{codec: mockCodec{}}, 42)
	require.NoError(t, err)
	require.Equal(t, int64(89*units.MiB), size)
	require.Equal(t, 2, pdCli.callCount)
	expectedStart, expectedEnd := expectedRegionRange(42)
	require.NotNil(t, pdCli.firstRange)
	require.Equal(t, 128, pdCli.firstLimit)
	require.Equal(t, expectedStart, pdCli.firstRange.StartKey)
	require.Equal(t, expectedEnd, pdCli.firstRange.EndKey)

	t.Run("EstimateRowSizeFromRegionUsesMaxApproximateSizes", func(t *testing.T) {
		tableID := int64(1024)
		tbl := tables.MockTableFromMeta(&model.TableInfo{ID: tableID})
		testCases := []struct {
			name            string
			approxSizeMiB   int64
			approxKvSizeMiB int64
			approxKeys      int64
			expectedBytes   int
		}{
			{
				name:            "kv-greater-than-size-uses-kv",
				approxSizeMiB:   4,
				approxKvSizeMiB: 10,
				approxKeys:      2,
				expectedBytes:   int(10 * units.MiB / 2),
			},
			{
				name:            "size-greater-than-kv-uses-size",
				approxSizeMiB:   12,
				approxKvSizeMiB: 3,
				approxKeys:      3,
				expectedBytes:   int(12 * units.MiB / 3),
			},
			{
				name:            "zero-kv-size-still-uses-max",
				approxSizeMiB:   9,
				approxKvSizeMiB: 0,
				approxKeys:      3,
				expectedBytes:   int(9 * units.MiB / 3),
			},
		}
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				rowPD := &mockPDHTTPClient{
					regionInfos: []*pdhttp.RegionsInfo{
						{
							Count: 3,
							Regions: []pdhttp.RegionInfo{
								{ID: 1, ApproximateSize: 1, ApproximateKvSize: 1, ApproximateKeys: 1},
								{
									ID:                2,
									ApproximateSize:   tc.approxSizeMiB,
									ApproximateKvSize: tc.approxKvSizeMiB,
									ApproximateKeys:   tc.approxKeys,
								},
								{ID: 3, ApproximateSize: 1, ApproximateKvSize: 1, ApproximateKeys: 1},
							},
						},
					},
				}
				rowSize, err := estimateRowSizeFromRegion(
					context.Background(),
					mockHelperStorage{codec: mockCodec{}, pdCli: rowPD},
					tbl,
				)
				require.NoError(t, err)
				require.Equal(t, tc.expectedBytes, rowSize)
				require.Equal(t, 1, rowPD.callCount)
				require.Equal(t, 3, rowPD.firstLimit)
				expectedStart, expectedEnd := expectedRegionRange(tableID)
				require.NotNil(t, rowPD.firstRange)
				require.Equal(t, expectedStart, rowPD.firstRange.StartKey)
				require.Equal(t, expectedEnd, rowPD.firstRange.EndKey)
			})
		}
	})
}

func TestSumTiKVStoreUsage(t *testing.T) {
	stores := []pdhttp.StoreInfo{
		{
			Store: pdhttp.MetaStore{
				ID:     1,
				Labels: []pdhttp.StoreLabel{{Key: "zone", Value: "z1"}},
			},
			Status: pdhttp.StoreStatus{Capacity: "1KiB", Available: "896B"},
		},
		{
			Store: pdhttp.MetaStore{
				ID:     2,
				Labels: []pdhttp.StoreLabel{{Key: "engine", Value: "tikv"}},
			},
			Status: pdhttp.StoreStatus{Capacity: "1KiB", Available: "768B"},
		},
		{
			Store: pdhttp.MetaStore{
				ID:     3,
				Labels: []pdhttp.StoreLabel{{Key: "engine", Value: "tiflash"}},
			},
			Status: pdhttp.StoreStatus{Capacity: "2KiB", Available: "1KiB"},
		},
		{
			Store: pdhttp.MetaStore{
				ID:     5,
				Labels: []pdhttp.StoreLabel{{Key: "engine", Value: "tiflash_compute"}},
			},
			Status: pdhttp.StoreStatus{Capacity: "2KiB", Available: "512B"},
		},
		{
			Store:  pdhttp.MetaStore{ID: 4},
			Status: pdhttp.StoreStatus{Capacity: "1KiB", Available: "1KiB"},
		},
	}

	t.Run("usage snapshot", func(t *testing.T) {
		usage, err := sumTiKVStoreUsage(stores)
		require.NoError(t, err)
		require.Equal(t, 3, usage.StoreCount)
		require.EqualValues(t, 384, usage.UsedBytes)
	})

	t.Run("capacity snapshot", func(t *testing.T) {
		capacity, err := sumTiKVStoreCapacity(stores)
		require.NoError(t, err)
		require.Equal(t, 3, capacity.StoreCount)
		require.EqualValues(t, 3*1024, capacity.TotalBytes)
		require.EqualValues(t, 3*1024-384, capacity.AvailableBytes)
		require.EqualValues(t, 384, capacity.UsedBytes)
		require.Len(t, capacity.Stores, 3)
		require.EqualValues(t, 1, capacity.Stores[0].StoreID)
		require.EqualValues(t, 2, capacity.Stores[1].StoreID)
		require.EqualValues(t, 4, capacity.Stores[2].StoreID)
	})
}

func TestCollectTiKVStoreUsage(t *testing.T) {
	pdCli := &mockPDHTTPClient{
		storesInfo: &pdhttp.StoresInfo{
			Stores: []pdhttp.StoreInfo{
				{
					Store: pdhttp.MetaStore{
						ID:     1,
						Labels: []pdhttp.StoreLabel{{Key: "engine", Value: "tikv"}},
					},
					Status: pdhttp.StoreStatus{Capacity: "1KiB", Available: "960B"},
				},
				{
					Store: pdhttp.MetaStore{
						ID:     2,
						Labels: []pdhttp.StoreLabel{{Key: "engine", Value: "tiflash"}},
					},
					Status: pdhttp.StoreStatus{Capacity: "2KiB", Available: "1KiB"},
				},
			},
		},
	}

	t.Run("usage snapshot", func(t *testing.T) {
		usage, err := collectTiKVStoreUsage(context.Background(), mockHelperStorage{codec: mockCodec{}, pdCli: pdCli})
		require.NoError(t, err)
		require.Equal(t, 1, usage.StoreCount)
		require.EqualValues(t, 64, usage.UsedBytes)
	})

	t.Run("capacity snapshot", func(t *testing.T) {
		capacity, err := collectTiKVStoreCapacity(context.Background(), mockHelperStorage{codec: mockCodec{}, pdCli: pdCli})
		require.NoError(t, err)
		require.Equal(t, 1, capacity.StoreCount)
		require.EqualValues(t, 1024, capacity.TotalBytes)
		require.EqualValues(t, 960, capacity.AvailableBytes)
		require.EqualValues(t, 64, capacity.UsedBytes)
		require.Len(t, capacity.Stores, 1)
		require.EqualValues(t, 1, capacity.Stores[0].StoreID)
	})

	t.Run("space precheck", func(t *testing.T) {
		err := checkTiKVSpaceForAddIndex(&TiKVClusterCapacity{
			TotalBytes:     1000,
			AvailableBytes: 380,
			StoreCount:     2,
			Stores: []TiKVStoreCapacity{
				{StoreID: 1, TotalBytes: 500, AvailableBytes: 260},
				{StoreID: 2, TotalBytes: 500, AvailableBytes: 120},
			},
		}, 60)
		require.NoError(t, err)

		err = checkTiKVSpaceForAddIndex(&TiKVClusterCapacity{
			TotalBytes:     1000,
			AvailableBytes: 210,
			StoreCount:     2,
			Stores: []TiKVStoreCapacity{
				{StoreID: 1, TotalBytes: 500, AvailableBytes: 160},
				{StoreID: 2, TotalBytes: 500, AvailableBytes: 50},
			},
		}, 20)
		require.ErrorContains(t, err, "cluster capacity check")

		err = checkTiKVSpaceForAddIndex(&TiKVClusterCapacity{
			TotalBytes:     1000,
			AvailableBytes: 320,
			StoreCount:     2,
			Stores: []TiKVStoreCapacity{
				{StoreID: 1, TotalBytes: 500, AvailableBytes: 260},
				{StoreID: 2, TotalBytes: 500, AvailableBytes: 60},
			},
		}, 120)
		require.ErrorContains(t, err, "store capacity check")
	})

	t.Run("prediction rowsize uses unique id stats", func(t *testing.T) {
		sessionVars := variable.NewSessionVars(nil)
		indexCols := []*expression.Column{
			{ID: 1, UniqueID: 101, RetType: types.NewFieldType(mysql.TypeLonglong)},
			{ID: 2, UniqueID: 102, RetType: types.NewFieldType(mysql.TypeVarString)},
			{ID: model.ExtraHandleID, UniqueID: 103, RetType: types.NewFieldType(mysql.TypeLonglong)},
		}
		statsColl := statistics.NewHistColl(1, 100, 0, 3, 0)
		statsColl.SetCol(1, &statistics.Column{
			Info:      &model.ColumnInfo{ID: 1},
			Histogram: *statistics.NewHistogram(1, 0, 0, 0, types.NewFieldType(mysql.TypeLonglong), 0, 100),
		})
		statsColl.SetCol(2, &statistics.Column{
			Info:      &model.ColumnInfo{ID: 2},
			Histogram: *statistics.NewHistogram(2, 0, 0, 0, types.NewFieldType(mysql.TypeVarString), 0, 3300),
		})
		statsColl.SetCol(model.ExtraHandleID, &statistics.Column{
			Info:      model.NewExtraHandleColInfo(),
			Histogram: *statistics.NewHistogram(model.ExtraHandleID, 0, 0, 0, types.NewFieldType(mysql.TypeLonglong), 0, 0),
			IsHandle:  true,
		})

		rawSize := rowsize.GetIndexAvgRowSize(sessionVars, statsColl, indexCols, false)
		mappedSize := rowsize.GetIndexAvgRowSize(sessionVars, statsColl.ID2UniqueID(indexCols), indexCols, false)

		require.Greater(t, mappedSize, rawSize)
		require.Greater(t, mappedSize, float64(60))
		require.Less(t, rawSize, float64(50))
	})

	t.Run("prediction value estimate includes restored data", func(t *testing.T) {
		sessionVars := variable.NewSessionVars(nil)
		intType := types.NewFieldType(mysql.TypeLonglong)
		intType.AddFlag(mysql.NotNullFlag)
		stringType := types.NewFieldType(mysql.TypeVarString)
		stringType.SetCharset(charset.CharsetUTF8MB4)
		stringType.SetCollate("utf8mb4_general_ci")
		tblInfo := &model.TableInfo{
			Columns: []*model.ColumnInfo{
				{ID: 1, Offset: 0, FieldType: *intType},
				{ID: 2, Offset: 1, FieldType: *stringType},
			},
		}
		idxInfo := &model.IndexInfo{
			Columns: []*model.IndexColumn{
				{Name: ast.NewCIStr("k"), Offset: 0, Length: types.UnspecifiedLength},
				{Name: ast.NewCIStr("pad"), Offset: 1, Length: 32},
			},
		}
		indexCols := []*expression.Column{
			{ID: 1, UniqueID: 101, RetType: &tblInfo.Columns[0].FieldType},
			{ID: 2, UniqueID: 102, RetType: &tblInfo.Columns[1].FieldType},
			{ID: model.ExtraHandleID, UniqueID: 103, RetType: types.NewFieldType(mysql.TypeLonglong)},
		}
		statsColl := statistics.NewHistColl(1, 4_000_000, 0, 3, 0)
		statsColl.SetCol(1, &statistics.Column{
			Info:      tblInfo.Columns[0],
			Histogram: *statistics.NewHistogram(1, 0, 0, 0, intType, 0, 32_000_000),
		})
		statsColl.SetCol(2, &statistics.Column{
			Info:      tblInfo.Columns[1],
			Histogram: *statistics.NewHistogram(2, 0, 0, 0, stringType, 0, 128_000_000),
		})
		statsColl.SetCol(model.ExtraHandleID, &statistics.Column{
			Info:      model.NewExtraHandleColInfo(),
			Histogram: *statistics.NewHistogram(model.ExtraHandleID, 0, 0, 0, types.NewFieldType(mysql.TypeLonglong), 0, 0),
			IsHandle:  true,
		})

		keyBytes := rowsize.GetIndexAvgRowSize(sessionVars, statsColl.ID2UniqueID(indexCols), indexCols, false)
		valueBytes := estimateIndexValueBytesPerRowForPrediction(tblInfo, idxInfo, statsColl)

		require.True(t, tables.NeedRestoredData(idxInfo.Columns, tblInfo.Columns))
		require.Greater(t, valueBytes, float64(60))
		require.Greater(t, keyBytes+valueBytes, keyBytes+float64(40))
	})

	t.Run("represent prediction exceeds basic prediction for restored data index", func(t *testing.T) {
		sctx := mock.NewContext()
		intType := types.NewFieldType(mysql.TypeLonglong)
		intType.AddFlag(mysql.NotNullFlag)
		stringType := types.NewFieldType(mysql.TypeVarString)
		stringType.SetCharset(charset.CharsetUTF8MB4)
		stringType.SetCollate("utf8mb4_general_ci")
		tblInfo := &model.TableInfo{
			ID: 100,
			Columns: []*model.ColumnInfo{
				{ID: 1, Offset: 0, FieldType: *intType},
				{ID: 2, Offset: 1, FieldType: *stringType},
			},
		}
		idxInfo := &model.IndexInfo{
			ID: 10,
			Columns: []*model.IndexColumn{
				{Name: ast.NewCIStr("k"), Offset: 0, Length: types.UnspecifiedLength},
				{Name: ast.NewCIStr("pad"), Offset: 1, Length: 32},
			},
		}
		indexCols := []*expression.Column{
			{ID: 1, UniqueID: 101, RetType: &tblInfo.Columns[0].FieldType},
			{ID: 2, UniqueID: 102, RetType: &tblInfo.Columns[1].FieldType},
			{ID: model.ExtraHandleID, UniqueID: 103, RetType: types.NewFieldType(mysql.TypeLonglong)},
		}
		statsColl := statistics.NewHistColl(1, 4_000_000, 0, 3, 0)
		statsColl.SetCol(1, &statistics.Column{
			Info:      tblInfo.Columns[0],
			Histogram: *statistics.NewHistogram(1, 0, 0, 0, intType, 0, 32_000_000),
		})
		statsColl.SetCol(2, &statistics.Column{
			Info:      tblInfo.Columns[1],
			Histogram: *statistics.NewHistogram(2, 0, 0, 0, stringType, 0, 128_000_000),
		})
		statsColl.SetCol(model.ExtraHandleID, &statistics.Column{
			Info:      model.NewExtraHandleColInfo(),
			Histogram: *statistics.NewHistogram(model.ExtraHandleID, 0, 0, 0, types.NewFieldType(mysql.TypeLonglong), 0, 0),
			IsHandle:  true,
		})
		statsTbl := &statistics.Table{HistColl: *statsColl}

		basicBytes := estimateIndexKVBytesPerRowForBasicPrediction(sctx, tblInfo, idxInfo, statsTbl, indexCols)
		representBytes, err := estimateIndexKVBytesPerRowForRepresentPrediction(sctx, tblInfo, idxInfo, tblInfo.ID, statsTbl)
		require.NoError(t, err)
		require.Greater(t, representBytes, basicBytes)
	})

	t.Run("sampled row prediction uses actual row values", func(t *testing.T) {
		sctx := mock.NewContext()
		intType := types.NewFieldType(mysql.TypeLonglong)
		intType.AddFlag(mysql.NotNullFlag)
		stringType := types.NewFieldType(mysql.TypeVarString)
		stringType.SetCharset(charset.CharsetUTF8MB4)
		stringType.SetCollate("utf8mb4_general_ci")
		tblInfo := &model.TableInfo{
			ID:    200,
			State: model.StatePublic,
			Columns: []*model.ColumnInfo{
				{ID: 1, Offset: 0, State: model.StatePublic, FieldType: *intType},
				{ID: 2, Offset: 1, State: model.StatePublic, FieldType: *stringType},
			},
			Indices: []*model.IndexInfo{
				{
					ID:    20,
					State: model.StatePublic,
					Columns: []*model.IndexColumn{
						{Name: ast.NewCIStr("k"), Offset: 0, Length: types.UnspecifiedLength},
						{Name: ast.NewCIStr("pad"), Offset: 1, Length: 32},
					},
				},
			},
		}
		physicalTbl := tables.MockTableFromMeta(tblInfo).(table.PhysicalTable)
		idxInfo := tblInfo.Indices[0]
		statsColl := statistics.NewHistColl(1, 4_000_000, 0, 3, 0)
		statsColl.SetCol(1, &statistics.Column{
			Info:      tblInfo.Columns[0],
			Histogram: *statistics.NewHistogram(1, 0, 0, 0, intType, 0, 32_000_000),
		})
		statsColl.SetCol(2, &statistics.Column{
			Info:      tblInfo.Columns[1],
			Histogram: *statistics.NewHistogram(2, 0, 0, 0, stringType, 0, 128_000_000),
		})
		statsTbl := &statistics.Table{HistColl: *statsColl}

		representBytes, err := estimateIndexKVBytesPerRowForRepresentPrediction(sctx, tblInfo, idxInfo, tblInfo.ID, statsTbl)
		require.NoError(t, err)

		row := []types.Datum{
			types.NewIntDatum(1),
			types.NewCollationStringDatum(strings.Repeat("界", 32), "utf8mb4_general_ci"),
		}
		sampledBytes, err := estimateIndexKVBytesForSampledRow(sctx, physicalTbl, physicalTbl.Indices(), row, kv.IntHandle(1))
		require.NoError(t, err)
		require.Greater(t, float64(sampledBytes), representBytes)
	})

	t.Run("sample prediction region selection caps at five", func(t *testing.T) {
		regions := make([]samplePredictionRegion, 0, 8)
		for i := 0; i < 8; i++ {
			regions = append(regions, samplePredictionRegion{
				StartKey: kv.Key{byte(i)},
				EndKey:   kv.Key{byte(i + 1)},
			})
		}
		selected := pickSamplePredictionRegions(regions, 12345)
		require.Len(t, selected, samplePredictionMaxRegionCount)
	})

	t.Run("sample prediction splits all-encoding and new-encoding estimates", func(t *testing.T) {
		sctx := mock.NewContext()
		intType := types.NewFieldType(mysql.TypeLonglong)
		intType.AddFlag(mysql.NotNullFlag)
		binStringType := types.NewFieldType(mysql.TypeVarString)
		binStringType.SetCharset(charset.CharsetUTF8MB4)
		binStringType.SetCollate(charset.CollationBin)
		restoredStringType := types.NewFieldType(mysql.TypeVarString)
		restoredStringType.SetCharset(charset.CharsetUTF8MB4)
		restoredStringType.SetCollate("utf8mb4_general_ci")

		numericTblInfo := &model.TableInfo{
			ID:    300,
			State: model.StatePublic,
			Columns: []*model.ColumnInfo{
				{ID: 1, Offset: 0, State: model.StatePublic, FieldType: *intType},
				{ID: 2, Offset: 1, State: model.StatePublic, FieldType: *intType},
				{ID: 3, Offset: 2, State: model.StatePublic, FieldType: *binStringType},
			},
			Indices: []*model.IndexInfo{
				{
					ID:    30,
					State: model.StatePublic,
					Columns: []*model.IndexColumn{
						{Name: ast.NewCIStr("k1"), Offset: 0, Length: types.UnspecifiedLength},
						{Name: ast.NewCIStr("k2"), Offset: 1, Length: types.UnspecifiedLength},
					},
				},
			},
		}
		numericTbl := tables.MockTableFromMeta(numericTblInfo).(table.PhysicalTable)
		numericIdx := numericTbl.Indices()
		var numericKVs []sampledIndexKV
		var numericLogicalBytes int64
		for i := 0; i < 1024; i++ {
			row := []types.Datum{
				types.NewIntDatum(int64(i + 1)),
				types.NewIntDatum(int64((i + 1) * 10)),
				types.NewCollationStringDatum(strings.Repeat("x", 32), charset.CollationBin),
			}
			rowKVs, rowBytes, err := collectIndexKVsForSampledRow(sctx, numericTbl, numericIdx, row, kv.IntHandle(i+1))
			require.NoError(t, err)
			numericKVs = append(numericKVs, rowKVs...)
			numericLogicalBytes += rowBytes
		}
		numericPrediction := estimateSampledIndexKVPredictionBytes(numericKVs)
		require.NoError(t, numericPrediction.AllEncodingErr)
		require.NoError(t, numericPrediction.NewEncodingErr)
		require.Greater(t, numericLogicalBytes, numericPrediction.AllEncodingBytes)
		require.Equal(t, numericLogicalBytes, numericPrediction.NewEncodingBytes)
		numericRatio := float64(numericPrediction.AllEncodingBytes) / float64(numericLogicalBytes)
		require.Less(t, numericRatio, float64(0.6))

		restoredTblInfo := &model.TableInfo{
			ID:    301,
			State: model.StatePublic,
			Columns: []*model.ColumnInfo{
				{ID: 1, Offset: 0, State: model.StatePublic, FieldType: *intType},
				{ID: 2, Offset: 1, State: model.StatePublic, FieldType: *restoredStringType},
			},
			Indices: []*model.IndexInfo{
				{
					ID:    31,
					State: model.StatePublic,
					Columns: []*model.IndexColumn{
						{Name: ast.NewCIStr("k"), Offset: 0, Length: types.UnspecifiedLength},
						{Name: ast.NewCIStr("pad"), Offset: 1, Length: 32},
					},
				},
			},
		}
		restoredTbl := tables.MockTableFromMeta(restoredTblInfo).(table.PhysicalTable)
		restoredIdx := restoredTbl.Indices()
		var restoredKVs []sampledIndexKV
		var restoredLogicalBytes int64
		for i := 0; i < 1024; i++ {
			row := []types.Datum{
				types.NewIntDatum(int64(((i + 1) * 17) % 1000003)),
				types.NewCollationStringDatum(
					fmt.Sprintf("payload-%09d-%s", i+1, strings.Repeat("x", 96)),
					"utf8mb4_general_ci",
				),
			}
			rowKVs, rowBytes, err := collectIndexKVsForSampledRow(sctx, restoredTbl, restoredIdx, row, kv.IntHandle(i+1))
			require.NoError(t, err)
			restoredKVs = append(restoredKVs, rowKVs...)
			restoredLogicalBytes += rowBytes
		}
		restoredPrediction := estimateSampledIndexKVPredictionBytes(restoredKVs)
		require.NoError(t, restoredPrediction.AllEncodingErr)
		require.NoError(t, restoredPrediction.NewEncodingErr)
		require.Greater(t, restoredLogicalBytes, restoredPrediction.AllEncodingBytes)
		require.Equal(t, restoredPrediction.AllEncodingBytes, restoredPrediction.NewEncodingBytes)
		restoredRatio := float64(restoredPrediction.AllEncodingBytes) / float64(restoredLogicalBytes)
		require.Less(t, restoredRatio, float64(1))

		mixedKVs := append(slices.Clone(numericKVs), restoredKVs...)
		mixedPrediction := estimateSampledIndexKVPredictionBytes(mixedKVs)
		require.NoError(t, mixedPrediction.AllEncodingErr)
		require.NoError(t, mixedPrediction.NewEncodingErr)
		require.Equal(t, numericLogicalBytes+restoredPrediction.NewEncodingBytes, mixedPrediction.NewEncodingBytes)
		require.Less(t, mixedPrediction.AllEncodingBytes, mixedPrediction.NewEncodingBytes)
		require.Less(t, mixedPrediction.NewEncodingBytes, numericLogicalBytes+restoredLogicalBytes)
	})
}

func TestSumTiKVStoreUsageError(t *testing.T) {
	stores := []pdhttp.StoreInfo{
		{
			Store:  pdhttp.MetaStore{ID: 7},
			Status: pdhttp.StoreStatus{Capacity: "broken", Available: "1KiB"},
		},
	}
	t.Run("usage snapshot", func(t *testing.T) {
		usage, err := sumTiKVStoreUsage(stores)
		require.Nil(t, usage)
		require.ErrorContains(t, err, "parse store 7 capacity")
	})
	t.Run("capacity snapshot", func(t *testing.T) {
		capacity, err := sumTiKVStoreCapacity(stores)
		require.Nil(t, capacity)
		require.ErrorContains(t, err, "parse store 7 capacity")
	})
}
