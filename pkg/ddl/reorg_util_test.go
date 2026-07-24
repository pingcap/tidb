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
	"testing"

	"github.com/docker/go-units"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
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
	return c.storesInfo, nil
}

func expectedRegionRange(tableID int64) ([]byte, []byte) {
	tableStart, tableEnd := tablecodec.GetTableHandleKeyRange(tableID)
	return mockCodec{}.EncodeRegionRange(tableStart, tableEnd)
}

func TestEstimateTableSizeByIDUsesMaxApproximateSizes(t *testing.T) {
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

func TestCollectTiKVStoreCapacityFromPDHTTP(t *testing.T) {
	pdCli := &mockPDHTTPClient{
		storesInfo: &pdhttp.StoresInfo{
			Stores: []pdhttp.StoreInfo{
				{
					Store:  pdhttp.MetaStore{ID: 1, State: 0},
					Status: pdhttp.StoreStatus{Capacity: "10 GiB", Available: "4 GiB"},
				},
				{
					Store: pdhttp.MetaStore{
						ID:     2,
						State:  0,
						Labels: []pdhttp.StoreLabel{{Key: "engine", Value: "tiflash"}},
					},
					Status: pdhttp.StoreStatus{Capacity: "20 GiB", Available: "8 GiB"},
				},
				{
					Store:  pdhttp.MetaStore{ID: 3, State: 2},
					Status: pdhttp.StoreStatus{Capacity: "30 GiB", Available: "12 GiB"},
				},
			},
		},
	}

	capacity, err := collectTiKVStoreCapacity(context.Background(), mockHelperStorage{pdCli: pdCli})
	require.NoError(t, err)
	require.EqualValues(t, 10*units.GiB, capacity.TotalBytes)
	require.EqualValues(t, 4*units.GiB, capacity.AvailableBytes)
	require.Equal(t, 1, capacity.StoreCount)
}

func TestIngestedSSTRecorder(t *testing.T) {
	recorder := newIngestedSSTRecorder()
	recorder.RecordIngestedSST("classic/uuid/default", 100)
	recorder.RecordIngestedSST("classic/uuid/default", 100)
	recorder.RecordIngestedSST("classic/uuid/write", 40)
	recorder.RecordIngestedSST("classic/zero/default", 0)
	recorder.RecordIngestedSST("", 10)
	observation := recorder.Snapshot()
	require.EqualValues(t, 140, observation.bytes)
	require.EqualValues(t, 3, observation.count)
	require.EqualValues(t, 1, observation.zeroSizeCount)
	require.EqualValues(t, 1, observation.invalidIdentityCount)
	require.False(t, observation.reliable)
	require.Equal(t, "ingested_sst_identity_unavailable", observation.reason)

	recorder.Reset()
	observation = recorder.Snapshot()
	require.Zero(t, observation.bytes)
	require.Zero(t, observation.count)
	require.Zero(t, observation.zeroSizeCount)
	require.Zero(t, observation.invalidIdentityCount)
	require.True(t, observation.reliable)
	require.Equal(t, "ok", observation.reason)

	recorder.RecordIngestedSST("classic/uuid/default", 100)
	observation = recorder.Snapshot()
	require.EqualValues(t, 100, observation.bytes)
	require.EqualValues(t, 1, observation.count)
	require.True(t, observation.reliable)
	require.Equal(t, "ok", observation.reason)

	recorder.Reset()
	recorder.RecordIngestedSST("classic/zero/default", 0)
	observation = recorder.Snapshot()
	require.False(t, observation.reliable)
	require.Equal(t, "ingested_sst_size_unavailable", observation.reason)
}
