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
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/stretchr/testify/require"
	pd "github.com/tikv/pd/client"
	pdhttp "github.com/tikv/pd/client/http"
)

func TestShouldWaitTiFlashPlacementBeforeScatter(t *testing.T) {
	tbInfo := &model.TableInfo{}
	require.False(t, shouldWaitTiFlashPlacementBeforeScatter(tbInfo, vardef.ScatterTable))

	tbInfo.TiFlashReplica = &model.TiFlashReplicaInfo{}
	require.False(t, shouldWaitTiFlashPlacementBeforeScatter(tbInfo, vardef.ScatterTable))

	tbInfo.TiFlashReplica.Count = 1
	require.False(t, shouldWaitTiFlashPlacementBeforeScatter(tbInfo, vardef.ScatterOff))
	require.True(t, shouldWaitTiFlashPlacementBeforeScatter(tbInfo, vardef.ScatterTable))
	require.True(t, shouldWaitTiFlashPlacementBeforeScatter(tbInfo, vardef.ScatterGlobal))
}

func TestHasPDHTTPClientForPlacementCheck(t *testing.T) {
	require.False(t, hasPDHTTPClientForPlacementCheck(mockSplittableStore{}))
	require.False(t, hasPDHTTPClientForPlacementCheck(mockSplittableStoreWithPD{}))
	require.True(t, hasPDHTTPClientForPlacementCheck(mockSplittableStoreWithPD{pdHTTPClient: mockPlacementPDHTTPClient{}}))
}

func TestWaitTiFlashPlacementBeforeScatterWaitsUntilScheduled(t *testing.T) {
	tbInfo := &model.TableInfo{Name: ast.NewCIStr("t")}
	physicalID := int64(123)
	expectedStartKey, expectedEndKey := tablePlacementRange(physicalID)

	var calls atomic.Int32
	waitTiFlashPlacementBeforeScatterWithCheck(
		context.Background(),
		tbInfo,
		0,
		func(_ context.Context, startKey []byte, endKey []byte) (infosync.PlacementScheduleState, error) {
			require.Equal(t, expectedStartKey, startKey)
			require.Equal(t, expectedEndKey, endKey)
			if calls.Add(1) == 1 {
				return infosync.PlacementScheduleStatePending, nil
			}
			return infosync.PlacementScheduleStateScheduled, nil
		},
		physicalID,
	)
	require.Equal(t, int32(2), calls.Load())
}

func TestWaitTiFlashPlacementBeforeScatterRetriesGetReplicationStateError(t *testing.T) {
	tbInfo := &model.TableInfo{Name: ast.NewCIStr("t")}

	var calls atomic.Int32
	waitTiFlashPlacementBeforeScatterWithCheck(
		context.Background(),
		tbInfo,
		0,
		func(context.Context, []byte, []byte) (infosync.PlacementScheduleState, error) {
			if calls.Add(1) == 1 {
				return infosync.PlacementScheduleStatePending, errors.New("temporary failure")
			}
			return infosync.PlacementScheduleStateScheduled, nil
		},
		123,
	)
	require.Equal(t, int32(2), calls.Load())
}

func TestWaitTiFlashPlacementBeforeScatterReturnsOnContextDone(t *testing.T) {
	tbInfo := &model.TableInfo{Name: ast.NewCIStr("t")}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var calls atomic.Int32
	start := time.Now()
	waitTiFlashPlacementBeforeScatterWithCheck(
		ctx,
		tbInfo,
		time.Hour,
		func(context.Context, []byte, []byte) (infosync.PlacementScheduleState, error) {
			calls.Add(1)
			return infosync.PlacementScheduleStatePending, nil
		},
		123,
	)
	require.Equal(t, int32(1), calls.Load())
	require.Less(t, time.Since(start), time.Second)
}

type mockSplittableStore struct{}

func (mockSplittableStore) SplitRegions(context.Context, [][]byte, bool, *int64) ([]uint64, error) {
	return nil, nil
}

func (mockSplittableStore) WaitScatterRegionFinish(context.Context, uint64, int) error {
	return nil
}

func (mockSplittableStore) CheckRegionInScattering(uint64) (bool, error) {
	return false, nil
}

type mockSplittableStoreWithPD struct {
	mockSplittableStore
	pdHTTPClient pdhttp.Client
}

func (mockSplittableStoreWithPD) GetPDClient() pd.Client {
	return nil
}

func (s mockSplittableStoreWithPD) GetPDHTTPClient() pdhttp.Client {
	return s.pdHTTPClient
}

type mockPlacementPDHTTPClient struct {
	pdhttp.Client
}

var (
	_ kv.SplittableStore = mockSplittableStore{}
	_ kv.SplittableStore = mockSplittableStoreWithPD{}
	_ kv.StorageWithPD   = mockSplittableStoreWithPD{}
	_ pdhttp.Client      = mockPlacementPDHTTPClient{}
)
