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
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
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

func TestWaitTiFlashPlacementBeforeScatterIfNeededSkipsWhenReplicationStateUnavailable(t *testing.T) {
	tests := []struct {
		name      string
		available bool
		err       error
	}{
		{
			name:      "client unavailable",
			available: false,
		},
		{
			name:      "availability check failed",
			available: false,
			err:       errors.New("replication state unavailable"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tbInfo := &model.TableInfo{
				Name:           ast.NewCIStr("t"),
				TiFlashReplica: &model.TiFlashReplicaInfo{Count: 1},
			}
			var calls atomic.Int32
			waitTiFlashPlacementBeforeScatterIfNeededWithCheck(
				mock.NewContext(),
				tbInfo,
				vardef.ScatterTable,
				func() (bool, error) {
					return tt.available, tt.err
				},
				func(context.Context, []byte, []byte) (infosync.PlacementScheduleState, error) {
					calls.Add(1)
					return infosync.PlacementScheduleStateScheduled, nil
				},
				123,
			)
			require.Equal(t, int32(0), calls.Load())
		})
	}
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

func TestWaitTiFlashPlacementBeforeScatterSkipsScheduledPhysicalIDs(t *testing.T) {
	tbInfo := &model.TableInfo{Name: ast.NewCIStr("t")}

	var calls atomic.Int32
	var scheduledIDCalls atomic.Int32
	waitTiFlashPlacementBeforeScatterWithCheck(
		context.Background(),
		tbInfo,
		0,
		func(_ context.Context, startKey []byte, _ []byte) (infosync.PlacementScheduleState, error) {
			call := calls.Add(1)
			expectedScheduledStartKey, _ := tablePlacementRange(1)
			if string(startKey) == string(expectedScheduledStartKey) {
				scheduledIDCalls.Add(1)
				return infosync.PlacementScheduleStateScheduled, nil
			}
			if call == 2 {
				return infosync.PlacementScheduleStatePending, nil
			}
			return infosync.PlacementScheduleStateScheduled, nil
		},
		1,
		2,
	)
	require.Equal(t, int32(3), calls.Load())
	require.Equal(t, int32(1), scheduledIDCalls.Load())
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
