// Copyright 2023 PingCAP, Inc.
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

package mppcoordmanager

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/mpp"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/store/copr"
	"github.com/stretchr/testify/require"
)

type IdleCoordinator struct {
}

// Execute implements MppCoordinator interface function.
func (*IdleCoordinator) Execute(context.Context) (kv.Response, []kv.KeyRange, error) {
	return nil, nil, nil
}

// Next implements MppCoordinator interface function.
func (*IdleCoordinator) Next(context.Context) (kv.ResultSubset, error) {
	return nil, nil
}

// ReportStatus implements MppCoordinator interface function.
func (*IdleCoordinator) ReportStatus(kv.ReportStatusRequest) error {
	return nil
}

// Close implements MppCoordinator interface function.
func (*IdleCoordinator) Close() error {
	return nil
}

// IsClosed implements MppCoordinator interface function.
func (*IdleCoordinator) IsClosed() bool {
	return true
}

// GetNodeCnt implements MppCoordinator interface function.
func (*IdleCoordinator) GetNodeCnt() int {
	return 0
}

func TestReportStatus(t *testing.T) {
	manager := newMPPCoordinatorManger()
	// A missing Meta must be rejected even if a coordinator with zero IDs exists.
	require.NoError(t, manager.Register(CoordinatorUniqueID{}, &IdleCoordinator{}))
	t.Cleanup(func() { manager.Unregister(CoordinatorUniqueID{}) })

	emptyRequest := new(mpp.ReportTaskStatusRequest)
	require.NoError(t, emptyRequest.Unmarshal(nil))
	require.Nil(t, emptyRequest.Meta)
	resp := manager.ReportStatus(emptyRequest)
	require.NotNil(t, resp)
	require.NotNil(t, resp.Error)
	require.Equal(t, "missing task metadata", resp.Error.Msg)

	// Present metadata with zero IDs still follows the normal coordinator lookup.
	resp = manager.ReportStatus(&mpp.ReportTaskStatusRequest{Meta: &mpp.TaskMeta{}})
	require.Nil(t, resp.Error)

	meta := &mpp.TaskMeta{QueryTs: 1, LocalQueryId: 2, ServerId: 3, GatherId: 4, MppVersion: 3}
	resp = manager.ReportStatus(&mpp.ReportTaskStatusRequest{Meta: meta})
	require.NotNil(t, resp.Error)
	require.Equal(t, "MppCoordinator not exists", resp.Error.Msg)
	require.Equal(t, meta.MppVersion, resp.Error.MppVersion)

	coordID := CoordinatorUniqueID{
		MPPQueryID: kv.MPPQueryID{QueryTs: meta.QueryTs, LocalQueryID: meta.LocalQueryId, ServerID: meta.ServerId},
		GatherID:   meta.GatherId,
	}
	require.NoError(t, manager.Register(coordID, &IdleCoordinator{}))
	t.Cleanup(func() { manager.Unregister(coordID) })
	resp = manager.ReportStatus(&mpp.ReportTaskStatusRequest{Meta: meta})
	require.Nil(t, resp.Error)
}

func TestDetectAndDelete(t *testing.T) {
	startTs := uint64(time.Now().UnixNano())
	InstanceMPPCoordinatorManager.maxLifeTime = uint64(copr.TiFlashReadTimeoutUltraLong.Nanoseconds() + detectFrequency.Nanoseconds())

	// OverTime One with GatherID 1
	queryID1 := kv.MPPQueryID{QueryTs: startTs}
	uniqueID1 := CoordinatorUniqueID{MPPQueryID: queryID1, GatherID: 1}
	InstanceMPPCoordinatorManager.coordinatorMap[uniqueID1] = &IdleCoordinator{}

	// Not OverTime One with GatherID 2
	queryID2 := kv.MPPQueryID{QueryTs: startTs + InstanceMPPCoordinatorManager.maxLifeTime}
	uniqueID2 := CoordinatorUniqueID{MPPQueryID: queryID2, GatherID: 2}
	InstanceMPPCoordinatorManager.coordinatorMap[uniqueID2] = &IdleCoordinator{}

	// Not OverTime One with GatherID 3
	queryID3 := kv.MPPQueryID{QueryTs: startTs + uint64(detectFrequency.Nanoseconds())}
	uniqueID3 := CoordinatorUniqueID{MPPQueryID: queryID3, GatherID: 3}
	InstanceMPPCoordinatorManager.coordinatorMap[uniqueID3] = &IdleCoordinator{}

	// OverTime One with GatherID 4
	queryID4 := kv.MPPQueryID{QueryTs: startTs + uint64(time.Minute.Nanoseconds())}
	uniqueID4 := CoordinatorUniqueID{MPPQueryID: queryID4, GatherID: 4}
	InstanceMPPCoordinatorManager.coordinatorMap[uniqueID4] = &IdleCoordinator{}

	// OverTime One with GatherID 5
	queryID5 := kv.MPPQueryID{QueryTs: startTs + uint64(time.Second.Nanoseconds()*20)}
	uniqueID5 := CoordinatorUniqueID{MPPQueryID: queryID5, GatherID: 5}
	InstanceMPPCoordinatorManager.coordinatorMap[uniqueID5] = &IdleCoordinator{}

	InstanceMPPCoordinatorManager.detectAndDelete(startTs + InstanceMPPCoordinatorManager.maxLifeTime + uint64(time.Minute.Nanoseconds()*2))
	require.True(t, len(InstanceMPPCoordinatorManager.coordinatorMap) == 2)
	for id := range InstanceMPPCoordinatorManager.coordinatorMap {
		require.True(t, id.GatherID == 2 || id.GatherID == 3)
	}
}
