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
	"testing"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/stretchr/testify/require"
)

func TestStorageClassTransitionOperationGrouping(t *testing.T) {
	tblInfo := &model.TableInfo{ID: 10, DBID: 2, Name: ast.NewCIStr("orders")}
	operations := make(map[storageClassTransitionKey]*storageClassTransitionOperation)

	key, err := addStorageClassTransitionTarget(
		operations, "test", "orders", tblInfo, "p0", 11, 11, model.StorageClassTierIA, 1234)
	require.NoError(t, err)
	op := operations[key]
	require.Equal(t, storageClassDirectionToIA, op.Direction)
	require.Equal(t, int64(11), op.PartitionID)
	require.Equal(t, "p0", op.PartitionName)

	_, err = addStorageClassTransitionTarget(
		operations, "test", "orders", tblInfo, "p1", 12, 12, model.StorageClassTierIA, 1234)
	require.NoError(t, err)
	require.Zero(t, op.PartitionID)
	require.Empty(t, op.PartitionName)
	require.Len(t, op.targets, 2)

	standardKey, err := addStorageClassTransitionTarget(
		operations, "test", "orders", tblInfo, "p2", 13, 13, model.StorageClassTierStandard, 1234)
	require.NoError(t, err)
	require.NotEqual(t, key, standardKey)
	require.Equal(t, storageClassDirectionToStandard, operations[standardKey].Direction)
}

func TestStorageClassTransitionCompletesOnOneFullObservation(t *testing.T) {
	operation := &storageClassTransitionOperation{}
	require.False(t, updateStorageClassTransitionProgress(operation, 0, 0))
	require.False(t, operation.ProgressValid)

	require.False(t, updateStorageClassTransitionProgress(operation, 1, 2))
	require.True(t, operation.ProgressValid)
	require.Equal(t, 0.5, operation.Progress)

	require.True(t, updateStorageClassTransitionProgress(operation, 2, 2))
	require.Equal(t, 1.0, operation.Progress)
}

func TestStorageClassTransitionPollDecisions(t *testing.T) {
	require.False(t, storageClassTransitionNeedsSession(0, 0, false))
	require.True(t, storageClassTransitionNeedsSession(1, 0, false))
	require.True(t, storageClassTransitionNeedsSession(0, 1, false))
	require.True(t, storageClassTransitionNeedsSession(0, 0, true))

	require.False(t, storageClassTransitionNeedsHistoryPrune(0, false, false))
	require.True(t, storageClassTransitionNeedsHistoryPrune(0, true, false))
	require.True(t, storageClassTransitionNeedsHistoryPrune(0, false, true))
	require.False(t, storageClassTransitionNeedsHistoryPrune(1, true, true))
}

func TestSupersededStorageClassTransitionKeepsLastObservation(t *testing.T) {
	start := model.TSConvert2Time(1234)
	key := storageClassTransitionKey{tableID: 10, target: model.StorageClassTierIA, startTS: 1234}
	manager := &storageClassTransitionManager{}
	manager.mu.observed = map[storageClassTransitionKey]StorageClassTransition{
		key: {
			TableID:           10,
			Direction:         storageClassDirectionToIA,
			TotalReplicas:     4,
			CompletedReplicas: 3,
			Progress:          0.75,
			ProgressValid:     true,
			StatusValid:       true,
			StartTime:         start,
			PhysicalTableIDs:  []int64{11, 12},
			startTS:           1234,
		},
	}
	operation := &storageClassTransitionOperation{
		StorageClassTransition: StorageClassTransition{
			TableID:          10,
			Direction:        storageClassDirectionToIA,
			State:            model.StorageClassTransitionStateSuperseded,
			StartTime:        start,
			PhysicalTableIDs: []int64{11, 12},
			startTS:          1234,
		},
		targets: []storageClassTransitionTarget{{target: model.StorageClassTierIA}},
	}

	manager.mergeObserved(operation)

	require.True(t, operation.StatusValid)
	require.Equal(t, uint64(4), operation.TotalReplicas)
	require.Equal(t, uint64(3), operation.CompletedReplicas)
	require.Equal(t, 0.75, operation.Progress)
}

func TestStorageClassTransitionSupersedesWholeOperation(t *testing.T) {
	tblInfo := &model.TableInfo{
		ID:               10,
		Name:             ast.NewCIStr("orders"),
		StorageClassTier: model.StorageClassTierIA,
		Partition: &model.PartitionInfo{Definitions: []model.PartitionDefinition{
			{
				ID: 11, Name: ast.NewCIStr("p0"), StorageClassTier: model.StorageClassTierIA,
				StorageClassTransitionTarget: model.StorageClassTierIA, StorageClassTransitionStartTS: 1234,
				StorageClassTransitionSchemaName: "test", StorageClassTransitionTableName: "orders",
				StorageClassTransitionPartitionName: "p0",
			},
			{
				ID: 12, Name: ast.NewCIStr("p1"), StorageClassTier: model.StorageClassTierIA,
				StorageClassTransitionTarget: model.StorageClassTierIA, StorageClassTransitionStartTS: 1234,
				StorageClassTransitionSchemaName: "test", StorageClassTransitionTableName: "orders",
				StorageClassTransitionPartitionName: "p1",
			},
		}},
	}
	old := snapshotStorageClassTransitionState(tblInfo)
	tblInfo.Partition.Definitions[0].StorageClassTier = model.StorageClassTierStandard

	updateStorageClassTransitionMarkers(tblInfo, old, 5678, "test", "orders")

	require.Len(t, tblInfo.StorageClassTransitionPendingHistory, 1)
	history := tblInfo.StorageClassTransitionPendingHistory[0]
	require.Equal(t, model.StorageClassTransitionStateSuperseded, history.State)
	require.Equal(t, uint64(1234), history.StartTS)
	require.Equal(t, uint64(5678), history.FinishTS)
	require.Equal(t, []int64{11, 12}, []int64{history.Targets[0].PhysicalID, history.Targets[1].PhysicalID})

	p0 := tblInfo.Partition.Definitions[0]
	p1 := tblInfo.Partition.Definitions[1]
	require.Equal(t, model.StorageClassTierStandard, p0.StorageClassTransitionTarget)
	require.Equal(t, model.StorageClassTierIA, p1.StorageClassTransitionTarget)
	require.Equal(t, uint64(5678), p0.StorageClassTransitionStartTS)
	require.Equal(t, uint64(5678), p1.StorageClassTransitionStartTS)
}

func TestStorageClassTransitionTracksPartitionedTableParent(t *testing.T) {
	tblInfo := &model.TableInfo{
		ID:   10,
		Name: ast.NewCIStr("orders"),
		Partition: &model.PartitionInfo{Definitions: []model.PartitionDefinition{
			{ID: 11, Name: ast.NewCIStr("p0")},
			{ID: 12, Name: ast.NewCIStr("p1")},
		}},
	}
	old := snapshotStorageClassTransitionState(tblInfo)
	tblInfo.StorageClassTier = model.StorageClassTierIA
	for i := range tblInfo.Partition.Definitions {
		tblInfo.Partition.Definitions[i].StorageClassTier = model.StorageClassTierIA
	}

	updateStorageClassTransitionMarkers(tblInfo, old, 1234, "test", "orders")

	require.Equal(t, uint64(1234), tblInfo.StorageClassTransitionStartTS)
	require.Equal(t, model.StorageClassTierIA, tblInfo.StorageClassTransitionTarget)
	for _, partition := range tblInfo.Partition.Definitions {
		require.Equal(t, uint64(1234), partition.StorageClassTransitionStartTS)
		require.Equal(t, model.StorageClassTierIA, partition.StorageClassTransitionTarget)
	}
}

func TestFinalizeStorageClassTransitionRejectsStaleObservation(t *testing.T) {
	tblInfo := &model.TableInfo{
		ID:                               10,
		Name:                             ast.NewCIStr("orders"),
		StorageClassTier:                 model.StorageClassTierStandard,
		StorageClassTransitionTarget:     model.StorageClassTierStandard,
		StorageClassTransitionStartTS:    5678,
		StorageClassTransitionSchemaName: "test",
		StorageClassTransitionTableName:  "orders",
	}
	args := &model.FinishStorageClassTransitionArgs{
		Action:            model.StorageClassTransitionActionFinalize,
		Target:            model.StorageClassTierIA,
		StartTS:           1234,
		FinishTS:          6000,
		TotalReplicas:     3,
		CompletedReplicas: 3,
	}

	staleKey := storageClassTransitionKey{tableID: 10, target: model.StorageClassTierIA, startTS: 1234}
	require.False(t, finalizeStorageClassTransition(tblInfo, staleKey, args))
	require.Equal(t, uint64(5678), tblInfo.StorageClassTransitionStartTS)
	require.Equal(t, model.StorageClassTierStandard, tblInfo.StorageClassTransitionTarget)
	require.Empty(t, tblInfo.StorageClassTransitionPendingHistory)

	args.Target = model.StorageClassTierStandard
	args.StartTS = 5678
	currentKey := storageClassTransitionKey{tableID: 10, target: model.StorageClassTierStandard, startTS: 5678}
	require.True(t, finalizeStorageClassTransition(tblInfo, currentKey, args))
	require.Zero(t, tblInfo.StorageClassTransitionStartTS)
	require.Len(t, tblInfo.StorageClassTransitionPendingHistory, 1)
	history := tblInfo.StorageClassTransitionPendingHistory[0]
	require.Equal(t, model.StorageClassTransitionStateCompleted, history.State)
	require.Equal(t, uint64(3), history.TotalReplicas)
	require.Equal(t, uint64(3), history.CompletedReplicas)
	require.True(t, history.StatusValid)

	require.True(t, cleanupPendingStorageClassTransitionHistory(tblInfo, currentKey))
	require.Empty(t, tblInfo.StorageClassTransitionPendingHistory)
}
