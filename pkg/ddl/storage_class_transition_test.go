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

func TestBuildStorageClassTransitionOperations(t *testing.T) {
	tblInfo := &model.TableInfo{
		ID:   10,
		Name: ast.NewCIStr("orders"),
		Partition: &model.PartitionInfo{Definitions: []model.PartitionDefinition{
			{ID: 11, Name: ast.NewCIStr("p0"), StorageClassTier: model.StorageClassTierIA},
			{ID: 12, Name: ast.NewCIStr("p1"), StorageClassTier: model.StorageClassTierIA},
			{ID: 13, Name: ast.NewCIStr("p2"), StorageClassTier: model.StorageClassTierStandard},
		}},
	}
	physicalIDs := map[int64]struct{}{11: {}, 12: {}, 13: {}}

	operations, err := buildStorageClassTransitionOperations(tblInfo, physicalIDs, 1234, "test", "orders")
	require.NoError(t, err)
	require.Len(t, operations, 2)

	byDirection := make(map[string]*storageClassTransitionOperation, len(operations))
	for _, operation := range operations {
		byDirection[operation.Direction] = operation
	}
	ia := byDirection[storageClassDirectionToIA]
	require.Equal(t, []int64{11, 12}, ia.PhysicalTableIDs)
	require.Zero(t, ia.PartitionID)
	require.Empty(t, ia.PartitionName)

	standard := byDirection[storageClassDirectionToStandard]
	require.Equal(t, []int64{13}, standard.PhysicalTableIDs)
	require.Equal(t, int64(13), standard.PartitionID)
	require.Equal(t, "p2", standard.PartitionName)
}

func TestStorageClassTransitionTracksPartitionedTableParent(t *testing.T) {
	tblInfo := &model.TableInfo{
		ID:               10,
		Name:             ast.NewCIStr("orders"),
		StorageClassTier: model.StorageClassTierIA,
		Partition: &model.PartitionInfo{Definitions: []model.PartitionDefinition{
			{ID: 11, Name: ast.NewCIStr("p0"), StorageClassTier: model.StorageClassTierIA},
			{ID: 12, Name: ast.NewCIStr("p1"), StorageClassTier: model.StorageClassTierIA},
		}},
	}
	physicalIDs := map[int64]struct{}{10: {}, 11: {}, 12: {}}

	operations, err := buildStorageClassTransitionOperations(tblInfo, physicalIDs, 1234, "test", "orders")
	require.NoError(t, err)
	require.Len(t, operations, 1)
	require.Equal(t, []int64{10, 11, 12}, operations[0].PhysicalTableIDs)
	require.Zero(t, operations[0].PartitionID)
	require.Empty(t, operations[0].PartitionName)
}

func TestChangedStorageClassPhysicalIDs(t *testing.T) {
	tblInfo := &model.TableInfo{
		ID: 10,
		Partition: &model.PartitionInfo{Definitions: []model.PartitionDefinition{
			{ID: 11, Name: ast.NewCIStr("p0"), StorageClassTier: model.StorageClassTierIA},
			{ID: 12, Name: ast.NewCIStr("p1"), StorageClassTier: model.StorageClassTierIA},
		}},
	}
	old := snapshotPhysicalStorageClasses(tblInfo)
	tblInfo.Partition.Definitions[0].StorageClassTier = model.StorageClassTierStandard

	changed := changedStorageClassPhysicalIDs(old, snapshotPhysicalStorageClasses(tblInfo))
	require.Equal(t, map[int64]struct{}{11: {}}, changed)
}

func TestAddCurrentStorageClassTransitionTargetsSkipsRemovedTargets(t *testing.T) {
	physicalIDs := map[int64]struct{}{13: {}}
	current := map[int64]physicalStorageClass{11: {}, 13: {}}
	addCurrentStorageClassTransitionTargets(physicalIDs, current, []storageClassTransitionTarget{
		{PhysicalID: 11},
		{PhysicalID: 12},
	})
	require.Equal(t, map[int64]struct{}{11: {}, 13: {}}, physicalIDs)
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

func TestStorageClassTransitionCacheKeepsLastObservation(t *testing.T) {
	key := storageClassTransitionKey{tableID: 10, direction: storageClassDirectionToIA, startTS: 1234}
	manager := &storageClassTransitionManager{}
	manager.mu.active = make(map[storageClassTransitionKey]StorageClassTransitionStatus)
	manager.mu.observed = map[storageClassTransitionKey]StorageClassTransitionStatus{
		key: {
			TableID:           10,
			Direction:         storageClassDirectionToIA,
			TotalReplicas:     4,
			CompletedReplicas: 3,
			Progress:          0.75,
			ProgressValid:     true,
			StatusValid:       true,
			StartTime:         model.TSConvert2Time(1234),
			PhysicalTableIDs:  []int64{11, 12},
			startTS:           1234,
		},
	}
	operation := &storageClassTransitionOperation{
		StorageClassTransitionStatus: StorageClassTransitionStatus{
			TableID:          10,
			Direction:        storageClassDirectionToIA,
			StartTime:        model.TSConvert2Time(1234),
			PhysicalTableIDs: []int64{11, 12},
			startTS:          1234,
		},
	}

	manager.setActive(map[storageClassTransitionKey]*storageClassTransitionOperation{key: operation})
	transition := manager.snapshot()[0]
	require.True(t, transition.StatusValid)
	require.Equal(t, uint64(4), transition.TotalReplicas)
	require.Equal(t, uint64(3), transition.CompletedReplicas)
	require.Equal(t, 0.75, transition.Progress)
}

func TestValidateStorageClassTransitionTargets(t *testing.T) {
	require.Error(t, validateStorageClassTransitionTargets(nil))
	require.Error(t, validateStorageClassTransitionTargets([]storageClassTransitionTarget{{PhysicalID: 0}}))
	require.Error(t, validateStorageClassTransitionTargets([]storageClassTransitionTarget{{PhysicalID: 1}, {PhysicalID: 1}}))
	require.NoError(t, validateStorageClassTransitionTargets([]storageClassTransitionTarget{{PhysicalID: 1}, {PhysicalID: 2}}))
}

func TestStorageClassTransitionTargetsExist(t *testing.T) {
	tblInfo := &model.TableInfo{
		ID: 10,
		Partition: &model.PartitionInfo{Definitions: []model.PartitionDefinition{
			{ID: 11, Name: ast.NewCIStr("p0")},
			{ID: 12, Name: ast.NewCIStr("p1")},
		}},
	}
	operation := &storageClassTransitionOperation{
		targets: []storageClassTransitionTarget{{PhysicalID: 11}, {PhysicalID: 12}},
	}
	require.True(t, storageClassTransitionTargetsExist(tblInfo, operation))

	tblInfo.Partition.Definitions = tblInfo.Partition.Definitions[:1]
	require.False(t, storageClassTransitionTargetsExist(tblInfo, operation))
}
