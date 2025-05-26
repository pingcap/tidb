// Copyright 2024 PingCAP, Inc.
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

package snapclient_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/br/pkg/metautil"
	snapclient "github.com/pingcap/tidb/br/pkg/restore/snap_client"
	restoreutils "github.com/pingcap/tidb/br/pkg/restore/utils"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	statstypes "github.com/pingcap/tidb/pkg/statistics/handle/types"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func generateMockCreatedTables(tableCount int) []*restoreutils.CreatedTable {
	createdTables := make([]*restoreutils.CreatedTable, 0, tableCount)
	for i := 1; i <= 100; i += 1 {
		createdTables = append(createdTables, &restoreutils.CreatedTable{
			Table: &model.TableInfo{ID: int64(i)},
		})
	}
	return createdTables
}

func TestPipelineConcurrentHandler1(t *testing.T) {
	handlerBuilder := &snapclient.PipelineConcurrentBuilder{}

	handlerBuilder.RegisterPipelineTask("task1", 4, func(ctx context.Context, ct *restoreutils.CreatedTable) error {
		ct.Table.ID += 10000
		return nil
	}, func(ctx context.Context) error {
		return nil
	})
	totalID := int64(0)
	handlerBuilder.RegisterPipelineTask("task2", 4, func(ctx context.Context, ct *restoreutils.CreatedTable) error {
		atomic.AddInt64(&totalID, ct.Table.ID)
		return nil
	}, func(ctx context.Context) error {
		totalID += 100
		return nil
	})

	ctx := context.Background()
	require.NoError(t, handlerBuilder.StartPipelineTask(ctx, generateMockCreatedTables(100)))
	require.Equal(t, int64(1005150), totalID)
}

func TestPipelineConcurrentHandler2(t *testing.T) {
	handlerBuilder := &snapclient.PipelineConcurrentBuilder{}

	count1, count2, count3 := int64(0), int64(0), int64(0)
	handlerBuilder.RegisterPipelineTask("task1", 4, func(ctx context.Context, ct *restoreutils.CreatedTable) error {
		atomic.AddInt64(&count1, 1)
		time.Sleep(time.Millisecond * 10)
		return nil
	}, func(ctx context.Context) error {
		return nil
	})
	concurrency := uint(4)
	handlerBuilder.RegisterPipelineTask("task2", concurrency, func(ctx context.Context, ct *restoreutils.CreatedTable) error {
		atomic.AddInt64(&count2, 1)
		if ct.Table.ID > int64(concurrency) {
			return errors.Errorf("failed in task2")
		}
		return nil
	}, func(ctx context.Context) error {
		return nil
	})
	handlerBuilder.RegisterPipelineTask("task3", concurrency, func(ctx context.Context, ct *restoreutils.CreatedTable) error {
		atomic.AddInt64(&count3, 1)
		<-ctx.Done()
		return errors.Annotate(ctx.Err(), "failed in task3")
	}, func(ctx context.Context) error {
		return nil
	})

	ctx := context.Background()
	tableCount := 100
	err := handlerBuilder.StartPipelineTask(ctx, generateMockCreatedTables(tableCount))
	require.Error(t, err)
	t.Log(count1)
	t.Log(count2)
	t.Log(count3)
	require.Less(t, count1, int64(tableCount))
	require.LessOrEqual(t, int64(concurrency+1), count2)
	require.LessOrEqual(t, count2, int64(2*concurrency+1))
	require.LessOrEqual(t, count3, int64(concurrency))
}

type mockStatsReadWriter struct {
	statstypes.StatsReadWriter

	rows map[int64]int64
}

func (m *mockStatsReadWriter) SaveMetaToStorage(_ string, _ bool, metaUpdates ...statstypes.MetaUpdate) (err error) {
	for _, metaUpdate := range metaUpdates {
		m.rows[metaUpdate.PhysicalID] += metaUpdate.Count
	}
	return nil
}

func generateStatsPartition(partitionIDs []int64) (*model.PartitionInfo, *model.PartitionInfo) {
	if len(partitionIDs) == 0 {
		return nil, nil
	}
	downDefs := make([]model.PartitionDefinition, 0)
	upDefs := make([]model.PartitionDefinition, 0)
	for _, partitionID := range partitionIDs {
		downDefs = append(downDefs, model.PartitionDefinition{
			ID:   partitionID,
			Name: ast.NewCIStr(fmt.Sprintf("p%d", partitionID)),
		})
		upDefs = append(upDefs, model.PartitionDefinition{
			ID:   partitionID + 1000,
			Name: ast.NewCIStr(fmt.Sprintf("p%d", partitionID)),
		})
	}
	return &model.PartitionInfo{Definitions: downDefs}, &model.PartitionInfo{Definitions: upDefs}
}

func generateStatsFiles(tableID int64, partitionIDs []int64, hasGlobalIndex bool) map[int64][]*backuppb.File {
	recordKey := tablecodec.EncodeRecordKey(tablecodec.GenTableRecordPrefix(tableID+1000), kv.IntHandle(0))
	indexKey := tablecodec.EncodeTableIndexPrefix(tableID+1000, 1)
	if len(partitionIDs) == 0 {
		return map[int64][]*backuppb.File{
			tableID + 1000: {
				{StartKey: recordKey, TotalKvs: uint64(tableID)},
				{StartKey: indexKey, TotalKvs: uint64(tableID)},
				{StartKey: indexKey, TotalKvs: uint64(tableID)},
				{StartKey: recordKey, TotalKvs: uint64(tableID + 1)},
				{StartKey: indexKey, TotalKvs: uint64(tableID + 1)},
				{StartKey: indexKey, TotalKvs: uint64(tableID + 1)},
			},
		}
	}
	files := map[int64][]*backuppb.File{}
	if hasGlobalIndex {
		files[tableID+1000] = []*backuppb.File{{TotalKvs: uint64(tableID)}, {TotalKvs: uint64(tableID + 1)}}
		for _, partitionID := range partitionIDs {
			files[partitionID+1000] = []*backuppb.File{
				{StartKey: recordKey, TotalKvs: uint64(partitionID)},
				{StartKey: indexKey, TotalKvs: uint64(partitionID)},
				{StartKey: recordKey, TotalKvs: uint64(partitionID + 1)},
				{StartKey: indexKey, TotalKvs: uint64(partitionID + 1)},
			}
		}
	} else {
		for _, partitionID := range partitionIDs {
			files[partitionID+1000] = []*backuppb.File{
				{StartKey: recordKey, TotalKvs: uint64(partitionID)},
				{StartKey: indexKey, TotalKvs: uint64(partitionID)},
				{StartKey: indexKey, TotalKvs: uint64(partitionID)},
				{StartKey: recordKey, TotalKvs: uint64(partitionID + 1)},
				{StartKey: indexKey, TotalKvs: uint64(partitionID + 1)},
				{StartKey: indexKey, TotalKvs: uint64(partitionID + 1)},
			}
		}
	}
	return files
}

func generateStatsIndices(hasGlobalIndex bool) []*model.IndexInfo {
	return []*model.IndexInfo{
		{Global: hasGlobalIndex},
		{Global: false},
	}
}

func generateStatsCreatedTables(hasGlobalIndex bool, tableID int64, partitionIDs ...int64) *restoreutils.CreatedTable {
	downPart, upPart := generateStatsPartition(partitionIDs)
	indices := generateStatsIndices(hasGlobalIndex)
	files := generateStatsFiles(tableID, partitionIDs, hasGlobalIndex)
	return &restoreutils.CreatedTable{
		Table: &model.TableInfo{
			ID:        tableID,
			Partition: downPart,
			Indices:   indices,
		},
		OldTable: &metautil.Table{
			DB: &model.DBInfo{Name: ast.NewCIStr("test")},
			Info: &model.TableInfo{
				ID:        tableID + 1000,
				Partition: upPart,
				Indices:   indices,
			},
			FilesOfPhysicals: files,
		},
	}
}

func TestUpdateStatsMeta(t *testing.T) {
	ctx := context.Background()
	initStatsCtx := mock.NewContext()
	initStatsCtx.Store = &mock.Store{
		Client: &mock.Client{},
	}
	dom := domain.NewMockDomain()
	err := dom.CreateStatsHandle(ctx, initStatsCtx)
	require.NoError(t, err)
	defer func() {
		dom.StatsHandle().Close()
	}()
	rows := make(map[int64]int64)
	handler := dom.StatsHandle()
	handler.StatsReadWriter = &mockStatsReadWriter{rows: rows}
	client := snapclient.MockClient(nil)
	client.SetDomain(dom)
	builder := &snapclient.PipelineConcurrentBuilder{}
	client.RegisterUpdateMetaAndLoadStats(builder, nil, MockUpdateCh{}, 1)
	err = builder.StartPipelineTask(ctx, []*restoreutils.CreatedTable{
		generateStatsCreatedTables(false, 100, 101, 102, 103),
		generateStatsCreatedTables(true, 104, 105, 106, 107),
		generateStatsCreatedTables(false, 116),
		generateStatsCreatedTables(true, 117),
	})
	require.NoError(t, err)
	require.Equal(t, map[int64]int64{
		100: 615,
		101: 203,
		102: 205,
		103: 207,
		104: 639,
		105: 211,
		106: 213,
		107: 215,
		116: 233,
		117: 235,
	}, rows)
}
