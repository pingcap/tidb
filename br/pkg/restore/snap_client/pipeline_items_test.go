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
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/errors"
	snapclient "github.com/pingcap/tidb/br/pkg/restore/snap_client"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/stretchr/testify/require"
)

func generateMockCreatedTables(tableCount int) []*snapclient.CreatedTable {
	createdTables := make([]*snapclient.CreatedTable, 0, tableCount)
	for i := 1; i <= 100; i += 1 {
		createdTables = append(createdTables, &snapclient.CreatedTable{
			Table: &model.TableInfo{ID: int64(i)},
		})
	}
	return createdTables
}

func TestPipelineConcurrentHandler1(t *testing.T) {
	handlerBuilder := &snapclient.PipelineConcurrentBuilder{}

	handlerBuilder.RegisterPipelineTask("task1", 4, func(ctx context.Context, ct *snapclient.CreatedTable) error {
		ct.Table.ID += 10000
		return nil
	}, func(ctx context.Context) error {
		return nil
	})
	totalID := int64(0)
	handlerBuilder.RegisterPipelineTask("task2", 4, func(ctx context.Context, ct *snapclient.CreatedTable) error {
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
	handlerBuilder.RegisterPipelineTask("task1", 4, func(ctx context.Context, ct *snapclient.CreatedTable) error {
		atomic.AddInt64(&count1, 1)
		time.Sleep(time.Millisecond * 10)
		return nil
	}, func(ctx context.Context) error {
		return nil
	})
	concurrency := uint(4)
	handlerBuilder.RegisterPipelineTask("task2", concurrency, func(ctx context.Context, ct *snapclient.CreatedTable) error {
		atomic.AddInt64(&count2, 1)
		if ct.Table.ID > int64(concurrency) {
			return errors.Errorf("failed in task2")
		}
		return nil
	}, func(ctx context.Context) error {
		return nil
	})
	handlerBuilder.RegisterPipelineTask("task3", concurrency, func(ctx context.Context, ct *snapclient.CreatedTable) error {
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
