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

package executor

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/executor/join"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

type closeCountExecutor struct {
	exec.BaseExecutorV2
	closed *atomic.Int32
}

func (e *closeCountExecutor) Close() error {
	e.closed.Add(1)
	return e.BaseExecutorV2.Close()
}

func TestBuildExecutorForIndexJoinHashJoinErrorCleansChildren(t *testing.T) {
	ctx := mock.NewContext()

	stats := &property.StatsInfo{RowCount: 1}
	lookupSchema := expression.NewSchema(&expression.Column{
		UniqueID: 1,
		RetType:  types.NewFieldType(mysql.TypeLonglong),
		Index:    0,
	})
	lookupPlan := physicalop.PhysicalTableDual{RowCount: 1}.Init(ctx, stats, 0)
	lookupPlan.SetSchema(lookupSchema)

	var lookupExecClosed atomic.Int32
	lookupExec := &closeCountExecutor{
		BaseExecutorV2: exec.NewBaseExecutorV2(ctx.GetSessionVars(), lookupSchema, 1),
		closed:         &lookupExecClosed,
	}
	lookupMockPlan := &mockPhysicalIndexReader{PhysicalPlan: lookupPlan, e: lookupExec}

	otherSchema := expression.NewSchema(&expression.Column{
		UniqueID: 2,
		RetType:  types.NewFieldType(mysql.TypeLonglong),
		Index:    0,
	})
	otherPlan := physicalop.PhysicalTableDual{RowCount: 1}.Init(ctx, stats, 0)
	otherPlan.SetSchema(otherSchema)
	var otherExecClosed atomic.Int32
	otherExec := &closeCountExecutor{
		BaseExecutorV2: exec.NewBaseExecutorV2(ctx.GetSessionVars(), otherSchema, 2),
		closed:         &otherExecClosed,
	}
	otherMockPlan := &mockPhysicalIndexReader{PhysicalPlan: otherPlan, e: otherExec}

	hashJoinPlan := physicalop.PhysicalHashJoin{
		BasePhysicalJoin: physicalop.BasePhysicalJoin{JoinType: base.InnerJoin},
	}.Init(ctx, stats, 0)
	hashJoinPlan.SetSchema(expression.MergeSchema(lookupSchema, otherSchema))
	hashJoinPlan.SetChildren(lookupMockPlan, otherMockPlan)

	execBuilder := newExecutorBuilder(ctx, nil, nil)
	execBuilder.forDataReaderBuilder = true
	execBuilder.dataReaderTS = 1
	readerBuilder, err := execBuilder.newDataReaderBuilder(hashJoinPlan)
	require.NoError(t, err)
	readerBuilder.indexJoinKeyUniqueIDs = []int64{1}

	_, err = readerBuilder.BuildExecutorForIndexJoin(
		context.Background(),
		[]*join.IndexJoinLookUpContent{{}},
		nil,
		nil,
		nil,
		true,
		nil,
		nil,
	)
	require.ErrorContains(t, err, "Unknown Plan *executor.mockPhysicalIndexReader")
	require.Equal(t, int32(1), lookupExecClosed.Load())
	require.Equal(t, int32(0), otherExecClosed.Load())
}

func TestBuildCTEStorageProducerCleansStoragesOnRecursiveBuildError(t *testing.T) {
	ctx := mock.NewContext()
	ctx.GetSessionVars().StmtCtx.CTEStorageMap = map[int]*CTEStorages{}
	builder := newExecutorBuilder(ctx, nil, nil)

	stats := &property.StatsInfo{RowCount: 1}
	schema := expression.NewSchema(&expression.Column{
		UniqueID: 1,
		RetType:  types.NewFieldType(mysql.TypeLonglong),
		Index:    0,
	})
	seedPlan := physicalop.PhysicalTableDual{RowCount: 1}.Init(ctx, stats, 0)
	seedPlan.SetSchema(schema)

	ctePlan := physicalop.PhysicalCTE{
		SeedPlan:  seedPlan,
		RecurPlan: &mockPhysicalIndexReader{PhysicalPlan: seedPlan},
		CTE:       &logicalop.CTEClass{IDForStorage: 1},
	}.Init(ctx, stats)
	ctePlan.SetSchema(schema)

	storages, err := builder.loadOrStoreCTEStorages(1)
	require.NoError(t, err)

	err = builder.buildCTEStorageProducer(ctePlan, storages)
	require.ErrorContains(t, err, "Unknown Plan *executor.mockPhysicalIndexReader")
	require.Nil(t, storages.ResTbl)
	require.Nil(t, storages.IterInTbl)
	require.Nil(t, storages.Producer)

	require.NoError(t, resetCTEStorageMap(ctx))
	require.Nil(t, ctx.GetSessionVars().StmtCtx.CTEStorageMap)
}
