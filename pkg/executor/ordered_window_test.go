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
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/executor/internal/testutil"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestBuildOrderedWindowExec(t *testing.T) {
	sctx := mock.NewContext()
	sctx.GetSessionVars().EnablePipelinedWindowExec = false

	colA := &expression.Column{Index: 0, UniqueID: 1, RetType: types.NewFieldType(mysql.TypeLong)}
	colB := &expression.Column{Index: 1, UniqueID: 2, RetType: types.NewFieldType(mysql.TypeLonglong)}
	childSchema := expression.NewSchema(colA, colB)
	colAData := []int64{1, 1, 2, 2}
	colBData := []int64{1, 2, 1, 2}
	childExec := testutil.BuildMockDataSource(testutil.MockDataSourceParameters{
		Ctx:        sctx,
		DataSchema: childSchema,
		Rows:       4,
		GenDataFunc: func(row int, typ *types.FieldType) any {
			if typ.GetType() == mysql.TypeLong {
				return colAData[row]
			}
			return colBData[row]
		},
	})
	childExec.PrepareChunks()

	windowFunc, err := aggregation.NewWindowFuncDesc(sctx.GetExprCtx(), ast.WindowFuncRowNumber, nil, false)
	require.NoError(t, err)
	windowSchema := childSchema.Clone()
	windowSchema.Append(&expression.Column{Index: 2, UniqueID: 3, RetType: types.NewFieldType(mysql.TypeLonglong)})
	windowPlan := physicalWindowForOrderedTest(sctx.GetPlanCtx(), windowSchema, colA, colB, windowFunc)

	orderedExec, err := BuildOrdered(sctx, windowPlan, childExec)
	require.NoError(t, err)
	require.IsType(t, &OrderedWindowExec{}, orderedExec)

	ctx := context.Background()
	require.NoError(t, orderedExec.Open(ctx))
	defer func() {
		require.NoError(t, orderedExec.Close())
	}()

	chk := exec.NewFirstChunk(orderedExec)
	rows := make([]string, 0, 4)
	for {
		require.NoError(t, orderedExec.Next(ctx, chk))
		if chk.NumRows() == 0 {
			break
		}
		for i := range chk.NumRows() {
			row := chk.GetRow(i)
			rows = append(rows, fmt.Sprintf("%d %d %d", row.GetInt64(0), row.GetInt64(1), row.GetInt64(2)))
		}
	}
	require.Equal(t, []string{"1 1 1", "1 2 2", "2 1 1", "2 2 2"}, rows)
}

func physicalWindowForOrderedTest(
	sctx base.PlanContext,
	schema *expression.Schema,
	partitionCol *expression.Column,
	orderCol *expression.Column,
	windowFunc *aggregation.WindowFuncDesc,
) *plannercore.PhysicalWindow {
	windowPlan := plannercore.PhysicalWindow{
		WindowFuncDescs: []*aggregation.WindowFuncDesc{windowFunc},
		PartitionBy:     []property.SortItem{{Col: partitionCol}},
		OrderBy:         []property.SortItem{{Col: orderCol}},
		Frame: &logicalop.WindowFrame{
			Type:  ast.Rows,
			Start: &logicalop.FrameBound{Type: ast.CurrentRow},
			End:   &logicalop.FrameBound{Type: ast.CurrentRow},
		},
	}.Init(sctx, nil, 0)
	windowPlan.SetSchema(schema)
	return windowPlan
}
