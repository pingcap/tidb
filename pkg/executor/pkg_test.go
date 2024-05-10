// Copyright 2021 PingCAP, Inc.
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
	"github.com/pingcap/tidb/pkg/executor/join"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestNestedLoopApply(t *testing.T) {
	ctx := context.Background()
	sctx := mock.NewContext()
	col0 := &expression.Column{Index: 0, RetType: types.NewFieldType(mysql.TypeLong)}
	col1 := &expression.Column{Index: 1, RetType: types.NewFieldType(mysql.TypeLong)}
	con := &expression.Constant{Value: types.NewDatum(6), RetType: types.NewFieldType(mysql.TypeLong)}
	outerSchema := expression.NewSchema(col0)
	outerExec := testutil.BuildMockDataSource(testutil.MockDataSourceParameters{
		DataSchema: outerSchema,
		Rows:       6,
		Ctx:        sctx,
		GenDataFunc: func(row int, typ *types.FieldType) any {
			return int64(row + 1)
		},
	})
	outerExec.PrepareChunks()

	innerSchema := expression.NewSchema(col1)
	innerExec := testutil.BuildMockDataSource(testutil.MockDataSourceParameters{
		DataSchema: innerSchema,
		Rows:       6,
		Ctx:        sctx,
		GenDataFunc: func(row int, typ *types.FieldType) any {
			return int64(row + 1)
		},
	})
	innerExec.PrepareChunks()

	outerFilter := expression.NewFunctionInternal(sctx, ast.LT, types.NewFieldType(mysql.TypeTiny), col0, con)
	innerFilter := outerFilter.Clone()
	otherFilter := expression.NewFunctionInternal(sctx, ast.EQ, types.NewFieldType(mysql.TypeTiny), col0, col1)
	joiner := join.NewJoiner(sctx, plannercore.InnerJoin, false,
		make([]types.Datum, innerExec.Schema().Len()), []expression.Expression{otherFilter},
		exec.RetTypes(outerExec), exec.RetTypes(innerExec), nil, false)
	joinSchema := expression.NewSchema(col0, col1)
	join := &join.NestedLoopApplyExec{
		BaseExecutor: exec.NewBaseExecutor(sctx, joinSchema, 0),
		OuterExec:    outerExec,
		InnerExec:    innerExec,
		OuterFilter:  []expression.Expression{outerFilter},
		InnerFilter:  []expression.Expression{innerFilter},
		Joiner:       joiner,
		Sctx:         sctx,
	}
	join.InnerList = chunk.NewList(exec.RetTypes(innerExec), innerExec.InitCap(), innerExec.MaxChunkSize())
	join.InnerChunk = exec.NewFirstChunk(innerExec)
	join.OuterChunk = exec.NewFirstChunk(outerExec)
	joinChk := exec.NewFirstChunk(join)
	it := chunk.NewIterator4Chunk(joinChk)
	for rowIdx := 1; ; {
		err := join.Next(ctx, joinChk)
		require.NoError(t, err)
		if joinChk.NumRows() == 0 {
			break
		}
		for row := it.Begin(); row != it.End(); row = it.Next() {
			correctResult := fmt.Sprintf("%v %v", rowIdx, rowIdx)
			obtainedResult := fmt.Sprintf("%v %v", row.GetInt64(0), row.GetInt64(1))
			require.Equal(t, correctResult, obtainedResult)
			rowIdx++
		}
	}
}

func TestMoveInfoSchemaToFront(t *testing.T) {
	dbss := [][]string{
		{},
		{"A", "B", "C", "a", "b", "c"},
		{"A", "B", "C", "INFORMATION_SCHEMA"},
		{"A", "B", "INFORMATION_SCHEMA", "a"},
		{"INFORMATION_SCHEMA"},
		{"A", "B", "C", "INFORMATION_SCHEMA", "a", "b"},
	}
	wanted := [][]string{
		{},
		{"A", "B", "C", "a", "b", "c"},
		{"INFORMATION_SCHEMA", "A", "B", "C"},
		{"INFORMATION_SCHEMA", "A", "B", "a"},
		{"INFORMATION_SCHEMA"},
		{"INFORMATION_SCHEMA", "A", "B", "C", "a", "b"},
	}

	for _, dbs := range dbss {
		moveInfoSchemaToFront(dbs)
	}

	for i, dbs := range wanted {
		require.Equal(t, len(dbs), len(dbss[i]))
		for j, db := range dbs {
			require.Equal(t, db, dbss[i][j])
		}
	}
}
