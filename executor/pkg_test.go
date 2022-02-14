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

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/mock"
	"github.com/stretchr/testify/require"
)

func TestNestedLoopApply(t *testing.T) {
	ctx := context.Background()
	sctx := mock.NewContext()
	col0 := &expression.Column{Index: 0, RetType: types.NewFieldType(mysql.TypeLong)}
	col1 := &expression.Column{Index: 1, RetType: types.NewFieldType(mysql.TypeLong)}
	con := &expression.Constant{Value: types.NewDatum(6), RetType: types.NewFieldType(mysql.TypeLong)}
	outerSchema := expression.NewSchema(col0)
	outerExec := buildMockDataSource(mockDataSourceParameters{
		schema: outerSchema,
		rows:   6,
		ctx:    sctx,
		genDataFunc: func(row int, typ *types.FieldType) interface{} {
			return int64(row + 1)
		},
	})
	outerExec.prepareChunks()

	innerSchema := expression.NewSchema(col1)
	innerExec := buildMockDataSource(mockDataSourceParameters{
		schema: innerSchema,
		rows:   6,
		ctx:    sctx,
		genDataFunc: func(row int, typ *types.FieldType) interface{} {
			return int64(row + 1)
		},
	})
	innerExec.prepareChunks()

	outerFilter := expression.NewFunctionInternal(sctx, ast.LT, types.NewFieldType(mysql.TypeTiny), col0, con)
	innerFilter := outerFilter.Clone()
	otherFilter := expression.NewFunctionInternal(sctx, ast.EQ, types.NewFieldType(mysql.TypeTiny), col0, col1)
	joiner := newJoiner(sctx, plannercore.InnerJoin, false,
		make([]types.Datum, innerExec.Schema().Len()), []expression.Expression{otherFilter},
		retTypes(outerExec), retTypes(innerExec), nil)
	joinSchema := expression.NewSchema(col0, col1)
	join := &NestedLoopApplyExec{
		baseExecutor: newBaseExecutor(sctx, joinSchema, 0),
		outerExec:    outerExec,
		innerExec:    innerExec,
		outerFilter:  []expression.Expression{outerFilter},
		innerFilter:  []expression.Expression{innerFilter},
		joiner:       joiner,
		ctx:          sctx,
	}
	join.innerList = chunk.NewList(retTypes(innerExec), innerExec.initCap, innerExec.maxChunkSize)
	join.innerChunk = newFirstChunk(innerExec)
	join.outerChunk = newFirstChunk(outerExec)
	joinChk := newFirstChunk(join)
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
