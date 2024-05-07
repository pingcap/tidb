// Copyright 2019 PingCAP, Inc.
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
	"math/rand"
	"testing"

	"github.com/pingcap/tidb/pkg/distsql"
	distsqlctx "github.com/pingcap/tidb/pkg/distsql/context"
	"github.com/pingcap/tidb/pkg/executor/internal/builder"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

type requiredRowsSelectResult struct {
	retTypes        []*types.FieldType
	totalRows       int
	count           int
	expectedRowsRet []int
	numNextCalled   int
}

func (r *requiredRowsSelectResult) NextRaw(context.Context) ([]byte, error) { return nil, nil }
func (r *requiredRowsSelectResult) Close() error                            { return nil }

func (r *requiredRowsSelectResult) Next(ctx context.Context, chk *chunk.Chunk) error {
	defer func() {
		if r.numNextCalled >= len(r.expectedRowsRet) {
			return
		}
		rowsRet := chk.NumRows()
		expected := r.expectedRowsRet[r.numNextCalled]
		if rowsRet != expected {
			panic(fmt.Sprintf("unexpected number of rows returned, obtain: %v, expected: %v", rowsRet, expected))
		}
		r.numNextCalled++
	}()
	chk.Reset()
	if r.count > r.totalRows {
		return nil
	}
	required := min(chk.RequiredRows(), r.totalRows-r.count)
	for i := 0; i < required; i++ {
		chk.AppendRow(r.genOneRow())
	}
	r.count += required
	return nil
}

func (r *requiredRowsSelectResult) genOneRow() chunk.Row {
	row := chunk.MutRowFromTypes(r.retTypes)
	for i := range r.retTypes {
		row.SetValue(i, r.genValue(r.retTypes[i]))
	}
	return row.ToRow()
}

func (r *requiredRowsSelectResult) genValue(valType *types.FieldType) any {
	switch valType.GetType() {
	case mysql.TypeLong, mysql.TypeLonglong:
		return int64(rand.Int())
	case mysql.TypeDouble:
		return rand.Float64()
	default:
		panic("not implement")
	}
}

type totalRowsContextKey struct{}

var totalRowsKey = totalRowsContextKey{}

type expectedRowsRetContextKey struct{}

var expectedRowsRetKey = expectedRowsRetContextKey{}

func mockDistsqlSelectCtxSet(totalRows int, expectedRowsRet []int) context.Context {
	ctx := context.Background()
	ctx = context.WithValue(ctx, totalRowsKey, totalRows)
	ctx = context.WithValue(ctx, expectedRowsRetKey, expectedRowsRet)
	return ctx
}

func mockDistsqlSelectCtxGet(ctx context.Context) (totalRows int, expectedRowsRet []int) {
	totalRows = ctx.Value(totalRowsKey).(int)
	expectedRowsRet = ctx.Value(expectedRowsRetKey).([]int)
	return
}

func mockSelectResult(ctx context.Context, dctx *distsqlctx.DistSQLContext, kvReq *kv.Request,
	fieldTypes []*types.FieldType, copPlanIDs []int) (distsql.SelectResult, error) {
	totalRows, expectedRowsRet := mockDistsqlSelectCtxGet(ctx)
	return &requiredRowsSelectResult{
		retTypes:        fieldTypes,
		totalRows:       totalRows,
		expectedRowsRet: expectedRowsRet,
	}, nil
}

func buildTableReader(sctx sessionctx.Context) exec.Executor {
	retTypes := []*types.FieldType{types.NewFieldType(mysql.TypeDouble), types.NewFieldType(mysql.TypeLonglong)}
	cols := make([]*expression.Column, len(retTypes))
	for i := range retTypes {
		cols[i] = &expression.Column{Index: i, RetType: retTypes[i]}
	}
	schema := expression.NewSchema(cols...)

	e := &TableReaderExecutor{
		BaseExecutorV2:             exec.NewBaseExecutorV2(sctx.GetSessionVars(), schema, 0),
		tableReaderExecutorContext: newTableReaderExecutorContext(sctx),
		table:                      &tables.TableCommon{},
		dagPB:                      buildMockDAGRequest(sctx),
		selectResultHook:           selectResultHook{mockSelectResult},
	}
	return e
}

func buildMockDAGRequest(sctx sessionctx.Context) *tipb.DAGRequest {
	req, err := builder.ConstructDAGReq(sctx, []base.PhysicalPlan{&core.PhysicalTableScan{
		Columns: []*model.ColumnInfo{},
		Table:   &model.TableInfo{ID: 12345, PKIsHandle: false},
		Desc:    false,
	}}, kv.TiKV)
	if err != nil {
		panic(err)
	}
	return req
}

func buildMockBaseExec(sctx sessionctx.Context) exec.BaseExecutor {
	retTypes := []*types.FieldType{types.NewFieldType(mysql.TypeDouble), types.NewFieldType(mysql.TypeLonglong)}
	cols := make([]*expression.Column, len(retTypes))
	for i := range retTypes {
		cols[i] = &expression.Column{Index: i, RetType: retTypes[i]}
	}
	schema := expression.NewSchema(cols...)
	baseExec := exec.NewBaseExecutor(sctx, schema, 0)
	return baseExec
}

func TestTableReaderRequiredRows(t *testing.T) {
	maxChunkSize := defaultCtx().GetSessionVars().MaxChunkSize
	testCases := []struct {
		totalRows      int
		requiredRows   []int
		expectedRows   []int
		expectedRowsDS []int
	}{
		{
			totalRows:      10,
			requiredRows:   []int{1, 5, 3, 10},
			expectedRows:   []int{1, 5, 3, 1},
			expectedRowsDS: []int{1, 5, 3, 1},
		},
		{
			totalRows:      maxChunkSize + 1,
			requiredRows:   []int{1, 5, 3, 10, maxChunkSize},
			expectedRows:   []int{1, 5, 3, 10, (maxChunkSize + 1) - 1 - 5 - 3 - 10},
			expectedRowsDS: []int{1, 5, 3, 10, (maxChunkSize + 1) - 1 - 5 - 3 - 10},
		},
		{
			totalRows:      3*maxChunkSize + 1,
			requiredRows:   []int{3, 10, maxChunkSize},
			expectedRows:   []int{3, 10, maxChunkSize},
			expectedRowsDS: []int{3, 10, maxChunkSize},
		},
	}
	for _, testCase := range testCases {
		sctx := defaultCtx()
		ctx := mockDistsqlSelectCtxSet(testCase.totalRows, testCase.expectedRowsDS)
		executor := buildTableReader(sctx)
		require.NoError(t, executor.Open(ctx))
		chk := exec.NewFirstChunk(executor)
		for i := range testCase.requiredRows {
			chk.SetRequiredRows(testCase.requiredRows[i], maxChunkSize)
			require.NoError(t, executor.Next(ctx, chk))
			require.Equal(t, testCase.expectedRows[i], chk.NumRows())
		}
		require.NoError(t, executor.Close())
	}
}

func buildIndexReader(sctx sessionctx.Context) exec.Executor {
	e := &IndexReaderExecutor{
		BaseExecutor:     buildMockBaseExec(sctx),
		dagPB:            buildMockDAGRequest(sctx),
		index:            &model.IndexInfo{},
		selectResultHook: selectResultHook{mockSelectResult},
	}
	return e
}

func TestIndexReaderRequiredRows(t *testing.T) {
	maxChunkSize := defaultCtx().GetSessionVars().MaxChunkSize
	testCases := []struct {
		totalRows      int
		requiredRows   []int
		expectedRows   []int
		expectedRowsDS []int
	}{
		{
			totalRows:      10,
			requiredRows:   []int{1, 5, 3, 10},
			expectedRows:   []int{1, 5, 3, 1},
			expectedRowsDS: []int{1, 5, 3, 1},
		},
		{
			totalRows:      maxChunkSize + 1,
			requiredRows:   []int{1, 5, 3, 10, maxChunkSize},
			expectedRows:   []int{1, 5, 3, 10, (maxChunkSize + 1) - 1 - 5 - 3 - 10},
			expectedRowsDS: []int{1, 5, 3, 10, (maxChunkSize + 1) - 1 - 5 - 3 - 10},
		},
		{
			totalRows:      3*maxChunkSize + 1,
			requiredRows:   []int{3, 10, maxChunkSize},
			expectedRows:   []int{3, 10, maxChunkSize},
			expectedRowsDS: []int{3, 10, maxChunkSize},
		},
	}
	for _, testCase := range testCases {
		sctx := defaultCtx()
		ctx := mockDistsqlSelectCtxSet(testCase.totalRows, testCase.expectedRowsDS)
		executor := buildIndexReader(sctx)
		require.NoError(t, executor.Open(ctx))
		chk := exec.NewFirstChunk(executor)
		for i := range testCase.requiredRows {
			chk.SetRequiredRows(testCase.requiredRows[i], maxChunkSize)
			require.NoError(t, executor.Next(ctx, chk))
			require.Equal(t, testCase.expectedRows[i], chk.NumRows())
		}
		require.NoError(t, executor.Close())
	}
}
