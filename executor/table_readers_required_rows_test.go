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

	"github.com/pingcap/tidb/distsql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/mathutil"
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
	required := mathutil.Min(chk.RequiredRows(), r.totalRows-r.count)
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

func (r *requiredRowsSelectResult) genValue(valType *types.FieldType) interface{} {
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

func mockSelectResult(ctx context.Context, sctx sessionctx.Context, kvReq *kv.Request,
	fieldTypes []*types.FieldType, fb *statistics.QueryFeedback, copPlanIDs []int) (distsql.SelectResult, error) {
	totalRows, expectedRowsRet := mockDistsqlSelectCtxGet(ctx)
	return &requiredRowsSelectResult{
		retTypes:        fieldTypes,
		totalRows:       totalRows,
		expectedRowsRet: expectedRowsRet,
	}, nil
}

func buildTableReader(sctx sessionctx.Context) Executor {
	e := &TableReaderExecutor{
		baseExecutor:     buildMockBaseExec(sctx),
		table:            &tables.TableCommon{},
		dagPB:            buildMockDAGRequest(sctx),
		selectResultHook: selectResultHook{mockSelectResult},
	}
	return e
}

func buildMockDAGRequest(sctx sessionctx.Context) *tipb.DAGRequest {
	req, _, err := constructDAGReq(sctx, []core.PhysicalPlan{&core.PhysicalTableScan{
		Columns: []*model.ColumnInfo{},
		Table:   &model.TableInfo{ID: 12345, PKIsHandle: false},
		Desc:    false,
	}}, kv.TiKV)
	if err != nil {
		panic(err)
	}
	return req
}

func buildMockBaseExec(sctx sessionctx.Context) baseExecutor {
	retTypes := []*types.FieldType{types.NewFieldType(mysql.TypeDouble), types.NewFieldType(mysql.TypeLonglong)}
	cols := make([]*expression.Column, len(retTypes))
	for i := range retTypes {
		cols[i] = &expression.Column{Index: i, RetType: retTypes[i]}
	}
	schema := expression.NewSchema(cols...)
	baseExec := newBaseExecutor(sctx, schema, 0)
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
		exec := buildTableReader(sctx)
		require.NoError(t, exec.Open(ctx))
		chk := newFirstChunk(exec)
		for i := range testCase.requiredRows {
			chk.SetRequiredRows(testCase.requiredRows[i], maxChunkSize)
			require.NoError(t, exec.Next(ctx, chk))
			require.Equal(t, testCase.expectedRows[i], chk.NumRows())
		}
		require.NoError(t, exec.Close())
	}
}

func buildIndexReader(sctx sessionctx.Context) Executor {
	e := &IndexReaderExecutor{
		baseExecutor:     buildMockBaseExec(sctx),
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
		exec := buildIndexReader(sctx)
		require.NoError(t, exec.Open(ctx))
		chk := newFirstChunk(exec)
		for i := range testCase.requiredRows {
			chk.SetRequiredRows(testCase.requiredRows[i], maxChunkSize)
			require.NoError(t, exec.Next(ctx, chk))
			require.Equal(t, testCase.expectedRows[i], chk.NumRows())
		}
		require.NoError(t, exec.Close())
	}
}
