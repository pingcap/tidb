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
	"testing"

	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestSelectionInitFiltersClassification(t *testing.T) {
	sctx := defaultCtx()
	ds := newRequiredRowsDataSource(sctx, 0, nil)
	corVal := types.NewDatum(int64(0))
	corCol := &expression.CorrelatedColumn{
		Column: expression.Column{UniqueID: 100, Index: 0, RetType: types.NewFieldType(mysql.TypeLonglong)},
		Data:   &corVal,
	}
	one := &expression.Constant{Value: types.NewDatum(int64(1)), RetType: types.NewFieldType(mysql.TypeLonglong)}
	buildFunc := func(funcName string, args ...expression.Expression) expression.Expression {
		f, err := expression.NewFunction(sctx.GetExprCtx(), funcName, types.NewFieldType(byte(types.ETInt)), args...)
		require.NoError(t, err)
		return f
	}
	corConstFilter := buildFunc(ast.GT, corCol, one)
	columnFilter := buildFunc(ast.GT, ds.Schema().Columns[1], one)
	corColumnFilter := buildFunc(ast.GT, ds.Schema().Columns[1], corCol)
	randFn, err := expression.NewFunction(sctx.GetExprCtx(), ast.Rand, types.NewFieldType(mysql.TypeDouble))
	require.NoError(t, err)
	corUnfoldableFilter := buildFunc(ast.GT, corCol, randFn)

	e := new(SelectionExec)
	e.initFilters([]expression.Expression{corConstFilter, columnFilter, corColumnFilter, corUnfoldableFilter})
	require.Equal(t, []expression.Expression{corConstFilter}, e.rowIndependentFilters)
	require.Equal(t, []expression.Expression{columnFilter, corColumnFilter, corUnfoldableFilter}, e.filters)

	// No row-independent filter: the condition slice is kept as-is.
	e = new(SelectionExec)
	conds := []expression.Expression{columnFilter, corColumnFilter}
	e.initFilters(conds)
	require.Nil(t, e.rowIndependentFilters)
	require.Equal(t, conds, e.filters)
}

func TestSelectionRowIndependentFilterShortcut(t *testing.T) {
	sctx := defaultCtx()
	ctx := context.Background()
	totalRows := 20
	ds := newRequiredRowsDataSource(sctx, totalRows, nil)

	corVal := types.NewDatum(int64(0))
	corCol := &expression.CorrelatedColumn{
		Column: expression.Column{UniqueID: 100, Index: 0, RetType: types.NewFieldType(mysql.TypeLonglong)},
		Data:   &corVal,
	}
	one := &expression.Constant{Value: types.NewDatum(int64(1)), RetType: types.NewFieldType(mysql.TypeLonglong)}
	// filter: corCol > 1, which references no column of the input rows.
	filter, err := expression.NewFunction(sctx.GetExprCtx(), ast.GT, types.NewFieldType(byte(types.ETInt)), corCol, one)
	require.NoError(t, err)

	e := &SelectionExec{
		selectionExecutorContext: newSelectionExecutorContext(sctx),
		BaseExecutorV2:           exec.NewBaseExecutorV2(sctx.GetSessionVars(), ds.Schema(), 0, ds),
	}
	e.initFilters([]expression.Expression{filter})
	require.Len(t, e.rowIndependentFilters, 1)
	require.Empty(t, e.filters)

	fetchAll := func() int {
		chk := exec.NewFirstChunk(e)
		fetched := 0
		for {
			require.NoError(t, e.Next(ctx, chk))
			if chk.NumRows() == 0 {
				return fetched
			}
			fetched += chk.NumRows()
		}
	}

	// The filter evaluates to false, so the child should never be read.
	corVal = types.NewDatum(int64(0))
	require.NoError(t, e.Open(ctx))
	require.Equal(t, 0, fetchAll())
	require.Equal(t, 0, ds.numNextCalled)
	require.NoError(t, e.Close())

	// Reopening rechecks the filter (like Apply does for each outer row): now it
	// evaluates to true and all child rows pass through.
	corVal = types.NewDatum(int64(2))
	require.NoError(t, e.Open(ctx))
	require.Equal(t, totalRows, fetchAll())
	require.Positive(t, ds.numNextCalled)
	require.NoError(t, e.Close())

	// A null filter result also shortcuts the execution after reopening.
	numNextCalled := ds.numNextCalled
	corVal = types.NewDatum(nil)
	require.NoError(t, e.Open(ctx))
	require.Equal(t, 0, fetchAll())
	require.Equal(t, numNextCalled, ds.numNextCalled)
	require.NoError(t, e.Close())
}
