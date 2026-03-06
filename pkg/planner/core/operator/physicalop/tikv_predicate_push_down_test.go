// Copyright 2025 PingCAP, Inc.
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

package physicalop

import (
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func newCol(id int64) *expression.Column {
	return &expression.Column{
		UniqueID: id,
		ID:       id,
		RetType:  types.NewFieldType(mysql.TypeLonglong),
	}
}

func newIntConst(val int64) *expression.Constant {
	return expression.NewInt64Const(val)
}

func TestIsTiKVBlockFilterableComparison(t *testing.T) {
	ctx := mock.NewContext().GetExprCtx()
	col := newCol(1)
	c := newIntConst(5)

	// col > const → selected
	for _, op := range []string{ast.GT, ast.GE, ast.LT, ast.LE, ast.EQ} {
		expr := expression.NewFunctionInternal(ctx, op, types.NewFieldType(mysql.TypeTiny), col, c)
		require.True(t, isTiKVBlockFilterable(expr), "expected %s to be filterable", op)
	}

	// const < col → also selected (reversed operand order)
	expr := expression.NewFunctionInternal(ctx, ast.LT, types.NewFieldType(mysql.TypeTiny), c, col)
	require.True(t, isTiKVBlockFilterable(expr))

	// col != const → NOT selected
	neExpr := expression.NewFunctionInternal(ctx, ast.NE, types.NewFieldType(mysql.TypeTiny), col, c)
	require.False(t, isTiKVBlockFilterable(neExpr))
}

func TestIsTiKVBlockFilterableIn(t *testing.T) {
	ctx := mock.NewContext().GetExprCtx()
	col := newCol(1)

	// col IN (1, 2, 3) → selected
	inExpr := expression.NewFunctionInternal(ctx, ast.In, types.NewFieldType(mysql.TypeTiny),
		col, newIntConst(1), newIntConst(2), newIntConst(3))
	require.True(t, isTiKVBlockFilterable(inExpr))

	// const IN (const, const) → NOT selected (first arg not a column)
	inExprBad := expression.NewFunctionInternal(ctx, ast.In, types.NewFieldType(mysql.TypeTiny),
		newIntConst(1), newIntConst(2), newIntConst(3))
	require.False(t, isTiKVBlockFilterable(inExprBad))
}

func TestIsTiKVBlockFilterableIsNull(t *testing.T) {
	ctx := mock.NewContext().GetExprCtx()
	col := newCol(1)

	// col IS NULL → selected
	isNullExpr := expression.NewFunctionInternal(ctx, ast.IsNull, types.NewFieldType(mysql.TypeTiny), col)
	require.True(t, isTiKVBlockFilterable(isNullExpr))

	// const IS NULL → NOT selected
	isNullBad := expression.NewFunctionInternal(ctx, ast.IsNull, types.NewFieldType(mysql.TypeTiny), newIntConst(1))
	require.False(t, isTiKVBlockFilterable(isNullBad))
}

func TestIsTiKVBlockFilterableLogical(t *testing.T) {
	ctx := mock.NewContext().GetExprCtx()
	col := newCol(1)
	c1 := newIntConst(1)
	c2 := newIntConst(10)

	gt := expression.NewFunctionInternal(ctx, ast.GT, types.NewFieldType(mysql.TypeTiny), col, c1)
	lt := expression.NewFunctionInternal(ctx, ast.LT, types.NewFieldType(mysql.TypeTiny), col, c2)

	// col > 1 AND col < 10 → selected
	andExpr := expression.NewFunctionInternal(ctx, ast.LogicAnd, types.NewFieldType(mysql.TypeTiny), gt, lt)
	require.True(t, isTiKVBlockFilterable(andExpr))

	// col > 1 OR col < 10 → selected
	orExpr := expression.NewFunctionInternal(ctx, ast.LogicOr, types.NewFieldType(mysql.TypeTiny), gt, lt)
	require.True(t, isTiKVBlockFilterable(orExpr))
}

func TestIsTiKVBlockFilterableNegative(t *testing.T) {
	ctx := mock.NewContext().GetExprCtx()
	col1 := newCol(1)
	col2 := newCol(2)

	// col > col2 → NOT selected (column vs column)
	colCmp := expression.NewFunctionInternal(ctx, ast.GT, types.NewFieldType(mysql.TypeTiny), col1, col2)
	require.False(t, isTiKVBlockFilterable(colCmp))

	// LIKE → NOT selected
	likeExpr := expression.NewFunctionInternal(ctx, ast.Like, types.NewFieldType(mysql.TypeTiny),
		col1, expression.NewStrConst("%abc"), expression.NewInt64Const(int64('\\')))
	require.False(t, isTiKVBlockFilterable(likeExpr))

	// plain column (not a ScalarFunction) → NOT selected
	require.False(t, isTiKVBlockFilterable(col1))

	// plain constant → NOT selected
	require.False(t, isTiKVBlockFilterable(newIntConst(42)))
}

func TestSelectTiKVBlockFilterPredicates(t *testing.T) {
	ctx := mock.NewContext().GetExprCtx()
	col := newCol(1)
	col2 := newCol(2)
	c := newIntConst(5)

	good := expression.NewFunctionInternal(ctx, ast.GT, types.NewFieldType(mysql.TypeTiny), col, c)
	bad := expression.NewFunctionInternal(ctx, ast.NE, types.NewFieldType(mysql.TypeTiny), col, c)
	colVsCol := expression.NewFunctionInternal(ctx, ast.GT, types.NewFieldType(mysql.TypeTiny), col, col2)

	conds := []expression.Expression{good, bad, colVsCol}
	result := selectTiKVBlockFilterPredicates(conds)
	require.Len(t, result, 1)
	require.Equal(t, good, result[0])
}

func TestSelectTiKVBlockFilterPredicatesEmpty(t *testing.T) {
	result := selectTiKVBlockFilterPredicates(nil)
	require.Nil(t, result)

	result = selectTiKVBlockFilterPredicates([]expression.Expression{})
	require.Nil(t, result)
}
