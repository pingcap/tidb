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

package util_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/coretestsdk"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/ranger"
	"github.com/stretchr/testify/require"
)

func TestCompareCol2Len(t *testing.T) {
	tests := []struct {
		c1         util.Col2Len
		c2         util.Col2Len
		res        int
		comparable bool
	}{
		{
			c1:         util.Col2Len{1: -1, 2: -1, 3: -1},
			c2:         util.Col2Len{1: -1, 2: 10},
			res:        1,
			comparable: true,
		},
		{
			c1:         util.Col2Len{1: 5},
			c2:         util.Col2Len{1: 10, 2: -1},
			res:        -1,
			comparable: true,
		},
		{
			c1:         util.Col2Len{1: -1, 2: -1},
			c2:         util.Col2Len{1: -1, 2: 5, 3: -1},
			res:        -1,
			comparable: false,
		},
		{
			c1:         util.Col2Len{1: -1, 2: 10},
			c2:         util.Col2Len{1: -1, 2: 5, 3: -1},
			res:        -1,
			comparable: false,
		},
		{
			c1:         util.Col2Len{1: -1, 2: 10},
			c2:         util.Col2Len{1: -1, 2: 10},
			res:        0,
			comparable: true,
		},
		{
			c1:         util.Col2Len{1: -1, 2: -1},
			c2:         util.Col2Len{1: -1, 2: 10},
			res:        -1,
			comparable: false,
		},
	}
	for _, tt := range tests {
		res, comparable1 := util.CompareCol2Len(tt.c1, tt.c2)
		require.Equal(t, tt.res, res)
		require.Equal(t, tt.comparable, comparable1)
	}
}

func TestOnlyPointRange(t *testing.T) {
	sctx := coretestsdk.MockContext()
	defer func() {
		do := domain.GetDomain(sctx)
		do.StatsHandle().Close()
	}()
	nullDatum := types.MinNotNullDatum()
	nullDatum.SetNull()
	nullPointRange := ranger.Range{
		LowVal:    []types.Datum{*nullDatum.Clone()},
		HighVal:   []types.Datum{*nullDatum.Clone()},
		Collators: collate.GetBinaryCollatorSlice(1),
	}
	onePointRange := ranger.Range{
		LowVal:    []types.Datum{types.NewIntDatum(1)},
		HighVal:   []types.Datum{types.NewIntDatum(1)},
		Collators: collate.GetBinaryCollatorSlice(1),
	}
	one2TwoRange := ranger.Range{
		LowVal:    []types.Datum{types.NewIntDatum(1)},
		HighVal:   []types.Datum{types.NewIntDatum(2)},
		Collators: collate.GetBinaryCollatorSlice(1),
	}

	tc := sctx.GetSessionVars().StmtCtx.TypeCtx()
	intHandlePath := &util.AccessPath{IsIntHandlePath: true}
	intHandlePath.Ranges = []*ranger.Range{&nullPointRange, &onePointRange}
	require.True(t, intHandlePath.OnlyPointRange(tc))
	intHandlePath.Ranges = []*ranger.Range{&onePointRange, &one2TwoRange}
	require.False(t, intHandlePath.OnlyPointRange(tc))

	indexPath := &util.AccessPath{Index: &model.IndexInfo{Columns: make([]*model.IndexColumn, 1)}}
	indexPath.Ranges = []*ranger.Range{&onePointRange}
	require.True(t, indexPath.OnlyPointRange(tc))
	indexPath.Ranges = []*ranger.Range{&nullPointRange, &onePointRange}
	require.False(t, indexPath.OnlyPointRange(tc))
	indexPath.Ranges = []*ranger.Range{&onePointRange, &one2TwoRange}
	require.False(t, indexPath.OnlyPointRange(tc))

	indexPath.Index.Columns = make([]*model.IndexColumn, 2)
	indexPath.Ranges = []*ranger.Range{&onePointRange}
	require.False(t, indexPath.OnlyPointRange(tc))

	t.Run("stable null reject fast path", func(t *testing.T) {
		col := &expression.Column{
			UniqueID: sctx.GetSessionVars().AllocPlanColumnID(),
			RetType:  types.NewFieldType(mysql.TypeLonglong),
		}
		schema := expression.NewSchema(col)
		one := &expression.Constant{
			Value:   types.NewIntDatum(1),
			RetType: types.NewFieldType(mysql.TypeLonglong),
		}

		gtExpr := expression.NewFunctionInternal(
			sctx.GetExprCtx(),
			ast.GT,
			types.NewFieldType(mysql.TypeTiny),
			col,
			one,
		)
		require.Equal(t, util.StableNullRejectYes, util.ClassifyStableNullRejectBySchema(sctx.GetPlanCtx(), schema, gtExpr))
		require.Equal(t, util.StableNullRejectYes, util.ClassifyStableNullRejectByColumn(sctx.GetPlanCtx(), col, gtExpr))

		addExpr := expression.NewFunctionInternal(
			sctx.GetExprCtx(),
			ast.Plus,
			types.NewFieldType(mysql.TypeLonglong),
			col,
			one,
		)
		addGTExpr := expression.NewFunctionInternal(
			sctx.GetExprCtx(),
			ast.GT,
			types.NewFieldType(mysql.TypeTiny),
			addExpr,
			one,
		)
		require.Equal(t, util.StableNullRejectYes, util.ClassifyStableNullRejectBySchema(sctx.GetPlanCtx(), schema, addGTExpr))
		require.Equal(t, util.StableNullRejectYes, util.ClassifyStableNullRejectBySchema(sctx.GetPlanCtx(), schema, addExpr))

		two := &expression.Constant{
			Value:   types.NewIntDatum(2),
			RetType: types.NewFieldType(mysql.TypeLonglong),
		}
		strictCases := []struct {
			name  string
			build func() expression.Expression
		}{
			{
				name: "abs",
				build: func() expression.Expression {
					return expression.NewFunctionInternal(
						sctx.GetExprCtx(),
						ast.Abs,
						types.NewFieldType(mysql.TypeLonglong),
						col,
					)
				},
			},
			{
				name: "minus",
				build: func() expression.Expression {
					return expression.NewFunctionInternal(
						sctx.GetExprCtx(),
						ast.Minus,
						types.NewFieldType(mysql.TypeLonglong),
						col,
						one,
					)
				},
			},
			{
				name: "mul",
				build: func() expression.Expression {
					return expression.NewFunctionInternal(
						sctx.GetExprCtx(),
						ast.Mul,
						types.NewFieldType(mysql.TypeLonglong),
						col,
						two,
					)
				},
			},
			{
				name: "div",
				build: func() expression.Expression {
					return expression.NewFunctionInternal(
						sctx.GetExprCtx(),
						ast.Div,
						types.NewFieldType(mysql.TypeNewDecimal),
						col,
						two,
					)
				},
			},
			{
				name: "int div",
				build: func() expression.Expression {
					return expression.NewFunctionInternal(
						sctx.GetExprCtx(),
						ast.IntDiv,
						types.NewFieldType(mysql.TypeLonglong),
						col,
						two,
					)
				},
			},
			{
				name: "mod",
				build: func() expression.Expression {
					return expression.NewFunctionInternal(
						sctx.GetExprCtx(),
						ast.Mod,
						types.NewFieldType(mysql.TypeLonglong),
						col,
						two,
					)
				},
			},
			{
				name: "unary minus",
				build: func() expression.Expression {
					return expression.NewFunctionInternal(
						sctx.GetExprCtx(),
						ast.UnaryMinus,
						types.NewFieldType(mysql.TypeLonglong),
						col,
					)
				},
			},
		}
		for _, tc := range strictCases {
			t.Run(tc.name, func(t *testing.T) {
				strictExpr := tc.build()
				predicate := expression.NewFunctionInternal(
					sctx.GetExprCtx(),
					ast.GT,
					types.NewFieldType(mysql.TypeTiny),
					strictExpr,
					one,
				)
				require.Equal(t, util.StableNullRejectYes, util.ClassifyStableNullRejectBySchema(sctx.GetPlanCtx(), schema, predicate))
			})
		}

		isNullExpr := expression.NewFunctionInternal(
			sctx.GetExprCtx(),
			ast.IsNull,
			types.NewFieldType(mysql.TypeTiny),
			col,
		)
		orExpr := expression.NewFunctionInternal(
			sctx.GetExprCtx(),
			ast.LogicOr,
			types.NewFieldType(mysql.TypeTiny),
			gtExpr,
			isNullExpr,
		)
		require.Equal(t, util.StableNullRejectNo, util.ClassifyStableNullRejectBySchema(sctx.GetPlanCtx(), schema, orExpr))

		notIsNullExpr := expression.NewFunctionInternal(
			sctx.GetExprCtx(),
			ast.UnaryNot,
			types.NewFieldType(mysql.TypeTiny),
			isNullExpr,
		)
		require.Equal(t, util.StableNullRejectYes, util.ClassifyStableNullRejectBySchema(sctx.GetPlanCtx(), schema, notIsNullExpr))

		nullEQExpr := expression.NewFunctionInternal(
			sctx.GetExprCtx(),
			ast.NullEQ,
			types.NewFieldType(mysql.TypeTiny),
			col,
			one,
		)
		require.Equal(t, util.StableNullRejectUnknown, util.ClassifyStableNullRejectBySchema(sctx.GetPlanCtx(), schema, nullEQExpr))
	})
}
