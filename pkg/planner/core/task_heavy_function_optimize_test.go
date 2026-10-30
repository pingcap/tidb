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

package core

import (
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/planner/util/coretestsdk"
	"github.com/pingcap/tidb/pkg/planner/util/fixcontrol"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
)

func TestGetPushedDownTopNHeavyFunctionNotFirstByItem(t *testing.T) {
	sctx := coretestsdk.MockContext()
	sctx.GetSessionVars().OptimizerFixControl = map[uint64]string{
		fixcontrol.Fix56318: "ON",
	}

	idCol := &expression.Column{
		ID:       1,
		UniqueID: 1,
		RetType:  types.NewFieldType(mysql.TypeLonglong),
		Index:    0,
	}
	scoreCol := &expression.Column{
		ID:       2,
		UniqueID: 2,
		RetType:  types.NewFieldType(mysql.TypeDouble),
		Index:    1,
	}
	vecCol := &expression.Column{
		ID:       3,
		UniqueID: 3,
		RetType:  types.NewFieldType(mysql.TypeTiDBVectorFloat32),
		Index:    2,
	}
	outputSchema := expression.NewSchema(
		idCol.Clone().(*expression.Column),
		scoreCol.Clone().(*expression.Column),
	)

	childStats := &property.StatsInfo{RowCount: 100}
	vecColInfo := &model.ColumnInfo{
		ID:        vecCol.ID,
		Name:      ast.NewCIStr("vec"),
		Offset:    vecCol.Index,
		State:     model.StatePublic,
		FieldType: *types.NewFieldType(mysql.TypeTiDBVectorFloat32),
	}
	tableInfo := &model.TableInfo{
		ID:   42,
		Name: ast.NewCIStr("t"),
		Columns: []*model.ColumnInfo{
			{
				ID:        idCol.ID,
				Name:      ast.NewCIStr("id"),
				Offset:    idCol.Index,
				State:     model.StatePublic,
				FieldType: *types.NewFieldType(mysql.TypeLonglong),
			},
			{
				ID:        scoreCol.ID,
				Name:      ast.NewCIStr("score"),
				Offset:    scoreCol.Index,
				State:     model.StatePublic,
				FieldType: *types.NewFieldType(mysql.TypeDouble),
			},
			vecColInfo,
		},
	}
	queryVector := types.MustCreateVectorFloat32([]float32{1, 1, 1})

	childPlanImpl := physicalop.PhysicalTableScan{
		Table:     tableInfo,
		Columns:   tableInfo.Columns,
		StoreType: kv.TiFlash,
		UsedColumnarIndexes: []*physicalop.ColumnarIndexExtra{
			buildVectorIndexExtra(
				&model.IndexInfo{ID: 100, Name: ast.NewCIStr("vector_index")},
				tipb.ANNQueryType_OrderBy,
				tipb.VectorDistanceMetric_COSINE,
				10,
				vecColInfo.Name.L,
				queryVector.SerializeTo(nil),
				&tipb.ColumnInfo{ColumnId: vecColInfo.ID},
			),
		},
	}.Init(sctx, 0)
	childPlanImpl.SetStats(childStats)
	childPlanImpl.SetSchema(expression.NewSchema(idCol, scoreCol, vecCol))

	var childPlan base.PhysicalPlan = childPlanImpl

	zero := &expression.Constant{
		Value:   types.NewFloat64Datum(0),
		RetType: types.NewFieldType(mysql.TypeDouble),
	}
	one := &expression.Constant{
		Value:   types.NewFloat64Datum(1),
		RetType: types.NewFieldType(mysql.TypeDouble),
	}
	vectorConst := &expression.Constant{
		Value:   types.NewVectorFloat32Datum(queryVector),
		RetType: types.NewFieldType(mysql.TypeTiDBVectorFloat32),
	}
	nonHeavyExpr := expression.NewFunctionInternal(
		sctx.GetExprCtx(),
		ast.Coalesce,
		types.NewFieldType(mysql.TypeDouble),
		scoreCol,
		zero,
	)
	distanceExpr := expression.NewFunctionInternal(
		sctx.GetExprCtx(),
		ast.VecCosineDistance,
		types.NewFieldType(mysql.TypeDouble),
		vecCol,
		vectorConst,
	)
	heavyExpr := expression.NewFunctionInternal(
		sctx.GetExprCtx(),
		ast.Plus,
		types.NewFieldType(mysql.TypeDouble),
		expression.NewFunctionInternal(
			sctx.GetExprCtx(),
			ast.Mul,
			types.NewFieldType(mysql.TypeDouble),
			one,
			expression.NewFunctionInternal(
				sctx.GetExprCtx(),
				ast.Minus,
				types.NewFieldType(mysql.TypeDouble),
				one,
				distanceExpr,
			),
		),
		expression.NewFunctionInternal(
			sctx.GetExprCtx(),
			ast.Mul,
			types.NewFieldType(mysql.TypeDouble),
			one.Clone().(*expression.Constant),
			expression.NewFunctionInternal(
				sctx.GetExprCtx(),
				ast.Minus,
				types.NewFieldType(mysql.TypeDouble),
				one.Clone().(*expression.Constant),
				distanceExpr.Clone(),
			),
		),
	)
	require.NotNil(t, nonHeavyExpr)
	require.NotNil(t, distanceExpr)
	require.NotNil(t, heavyExpr)
	require.False(t, ContainHeavyFunction(nonHeavyExpr))
	require.True(t, ContainHeavyFunction(heavyExpr))

	reqProp := property.NewPhysicalProperty(property.RootTaskType, nil, false, 0, false)
	topN := physicalop.PhysicalTopN{
		ByItems: []*util.ByItems{
			{Expr: nonHeavyExpr, Desc: true},
			{Expr: heavyExpr, Desc: true},
		},
		Count: 10,
	}.Init(sctx, &property.StatsInfo{RowCount: 100}, 0, reqProp)
	topN.SetSchema(outputSchema)

	require.NotPanics(t, func() {
		pushedDownTopN, globalTopN := getPushedDownTopN(topN, childPlan, kv.TiFlash)
		require.NotNil(t, pushedDownTopN)
		require.NotNil(t, globalTopN)
		_, ok := pushedDownTopN.ByItems[0].Expr.(*expression.Column)
		require.False(t, ok)
		pushedCol, ok := pushedDownTopN.ByItems[1].Expr.(*expression.Column)
		require.True(t, ok)
		require.Equal(t, outputSchema.Len(), pushedCol.Index)

		globalCol, ok := globalTopN.ByItems[1].Expr.(*expression.Column)
		require.True(t, ok)
		require.Equal(t, outputSchema.Len(), globalCol.Index)
	})
}
