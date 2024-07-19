// Copyright 2018 PingCAP, Inc.
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

package memo

import (
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/expression"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/pattern"
	"github.com/stretchr/testify/require"
	"go.opencensus.io/stats/view"
)

func TestNewExprIterFromGroupElem(t *testing.T) {
	defer view.Stop()
	ctx := plannercore.MockContext()
	schema := expression.NewSchema()
	defer func() {
		do := domain.GetDomain(ctx)
		do.StatsHandle().Close()
	}()
	g0 := NewGroupWithSchema(NewGroupExpr(plannercore.LogicalSelection{}.Init(ctx, 0)), schema)
	g0.Insert(NewGroupExpr(plannercore.LogicalLimit{}.Init(ctx, 0)))
	g0.Insert(NewGroupExpr(plannercore.LogicalProjection{}.Init(ctx, 0)))
	g0.Insert(NewGroupExpr(plannercore.LogicalLimit{}.Init(ctx, 0)))

	g1 := NewGroupWithSchema(NewGroupExpr(plannercore.LogicalSelection{}.Init(ctx, 0)), schema)
	g1.Insert(NewGroupExpr(plannercore.LogicalLimit{}.Init(ctx, 0)))
	g1.Insert(NewGroupExpr(plannercore.LogicalProjection{}.Init(ctx, 0)))
	g1.Insert(NewGroupExpr(plannercore.LogicalLimit{}.Init(ctx, 0)))

	expr := NewGroupExpr(plannercore.LogicalJoin{}.Init(ctx, 0))
	expr.Children = append(expr.Children, g0)
	expr.Children = append(expr.Children, g1)
	g2 := NewGroupWithSchema(expr, schema)

	pat := pattern.BuildPattern(pattern.OperandJoin, pattern.EngineAll,
		pattern.BuildPattern(pattern.OperandProjection, pattern.EngineAll),
		pattern.BuildPattern(pattern.OperandSelection, pattern.EngineAll))
	iter := NewExprIterFromGroupElem(g2.Equivalents.Front(), pat)

	require.NotNil(t, iter)
	require.Nil(t, iter.Group)
	require.Equal(t, g2.Equivalents.Front(), iter.Element)
	require.True(t, iter.matched)
	require.Equal(t, pattern.OperandJoin, iter.Operand)
	require.Len(t, iter.Children, 2)

	require.Equal(t, g0, iter.Children[0].Group)
	require.Equal(t, g0.GetFirstElem(pattern.OperandProjection), iter.Children[0].Element)
	require.True(t, iter.Children[0].matched)
	require.Equal(t, pattern.OperandProjection, iter.Children[0].Operand)
	require.Len(t, iter.Children[0].Children, 0)

	require.Equal(t, g1, iter.Children[1].Group)
	require.Equal(t, g1.GetFirstElem(pattern.OperandSelection), iter.Children[1].Element)
	require.True(t, iter.Children[1].matched)
	require.Equal(t, pattern.OperandSelection, iter.Children[1].Operand)
	require.Len(t, iter.Children[0].Children, 0)
}

func TestExprIterNext(t *testing.T) {
	defer view.Stop()
	ctx := plannercore.MockContext()
	schema := expression.NewSchema()
	defer func() {
		do := domain.GetDomain(ctx)
		do.StatsHandle().Close()
	}()
	g0 := NewGroupWithSchema(NewGroupExpr(plannercore.LogicalProjection{Exprs: []expression.Expression{expression.NewZero()}}.Init(ctx, 0)), schema)
	g0.Insert(NewGroupExpr(plannercore.LogicalLimit{Count: 1}.Init(ctx, 0)))
	g0.Insert(NewGroupExpr(plannercore.LogicalProjection{Exprs: []expression.Expression{expression.NewOne()}}.Init(ctx, 0)))
	g0.Insert(NewGroupExpr(plannercore.LogicalLimit{Count: 2}.Init(ctx, 0)))
	g0.Insert(NewGroupExpr(plannercore.LogicalProjection{Exprs: []expression.Expression{expression.NewNull()}}.Init(ctx, 0)))

	g1 := NewGroupWithSchema(NewGroupExpr(plannercore.LogicalSelection{Conditions: []expression.Expression{expression.NewNull()}}.Init(ctx, 0)), schema)
	g1.Insert(NewGroupExpr(plannercore.LogicalLimit{Count: 3}.Init(ctx, 0)))
	g1.Insert(NewGroupExpr(plannercore.LogicalSelection{Conditions: []expression.Expression{expression.NewOne()}}.Init(ctx, 0)))
	g1.Insert(NewGroupExpr(plannercore.LogicalLimit{Count: 4}.Init(ctx, 0)))
	g1.Insert(NewGroupExpr(plannercore.LogicalSelection{Conditions: []expression.Expression{expression.NewZero()}}.Init(ctx, 0)))

	expr := NewGroupExpr(plannercore.LogicalJoin{}.Init(ctx, 0))
	expr.Children = append(expr.Children, g0)
	expr.Children = append(expr.Children, g1)
	g2 := NewGroupWithSchema(expr, schema)

	pat := pattern.BuildPattern(pattern.OperandJoin, pattern.EngineAll,
		pattern.BuildPattern(pattern.OperandProjection, pattern.EngineAll),
		pattern.BuildPattern(pattern.OperandSelection, pattern.EngineAll))
	iter := NewExprIterFromGroupElem(g2.Equivalents.Front(), pat)
	require.NotNil(t, iter)

	count := 0
	for ; iter.Matched(); iter.Next() {
		count++
		require.Nil(t, iter.Group)
		require.True(t, iter.matched)
		require.Equal(t, pattern.OperandJoin, iter.Operand)
		require.Len(t, iter.Children, 2)

		require.Equal(t, g0, iter.Children[0].Group)
		require.True(t, iter.Children[0].matched)
		require.Equal(t, pattern.OperandProjection, iter.Children[0].Operand)
		require.Len(t, iter.Children[0].Children, 0)

		require.Equal(t, g1, iter.Children[1].Group)
		require.True(t, iter.Children[1].matched)
		require.Equal(t, pattern.OperandSelection, iter.Children[1].Operand)
		require.Len(t, iter.Children[1].Children, 0)
	}

	require.Equal(t, 9, count)
}

func TestExprIterReset(t *testing.T) {
	defer view.Stop()
	ctx := plannercore.MockContext()
	schema := expression.NewSchema()
	defer func() {
		do := domain.GetDomain(ctx)
		do.StatsHandle().Close()
	}()
	g0 := NewGroupWithSchema(NewGroupExpr(plannercore.LogicalProjection{Exprs: []expression.Expression{expression.NewZero()}}.Init(ctx, 0)), schema)
	g0.Insert(NewGroupExpr(plannercore.LogicalLimit{Count: 1}.Init(ctx, 0)))
	g0.Insert(NewGroupExpr(plannercore.LogicalProjection{Exprs: []expression.Expression{expression.NewOne()}}.Init(ctx, 0)))
	g0.Insert(NewGroupExpr(plannercore.LogicalLimit{Count: 2}.Init(ctx, 0)))
	g0.Insert(NewGroupExpr(plannercore.LogicalProjection{Exprs: []expression.Expression{expression.NewNull()}}.Init(ctx, 0)))

	sel1 := NewGroupExpr(plannercore.LogicalSelection{Conditions: []expression.Expression{expression.NewNull()}}.Init(ctx, 0))
	sel2 := NewGroupExpr(plannercore.LogicalSelection{Conditions: []expression.Expression{expression.NewOne()}}.Init(ctx, 0))
	sel3 := NewGroupExpr(plannercore.LogicalSelection{Conditions: []expression.Expression{expression.NewZero()}}.Init(ctx, 0))
	g1 := NewGroupWithSchema(sel1, schema)
	g1.Insert(NewGroupExpr(plannercore.LogicalLimit{Count: 3}.Init(ctx, 0)))
	g1.Insert(sel2)
	g1.Insert(NewGroupExpr(plannercore.LogicalLimit{Count: 4}.Init(ctx, 0)))
	g1.Insert(sel3)

	g2 := NewGroupWithSchema(NewGroupExpr(plannercore.LogicalSelection{Conditions: []expression.Expression{expression.NewNull()}}.Init(ctx, 0)), schema)
	g2.Insert(NewGroupExpr(plannercore.LogicalLimit{Count: 3}.Init(ctx, 0)))
	g2.Insert(NewGroupExpr(plannercore.LogicalSelection{Conditions: []expression.Expression{expression.NewOne()}}.Init(ctx, 0)))
	g2.Insert(NewGroupExpr(plannercore.LogicalLimit{Count: 4}.Init(ctx, 0)))
	g2.Insert(NewGroupExpr(plannercore.LogicalSelection{Conditions: []expression.Expression{expression.NewZero()}}.Init(ctx, 0)))

	// link join with Group 0 and 1
	expr := NewGroupExpr(plannercore.LogicalJoin{}.Init(ctx, 0))
	expr.Children = append(expr.Children, g0)
	expr.Children = append(expr.Children, g1)
	g3 := NewGroupWithSchema(expr, schema)

	// link sel 1~3 with Group 2
	sel1.Children = append(sel1.Children, g2)
	sel2.Children = append(sel2.Children, g2)
	sel3.Children = append(sel3.Children, g2)

	// create a pattern: join(proj, sel(limit))
	lhsPattern := pattern.BuildPattern(pattern.OperandProjection, pattern.EngineAll)
	rhsPattern := pattern.BuildPattern(pattern.OperandSelection, pattern.EngineAll,
		pattern.BuildPattern(pattern.OperandLimit, pattern.EngineAll))
	pat := pattern.BuildPattern(pattern.OperandJoin, pattern.EngineAll, lhsPattern, rhsPattern)

	// create expression iterator for the pattern on join
	iter := NewExprIterFromGroupElem(g3.Equivalents.Front(), pat)
	require.NotNil(t, iter)

	count := 0
	for ; iter.Matched(); iter.Next() {
		count++
		require.Nil(t, iter.Group)
		require.True(t, iter.matched)
		require.Equal(t, pattern.OperandJoin, iter.Operand)
		require.Len(t, iter.Children, 2)

		require.Equal(t, g0, iter.Children[0].Group)
		require.True(t, iter.Children[0].matched)
		require.Equal(t, pattern.OperandProjection, iter.Children[0].Operand)
		require.Len(t, iter.Children[0].Children, 0)

		require.Equal(t, g1, iter.Children[1].Group)
		require.True(t, iter.Children[1].matched)
		require.Equal(t, pattern.OperandSelection, iter.Children[1].Operand)
		require.Len(t, iter.Children[1].Children, 1)

		require.Equal(t, g2, iter.Children[1].Children[0].Group)
		require.True(t, iter.Children[1].Children[0].matched)
		require.Equal(t, pattern.OperandLimit, iter.Children[1].Children[0].Operand)
		require.Len(t, iter.Children[1].Children[0].Children, 0)
	}

	require.Equal(t, 18, count)
}

func TestExprIterWithEngineType(t *testing.T) {
	defer view.Stop()
	ctx := plannercore.MockContext()
	schema := expression.NewSchema()
	defer func() {
		do := domain.GetDomain(ctx)
		do.StatsHandle().Close()
	}()
	g1 := NewGroupWithSchema(NewGroupExpr(plannercore.LogicalSelection{Conditions: []expression.Expression{expression.NewOne()}}.Init(ctx, 0)), schema).SetEngineType(pattern.EngineTiFlash)
	g1.Insert(NewGroupExpr(plannercore.LogicalLimit{Count: 1}.Init(ctx, 0)))
	g1.Insert(NewGroupExpr(plannercore.LogicalProjection{Exprs: []expression.Expression{expression.NewOne()}}.Init(ctx, 0)))
	g1.Insert(NewGroupExpr(plannercore.LogicalLimit{Count: 2}.Init(ctx, 0)))

	g2 := NewGroupWithSchema(NewGroupExpr(plannercore.LogicalSelection{Conditions: []expression.Expression{expression.NewOne()}}.Init(ctx, 0)), schema).SetEngineType(pattern.EngineTiKV)
	g2.Insert(NewGroupExpr(plannercore.LogicalLimit{Count: 2}.Init(ctx, 0)))
	g2.Insert(NewGroupExpr(plannercore.LogicalProjection{Exprs: []expression.Expression{expression.NewOne()}}.Init(ctx, 0)))
	g2.Insert(NewGroupExpr(plannercore.LogicalLimit{Count: 3}.Init(ctx, 0)))

	flashGather := NewGroupExpr(plannercore.TiKVSingleGather{}.Init(ctx, 0))
	flashGather.Children = append(flashGather.Children, g1)
	g3 := NewGroupWithSchema(flashGather, schema).SetEngineType(pattern.EngineTiDB)

	tikvGather := NewGroupExpr(plannercore.TiKVSingleGather{}.Init(ctx, 0))
	tikvGather.Children = append(tikvGather.Children, g2)
	g3.Insert(tikvGather)

	join := NewGroupExpr(plannercore.LogicalJoin{}.Init(ctx, 0))
	join.Children = append(join.Children, g3, g3)
	g4 := NewGroupWithSchema(join, schema).SetEngineType(pattern.EngineTiDB)

	// The Groups look like this:
	// Group 4
	//     Join input:[Group3, Group3]
	// Group 3
	//     TiKVSingleGather input:[Group2] EngineTiKV
	//     TiKVSingleGather input:[Group1] EngineTiFlash
	// Group 2
	//     Selection
	//     Projection
	//     Limit
	//     Limit
	// Group 1
	//     Selection
	//     Projection
	//     Limit
	//     Limit

	p0 := pattern.BuildPattern(pattern.OperandTiKVSingleGather, pattern.EngineTiDBOnly,
		pattern.BuildPattern(pattern.OperandLimit, pattern.EngineTiKVOnly))
	require.Equal(t, 2, countMatchedIter(g3, p0))
	p1 := pattern.BuildPattern(pattern.OperandTiKVSingleGather, pattern.EngineTiDBOnly,
		pattern.BuildPattern(pattern.OperandLimit, pattern.EngineTiFlashOnly))
	require.Equal(t, 2, countMatchedIter(g3, p1))
	p2 := pattern.BuildPattern(pattern.OperandTiKVSingleGather, pattern.EngineTiDBOnly,
		pattern.BuildPattern(pattern.OperandLimit, pattern.EngineTiKVOrTiFlash))
	require.Equal(t, 4, countMatchedIter(g3, p2))
	p3 := pattern.BuildPattern(pattern.OperandTiKVSingleGather, pattern.EngineTiDBOnly,
		pattern.BuildPattern(pattern.OperandSelection, pattern.EngineTiFlashOnly))
	require.Equal(t, 1, countMatchedIter(g3, p3))
	p4 := pattern.BuildPattern(pattern.OperandTiKVSingleGather, pattern.EngineTiDBOnly,
		pattern.BuildPattern(pattern.OperandProjection, pattern.EngineTiKVOnly))
	require.Equal(t, 1, countMatchedIter(g3, p4))
	p5 := pattern.BuildPattern(
		pattern.OperandJoin,
		pattern.EngineTiDBOnly,
		pattern.BuildPattern(pattern.OperandTiKVSingleGather, pattern.EngineTiDBOnly,
			pattern.BuildPattern(pattern.OperandLimit, pattern.EngineTiKVOnly)),
		pattern.BuildPattern(pattern.OperandTiKVSingleGather, pattern.EngineTiDBOnly,
			pattern.BuildPattern(pattern.OperandLimit, pattern.EngineTiKVOnly)),
	)
	require.Equal(t, 4, countMatchedIter(g4, p5))
	p6 := pattern.BuildPattern(
		pattern.OperandJoin,
		pattern.EngineTiDBOnly,
		pattern.BuildPattern(pattern.OperandTiKVSingleGather, pattern.EngineTiDBOnly,
			pattern.BuildPattern(pattern.OperandLimit, pattern.EngineTiFlashOnly)),
		pattern.BuildPattern(pattern.OperandTiKVSingleGather, pattern.EngineTiDBOnly,
			pattern.BuildPattern(pattern.OperandLimit, pattern.EngineTiKVOnly)),
	)
	require.Equal(t, 4, countMatchedIter(g4, p6))
	p7 := pattern.BuildPattern(
		pattern.OperandJoin,
		pattern.EngineTiDBOnly,
		pattern.BuildPattern(pattern.OperandTiKVSingleGather, pattern.EngineTiDBOnly,
			pattern.BuildPattern(pattern.OperandLimit, pattern.EngineTiKVOrTiFlash)),
		pattern.BuildPattern(pattern.OperandTiKVSingleGather, pattern.EngineTiDBOnly,
			pattern.BuildPattern(pattern.OperandLimit, pattern.EngineTiKVOrTiFlash)),
	)
	require.Equal(t, 16, countMatchedIter(g4, p7))

	// This is not a test case for EngineType. This case is to test
	// the Pattern without a leaf AnyOperand. It is more efficient to
	// test it here.
	p8 := pattern.BuildPattern(
		pattern.OperandJoin,
		pattern.EngineTiDBOnly,
		pattern.BuildPattern(pattern.OperandTiKVSingleGather, pattern.EngineTiDBOnly),
		pattern.BuildPattern(pattern.OperandTiKVSingleGather, pattern.EngineTiDBOnly),
	)
	require.Equal(t, 4, countMatchedIter(g4, p8))
}

func countMatchedIter(group *Group, pattern *pattern.Pattern) int {
	count := 0
	for elem := group.Equivalents.Front(); elem != nil; elem = elem.Next() {
		iter := NewExprIterFromGroupElem(elem, pattern)
		if iter == nil {
			continue
		}
		for ; iter.Matched(); iter.Next() {
			count++
		}
	}
	return count
}
