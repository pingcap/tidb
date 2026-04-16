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

package joinorder

import (
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util/coretestsdk"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/intset"
	"github.com/stretchr/testify/require"
)

func newTypedLeaf(ctx base.PlanContext, name string, tp byte, count float64) *logicalop.LogicalTableDual {
	dual := logicalop.LogicalTableDual{RowCount: 1}.Init(ctx, 0)
	dual.SetSchema(expression.NewSchema())
	dual.Schema().Append(&expression.Column{
		UniqueID: ctx.GetSessionVars().PlanColumnID.Add(1),
		RetType:  types.NewFieldType(tp),
	})
	dual.SetStats(&property.StatsInfo{RowCount: count})
	return dual
}

func newLeafProjection(ctx base.PlanContext, child base.LogicalPlan) *logicalop.LogicalProjection {
	proj := logicalop.LogicalProjection{
		Exprs: expression.Column2Exprs(child.Schema().Columns),
	}.Init(ctx, 0)
	proj.SetSchema(child.Schema().Clone())
	proj.SetChildren(child)
	proj.SetStats(&property.StatsInfo{RowCount: child.StatsInfo().RowCount})
	return proj
}

func singletonBitSet(v int) intset.FastIntSet {
	var s intset.FastIntSet
	s.Insert(v)
	return s
}

func TestSpeculativeJoinBuildDoesNotMutateLeafPlans(t *testing.T) {
	ctx := coretestsdk.MockContext()
	defer domain.GetDomain(ctx).StatsHandle().Close()

	stringLeaf := newLeafProjection(ctx, newTypedLeaf(ctx, "s", mysql.TypeVarchar, 1))
	intLeaf := newTypedLeaf(ctx, "i", mysql.TypeLonglong, 1)

	castStringToInt := expression.BuildCastFunction(ctx.GetExprCtx(), stringLeaf.Schema().Columns[0], types.NewFieldType(mysql.TypeLonglong))
	eqCond, ok := expression.NewFunctionInternal(ctx.GetExprCtx(), ast.EQ, types.NewFieldType(mysql.TypeTiny),
		castStringToInt, intLeaf.Schema().Columns[0]).(*expression.ScalarFunction)
	require.True(t, ok)

	leftNode := &Node{
		bitSet:    singletonBitSet(0),
		p:         intLeaf,
		cumCost:   1,
		usedEdges: make(map[uint64]struct{}),
	}
	rightNode := &Node{
		bitSet:    singletonBitSet(1),
		p:         stringLeaf,
		cumCost:   1,
		usedEdges: make(map[uint64]struct{}),
	}
	all := leftNode.bitSet.Union(rightNode.bitSet)
	detector := &ConflictDetector{
		ctx: ctx,
		innerEdges: []*edge{{
			idx:       1,
			joinType:  base.InnerJoin,
			eqConds:   []*expression.ScalarFunction{eqCond},
			tes:       all,
			skipRules: true,
		}},
	}

	origExprCnt := len(stringLeaf.Exprs)
	origSchemaLen := stringLeaf.Schema().Len()
	_, newNode, err := checkConnectionAndMakeJoin(detector, leftNode, rightNode, nil, false)
	require.NoError(t, err)
	require.NotNil(t, newNode)
	join, ok := newNode.p.(*logicalop.LogicalJoin)
	require.True(t, ok)
	clonedRight, ok := join.Children()[1].(*logicalop.LogicalProjection)
	require.True(t, ok)
	require.NotSame(t, stringLeaf, clonedRight)
	require.Same(t, clonedRight, clonedRight.Self())
	require.Len(t, clonedRight.Exprs, origExprCnt+1)
	require.Equal(t, origSchemaLen+1, clonedRight.Schema().Len())
	require.NotNil(t, clonedRight.StatsInfo())
	_, ok = clonedRight.StatsInfo().ColNDVs[clonedRight.Schema().Columns[origSchemaLen].UniqueID]
	require.True(t, ok)
	require.Same(t, stringLeaf, rightNode.p)
	require.Len(t, stringLeaf.Exprs, origExprCnt)
	require.Equal(t, origSchemaLen, stringLeaf.Schema().Len())
}

func TestDegeneratePredicateDoesNotCountAsGreedySeedJoinCondition(t *testing.T) {
	ctx := coretestsdk.MockContext()
	defer domain.GetDomain(ctx).StatsHandle().Close()

	leftLeaf := newTypedLeaf(ctx, "a", mysql.TypeLonglong, 1)
	midLeaf := newTypedLeaf(ctx, "b", mysql.TypeLonglong, 1)
	rightLeaf := newTypedLeaf(ctx, "c", mysql.TypeLonglong, 1)

	leftNode := &Node{
		bitSet:    singletonBitSet(0),
		p:         leftLeaf,
		cumCost:   1,
		usedEdges: make(map[uint64]struct{}),
	}
	midNode := &Node{
		bitSet:    singletonBitSet(1),
		p:         midLeaf,
		cumCost:   1,
		usedEdges: make(map[uint64]struct{}),
	}
	rightNode := &Node{
		bitSet:    singletonBitSet(2),
		p:         rightLeaf,
		cumCost:   1,
		usedEdges: make(map[uint64]struct{}),
	}

	degenerateCond, ok := expression.NewFunctionInternal(ctx.GetExprCtx(), ast.GT, types.NewFieldType(mysql.TypeTiny),
		leftLeaf.Schema().Columns[0], expression.NewZero()).(*expression.ScalarFunction)
	require.True(t, ok)
	crossCond, ok := expression.NewFunctionInternal(ctx.GetExprCtx(), ast.GT, types.NewFieldType(mysql.TypeTiny),
		midLeaf.Schema().Columns[0], rightLeaf.Schema().Columns[0]).(*expression.ScalarFunction)
	require.True(t, ok)

	detector := &ConflictDetector{
		ctx:          ctx,
		allInnerJoin: true,
	}
	degenerateEdge := detector.makeEdge(base.InnerJoin, []expression.Expression{degenerateCond}, leftNode.bitSet, midNode.bitSet, nil, nil)
	degenerateEdge.nonEQConds = append(degenerateEdge.nonEQConds, degenerateCond)
	crossEdge := detector.makeEdge(base.InnerJoin, []expression.Expression{crossCond}, midNode.bitSet, rightNode.bitSet, nil, nil)
	crossEdge.nonEQConds = append(crossEdge.nonEQConds, crossCond)

	checkResult, err := detector.CheckConnection(leftNode, midNode)
	require.NoError(t, err)
	require.True(t, checkResult.Connected())
	require.True(t, checkResult.NoEQEdge())
	require.False(t, checkResult.HasJoinCondition())

	checkResult, err = detector.CheckConnection(midNode, rightNode)
	require.NoError(t, err)
	require.True(t, checkResult.Connected())
	require.True(t, checkResult.NoEQEdge())
	require.True(t, checkResult.HasJoinCondition())

	componentIdxs, err := collectGreedySeedComponentIndices(detector, []*Node{leftNode, midNode, rightNode}, 0)
	require.NoError(t, err)
	require.Equal(t, []int{0}, componentIdxs)

	componentIdxs, err = collectGreedySeedComponentIndices(detector, []*Node{leftNode, midNode, rightNode}, 1)
	require.NoError(t, err)
	require.Equal(t, []int{1, 2}, componentIdxs)
}
