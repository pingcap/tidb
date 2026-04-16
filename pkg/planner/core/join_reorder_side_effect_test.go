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

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util/coretestsdk"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func newLegacyTypedLeaf(ctx base.PlanContext, tp byte, count float64) *logicalop.LogicalTableDual {
	dual := logicalop.LogicalTableDual{RowCount: 1}.Init(ctx, 0)
	dual.SetSchema(expression.NewSchema())
	dual.Schema().Append(&expression.Column{
		UniqueID: ctx.GetSessionVars().PlanColumnID.Add(1),
		RetType:  types.NewFieldType(tp),
	})
	dual.SetStats(&property.StatsInfo{RowCount: count})
	return dual
}

func TestGreedyConnectivityProbeKeepsProjectionLeafImmutable(t *testing.T) {
	ctx := coretestsdk.MockContext()
	defer domain.GetDomain(ctx).StatsHandle().Close()

	stringLeaf := newLegacyTypedLeaf(ctx, mysql.TypeVarchar, 1)
	intLeaf := newLegacyTypedLeaf(ctx, mysql.TypeLonglong, 1)

	// Simulate a projected join key that originally participated in a col=col
	// equality edge. When the legacy greedy connectivity probe flips the pair
	// direction, the rebuilt equality injects helper expressions. The probe must
	// not mutate the original projection leaf in place.
	projectedKey := &expression.Column{
		UniqueID: ctx.GetSessionVars().PlanColumnID.Add(1),
		RetType:  types.NewFieldType(mysql.TypeLonglong),
	}
	projectionLeaf := logicalop.LogicalProjection{
		Exprs: []expression.Expression{stringLeaf.Schema().Columns[0]},
	}.Init(ctx, 0)
	projectionLeaf.SetSchema(expression.NewSchema(projectedKey))
	projectionLeaf.SetChildren(stringLeaf)
	projectionLeaf.SetStats(&property.StatsInfo{RowCount: stringLeaf.StatsInfo().RowCount})

	eqCond, ok := expression.NewFunctionInternal(ctx.GetExprCtx(), ast.EQ, types.NewFieldType(mysql.TypeTiny),
		projectedKey, intLeaf.Schema().Columns[0]).(*expression.ScalarFunction)
	require.True(t, ok)

	// Reversing the join side now requires casts, which exercises the helper
	// projection path during speculative join construction.
	projectedKey.RetType = types.NewFieldType(mysql.TypeVarchar)

	solver := &joinReorderGreedySolver{
		allInnerJoin: true,
		baseSingleGroupJoinOrderSolver: &baseSingleGroupJoinOrderSolver{
			ctx: ctx,
			basicJoinGroupInfo: &basicJoinGroupInfo{
				eqEdges: []*expression.ScalarFunction{eqCond},
				joinTypes: []*joinTypeWithExtMsg{{
					JoinType: base.InnerJoin,
				}},
			},
		},
	}

	origExprCnt := len(projectionLeaf.Exprs)
	origSchemaLen := projectionLeaf.Schema().Len()
	probe := solver.probeConnection(intLeaf, projectionLeaf)
	require.True(t, probe.HasEQEdge())
	require.True(t, probe.HasJoinCondition())
	require.False(t, probe.IsCartesian())
	require.Equal(t, base.InnerJoin, probe.joinType.JoinType)
	require.Same(t, intLeaf, probe.leftPlan)
	require.Same(t, projectionLeaf, probe.rightPlan)
	require.Len(t, projectionLeaf.Exprs, origExprCnt)
	require.Equal(t, origSchemaLen, projectionLeaf.Schema().Len())

	join, remainOtherConds, err := solver.buildJoinFromProbe(probe)
	require.NoError(t, err)
	require.Empty(t, remainOtherConds)
	_, _, err = join.RecursiveDeriveStats(nil)
	require.NoError(t, err)
	logicalJoin, ok := join.(*logicalop.LogicalJoin)
	require.True(t, ok)
	clonedRight, ok := logicalJoin.Children()[1].(*logicalop.LogicalProjection)
	require.True(t, ok)
	require.NotSame(t, projectionLeaf, clonedRight)
	require.Len(t, clonedRight.Exprs, origExprCnt+1)
	require.Equal(t, origSchemaLen+1, clonedRight.Schema().Len())
	require.NotNil(t, clonedRight.StatsInfo())
	_, ok = clonedRight.StatsInfo().ColNDVs[clonedRight.Schema().Columns[origSchemaLen].UniqueID]
	require.True(t, ok)
	require.Len(t, projectionLeaf.Exprs, origExprCnt)
	require.Equal(t, origSchemaLen, projectionLeaf.Schema().Len())
}
