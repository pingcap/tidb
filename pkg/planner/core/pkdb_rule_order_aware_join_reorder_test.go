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
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/joinorder"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

// orderAwareCountingSource counts structural visits to a leaf outside the
// required-order path, including visits that annotation de-duplication hides.
type orderAwareCountingSource struct {
	*logicalop.DataSource
	childVisits int
}

func (p *orderAwareCountingSource) Children() []base.LogicalPlan {
	p.childVisits++
	return p.DataSource.Children()
}

func TestOrderAwareJoinReorderOrderingRegions(t *testing.T) {
	ctx := mock.NewContext().GetPlanCtx()
	newSource := func(name string, columnCount int, indexedColumn int) *logicalop.DataSource {
		ds := logicalop.DataSource{TableInfo: &model.TableInfo{}}.Init(ctx, 0)
		for range columnCount {
			ds.TblCols = append(ds.TblCols, &expression.Column{
				UniqueID: ctx.GetSessionVars().AllocPlanColumnID(),
				RetType:  types.NewFieldType(mysql.TypeLonglong),
			})
		}
		ds.SetSchema(expression.NewSchema(ds.TblCols...))
		names := make(types.NameSlice, columnCount)
		for i := range names {
			names[i] = &types.FieldName{TblName: pmodel.NewCIStr(name)}
		}
		ds.SetOutputNames(names)
		if indexedColumn >= 0 {
			ds.PossibleAccessPaths = []*plannerutil.AccessPath{{
				StoreType: kv.TiKV,
				Index: &model.IndexInfo{
					State: model.StatePublic,
					Tp:    pmodel.IndexTypeBtree,
				},
				FullIdxCols:    []*expression.Column{ds.TblCols[indexedColumn]},
				FullIdxColLens: []int{types.UnspecifiedLength},
			}}
		}
		return ds
	}
	newTopN := func(child base.LogicalPlan, column *expression.Column) *logicalop.LogicalTopN {
		topN := logicalop.LogicalTopN{
			ByItems: []*plannerutil.ByItems{{Expr: column}},
			Count:   5,
		}.Init(ctx, 0)
		topN.SetChildren(child)
		return topN
	}
	newJoin := func(left, right base.LogicalPlan) *logicalop.LogicalJoin {
		eq, err := expression.NewFunction(ctx.GetExprCtx(), ast.EQ, types.NewFieldType(mysql.TypeLonglong),
			left.Schema().Columns[0], right.Schema().Columns[0])
		require.NoError(t, err)
		join := logicalop.LogicalJoin{
			JoinType:        logicalop.InnerJoin,
			EqualConditions: []*expression.ScalarFunction{eq.(*expression.ScalarFunction)},
		}.Init(ctx, 0)
		join.SetChildren(left, right)
		join.SetSchema(expression.MergeSchema(left.Schema(), right.Schema()))
		return join
	}
	newDerived := func(child base.LogicalPlan) *logicalop.LogicalProjection {
		proj := logicalop.LogicalProjection{Exprs: expression.Column2Exprs(child.Schema().Columns)}.Init(ctx, 0)
		proj.SetChildren(child)
		proj.SetSchema(child.Schema().Clone())
		names := make(types.NameSlice, proj.Schema().Len())
		for i := range names {
			names[i] = &types.FieldName{TblName: pmodel.NewCIStr("derived")}
		}
		proj.SetOutputNames(names)
		return proj
	}

	t.Run("nested TopN cannot satisfy the outer order", func(t *testing.T) {
		ds := newSource("l", 2, 1)
		innerTopN := newTopN(ds, ds.TblCols[1])
		join := newJoin(newDerived(innerTopN), newSource("r", 1, -1))
		outerTopN := newTopN(join, ds.TblCols[0])

		found, err := (&orderAwareOptimizer{}).optimize(outerTopN)
		require.NoError(t, err)
		require.False(t, found)
	})

	t.Run("failed carrier preserves independent TopN in sibling", func(t *testing.T) {
		carrier := newSource("l", 1, -1)
		innerSource := newSource("inner_l", 1, 0)
		innerJoin := newJoin(innerSource, newSource("inner_r", 1, -1))
		innerTopN := newTopN(innerJoin, innerSource.TblCols[0])
		outerJoin := newJoin(carrier, newDerived(innerTopN))
		outerTopN := newTopN(outerJoin, carrier.TblCols[0])

		annotations := make([]*joinorder.OrderedLeadingAnnotation, 0)
		optimizer := &orderAwareOptimizer{}
		require.False(t, optimizer.collectForOrder(outerJoin,
			orderAwareContext{requiredOrder: []property.SortItem{{Col: carrier.TblCols[0]}}}, &annotations))
		require.Empty(t, annotations, "order validation must not discover independent regions")
		optimizer.collect(outerJoin, &annotations)
		require.Len(t, annotations, 1)
		require.Same(t, innerJoin, annotations[0].Anchor)

		found, err := (&orderAwareOptimizer{mode: orderAwareAnnotate}).optimize(outerTopN)
		require.NoError(t, err)
		require.True(t, found)
		require.True(t, innerJoin.InternalPreferJoinOrder)
		require.False(t, outerJoin.InternalPreferJoinOrder)
	})

	t.Run("nested carrier TopN is still optimized independently", func(t *testing.T) {
		ds := newSource("inner_l", 2, 1)
		innerJoin := newJoin(ds, newSource("inner_r", 1, -1))
		innerTopN := newTopN(innerJoin, ds.TblCols[1])
		outerJoin := newJoin(newDerived(innerTopN), newSource("r", 1, -1))
		outerTopN := newTopN(outerJoin, ds.TblCols[0])

		found, err := (&orderAwareOptimizer{mode: orderAwareAnnotate}).optimize(outerTopN)
		require.NoError(t, err)
		require.True(t, found)
		require.True(t, innerJoin.InternalPreferJoinOrder)
		require.False(t, outerJoin.InternalPreferJoinOrder)
	})

	t.Run("successful carrier preserves independent TopN in sibling", func(t *testing.T) {
		carrier := newSource("l", 1, 0)
		innerSource := newSource("inner_l", 1, 0)
		innerJoin := newJoin(innerSource, newSource("inner_r", 1, -1))
		innerTopN := newTopN(innerJoin, innerSource.TblCols[0])
		outerJoin := newJoin(carrier, newDerived(innerTopN))
		outerTopN := newTopN(outerJoin, carrier.TblCols[0])

		annotations := make([]*joinorder.OrderedLeadingAnnotation, 0)
		(&orderAwareOptimizer{}).collect(outerTopN, &annotations)
		require.Len(t, annotations, 2)
		require.ElementsMatch(t, []*logicalop.LogicalJoin{outerJoin, innerJoin},
			[]*logicalop.LogicalJoin{annotations[0].Anchor, annotations[1].Anchor})
	})

	t.Run("nested TopN regions are collected once", func(t *testing.T) {
		for _, depth := range []int{0, 1, 5, 15, 25} {
			t.Run(fmt.Sprintf("depth_%d", depth), func(t *testing.T) {
				carrier := newSource("l", 1, 0)
				right := &orderAwareCountingSource{DataSource: newSource("r", 1, -1)}
				baseJoin := newJoin(carrier, right)
				var root base.LogicalPlan = newTopN(baseJoin, carrier.TblCols[0])
				for level := 0; level < depth; level++ {
					join := newJoin(newDerived(root), newSource("outer_r", 1, -1))
					root = newTopN(join, carrier.TblCols[0])
				}

				annotations := make([]*joinorder.OrderedLeadingAnnotation, 0)
				right.childVisits = 0
				(&orderAwareOptimizer{}).collect(root, &annotations)
				// Check the raw list so late de-duplication cannot hide repeated work.
				require.Equal(t, 1, len(annotations))
				require.Same(t, baseJoin, annotations[0].Anchor)
				// Finding each region can inspect its descendants for join-group
				// eligibility. The leaf must still be visited only linearly in depth.
				require.LessOrEqual(t, right.childVisits, 4*(depth+1))
			})
		}
	})

	t.Run("ambiguous carrier hint does not schedule a round", func(t *testing.T) {
		carrier := newSource("duplicate", 1, 0)
		join := newJoin(carrier, newSource("duplicate", 1, -1))
		topN := newTopN(join, carrier.TblCols[0])

		_, _, err := (&orderAwareCandidateDetector{}).Optimize(context.Background(), topN, nil)
		require.NoError(t, err)
		require.False(t, ctx.GetSessionVars().StmtCtx.NeedAlternativeLogicalPlanOrderAwareJoinReorder())
		require.False(t, join.InternalPreferJoinOrder)
	})
}
