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
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	"github.com/pingcap/tidb/pkg/planner/core/stats"
	"github.com/pingcap/tidb/pkg/types"
)

// limitOneToMinMax rewrites a correlated scalar subquery
//
//	SELECT c FROM s WHERE s.p1 = outer.x AND ... ORDER BY s.p1, ..., c [DESC] LIMIT 1
//
// into SELECT MIN(c) (MAX(c) for DESC). Every ORDER BY item before c is fixed
// by an equality to a correlated column or a constant, so the first row in that
// order holds the smallest (largest) c. An empty input gives NULL either way:
// the Limit returns no row, which the outer join NULL-extends, and the
// aggregation returns one NULL row.
//
// A correlated Limit can't be decorrelated, so the subquery runs as an Apply,
// one lookup per outer row. The aggregation decorrelates into a join, and when
// it becomes an index join the inner side reads only the first row of each
// lookup range (see index_join_first_row.go). The rewrite is applied only when
// that will happen: an index on (p1, ..., c), c NOT NULL and not a string, no
// partitioning, and statistics showing large groups. Otherwise the index join
// would read every row of each group, which costs more than the Apply.
//
// The inner plan matched is [Projection(c)] -> Limit 1 -> Sort -> [Projection
// of plain columns]* -> Selection -> DataSource. It returns the new inner plan,
// a projection over the aggregation that keeps the inner plan's output column.
func limitOneToMinMax(apply *logicalop.LogicalApply, inner base.LogicalPlan) (base.LogicalPlan, bool) {
	if apply.JoinType != base.LeftOuterJoin ||
		len(apply.EqualConditions)+len(apply.NAEQConditions)+len(apply.LeftConditions)+
			len(apply.RightConditions)+len(apply.OtherConditions) > 0 {
		return nil, false
	}
	proj, _ := inner.(*logicalop.LogicalProjection)
	if proj != nil {
		inner = proj.Children()[0]
	}
	li, ok := inner.(*logicalop.LogicalLimit)
	if !ok || li.Count != 1 || li.Offset != 0 || len(li.PartitionBy) > 0 {
		return nil, false
	}
	sort, ok := li.Children()[0].(*logicalop.LogicalSort)
	if !ok || len(sort.ByItems) == 0 {
		return nil, false
	}
	last := sort.ByItems[len(sort.ByItems)-1]
	target, ok := last.Expr.(*expression.Column)
	if !ok {
		return nil, false
	}
	// The subquery must return c itself. Any other expression, even a constant,
	// would turn the NULL row from an empty aggregation into a value.
	outputs := expression.Column2Exprs(li.Schema().Columns)
	if proj != nil {
		outputs = proj.Exprs
	}
	for _, e := range outputs {
		if col, ok := e.(*expression.Column); !ok || !col.EqualColumn(target) {
			return nil, false
		}
	}

	// Map the sort columns down through plain-column projections to the
	// selection's input.
	below := sort.Children()[0]
	resolve := func(c *expression.Column) *expression.Column { return c }
	for {
		p, ok := below.(*logicalop.LogicalProjection)
		if !ok {
			break
		}
		outer := resolve
		schema, exprs := p.Schema(), p.Exprs
		for _, e := range exprs {
			if _, ok := e.(*expression.Column); !ok {
				return nil, false
			}
		}
		resolve = func(c *expression.Column) *expression.Column {
			c = outer(c)
			if c == nil {
				return nil
			}
			if idx := schema.ColumnIndex(c); idx >= 0 {
				return exprs[idx].(*expression.Column)
			}
			return nil
		}
		below = p.Children()[0]
	}
	sel, ok := below.(*logicalop.LogicalSelection)
	if !ok {
		return nil, false
	}
	ds, ok := sel.Children()[0].(*logicalop.DataSource)
	if !ok || ds.TableInfo.GetPartitionInfo() != nil {
		return nil, false
	}

	aggArg := resolve(target)
	if aggArg == nil || ds.Schema().ColumnIndex(aggArg) < 0 ||
		!mysql.HasNotNullFlag(aggArg.RetType.GetFlag()) || !firstRowOrderMatchesMinMax(aggArg.RetType.GetType()) {
		return nil, false
	}
	// Columns fixed per outer row by an equality, keyed by column ID. The value
	// is whether the equality is to a column of the outer side, which becomes a
	// join key. Any other correlated condition would keep the Apply, now over an
	// aggregation that reads the whole group, so it rules the rewrite out.
	outerSchema := apply.Children()[0].Schema()
	fixed := make(map[int64]bool)
	var groupBy []expression.Expression
	for _, cond := range sel.Conditions {
		col, other := fixedByEquality(cond)
		if col == nil || ds.Schema().ColumnIndex(col) < 0 {
			if len(expression.ExtractCorColumns(cond)) > 0 {
				return nil, false
			}
			continue
		}
		correlated := false
		if corCol, ok := other.(*expression.CorrelatedColumn); ok {
			if !outerSchema.Contains(&corCol.Column) {
				return nil, false
			}
			correlated = true
		}
		if correlated && !fixed[col.ID] {
			groupBy = append(groupBy, col)
		}
		fixed[col.ID] = fixed[col.ID] || correlated
	}
	for _, item := range sort.ByItems[:len(sort.ByItems)-1] {
		col, ok := item.Expr.(*expression.Column)
		if !ok {
			return nil, false
		}
		col = resolve(col)
		if col == nil || ds.Schema().ColumnIndex(col) < 0 {
			return nil, false
		}
		if _, ok := fixed[col.ID]; !ok {
			return nil, false
		}
	}
	// The index columns before the target must be exactly the columns the
	// correlated equalities fix. Then they are the join keys and the GROUP BY,
	// which is the shape the first-row read needs. A column fixed only by a
	// constant isn't a join key, and a correlated column outside the index
	// isn't part of the lookup range, and either way the index join would read
	// every row of each group.
	prefix := firstRowIndexPrefix(ds, fixed, len(groupBy), aggArg)
	if len(prefix) == 0 || len(prefix) != len(groupBy) {
		return nil, false
	}
	// The DataSource gets its statistics only at stats derivation, after this
	// rule. Column histograms are loaded for predicate columns later still, so
	// on a cold cache the check fails and the subquery stays an Apply.
	statsTbl := ds.StatisticTable
	if statsTbl == nil {
		statsTbl = stats.GetStatsTable(ds.SCtx(), ds.TableInfo, ds.PhysicalTableID)
	}
	if statsTbl == nil || !avgGroupRowsAreLarge(&statsTbl.HistColl, prefix) {
		return nil, false
	}

	name := ast.AggFuncMin
	if last.Desc {
		name = ast.AggFuncMax
	}
	sctx := li.SCtx()
	desc, err := aggregation.NewAggFuncDesc(sctx.GetExprCtx(), name, []expression.Expression{aggArg}, false)
	if err != nil {
		return nil, false
	}
	// Grouping by the columns the correlated equalities fix changes nothing per
	// outer row, where there is at most one such group, and an empty group gives
	// no row, which the outer join NULL-extends as it does for the Limit. It
	// makes decorrelation turn those equalities into join keys and keep the
	// aggregation on the inner side, instead of pulling it above the join where
	// it would read every row of every group.
	agg := logicalop.LogicalAggregation{AggFuncs: []*aggregation.AggFuncDesc{desc}, GroupByItems: groupBy}.Init(sctx, li.QueryBlockOffset())
	aggCol := &expression.Column{
		UniqueID: sctx.GetSessionVars().AllocPlanColumnID(),
		RetType:  desc.RetTp,
	}
	agg.SetSchema(expression.NewSchema(aggCol))
	agg.SetChildren(sel)
	if proj == nil {
		proj = logicalop.LogicalProjection{Exprs: make([]expression.Expression, li.Schema().Len())}.Init(sctx, li.QueryBlockOffset())
		proj.SetSchema(expression.NewSchema(li.Schema().Columns...))
	}
	for i := range proj.Exprs {
		proj.Exprs[i] = aggCol
	}
	proj.SetChildren(agg)
	return proj, true
}

// fixedByEquality matches a condition col = correlated column or col =
// constant, in either order, and returns col and the other side.
func fixedByEquality(cond expression.Expression) (*expression.Column, expression.Expression) {
	sf, ok := cond.(*expression.ScalarFunction)
	if !ok || sf.FuncName.L != ast.EQ {
		return nil, nil
	}
	args := sf.GetArgs()
	for i := range 2 {
		col, ok := args[i].(*expression.Column)
		if !ok {
			continue
		}
		switch args[1-i].(type) {
		case *expression.CorrelatedColumn, *expression.Constant:
			return col, args[1-i]
		}
	}
	return nil, nil
}

// firstRowIndexPrefix finds an index whose first n columns are all fixed by
// correlated equalities and whose next column is target, so that each value of
// those columns is one lookup range with target in order. It returns the IDs of
// the n leading columns, or nil if there is no such index.
func firstRowIndexPrefix(ds *logicalop.DataSource, fixed map[int64]bool, n int, target *expression.Column) []int64 {
	for _, path := range ds.AllPossibleAccessPaths {
		idx := path.Index
		if idx == nil || idx.MVIndex {
			continue
		}
		var prefix []int64
		for _, ic := range idx.Columns {
			if ic.Length != types.UnspecifiedLength {
				break
			}
			id := ds.TableInfo.Columns[ic.Offset].ID
			if id == target.ID {
				if len(prefix) == n {
					return prefix
				}
				break
			}
			if !fixed[id] {
				break
			}
			prefix = append(prefix, id)
		}
	}
	return nil
}
