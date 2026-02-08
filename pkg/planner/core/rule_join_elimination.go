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

package core

import (
	"context"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	ruleutil "github.com/pingcap/tidb/pkg/planner/core/rule/util"
	coreusage "github.com/pingcap/tidb/pkg/planner/util/coreusage"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util/intset"
)

// OuterJoinEliminator is used to eliminate outer join.
type OuterJoinEliminator struct {
}

// appendUniqueCorrelatedCols appends correlated columns to the target slice, avoiding duplicates.
func appendUniqueCorrelatedCols(target []*expression.Column, corCols []*expression.CorrelatedColumn) []*expression.Column {
	if len(corCols) == 0 {
		return target
	}
	seen := make(map[int64]struct{})
	for _, cc := range corCols {
		uid := cc.Column.UniqueID
		if _, ok := seen[uid]; ok {
			continue
		}
		seen[uid] = struct{}{}
		target = append(target, &cc.Column)
	}
	return target
}

// tryToEliminateOuterJoin will eliminate outer join plan base on the following rules
//  1. outer join elimination: For example left outer join, if the parent doesn't use the
//     columns from right table and the join key of right table(the inner table) is a unique
//     key of the right table. the left outer join can be eliminated.
//  2. outer join elimination with duplicate agnostic aggregate functions: For example left outer join.
//     If the parent only use the columns from left table with 'distinct' label. The left outer join can
//     be eliminated.
func (o *OuterJoinEliminator) tryToEliminateOuterJoin(p *logicalop.LogicalJoin, aggCols []*expression.Column, parentCols []*expression.Column) (base.LogicalPlan, bool, error) {
	var innerChildIdx int
	switch p.JoinType {
	case base.LeftOuterJoin:
		innerChildIdx = 1
	case base.RightOuterJoin:
		innerChildIdx = 0
	default:
		return p, false, nil
	}

	outerPlan := p.Children()[1^innerChildIdx]
	innerPlan := p.Children()[innerChildIdx]

	// in case of count(*) FROM R LOJ S, the parentCols is empty, but
	// still need to proceed to check whether we can eliminate outer join.
	// In fact, we only care about whether there is any column from inner
	// table, if there is none, we are good.
	if len(parentCols) > 0 {
		outerUniqueIDs := intset.NewFastIntSet()
		for _, outerCol := range outerPlan.Schema().Columns {
			outerUniqueIDs.Insert(int(outerCol.UniqueID))
		}
		matched := ruleutil.IsColsAllFromOuterTable(parentCols, &outerUniqueIDs)
		if !matched {
			return p, false, nil
		}
	}

	if len(aggCols) > 0 {
		innerUniqueIDs := intset.NewFastIntSet()
		for _, innerCol := range innerPlan.Schema().Columns {
			innerUniqueIDs.Insert(int(innerCol.UniqueID))
		}
		// Check if any column is from the inner table.
		// If any column is from the inner table, we cannot eliminate the outer join.
		innerFound := ruleutil.IsColFromInnerTable(aggCols, &innerUniqueIDs)
		if !innerFound {
			return outerPlan, true, nil
		}
	}
	// outer join elimination without duplicate agnostic aggregate functions
	innerJoinKeys, innerNullEQKeys := o.extractInnerJoinKeys(p, innerChildIdx)
	contain, err := o.isInnerJoinKeysContainUniqueKey(innerPlan, innerJoinKeys, innerNullEQKeys)
	if err != nil {
		return p, false, err
	}
	if contain {
		return outerPlan, true, nil
	}
	contain, err = o.isInnerJoinKeysContainIndex(innerPlan, innerJoinKeys, innerNullEQKeys)
	if err != nil {
		return p, false, err
	}
	if contain {
		return outerPlan, true, nil
	}

	return p, false, nil
}

// extract join keys as a schema for inner child of a outer join, and record which inner join keys use NullEQ (<=>).
func (*OuterJoinEliminator) extractInnerJoinKeys(join *logicalop.LogicalJoin, innerChildIdx int) (*expression.Schema, intset.FastIntSet) {
	joinKeys := make([]*expression.Column, 0, len(join.EqualConditions))
	innerNullEQKeys := intset.NewFastIntSet()
	for _, eqCond := range join.EqualConditions {
		innerKey := eqCond.GetArgs()[innerChildIdx].(*expression.Column)
		joinKeys = append(joinKeys, innerKey)
		if eqCond.FuncName.L == ast.NullEQ {
			innerNullEQKeys.Insert(int(innerKey.UniqueID))
		}
	}
	return expression.NewSchema(joinKeys...), innerNullEQKeys
}

// check whether one of unique keys sets is contained by inner join keys
func (*OuterJoinEliminator) isInnerJoinKeysContainUniqueKey(innerPlan base.LogicalPlan, joinKeys *expression.Schema, innerNullEQKeys intset.FastIntSet) (bool, error) {
	for _, keyInfo := range innerPlan.Schema().PKOrUK {
		joinKeysContainKeyInfo := true
		for _, col := range keyInfo {
			if !joinKeys.Contains(col) {
				joinKeysContainKeyInfo = false
				break
			}
		}
		if joinKeysContainKeyInfo {
			return true, nil
		}
	}
	for _, keyInfo := range innerPlan.Schema().NullableUK {
		joinKeysContainKeyInfo := true
		for _, col := range keyInfo {
			if !joinKeys.Contains(col) {
				joinKeysContainKeyInfo = false
				break
			}
			if innerNullEQKeys.Has(int(col.UniqueID)) {
				joinKeysContainKeyInfo = false
				break
			}
		}
		if joinKeysContainKeyInfo {
			return true, nil
		}
	}
	return false, nil
}

// check whether one of index sets is contained by inner join index
func (*OuterJoinEliminator) isInnerJoinKeysContainIndex(innerPlan base.LogicalPlan, joinKeys *expression.Schema, innerNullEQKeys intset.FastIntSet) (bool, error) {
	ds, ok := innerPlan.(*logicalop.DataSource)
	if !ok {
		return false, nil
	}
	for _, path := range ds.AllPossibleAccessPaths {
		if path.IsIntHandlePath || !path.Index.Unique || len(path.IdxCols) == 0 {
			continue
		}
		joinKeysContainIndex := true
		for _, idxCol := range path.IdxCols {
			if !joinKeys.Contains(idxCol) {
				joinKeysContainIndex = false
				break
			}
			if !mysql.HasNotNullFlag(idxCol.RetType.GetFlag()) && innerNullEQKeys.Has(int(idxCol.UniqueID)) {
				joinKeysContainIndex = false
				break
			}
		}
		if joinKeysContainIndex {
			return true, nil
		}
	}
	return false, nil
}

func (o *OuterJoinEliminator) doOptimize(p base.LogicalPlan, aggCols []*expression.Column, parentCols []*expression.Column) (base.LogicalPlan, error) {
	// CTE's logical optimization is independent.
	if _, ok := p.(*logicalop.LogicalCTE); ok {
		return p, nil
	}
	var err error
	var isEliminated bool
	for join, isJoin := p.(*logicalop.LogicalJoin); isJoin; join, isJoin = p.(*logicalop.LogicalJoin) {
		p, isEliminated, err = o.tryToEliminateOuterJoin(join, aggCols, parentCols)
		if err != nil {
			return p, err
		}
		if !isEliminated {
			break
		}
	}

	switch x := p.(type) {
	case *logicalop.LogicalJoin:
		parentCols = parentCols[:0]
		parentCols = append(parentCols, x.Schema().Columns...)
		if x.JoinType == base.LeftOuterJoin || x.JoinType == base.RightOuterJoin {
			outerIdx := 0
			if x.JoinType == base.RightOuterJoin {
				outerIdx = 1
			}
			outerSchema := x.Children()[outerIdx].Schema()
			if join, ok := x.Children()[outerIdx].(*logicalop.LogicalJoin); ok && join.FullSchema != nil {
				outerSchema = join.FullSchema
			}
			condCols := make([]*expression.Column, 0)
			condCols = append(condCols, expression.ExtractColumnsFromExpressions(x.OtherConditions, nil)...)
			condCols = append(condCols, expression.ExtractColumnsFromExpressions(x.LeftConditions, nil)...)
			condCols = append(condCols, expression.ExtractColumnsFromExpressions(x.RightConditions, nil)...)
			condCols = append(condCols, expression.ExtractColumnsFromExpressions(expression.ScalarFuncs2Exprs(x.EqualConditions), nil)...)
			condCols = append(condCols, expression.ExtractColumnsFromExpressions(expression.ScalarFuncs2Exprs(x.NAEQConditions), nil)...)
			for _, col := range condCols {
				if outerSchema != nil && outerSchema.Contains(col) {
					parentCols = append(parentCols, col)
				}
			}
		}
	case *logicalop.LogicalApply:
		// TODO: this is tied to variable tidb_opt_enable_no_decorrelate_in_select
		// to enable outer join elimination when correlated subqueries exist in the select
		// list. Future enhancement can remove the variable check.
		x.SCtx().GetSessionVars().RecordRelevantOptVar(vardef.TiDBOptEnableNoDecorrelateInSelect)
		if x.SCtx().GetSessionVars().EnableNoDecorrelateInSelect {
			// For Apply, only columns from the left child are visible to the parent at this layer.
			// Filter incoming parentCols to left child's schema and also include correlated columns
			// from the right child that reference the left schema (required by subqueries).
			leftSchema := x.Children()[0].Schema()
			filtered := make([]*expression.Column, 0, len(parentCols))
			for _, col := range parentCols {
				if leftSchema.Contains(col) {
					filtered = append(filtered, col)
				}
			}
			// Add correlated columns from the right child that map to the left schema
			corCols := coreusage.ExtractCorColumnsBySchema4LogicalPlan(x.Children()[1], leftSchema)
			filtered = appendUniqueCorrelatedCols(filtered, corCols)
			parentCols = filtered
		} else {
			parentCols = append(parentCols[:0], p.Schema().Columns...)
		}
	case *logicalop.LogicalProjection:
		parentCols = parentCols[:0]
		for _, expr := range x.Exprs {
			parentCols = append(parentCols, expression.ExtractColumns(expr)...)
		}
		// Include columns required by subqueries (appear as correlated columns in child subtree)
		// TODO: this is tied to variable tidb_opt_enable_no_decorrelate_in_select
		// to enable outer join elimination when correlated subqueries exist in the select
		// list. Future enhancement can remove the variable check.
		if len(x.Children()) > 0 {
			x.SCtx().GetSessionVars().RecordRelevantOptVar(vardef.TiDBOptEnableNoDecorrelateInSelect)
			if x.SCtx().GetSessionVars().EnableNoDecorrelateInSelect {
				corCols := coreusage.ExtractCorrelatedCols4LogicalPlan(x.Children()[0])
				parentCols = appendUniqueCorrelatedCols(parentCols, corCols)
			}
		}
	case *logicalop.LogicalAggregation:
		parentCols = parentCols[:0]
		for _, groupByItem := range x.GroupByItems {
			parentCols = append(parentCols, expression.ExtractColumns(groupByItem)...)
		}
		for _, aggDesc := range x.AggFuncs {
			for _, expr := range aggDesc.Args {
				parentCols = append(parentCols, expression.ExtractColumns(expr)...)
			}
			for _, byItem := range aggDesc.OrderByItems {
				parentCols = append(parentCols, expression.ExtractColumns(byItem.Expr)...)
			}
		}
	default:
		parentCols = append(parentCols[:0], p.Schema().Columns...)
	}

	if ok, newCols := logicalop.GetDupAgnosticAggCols(p, aggCols); ok {
		aggCols = newCols
	}

	for i, child := range p.Children() {
		newChild, err := o.doOptimize(child, aggCols, parentCols)
		if err != nil {
			return nil, err
		}
		p.SetChild(i, newChild)
	}
	return p, nil
}

// Optimize implements base.LogicalOptRule.<0th> interface.
func (o *OuterJoinEliminator) Optimize(_ context.Context, p base.LogicalPlan) (base.LogicalPlan, bool, error) {
	planChanged := false
	p, err := o.doOptimize(p, nil, nil)
	return p, planChanged, err
}

// Name implements base.LogicalOptRule.<1st> interface.
func (*OuterJoinEliminator) Name() string {
	return "outer_join_eliminate"
}
