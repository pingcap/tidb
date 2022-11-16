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
	"bytes"
	"context"
	"fmt"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/util/set"
)

type outerJoinEliminator struct {
}

// tryToEliminateOuterJoin will eliminate outer join plan base on the following rules
//  1. outer join elimination: For example left outer join, if the parent only use the
//     columns from left table and the join key of right table(the inner table) is a unique
//     key of the right table. the left outer join can be eliminated.
//  2. outer join elimination with duplicate agnostic aggregate functions: For example left outer join.
//     If the parent only use the columns from left table with 'distinct' label. The left outer join can
//     be eliminated.
func (o *outerJoinEliminator) tryToEliminateOuterJoin(p *LogicalJoin, aggCols []*expression.Column, parentCols []*expression.Column, opt *logicalOptimizeOp) (LogicalPlan, bool, error) {
	var innerChildIdx int
	switch p.JoinType {
	case LeftOuterJoin:
		innerChildIdx = 1
	case RightOuterJoin:
		innerChildIdx = 0
	default:
		return p, false, nil
	}

	outerPlan := p.children[1^innerChildIdx]
	innerPlan := p.children[innerChildIdx]
	outerUniqueIDs := set.NewInt64Set()
	for _, outerCol := range outerPlan.Schema().Columns {
		outerUniqueIDs.Insert(outerCol.UniqueID)
	}
	matched := IsColsAllFromOuterTable(parentCols, outerUniqueIDs)
	if !matched {
		return p, false, nil
	}
	// outer join elimination with duplicate agnostic aggregate functions
	matched = IsColsAllFromOuterTable(aggCols, outerUniqueIDs)
	if matched {
		appendOuterJoinEliminateAggregationTraceStep(p, outerPlan, aggCols, opt)
		return outerPlan, true, nil
	}
	// outer join elimination without duplicate agnostic aggregate functions
	innerJoinKeys := o.extractInnerJoinKeys(p, innerChildIdx)
	contain, err := o.isInnerJoinKeysContainUniqueKey(innerPlan, innerJoinKeys)
	if err != nil {
		return p, false, err
	}
	if contain {
		appendOuterJoinEliminateTraceStep(p, outerPlan, parentCols, innerJoinKeys, opt)
		return outerPlan, true, nil
	}
	contain, err = o.isInnerJoinKeysContainIndex(innerPlan, innerJoinKeys)
	if err != nil {
		return p, false, err
	}
	if contain {
		appendOuterJoinEliminateTraceStep(p, outerPlan, parentCols, innerJoinKeys, opt)
		return outerPlan, true, nil
	}

	return p, false, nil
}

// extract join keys as a schema for inner child of a outer join
func (*outerJoinEliminator) extractInnerJoinKeys(join *LogicalJoin, innerChildIdx int) *expression.Schema {
	joinKeys := make([]*expression.Column, 0, len(join.EqualConditions))
	for _, eqCond := range join.EqualConditions {
		joinKeys = append(joinKeys, eqCond.GetArgs()[innerChildIdx].(*expression.Column))
	}
	return expression.NewSchema(joinKeys...)
}

// IsColsAllFromOuterTable check whether the cols all from outer plan
func IsColsAllFromOuterTable(cols []*expression.Column, outerUniqueIDs set.Int64Set) bool {
	// There are two cases "return false" here:
	// 1. If cols represents aggCols, then "len(cols) == 0" means not all aggregate functions are duplicate agnostic before.
	// 2. If cols represents parentCols, then "len(cols) == 0" means no parent logical plan of this join plan.
	if len(cols) == 0 {
		return false
	}
	for _, col := range cols {
		if !outerUniqueIDs.Exist(col.UniqueID) {
			return false
		}
	}
	return true
}

// check whether one of unique keys sets is contained by inner join keys
func (*outerJoinEliminator) isInnerJoinKeysContainUniqueKey(innerPlan LogicalPlan, joinKeys *expression.Schema) (bool, error) {
	for _, keyInfo := range innerPlan.Schema().Keys {
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
	return false, nil
}

// check whether one of index sets is contained by inner join index
func (*outerJoinEliminator) isInnerJoinKeysContainIndex(innerPlan LogicalPlan, joinKeys *expression.Schema) (bool, error) {
	ds, ok := innerPlan.(*DataSource)
	if !ok {
		return false, nil
	}
	for _, path := range ds.possibleAccessPaths {
		if path.IsIntHandlePath || !path.Index.Unique || len(path.IdxCols) == 0 {
			continue
		}
		joinKeysContainIndex := true
		for _, idxCol := range path.IdxCols {
			if !joinKeys.Contains(idxCol) {
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

// GetDupAgnosticAggCols checks whether a LogicalPlan is LogicalAggregation.
// It extracts all the columns from the duplicate agnostic aggregate functions.
// The returned column set is nil if not all the aggregate functions are duplicate agnostic.
// Only the following functions are considered to be duplicate agnostic:
//  1. MAX(arg)
//  2. MIN(arg)
//  3. FIRST_ROW(arg)
//  4. Other agg functions with DISTINCT flag, like SUM(DISTINCT arg)
func GetDupAgnosticAggCols(
	p LogicalPlan,
	oldAggCols []*expression.Column, // Reuse the original buffer.
) (isAgg bool, newAggCols []*expression.Column) {
	agg, ok := p.(*LogicalAggregation)
	if !ok {
		return false, nil
	}
	newAggCols = oldAggCols[:0]
	for _, aggDesc := range agg.AggFuncs {
		if !aggDesc.HasDistinct &&
			aggDesc.Name != ast.AggFuncFirstRow &&
			aggDesc.Name != ast.AggFuncMax &&
			aggDesc.Name != ast.AggFuncMin &&
			aggDesc.Name != ast.AggFuncApproxCountDistinct {
			// If not all aggregate functions are duplicate agnostic,
			// we should clean the aggCols, so `return true, newAggCols[:0]`.
			return true, newAggCols[:0]
		}
		for _, expr := range aggDesc.Args {
			newAggCols = append(newAggCols, expression.ExtractColumns(expr)...)
		}
	}
	return true, newAggCols
}

func (o *outerJoinEliminator) doOptimize(p LogicalPlan, aggCols []*expression.Column, parentCols []*expression.Column, opt *logicalOptimizeOp) (LogicalPlan, error) {
	var err error
	var isEliminated bool
	for join, isJoin := p.(*LogicalJoin); isJoin; join, isJoin = p.(*LogicalJoin) {
		p, isEliminated, err = o.tryToEliminateOuterJoin(join, aggCols, parentCols, opt)
		if err != nil {
			return p, err
		}
		if !isEliminated {
			break
		}
	}

	switch x := p.(type) {
	case *LogicalProjection:
		parentCols = parentCols[:0]
		for _, expr := range x.Exprs {
			parentCols = append(parentCols, expression.ExtractColumns(expr)...)
		}
	case *LogicalAggregation:
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

	if ok, newCols := GetDupAgnosticAggCols(p, aggCols); ok {
		aggCols = newCols
	}

	for i, child := range p.Children() {
		newChild, err := o.doOptimize(child, aggCols, parentCols, opt)
		if err != nil {
			return nil, err
		}
		p.SetChild(i, newChild)
	}
	return p, nil
}

func (o *outerJoinEliminator) optimize(_ context.Context, p LogicalPlan, opt *logicalOptimizeOp) (LogicalPlan, error) {
	p, err := o.doOptimize(p, nil, nil, opt)
	return p, err
}

func (*outerJoinEliminator) name() string {
	return "outer_join_eliminate"
}

func appendOuterJoinEliminateTraceStep(join *LogicalJoin, outerPlan LogicalPlan, parentCols []*expression.Column,
	innerJoinKeys *expression.Schema, opt *logicalOptimizeOp) {
	reason := func() string {
		buffer := bytes.NewBufferString("The columns[")
		for i, col := range parentCols {
			if i > 0 {
				buffer.WriteString(",")
			}
			buffer.WriteString(col.String())
		}
		buffer.WriteString("] are from outer table, and the inner join keys[")
		for i, key := range innerJoinKeys.Columns {
			if i > 0 {
				buffer.WriteString(",")
			}
			buffer.WriteString(key.String())
		}
		buffer.WriteString("] are unique")
		return buffer.String()
	}
	action := func() string {
		return fmt.Sprintf("Outer %v_%v is eliminated and become %v_%v", join.TP(), join.ID(), outerPlan.TP(), outerPlan.ID())
	}
	opt.appendStepToCurrent(join.ID(), join.TP(), reason, action)
}

func appendOuterJoinEliminateAggregationTraceStep(join *LogicalJoin, outerPlan LogicalPlan, aggCols []*expression.Column, opt *logicalOptimizeOp) {
	reason := func() string {
		buffer := bytes.NewBufferString("The columns[")
		for i, col := range aggCols {
			if i > 0 {
				buffer.WriteString(",")
			}
			buffer.WriteString(col.String())
		}
		buffer.WriteString("] in agg are from outer table, and the agg functions are duplicate agnostic")
		return buffer.String()
	}
	action := func() string {
		return fmt.Sprintf("Outer %v_%v is eliminated and become %v_%v", join.TP(), join.ID(), outerPlan.TP(), outerPlan.ID())
	}
	opt.appendStepToCurrent(join.ID(), join.TP(), reason, action)
}
