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
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"context"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/util/set"
)

type outerJoinEliminator struct {
}

// tryToEliminateOuterJoin will eliminate outer join plan base on the following rules
// 1. outer join elimination: For example left outer join, if the parent only use the
//    columns from left table and the join key of right table(the inner table) is a unique
//    key of the right table. the left outer join can be eliminated.
// 2. outer join elimination with duplicate agnostic aggregate functions: For example left outer join.
//    If the parent only use the columns from left table with 'distinct' label. The left outer join can
//    be eliminated.
func (o *outerJoinEliminator) tryToEliminateOuterJoin(p *LogicalJoin, aggCols []*expression.Column, parentCols []*expression.Column) (LogicalPlan, bool, error) {
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
	matched := o.isColsAllFromOuterTable(parentCols, outerUniqueIDs)
	if !matched {
		return p, false, nil
	}
	// outer join elimination with duplicate agnostic aggregate functions
	matched = o.isColsAllFromOuterTable(aggCols, outerUniqueIDs)
	if matched {
		return outerPlan, true, nil
	}
	// outer join elimination without duplicate agnostic aggregate functions
	innerJoinKeys := o.extractInnerJoinKeys(p, innerChildIdx)
	contain, err := o.isInnerJoinKeysContainUniqueKey(innerPlan, innerJoinKeys)
	if err != nil {
		return p, false, err
	}
	if contain {
		return outerPlan, true, nil
	}
	contain, err = o.isInnerJoinKeysContainIndex(innerPlan, innerJoinKeys)
	if err != nil {
		return p, false, err
	}
	if contain {
		return outerPlan, true, nil
	}

	return p, false, nil
}

// extract join keys as a schema for inner child of a outer join
func (o *outerJoinEliminator) extractInnerJoinKeys(join *LogicalJoin, innerChildIdx int) *expression.Schema {
	joinKeys := make([]*expression.Column, 0, len(join.EqualConditions))
	for _, eqCond := range join.EqualConditions {
		joinKeys = append(joinKeys, eqCond.GetArgs()[innerChildIdx].(*expression.Column))
	}
	return expression.NewSchema(joinKeys...)
}

// check whether the cols all from outer plan
func (o *outerJoinEliminator) isColsAllFromOuterTable(cols []*expression.Column, outerUniqueIDs set.Int64Set) bool {
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
func (o *outerJoinEliminator) isInnerJoinKeysContainUniqueKey(innerPlan LogicalPlan, joinKeys *expression.Schema) (bool, error) {
	for _, keyInfo := range innerPlan.Schema().Keys {
		joinKeysContainKeyInfo := true
		for _, col := range keyInfo {
			columnName := &ast.ColumnName{Schema: col.DBName, Table: col.TblName, Name: col.ColName}
			c, err := joinKeys.FindColumn(columnName)
			if err != nil {
				return false, err
			}
			if c == nil {
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
func (o *outerJoinEliminator) isInnerJoinKeysContainIndex(innerPlan LogicalPlan, joinKeys *expression.Schema) (bool, error) {
	ds, ok := innerPlan.(*DataSource)
	if !ok {
		return false, nil
	}
	for _, path := range ds.possibleAccessPaths {
		if path.isTablePath {
			continue
		}
		idx := path.index
		if !idx.Unique {
			continue
		}
		joinKeysContainIndex := true
		for _, idxCol := range idx.Columns {
			columnName := &ast.ColumnName{Schema: ds.DBName, Table: ds.tableInfo.Name, Name: idxCol.Name}
			c, err := joinKeys.FindColumn(columnName)
			if err != nil {
				return false, err
			}
			if c == nil {
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

// getDupAgnosticAggCols checks whether a LogicalPlan is LogicalAggregation.
// It extracts all the columns from the duplicate agnostic aggregate functions.
// The returned column set is nil if not all the aggregate functions are duplicate agnostic.
// Only the following functions are considered to be duplicate agnostic:
//   1. MAX(arg)
//   2. MIN(arg)
//   3. FIRST_ROW(arg)
//   4. Other agg functions with DISTINCT flag, like SUM(DISTINCT arg)
func (o *outerJoinEliminator) getDupAgnosticAggCols(
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
			aggDesc.Name != ast.AggFuncMin {
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

func (o *outerJoinEliminator) doOptimize(p LogicalPlan, aggCols []*expression.Column, parentCols []*expression.Column) (LogicalPlan, error) {
	var err error
	var isEliminated bool
	for join, isJoin := p.(*LogicalJoin); isJoin; join, isJoin = p.(*LogicalJoin) {
		p, isEliminated, err = o.tryToEliminateOuterJoin(join, aggCols, parentCols)
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
		}
	default:
		parentCols = append(parentCols[:0], p.Schema().Columns...)
	}

	if ok, newCols := o.getDupAgnosticAggCols(p, aggCols); ok {
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

func (o *outerJoinEliminator) optimize(ctx context.Context, p LogicalPlan) (LogicalPlan, error) {
	return o.doOptimize(p, nil, nil)
}

func (*outerJoinEliminator) name() string {
	return "outer_join_eliminate"
}
