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
	"fmt"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/expression"
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
	for _, col := range aggCols {
		fmt.Println("aggCols", *col)
	}
	for _, col := range parentCols {
		fmt.Println("parentCols", *col)
	}

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
	// outer join elimination with duplicate agnostic aggregate functions
	matched := o.isAggColsAllFromOuterTable(outerPlan, aggCols)
	if matched {
		return outerPlan, true, nil
	}
	// outer join elimination without duplicate agnostic aggregate functions
	matched = o.isParentColsAllFromOuterTable(outerPlan, parentCols)
	if !matched {
		return p, false, nil
	}
	innerJoinKeys := o.extractInnerJoinKeys(p, innerChildIdx)
	contain, err := o.isInnerJoinKeysContainUniqueKey(innerPlan, innerJoinKeys)
	if err != nil || contain {
		return outerPlan, true, err
	}
	contain, err = o.isInnerJoinKeysContainIndex(innerPlan, innerJoinKeys)
	if err != nil || contain {
		return outerPlan, true, err
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

func (o *outerJoinEliminator) isAggColsAllFromOuterTable(outerPlan LogicalPlan, aggCols []*expression.Column) bool {
	if len(aggCols) == 0 {
		return false
	}
	for _, aggCol := range aggCols {
		isAggColInOuterSchema := false
		for _, outerCol := range outerPlan.Schema().Columns {
			if aggCol.UniqueID == outerCol.UniqueID {
				isAggColInOuterSchema = true
				break
			}
		}
		if !isAggColInOuterSchema {
			return false
		}
	}
	return true
}

// check whether schema cols of join's parent plan are all from outer join table
func (o *outerJoinEliminator) isParentColsAllFromOuterTable(outerPlan LogicalPlan, parentCols []*expression.Column) bool {
	if len(parentCols) == 0 {
		return false
	}
	for _, parentCol := range parentCols {
		isParentColInOuterSchema := false
		for _, outerCol := range outerPlan.Schema().Columns {
			if parentCol.DBName == outerCol.DBName && parentCol.TblName == outerCol.TblName && parentCol.OrigColName == outerCol.OrigColName {
				isParentColInOuterSchema = true
				break
			}
		}
		if !isParentColInOuterSchema {
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

// Check whether a LogicalPlan is a LogicalAggregation and its all aggregate functions is duplicate agnostic.
// Also, check all the args are expression.Column.
func (o *outerJoinEliminator) isDuplicateAgnosticAgg(p LogicalPlan) (_ bool, cols []*expression.Column) {
	agg, ok := p.(*LogicalAggregation)
	if !ok {
		return false, nil
	}
	cols = agg.groupByCols
	for _, aggDesc := range agg.AggFuncs {
		if !aggDesc.HasDistinct &&
			aggDesc.Name != ast.AggFuncFirstRow &&
			aggDesc.Name != ast.AggFuncMax &&
			aggDesc.Name != ast.AggFuncMin {
			return false, nil
		}
		for _, expr := range aggDesc.Args {
			// ExtractColumns will trans the expr to cols.
			cols = append(cols, expression.ExtractColumns(expr)...)
		}
	}
	return true, cols
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
		aggCols = aggCols[:0]
	}

	var colsInSchema []*expression.Column
	switch x := p.(type) {
	case *LogicalProjection:
		for _, expr := range x.Exprs {
			colsInSchema = append(colsInSchema, expression.ExtractColumns(expr)...)
		}
		for i := len(aggCols) - 1; i >= 0; i-- {
			idx := p.Schema().ColumnIndex(aggCols[i])
			if idx == -1 {
				aggCols = append(aggCols[:i], aggCols[i+1:]...)
				continue
			}
			if col, ok := x.Exprs[idx].(*expression.Column); ok {
				aggCols[i] = col
			} else {
				aggCols = append(aggCols[:i], aggCols[i+1:]...)
			}
		}
	default:
		colsInSchema = p.Schema().Columns
	}

	// check the duplicate agnostic aggregate functions
	if ok, newCols := o.isDuplicateAgnosticAgg(p); ok {
		aggCols = newCols
	} else if len(aggCols) > 0 {
		aggCols = append(aggCols, colsInSchema...)
	}

	newChildren := make([]LogicalPlan, 0, len(p.Children()))
	for _, child := range p.Children() {
		newChild, err := o.doOptimize(child, aggCols, colsInSchema)
		if err != nil {
			return nil, err
		}
		newChildren = append(newChildren, newChild)
	}
	p.SetChildren(newChildren...)
	return p, nil
}

func (o *outerJoinEliminator) optimize(p LogicalPlan) (LogicalPlan, error) {
	return o.doOptimize(p, nil, []*expression.Column{})
}
