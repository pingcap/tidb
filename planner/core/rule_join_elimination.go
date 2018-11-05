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
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/tidb/expression"
)

type outerJoinEliminator struct {
	// save the duplicate agnostic aggregate function's args in recursion
	cols [][]*expression.Column
	// save the LogicalJoin's parent schema in recursion
	schemas []*expression.Schema
}

// tryToEliminateOuterJoin will eliminate outer join plan base on the following rules
// 1. outer join elimination: For example left outer join, if the parent only use the
//    columns from left table and the join key of right table(the inner table) is a unique
//    key of the right table. the left outer join can be eliminated.
// 2. outer join elimination with duplicate agnostic aggregate functions: For example left outer join.
//    If the parent only use the columns from left table with 'distinct' label. The left outer join can
//    be eliminated.
func (o *outerJoinEliminator) tryToEliminateOuterJoin(p *LogicalJoin) LogicalPlan {
	switch p.JoinType {
	case LeftOuterJoin:
		return o.doEliminate(p, 1)
	case RightOuterJoin:
		return o.doEliminate(p, 0)
	default:
		return nil
	}
}

func (o *outerJoinEliminator) doEliminate(p *LogicalJoin, innerChildIdx int) LogicalPlan {
	// outer join elimination with duplicate agnostic aggregate functions
	if len(o.cols) > 0 {
		cols := o.cols[len(o.cols)-1]
		allColsInSchema := true
		for _, col := range cols {
			columnName := &ast.ColumnName{Schema: col.DBName, Table: col.TblName, Name: col.ColName}
			if c, _ := p.children[1^innerChildIdx].Schema().FindColumn(columnName); c == nil {
				allColsInSchema = false
				break
			}
		}
		if allColsInSchema == true {
			return p.children[1^innerChildIdx]
		}
	}

	// outer join elimination without duplicate agnostic aggregate functions
	// first, check whether the parent's schema columns are all in left or right
	if len(o.schemas) == 0 {
		return nil
	}
	for _, col := range o.schemas[len(o.schemas)-1].Columns {
		columnName := &ast.ColumnName{Schema: col.DBName, Table: col.TblName, Name: col.ColName}
		if c, _ := p.children[1^innerChildIdx].Schema().FindColumn(columnName); c == nil {
			return nil
		}
	}
	// second, check whether the other side join keys are unique keys
	p.children[innerChildIdx].buildKeyInfo()
	var joinKeys []*expression.Column
	for _, eqCond := range p.EqualConditions {
		joinKeys = append(joinKeys, eqCond.GetArgs()[innerChildIdx].(*expression.Column))
	}
	tmpSchema := expression.NewSchema(joinKeys...)
	for _, keyInfo := range p.children[innerChildIdx].Schema().Keys {
		joinKeysContainKeyInfo := true
		for _, col := range keyInfo {
			columnName := &ast.ColumnName{Schema: col.DBName, Table: col.TblName, Name: col.ColName}
			if c, _ := tmpSchema.FindColumn(columnName); c == nil {
				joinKeysContainKeyInfo = false
				break
			}
		}
		if joinKeysContainKeyInfo {
			return p.children[1^innerChildIdx]
		}
	}
	// Third, if p.children[innerChildIdx] is datasource, we must check specially index.
	// Because buildKeyInfo() don't save the index without notnull flag.
	// But in outer join, null==null don't return true. The null index do not affect the res.
	if ds, ok := p.children[innerChildIdx].(*DataSource); ok {
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
				if c, _ := tmpSchema.FindColumn(columnName); c == nil {
					joinKeysContainIndex = false
					break
				}
			}
			if joinKeysContainIndex {
				return p.children[1^innerChildIdx]
			}
		}
	}
	return nil
}

// Check whether a LogicalPlan is a LogicalAggregation and its all aggregate functions is duplicate aggnostic.
// Also, check all the args are expression.Column.
func (o *outerJoinEliminator) isDuplicateAgnosticAgg(p LogicalPlan) (isDuplicateAgnosticAgg bool, cols []*expression.Column) {
	if agg, ok := p.(*LogicalAggregation); ok {
		isDuplicateAgnosticAgg = true
		for _, aggDesc := range agg.AggFuncs {
			if aggDesc.Name != ast.AggFuncFirstRow &&
				aggDesc.Name != ast.AggFuncMax &&
				aggDesc.Name != ast.AggFuncMin &&
				(aggDesc.Name != ast.AggFuncAvg || !aggDesc.HasDistinct) &&
				(aggDesc.Name != ast.AggFuncSum || !aggDesc.HasDistinct) &&
				(aggDesc.Name != ast.AggFuncCount || !aggDesc.HasDistinct) {
				isDuplicateAgnosticAgg = false
				break
			}
			for _, expr := range aggDesc.Args {
				if col, ok := expr.(*expression.Column); ok {
					cols = append(cols, col)
				} else {
					isDuplicateAgnosticAgg = false
					break
				}
			}
			if !isDuplicateAgnosticAgg {
				break
			}
		}
		return
	}
	return false, nil
}

func (o *outerJoinEliminator) optimize(p LogicalPlan) (LogicalPlan, error) {
	// check the duplicate agnostic aggregate functions
	if ok, cols := o.isDuplicateAgnosticAgg(p); ok {
		o.cols = append(o.cols, cols)
		defer func() {
			o.cols = o.cols[:len(o.cols)-1]
		}()
	}

	newChildren := make([]LogicalPlan, 0, len(p.Children()))
	for _, child := range p.Children() {
		// if child is logical join, then save the parent schema
		if _, ok := child.(*LogicalJoin); ok {
			o.schemas = append(o.schemas, p.Schema())
			defer func() {
				o.schemas = o.schemas[:len(o.schemas)-1]
			}()
		}
		newChild, _ := o.optimize(child)
		newChildren = append(newChildren, newChild)
	}
	p.SetChildren(newChildren...)
	join, ok := p.(*LogicalJoin)
	if !ok {
		return p, nil
	}
	if proj := o.tryToEliminateOuterJoin(join); proj != nil {
		return proj, nil
	}
	return p, nil
}
