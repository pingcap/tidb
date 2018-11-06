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
}

// tryToEliminateOuterJoin will eliminate outer join plan base on the following rules
// 1. outer join elimination: For example left outer join, if the parent only use the
//    columns from left table and the join key of right table(the inner table) is a unique
//    key of the right table. the left outer join can be eliminated.
// 2. outer join elimination with duplicate agnostic aggregate functions: For example left outer join.
//    If the parent only use the columns from left table with 'distinct' label. The left outer join can
//    be eliminated.
func (o *outerJoinEliminator) tryToEliminateOuterJoin(p *LogicalJoin,
	cols []*expression.Column, schema *expression.Schema) LogicalPlan {
	switch p.JoinType {
	case LeftOuterJoin:
		return o.doEliminate(p, 1, cols, schema)
	case RightOuterJoin:
		return o.doEliminate(p, 0, cols, schema)
	default:
		return nil
	}
}

func (o *outerJoinEliminator) doEliminate(p *LogicalJoin, innerChildIdx int,
	cols []*expression.Column, schema *expression.Schema) LogicalPlan {
	// outer join elimination with duplicate agnostic aggregate functions
	if len(cols) > 0 {
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
	if schema == nil {
		return nil
	}
	for _, col := range schema.Columns {
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

// Check whether a LogicalPlan is a LogicalAggregation and its all aggregate functions is duplicate agnostic.
// Also, check all the args are expression.Column.
func (o *outerJoinEliminator) isDuplicateAgnosticAgg(p LogicalPlan) (isDuplicateAgnosticAgg bool, cols []*expression.Column) {
	if agg, ok := p.(*LogicalAggregation); ok {
		isDuplicateAgnosticAgg = true
		cols = agg.groupByCols
		for _, aggDesc := range agg.AggFuncs {
			if !aggDesc.HasDistinct &&
				aggDesc.Name != ast.AggFuncFirstRow &&
				aggDesc.Name != ast.AggFuncMax &&
				aggDesc.Name != ast.AggFuncMin {
				return false, nil
			}
			for _, expr := range aggDesc.Args {
				if col, ok := expr.(*expression.Column); ok {
					cols = append(cols, col)
				} else {
					return false, nil
				}
			}
		}
		return
	}
	return false, nil
}

func (o *outerJoinEliminator) doOptimize(p LogicalPlan, cols []*expression.Column, schema *expression.Schema) (LogicalPlan, error) {
	oldCols, oldSchema := cols, schema
	// check the duplicate agnostic aggregate functions
	if ok, newCols := o.isDuplicateAgnosticAgg(p); ok {
		cols = newCols
	}

	newChildren := make([]LogicalPlan, 0, len(p.Children()))
	for _, child := range p.Children() {
		// if child is logical join, then save the parent schema
		if _, ok := child.(*LogicalJoin); ok {
			schema = p.Schema()
		}
		newChild, _ := o.doOptimize(child, cols, schema)
		newChildren = append(newChildren, newChild)
	}
	cols, schema = oldCols, oldSchema
	p.SetChildren(newChildren...)
	join, ok := p.(*LogicalJoin)
	if !ok {
		return p, nil
	}
	if proj := o.tryToEliminateOuterJoin(join, cols, schema); proj != nil {
		return proj, nil
	}
	return p, nil
}

func (o *outerJoinEliminator) optimize(p LogicalPlan) (LogicalPlan, error) {
	return o.doOptimize(p, make([]*expression.Column, 0, 0), nil)
}
