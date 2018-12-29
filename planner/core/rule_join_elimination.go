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
	log "github.com/sirupsen/logrus"
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
func (o *outerJoinEliminator) tryToEliminateOuterJoin(p *LogicalJoin, aggCols []*expression.Column, parentSchema *expression.Schema) LogicalPlan {
	var innerChildIdx int
	switch p.JoinType {
	case LeftOuterJoin:
		innerChildIdx = 1
	case RightOuterJoin:
		innerChildIdx = 0
	default:
		return p
	}

	outerPlan := p.children[1^innerChildIdx]
	innerPlan := p.children[innerChildIdx]
	// outer join elimination with duplicate agnostic aggregate functions
	if o.isAggColsAllFromOuterTable(outerPlan, aggCols) {
		return outerPlan
	}
	// outer join elimination without duplicate agnostic aggregate functions
	if !o.isParentColsAllFromOuterTable(outerPlan, parentSchema) {
		return p
	}
	innerJoinKeys := o.extractInnerJoinKeys(p, innerChildIdx)
	if o.isInnerJoinKeysContainUniqueKey(innerPlan, innerJoinKeys) {
		return outerPlan
	}
	if o.isInnerJoinKeysContainIndex(innerPlan, innerJoinKeys) {
		return outerPlan
	}
	return p
}

// extract join keys as a schema for inner child of a outer join
func (o *outerJoinEliminator) extractInnerJoinKeys(join *LogicalJoin, innerChildIdx int) *expression.Schema {
	var joinKeys []*expression.Column
	for _, eqCond := range join.EqualConditions {
		joinKeys = append(joinKeys, eqCond.GetArgs()[innerChildIdx].(*expression.Column))
	}
	return expression.NewSchema(joinKeys...)
}

func (o *outerJoinEliminator) isAggColsAllFromOuterTable(outerPlan LogicalPlan, aggCols []*expression.Column) bool {
	if len(aggCols) == 0 {
		return false
	}
	for _, col := range aggCols {
		columnName := &ast.ColumnName{Schema: col.DBName, Table: col.TblName, Name: col.ColName}
		if c, err := outerPlan.Schema().FindColumn(columnName); c == nil {
			log.Warn(err)
			return false
		}
	}
	return true
}

// check whether schema cols of join's parent plan are all from outer join table
func (o *outerJoinEliminator) isParentColsAllFromOuterTable(outerPlan LogicalPlan, parentSchema *expression.Schema) bool {
	if parentSchema == nil {
		return false
	}
	for _, col := range parentSchema.Columns {
		columnName := &ast.ColumnName{Schema: col.DBName, Table: col.TblName, Name: col.ColName}
		if c, err := outerPlan.Schema().FindColumn(columnName); c == nil {
			log.Warn(err)
			return false
		}
	}
	return true
}

// check whether one of unique keys sets is contained by inner join keys
func (o *outerJoinEliminator) isInnerJoinKeysContainUniqueKey(innerPlan LogicalPlan, joinKeys *expression.Schema) bool {
	for _, keyInfo := range innerPlan.Schema().Keys {
		joinKeysContainKeyInfo := true
		for _, col := range keyInfo {
			columnName := &ast.ColumnName{Schema: col.DBName, Table: col.TblName, Name: col.ColName}
			if c, err := joinKeys.FindColumn(columnName); c == nil {
				log.Warn(err)
				joinKeysContainKeyInfo = false
				break
			}
		}
		if joinKeysContainKeyInfo {
			return true
		}
	}
	return false
}

// check whether one of index sets is contained by inner join index
func (o *outerJoinEliminator) isInnerJoinKeysContainIndex(innerPlan LogicalPlan, joinKeys *expression.Schema) bool {
	ds, ok := innerPlan.(*DataSource)
	if !ok {
		return false
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
			if c, err := joinKeys.FindColumn(columnName); c == nil {
				log.Warn(err)
				joinKeysContainIndex = false
				break
			}
		}
		if joinKeysContainIndex {
			return true
		}
	}
	return false
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
			if col, ok := expr.(*expression.Column); ok {
				cols = append(cols, col)
			} else {
				return false, nil
			}
		}
	}
	return true, cols
}

func (o *outerJoinEliminator) doOptimize(p LogicalPlan, aggCols []*expression.Column, parentSchema *expression.Schema) LogicalPlan {
	// check the duplicate agnostic aggregate functions
	if ok, newCols := o.isDuplicateAgnosticAgg(p); ok {
		aggCols = newCols
	}

	newChildren := make([]LogicalPlan, 0, len(p.Children()))
	for _, child := range p.Children() {
		newChild := o.doOptimize(child, aggCols, p.Schema())
		newChildren = append(newChildren, newChild)
	}
	p.SetChildren(newChildren...)
	join, isJoin := p.(*LogicalJoin)
	if !isJoin {
		return p
	}
	return o.tryToEliminateOuterJoin(join, aggCols, parentSchema)
}

func (o *outerJoinEliminator) optimize(p LogicalPlan) (LogicalPlan, error) {
	return o.doOptimize(p, nil, nil), nil
}
