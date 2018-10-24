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
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/expression"
)

type outerJoinEliminator struct {
	hasDistinct int
	cols        [][]*expression.Column
	schemas     []*expression.Schema
}

// tryToEliminateOuterJoin will eliminate outer join plan base on the following rules
// 1. outer join elimination: For example left outer join, if the parent only use the
//    columns from left table and the join key of right table(the inner table) is a unique
//    key of the right table. the left outer join can be eliminated.
// 2. outer join elimination with distinct: For example left outer join. If the parent
//    only use the columns from left table with 'distinct' label. The left outer join can
//    be eliminated.
func (o *outerJoinEliminator) tryToEliminateOuterJoin(p *LogicalJoin) LogicalPlan {
	if len(p.children) != 2 {
		return nil
	}
	switch p.JoinType {
	case LeftOuterJoin:
		return o.doEliminate(p, 1)
	case RightOuterJoin:
		return o.doEliminate(p, 0)
	default:
		return nil
	}
}

func (o *outerJoinEliminator) doEliminate(p *LogicalJoin, isLeft int) LogicalPlan {
	// outer join elimination with distinct
	if o.hasDistinct > 0 {
		cols := o.cols[o.hasDistinct-1]
		allColsInSchema := true
		for _, col := range cols {
			if !p.children[1^isLeft].Schema().Contains(col) {
				allColsInSchema = false
				break
			}
		}
		if allColsInSchema == true {
			return p.children[1^isLeft]
		}
	}

	// outer join elimination without distinct
	// first, check whether the parent's schema columns are all in left or right
	if len(o.schemas) < 1 {
		return nil
	}
	for _, col := range o.schemas[len(o.schemas)-1].Columns {
		columnName := &ast.ColumnName{Schema: col.DBName, Table: col.TblName, Name: col.ColName}
		if c, _ := p.children[1^isLeft].Schema().FindColumn(columnName); c == nil {
			return nil
		}
	}
	// second, check whether the other side join keys are unique keys
	for _, keyInfo := range p.children[isLeft].Schema().Keys {
		keyInfoContainAllCols := true
		var joinKeys []*expression.Column
		if isLeft > 0 {
			joinKeys = p.RightJoinKeys
		} else {
			joinKeys = p.LeftJoinKeys
		}
		for _, col := range joinKeys {
			columnName := &ast.ColumnName{Schema: col.DBName, Table: col.TblName, Name: col.ColName}
			if c, _ := expression.NewSchema(keyInfo...).FindColumn(columnName); c == nil {
				keyInfoContainAllCols = false
				break
			}
		}
		if keyInfoContainAllCols {
			return p.children[1^isLeft]
		}
	}

	return nil
}

func (o *outerJoinEliminator) optimize(p LogicalPlan) (LogicalPlan, error) {
	// check the distinct
	if agg, ok := p.(*LogicalAggregation); ok && len(agg.groupByCols) > 0 {
		isDistinctAgg := true
		for _, aggDesc := range agg.AggFuncs {
			if aggDesc.Name != ast.AggFuncFirstRow {
				isDistinctAgg = false
				break
			}
			if len(aggDesc.Args) != 1 {
				isDistinctAgg = false
				break
			}
			if _, ok := aggDesc.Args[0].(*expression.Column); !ok {
				isDistinctAgg = false
				break
			}
		}
		if isDistinctAgg {
			o.hasDistinct++
			o.cols = append(o.cols, agg.groupByCols)
			defer func() {
				o.hasDistinct--
				o.cols = o.cols[:o.hasDistinct]
			}()
		}
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
