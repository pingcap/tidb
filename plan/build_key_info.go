// Copyright 2017 PingCAP, Inc.
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

package plan

import (
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/mysql"
)

type buildKeySolver struct{}

func (s *buildKeySolver) optimize(lp LogicalPlan, _ context.Context) (LogicalPlan, error) {
	lp.buildKeyInfo()
	return lp, nil
}

func (p *LogicalAggregation) buildKeyInfo() {
	p.baseLogicalPlan.buildKeyInfo()
	for _, key := range p.Children()[0].Schema().Keys {
		indices := p.schema.ColumnsIndices(key)
		if indices == nil {
			continue
		}
		newKey := make([]*expression.Column, 0, len(key))
		for _, i := range indices {
			newKey = append(newKey, p.schema.Columns[i])
		}
		p.schema.Keys = append(p.schema.Keys, newKey)
	}
	if len(p.groupByCols) == len(p.GroupByItems) && len(p.GroupByItems) > 0 {
		indices := p.schema.ColumnsIndices(p.groupByCols)
		if indices != nil {
			newKey := make([]*expression.Column, 0, len(indices))
			for _, i := range indices {
				newKey = append(newKey, p.schema.Columns[i])
			}
			p.schema.Keys = append(p.schema.Keys, newKey)
		}
	}
	if len(p.GroupByItems) == 0 {
		p.schema.MaxOneRow = true
	}
}

// If a condition is the form of (uniqueKey = constant) or (uniqueKey = Correlated column), it returns at most one row.
// This function will check it.
func (p *LogicalSelection) checkMaxOneRowCond(unique expression.Expression, constOrCorCol expression.Expression) bool {
	col, ok := unique.(*expression.Column)
	if !ok {
		return false
	}
	if !p.children[0].Schema().IsUniqueKey(col) {
		return false
	}
	_, okCon := constOrCorCol.(*expression.Constant)
	if okCon {
		return true
	}
	_, okCorCol := constOrCorCol.(*expression.CorrelatedColumn)
	return okCorCol
}

func (p *LogicalSelection) buildKeyInfo() {
	p.baseLogicalPlan.buildKeyInfo()
	p.schema.MaxOneRow = p.children[0].Schema().MaxOneRow
	for _, cond := range p.Conditions {
		if sf, ok := cond.(*expression.ScalarFunction); ok && sf.FuncName.L == ast.EQ {
			if p.checkMaxOneRowCond(sf.GetArgs()[0], sf.GetArgs()[1]) || p.checkMaxOneRowCond(sf.GetArgs()[1], sf.GetArgs()[0]) {
				p.schema.MaxOneRow = true
				break
			}
		}
	}
}

// A bijection exists between columns of a projection's schema and this projection's Exprs.
// Sometimes we need a schema made by expr of Exprs to convert a column in child's schema to a column in this projection's Schema.
func (p *LogicalProjection) buildSchemaByExprs() *expression.Schema {
	schema := expression.NewSchema(make([]*expression.Column, 0, p.schema.Len())...)
	for _, expr := range p.Exprs {
		if col, isCol := expr.(*expression.Column); isCol {
			schema.Append(col)
		} else {
			// If the expression is not a column, we add a column to occupy the position.
			schema.Append(&expression.Column{
				Position: -1, RetType: expr.GetType()})
		}
	}
	return schema
}

func (p *LogicalProjection) buildKeyInfo() {
	p.baseLogicalPlan.buildKeyInfo()
	p.schema.MaxOneRow = p.children[0].Schema().MaxOneRow
	schema := p.buildSchemaByExprs()
	for _, key := range p.Children()[0].Schema().Keys {
		indices := schema.ColumnsIndices(key)
		if indices == nil {
			continue
		}
		newKey := make([]*expression.Column, 0, len(key))
		for _, i := range indices {
			newKey = append(newKey, p.schema.Columns[i])
		}
		p.schema.Keys = append(p.schema.Keys, newKey)
	}
}

func (p *LogicalJoin) buildKeyInfo() {
	p.baseLogicalPlan.buildKeyInfo()
	p.schema.MaxOneRow = p.children[0].Schema().MaxOneRow && p.children[1].Schema().MaxOneRow
	switch p.JoinType {
	case SemiJoin, LeftOuterSemiJoin, AntiSemiJoin, AntiLeftOuterSemiJoin:
		p.schema.Keys = p.children[0].Schema().Clone().Keys
	case InnerJoin, LeftOuterJoin, RightOuterJoin:
		// If there is no equal conditions, then cartesian product can't be prevented and unique key information will destroy.
		if len(p.EqualConditions) == 0 {
			return
		}
		lOk := false
		rOk := false
		// Such as 'select * from t1 join t2 where t1.a = t2.a and t1.b = t2.b'.
		// If one sides (a, b) is a unique key, then the unique key information is remained.
		// But we don't consider this situation currently.
		// Only key made by one column is considered now.
		for _, expr := range p.EqualConditions {
			ln := expr.GetArgs()[0].(*expression.Column)
			rn := expr.GetArgs()[1].(*expression.Column)
			for _, key := range p.children[0].Schema().Keys {
				if len(key) == 1 && key[0].Equal(ln, p.ctx) {
					lOk = true
					break
				}
			}
			for _, key := range p.children[1].Schema().Keys {
				if len(key) == 1 && key[0].Equal(rn, p.ctx) {
					rOk = true
					break
				}
			}
		}
		// For inner join, if one side of one equal condition is unique key,
		// another side's unique key information will all be reserved.
		// If it's an outer join, NULL value will fill some position, which will destroy the unique key information.
		if lOk && p.JoinType != LeftOuterJoin {
			p.schema.Keys = append(p.schema.Keys, p.children[1].Schema().Keys...)
		}
		if rOk && p.JoinType != RightOuterJoin {
			p.schema.Keys = append(p.schema.Keys, p.children[0].Schema().Keys...)
		}
	}
}

func (p *DataSource) buildKeyInfo() {
	p.baseLogicalPlan.buildKeyInfo()
	indices := p.availableIndices.indices
	for _, idx := range indices {
		if !idx.Unique {
			continue
		}
		newKey := make([]*expression.Column, 0, len(idx.Columns))
		ok := true
		for _, idxCol := range idx.Columns {
			// The columns of this index should all occur in column schema.
			// Since null value could be duplicate in unique key. So we check NotNull flag of every column.
			find := false
			for i, col := range p.schema.Columns {
				if idxCol.Name.L == col.ColName.L {
					if !mysql.HasNotNullFlag(p.Columns[i].Flag) {
						break
					}
					newKey = append(newKey, p.schema.Columns[i])
					find = true
					break
				}
			}
			if !find {
				ok = false
				break
			}
		}
		if ok {
			p.schema.Keys = append(p.schema.Keys, newKey)
		}
	}
	if p.tableInfo.PKIsHandle {
		for i, col := range p.Columns {
			if mysql.HasPriKeyFlag(col.Flag) {
				p.schema.Keys = append(p.schema.Keys, []*expression.Column{p.schema.Columns[i]})
				break
			}
		}
	}
}
