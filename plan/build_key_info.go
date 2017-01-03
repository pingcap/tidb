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
	"github.com/pingcap/tidb/expression"
)

func (p *Aggregation) buildKeyInfo() {
	p.baseLogicalPlan.buildKeyInfo()
	schema := expression.NewSchema(make([]*expression.Column, 0, p.schema.Len()), nil)
	for _, fun := range p.AggFuncs {
		if col, isCol := fun.GetArgs()[0].(*expression.Column); isCol && fun.GetName() == ast.AggFuncFirstRow {
			schema.Append(col)
		} else {
			schema.Append(&expression.Column{
				FromID:   "",
				Position: -1})
		}
	}
	for _, key := range p.GetChildren()[0].GetSchema().Keys {
		ok := true
		newKey := make([]*expression.Column, 0, len(key))
		for _, keyCol := range key {
			pos := schema.GetColumnIndex(keyCol)
			if pos == -1 {
				ok = false
				break
			} else {
				newKey = append(newKey, p.schema.Columns[pos])
			}
		}
		if ok {
			p.schema.Keys = append(p.schema.Keys, newKey)
		}
	}
}

func (p *Projection) buildKeyInfo() {
	p.baseLogicalPlan.buildKeyInfo()
	schema := expression.NewSchema(make([]*expression.Column, 0, p.schema.Len()), nil)
	for _, expr := range p.Exprs {
		if col, isCol := expr.(*expression.Column); isCol {
			schema.Append(col)
		} else {
			schema.Append(&expression.Column{
				FromID:   "",
				Position: -1})
		}
	}
	for _, key := range p.GetChildren()[0].GetSchema().Keys {
		ok := true
		newKey := make([]*expression.Column, 0, len(key))
		for _, keyCol := range key {
			pos := schema.GetColumnIndex(keyCol)
			if pos == -1 {
				ok = false
				break
			} else {
				newKey = append(newKey, p.schema.Columns[pos])
			}
		}
		if ok {
			p.schema.Keys = append(p.schema.Keys, newKey)
		}
	}
}

func (p *Trim) buildKeyInfo() {
	p.baseLogicalPlan.buildKeyInfo()
	for _, key := range p.GetSchema().Keys {
		ok := false
		newKey := make([]*expression.Column, 0, len(key))
		for _, col := range key {
			pos := p.schema().GetColumnIndex(col)
			if pos == -1 {
				ok = false
				break
			}
			newKey = append(newKey, p.schema.Columns[pos])
		}
		if ok {
			p.schema.Keys = append(p.schema.Keys, newKey)
		}
	}
}

func (p *Join) buildKeyInfo() {
	p.baseLogicalPlan.buildKeyInfo()
	switch p.JoinType {
	case SemiJoin, SemiJoinWithAux:
		p.schema.Keys = p.children[0].GetSchema().Clone().Keys
	case InnerJoin, LeftOuterJoin, RightOuterJoin:
		p.schema.Keys = append(p.children[0].GetSchema().Clone().Keys, p.children[1].GetSchema().Clone().Keys...)
	}
}

func (p *Selection) buildKeyInfo() {
	p.baseLogicalPlan.buildKeyInfo()
	p.schema.Keys = p.children[0].GetSchema().Clone().Keys
}

func (p *Distinct) buildKeyInfo() {
	p.baseLogicalPlan.buildKeyInfo()
	p.schema.Keys = p.children[0].GetSchema().Clone().Keys
}

func (p *Union) buildKeyInfo() {
	p.baseLogicalPlan.buildKeyInfo()
	p.schema.Keys = nil
}

func (p *Sort) buildKeyInfo() {
	p.baseLogicalPlan.buildKeyInfo()
	p.schema.Keys = p.children[0].GetSchema().Clone().Keys
}

func (p *Limit) buildKeyInfo() {
	p.baseLogicalPlan.buildKeyInfo()
	p.schema.Keys = p.children[0].GetSchema().Clone().Keys
}

func (p *DataSource) buildKeyInfo() {
	p.baseLogicalPlan.buildKeyInfo()
	indices, _ := availableIndices(p.indexHints, p.tableInfo)
	for _, idx := range indices {
		if !idx.Unique {
			continue
		}
		newKey := make([]*expression.Column, 0, len(idx.Columns))
		ok := true
		for _, idxCol := range idx.Columns {
			find := false
			for i, col := range p.schema.Columns {
				if idxCol.Name.L == col.ColName.L {
					newKey = append(newKey, p.schema.Columns[i])
					find = true
					break
				}
			}
			if ok = ok && find; !ok {
				break
			}
		}
		if ok {
			p.schema.Keys = append(p.schema.Keys, newKey)
		}
	}
}

func (p *Apply) buildKeyInfo() {
	p.baseLogicalPlan.buildKeyInfo()
	p.schema.Keys = append(p.children[0].GetSchema().Clone().Keys, p.children[1].GetSchema().Clone().Keys...)
}

func (p *Exists) buildKeyInfo() {
	p.baseLogicalPlan.buildKeyInfo()
	p.schema.Keys = nil
}

func (p *MaxOneRow) buildKeyInfo() {
	p.baseLogicalPlan.buildKeyInfo()
	p.schema.Keys = p.children[0].GetSchema().Clone().Keys
}

func (p *Update) buildKeyInfo() {
	p.baseLogicalPlan.buildKeyInfo()
	p.schema.Keys = p.children[0].GetSchema().Clone().Keys
}

func (p *SelectLock) buildKeyInfo() {
	p.baseLogicalPlan.buildKeyInfo()
	p.schema.Keys = p.children[0].GetSchema().Keys
}
