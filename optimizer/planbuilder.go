// Copyright 2015 PingCAP, Inc.
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

package optimizer

import (
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/optimizer/plan"
	"github.com/pingcap/tidb/parser/opcode"
)

func buildPlan(node ast.Node) plan.Plan {
	var builder planBuilder
	return builder.build(node)
}

// planBuilder builds Plan from an ast.Node.
// It just build the ast node straightforwardly.
type planBuilder struct {
}

func (b *planBuilder) build(node ast.Node) plan.Plan {
	switch x := node.(type) {
	case *ast.SelectStmt:
		return b.buildSelect(x)
	}
	panic("not supported")
}

func (b *planBuilder) buildSelect(sel *ast.SelectStmt) plan.Plan {
	var p plan.Plan
	if sel.From != nil {
		p = b.buildJoin(sel.From.TableRefs)
		if sel.Where != nil {
			p = b.buildFilter(p, sel.Where)
		}
		if sel.LockTp != ast.SelectLockNone {
			p = b.buildSelectLock(p, sel.LockTp)
		}
	}
	p = b.buildSelectFields(p, sel.GetResultFields())
	if sel.OrderBy != nil {
		p = b.buildSort(p, sel.OrderBy.Items)
	}
	if sel.Limit != nil {
		p = b.buildLimit(p, sel.Limit)
	}
	return p
}

func (b *planBuilder) buildJoin(from *ast.Join) plan.Plan {
	// Only support single table for now.
	ts, ok := from.Left.(*ast.TableSource)
	if !ok {
		panic("not supported")
	}
	tn, ok := ts.Source.(*ast.TableName)
	if !ok {
		panic("not supported")
	}
	p := &plan.TableScan{
		Table: tn.TableInfo,
	}
	p.SetFields(tn.GetResultFields())
	return p
}

// splitWhere split a where expression to a list of AND conditions.
func (b *planBuilder) splitWhere(where ast.ExprNode) []ast.ExprNode {
	var conditions []ast.ExprNode
	switch x := where.(type) {
	case *ast.BinaryOperationExpr:
		if x.Op == opcode.AndAnd {
			conditions = append(conditions, x.L)
			conditions = append(conditions, b.splitWhere(x.R)...)
		} else {
			conditions = append(conditions, x)
		}
	default:
		conditions = append(conditions, where)
	}
	return conditions
}

func (b *planBuilder) buildFilter(src plan.Plan, where ast.ExprNode) *plan.Filter {
	filter := &plan.Filter{
		Conditions: b.splitWhere(where),
	}
	filter.SetSrc(src)
	filter.SetFields(src.Fields())
	return filter
}

func (b *planBuilder) buildSelectLock(src plan.Plan, lock ast.SelectLockType) *plan.SelectLock {
	selectLock := &plan.SelectLock{
		Lock: lock,
	}
	selectLock.SetSrc(src)
	selectLock.SetFields(src.Fields())
	return selectLock
}

func (b *planBuilder) buildSelectFields(src plan.Plan, fields []*ast.ResultField) plan.Plan {
	selectFields := &plan.SelectFields{}
	selectFields.SetSrc(src)
	selectFields.SetFields(fields)
	return selectFields
}

func (b *planBuilder) buildSort(src plan.Plan, byItems []*ast.ByItem) plan.Plan {
	sort := &plan.Sort{
		ByItems: byItems,
	}
	sort.SetSrc(src)
	sort.SetFields(src.Fields())
	return sort
}

func (b *planBuilder) buildLimit(src plan.Plan, limit *ast.Limit) plan.Plan {
	li := &plan.Limit{
		Offset: limit.Offset,
		Count:  limit.Count,
	}
	li.SetSrc(src)
	li.SetFields(src.Fields())
	return li
}
