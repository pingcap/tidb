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

package plan

import (
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/parser/opcode"
)

// BuildPlan builds a plan from a node.
func BuildPlan(node ast.Node) Plan {
	var builder planBuilder
	p := builder.build(node)
	refine(p)
	return p
}

// planBuilder builds Plan from an ast.Node.
// It just build the ast node straightforwardly.
type planBuilder struct {
}

func (b *planBuilder) build(node ast.Node) Plan {
	switch x := node.(type) {
	case *ast.SelectStmt:
		return b.buildSelect(x)
	}
	log.Fatalf("Unsupported type %T", node)
	return nil
}

func (b *planBuilder) buildSelect(sel *ast.SelectStmt) Plan {
	var p Plan
	if sel.From != nil {
		p = b.buildJoin(sel.From.TableRefs)
		if sel.Where != nil {
			p = b.buildFilter(p, sel.Where)
		}
		if sel.LockTp != ast.SelectLockNone {
			p = b.buildSelectLock(p, sel.LockTp)
		}
		p = b.buildSelectFields(p, sel.GetResultFields())
	} else {
		p = b.buildSelectFields(p, sel.GetResultFields())
		if sel.Where != nil {
			p = b.buildFilter(p, sel.Where)
		}
	}
	if sel.OrderBy != nil {
		p = b.buildSort(p, sel.OrderBy.Items)
	}
	if sel.Limit != nil {
		p = b.buildLimit(p, sel.Limit)
	}
	return p
}

func (b *planBuilder) buildJoin(from *ast.Join) Plan {
	// Only support single table for now.
	ts, ok := from.Left.(*ast.TableSource)
	if !ok {
		log.Fatalf("Unsupported type %T", from.Left)
	}
	tn, ok := ts.Source.(*ast.TableName)
	if !ok {
		log.Fatalf("Unsupported type %T", ts.Source)
	}
	p := &TableScan{
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

func (b *planBuilder) buildFilter(src Plan, where ast.ExprNode) *Filter {
	filter := &Filter{
		Conditions: b.splitWhere(where),
	}
	filter.SetSrc(src)
	filter.SetFields(src.Fields())
	return filter
}

func (b *planBuilder) buildSelectLock(src Plan, lock ast.SelectLockType) *SelectLock {
	selectLock := &SelectLock{
		Lock: lock,
	}
	selectLock.SetSrc(src)
	selectLock.SetFields(src.Fields())
	return selectLock
}

func (b *planBuilder) buildSelectFields(src Plan, fields []*ast.ResultField) Plan {
	selectFields := &SelectFields{}
	selectFields.SetSrc(src)
	selectFields.SetFields(fields)
	return selectFields
}

func (b *planBuilder) buildSort(src Plan, byItems []*ast.ByItem) Plan {
	sort := &Sort{
		ByItems: byItems,
	}
	sort.SetSrc(src)
	sort.SetFields(src.Fields())
	return sort
}

func (b *planBuilder) buildLimit(src Plan, limit *ast.Limit) Plan {
	li := &Limit{
		Offset: limit.Offset,
		Count:  limit.Count,
	}
	li.SetSrc(src)
	li.SetFields(src.Fields())
	return li
}
