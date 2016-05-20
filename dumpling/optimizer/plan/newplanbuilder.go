// Copyright 2016 PingCAP, Inc.
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
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/parser/opcode"
)

// UseNewPlanner means if use the new planner.
var UseNewPlanner = false

func (b *planBuilder) buildNewSinglePathPlan(node ast.ResultSetNode) Plan {
	switch x := node.(type) {
	case *ast.Join:
		return b.buildNewJoin(x)
	case *ast.TableSource:
		switch v := x.Source.(type) {
		case *ast.SelectStmt:
			return b.buildNewSelect(v)
		case *ast.UnionStmt:
			return b.buildUnion(v)
		case *ast.TableName:
			//TODO: select physical algorithm during cbo phase.
			return b.buildNewTableScanPlan(v)
		default:
			b.err = ErrUnsupportedType.Gen("unsupported table source type %T", v)
			return nil
		}
	default:
		b.err = ErrUnsupportedType.Gen("unsupported table source type %T", x)
		return nil
	}
}

func fromFields(col *ast.ColumnNameExpr, fields []*ast.ResultField) bool {
	for _, field := range fields {
		if field == col.Refer {
			return true
		}
	}
	return false
}

type columnsExtractor struct {
	result []*ast.ColumnNameExpr
}

func (ce *columnsExtractor) Enter(expr ast.Node) (ret ast.Node, skipChildren bool) {
	switch v := expr.(type) {
	case *ast.ColumnNameExpr:
		ce.result = append(ce.result, v)
	}
	return expr, false
}

func (ce *columnsExtractor) Leave(expr ast.Node) (ret ast.Node, skipChildren bool) {
	return expr, true
}

func extractOnCondition(conditions []ast.ExprNode, left Plan, right Plan) (eqCond []ast.ExprNode, leftCond []ast.ExprNode, rightCond []ast.ExprNode, otherCond []ast.ExprNode) {
	for _, expr := range conditions {
		binop, ok := expr.(*ast.BinaryOperationExpr)
		if ok && binop.Op == opcode.EQ {
			ln, lOK := binop.L.(*ast.ColumnNameExpr)
			rn, rOK := binop.R.(*ast.ColumnNameExpr)
			if lOK && rOK {
				if fromFields(ln, left.Fields()) && fromFields(rn, right.Fields()) {
					eqCond = append(eqCond, expr)
					continue
				} else if fromFields(rn, left.Fields()) && fromFields(ln, right.Fields()) {
					eqCond = append(eqCond, &ast.BinaryOperationExpr{Op: opcode.EQ, L: rn, R: ln})
					continue
				}
			}
		}
		ce := &columnsExtractor{}
		expr.Accept(ce)
		columns := ce.result
		allFromLeft, allFromRight := true, true
		for _, col := range columns {
			if fromFields(col, left.Fields()) {
				allFromRight = false
			} else {
				allFromLeft = false
			}
		}
		if allFromRight {
			rightCond = append(rightCond, expr)
		} else if allFromLeft {
			leftCond = append(leftCond, expr)
		} else {
			otherCond = append(otherCond, expr)
		}
	}
	return eqCond, leftCond, rightCond, otherCond
}

func (b *planBuilder) buildNewJoin(join *ast.Join) Plan {
	if join.Right == nil {
		return b.buildNewSinglePathPlan(join.Left)
	}
	leftPlan := b.buildNewSinglePathPlan(join.Left)
	rightPlan := b.buildNewSinglePathPlan(join.Right)
	var eqCond, leftCond, rightCond, otherCond []ast.ExprNode
	if join.On != nil {
		onCondition := splitWhere(join.On.Expr)
		eqCond, leftCond, rightCond, otherCond = extractOnCondition(onCondition, leftPlan, rightPlan)
	}
	joinPlan := &Join{EqualConditions: eqCond, LeftConditions: leftCond, RightConditions: rightCond, OtherConditions: otherCond}
	if join.Tp == ast.LeftJoin {
		joinPlan.JoinType = LeftOuterJoin
	} else if join.Tp == ast.RightJoin {
		joinPlan.JoinType = RightOuterJoin
	} else {
		joinPlan.JoinType = InnerJoin
	}
	addChild(joinPlan, leftPlan)
	addChild(joinPlan, rightPlan)
	joinPlan.SetFields(append(leftPlan.Fields(), rightPlan.Fields()...))
	return joinPlan
}

func (b *planBuilder) buildFilter(p Plan, where ast.ExprNode) Plan {
	conditions := splitWhere(where)
	filter := &Filter{Conditions: conditions}
	addChild(filter, p)
	filter.SetFields(p.Fields())
	return filter
}

func (b *planBuilder) buildNewSelect(sel *ast.SelectStmt) Plan {
	var aggFuncs []*ast.AggregateFuncExpr
	hasAgg := b.detectSelectAgg(sel)
	if hasAgg {
		aggFuncs = b.extractSelectAgg(sel)
	}
	// Build subquery
	// Convert subquery to expr with plan
	b.buildSubquery(sel)
	var p Plan
	if sel.From != nil {
		p = b.buildNewSinglePathPlan(sel.From.TableRefs)
		if sel.Where != nil {
			p = b.buildFilter(p, sel.Where)
		}
		if b.err != nil {
			return nil
		}
		if sel.LockTp != ast.SelectLockNone {
			p = b.buildSelectLock(p, sel.LockTp)
			if b.err != nil {
				return nil
			}
		}
		if hasAgg {
			p = b.buildAggregate(p, aggFuncs, sel.GroupBy)
		}
		p = b.buildSelectFields(p, sel.GetResultFields())
		if b.err != nil {
			return nil
		}
	} else {
		if sel.Where != nil {
			p = b.buildTableDual(sel)
		}
		if hasAgg {
			p = b.buildAggregate(p, aggFuncs, nil)
		}
		p = b.buildSelectFields(p, sel.GetResultFields())
		if b.err != nil {
			return nil
		}
	}
	if sel.Having != nil {
		p = b.buildFilter(p, sel.Having.Expr)
		if b.err != nil {
			return nil
		}
	}
	if sel.Distinct {
		p = b.buildDistinct(p)
		if b.err != nil {
			return nil
		}
	}
	if sel.OrderBy != nil && !pushOrder(p, sel.OrderBy.Items) {
		p = b.buildSort(p, sel.OrderBy.Items)
		if b.err != nil {
			return nil
		}
	}
	if sel.Limit != nil {
		p = b.buildLimit(p, sel.Limit)
		if b.err != nil {
			return nil
		}
	}
	return p
}

func (ts *TableScan) attachCondition(conditions []ast.ExprNode) {
	var pkName model.CIStr
	if ts.Table.PKIsHandle {
		for _, colInfo := range ts.Table.Columns {
			if mysql.HasPriKeyFlag(colInfo.Flag) {
				pkName = colInfo.Name
			}
		}
	}
	for _, con := range conditions {
		if pkName.L != "" {
			checker := conditionChecker{tableName: ts.Table.Name, pkName: pkName}
			if checker.check(con) {
				ts.AccessConditions = append(ts.AccessConditions, con)
			} else {
				ts.FilterConditions = append(ts.FilterConditions, con)
			}
		} else {
			ts.FilterConditions = append(ts.FilterConditions, con)
		}
	}
}

func (b *planBuilder) buildNewTableScanPlan(tn *ast.TableName) Plan {
	p := &TableScan{
		Table:     tn.TableInfo,
		TableName: tn,
	}
	// Equal condition contains a column from previous joined table.
	p.RefAccess = false
	p.SetFields(tn.GetResultFields())
	p.TableAsName = getTableAsName(p.Fields())
	return p
}
