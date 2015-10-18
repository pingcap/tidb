// Copyright 2014 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

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

package plans

import (
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/field"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/util/format"
)

var (
	_ plan.Plan = (*JoinPlan)(nil)
)

// Ref: http://www.w3schools.com/sql/sql_join.asp
const (
	// CrossJoin are used to combine rows from two or more tables
	CrossJoin = "CROSS"
	// LeftJoin returns all rows from the left table (table1), with the matching rows in the right table (table2). The result is NULL in the right side when there is no match.
	LeftJoin = "LEFT"
	// RightJoin returns all rows from the right table (table2), with the matching rows in the left table (table1). The result is NULL in the left side when there is no match.
	RightJoin = "RIGHT"
)

// JoinPlan handles JOIN query.
// The whole join plan is a tree
// e.g, from (t1 left join t2 on t1.c1 = t2.c2), (t3 right join t4 on t3.c1 = t4.c1)
// the executing order maylook:
//           Table Result
//                |
//          -------------
//         |             |
//      t1 x t2       t3 x t4
//
// TODO: add Parent field, optimize join plan
type JoinPlan struct {
	Left  plan.Plan
	Right plan.Plan

	Type string

	Fields      []*field.ResultField
	On          expression.Expression
	curRow      *plan.Row
	tempPlan    plan.Plan
	matchedRows []*plan.Row
	cursor      int
	evalArgs    map[interface{}]interface{}
}

// Explain implements plan.Plan Explain interface.
func (r *JoinPlan) Explain(w format.Formatter) {
	// TODO: show more useful join plan
	if r.Right == nil {
		// if right is nil, we don't do a join, just simple select table
		r.Left.Explain(w)
		return
	}

	w.Format("┌Compute %s Cartesian product of\n", r.Type)

	r.explainNode(w, r.Left)
	r.explainNode(w, r.Right)

	w.Format("└Output field names %v\n", field.RFQNames(r.Fields))
}

func (r *JoinPlan) explainNode(w format.Formatter, node plan.Plan) {
	sel := !isTableOrIndex(node)
	if sel {
		w.Format("┌Iterate all rows of virtual table\n")
	}
	node.Explain(w)
	if sel {
		w.Format("└Output field names %v\n", field.RFQNames(node.GetFields()))
	}

}

func (r *JoinPlan) filterNode(ctx context.Context, expr expression.Expression, node plan.Plan) (plan.Plan, bool, error) {
	if node == nil {
		return nil, false, nil
	}

	e2 := expr.Clone()
	return node.Filter(ctx, e2)
}

// Filter implements plan.Plan Filter interface, it returns one of the two
// plans' Filter result, maybe we could do some optimizations here.
func (r *JoinPlan) Filter(ctx context.Context, expr expression.Expression) (plan.Plan, bool, error) {
	// TODO: do more optimization for join plan
	// now we only use where expression for Filter, but for join
	// we must use On expression too.
	newPlan := &JoinPlan{
		Fields: r.Fields,
		Type:   r.Type,
		On:     r.On,
	}

	p, filteredLeft, err := r.filterNode(ctx, expr, r.Left)
	if err != nil {
		return nil, false, errors.Trace(err)
	}
	newPlan.Left = p
	p, filteredRight, err := r.filterNode(ctx, expr, r.Right)
	if err != nil {
		return nil, false, errors.Trace(err)
	}
	newPlan.Right = p
	return newPlan, filteredLeft || filteredRight, nil
}

// GetFields implements plan.Plan GetFields interface.
func (r *JoinPlan) GetFields() []*field.ResultField {
	return r.Fields
}

// Next implements plan.Plan Next interface.
func (r *JoinPlan) Next(ctx context.Context) (row *plan.Row, err error) {
	if r.Right == nil {
		return r.Left.Next(ctx)
	}
	if r.evalArgs == nil {
		r.evalArgs = map[interface{}]interface{}{}
	}
	switch r.Type {
	case LeftJoin:
		return r.nextLeftJoin(ctx)
	case RightJoin:
		return r.nextRightJoin(ctx)
	default:
		return r.nextCrossJoin(ctx)
	}
}

func (r *JoinPlan) nextLeftJoin(ctx context.Context) (row *plan.Row, err error) {
	visitor := newJoinIdentVisitor(0)
	for {
		if r.cursor < len(r.matchedRows) {
			row = r.matchedRows[r.cursor]
			r.cursor++
			return
		}
		var leftRow *plan.Row
		leftRow, err = r.Left.Next(ctx)
		if leftRow == nil || err != nil {
			return nil, errors.Trace(err)
		}
		tempExpr := r.On.Clone()
		visitor.row = leftRow.Data
		_, err = tempExpr.Accept(visitor)
		if err != nil {
			return nil, errors.Trace(err)
		}
		var filtered bool
		var p plan.Plan
		p, filtered, err = r.Right.Filter(ctx, tempExpr)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if filtered {
			log.Debugf("left join use index")
		}
		err = r.findMatchedRows(ctx, leftRow, p, false)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
}

func (r *JoinPlan) nextRightJoin(ctx context.Context) (row *plan.Row, err error) {
	visitor := newJoinIdentVisitor(len(r.Left.GetFields()))
	for {
		if r.cursor < len(r.matchedRows) {
			row = r.matchedRows[r.cursor]
			r.cursor++
			return
		}
		var rightRow *plan.Row
		rightRow, err = r.Right.Next(ctx)
		if rightRow == nil || err != nil {
			return nil, errors.Trace(err)
		}

		tempExpr := r.On.Clone()
		visitor.row = rightRow.Data
		_, err = tempExpr.Accept(visitor)
		if err != nil {
			return nil, errors.Trace(err)
		}
		var filtered bool
		var p plan.Plan
		p, filtered, err = r.Left.Filter(ctx, tempExpr)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if filtered {
			log.Debugf("right join use index")
		}
		err = r.findMatchedRows(ctx, rightRow, p, true)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
}

func (r *JoinPlan) findMatchedRows(ctx context.Context, row *plan.Row, p plan.Plan, right bool) (err error) {
	r.cursor = 0
	r.matchedRows = nil
	p.Close()
	for {
		var cmpRow *plan.Row
		cmpRow, err = p.Next(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		if cmpRow == nil {
			break
		}

		// Do append(s1, s2) safely. Sometime the s1 capability is larger than its real length, so
		// multi append may overwrite last valid data, e.g,
		//   s1 = make([]interface{}, 0, 1)
		//   s = append(s1, []interface{}{1})
		//   ss = append(ss, s)
		//   s = append(s1, []interface{}{2})
		//   ss = append(ss, s)
		// We will see that ss only contains 2.
		joined := make([]interface{}, 0, len(cmpRow.Data)+len(row.Data))
		if right {
			joined = append(append(joined, cmpRow.Data...), row.Data...)
		} else {
			joined = append(append(joined, row.Data...), cmpRow.Data...)
		}
		r.evalArgs[expression.ExprEvalIdentReferFunc] = func(name string, scope int, index int) (interface{}, error) {
			return joined[index], nil
		}
		var b bool
		b, err = expression.EvalBoolExpr(ctx, r.On, r.evalArgs)
		if err != nil {
			return errors.Trace(err)
		}
		if b {
			cmpRow.Data = joined
			keys := make([]*plan.RowKeyEntry, 0, len(row.RowKeys)+len(cmpRow.RowKeys))
			cmpRow.RowKeys = append(append(keys, row.RowKeys...), cmpRow.RowKeys...)
			r.matchedRows = append(r.matchedRows, cmpRow)
		}
	}
	if len(r.matchedRows) == 0 {
		if right {
			leftLen := len(r.Fields) - len(r.Right.GetFields())
			row.Data = append(make([]interface{}, leftLen), row.Data...)
		} else {
			rightLen := len(r.Fields) - len(r.Left.GetFields())
			row.Data = append(row.Data, make([]interface{}, rightLen)...)
		}
		r.matchedRows = append(r.matchedRows, row)
	}
	return nil
}

func (r *JoinPlan) nextCrossJoin(ctx context.Context) (row *plan.Row, err error) {
	visitor := newJoinIdentVisitor(0)
	for {
		if r.curRow == nil {
			r.curRow, err = r.Left.Next(ctx)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if r.curRow == nil {
				return nil, nil
			}

			if r.On != nil {
				tempExpr := r.On.Clone()
				visitor.row = r.curRow.Data
				_, err = tempExpr.Accept(visitor)
				if err != nil {
					return nil, errors.Trace(err)
				}
				var filtered bool
				r.tempPlan, filtered, err = r.Right.Filter(ctx, tempExpr)
				if filtered {
					log.Debugf("cross join use index")
				}
			} else {
				r.tempPlan = r.Right
			}
		}

		var rightRow *plan.Row
		rightRow, err = r.tempPlan.Next(ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if rightRow == nil {
			r.curRow = nil
			r.tempPlan.Close()
			continue
		}

		// To prevent outer modify the slice. See comment above.
		joinedRow := make([]interface{}, 0, len(r.curRow.Data)+len(rightRow.Data))
		joinedRow = append(append(joinedRow, r.curRow.Data...), rightRow.Data...)

		if r.On != nil {
			r.evalArgs[expression.ExprEvalIdentReferFunc] = func(name string, scope int, index int) (interface{}, error) {
				return joinedRow[index], nil
			}

			b, err := expression.EvalBoolExpr(ctx, r.On, r.evalArgs)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if !b {
				// If On condition not satisified, drop this row
				continue
			}
		}
		row = &plan.Row{
			Data:    joinedRow,
			RowKeys: append(r.curRow.RowKeys, rightRow.RowKeys...),
		}
		return
	}
}

// Close implements plan.Plan Close interface.
func (r *JoinPlan) Close() error {
	r.curRow = nil
	r.tempPlan = nil
	r.matchedRows = nil
	r.cursor = 0
	if r.Right != nil {
		r.Right.Close()
	}
	return r.Left.Close()
}

// joinIdentVisitor converts Ident expression to value expression in ON expression.
type joinIdentVisitor struct {
	expression.BaseVisitor
	row    []interface{}
	offset int
}

// newJoinIdentVisitor creates a new joinIdentVisitor.
func newJoinIdentVisitor(offset int) *joinIdentVisitor {
	iev := &joinIdentVisitor{offset: offset}
	iev.BaseVisitor.V = iev
	return iev
}

// VisitIdent implements Visitor interface.
func (iev *joinIdentVisitor) VisitIdent(i *expression.Ident) (expression.Expression, error) {
	// the row here may be just left part or right part, but identifier may reference another part,
	// so here we must check its reference index validation.
	if i.ReferIndex < iev.offset || i.ReferIndex >= iev.offset+len(iev.row) {
		return i, nil
	}
	v := iev.row[i.ReferIndex-iev.offset]
	return expression.Value{Val: v}, nil
}

// VisitBinaryOperation swaps the right side identifier to left side if left side expression is static.
// So it can be used in index plan.
func (iev *joinIdentVisitor) VisitBinaryOperation(binop *expression.BinaryOperation) (expression.Expression, error) {
	var err error
	binop.L, err = binop.L.Accept(iev)
	if err != nil {
		return binop, errors.Trace(err)
	}
	binop.R, err = binop.R.Accept(iev)
	if err != nil {
		return binop, errors.Trace(err)
	}

	if binop.L.IsStatic() {
		if _, ok := binop.R.(*expression.Ident); ok {
			switch binop.Op {
			case opcode.EQ:
			case opcode.NE:
			case opcode.NullEQ:
			case opcode.LT:
				binop.Op = opcode.GE
			case opcode.LE:
				binop.Op = opcode.GT
			case opcode.GE:
				binop.Op = opcode.LT
			case opcode.GT:
				binop.Op = opcode.LE
			default:
				// unsupported opcode
				return binop, nil
			}
			binop.L, binop.R = binop.R, binop.L
		}
	}
	return binop, nil
}
