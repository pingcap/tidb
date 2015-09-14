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
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/expressions"
	"github.com/pingcap/tidb/field"
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
	// FullJoin returns all rows from the left table (table1) and from the right table (table2).
	FullJoin = "FULL"
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

	Fields []*field.ResultField
	On     expression.Expression
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
		return r, false, nil
	}

	e2, err := expr.Clone()
	if err != nil {
		return nil, false, err
	}

	return node.Filter(ctx, e2)
}

// Filter implements plan.Plan Filter interface, it returns one of the two
// plans' Filter result, maybe we could do some optimizations here.
func (r *JoinPlan) Filter(ctx context.Context, expr expression.Expression) (plan.Plan, bool, error) {
	// TODO: do more optimization for join plan
	// now we only use where expression for Filter, but for join
	// we must use On expression too.

	p, filtered, err := r.filterNode(ctx, expr, r.Left)
	if err != nil {
		return nil, false, err
	}
	if filtered {
		r.Left = p
		return r, true, nil
	}

	p, filtered, err = r.filterNode(ctx, expr, r.Right)
	if err != nil {
		return nil, false, err
	}
	if filtered {
		r.Right = p
		return r, true, nil
	}
	return r, false, nil
}

// GetFields implements plan.Plan GetFields interface.
func (r *JoinPlan) GetFields() []*field.ResultField {
	return r.Fields
}

// Do implements plan.Plan Do interface, it executes join method
// accourding to given type.
func (r *JoinPlan) Do(ctx context.Context, f plan.RowIterFunc) error {
	if r.Right == nil {
		return r.Left.Do(ctx, f)
	}

	switch r.Type {
	case LeftJoin:
		return r.doLeftJoin(ctx, f)
	case RightJoin:
		return r.doRightJoin(ctx, f)
	case FullJoin:
		return r.doFullJoin(ctx, f)
	default:
		return r.doCrossJoin(ctx, f)
	}
}

func (r *JoinPlan) doCrossJoin(ctx context.Context, f plan.RowIterFunc) error {
	return r.Left.Do(ctx, func(rid interface{}, in []interface{}) (more bool, err error) {
		leftRow := appendRow(nil, in)
		m := map[interface{}]interface{}{}
		if err := r.Right.Do(ctx, func(rid interface{}, in []interface{}) (more bool, err error) {
			row := appendRow(leftRow, in)
			if r.On != nil {
				m[expressions.ExprEvalIdentFunc] = func(name string) (interface{}, error) {
					return getIdentValue(name, r.Fields, row, field.DefaultFieldFlag)
				}

				b, err := expressions.EvalBoolExpr(ctx, r.On, m)
				if err != nil {
					return false, err
				}
				if !b {
					// If On condition not satisified, drop this row
					return true, nil
				}
			}

			return f(rid, row)
		}); err != nil {
			return false, err
		}

		return true, nil
	})
}

func (r *JoinPlan) doLeftJoin(ctx context.Context, f plan.RowIterFunc) error {
	return r.Left.Do(ctx, func(rid interface{}, in []interface{}) (more bool, err error) {
		leftRow := appendRow(nil, in)
		matched := false
		m := map[interface{}]interface{}{}
		if err := r.Right.Do(ctx, func(rid interface{}, in []interface{}) (more bool, err error) {
			row := appendRow(leftRow, in)

			m[expressions.ExprEvalIdentFunc] = func(name string) (interface{}, error) {
				return getIdentValue(name, r.Fields, row, field.DefaultFieldFlag)
			}

			b, err := expressions.EvalBoolExpr(ctx, r.On, m)
			if err != nil {
				return false, err
			}
			if !b {
				return true, nil
			}

			matched = true

			return f(rid, row)
		}); err != nil {
			return false, err
		}

		if !matched {
			// Fill right with NULL
			rightLen := len(r.Fields) - len(r.Left.GetFields())
			return f(rid, appendRow(leftRow, make([]interface{}, rightLen)))
		}

		return true, nil
	})
}

func (r *JoinPlan) doRightJoin(ctx context.Context, f plan.RowIterFunc) error {
	// right join is the same as left join, only reverse the row result
	return r.Right.Do(ctx, func(rid interface{}, in []interface{}) (more bool, err error) {
		rightRow := appendRow(nil, in)
		matched := false
		m := map[interface{}]interface{}{}
		if err := r.Left.Do(ctx, func(rid interface{}, in []interface{}) (more bool, err error) {
			row := appendRow(in, rightRow)

			m[expressions.ExprEvalIdentFunc] = func(name string) (interface{}, error) {
				return getIdentValue(name, r.Fields, row, field.DefaultFieldFlag)
			}

			b, err := expressions.EvalBoolExpr(ctx, r.On, m)
			if err != nil {
				return false, err
			}
			if !b {
				return true, nil
			}

			matched = true

			return f(rid, row)
		}); err != nil {
			return false, err
		}

		if !matched {
			// Fill left with NULL
			leftLen := len(r.Fields) - len(r.Right.GetFields())
			return f(rid, appendRow(make([]interface{}, leftLen), rightRow))
		}

		return true, nil
	})
}

func (r *JoinPlan) doFullJoin(ctx context.Context, f plan.RowIterFunc) error {
	// we just support full join simplify, because MySQL doesn't support it
	// for full join, we can use two phases
	// 1, t1 LEFT JOIN t2
	// 2, t2 anti semi LEFT JOIN t1
	if err := r.doLeftJoin(ctx, f); err != nil {
		return err
	}

	// anti semi left join
	return r.Right.Do(ctx, func(rid interface{}, in []interface{}) (more bool, err error) {
		rightRow := appendRow(nil, in)
		matched := false
		m := map[interface{}]interface{}{}
		if err := r.Left.Do(ctx, func(rid interface{}, in []interface{}) (more bool, err error) {
			row := appendRow(in, rightRow)

			m[expressions.ExprEvalIdentFunc] = func(name string) (interface{}, error) {
				return getIdentValue(name, r.Fields, row, field.DefaultFieldFlag)
			}

			b, err := expressions.EvalBoolExpr(ctx, r.On, m)
			if err != nil {
				return false, err
			}
			if b {
				// here means the condition matched, we can skip this row
				matched = true
				return false, nil
			}

			return true, nil
		}); err != nil {
			return false, err
		}

		if !matched {
			// Fill left with NULL
			leftLen := len(r.Fields) - len(r.Right.GetFields())
			return f(rid, appendRow(make([]interface{}, leftLen), rightRow))
		}

		return true, nil
	})
}

/*
 * The last value in prefix/in maybe RowKeyList
 * Append values of prefix/in together and merge RowKeyLists to the tail entry
 */
func appendRow(prefix []interface{}, in []interface{}) []interface{} {
	rks := &RowKeyList{}
	if prefix != nil && len(prefix) > 0 {
		t := prefix[len(prefix)-1]
		switch vt := t.(type) {
		case *RowKeyList:
			rks.appendKeys(vt.Keys...)
			prefix = prefix[:len(prefix)-1]
		}
	}
	if in != nil && len(in) > 0 {
		t := in[len(in)-1]
		switch vt := t.(type) {
		case *RowKeyList:
			rks.appendKeys(vt.Keys...)
			in = in[:len(in)-1]
		}
	}
	row := make([]interface{}, 0, len(prefix)+len(in))
	row = append(row, prefix...)
	row = append(row, in...)
	row = append(row, rks)
	return row
}
