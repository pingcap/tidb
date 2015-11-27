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

package rsets

import (
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/plan/plans"
	"github.com/pingcap/tidb/util/types"
)

var (
	_ plan.Planner = (*WhereRset)(nil)
)

// WhereRset is record set for where filter.
type WhereRset struct {
	Expr expression.Expression
	Src  plan.Plan
}

func (r *WhereRset) planBinOp(ctx context.Context, x *expression.BinaryOperation) (plan.Plan, error) {
	var err error
	var p2 plan.Plan
	var filtered bool

	p := r.Src
	switch x.Op {
	case opcode.EQ, opcode.GE, opcode.GT, opcode.LE, opcode.LT, opcode.NE:
		if p2, filtered, err = p.Filter(ctx, x); err != nil {
			return nil, err
		}
		if filtered {
			return p2, nil
		}
	case opcode.AndAnd:
		var in []expression.Expression
		var f func(expression.Expression)
		f = func(e expression.Expression) {
			b, ok := e.(*expression.BinaryOperation)
			if !ok || b.Op != opcode.AndAnd {
				in = append(in, e)
				return
			}

			f(b.L)
			f(b.R)
		}
		f(x)
		out := []expression.Expression{}
		p = r.Src
		isNewPlan := false
		for _, e := range in {
			p2, filtered, err = p.Filter(ctx, e)
			if err != nil {
				return nil, err
			}

			if !filtered {
				out = append(out, e)
				continue
			}

			p = p2
			isNewPlan = true
		}

		if !isNewPlan {
			break
		}

		if len(out) == 0 {
			return p, nil
		}

		for len(out) > 1 {
			n := len(out)
			e := expression.NewBinaryOperation(opcode.AndAnd, out[n-2], out[n-1])

			out = out[:n-1]
			out[n-2] = e
		}

		return &plans.FilterDefaultPlan{Plan: p, Expr: out[0]}, nil
	default:
		// TODO: better plan for `OR`.
		log.Warn("TODO: better plan for", x.Op)
	}

	return &plans.FilterDefaultPlan{Plan: p, Expr: x}, nil
}

func (r *WhereRset) planIdent(ctx context.Context, x *expression.Ident) (plan.Plan, error) {
	p := r.Src
	p2, filtered, err := p.Filter(ctx, x)
	if err != nil {
		return nil, err
	}

	if filtered {
		return p2, nil
	}

	return &plans.FilterDefaultPlan{Plan: p, Expr: x}, nil
}

func (r *WhereRset) planIsNull(ctx context.Context, x *expression.IsNull) (plan.Plan, error) {
	p := r.Src

	cns := expression.MentionedColumns(x.Expr)
	if len(cns) == 0 {
		return &plans.FilterDefaultPlan{Plan: p, Expr: x}, nil
	}

	p2, filtered, err := p.Filter(ctx, x)
	if err != nil {
		return nil, err
	}

	if filtered {
		return p2, nil
	}

	return &plans.FilterDefaultPlan{Plan: p, Expr: x}, nil
}

func (r *WhereRset) planUnaryOp(ctx context.Context, x *expression.UnaryOperation) (plan.Plan, error) {
	p := r.Src
	p2, filtered, err := p.Filter(ctx, x)
	if err != nil {
		return nil, err
	}

	if filtered {
		return p2, nil
	}

	return &plans.FilterDefaultPlan{Plan: p, Expr: x}, nil
}

func (r *WhereRset) planStatic(ctx context.Context, e expression.Expression) (plan.Plan, error) {
	val, err := e.Eval(nil, nil)
	if err != nil {
		return nil, err
	}
	if val == nil {
		// like `select * from t where null`.
		return &plans.NullPlan{Fields: r.Src.GetFields()}, nil
	}

	n, err := types.ToBool(val)
	if err != nil {
		return nil, err
	}
	if n == 0 {
		// like `select * from t where 0`.
		return &plans.NullPlan{Fields: r.Src.GetFields()}, nil
	}

	return &plans.FilterDefaultPlan{Plan: r.Src, Expr: e}, nil
}

func (r *WhereRset) updateWhereFieldsRefer() error {
	visitor := NewFromIdentVisitor(r.Src.GetFields(), WhereClause)

	e, err := r.Expr.Accept(visitor)
	if err != nil {
		return errors.Trace(err)
	}

	r.Expr = e
	return nil
}

// Plan gets NullPlan/FilterDefaultPlan.
func (r *WhereRset) Plan(ctx context.Context) (plan.Plan, error) {
	// Update where fields refer expr by using fromIdentVisitor.
	if err := r.updateWhereFieldsRefer(); err != nil {
		return nil, errors.Trace(err)
	}

	expr := r.Expr.Clone()
	if expr.IsStatic() {
		// IsStaic means we have a const value for where condition, and we don't need any index.
		return r.planStatic(ctx, expr)
	}

	var (
		src = r.Src
		err error
	)

	switch x := expr.(type) {
	case *expression.BinaryOperation:
		src, err = r.planBinOp(ctx, x)
	case *expression.Ident:
		src, err = r.planIdent(ctx, x)
	case *expression.IsNull:
		src, err = r.planIsNull(ctx, x)
	case *expression.PatternIn:
		// TODO: optimize
		// TODO: show plan
	case *expression.PatternLike:
		// TODO: optimize
	case *expression.PatternRegexp:
		// TODO: optimize
	case *expression.UnaryOperation:
		src, err = r.planUnaryOp(ctx, x)
	default:
		log.Warnf("%v not supported in where rset now", r.Expr)
	}

	if err != nil {
		return nil, errors.Trace(err)
	}

	if _, ok := src.(*plans.FilterDefaultPlan); ok {
		return src, nil
	}

	// We must use a FilterDefaultPlan here to wrap filtered plan.
	// Alghough we can check where condition using index plan, we still need
	// to check again after the FROM phase if the FROM phase contains outer join.
	// TODO: if FROM phase doesn't contain outer join, we can return filtered plan directly.
	return &plans.FilterDefaultPlan{Plan: src, Expr: expr}, nil
}
