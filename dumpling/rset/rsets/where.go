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
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/expressions"
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

func (r *WhereRset) planBinOp(ctx context.Context, x *expressions.BinaryOperation) (plan.Plan, error) {
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
			b, ok := e.(*expressions.BinaryOperation)
			if !ok || b.Op != opcode.AndAnd {
				in = append(in, e)
				return
			}

			f(b.L)
			f(b.R)
		}
		f(x)
		out := []expression.Expression{}
		p := r.Src
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
			e := expressions.NewBinaryOperation(opcode.AndAnd, out[n-2], out[n-1])

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

func (r *WhereRset) planIdent(ctx context.Context, x *expressions.Ident) (plan.Plan, error) {
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

func (r *WhereRset) planIsNull(ctx context.Context, x *expressions.IsNull) (plan.Plan, error) {
	p := r.Src

	cns := expressions.MentionedColumns(x.Expr)
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

func (r *WhereRset) planUnaryOp(ctx context.Context, x *expressions.UnaryOperation) (plan.Plan, error) {
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

// Plan gets NullPlan/FilterDefaultPlan.
func (r *WhereRset) Plan(ctx context.Context) (plan.Plan, error) {
	expr := r.Expr.Clone()
	if expr.IsStatic() {
		// IsStaic means we have a const value for where condition, and we don't need any index.
		return r.planStatic(ctx, expr)
	}

	switch x := expr.(type) {
	case *expressions.BinaryOperation:
		return r.planBinOp(ctx, x)
	case *expressions.Ident:
		return r.planIdent(ctx, x)
	case *expressions.IsNull:
		return r.planIsNull(ctx, x)
	case *expressions.PatternIn:
		// TODO: optimize
		// TODO: show plan
	case *expressions.PatternLike:
		// TODO: optimize
	case *expressions.PatternRegexp:
		// TODO: optimize
	case *expressions.UnaryOperation:
		return r.planUnaryOp(ctx, x)
	default:
		log.Warnf("%v not supported in where rset now", r.Expr)
	}

	return &plans.FilterDefaultPlan{Plan: r.Src, Expr: expr}, nil
}
