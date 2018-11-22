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

package expression

import (
	"bytes"

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	log "github.com/sirupsen/logrus"
)

// exprSet is a Set container for expressions, each expression in it is unique.
// `tombstone` is deleted mark, if tombstone[i] is true, data[i] is invalid.
// `index` use expr.HashCode() as key, to implement the unique property.
type exprSet struct {
	data       []Expression
	tombstone  []bool
	exists     map[string]struct{}
	constfalse bool
}

func (s *exprSet) Append(e Expression) bool {
	if _, ok := s.exists[string(e.HashCode(nil))]; ok {
		return false
	}

	s.data = append(s.data, e)
	s.tombstone = append(s.tombstone, false)
	s.exists[string(e.HashCode(nil))] = struct{}{}
	return true
}

// Slice returns the valid expressions in the exprSet, this function has side effect.
func (s *exprSet) Slice() []Expression {
	if s.constfalse {
		return []Expression{&Constant{
			Value:   types.NewDatum(false),
			RetType: types.NewFieldType(mysql.TypeTiny),
		}}
	}

	idx := 0
	for i := 0; i < len(s.data); i++ {
		if !s.tombstone[i] {
			s.data[idx] = s.data[i]
			idx++
		}
	}
	return s.data[:idx]
}

func (s *exprSet) SetConstFalse() {
	s.constfalse = true
}

func newExprSet(conditions []Expression) *exprSet {
	var exprs exprSet
	exprs.data = make([]Expression, 0, len(conditions))
	exprs.tombstone = make([]bool, 0, len(conditions))
	exprs.exists = make(map[string]struct{}, len(conditions))
	for _, v := range conditions {
		exprs.Append(v)
	}
	return &exprs
}

type pgSolver2 struct{}

// PropagateConstant propagate constant values of deterministic predicates in a condition.
func (s pgSolver2) PropagateConstant(ctx sessionctx.Context, conditions []Expression) []Expression {
	exprs := newExprSet(conditions)
	s.fixPoint(ctx, exprs)
	return exprs.Slice()
}

// fixPoint is the core of the constant propagation algorithm.
// It will iterate the expression set over and over again, pick two expressions,
// apply one to another.
// If new conditions can be infered, they will be append into the expression set.
// Until no more conditions can be infered from the set, the algorithm finish.
func (s pgSolver2) fixPoint(ctx sessionctx.Context, exprs *exprSet) {
	for {
		saveLen := len(exprs.data)
		iterOnce(ctx, exprs)
		if saveLen == len(exprs.data) {
			break
		}
	}
	return
}

// iterOnce picks two expressions from the set, try to propagate new conditions from them.
func iterOnce(ctx sessionctx.Context, exprs *exprSet) {
	for i := 0; i < len(exprs.data); i++ {
		if exprs.tombstone[i] {
			continue
		}
		for j := 0; j < len(exprs.data); j++ {
			if exprs.tombstone[j] {
				continue
			}
			if i == j {
				continue
			}
			solve(ctx, i, j, exprs)
		}
	}
}

// solve uses exprs[i] exprs[j] to propagate new conditions.
func solve(ctx sessionctx.Context, i, j int, exprs *exprSet) {
	for _, rule := range rules {
		rule(ctx, i, j, exprs)
	}
}

type constantPropagateRule func(ctx sessionctx.Context, i, j int, exprs *exprSet)

var rules = []constantPropagateRule{
	ruleConstantFalse,
	ruleColumnEQConst,
}

// ruleConstantFalse propagates from CNF condition that false plus anything returns false.
// false, a = 1, b = c ... => false
func ruleConstantFalse(ctx sessionctx.Context, i, j int, exprs *exprSet) {
	cond := exprs.data[i]
	if cons, ok := cond.(*Constant); ok {
		v, isNull, err := cons.EvalInt(ctx, chunk.Row{})
		if err != nil {
			log.Error(err)
			return
		}
		if !isNull && v == 0 {
			exprs.SetConstFalse()
		}
	}
}

// ruleColumnEQConst propagates the "column = const" condition.
// "a = 3, b = a, c = a, d = b" => "a = 3, b = 3, c = 3, d = 3"
func ruleColumnEQConst(ctx sessionctx.Context, i, j int, exprs *exprSet) {
	col, cons := validEqualCond(exprs.data[i])
	if col != nil {
		expr := ColumnSubstitute(exprs.data[j], NewSchema(col), []Expression{cons})
		stmtctx := ctx.GetSessionVars().StmtCtx
		if bytes.Compare(expr.HashCode(stmtctx), exprs.data[j].HashCode(stmtctx)) != 0 {
			exprs.Append(expr)
			exprs.tombstone[j] = true
		}
	}
}
