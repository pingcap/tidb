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

package expression

import (
	"fmt"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"

	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/util/types"
)

// CompareSubQuery is the expression for "expr cmp (select ...)".
// See: https://dev.mysql.com/doc/refman/5.7/en/comparisons-using-subqueries.html
// See: https://dev.mysql.com/doc/refman/5.7/en/any-in-some-subqueries.html
// See: https://dev.mysql.com/doc/refman/5.7/en/all-subqueries.html
type CompareSubQuery struct {
	// L is the left expression
	L Expression
	// Op is the comparison opcode.
	Op opcode.Op
	// R is the sub query for right expression.
	R SubQuery
	// All is true, we should compare all records in subquery.
	All bool
}

// Clone implements the Expression Clone interface.
func (cs *CompareSubQuery) Clone() Expression {
	l := cs.L.Clone()
	r := cs.R.Clone()
	return &CompareSubQuery{L: l, Op: cs.Op, R: r.(SubQuery), All: cs.All}
}

// IsStatic implements the Expression IsStatic interface.
func (cs *CompareSubQuery) IsStatic() bool {
	return cs.L.IsStatic() && cs.R.IsStatic()
}

// String implements the Expression String interface.
func (cs *CompareSubQuery) String() string {
	anyOrAll := "ANY"
	if cs.All {
		anyOrAll = "ALL"
	}

	return fmt.Sprintf("%s %s %s %s", cs.L, cs.Op, anyOrAll, cs.R)
}

// Eval implements the Expression Eval interface.
func (cs *CompareSubQuery) Eval(ctx context.Context, args map[interface{}]interface{}) (interface{}, error) {
	if err := hasSameColumnCount(ctx, cs.L, cs.R); err != nil {
		return nil, errors.Trace(err)
	}

	lv, err := cs.L.Eval(ctx, args)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if lv == nil {
		return nil, nil
	}

	if !cs.R.UseOuterQuery() && cs.R.Value() != nil {
		return cs.checkResult(lv, cs.R.Value().([]interface{}))
	}

	res, err := cs.R.EvalRows(ctx, args, -1)
	if err != nil {
		return nil, errors.Trace(err)
	}

	cs.R.SetValue(res)
	return cs.checkResult(lv, cs.R.Value().([]interface{}))
}

// Accept implements Expression Accept interface.
func (cs *CompareSubQuery) Accept(v Visitor) (Expression, error) {
	return v.VisitCompareSubQuery(cs)
}

func (cs *CompareSubQuery) checkAllResult(lv interface{}, result []interface{}) (interface{}, error) {
	hasNull := false
	for _, v := range result {
		if v == nil {
			hasNull = true
			continue
		}

		comRes, err := types.Compare(lv, v)
		if err != nil {
			return nil, errors.Trace(err)
		}

		res, err := getCompResult(cs.Op, comRes)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if !res {
			return false, nil
		}
	}

	if hasNull {
		// If no matched but we get null, return null.
		// Like `insert t (c) values (1),(2),(null)`, then
		// `select 3 > all (select c from t)`, returns null.
		return nil, nil
	}

	return true, nil
}

func (cs *CompareSubQuery) checkAnyResult(lv interface{}, result []interface{}) (interface{}, error) {
	hasNull := false
	for _, v := range result {
		if v == nil {
			hasNull = true
			continue
		}

		comRes, err := types.Compare(lv, v)
		if err != nil {
			return nil, errors.Trace(err)
		}

		res, err := getCompResult(cs.Op, comRes)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if res {
			return true, nil
		}
	}

	if hasNull {
		// If no matched but we get null, return null.
		// Like `insert t (c) values (1),(2),(null)`, then
		// `select 0 > any (select c from t)`, returns null.
		return nil, nil
	}

	return false, nil
}

func (cs *CompareSubQuery) checkResult(lv interface{}, result []interface{}) (interface{}, error) {
	if cs.All {
		return cs.checkAllResult(lv, result)
	}

	return cs.checkAnyResult(lv, result)
}

// NewCompareSubQuery creates a CompareSubQuery object.
func NewCompareSubQuery(op opcode.Op, lhs Expression, rhs SubQuery, all bool) *CompareSubQuery {
	return &CompareSubQuery{
		Op:  op,
		L:   lhs,
		R:   rhs,
		All: all,
	}
}
