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

package expressions

import (
	"fmt"
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/stmt"
)

// SubQueryStatement implements stmt.Statement and plan.Planner interface.
type SubQueryStatement interface {
	stmt.Statement
	plan.Planner
}

var _ expression.Expression = (*SubQuery)(nil)

// SubQuery expresion holds a select statement.
// TODO: complete according to https://dev.mysql.com/doc/refman/5.7/en/subquery-restrictions.html
type SubQuery struct {
	// Stmt is the sub select statement.
	Stmt SubQueryStatement
	// Value holds the sub select result.
	Value interface{}

	// UseOuterQuery represents that whether subquery uses reference to a table for the outer query.
	// If use, we cannot cache the sub query result.
	UseOuterQuery bool

	p plan.Plan
}

// Clone implements the Expression Clone interface.
func (sq *SubQuery) Clone() expression.Expression {
	nsq := &SubQuery{Stmt: sq.Stmt, Value: sq.Value, p: sq.p, UseOuterQuery: sq.UseOuterQuery}
	return nsq
}

// Eval implements the Expression Eval interface.
// Eval doesn't support multi rows return, so we can only get a scalar or a row result.
// If you want to get multi rows, use EvalRows instead.
func (sq *SubQuery) Eval(ctx context.Context, args map[interface{}]interface{}) (v interface{}, err error) {
	if !sq.UseOuterQuery && sq.Value != nil {
		return sq.Value, nil
	}

	rows, err := sq.EvalRows(ctx, args, 2)
	if err != nil {
		return nil, errors.Trace(err)
	}

	switch len(rows) {
	case 0:
		return nil, nil
	case 1:
		sq.Value = rows[0]
		return sq.Value, nil
	default:
		return nil, errors.Errorf("Subquery returns more than 1 row")
	}
}

// IsStatic implements the Expression IsStatic interface, always returns false.
func (sq *SubQuery) IsStatic() bool {
	return false
}

// String implements the Expression String interface.
func (sq *SubQuery) String() string {
	if sq.Stmt != nil {
		stmtStr := strings.TrimSuffix(sq.Stmt.OriginText(), ";")
		return fmt.Sprintf("(%s)", stmtStr)
	}
	return ""
}

// ColumnCount returns column count for the sub query.
func (sq *SubQuery) ColumnCount(ctx context.Context) (int, error) {
	p, err := sq.Plan(ctx)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return len(p.GetFields()), nil
}

// Plan implements plan.Planner interface.
func (sq *SubQuery) Plan(ctx context.Context) (plan.Plan, error) {
	if sq.p != nil {
		return sq.p, nil
	}

	var err error
	sq.p, err = sq.Stmt.Plan(ctx)
	return sq.p, errors.Trace(err)
}

// EvalRows executes the subquery and returns the multi rows with rowCount.
// rowCount < 0 means no limit.
// If the ColumnCount is 1, we will return a column result like {1, 2, 3},
// otherwise, we will return a table result like {{1, 1}, {2, 2}}.
func (sq *SubQuery) EvalRows(ctx context.Context, args map[interface{}]interface{}, rowCount int) ([]interface{}, error) {
	p, err := sq.Plan(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer p.Close()

	sq.push(ctx)

	var (
		row *plan.Row
		res = []interface{}{}
	)

	for rowCount != 0 {
		row, err = p.Next(ctx)
		if err != nil {
			break
		}
		if row == nil {
			break
		}
		if len(row.Data) == 1 {
			res = append(res, row.Data[0])
		} else {
			res = append(res, row.Data)
		}

		if rowCount > 0 {
			rowCount--
		}
	}

	err0 := sq.pop(ctx)
	if err0 != nil {
		return res, errors.Wrap(err, err0)
	}

	return res, errors.Trace(err)
}

// A dummy type to avoid naming collision in context.
type subQueryStackKeyType int

// String defines a Stringer function for debugging and pretty printing.
func (k subQueryStackKeyType) String() string {
	return "sub query stack"
}

// subQueryStackKey holds the running sub query's stack.
const subQueryStackKey subQueryStackKeyType = 0

func (sq *SubQuery) push(ctx context.Context) {
	var st []*SubQuery
	v := ctx.Value(subQueryStackKey)
	if v == nil {
		st = []*SubQuery{}
	} else {
		// must ok
		st = v.([]*SubQuery)
	}

	st = append(st, sq)
	ctx.SetValue(subQueryStackKey, st)
}

func (sq *SubQuery) pop(ctx context.Context) error {
	v := ctx.Value(subQueryStackKey)
	if v == nil {
		return errors.Errorf("pop empty sub query stack")
	}

	st := v.([]*SubQuery)

	// can not empty
	n := len(st) - 1
	if st[n] != sq {
		return errors.Errorf("pop invalid top sub query in stack, want %v, but top is %v", sq, st[n])
	}

	st[n] = nil
	st = st[0:n]
	if len(st) == 0 {
		ctx.ClearValue(subQueryStackKey)
		return nil
	}

	ctx.SetValue(subQueryStackKey, st)
	return nil
}

// SetOuterQueryUsed is called when current running subquery uses outer query.
func SetOuterQueryUsed(ctx context.Context) {
	v := ctx.Value(subQueryStackKey)
	if v == nil {
		return
	}

	st := v.([]*SubQuery)

	// if current sub query uses outer query, the select result can not be cached,
	// at the same time, all the upper sub query must not cache the result too.
	for i := len(st) - 1; i >= 0; i-- {
		st[i].UseOuterQuery = true
	}
}
