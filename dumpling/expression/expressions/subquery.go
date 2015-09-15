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

	p plan.Plan
}

// Clone implements the Expression Clone interface.
func (sq *SubQuery) Clone() (expression.Expression, error) {
	nsq := &SubQuery{Stmt: sq.Stmt, Value: sq.Value, p: sq.p}
	return nsq, nil
}

// Eval implements the Expression Eval interface.
// Eval doesn't support multi rows return, so we can only get a scalar or a row result.
// If you want to get multi rows, use Plan to get a execution plan and run Do() directly.
func (sq *SubQuery) Eval(ctx context.Context, args map[interface{}]interface{}) (v interface{}, err error) {
	if sq.Value != nil {
		return sq.Value, nil
	}

	p, err := sq.Plan(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}

	row, err := p.Next(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if row == nil {
		return sq.Value, nil
	}
	if len(p.GetFields()) == 1 {
		// a scalar value is a single value
		sq.Value = row.Data[0]
	} else {
		// a row value is []interface{}
		sq.Value = row.Data
	}
	row, err = p.Next(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if row != nil {
		return nil, errors.Errorf("Subquery returns more than 1 row")
	}
	return sq.Value, nil
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
