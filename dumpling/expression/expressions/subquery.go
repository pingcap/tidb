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

	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/stmt"
)

var _ expression.Expression = (*SubQuery)(nil)

// SubQuery expresion holds a select statement.
// TODO: complete according to https://dev.mysql.com/doc/refman/5.7/en/subquery-restrictions.html
type SubQuery struct {
	// Stmt is the sub select statement.
	Stmt stmt.Statement
	// Value holds the sub select result.
	Value interface{}
}

// Clone implements the Expression Clone interface.
func (sq *SubQuery) Clone() (expression.Expression, error) {
	// TODO: Statement does not have Clone interface. So we need to check this
	nsq := &SubQuery{Stmt: sq.Stmt, Value: sq.Value}
	return nsq, nil
}

// Eval implements the Expression Eval interface.
func (sq *SubQuery) Eval(ctx context.Context, args map[interface{}]interface{}) (v interface{}, err error) {
	if sq.Value != nil {
		return sq.Value, nil
	}
	rs, err := sq.Stmt.Exec(ctx)
	if err != nil {
		return nil, err
	}
	// TODO: check row/column number
	// Output all the data and let the outer caller check the row/column number
	// This simple implementation is used to pass tpc-c
	rows, err := rs.Rows(1, 0)
	if err != nil || len(rows) == 0 || len(rows[0]) == 0 {
		return nil, err
	}
	sq.Value = rows[0][0]
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
