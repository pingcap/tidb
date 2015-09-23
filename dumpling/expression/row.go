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
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
)

// Row is the expression for row constructor.
// See https://dev.mysql.com/doc/refman/5.7/en/row-subqueries.html
type Row struct {
	Values []Expression
}

// Clone implements the Expression Clone interface.
func (r *Row) Clone() Expression {
	values := make([]Expression, len(r.Values))
	for i, expr := range r.Values {
		values[i] = expr.Clone()
	}
	return &Row{Values: values}
}

// IsStatic implements the Expression IsStatic interface.
func (r *Row) IsStatic() bool {
	for _, e := range r.Values {
		if !e.IsStatic() {
			return false
		}
	}

	return true
}

// String implements the Expression String interface.
func (r *Row) String() string {
	ss := make([]string, len(r.Values))
	for i := range ss {
		ss[i] = r.Values[i].String()
	}

	return fmt.Sprintf("ROW(%s)", strings.Join(ss, ", "))
}

// Eval implements the Expression Eval interface.
func (r *Row) Eval(ctx context.Context, args map[interface{}]interface{}) (interface{}, error) {
	row := make([]interface{}, len(r.Values))
	var err error
	for i, expr := range r.Values {
		row[i], err = expr.Eval(ctx, args)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	return row, nil
}

// Accept implements Expression Accept interface.
func (r *Row) Accept(v Visitor) (Expression, error) {
	return v.VisitRow(r)
}
