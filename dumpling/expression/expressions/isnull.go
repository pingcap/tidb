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

	"github.com/juju/errors"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
)

var (
	_ expression.Expression = (*IsNull)(nil)
)

// IsNull is the expression for null check.
type IsNull struct {
	// Expr is the expression to be checked.
	Expr expression.Expression
	// Not is true, the expression is "is not null".
	Not bool
}

// Clone implements the Expression Clone interface.
func (is *IsNull) Clone() (expression.Expression, error) {
	expr, err := is.Expr.Clone()
	if err != nil {
		return nil, err
	}

	return &IsNull{Expr: expr, Not: is.Not}, nil
}

// IsStatic implements the Expression IsStatic interface.
func (is *IsNull) IsStatic() bool {
	return is.Expr.IsStatic()
}

// String implements the Expression String interface.
func (is *IsNull) String() string {
	if is.Not {
		return fmt.Sprintf("%s IS NOT NULL", is.Expr)
	}

	return fmt.Sprintf("%s IS NULL", is.Expr)
}

// Eval implements the Expression Eval interface.
func (is *IsNull) Eval(ctx context.Context, args map[interface{}]interface{}) (v interface{}, err error) {
	if err := CheckOneColumn(is.Expr); err != nil {
		return nil, errors.Trace(err)
	}

	val, err := is.Expr.Eval(ctx, args)
	if err != nil {
		return
	}

	return val == nil != is.Not, nil
}
