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

	"github.com/pingcap/tidb/util/types"
)

var (
	_ Expression = (*IsTruth)(nil)
)

// IsTruth is the expression for true/false check.
type IsTruth struct {
	// Expr is the expression to be checked.
	Expr Expression
	// Not is true, the expression is "is not true/false".
	Not bool
	// True indicates checking true or false.
	True int64
}

// Clone implements the Expression Clone interface.
func (is *IsTruth) Clone() Expression {
	expr := is.Expr.Clone()
	return &IsTruth{Expr: expr, Not: is.Not, True: is.True}
}

// IsStatic implements the Expression IsStatic interface.
func (is *IsTruth) IsStatic() bool {
	return is.Expr.IsStatic()
}

// String implements the Expression String interface.
func (is *IsTruth) String() string {
	not := ""
	if is.Not {
		not = "NOT "
	}

	truth := "TRUE"
	if is.True == 0 {
		truth = "FALSE"
	}

	return fmt.Sprintf("%s IS %s%s", is.Expr, not, truth)
}

// Eval implements the Expression Eval interface.
func (is *IsTruth) Eval(ctx context.Context, args map[interface{}]interface{}) (interface{}, error) {
	if err := CheckOneColumn(ctx, is.Expr); err != nil {
		return nil, errors.Trace(err)
	}

	val, err := is.Expr.Eval(ctx, args)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if types.IsNil(val) {
		// null is true/false -> false
		// null is not true/false -> true
		return is.Not, nil
	}

	b, err := types.ToBool(val)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if !is.Not {
		// true/false is true/false
		return b == is.True, nil
	}

	// true/false is not true/false
	return b != is.True, nil
}

// Accept implements Expression Accept interface.
func (is *IsTruth) Accept(v Visitor) (Expression, error) {
	return v.VisitIsTruth(is)
}
