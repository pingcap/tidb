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
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
)

var _ expression.Expression = (*ParamMarker)(nil)

// ParamMarker expresion holds a place for another expression.
// Used in parsing prepare statement.
type ParamMarker struct {
	// Expr is the expression to be evaluated in this place holder.
	Expr expression.Expression
}

// Clone implements the Expression Clone interface.
func (pm *ParamMarker) Clone() (expression.Expression, error) {
	np := &ParamMarker{}
	if pm.Expr != nil {
		var err error
		np.Expr, err = pm.Expr.Clone()
		if err != nil {
			return nil, err
		}
	}
	return np, nil
}

// Eval implements the Expression Eval interface.
func (pm *ParamMarker) Eval(ctx context.Context, args map[interface{}]interface{}) (v interface{}, err error) {
	if pm.Expr != nil {
		return pm.Expr.Eval(ctx, args)
	}
	return nil, nil
}

// IsStatic implements the Expression IsStatic interface, always returns false.
func (pm *ParamMarker) IsStatic() bool {
	return false
}

// String implements the Expression String interface.
func (pm *ParamMarker) String() string {
	if pm.Expr != nil {
		return pm.Expr.String()
	}
	return "?"
}
