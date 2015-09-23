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
	"github.com/pingcap/tidb/context"
)

var _ Expression = (*ParamMarker)(nil)

// ParamMarker expresion holds a place for another expression.
// Used in parsing prepare statement.
type ParamMarker struct {
	// Expr is the expression to be evaluated in this place holder.
	Expr Expression
}

// Clone implements the Expression Clone interface.
func (pm *ParamMarker) Clone() Expression {
	np := &ParamMarker{}
	if pm.Expr != nil {
		np.Expr = pm.Expr.Clone()
	}
	return np
}

// Eval implements the Expression Eval interface.
func (pm *ParamMarker) Eval(ctx context.Context, args map[interface{}]interface{}) (v interface{}, err error) {
	if pm.Expr != nil {
		return pm.Expr.Eval(ctx, args)
	}
	return nil, nil
}

// Accept implements Expression Accept interface.
func (pm *ParamMarker) Accept(v Visitor) (Expression, error) {
	return v.VisitParamMaker(pm)
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
