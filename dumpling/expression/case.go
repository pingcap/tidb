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

	"github.com/pingcap/tidb/context"

	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/util/types"
)

// See https://dev.mysql.com/doc/refman/5.7/en/control-flow-functions.html#operator_case

var (
	_ Expression = (*FunctionCase)(nil)
	_ Expression = (*WhenClause)(nil)
)

// WhenClause is the expression in Case expression for "when condition then result".
type WhenClause struct {
	// Expr is the condition expression in WhenClause.
	Expr Expression
	// Result is the result expression in WhenClause.
	Result Expression
}

// Check if satisified the target condition.
// If satisified, the second returned value is true.
func (w *WhenClause) evalAndCheck(ctx context.Context, args map[interface{}]interface{}, target interface{}) (interface{}, bool, error) {
	o := NewBinaryOperation(opcode.EQ, &Value{target}, w.Expr)

	// types.Compare wil return true/false for NULL
	// We must use BinaryOperation with opcode.Eq
	eq, err := o.Eval(ctx, args)
	if err != nil {
		return nil, false, err
	}
	if eq == nil {
		return nil, false, err
	}
	beq, err := types.ToBool(eq)
	if beq == 0 || err != nil {
		return nil, false, err
	}
	rv, err := w.Result.Eval(ctx, args)
	return rv, true, err
}

// Eval implements the Expression Eval interface.
func (w *WhenClause) Eval(ctx context.Context, args map[interface{}]interface{}) (interface{}, error) {
	return w.Result.Eval(ctx, args)
}

// Accept implements Expression Accept interface.
func (w *WhenClause) Accept(v Visitor) (Expression, error) {
	return v.VisitWhenClause(w)
}

// String implements the Expression String interface.
func (w *WhenClause) String() string {
	return fmt.Sprintf("WHEN %s THEN %s", w.Expr.String(), w.Result.String())
}

// Clone implements the Expression Clone interface.
func (w *WhenClause) Clone() Expression {
	ne := w.Expr.Clone()
	nr := w.Result.Clone()
	return &WhenClause{Expr: ne, Result: nr}
}

// IsStatic implements the Expression IsStatic interface.
func (w *WhenClause) IsStatic() bool {
	return w.Expr.IsStatic() && w.Result.IsStatic()
}

// FunctionCase is the case expression.
type FunctionCase struct {
	// Value is the compare value expression.
	Value Expression
	// WhenClauses is the condition check expression.
	WhenClauses []*WhenClause
	// ElseClause is the else result expression.
	ElseClause Expression
}

// Clone implements the Expression Clone interface.
func (f *FunctionCase) Clone() Expression {
	var (
		nv Expression
		ne Expression
		nw Expression
	)
	if f.Value != nil {
		nv = f.Value.Clone()
	}
	ws := make([]*WhenClause, 0, len(f.WhenClauses))
	for _, w := range f.WhenClauses {
		nw = w.Clone()
		ws = append(ws, nw.(*WhenClause))
	}

	if f.ElseClause != nil {
		ne = f.ElseClause.Clone()
	}
	return &FunctionCase{
		Value:       nv,
		WhenClauses: ws,
		ElseClause:  ne,
	}
}

// IsStatic implements the Expression IsStatic interface.
func (f *FunctionCase) IsStatic() bool {
	if f.Value != nil && !f.Value.IsStatic() {
		return false
	}
	for _, w := range f.WhenClauses {
		if !w.IsStatic() {
			return false
		}
	}
	if f.ElseClause != nil && !f.ElseClause.IsStatic() {
		return false
	}
	return true
}

// String implements the Expression String interface.
func (f *FunctionCase) String() string {
	strs := make([]string, 0, len(f.WhenClauses)+2)
	if f.Value != nil {
		strs = append(strs, f.Value.String())
	}
	for _, w := range f.WhenClauses {
		strs = append(strs, w.String())
	}
	if f.ElseClause != nil {
		strs = append(strs, "ELSE "+f.ElseClause.String())
	}
	return fmt.Sprintf("CASE %s END", strings.Join(strs, " "))
}

// Eval implements the Expression Eval interface.
func (f *FunctionCase) Eval(ctx context.Context, args map[interface{}]interface{}) (interface{}, error) {
	var (
		target interface{} = true
		err    error
	)
	if f.Value != nil {
		target, err = f.Value.Eval(ctx, args)
		if err != nil {
			return nil, err
		}
	}
	for _, w := range f.WhenClauses {
		r, match, err := w.evalAndCheck(ctx, args, target)
		if err != nil {
			return nil, err
		}
		if match {
			return r, nil
		}
	}
	// If there was no matching result value, the result after ELSE is returned,
	// or NULL if there is no ELSE part.
	if f.ElseClause != nil {
		return f.ElseClause.Eval(ctx, args)
	}
	return nil, nil
}

// Accept implements Expression Accept interface.
func (f *FunctionCase) Accept(v Visitor) (Expression, error) {
	return v.VisitFunctionCase(f)
}
