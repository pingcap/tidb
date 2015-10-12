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

package ast

import (
	"github.com/pingcap/tidb/expression/builtin"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/util/types"
	"strings"
)

// tpNode is the struct implements partial expression interface.
// can be embeded by other nodes.
type tpNode struct {
	tp *types.FieldType
}

// SetType implements Expression interface.
func (tn *tpNode) SetType(tp *types.FieldType) {
	tn.tp = tp
}

// GetType implements Expression interface.
func (tn *tpNode) GetType() *types.FieldType {
	return tn.tp
}

// Value is the simple value expression.
type Value struct {
	txtNode
	tpNode
	// Val is the literal value.
	Val interface{}
}

// IsStatic implements Expression.
func (val *Value) IsStatic() bool {
	return true
}

// Accept implements Node interface.
func (val *Value) Accept(v Visitor) (Node, bool) {
	if !v.Enter(val) {
		return val, false
	}
	return v.Leave(val)
}

// Between is for "between and" or "not between and" expression.
type Between struct {
	txtNode
	tpNode
	// Expr is the expression to be checked.
	Expr Expression
	// Left is the expression for minimal value in the range.
	Left Expression
	// Right is the expression for maximum value in the range.
	Right Expression
	// Not is true, the expression is "not between and".
	Not bool
}

// Accept implements Node interface.
func (b *Between) Accept(v Visitor) (Node, bool) {
	if !v.Enter(b) {
		return b, false
	}

	node, ok := b.Expr.Accept(v)
	if !ok {
		return b, false
	}
	b.Expr = node.(Expression)

	node, ok = b.Left.Accept(v)
	if !ok {
		return b, false
	}
	b.Left = node.(Expression)

	node, ok = b.Right.Accept(v)
	if !ok {
		return b, false
	}
	b.Right = node.(Expression)

	return v.Leave(b)
}

// IsStatic implements the Expression IsStatic interface.
func (b *Between) IsStatic() bool {
	return b.Expr.IsStatic() && b.Left.IsStatic() && b.Right.IsStatic()
}

// BinaryOperation is for binary operation like 1 + 1, 1 - 1, etc.
type BinaryOperation struct {
	txtNode
	tpNode
	// Op is the operator code for BinaryOperation.
	Op opcode.Op
	// L is the left expression in BinaryOperation.
	L Expression
	// R is the right expression in BinaryOperation.
	R Expression
}

// Accept implements Node interface.
func (o *BinaryOperation) Accept(v Visitor) (Node, bool) {
	if !v.Enter(o) {
		return o, false
	}

	node, ok := o.L.Accept(v)
	if !ok {
		return o, false
	}
	o.L = node.(Expression)

	node, ok = o.R.Accept(v)
	if !ok {
		return o, false
	}
	o.R = node.(Expression)

	return v.Leave(o)
}

// IsStatic implements the Expression IsStatic interface.
func (o *BinaryOperation) IsStatic() bool {
	return o.L.IsStatic() && o.R.IsStatic()
}

// Call is for function expression.
type Call struct {
	txtNode
	tpNode
	// F is the function name.
	F string
	// Args is the function args.
	Args []Expression
	// Distinct only affetcts sum, avg, count, group_concat,
	// so we can ignore it in other functions
	Distinct bool
}

// Accept implements Node interface.
func (c *Call) Accept(v Visitor) (Node, bool) {
	if !v.Enter(c) {
		return c, false
	}
	for i, val := range c.Args {
		node, ok := val.Accept(v)
		if !ok {
			return c, false
		}
		c.Args[i] = node.(Expression)
	}
	return v.Leave(c)
}

// IsStatic implements the Expression IsStatic interface.
func (c *Call) IsStatic() bool {
	v := builtin.Funcs[strings.ToLower(c.F)]
	if v.F == nil || !v.IsStatic {
		return false
	}

	for _, v := range c.Args {
		if !v.IsStatic() {
			return false
		}
	}
	return true
}

// WhenClause is the expression in Case expression for "when condition then result".
type WhenClause struct {
	txtNode
	tpNode
	// Expr is the condition expression in WhenClause.
	Expr Expression
	// Result is the result expression in WhenClause.
	Result Expression
}

// Accept implements Node Accept interface.
func (w *WhenClause) Accept(v Visitor) (Node, bool) {
	if !v.Enter(w) {
		return w, false
	}
	node, ok := w.Expr.Accept(v)
	if !ok {
		return w, false
	}
	w.Expr = node.(Expression)

	node, ok = w.Result.Accept(v)
	if !ok {
		return w, false
	}
	w.Result = node.(Expression)
	return v.Leave(w)
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

// Accept implements Node Accept interface.
func (f *FunctionCase) Accept(v Visitor) (Expression, bool) {
	if !v.Enter(f) {
		return f, false
	}
	node, ok := f.Value.Accept(v)
	if !ok {
		return f, false
	}
	f.Value = node.(Expression)
	for i, val := range f.WhenClauses {
		node, ok = val.Accept(v)
		if !ok {
			return f, false
		}
		f.WhenClauses[i] = node.(*WhenClause)
	}
	node, ok = f.ElseClause.Accept(v)
	if !ok {
		return f, false
	}
	f.ElseClause = node.(Expression)
	return v.Leave(f)
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

// castOperatopr is the operator type for cast function.
type castFunctionType int

const (
	// CastFunction is CAST function.
	CastFunction castFunctionType = iota + 1
	// ConvertFunction is CONVERT function.
	ConvertFunction
	// BinaryOperator is BINARY operator.
	BinaryOperator
)

// FunctionCast is the cast function converting value to another type, e.g, cast(expr AS signed).
// See https://dev.mysql.com/doc/refman/5.7/en/cast-functions.html
type FunctionCast struct {
	// Expr is the expression to be converted.
	Expr Expression
	// Tp is the conversion type.
	Tp *types.FieldType
	// Cast, Convert and Binary share this struct.
	FunctionType castFunctionType
}

// IsStatic implements the Expression IsStatic interface.
func (f *FunctionCast) IsStatic() bool {
	return f.Expr.IsStatic()
}

// Accept implements Node Accept interface.
func (f *FunctionCast) Accept(v Visitor) (Node, bool) {
	if !v.Enter(v) {
		return f, false
	}
	node, ok := f.Expr.Accept(v)
	if !ok {
		return f, false
	}
	f.Expr = node.(Expression)
	return v.Leave(f)
}

// SubQuery represents a sub query.
type SubQuery struct {
	// Query is the query SelectNode.
	Query SelectNode
}

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

// ColumnRef represents a column reference.
type ColumnRef struct {
	txtNode
	tpNode

	// Name is the referenced column name.
	Name model.CIStr
}

// Accept implements Node Accept interface.
func (cr *ColumnRef) Accept(v Visitor) (Node, bool) {
	if !v.Enter(v) {
		return cr, false
	}
	return v.Leave(cr)
}

// IsStatic implements the Expression IsStatic interface.
func (cr *ColumnRef) IsStatic() bool {
	return false
}
