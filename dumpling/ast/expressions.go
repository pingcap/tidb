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

var (
	_ Expression = &Value{}
	_ Expression = &Between{}
	_ Expression = &BinaryOperation{}
	_ Expression = &Call{}
	_ Expression = &WhenClause{}
	_ Expression = &FunctionCase{}
	_ Expression = &FunctionCast{}
	_ Expression = &SubQuery{}
	_ Expression = &CompareSubQuery{}
	_ Expression = &ColumnRef{}
	_ Expression = &FunctionConvert{}
	_ Expression = &Default{}
	_ Expression = &ExistsSubQuery{}
	_ Expression = &Extract{}
	_ Expression = &PatternIn{}
	_ Expression = &IsNull{}
	_ Expression = &IsTruth{}
	_ Expression = &PatternLike{}
	_ Expression = &ParamMarker{}
	_ Expression = &PExpr{}
	_ Expression = &Position{}
	_ Expression = &PatternRegexp{}
	_ Expression = &Row{}
	_ Expression = &FunctionSubstring{}
	_ Expression = &FunctionTrim{}
	_ Expression = &UnaryOperation{}
	_ Expression = &Values{}
	_ Expression = &Variable{}
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

type nonStatic struct{}

func (ns *nonStatic) IsStatic() bool {
	return false
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
	txtNode
	tpNode
	// Value is the compare value expression.
	Value Expression
	// WhenClauses is the condition check expression.
	WhenClauses []*WhenClause
	// ElseClause is the else result expression.
	ElseClause Expression
}

// Accept implements Node Accept interface.
func (f *FunctionCase) Accept(v Visitor) (Node, bool) {
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
	txtNode
	tpNode
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
	if !v.Enter(f) {
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
	txtNode
	tpNode
	nonStatic
	// Query is the query SelectNode.
	Query *SelectNode
}

// Accept implements Node Accept interface.
func (sq *SubQuery) Accept(v Visitor) (Node, bool) {
	if !v.Enter(sq) {
		return sq, false
	}
	node, ok := sq.Query.Accept(v)
	if !ok {
		return sq, false
	}
	sq.Query = node.(*SelectNode)
	return v.Leave(sq)
}

// CompareSubQuery is the expression for "expr cmp (select ...)".
// See: https://dev.mysql.com/doc/refman/5.7/en/comparisons-using-subqueries.html
// See: https://dev.mysql.com/doc/refman/5.7/en/any-in-some-subqueries.html
// See: https://dev.mysql.com/doc/refman/5.7/en/all-subqueries.html
type CompareSubQuery struct {
	txtNode
	tpNode
	nonStatic
	// L is the left expression
	L Expression
	// Op is the comparison opcode.
	Op opcode.Op
	// R is the sub query for right expression.
	R *SubQuery
	// All is true, we should compare all records in subquery.
	All bool
}

// Accept implements Node Accept interface.
func (cs *CompareSubQuery) Accept(v Visitor) (Node, bool) {
	if !v.Enter(cs) {
		return cs, false
	}
	node, ok := cs.L.Accept(v)
	if !ok {
		return cs, false
	}
	cs.L = node.(Expression)
	node, ok = cs.R.Accept(v)
	if !ok {
		return cs, false
	}
	cs.R = node.(*SubQuery)
	return v.Leave(cs)
}

// ColumnRef represents a column reference.
type ColumnRef struct {
	txtNode
	tpNode
	nonStatic

	// Name is the referenced column name.
	Name model.CIStr
}

// Accept implements Node Accept interface.
func (cr *ColumnRef) Accept(v Visitor) (Node, bool) {
	if !v.Enter(cr) {
		return cr, false
	}
	return v.Leave(cr)
}

// FunctionConvert provides a way to convert data between different character sets.
// See: https://dev.mysql.com/doc/refman/5.7/en/cast-functions.html#function_convert
type FunctionConvert struct {
	txtNode
	tpNode
	// Expr is the expression to be converted.
	Expr Expression
	// Charset is the target character set to convert.
	Charset string
}

// IsStatic implements the Expression IsStatic interface.
func (f *FunctionConvert) IsStatic() bool {
	return f.Expr.IsStatic()
}

// Accept implements Node Accept interface.
func (f *FunctionConvert) Accept(v Visitor) (Node, bool) {
	if !v.Enter(f) {
		return f, false
	}
	node, ok := f.Expr.Accept(v)
	if !ok {
		return f, false
	}
	f.Expr = node.(Expression)
	return v.Leave(f)
}

// Default is the default expression using default value for a column.
type Default struct {
	txtNode
	tpNode
	nonStatic
	// Name is the column name.
	Name string
}

// Accept implements Node Accept interface.
func (d *Default) Accept(v Visitor) (Node, bool) {
	if !v.Enter(d) {
		return d, false
	}
	return v.Leave(d)
}

// ExistsSubQuery is the expression for "exists (select ...)".
// https://dev.mysql.com/doc/refman/5.7/en/exists-and-not-exists-subqueries.html
type ExistsSubQuery struct {
	txtNode
	tpNode
	nonStatic
	// Sel is the sub query.
	Sel *SubQuery
}

// Accept implements Node Accept interface.
func (es *ExistsSubQuery) Accept(v Visitor) (Node, bool) {
	if !v.Enter(es) {
		return es, false
	}
	node, ok := es.Sel.Accept(v)
	if !ok {
		return es, false
	}
	es.Sel = node.(*SubQuery)
	return v.Leave(es)
}

// Extract is for time extract function.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_extract
type Extract struct {
	txtNode
	tpNode

	Unit string
	Date Expression
}

// Accept implements Node Accept interface.
func (ex *Extract) Accept(v Visitor) (Node, bool) {
	if !v.Enter(ex) {
		return ex, false
	}
	node, ok := ex.Date.Accept(v)
	if !ok {
		return ex, false
	}
	ex.Date = node.(Expression)
	return v.Leave(ex)
}

// IsStatic implements the Expression IsStatic interface.
func (ex *Extract) IsStatic() bool {
	return ex.Date.IsStatic()
}

// PatternIn is the expression for in operator, like "expr in (1, 2, 3)" or "expr in (select c from t)".
type PatternIn struct {
	txtNode
	tpNode
	nonStatic
	// Expr is the value expression to be compared.
	Expr Expression
	// List is the list expression in compare list.
	List []Expression
	// Not is true, the expression is "not in".
	Not bool
	// Sel is the sub query.
	Sel *SubQuery
}

// Accept implements Node Accept interface.
func (pi *PatternIn) Accept(v Visitor) (Node, bool) {
	if !v.Enter(pi) {
		return pi, false
	}
	node, ok := pi.Expr.Accept(v)
	if !ok {
		return pi, false
	}
	pi.Expr = node.(Expression)
	for i, val := range pi.List {
		node, ok = val.Accept(v)
		if !ok {
			return pi, false
		}
		pi.List[i] = node.(Expression)
	}
	if pi.Sel != nil {
		node, ok = pi.Sel.Accept(v)
		if !ok {
			return pi, false
		}
		pi.Sel = node.(*SubQuery)
	}
	return v.Leave(pi)
}

// IsNull is the expression for null check.
type IsNull struct {
	txtNode
	tpNode
	// Expr is the expression to be checked.
	Expr Expression
	// Not is true, the expression is "is not null".
	Not bool
}

// Accept implements Node Accept interface.
func (is *IsNull) Accept(v Visitor) (Node, bool) {
	if !v.Enter(is) {
		return is, false
	}
	node, ok := is.Expr.Accept(v)
	if !ok {
		return is, false
	}
	is.Expr = node.(Expression)
	return v.Leave(is)
}

// IsStatic implements the Expression IsStatic interface.
func (is *IsNull) IsStatic() bool {
	return is.Expr.IsStatic()
}

// IsTruth is the expression for true/false check.
type IsTruth struct {
	txtNode
	tpNode
	// Expr is the expression to be checked.
	Expr Expression
	// Not is true, the expression is "is not true/false".
	Not bool
	// True indicates checking true or false.
	True int64
}

// Accept implements Node Accept interface.
func (is *IsTruth) Accept(v Visitor) (Node, bool) {
	if !v.Enter(is) {
		return is, false
	}
	node, ok := is.Expr.Accept(v)
	if !ok {
		return is, false
	}
	is.Expr = node.(Expression)
	return v.Leave(is)
}

// IsStatic implements the Expression IsStatic interface.
func (is *IsTruth) IsStatic() bool {
	return is.Expr.IsStatic()
}

// PatternLike is the expression for like operator, e.g, expr like "%123%"
type PatternLike struct {
	txtNode
	tpNode
	// Expr is the expression to be checked.
	Expr Expression
	// Pattern is the like expression.
	Pattern Expression
	// Not is true, the expression is "not like".
	Not bool
}

// Accept implements Node Accept interface.
func (pl *PatternLike) Accept(v Visitor) (Node, bool) {
	if !v.Enter(pl) {
		return pl, false
	}
	node, ok := pl.Expr.Accept(v)
	if !ok {
		return pl, false
	}
	pl.Expr = node.(Expression)
	node, ok = pl.Pattern.Accept(v)
	if !ok {
		return pl, false
	}
	pl.Pattern = node.(Expression)
	return v.Leave(pl)
}

// IsStatic implements the Expression IsStatic interface.
func (pl *PatternLike) IsStatic() bool {
	return pl.Expr.IsStatic() && pl.Pattern.IsStatic()
}

// ParamMarker expresion holds a place for another expression.
// Used in parsing prepare statement.
type ParamMarker struct {
	txtNode
	tpNode
	// Expr is the expression to be evaluated in this place holder.
	Expr Expression
}

// Accept implements Node Accept interface.
func (pm *ParamMarker) Accept(v Visitor) (Node, bool) {
	if !v.Enter(pm) {
		return pm, false
	}
	node, ok := pm.Expr.Accept(v)
	if !ok {
		return pm, false
	}
	pm.Expr = node.(Expression)
	return v.Leave(pm)
}

// IsStatic implements the Expression IsStatic interface.
func (pm *ParamMarker) IsStatic() bool {
	return pm.Expr.IsStatic()
}

// PExpr is the parenthesis expression.
type PExpr struct {
	txtNode
	tpNode
	// Expr is the expression in parenthesis.
	Expr Expression
}

// Accept implements Node Accept interface.
func (p *PExpr) Accept(v Visitor) (Node, bool) {
	if !v.Enter(p) {
		return p, false
	}
	node, ok := p.Expr.Accept(v)
	if !ok {
		return p, false
	}
	p.Expr = node.(Expression)
	return v.Leave(p)
}

// IsStatic implements the Expression IsStatic interface.
func (p *PExpr) IsStatic() bool {
	return p.Expr.IsStatic()
}

// Position is the expression for order by and group by position.
// MySQL use position expression started from 1, it looks a little confused inner.
// maybe later we will use 0 at first.
type Position struct {
	txtNode
	tpNode
	// N is the position, started from 1 now.
	N int
	// Name is the corresponding field name if we want better format and explain instead of position.
	Name string
}

// IsStatic implements the Expression IsStatic interface.
func (p *Position) IsStatic() bool {
	return true
}

// Accept implements Node Accept interface.
func (p *Position) Accept(v Visitor) (Node, bool) {
	if !v.Enter(p) {
		return p, false
	}
	return v.Leave(p)
}

// PatternRegexp is the pattern expression for pattern match.
type PatternRegexp struct {
	txtNode
	tpNode
	// Expr is the expression to be checked.
	Expr Expression
	// Pattern is the expression for pattern.
	Pattern Expression
	// Not is true, the expression is "not rlike",
	Not bool
}

// Accept implements Node Accept interface.
func (p *PatternRegexp) Accept(v Visitor) (Node, bool) {
	if !v.Enter(p) {
		return p, false
	}
	node, ok := p.Expr.Accept(v)
	if !ok {
		return p, false
	}
	p.Expr = node.(Expression)
	node, ok = p.Pattern.Accept(v)
	if !ok {
		return p, false
	}
	p.Pattern = node.(Expression)
	return v.Leave(p)
}

// IsStatic implements the Expression IsStatic interface.
func (p *PatternRegexp) IsStatic() bool {
	return p.Expr.IsStatic() && p.Pattern.IsStatic()
}

// Row is the expression for row constructor.
// See https://dev.mysql.com/doc/refman/5.7/en/row-subqueries.html
type Row struct {
	txtNode
	tpNode

	Values []Expression
}

// Accept implements Node Accept interface.
func (r *Row) Accept(v Visitor) (Node, bool) {
	if !v.Enter(r) {
		return r, false
	}
	for i, val := range r.Values {
		node, ok := val.Accept(v)
		if !ok {
			return r, false
		}
		r.Values[i] = node.(Expression)
	}
	return v.Leave(r)
}

// IsStatic implements the Expression IsStatic interface.
func (r *Row) IsStatic() bool {
	for _, v := range r.Values {
		if !v.IsStatic() {
			return false
		}
	}
	return true
}

// FunctionSubstring returns the substring as specified.
// See: https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_substring
type FunctionSubstring struct {
	txtNode
	tpNode

	StrExpr Expression
	Pos     Expression
	Len     Expression
}

// Accept implements Node Accept interface.
func (fs *FunctionSubstring) Accept(v Visitor) (Node, bool) {
	if !v.Enter(fs) {
		return fs, false
	}
	node, ok := fs.StrExpr.Accept(v)
	if !ok {
		return fs, false
	}
	fs.StrExpr = node.(Expression)
	node, ok = fs.Pos.Accept(v)
	if !ok {
		return fs, false
	}
	fs.Pos = node.(Expression)
	node, ok = fs.Len.Accept(v)
	if !ok {
		return fs, false
	}
	fs.Len = node.(Expression)
	return v.Leave(fs)
}

// IsStatic implements the Expression IsStatic interface.
func (fs *FunctionSubstring) IsStatic() bool {
	return fs.StrExpr.IsStatic() && fs.Pos.IsStatic() && fs.Len.IsStatic()
}

const (
	// TrimBothDefault trims from both direction by default.
	TrimBothDefault = iota
	// TrimBoth trims from both direction with explicit notation.
	TrimBoth
	// TrimLeading trims from left.
	TrimLeading
	// TrimTrailing trims from right.
	TrimTrailing
)

// FunctionTrim remove leading/trailing/both remstr.
// See: https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_trim
type FunctionTrim struct {
	txtNode
	tpNode

	Str       Expression
	RemStr    Expression
	Direction int
}

// Accept implements Node Accept interface.
func (ft *FunctionTrim) Accept(v Visitor) (Node, bool) {
	if !v.Enter(ft) {
		return ft, false
	}
	node, ok := ft.Str.Accept(v)
	if !ok {
		return ft, false
	}
	ft.Str = node.(Expression)
	node, ok = ft.RemStr.Accept(v)
	if !ok {
		return ft, false
	}
	ft.RemStr = node.(Expression)
	return v.Leave(ft)
}

// IsStatic implements the Expression IsStatic interface.
func (ft *FunctionTrim) IsStatic() bool {
	return ft.Str.IsStatic() && ft.RemStr.IsStatic()
}

// UnaryOperation is the expression for unary operator.
type UnaryOperation struct {
	txtNode
	tpNode
	// Op is the operator opcode.
	Op opcode.Op
	// V is the unary expression.
	V Expression
}

// Accept implements Node Accept interface.
func (u *UnaryOperation) Accept(v Visitor) (Node, bool) {
	if !v.Enter(u) {
		return u, false
	}
	node, ok := u.V.Accept(v)
	if !ok {
		return u, false
	}
	u.V = node.(Expression)
	return v.Leave(u)
}

// IsStatic implements the Expression IsStatic interface.
func (u *UnaryOperation) IsStatic() bool {
	return u.V.IsStatic()
}

// Values is the expression used in INSERT VALUES
type Values struct {
	txtNode
	tpNode
	nonStatic
	// model.CIStr is column name.
	Column *ColumnRef
}

// Accept implements Node Accept interface.
func (va *Values) Accept(v Visitor) (Node, bool) {
	if !v.Enter(va) {
		return va, false
	}
	node, ok := va.Column.Accept(v)
	if !ok {
		return va, false
	}
	va.Column = node.(*ColumnRef)
	return v.Leave(va)
}

// Variable is the expression for variable.
type Variable struct {
	txtNode
	tpNode
	nonStatic
	// Name is the variable name.
	Name string
	// IsGlobal indicates whether this variable is global.
	IsGlobal bool
	// IsSystem indicates whether this variable is a global variable in current session.
	IsSystem bool
}

// Accept implements Node Accept interface.
func (va *Variable) Accept(v Visitor) (Node, bool) {
	if !v.Enter(va) {
		return va, false
	}
	return v.Leave(va)
}
