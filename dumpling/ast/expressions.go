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
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/parser/opcode"
)

var (
	_ ExprNode = &ValueExpr{}
	_ ExprNode = &BetweenExpr{}
	_ ExprNode = &BinaryOperationExpr{}
	_ Node     = &WhenClause{}
	_ ExprNode = &CaseExpr{}
	_ ExprNode = &SubqueryExpr{}
	_ ExprNode = &CompareSubqueryExpr{}
	_ ExprNode = &ColumnNameExpr{}
	_ ExprNode = &DefaultExpr{}
	_ ExprNode = &IdentifierExpr{}
	_ ExprNode = &ExistsSubqueryExpr{}
	_ ExprNode = &PatternInExpr{}
	_ ExprNode = &IsNullExpr{}
	_ ExprNode = &IsTruthExpr{}
	_ ExprNode = &PatternLikeExpr{}
	_ ExprNode = &ParamMarkerExpr{}
	_ ExprNode = &ParenthesesExpr{}
	_ ExprNode = &PositionExpr{}
	_ ExprNode = &PatternRegexpExpr{}
	_ ExprNode = &RowExpr{}
	_ ExprNode = &UnaryOperationExpr{}
	_ ExprNode = &ValuesExpr{}
	_ ExprNode = &VariableExpr{}
)

// ValueExpr is the simple value expression.
type ValueExpr struct {
	exprNode
	// Val is the literal value.
	Val interface{}
}

// IsStatic implements ExprNode interface.
func (val *ValueExpr) IsStatic() bool {
	return true
}

// Accept implements Node interface.
func (val *ValueExpr) Accept(v Visitor) (Node, bool) {
	if skipChildren, ok := v.Enter(val); skipChildren {
		return val, ok
	}
	return v.Leave(val)
}

// BetweenExpr is for "between and" or "not between and" expression.
type BetweenExpr struct {
	exprNode
	// Expr is the expression to be checked.
	Expr ExprNode
	// Left is the expression for minimal value in the range.
	Left ExprNode
	// Right is the expression for maximum value in the range.
	Right ExprNode
	// Not is true, the expression is "not between and".
	Not bool
}

// Accept implements Node interface.
func (b *BetweenExpr) Accept(v Visitor) (Node, bool) {
	if skipChildren, ok := v.Enter(b); skipChildren {
		return b, ok
	}

	node, ok := b.Expr.Accept(v)
	if !ok {
		return b, false
	}
	b.Expr = node.(ExprNode)

	node, ok = b.Left.Accept(v)
	if !ok {
		return b, false
	}
	b.Left = node.(ExprNode)

	node, ok = b.Right.Accept(v)
	if !ok {
		return b, false
	}
	b.Right = node.(ExprNode)

	return v.Leave(b)
}

// IsStatic implements the ExprNode IsStatic interface.
func (b *BetweenExpr) IsStatic() bool {
	return b.Expr.IsStatic() && b.Left.IsStatic() && b.Right.IsStatic()
}

// BinaryOperationExpr is for binary operation like 1 + 1, 1 - 1, etc.
type BinaryOperationExpr struct {
	exprNode
	// Op is the operator code for BinaryOperation.
	Op opcode.Op
	// L is the left expression in BinaryOperation.
	L ExprNode
	// R is the right expression in BinaryOperation.
	R ExprNode
}

// Accept implements Node interface.
func (o *BinaryOperationExpr) Accept(v Visitor) (Node, bool) {
	if skipChildren, ok := v.Enter(o); skipChildren {
		return o, ok
	}

	node, ok := o.L.Accept(v)
	if !ok {
		return o, false
	}
	o.L = node.(ExprNode)

	node, ok = o.R.Accept(v)
	if !ok {
		return o, false
	}
	o.R = node.(ExprNode)

	return v.Leave(o)
}

// IsStatic implements the ExprNode IsStatic interface.
func (o *BinaryOperationExpr) IsStatic() bool {
	return o.L.IsStatic() && o.R.IsStatic()
}

// WhenClause is the when clause in Case expression for "when condition then result".
type WhenClause struct {
	node
	// Expr is the condition expression in WhenClause.
	Expr ExprNode
	// Result is the result expression in WhenClause.
	Result ExprNode
}

// Accept implements Node Accept interface.
func (w *WhenClause) Accept(v Visitor) (Node, bool) {
	if skipChildren, ok := v.Enter(w); skipChildren {
		return w, ok
	}
	node, ok := w.Expr.Accept(v)
	if !ok {
		return w, false
	}
	w.Expr = node.(ExprNode)

	node, ok = w.Result.Accept(v)
	if !ok {
		return w, false
	}
	w.Result = node.(ExprNode)
	return v.Leave(w)
}

// IsStatic implements the ExprNode IsStatic interface.
func (w *WhenClause) IsStatic() bool {
	return w.Expr.IsStatic() && w.Result.IsStatic()
}

// CaseExpr is the case expression.
type CaseExpr struct {
	exprNode
	// Value is the compare value expression.
	Value ExprNode
	// WhenClauses is the condition check expression.
	WhenClauses []*WhenClause
	// ElseClause is the else result expression.
	ElseClause ExprNode
}

// Accept implements Node Accept interface.
func (f *CaseExpr) Accept(v Visitor) (Node, bool) {
	if skipChildren, ok := v.Enter(f); skipChildren {
		return f, ok
	}
	if f.Value != nil {
		node, ok := f.Value.Accept(v)
		if !ok {
			return f, false
		}
		f.Value = node.(ExprNode)
	}
	for i, val := range f.WhenClauses {
		node, ok := val.Accept(v)
		if !ok {
			return f, false
		}
		f.WhenClauses[i] = node.(*WhenClause)
	}
	if f.ElseClause != nil {
		node, ok := f.ElseClause.Accept(v)
		if !ok {
			return f, false
		}
		f.ElseClause = node.(ExprNode)
	}
	return v.Leave(f)
}

// IsStatic implements the ExprNode IsStatic interface.
func (f *CaseExpr) IsStatic() bool {
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

// SubqueryExpr represents a sub query.
type SubqueryExpr struct {
	exprNode
	// Query is the query SelectNode.
	Query *SelectStmt
}

// Accept implements Node Accept interface.
func (sq *SubqueryExpr) Accept(v Visitor) (Node, bool) {
	if skipChildren, ok := v.Enter(sq); skipChildren {
		return sq, ok
	}
	node, ok := sq.Query.Accept(v)
	if !ok {
		return sq, false
	}
	sq.Query = node.(*SelectStmt)
	return v.Leave(sq)
}

// CompareSubqueryExpr is the expression for "expr cmp (select ...)".
// See: https://dev.mysql.com/doc/refman/5.7/en/comparisons-using-subqueries.html
// See: https://dev.mysql.com/doc/refman/5.7/en/any-in-some-subqueries.html
// See: https://dev.mysql.com/doc/refman/5.7/en/all-subqueries.html
type CompareSubqueryExpr struct {
	exprNode
	// L is the left expression
	L ExprNode
	// Op is the comparison opcode.
	Op opcode.Op
	// R is the sub query for right expression.
	R *SubqueryExpr
	// All is true, we should compare all records in subquery.
	All bool
}

// Accept implements Node Accept interface.
func (cs *CompareSubqueryExpr) Accept(v Visitor) (Node, bool) {
	if skipChildren, ok := v.Enter(cs); skipChildren {
		return cs, ok
	}
	node, ok := cs.L.Accept(v)
	if !ok {
		return cs, false
	}
	cs.L = node.(ExprNode)
	node, ok = cs.R.Accept(v)
	if !ok {
		return cs, false
	}
	cs.R = node.(*SubqueryExpr)
	return v.Leave(cs)
}

// ColumnName represents column name.
type ColumnName struct {
	node
	Schema model.CIStr
	Table  model.CIStr
	Name   model.CIStr
}

// Accept implements Node Accept interface.
func (cn *ColumnName) Accept(v Visitor) (Node, bool) {
	if skipChildren, ok := v.Enter(cn); skipChildren {
		return cn, ok
	}
	return v.Leave(cn)
}

// ColumnNameExpr represents a column name expression.
type ColumnNameExpr struct {
	exprNode

	// Name is the referenced column name.
	Name *ColumnName
}

// Accept implements Node Accept interface.
func (cr *ColumnNameExpr) Accept(v Visitor) (Node, bool) {
	if skipChildren, ok := v.Enter(cr); skipChildren {
		return cr, ok
	}
	node, ok := cr.Name.Accept(v)
	if !ok {
		return cr, false
	}
	cr.Name = node.(*ColumnName)
	return v.Leave(cr)
}

// DefaultExpr is the default expression using default value for a column.
type DefaultExpr struct {
	exprNode
	// Name is the column name.
	Name *ColumnName
}

// Accept implements Node Accept interface.
func (d *DefaultExpr) Accept(v Visitor) (Node, bool) {
	if skipChildren, ok := v.Enter(d); skipChildren {
		return d, ok
	}
	if d.Name != nil {
		node, ok := d.Name.Accept(v)
		if !ok {
			return d, false
		}
		d.Name = node.(*ColumnName)
	}
	return v.Leave(d)
}

// IdentifierExpr represents an identifier expression.
type IdentifierExpr struct {
	exprNode
	// Name is the identifier name.
	Name model.CIStr
}

// Accept implements Node Accept interface.
func (i *IdentifierExpr) Accept(v Visitor) (Node, bool) {
	if skipChildren, ok := v.Enter(i); skipChildren {
		return i, ok
	}
	return v.Leave(i)
}

// ExistsSubqueryExpr is the expression for "exists (select ...)".
// https://dev.mysql.com/doc/refman/5.7/en/exists-and-not-exists-subqueries.html
type ExistsSubqueryExpr struct {
	exprNode
	// Sel is the sub query.
	Sel *SubqueryExpr
}

// Accept implements Node Accept interface.
func (es *ExistsSubqueryExpr) Accept(v Visitor) (Node, bool) {
	if skipChildren, ok := v.Enter(es); skipChildren {
		return es, ok
	}
	node, ok := es.Sel.Accept(v)
	if !ok {
		return es, false
	}
	es.Sel = node.(*SubqueryExpr)
	return v.Leave(es)
}

// PatternInExpr is the expression for in operator, like "expr in (1, 2, 3)" or "expr in (select c from t)".
type PatternInExpr struct {
	exprNode
	// Expr is the value expression to be compared.
	Expr ExprNode
	// List is the list expression in compare list.
	List []ExprNode
	// Not is true, the expression is "not in".
	Not bool
	// Sel is the sub query.
	Sel *SubqueryExpr
}

// Accept implements Node Accept interface.
func (pi *PatternInExpr) Accept(v Visitor) (Node, bool) {
	if skipChildren, ok := v.Enter(pi); skipChildren {
		return pi, ok
	}
	node, ok := pi.Expr.Accept(v)
	if !ok {
		return pi, false
	}
	pi.Expr = node.(ExprNode)
	for i, val := range pi.List {
		node, ok = val.Accept(v)
		if !ok {
			return pi, false
		}
		pi.List[i] = node.(ExprNode)
	}
	if pi.Sel != nil {
		node, ok = pi.Sel.Accept(v)
		if !ok {
			return pi, false
		}
		pi.Sel = node.(*SubqueryExpr)
	}
	return v.Leave(pi)
}

// IsNullExpr is the expression for null check.
type IsNullExpr struct {
	exprNode
	// Expr is the expression to be checked.
	Expr ExprNode
	// Not is true, the expression is "is not null".
	Not bool
}

// Accept implements Node Accept interface.
func (is *IsNullExpr) Accept(v Visitor) (Node, bool) {
	if skipChildren, ok := v.Enter(is); skipChildren {
		return is, ok
	}
	node, ok := is.Expr.Accept(v)
	if !ok {
		return is, false
	}
	is.Expr = node.(ExprNode)
	return v.Leave(is)
}

// IsStatic implements the ExprNode IsStatic interface.
func (is *IsNullExpr) IsStatic() bool {
	return is.Expr.IsStatic()
}

// IsTruthExpr is the expression for true/false check.
type IsTruthExpr struct {
	exprNode
	// Expr is the expression to be checked.
	Expr ExprNode
	// Not is true, the expression is "is not true/false".
	Not bool
	// True indicates checking true or false.
	True int64
}

// Accept implements Node Accept interface.
func (is *IsTruthExpr) Accept(v Visitor) (Node, bool) {
	if skipChildren, ok := v.Enter(is); skipChildren {
		return is, ok
	}
	node, ok := is.Expr.Accept(v)
	if !ok {
		return is, false
	}
	is.Expr = node.(ExprNode)
	return v.Leave(is)
}

// IsStatic implements the ExprNode IsStatic interface.
func (is *IsTruthExpr) IsStatic() bool {
	return is.Expr.IsStatic()
}

// PatternLikeExpr is the expression for like operator, e.g, expr like "%123%"
type PatternLikeExpr struct {
	exprNode
	// Expr is the expression to be checked.
	Expr ExprNode
	// Pattern is the like expression.
	Pattern ExprNode
	// Not is true, the expression is "not like".
	Not bool

	Escape byte
}

// Accept implements Node Accept interface.
func (pl *PatternLikeExpr) Accept(v Visitor) (Node, bool) {
	if skipChildren, ok := v.Enter(pl); skipChildren {
		return pl, ok
	}
	node, ok := pl.Expr.Accept(v)
	if !ok {
		return pl, false
	}
	pl.Expr = node.(ExprNode)
	node, ok = pl.Pattern.Accept(v)
	if !ok {
		return pl, false
	}
	pl.Pattern = node.(ExprNode)
	return v.Leave(pl)
}

// IsStatic implements the ExprNode IsStatic interface.
func (pl *PatternLikeExpr) IsStatic() bool {
	return pl.Expr.IsStatic() && pl.Pattern.IsStatic()
}

// ParamMarkerExpr expresion holds a place for another expression.
// Used in parsing prepare statement.
type ParamMarkerExpr struct {
	exprNode
}

// Accept implements Node Accept interface.
func (pm *ParamMarkerExpr) Accept(v Visitor) (Node, bool) {
	if skipChildren, ok := v.Enter(pm); skipChildren {
		return pm, ok
	}
	return v.Leave(pm)
}

// ParenthesesExpr is the parentheses expression.
type ParenthesesExpr struct {
	exprNode
	// Expr is the expression in parentheses.
	Expr ExprNode
}

// Accept implements Node Accept interface.
func (p *ParenthesesExpr) Accept(v Visitor) (Node, bool) {
	if skipChildren, ok := v.Enter(p); skipChildren {
		return p, ok
	}
	if p.Expr != nil {
		node, ok := p.Expr.Accept(v)
		if !ok {
			return p, false
		}
		p.Expr = node.(ExprNode)
	}
	return v.Leave(p)
}

// IsStatic implements the ExprNode IsStatic interface.
func (p *ParenthesesExpr) IsStatic() bool {
	return p.Expr.IsStatic()
}

// PositionExpr is the expression for order by and group by position.
// MySQL use position expression started from 1, it looks a little confused inner.
// maybe later we will use 0 at first.
type PositionExpr struct {
	exprNode
	// N is the position, started from 1 now.
	N int
	// Name is the corresponding field name if we want better format and explain instead of position.
	Name string
}

// IsStatic implements the ExprNode IsStatic interface.
func (p *PositionExpr) IsStatic() bool {
	return true
}

// Accept implements Node Accept interface.
func (p *PositionExpr) Accept(v Visitor) (Node, bool) {
	if skipChildren, ok := v.Enter(p); skipChildren {
		return p, ok
	}
	return v.Leave(p)
}

// PatternRegexpExpr is the pattern expression for pattern match.
type PatternRegexpExpr struct {
	exprNode
	// Expr is the expression to be checked.
	Expr ExprNode
	// Pattern is the expression for pattern.
	Pattern ExprNode
	// Not is true, the expression is "not rlike",
	Not bool
}

// Accept implements Node Accept interface.
func (p *PatternRegexpExpr) Accept(v Visitor) (Node, bool) {
	if skipChildren, ok := v.Enter(p); skipChildren {
		return p, ok
	}
	node, ok := p.Expr.Accept(v)
	if !ok {
		return p, false
	}
	p.Expr = node.(ExprNode)
	node, ok = p.Pattern.Accept(v)
	if !ok {
		return p, false
	}
	p.Pattern = node.(ExprNode)
	return v.Leave(p)
}

// IsStatic implements the ExprNode IsStatic interface.
func (p *PatternRegexpExpr) IsStatic() bool {
	return p.Expr.IsStatic() && p.Pattern.IsStatic()
}

// RowExpr is the expression for row constructor.
// See https://dev.mysql.com/doc/refman/5.7/en/row-subqueries.html
type RowExpr struct {
	exprNode

	Values []ExprNode
}

// Accept implements Node Accept interface.
func (r *RowExpr) Accept(v Visitor) (Node, bool) {
	if skipChildren, ok := v.Enter(r); skipChildren {
		return r, ok
	}
	for i, val := range r.Values {
		node, ok := val.Accept(v)
		if !ok {
			return r, false
		}
		r.Values[i] = node.(ExprNode)
	}
	return v.Leave(r)
}

// IsStatic implements the ExprNode IsStatic interface.
func (r *RowExpr) IsStatic() bool {
	for _, v := range r.Values {
		if !v.IsStatic() {
			return false
		}
	}
	return true
}

// UnaryOperationExpr is the expression for unary operator.
type UnaryOperationExpr struct {
	exprNode
	// Op is the operator opcode.
	Op opcode.Op
	// V is the unary expression.
	V ExprNode
}

// Accept implements Node Accept interface.
func (u *UnaryOperationExpr) Accept(v Visitor) (Node, bool) {
	if skipChildren, ok := v.Enter(u); skipChildren {
		return u, ok
	}
	node, ok := u.V.Accept(v)
	if !ok {
		return u, false
	}
	u.V = node.(ExprNode)
	return v.Leave(u)
}

// IsStatic implements the ExprNode IsStatic interface.
func (u *UnaryOperationExpr) IsStatic() bool {
	return u.V.IsStatic()
}

// ValuesExpr is the expression used in INSERT VALUES
type ValuesExpr struct {
	exprNode
	// model.CIStr is column name.
	Column *ColumnName
}

// Accept implements Node Accept interface.
func (va *ValuesExpr) Accept(v Visitor) (Node, bool) {
	if skipChildren, ok := v.Enter(va); skipChildren {
		return va, ok
	}
	node, ok := va.Column.Accept(v)
	if !ok {
		return va, false
	}
	va.Column = node.(*ColumnName)
	return v.Leave(va)
}

// VariableExpr is the expression for variable.
type VariableExpr struct {
	exprNode
	// Name is the variable name.
	Name string
	// IsGlobal indicates whether this variable is global.
	IsGlobal bool
	// IsSystem indicates whether this variable is a global variable in current session.
	IsSystem bool
}

// Accept implements Node Accept interface.
func (va *VariableExpr) Accept(v Visitor) (Node, bool) {
	if skipChildren, ok := v.Enter(va); skipChildren {
		return va, ok
	}
	return v.Leave(va)
}
