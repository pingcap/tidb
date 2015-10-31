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
	"fmt"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/util/types"
)

var (
	_ ExprNode = &ValueExpr{}
	_ ExprNode = &BetweenExpr{}
	_ ExprNode = &BinaryOperationExpr{}
	_ Node     = &WhenClause{}
	_ ExprNode = &CaseExpr{}
	_ ExprNode = &SubqueryExpr{}
	_ ExprNode = &CompareSubqueryExpr{}
	_ Node     = &ColumnName{}
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
}

// NewValueExpr creates a ValueExpr with value, and sets default field type.
func NewValueExpr(value interface{}) *ValueExpr {
	ve := &ValueExpr{}
	ve.Data = types.RawData(value)
	// TODO: make it more precise.
	switch value.(type) {
	case nil:
		ve.Type = types.NewFieldType(mysql.TypeNull)
	case bool, int64:
		ve.Type = types.NewFieldType(mysql.TypeLonglong)
	case uint64:
		ve.Type = types.NewFieldType(mysql.TypeLonglong)
		ve.Type.Flag |= mysql.UnsignedFlag
	case string:
		ve.Type = types.NewFieldType(mysql.TypeVarchar)
		ve.Type.Charset = mysql.DefaultCharset
		ve.Type.Collate = mysql.DefaultCollationName
	case float64:
		ve.Type = types.NewFieldType(mysql.TypeDouble)
	case []byte:
		ve.Type = types.NewFieldType(mysql.TypeBlob)
		ve.Type.Charset = "binary"
		ve.Type.Collate = "binary"
	case mysql.Bit:
		ve.Type = types.NewFieldType(mysql.TypeBit)
	case mysql.Hex:
		ve.Type = types.NewFieldType(mysql.TypeVarchar)
		ve.Type.Charset = "binary"
		ve.Type.Collate = "binary"
	case *types.DataItem:
		ve.Type = value.(*types.DataItem).Type
	default:
		panic(fmt.Sprintf("illegal literal value type:%T", value))
	}
	return ve
}

// IsStatic implements ExprNode interface.
func (nod *ValueExpr) IsStatic() bool {
	return true
}

// Accept implements Node interface.
func (nod *ValueExpr) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*ValueExpr)
	return v.Leave(nod)
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
func (nod *BetweenExpr) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*BetweenExpr)

	node, ok := nod.Expr.Accept(v)
	if !ok {
		return nod, false
	}
	nod.Expr = node.(ExprNode)

	node, ok = nod.Left.Accept(v)
	if !ok {
		return nod, false
	}
	nod.Left = node.(ExprNode)

	node, ok = nod.Right.Accept(v)
	if !ok {
		return nod, false
	}
	nod.Right = node.(ExprNode)

	return v.Leave(nod)
}

// IsStatic implements the ExprNode IsStatic interface.
func (nod *BetweenExpr) IsStatic() bool {
	return nod.Expr.IsStatic() && nod.Left.IsStatic() && nod.Right.IsStatic()
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
func (nod *BinaryOperationExpr) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*BinaryOperationExpr)

	node, ok := nod.L.Accept(v)
	if !ok {
		return nod, false
	}
	nod.L = node.(ExprNode)

	node, ok = nod.R.Accept(v)
	if !ok {
		return nod, false
	}
	nod.R = node.(ExprNode)

	return v.Leave(nod)
}

// IsStatic implements the ExprNode IsStatic interface.
func (nod *BinaryOperationExpr) IsStatic() bool {
	return nod.L.IsStatic() && nod.R.IsStatic()
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
func (nod *WhenClause) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*WhenClause)
	node, ok := nod.Expr.Accept(v)
	if !ok {
		return nod, false
	}
	nod.Expr = node.(ExprNode)

	node, ok = nod.Result.Accept(v)
	if !ok {
		return nod, false
	}
	nod.Result = node.(ExprNode)
	return v.Leave(nod)
}

// IsStatic implements the ExprNode IsStatic interface.
func (nod *WhenClause) IsStatic() bool {
	return nod.Expr.IsStatic() && nod.Result.IsStatic()
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
func (nod *CaseExpr) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*CaseExpr)
	if nod.Value != nil {
		node, ok := nod.Value.Accept(v)
		if !ok {
			return nod, false
		}
		nod.Value = node.(ExprNode)
	}
	for i, val := range nod.WhenClauses {
		node, ok := val.Accept(v)
		if !ok {
			return nod, false
		}
		nod.WhenClauses[i] = node.(*WhenClause)
	}
	if nod.ElseClause != nil {
		node, ok := nod.ElseClause.Accept(v)
		if !ok {
			return nod, false
		}
		nod.ElseClause = node.(ExprNode)
	}
	return v.Leave(nod)
}

// IsStatic implements the ExprNode IsStatic interface.
func (nod *CaseExpr) IsStatic() bool {
	if nod.Value != nil && !nod.Value.IsStatic() {
		return false
	}
	for _, w := range nod.WhenClauses {
		if !w.IsStatic() {
			return false
		}
	}
	if nod.ElseClause != nil && !nod.ElseClause.IsStatic() {
		return false
	}
	return true
}

// SubqueryExpr represents a sub query.
type SubqueryExpr struct {
	exprNode
	// Query is the query SelectNode.
	Query ResultSetNode
}

// Accept implements Node Accept interface.
func (nod *SubqueryExpr) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*SubqueryExpr)
	node, ok := nod.Query.Accept(v)
	if !ok {
		return nod, false
	}
	nod.Query = node.(ResultSetNode)
	return v.Leave(nod)
}

// SetResultFields implements ResultSet interface.
func (nod *SubqueryExpr) SetResultFields(rfs []*ResultField) {
	nod.Query.SetResultFields(rfs)
}

// GetResultFields implements ResultSet interface.
func (nod *SubqueryExpr) GetResultFields() []*ResultField {
	return nod.Query.GetResultFields()
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
func (nod *CompareSubqueryExpr) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*CompareSubqueryExpr)
	node, ok := nod.L.Accept(v)
	if !ok {
		return nod, false
	}
	nod.L = node.(ExprNode)
	node, ok = nod.R.Accept(v)
	if !ok {
		return nod, false
	}
	nod.R = node.(*SubqueryExpr)
	return v.Leave(nod)
}

// ColumnName represents column name.
type ColumnName struct {
	node
	Schema model.CIStr
	Table  model.CIStr
	Name   model.CIStr

	DBInfo     *model.DBInfo
	TableInfo  *model.TableInfo
	ColumnInfo *model.ColumnInfo
}

// Accept implements Node Accept interface.
func (nod *ColumnName) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*ColumnName)
	return v.Leave(nod)
}

// ColumnNameExpr represents a column name expression.
type ColumnNameExpr struct {
	exprNode

	// Name is the referenced column name.
	Name *ColumnName
}

// Accept implements Node Accept interface.
func (nod *ColumnNameExpr) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*ColumnNameExpr)
	node, ok := nod.Name.Accept(v)
	if !ok {
		return nod, false
	}
	nod.Name = node.(*ColumnName)
	return v.Leave(nod)
}

// DefaultExpr is the default expression using default value for a column.
type DefaultExpr struct {
	exprNode
	// Name is the column name.
	Name *ColumnName
}

// Accept implements Node Accept interface.
func (nod *DefaultExpr) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*DefaultExpr)
	if nod.Name != nil {
		node, ok := nod.Name.Accept(v)
		if !ok {
			return nod, false
		}
		nod.Name = node.(*ColumnName)
	}
	return v.Leave(nod)
}

// IdentifierExpr represents an identifier expression.
type IdentifierExpr struct {
	exprNode
	// Name is the identifier name.
	Name model.CIStr
}

// Accept implements Node Accept interface.
func (nod *IdentifierExpr) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*IdentifierExpr)
	return v.Leave(nod)
}

// ExistsSubqueryExpr is the expression for "exists (select ...)".
// https://dev.mysql.com/doc/refman/5.7/en/exists-and-not-exists-subqueries.html
type ExistsSubqueryExpr struct {
	exprNode
	// Sel is the sub query.
	Sel *SubqueryExpr
}

// Accept implements Node Accept interface.
func (nod *ExistsSubqueryExpr) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*ExistsSubqueryExpr)
	node, ok := nod.Sel.Accept(v)
	if !ok {
		return nod, false
	}
	nod.Sel = node.(*SubqueryExpr)
	return v.Leave(nod)
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
func (nod *PatternInExpr) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*PatternInExpr)
	node, ok := nod.Expr.Accept(v)
	if !ok {
		return nod, false
	}
	nod.Expr = node.(ExprNode)
	for i, val := range nod.List {
		node, ok = val.Accept(v)
		if !ok {
			return nod, false
		}
		nod.List[i] = node.(ExprNode)
	}
	if nod.Sel != nil {
		node, ok = nod.Sel.Accept(v)
		if !ok {
			return nod, false
		}
		nod.Sel = node.(*SubqueryExpr)
	}
	return v.Leave(nod)
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
func (nod *IsNullExpr) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*IsNullExpr)
	node, ok := nod.Expr.Accept(v)
	if !ok {
		return nod, false
	}
	nod.Expr = node.(ExprNode)
	return v.Leave(nod)
}

// IsStatic implements the ExprNode IsStatic interface.
func (nod *IsNullExpr) IsStatic() bool {
	return nod.Expr.IsStatic()
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
func (nod *IsTruthExpr) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*IsTruthExpr)
	node, ok := nod.Expr.Accept(v)
	if !ok {
		return nod, false
	}
	nod.Expr = node.(ExprNode)
	return v.Leave(nod)
}

// IsStatic implements the ExprNode IsStatic interface.
func (nod *IsTruthExpr) IsStatic() bool {
	return nod.Expr.IsStatic()
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
func (nod *PatternLikeExpr) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*PatternLikeExpr)
	if nod.Expr != nil {
		node, ok := nod.Expr.Accept(v)
		if !ok {
			return nod, false
		}
		nod.Expr = node.(ExprNode)
	}
	if nod.Pattern != nil {
		node, ok := nod.Pattern.Accept(v)
		if !ok {
			return nod, false
		}
		nod.Pattern = node.(ExprNode)
	}
	return v.Leave(nod)
}

// IsStatic implements the ExprNode IsStatic interface.
func (nod *PatternLikeExpr) IsStatic() bool {
	return nod.Expr.IsStatic() && nod.Pattern.IsStatic()
}

// ParamMarkerExpr expresion holds a place for another expression.
// Used in parsing prepare statement.
type ParamMarkerExpr struct {
	exprNode
}

// Accept implements Node Accept interface.
func (nod *ParamMarkerExpr) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*ParamMarkerExpr)
	return v.Leave(nod)
}

// ParenthesesExpr is the parentheses expression.
type ParenthesesExpr struct {
	exprNode
	// Expr is the expression in parentheses.
	Expr ExprNode
}

// Accept implements Node Accept interface.
func (nod *ParenthesesExpr) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*ParenthesesExpr)
	if nod.Expr != nil {
		node, ok := nod.Expr.Accept(v)
		if !ok {
			return nod, false
		}
		nod.Expr = node.(ExprNode)
	}
	return v.Leave(nod)
}

// IsStatic implements the ExprNode IsStatic interface.
func (nod *ParenthesesExpr) IsStatic() bool {
	return nod.Expr.IsStatic()
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
func (nod *PositionExpr) IsStatic() bool {
	return true
}

// Accept implements Node Accept interface.
func (nod *PositionExpr) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*PositionExpr)
	return v.Leave(nod)
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
func (nod *PatternRegexpExpr) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*PatternRegexpExpr)
	node, ok := nod.Expr.Accept(v)
	if !ok {
		return nod, false
	}
	nod.Expr = node.(ExprNode)
	node, ok = nod.Pattern.Accept(v)
	if !ok {
		return nod, false
	}
	nod.Pattern = node.(ExprNode)
	return v.Leave(nod)
}

// IsStatic implements the ExprNode IsStatic interface.
func (nod *PatternRegexpExpr) IsStatic() bool {
	return nod.Expr.IsStatic() && nod.Pattern.IsStatic()
}

// RowExpr is the expression for row constructor.
// See https://dev.mysql.com/doc/refman/5.7/en/row-subqueries.html
type RowExpr struct {
	exprNode

	Values []ExprNode
}

// Accept implements Node Accept interface.
func (nod *RowExpr) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*RowExpr)
	for i, val := range nod.Values {
		node, ok := val.Accept(v)
		if !ok {
			return nod, false
		}
		nod.Values[i] = node.(ExprNode)
	}
	return v.Leave(nod)
}

// IsStatic implements the ExprNode IsStatic interface.
func (nod *RowExpr) IsStatic() bool {
	for _, v := range nod.Values {
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
func (nod *UnaryOperationExpr) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*UnaryOperationExpr)
	node, ok := nod.V.Accept(v)
	if !ok {
		return nod, false
	}
	nod.V = node.(ExprNode)
	return v.Leave(nod)
}

// IsStatic implements the ExprNode IsStatic interface.
func (nod *UnaryOperationExpr) IsStatic() bool {
	return nod.V.IsStatic()
}

// ValuesExpr is the expression used in INSERT VALUES
type ValuesExpr struct {
	exprNode
	// model.CIStr is column name.
	Column *ColumnName
}

// Accept implements Node Accept interface.
func (nod *ValuesExpr) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*ValuesExpr)
	node, ok := nod.Column.Accept(v)
	if !ok {
		return nod, false
	}
	nod.Column = node.(*ColumnName)
	return v.Leave(nod)
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
func (nod *VariableExpr) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*VariableExpr)
	return v.Leave(nod)
}
