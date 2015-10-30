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
	"strings"

	"github.com/pingcap/tidb/expression/builtin"
	"github.com/pingcap/tidb/util/types"
)

var (
	_ FuncNode = &FuncCallExpr{}
	_ FuncNode = &FuncExtractExpr{}
	_ FuncNode = &FuncConvertExpr{}
	_ FuncNode = &FuncCastExpr{}
	_ FuncNode = &FuncSubstringExpr{}
	_ FuncNode = &FuncLocateExpr{}
	_ FuncNode = &FuncTrimExpr{}
	_ FuncNode = &AggregateFuncExpr{}
)

// FuncCallExpr is for function expression.
type FuncCallExpr struct {
	funcNode
	// F is the function name.
	F string
	// Args is the function args.
	Args []ExprNode
}

// Accept implements Node interface.
func (nod *FuncCallExpr) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*FuncCallExpr)
	for i, val := range nod.Args {
		node, ok := val.Accept(v)
		if !ok {
			return nod, false
		}
		nod.Args[i] = node.(ExprNode)
	}
	return v.Leave(nod)
}

// IsStatic implements the ExprNode IsStatic interface.
func (nod *FuncCallExpr) IsStatic() bool {
	v := builtin.Funcs[strings.ToLower(nod.F)]
	if v.F == nil || !v.IsStatic {
		return false
	}

	for _, v := range nod.Args {
		if !v.IsStatic() {
			return false
		}
	}
	return true
}

// FuncExtractExpr is for time extract function.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_extract
type FuncExtractExpr struct {
	funcNode

	Unit string
	Date ExprNode
}

// Accept implements Node Accept interface.
func (nod *FuncExtractExpr) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*FuncExtractExpr)
	node, ok := nod.Date.Accept(v)
	if !ok {
		return nod, false
	}
	nod.Date = node.(ExprNode)
	return v.Leave(nod)
}

// IsStatic implements the ExprNode IsStatic interface.
func (nod *FuncExtractExpr) IsStatic() bool {
	return nod.Date.IsStatic()
}

// FuncConvertExpr provides a way to convert data between different character sets.
// See: https://dev.mysql.com/doc/refman/5.7/en/cast-functions.html#function_convert
type FuncConvertExpr struct {
	funcNode
	// Expr is the expression to be converted.
	Expr ExprNode
	// Charset is the target character set to convert.
	Charset string
}

// IsStatic implements the ExprNode IsStatic interface.
func (nod *FuncConvertExpr) IsStatic() bool {
	return nod.Expr.IsStatic()
}

// Accept implements Node Accept interface.
func (nod *FuncConvertExpr) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*FuncConvertExpr)
	node, ok := nod.Expr.Accept(v)
	if !ok {
		return nod, false
	}
	nod.Expr = node.(ExprNode)
	return v.Leave(nod)
}

// CastFunctionType is the type for cast function.
type CastFunctionType int

// CastFunction types
const (
	CastFunction CastFunctionType = iota + 1
	CastConvertFunction
	CastBinaryOperator
)

// FuncCastExpr is the cast function converting value to another type, e.g, cast(expr AS signed).
// See https://dev.mysql.com/doc/refman/5.7/en/cast-functions.html
type FuncCastExpr struct {
	funcNode
	// Expr is the expression to be converted.
	Expr ExprNode
	// Tp is the conversion type.
	Tp *types.FieldType
	// Cast, Convert and Binary share this struct.
	FunctionType CastFunctionType
}

// IsStatic implements the ExprNode IsStatic interface.
func (nod *FuncCastExpr) IsStatic() bool {
	return nod.Expr.IsStatic()
}

// Accept implements Node Accept interface.
func (nod *FuncCastExpr) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*FuncCastExpr)
	node, ok := nod.Expr.Accept(v)
	if !ok {
		return nod, false
	}
	nod.Expr = node.(ExprNode)
	return v.Leave(nod)
}

// FuncSubstringExpr returns the substring as specified.
// See: https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_substring
type FuncSubstringExpr struct {
	funcNode

	StrExpr ExprNode
	Pos     ExprNode
	Len     ExprNode
}

// Accept implements Node Accept interface.
func (nod *FuncSubstringExpr) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*FuncSubstringExpr)
	node, ok := nod.StrExpr.Accept(v)
	if !ok {
		return nod, false
	}
	nod.StrExpr = node.(ExprNode)
	node, ok = nod.Pos.Accept(v)
	if !ok {
		return nod, false
	}
	nod.Pos = node.(ExprNode)
	if nod.Len != nil {
		node, ok = nod.Len.Accept(v)
		if !ok {
			return nod, false
		}
		nod.Len = node.(ExprNode)
	}
	return v.Leave(nod)
}

// IsStatic implements the ExprNode IsStatic interface.
func (nod *FuncSubstringExpr) IsStatic() bool {
	return nod.StrExpr.IsStatic() && nod.Pos.IsStatic() && nod.Len.IsStatic()
}

// FuncSubstringIndexExpr returns the substring as specified.
// See: https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_substring-index
type FuncSubstringIndexExpr struct {
	funcNode

	StrExpr ExprNode
	Delim   ExprNode
	Count   ExprNode
}

// Accept implements Node Accept interface.
func (nod *FuncSubstringIndexExpr) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*FuncSubstringIndexExpr)
	node, ok := nod.StrExpr.Accept(v)
	if !ok {
		return nod, false
	}
	nod.StrExpr = node.(ExprNode)
	node, ok = nod.Delim.Accept(v)
	if !ok {
		return nod, false
	}
	nod.Delim = node.(ExprNode)
	node, ok = nod.Count.Accept(v)
	if !ok {
		return nod, false
	}
	nod.Count = node.(ExprNode)
	return v.Leave(nod)
}

// FuncLocateExpr returns the position of the first occurrence of substring.
// See: https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_locate
type FuncLocateExpr struct {
	funcNode

	Str    ExprNode
	SubStr ExprNode
	Pos    ExprNode
}

// Accept implements Node Accept interface.
func (nod *FuncLocateExpr) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*FuncLocateExpr)
	node, ok := nod.Str.Accept(v)
	if !ok {
		return nod, false
	}
	nod.Str = node.(ExprNode)
	node, ok = nod.SubStr.Accept(v)
	if !ok {
		return nod, false
	}
	nod.SubStr = node.(ExprNode)
	node, ok = nod.Pos.Accept(v)
	if !ok {
		return nod, false
	}
	nod.Pos = node.(ExprNode)
	return v.Leave(nod)
}

// TrimDirectionType is the type for trim direction.
type TrimDirectionType int

const (
	// TrimBothDefault trims from both direction by default.
	TrimBothDefault TrimDirectionType = iota
	// TrimBoth trims from both direction with explicit notation.
	TrimBoth
	// TrimLeading trims from left.
	TrimLeading
	// TrimTrailing trims from right.
	TrimTrailing
)

// FuncTrimExpr remove leading/trailing/both remstr.
// See: https://dev.mysql.com/doc/refman/5.7/en/string-functions.html#function_trim
type FuncTrimExpr struct {
	funcNode

	Str       ExprNode
	RemStr    ExprNode
	Direction TrimDirectionType
}

// Accept implements Node Accept interface.
func (nod *FuncTrimExpr) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*FuncTrimExpr)
	node, ok := nod.Str.Accept(v)
	if !ok {
		return nod, false
	}
	nod.Str = node.(ExprNode)
	node, ok = nod.RemStr.Accept(v)
	if !ok {
		return nod, false
	}
	nod.RemStr = node.(ExprNode)
	return v.Leave(nod)
}

// IsStatic implements the ExprNode IsStatic interface.
func (nod *FuncTrimExpr) IsStatic() bool {
	return nod.Str.IsStatic() && nod.RemStr.IsStatic()
}

// AggregateFuncExpr represents aggregate function expression.
type AggregateFuncExpr struct {
	funcNode
	// F is the function name.
	F string
	// Args is the function args.
	Args []ExprNode
	// If distinct is true, the function only aggregate distinct values.
	// For example, column c1 values are "1", "2", "2",  "sum(c1)" is "5",
	// but "sum(distinct c1)" is "3".
	Distinct bool
}

// Accept implements Node Accept interface.
func (nod *AggregateFuncExpr) Accept(v Visitor) (Node, bool) {
	newNod, skipChildren := v.Enter(nod)
	if skipChildren {
		return v.Leave(newNod)
	}
	nod = newNod.(*AggregateFuncExpr)
	for i, val := range nod.Args {
		node, ok := val.Accept(v)
		if !ok {
			return nod, false
		}
		nod.Args[i] = node.(ExprNode)
	}
	return v.Leave(nod)
}
