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
	_ FuncNode = &FuncTrimExpr{}
)

// FuncCallExpr is for function expression.
type FuncCallExpr struct {
	funcNode
	// F is the function name.
	F string
	// Args is the function args.
	Args []ExprNode
	// Distinct only affetcts sum, avg, count, group_concat,
	// so we can ignore it in other functions
	Distinct bool
}

// Accept implements Node interface.
func (c *FuncCallExpr) Accept(v Visitor) (Node, bool) {
	if !v.Enter(c) {
		return c, v.OK()
	}
	for i, val := range c.Args {
		node, ok := val.Accept(v)
		if !ok {
			return c, false
		}
		c.Args[i] = node.(ExprNode)
	}
	return v.Leave(c)
}

// IsStatic implements the ExprNode IsStatic interface.
func (c *FuncCallExpr) IsStatic() bool {
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

// FuncExtractExpr is for time extract function.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_extract
type FuncExtractExpr struct {
	funcNode

	Unit string
	Date ExprNode
}

// Accept implements Node Accept interface.
func (ex *FuncExtractExpr) Accept(v Visitor) (Node, bool) {
	if !v.Enter(ex) {
		return ex, v.OK()
	}
	node, ok := ex.Date.Accept(v)
	if !ok {
		return ex, false
	}
	ex.Date = node.(ExprNode)
	return v.Leave(ex)
}

// IsStatic implements the ExprNode IsStatic interface.
func (ex *FuncExtractExpr) IsStatic() bool {
	return ex.Date.IsStatic()
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
func (f *FuncConvertExpr) IsStatic() bool {
	return f.Expr.IsStatic()
}

// Accept implements Node Accept interface.
func (f *FuncConvertExpr) Accept(v Visitor) (Node, bool) {
	if !v.Enter(f) {
		return f, v.OK()
	}
	node, ok := f.Expr.Accept(v)
	if !ok {
		return f, false
	}
	f.Expr = node.(ExprNode)
	return v.Leave(f)
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
func (f *FuncCastExpr) IsStatic() bool {
	return f.Expr.IsStatic()
}

// Accept implements Node Accept interface.
func (f *FuncCastExpr) Accept(v Visitor) (Node, bool) {
	if !v.Enter(f) {
		return f, v.OK()
	}
	node, ok := f.Expr.Accept(v)
	if !ok {
		return f, false
	}
	f.Expr = node.(ExprNode)
	return v.Leave(f)
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
func (sf *FuncSubstringExpr) Accept(v Visitor) (Node, bool) {
	if !v.Enter(sf) {
		return sf, v.OK()
	}
	node, ok := sf.StrExpr.Accept(v)
	if !ok {
		return sf, false
	}
	sf.StrExpr = node.(ExprNode)
	node, ok = sf.Pos.Accept(v)
	if !ok {
		return sf, false
	}
	sf.Pos = node.(ExprNode)
	node, ok = sf.Len.Accept(v)
	if !ok {
		return sf, false
	}
	sf.Len = node.(ExprNode)
	return v.Leave(sf)
}

// IsStatic implements the ExprNode IsStatic interface.
func (sf *FuncSubstringExpr) IsStatic() bool {
	return sf.StrExpr.IsStatic() && sf.Pos.IsStatic() && sf.Len.IsStatic()
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
func (si *FuncSubstringIndexExpr) Accept(v Visitor) (Node, bool) {
	if !v.Enter(si) {
		return si, v.OK()
	}
	node, ok := si.StrExpr.Accept(v)
	if !ok {
		return si, false
	}
	si.StrExpr = node.(ExprNode)
	node, ok = si.Delim.Accept(v)
	if !ok {
		return si, false
	}
	si.Delim = node.(ExprNode)
	node, ok = si.Count.Accept(v)
	if !ok {
		return si, false
	}
	si.Count = node.(ExprNode)
	return v.Leave(si)
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
func (le *FuncLocateExpr) Accept(v Visitor) (Node, bool) {
	if !v.Enter(le) {
		return le, v.OK()
	}
	node, ok := le.Str.Accept(v)
	if !ok {
		return le, false
	}
	le.Str = node.(ExprNode)
	node, ok = le.SubStr.Accept(v)
	if !ok {
		return le, false
	}
	le.SubStr = node.(ExprNode)
	node, ok = le.Pos.Accept(v)
	if !ok {
		return le, false
	}
	le.Pos = node.(ExprNode)
	return v.Leave(le)
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
func (tf *FuncTrimExpr) Accept(v Visitor) (Node, bool) {
	if !v.Enter(tf) {
		return tf, v.OK()
	}
	node, ok := tf.Str.Accept(v)
	if !ok {
		return tf, false
	}
	tf.Str = node.(ExprNode)
	node, ok = tf.RemStr.Accept(v)
	if !ok {
		return tf, false
	}
	tf.RemStr = node.(ExprNode)
	return v.Leave(tf)
}

// IsStatic implements the ExprNode IsStatic interface.
func (tf *FuncTrimExpr) IsStatic() bool {
	return tf.Str.IsStatic() && tf.RemStr.IsStatic()
}

// TypeStar is a special type for "*".
type TypeStar string
