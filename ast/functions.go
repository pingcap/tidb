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

	"github.com/juju/errors"
	"github.com/pingcap/tidb/kv/memkv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/util/types"
)

var (
	_ FuncNode = &AggregateFuncExpr{}
	_ FuncNode = &FuncCallExpr{}
	_ FuncNode = &FuncCastExpr{}
	_ FuncNode = &FuncConvertExpr{}
	_ FuncNode = &FuncDateArithExpr{}
	_ FuncNode = &FuncExtractExpr{}
	_ FuncNode = &FuncLocateExpr{}
	_ FuncNode = &FuncSubstringExpr{}
	_ FuncNode = &FuncSubstringIndexExpr{}
	_ FuncNode = &FuncTrimExpr{}
)

// UnquoteString is not quoted when printed.
type UnquoteString string

// FuncCallExpr is for function expression.
type FuncCallExpr struct {
	funcNode
	// FnName is the function name.
	FnName model.CIStr
	// Args is the function args.
	Args []ExprNode
}

// Accept implements Node interface.
func (n *FuncCallExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*FuncCallExpr)
	for i, val := range n.Args {
		node, ok := val.Accept(v)
		if !ok {
			return n, false
		}
		n.Args[i] = node.(ExprNode)
	}
	return v.Leave(n)
}

// FuncExtractExpr is for time extract function.
// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_extract
type FuncExtractExpr struct {
	funcNode

	Unit string
	Date ExprNode
}

// Accept implements Node Accept interface.
func (n *FuncExtractExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*FuncExtractExpr)
	node, ok := n.Date.Accept(v)
	if !ok {
		return n, false
	}
	n.Date = node.(ExprNode)
	return v.Leave(n)
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

// Accept implements Node Accept interface.
func (n *FuncConvertExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*FuncConvertExpr)
	node, ok := n.Expr.Accept(v)
	if !ok {
		return n, false
	}
	n.Expr = node.(ExprNode)
	return v.Leave(n)
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

// Accept implements Node Accept interface.
func (n *FuncCastExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*FuncCastExpr)
	node, ok := n.Expr.Accept(v)
	if !ok {
		return n, false
	}
	n.Expr = node.(ExprNode)
	return v.Leave(n)
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
func (n *FuncSubstringExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*FuncSubstringExpr)
	node, ok := n.StrExpr.Accept(v)
	if !ok {
		return n, false
	}
	n.StrExpr = node.(ExprNode)
	node, ok = n.Pos.Accept(v)
	if !ok {
		return n, false
	}
	n.Pos = node.(ExprNode)
	if n.Len != nil {
		node, ok = n.Len.Accept(v)
		if !ok {
			return n, false
		}
		n.Len = node.(ExprNode)
	}
	return v.Leave(n)
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
func (n *FuncSubstringIndexExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*FuncSubstringIndexExpr)
	node, ok := n.StrExpr.Accept(v)
	if !ok {
		return n, false
	}
	n.StrExpr = node.(ExprNode)
	node, ok = n.Delim.Accept(v)
	if !ok {
		return n, false
	}
	n.Delim = node.(ExprNode)
	node, ok = n.Count.Accept(v)
	if !ok {
		return n, false
	}
	n.Count = node.(ExprNode)
	return v.Leave(n)
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
func (n *FuncLocateExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*FuncLocateExpr)
	node, ok := n.Str.Accept(v)
	if !ok {
		return n, false
	}
	n.Str = node.(ExprNode)
	node, ok = n.SubStr.Accept(v)
	if !ok {
		return n, false
	}
	n.SubStr = node.(ExprNode)
	node, ok = n.Pos.Accept(v)
	if !ok {
		return n, false
	}
	n.Pos = node.(ExprNode)
	return v.Leave(n)
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
func (n *FuncTrimExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*FuncTrimExpr)
	node, ok := n.Str.Accept(v)
	if !ok {
		return n, false
	}
	n.Str = node.(ExprNode)
	if n.RemStr != nil {
		node, ok = n.RemStr.Accept(v)
		if !ok {
			return n, false
		}
		n.RemStr = node.(ExprNode)
	}
	return v.Leave(n)
}

// DateArithType is type for DateArith type.
type DateArithType byte

const (
	// DateAdd is to run adddate or date_add function option.
	// See: https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_adddate
	// See: https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_date-add
	DateAdd DateArithType = iota + 1
	// DateSub is to run subdate or date_sub function option.
	// See: https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subdate
	// See: https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_date-sub
	DateSub
)

// DateArithInterval is the struct of DateArith interval part.
type DateArithInterval struct {
	Unit     string
	Interval ExprNode
}

// FuncDateArithExpr is the struct for date arithmetic functions.
type FuncDateArithExpr struct {
	funcNode

	// Op is used for distinguishing date_add and date_sub.
	Op   DateArithType
	Date ExprNode
	DateArithInterval
}

// Accept implements Node Accept interface.
func (n *FuncDateArithExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*FuncDateArithExpr)
	if n.Date != nil {
		node, ok := n.Date.Accept(v)
		if !ok {
			return n, false
		}
		n.Date = node.(ExprNode)
	}
	if n.Interval != nil {
		node, ok := n.Interval.Accept(v)
		if !ok {
			return n, false
		}
		n.Interval = node.(ExprNode)
	}
	return v.Leave(n)
}

const (
	// AggFuncCount is the name of Count function.
	AggFuncCount = "count"
	// AggFuncSum is the name of Sum function.
	AggFuncSum = "sum"
	// AggFuncAvg is the name of Avg function.
	AggFuncAvg = "avg"
	// AggFuncFirstRow is the name of FirstRowColumn function.
	AggFuncFirstRow = "firstrow"
)

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

	CurrentGroup string
	// ContextPerGroup is used to store aggregate evaluation context.
	// Each entry for a group.
	ContextPerGroup map[string](*AggEvaluateContext)
}

// Accept implements Node Accept interface.
func (n *AggregateFuncExpr) Accept(v Visitor) (Node, bool) {
	newNode, skipChildren := v.Enter(n)
	if skipChildren {
		return v.Leave(newNode)
	}
	n = newNode.(*AggregateFuncExpr)
	for i, val := range n.Args {
		node, ok := val.Accept(v)
		if !ok {
			return n, false
		}
		n.Args[i] = node.(ExprNode)
	}
	return v.Leave(n)
}

// Update is used for update aggregate context.
func (n *AggregateFuncExpr) Update() error {
	name := strings.ToLower(n.F)
	switch name {
	case AggFuncCount:
		return n.updateCount()
	case AggFuncFirstRow:
		return n.updateFirstRow()
	}
	return nil
}

// GetContext get aggregate evaluation context for the current group.
// If it is nil, add a new context into ContextPerGroup.
func (n *AggregateFuncExpr) GetContext() *AggEvaluateContext {
	if n.ContextPerGroup == nil {
		n.ContextPerGroup = make(map[string](*AggEvaluateContext))
	}
	if _, ok := n.ContextPerGroup[n.CurrentGroup]; !ok {
		c := &AggEvaluateContext{}
		if n.Distinct {
			c.Distinct = CreateAggregateDistinct(n.F, n.Distinct)
		}
		n.ContextPerGroup[n.CurrentGroup] = c
	}
	return n.ContextPerGroup[n.CurrentGroup]
}

func (n *AggregateFuncExpr) updateCount() error {
	ctx := n.GetContext()
	var value interface{}
	if len(n.Args) > 0 {
		hasNil := false
		for _, a := range n.Args {
			value = a.GetValue()
			if value == nil {
				hasNil = true
			}
		}
		if hasNil {
			return nil
		}
	}
	if n.Distinct {
		// TODO: compose values into value. For example select count(DISTINCT c1, c2) from t;
		d, err := ctx.Distinct.IsDistinct(value)
		if err != nil {
			return errors.Trace(err)
		}
		if !d {
			return nil
		}
	}
	ctx.Count++
	return nil
}

func (n *AggregateFuncExpr) updateFirstRow() error {
	ctx := n.GetContext()
	if ctx.evaluated {
		return nil
	}
	if len(n.Args) != 1 {
		return errors.New("Wrong number of args for AggFuncFirstRow")
	}
	ctx.Value = n.Args[0].GetValue()
	ctx.evaluated = true
	return nil
}

// AggFuncDetector visits Expr tree to check if it contains AggregateFuncExpr.
type AggFuncDetector struct {
	HasAggFunc bool
	detecting  bool
}

// Enter implemets Visitor interface.
func (a *AggFuncDetector) Enter(n Node) (node Node, skipChildren bool) {
	defer func() {
		a.detecting = true
	}()
	switch n.(type) {
	case *AggregateFuncExpr, *GroupByClause:
		a.HasAggFunc = true
	case *SelectStmt, *InsertStmt, *DeleteStmt, *UpdateStmt:
		if a.detecting {
			// Enter a new context, skip it.
			// For example: select sum(c) + c + exists(select c from t) from t;
			return n, true
		}
	}
	return n, a.HasAggFunc
}

// Leave implemets Visitor interface.
func (a *AggFuncDetector) Leave(n Node) (node Node, ok bool) {
	return n, !a.HasAggFunc
}

// AggregateFuncExtractor visit Expr tree.
// It converts ColunmNameExpr to AggregateFuncExpr and collects AggregateFuncExpr.
type AggregateFuncExtractor struct {
	inAggregateFuncExpr bool
	inSelectFields      bool
	// AggFuncs is the collected AggregateFuncExprs.
	AggFuncs  []*AggregateFuncExpr
	detecting bool
}

// Enter implemets Visitor interface.
func (a *AggregateFuncExtractor) Enter(n Node) (node Node, skipChildren bool) {
	defer func() {
		a.detecting = true
	}()
	switch v := n.(type) {
	case *AggregateFuncExpr:
		a.inAggregateFuncExpr = true
		a.AggFuncs = append(a.AggFuncs, v)
	case *SelectStmt, *InsertStmt, *DeleteStmt, *UpdateStmt:
		// Enter a new context, skip it.
		// For example: select sum(c) + c + exists(select c from t) from t;
		if a.detecting {
			return n, true
		}
	}
	return n, false
}

// Leave implemets Visitor interface.
func (a *AggregateFuncExtractor) Leave(n Node) (node Node, ok bool) {
	switch n.(type) {
	case *AggregateFuncExpr:
		a.inAggregateFuncExpr = false
	case *ColumnNameExpr:
		// compose new AggregateFuncExpr
		if !a.inAggregateFuncExpr {
			// For example: select sum(c) + c from t;
			// The c in sum() should be evaluated for each row.
			// The c after plus should be evaluated only once.
			agg := &AggregateFuncExpr{
				F:    AggFuncFirstRow,
				Args: []ExprNode{n.(ExprNode)},
			}
			a.AggFuncs = append(a.AggFuncs, agg)
			return agg, true
		}
	}
	return n, true
}

// AggregateDistinct handles distinct data for aggregate function: count, sum, avg, and group_concat.
type AggregateDistinct struct {
	// Distinct is a memory key-value map.
	// Now we have to use memkv Temp, later may be use map directly
	Distinct memkv.Temp
}

// CreateAggregateDistinct creates a distinct for function f.
func CreateAggregateDistinct(f string, distinct bool) *AggregateDistinct {
	a := &AggregateDistinct{}

	switch strings.ToLower(f) {
	case "count", "sum", "avg", "group_concat":
		// only these aggregate functions support distinct
		if distinct {
			a.Distinct, _ = memkv.CreateTemp(true)
		}
	}

	return a
}

// IsDistinct checks whether v is distinct or not. It returns true for distinct value.
func (a *AggregateDistinct) IsDistinct(v ...interface{}) (bool, error) {
	// no distinct flag
	if a.Distinct == nil {
		return true, nil
	}

	k := v
	r, err := a.Distinct.Get(k)
	if err != nil {
		return false, nil
	}

	if len(r) > 0 {
		// we save a same value before
		return false, nil
	}

	if err := a.Distinct.Set(k, []interface{}{true}); err != nil {
		return false, err
	}

	return true, nil
}

// Clear does cleanup job.
func (a *AggregateDistinct) Clear() {
	if a.Distinct == nil {
		return
	}

	// drop does nothing, no need to check error
	a.Distinct.Drop()
	// CreateTemp returns no error, no need to check error
	// later we may use another better way instead of memkv
	a.Distinct, _ = memkv.CreateTemp(true)
}

// AggEvaluateContext is used to store intermediate result when caculation aggregate functions.
type AggEvaluateContext struct {
	Distinct  *AggregateDistinct
	Count     int64
	Value     interface{}
	evaluated bool
}
