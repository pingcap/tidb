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
	"bytes"
	"fmt"
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/util/distinct"
	"github.com/pingcap/tidb/util/types"
)

var (
	_ FuncNode = &AggregateFuncExpr{}
	_ FuncNode = &FuncCallExpr{}
	_ FuncNode = &FuncCastExpr{}
)

// List scalar function names.
const (
	AndAnd     = "and"
	LeftShift  = "leftshift"
	RightShift = "rightshift"
	OrOr       = "or"
	GE         = "ge"
	LE         = "le"
	EQ         = "eq"
	NE         = "ne"
	LT         = "lt"
	GT         = "gt"
	Plus       = "plus"
	Minus      = "minus"
	And        = "bitand"
	Or         = "bitor"
	Mod        = "mod"
	Xor        = "bitxor"
	Div        = "div"
	Mul        = "mul"
	UnaryNot   = "not" // Avoid name conflict with Not in github/pingcap/check.
	BitNeg     = "bitneg"
	IntDiv     = "intdiv"
	LogicXor   = "xor"
	NullEQ     = "nulleq"
	UnaryPlus  = "unaryplus"
	UnaryMinus = "unaryminus"
	In         = "in"
	Like       = "like"
	Case       = "case"
	Regexp     = "regexp"
	IsNull     = "isnull"
	IsTruth    = "istrue"  // Avoid name conflict with IsTrue in github/pingcap/check.
	IsFalsity  = "isfalse" // Avoid name conflict with IsFalse in github/pingcap/check.
	RowFunc    = "row"
	SetVar     = "setvar"
	GetVar     = "getvar"

	// common functions
	Coalesce = "coalesce"
	Greatest = "greatest"

	// math functions
	Abs     = "abs"
	Ceil    = "ceil"
	Ceiling = "ceiling"
	Pow     = "pow"
	Power   = "power"
	Rand    = "rand"
	Round   = "round"

	// time functions
	Curdate          = "curdate"
	CurrentDate      = "current_date"
	CurrentTime      = "current_time"
	CurrentTimestamp = "current_timestamp"
	Curtime          = "curtime"
	Date             = "date"
	DateArith        = "date_arith"
	DateFormat       = "date_format"
	Day              = "day"
	DayName          = "dayname"
	DayOfMonth       = "dayofmonth"
	DayOfWeek        = "dayofweek"
	DayOfYear        = "dayofyear"
	Extract          = "extract"
	Hour             = "hour"
	MicroSecond      = "microsecond"
	Minute           = "minute"
	Month            = "month"
	MonthName        = "monthname"
	Now              = "now"
	Second           = "second"
	Sysdate          = "sysdate"
	Time             = "time"
	UTCDate          = "utc_date"
	Week             = "week"
	Weekday          = "weekday"
	WeekOfYear       = "weekofyear"
	Year             = "year"
	YearWeek         = "yearweek"

	// string functions
	ASCII          = "ascii"
	Concat         = "concat"
	ConcatWS       = "concat_ws"
	Convert        = "convert"
	Lcase          = "lcase"
	Left           = "left"
	Length         = "length"
	Locate         = "locate"
	Lower          = "lower"
	Ltrim          = "ltrim"
	Repeat         = "repeat"
	Replace        = "replace"
	Reverse        = "reverse"
	Rtrim          = "rtrim"
	Space          = "space"
	Strcmp         = "strcmp"
	Substring      = "substring"
	SubstringIndex = "substring_index"
	Trim           = "trim"
	Upper          = "upper"
	Ucase          = "ucase"
	Hex            = "hex"
	Unhex          = "unhex"

	// information functions
	ConnectionID = "connection_id"
	CurrentUser  = "current_user"
	Database     = "database"
	FoundRows    = "found_rows"
	LastInsertId = "last_insert_id"
	User         = "user"
	Version      = "version"

	// control functions
	If     = "if"
	Ifnull = "ifnull"
	Nullif = "nullif"

	// miscellaneous functions
	Sleep = "sleep"

	// get_lock() and release_lock() is parsed but do nothing.
	// It is used for preventing error in Ruby's activerecord migrations.
	GetLock     = "get_lock"
	ReleaseLock = "release_lock"
)

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

// DateArithType is type for DateArith type.
type DateArithType byte

const (
	// DateAdd is to run adddate or date_add function option.
	// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_adddate
	// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_date-add
	DateAdd DateArithType = iota + 1
	// DateSub is to run subdate or date_sub function option.
	// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_subdate
	// See https://dev.mysql.com/doc/refman/5.7/en/date-and-time-functions.html#function_date-sub
	DateSub
)

// DateArithInterval is the struct of DateArith interval part.
type DateArithInterval struct {
	Unit     string
	Interval ExprNode
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
	// AggFuncMax is the name of max function.
	AggFuncMax = "max"
	// AggFuncMin is the name of min function.
	AggFuncMin = "min"
	// AggFuncGroupConcat is the name of group_concat function.
	AggFuncGroupConcat = "group_concat"
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

	CurrentGroup []byte
	// contextPerGroupMap is used to store aggregate evaluation context.
	// Each entry for a group.
	contextPerGroupMap map[string](*AggEvaluateContext)
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

// Clear clears aggregate computing context.
func (n *AggregateFuncExpr) Clear() {
	n.CurrentGroup = []byte{}
	n.contextPerGroupMap = nil
}

// Update is used for update aggregate context.
func (n *AggregateFuncExpr) Update() error {
	name := strings.ToLower(n.F)
	switch name {
	case AggFuncCount:
		return n.updateCount()
	case AggFuncFirstRow:
		return n.updateFirstRow()
	case AggFuncGroupConcat:
		return n.updateGroupConcat()
	case AggFuncMax:
		return n.updateMaxMin(true)
	case AggFuncMin:
		return n.updateMaxMin(false)
	case AggFuncSum, AggFuncAvg:
		return n.updateSum()
	}
	return nil
}

// GetContext gets aggregate evaluation context for the current group.
// If it is nil, add a new context into contextPerGroupMap.
func (n *AggregateFuncExpr) GetContext() *AggEvaluateContext {
	if n.contextPerGroupMap == nil {
		n.contextPerGroupMap = make(map[string](*AggEvaluateContext))
	}
	if _, ok := n.contextPerGroupMap[string(n.CurrentGroup)]; !ok {
		c := &AggEvaluateContext{}
		if n.Distinct {
			c.DistinctChecker = distinct.CreateDistinctChecker()
		}
		n.contextPerGroupMap[string(n.CurrentGroup)] = c
	}
	return n.contextPerGroupMap[string(n.CurrentGroup)]
}

// SetContext sets the aggregate expr evaluation context.
func (n *AggregateFuncExpr) SetContext(ctx map[string](*AggEvaluateContext)) {
	n.contextPerGroupMap = ctx
}

func (n *AggregateFuncExpr) updateCount() error {
	ctx := n.GetContext()
	vals := make([]interface{}, 0, len(n.Args))
	for _, a := range n.Args {
		value := a.GetValue()
		if value == nil {
			return nil
		}
		vals = append(vals, value)
	}
	if n.Distinct {
		d, err := ctx.DistinctChecker.Check(vals)
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
	if !ctx.Value.IsNull() {
		return nil
	}
	if len(n.Args) != 1 {
		return errors.New("Wrong number of args for AggFuncFirstRow")
	}
	ctx.Value = *n.Args[0].GetDatum()
	return nil
}

func (n *AggregateFuncExpr) updateMaxMin(max bool) error {
	ctx := n.GetContext()
	if len(n.Args) != 1 {
		return errors.New("Wrong number of args for AggFuncFirstRow")
	}
	v := *n.Args[0].GetDatum()
	if ctx.Value.IsNull() {
		ctx.Value = v
		return nil
	}
	c, err := ctx.Value.CompareDatum(v)
	if err != nil {
		return errors.Trace(err)
	}
	if max {
		if c == -1 {
			ctx.Value = v
		}
	} else {
		if c == 1 {
			ctx.Value = v
		}

	}
	return nil
}

func (n *AggregateFuncExpr) updateSum() error {
	ctx := n.GetContext()
	value := *n.Args[0].GetDatum()
	if value.IsNull() {
		return nil
	}
	if n.Distinct {
		d, err := ctx.DistinctChecker.Check([]interface{}{value.GetValue()})
		if err != nil {
			return errors.Trace(err)
		}
		if !d {
			return nil
		}
	}
	var err error
	ctx.Value, err = types.CalculateSum(ctx.Value, value)
	if err != nil {
		return errors.Trace(err)
	}
	ctx.Count++
	return nil
}

func (n *AggregateFuncExpr) updateGroupConcat() error {
	ctx := n.GetContext()
	vals := make([]interface{}, 0, len(n.Args))
	for _, a := range n.Args {
		value := a.GetValue()
		if value == nil {
			return nil
		}
		vals = append(vals, value)
	}
	if n.Distinct {
		d, err := ctx.DistinctChecker.Check(vals)
		if err != nil {
			return errors.Trace(err)
		}
		if !d {
			return nil
		}
	}
	if ctx.Buffer == nil {
		ctx.Buffer = &bytes.Buffer{}
	} else {
		// now use comma separator
		ctx.Buffer.WriteString(",")
	}
	for _, val := range vals {
		ctx.Buffer.WriteString(fmt.Sprintf("%v", val))
	}
	// TODO: if total length is greater than global var group_concat_max_len, truncate it.
	return nil
}

// AggregateFuncExtractor visits Expr tree.
// It converts ColunmNameExpr to AggregateFuncExpr and collects AggregateFuncExpr.
type AggregateFuncExtractor struct {
	inAggregateFuncExpr bool
	// AggFuncs is the collected AggregateFuncExprs.
	AggFuncs   []*AggregateFuncExpr
	extracting bool
}

// Enter implements Visitor interface.
func (a *AggregateFuncExtractor) Enter(n Node) (node Node, skipChildren bool) {
	switch n.(type) {
	case *AggregateFuncExpr:
		a.inAggregateFuncExpr = true
	case *SelectStmt, *InsertStmt, *DeleteStmt, *UpdateStmt:
		// Enter a new context, skip it.
		// For example: select sum(c) + c + exists(select c from t) from t;
		if a.extracting {
			return n, true
		}
	}
	a.extracting = true
	return n, false
}

// Leave implements Visitor interface.
func (a *AggregateFuncExtractor) Leave(n Node) (node Node, ok bool) {
	switch v := n.(type) {
	case *AggregateFuncExpr:
		a.inAggregateFuncExpr = false
		a.AggFuncs = append(a.AggFuncs, v)
	case *ColumnNameExpr:
		// compose new AggregateFuncExpr
		if !a.inAggregateFuncExpr {
			// For example: select sum(c) + c from t;
			// The c in sum() should be evaluated for each row.
			// The c after plus should be evaluated only once.
			agg := &AggregateFuncExpr{
				F:    AggFuncFirstRow,
				Args: []ExprNode{v},
			}
			agg.SetFlag((v.GetFlag() | FlagHasAggregateFunc))
			agg.SetType(v.GetType())
			a.AggFuncs = append(a.AggFuncs, agg)
			return agg, true
		}
	}
	return n, true
}

// AggEvaluateContext is used to store intermediate result when calculating aggregate functions.
type AggEvaluateContext struct {
	DistinctChecker *distinct.Checker
	Count           int64
	Value           types.Datum
	Buffer          *bytes.Buffer // Buffer is used for group_concat.
}
