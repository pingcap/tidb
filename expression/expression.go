// Copyright 2016 PingCAP, Inc.
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
	"github.com/coreos/etcd/cmd/vendor/golang.org/x/net/context"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/evaluator"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/util/types"
)

type aggrMapper map[*ast.AggregateFuncExpr]int

func ExpressionRewrite(expr ast.ExprNode, schema Schema, AggrMapper aggrMapper) (newExpr Expression, err error) {
	er := &expressionRewriter{curTop: 0, schema: schema, aggrMap: AggrMapper}
	expr.Accept(er)
	if len(er.ctxStack) != 1 {
		err = errors.New(fmt.Sprintf("context len %v is invalid", len(er.ctxStack)))
	}
	return er.ctxStack[0], nil
}

type expressionRewriter struct {
	ctxStack []Expression
	schema   Schema
	curTop   int
	err      error
	aggrMap  aggrMapper
}

type Expression interface {
	Eval(row []types.Datum) (types.Datum, error)
	GetType() types.FieldType
	DeepCopy() Expression
}

type Column struct {
	FromID  int
	ColName model.CIStr
	DbName  model.CIStr
	TblName model.CIStr
	RetType types.FieldType

	// only used during execution
	Index int
}

// GetType implements Expression interface.
func (col *Column) GetType() types.FieldType {
	return col.RetType
}

func (col *Column) Eval(row []types.Datum) (types.Datum, error) {
	if col.Index >= 0 && col.Index <= len(row) {
		return errors.New("Index out of range!")
	}
	return row[col.Index], nil
}

func (col *Column) DeepCopy() Expression {
	return &Column{FromID: col.FromID, ColName: col.ColName, DbName: col.DbName, TblName: col.TblName, RetType: col.RetType, Index: col.Index}
}

type Schema []*Column

func (s *Schema) FindColumn(astCol ast.ColumnName) (*Column, error) {
	dbName, tblName, colName := astCol.Schema, astCol.Table, astCol.Name
	idx := -1
	for i, col := range s {
		if (dbName.L == "" || dbName.L == col.DbName.L) && (tblName == "" || tblName == col.TblName.L) && (colName == col.ColName.L) {
			if idx != i {
				return nil, errors.New(fmt.Sprintf("Column '%s' is ambiguous", astCol.Text()))
			}
		}
	}
	if idx == -1 {
		return nil, errors.New(fmt.Sprintf("Unknown column %d.", astCol.Text()))
	}
	return s[idx], nil
}

func (s *Schema) GetIndex(col *Column) int {
	for i, c := range s {
		if c.FromID == col.FromID && c.ColName == col.ColName {
			return i
		}
	}
	return -1
}

type ScalarFunction struct {
	Args     []Expression
	FuncName model.CIStr
	retType  types.FieldType
	function evaluator.BuiltinFunc
	ctx      context.Context
}

func NewFunction(funcName string, args []Expression) *ScalarFunction {
	return &ScalarFunction{Args: args, FuncName: funcName, function: evaluator.Funcs[funcName].F}
}

func (sf *ScalarFunction) DeepCopy() Expression {
	newFunc := &ScalarFunction{FuncName: sf.FuncName, function: sf.function, ctx: sf.ctx, retType: sf.retType}
	for _, arg := range sf.Args {
		newFunc.Args = append(newFunc.Args, arg.DeepCopy())
	}
	return newFunc
}

// GetType implements Expression interface.
func (sf *ScalarFunction) GetType() types.FieldType {
	return sf.retType
}

// GetType implements Expression interface.
func (sf *ScalarFunction) Eval(row []types.Datum) (types.Datum, error) {
	args := make([]types.Datum, len(sf.Args))
	for _, arg := range sf.Args {
		result, err := arg.Eval(row)
		if err != nil {
			args = append(args, result)
		} else {
			return nil, err
		}
	}
	return sf.function(args, sf.ctx)
}

type Constant struct {
	value   types.Datum
	retType types.FieldType
}

func (c *Constant) DeepCopy() Expression {
	return &Constant{value: c.value, retType: c.retType}
}

func (c *Constant) GetType() types.FieldType {
	return c.retType
}

func (c *Constant) Eval(_ []types.Datum) (types.Datum, error) {
	return c.value, nil
}

func (er *expressionRewriter) Enter(inNode ast.Node) (retNode ast.Node, skipChildren bool) {
	er.curTop++
	switch v := inNode.(type) {
	case *ast.AggregateFuncExpr:
		index, ok := -1, false
		if er.aggrMap != nil {
			index, ok = er.aggrMap[v]
		}
		if !ok {
			er.err = errors.New("Can't appear aggrFunctions")
			return inNode, true
		}
		er.ctxStack = append(er.ctxStack, er.schema[index])
		return inNode, true
	}
	return inNode, false
}

func (er *expressionRewriter) Leave(inNode ast.Node) (retNode ast.Node, ok bool) {
	er.curTop--
	switch v := inNode.(type) {
	case *ast.AggregateFuncExpr:
	case *ast.FuncCallExpr:
		function := &ScalarFunction{FuncName: v.FnName}
		for i := er.curTop; i < len(er.ctxStack); i++ {
			function.Args = append(function.Args, er.ctxStack[i])
		}
		f := evaluator.Funcs[v.FnName.L]
		if len(function.Args) < f.MinArgs || (f.MaxArgs != -1 && len(function.Args) > f.MaxArgs) {
			er.err = evaluator.ErrInvalidOperation.Gen("number of function arguments must in [%d, %d].", f.MinArgs, f.MaxArgs)
			return retNode, false
		}
		function.function = f.F
		er.ctxStack = er.ctxStack[:er.curTop]
		er.ctxStack = append(er.ctxStack, function)
	case *ast.ColumnName:
		column, err := er.schema.FindColumn(v)
		if err != nil {
			er.err = errors.Trace(err)
			return retNode, false
		}
		er.ctxStack = append(er.ctxStack, column)
	case *ast.ColumnNameExpr, *ast.ParenthesesExpr, *ast.WhenClause:
	case *ast.ValueExpr:
		value := &Constant{value: v.Datum}
		er.ctxStack = append(er.ctxStack, value)
	case *ast.FuncCastExpr:
		function := &ScalarFunction{FuncName: "cast", Args: []types.Datum{er.ctxStack[er.curTop]}}
		er.ctxStack = er.ctxStack[:er.curTop]
		function.function = evaluator.CastFactory(v.Tp)
		er.ctxStack = append(er.ctxStack, function)
	case *ast.IsNullExpr:
		function := ScalarFunction{Args: []Expression{er.ctxStack[er.curTop]}, FuncName: "isnull"}
		f, ok := evaluator.Funcs[function.FuncName]
		if !ok {
			er.err = errors.New("Can't find function!")
			return retNode, false
		}
		function.function = f.F
		er.ctxStack = er.ctxStack[:er.curTop]
		er.ctxStack = append(er.ctxStack, function)
	case *ast.IsTruthExpr:
		function := ScalarFunction{Args: []Expression{er.ctxStack[er.curTop]}, FuncName: "istruth"}
		f := evaluator.IsTruthFactory(v.Not, v.True)
		function.function = f
		er.ctxStack = er.ctxStack[:er.curTop]
		er.ctxStack = append(er.ctxStack, function)
	case *ast.BinaryOperationExpr:
		function := ScalarFunction{Args: []Expression{er.ctxStack[er.curTop], er.ctxStack[er.curTop+1]}}
		switch v.Op {
		case opcode.EQ:
			function.FuncName = model.NewCIStr("EQ")
		case opcode.AndAnd:
			function.FuncName = model.NewCIStr("AndAnd")
		case opcode.OrOr:
			function.FuncName = model.NewCIStr("OrOr")
		case opcode.LogicXor:
			function.FuncName = model.NewCIStr("logicxor")
		case opcode.LT:
			function.FuncName = model.NewCIStr("lt")
		case opcode.LE:
			function.FuncName = model.NewCIStr("le")
		case opcode.GT:
			function.FuncName = model.NewCIStr("gt")
		case opcode.GE:
			function.FuncName = model.NewCIStr("ge")
		case opcode.NullEQ:
			function.FuncName = model.NewCIStr("nulleq")
		case opcode.Plus:
			function.FuncName = model.NewCIStr("plus")
		case opcode.Minus:
			function.FuncName = model.NewCIStr("minus")
		case opcode.Mul:
			function.FuncName = model.NewCIStr("mul")
		case opcode.Div:
			function.FuncName = model.NewCIStr("div")
		case opcode.IntDiv:
			function.FuncName = model.NewCIStr("intdiv")
		}
		f, ok := evaluator.Funcs[function.FuncName]
		if !ok {
			er.err = errors.New("Can't find function!")
			return retNode, false
		}
		function.function = f.F
		function.retType = types.KindInt64
		er.ctxStack = er.ctxStack[:er.curTop]
		er.ctxStack = append(er.ctxStack, function)
	case *ast.UnaryOperationExpr:
		function := ScalarFunction{Args: []Expression{er.ctxStack[er.curTop]}}
		switch v.Op {
		case opcode.Not:
			function.FuncName = model.NewCIStr("not")
		case opcode.BitNeg:
			function.FuncName = model.NewCIStr("bitneg")
		case opcode.Plus:
			function.FuncName = model.NewCIStr("unaryplus")
		case opcode.Minus:
			function.FuncName = model.NewCIStr("unaryminus")
		}
		er.ctxStack = er.ctxStack[:er.curTop]
		er.ctxStack = append(er.ctxStack, function)
	case *ast.PatternRegexpExpr:
		function := ScalarFunction{Args: []Expression{er.ctxStack[er.curTop], er.ctxStack[er.curTop+1]}}
		function.FuncName = "rlike"
		function.function = evaluator.RLikeFactory(v)
		function.retType = types.KindString
		er.ctxStack = er.ctxStack[:er.curTop]
		er.ctxStack = append(er.ctxStack, function)
	case *ast.PatternLikeExpr:
		function := ScalarFunction{Args: []Expression{er.ctxStack[er.curTop], er.ctxStack[er.curTop+1]}}
		function.FuncName = "like"
		function.function = evaluator.LikeFactory(v)
		function.retType = types.KindString
		er.ctxStack = er.ctxStack[:er.curTop]
		er.ctxStack = append(er.ctxStack, function)
	case *ast.CaseExpr:
		function := ScalarFunction{Args: []Expression{er.ctxStack[er.curTop], er.ctxStack[er.curTop+1]}}
		if v.Value == nil {
			function.FuncName = "when"
		} else {
			function.FuncName = "case"
		}
		f, ok := evaluator.Funcs[function.FuncName]
		if !ok {
			er.err = errors.New("Can't find function!")
			return retNode, false
		}
		function.function = f.F
		er.ctxStack = er.ctxStack[:er.curTop]
		er.ctxStack = append(er.ctxStack, function)
	default:
		er.err = errors.New(fmt.Sprintf("UnkownType: %T", v))
	}
	return inNode, true
}
