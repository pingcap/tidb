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
	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/evaluator"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/util/types"
)

// Rewrite rewrites ast to Expression.
func Rewrite(expr ast.ExprNode, schema Schema, AggrMapper map[*ast.AggregateFuncExpr]int) (newExpr Expression, err error) {
	er := &expressionRewriter{schema: schema, aggrMap: AggrMapper}
	expr.Accept(er)
	if er.err != nil {
		return nil, errors.Trace(er.err)
	}
	if len(er.ctxStack) != 1 {
		return nil, fmt.Errorf("context len %v is invalid", len(er.ctxStack))
	}
	return er.ctxStack[0], nil
}

type expressionRewriter struct {
	ctxStack []Expression
	schema   Schema
	err      error
	aggrMap  map[*ast.AggregateFuncExpr]int
}

// Expression represents all scalar expression in SQL.
type Expression interface {
	// Eval evaluates an expression through a row.
	Eval(row []types.Datum, ctx context.Context) (types.Datum, error)

	// Get the expression return type.
	GetType() *types.FieldType

	// DeepCopy copies an expression totally.
	DeepCopy() Expression

	// ToString converts an expression into a string.
	ToString() string
}

// EvalBool evaluates expression to a boolean value.
func EvalBool(expr Expression, row []types.Datum, ctx context.Context) (bool, error) {
	data, err := expr.Eval(row, ctx)
	if err != nil {
		return false, errors.Trace(err)
	}
	if data.Kind() == types.KindNull {
		return false, nil
	}

	i, err := data.ToBool()
	if err != nil {
		return false, errors.Trace(err)
	}
	return i != 0, nil
}

// Column represents a column.
type Column struct {
	FromID  string
	ColName model.CIStr
	DbName  model.CIStr
	TblName model.CIStr
	RetType *types.FieldType

	// only used during execution
	Index int
}

// ToString implements Expression interface.
func (col *Column) ToString() string {
	result := col.ColName.L
	if col.TblName.L != "" {
		result = col.TblName.L + "." + result
	}
	if col.DbName.L != "" {
		result = col.DbName.L + "." + result
	}
	return result
}

// GetType implements Expression interface.
func (col *Column) GetType() *types.FieldType {
	return col.RetType
}

// Eval implements Expression interface.
func (col *Column) Eval(row []types.Datum, _ context.Context) (d types.Datum, err error) {
	return row[col.Index], nil
}

// DeepCopy implements Expression interface.
func (col *Column) DeepCopy() Expression {
	newCol := *col
	return &newCol
}

// Schema stands for the row schema get from input.
type Schema []*Column

// FindColumn replaces an ast column to an expression column.
func (s Schema) FindColumn(astCol *ast.ColumnName) (*Column, error) {
	dbName, tblName, colName := astCol.Schema, astCol.Table, astCol.Name
	idx := -1
	for i, col := range s {
		if (dbName.L == "" || dbName.L == col.DbName.L) && (tblName.L == "" || tblName.L == col.TblName.L) && (colName.L == col.ColName.L) {
			if idx != -1 {
				return nil, fmt.Errorf("Column '%s' is ambiguous", colName.L)
			}
			idx = i
		}
	}
	if idx == -1 {
		return nil, fmt.Errorf("Unknown column %s %s %s.", dbName.L, tblName.L, colName.L)
	}
	return s[idx], nil
}

// GetIndex finds the index for a column
func (s Schema) GetIndex(col *Column) int {
	for i, c := range s {
		if c.FromID == col.FromID && c.ColName.L == col.ColName.L {
			return i
		}
	}
	return -1
}

// ScalarFunction is the function that returns a value.
type ScalarFunction struct {
	Args     []Expression
	FuncName model.CIStr
	// TODO: Implement type inference here, now we use ast's return type temporarily.
	retType  *types.FieldType
	function evaluator.BuiltinFunc
}

// ToString implements Expression interface.
func (sf *ScalarFunction) ToString() string {
	result := sf.FuncName.L + "("
	for _, arg := range sf.Args {
		result += arg.ToString()
		result += ","
	}
	result += ")"
	return result
}

// NewFunction creates a new scalar function.
func NewFunction(funcName model.CIStr, args []Expression) *ScalarFunction {
	return &ScalarFunction{Args: args, FuncName: funcName, function: evaluator.Funcs[funcName.L].F}
}

//Schema2Exprs converts []*Column to []Expression.
func Schema2Exprs(schema Schema) []Expression {
	result := make([]Expression, 0, len(schema))
	for _, col := range schema {
		result = append(result, col)
	}
	return result
}

//ScalarFuncs2Exprs converts []*ScalarFunction to []Expression.
func ScalarFuncs2Exprs(funcs []*ScalarFunction) []Expression {
	result := make([]Expression, 0, len(funcs))
	for _, col := range funcs {
		result = append(result, col)
	}
	return result
}

// DeepCopy implements Expression interface.
func (sf *ScalarFunction) DeepCopy() Expression {
	newFunc := &ScalarFunction{FuncName: sf.FuncName, function: sf.function, retType: sf.retType}
	for _, arg := range sf.Args {
		newFunc.Args = append(newFunc.Args, arg.DeepCopy())
	}
	return newFunc
}

// GetType implements Expression interface.
func (sf *ScalarFunction) GetType() *types.FieldType {
	return sf.retType
}

// Eval implements Expression interface.
func (sf *ScalarFunction) Eval(row []types.Datum, ctx context.Context) (types.Datum, error) {
	args := make([]types.Datum, len(sf.Args))
	for _, arg := range sf.Args {
		result, err := arg.Eval(row, ctx)
		if err != nil {
			args = append(args, result)
		} else {
			return types.Datum{}, err
		}
	}
	return sf.function(args, ctx)
}

// Constant stands for a constant value.
type Constant struct {
	value   types.Datum
	retType *types.FieldType
}

// ToString implements Expression interface.
func (c *Constant) ToString() string {
	return fmt.Sprintf("%v", c.value.GetValue())
}

// DeepCopy implements Expression interface.
func (c *Constant) DeepCopy() Expression {
	con := *c
	return &con
}

// GetType implements Expression interface.
func (c *Constant) GetType() *types.FieldType {
	return c.retType
}

// Eval implements Expression interface.
func (c *Constant) Eval(_ []types.Datum, _ context.Context) (types.Datum, error) {
	return c.value, nil
}

// Enter implements Visitor interface.
func (er *expressionRewriter) Enter(inNode ast.Node) (retNode ast.Node, skipChildren bool) {
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

// Leave implements Visitor interface.
func (er *expressionRewriter) Leave(inNode ast.Node) (retNode ast.Node, ok bool) {
	length := len(er.ctxStack)
	switch v := inNode.(type) {
	case *ast.AggregateFuncExpr:
	case *ast.FuncCallExpr:
		function := &ScalarFunction{FuncName: v.FnName}
		for i := length - len(v.Args); i < length; i++ {
			function.Args = append(function.Args, er.ctxStack[i])
		}
		f := evaluator.Funcs[v.FnName.L]
		if len(function.Args) < f.MinArgs || (f.MaxArgs != -1 && len(function.Args) > f.MaxArgs) {
			er.err = evaluator.ErrInvalidOperation.Gen("number of function arguments must in [%d, %d].", f.MinArgs, f.MaxArgs)
			return retNode, false
		}
		function.function = f.F
		function.retType = v.Type
		er.ctxStack = er.ctxStack[:length-len(v.Args)]
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
		value := &Constant{value: v.Datum, retType: v.Type}
		er.ctxStack = append(er.ctxStack, value)
	case *ast.IsNullExpr:
		function := &ScalarFunction{
			Args:     []Expression{er.ctxStack[length-1]},
			FuncName: model.NewCIStr("isnull"),
			retType:  v.Type,
		}
		f, ok := evaluator.Funcs[function.FuncName.L]
		if !ok {
			er.err = errors.New("Can't find function!")
			return retNode, false
		}
		function.function = f.F
		er.ctxStack = er.ctxStack[:length-1]
		er.ctxStack = append(er.ctxStack, function)
	case *ast.BinaryOperationExpr:
		function := &ScalarFunction{Args: []Expression{er.ctxStack[length-2], er.ctxStack[length-1]}, retType: v.Type}
		funcName, ok := opcode.Ops[v.Op]
		if !ok {
			er.err = fmt.Errorf("Unknown opcode %v", v.Op)
			return retNode, false
		}
		function.FuncName = model.NewCIStr(funcName)
		f, ok := evaluator.Funcs[function.FuncName.L]
		if !ok {
			er.err = errors.New("Can't find function!")
			return retNode, false
		}
		function.function = f.F
		er.ctxStack = er.ctxStack[:length-2]
		er.ctxStack = append(er.ctxStack, function)
	case *ast.UnaryOperationExpr:
		function := &ScalarFunction{Args: []Expression{er.ctxStack[length-1]}, retType: v.Type}
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
		er.ctxStack = er.ctxStack[:length-1]
		er.ctxStack = append(er.ctxStack, function)
	default:
		er.err = fmt.Errorf("UnkownType: %T", v)
	}
	return inNode, true
}
