package plan

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/evaluator"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/parser/opcode"
)

func (b *planBuilder) rewrite(expr ast.ExprNode, schema expression.Schema, AggrMapper map[*ast.AggregateFuncExpr]int) (newExpr expression.Expression, err error) {
	er := &expressionRewriter{aggrMap: AggrMapper, schema: schema}
	expr.Accept(er)
	if er.err != nil {
		return nil, errors.Trace(er.err)
	}
	if len(er.ctxStack) != 1 {
		return nil, errors.Errorf("context len %v is invalid", len(er.ctxStack))
	}
	return er.ctxStack[0], nil
}

type expressionRewriter struct {
	ctxStack []expression.Expression
	p        Plan
	schema   expression.Schema
	err      error
	aggrMap  map[*ast.AggregateFuncExpr]int
	isSubq   bool
	b        *planBuilder
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
		function := &expression.ScalarFunction{FuncName: v.FnName}
		for i := length - len(v.Args); i < length; i++ {
			function.Args = append(function.Args, er.ctxStack[i])
		}
		f := evaluator.Funcs[v.FnName.L]
		if len(function.Args) < f.MinArgs || (f.MaxArgs != -1 && len(function.Args) > f.MaxArgs) {
			er.err = evaluator.ErrInvalidOperation.Gen("number of function arguments must in [%d, %d].", f.MinArgs, f.MaxArgs)
			return retNode, false
		}
		function.Function = f.F
		function.RetType = v.Type
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
		value := &expression.Constant{Value: v.Datum, RetType: v.Type}
		er.ctxStack = append(er.ctxStack, value)
	case *ast.IsNullExpr:
		function := &expression.ScalarFunction{
			Args:     []expression.Expression{er.ctxStack[length-1]},
			FuncName: model.NewCIStr("isnull"),
			RetType:  v.Type,
		}
		f, ok := evaluator.Funcs[function.FuncName.L]
		if !ok {
			er.err = errors.New("Can't find function!")
			return retNode, false
		}
		function.Function = f.F
		er.ctxStack = er.ctxStack[:length-1]
		er.ctxStack = append(er.ctxStack, function)
	case *ast.BinaryOperationExpr:
		function := &expression.ScalarFunction{Args: []expression.Expression{er.ctxStack[length-2], er.ctxStack[length-1]}, RetType: v.Type}
		funcName, ok := opcode.Ops[v.Op]
		if !ok {
			er.err = errors.Errorf("Unknown opcode %v", v.Op)
			return retNode, false
		}
		function.FuncName = model.NewCIStr(funcName)
		f, ok := evaluator.Funcs[function.FuncName.L]
		if !ok {
			er.err = errors.New("Can't find function!")
			return retNode, false
		}
		function.Function = f.F
		er.ctxStack = er.ctxStack[:length-2]
		er.ctxStack = append(er.ctxStack, function)
	case *ast.UnaryOperationExpr:
		function := &expression.ScalarFunction{Args: []expression.Expression{er.ctxStack[length-1]}, RetType: v.Type}
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
		f, ok := evaluator.Funcs[function.FuncName.L]
		if !ok {
			er.err = errors.New("Can't find function!")
			return retNode, false
		}
		function.Function = f.F
		er.ctxStack = er.ctxStack[:length-1]
		er.ctxStack = append(er.ctxStack, function)

	default:
		er.err = errors.Errorf("UnkownType: %T", v)
	}
	return inNode, true
}
