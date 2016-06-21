package plan

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/evaluator"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/util/types"
)

// EvalSubquery evaluates incorrelated subqueries once.
var EvalSubquery func(p Plan, is infoschema.InfoSchema, ctx context.Context) ([]types.Datum, error)

func (b *planBuilder) rewrite(expr ast.ExprNode, p Plan, aggMapper map[*ast.AggregateFuncExpr]int) (
	newExpr expression.Expression, newPlan Plan, correlated bool, err error) {
	er := &expressionRewriter{p: p, aggrMap: aggMapper, schema: p.GetSchema(), b: b}
	expr.Accept(er)
	if er.err != nil {
		return nil, nil, false, errors.Trace(er.err)
	}
	if len(er.ctxStack) != 1 {
		return nil, nil, false, errors.Errorf("context len %v is invalid", len(er.ctxStack))
	}
	return er.ctxStack[0], er.p, er.correlated, nil
}

type expressionRewriter struct {
	ctxStack   []expression.Expression
	p          Plan
	schema     expression.Schema
	err        error
	aggrMap    map[*ast.AggregateFuncExpr]int
	columnMap  map[*ast.ColumnNameExpr]expression.Expression
	b          *planBuilder
	correlated bool
}

func (er *expressionRewriter) buildSubquery(subq *ast.SubqueryExpr) (Plan, expression.Schema) {
	if len(er.b.outerSchemas) > 0 {
		er.err = errors.New("Nested subqueries is not supported.")
		return nil, nil
	}
	outerSchema := er.schema.DeepCopy()
	for _, col := range outerSchema {
		col.Correlated = true
	}
	er.b.outerSchemas = append(er.b.outerSchemas, outerSchema)
	np := er.b.buildResultSetNode(subq.Query)
	er.b.outerSchemas = er.b.outerSchemas[0 : len(er.b.outerSchemas)-1]
	if er.b.err != nil {
		er.err = errors.Trace(er.b.err)
		return nil, nil
	}
	er.b.err = Refine(np)
	return np, outerSchema
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
	case *ast.ColumnNameExpr:
		if col, ok := er.b.colMapper[v]; ok {
			er.ctxStack = append(er.ctxStack, col)
			return inNode, true
		}
	case *ast.ExistsSubqueryExpr:
		if subq, ok := v.Sel.(*ast.SubqueryExpr); ok {
			np, outerSchema := er.buildSubquery(subq)
			if er.err != nil {
				return retNode, true
			}
			np = er.b.buildExists(np)
			if np.IsCorrelated() {
				ap := er.b.buildApply(er.p, np, outerSchema)
				er.p = ap
				er.ctxStack = append(er.ctxStack, ap.GetSchema()[len(ap.GetSchema())-1])
			} else {
				d, err := EvalSubquery(np, er.b.is, er.b.ctx)
				if err != nil {
					er.err = errors.Trace(err)
					return retNode, true
				}
				er.ctxStack = append(er.ctxStack, &expression.Constant{Value: d[0], RetType: np.GetSchema()[0].GetType()})
			}
			return inNode, true
		}
		er.err = errors.Errorf("Unknown exists type %T.", v.Sel)
		return inNode, true
	case *ast.SubqueryExpr:
		np, outerSchema := er.buildSubquery(v)
		if er.err != nil {
			return retNode, true
		}
		np = er.b.buildMaxOneRow(np)
		if np.IsCorrelated() {
			ap := er.b.buildApply(er.p, np, outerSchema)
			er.p = ap
			er.ctxStack = append(er.ctxStack, ap.GetSchema()[len(ap.GetSchema())-1])
		} else {
			d, err := EvalSubquery(np, er.b.is, er.b.ctx)
			if err != nil {
				er.err = errors.Trace(err)
				return retNode, true
			}
			er.ctxStack = append(er.ctxStack, &expression.Constant{Value: d[0], RetType: np.GetSchema()[0].GetType()})
		}
		return inNode, true
	}
	return inNode, false
}

// Leave implements Visitor interface.
func (er *expressionRewriter) Leave(inNode ast.Node) (retNode ast.Node, ok bool) {
	length := len(er.ctxStack)
	if er.err != nil {
		return retNode, false
	}
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
	case *ast.PositionExpr:
		if v.N > 0 && v.N <= len(er.schema) {
			er.ctxStack = append(er.ctxStack, er.schema[v.N-1])
		} else {
			er.err = errors.Errorf("Position %d is out of range", v.N)
		}
	case *ast.ColumnName:
		column, err := er.schema.FindColumn(v)
		if err != nil {
			er.err = errors.Trace(err)
			return retNode, false
		}
		if column == nil {
			for i := len(er.b.outerSchemas) - 1; i >= 0; i-- {
				outer := er.b.outerSchemas[i]
				column, err = outer.FindColumn(v)
				if err != nil {
					er.err = errors.Trace(err)
					return retNode, false
				}
				if column != nil {
					er.correlated = true
					break
				}
			}
		}
		if column == nil {
			er.err = errors.Errorf("Unknown column %s %s %s.", v.Schema.L, v.Table.L, v.Name.L)
			return retNode, false
		}
		er.ctxStack = append(er.ctxStack, column)
	case *ast.ColumnNameExpr, *ast.ParenthesesExpr, *ast.WhenClause, *ast.SubqueryExpr, *ast.ExistsSubqueryExpr:
	case *ast.ValueExpr:
		value := &expression.Constant{Value: v.Datum, RetType: v.Type}
		er.ctxStack = append(er.ctxStack, value)
	case *ast.IsNullExpr:
		function := &expression.ScalarFunction{
			Args:     []expression.Expression{er.ctxStack[length-1]},
			FuncName: model.NewCIStr(ast.IsNull),
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
			function.FuncName = model.NewCIStr(ast.UnaryNot)
		case opcode.BitNeg:
			function.FuncName = model.NewCIStr(ast.BitNeg)
		case opcode.Plus:
			function.FuncName = model.NewCIStr(ast.UnaryPlus)
		case opcode.Minus:
			function.FuncName = model.NewCIStr(ast.UnaryMinus)
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
		er.err = errors.Errorf("UnknownType: %T", v)
	}
	return inNode, true
}
