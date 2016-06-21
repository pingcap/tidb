package plan

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/evaluator"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
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
	b          *planBuilder
	correlated bool
}

func (er *expressionRewriter) buildSubquery(subq *ast.SubqueryExpr) (Plan, expression.Schema) {
	if len(er.b.outerSchemas) > 0 {
		er.err = errors.New("Nested subqueries is not currently supported.")
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
	case *ast.ExistsSubqueryExpr:
		subq, ok := v.Sel.(*ast.SubqueryExpr)
		if !ok {
			er.err = errors.Errorf("Unknown exists type %T.", v.Sel)
			return inNode, true
		}
		np, outerSchema := er.buildSubquery(subq)
		if er.err != nil {
			return retNode, true
		}
		np = er.b.buildExists(np)
		if np.IsCorrelated() {
			er.p = er.b.buildApply(er.p, np, outerSchema)
			er.ctxStack = append(er.ctxStack, er.p.GetSchema()[len(er.p.GetSchema())-1])
		} else {
			d, err := EvalSubquery(np, er.b.is, er.b.ctx)
			if err != nil {
				er.err = errors.Trace(err)
				return retNode, true
			}
			er.ctxStack = append(er.ctxStack, &expression.Constant{Value: d[0], RetType: np.GetSchema()[0].GetType()})
		}
		return inNode, true
	case *ast.SubqueryExpr:
		np, outerSchema := er.buildSubquery(v)
		if er.err != nil {
			return retNode, true
		}
		np = er.b.buildMaxOneRow(np)
		if np.IsCorrelated() {
			er.p = er.b.buildApply(er.p, np, outerSchema)
			er.ctxStack = append(er.ctxStack, er.p.GetSchema()[len(er.p.GetSchema())-1])
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
		f, ok := evaluator.Funcs[v.FnName.L]
		if !ok {
			er.err = errors.New("Can't find function!")
			return retNode, false
		}
		if len(function.Args) < f.MinArgs || (f.MaxArgs != -1 && len(function.Args) > f.MaxArgs) {
			er.err = evaluator.ErrInvalidOperation.Gen("number of function arguments must in [%d, %d].",
				f.MinArgs, f.MaxArgs)
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
		if column != nil {
			er.ctxStack = append(er.ctxStack, column)
			break
		}

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
		function, err := expression.NewFunction(ast.IsNull, []expression.Expression{er.ctxStack[length-1]}, v.Type)
		if err != nil {
			er.err = errors.Trace(err)
			return retNode, false
		}
		er.ctxStack = er.ctxStack[:length-1]
		er.ctxStack = append(er.ctxStack, function)
	case *ast.BinaryOperationExpr:
		funcName, ok := opcode.Ops[v.Op]
		if !ok {
			er.err = errors.Errorf("Unknown opcode %v", v.Op)
			return retNode, false
		}
		function, err := expression.NewFunction(funcName,
			[]expression.Expression{er.ctxStack[length-2], er.ctxStack[length-1]}, v.Type)
		if err != nil {
			er.err = errors.Trace(err)
			return retNode, false
		}
		er.ctxStack = er.ctxStack[:length-2]
		er.ctxStack = append(er.ctxStack, function)
	case *ast.UnaryOperationExpr:
		funcName, ok := opcode.Ops[v.Op]
		if !ok {
			er.err = errors.Errorf("Unknown opcode %v", v.Op)
			return retNode, false
		}
		function, err := expression.NewFunction(funcName, []expression.Expression{er.ctxStack[length-1]}, v.Type)
		if err != nil {
			er.err = errors.Trace(err)
			return retNode, false
		}
		er.ctxStack = er.ctxStack[:length-1]
		er.ctxStack = append(er.ctxStack, function)

	default:
		er.err = errors.Errorf("UnknownType: %T", v)
		return retNode, false
	}
	return inNode, true
}
