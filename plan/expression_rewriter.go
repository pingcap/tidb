package plan

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/evaluator"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/mysql"
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
	if checkRowLen(er.ctxStack[0]) != 1 {
		return nil, nil, false, errors.New("Operand should contain 1 column(s)")
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

func checkRowLen(e expression.Expression) int {
	if f, ok := e.(*expression.ScalarFunction); ok && f.FuncName.L == ast.RowFunc {
		return len(f.Args)
	}
	if f, ok := e.(*expression.Constant); ok && f.RetType.Tp == types.KindRow {
		return len(f.Value.GetRow())
	}
	return 1
}

func getRowArg(e expression.Expression, idx int) expression.Expression {
	if f, ok := e.(*expression.ScalarFunction); ok {
		return f.Args[idx]
	}
	c, _ := e.(*expression.Constant)
	d := c.Value.GetRow()[idx]
	return &expression.Constant{Value: d, RetType: types.NewFieldType(d.Kind())}
}

func constructBinaryOpFunction(l expression.Expression, r expression.Expression, op string) (*expression.ScalarFunction, error) {
	lLen, rLen := checkRowLen(l), checkRowLen(r)
	if lLen == 1 && rLen == 1 {
		return expression.NewFunction(op, types.NewFieldType(mysql.TypeTiny), l, r), nil
	} else if rLen != lLen {
		return nil, errors.Errorf("Operand should contain %d column(s)", lLen)
	}
	funcs := make([]expression.Expression, lLen)
	for i := 0; i < lLen; i++ {
		var err error
		funcs[i], err = constructBinaryOpFunction(getRowArg(l, i), getRowArg(r, i), op)
		if err != nil {
			return nil, err
		}
	}
	return expression.ComposeCondition(funcs, ast.AndAnd).(*expression.ScalarFunction), nil
}

func (er *expressionRewriter) buildSubquery(subq *ast.SubqueryExpr) (Plan, expression.Schema) {
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
	_, err := er.b.predicatePushDown(np, nil)
	if err != nil {
		er.err = errors.Trace(err)
		return np, outerSchema
	}
	er.err = Refine(np)
	return np, outerSchema
}

// Enter implements Visitor interface.
func (er *expressionRewriter) Enter(inNode ast.Node) (ast.Node, bool) {
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
		subq, ok := v.Sel.(*ast.SubqueryExpr)
		if !ok {
			er.err = errors.Errorf("Unknown exists type %T.", v.Sel)
			return inNode, true
		}
		np, outerSchema := er.buildSubquery(subq)
		if er.err != nil {
			return inNode, true
		}
		np = er.b.buildExists(np)
		if np.IsCorrelated() {
			er.p = er.b.buildApply(er.p, np, outerSchema)
			er.ctxStack = append(er.ctxStack, er.p.GetSchema()[len(er.p.GetSchema())-1])
		} else {
			_, err := pruneColumnsAndResolveIndices(np, np.GetSchema())
			if err != nil {
				er.err = errors.Trace(err)
				return inNode, true
			}
			d, err := EvalSubquery(np, er.b.is, er.b.ctx)
			if err != nil {
				er.err = errors.Trace(err)
				return inNode, true
			}
			er.ctxStack = append(er.ctxStack, &expression.Constant{
				Value:   d[0],
				RetType: np.GetSchema()[0].GetType()})
		}
		return inNode, true
	case *ast.SubqueryExpr:
		np, outerSchema := er.buildSubquery(v)
		if er.err != nil {
			return inNode, true
		}
		np = er.b.buildMaxOneRow(np)
		if np.IsCorrelated() {
			er.p = er.b.buildApply(er.p, np, outerSchema)
			newCols := make([]expression.Expression, 0, len(np.GetSchema()))
			for _, col := range np.GetSchema() {
				newCols = append(newCols, col.DeepCopy())
			}
			if len(newCols) > 1 {
				er.ctxStack = append(er.ctxStack, expression.NewFunction(ast.RowFunc,
					types.NewFieldType(types.KindRow),
					newCols...))
			} else {
				er.ctxStack = append(er.ctxStack, newCols[0])
			}
		} else {
			_, err := pruneColumnsAndResolveIndices(np, np.GetSchema())
			if err != nil {
				er.err = errors.Trace(err)
				return inNode, true
			}
			d, err := EvalSubquery(np, er.b.is, er.b.ctx)
			if err != nil {
				er.err = errors.Trace(err)
				return inNode, true
			}
			newCols := make([]expression.Expression, 0, len(np.GetSchema()))
			for i, data := range d {
				newCols = append(newCols, &expression.Constant{
					Value:   data,
					RetType: np.GetSchema()[i].GetType()})
			}
			if len(newCols) > 1 {
				er.ctxStack = append(er.ctxStack, expression.NewFunction(ast.RowFunc, types.NewFieldType(types.KindRow), newCols...))
			} else {
				er.ctxStack = append(er.ctxStack, newCols[0])
			}
		}
		return inNode, true
	}
	return inNode, false
}

// Leave implements Visitor interface.
func (er *expressionRewriter) Leave(inNode ast.Node) (retNode ast.Node, ok bool) {
	stkLen := len(er.ctxStack)
	if er.err != nil {
		return retNode, false
	}
	switch v := inNode.(type) {
	case *ast.AggregateFuncExpr:
	case *ast.RowExpr:
		length := len(v.Values)
		rows := make([]expression.Expression, 0, length)
		for i := stkLen - length; i < stkLen; i++ {
			rows = append(rows, er.ctxStack[i])
		}
		er.ctxStack = er.ctxStack[:stkLen-length]
		er.ctxStack = append(er.ctxStack, expression.NewFunction(ast.RowFunc, types.NewFieldType(types.KindRow), rows...))

	case *ast.FuncCallExpr:
		function := &expression.ScalarFunction{FuncName: v.FnName}
		for i := stkLen - len(v.Args); i < stkLen; i++ {
			if checkRowLen(er.ctxStack[i]) != 1 {
				er.err = errors.New("Operand should contain 1 column(s)")
				return retNode, false
			}
			function.Args = append(function.Args, er.ctxStack[i])
		}
		f, ok := evaluator.Funcs[v.FnName.L]
		if !ok {
			er.err = errors.Errorf("Can't find function %s!", v.FnName.L)
			return retNode, false
		}
		if len(function.Args) < f.MinArgs || (f.MaxArgs != -1 && len(function.Args) > f.MaxArgs) {
			er.err = evaluator.ErrInvalidOperation.Gen("number of function arguments must in [%d, %d].",
				f.MinArgs, f.MaxArgs)
			return retNode, false
		}
		function.Function = f.F
		function.RetType = v.Type
		er.ctxStack = er.ctxStack[:stkLen-len(v.Args)]
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
		if column != nil {
			er.ctxStack = append(er.ctxStack, column)
			return inNode, true
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
		value := &expression.Constant{Value: v.Datum, RetType: types.NewFieldType(v.Datum.Kind())}
		er.ctxStack = append(er.ctxStack, value)
	case *ast.IsNullExpr:
		if checkRowLen(er.ctxStack[stkLen-1]) != 1 {
			er.err = errors.New("Operand should contain 1 column(s)")
			return retNode, false
		}
		function := expression.NewFunction(ast.IsNull, v.Type, er.ctxStack[stkLen-1])
		er.ctxStack = er.ctxStack[:stkLen-1]
		er.ctxStack = append(er.ctxStack, function)
	case *ast.BinaryOperationExpr:
		funcName, ok := opcode.Ops[v.Op]
		if !ok {
			er.err = errors.Errorf("Unknown opcode %v", v.Op)
			return retNode, false
		}
		var function *expression.ScalarFunction
		switch v.Op {
		case opcode.EQ, opcode.NE, opcode.NullEQ:
			var err error
			function, err = constructBinaryOpFunction(er.ctxStack[stkLen-2], er.ctxStack[stkLen-1], funcName)
			if err != nil {
				er.err = errors.Trace(err)
				return retNode, false
			}
		default:
			function = expression.NewFunction(funcName, v.Type, er.ctxStack[stkLen-2], er.ctxStack[stkLen-1])
		}
		er.ctxStack = er.ctxStack[:stkLen-2]
		er.ctxStack = append(er.ctxStack, function)
	case *ast.UnaryOperationExpr:
		funcName, ok := opcode.Ops[v.Op]
		if !ok {
			er.err = errors.Errorf("Unknown opcode %v", v.Op)
			return retNode, false
		}
		if checkRowLen(er.ctxStack[stkLen-1]) != 1 {
			er.err = errors.New("Operand should contain 1 column(s)")
			return retNode, false
		}
		function := expression.NewFunction(funcName, v.Type, er.ctxStack[stkLen-1])
		er.ctxStack = er.ctxStack[:stkLen-1]
		er.ctxStack = append(er.ctxStack, function)

	default:
		er.err = errors.Errorf("UnknownType: %T", v)
		return retNode, false
	}
	return inNode, true
}
