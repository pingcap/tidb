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

type rowExpr struct {
	rows []*rowExpr
	expr expression.Expression
}

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
	if er.ctxStack[0].expr == nil {
		return nil, nil, false, errors.New("Operand should contain 1 column(s)")
	}
	return er.ctxStack[0].expr, er.p, er.correlated, nil
}

type expressionRewriter struct {
	ctxStack   []*rowExpr
	p          Plan
	schema     expression.Schema
	err        error
	aggrMap    map[*ast.AggregateFuncExpr]int
	columnMap  map[*ast.ColumnNameExpr]expression.Expression
	b          *planBuilder
	correlated bool
}

func constructBinaryOpFunction(l *rowExpr, r *rowExpr, op opcode.Op) (*expression.ScalarFunction, error) {
	if l.expr != nil && r.expr != nil {
		return expression.NewFunction(opcode.Ops[op], []expression.Expression{l.expr, r.expr}, types.NewFieldType(mysql.TypeTiny))
	} else if l.expr != nil {
		return nil, errors.New("Operand should contain 1 column(s)")
	} else if r.expr != nil || len(l.rows) != len(r.rows) {
		return nil, errors.Errorf("Operand should contain %d column(s)", len(l.rows))
	}
	result, err := constructBinaryOpFunction(l.rows[0], r.rows[0], op)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if op == opcode.EQ || op == opcode.NullEQ {
		for i := 1; i < len(l.rows); i++ {
			item, err := constructBinaryOpFunction(l.rows[i], r.rows[i], op)
			if err != nil {
				return nil, errors.Trace(err)
			}
			result, err = expression.NewFunction(opcode.Ops[opcode.AndAnd],
				[]expression.Expression{result, item},
				types.NewFieldType(mysql.TypeTiny))
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
	} else {
		for i := 1; i < len(l.rows); i++ {
			eq, err := constructBinaryOpFunction(l.rows[i-1], r.rows[i-1], opcode.EQ)
			if err != nil {
				return nil, errors.Trace(err)
			}
			item, err := constructBinaryOpFunction(l.rows[i], r.rows[i], op)
			if err != nil {
				return nil, errors.Trace(err)
			}
			and, err := expression.NewFunction(opcode.Ops[opcode.AndAnd],
				[]expression.Expression{eq, item},
				types.NewFieldType(mysql.TypeTiny))
			if err != nil {
				return nil, errors.Trace(err)
			}
			result, err = expression.NewFunction(opcode.Ops[opcode.OrOr],
				[]expression.Expression{result, and},
				types.NewFieldType(mysql.TypeTiny))
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
	}
	return result, nil
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
		er.ctxStack = append(er.ctxStack, &rowExpr{expr: er.schema[index]})
		return inNode, true
	case *ast.ColumnNameExpr:
		if col, ok := er.b.colMapper[v]; ok {
			er.ctxStack = append(er.ctxStack, &rowExpr{expr: col})
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
			return retNode, true
		}
		np = er.b.buildExists(np)
		if np.IsCorrelated() {
			er.p = er.b.buildApply(er.p, np, outerSchema)
			er.ctxStack = append(er.ctxStack, &rowExpr{expr: er.p.GetSchema()[len(er.p.GetSchema())-1]})
		} else {
			_, err := pruneColumnsAndResolveIndices(np, np.GetSchema())
			if err != nil {
				er.err = errors.Trace(err)
				return retNode, true
			}
			d, err := EvalSubquery(np, er.b.is, er.b.ctx)
			if err != nil {
				er.err = errors.Trace(err)
				return retNode, true
			}
			er.ctxStack = append(er.ctxStack, &rowExpr{expr: &expression.Constant{
				Value:   d[0],
				RetType: np.GetSchema()[0].GetType()}})
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
			newExpr := make([]*rowExpr, 0, len(np.GetSchema()))
			for _, col := range np.GetSchema() {
				newExpr = append(newExpr, &rowExpr{expr: col.DeepCopy()})
			}
			if len(newExpr) > 1 {
				er.ctxStack = append(er.ctxStack, &rowExpr{rows: newExpr})
			} else {
				er.ctxStack = append(er.ctxStack, &rowExpr{expr: newExpr[0].expr})
			}
		} else {
			_, err := pruneColumnsAndResolveIndices(np, np.GetSchema())
			if err != nil {
				er.err = errors.Trace(err)
				return retNode, true
			}
			d, err := EvalSubquery(np, er.b.is, er.b.ctx)
			if err != nil {
				er.err = errors.Trace(err)
				return retNode, true
			}
			newExpr := make([]*rowExpr, 0, len(np.GetSchema()))
			for i, data := range d {
				newExpr = append(newExpr, &rowExpr{expr: &expression.Constant{
					Value:   data,
					RetType: np.GetSchema()[i].GetType()}})
			}
			if len(newExpr) > 1 {
				er.ctxStack = append(er.ctxStack, &rowExpr{rows: newExpr})
			} else {
				er.ctxStack = append(er.ctxStack, &rowExpr{expr: newExpr[0].expr})
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
		rows := make([]*rowExpr, 0, length)
		for i := stkLen - length; i < stkLen; i++ {
			rows = append(rows, er.ctxStack[i])
		}
		er.ctxStack = er.ctxStack[:len(er.ctxStack)-length]
		er.ctxStack = append(er.ctxStack, &rowExpr{rows: rows})

	case *ast.FuncCallExpr:
		function := &expression.ScalarFunction{FuncName: v.FnName}
		for i := stkLen - len(v.Args); i < stkLen; i++ {
			if er.ctxStack[i].expr != nil {
				function.Args = append(function.Args, er.ctxStack[i].expr)
			} else {
				er.err = errors.New("Operand should contain 1 column(s)")
				return retNode, false
			}
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
		er.ctxStack = er.ctxStack[:stkLen-len(v.Args)]
		er.ctxStack = append(er.ctxStack, &rowExpr{expr: function})
	case *ast.PositionExpr:
		if v.N > 0 && v.N <= len(er.schema) {
			er.ctxStack = append(er.ctxStack, &rowExpr{expr: er.schema[v.N-1]})
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
			er.ctxStack = append(er.ctxStack, &rowExpr{expr: column})
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
		er.ctxStack = append(er.ctxStack, &rowExpr{expr: column})
	case *ast.ColumnNameExpr, *ast.ParenthesesExpr, *ast.WhenClause, *ast.SubqueryExpr, *ast.ExistsSubqueryExpr:
	case *ast.ValueExpr:
		if v.Datum.Kind() == types.KindRow {
			datums := v.Datum.GetRow()
			rows := make([]*rowExpr, 0, len(datums))
			for _, d := range datums {
				rows = append(rows, &rowExpr{expr: &expression.Constant{
					Value:   d,
					RetType: types.NewFieldType(d.Kind()),
				}})
			}
			er.ctxStack = append(er.ctxStack, &rowExpr{rows: rows})
		} else {
			value := &expression.Constant{Value: v.Datum, RetType: v.Type}
			er.ctxStack = append(er.ctxStack, &rowExpr{expr: value})
		}
	case *ast.IsNullExpr:
		if er.ctxStack[stkLen-1].expr == nil {
			er.err = errors.New("Operand should contain 1 column(s)")
			return retNode, false
		}
		function, err := expression.NewFunction(ast.IsNull, []expression.Expression{er.ctxStack[stkLen-1].expr}, v.Type)
		if err != nil {
			er.err = errors.Trace(err)
			return retNode, false
		}
		er.ctxStack = er.ctxStack[:stkLen-1]
		er.ctxStack = append(er.ctxStack, &rowExpr{expr: function})
	case *ast.BinaryOperationExpr:
		funcName, ok := opcode.Ops[v.Op]
		if !ok {
			er.err = errors.Errorf("Unknown opcode %v", v.Op)
			return retNode, false
		}
		var function *expression.ScalarFunction
		var err error
		switch v.Op {
		case opcode.EQ, opcode.LE, opcode.GE, opcode.LT, opcode.GT, opcode.NE, opcode.NullEQ:
			function, err = constructBinaryOpFunction(er.ctxStack[stkLen-2], er.ctxStack[stkLen-1], v.Op)
		default:
			function, err = expression.NewFunction(funcName,
				[]expression.Expression{er.ctxStack[stkLen-2].expr, er.ctxStack[stkLen-1].expr}, v.Type)
		}
		if err != nil {
			er.err = errors.Trace(err)
			return retNode, false
		}
		er.ctxStack = er.ctxStack[:stkLen-2]
		er.ctxStack = append(er.ctxStack, &rowExpr{expr: function})
	case *ast.UnaryOperationExpr:
		funcName, ok := opcode.Ops[v.Op]
		if !ok {
			er.err = errors.Errorf("Unknown opcode %v", v.Op)
			return retNode, false
		}
		if er.ctxStack[stkLen-1].expr == nil {
			er.err = errors.New("Operand should contain 1 column(s)")
			return retNode, false
		}
		function, err := expression.NewFunction(funcName, []expression.Expression{er.ctxStack[stkLen-1].expr}, v.Type)
		if err != nil {
			er.err = errors.Trace(err)
			return retNode, false
		}
		er.ctxStack = er.ctxStack[:stkLen-1]
		er.ctxStack = append(er.ctxStack, &rowExpr{expr: function})

	default:
		er.err = errors.Errorf("UnknownType: %T", v)
		return retNode, false
	}
	return inNode, true
}
