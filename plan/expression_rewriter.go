package plan

import (
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/evaluator"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/types"
)

// EvalSubquery evaluates incorrelated subqueries once.
var EvalSubquery func(p PhysicalPlan, is infoschema.InfoSchema, ctx context.Context) ([]types.Datum, error)

// rewrite function rewrites ast expr to expression.Expression.
// aggMapper maps ast.AggregateFuncExpr to the columns offset in p's output schema.
// asScalar means whether this expression must be treated as a scalar expression.
// And this function returns a result expression, a new plan that may have apply or semi-join, and bool value that
// standing for if the expression containing a correlated column.
func (b *planBuilder) rewrite(expr ast.ExprNode, p LogicalPlan, aggMapper map[*ast.AggregateFuncExpr]int, asScalar bool) (
	expression.Expression, LogicalPlan, bool, error) {
	er := &expressionRewriter{
		p:        p,
		aggrMap:  aggMapper,
		schema:   p.GetSchema(),
		b:        b,
		asScalar: asScalar,
	}
	expr.Accept(er)
	if er.err != nil {
		return nil, nil, false, errors.Trace(er.err)
	}
	if !asScalar && len(er.ctxStack) == 0 {
		return nil, er.p, er.correlated, nil
	}
	if len(er.ctxStack) != 1 {
		return nil, nil, false, errors.Errorf("context len %v is invalid", len(er.ctxStack))
	}
	if getRowLen(er.ctxStack[0]) != 1 {
		return nil, nil, false, errors.New("Operand should contain 1 column(s)")
	}
	return er.ctxStack[0], er.p, er.correlated, nil
}

type expressionRewriter struct {
	ctxStack   []expression.Expression
	p          LogicalPlan
	schema     expression.Schema
	err        error
	aggrMap    map[*ast.AggregateFuncExpr]int
	columnMap  map[*ast.ColumnNameExpr]expression.Expression
	b          *planBuilder
	correlated bool
	// asScalar means the return value must be a scalar value.
	asScalar bool
}

func getRowLen(e expression.Expression) int {
	if f, ok := e.(*expression.ScalarFunction); ok && f.FuncName.L == ast.RowFunc {
		return len(f.Args)
	}
	if c, ok := e.(*expression.Constant); ok && c.Value.Kind() == types.KindRow {
		return len(c.Value.GetRow())
	}
	return 1
}

func getRowArg(e expression.Expression, idx int) expression.Expression {
	if f, ok := e.(*expression.ScalarFunction); ok {
		return f.Args[idx]
	}
	c, _ := e.(*expression.Constant)
	d := c.Value.GetRow()[idx]
	return &expression.Constant{Value: d, RetType: c.GetType()}
}

// constructBinaryOpFunctions converts (a0,a1,a2) op (b0,b1,b2) to (a0 op b0) and (a1 op b1) and (a2 op b2).
func constructBinaryOpFunction(l expression.Expression, r expression.Expression, op string) (expression.Expression, error) {
	lLen, rLen := getRowLen(l), getRowLen(r)
	if lLen == 1 && rLen == 1 {
		return expression.NewFunction(op, types.NewFieldType(mysql.TypeTiny), l, r)
	} else if rLen != lLen {
		return nil, errors.Errorf("Operand should contain %d column(s)", lLen)
	}
	funcs := make([]expression.Expression, lLen)
	for i := 0; i < lLen; i++ {
		var err error
		funcs[i], err = constructBinaryOpFunction(getRowArg(l, i), getRowArg(r, i), op)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return expression.ComposeCNFCondition(funcs), nil
}

func (er *expressionRewriter) buildSubquery(subq *ast.SubqueryExpr) (LogicalPlan, expression.Schema) {
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
		if index, ok := er.b.colMapper[v]; ok {
			er.ctxStack = append(er.ctxStack, er.schema[index])
			return inNode, true
		}
	case *ast.CompareSubqueryExpr:
		return er.handleCompareSubquery(v)
	case *ast.ExistsSubqueryExpr:
		return er.handleExistSubquery(v)
	case *ast.PatternInExpr:
		if v.Sel != nil {
			return er.handleInSubquery(v)
		}
	case *ast.SubqueryExpr:
		return er.handleScalarSubquery(v)
	case *ast.ParenthesesExpr:
	default:
		er.asScalar = true
	}
	return inNode, false
}

func (er *expressionRewriter) handleCompareSubquery(v *ast.CompareSubqueryExpr) (ast.Node, bool) {
	v.L.Accept(er)
	if er.err != nil {
		return v, true
	}
	lexpr := er.ctxStack[len(er.ctxStack)-1]
	subq, ok := v.R.(*ast.SubqueryExpr)
	if !ok {
		er.err = errors.Errorf("Unknown compare type %T.", v.R)
		return v, true
	}
	np, outerSchema := er.buildSubquery(subq)
	if er.err != nil {
		return v, true
	}
	// Only (a,b,c) = all (...) and (a,b,c) != any () can use row expression.
	canMultiCol := (!v.All && v.Op == opcode.EQ) || (v.All && v.Op == opcode.NE)
	if !canMultiCol && (getRowLen(lexpr) != 1 || len(np.GetSchema()) != 1) {
		er.err = errors.New("Operand should contain 1 column(s)")
		return v, true
	}
	if getRowLen(lexpr) != len(np.GetSchema()) {
		er.err = errors.Errorf("Operand should contain %d column(s)", getRowLen(lexpr))
		return v, true
	}
	var checkCondition expression.Expression
	var rexpr expression.Expression
	if len(np.GetSchema()) == 1 {
		rexpr = np.GetSchema()[0].DeepCopy()
	} else {
		args := make([]expression.Expression, 0, len(np.GetSchema()))
		for _, col := range np.GetSchema() {
			args = append(args, col.DeepCopy())
		}
		rexpr, er.err = expression.NewFunction(ast.RowFunc, types.NewFieldType(types.KindRow), args...)
		if er.err != nil {
			er.err = errors.Trace(er.err)
			return v, true
		}
	}
	switch v.Op {
	// Only EQ, NE and NullEQ can be composed with and.
	case opcode.EQ, opcode.NE, opcode.NullEQ:
		checkCondition, er.err = constructBinaryOpFunction(lexpr, rexpr, opcode.Ops[v.Op])
		if er.err != nil {
			er.err = errors.Trace(er.err)
			return v, true
		}
	// If op is not EQ, NE, NullEQ, say LT, it will remain as row(a,b) < row(c,d), and be compared as row datum.
	default:
		checkCondition, er.err = expression.NewFunction(opcode.Ops[v.Op],
			types.NewFieldType(mysql.TypeTiny), lexpr, rexpr)
		if er.err != nil {
			er.err = errors.Trace(er.err)
			return v, true
		}
	}
	er.p = er.b.buildApply(er.p, np, outerSchema, &ApplyConditionChecker{Condition: checkCondition, All: v.All})
	if er.p.IsCorrelated() {
		er.correlated = true
	}
	// The parent expression only use the last column in schema, which represents whether the condition is matched.
	er.ctxStack[len(er.ctxStack)-1] = er.p.GetSchema()[len(er.p.GetSchema())-1]
	return v, true
}

func (er *expressionRewriter) handleExistSubquery(v *ast.ExistsSubqueryExpr) (ast.Node, bool) {
	subq, ok := v.Sel.(*ast.SubqueryExpr)
	if !ok {
		er.err = errors.Errorf("Unknown exists type %T.", v.Sel)
		return v, true
	}
	np, outerSchema := er.buildSubquery(subq)
	if er.err != nil {
		return v, true
	}
	np = er.b.buildExists(np)
	if np.IsCorrelated() {
		if sel, ok := np.GetChildByIndex(0).(*Selection); ok && !sel.GetChildByIndex(0).IsCorrelated() {
			er.p = er.b.buildSemiJoin(er.p, sel.GetChildByIndex(0).(LogicalPlan), sel.Conditions, er.asScalar, false)
			if !er.asScalar {
				return v, true
			}
		} else {
			// Can't be built as semi-join
			er.p = er.b.buildApply(er.p, np, outerSchema, nil)
		}
		if er.p.IsCorrelated() {
			er.correlated = true
		}
		er.ctxStack = append(er.ctxStack, er.p.GetSchema()[len(er.p.GetSchema())-1])
	} else {
		_, np, er.err = np.PredicatePushDown(nil)
		if er.err != nil {
			return v, true
		}
		_, err := np.PruneColumnsAndResolveIndices(np.GetSchema())
		if err != nil {
			er.err = errors.Trace(err)
			return v, true
		}
		info, err := np.convert2PhysicalPlan(&requiredProperty{})
		if err != nil {
			er.err = errors.Trace(err)
			return v, true
		}
		d, err := EvalSubquery(info.p, er.b.is, er.b.ctx)
		if err != nil {
			er.err = errors.Trace(err)
			return v, true
		}
		er.ctxStack = append(er.ctxStack, &expression.Constant{
			Value:   d[0],
			RetType: np.GetSchema()[0].GetType()})
	}
	return v, true
}

func (er *expressionRewriter) handleInSubquery(v *ast.PatternInExpr) (ast.Node, bool) {
	asScalar := er.asScalar
	er.asScalar = true
	v.Expr.Accept(er)
	if er.err != nil {
		return v, true
	}
	lexpr := er.ctxStack[len(er.ctxStack)-1]
	subq, ok := v.Sel.(*ast.SubqueryExpr)
	if !ok {
		er.err = errors.Errorf("Unknown compare type %T.", v.Sel)
		return v, true
	}
	np, outerSchema := er.buildSubquery(subq)
	if er.err != nil {
		return v, true
	}
	if getRowLen(lexpr) != len(np.GetSchema()) {
		er.err = errors.Errorf("Operand should contain %d column(s)", getRowLen(lexpr))
		return v, true
	}
	var rexpr expression.Expression
	if len(np.GetSchema()) == 1 {
		rexpr = np.GetSchema()[0].DeepCopy()
	} else {
		args := make([]expression.Expression, 0, len(np.GetSchema()))
		for _, col := range np.GetSchema() {
			args = append(args, col.DeepCopy())
		}
		rexpr, er.err = expression.NewFunction(ast.RowFunc, nil, args...)
		if er.err != nil {
			er.err = errors.Trace(er.err)
			return v, true
		}
	}
	// a in (subq) will be rewrited as a = any(subq).
	// a not in (subq) will be rewrited as a != all(subq).
	checkCondition, err := constructBinaryOpFunction(lexpr, rexpr, ast.EQ)
	if !np.IsCorrelated() {
		er.p = er.b.buildSemiJoin(er.p, np, expression.SplitCNFItems(checkCondition), asScalar, v.Not)
		if asScalar {
			col := er.p.GetSchema()[len(er.p.GetSchema())-1]
			er.ctxStack[len(er.ctxStack)-1] = col
		} else {
			er.ctxStack = er.ctxStack[:len(er.ctxStack)-1]
		}
		return v, true
	}
	if v.Not {
		checkCondition, _ = expression.NewFunction(ast.UnaryNot, &v.Type, checkCondition)
	}
	if err != nil {
		er.err = errors.Trace(err)
		return v, true
	}
	er.p = er.b.buildApply(er.p, np, outerSchema, &ApplyConditionChecker{Condition: checkCondition, All: v.Not})
	if er.p.IsCorrelated() {
		er.correlated = true
	}
	// The parent expression only use the last column in schema, which represents whether the condition is matched.
	er.ctxStack[len(er.ctxStack)-1] = er.p.GetSchema()[len(er.p.GetSchema())-1]
	return v, true

}

func (er *expressionRewriter) handleScalarSubquery(v *ast.SubqueryExpr) (ast.Node, bool) {
	np, outerSchema := er.buildSubquery(v)
	if er.err != nil {
		return v, true
	}
	np = er.b.buildMaxOneRow(np)
	if np.IsCorrelated() {
		er.p = er.b.buildApply(er.p, np, outerSchema, nil)
		if er.p.IsCorrelated() {
			er.correlated = true
		}
		if len(np.GetSchema()) > 1 {
			newCols := make([]expression.Expression, 0, len(np.GetSchema()))
			for _, col := range np.GetSchema() {
				newCols = append(newCols, col.DeepCopy())
			}
			expr, err := expression.NewFunction(ast.RowFunc, nil, newCols...)
			if err != nil {
				er.err = errors.Trace(err)
				return v, true
			}
			er.ctxStack = append(er.ctxStack, expr)
		} else {
			er.ctxStack = append(er.ctxStack, er.p.GetSchema()[len(er.p.GetSchema())-1])
		}
		return v, true
	}
	_, np, er.err = np.PredicatePushDown(nil)
	if er.err != nil {
		return v, true
	}
	_, err := np.PruneColumnsAndResolveIndices(np.GetSchema())
	if err != nil {
		er.err = errors.Trace(err)
		return v, true
	}
	info, err := np.convert2PhysicalPlan(&requiredProperty{})
	if err != nil {
		er.err = errors.Trace(err)
		return v, true
	}
	d, err := EvalSubquery(info.p, er.b.is, er.b.ctx)
	if err != nil {
		er.err = errors.Trace(err)
		return v, true
	}
	if len(np.GetSchema()) > 1 {
		newCols := make([]expression.Expression, 0, len(np.GetSchema()))
		for i, data := range d {
			newCols = append(newCols, &expression.Constant{
				Value:   data,
				RetType: np.GetSchema()[i].GetType()})
		}
		expr, err1 := expression.NewFunction(ast.RowFunc, nil, newCols...)
		if err1 != nil {
			er.err = errors.Trace(err1)
			return v, true
		}
		er.ctxStack = append(er.ctxStack, expr)
	} else {
		er.ctxStack = append(er.ctxStack, &expression.Constant{
			Value:   d[0],
			RetType: np.GetSchema()[0].GetType(),
		})
	}
	return v, true
}

// Leave implements Visitor interface.
func (er *expressionRewriter) Leave(inNode ast.Node) (retNode ast.Node, ok bool) {
	if er.err != nil {
		return retNode, false
	}

	switch v := inNode.(type) {
	case *ast.AggregateFuncExpr, *ast.ColumnNameExpr, *ast.ParenthesesExpr, *ast.WhenClause,
		*ast.SubqueryExpr, *ast.ExistsSubqueryExpr, *ast.CompareSubqueryExpr:
	case *ast.ValueExpr:
		value := &expression.Constant{Value: v.Datum, RetType: &v.Type}
		er.ctxStack = append(er.ctxStack, value)
	case *ast.ParamMarkerExpr:
		value := &expression.Constant{Value: v.Datum, RetType: &v.Type}
		er.ctxStack = append(er.ctxStack, value)
	case *ast.VariableExpr:
		er.rewriteVariable(v)
	case *ast.FuncCallExpr:
		er.funcCallToExpression(v)
	case *ast.ColumnName:
		er.toColumn(v)
	case *ast.UnaryOperationExpr:
		er.unaryOpToExpression(v)
	case *ast.BinaryOperationExpr:
		er.binaryOpToExpression(v)
	case *ast.BetweenExpr:
		er.betweenToExpression(v)
	case *ast.CaseExpr:
		er.caseToExpression(v)
	case *ast.FuncCastExpr:
		er.castToScalarFunc(v)
	case *ast.PatternLikeExpr:
		er.likeToScalarFunc(v)
	case *ast.PatternRegexpExpr:
		er.regexpToScalarFunc(v)
	case *ast.RowExpr:
		er.rowToScalarFunc(v)
	case *ast.PatternInExpr:
		er.inToExpression(v)
	case *ast.PositionExpr:
		er.positionToScalarFunc(v)
	case *ast.IsNullExpr:
		er.isnullToExpression(v)
	case *ast.IsTruthExpr:
		er.istrueToScalarFunc(v)
	default:
		er.err = errors.Errorf("UnknownType: %T", v)
		return retNode, false
	}

	if er.err != nil {
		return retNode, false
	}
	return inNode, true
}

func datumToConstant(d types.Datum, tp byte) *expression.Constant {
	return &expression.Constant{Value: d, RetType: types.NewFieldType(tp)}
}

func (er *expressionRewriter) rewriteVariable(v *ast.VariableExpr) {
	stkLen := len(er.ctxStack)
	name := strings.ToLower(v.Name)
	sessionVars := variable.GetSessionVars(er.b.ctx)
	globalVars := variable.GetGlobalVarAccessor(er.b.ctx)
	if !v.IsSystem {
		if v.Value != nil {
			er.ctxStack[stkLen-1], er.err = expression.NewFunction(ast.SetVar,
				er.ctxStack[stkLen-1].GetType(),
				datumToConstant(types.NewDatum(name), mysql.TypeString),
				er.ctxStack[stkLen-1])
			return
		}
		if _, ok := sessionVars.Users[name]; ok {
			f, err := expression.NewFunction(ast.GetVar,
				// TODO: Here is wrong, the sessionVars should store a name -> Datum map. Will fix it later.
				types.NewFieldType(mysql.TypeString),
				datumToConstant(types.NewStringDatum(name), mysql.TypeString))
			if err != nil {
				er.err = errors.Trace(err)
				return
			}
			er.ctxStack = append(er.ctxStack, f)
		} else {
			// select null user vars is permitted.
			er.ctxStack = append(er.ctxStack, &expression.Constant{RetType: types.NewFieldType(mysql.TypeNull)})
		}
		return
	}

	sysVar, ok := variable.SysVars[name]
	if !ok {
		// select null sys vars is not permitted
		er.err = variable.UnknownSystemVar.Gen("Unknown system variable '%s'", name)
		return
	}
	if sysVar.Scope == variable.ScopeNone {
		er.ctxStack = append(er.ctxStack, datumToConstant(types.NewDatum(sysVar.Value), mysql.TypeString))
		return
	}

	if v.IsGlobal {
		value, err := globalVars.GetGlobalSysVar(er.b.ctx, name)
		if err != nil {
			er.err = errors.Trace(err)
			return
		}
		er.ctxStack = append(er.ctxStack, datumToConstant(types.NewDatum(value), mysql.TypeString))
		return
	}
	d := sessionVars.GetSystemVar(name)
	if d.IsNull() {
		if sysVar.Scope&variable.ScopeGlobal == 0 {
			d.SetString(sysVar.Value)
		} else {
			// Get global system variable and fill it in session.
			globalVal, err := globalVars.GetGlobalSysVar(er.b.ctx, name)
			if err != nil {
				er.err = errors.Trace(err)
				return
			}
			d.SetString(globalVal)
			err = sessionVars.SetSystemVar(name, d)
			if err != nil {
				er.err = errors.Trace(err)
				return
			}
		}
	}
	er.ctxStack = append(er.ctxStack, datumToConstant(d, mysql.TypeString))
	return
}

func (er *expressionRewriter) unaryOpToExpression(v *ast.UnaryOperationExpr) {
	stkLen := len(er.ctxStack)
	if getRowLen(er.ctxStack[stkLen-1]) != 1 {
		er.err = errors.New("Operand should contain 1 column(s)")
	}
	var op string
	switch v.Op {
	case opcode.Plus:
		// expression (+ a) is equal to a
		return
	case opcode.Minus:
		op = ast.UnaryMinus
	case opcode.BitNeg:
		op = ast.BitNeg
	case opcode.Not:
		op = ast.UnaryNot
	default:
		er.err = errors.Errorf("Unknown Unary Op %T", v.Op)
		return
	}
	er.ctxStack[stkLen-1], er.err = expression.NewFunction(op, &v.Type, er.ctxStack[stkLen-1])
}

func (er *expressionRewriter) binaryOpToExpression(v *ast.BinaryOperationExpr) {
	stkLen := len(er.ctxStack)
	var function expression.Expression
	switch v.Op {
	case opcode.EQ, opcode.NE, opcode.NullEQ:
		function, er.err = constructBinaryOpFunction(er.ctxStack[stkLen-2], er.ctxStack[stkLen-1],
			opcode.Ops[v.Op])
	default:
		function, er.err = expression.NewFunction(opcode.Ops[v.Op], &v.Type, er.ctxStack[stkLen-2:]...)
	}
	if er.err != nil {
		er.err = errors.Trace(er.err)
		return
	}
	er.ctxStack = er.ctxStack[:stkLen-2]
	er.ctxStack = append(er.ctxStack, function)
}

func (er *expressionRewriter) notToExpression(hasNot bool, op string, tp *types.FieldType,
	args ...expression.Expression) expression.Expression {
	opFunc, err := expression.NewFunction(op, tp, args...)
	if err != nil {
		er.err = errors.Trace(err)
		return nil
	}
	if !hasNot {
		return opFunc
	}

	opFunc, err = expression.NewFunction(ast.UnaryNot, tp, opFunc)
	if err != nil {
		er.err = errors.Trace(err)
		return nil
	}
	return opFunc
}

func (er *expressionRewriter) isnullToExpression(v *ast.IsNullExpr) {
	stkLen := len(er.ctxStack)
	if getRowLen(er.ctxStack[stkLen-1]) != 1 {
		er.err = errors.New("Operand should contain 1 column(s)")
		return
	}
	function := er.notToExpression(v.Not, ast.IsNull, &v.Type, er.ctxStack[stkLen-1])
	er.ctxStack = er.ctxStack[:stkLen-1]
	er.ctxStack = append(er.ctxStack, function)
}

func (er *expressionRewriter) positionToScalarFunc(v *ast.PositionExpr) {
	if v.N > 0 && v.N <= len(er.schema) {
		er.ctxStack = append(er.ctxStack, er.schema[v.N-1])
	} else {
		er.err = errors.Errorf("Position %d is out of range", v.N)
	}
}

func (er *expressionRewriter) istrueToScalarFunc(v *ast.IsTruthExpr) {
	stkLen := len(er.ctxStack)
	op := ast.IsTruth
	if v.True == 0 {
		op = ast.IsFalsity
	}
	function := er.notToExpression(v.Not, op, &v.Type, er.ctxStack[stkLen-1])
	er.ctxStack = er.ctxStack[:stkLen-1]
	er.ctxStack = append(er.ctxStack, function)
}

func (er *expressionRewriter) inToExpression(v *ast.PatternInExpr) {
	if v.Sel != nil {
		return
	}
	stkLen := len(er.ctxStack)
	lLen := len(v.List)
	function := er.notToExpression(v.Not, ast.In, &v.Type, er.ctxStack[stkLen-lLen-1:stkLen]...)
	er.ctxStack = er.ctxStack[:stkLen-lLen-1]
	er.ctxStack = append(er.ctxStack, function)
}

func (er *expressionRewriter) caseToExpression(v *ast.CaseExpr) {
	stkLen := len(er.ctxStack)
	argsLen := 2 * len(v.WhenClauses)
	if v.ElseClause != nil {
		argsLen++
	}

	// value                          -> ctxStack[stkLen-argsLen-1]
	// when clause(condition, result) -> ctxStack[stkLen-argsLen:stkLen-1];
	// else clause                    -> ctxStack[stkLen-1]
	var args []expression.Expression
	if v.Value != nil {
		// args:  eq scalar func(args: value, condition1), result1,
		//        eq scalar func(args: value, condition2), result2,
		//        ...
		//        else clause
		value := er.ctxStack[stkLen-argsLen-1]
		args = make([]expression.Expression, 0, argsLen)
		for i := stkLen - argsLen; i < stkLen-1; i += 2 {
			arg, err := expression.NewFunction(ast.EQ, types.NewFieldType(mysql.TypeTiny), value.DeepCopy(), er.ctxStack[i])
			if err != nil {
				er.err = errors.Trace(err)
				return
			}
			args = append(args, arg)
			args = append(args, er.ctxStack[i+1])
		}
		if v.ElseClause != nil {
			args = append(args, er.ctxStack[stkLen-1])
		}
		argsLen++ // for trimming the value element later
	} else {
		// args:  condition1, result1,
		//        condition2, result2,
		//        ...
		//        else clause
		args = er.ctxStack[stkLen-argsLen : stkLen]
	}
	function, err := expression.NewFunction(ast.Case, &v.Type, args...)
	if err != nil {
		er.err = errors.Trace(err)
		return
	}
	er.ctxStack = er.ctxStack[:stkLen-argsLen]
	er.ctxStack = append(er.ctxStack, function)
}

func (er *expressionRewriter) likeToScalarFunc(v *ast.PatternLikeExpr) {
	l := len(er.ctxStack)
	function := er.notToExpression(v.Not, ast.Like, &v.Type,
		er.ctxStack[l-2], er.ctxStack[l-1], &expression.Constant{Value: types.NewIntDatum(int64(v.Escape))})
	er.ctxStack = er.ctxStack[:l-2]
	er.ctxStack = append(er.ctxStack, function)
}

func (er *expressionRewriter) regexpToScalarFunc(v *ast.PatternRegexpExpr) {
	l := len(er.ctxStack)
	function := er.notToExpression(v.Not, ast.Regexp, &v.Type, er.ctxStack[l-2], er.ctxStack[l-1])
	er.ctxStack = er.ctxStack[:l-2]
	er.ctxStack = append(er.ctxStack, function)
}

func (er *expressionRewriter) rowToScalarFunc(v *ast.RowExpr) {
	stkLen := len(er.ctxStack)
	length := len(v.Values)
	rows := make([]expression.Expression, 0, length)
	for i := stkLen - length; i < stkLen; i++ {
		rows = append(rows, er.ctxStack[i])
	}
	er.ctxStack = er.ctxStack[:stkLen-length]
	function, err := expression.NewFunction(ast.RowFunc, nil, rows...)
	if err != nil {
		er.err = errors.Trace(err)
		return
	}
	er.ctxStack = append(er.ctxStack, function)
}

func (er *expressionRewriter) betweenToExpression(v *ast.BetweenExpr) {
	stkLen := len(er.ctxStack)
	var op string
	var l, r expression.Expression
	if v.Not {
		l, er.err = expression.NewFunction(ast.LT, &v.Type, er.ctxStack[stkLen-3], er.ctxStack[stkLen-2])
		if er.err == nil {
			r, er.err = expression.NewFunction(ast.GT, &v.Type, er.ctxStack[stkLen-3].DeepCopy(), er.ctxStack[stkLen-1])
		}
		op = ast.OrOr
	} else {
		l, er.err = expression.NewFunction(ast.GE, &v.Type, er.ctxStack[stkLen-3], er.ctxStack[stkLen-2])
		if er.err == nil {
			r, er.err = expression.NewFunction(ast.LE, &v.Type, er.ctxStack[stkLen-3].DeepCopy(), er.ctxStack[stkLen-1])
		}
		op = ast.AndAnd
	}
	if er.err != nil {
		er.err = errors.Trace(er.err)
		return
	}
	function, err := expression.NewFunction(op, &v.Type, l, r)
	if err != nil {
		er.err = errors.Trace(err)
		return
	}
	er.ctxStack = er.ctxStack[:stkLen-3]
	er.ctxStack = append(er.ctxStack, function)
}

func (er *expressionRewriter) funcCallToExpression(v *ast.FuncCallExpr) {
	stackLen := len(er.ctxStack)
	var function expression.Expression
	function, er.err = expression.NewFunction(v.FnName.L, &v.Type, er.ctxStack[stackLen-len(v.Args):]...)
	er.ctxStack = er.ctxStack[:stackLen-len(v.Args)]
	er.ctxStack = append(er.ctxStack, function)
}

func (er *expressionRewriter) toColumn(v *ast.ColumnName) {
	column, err := er.schema.FindColumn(v)
	if err != nil {
		er.err = errors.Trace(err)
		return
	}
	if column != nil {
		er.ctxStack = append(er.ctxStack, column)
		return
	}

	for i := len(er.b.outerSchemas) - 1; i >= 0; i-- {
		outer := er.b.outerSchemas[i]
		column, err = outer.FindColumn(v)
		if err != nil {
			er.err = errors.Trace(err)
			return
		}
		if column != nil {
			er.correlated = true
			break
		}
	}
	if column == nil {
		er.err = errors.Errorf("Unknown column %s %s %s.", v.Schema.L, v.Table.L, v.Name.L)
		return
	}
	er.ctxStack = append(er.ctxStack, column)
}

func (er *expressionRewriter) castToScalarFunc(v *ast.FuncCastExpr) {
	bt, err := evaluator.CastFuncFactory(v.Tp)
	if err != nil {
		er.err = errors.Trace(err)
		return
	}
	function := &expression.ScalarFunction{
		Args:      []expression.Expression{er.ctxStack[len(er.ctxStack)-1]},
		FuncName:  model.NewCIStr("cast"),
		RetType:   v.Tp,
		Function:  bt,
		ArgValues: make([]types.Datum, 1)}
	er.ctxStack[len(er.ctxStack)-1] = function
}
