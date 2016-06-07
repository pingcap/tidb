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

package evaluator

import (
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/types"
)

// Error instances.
var (
	ErrInvalidOperation = terror.ClassEvaluator.New(CodeInvalidOperation, "invalid operation")
)

// Error codes.
const (
	CodeInvalidOperation terror.ErrCode = 1
)

// Eval evaluates an expression to a datum.
func Eval(ctx context.Context, expr ast.ExprNode) (d types.Datum, err error) {
	if ast.IsEvaluated(expr) {
		return *expr.GetDatum(), nil
	}
	e := &Evaluator{ctx: ctx}
	expr.Accept(e)
	if e.err != nil {
		return d, errors.Trace(e.err)
	}
	if ast.IsPreEvaluable(expr) && (expr.GetFlag()&ast.FlagHasFunc == 0) {
		expr.SetFlag(expr.GetFlag() | ast.FlagPreEvaluated)
	}
	return *expr.GetDatum(), nil
}

// EvalBool evalueates an expression to a boolean value.
func EvalBool(ctx context.Context, expr ast.ExprNode) (bool, error) {
	val, err := Eval(ctx, expr)
	if err != nil {
		return false, errors.Trace(err)
	}
	if val.IsNull() {
		return false, nil
	}

	i, err := val.ToBool()
	if err != nil {
		return false, errors.Trace(err)
	}
	return i != 0, nil
}

func boolToInt64(v bool) int64 {
	if v {
		return int64(1)
	}
	return int64(0)
}

// Evaluator is an ast Visitor that evaluates an expression.
type Evaluator struct {
	ctx context.Context
	err error
}

// Enter implements ast.Visitor interface.
func (e *Evaluator) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	return in, false
}

// Leave implements ast.Visitor interface.
func (e *Evaluator) Leave(in ast.Node) (out ast.Node, ok bool) {
	switch v := in.(type) {
	case *ast.AggregateFuncExpr:
		ok = e.aggregateFunc(v)
	case *ast.BetweenExpr:
		ok = e.between(v)
	case *ast.BinaryOperationExpr:
		ok = e.binaryOperation(v)
	case *ast.CaseExpr:
		ok = e.caseExpr(v)
	case *ast.ColumnName:
		ok = true
	case *ast.ColumnNameExpr:
		ok = e.columnName(v)
	case *ast.CompareSubqueryExpr:
		ok = e.compareSubquery(v)
	case *ast.DefaultExpr:
		ok = e.defaultExpr(v)
	case *ast.ExistsSubqueryExpr:
		ok = e.existsSubquery(v)
	case *ast.FuncCallExpr:
		ok = e.funcCall(v)
	case *ast.FuncCastExpr:
		ok = e.funcCast(v)
	case *ast.IsNullExpr:
		ok = e.isNull(v)
	case *ast.IsTruthExpr:
		ok = e.isTruth(v)
	case *ast.ParamMarkerExpr:
		ok = e.paramMarker(v)
	case *ast.ParenthesesExpr:
		ok = e.parentheses(v)
	case *ast.PatternInExpr:
		ok = e.patternIn(v)
	case *ast.PatternLikeExpr:
		ok = e.patternLike(v)
	case *ast.PatternRegexpExpr:
		ok = e.patternRegexp(v)
	case *ast.PositionExpr:
		ok = e.position(v)
	case *ast.RowExpr:
		ok = e.row(v)
	case *ast.SubqueryExpr:
		ok = e.subqueryExpr(v)
	case *ast.UnaryOperationExpr:
		ok = e.unaryOperation(v)
	case *ast.ValueExpr:
		ok = true
	case *ast.ValuesExpr:
		ok = e.values(v)
	case *ast.VariableExpr:
		ok = e.variable(v)
	case *ast.WhenClause:
		ok = true
	}
	out = in
	return
}

func (e *Evaluator) between(v *ast.BetweenExpr) bool {
	var l, r ast.ExprNode
	op := opcode.AndAnd

	if v.Not {
		// v < lv || v > rv
		op = opcode.OrOr
		l = &ast.BinaryOperationExpr{Op: opcode.LT, L: v.Expr, R: v.Left}
		r = &ast.BinaryOperationExpr{Op: opcode.GT, L: v.Expr, R: v.Right}
	} else {
		// v >= lv && v <= rv
		l = &ast.BinaryOperationExpr{Op: opcode.GE, L: v.Expr, R: v.Left}
		r = &ast.BinaryOperationExpr{Op: opcode.LE, L: v.Expr, R: v.Right}
	}
	ast.MergeChildrenFlags(l, v.Expr, v.Left)
	ast.MergeChildrenFlags(l, v.Expr, v.Right)
	ret := &ast.BinaryOperationExpr{Op: op, L: l, R: r}
	ast.MergeChildrenFlags(ret, l, r)
	ret.Accept(e)
	if e.err != nil {
		return false
	}
	v.SetDatum(*ret.GetDatum())
	return true
}

func (e *Evaluator) caseExpr(v *ast.CaseExpr) bool {
	tmp := types.NewDatum(boolToInt64(true))
	target := &tmp
	if v.Value != nil {
		target = v.Value.GetDatum()
	}
	if !target.IsNull() {
		for _, val := range v.WhenClauses {
			cmp, err := target.CompareDatum(*val.Expr.GetDatum())
			if err != nil {
				e.err = errors.Trace(err)
				return false
			}
			if cmp == 0 {
				v.SetDatum(*val.Result.GetDatum())
				return true
			}
		}
	}
	if v.ElseClause != nil {
		v.SetDatum(*v.ElseClause.GetDatum())
	} else {
		v.SetNull()
	}
	return true
}

func (e *Evaluator) columnName(v *ast.ColumnNameExpr) bool {
	v.SetDatum(*v.Refer.Expr.GetDatum())
	return true
}

func (e *Evaluator) defaultExpr(v *ast.DefaultExpr) bool {
	return true
}

func (e *Evaluator) compareSubquery(cs *ast.CompareSubqueryExpr) bool {
	lv := *cs.L.GetDatum()
	if lv.IsNull() {
		cs.SetNull()
		return true
	}
	x, err := e.checkResult(cs, lv, cs.R.GetDatum().GetRow())
	if err != nil {
		e.err = errors.Trace(err)
		return false
	}
	cs.SetDatum(x)
	return true
}

func (e *Evaluator) checkResult(cs *ast.CompareSubqueryExpr, lv types.Datum, result []types.Datum) (types.Datum, error) {
	if cs.All {
		return e.checkAllResult(cs, lv, result)
	}
	return e.checkAnyResult(cs, lv, result)
}

func (e *Evaluator) checkAllResult(cs *ast.CompareSubqueryExpr, lv types.Datum, result []types.Datum) (d types.Datum, err error) {
	hasNull := false
	for _, v := range result {
		if v.IsNull() {
			hasNull = true
			continue
		}

		comRes, err1 := lv.CompareDatum(v)
		if err1 != nil {
			return d, errors.Trace(err1)
		}

		res, err1 := getCompResult(cs.Op, comRes)
		if err1 != nil {
			return d, errors.Trace(err1)
		}
		if !res {
			d.SetInt64(boolToInt64(false))
			return d, nil
		}
	}
	if hasNull {
		// If no matched but we get null, return null.
		// Like `insert t (c) values (1),(2),(null)`, then
		// `select 3 > all (select c from t)`, returns null.
		return d, nil
	}
	d.SetInt64(boolToInt64(true))
	return d, nil
}

func (e *Evaluator) checkAnyResult(cs *ast.CompareSubqueryExpr, lv types.Datum, result []types.Datum) (d types.Datum, err error) {
	hasNull := false
	for _, v := range result {
		if v.IsNull() {
			hasNull = true
			continue
		}

		comRes, err1 := lv.CompareDatum(v)
		if err1 != nil {
			return d, errors.Trace(err1)
		}

		res, err1 := getCompResult(cs.Op, comRes)
		if err1 != nil {
			return d, errors.Trace(err1)
		}
		if res {
			d.SetInt64(boolToInt64(true))
			return d, nil
		}
	}

	if hasNull {
		// If no matched but we get null, return null.
		// Like `insert t (c) values (1),(2),(null)`, then
		// `select 0 > any (select c from t)`, returns null.
		return d, nil
	}

	d.SetInt64(boolToInt64(false))
	return d, nil
}

func (e *Evaluator) existsSubquery(v *ast.ExistsSubqueryExpr) bool {
	d := v.Sel.GetDatum()
	if d.IsNull() {
		v.SetInt64(0)
		return true
	}
	rows := d.GetRow()
	if len(rows) > 0 {
		v.SetInt64(1)
	} else {
		v.SetInt64(0)
	}
	return true
}

// Evaluate SubqueryExpr.
// Get the value from v.SubQuery and set it to v.
func (e *Evaluator) subqueryExpr(v *ast.SubqueryExpr) bool {
	if v.Evaluated && !v.Correlated {
		// Subquery do not use outer context should only evaluate once.
		return true
	}
	err := EvalSubquery(e.ctx, v)
	if err != nil {
		e.err = errors.Trace(err)
		return false
	}
	return true
}

// EvalSubquery evaluates a subquery.
func EvalSubquery(ctx context.Context, v *ast.SubqueryExpr) error {
	if v.SubqueryExec != nil {
		rowCount := 2
		if v.MultiRows {
			rowCount = -1
		} else if v.Exists {
			rowCount = 1
		}
		rows, err := v.SubqueryExec.EvalRows(ctx, rowCount)
		if err != nil {
			return errors.Trace(err)
		}
		if v.MultiRows || v.Exists {
			v.GetDatum().SetRow(rows)
			v.Evaluated = true
			return nil
		}
		switch len(rows) {
		case 0:
			v.SetNull()
		case 1:
			v.SetDatum(rows[0])
		default:
			return errors.New("Subquery returns more than 1 row")
		}
	}
	v.Evaluated = true
	return nil
}

func (e *Evaluator) checkInList(not bool, in types.Datum, list []types.Datum) (d types.Datum) {
	hasNull := false
	for _, v := range list {
		if v.IsNull() {
			hasNull = true
			continue
		}

		a, b := types.CoerceDatum(in, v)
		r, err := a.CompareDatum(b)
		if err != nil {
			e.err = errors.Trace(err)
			return d
		}
		if r == 0 {
			if !not {
				d.SetInt64(1)
				return d
			}
			d.SetInt64(0)
			return d
		}
	}

	if hasNull {
		// if no matched but we got null in In, return null
		// e.g 1 in (null, 2, 3) returns null
		return d
	}
	if not {
		d.SetInt64(1)
		return d
	}
	d.SetInt64(0)
	return d
}

func (e *Evaluator) patternIn(n *ast.PatternInExpr) bool {
	lhs := *n.Expr.GetDatum()
	if lhs.IsNull() {
		n.SetNull()
		return true
	}
	if n.Sel == nil {
		ds := make([]types.Datum, 0, len(n.List))
		for _, ei := range n.List {
			ds = append(ds, *ei.GetDatum())
		}
		x := e.checkInList(n.Not, lhs, ds)
		if e.err != nil {
			return false
		}
		n.SetDatum(x)
		return true
	}
	res := n.Sel.GetDatum().GetRow()
	x := e.checkInList(n.Not, lhs, res)
	if e.err != nil {
		return false
	}
	n.SetDatum(x)
	return true
}

func (e *Evaluator) isNull(v *ast.IsNullExpr) bool {
	var boolVal bool
	if v.Expr.GetDatum().IsNull() {
		boolVal = true
	}
	if v.Not {
		boolVal = !boolVal
	}
	v.SetInt64(boolToInt64(boolVal))
	return true
}

func (e *Evaluator) isTruth(v *ast.IsTruthExpr) bool {
	var boolVal bool
	datum := v.Expr.GetDatum()
	if !datum.IsNull() {
		ival, err := datum.ToBool()
		if err != nil {
			e.err = errors.Trace(err)
			return false
		}
		if ival == v.True {
			boolVal = true
		}
	}
	if v.Not {
		boolVal = !boolVal
	}
	v.GetDatum().SetInt64(boolToInt64(boolVal))
	return true
}

func (e *Evaluator) paramMarker(v *ast.ParamMarkerExpr) bool {
	return true
}

func (e *Evaluator) parentheses(v *ast.ParenthesesExpr) bool {
	v.SetDatum(*v.Expr.GetDatum())
	return true
}

func (e *Evaluator) position(v *ast.PositionExpr) bool {
	v.SetDatum(*v.Refer.Expr.GetDatum())
	return true
}

func (e *Evaluator) row(v *ast.RowExpr) bool {
	row := make([]types.Datum, 0, len(v.Values))
	for _, val := range v.Values {
		row = append(row, *val.GetDatum())
	}
	v.GetDatum().SetRow(row)
	return true
}

func (e *Evaluator) unaryOperation(u *ast.UnaryOperationExpr) bool {
	defer func() {
		if er := recover(); er != nil {
			e.err = errors.Errorf("%v", er)
		}
	}()
	aDatum := u.V.GetDatum()
	if aDatum.IsNull() {
		u.SetNull()
		return true
	}
	switch op := u.Op; op {
	case opcode.Not:
		n, err := aDatum.ToBool()
		if err != nil {
			e.err = errors.Trace(err)
		} else if n == 0 {
			u.SetInt64(1)
		} else {
			u.SetInt64(0)
		}
	case opcode.BitNeg:
		// for bit operation, we will use int64 first, then return uint64
		n, err := aDatum.ToInt64()
		if err != nil {
			e.err = errors.Trace(err)
			return false
		}
		u.SetUint64(uint64(^n))
	case opcode.Plus:
		switch aDatum.Kind() {
		case types.KindInt64,
			types.KindUint64,
			types.KindFloat64,
			types.KindFloat32,
			types.KindMysqlDuration,
			types.KindMysqlTime,
			types.KindString,
			types.KindMysqlDecimal,
			types.KindBytes,
			types.KindMysqlHex,
			types.KindMysqlBit,
			types.KindMysqlEnum,
			types.KindMysqlSet:
			u.SetDatum(*aDatum)
		default:
			e.err = ErrInvalidOperation
			return false
		}
	case opcode.Minus:
		switch aDatum.Kind() {
		case types.KindInt64:
			u.SetInt64(-aDatum.GetInt64())
		case types.KindUint64:
			u.SetInt64(-int64(aDatum.GetUint64()))
		case types.KindFloat64:
			u.SetFloat64(-aDatum.GetFloat64())
		case types.KindFloat32:
			u.SetFloat32(-aDatum.GetFloat32())
		case types.KindMysqlDuration:
			u.SetMysqlDecimal(mysql.ZeroDecimal.Sub(aDatum.GetMysqlDuration().ToNumber()))
		case types.KindMysqlTime:
			u.SetMysqlDecimal(mysql.ZeroDecimal.Sub(aDatum.GetMysqlTime().ToNumber()))
		case types.KindString, types.KindBytes:
			f, err := types.StrToFloat(aDatum.GetString())
			e.err = errors.Trace(err)
			u.SetFloat64(-f)
		case types.KindMysqlDecimal:
			f, _ := aDatum.GetMysqlDecimal().Float64()
			u.SetMysqlDecimal(mysql.NewDecimalFromFloat(-f))
		case types.KindMysqlHex:
			u.SetFloat64(-aDatum.GetMysqlHex().ToNumber())
		case types.KindMysqlBit:
			u.SetFloat64(-aDatum.GetMysqlBit().ToNumber())
		case types.KindMysqlEnum:
			u.SetFloat64(-aDatum.GetMysqlEnum().ToNumber())
		case types.KindMysqlSet:
			u.SetFloat64(-aDatum.GetMysqlSet().ToNumber())
		default:
			e.err = ErrInvalidOperation
			return false
		}
	default:
		e.err = ErrInvalidOperation
		return false
	}

	return true
}

func (e *Evaluator) values(v *ast.ValuesExpr) bool {
	v.SetDatum(*v.Column.GetDatum())
	return true
}

func (e *Evaluator) variable(v *ast.VariableExpr) bool {
	name := strings.ToLower(v.Name)
	sessionVars := variable.GetSessionVars(e.ctx)
	globalVars := variable.GetGlobalVarAccessor(e.ctx)
	if !v.IsSystem {
		if v.Value != nil && !v.Value.GetDatum().IsNull() {
			strVal, err := v.Value.GetDatum().ToString()
			if err != nil {
				e.err = errors.Trace(err)
				return false
			}
			sessionVars.Users[name] = strings.ToLower(strVal)
			v.SetString(strVal)
			return true
		}
		// user vars
		if value, ok := sessionVars.Users[name]; ok {
			v.SetString(value)
			return true
		}
		// select null user vars is permitted.
		v.SetNull()
		return true
	}

	sysVar, ok := variable.SysVars[name]
	if !ok {
		// select null sys vars is not permitted
		e.err = variable.UnknownSystemVar.Gen("Unknown system variable '%s'", name)
		return false
	}
	if sysVar.Scope == variable.ScopeNone {
		v.SetString(sysVar.Value)
		return true
	}

	if !v.IsGlobal {
		d := sessionVars.GetSystemVar(name)
		if d.IsNull() {
			if sysVar.Scope&variable.ScopeGlobal == 0 {
				d.SetString(sysVar.Value)
			} else {
				// Get global system variable and fill it in session.
				globalVal, err := globalVars.GetGlobalSysVar(e.ctx, name)
				if err != nil {
					e.err = errors.Trace(err)
					return false
				}
				d.SetString(globalVal)
				err = sessionVars.SetSystemVar(name, d)
				if err != nil {
					e.err = errors.Trace(err)
					return false
				}
			}
		}
		v.SetDatum(d)
		return true
	}
	value, err := globalVars.GetGlobalSysVar(e.ctx, name)
	if err != nil {
		e.err = errors.Trace(err)
		return false
	}

	v.SetString(value)
	return true
}

func (e *Evaluator) funcCall(v *ast.FuncCallExpr) bool {
	f, ok := Funcs[v.FnName.L]
	if !ok {
		e.err = ErrInvalidOperation.Gen("unknown function %s", v.FnName.O)
		return false
	}
	if len(v.Args) < f.MinArgs || (f.MaxArgs != -1 && len(v.Args) > f.MaxArgs) {
		e.err = ErrInvalidOperation.Gen("number of function arguments must in [%d, %d].", f.MinArgs, f.MaxArgs)
		return false
	}
	a := make([]types.Datum, len(v.Args))
	for i, arg := range v.Args {
		a[i] = *arg.GetDatum()
	}
	val, err := f.F(a, e.ctx)
	if err != nil {
		e.err = errors.Trace(err)
		return false
	}
	v.SetDatum(val)
	return true
}

func (e *Evaluator) funcCast(v *ast.FuncCastExpr) bool {
	d := *v.Expr.GetDatum()
	// Casting nil to any type returns null
	if d.IsNull() {
		v.SetNull()
		return true
	}
	var err error
	d, err = d.Cast(v.Tp)
	if err != nil {
		e.err = errors.Trace(err)
		return false
	}
	v.SetDatum(d)
	return true
}

func (e *Evaluator) aggregateFunc(v *ast.AggregateFuncExpr) bool {
	name := strings.ToLower(v.F)
	switch name {
	case ast.AggFuncAvg:
		e.evalAggAvg(v)
	case ast.AggFuncCount:
		e.evalAggCount(v)
	case ast.AggFuncFirstRow, ast.AggFuncMax, ast.AggFuncMin, ast.AggFuncSum:
		e.evalAggSetValue(v)
	case ast.AggFuncGroupConcat:
		e.evalAggGroupConcat(v)
	}
	return e.err == nil
}

func (e *Evaluator) evalAggCount(v *ast.AggregateFuncExpr) {
	ctx := v.GetContext()
	v.SetInt64(ctx.Count)
}

func (e *Evaluator) evalAggSetValue(v *ast.AggregateFuncExpr) {
	ctx := v.GetContext()
	v.SetValue(ctx.Value)
}

func (e *Evaluator) evalAggAvg(v *ast.AggregateFuncExpr) {
	ctx := v.GetContext()
	switch x := ctx.Value.(type) {
	case float64:
		t := x / float64(ctx.Count)
		ctx.Value = t
		v.SetFloat64(t)
	case mysql.Decimal:
		t := x.Div(mysql.NewDecimalFromUint(uint64(ctx.Count), 0))
		ctx.Value = t
		v.SetMysqlDecimal(t)
	}
}

func (e *Evaluator) evalAggGroupConcat(v *ast.AggregateFuncExpr) {
	ctx := v.GetContext()
	if ctx.Buffer != nil {
		v.SetString(ctx.Buffer.String())
	} else {
		v.SetNull()
	}
}
