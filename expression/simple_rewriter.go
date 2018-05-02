// Copyright 2018 PingCAP, Inc.
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
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
)

type simpleRewriter struct {
	tbl *model.TableInfo

	stack *ExprStack
	err   error
	ctx   sessionctx.Context
}

// ParseSimpleExpr parses simple expression string to Expression.
// The expression string must only reference the column in table Info.
func ParseSimpleExpr(ctx sessionctx.Context, exprStr string, tableInfo *model.TableInfo) (Expression, error) {
	exprStr = fmt.Sprintf("select %s", exprStr)
	stmts, err := parser.New().Parse(exprStr, "", "")
	if err != nil {
		return nil, errors.Trace(err)
	}
	expr := stmts[0].(*ast.SelectStmt).Fields.Fields[0].Expr
	rewriter := &simpleRewriter{tbl: tableInfo, ctx: ctx, stack: new(ExprStack)}
	expr.Accept(rewriter)
	if rewriter.err != nil {
		return nil, errors.Trace(rewriter.err)
	}
	return rewriter.stack.Pop(), nil
}

func (sr *simpleRewriter) rewriteColumn(cn *ast.ColumnNameExpr) (*Column, error) {
	cols := sr.tbl.Columns
	for i, col := range cols {
		if col.Name.L == cn.Name.Name.L {
			eCol := &Column{
				FromID:      1,
				ColName:     col.Name,
				OrigTblName: sr.tbl.Name,
				DBName:      model.NewCIStr(sr.ctx.GetSessionVars().CurrentDB),
				TblName:     sr.tbl.Name,
				RetType:     &col.FieldType,
				ID:          col.ID,
				Position:    col.Offset,
				Index:       i,
			}
			return eCol, nil
		}
	}
	return nil, errBadField.Gen(cn.Name.Name.O, "expression")
}

func (sr *simpleRewriter) Enter(inNode ast.Node) (ast.Node, bool) {
	return inNode, false
}

func (sr *simpleRewriter) Leave(originInNode ast.Node) (retNode ast.Node, ok bool) {
	switch v := originInNode.(type) {
	case *ast.ColumnNameExpr:
		column, err := sr.rewriteColumn(v)
		if err != nil {
			sr.err = err
			return originInNode, false
		}
		sr.stack.Push(column)
	case *ast.ValueExpr:
		value := &Constant{Value: v.Datum, RetType: &v.Type}
		sr.stack.Push(value)
	case *ast.FuncCallExpr:
		sr.funcCallToExpression(v)
	case *ast.FuncCastExpr:
		arg := sr.stack.Pop()
		sr.err = CheckArgsNotMultiColumnRow(arg)
		if sr.err != nil {
			return retNode, false
		}
		sr.stack.Push(BuildCastFunction(sr.ctx, arg, v.Tp))
	case *ast.BinaryOperationExpr:
		sr.binaryOpToExpression(v)
	case *ast.UnaryOperationExpr:
		sr.unaryOpToExpression(v)
	case *ast.BetweenExpr:
		sr.betweenToExpression(v)
	case *ast.IsNullExpr:
		sr.isNullToExpression(v)
	case *ast.IsTruthExpr:
		sr.isTrueToScalarFunc(v)
	case *ast.PatternLikeExpr:
		sr.likeToScalarFunc(v)
	case *ast.PatternRegexpExpr:
		sr.regexpToScalarFunc(v)
	case *ast.PatternInExpr:
		if v.Sel == nil {
			sr.inToExpression(len(v.List), v.Not, &v.Type)
		}
	case *ast.RowExpr:
		sr.rowToScalarFunc(v)
	case *ast.ParenthesesExpr:
	case *ast.ColumnName:
	default:
		sr.err = errors.Errorf("UnknownType: %T", v)
		return retNode, false
	}
	if sr.err != nil {
		return retNode, false
	}
	return originInNode, true
}

func (sr *simpleRewriter) binaryOpToExpression(v *ast.BinaryOperationExpr) {
	right := sr.stack.Pop()
	left := sr.stack.Pop()
	var function Expression
	switch v.Op {
	case opcode.EQ, opcode.NE, opcode.NullEQ, opcode.GT, opcode.GE, opcode.LT, opcode.LE:
		function, sr.err = sr.constructBinaryOpFunction(left, right,
			v.Op.String())
	default:
		lLen := GetRowLen(left)
		rLen := GetRowLen(right)
		if lLen != 1 || rLen != 1 {
			sr.err = ErrOperandColumns.GenByArgs(1)
			return
		}
		function, sr.err = NewFunction(sr.ctx, v.Op.String(), types.NewFieldType(mysql.TypeUnspecified), left, right)
	}
	if sr.err != nil {
		sr.err = errors.Trace(sr.err)
		return
	}
	sr.stack.Push(function)
}

func (sr *simpleRewriter) funcCallToExpression(v *ast.FuncCallExpr) {
	args := sr.stack.PopN(len(v.Args))
	sr.err = CheckArgsNotMultiColumnRow(args...)
	if sr.err != nil {
		return
	}
	if sr.rewriteFuncCall(v) {
		return
	}
	var function Expression
	function, sr.err = NewFunction(sr.ctx, v.FnName.L, &v.Type, args...)
	sr.stack.Push(function)
}

func (sr *simpleRewriter) rewriteFuncCall(v *ast.FuncCallExpr) bool {
	switch v.FnName.L {
	case ast.Nullif:
		if len(v.Args) != 2 {
			sr.err = ErrIncorrectParameterCount.GenByArgs(v.FnName.O)
			return true
		}
		param2 := sr.stack.Pop()
		param1 := sr.stack.Pop()
		// param1 = param2
		funcCompare, err := sr.constructBinaryOpFunction(param1, param2, ast.EQ)
		if err != nil {
			sr.err = err
			return true
		}
		// NULL
		nullTp := types.NewFieldType(mysql.TypeNull)
		nullTp.Flen, nullTp.Decimal = mysql.GetDefaultFieldLengthAndDecimal(mysql.TypeNull)
		paramNull := &Constant{
			Value:   types.NewDatum(nil),
			RetType: nullTp,
		}
		// if(param1 = param2, NULL, param1)
		funcIf, err := NewFunction(sr.ctx, ast.If, &v.Type, funcCompare, paramNull, param1)
		if err != nil {
			sr.err = err
			return true
		}
		sr.stack.Push(funcIf)
		return true
	default:
		return false
	}
}

// 1. If op are EQ or NE or NullEQ, constructBinaryOpFunctions converts (a0,a1,a2) op (b0,b1,b2) to (a0 op b0) and (a1 op b1) and (a2 op b2)
// 2. If op are LE or GE, constructBinaryOpFunctions converts (a0,a1,a2) op (b0,b1,b2) to
// `IF( (a0 op b0) EQ 0, 0,
//      IF ( (a1 op b1) EQ 0, 0, a2 op b2))`
// 3. If op are LT or GT, constructBinaryOpFunctions converts (a0,a1,a2) op (b0,b1,b2) to
// `IF( a0 NE b0, a0 op b0,
//      IF( a1 NE b1,
//          a1 op b1,
//          a2 op b2)
// )`
func (sr *simpleRewriter) constructBinaryOpFunction(l Expression, r Expression, op string) (Expression, error) {
	lLen, rLen := GetRowLen(l), GetRowLen(r)
	if lLen == 1 && rLen == 1 {
		return NewFunction(sr.ctx, op, types.NewFieldType(mysql.TypeTiny), l, r)
	} else if rLen != lLen {
		return nil, ErrOperandColumns.GenByArgs(lLen)
	}
	switch op {
	case ast.EQ, ast.NE, ast.NullEQ:
		funcs := make([]Expression, lLen)
		for i := 0; i < lLen; i++ {
			var err error
			funcs[i], err = sr.constructBinaryOpFunction(GetFuncArg(l, i), GetFuncArg(r, i), op)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		return ComposeCNFCondition(sr.ctx, funcs...), nil
	default:
		larg0, rarg0 := GetFuncArg(l, 0), GetFuncArg(r, 0)
		var expr1, expr2, expr3 Expression
		if op == ast.LE || op == ast.GE {
			expr1 = NewFunctionInternal(sr.ctx, op, types.NewFieldType(mysql.TypeTiny), larg0, rarg0)
			expr1 = NewFunctionInternal(sr.ctx, ast.EQ, types.NewFieldType(mysql.TypeTiny), expr1, Zero)
			expr2 = Zero
		} else if op == ast.LT || op == ast.GT {
			expr1 = NewFunctionInternal(sr.ctx, ast.NE, types.NewFieldType(mysql.TypeTiny), larg0, rarg0)
			expr2 = NewFunctionInternal(sr.ctx, op, types.NewFieldType(mysql.TypeTiny), larg0, rarg0)
		}
		var err error
		l, err = PopRowFirstArg(sr.ctx, l)
		if err != nil {
			return nil, errors.Trace(err)
		}
		r, err = PopRowFirstArg(sr.ctx, r)
		if err != nil {
			return nil, errors.Trace(err)
		}
		expr3, err = sr.constructBinaryOpFunction(l, r, op)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return NewFunction(sr.ctx, ast.If, types.NewFieldType(mysql.TypeTiny), expr1, expr2, expr3)
	}
}

func (sr *simpleRewriter) unaryOpToExpression(v *ast.UnaryOperationExpr) {
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
		sr.err = errors.Errorf("Unknown Unary Op %T", v.Op)
		return
	}
	expr := sr.stack.Pop()
	if GetRowLen(expr) != 1 {
		sr.err = ErrOperandColumns.GenByArgs(1)
		return
	}
	newExpr, err := NewFunction(sr.ctx, op, &v.Type, expr)
	sr.err = err
	sr.stack.Push(newExpr)
}

func (sr *simpleRewriter) likeToScalarFunc(v *ast.PatternLikeExpr) {
	pattern := sr.stack.Pop()
	expr := sr.stack.Pop()
	sr.err = CheckArgsNotMultiColumnRow(expr, pattern)
	if sr.err != nil {
		return
	}
	escapeTp := &types.FieldType{}
	types.DefaultTypeForValue(int(v.Escape), escapeTp)
	function := sr.notToExpression(v.Not, ast.Like, &v.Type,
		expr, pattern, &Constant{Value: types.NewIntDatum(int64(v.Escape)), RetType: escapeTp})
	sr.stack.Push(function)
}

func (sr *simpleRewriter) regexpToScalarFunc(v *ast.PatternRegexpExpr) {
	parttern := sr.stack.Pop()
	expr := sr.stack.Pop()
	sr.err = CheckArgsNotMultiColumnRow(expr, parttern)
	if sr.err != nil {
		return
	}
	function := sr.notToExpression(v.Not, ast.Regexp, &v.Type, expr, parttern)
	sr.stack.Push(function)
}

func (sr *simpleRewriter) rowToScalarFunc(v *ast.RowExpr) {
	elems := sr.stack.PopN(len(v.Values))
	function, err := NewFunction(sr.ctx, ast.RowFunc, elems[0].GetType(), elems...)
	if err != nil {
		sr.err = errors.Trace(err)
		return
	}
	sr.stack.Push(function)
}

func (sr *simpleRewriter) betweenToExpression(v *ast.BetweenExpr) {
	right := sr.stack.Pop()
	left := sr.stack.Pop()
	expr := sr.stack.Pop()
	sr.err = CheckArgsNotMultiColumnRow(expr)
	if sr.err != nil {
		return
	}
	var l, r Expression
	l, sr.err = NewFunction(sr.ctx, ast.GE, &v.Type, expr, left)
	if sr.err == nil {
		r, sr.err = NewFunction(sr.ctx, ast.LE, &v.Type, expr, right)
	}
	if sr.err != nil {
		sr.err = errors.Trace(sr.err)
		return
	}
	function, err := NewFunction(sr.ctx, ast.LogicAnd, &v.Type, l, r)
	if err != nil {
		sr.err = errors.Trace(err)
		return
	}
	if v.Not {
		function, err = NewFunction(sr.ctx, ast.UnaryNot, &v.Type, function)
		if err != nil {
			sr.err = errors.Trace(err)
			return
		}
	}
	sr.stack.Push(function)
}

func (sr *simpleRewriter) isNullToExpression(v *ast.IsNullExpr) {
	arg := sr.stack.Pop()
	if GetRowLen(arg) != 1 {
		sr.err = ErrOperandColumns.GenByArgs(1)
		return
	}
	function := sr.notToExpression(v.Not, ast.IsNull, &v.Type, arg)
	sr.stack.Push(function)
}

func (sr *simpleRewriter) notToExpression(hasNot bool, op string, tp *types.FieldType,
	args ...Expression) Expression {
	opFunc, err := NewFunction(sr.ctx, op, tp, args...)
	if err != nil {
		sr.err = errors.Trace(err)
		return nil
	}
	if !hasNot {
		return opFunc
	}

	opFunc, err = NewFunction(sr.ctx, ast.UnaryNot, tp, opFunc)
	if err != nil {
		sr.err = errors.Trace(err)
		return nil
	}
	return opFunc
}

func (sr *simpleRewriter) isTrueToScalarFunc(v *ast.IsTruthExpr) {
	arg := sr.stack.Pop()
	op := ast.IsTruth
	if v.True == 0 {
		op = ast.IsFalsity
	}
	if GetRowLen(arg) != 1 {
		sr.err = ErrOperandColumns.GenByArgs(1)
		return
	}
	function := sr.notToExpression(v.Not, op, &v.Type, arg)
	sr.stack.Push(function)
}

// inToExpression converts in expression to a scalar function. The argument lLen means the length of in list.
// The argument not means if the expression is not in. The tp stands for the expression type, which is always bool.
// a in (b, c, d) will be rewritten as `(a = b) or (a = c) or (a = d)`.
func (sr *simpleRewriter) inToExpression(lLen int, not bool, tp *types.FieldType) {
	exprs := sr.stack.PopN(lLen + 1)
	leftExpr := exprs[0]
	elems := exprs[1:]
	l := GetRowLen(leftExpr)
	for i := 0; i < lLen; i++ {
		if l != GetRowLen(elems[i]) {
			sr.err = ErrOperandColumns.GenByArgs(l)
			return
		}
	}
	leftIsNull := leftExpr.GetType().Tp == mysql.TypeNull
	if leftIsNull {
		sr.stack.Push(Null.Clone())
		return
	}
	leftEt := leftExpr.GetType().EvalType()
	if leftEt == types.ETInt {
		for i := 0; i < len(elems); i++ {
			if c, ok := elems[i].(*Constant); ok {
				elems[i] = RefineConstantArg(sr.ctx, c, opcode.EQ)
			}
		}
	}
	allSameType := true
	for _, elem := range elems {
		if elem.GetType().Tp != mysql.TypeNull && GetAccurateCmpType(leftExpr, elem) != leftEt {
			allSameType = false
			break
		}
	}
	var function Expression
	if allSameType && l == 1 {
		function = sr.notToExpression(not, ast.In, tp, exprs...)
	} else {
		eqFunctions := make([]Expression, 0, lLen)
		for i := 0; i < len(elems); i++ {
			expr, err := sr.constructBinaryOpFunction(leftExpr, elems[i], ast.EQ)
			if err != nil {
				sr.err = err
				return
			}
			eqFunctions = append(eqFunctions, expr)
		}
		function = ComposeDNFCondition(sr.ctx, eqFunctions...)
		if not {
			var err error
			function, err = NewFunction(sr.ctx, ast.UnaryNot, tp, function)
			if err != nil {
				sr.err = err
				return
			}
		}
	}
	sr.stack.Push(function)
}
