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
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/opcode"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	driver "github.com/pingcap/tidb/types/parser_driver"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/collate"
)

type simpleRewriter struct {
	exprStack

	schema *Schema
	err    error
	ctx    sessionctx.Context
	names  []*types.FieldName
}

// ParseSimpleExprWithTableInfo parses simple expression string to Expression.
// The expression string must only reference the column in table Info.
func ParseSimpleExprWithTableInfo(ctx sessionctx.Context, exprStr string, tableInfo *model.TableInfo) (Expression, error) {
	exprStr = "select " + exprStr
	var stmts []ast.StmtNode
	var err error
	var warns []error
	if p, ok := ctx.(interface {
		ParseSQL(context.Context, string, string, string) ([]ast.StmtNode, []error, error)
	}); ok {
		stmts, warns, err = p.ParseSQL(context.Background(), exprStr, "", "")
	} else {
		stmts, warns, err = parser.New().Parse(exprStr, "", "")
	}
	for _, warn := range warns {
		ctx.GetSessionVars().StmtCtx.AppendWarning(util.SyntaxWarn(warn))
	}

	if err != nil {
		return nil, errors.Trace(err)
	}
	expr := stmts[0].(*ast.SelectStmt).Fields.Fields[0].Expr
	return RewriteSimpleExprWithTableInfo(ctx, tableInfo, expr)
}

// ParseSimpleExprCastWithTableInfo parses simple expression string to Expression.
// And the expr returns will cast to the target type.
func ParseSimpleExprCastWithTableInfo(ctx sessionctx.Context, exprStr string, tableInfo *model.TableInfo, targetFt *types.FieldType) (Expression, error) {
	e, err := ParseSimpleExprWithTableInfo(ctx, exprStr, tableInfo)
	if err != nil {
		return nil, err
	}
	e = BuildCastFunction(ctx, e, targetFt)
	return e, nil
}

// RewriteSimpleExprWithTableInfo rewrites simple ast.ExprNode to expression.Expression.
func RewriteSimpleExprWithTableInfo(ctx sessionctx.Context, tbl *model.TableInfo, expr ast.ExprNode) (Expression, error) {
	dbName := model.NewCIStr(ctx.GetSessionVars().CurrentDB)
	columns, names := ColumnInfos2ColumnsAndNames(ctx, dbName, tbl.Name, tbl.Columns)
	rewriter := &simpleRewriter{ctx: ctx, schema: NewSchema(columns...), names: names}
	expr.Accept(rewriter)
	if rewriter.err != nil {
		return nil, rewriter.err
	}
	return rewriter.pop(), nil
}

// ParseSimpleExprsWithSchema parses simple expression string to Expression.
// The expression string must only reference the column in the given schema.
func ParseSimpleExprsWithSchema(ctx sessionctx.Context, exprStr string, schema *Schema) ([]Expression, error) {
	exprStr = "select " + exprStr
	stmts, warns, err := parser.New().Parse(exprStr, "", "")
	if err != nil {
		return nil, util.SyntaxWarn(err)
	}
	for _, warn := range warns {
		ctx.GetSessionVars().StmtCtx.AppendWarning(util.SyntaxWarn(warn))
	}

	fields := stmts[0].(*ast.SelectStmt).Fields.Fields
	exprs := make([]Expression, 0, len(fields))
	for _, field := range fields {
		expr, err := RewriteSimpleExprWithSchema(ctx, field.Expr, schema)
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, expr)
	}
	return exprs, nil
}

// ParseSimpleExprsWithNames parses simple expression string to Expression.
// The expression string must only reference the column in the given NameSlice.
func ParseSimpleExprsWithNames(ctx sessionctx.Context, exprStr string, schema *Schema, names types.NameSlice) ([]Expression, error) {
	exprStr = "select " + exprStr
	var stmts []ast.StmtNode
	var err error
	var warns []error
	if p, ok := ctx.(interface {
		ParseSQL(context.Context, string, string, string) ([]ast.StmtNode, []error, error)
	}); ok {
		stmts, warns, err = p.ParseSQL(context.Background(), exprStr, "", "")
	} else {
		stmts, warns, err = parser.New().Parse(exprStr, "", "")
	}
	if err != nil {
		return nil, util.SyntaxWarn(err)
	}
	for _, warn := range warns {
		ctx.GetSessionVars().StmtCtx.AppendWarning(util.SyntaxWarn(warn))
	}

	fields := stmts[0].(*ast.SelectStmt).Fields.Fields
	exprs := make([]Expression, 0, len(fields))
	for _, field := range fields {
		expr, err := RewriteSimpleExprWithNames(ctx, field.Expr, schema, names)
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, expr)
	}
	return exprs, nil
}

// RewriteSimpleExprWithNames rewrites simple ast.ExprNode to expression.Expression.
func RewriteSimpleExprWithNames(ctx sessionctx.Context, expr ast.ExprNode, schema *Schema, names []*types.FieldName) (Expression, error) {
	rewriter := &simpleRewriter{ctx: ctx, schema: schema, names: names}
	expr.Accept(rewriter)
	if rewriter.err != nil {
		return nil, rewriter.err
	}
	return rewriter.pop(), nil
}

// RewriteSimpleExprWithSchema rewrites simple ast.ExprNode to expression.Expression.
func RewriteSimpleExprWithSchema(ctx sessionctx.Context, expr ast.ExprNode, schema *Schema) (Expression, error) {
	rewriter := &simpleRewriter{ctx: ctx, schema: schema}
	expr.Accept(rewriter)
	if rewriter.err != nil {
		return nil, rewriter.err
	}
	return rewriter.pop(), nil
}

// FindFieldName finds the column name from NameSlice.
func FindFieldName(names types.NameSlice, astCol *ast.ColumnName) (int, error) {
	dbName, tblName, colName := astCol.Schema, astCol.Table, astCol.Name
	idx := -1
	for i, name := range names {
		if (dbName.L == "" || dbName.L == name.DBName.L) &&
			(tblName.L == "" || tblName.L == name.TblName.L) &&
			(colName.L == name.ColName.L) {
			if idx == -1 {
				idx = i
			} else {
				return -1, errNonUniq.GenWithStackByArgs(name.String(), "field list")
			}
		}
	}
	return idx, nil
}

// FindFieldNameIdxByColName finds the index of corresponding name in the given slice. -1 for not found.
func FindFieldNameIdxByColName(names []*types.FieldName, colName string) int {
	for i, name := range names {
		if name.ColName.L == colName {
			return i
		}
	}
	return -1
}

func (sr *simpleRewriter) rewriteColumn(nodeColName *ast.ColumnNameExpr) (*Column, error) {
	idx, err := FindFieldName(sr.names, nodeColName.Name)
	if idx >= 0 && err == nil {
		return sr.schema.Columns[idx], nil
	}
	return nil, errBadField.GenWithStackByArgs(nodeColName.Name.Name.O, "expression")
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
		sr.push(column)
	case *driver.ValueExpr:
		value := &Constant{Value: v.Datum, RetType: &v.Type}
		sr.push(value)
	case *ast.FuncCallExpr:
		sr.funcCallToExpression(v)
	case *ast.FuncCastExpr:
		arg := sr.pop()
		sr.err = CheckArgsNotMultiColumnRow(arg)
		if sr.err != nil {
			return retNode, false
		}
		sr.push(BuildCastFunction(sr.ctx, arg, v.Tp))
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
	case *driver.ParamMarkerExpr:
		var value Expression
		value, sr.err = ParamMarkerExpression(sr.ctx, v)
		if sr.err != nil {
			return retNode, false
		}
		sr.push(value)
	case *ast.RowExpr:
		sr.rowToScalarFunc(v)
	case *ast.ParenthesesExpr:
	case *ast.ColumnName:
	// TODO: Perhaps we don't need to transcode these back to generic integers/strings
	case *ast.TrimDirectionExpr:
		sr.push(&Constant{
			Value:   types.NewIntDatum(int64(v.Direction)),
			RetType: types.NewFieldType(mysql.TypeTiny),
		})
	case *ast.TimeUnitExpr:
		sr.push(&Constant{
			Value:   types.NewStringDatum(v.Unit.String()),
			RetType: types.NewFieldType(mysql.TypeVarchar),
		})
	case *ast.GetFormatSelectorExpr:
		sr.push(&Constant{
			Value:   types.NewStringDatum(v.Selector.String()),
			RetType: types.NewFieldType(mysql.TypeVarchar),
		})
	case *ast.SetCollationExpr:
		arg := sr.stack[len(sr.stack)-1]
		if collate.NewCollationEnabled() {
			var collInfo *charset.Collation
			// TODO(bb7133): use charset.ValidCharsetAndCollation when its bug is fixed.
			if collInfo, sr.err = collate.GetCollationByName(v.Collate); sr.err != nil {
				break
			}
			chs := arg.GetType().Charset
			if chs != "" && collInfo.CharsetName != chs {
				sr.err = charset.ErrCollationCharsetMismatch.GenWithStackByArgs(collInfo.Name, chs)
				break
			}
		}
		// SetCollationExpr sets the collation explicitly, even when the evaluation type of the expression is non-string.
		if _, ok := arg.(*Column); ok {
			// Wrap a cast here to avoid changing the original FieldType of the column expression.
			exprType := arg.GetType().Clone()
			exprType.Collate = v.Collate
			casted := BuildCastFunction(sr.ctx, arg, exprType)
			sr.pop()
			sr.push(casted)
		} else {
			// For constant and scalar function, we can set its collate directly.
			arg.GetType().Collate = v.Collate
		}
		sr.stack[len(sr.stack)-1].SetCoercibility(CoercibilityExplicit)
	default:
		sr.err = errors.Errorf("UnknownType: %T", v)
		return retNode, false
	}
	if sr.err != nil {
		return retNode, false
	}
	return originInNode, true
}

func (sr *simpleRewriter) useCache() bool {
	return sr.ctx.GetSessionVars().StmtCtx.UseCache
}

func (sr *simpleRewriter) binaryOpToExpression(v *ast.BinaryOperationExpr) {
	right := sr.pop()
	left := sr.pop()
	var function Expression
	switch v.Op {
	case opcode.EQ, opcode.NE, opcode.NullEQ, opcode.GT, opcode.GE, opcode.LT, opcode.LE:
		function, sr.err = sr.constructBinaryOpFunction(left, right,
			v.Op.String())
	default:
		lLen := GetRowLen(left)
		rLen := GetRowLen(right)
		if lLen != 1 || rLen != 1 {
			sr.err = ErrOperandColumns.GenWithStackByArgs(1)
			return
		}
		function, sr.err = NewFunction(sr.ctx, v.Op.String(), types.NewFieldType(mysql.TypeUnspecified), left, right)
	}
	if sr.err != nil {
		return
	}
	sr.push(function)
}

func (sr *simpleRewriter) funcCallToExpression(v *ast.FuncCallExpr) {
	args := sr.popN(len(v.Args))
	sr.err = CheckArgsNotMultiColumnRow(args...)
	if sr.err != nil {
		return
	}
	if sr.rewriteFuncCall(v, args) {
		return
	}
	var function Expression
	function, sr.err = NewFunction(sr.ctx, v.FnName.L, &v.Type, args...)
	sr.push(function)
}

func (sr *simpleRewriter) rewriteFuncCall(v *ast.FuncCallExpr, args []Expression) bool {
	switch v.FnName.L {
	case ast.Nullif:
		if len(args) != 2 {
			sr.err = ErrIncorrectParameterCount.GenWithStackByArgs(v.FnName.O)
			return true
		}
		param2 := args[1]
		param1 := args[0]
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
		sr.push(funcIf)
		return true
	default:
		return false
	}
}

// constructBinaryOpFunction works as following:
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
		return nil, ErrOperandColumns.GenWithStackByArgs(lLen)
	}
	switch op {
	case ast.EQ, ast.NE, ast.NullEQ:
		funcs := make([]Expression, lLen)
		for i := 0; i < lLen; i++ {
			var err error
			funcs[i], err = sr.constructBinaryOpFunction(GetFuncArg(l, i), GetFuncArg(r, i), op)
			if err != nil {
				return nil, err
			}
		}
		if op == ast.NE {
			return ComposeDNFCondition(sr.ctx, funcs...), nil
		}
		return ComposeCNFCondition(sr.ctx, funcs...), nil
	default:
		larg0, rarg0 := GetFuncArg(l, 0), GetFuncArg(r, 0)
		var expr1, expr2, expr3 Expression
		if op == ast.LE || op == ast.GE {
			expr1 = NewFunctionInternal(sr.ctx, op, types.NewFieldType(mysql.TypeTiny), larg0, rarg0)
			expr1 = NewFunctionInternal(sr.ctx, ast.EQ, types.NewFieldType(mysql.TypeTiny), expr1, NewZero())
			expr2 = NewZero()
		} else if op == ast.LT || op == ast.GT {
			expr1 = NewFunctionInternal(sr.ctx, ast.NE, types.NewFieldType(mysql.TypeTiny), larg0, rarg0)
			expr2 = NewFunctionInternal(sr.ctx, op, types.NewFieldType(mysql.TypeTiny), larg0, rarg0)
		}
		var err error
		l, err = PopRowFirstArg(sr.ctx, l)
		if err != nil {
			return nil, err
		}
		r, err = PopRowFirstArg(sr.ctx, r)
		if err != nil {
			return nil, err
		}
		expr3, err = sr.constructBinaryOpFunction(l, r, op)
		if err != nil {
			return nil, err
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
	expr := sr.pop()
	if GetRowLen(expr) != 1 {
		sr.err = ErrOperandColumns.GenWithStackByArgs(1)
		return
	}
	newExpr, err := NewFunction(sr.ctx, op, &v.Type, expr)
	sr.err = err
	sr.push(newExpr)
}

func (sr *simpleRewriter) likeToScalarFunc(v *ast.PatternLikeExpr) {
	pattern := sr.pop()
	expr := sr.pop()
	sr.err = CheckArgsNotMultiColumnRow(expr, pattern)
	if sr.err != nil {
		return
	}
	escapeTp := &types.FieldType{}
	char, col := sr.ctx.GetSessionVars().GetCharsetInfo()
	types.DefaultTypeForValue(int(v.Escape), escapeTp, char, col)
	function := sr.notToExpression(v.Not, ast.Like, &v.Type,
		expr, pattern, &Constant{Value: types.NewIntDatum(int64(v.Escape)), RetType: escapeTp})
	sr.push(function)
}

func (sr *simpleRewriter) regexpToScalarFunc(v *ast.PatternRegexpExpr) {
	parttern := sr.pop()
	expr := sr.pop()
	sr.err = CheckArgsNotMultiColumnRow(expr, parttern)
	if sr.err != nil {
		return
	}
	function := sr.notToExpression(v.Not, ast.Regexp, &v.Type, expr, parttern)
	sr.push(function)
}

func (sr *simpleRewriter) rowToScalarFunc(v *ast.RowExpr) {
	elems := sr.popN(len(v.Values))
	function, err := NewFunction(sr.ctx, ast.RowFunc, elems[0].GetType(), elems...)
	if err != nil {
		sr.err = err
		return
	}
	sr.push(function)
}

func (sr *simpleRewriter) betweenToExpression(v *ast.BetweenExpr) {
	right := sr.pop()
	left := sr.pop()
	expr := sr.pop()
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
		return
	}
	function, err := NewFunction(sr.ctx, ast.LogicAnd, &v.Type, l, r)
	if err != nil {
		sr.err = err
		return
	}
	if v.Not {
		function, err = NewFunction(sr.ctx, ast.UnaryNot, &v.Type, function)
		if err != nil {
			sr.err = err
			return
		}
	}
	sr.push(function)
}

func (sr *simpleRewriter) isNullToExpression(v *ast.IsNullExpr) {
	arg := sr.pop()
	if GetRowLen(arg) != 1 {
		sr.err = ErrOperandColumns.GenWithStackByArgs(1)
		return
	}
	function := sr.notToExpression(v.Not, ast.IsNull, &v.Type, arg)
	sr.push(function)
}

func (sr *simpleRewriter) notToExpression(hasNot bool, op string, tp *types.FieldType,
	args ...Expression) Expression {
	opFunc, err := NewFunction(sr.ctx, op, tp, args...)
	if err != nil {
		sr.err = err
		return nil
	}
	if !hasNot {
		return opFunc
	}

	opFunc, err = NewFunction(sr.ctx, ast.UnaryNot, tp, opFunc)
	if err != nil {
		sr.err = err
		return nil
	}
	return opFunc
}

func (sr *simpleRewriter) isTrueToScalarFunc(v *ast.IsTruthExpr) {
	arg := sr.pop()
	op := ast.IsTruth
	if v.True == 0 {
		op = ast.IsFalsity
	}
	if GetRowLen(arg) != 1 {
		sr.err = ErrOperandColumns.GenWithStackByArgs(1)
		return
	}
	function := sr.notToExpression(v.Not, op, &v.Type, arg)
	sr.push(function)
}

// inToExpression converts in expression to a scalar function. The argument lLen means the length of in list.
// The argument not means if the expression is not in. The tp stands for the expression type, which is always bool.
// a in (b, c, d) will be rewritten as `(a = b) or (a = c) or (a = d)`.
func (sr *simpleRewriter) inToExpression(lLen int, not bool, tp *types.FieldType) {
	exprs := sr.popN(lLen + 1)
	leftExpr := exprs[0]
	elems := exprs[1:]
	l, leftFt := GetRowLen(leftExpr), leftExpr.GetType()
	for i := 0; i < lLen; i++ {
		if l != GetRowLen(elems[i]) {
			sr.err = ErrOperandColumns.GenWithStackByArgs(l)
			return
		}
	}
	leftIsNull := leftFt.Tp == mysql.TypeNull
	if leftIsNull {
		sr.push(NewNull())
		return
	}
	leftEt := leftFt.EvalType()

	if leftEt == types.ETInt {
		for i := 0; i < len(elems); i++ {
			if c, ok := elems[i].(*Constant); ok {
				var isExceptional bool
				elems[i], isExceptional = RefineComparedConstant(sr.ctx, *leftFt, c, opcode.EQ)
				if isExceptional {
					elems[i] = c
				}
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
	sr.push(function)
}
