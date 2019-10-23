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
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/opcode"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/types/parser_driver"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
	"golang.org/x/tools/container/intsets"
)

// cowExprRef is a copy-on-write slice ref util using in `ColumnSubstitute`
// to reduce unnecessary allocation for Expression arguments array
type cowExprRef struct {
	ref []Expression
	new []Expression
}

// Set will allocate new array if changed flag true
func (c *cowExprRef) Set(i int, changed bool, val Expression) {
	if c.new != nil {
		c.new[i] = val
		return
	}
	if !changed {
		return
	}
	c.new = make([]Expression, len(c.ref))
	copy(c.new, c.ref[:i])
	c.new[i] = val
}

// Result return the final reference
func (c *cowExprRef) Result() []Expression {
	if c.new != nil {
		return c.new
	}
	return c.ref
}

// Filter the input expressions, append the results to result.
func Filter(result []Expression, input []Expression, filter func(Expression) bool) []Expression {
	for _, e := range input {
		if filter(e) {
			result = append(result, e)
		}
	}
	return result
}

// FilterOutInPlace do the filtering out in place.
// The remained are the ones who doesn't match the filter, storing in the original slice.
// The filteredOut are the ones match the filter, storing in a new slice.
func FilterOutInPlace(input []Expression, filter func(Expression) bool) (remained, filteredOut []Expression) {
	for i := len(input) - 1; i >= 0; i-- {
		if filter(input[i]) {
			filteredOut = append(filteredOut, input[i])
			input = append(input[:i], input[i+1:]...)
		}
	}
	return input, filteredOut
}

// ExtractColumns extracts all columns from an expression.
func ExtractColumns(expr Expression) (cols []*Column) {
	// Pre-allocate a slice to reduce allocation, 8 doesn't have special meaning.
	result := make([]*Column, 0, 8)
	return extractColumns(result, expr, nil)
}

// ExtractCorColumns extracts correlated column from given expression.
func ExtractCorColumns(expr Expression) (cols []*CorrelatedColumn) {
	switch v := expr.(type) {
	case *CorrelatedColumn:
		return []*CorrelatedColumn{v}
	case *ScalarFunction:
		for _, arg := range v.GetArgs() {
			cols = append(cols, ExtractCorColumns(arg)...)
		}
	}
	return
}

// ExtractColumnsFromExpressions is a more efficient version of ExtractColumns for batch operation.
// filter can be nil, or a function to filter the result column.
// It's often observed that the pattern of the caller like this:
//
// cols := ExtractColumns(...)
// for _, col := range cols {
//     if xxx(col) {...}
// }
//
// Provide an additional filter argument, this can be done in one step.
// To avoid allocation for cols that not need.
func ExtractColumnsFromExpressions(result []*Column, exprs []Expression, filter func(*Column) bool) []*Column {
	for _, expr := range exprs {
		result = extractColumns(result, expr, filter)
	}
	return result
}

func extractColumns(result []*Column, expr Expression, filter func(*Column) bool) []*Column {
	switch v := expr.(type) {
	case *Column:
		if filter == nil || filter(v) {
			result = append(result, v)
		}
	case *ScalarFunction:
		for _, arg := range v.GetArgs() {
			result = extractColumns(result, arg, filter)
		}
	}
	return result
}

// ExtractColumnSet extracts the different values of `UniqueId` for columns in expressions.
func ExtractColumnSet(exprs []Expression) *intsets.Sparse {
	set := &intsets.Sparse{}
	for _, expr := range exprs {
		extractColumnSet(expr, set)
	}
	return set
}

func extractColumnSet(expr Expression, set *intsets.Sparse) {
	switch v := expr.(type) {
	case *Column:
		set.Insert(int(v.UniqueID))
	case *ScalarFunction:
		for _, arg := range v.GetArgs() {
			extractColumnSet(arg, set)
		}
	}
}

func setExprColumnInOperand(expr Expression) Expression {
	switch v := expr.(type) {
	case *Column:
		col := v.Clone().(*Column)
		col.InOperand = true
		return col
	case *ScalarFunction:
		args := v.GetArgs()
		for i, arg := range args {
			args[i] = setExprColumnInOperand(arg)
		}
	}
	return expr
}

// ColumnSubstitute substitutes the columns in filter to expressions in select fields.
// e.g. select * from (select b as a from t) k where a < 10 => select * from (select b as a from t where b < 10) k.
func ColumnSubstitute(expr Expression, schema *Schema, newExprs []Expression) Expression {
	_, resExpr := ColumnSubstituteImpl(expr, schema, newExprs)
	return resExpr
}

// ColumnSubstituteImpl tries to substitute column expr using newExprs,
// the newFunctionInternal is only called if its child is substituted
func ColumnSubstituteImpl(expr Expression, schema *Schema, newExprs []Expression) (bool, Expression) {
	switch v := expr.(type) {
	case *Column:
		id := schema.ColumnIndex(v)
		if id == -1 {
			return false, v
		}
		newExpr := newExprs[id]
		if v.InOperand {
			newExpr = setExprColumnInOperand(newExpr)
		}
		return true, newExpr
	case *ScalarFunction:
		if v.FuncName.L == ast.Cast {
			newFunc := v.Clone().(*ScalarFunction)
			_, newFunc.GetArgs()[0] = ColumnSubstituteImpl(newFunc.GetArgs()[0], schema, newExprs)
			return true, newFunc
		}
		// cowExprRef is a copy-on-write util, args array allocation happens only
		// when expr in args is changed
		refExprArr := cowExprRef{v.GetArgs(), nil}
		substituted := false
		for idx, arg := range v.GetArgs() {
			changed, newFuncExpr := ColumnSubstituteImpl(arg, schema, newExprs)
			refExprArr.Set(idx, changed, newFuncExpr)
			if changed {
				substituted = true
			}
		}
		if substituted {
			return true, NewFunctionInternal(v.GetCtx(), v.FuncName.L, v.RetType, refExprArr.Result()...)
		}
	}
	return false, expr
}

// getValidPrefix gets a prefix of string which can parsed to a number with base. the minimum base is 2 and the maximum is 36.
func getValidPrefix(s string, base int64) string {
	var (
		validLen int
		upper    rune
	)
	switch {
	case base >= 2 && base <= 9:
		upper = rune('0' + base)
	case base <= 36:
		upper = rune('A' + base - 10)
	default:
		return ""
	}
Loop:
	for i := 0; i < len(s); i++ {
		c := rune(s[i])
		switch {
		case unicode.IsDigit(c) || unicode.IsLower(c) || unicode.IsUpper(c):
			c = unicode.ToUpper(c)
			if c < upper {
				validLen = i + 1
			} else {
				break Loop
			}
		case c == '+' || c == '-':
			if i != 0 {
				break Loop
			}
		default:
			break Loop
		}
	}
	if validLen > 1 && s[0] == '+' {
		return s[1:validLen]
	}
	return s[:validLen]
}

// SubstituteCorCol2Constant will substitute correlated column to constant value which it contains.
// If the args of one scalar function are all constant, we will substitute it to constant.
func SubstituteCorCol2Constant(expr Expression) (Expression, error) {
	switch x := expr.(type) {
	case *ScalarFunction:
		allConstant := true
		newArgs := make([]Expression, 0, len(x.GetArgs()))
		for _, arg := range x.GetArgs() {
			newArg, err := SubstituteCorCol2Constant(arg)
			if err != nil {
				return nil, err
			}
			_, ok := newArg.(*Constant)
			newArgs = append(newArgs, newArg)
			allConstant = allConstant && ok
		}
		if allConstant {
			val, err := x.Eval(chunk.Row{})
			if err != nil {
				return nil, err
			}
			return &Constant{Value: val, RetType: x.GetType()}, nil
		}
		var newSf Expression
		if x.FuncName.L == ast.Cast {
			newSf = BuildCastFunction(x.GetCtx(), newArgs[0], x.RetType)
		} else {
			newSf = NewFunctionInternal(x.GetCtx(), x.FuncName.L, x.GetType(), newArgs...)
		}
		return newSf, nil
	case *CorrelatedColumn:
		return &Constant{Value: *x.Data, RetType: x.GetType()}, nil
	case *Constant:
		if x.DeferredExpr != nil {
			newExpr := FoldConstant(x)
			return &Constant{Value: newExpr.(*Constant).Value, RetType: x.GetType()}, nil
		}
	}
	return expr, nil
}

// timeZone2Duration converts timezone whose format should satisfy the regular condition
// `(^(+|-)(0?[0-9]|1[0-2]):[0-5]?\d$)|(^+13:00$)` to time.Duration.
func timeZone2Duration(tz string) time.Duration {
	sign := 1
	if strings.HasPrefix(tz, "-") {
		sign = -1
	}

	i := strings.Index(tz, ":")
	h, err := strconv.Atoi(tz[1:i])
	terror.Log(err)
	m, err := strconv.Atoi(tz[i+1:])
	terror.Log(err)
	return time.Duration(sign) * (time.Duration(h)*time.Hour + time.Duration(m)*time.Minute)
}

var oppositeOp = map[string]string{
	ast.LT: ast.GE,
	ast.GE: ast.LT,
	ast.GT: ast.LE,
	ast.LE: ast.GT,
	ast.EQ: ast.NE,
	ast.NE: ast.EQ,
}

// a op b is equal to b symmetricOp a
var symmetricOp = map[opcode.Op]opcode.Op{
	opcode.LT:     opcode.GT,
	opcode.GE:     opcode.LE,
	opcode.GT:     opcode.LT,
	opcode.LE:     opcode.GE,
	opcode.EQ:     opcode.EQ,
	opcode.NE:     opcode.NE,
	opcode.NullEQ: opcode.NullEQ,
}

func doPushDownNot(ctx sessionctx.Context, exprs []Expression, not bool) []Expression {
	newExprs := make([]Expression, 0, len(exprs))
	for _, expr := range exprs {
		newExprs = append(newExprs, PushDownNot(ctx, expr, not))
	}
	return newExprs
}

// PushDownNot pushes the `not` function down to the expression's arguments.
func PushDownNot(ctx sessionctx.Context, expr Expression, not bool) Expression {
	if f, ok := expr.(*ScalarFunction); ok {
		switch f.FuncName.L {
		case ast.UnaryNot:
			return PushDownNot(f.GetCtx(), f.GetArgs()[0], !not)
		case ast.LT, ast.GE, ast.GT, ast.LE, ast.EQ, ast.NE:
			if not {
				return NewFunctionInternal(f.GetCtx(), oppositeOp[f.FuncName.L], f.GetType(), f.GetArgs()...)
			}
			newArgs := doPushDownNot(f.GetCtx(), f.GetArgs(), false)
			return NewFunctionInternal(f.GetCtx(), f.FuncName.L, f.GetType(), newArgs...)
		case ast.LogicAnd:
			if not {
				newArgs := doPushDownNot(f.GetCtx(), f.GetArgs(), true)
				return NewFunctionInternal(f.GetCtx(), ast.LogicOr, f.GetType(), newArgs...)
			}
			newArgs := doPushDownNot(f.GetCtx(), f.GetArgs(), false)
			return NewFunctionInternal(f.GetCtx(), f.FuncName.L, f.GetType(), newArgs...)
		case ast.LogicOr:
			if not {
				newArgs := doPushDownNot(f.GetCtx(), f.GetArgs(), true)
				return NewFunctionInternal(f.GetCtx(), ast.LogicAnd, f.GetType(), newArgs...)
			}
			newArgs := doPushDownNot(f.GetCtx(), f.GetArgs(), false)
			return NewFunctionInternal(f.GetCtx(), f.FuncName.L, f.GetType(), newArgs...)
		}
	}
	if not {
		expr = NewFunctionInternal(ctx, ast.UnaryNot, types.NewFieldType(mysql.TypeTiny), expr)
	}
	return expr
}

// Contains tests if `exprs` contains `e`.
func Contains(exprs []Expression, e Expression) bool {
	for _, expr := range exprs {
		if e == expr {
			return true
		}
	}
	return false
}

// ExtractFiltersFromDNFs checks whether the cond is DNF. If so, it will get the extracted part and the remained part.
// The original DNF will be replaced by the remained part or just be deleted if remained part is nil.
// And the extracted part will be appended to the end of the orignal slice.
func ExtractFiltersFromDNFs(ctx sessionctx.Context, conditions []Expression) []Expression {
	var allExtracted []Expression
	for i := len(conditions) - 1; i >= 0; i-- {
		if sf, ok := conditions[i].(*ScalarFunction); ok && sf.FuncName.L == ast.LogicOr {
			extracted, remained := extractFiltersFromDNF(ctx, sf)
			allExtracted = append(allExtracted, extracted...)
			if remained == nil {
				conditions = append(conditions[:i], conditions[i+1:]...)
			} else {
				conditions[i] = remained
			}
		}
	}
	return append(conditions, allExtracted...)
}

// extractFiltersFromDNF extracts the same condition that occurs in every DNF item and remove them from dnf leaves.
func extractFiltersFromDNF(ctx sessionctx.Context, dnfFunc *ScalarFunction) ([]Expression, Expression) {
	dnfItems := FlattenDNFConditions(dnfFunc)
	sc := ctx.GetSessionVars().StmtCtx
	codeMap := make(map[string]int)
	hashcode2Expr := make(map[string]Expression)
	for i, dnfItem := range dnfItems {
		innerMap := make(map[string]struct{})
		cnfItems := SplitCNFItems(dnfItem)
		for _, cnfItem := range cnfItems {
			code := cnfItem.HashCode(sc)
			if i == 0 {
				codeMap[string(code)] = 1
				hashcode2Expr[string(code)] = cnfItem
			} else if _, ok := codeMap[string(code)]; ok {
				// We need this check because there may be the case like `select * from t, t1 where (t.a=t1.a and t.a=t1.a) or (something).
				// We should make sure that the two `t.a=t1.a` contributes only once.
				// TODO: do this out of this function.
				if _, ok = innerMap[string(code)]; !ok {
					codeMap[string(code)]++
					innerMap[string(code)] = struct{}{}
				}
			}
		}
	}
	// We should make sure that this item occurs in every DNF item.
	for hashcode, cnt := range codeMap {
		if cnt < len(dnfItems) {
			delete(hashcode2Expr, hashcode)
		}
	}
	if len(hashcode2Expr) == 0 {
		return nil, dnfFunc
	}
	newDNFItems := make([]Expression, 0, len(dnfItems))
	onlyNeedExtracted := false
	for _, dnfItem := range dnfItems {
		cnfItems := SplitCNFItems(dnfItem)
		newCNFItems := make([]Expression, 0, len(cnfItems))
		for _, cnfItem := range cnfItems {
			code := cnfItem.HashCode(sc)
			_, ok := hashcode2Expr[string(code)]
			if !ok {
				newCNFItems = append(newCNFItems, cnfItem)
			}
		}
		// If the extracted part is just one leaf of the DNF expression. Then the value of the total DNF expression is
		// always the same with the value of the extracted part.
		if len(newCNFItems) == 0 {
			onlyNeedExtracted = true
			break
		}
		newDNFItems = append(newDNFItems, ComposeCNFCondition(ctx, newCNFItems...))
	}
	extractedExpr := make([]Expression, 0, len(hashcode2Expr))
	for _, expr := range hashcode2Expr {
		extractedExpr = append(extractedExpr, expr)
	}
	if onlyNeedExtracted {
		return extractedExpr, nil
	}
	return extractedExpr, ComposeDNFCondition(ctx, newDNFItems...)
}

// DeriveRelaxedFiltersFromDNF given a DNF expression, derive a relaxed DNF expression which only contains columns
// in specified schema; the derived expression is a superset of original expression, i.e, any tuple satisfying
// the original expression must satisfy the derived expression. Return nil when the derived expression is universal set.
// A running example is: for schema of t1, `(t1.a=1 and t2.a=1) or (t1.a=2 and t2.a=2)` would be derived as
// `t1.a=1 or t1.a=2`, while `t1.a=1 or t2.a=1` would get nil.
func DeriveRelaxedFiltersFromDNF(expr Expression, schema *Schema) Expression {
	sf, ok := expr.(*ScalarFunction)
	if !ok || sf.FuncName.L != ast.LogicOr {
		return nil
	}
	ctx := sf.GetCtx()
	dnfItems := FlattenDNFConditions(sf)
	newDNFItems := make([]Expression, 0, len(dnfItems))
	for _, dnfItem := range dnfItems {
		cnfItems := SplitCNFItems(dnfItem)
		newCNFItems := make([]Expression, 0, len(cnfItems))
		for _, cnfItem := range cnfItems {
			if itemSF, ok := cnfItem.(*ScalarFunction); ok && itemSF.FuncName.L == ast.LogicOr {
				relaxedCNFItem := DeriveRelaxedFiltersFromDNF(cnfItem, schema)
				if relaxedCNFItem != nil {
					newCNFItems = append(newCNFItems, relaxedCNFItem)
				}
				// If relaxed expression for embedded DNF is universal set, just drop this CNF item
				continue
			}
			// This cnfItem must be simple expression now
			// If it cannot be fully covered by schema, just drop this CNF item
			if ExprFromSchema(cnfItem, schema) {
				newCNFItems = append(newCNFItems, cnfItem)
			}
		}
		// If this DNF item involves no column of specified schema, the relaxed expression must be universal set
		if len(newCNFItems) == 0 {
			return nil
		}
		newDNFItems = append(newDNFItems, ComposeCNFCondition(ctx, newCNFItems...))
	}
	return ComposeDNFCondition(ctx, newDNFItems...)
}

// GetRowLen gets the length if the func is row, returns 1 if not row.
func GetRowLen(e Expression) int {
	if f, ok := e.(*ScalarFunction); ok && f.FuncName.L == ast.RowFunc {
		return len(f.GetArgs())
	}
	return 1
}

// CheckArgsNotMultiColumnRow checks the args are not multi-column row.
func CheckArgsNotMultiColumnRow(args ...Expression) error {
	for _, arg := range args {
		if GetRowLen(arg) != 1 {
			return ErrOperandColumns.GenWithStackByArgs(1)
		}
	}
	return nil
}

// GetFuncArg gets the argument of the function at idx.
func GetFuncArg(e Expression, idx int) Expression {
	if f, ok := e.(*ScalarFunction); ok {
		return f.GetArgs()[idx]
	}
	return nil
}

// PopRowFirstArg pops the first element and returns the rest of row.
// e.g. After this function (1, 2, 3) becomes (2, 3).
func PopRowFirstArg(ctx sessionctx.Context, e Expression) (ret Expression, err error) {
	if f, ok := e.(*ScalarFunction); ok && f.FuncName.L == ast.RowFunc {
		args := f.GetArgs()
		if len(args) == 2 {
			return args[1], nil
		}
		ret, err = NewFunction(ctx, ast.RowFunc, f.GetType(), args[1:]...)
		return ret, err
	}
	return
}

// exprStack is a stack of expressions.
type exprStack struct {
	stack []Expression
}

// pop pops an expression from the stack.
func (s *exprStack) pop() Expression {
	if s.len() == 0 {
		return nil
	}
	lastIdx := s.len() - 1
	expr := s.stack[lastIdx]
	s.stack = s.stack[:lastIdx]
	return expr
}

// popN pops n expressions from the stack.
// If n greater than stack length or n is negative, it pops all the expressions.
func (s *exprStack) popN(n int) []Expression {
	if n > s.len() || n < 0 {
		n = s.len()
	}
	idx := s.len() - n
	exprs := s.stack[idx:]
	s.stack = s.stack[:idx]
	return exprs
}

// push pushes one expression to the stack.
func (s *exprStack) push(expr Expression) {
	s.stack = append(s.stack, expr)
}

// len returns the length of th stack.
func (s *exprStack) len() int {
	return len(s.stack)
}

// DatumToConstant generates a Constant expression from a Datum.
func DatumToConstant(d types.Datum, tp byte) *Constant {
	return &Constant{Value: d, RetType: types.NewFieldType(tp)}
}

// ParamMarkerExpression generate a getparam function expression.
func ParamMarkerExpression(ctx sessionctx.Context, v *driver.ParamMarkerExpr) (Expression, error) {
	useCache := ctx.GetSessionVars().StmtCtx.UseCache
	isPointExec := ctx.GetSessionVars().StmtCtx.PointExec
	tp := types.NewFieldType(mysql.TypeUnspecified)
	types.DefaultParamTypeForValue(v.GetValue(), tp)
	value := &Constant{Value: v.Datum, RetType: tp}
	if useCache || isPointExec {
		value.ParamMarker = &ParamMarker{
			order: v.Order,
			ctx:   ctx,
		}
	}
	return value, nil
}

// DisableParseJSONFlag4Expr disables ParseToJSONFlag for `expr` except Column.
// We should not *PARSE* a string as JSON under some scenarios. ParseToJSONFlag
// is 0 for JSON column yet(as well as JSON correlated column), so we can skip
// it. Moreover, Column.RetType refers to the infoschema, if we modify it, data
// race may happen if another goroutine read from the infoschema at the same
// time.
func DisableParseJSONFlag4Expr(expr Expression) {
	if _, isColumn := expr.(*Column); isColumn {
		return
	}
	if _, isCorCol := expr.(*CorrelatedColumn); isCorCol {
		return
	}
	expr.GetType().Flag &= ^mysql.ParseToJSONFlag
}

// ConstructPositionExpr constructs PositionExpr with the given ParamMarkerExpr.
func ConstructPositionExpr(p *driver.ParamMarkerExpr) *ast.PositionExpr {
	return &ast.PositionExpr{P: p}
}

// PosFromPositionExpr generates a position value from PositionExpr.
func PosFromPositionExpr(ctx sessionctx.Context, v *ast.PositionExpr) (int, bool, error) {
	if v.P == nil {
		return v.N, false, nil
	}
	value, err := ParamMarkerExpression(ctx, v.P.(*driver.ParamMarkerExpr))
	if err != nil {
		return 0, true, err
	}
	pos, isNull, err := GetIntFromConstant(ctx, value)
	if err != nil || isNull {
		return 0, true, err
	}
	return pos, false, nil
}

// GetStringFromConstant gets a string value from the Constant expression.
func GetStringFromConstant(ctx sessionctx.Context, value Expression) (string, bool, error) {
	con, ok := value.(*Constant)
	if !ok {
		err := errors.Errorf("Not a Constant expression %+v", value)
		return "", true, err
	}
	str, isNull, err := con.EvalString(ctx, chunk.Row{})
	if err != nil || isNull {
		return "", true, err
	}
	return str, false, nil
}

// GetIntFromConstant gets an interger value from the Constant expression.
func GetIntFromConstant(ctx sessionctx.Context, value Expression) (int, bool, error) {
	str, isNull, err := GetStringFromConstant(ctx, value)
	if err != nil || isNull {
		return 0, true, err
	}
	intNum, err := strconv.Atoi(str)
	if err != nil {
		return 0, true, nil
	}
	return intNum, false, nil
}

// BuildNotNullExpr wraps up `not(isnull())` for given expression.
func BuildNotNullExpr(ctx sessionctx.Context, expr Expression) Expression {
	isNull := NewFunctionInternal(ctx, ast.IsNull, types.NewFieldType(mysql.TypeTiny), expr)
	notNull := NewFunctionInternal(ctx, ast.UnaryNot, types.NewFieldType(mysql.TypeTiny), isNull)
	return notNull
}

// IsMutableEffectsExpr checks if expr contains function which is mutable or has side effects.
func IsMutableEffectsExpr(expr Expression) bool {
	switch x := expr.(type) {
	case *ScalarFunction:
		if _, ok := mutableEffectsFunctions[x.FuncName.L]; ok {
			return true
		}
		for _, arg := range x.GetArgs() {
			if IsMutableEffectsExpr(arg) {
				return true
			}
		}
	case *Column:
	case *Constant:
		if x.DeferredExpr != nil {
			return IsMutableEffectsExpr(x.DeferredExpr)
		}
	}
	return false
}

// RemoveDupExprs removes identical exprs. Not that if expr contains functions which
// are mutable or have side effects, we cannot remove it even if it has duplicates.
func RemoveDupExprs(ctx sessionctx.Context, exprs []Expression) []Expression {
	res := make([]Expression, 0, len(exprs))
	exists := make(map[string]struct{}, len(exprs))
	sc := ctx.GetSessionVars().StmtCtx
	for _, expr := range exprs {
		key := string(expr.HashCode(sc))
		if _, ok := exists[key]; !ok || IsMutableEffectsExpr(expr) {
			res = append(res, expr)
			exists[key] = struct{}{}
		}
	}
	return res
}

// GetUint64FromConstant gets a uint64 from constant expression.
func GetUint64FromConstant(expr Expression) (uint64, bool, bool) {
	con, ok := expr.(*Constant)
	if !ok {
		logutil.BgLogger().Warn("not a constant expression", zap.String("expression", expr.ExplainInfo()))
		return 0, false, false
	}
	dt := con.Value
	if con.ParamMarker != nil {
		dt = con.ParamMarker.GetUserVar()
	} else if con.DeferredExpr != nil {
		var err error
		dt, err = con.DeferredExpr.Eval(chunk.Row{})
		if err != nil {
			logutil.BgLogger().Warn("eval deferred expr failed", zap.Error(err))
			return 0, false, false
		}
	}
	switch dt.Kind() {
	case types.KindNull:
		return 0, true, true
	case types.KindInt64:
		val := dt.GetInt64()
		if val < 0 {
			return 0, false, false
		}
		return uint64(val), false, true
	case types.KindUint64:
		return dt.GetUint64(), false, true
	}
	return 0, false, false
}
