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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package expression

import (
	"math"
	"slices"
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

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
func PopRowFirstArg(ctx BuildContext, e Expression) (ret Expression, err error) {
	if f, ok := e.(*ScalarFunction); ok && f.FuncName.L == ast.RowFunc {
		args := f.GetArgs()
		if len(args) == 2 {
			return args[1], nil
		}
		ret, err = NewFunction(ctx, ast.RowFunc, f.GetType(ctx.GetEvalCtx()), args[1:]...)
		return ret, err
	}
	return
}

// DatumToConstant generates a Constant expression from a Datum.
func DatumToConstant(d types.Datum, tp byte, flag uint) *Constant {
	t := types.NewFieldType(tp)
	t.AddFlag(flag)
	return &Constant{Value: d, RetType: t}
}

// ParamMarkerExpression generate a getparam function expression.
func ParamMarkerExpression(ctx BuildContext, v *driver.ParamMarkerExpr, needParam bool) (*Constant, error) {
	useCache := ctx.IsUseCache()
	tp := types.NewFieldType(mysql.TypeUnspecified)
	types.InferParamTypeFromDatum(&v.Datum, tp)
	value := &Constant{Value: v.Datum, RetType: tp}
	if useCache || needParam {
		value.ParamMarker = &ParamMarker{
			order: v.Order,
		}
	}
	return value, nil
}

// ParamMarkerInPrepareChecker checks whether the given ast tree has paramMarker and is in prepare statement.
type ParamMarkerInPrepareChecker struct {
	InPrepareStmt bool
}

// Enter implements Visitor Interface.
func (pc *ParamMarkerInPrepareChecker) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	switch v := in.(type) {
	case *driver.ParamMarkerExpr:
		pc.InPrepareStmt = !v.InExecute
		return v, true
	}
	return in, false
}

// Leave implements Visitor Interface.
func (pc *ParamMarkerInPrepareChecker) Leave(in ast.Node) (out ast.Node, ok bool) {
	return in, true
}

// DisableParseJSONFlag4Expr disables ParseToJSONFlag for `expr` except Column.
// We should not *PARSE* a string as JSON under some scenarios. ParseToJSONFlag
// is 0 for JSON column yet(as well as JSON correlated column), so we can skip
// it. Moreover, Column.RetType refers to the infoschema, if we modify it, data
// race may happen if another goroutine read from the infoschema at the same
// time.
func DisableParseJSONFlag4Expr(ctx EvalContext, expr Expression) {
	if _, isColumn := expr.(*Column); isColumn {
		return
	}
	if _, isCorCol := expr.(*CorrelatedColumn); isCorCol {
		return
	}
	expr.GetType(ctx).SetFlag(expr.GetType(ctx).GetFlag() & ^mysql.ParseToJSONFlag)
}

// ConstructPositionExpr constructs PositionExpr with the given ParamMarkerExpr.
func ConstructPositionExpr(p *driver.ParamMarkerExpr) *ast.PositionExpr {
	return &ast.PositionExpr{P: p}
}

// PosFromPositionExpr generates a position value from PositionExpr.
func PosFromPositionExpr(ctx BuildContext, v *ast.PositionExpr) (int, bool, error) {
	if v.P == nil {
		return v.N, false, nil
	}
	value, err := ParamMarkerExpression(ctx, v.P.(*driver.ParamMarkerExpr), false)
	if err != nil {
		return 0, true, err
	}
	pos, isNull, err := GetIntFromConstant(ctx.GetEvalCtx(), value)
	if err != nil || isNull {
		return 0, true, err
	}
	return pos, false, nil
}

// GetStringFromConstant gets a string value from the Constant expression.
func GetStringFromConstant(ctx EvalContext, value Expression) (string, bool, error) {
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

// GetIntFromConstant gets an integer value from the Constant expression.
func GetIntFromConstant(ctx EvalContext, value Expression) (int, bool, error) {
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
func BuildNotNullExpr(ctx BuildContext, expr Expression) Expression {
	isNull := NewFunctionInternal(ctx, ast.IsNull, types.NewFieldType(mysql.TypeTiny), expr)
	notNull := NewFunctionInternal(ctx, ast.UnaryNot, types.NewFieldType(mysql.TypeTiny), isNull)
	return notNull
}

// IsRuntimeConstExpr checks if a expr can be treated as a constant in **executor**.
func IsRuntimeConstExpr(expr Expression) bool {
	switch x := expr.(type) {
	case *ScalarFunction:
		if _, ok := unFoldableFunctions[x.FuncName.L]; ok {
			return false
		}
		for _, arg := range x.GetArgs() {
			if !IsRuntimeConstExpr(arg) {
				return false
			}
		}
		return true
	case *Column:
		return false
	case *Constant, *CorrelatedColumn:
		return true
	}
	return false
}

// CheckNonDeterministic checks whether the current expression contains a non-deterministic func.
func CheckNonDeterministic(e Expression) bool {
	switch x := e.(type) {
	case *Constant, *Column, *CorrelatedColumn:
		return false
	case *ScalarFunction:
		if _, ok := unFoldableFunctions[x.FuncName.L]; ok {
			return true
		}
		if slices.ContainsFunc(x.GetArgs(), CheckNonDeterministic) {
			return true
		}
	}
	return false
}

// CheckFuncInExpr checks whether there's a given function in the expression.
func CheckFuncInExpr(e Expression, funcName string) bool {
	switch x := e.(type) {
	case *Constant, *Column, *CorrelatedColumn:
		return false
	case *ScalarFunction:
		if x.FuncName.L == funcName {
			return true
		}
		for _, arg := range x.GetArgs() {
			if CheckFuncInExpr(arg, funcName) {
				return true
			}
		}
	}
	return false
}

// IsMutableEffectsExpr checks if expr contains function which is mutable or has side effects.
func IsMutableEffectsExpr(expr Expression) bool {
	switch x := expr.(type) {
	case *ScalarFunction:
		if _, ok := mutableEffectsFunctions[x.FuncName.L]; ok {
			return true
		}
		if slices.ContainsFunc(x.GetArgs(), IsMutableEffectsExpr) {
			return true
		}
	case *Column:
	case *Constant:
		if x.DeferredExpr != nil {
			return IsMutableEffectsExpr(x.DeferredExpr)
		}
	}
	return false
}

// IsImmutableFunc checks whether this expression only consists of foldable functions.
// This expression can be evaluated by using `expr.Eval(chunk.Row{})` directly and the result won't change if it's immutable.
func IsImmutableFunc(expr Expression) bool {
	switch x := expr.(type) {
	case *ScalarFunction:
		if _, ok := unFoldableFunctions[x.FuncName.L]; ok {
			return false
		}
		if _, ok := mutableEffectsFunctions[x.FuncName.L]; ok {
			return false
		}
		for _, arg := range x.GetArgs() {
			if !IsImmutableFunc(arg) {
				return false
			}
		}
		return true
	default:
		return true
	}
}

// RemoveDupExprs removes identical exprs. Not that if expr contains functions which
// are mutable or have side effects, we cannot remove it even if it has duplicates;
// if the plan is going to be cached, we cannot remove expressions containing `?` neither.
func RemoveDupExprs(exprs []Expression) []Expression {
	if len(exprs) <= 1 {
		return exprs
	}
	exists := make(map[string]struct{}, len(exprs))
	return slices.DeleteFunc(exprs, func(expr Expression) bool {
		key := string(expr.HashCode())
		if _, ok := exists[key]; !ok || IsMutableEffectsExpr(expr) {
			exists[key] = struct{}{}
			return false
		}
		return true
	})
}

// GetUint64FromConstant gets a uint64 from constant expression.
func GetUint64FromConstant(ctx EvalContext, expr Expression) (uint64, bool, bool) {
	con, ok := expr.(*Constant)
	if !ok {
		logutil.BgLogger().Warn("not a constant expression", zap.String("expression", expr.ExplainInfo(ctx)))
		return 0, false, false
	}
	dt := con.Value
	if con.ParamMarker != nil {
		var err error
		dt, err = con.ParamMarker.GetUserVar(ctx)
		if err != nil {
			logutil.BgLogger().Warn("get param failed", zap.Error(err))
			return 0, false, false
		}
	} else if con.DeferredExpr != nil {
		var err error
		dt, err = con.DeferredExpr.Eval(ctx, chunk.Row{})
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

// ContainVirtualColumn checks if the expressions contain a virtual column
func ContainVirtualColumn(exprs []Expression) bool {
	for _, expr := range exprs {
		switch v := expr.(type) {
		case *Column:
			if v.VirtualExpr != nil {
				return true
			}
		case *ScalarFunction:
			if ContainVirtualColumn(v.GetArgs()) {
				return true
			}
		}
	}
	return false
}

// ContainCorrelatedColumn checks if the expressions contain a correlated column
func ContainCorrelatedColumn(exprs ...Expression) bool {
	for _, expr := range exprs {
		switch v := expr.(type) {
		case *CorrelatedColumn:
			return true
		case *ScalarFunction:
			if ContainCorrelatedColumn(v.GetArgs()...) {
				return true
			}
		}
	}
	return false
}

func jsonUnquoteFunctionBenefitsFromPushedDown(sf *ScalarFunction) bool {
	arg0 := sf.GetArgs()[0]
	// Only `->>` which parsed to JSONUnquote(CAST(JSONExtract() AS string)) can be pushed down to tikv
	if fChild, ok := arg0.(*ScalarFunction); ok {
		if fChild.FuncName.L == ast.Cast {
			if fGrand, ok := fChild.GetArgs()[0].(*ScalarFunction); ok {
				if fGrand.FuncName.L == ast.JSONExtract {
					return true
				}
			}
		}
	}
	return false
}

// ProjectionBenefitsFromPushedDown evaluates if the expressions can improve performance when pushed down to TiKV
// Projections are not pushed down to tikv by default, thus we need to check strictly here to avoid potential performance degradation.
// Note: virtual column is not considered here, since this function cares performance instead of functionality
func ProjectionBenefitsFromPushedDown(exprs []Expression, inputSchemaLen int) bool {
	// In debug usage, we need to force push down projections to tikv to check tikv expression behavior.
	failpoint.Inject("forcePushDownTiKV", func() {
		failpoint.Return(true)
	})
	allColRef := true
	colRefCount := 0
	for _, expr := range exprs {
		switch v := expr.(type) {
		case *Column:
			colRefCount = colRefCount + 1
			continue
		case *ScalarFunction:
			allColRef = false
			switch v.FuncName.L {
			case ast.JSONDepth, ast.JSONLength, ast.JSONType, ast.JSONValid, ast.JSONContains, ast.JSONContainsPath,
				ast.JSONExtract, ast.JSONKeys, ast.JSONSearch, ast.JSONMemberOf, ast.JSONOverlaps:
				continue
			case ast.JSONUnquote:
				if jsonUnquoteFunctionBenefitsFromPushedDown(v) {
					continue
				}
				return false
			default:
				return false
			}
		default:
			return false
		}
	}
	// For all col refs, only push down column pruning projections
	if allColRef {
		return colRefCount < inputSchemaLen
	}
	return true
}

// MaybeOverOptimized4PlanCache used to check whether an optimization can work
// for the statement when we enable the plan cache.
// In some situations, some optimizations maybe over-optimize and cache an
// overOptimized plan. The cached plan may not get the correct result when we
// reuse the plan for other statements.
// For example, `pk>=$a and pk<=$b` can be optimized to a PointGet when
// `$a==$b`, but it will cause wrong results when `$a!=$b`.
// So we need to do the check here. The check includes the following aspects:
// 1. Whether the plan cache switch is enable.
// 2. Whether the statement can be cached.
// 3. Whether the expressions contain a lazy constant.
// TODO: Do more careful check here.
func MaybeOverOptimized4PlanCache(ctx BuildContext, exprs ...Expression) bool {
	// If we do not enable plan cache, all the optimization can work correctly.
	if !ctx.IsUseCache() {
		return false
	}
	return containMutableConst(ctx.GetEvalCtx(), exprs)
}

// containMutableConst checks if the expressions contain a lazy constant.
func containMutableConst(ctx EvalContext, exprs []Expression) bool {
	for _, expr := range exprs {
		switch v := expr.(type) {
		case *Constant:
			if v.ParamMarker != nil || v.DeferredExpr != nil {
				return true
			}
		case *ScalarFunction:
			if containMutableConst(ctx, v.GetArgs()) {
				return true
			}
		}
	}
	return false
}

// RemoveMutableConst used to remove the `ParamMarker` and `DeferredExpr` in the `Constant` expr.
func RemoveMutableConst(ctx BuildContext, exprs ...Expression) (err error) {
	for _, expr := range exprs {
		switch v := expr.(type) {
		case *Constant:
			v.ParamMarker = nil
			if v.DeferredExpr != nil { // evaluate and update v.Value to convert v to a complete immutable constant.
				// TODO: remove or hide DeferredExpr since it's too dangerous (hard to be consistent with v.Value all the time).
				v.Value, err = v.DeferredExpr.Eval(ctx.GetEvalCtx(), chunk.Row{})
				if err != nil {
					return err
				}
				v.DeferredExpr = nil
			}
			v.DeferredExpr = nil // do nothing since v.Value has already been evaluated in this case.
		case *ScalarFunction:
			err := RemoveMutableConst(ctx, v.GetArgs()...)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

const (
	_   = iota
	kib = 1 << (10 * iota)
	mib = 1 << (10 * iota)
	gib = 1 << (10 * iota)
	tib = 1 << (10 * iota)
	pib = 1 << (10 * iota)
	eib = 1 << (10 * iota)
)

const (
	nano    = 1
	micro   = 1000 * nano
	milli   = 1000 * micro
	sec     = 1000 * milli
	minute  = 60 * sec
	hour    = 60 * minute
	dayTime = 24 * hour
)

// GetFormatBytes convert byte count to value with units.
func GetFormatBytes(bytes float64) string {
	var divisor float64
	var unit string

	bytesAbs := math.Abs(bytes)
	if bytesAbs >= eib {
		divisor = eib
		unit = "EiB"
	} else if bytesAbs >= pib {
		divisor = pib
		unit = "PiB"
	} else if bytesAbs >= tib {
		divisor = tib
		unit = "TiB"
	} else if bytesAbs >= gib {
		divisor = gib
		unit = "GiB"
	} else if bytesAbs >= mib {
		divisor = mib
		unit = "MiB"
	} else if bytesAbs >= kib {
		divisor = kib
		unit = "KiB"
	} else {
		divisor = 1
		unit = "bytes"
	}

	if divisor == 1 {
		return strconv.FormatFloat(bytes, 'f', 0, 64) + " " + unit
	}
	value := bytes / divisor
	if math.Abs(value) >= 100000.0 {
		return strconv.FormatFloat(value, 'e', 2, 64) + " " + unit
	}
	return strconv.FormatFloat(value, 'f', 2, 64) + " " + unit
}

// GetFormatNanoTime convert time in nanoseconds to value with units.
func GetFormatNanoTime(time float64) string {
	var divisor float64
	var unit string

	timeAbs := math.Abs(time)
	if timeAbs >= dayTime {
		divisor = dayTime
		unit = "d"
	} else if timeAbs >= hour {
		divisor = hour
		unit = "h"
	} else if timeAbs >= minute {
		divisor = minute
		unit = "min"
	} else if timeAbs >= sec {
		divisor = sec
		unit = "s"
	} else if timeAbs >= milli {
		divisor = milli
		unit = "ms"
	} else if timeAbs >= micro {
		divisor = micro
		unit = "us"
	} else {
		divisor = 1
		unit = "ns"
	}

	if divisor == 1 {
		return strconv.FormatFloat(time, 'f', 0, 64) + " " + unit
	}
	value := time / divisor
	if math.Abs(value) >= 100000.0 {
		return strconv.FormatFloat(value, 'e', 2, 64) + " " + unit
	}
	return strconv.FormatFloat(value, 'f', 2, 64) + " " + unit
}
