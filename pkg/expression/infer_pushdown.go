// Copyright 2023 PingCAP, Inc.
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
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	contextutil "github.com/pingcap/tidb/pkg/util/context"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tipb/go-tipb"
	"go.uber.org/zap"
)

// DefaultExprPushDownBlacklist indicates the expressions which can not be pushed down to TiKV.
var DefaultExprPushDownBlacklist *atomic.Value

// ExprPushDownBlackListReloadTimeStamp is used to record the last time when the push-down black list is reloaded.
// This is for plan cache, when the push-down black list is updated, we invalid all cached plans to avoid error.
var ExprPushDownBlackListReloadTimeStamp *atomic.Int64

func canFuncBePushed(ctx EvalContext, sf *ScalarFunction, storeType kv.StoreType) bool {
	// Use the failpoint to control whether to push down an expression in the integration test.
	// Push down all expression if the `failpoint expression` is `all`, otherwise, check
	// whether scalar function's name is contained in the enabled expression list (e.g.`ne,eq,lt`).
	// If neither of the above is true, switch to original logic.
	failpoint.Inject("PushDownTestSwitcher", func(val failpoint.Value) {
		enabled := val.(string)
		if enabled == "all" {
			failpoint.Return(true)
		}
		exprs := strings.Split(enabled, ",")
		for _, expr := range exprs {
			if strings.ToLower(strings.TrimSpace(expr)) == sf.FuncName.L {
				failpoint.Return(true)
			}
		}
		failpoint.Return(false)
	})

	ret := false

	switch storeType {
	case kv.TiFlash:
		ret = scalarExprSupportedByFlash(ctx, sf)
	case kv.TiKV:
		ret = scalarExprSupportedByTiKV(ctx, sf)
	case kv.TiDB:
		ret = scalarExprSupportedByTiDB(ctx, sf)
	case kv.UnSpecified:
		ret = scalarExprSupportedByTiDB(ctx, sf) || scalarExprSupportedByTiKV(ctx, sf) || scalarExprSupportedByFlash(ctx, sf)
	}

	if ret {
		funcFullName := fmt.Sprintf("%s.%s", sf.FuncName.L, strings.ToLower(sf.Function.PbCode().String()))
		// Aside from checking function name, also check the pb name in case only the specific push down is disabled.
		ret = IsPushDownEnabled(sf.FuncName.L, storeType) && IsPushDownEnabled(funcFullName, storeType)
	}
	return ret
}

func canScalarFuncPushDown(ctx PushDownContext, scalarFunc *ScalarFunction, storeType kv.StoreType) bool {
	pbCode := scalarFunc.Function.PbCode()
	// Check whether this function can be pushed.
	if unspecified := pbCode <= tipb.ScalarFuncSig_Unspecified; unspecified || !canFuncBePushed(ctx.EvalCtx(), scalarFunc, storeType) {
		if unspecified {
			failpoint.Inject("PanicIfPbCodeUnspecified", func() {
				panic(errors.Errorf("unspecified PbCode: %T", scalarFunc.Function))
			})
		}
		storageName := storeType.Name()
		if storeType == kv.UnSpecified {
			storageName = "storage layer"
		}
		warnErr := errors.NewNoStackError("Scalar function '" + scalarFunc.FuncName.L + "'(signature: " + scalarFunc.Function.PbCode().String() + ", return type: " + scalarFunc.RetType.CompactStr() + ") is not supported to push down to " + storageName + " now.")

		ctx.AppendWarning(warnErr)
		return false
	}
	canEnumPush := canEnumPushdownPreliminarily(scalarFunc)
	// Check whether all of its parameters can be pushed.
	for _, arg := range scalarFunc.GetArgs() {
		if !canExprPushDown(ctx, arg, storeType, canEnumPush) {
			return false
		}
	}

	if metadata := scalarFunc.Function.metadata(); metadata != nil {
		var err error
		_, err = proto.Marshal(metadata)
		if err != nil {
			logutil.BgLogger().Error("encode metadata", zap.Any("metadata", metadata), zap.Error(err))
			return false
		}
	}
	return true
}

func canExprPushDown(ctx PushDownContext, expr Expression, storeType kv.StoreType, canEnumPush bool) bool {
	pc := ctx.PbConverter()
	if storeType == kv.TiFlash {
		switch expr.GetType(ctx.EvalCtx()).GetType() {
		case mysql.TypeEnum, mysql.TypeBit, mysql.TypeSet, mysql.TypeGeometry, mysql.TypeUnspecified:
			if expr.GetType(ctx.EvalCtx()).GetType() == mysql.TypeEnum && canEnumPush {
				break
			}
			warnErr := errors.NewNoStackError("Expression about '" + expr.String() + "' can not be pushed to TiFlash because it contains unsupported calculation of type '" + types.TypeStr(expr.GetType(ctx.EvalCtx()).GetType()) + "'.")
			ctx.AppendWarning(warnErr)
			return false
		case mysql.TypeNewDecimal:
			if !expr.GetType(ctx.EvalCtx()).IsDecimalValid() {
				warnErr := errors.NewNoStackError("Expression about '" + expr.String() + "' can not be pushed to TiFlash because it contains invalid decimal('" + strconv.Itoa(expr.GetType(ctx.EvalCtx()).GetFlen()) + "','" + strconv.Itoa(expr.GetType(ctx.EvalCtx()).GetDecimal()) + "').")
				ctx.AppendWarning(warnErr)
				return false
			}
		}
	}
	switch x := expr.(type) {
	case *CorrelatedColumn:
		return pc.conOrCorColToPBExpr(expr) != nil && pc.columnToPBExpr(&x.Column, true) != nil
	case *Constant:
		return pc.conOrCorColToPBExpr(expr) != nil
	case *Column:
		return pc.columnToPBExpr(x, true) != nil
	case *ScalarFunction:
		return canScalarFuncPushDown(ctx, x, storeType)
	}
	return false
}

func scalarExprSupportedByTiDB(ctx EvalContext, function *ScalarFunction) bool {
	// TiDB can support all functions, but TiPB may not include some functions.
	return scalarExprSupportedByTiKV(ctx, function) || scalarExprSupportedByFlash(ctx, function)
}

// supported functions tracked by https://github.com/tikv/tikv/issues/5751
func scalarExprSupportedByTiKV(ctx EvalContext, sf *ScalarFunction) bool {
	switch sf.FuncName.L {
	case
		// op functions.
		ast.LogicAnd, ast.LogicOr, ast.LogicXor, ast.UnaryNot, ast.And, ast.Or, ast.Xor, ast.BitNeg, ast.LeftShift, ast.RightShift, ast.UnaryMinus,

		// compare functions.
		ast.LT, ast.LE, ast.EQ, ast.NE, ast.GE, ast.GT, ast.NullEQ, ast.In, ast.IsNull, ast.Like, ast.IsTruthWithoutNull, ast.IsTruthWithNull, ast.IsFalsity,
		// ast.Greatest, ast.Least, ast.Interval

		// arithmetical functions.
		ast.PI, /* ast.Truncate */
		ast.Plus, ast.Minus, ast.Mul, ast.Div, ast.Abs, ast.Mod, ast.IntDiv,

		// math functions.
		ast.Ceil, ast.Ceiling, ast.Floor, ast.Sqrt, ast.Sign, ast.Ln, ast.Log, ast.Log2, ast.Log10, ast.Exp, ast.Pow, ast.Power,

		// Rust use the llvm math functions, which have different precision with Golang/MySQL(cmath)
		// open the following switchers if we implement them in coprocessor via `cmath`
		ast.Sin, ast.Asin, ast.Cos, ast.Acos /* ast.Tan */, ast.Atan, ast.Atan2, ast.Cot,
		ast.Radians, ast.Degrees, ast.CRC32,

		// control flow functions.
		ast.Case, ast.If, ast.Ifnull, ast.Coalesce,

		// string functions.
		// ast.Bin, ast.Unhex, ast.Locate, ast.Ord, ast.Lpad, ast.Rpad,
		// ast.Trim, ast.FromBase64, ast.ToBase64, ast.InsertFunc,
		// ast.MakeSet, ast.SubstringIndex, ast.Instr, ast.Quote, ast.Oct,
		// ast.FindInSet, ast.Repeat,
		ast.Upper, ast.Lower,
		ast.Length, ast.BitLength, ast.Concat, ast.ConcatWS, ast.Replace, ast.ASCII, ast.Hex,
		ast.Reverse, ast.LTrim, ast.RTrim, ast.Strcmp, ast.Space, ast.Elt, ast.Field,
		InternalFuncFromBinary, InternalFuncToBinary, ast.Mid, ast.Substring, ast.Substr, ast.CharLength,
		ast.Right, /* ast.Left */

		// json functions.
		ast.JSONType, ast.JSONExtract, ast.JSONObject, ast.JSONArray, ast.JSONMerge, ast.JSONSet,
		ast.JSONInsert, ast.JSONReplace, ast.JSONRemove, ast.JSONLength, ast.JSONMergePatch,
		ast.JSONUnquote, ast.JSONContains, ast.JSONValid, ast.JSONMemberOf, ast.JSONArrayAppend,

		// date functions.
		ast.Date, ast.Week /* ast.YearWeek, ast.ToSeconds */, ast.DateDiff,
		/* ast.TimeDiff, ast.AddTime,  ast.SubTime, */
		ast.MonthName, ast.MakeDate, ast.TimeToSec, ast.MakeTime,
		ast.DateFormat,
		ast.Hour, ast.Minute, ast.Second, ast.MicroSecond, ast.Month,
		/* ast.DayName */ ast.DayOfMonth, ast.DayOfWeek, ast.DayOfYear,
		/* ast.Weekday */ ast.WeekOfYear, ast.Year,
		ast.FromDays,                  /* ast.ToDays */
		ast.PeriodAdd, ast.PeriodDiff, /*ast.TimestampDiff, ast.DateAdd, ast.FromUnixTime,*/
		/* ast.LastDay */
		ast.Sysdate,

		// encryption functions.
		ast.MD5, ast.SHA1, ast.UncompressedLength,

		ast.Cast,

		// misc functions.
		// TODO(#26942): enable functions below after them are fully tested in TiKV.
		/*ast.InetNtoa, ast.InetAton, ast.Inet6Ntoa, ast.Inet6Aton, ast.IsIPv4, ast.IsIPv4Compat, ast.IsIPv4Mapped, ast.IsIPv6,*/
		ast.UUID:

		return true
	// Rust use the llvm math functions, which have different precision with Golang/MySQL(cmath)
	// open the following switchers if we implement them in coprocessor via `cmath`
	case ast.Conv:
		arg0 := sf.GetArgs()[0]
		// To be aligned with MySQL, tidb handles hybrid type argument and binary literal specially, tikv can't be consistent with tidb now.
		if f, ok := arg0.(*ScalarFunction); ok {
			if f.FuncName.L == ast.Cast && (f.GetArgs()[0].GetType(ctx).Hybrid() || IsBinaryLiteral(f.GetArgs()[0])) {
				return false
			}
		}
		return true
	case ast.Round:
		switch sf.Function.PbCode() {
		case tipb.ScalarFuncSig_RoundReal, tipb.ScalarFuncSig_RoundInt, tipb.ScalarFuncSig_RoundDec:
			// We don't push round with frac due to mysql's round with frac has its special behavior:
			// https://dev.mysql.com/doc/refman/5.7/en/mathematical-functions.html#function_round
			return true
		}
	case ast.Rand:
		switch sf.Function.PbCode() {
		case tipb.ScalarFuncSig_RandWithSeedFirstGen:
			return true
		}
	case ast.Regexp, ast.RegexpLike, ast.RegexpSubstr, ast.RegexpInStr, ast.RegexpReplace:
		funcCharset, funcCollation := sf.Function.CharsetAndCollation()
		if funcCharset == charset.CharsetBin && funcCollation == charset.CollationBin {
			return false
		}
		return true
	}
	return false
}

func scalarExprSupportedByFlash(ctx EvalContext, function *ScalarFunction) bool {
	switch function.FuncName.L {
	case ast.Floor, ast.Ceil, ast.Ceiling:
		switch function.Function.PbCode() {
		case tipb.ScalarFuncSig_FloorIntToDec, tipb.ScalarFuncSig_CeilIntToDec:
			return false
		default:
			return true
		}
	case
		ast.LogicOr, ast.LogicAnd, ast.UnaryNot, ast.BitNeg, ast.Xor, ast.And, ast.Or, ast.RightShift, ast.LeftShift,
		ast.GE, ast.LE, ast.EQ, ast.NE, ast.LT, ast.GT, ast.In, ast.IsNull, ast.Like, ast.Ilike, ast.Strcmp,
		ast.Plus, ast.Minus, ast.Div, ast.Mul, ast.Abs, ast.Mod,
		ast.If, ast.Ifnull, ast.Case,
		ast.Concat, ast.ConcatWS,
		ast.Date, ast.Year, ast.Month, ast.Day, ast.Quarter, ast.DayName, ast.MonthName,
		ast.DateDiff, ast.TimestampDiff, ast.DateFormat, ast.FromUnixTime,
		ast.DayOfWeek, ast.DayOfMonth, ast.DayOfYear, ast.LastDay, ast.WeekOfYear, ast.ToSeconds,
		ast.FromDays, ast.ToDays,

		ast.Sqrt, ast.Log, ast.Log2, ast.Log10, ast.Ln, ast.Exp, ast.Pow, ast.Power, ast.Sign,
		ast.Radians, ast.Degrees, ast.Conv, ast.CRC32,
		ast.JSONLength, ast.JSONDepth, ast.JSONExtract, ast.JSONUnquote, ast.JSONArray, ast.JSONContainsPath, ast.JSONValid, ast.JSONKeys,
		ast.Repeat, ast.InetNtoa, ast.InetAton, ast.Inet6Ntoa, ast.Inet6Aton,
		ast.Coalesce, ast.ASCII, ast.Length, ast.Trim, ast.Position, ast.Format, ast.Elt,
		ast.LTrim, ast.RTrim, ast.Lpad, ast.Rpad,
		ast.Hour, ast.Minute, ast.Second, ast.MicroSecond,
		ast.TimeToSec:
		switch function.Function.PbCode() {
		case tipb.ScalarFuncSig_InDuration,
			tipb.ScalarFuncSig_CoalesceDuration,
			tipb.ScalarFuncSig_IfNullDuration,
			tipb.ScalarFuncSig_IfDuration,
			tipb.ScalarFuncSig_CaseWhenDuration:
			return false
		}
		return true
	case ast.Regexp, ast.RegexpLike, ast.RegexpInStr, ast.RegexpSubstr, ast.RegexpReplace:
		funcCharset, funcCollation := function.Function.CharsetAndCollation()
		if funcCharset == charset.CharsetBin && funcCollation == charset.CollationBin {
			return false
		}
		return true
	case ast.Substr, ast.Substring, ast.Left, ast.Right, ast.CharLength, ast.SubstringIndex, ast.Reverse:
		switch function.Function.PbCode() {
		case
			tipb.ScalarFuncSig_LeftUTF8,
			tipb.ScalarFuncSig_RightUTF8,
			tipb.ScalarFuncSig_CharLengthUTF8,
			tipb.ScalarFuncSig_Substring2ArgsUTF8,
			tipb.ScalarFuncSig_Substring3ArgsUTF8,
			tipb.ScalarFuncSig_SubstringIndex,
			tipb.ScalarFuncSig_ReverseUTF8,
			tipb.ScalarFuncSig_Reverse:
			return true
		}
	case ast.Cast:
		sourceType := function.GetArgs()[0].GetType(ctx)
		retType := function.RetType
		switch function.Function.PbCode() {
		case tipb.ScalarFuncSig_CastDecimalAsInt, tipb.ScalarFuncSig_CastIntAsInt, tipb.ScalarFuncSig_CastRealAsInt, tipb.ScalarFuncSig_CastTimeAsInt,
			tipb.ScalarFuncSig_CastStringAsInt /*, tipb.ScalarFuncSig_CastDurationAsInt, tipb.ScalarFuncSig_CastJsonAsInt*/ :
			// TiFlash cast only support cast to Int64 or the source type is the same as the target type
			return (sourceType.GetType() == retType.GetType() && mysql.HasUnsignedFlag(sourceType.GetFlag()) == mysql.HasUnsignedFlag(retType.GetFlag())) || retType.GetType() == mysql.TypeLonglong
		case tipb.ScalarFuncSig_CastIntAsReal, tipb.ScalarFuncSig_CastRealAsReal, tipb.ScalarFuncSig_CastStringAsReal, tipb.ScalarFuncSig_CastTimeAsReal, tipb.ScalarFuncSig_CastDecimalAsReal: /*
			  tipb.ScalarFuncSig_CastDurationAsReal, tipb.ScalarFuncSig_CastJsonAsReal*/
			// TiFlash cast only support cast to Float64 or the source type is the same as the target type
			return sourceType.GetType() == retType.GetType() || retType.GetType() == mysql.TypeDouble
		case tipb.ScalarFuncSig_CastDecimalAsDecimal, tipb.ScalarFuncSig_CastIntAsDecimal, tipb.ScalarFuncSig_CastRealAsDecimal, tipb.ScalarFuncSig_CastTimeAsDecimal,
			tipb.ScalarFuncSig_CastStringAsDecimal /*, tipb.ScalarFuncSig_CastDurationAsDecimal, tipb.ScalarFuncSig_CastJsonAsDecimal*/ :
			return function.RetType.IsDecimalValid()
		case tipb.ScalarFuncSig_CastDecimalAsString, tipb.ScalarFuncSig_CastIntAsString, tipb.ScalarFuncSig_CastRealAsString, tipb.ScalarFuncSig_CastTimeAsString,
			tipb.ScalarFuncSig_CastStringAsString, tipb.ScalarFuncSig_CastJsonAsString /*, tipb.ScalarFuncSig_CastDurationAsString*/ :
			return true
		case tipb.ScalarFuncSig_CastDecimalAsTime, tipb.ScalarFuncSig_CastIntAsTime, tipb.ScalarFuncSig_CastRealAsTime, tipb.ScalarFuncSig_CastTimeAsTime,
			tipb.ScalarFuncSig_CastStringAsTime /*, tipb.ScalarFuncSig_CastDurationAsTime, tipb.ScalarFuncSig_CastJsonAsTime*/ :
			// ban the function of casting year type as time type pushing down to tiflash because of https://github.com/pingcap/tidb/issues/26215
			return function.GetArgs()[0].GetType(ctx).GetType() != mysql.TypeYear
		case tipb.ScalarFuncSig_CastTimeAsDuration:
			return retType.GetType() == mysql.TypeDuration
		case tipb.ScalarFuncSig_CastIntAsJson, tipb.ScalarFuncSig_CastRealAsJson, tipb.ScalarFuncSig_CastDecimalAsJson, tipb.ScalarFuncSig_CastStringAsJson,
			tipb.ScalarFuncSig_CastTimeAsJson, tipb.ScalarFuncSig_CastDurationAsJson, tipb.ScalarFuncSig_CastJsonAsJson:
			return true
		}
	case ast.DateAdd, ast.AddDate:
		switch function.Function.PbCode() {
		case tipb.ScalarFuncSig_AddDateDatetimeInt, tipb.ScalarFuncSig_AddDateStringInt, tipb.ScalarFuncSig_AddDateStringReal:
			return true
		}
	case ast.DateSub, ast.SubDate:
		switch function.Function.PbCode() {
		case tipb.ScalarFuncSig_SubDateDatetimeInt, tipb.ScalarFuncSig_SubDateStringInt, tipb.ScalarFuncSig_SubDateStringReal:
			return true
		}
	case ast.UnixTimestamp:
		switch function.Function.PbCode() {
		case tipb.ScalarFuncSig_UnixTimestampInt, tipb.ScalarFuncSig_UnixTimestampDec:
			return true
		}
	case ast.Round:
		switch function.Function.PbCode() {
		case tipb.ScalarFuncSig_RoundInt, tipb.ScalarFuncSig_RoundReal, tipb.ScalarFuncSig_RoundDec,
			tipb.ScalarFuncSig_RoundWithFracInt, tipb.ScalarFuncSig_RoundWithFracReal, tipb.ScalarFuncSig_RoundWithFracDec:
			return true
		}
	case ast.Extract:
		switch function.Function.PbCode() {
		case tipb.ScalarFuncSig_ExtractDatetime, tipb.ScalarFuncSig_ExtractDuration:
			return true
		}
	case ast.Replace:
		switch function.Function.PbCode() {
		case tipb.ScalarFuncSig_Replace:
			return true
		}
	case ast.StrToDate:
		switch function.Function.PbCode() {
		case
			tipb.ScalarFuncSig_StrToDateDate,
			tipb.ScalarFuncSig_StrToDateDatetime:
			return true
		default:
			return false
		}
	case ast.Upper, ast.Ucase, ast.Lower, ast.Lcase, ast.Space:
		return true
	case ast.Sysdate:
		return true
	case ast.Least, ast.Greatest:
		switch function.Function.PbCode() {
		case tipb.ScalarFuncSig_GreatestInt, tipb.ScalarFuncSig_GreatestReal,
			tipb.ScalarFuncSig_LeastInt, tipb.ScalarFuncSig_LeastReal, tipb.ScalarFuncSig_LeastString, tipb.ScalarFuncSig_GreatestString:
			return true
		}
	case ast.IsTruthWithNull, ast.IsTruthWithoutNull, ast.IsFalsity:
		return true
	case ast.Hex, ast.Unhex, ast.Bin:
		return true
	case ast.GetFormat:
		return true
	case ast.IsIPv4, ast.IsIPv6:
		return true
	case ast.Grouping: // grouping function for grouping sets identification.
		return true
	}
	return false
}

func canEnumPushdownPreliminarily(scalarFunc *ScalarFunction) bool {
	switch scalarFunc.FuncName.L {
	case ast.Cast:
		return scalarFunc.RetType.EvalType() == types.ETInt || scalarFunc.RetType.EvalType() == types.ETReal || scalarFunc.RetType.EvalType() == types.ETDecimal
	default:
		return false
	}
}

// IsPushDownEnabled returns true if the input expr is not in the expr_pushdown_blacklist
func IsPushDownEnabled(name string, storeType kv.StoreType) bool {
	value, exists := DefaultExprPushDownBlacklist.Load().(map[string]uint32)[name]
	if exists {
		mask := storeTypeMask(storeType)
		return !(value&mask == mask)
	}

	if storeType != kv.TiFlash && name == ast.AggFuncApproxCountDistinct {
		// Can not push down approx_count_distinct to other store except tiflash by now.
		return false
	}

	return true
}

// PushDownContext is the context used for push down expressions
type PushDownContext struct {
	evalCtx           EvalContext
	client            kv.Client
	warnHandler       contextutil.WarnAppender
	groupConcatMaxLen uint64
}

// NewPushDownContext returns a new PushDownContext
func NewPushDownContext(evalCtx EvalContext, client kv.Client, inExplainStmt bool,
	warnHandler contextutil.WarnAppender, extraWarnHandler contextutil.WarnAppender, groupConcatMaxLen uint64) PushDownContext {
	var newWarnHandler contextutil.WarnAppender
	if warnHandler != nil && extraWarnHandler != nil {
		if inExplainStmt {
			newWarnHandler = warnHandler
		} else {
			newWarnHandler = extraWarnHandler
		}
	}

	return PushDownContext{
		evalCtx:           evalCtx,
		client:            client,
		warnHandler:       newWarnHandler,
		groupConcatMaxLen: groupConcatMaxLen,
	}
}

// NewPushDownContextFromSessionVars builds a new PushDownContext from session vars.
func NewPushDownContextFromSessionVars(evalCtx EvalContext, sessVars *variable.SessionVars, client kv.Client) PushDownContext {
	return NewPushDownContext(
		evalCtx,
		client,
		sessVars.StmtCtx.InExplainStmt,
		sessVars.StmtCtx.WarnHandler,
		sessVars.StmtCtx.ExtraWarnHandler,
		sessVars.GroupConcatMaxLen)
}

// EvalCtx returns the eval context
func (ctx PushDownContext) EvalCtx() EvalContext {
	return ctx.evalCtx
}

// PbConverter returns a new PbConverter
func (ctx PushDownContext) PbConverter() PbConverter {
	return NewPBConverter(ctx.client, ctx.evalCtx)
}

// Client returns the kv client
func (ctx PushDownContext) Client() kv.Client {
	return ctx.client
}

// GetGroupConcatMaxLen returns the max length of group_concat
func (ctx PushDownContext) GetGroupConcatMaxLen() uint64 {
	return ctx.groupConcatMaxLen
}

// AppendWarning appends a warning to be handled by the internal handler
func (ctx PushDownContext) AppendWarning(err error) {
	if ctx.warnHandler != nil {
		ctx.warnHandler.AppendWarning(err)
	}
}

// PushDownExprsWithExtraInfo split the input exprs into pushed and remained, pushed include all the exprs that can be pushed down
func PushDownExprsWithExtraInfo(ctx PushDownContext, exprs []Expression, storeType kv.StoreType, canEnumPush bool) (pushed []Expression, remained []Expression) {
	for _, expr := range exprs {
		if canExprPushDown(ctx, expr, storeType, canEnumPush) {
			pushed = append(pushed, expr)
		} else {
			remained = append(remained, expr)
		}
	}
	return
}

// PushDownExprs split the input exprs into pushed and remained, pushed include all the exprs that can be pushed down
func PushDownExprs(ctx PushDownContext, exprs []Expression, storeType kv.StoreType) (pushed []Expression, remained []Expression) {
	return PushDownExprsWithExtraInfo(ctx, exprs, storeType, false)
}

// CanExprsPushDownWithExtraInfo return true if all the expr in exprs can be pushed down
func CanExprsPushDownWithExtraInfo(ctx PushDownContext, exprs []Expression, storeType kv.StoreType, canEnumPush bool) bool {
	_, remained := PushDownExprsWithExtraInfo(ctx, exprs, storeType, canEnumPush)
	return len(remained) == 0
}

// CanExprsPushDown return true if all the expr in exprs can be pushed down
func CanExprsPushDown(ctx PushDownContext, exprs []Expression, storeType kv.StoreType) bool {
	return CanExprsPushDownWithExtraInfo(ctx, exprs, storeType, false)
}

func storeTypeMask(storeType kv.StoreType) uint32 {
	if storeType == kv.UnSpecified {
		return 1<<kv.TiKV | 1<<kv.TiFlash | 1<<kv.TiDB
	}
	return 1 << storeType
}

func init() {
	DefaultExprPushDownBlacklist = new(atomic.Value)
	DefaultExprPushDownBlacklist.Store(make(map[string]uint32))
	ExprPushDownBlackListReloadTimeStamp = new(atomic.Int64)
}
