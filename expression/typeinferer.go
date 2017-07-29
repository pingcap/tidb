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

package expression

import (
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/parser/opcode"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/charset"
	"github.com/pingcap/tidb/util/types"
)

// InferType infers result type for ast.ExprNode.
func InferType(sc *variable.StatementContext, node ast.Node) error {
	var inferrer typeInferrer
	inferrer.sc = sc
	// TODO: get the default charset from ctx
	inferrer.defaultCharset = "utf8"
	node.Accept(&inferrer)
	return inferrer.err
}

type typeInferrer struct {
	sc             *variable.StatementContext
	err            error
	defaultCharset string
}

func (v *typeInferrer) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	switch in.(type) {
	case *ast.ColumnOption:
		return in, true
	}
	return in, false
}

func (v *typeInferrer) Leave(in ast.Node) (out ast.Node, ok bool) {
	switch x := in.(type) {
	case *ast.BetweenExpr:
		x.SetType(types.NewFieldType(mysql.TypeLonglong))
		types.SetBinChsClnFlag(&x.Type)
	case *ast.BinaryOperationExpr:
		v.binaryOperation(x)
	case *ast.CaseExpr:
		v.handleCaseExpr(x)
	case *ast.ColumnNameExpr:
		x.SetType(&x.Refer.Column.FieldType)
	case *ast.CompareSubqueryExpr:
		x.SetType(types.NewFieldType(mysql.TypeLonglong))
		types.SetBinChsClnFlag(&x.Type)
	case *ast.ExistsSubqueryExpr:
		x.SetType(types.NewFieldType(mysql.TypeLonglong))
		types.SetBinChsClnFlag(&x.Type)
	case *ast.FuncCallExpr:
		v.handleFuncCallExpr(x)
	case *ast.FuncCastExpr:
		// Copy a new field type.
		tp := *x.Tp
		x.SetType(&tp)
		if len(x.Type.Charset) == 0 {
			x.Type.Charset, x.Type.Collate = types.DefaultCharsetForType(x.Type.Tp)
		}
		if x.Type.Charset == charset.CharsetBin {
			x.Type.Flag |= mysql.BinaryFlag
		}
	case *ast.IsNullExpr:
		x.SetType(types.NewFieldType(mysql.TypeLonglong))
		types.SetBinChsClnFlag(&x.Type)
	case *ast.IsTruthExpr:
		x.SetType(types.NewFieldType(mysql.TypeLonglong))
		types.SetBinChsClnFlag(&x.Type)
	case *ast.ParamMarkerExpr:
		types.DefaultTypeForValue(x.GetValue(), x.GetType())
	case *ast.ParenthesesExpr:
		x.SetType(x.Expr.GetType())
	case *ast.PatternInExpr:
		x.SetType(types.NewFieldType(mysql.TypeLonglong))
		types.SetBinChsClnFlag(&x.Type)
		v.convertValueToColumnTypeIfNeeded(x)
	case *ast.PatternRegexpExpr:
		v.handleRegexpExpr(x)
	case *ast.SelectStmt:
		v.selectStmt(x)
	case *ast.UnaryOperationExpr:
		v.unaryOperation(x)
	case *ast.ValuesExpr:
		v.handleValuesExpr(x)
	case *ast.VariableExpr:
		x.SetType(types.NewFieldType(mysql.TypeVarString))
		x.Type.Charset = v.defaultCharset
		cln, err := charset.GetDefaultCollation(v.defaultCharset)
		if err != nil {
			v.err = err
		}
		x.Type.Collate = cln
		// TODO: handle all expression types.
	}
	return in, true
}

func (v *typeInferrer) selectStmt(x *ast.SelectStmt) {
	rf := x.GetResultFields()
	for _, val := range rf {
		// column ID is 0 means it is not a real column from table, but a temporary column,
		// so its type is not pre-defined, we need to set it.
		if val.Column.ID == 0 && val.Expr.GetType() != nil {
			val.Column.FieldType = *(val.Expr.GetType())
		}
	}
}

func (v *typeInferrer) binaryOperation(x *ast.BinaryOperationExpr) {
	switch x.Op {
	case opcode.LogicAnd, opcode.LogicOr, opcode.LogicXor:
		x.Type.Init(mysql.TypeLonglong)
	case opcode.LT, opcode.LE, opcode.GE, opcode.GT, opcode.EQ, opcode.NE, opcode.NullEQ:
		x.Type.Init(mysql.TypeLonglong)
	case opcode.RightShift, opcode.LeftShift, opcode.And, opcode.Or, opcode.Xor:
		x.Type.Init(mysql.TypeLonglong)
		x.Type.Flag |= mysql.UnsignedFlag
	case opcode.IntDiv:
		x.Type.Init(mysql.TypeLonglong)
	case opcode.Plus, opcode.Minus, opcode.Mul, opcode.Mod:
		if x.L.GetType() != nil && x.R.GetType() != nil {
			xTp := mergeArithType(x.L.GetType(), x.R.GetType())
			x.Type.Init(xTp)
			leftUnsigned := x.L.GetType().Flag & mysql.UnsignedFlag
			rightUnsigned := x.R.GetType().Flag & mysql.UnsignedFlag
			// If both operands are unsigned, result is unsigned.
			x.Type.Flag |= (leftUnsigned & rightUnsigned)
		}
	case opcode.Div:
		if x.L.GetType() != nil && x.R.GetType() != nil {
			xTp := mergeArithType(x.L.GetType(), x.R.GetType())
			if xTp == mysql.TypeLonglong {
				xTp = mysql.TypeNewDecimal
			}
			x.Type.Init(xTp)
		}
	}
	types.SetBinChsClnFlag(&x.Type)
}

// toArithType converts DateTime, Duration and Timestamp types to NewDecimal type if Decimal > 0.
func toArithType(ft *types.FieldType) (tp byte) {
	tp = ft.Tp
	if types.IsTypeFractionable(tp) {
		if ft.Decimal > 0 {
			tp = mysql.TypeNewDecimal
		} else {
			tp = mysql.TypeLonglong
		}
	}
	return
}

func mergeArithType(fta, ftb *types.FieldType) byte {
	a, b := toArithType(fta), toArithType(ftb)
	switch a {
	case mysql.TypeString, mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeDouble, mysql.TypeFloat, mysql.TypeEnum, mysql.TypeSet:
		return mysql.TypeDouble
	}
	switch b {
	case mysql.TypeString, mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeDouble, mysql.TypeFloat, mysql.TypeEnum, mysql.TypeSet:
		return mysql.TypeDouble
	}
	if a == mysql.TypeNewDecimal || b == mysql.TypeNewDecimal {
		return mysql.TypeNewDecimal
	}
	return mysql.TypeLonglong
}

func mergeCmpType(fta, ftb *types.FieldType) (ft *types.FieldType) {
	ft = &types.FieldType{}
	if fta.Charset == charset.CharsetUTF8 && ftb.Charset == charset.CharsetUTF8 {
		ft.Charset = charset.CharsetUTF8
		ft.Collate = mysql.UTF8DefaultCollation
	} else {
		ft.Flag |= mysql.BinaryFlag
	}
	isFtaTime, isFtbTime := types.IsTypeFractionable(fta.Tp), types.IsTypeFractionable(ftb.Tp)
	if types.IsTypeBlob(fta.Tp) || types.IsTypeBlob(ftb.Tp) {
		ft.Tp = mysql.TypeBlob
	} else if types.IsTypeVarchar(fta.Tp) || types.IsTypeVarchar(ftb.Tp) {
		ft.Tp = mysql.TypeVarString
	} else if types.IsTypeChar(fta.Tp) || types.IsTypeChar(ftb.Tp) {
		ft.Tp = mysql.TypeString
	} else if isFtaTime && isFtbTime {
		ft.Tp = mysql.TypeDatetime
	} else if isFtaTime || isFtbTime {
		ft.Tp = mysql.TypeVarString
	} else if fta.Tp == mysql.TypeEnum || ftb.Tp == mysql.TypeEnum || fta.Tp == mysql.TypeSet || ftb.Tp == mysql.TypeSet {
		ft.Tp = mysql.TypeString
	} else if fta.Tp == mysql.TypeDouble || ftb.Tp == mysql.TypeDouble {
		ft.Tp = mysql.TypeDouble
	} else if fta.Tp == mysql.TypeFloat || ftb.Tp == mysql.TypeFloat {
		ft.Tp = mysql.TypeFloat
	} else if fta.Tp == mysql.TypeNewDecimal || ftb.Tp == mysql.TypeNewDecimal {
		ft.Tp = mysql.TypeNewDecimal
	} else if fta.Tp == mysql.TypeLonglong || ftb.Tp == mysql.TypeLonglong {
		ft.Tp = mysql.TypeLonglong
	} else {
		ft.Tp = mysql.TypeLong
	}
	return ft
}

func (v *typeInferrer) unaryOperation(x *ast.UnaryOperationExpr) {
	switch x.Op {
	case opcode.Not:
		x.Type.Init(mysql.TypeLonglong)
	case opcode.BitNeg:
		x.Type.Init(mysql.TypeLonglong)
		x.Type.Flag |= mysql.UnsignedFlag
	case opcode.Plus:
		x.Type = *x.V.GetType()
	case opcode.Minus:
		x.Type.Init(mysql.TypeLonglong)
		if x.V.GetType() != nil {
			switch x.V.GetType().Tp {
			case mysql.TypeString, mysql.TypeVarchar, mysql.TypeVarString, mysql.TypeDouble, mysql.TypeFloat, mysql.TypeDatetime, mysql.TypeDuration, mysql.TypeTimestamp:
				x.Type.Tp = mysql.TypeDouble
			case mysql.TypeNewDecimal:
				x.Type.Tp = mysql.TypeNewDecimal
			}
		}
	}
	types.SetBinChsClnFlag(&x.Type)
}

func (v *typeInferrer) handleValuesExpr(x *ast.ValuesExpr) {
	x.SetType(x.Column.GetType())
}

func (v *typeInferrer) getFsp(x *ast.FuncCallExpr) int {
	if len(x.Args) == 1 {
		fsp, err := x.Args[0].GetDatum().ToInt64(v.sc)
		if err != nil {
			v.err = err
		}
		return int(fsp)
	}
	return 0
}

// handleFuncCallExpr ...
// TODO: (zhexuany) this function contains too much redundant things. Maybe replace with a map like
// we did for error in mysql package.
func (v *typeInferrer) handleFuncCallExpr(x *ast.FuncCallExpr) {
	var (
		tp  *types.FieldType
		chs = charset.CharsetBin
	)
	switch x.FnName.L {
	case ast.Abs, ast.Ifnull, ast.Nullif:
		if len(x.Args) == 0 {
			tp = types.NewFieldType(mysql.TypeNull)
			break
		}
		tp = x.Args[0].GetType()
		// TODO: We should cover all types.
		if x.FnName.L == ast.Abs && tp.Tp == mysql.TypeDatetime {
			tp = types.NewFieldType(mysql.TypeDouble)
		}
	case ast.Round, ast.Truncate:
		if len(x.Args) == 0 {
			tp = types.NewFieldType(mysql.TypeNull)
			break
		}
		t := x.Args[0].GetType().Tp
		switch t {
		case mysql.TypeBit, mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLonglong:
			tp = types.NewFieldType(mysql.TypeLonglong)
		case mysql.TypeNewDecimal:
			tp = types.NewFieldType(mysql.TypeNewDecimal)
		default:
			tp = types.NewFieldType(mysql.TypeDouble)
		}
	case ast.Greatest, ast.Least:
		for _, arg := range x.Args {
			InferType(v.sc, arg)
		}
		if len(x.Args) > 0 {
			tp = x.Args[0].GetType()
			for i := 1; i < len(x.Args); i++ {
				tp = mergeCmpType(tp, x.Args[i].GetType())
			}
		} else {
			tp = types.NewFieldType(mysql.TypeNull)
		}
	case ast.Ceil, ast.Ceiling, ast.Floor:
		if len(x.Args) > 0 {
			t := x.Args[0].GetType().Tp
			if t == mysql.TypeNull || t == mysql.TypeFloat || t == mysql.TypeDouble || t == mysql.TypeVarchar ||
				t == mysql.TypeTinyBlob || t == mysql.TypeMediumBlob || t == mysql.TypeLongBlob ||
				t == mysql.TypeBlob || t == mysql.TypeVarString || t == mysql.TypeString {
				tp = types.NewFieldType(mysql.TypeDouble)
			} else {
				tp = types.NewFieldType(mysql.TypeLonglong)
			}
		} else {
			tp = types.NewFieldType(mysql.TypeNull)
		}
	case ast.FromUnixTime:
		if len(x.Args) == 1 {
			tp = types.NewFieldType(mysql.TypeDatetime)
			tp.Decimal = x.Args[0].GetType().Decimal
		} else {
			tp = types.NewFieldType(mysql.TypeVarString)
			chs = v.defaultCharset
		}
	case ast.Coalesce:
		tp = aggFieldType(x.Args)
		if tp.Tp == mysql.TypeVarchar {
			tp.Tp = mysql.TypeVarString
		}
		classType := aggTypeClass(x.Args, &tp.Flag)
		if classType == types.ClassString && !mysql.HasBinaryFlag(tp.Flag) {
			tp.Charset, tp.Collate = types.DefaultCharsetForType(tp.Tp)
		}
	// number related
	case ast.Ln, ast.Log, ast.Log2, ast.Log10, ast.Sqrt, ast.PI, ast.Exp, ast.Degrees, ast.Sin, ast.Cos, ast.Tan,
		ast.Cot, ast.Acos, ast.Asin, ast.Atan, ast.Pow, ast.Power, ast.Rand, ast.Radians:
		tp = types.NewFieldType(mysql.TypeDouble)
	case ast.MicroSecond, ast.Second, ast.Minute, ast.Hour, ast.Day, ast.Week, ast.Month, ast.Year,
		ast.DayOfWeek, ast.DayOfMonth, ast.DayOfYear, ast.Weekday, ast.WeekOfYear, ast.YearWeek, ast.DateDiff,
		ast.FoundRows, ast.Length, ast.ASCII, ast.Extract, ast.Locate, ast.UnixTimestamp, ast.Quarter, ast.IsIPv4, ast.ToDays,
		ast.ToSeconds, ast.Strcmp, ast.IsNull, ast.BitLength, ast.CharLength, ast.CRC32, ast.TimestampDiff,
		ast.Sign, ast.IsIPv6, ast.Ord, ast.Instr, ast.BitCount, ast.TimeToSec, ast.FindInSet, ast.Field,
		ast.GetLock, ast.ReleaseLock, ast.Interval, ast.Position, ast.PeriodAdd, ast.PeriodDiff, ast.IsIPv4Mapped, ast.IsIPv4Compat, ast.UncompressedLength:
		tp = types.NewFieldType(mysql.TypeLonglong)
	case ast.ConnectionID, ast.InetAton:
		tp = types.NewFieldType(mysql.TypeLonglong)
		tp.Flag |= mysql.UnsignedFlag
	// time related
	case ast.AddTime:
		switch x.Args[0].GetType().Tp {
		case mysql.TypeDatetime, mysql.TypeDate, mysql.TypeTimestamp:
			tp = types.NewFieldType(mysql.TypeDatetime)
		case mysql.TypeDuration:
			tp = types.NewFieldType(mysql.TypeDuration)
		default:
			tp = types.NewFieldType(mysql.TypeVarString)
		}
		tp.Charset, tp.Collate = types.DefaultCharsetForType(tp.Tp)
	case ast.SubTime:
		switch t := x.Args[0].GetType().Tp; t {
		case mysql.TypeDatetime, mysql.TypeDate, mysql.TypeTimestamp:
			tp = types.NewFieldType(mysql.TypeDatetime)
		case mysql.TypeDuration:
			tp = types.NewFieldType(mysql.TypeDuration)
		default:
			tp = types.NewFieldType(mysql.TypeVarString)
		}
		tp.Charset, tp.Collate = types.DefaultCharsetForType(tp.Tp)
	case ast.Curtime, ast.CurrentTime, ast.TimeDiff, ast.MakeTime, ast.SecToTime, ast.UTCTime:
		tp = types.NewFieldType(mysql.TypeDuration)
		tp.Decimal = v.getFsp(x)
	case ast.Curdate, ast.CurrentDate, ast.Date, ast.FromDays, ast.MakeDate:
		tp = types.NewFieldType(mysql.TypeDate)
	case ast.DateAdd, ast.DateSub, ast.AddDate, ast.SubDate, ast.Timestamp, ast.TimestampAdd, ast.StrToDate, ast.ConvertTz:
		tp = types.NewFieldType(mysql.TypeDatetime)
	case ast.Now, ast.Sysdate, ast.CurrentTimestamp, ast.UTCTimestamp:
		tp = types.NewFieldType(mysql.TypeDatetime)
		tp.Decimal = v.getFsp(x)
	case ast.MD5:
		tp = types.NewFieldType(mysql.TypeVarString)
		chs = v.defaultCharset
		tp.Flen = 32
	case ast.SHA, ast.SHA1:
		tp = types.NewFieldType(mysql.TypeVarString)
		chs = v.defaultCharset
		tp.Flen = 40
	case ast.DayName, ast.Version, ast.Database, ast.User, ast.CurrentUser, ast.Schema,
		ast.Concat, ast.ConcatWS, ast.Left, ast.Right, ast.Lcase, ast.Lower, ast.Repeat,
		ast.Replace, ast.Ucase, ast.Upper, ast.Convert, ast.Substring, ast.Elt,
		ast.SubstringIndex, ast.Trim, ast.LTrim, ast.RTrim, ast.Reverse, ast.Hex, ast.Unhex,
		ast.DateFormat, ast.Rpad, ast.Lpad, ast.CharFunc, ast.Conv, ast.MakeSet, ast.Oct, ast.UUID,
		ast.InsertFunc, ast.Bin, ast.Quote, ast.Format, ast.FromBase64, ast.ToBase64,
		ast.ExportSet, ast.AesEncrypt, ast.AesDecrypt, ast.SHA2, ast.InetNtoa, ast.Inet6Aton,
		ast.Inet6Ntoa, ast.PasswordFunc, ast.TiDBVersion:
		tp = types.NewFieldType(mysql.TypeVarString)
		chs = v.defaultCharset
	case ast.RandomBytes:
		tp = types.NewFieldType(mysql.TypeVarString)
	case ast.If:
		// TODO: fix this
		// See https://dev.mysql.com/doc/refman/5.5/en/control-flow-functions.html#function_if
		// The default return type of IF() (which may matter when it is stored into a temporary table) is calculated as follows.
		// Expression	Return Value
		// expr2 or expr3 returns a string	string
		// expr2 or expr3 returns a floating-point value	floating-point
		// expr2 or expr3 returns an integer	integer
		tp = x.Args[1].GetType()
	case ast.Compress:
		tp = types.NewFieldType(mysql.TypeBlob)
	case ast.Uncompress:
		tp = types.NewFieldType(mysql.TypeLongBlob)
	case ast.JSONType, ast.JSONUnquote:
		tp = types.NewFieldType(mysql.TypeVarString)
		chs = v.defaultCharset
	case ast.JSONExtract, ast.JSONSet, ast.JSONInsert, ast.JSONReplace, ast.JSONMerge:
		tp = types.NewFieldType(mysql.TypeJSON)
		chs = v.defaultCharset
	case ast.AnyValue:
		tp = x.Args[0].GetType()
	case ast.RowFunc:
		tp = x.Args[0].GetType()
	default:
		tp = types.NewFieldType(mysql.TypeUnspecified)
	}
	// If charset is unspecified.
	if len(tp.Charset) == 0 {
		cln, err := charset.GetDefaultCollation(chs)
		if err != nil {
			v.err = err
		}
		tp.Charset, tp.Collate = chs, cln
	}
	if tp.Charset == charset.CharsetBin {
		tp.Flag |= mysql.BinaryFlag
	}
	x.SetType(tp)
}

// handleCaseExpr decides the return type of a CASE expression which is the compatible aggregated type of all return values,
// but also depends on the context in which it is used.
// If used in a string context, the result is returned as a string.
// If used in a numeric context, the result is returned as a decimal, real, or integer value.
func (v *typeInferrer) handleCaseExpr(x *ast.CaseExpr) {
	exprs := make([]ast.ExprNode, 0, len(x.WhenClauses)+1)
	for _, w := range x.WhenClauses {
		exprs = append(exprs, w.Result)
	}
	if x.ElseClause != nil {
		exprs = append(exprs, x.ElseClause)
	}
	tp := aggFieldType(exprs)
	if tp.Tp == mysql.TypeVarchar {
		tp.Tp = mysql.TypeVarString
	}
	classType := aggTypeClass(exprs, &tp.Flag)
	if classType == types.ClassString && !mysql.HasBinaryFlag(tp.Flag) {
		tp.Charset, tp.Collate = types.DefaultCharsetForType(tp.Tp)
	} else {
		types.SetBinChsClnFlag(tp)
	}
	x.SetType(tp)
}

// handleRegexpExpr expects the target expression and pattern to be a string, if it's not, we add a cast function.
func (v *typeInferrer) handleRegexpExpr(x *ast.PatternRegexpExpr) {
	x.SetType(types.NewFieldType(mysql.TypeLonglong))
	types.SetBinChsClnFlag(&x.Type)
	x.Expr = v.addCastToString(x.Expr)
	x.Pattern = v.addCastToString(x.Pattern)
}

// addCastToString adds a cast function to string type if the expr charset is not UTF8.
func (v *typeInferrer) addCastToString(expr ast.ExprNode) ast.ExprNode {
	if !mysql.IsUTF8Charset(expr.GetType().Charset) {
		castTp := types.NewFieldType(mysql.TypeString)
		castTp.Charset, castTp.Collate = types.DefaultCharsetForType(mysql.TypeString)
		if val, ok := expr.(*ast.ValueExpr); ok {
			newVal, err := val.Datum.ConvertTo(v.sc, castTp)
			if err != nil {
				v.err = errors.Trace(err)
			}
			expr.SetDatum(newVal)
		} else {
			castFunc := &ast.FuncCastExpr{
				Expr:         expr,
				Tp:           castTp,
				FunctionType: ast.CastFunction,
			}
			expr = castFunc
		}
		expr.SetType(castTp)
	}
	return expr
}

// convertValueToColumnTypeIfNeeded checks if the expr in PatternInExpr is column name,
// and casts function to the items in the list.
func (v *typeInferrer) convertValueToColumnTypeIfNeeded(x *ast.PatternInExpr) {
	if cn, ok := x.Expr.(*ast.ColumnNameExpr); ok && cn.Refer != nil {
		ft := cn.Refer.Column.FieldType
		for _, expr := range x.List {
			if valueExpr, ok := expr.(*ast.ValueExpr); ok {
				newDatum, err := valueExpr.Datum.ConvertTo(v.sc, &ft)
				if err != nil {
					v.err = errors.Trace(err)
				}
				cmp, err := newDatum.CompareDatum(v.sc, valueExpr.Datum)
				if err != nil {
					v.err = errors.Trace(err)
				}
				if cmp != 0 {
					// The value will never match the column, do not set newDatum.
					continue
				}
				valueExpr.SetDatum(newDatum)
			}
		}
		if v.err != nil {
			// TODO: Errors should be handled differently according to query context.
			log.Errorf("inferor type for pattern in error %v", v.err)
			v.err = nil
		}
	}
}

func aggFieldType(args []ast.ExprNode) *types.FieldType {
	var currType types.FieldType
	for _, arg := range args {
		t := arg.GetType()
		if currType.Tp == mysql.TypeUnspecified {
			currType = *t
			continue
		}
		mtp := types.MergeFieldType(currType.Tp, t.Tp)
		currType.Tp = mtp
	}
	return &currType
}

func aggTypeClass(args []ast.ExprNode, flag *uint) types.TypeClass {
	var (
		tpClass      = types.ClassString
		unsigned     bool
		gotFirst     bool
		gotBinString bool
	)
	for _, arg := range args {
		argFieldType := arg.GetType()
		if argFieldType.Tp == mysql.TypeNull {
			continue
		}
		argTypeClass := argFieldType.ToClass()
		if argTypeClass == types.ClassString && mysql.HasBinaryFlag(argFieldType.Flag) {
			gotBinString = true
		}
		if !gotFirst {
			gotFirst = true
			tpClass = argTypeClass
			unsigned = mysql.HasUnsignedFlag(argFieldType.Flag)
		} else {
			tpClass = mergeTypeClass(tpClass, argTypeClass, unsigned, mysql.HasUnsignedFlag(argFieldType.Flag))
			unsigned = unsigned && mysql.HasUnsignedFlag(argFieldType.Flag)
		}
	}
	setTypeFlag(flag, uint(mysql.UnsignedFlag), unsigned)
	setTypeFlag(flag, uint(mysql.BinaryFlag), tpClass != types.ClassString || gotBinString)
	return tpClass
}

func setTypeFlag(flag *uint, flagItem uint, on bool) {
	if on {
		*flag |= flagItem
	} else {
		*flag &= ^flagItem
	}
}

func mergeTypeClass(a, b types.TypeClass, aUnsigned, bUnsigned bool) types.TypeClass {
	if a == types.ClassString || b == types.ClassString {
		return types.ClassString
	} else if a == types.ClassReal || b == types.ClassReal {
		return types.ClassReal
	} else if a == types.ClassDecimal || b == types.ClassDecimal || aUnsigned != bUnsigned {
		return types.ClassDecimal
	}
	return types.ClassInt
}

// IsHybridType checks whether a ClassString expression is a hybrid type value which will return different types of value in different context.
//
// For ENUM/SET which is consist of a string attribute `Name` and an int attribute `Value`,
// it will cause an error if we convert ENUM/SET to int as a string value.
//
// For Bit/Hex, we will get a wrong result if we convert it to int as a string value.
// For example, when convert `0b101` to int, the result should be 5, but we will get 101 if we regard it as a string.
func IsHybridType(expr Expression) bool {
	switch expr.GetType().Tp {
	case mysql.TypeEnum, mysql.TypeBit, mysql.TypeSet:
		return true
	}
	// For a constant, the field type will be inferred as `VARCHAR` when the kind of it is `HEX` or `BIT`.
	if con, ok := expr.(*Constant); ok {
		switch con.Value.Kind() {
		case types.KindMysqlHex, types.KindMysqlBit:
			return true
		}
	}
	return false
}
