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

package aggregation

import (
	"bytes"
	"fmt"
	"math"
	"strings"

	"github.com/cznic/mathutil"
	"github.com/pingcap/br/pkg/utils"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

// baseFuncDesc describes an function signature, only used in planner.
type baseFuncDesc struct {
	// Name represents the function name.
	Name string
	// Args represents the arguments of the function.
	Args []expression.Expression
	// RetTp represents the return type of the function.
	RetTp *types.FieldType
}

func newBaseFuncDesc(ctx sessionctx.Context, name string, args []expression.Expression) (baseFuncDesc, error) {
	b := baseFuncDesc{Name: strings.ToLower(name), Args: args}
	err := b.typeInfer(ctx)
	return b, err
}

func (a *baseFuncDesc) equal(ctx sessionctx.Context, other *baseFuncDesc) bool {
	if a.Name != other.Name || len(a.Args) != len(other.Args) {
		return false
	}
	for i := range a.Args {
		if !a.Args[i].Equal(ctx, other.Args[i]) {
			return false
		}
	}
	return true
}

func (a *baseFuncDesc) clone() *baseFuncDesc {
	clone := *a
	newTp := *a.RetTp
	clone.RetTp = &newTp
	clone.Args = make([]expression.Expression, len(a.Args))
	for i := range a.Args {
		clone.Args[i] = a.Args[i].Clone()
	}
	return &clone
}

// String implements the fmt.Stringer interface.
func (a *baseFuncDesc) String() string {
	buffer := bytes.NewBufferString(a.Name)
	buffer.WriteString("(")
	for i, arg := range a.Args {
		buffer.WriteString(arg.String())
		if i+1 != len(a.Args) {
			buffer.WriteString(", ")
		}
	}
	buffer.WriteString(")")
	return buffer.String()
}

// typeInfer infers the arguments and return types of an function.
func (a *baseFuncDesc) typeInfer(ctx sessionctx.Context) error {
	switch a.Name {
	case ast.AggFuncCount:
		a.typeInfer4Count(ctx)
	case ast.AggFuncApproxCountDistinct:
		a.typeInfer4ApproxCountDistinct(ctx)
	case ast.AggFuncApproxPercentile:
		return a.typeInfer4ApproxPercentile(ctx)
	case ast.AggFuncSum:
		a.typeInfer4Sum(ctx)
	case ast.AggFuncAvg:
		a.typeInfer4Avg(ctx)
	case ast.AggFuncGroupConcat:
		a.typeInfer4GroupConcat(ctx)
	case ast.AggFuncMax, ast.AggFuncMin, ast.AggFuncFirstRow,
		ast.WindowFuncFirstValue, ast.WindowFuncLastValue, ast.WindowFuncNthValue:
		a.typeInfer4MaxMin(ctx)
	case ast.AggFuncBitAnd, ast.AggFuncBitOr, ast.AggFuncBitXor:
		a.typeInfer4BitFuncs(ctx)
	case ast.WindowFuncRowNumber, ast.WindowFuncRank, ast.WindowFuncDenseRank:
		a.typeInfer4NumberFuncs()
	case ast.WindowFuncCumeDist:
		a.typeInfer4CumeDist()
	case ast.WindowFuncNtile:
		a.typeInfer4Ntile()
	case ast.WindowFuncPercentRank:
		a.typeInfer4PercentRank()
	case ast.WindowFuncLead, ast.WindowFuncLag:
		a.typeInfer4LeadLag(ctx)
	case ast.AggFuncVarPop, ast.AggFuncStddevPop, ast.AggFuncVarSamp, ast.AggFuncStddevSamp:
		a.typeInfer4PopOrSamp(ctx)
	case ast.AggFuncJsonObjectAgg:
		a.typeInfer4JsonFuncs(ctx)
	default:
		return errors.Errorf("unsupported agg function: %s", a.Name)
	}
	return nil
}

func (a *baseFuncDesc) typeInfer4Count(ctx sessionctx.Context) {
	a.RetTp = types.NewFieldType(mysql.TypeLonglong)
	a.RetTp.Flen = 21
	a.RetTp.Decimal = 0
	// count never returns null
	a.RetTp.Flag |= mysql.NotNullFlag
	types.SetBinChsClnFlag(a.RetTp)
}

func (a *baseFuncDesc) typeInfer4ApproxCountDistinct(ctx sessionctx.Context) {
	a.typeInfer4Count(ctx)
}

func (a *baseFuncDesc) typeInfer4ApproxPercentile(ctx sessionctx.Context) error {
	if len(a.Args) != 2 {
		return errors.New("APPROX_PERCENTILE should take 2 arguments")
	}

	if !a.Args[1].ConstItem(ctx.GetSessionVars().StmtCtx) {
		return errors.New("APPROX_PERCENTILE should take a constant expression as percentage argument")
	}
	percent, isNull, err := a.Args[1].EvalInt(ctx, chunk.Row{})
	if err != nil {
		return errors.New(fmt.Sprintf("APPROX_PERCENTILE: Invalid argument %s", a.Args[1].String()))
	}
	if percent <= 0 || percent > 100 || isNull {
		if isNull {
			return errors.New("APPROX_PERCENTILE: Percentage value cannot be NULL")
		}
		return errors.New(fmt.Sprintf("Percentage value %d is out of range [1, 100]", percent))
	}

	switch a.Args[0].GetType().Tp {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		a.RetTp = types.NewFieldType(mysql.TypeLonglong)
	case mysql.TypeDouble, mysql.TypeFloat:
		a.RetTp = types.NewFieldType(mysql.TypeDouble)
	case mysql.TypeNewDecimal:
		a.RetTp = types.NewFieldType(mysql.TypeNewDecimal)
		a.RetTp.Flen, a.RetTp.Decimal = mysql.MaxDecimalWidth, a.Args[0].GetType().Decimal
		if a.RetTp.Decimal < 0 || a.RetTp.Decimal > mysql.MaxDecimalScale {
			a.RetTp.Decimal = mysql.MaxDecimalScale
		}
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeNewDate, mysql.TypeTimestamp:
		a.RetTp = a.Args[0].GetType().Clone()
	default:
		a.RetTp = a.Args[0].GetType().Clone()
		a.RetTp.Flag &= ^mysql.NotNullFlag
	}
	return nil
}

// typeInfer4Sum should returns a "decimal", otherwise it returns a "double".
// Because child returns integer or decimal type.
func (a *baseFuncDesc) typeInfer4Sum(ctx sessionctx.Context) {
	switch a.Args[0].GetType().Tp {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeYear:
		a.RetTp = types.NewFieldType(mysql.TypeNewDecimal)
		a.RetTp.Flen, a.RetTp.Decimal = utils.MinInt(a.Args[0].GetType().Flen+21, mysql.MaxDecimalWidth), 0
		if a.Args[0].GetType().Flen < 0 {
			a.RetTp.Flen = mysql.MaxDecimalWidth
		}
	case mysql.TypeNewDecimal:
		a.RetTp = types.NewFieldType(mysql.TypeNewDecimal)
		a.RetTp.Flen, a.RetTp.Decimal = utils.MinInt(a.Args[0].GetType().Flen+22), a.Args[0].GetType().Decimal
		if a.Args[0].GetType().Flen < 0 {
			a.RetTp.Flen = mysql.MaxDecimalWidth
		}
		if a.RetTp.Decimal < 0 || a.RetTp.Decimal > mysql.MaxDecimalScale {
			a.RetTp.Decimal = mysql.MaxDecimalScale
		}
	case mysql.TypeDouble, mysql.TypeFloat:
		a.RetTp = types.NewFieldType(mysql.TypeDouble)
		a.RetTp.Flen, a.RetTp.Decimal = mysql.MaxRealWidth, a.Args[0].GetType().Decimal
	default:
		a.RetTp = types.NewFieldType(mysql.TypeDouble)
		a.RetTp.Flen, a.RetTp.Decimal = mysql.MaxRealWidth, types.UnspecifiedLength
	}
	types.SetBinChsClnFlag(a.RetTp)
}

// typeInfer4Avg should returns a "decimal", otherwise it returns a "double".
// Because child returns integer or decimal type.
func (a *baseFuncDesc) typeInfer4Avg(ctx sessionctx.Context) {
	switch a.Args[0].GetType().Tp {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeYear, mysql.TypeNewDecimal:
		a.RetTp = types.NewFieldType(mysql.TypeNewDecimal)
		if a.Args[0].GetType().Decimal < 0 {
			a.RetTp.Decimal = mysql.MaxDecimalScale
		} else {
			a.RetTp.Decimal = mathutil.Min(a.Args[0].GetType().Decimal+types.DivFracIncr, mysql.MaxDecimalScale)
		}
		a.RetTp.Flen = utils.MinInt(mysql.MaxDecimalWidth, a.Args[0].GetType().Flen+types.DivFracIncr)
		if a.Args[0].GetType().Flen < 0 {
			a.RetTp.Flen = mysql.MaxDecimalWidth
		}
	case mysql.TypeDouble, mysql.TypeFloat:
		a.RetTp = types.NewFieldType(mysql.TypeDouble)
		a.RetTp.Flen, a.RetTp.Decimal = mysql.MaxRealWidth, a.Args[0].GetType().Decimal
	default:
		a.RetTp = types.NewFieldType(mysql.TypeDouble)
		a.RetTp.Flen, a.RetTp.Decimal = mysql.MaxRealWidth, types.UnspecifiedLength
	}
	types.SetBinChsClnFlag(a.RetTp)
}

func (a *baseFuncDesc) typeInfer4GroupConcat(ctx sessionctx.Context) {
	a.RetTp = types.NewFieldType(mysql.TypeVarString)
	a.RetTp.Charset, a.RetTp.Collate = charset.GetDefaultCharsetAndCollate()

	a.RetTp.Flen, a.RetTp.Decimal = mysql.MaxBlobWidth, 0
	// TODO: a.Args[i] = expression.WrapWithCastAsString(ctx, a.Args[i])
}

func (a *baseFuncDesc) typeInfer4MaxMin(ctx sessionctx.Context) {
	_, argIsScalaFunc := a.Args[0].(*expression.ScalarFunction)
	if argIsScalaFunc && a.Args[0].GetType().Tp == mysql.TypeFloat {
		// For scalar function, the result of "float32" is set to the "float64"
		// field in the "Datum". If we do not wrap a cast-as-double function on a.Args[0],
		// error would happen when extracting the evaluation of a.Args[0] to a ProjectionExec.
		tp := types.NewFieldType(mysql.TypeDouble)
		tp.Flen, tp.Decimal = mysql.MaxRealWidth, types.UnspecifiedLength
		types.SetBinChsClnFlag(tp)
		a.Args[0] = expression.BuildCastFunction(ctx, a.Args[0], tp)
	}
	a.RetTp = a.Args[0].GetType()
	if (a.Name == ast.AggFuncMax || a.Name == ast.AggFuncMin) && a.RetTp.Tp != mysql.TypeBit {
		a.RetTp = a.Args[0].GetType().Clone()
		a.RetTp.Flag &^= mysql.NotNullFlag
	}
	// issue #13027, #13961
	if (a.RetTp.Tp == mysql.TypeEnum || a.RetTp.Tp == mysql.TypeSet) &&
		(a.Name != ast.AggFuncFirstRow && a.Name != ast.AggFuncMax && a.Name != ast.AggFuncMin) {
		a.RetTp = &types.FieldType{Tp: mysql.TypeString, Flen: mysql.MaxFieldCharLength}
	}
}

func (a *baseFuncDesc) typeInfer4BitFuncs(ctx sessionctx.Context) {
	a.RetTp = types.NewFieldType(mysql.TypeLonglong)
	a.RetTp.Flen = 21
	types.SetBinChsClnFlag(a.RetTp)
	a.RetTp.Flag |= mysql.UnsignedFlag | mysql.NotNullFlag
	// TODO: a.Args[0] = expression.WrapWithCastAsInt(ctx, a.Args[0])
}

func (a *baseFuncDesc) typeInfer4JsonFuncs(ctx sessionctx.Context) {
	a.RetTp = types.NewFieldType(mysql.TypeJSON)
	types.SetBinChsClnFlag(a.RetTp)
}

func (a *baseFuncDesc) typeInfer4NumberFuncs() {
	a.RetTp = types.NewFieldType(mysql.TypeLonglong)
	a.RetTp.Flen = 21
	types.SetBinChsClnFlag(a.RetTp)
}

func (a *baseFuncDesc) typeInfer4CumeDist() {
	a.RetTp = types.NewFieldType(mysql.TypeDouble)
	a.RetTp.Flen, a.RetTp.Decimal = mysql.MaxRealWidth, mysql.NotFixedDec
}

func (a *baseFuncDesc) typeInfer4Ntile() {
	a.RetTp = types.NewFieldType(mysql.TypeLonglong)
	a.RetTp.Flen = 21
	types.SetBinChsClnFlag(a.RetTp)
	a.RetTp.Flag |= mysql.UnsignedFlag
}

func (a *baseFuncDesc) typeInfer4PercentRank() {
	a.RetTp = types.NewFieldType(mysql.TypeDouble)
	a.RetTp.Flag, a.RetTp.Decimal = mysql.MaxRealWidth, mysql.NotFixedDec
}

func (a *baseFuncDesc) typeInfer4LeadLag(ctx sessionctx.Context) {
	if len(a.Args) <= 2 {
		a.typeInfer4MaxMin(ctx)
	} else {
		// Merge the type of first and third argument.
		a.RetTp = expression.InferType4ControlFuncs(a.Args[0], a.Args[2])
	}
}

func (a *baseFuncDesc) typeInfer4PopOrSamp(ctx sessionctx.Context) {
	//var_pop/std/var_samp/stddev_samp's return value type is double
	a.RetTp = types.NewFieldType(mysql.TypeDouble)
	a.RetTp.Flen, a.RetTp.Decimal = mysql.MaxRealWidth, types.UnspecifiedLength
}

// GetDefaultValue gets the default value when the function's input is null.
// According to MySQL, default values of the function are listed as follows:
// e.g.
// Table t which is empty:
// +-------+---------+---------+
// | Table | Field   | Type    |
// +-------+---------+---------+
// | t     | a       | int(11) |
// +-------+---------+---------+
//
// Query: `select avg(a), sum(a), count(a), bit_xor(a), bit_or(a), bit_and(a), max(a), min(a), group_concat(a), approx_count_distinct(a), approx_percentile(a, 50) from test.t;`
// +--------+--------+----------+------------+-----------+----------------------+--------+--------+-----------------+--------------------------+--------------------------+
// | avg(a) | sum(a) | count(a) | bit_xor(a) | bit_or(a) | bit_and(a)           | max(a) | min(a) | group_concat(a) | approx_count_distinct(a) | approx_percentile(a, 50) |
// +--------+--------+----------+------------+-----------+----------------------+--------+--------+-----------------+--------------------------+--------------------------+
// |   NULL |   NULL |        0 |          0 |         0 | 18446744073709551615 |   NULL |   NULL | NULL            |                        0 |                     NULL |
// +--------+--------+----------+------------+-----------+----------------------+--------+--------+-----------------+--------------------------+--------------------------+
func (a *baseFuncDesc) GetDefaultValue() (v types.Datum) {
	switch a.Name {
	case ast.AggFuncCount, ast.AggFuncBitOr, ast.AggFuncBitXor:
		v = types.NewIntDatum(0)
	case ast.AggFuncApproxCountDistinct:
		if a.RetTp.Tp != mysql.TypeString {
			v = types.NewIntDatum(0)
		}
	case ast.AggFuncFirstRow, ast.AggFuncAvg, ast.AggFuncSum, ast.AggFuncMax,
		ast.AggFuncMin, ast.AggFuncGroupConcat, ast.AggFuncApproxPercentile:
		v = types.Datum{}
	case ast.AggFuncBitAnd:
		v = types.NewUintDatum(uint64(math.MaxUint64))
	}
	return
}

// We do not need to wrap cast upon these functions,
// since the EvalXXX method called by the arg is determined by the corresponding arg type.
var noNeedCastAggFuncs = map[string]struct{}{
	ast.AggFuncCount:               {},
	ast.AggFuncApproxCountDistinct: {},
	ast.AggFuncApproxPercentile:    {},
	ast.AggFuncMax:                 {},
	ast.AggFuncMin:                 {},
	ast.AggFuncFirstRow:            {},
	ast.WindowFuncNtile:            {},
	ast.AggFuncJsonObjectAgg:       {},
}

// WrapCastAsDecimalForAggArgs wraps the args of some specific aggregate functions
// with a cast as decimal function. See issue #19426
func (a *baseFuncDesc) WrapCastAsDecimalForAggArgs(ctx sessionctx.Context) {
	if a.Name == ast.AggFuncGroupConcat {
		for i := 0; i < len(a.Args)-1; i++ {
			if tp := a.Args[i].GetType(); tp.Tp == mysql.TypeNewDecimal {
				a.Args[i] = expression.BuildCastFunction(ctx, a.Args[i], tp)
			}
		}
	}
}

// WrapCastForAggArgs wraps the args of an aggregate function with a cast function.
func (a *baseFuncDesc) WrapCastForAggArgs(ctx sessionctx.Context) {
	if len(a.Args) == 0 {
		return
	}
	if _, ok := noNeedCastAggFuncs[a.Name]; ok {
		return
	}
	var castFunc func(ctx sessionctx.Context, expr expression.Expression) expression.Expression
	switch retTp := a.RetTp; retTp.EvalType() {
	case types.ETInt:
		castFunc = expression.WrapWithCastAsInt
	case types.ETReal:
		castFunc = expression.WrapWithCastAsReal
	case types.ETString:
		castFunc = expression.WrapWithCastAsString
	case types.ETDecimal:
		castFunc = expression.WrapWithCastAsDecimal
	case types.ETDatetime, types.ETTimestamp:
		castFunc = func(ctx sessionctx.Context, expr expression.Expression) expression.Expression {
			return expression.WrapWithCastAsTime(ctx, expr, retTp)
		}
	case types.ETDuration:
		castFunc = expression.WrapWithCastAsDuration
	case types.ETJson:
		castFunc = expression.WrapWithCastAsJSON
	default:
		panic("should never happen in baseFuncDesc.WrapCastForAggArgs")
	}
	for i := range a.Args {
		// Do not cast the second args of these functions, as they are simply non-negative numbers.
		if i == 1 && (a.Name == ast.WindowFuncLead || a.Name == ast.WindowFuncLag || a.Name == ast.WindowFuncNthValue) {
			continue
		}
		a.Args[i] = castFunc(ctx, a.Args[i])
		if a.Name != ast.AggFuncAvg && a.Name != ast.AggFuncSum {
			continue
		}
		// After wrapping cast on the argument, flen etc. may not the same
		// as the type of the aggregation function. The following part set
		// the type of the argument exactly as the type of the aggregation
		// function.
		// Note: If the `Tp` of argument is the same as the `Tp` of the
		// aggregation function, it will not wrap cast function on it
		// internally. The reason of the special handling for `Column` is
		// that the `RetType` of `Column` refers to the `infoschema`, so we
		// need to set a new variable for it to avoid modifying the
		// definition in `infoschema`.
		if col, ok := a.Args[i].(*expression.Column); ok {
			col.RetType = types.NewFieldType(col.RetType.Tp)
		}
		// originTp is used when the the `Tp` of column is TypeFloat32 while
		// the type of the aggregation function is TypeFloat64.
		originTp := a.Args[i].GetType().Tp
		*(a.Args[i].GetType()) = *(a.RetTp)
		a.Args[i].GetType().Tp = originTp
	}
}
