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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package aggregation

import (
	"bytes"
	"fmt"
	"math"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/mathutil"
	"github.com/pingcap/tidb/pkg/util/size"
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

func newBaseFuncDesc(ctx expression.BuildContext, name string, args []expression.Expression) (baseFuncDesc, error) {
	b := baseFuncDesc{Name: strings.ToLower(name), Args: args}
	err := b.TypeInfer(ctx)
	return b, err
}

func (a *baseFuncDesc) equal(ctx expression.EvalContext, other *baseFuncDesc) bool {
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

// TypeInfer infers the arguments and return types of an function.
func (a *baseFuncDesc) TypeInfer(ctx expression.BuildContext) error {
	switch a.Name {
	case ast.AggFuncCount:
		a.typeInfer4Count()
	case ast.AggFuncApproxCountDistinct:
		a.typeInfer4ApproxCountDistinct()
	case ast.AggFuncApproxPercentile:
		return a.typeInfer4ApproxPercentile(ctx.GetEvalCtx())
	case ast.AggFuncSum:
		a.typeInfer4Sum(ctx.GetEvalCtx())
	case ast.AggFuncAvg:
		a.typeInfer4Avg(ctx.GetEvalCtx())
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
		a.typeInfer4PopOrSamp()
	case ast.AggFuncJsonArrayagg:
		a.typeInfer4JsonArrayAgg()
	case ast.AggFuncJsonObjectAgg:
		return a.typeInfer4JsonObjectAgg(ctx)
	default:
		return errors.Errorf("unsupported agg function: %s", a.Name)
	}
	return nil
}

func (a *baseFuncDesc) typeInfer4Count() {
	a.RetTp = types.NewFieldType(mysql.TypeLonglong)
	a.RetTp.SetFlen(21)
	a.RetTp.SetDecimal(0)
	// count never returns null
	a.RetTp.AddFlag(mysql.NotNullFlag)
	types.SetBinChsClnFlag(a.RetTp)
}

func (a *baseFuncDesc) typeInfer4ApproxCountDistinct() {
	a.typeInfer4Count()
}

func (a *baseFuncDesc) typeInfer4ApproxPercentile(ctx expression.EvalContext) error {
	if len(a.Args) != 2 {
		return errors.New("APPROX_PERCENTILE should take 2 arguments")
	}

	if a.Args[1].ConstLevel() == expression.ConstNone {
		return errors.New("APPROX_PERCENTILE should take a constant expression as percentage argument")
	}
	percent, isNull, err := a.Args[1].EvalInt(ctx, chunk.Row{})
	if err != nil {
		return fmt.Errorf("APPROX_PERCENTILE: Invalid argument %s", a.Args[1].String())
	}
	if percent <= 0 || percent > 100 || isNull {
		if isNull {
			return errors.New("APPROX_PERCENTILE: Percentage value cannot be NULL")
		}
		return fmt.Errorf("Percentage value %d is out of range [1, 100]", percent)
	}

	switch a.Args[0].GetType(ctx).GetType() {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		a.RetTp = types.NewFieldType(mysql.TypeLonglong)
	case mysql.TypeDouble, mysql.TypeFloat:
		a.RetTp = types.NewFieldType(mysql.TypeDouble)
	case mysql.TypeNewDecimal:
		a.RetTp = types.NewFieldType(mysql.TypeNewDecimal)
		a.RetTp.SetFlen(mysql.MaxDecimalWidth)
		a.RetTp.SetDecimal(a.Args[0].GetType(ctx).GetDecimal())
		if a.RetTp.GetDecimal() < 0 || a.RetTp.GetDecimal() > mysql.MaxDecimalScale {
			a.RetTp.SetDecimal(mysql.MaxDecimalScale)
		}
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeNewDate, mysql.TypeTimestamp:
		a.RetTp = a.Args[0].GetType(ctx).Clone()
	default:
		a.RetTp = a.Args[0].GetType(ctx).Clone()
		a.RetTp.DelFlag(mysql.NotNullFlag)
	}
	return nil
}

// typeInfer4Sum should return a "decimal", otherwise it returns a "double".
// Because child returns integer or decimal type.
func (a *baseFuncDesc) typeInfer4Sum(ctx expression.EvalContext) {
	switch a.Args[0].GetType(ctx).GetType() {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeYear:
		a.RetTp = types.NewFieldType(mysql.TypeNewDecimal)
		a.RetTp.SetFlenUnderLimit(a.Args[0].GetType(ctx).GetFlen() + 21)
		a.RetTp.SetDecimal(0)
		if a.Args[0].GetType(ctx).GetFlen() < 0 {
			a.RetTp.SetFlen(mysql.MaxDecimalWidth)
		}
	case mysql.TypeNewDecimal:
		a.RetTp = types.NewFieldType(mysql.TypeNewDecimal)
		a.RetTp.UpdateFlenAndDecimalUnderLimit(a.Args[0].GetType(ctx), 0, 22)
	case mysql.TypeDouble, mysql.TypeFloat:
		a.RetTp = types.NewFieldType(mysql.TypeDouble)
		a.RetTp.SetFlen(mysql.MaxRealWidth)
		a.RetTp.SetDecimal(a.Args[0].GetType(ctx).GetDecimal())
	default:
		a.RetTp = types.NewFieldType(mysql.TypeDouble)
		a.RetTp.SetFlen(mysql.MaxRealWidth)
		a.RetTp.SetDecimal(types.UnspecifiedLength)
	}
	types.SetBinChsClnFlag(a.RetTp)
}

// TypeInfer4AvgSum infers the type of sum from avg, which should extend the precision of decimal
// compatible with mysql.
func (a *baseFuncDesc) TypeInfer4AvgSum(avgRetType *types.FieldType) {
	if avgRetType.GetType() == mysql.TypeNewDecimal {
		a.RetTp.SetFlen(mathutil.Min(mysql.MaxDecimalWidth, a.RetTp.GetFlen()+22))
	}
}

// TypeInfer4FinalCount infers the type of sum agg which is rewritten from final count agg run on MPP mode.
func (a *baseFuncDesc) TypeInfer4FinalCount(finalCountRetType *types.FieldType) {
	a.RetTp = finalCountRetType.Clone()
}

// typeInfer4Avg should returns a "decimal", otherwise it returns a "double".
// Because child returns integer or decimal type.
func (a *baseFuncDesc) typeInfer4Avg(ctx expression.EvalContext) {
	divPrecIncre := ctx.GetDivPrecisionIncrement()
	switch a.Args[0].GetType(ctx).GetType() {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong:
		a.RetTp = types.NewFieldType(mysql.TypeNewDecimal)
		a.RetTp.SetDecimalUnderLimit(divPrecIncre)
		flen, _ := mysql.GetDefaultFieldLengthAndDecimal(a.Args[0].GetType(ctx).GetType())
		a.RetTp.SetFlenUnderLimit(flen + divPrecIncre)
	case mysql.TypeYear, mysql.TypeNewDecimal:
		a.RetTp = types.NewFieldType(mysql.TypeNewDecimal)
		a.RetTp.UpdateFlenAndDecimalUnderLimit(a.Args[0].GetType(ctx), divPrecIncre, divPrecIncre)
	case mysql.TypeDouble, mysql.TypeFloat:
		a.RetTp = types.NewFieldType(mysql.TypeDouble)
		a.RetTp.SetFlen(mysql.MaxRealWidth)
		a.RetTp.SetDecimal(a.Args[0].GetType(ctx).GetDecimal())
	case mysql.TypeDate, mysql.TypeDuration, mysql.TypeDatetime, mysql.TypeTimestamp:
		a.RetTp = types.NewFieldType(mysql.TypeDouble)
		a.RetTp.SetFlen(mysql.MaxRealWidth)
		a.RetTp.SetDecimal(4)
	default:
		a.RetTp = types.NewFieldType(mysql.TypeDouble)
		a.RetTp.SetFlen(mysql.MaxRealWidth)
		a.RetTp.SetDecimal(types.UnspecifiedLength)
	}
	types.SetBinChsClnFlag(a.RetTp)
}

func (a *baseFuncDesc) typeInfer4GroupConcat(ctx expression.BuildContext) {
	a.RetTp = types.NewFieldType(mysql.TypeVarString)
	charset, collate := charset.GetDefaultCharsetAndCollate()
	a.RetTp.SetCharset(charset)
	a.RetTp.SetCollate(collate)

	a.RetTp.SetFlen(mysql.MaxBlobWidth)
	a.RetTp.SetDecimal(0)
	// TODO: a.Args[i] = expression.WrapWithCastAsString(ctx, a.Args[i])
	for i := 0; i < len(a.Args)-1; i++ {
		if tp := a.Args[i].GetType(ctx.GetEvalCtx()); tp.GetType() == mysql.TypeNewDecimal {
			a.Args[i] = expression.BuildCastFunction(ctx, a.Args[i], tp)
		}
	}
}

func (a *baseFuncDesc) typeInfer4MaxMin(ctx expression.BuildContext) {
	_, argIsScalaFunc := a.Args[0].(*expression.ScalarFunction)
	if argIsScalaFunc && a.Args[0].GetType(ctx.GetEvalCtx()).GetType() == mysql.TypeFloat {
		// For scalar function, the result of "float32" is set to the "float64"
		// field in the "Datum". If we do not wrap a cast-as-double function on a.Args[0],
		// error would happen when extracting the evaluation of a.Args[0] to a ProjectionExec.
		tp := types.NewFieldType(mysql.TypeDouble)
		tp.SetFlen(mysql.MaxRealWidth)
		tp.SetDecimal(types.UnspecifiedLength)
		types.SetBinChsClnFlag(tp)
		a.Args[0] = expression.BuildCastFunction(ctx, a.Args[0], tp)
	}
	a.RetTp = a.Args[0].GetType(ctx.GetEvalCtx())
	if a.Name == ast.AggFuncMax || a.Name == ast.AggFuncMin ||
		a.Name == ast.WindowFuncLead || a.Name == ast.WindowFuncLag {
		a.RetTp = a.Args[0].GetType(ctx.GetEvalCtx()).Clone()
		a.RetTp.DelFlag(mysql.NotNullFlag)
	}
	// issue #13027, #13961
	if (a.RetTp.GetType() == mysql.TypeEnum || a.RetTp.GetType() == mysql.TypeSet) &&
		(a.Name != ast.AggFuncFirstRow && a.Name != ast.AggFuncMax && a.Name != ast.AggFuncMin) {
		a.RetTp = types.NewFieldTypeBuilder().SetType(mysql.TypeString).SetFlen(mysql.MaxFieldCharLength).BuildP()
	}
}

func (a *baseFuncDesc) typeInfer4BitFuncs(ctx expression.BuildContext) {
	a.RetTp = types.NewFieldType(mysql.TypeLonglong)
	a.RetTp.SetFlen(21)
	types.SetBinChsClnFlag(a.RetTp)
	a.RetTp.AddFlag(mysql.UnsignedFlag | mysql.NotNullFlag)
	a.Args[0] = expression.WrapWithCastAsInt(ctx, a.Args[0])
}

func (a *baseFuncDesc) typeInfer4JsonArrayAgg() {
	a.RetTp = types.NewFieldType(mysql.TypeJSON)
	types.SetBinChsClnFlag(a.RetTp)
}

func (a *baseFuncDesc) typeInfer4JsonObjectAgg(ctx expression.BuildContext) error {
	a.RetTp = types.NewFieldType(mysql.TypeJSON)
	types.SetBinChsClnFlag(a.RetTp)
	a.Args[0] = expression.WrapWithCastAsString(ctx, a.Args[0])
	return nil
}

func (a *baseFuncDesc) typeInfer4NumberFuncs() {
	a.RetTp = types.NewFieldType(mysql.TypeLonglong)
	a.RetTp.SetFlen(21)
	types.SetBinChsClnFlag(a.RetTp)
}

func (a *baseFuncDesc) typeInfer4CumeDist() {
	a.RetTp = types.NewFieldType(mysql.TypeDouble)
	a.RetTp.SetFlen(mysql.MaxRealWidth)
	a.RetTp.SetDecimal(mysql.NotFixedDec)
}

func (a *baseFuncDesc) typeInfer4Ntile() {
	a.RetTp = types.NewFieldType(mysql.TypeLonglong)
	a.RetTp.SetFlen(21)
	types.SetBinChsClnFlag(a.RetTp)
	a.RetTp.AddFlag(mysql.UnsignedFlag)
}

func (a *baseFuncDesc) typeInfer4PercentRank() {
	a.RetTp = types.NewFieldType(mysql.TypeDouble)
	a.RetTp.SetFlag(mysql.MaxRealWidth)
	a.RetTp.SetDecimal(mysql.NotFixedDec)
}

func (a *baseFuncDesc) typeInfer4LeadLag(ctx expression.BuildContext) {
	if len(a.Args) < 3 {
		a.typeInfer4MaxMin(ctx)
	} else {
		// Merge the type of first and third argument.
		// FIXME: select lead(b collate utf8mb4_unicode_ci, 1, 'lead' collate utf8mb4_general_ci) over() as a from t; should report error.
		a.RetTp, _ = expression.InferType4ControlFuncs(ctx, a.Name, a.Args[0], a.Args[2])
	}
}

func (a *baseFuncDesc) typeInfer4PopOrSamp() {
	// var_pop/std/var_samp/stddev_samp's return value type is double
	a.RetTp = types.NewFieldType(mysql.TypeDouble)
	a.RetTp.SetFlen(mysql.MaxRealWidth)
	a.RetTp.SetDecimal(types.UnspecifiedLength)
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
		if a.RetTp.GetType() != mysql.TypeString {
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
	ast.AggFuncJsonArrayagg:        {},
	ast.AggFuncJsonObjectAgg:       {},
}

// WrapCastForAggArgs wraps the args of an aggregate function with a cast function.
func (a *baseFuncDesc) WrapCastForAggArgs(ctx expression.BuildContext) {
	if len(a.Args) == 0 {
		return
	}
	if _, ok := noNeedCastAggFuncs[a.Name]; ok {
		return
	}
	var castFunc func(ctx expression.BuildContext, expr expression.Expression) expression.Expression
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
		castFunc = func(ctx expression.BuildContext, expr expression.Expression) expression.Expression {
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
		if a.Args[i].GetType(ctx.GetEvalCtx()).GetType() == mysql.TypeNull {
			continue
		}
		a.Args[i] = castFunc(ctx, a.Args[i])
	}
}

// MemoryUsage return the memory usage of baseFuncDesc
func (a *baseFuncDesc) MemoryUsage() (sum int64) {
	if a == nil {
		return
	}

	sum = size.SizeOfString + int64(len(a.Name))
	if a.RetTp != nil {
		sum += a.RetTp.MemoryUsage()
	}
	for _, expr := range a.Args {
		sum += expr.MemoryUsage()
	}
	return
}
