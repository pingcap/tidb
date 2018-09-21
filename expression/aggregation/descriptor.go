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
	"strconv"
	"strings"

	"github.com/cznic/mathutil"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/charset"
)

// AggFuncDesc describes an aggregation function signature, only used in planner.
type AggFuncDesc struct {
	typeInferer AggFuncTypeInferer
	// Name represents the aggregation function name.
	Name string
	// Args represents the arguments of the aggregation function.
	Args []expression.Expression
	// RetTp represents the return type of the aggregation function.
	RetTp *types.FieldType
	// Mode represents the execution mode of the aggregation function.
	Mode AggFunctionMode
	// HasDistinct represents whether the aggregation function contains distinct attribute.
	HasDistinct bool
}

// AggFuncTypeInferer infers the type of aggregate functions.
type AggFuncTypeInferer struct {
}

// NewAggFuncDesc creates an aggregation function signature descriptor.
func NewAggFuncDesc(ctx sessionctx.Context, name string, args []expression.Expression, hasDistinct bool) *AggFuncDesc {
	a := &AggFuncDesc{
		Name:        strings.ToLower(name),
		Args:        args,
		HasDistinct: hasDistinct,
	}
	a.typeInfer(ctx)
	return a
}

// Equal checks whether two aggregation function signatures are equal.
func (a *AggFuncDesc) Equal(ctx sessionctx.Context, other *AggFuncDesc) bool {
	if a.Name != other.Name || a.HasDistinct != other.HasDistinct || len(a.Args) != len(other.Args) {
		return false
	}
	for i := range a.Args {
		if !a.Args[i].Equal(ctx, other.Args[i]) {
			return false
		}
	}
	return true
}

// Clone copies an aggregation function signature totally.
func (a *AggFuncDesc) Clone() *AggFuncDesc {
	clone := *a
	newTp := *a.RetTp
	clone.RetTp = &newTp
	for i := range a.Args {
		clone.Args[i] = a.Args[i].Clone()
	}
	return &clone
}

// Split splits `a` into two aggregate descriptors for partial phase and
// final phase individually.
// This function is only used when executing aggregate function parallelly.
// ordinal indicates the column ordinal of the intermediate result.
func (a *AggFuncDesc) Split(ordinal []int) (finalAggDesc *AggFuncDesc) {
	if a.Mode == CompleteMode {
		a.Mode = Partial1Mode
	} else if a.Mode == FinalMode {
		a.Mode = Partial2Mode
	} else {
		return
	}
	finalAggDesc = &AggFuncDesc{
		Name:        a.Name,
		Mode:        FinalMode, // We only support FinalMode now in final phase.
		HasDistinct: a.HasDistinct,
		RetTp:       a.RetTp,
	}
	switch a.Name {
	case ast.AggFuncAvg:
		args := make([]expression.Expression, 0, 2)
		args = append(args, &expression.Column{
			ColName: model.NewCIStr(fmt.Sprintf("avg_final_col_%d", ordinal[0])),
			Index:   ordinal[0],
			RetType: types.NewFieldType(mysql.TypeLonglong),
		})
		args = append(args, &expression.Column{
			ColName: model.NewCIStr(fmt.Sprintf("avg_final_col_%d", ordinal[1])),
			Index:   ordinal[1],
			RetType: a.RetTp,
		})
		finalAggDesc.Args = args
	default:
		args := make([]expression.Expression, 0, 1)
		args = append(args, &expression.Column{
			ColName: model.NewCIStr(fmt.Sprintf("%s_final_col_%d", a.Name, ordinal[0])),
			Index:   ordinal[0],
			RetType: a.RetTp,
		})
		finalAggDesc.Args = args
		if finalAggDesc.Name == ast.AggFuncGroupConcat {
			finalAggDesc.Args = append(finalAggDesc.Args, a.Args[len(a.Args)-1]) // separator
		}
	}
	return finalAggDesc
}

// String implements the fmt.Stringer interface.
func (a *AggFuncDesc) String() string {
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

// typeInfer infers the arguments and return types of an aggregation function.
func (a *AggFuncDesc) typeInfer(ctx sessionctx.Context) {
	switch a.Name {
	case ast.AggFuncCount:
		a.RetTp = a.typeInferer.InferCount(ctx)
	case ast.AggFuncSum:
		//TODO: a.Args[0] = expression.WrapWithCastAsReal(ctx, a.Args[0])
		a.RetTp = a.typeInferer.InferSum(ctx, a.Args[0])
	case ast.AggFuncAvg:
		a.RetTp = a.typeInferer.InferAvg(ctx, a.Args[0])
		// TODO: a.Args[0] = expression.WrapWithCastAsDecimal(ctx, a.Args[0])
	case ast.AggFuncGroupConcat:
		a.RetTp = a.typeInferer.InferGroupConcat(ctx)
		// TODO: a.Args[i] = expression.WrapWithCastAsString(ctx, a.Args[i])
	case ast.AggFuncMax, ast.AggFuncMin, ast.AggFuncFirstRow:
		a.RetTp = a.typeInferer.InferMaxMin(ctx, a.Args[0])
	case ast.AggFuncBitAnd, ast.AggFuncBitOr, ast.AggFuncBitXor:
		a.RetTp = a.typeInferer.InferBitFuncs(ctx)
		// TODO: a.Args[0] = expression.WrapWithCastAsInt(ctx, a.Args[0])
	default:
		panic("unsupported agg function: " + a.Name)
	}
}

// EvalNullValueInOuterJoin gets the null value when the aggregation is upon an outer join,
// and the aggregation function's input is null.
// If there is no matching row for the inner table of an outer join,
// an aggregation function only involves constant and/or columns belongs to the inner table
// will be set to the null value.
// The input stands for the schema of Aggregation's child. If the function can't produce a null value, the second
// return value will be false.
// e.g.
// Table t with only one row:
// +-------+---------+---------+
// | Table | Field   | Type    |
// +-------+---------+---------+
// | t     | a       | int(11) |
// +-------+---------+---------+
// +------+
// | a    |
// +------+
// |    1 |
// +------+
//
// Table s which is empty:
// +-------+---------+---------+
// | Table | Field   | Type    |
// +-------+---------+---------+
// | s     | a       | int(11) |
// +-------+---------+---------+
//
// Query: `select t.a as `t.a`,  count(95), sum(95), avg(95), bit_or(95), bit_and(95), bit_or(95), max(95), min(95), s.a as `s.a`, avg(95) from t left join s on t.a = s.a;`
// +------+-----------+---------+---------+------------+-------------+------------+---------+---------+------+----------+
// | t.a  | count(95) | sum(95) | avg(95) | bit_or(95) | bit_and(95) | bit_or(95) | max(95) | min(95) | s.a  | avg(s.a) |
// +------+-----------+---------+---------+------------+-------------+------------+---------+---------+------+----------+
// |    1 |         1 |      95 | 95.0000 |         95 |          95 |         95 |      95 |      95 | NULL |     NULL |
// +------+-----------+---------+---------+------------+-------------+------------+---------+---------+------+----------+
func (a *AggFuncDesc) EvalNullValueInOuterJoin(ctx sessionctx.Context, schema *expression.Schema) (types.Datum, bool) {
	switch a.Name {
	case ast.AggFuncCount:
		return a.evalNullValueInOuterJoin4Count(ctx, schema)
	case ast.AggFuncSum, ast.AggFuncMax, ast.AggFuncMin,
		ast.AggFuncFirstRow:
		return a.evalNullValueInOuterJoin4Sum(ctx, schema)
	case ast.AggFuncAvg, ast.AggFuncGroupConcat:
		return types.Datum{}, false
	case ast.AggFuncBitAnd:
		return a.evalNullValueInOuterJoin4BitAnd(ctx, schema)
	case ast.AggFuncBitOr, ast.AggFuncBitXor:
		return a.evalNullValueInOuterJoin4BitOr(ctx, schema)
	default:
		panic("unsupported agg function")
	}
}

// GetDefaultValue gets the default value when the aggregation function's input is null.
// According to MySQL, default values of the aggregation function are listed as follows:
// e.g.
// Table t which is empty:
// +-------+---------+---------+
// | Table | Field   | Type    |
// +-------+---------+---------+
// | t     | a       | int(11) |
// +-------+---------+---------+
//
// Query: `select a, avg(a), sum(a), count(a), bit_xor(a), bit_or(a), bit_and(a), max(a), min(a), group_concat(a) from t;`
// +------+--------+--------+----------+------------+-----------+----------------------+--------+--------+-----------------+
// | a    | avg(a) | sum(a) | count(a) | bit_xor(a) | bit_or(a) | bit_and(a)           | max(a) | min(a) | group_concat(a) |
// +------+--------+--------+----------+------------+-----------+----------------------+--------+--------+-----------------+
// | NULL |   NULL |   NULL |        0 |          0 |         0 | 18446744073709551615 |   NULL |   NULL | NULL            |
// +------+--------+--------+----------+------------+-----------+----------------------+--------+--------+-----------------+
func (a *AggFuncDesc) GetDefaultValue() (v types.Datum) {
	switch a.Name {
	case ast.AggFuncCount, ast.AggFuncBitOr, ast.AggFuncBitXor:
		v = types.NewIntDatum(0)
	case ast.AggFuncFirstRow, ast.AggFuncAvg, ast.AggFuncSum, ast.AggFuncMax,
		ast.AggFuncMin, ast.AggFuncGroupConcat:
		v = types.Datum{}
	case ast.AggFuncBitAnd:
		v = types.NewUintDatum(uint64(math.MaxUint64))
	}
	return
}

// GetAggFunc gets an evaluator according to the aggregation function signature.
func (a *AggFuncDesc) GetAggFunc(ctx sessionctx.Context) Aggregation {
	aggFunc := aggFunction{AggFuncDesc: a}
	switch a.Name {
	case ast.AggFuncSum:
		return &sumFunction{aggFunction: aggFunc}
	case ast.AggFuncCount:
		return &countFunction{aggFunction: aggFunc}
	case ast.AggFuncAvg:
		return &avgFunction{aggFunction: aggFunc}
	case ast.AggFuncGroupConcat:
		var s string
		var err error
		var maxLen uint64
		s, err = variable.GetSessionSystemVar(ctx.GetSessionVars(), variable.GroupConcatMaxLen)
		if err != nil {
			panic(fmt.Sprintf("Error happened when GetAggFunc: no system variable named '%s'", variable.GroupConcatMaxLen))
		}
		maxLen, err = strconv.ParseUint(s, 10, 64)
		if err != nil {
			panic(fmt.Sprintf("Error happened when GetAggFunc: illegal value for system variable named '%s'", variable.GroupConcatMaxLen))
		}
		return &concatFunction{aggFunction: aggFunc, maxLen: maxLen}
	case ast.AggFuncMax:
		return &maxMinFunction{aggFunction: aggFunc, isMax: true}
	case ast.AggFuncMin:
		return &maxMinFunction{aggFunction: aggFunc, isMax: false}
	case ast.AggFuncFirstRow:
		return &firstRowFunction{aggFunction: aggFunc}
	case ast.AggFuncBitOr:
		return &bitOrFunction{aggFunction: aggFunc}
	case ast.AggFuncBitXor:
		return &bitXorFunction{aggFunction: aggFunc}
	case ast.AggFuncBitAnd:
		return &bitAndFunction{aggFunction: aggFunc}
	default:
		panic("unsupported agg function")
	}
}

// InferCount infers the type of COUNT function.
func (a *AggFuncTypeInferer) InferCount(ctx sessionctx.Context) (retTp *types.FieldType) {
	retTp = types.NewFieldType(mysql.TypeLonglong)
	retTp.Flen = 21
	types.SetBinChsClnFlag(retTp)
	return
}

// InferSum infers the type of SUM function. It should returns a "decimal" for exact numeric values, otherwise it returns a "double".
func (a *AggFuncTypeInferer) InferSum(ctx sessionctx.Context, arg expression.Expression) (retTp *types.FieldType) {
	switch arg.GetType().Tp {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeNewDecimal:
		retTp = types.NewFieldType(mysql.TypeNewDecimal)
		retTp.Flen, retTp.Decimal = mysql.MaxDecimalWidth, arg.GetType().Decimal
		if retTp.Decimal < 0 || retTp.Decimal > mysql.MaxDecimalScale {
			retTp.Decimal = mysql.MaxDecimalScale
		}
		// TODO: cast arg as expression.WrapWithCastAsDecimal(ctx, arg)
	default:
		retTp = types.NewFieldType(mysql.TypeDouble)
		retTp.Flen, retTp.Decimal = mysql.MaxRealWidth, arg.GetType().Decimal
		// TODO: cast arg as expression.WrapWithCastAsDecimal(ctx, arg)
	}
	types.SetBinChsClnFlag(retTp)
	return retTp
}

// InferAvg infers the type of AVG function. It should returns a "decimal" for exact numeric values, otherwise it returns a "double".
func (a *AggFuncTypeInferer) InferAvg(ctx sessionctx.Context, arg expression.Expression) (retTp *types.FieldType) {
	switch arg.GetType().Tp {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeNewDecimal:
		retTp = types.NewFieldType(mysql.TypeNewDecimal)
		if arg.GetType().Decimal < 0 {
			retTp.Decimal = mysql.MaxDecimalScale
		} else {
			retTp.Decimal = mathutil.Min(arg.GetType().Decimal+types.DivFracIncr, mysql.MaxDecimalScale)
		}
		retTp.Flen = mysql.MaxDecimalWidth
		// TODO: arg = expression.WrapWithCastAsDecimal(ctx, arg)
	default:
		retTp = types.NewFieldType(mysql.TypeDouble)
		retTp.Flen, retTp.Decimal = mysql.MaxRealWidth, arg.GetType().Decimal
		// TODO: arg = expression.WrapWithCastAsReal(ctx, arg)
	}
	types.SetBinChsClnFlag(retTp)
	return
}

// InferGroupConcat infers type of GROUP_CONCAT function.
func (a *AggFuncTypeInferer) InferGroupConcat(ctx sessionctx.Context) (retTp *types.FieldType) {
	retTp = types.NewFieldType(mysql.TypeVarString)
	retTp.Charset = charset.CharsetUTF8
	retTp.Collate = charset.CollationUTF8
	retTp.Flen, retTp.Decimal = mysql.MaxBlobWidth, 0
	return
}

// InferMaxMin infers type of MAX/MIN/FIRST_ROW function.
func (a *AggFuncTypeInferer) InferMaxMin(ctx sessionctx.Context, arg expression.Expression) (retTp *types.FieldType) {
	_, argIsScalaFunc := arg.(*expression.ScalarFunction)
	if argIsScalaFunc && arg.GetType().Tp == mysql.TypeFloat {
		// For scalar function, the result of "float32" is set to the "float64"
		// field in the "Datum". If we do not wrap a cast-as-double function on arg,
		// error would happen when extracting the evaluation of arg to a ProjectionExec.
		tp := types.NewFieldType(mysql.TypeDouble)
		tp.Flen, tp.Decimal = mysql.MaxRealWidth, types.UnspecifiedLength
		types.SetBinChsClnFlag(tp)
		arg = expression.BuildCastFunction(ctx, arg, tp)
	}
	retTp = arg.GetType()
	if retTp.Tp == mysql.TypeEnum || retTp.Tp == mysql.TypeSet {
		retTp = &types.FieldType{Tp: mysql.TypeString, Flen: mysql.MaxFieldCharLength}
	}
	return retTp
}

// InferBitFuncs infers type of bit functions, such as BIT_XOR, BIT_OR ...
func (a *AggFuncTypeInferer) InferBitFuncs(ctx sessionctx.Context) (retTp *types.FieldType) {
	retTp = types.NewFieldType(mysql.TypeLonglong)
	retTp.Flen = 21
	types.SetBinChsClnFlag(retTp)
	retTp.Flag |= mysql.UnsignedFlag | mysql.NotNullFlag
	// TODO: a.Args[0] = expression.WrapWithCastAsInt(ctx, a.Args[0])
	return
}

func (a *AggFuncDesc) evalNullValueInOuterJoin4Count(ctx sessionctx.Context, schema *expression.Schema) (types.Datum, bool) {
	for _, arg := range a.Args {
		result := expression.EvaluateExprWithNull(ctx, schema, arg)
		con, ok := result.(*expression.Constant)
		if !ok || con.Value.IsNull() {
			return types.Datum{}, ok
		}
	}
	return types.NewDatum(1), true
}

func (a *AggFuncDesc) evalNullValueInOuterJoin4Sum(ctx sessionctx.Context, schema *expression.Schema) (types.Datum, bool) {
	result := expression.EvaluateExprWithNull(ctx, schema, a.Args[0])
	con, ok := result.(*expression.Constant)
	if !ok || con.Value.IsNull() {
		return types.Datum{}, ok
	}
	return con.Value, true
}

func (a *AggFuncDesc) evalNullValueInOuterJoin4BitAnd(ctx sessionctx.Context, schema *expression.Schema) (types.Datum, bool) {
	result := expression.EvaluateExprWithNull(ctx, schema, a.Args[0])
	con, ok := result.(*expression.Constant)
	if !ok || con.Value.IsNull() {
		return types.NewDatum(uint64(math.MaxUint64)), true
	}
	return con.Value, true
}

func (a *AggFuncDesc) evalNullValueInOuterJoin4BitOr(ctx sessionctx.Context, schema *expression.Schema) (types.Datum, bool) {
	result := expression.EvaluateExprWithNull(ctx, schema, a.Args[0])
	con, ok := result.(*expression.Constant)
	if !ok || con.Value.IsNull() {
		return types.NewDatum(0), true
	}
	return con.Value, true
}
