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
	"math"
	"strings"

	"github.com/cznic/mathutil"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/charset"
)

// AggFuncDesc describes an aggregation function signature, only used in planner.
type AggFuncDesc struct {
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

// NewAggFuncDesc creates an aggregation function signature descriptor.
func NewAggFuncDesc(ctx context.Context, name string, args []expression.Expression, hasDistinct bool) *AggFuncDesc {
	a := &AggFuncDesc{
		Name:        strings.ToLower(name),
		Args:        args,
		HasDistinct: hasDistinct,
	}
	a.typeInfer(ctx)
	return a
}

// Equal checks whether two aggregation function signatures are equal.
func (a *AggFuncDesc) Equal(ctx context.Context, other *AggFuncDesc) bool {
	if a.Name != other.Name || a.HasDistinct != other.HasDistinct || len(a.Args) != len(other.Args) {
		return false
	}
	for i := range a.Args {
		if !a.Args[i].Equal(other.Args[i], ctx) {
			return false
		}
	}
	return true
}

// Clone copies an aggregation function signature totally.
func (a *AggFuncDesc) Clone() *AggFuncDesc {
	clone := *a
	for i := range a.Args {
		clone.Args[i] = a.Args[i].Clone()
	}
	return &clone
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
func (a *AggFuncDesc) typeInfer(ctx context.Context) {
	switch a.Name {
	case ast.AggFuncCount:
		a.typeInfer4Count(ctx)
	case ast.AggFuncSum:
		a.typeInfer4Sum(ctx)
	case ast.AggFuncAvg:
		a.typeInfer4Avg(ctx)
	case ast.AggFuncGroupConcat:
		a.typeInfer4GroupConcat(ctx)
	case ast.AggFuncMax, ast.AggFuncMin, ast.AggFuncFirstRow:
		a.typeInfer4MaxMin(ctx)
	case ast.AggFuncBitAnd, ast.AggFuncBitOr, ast.AggFuncBitXor:
		a.typeInfer4BitFuncs(ctx)
	default:
		panic("unsupported agg function: " + a.Name)
	}
}

// CalculateDefaultValue gets the default value when the aggregation function's input is null.
// The input stands for the schema of Aggregation's child. If the function can't produce a default value, the second
// return value will be false.
func (a *AggFuncDesc) CalculateDefaultValue(ctx context.Context, schema *expression.Schema) (types.Datum, bool) {
	switch a.Name {
	case ast.AggFuncCount:
		return a.calculateDefaultValue4Count(ctx, schema)
	case ast.AggFuncSum, ast.AggFuncMax, ast.AggFuncMin, ast.AggFuncFirstRow:
		return a.calculateDefaultValue4Sum(ctx, schema)
	case ast.AggFuncAvg, ast.AggFuncGroupConcat:
		return types.Datum{}, false
	case ast.AggFuncBitAnd:
		return a.calculateDefaultValue4BitAnd(ctx, schema)
	case ast.AggFuncBitOr, ast.AggFuncBitXor:
		return a.calculateDefaultValue4BitOr(ctx, schema)
	default:
		panic("unsupported agg function")
	}
}

// GetAggFunc gets an evaluator according to the aggregation function signature.
func (a *AggFuncDesc) GetAggFunc() Aggregation {
	aggFunc := aggFunction{AggFuncDesc: a}
	switch a.Name {
	case ast.AggFuncSum:
		return &sumFunction{aggFunction: aggFunc}
	case ast.AggFuncCount:
		return &countFunction{aggFunction: aggFunc}
	case ast.AggFuncAvg:
		return &avgFunction{aggFunction: aggFunc}
	case ast.AggFuncGroupConcat:
		return &concatFunction{aggFunction: aggFunc}
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

func (a *AggFuncDesc) typeInfer4Count(ctx context.Context) {
	a.RetTp = types.NewFieldType(mysql.TypeLonglong)
	a.RetTp.Flen = 21
	types.SetBinChsClnFlag(a.RetTp)
}

// For child returns integer or decimal type, "sum" should returns a "decimal", otherwise it returns a "double".
func (a *AggFuncDesc) typeInfer4Sum(ctx context.Context) {
	switch a.Args[0].GetType().Tp {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeNewDecimal:
		a.RetTp = types.NewFieldType(mysql.TypeNewDecimal)
		a.RetTp.Flen, a.RetTp.Decimal = mysql.MaxDecimalWidth, a.Args[0].GetType().Decimal
		if a.RetTp.Decimal < 0 || a.RetTp.Decimal > mysql.MaxDecimalScale {
			a.RetTp.Decimal = mysql.MaxDecimalScale
		}
		// TODO: a.Args[0] = expression.WrapWithCastAsDecimal(ctx, a.Args[0])
	default:
		a.RetTp = types.NewFieldType(mysql.TypeDouble)
		a.RetTp.Flen, a.RetTp.Decimal = mysql.MaxRealWidth, a.Args[0].GetType().Decimal
		//TODO: a.Args[0] = expression.WrapWithCastAsReal(ctx, a.Args[0])
	}
	types.SetBinChsClnFlag(a.RetTp)
}

// For child returns integer or decimal type, "avg" should returns a "decimal", otherwise it returns a "double".
func (a *AggFuncDesc) typeInfer4Avg(ctx context.Context) {
	switch a.Args[0].GetType().Tp {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong, mysql.TypeNewDecimal:
		a.RetTp = types.NewFieldType(mysql.TypeNewDecimal)
		if a.Args[0].GetType().Decimal < 0 {
			a.RetTp.Decimal = mysql.MaxDecimalScale
		} else {
			a.RetTp.Decimal = mathutil.Min(a.Args[0].GetType().Decimal+types.DivFracIncr, mysql.MaxDecimalScale)
		}
		a.RetTp.Flen = mysql.MaxDecimalWidth
		// TODO: a.Args[0] = expression.WrapWithCastAsDecimal(ctx, a.Args[0])
	default:
		a.RetTp = types.NewFieldType(mysql.TypeDouble)
		a.RetTp.Flen, a.RetTp.Decimal = mysql.MaxRealWidth, a.Args[0].GetType().Decimal
		// TODO: a.Args[0] = expression.WrapWithCastAsReal(ctx, a.Args[0])
	}
	types.SetBinChsClnFlag(a.RetTp)
}

func (a *AggFuncDesc) typeInfer4GroupConcat(ctx context.Context) {
	a.RetTp = types.NewFieldType(mysql.TypeVarString)
	a.RetTp.Charset = charset.CharsetUTF8
	a.RetTp.Collate = charset.CollationUTF8
	a.RetTp.Flen, a.RetTp.Decimal = mysql.MaxBlobWidth, 0
	// TODO: a.Args[i] = expression.WrapWithCastAsString(ctx, a.Args[i])
}

func (a *AggFuncDesc) typeInfer4MaxMin(ctx context.Context) {
	a.RetTp = a.Args[0].GetType()
}

func (a *AggFuncDesc) typeInfer4BitFuncs(ctx context.Context) {
	a.RetTp = types.NewFieldType(mysql.TypeLonglong)
	a.RetTp.Flen = 21
	types.SetBinChsClnFlag(a.RetTp)
	a.RetTp.Flag |= mysql.UnsignedFlag | mysql.NotNullFlag
	// TODO: a.Args[0] = expression.WrapWithCastAsInt(ctx, a.Args[0])
}

func (a *AggFuncDesc) calculateDefaultValue4Count(ctx context.Context, schema *expression.Schema) (types.Datum, bool) {
	for _, arg := range a.Args {
		result := expression.EvaluateExprWithNull(ctx, schema, arg)
		con, ok := result.(*expression.Constant)
		if !ok || con.Value.IsNull() {
			return types.Datum{}, ok
		}
	}
	return types.NewDatum(1), true
}

func (a *AggFuncDesc) calculateDefaultValue4Sum(ctx context.Context, schema *expression.Schema) (types.Datum, bool) {
	result := expression.EvaluateExprWithNull(ctx, schema, a.Args[0])
	con, ok := result.(*expression.Constant)
	if !ok || con.Value.IsNull() {
		return types.Datum{}, ok
	}
	return con.Value, true
}

func (a *AggFuncDesc) calculateDefaultValue4BitAnd(ctx context.Context, schema *expression.Schema) (types.Datum, bool) {
	result := expression.EvaluateExprWithNull(ctx, schema, a.Args[0])
	con, ok := result.(*expression.Constant)
	if !ok || con.Value.IsNull() {
		return types.NewDatum(uint64(math.MaxUint64)), true
	}
	return con.Value, true
}

func (a *AggFuncDesc) calculateDefaultValue4BitOr(ctx context.Context, schema *expression.Schema) (types.Datum, bool) {
	result := expression.EvaluateExprWithNull(ctx, schema, a.Args[0])
	con, ok := result.(*expression.Constant)
	if !ok || con.Value.IsNull() {
		return types.NewDatum(0), true
	}
	return con.Value, true
}
