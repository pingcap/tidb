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

package aggfuncs

import (
	"fmt"
	"strconv"

	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// Build is used to build a specific AggFunc implementation according to the
// input aggFuncDesc.
func Build(ctx sessionctx.Context, aggFuncDesc *aggregation.AggFuncDesc, ordinal int) AggFunc {
	switch aggFuncDesc.Name {
	case ast.AggFuncCount:
		return buildCount(aggFuncDesc, ordinal)
	case ast.AggFuncSum:
		return buildSum(ctx, aggFuncDesc, ordinal)
	case ast.AggFuncAvg:
		return buildAvg(ctx, aggFuncDesc, ordinal)
	case ast.AggFuncFirstRow:
		return buildFirstRow(aggFuncDesc, ordinal)
	case ast.AggFuncMax:
		return buildMaxMin(aggFuncDesc, ordinal, true)
	case ast.AggFuncMin:
		return buildMaxMin(aggFuncDesc, ordinal, false)
	case ast.AggFuncGroupConcat:
		return buildGroupConcat(ctx, aggFuncDesc, ordinal)
	case ast.AggFuncBitOr:
		return buildBitOr(aggFuncDesc, ordinal)
	case ast.AggFuncBitXor:
		return buildBitXor(aggFuncDesc, ordinal)
	case ast.AggFuncBitAnd:
		return buildBitAnd(aggFuncDesc, ordinal)
	case ast.AggFuncVarPop:
		return buildVarPop(aggFuncDesc, ordinal)
	case ast.AggFuncStddevPop:
		return buildStdDevPop(aggFuncDesc, ordinal)
	case ast.AggFuncJsonArrayagg:
		return buildJSONArrayagg(aggFuncDesc, ordinal)
	case ast.AggFuncJsonObjectAgg:
		return buildJSONObjectAgg(aggFuncDesc, ordinal)
	case ast.AggFuncApproxCountDistinct:
		return buildApproxCountDistinct(aggFuncDesc, ordinal)
	case ast.AggFuncApproxPercentile:
		return buildApproxPercentile(ctx, aggFuncDesc, ordinal)
	case ast.AggFuncVarSamp:
		return buildVarSamp(aggFuncDesc, ordinal)
	case ast.AggFuncStddevSamp:
		return buildStddevSamp(aggFuncDesc, ordinal)
	}
	return nil
}

// BuildWindowFunctions builds specific window function according to function description and order by columns.
func BuildWindowFunctions(ctx sessionctx.Context, windowFuncDesc *aggregation.AggFuncDesc, ordinal int, orderByCols []*expression.Column) AggFunc {
	switch windowFuncDesc.Name {
	case ast.WindowFuncRank:
		return buildRank(ordinal, orderByCols, false)
	case ast.WindowFuncDenseRank:
		return buildRank(ordinal, orderByCols, true)
	case ast.WindowFuncRowNumber:
		return buildRowNumber(windowFuncDesc, ordinal)
	case ast.WindowFuncFirstValue:
		return buildFirstValue(windowFuncDesc, ordinal)
	case ast.WindowFuncLastValue:
		return buildLastValue(windowFuncDesc, ordinal)
	case ast.WindowFuncCumeDist:
		return buildCumeDist(ordinal, orderByCols)
	case ast.WindowFuncNthValue:
		return buildNthValue(windowFuncDesc, ordinal)
	case ast.WindowFuncNtile:
		return buildNtile(windowFuncDesc, ordinal)
	case ast.WindowFuncPercentRank:
		return buildPercentRank(ordinal, orderByCols)
	case ast.WindowFuncLead:
		return buildLead(ctx, windowFuncDesc, ordinal)
	case ast.WindowFuncLag:
		return buildLag(ctx, windowFuncDesc, ordinal)
	case ast.AggFuncMax:
		// The max/min aggFunc using in the window function will using the sliding window algo.
		return buildMaxMinInWindowFunction(windowFuncDesc, ordinal, true)
	case ast.AggFuncMin:
		return buildMaxMinInWindowFunction(windowFuncDesc, ordinal, false)
	default:
		return Build(ctx, windowFuncDesc, ordinal)
	}
}

func buildApproxCountDistinct(aggFuncDesc *aggregation.AggFuncDesc, ordinal int) AggFunc {
	base := baseApproxCountDistinct{baseAggFunc{
		args:    aggFuncDesc.Args,
		ordinal: ordinal,
	}}

	// In partition table, union need to compute partial result into partial result.
	// We can detect and handle this case by checking whether return type is string.

	switch aggFuncDesc.RetTp.GetType() {
	case mysql.TypeLonglong:
		switch aggFuncDesc.Mode {
		case aggregation.CompleteMode:
			return &approxCountDistinctOriginal{base}
		case aggregation.Partial1Mode:
			return &approxCountDistinctPartial1{approxCountDistinctOriginal{base}}
		case aggregation.Partial2Mode:
			return &approxCountDistinctPartial2{approxCountDistinctPartial1{approxCountDistinctOriginal{base}}}
		case aggregation.FinalMode:
			return &approxCountDistinctFinal{approxCountDistinctPartial2{approxCountDistinctPartial1{approxCountDistinctOriginal{base}}}}
		}
	case mysql.TypeString:
		switch aggFuncDesc.Mode {
		case aggregation.CompleteMode, aggregation.Partial1Mode:
			return &approxCountDistinctPartial1{approxCountDistinctOriginal{base}}
		case aggregation.Partial2Mode, aggregation.FinalMode:
			return &approxCountDistinctPartial2{approxCountDistinctPartial1{approxCountDistinctOriginal{base}}}
		}
	}

	return nil
}

func buildApproxPercentile(sctx sessionctx.Context, aggFuncDesc *aggregation.AggFuncDesc, ordinal int) AggFunc {
	if aggFuncDesc.Mode == aggregation.DedupMode {
		return nil
	}

	// Checked while building descriptor
	percent, _, err := aggFuncDesc.Args[1].EvalInt(sctx, chunk.Row{})
	if err != nil {
		// Should not reach here
		logutil.BgLogger().Error("Error happened when buildApproxPercentile", zap.Error(err))
		return nil
	}

	base := basePercentile{percent: int(percent), baseAggFunc: baseAggFunc{args: aggFuncDesc.Args, ordinal: ordinal}}

	evalType := aggFuncDesc.Args[0].GetType().EvalType()
	if aggFuncDesc.Args[0].GetType().GetType() == mysql.TypeBit {
		evalType = types.ETString // same as other aggregate function
	}
	switch aggFuncDesc.Mode {
	case aggregation.CompleteMode, aggregation.Partial1Mode, aggregation.FinalMode:
		switch evalType {
		case types.ETInt:
			return &percentileOriginal4Int{base}
		case types.ETReal:
			return &percentileOriginal4Real{base}
		case types.ETDecimal:
			return &percentileOriginal4Decimal{base}
		case types.ETDatetime, types.ETTimestamp:
			return &percentileOriginal4Time{base}
		case types.ETDuration:
			return &percentileOriginal4Duration{base}
		default:
			// Return NULL in any case
			return &base
		}
	}

	return nil
}

// buildCount builds the AggFunc implementation for function "COUNT".
func buildCount(aggFuncDesc *aggregation.AggFuncDesc, ordinal int) AggFunc {
	// If mode is DedupMode, we return nil for not implemented.
	if aggFuncDesc.Mode == aggregation.DedupMode {
		return nil // not implemented yet.
	}

	base := baseAggFunc{
		args:    aggFuncDesc.Args,
		ordinal: ordinal,
		retTp:   aggFuncDesc.RetTp,
	}

	// If HasDistinct and mode is CompleteMode or Partial1Mode, we should
	// use countOriginalWithDistinct.
	if aggFuncDesc.HasDistinct &&
		(aggFuncDesc.Mode == aggregation.CompleteMode || aggFuncDesc.Mode == aggregation.Partial1Mode) {
		if len(base.args) == 1 {
			// optimize with single column
			// TODO: because Time and JSON does not have `hashcode()` or similar method
			// so they're in exception for now.
			// TODO: add hashCode method for all evaluate types (decimal, Time, Duration, JSON).
			// https://github.com/pingcap/tidb/issues/15857
			switch aggFuncDesc.Args[0].GetType().EvalType() {
			case types.ETInt:
				return &countOriginalWithDistinct4Int{baseCount{base}}
			case types.ETReal:
				return &countOriginalWithDistinct4Real{baseCount{base}}
			case types.ETDecimal:
				return &countOriginalWithDistinct4Decimal{baseCount{base}}
			case types.ETDuration:
				return &countOriginalWithDistinct4Duration{baseCount{base}}
			case types.ETString:
				return &countOriginalWithDistinct4String{baseCount{base}}
			}
		}
		return &countOriginalWithDistinct{baseCount{base}}
	}

	switch aggFuncDesc.Mode {
	case aggregation.CompleteMode, aggregation.Partial1Mode:
		switch aggFuncDesc.Args[0].GetType().EvalType() {
		case types.ETInt:
			return &countOriginal4Int{baseCount{base}}
		case types.ETReal:
			return &countOriginal4Real{baseCount{base}}
		case types.ETDecimal:
			return &countOriginal4Decimal{baseCount{base}}
		case types.ETTimestamp, types.ETDatetime:
			return &countOriginal4Time{baseCount{base}}
		case types.ETDuration:
			return &countOriginal4Duration{baseCount{base}}
		case types.ETJson:
			return &countOriginal4JSON{baseCount{base}}
		case types.ETString:
			return &countOriginal4String{baseCount{base}}
		}
	case aggregation.Partial2Mode, aggregation.FinalMode:
		return &countPartial{baseCount{base}}
	}

	return nil
}

// buildSum builds the AggFunc implementation for function "SUM".
func buildSum(ctx sessionctx.Context, aggFuncDesc *aggregation.AggFuncDesc, ordinal int) AggFunc {
	base := baseSumAggFunc{
		baseAggFunc: baseAggFunc{
			args:    aggFuncDesc.Args,
			ordinal: ordinal,
			retTp:   aggFuncDesc.RetTp,
		},
	}
	switch aggFuncDesc.Mode {
	case aggregation.DedupMode:
		return nil
	default:
		switch aggFuncDesc.RetTp.EvalType() {
		case types.ETDecimal:
			if aggFuncDesc.HasDistinct {
				return &sum4DistinctDecimal{base}
			}
			return &sum4Decimal{base}
		default:
			if aggFuncDesc.HasDistinct {
				return &sum4DistinctFloat64{base}
			}
			if ctx.GetSessionVars().WindowingUseHighPrecision {
				return &sum4Float64HighPrecision{baseSum4Float64{base}}
			}
			return &sum4Float64{baseSum4Float64{base}}
		}
	}
}

// buildAvg builds the AggFunc implementation for function "AVG".
func buildAvg(ctx sessionctx.Context, aggFuncDesc *aggregation.AggFuncDesc, ordinal int) AggFunc {
	base := baseAggFunc{
		args:    aggFuncDesc.Args,
		ordinal: ordinal,
		retTp:   aggFuncDesc.RetTp,
	}
	switch aggFuncDesc.Mode {
	// Build avg functions which consume the original data and remove the
	// duplicated input of the same group.
	case aggregation.DedupMode:
		return nil // not implemented yet.

	// Build avg functions which consume the original data and update their
	// partial results.
	case aggregation.CompleteMode, aggregation.Partial1Mode:
		switch aggFuncDesc.RetTp.EvalType() {
		case types.ETDecimal:
			if aggFuncDesc.HasDistinct {
				return &avgOriginal4DistinctDecimal{base}
			}
			return &avgOriginal4Decimal{baseAvgDecimal{base}}
		default:
			if aggFuncDesc.HasDistinct {
				return &avgOriginal4DistinctFloat64{base}
			}
			if ctx.GetSessionVars().WindowingUseHighPrecision {
				return &avgOriginal4Float64HighPrecision{baseAvgFloat64{base}}
			}
			return &avgOriginal4Float64{avgOriginal4Float64HighPrecision{baseAvgFloat64{base}}}
		}

	// Build avg functions which consume the partial result of other avg
	// functions and update their partial results.
	case aggregation.Partial2Mode, aggregation.FinalMode:
		switch aggFuncDesc.RetTp.GetType() {
		case mysql.TypeNewDecimal:
			return &avgPartial4Decimal{baseAvgDecimal{base}}
		case mysql.TypeDouble:
			return &avgPartial4Float64{baseAvgFloat64{base}}
		}
	}
	return nil
}

// buildFirstRow builds the AggFunc implementation for function "FIRST_ROW".
func buildFirstRow(aggFuncDesc *aggregation.AggFuncDesc, ordinal int) AggFunc {
	base := baseAggFunc{
		args:    aggFuncDesc.Args,
		ordinal: ordinal,
		retTp:   aggFuncDesc.RetTp,
	}
	evalType, fieldType := aggFuncDesc.RetTp.EvalType(), aggFuncDesc.RetTp
	if fieldType.GetType() == mysql.TypeBit {
		evalType = types.ETString
	}
	switch aggFuncDesc.Mode {
	case aggregation.DedupMode:
	default:
		switch fieldType.GetType() {
		case mysql.TypeEnum:
			return &firstRow4Enum{base}
		case mysql.TypeSet:
			return &firstRow4Set{base}
		}

		switch evalType {
		case types.ETInt:
			return &firstRow4Int{base}
		case types.ETReal:
			switch fieldType.GetType() {
			case mysql.TypeFloat:
				return &firstRow4Float32{base}
			case mysql.TypeDouble:
				return &firstRow4Float64{base}
			}
		case types.ETDecimal:
			return &firstRow4Decimal{base}
		case types.ETDatetime, types.ETTimestamp:
			return &firstRow4Time{base}
		case types.ETDuration:
			return &firstRow4Duration{base}
		case types.ETString:
			return &firstRow4String{base}
		case types.ETJson:
			return &firstRow4JSON{base}
		}
	}
	return nil
}

// buildMaxMin builds the AggFunc implementation for function "MAX" and "MIN".
func buildMaxMin(aggFuncDesc *aggregation.AggFuncDesc, ordinal int, isMax bool) AggFunc {
	base := baseMaxMinAggFunc{
		baseAggFunc: baseAggFunc{
			args:    aggFuncDesc.Args,
			ordinal: ordinal,
			retTp:   aggFuncDesc.RetTp,
		},
		isMax:    isMax,
		collator: collate.GetCollator(aggFuncDesc.RetTp.GetCollate()),
	}
	evalType, fieldType := aggFuncDesc.RetTp.EvalType(), aggFuncDesc.RetTp
	if fieldType.GetType() == mysql.TypeBit {
		evalType = types.ETString
	}
	switch aggFuncDesc.Mode {
	case aggregation.DedupMode:
	default:
		switch fieldType.GetType() {
		case mysql.TypeEnum:
			return &maxMin4Enum{base}
		case mysql.TypeSet:
			return &maxMin4Set{base}
		}

		switch evalType {
		case types.ETInt:
			if mysql.HasUnsignedFlag(fieldType.GetFlag()) {
				return &maxMin4Uint{base}
			}
			return &maxMin4Int{base}
		case types.ETReal:
			switch fieldType.GetType() {
			case mysql.TypeFloat:
				return &maxMin4Float32{base}
			case mysql.TypeDouble:
				return &maxMin4Float64{base}
			}
		case types.ETDecimal:
			return &maxMin4Decimal{base}
		case types.ETString:
			return &maxMin4String{baseMaxMinAggFunc: base, retTp: aggFuncDesc.RetTp}
		case types.ETDatetime, types.ETTimestamp:
			return &maxMin4Time{base}
		case types.ETDuration:
			return &maxMin4Duration{base}
		case types.ETJson:
			return &maxMin4JSON{base}
		}
	}
	return nil
}

// buildMaxMin builds the AggFunc implementation for function "MAX" and "MIN" using by window function.
func buildMaxMinInWindowFunction(aggFuncDesc *aggregation.AggFuncDesc, ordinal int, isMax bool) AggFunc {
	base := buildMaxMin(aggFuncDesc, ordinal, isMax)
	// build max/min aggFunc for window function using sliding window
	switch baseAggFunc := base.(type) {
	case *maxMin4Int:
		return &maxMin4IntSliding{*baseAggFunc, windowInfo{}}
	case *maxMin4Uint:
		return &maxMin4UintSliding{*baseAggFunc, windowInfo{}}
	case *maxMin4Float32:
		return &maxMin4Float32Sliding{*baseAggFunc, windowInfo{}}
	case *maxMin4Float64:
		return &maxMin4Float64Sliding{*baseAggFunc, windowInfo{}}
	case *maxMin4Decimal:
		return &maxMin4DecimalSliding{*baseAggFunc, windowInfo{}}
	case *maxMin4String:
		return &maxMin4StringSliding{*baseAggFunc, windowInfo{}}
	case *maxMin4Time:
		return &maxMin4TimeSliding{*baseAggFunc, windowInfo{}}
	case *maxMin4Duration:
		return &maxMin4DurationSliding{*baseAggFunc, windowInfo{}}
	}
	return base
}

// buildGroupConcat builds the AggFunc implementation for function "GROUP_CONCAT".
func buildGroupConcat(ctx sessionctx.Context, aggFuncDesc *aggregation.AggFuncDesc, ordinal int) AggFunc {
	switch aggFuncDesc.Mode {
	case aggregation.DedupMode:
		return nil
	default:
		// The last arg is promised to be a not-null string constant, so the error can be ignored.
		c, _ := aggFuncDesc.Args[len(aggFuncDesc.Args)-1].(*expression.Constant)
		sep, _, err := c.EvalString(nil, chunk.Row{})
		// This err should never happen.
		if err != nil {
			panic(fmt.Sprintf("Error happened when buildGroupConcat: %s", err.Error()))
		}
		var s string
		s, err = variable.GetSessionOrGlobalSystemVar(ctx.GetSessionVars(), variable.GroupConcatMaxLen)
		if err != nil {
			panic(fmt.Sprintf("Error happened when buildGroupConcat: no system variable named '%s'", variable.GroupConcatMaxLen))
		}
		maxLen, err := strconv.ParseUint(s, 10, 64)
		// Should never happen
		if err != nil {
			panic(fmt.Sprintf("Error happened when buildGroupConcat: %s", err.Error()))
		}
		var truncated int32
		base := baseGroupConcat4String{
			baseAggFunc: baseAggFunc{
				args:    aggFuncDesc.Args[:len(aggFuncDesc.Args)-1],
				ordinal: ordinal,
			},
			byItems:   aggFuncDesc.OrderByItems,
			sep:       sep,
			maxLen:    maxLen,
			truncated: &truncated,
		}
		if aggFuncDesc.HasDistinct {
			if len(aggFuncDesc.OrderByItems) > 0 {
				return &groupConcatDistinctOrder{base}
			}
			return &groupConcatDistinct{base}
		}
		if len(aggFuncDesc.OrderByItems) > 0 {
			return &groupConcatOrder{base}
		}
		return &groupConcat{base}
	}
}

// buildBitOr builds the AggFunc implementation for function "BIT_OR".
func buildBitOr(aggFuncDesc *aggregation.AggFuncDesc, ordinal int) AggFunc {
	base := baseAggFunc{
		args:    aggFuncDesc.Args,
		ordinal: ordinal,
	}
	return &bitOrUint64{baseBitAggFunc{base}}
}

// buildBitXor builds the AggFunc implementation for function "BIT_XOR".
func buildBitXor(aggFuncDesc *aggregation.AggFuncDesc, ordinal int) AggFunc {
	base := baseAggFunc{
		args:    aggFuncDesc.Args,
		ordinal: ordinal,
	}
	return &bitXorUint64{baseBitAggFunc{base}}
}

// buildBitAnd builds the AggFunc implementation for function "BIT_AND".
func buildBitAnd(aggFuncDesc *aggregation.AggFuncDesc, ordinal int) AggFunc {
	base := baseAggFunc{
		args:    aggFuncDesc.Args,
		ordinal: ordinal,
	}
	return &bitAndUint64{baseBitAggFunc{base}}
}

// buildVarPop builds the AggFunc implementation for function "VAR_POP".
func buildVarPop(aggFuncDesc *aggregation.AggFuncDesc, ordinal int) AggFunc {
	base := baseVarPopAggFunc{
		baseAggFunc{
			args:    aggFuncDesc.Args,
			ordinal: ordinal,
		},
	}
	switch aggFuncDesc.Mode {
	case aggregation.DedupMode:
		return nil
	default:
		if aggFuncDesc.HasDistinct {
			return &varPop4DistinctFloat64{base}
		}
		return &varPop4Float64{base}
	}
}

// buildStdDevPop builds the AggFunc implementation for function "STD()/STDDEV()/STDDEV_POP()"
func buildStdDevPop(aggFuncDesc *aggregation.AggFuncDesc, ordinal int) AggFunc {
	base := baseVarPopAggFunc{
		baseAggFunc{
			args:    aggFuncDesc.Args,
			ordinal: ordinal,
		},
	}
	switch aggFuncDesc.Mode {
	case aggregation.DedupMode:
		return nil
	default:
		if aggFuncDesc.HasDistinct {
			return &stdDevPop4DistinctFloat64{varPop4DistinctFloat64{base}}
		}
		return &stdDevPop4Float64{varPop4Float64{base}}
	}
}

// buildVarSamp builds the AggFunc implementation for function "VAR_SAMP()"
func buildVarSamp(aggFuncDesc *aggregation.AggFuncDesc, ordinal int) AggFunc {
	base := baseVarPopAggFunc{
		baseAggFunc{
			args:    aggFuncDesc.Args,
			ordinal: ordinal,
		},
	}
	switch aggFuncDesc.Mode {
	case aggregation.DedupMode:
		return nil
	default:
		if aggFuncDesc.HasDistinct {
			return &varSamp4DistinctFloat64{varPop4DistinctFloat64{base}}
		}
		return &varSamp4Float64{varPop4Float64{base}}
	}
}

// buildStddevSamp builds the AggFunc implementation for function "STDDEV_SAMP()"
func buildStddevSamp(aggFuncDesc *aggregation.AggFuncDesc, ordinal int) AggFunc {
	base := baseVarPopAggFunc{
		baseAggFunc{
			args:    aggFuncDesc.Args,
			ordinal: ordinal,
		},
	}
	switch aggFuncDesc.Mode {
	case aggregation.DedupMode:
		return nil
	default:
		if aggFuncDesc.HasDistinct {
			return &stddevSamp4DistinctFloat64{varPop4DistinctFloat64{base}}
		}
		return &stddevSamp4Float64{varPop4Float64{base}}
	}
}

// buildJSONArrayagg builds the AggFunc implementation for function "json_arrayagg".
func buildJSONArrayagg(aggFuncDesc *aggregation.AggFuncDesc, ordinal int) AggFunc {
	base := baseAggFunc{
		args:    aggFuncDesc.Args,
		ordinal: ordinal,
	}
	switch aggFuncDesc.Mode {
	case aggregation.DedupMode:
		return nil
	default:
		return &jsonArrayagg{base}
	}
}

// buildJSONObjectAgg builds the AggFunc implementation for function "json_objectagg".
func buildJSONObjectAgg(aggFuncDesc *aggregation.AggFuncDesc, ordinal int) AggFunc {
	base := baseAggFunc{
		args:    aggFuncDesc.Args,
		ordinal: ordinal,
	}
	switch aggFuncDesc.Mode {
	case aggregation.DedupMode:
		return nil
	default:
		return &jsonObjectAgg{base}
	}
}

// buildRowNumber builds the AggFunc implementation for function "ROW_NUMBER".
func buildRowNumber(aggFuncDesc *aggregation.AggFuncDesc, ordinal int) AggFunc {
	base := baseAggFunc{
		args:    aggFuncDesc.Args,
		ordinal: ordinal,
	}
	return &rowNumber{base}
}

func buildRank(ordinal int, orderByCols []*expression.Column, isDense bool) AggFunc {
	base := baseAggFunc{
		ordinal: ordinal,
	}
	r := &rank{baseAggFunc: base, isDense: isDense, rowComparer: buildRowComparer(orderByCols)}
	return r
}

func buildFirstValue(aggFuncDesc *aggregation.AggFuncDesc, ordinal int) AggFunc {
	base := baseAggFunc{
		args:    aggFuncDesc.Args,
		ordinal: ordinal,
	}
	return &firstValue{baseAggFunc: base, tp: aggFuncDesc.RetTp}
}

func buildLastValue(aggFuncDesc *aggregation.AggFuncDesc, ordinal int) AggFunc {
	base := baseAggFunc{
		args:    aggFuncDesc.Args,
		ordinal: ordinal,
	}
	return &lastValue{baseAggFunc: base, tp: aggFuncDesc.RetTp}
}

func buildCumeDist(ordinal int, orderByCols []*expression.Column) AggFunc {
	base := baseAggFunc{
		ordinal: ordinal,
	}
	r := &cumeDist{baseAggFunc: base, rowComparer: buildRowComparer(orderByCols)}
	return r
}

func buildNthValue(aggFuncDesc *aggregation.AggFuncDesc, ordinal int) AggFunc {
	base := baseAggFunc{
		args:    aggFuncDesc.Args,
		ordinal: ordinal,
	}
	// Already checked when building the function description.
	nth, _, _ := expression.GetUint64FromConstant(aggFuncDesc.Args[1])
	return &nthValue{baseAggFunc: base, tp: aggFuncDesc.RetTp, nth: nth}
}

func buildNtile(aggFuncDes *aggregation.AggFuncDesc, ordinal int) AggFunc {
	base := baseAggFunc{
		args:    aggFuncDes.Args,
		ordinal: ordinal,
	}
	n, _, _ := expression.GetUint64FromConstant(aggFuncDes.Args[0])
	return &ntile{baseAggFunc: base, n: n}
}

func buildPercentRank(ordinal int, orderByCols []*expression.Column) AggFunc {
	base := baseAggFunc{
		ordinal: ordinal,
	}
	return &percentRank{baseAggFunc: base, rowComparer: buildRowComparer(orderByCols)}
}

func buildLeadLag(ctx sessionctx.Context, aggFuncDesc *aggregation.AggFuncDesc, ordinal int) baseLeadLag {
	offset := uint64(1)
	if len(aggFuncDesc.Args) >= 2 {
		offset, _, _ = expression.GetUint64FromConstant(aggFuncDesc.Args[1])
	}
	var defaultExpr expression.Expression
	defaultExpr = expression.NewNull()
	if len(aggFuncDesc.Args) == 3 {
		defaultExpr = aggFuncDesc.Args[2]
		switch et := defaultExpr.(type) {
		case *expression.Constant:
			res, err1 := et.Value.ConvertTo(ctx.GetSessionVars().StmtCtx, aggFuncDesc.RetTp)
			if err1 == nil {
				defaultExpr = &expression.Constant{Value: res, RetType: aggFuncDesc.RetTp}
			}
		}
	}
	base := baseAggFunc{
		args:    aggFuncDesc.Args,
		ordinal: ordinal,
	}
	ve, _ := buildValueEvaluator(aggFuncDesc.RetTp)
	return baseLeadLag{baseAggFunc: base, offset: offset, defaultExpr: defaultExpr, valueEvaluator: ve}
}

func buildLead(ctx sessionctx.Context, aggFuncDesc *aggregation.AggFuncDesc, ordinal int) AggFunc {
	return &lead{buildLeadLag(ctx, aggFuncDesc, ordinal)}
}

func buildLag(ctx sessionctx.Context, aggFuncDesc *aggregation.AggFuncDesc, ordinal int) AggFunc {
	return &lag{buildLeadLag(ctx, aggFuncDesc, ordinal)}
}
