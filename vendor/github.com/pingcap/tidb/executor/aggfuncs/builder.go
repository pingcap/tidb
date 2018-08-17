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

package aggfuncs

import (
	"fmt"
	"strconv"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

// Build is used to build a specific AggFunc implementation according to the
// input aggFuncDesc.
func Build(ctx sessionctx.Context, aggFuncDesc *aggregation.AggFuncDesc, ordinal int) AggFunc {
	switch aggFuncDesc.Name {
	case ast.AggFuncCount:
		return buildCount(aggFuncDesc, ordinal)
	case ast.AggFuncSum:
		return buildSum(aggFuncDesc, ordinal)
	case ast.AggFuncAvg:
		return buildAvg(aggFuncDesc, ordinal)
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
	}

	// If HasDistinct and mode is CompleteMode or Partial1Mode, we should
	// use countOriginalWithDistinct.
	if aggFuncDesc.HasDistinct &&
		(aggFuncDesc.Mode == aggregation.CompleteMode || aggFuncDesc.Mode == aggregation.Partial1Mode) {
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
func buildSum(aggFuncDesc *aggregation.AggFuncDesc, ordinal int) AggFunc {
	base := baseSumAggFunc{
		baseAggFunc: baseAggFunc{
			args:    aggFuncDesc.Args,
			ordinal: ordinal,
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
			return &sum4Float64{base}
		}
	}
}

// buildAvg builds the AggFunc implementation for function "AVG".
func buildAvg(aggFuncDesc *aggregation.AggFuncDesc, ordinal int) AggFunc {
	base := baseAggFunc{
		args:    aggFuncDesc.Args,
		ordinal: ordinal,
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
			return &avgOriginal4Float64{baseAvgFloat64{base}}
		}

	// Build avg functions which consume the partial result of other avg
	// functions and update their partial results.
	case aggregation.Partial2Mode, aggregation.FinalMode:
		switch aggFuncDesc.RetTp.Tp {
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
	}

	evalType, fieldType := aggFuncDesc.RetTp.EvalType(), aggFuncDesc.RetTp
	if fieldType.Tp == mysql.TypeBit {
		evalType = types.ETString
	}
	switch aggFuncDesc.Mode {
	case aggregation.DedupMode:
	default:
		switch evalType {
		case types.ETInt:
			return &firstRow4Int{base}
		case types.ETReal:
			switch fieldType.Tp {
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
		},
		isMax: isMax,
	}

	evalType, fieldType := aggFuncDesc.RetTp.EvalType(), aggFuncDesc.RetTp
	if fieldType.Tp == mysql.TypeBit {
		evalType = types.ETString
	}
	switch aggFuncDesc.Mode {
	case aggregation.DedupMode:
	default:
		switch evalType {
		case types.ETInt:
			if mysql.HasUnsignedFlag(fieldType.Flag) {
				return &maxMin4Uint{base}
			}
			return &maxMin4Int{base}
		case types.ETReal:
			switch fieldType.Tp {
			case mysql.TypeFloat:
				return &maxMin4Float32{base}
			case mysql.TypeDouble:
				return &maxMin4Float64{base}
			}
		case types.ETDecimal:
			return &maxMin4Decimal{base}
		case types.ETString:
			return &maxMin4String{base}
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

// buildGroupConcat builds the AggFunc implementation for function "GROUP_CONCAT".
func buildGroupConcat(ctx sessionctx.Context, aggFuncDesc *aggregation.AggFuncDesc, ordinal int) AggFunc {
	switch aggFuncDesc.Mode {
	case aggregation.DedupMode:
		return nil
	default:
		base := baseAggFunc{
			args:    aggFuncDesc.Args[:len(aggFuncDesc.Args)-1],
			ordinal: ordinal,
		}
		// The last arg is promised to be a not-null string constant, so the error can be ignored.
		c, _ := aggFuncDesc.Args[len(aggFuncDesc.Args)-1].(*expression.Constant)
		sep, _, err := c.EvalString(nil, chunk.Row{})
		// This err should never happen.
		if err != nil {
			panic(fmt.Sprintf("Error happened when buildGroupConcat: %s", errors.Trace(err).Error()))
		}
		var s string
		s, err = variable.GetSessionSystemVar(ctx.GetSessionVars(), variable.GroupConcatMaxLen)
		if err != nil {
			panic(fmt.Sprintf("Error happened when buildGroupConcat: no system variable named '%s'", variable.GroupConcatMaxLen))
		}
		maxLen, err := strconv.ParseUint(s, 10, 64)
		// Should never happen
		if err != nil {
			panic(fmt.Sprintf("Error happened when buildGroupConcat: %s", errors.Trace(err).Error()))
		}
		if aggFuncDesc.HasDistinct {
			return &groupConcatDistinct{baseGroupConcat4String{baseAggFunc: base, sep: sep, maxLen: maxLen}}
		}
		return &groupConcat{baseGroupConcat4String{baseAggFunc: base, sep: sep, maxLen: maxLen}}
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
