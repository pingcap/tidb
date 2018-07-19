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
	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/expression/aggregation"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/types"
)

// Build is used to build a specific AggFunc implementation according to the
// input aggFuncDesc.
func Build(aggFuncDesc *aggregation.AggFuncDesc, ordinal int) AggFunc {
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
		return buildGroupConcat(aggFuncDesc, ordinal)
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
	return nil
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
		switch aggFuncDesc.Args[0].GetType().Tp {
		case mysql.TypeNewDecimal:
			if aggFuncDesc.HasDistinct {
				return &avgOriginal4DistinctDecimal{base}
			}
			return &avgOriginal4Decimal{baseAvgDecimal{base}}
		case mysql.TypeFloat, mysql.TypeDouble:
			if aggFuncDesc.HasDistinct {
				return &avgOriginal4DistinctFloat64{base}
			}
			return &avgOriginal4Float64{baseAvgFloat64{base}}
		}

	// Build avg functions which consume the partial result of other avg
	// functions and update their partial results.
	case aggregation.Partial2Mode, aggregation.FinalMode:
		switch aggFuncDesc.Args[1].GetType().Tp {
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
func buildGroupConcat(aggFuncDesc *aggregation.AggFuncDesc, ordinal int) AggFunc {
	// TODO: There might be different kind of types of the args,
	// we should add CastAsString upon every arg after cast can be pushed down to coprocessor.
	// And this check can be removed at that time.
	for _, arg := range aggFuncDesc.Args {
		if arg.GetType().EvalType() != types.ETString {
			return nil
		}
	}
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
		sep, _, err := c.EvalString(nil, nil)
		// This err should never happen.
		if err != nil {
			panic(fmt.Sprintf("Error happened when buildGroupConcat: %s", errors.Trace(err).Error()))
		}
		if aggFuncDesc.HasDistinct {
			return &groupConcatDistinct{baseGroupConcat4String{baseAggFunc: base, sep: sep}}
		}
		return &groupConcat{baseGroupConcat4String{baseAggFunc: base, sep: sep}}
	}
}

// buildBitOr builds the AggFunc implementation for function "BIT_OR".
func buildBitOr(aggFuncDesc *aggregation.AggFuncDesc, ordinal int) AggFunc {
	switch aggFuncDesc.Args[0].GetType().EvalType() {
	case types.ETInt:
		base := baseAggFunc{
			args:    aggFuncDesc.Args,
			ordinal: ordinal,
		}
		return &bitOrUint64{baseBitAggFunc{base}}
	}
	return nil
}

// buildBitXor builds the AggFunc implementation for function "BIT_XOR".
func buildBitXor(aggFuncDesc *aggregation.AggFuncDesc, ordinal int) AggFunc {
	switch aggFuncDesc.Args[0].GetType().EvalType() {
	case types.ETInt:
		base := baseAggFunc{
			args:    aggFuncDesc.Args,
			ordinal: ordinal,
		}
		return &bitXorUint64{baseBitAggFunc{base}}
	}
	return nil
}

// buildBitAnd builds the AggFunc implementation for function "BIT_AND".
func buildBitAnd(aggFuncDesc *aggregation.AggFuncDesc, ordinal int) AggFunc {
	switch aggFuncDesc.Args[0].GetType().EvalType() {
	case types.ETInt:
		base := baseAggFunc{
			args:    aggFuncDesc.Args,
			ordinal: ordinal,
		}
		return &bitAndUint64{baseBitAggFunc{base}}
	}
	return nil
}
