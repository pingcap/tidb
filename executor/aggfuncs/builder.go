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
	"github.com/pingcap/tidb/ast"
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
		return buildMax(aggFuncDesc, ordinal)
	case ast.AggFuncMin:
		return buildMin(aggFuncDesc, ordinal)
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
	return nil
}

// buildCount builds the AggFunc implementation for function "SUM".
func buildSum(aggFuncDesc *aggregation.AggFuncDesc, ordinal int, isNull bool) AggFunc {
	base := baseSumAggFunc{
		baseAggFunc: baseAggFunc{
			args:    aggFuncDesc.Args,
			ordinal: ordinal,
		},
	}
	evalType, fieldType := aggFuncDesc.RetTp.EvalType(), aggFuncDesc.RetTp
	switch aggFuncDesc.Mode {
	case aggregation.DedupMode:
	default:
		switch evalType {
		case types.ETInt:
			return nil
		case types.ETReal:
			switch fieldType.Tp {
			case mysql.TypeFloat:
				return &sumAggFunc4Float64{base}
			case mysql.TypeDouble:
				return &sumAggFunc4Float64{base}
			}
		case types.ETDecimal:
			return &sumAggFunc4Decimal{base}
		default:
			return nil
		}
	}
	return nil
}

// buildCount builds the AggFunc implementation for function "AVG".
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
				return nil // not implemented yet.
			}
			return &avgOriginal4Decimal{baseAvgDecimal{base}}
		case mysql.TypeFloat, mysql.TypeDouble:
			if aggFuncDesc.HasDistinct {
				return nil // not implemented yet.
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

// buildCount builds the AggFunc implementation for function "FIRST_ROW".
func buildFirstRow(aggFuncDesc *aggregation.AggFuncDesc, ordinal int) AggFunc {
	return nil
}

// buildCount builds the AggFunc implementation for function "MAX".
func buildMax(aggFuncDesc *aggregation.AggFuncDesc, ordinal int) AggFunc {
	return nil
}

// buildCount builds the AggFunc implementation for function "MIN".
func buildMin(aggFuncDesc *aggregation.AggFuncDesc, ordinal int) AggFunc {
	return nil
}

// buildCount builds the AggFunc implementation for function "GROUP_CONCAT".
func buildGroupConcat(aggFuncDesc *aggregation.AggFuncDesc, ordinal int) AggFunc {
	return nil
}

// buildCount builds the AggFunc implementation for function "BIT_OR".
func buildBitOr(aggFuncDesc *aggregation.AggFuncDesc, ordinal int) AggFunc {
	// BIT_OR doesn't need to handle the distinct property.
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

// buildCount builds the AggFunc implementation for function "BIT_XOR".
func buildBitXor(aggFuncDesc *aggregation.AggFuncDesc, ordinal int) AggFunc {
	return nil
}

// buildCount builds the AggFunc implementation for function "BIT_AND".
func buildBitAnd(aggFuncDesc *aggregation.AggFuncDesc, ordinal int) AggFunc {
	return nil
}
