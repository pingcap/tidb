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
func buildSum(aggFuncDesc *aggregation.AggFuncDesc, ordinal int) AggFunc {
	return nil
}

// buildCount builds the AggFunc implementation for function "AVG".
func buildAvg(aggFuncDesc *aggregation.AggFuncDesc, ordinal int) AggFunc {
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
