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
func Build(aggFuncDesc *aggregation.AggFuncDesc, output []int) AggFunc {
	switch aggFuncDesc.Name {
	case ast.AggFuncCount:
		return buildCount(aggFuncDesc, output)
	case ast.AggFuncSum:
		return buildSum(aggFuncDesc, output)
	case ast.AggFuncAvg:
		return buildAvg(aggFuncDesc, output)
	case ast.AggFuncFirstRow:
		return buildFirstRow(aggFuncDesc, output)
	case ast.AggFuncMax:
		return buildMax(aggFuncDesc, output)
	case ast.AggFuncMin:
		return buildMin(aggFuncDesc, output)
	case ast.AggFuncGroupConcat:
		return buildGroupConcat(aggFuncDesc, output)
	case ast.AggFuncBitOr:
		return buildBitOr(aggFuncDesc, output)
	case ast.AggFuncBitXor:
		return buildBitXor(aggFuncDesc, output)
	case ast.AggFuncBitAnd:
		return buildBitAnd(aggFuncDesc, output)
	}
	return nil
}

// buildCount builds the AggFunc implementation for function "COUNT".
func buildCount(aggFuncDesc *aggregation.AggFuncDesc, output []int) AggFunc {
	return nil
}

// buildCount builds the AggFunc implementation for function "SUM".
func buildSum(aggFuncDesc *aggregation.AggFuncDesc, output []int) AggFunc {
	return nil
}

// buildCount builds the AggFunc implementation for function "AVG".
func buildAvg(aggFuncDesc *aggregation.AggFuncDesc, output []int) AggFunc {
	return nil
}

// buildCount builds the AggFunc implementation for function "FIRST_ROW".
func buildFirstRow(aggFuncDesc *aggregation.AggFuncDesc, output []int) AggFunc {
	return nil
}

// buildCount builds the AggFunc implementation for function "MAX".
func buildMax(aggFuncDesc *aggregation.AggFuncDesc, output []int) AggFunc {
	return nil
}

// buildCount builds the AggFunc implementation for function "MIN".
func buildMin(aggFuncDesc *aggregation.AggFuncDesc, output []int) AggFunc {
	return nil
}

// buildCount builds the AggFunc implementation for function "GROUP_CONCAT".
func buildGroupConcat(aggFuncDesc *aggregation.AggFuncDesc, output []int) AggFunc {
	return nil
}

// buildCount builds the AggFunc implementation for function "BIT_OR".
func buildBitOr(aggFuncDesc *aggregation.AggFuncDesc, output []int) AggFunc {
	return nil
}

// buildCount builds the AggFunc implementation for function "BIT_XOR".
func buildBitXor(aggFuncDesc *aggregation.AggFuncDesc, output []int) AggFunc {
	return nil
}

// buildCount builds the AggFunc implementation for function "BIT_AND".
func buildBitAnd(aggFuncDesc *aggregation.AggFuncDesc, output []int) AggFunc {
	return nil
}
