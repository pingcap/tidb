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

// Build is used to build AggFunc according to the aggFuncDesc
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

func buildCount(aggFuncDesc *aggregation.AggFuncDesc, output []int) AggFunc {
	return nil
}

func buildSum(aggFuncDesc *aggregation.AggFuncDesc, output []int) AggFunc {
	base := baseAggFunc{
		input:  aggFuncDesc.Args,
		output: output,
	}

	switch aggFuncDesc.Mode {
	// Build sum functions which consume the original data and remove the
	// duplicated input of the same group.
	case aggregation.DedupMode:
		return nil

	// Build sum functions which consume the original data and update their
	// partial results.
	case aggregation.CompleteMode, aggregation.Partial1Mode:
		switch aggFuncDesc.Args[0].GetType().Tp {
		case mysql.TypeNewDecimal:
			if aggFuncDesc.HasDistinct {
				return &sum4Decimal{base, make(map[types.MyDecimal]bool)}
			}
			return &sum4Decimal{base, nil}
		}

	// Build sum functions which consume the partial result of other agg
	// functions and update their partial results.
	case aggregation.Partial2Mode, aggregation.FinalMode:
		switch aggFuncDesc.Args[0].GetType().Tp {
		case mysql.TypeNewDecimal:
			return &sum4Decimal{base, nil}
		}
	}
	return nil
}

func buildAvg(aggFuncDesc *aggregation.AggFuncDesc, output []int) AggFunc {
	base := baseAggFunc{
		input:  aggFuncDesc.Args,
		output: output,
	}

	switch aggFuncDesc.Mode {
	// Build avg functions which consume the original data and remove the
	// duplicated input of the same group.
	case aggregation.DedupMode:
		return nil
		// TODO: implement UpdatePartialResult for the following structs.
		// switch aggFuncDesc.Args[0].GetType().Tp {
		// case mysql.TypeDecimal:
		// 	return &avgDedup4Decimal{baseAvgDecimal{base}}
		// case mysql.TypeFloat:
		// 	return &avgDedup4Float32{baseAvgFloat32{base}}
		// case mysql.TypeDouble:
		// 	return &avgDedup4Float64{baseAvgFloat64{base}}
		// }

	// Build avg functions which consume the original data and update their
	// partial results.
	case aggregation.CompleteMode, aggregation.Partial1Mode:
		switch aggFuncDesc.Args[0].GetType().Tp {
		case mysql.TypeNewDecimal:
			if aggFuncDesc.HasDistinct {
				return &avgOriginal4Decimal{baseAvgDecimal{base}, make(map[types.MyDecimal]bool)}
			}
			return &avgOriginal4Decimal{baseAvgDecimal{base}, nil}
		case mysql.TypeFloat:
			if aggFuncDesc.HasDistinct {
				return &avgOriginal4Float32{baseAvgFloat32{base}, make(map[float32]bool)}
			}
			return &avgOriginal4Float32{baseAvgFloat32{base}, nil}
		case mysql.TypeDouble:
			if aggFuncDesc.HasDistinct {
				return &avgOriginal4Float64{baseAvgFloat64{base}, make(map[float64]bool)}
			}
			return &avgOriginal4Float64{baseAvgFloat64{base}, nil}
		}

	// Build avg functions which consume the partial result of other agg
	// functions and update their partial results.
	case aggregation.Partial2Mode, aggregation.FinalMode:
		switch aggFuncDesc.Args[1].GetType().Tp {
		case mysql.TypeNewDecimal:
			return &avgPartial4Decimal{baseAvgDecimal{base}}
		case mysql.TypeFloat:
			return &avgPartial4Float32{baseAvgFloat32{base}}
		case mysql.TypeDouble:
			return &avgPartial4Float64{baseAvgFloat64{base}}
		}
	}
	return nil
}

func buildFirstRow(aggFuncDesc *aggregation.AggFuncDesc, output []int) AggFunc {
	return nil
}

func buildMax(aggFuncDesc *aggregation.AggFuncDesc, output []int) AggFunc {
	return nil
}

func buildMin(aggFuncDesc *aggregation.AggFuncDesc, output []int) AggFunc {
	return nil
}

func buildGroupConcat(aggFuncDesc *aggregation.AggFuncDesc, output []int) AggFunc {
	return nil
}

func buildBitOr(aggFuncDesc *aggregation.AggFuncDesc, output []int) AggFunc {
	return nil
}

func buildBitXor(aggFuncDesc *aggregation.AggFuncDesc, output []int) AggFunc {
	return nil
}

func buildBitAnd(aggFuncDesc *aggregation.AggFuncDesc, output []int) AggFunc {
	return nil
}
