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

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/collate"
)

// buildGroupConcat builds the AggFunc implementation for function "GROUP_CONCAT".
func buildGroupConcat(ctx AggFuncBuildContext, aggFuncDesc *aggregation.AggFuncDesc, ordinal int) AggFunc {
	switch aggFuncDesc.Mode {
	case aggregation.DedupMode:
		return nil
	default:
		// The last arg is promised to be a not-null string constant, so the error can be ignored.
		c, _ := aggFuncDesc.Args[len(aggFuncDesc.Args)-1].(*expression.Constant)
		sep, _, err := c.EvalString(ctx.GetEvalCtx(), chunk.Row{})
		// This err should never happen.
		if err != nil {
			panic(fmt.Sprintf("Error happened when buildGroupConcat: %s", err.Error()))
		}
		maxLen := ctx.GetGroupConcatMaxLen()
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
				desc := make([]bool, len(base.byItems))
				ctors := make([]collate.Collator, 0, len(base.byItems))
				for i, byItem := range base.byItems {
					desc[i] = byItem.Desc
					ctors = append(ctors, collate.GetCollator(byItem.Expr.GetType(ctx.GetEvalCtx()).GetCollate()))
				}
				return &groupConcatDistinctOrder{base, ctors, desc}
			}
			return &groupConcatDistinct{base}
		}
		if len(aggFuncDesc.OrderByItems) > 0 {
			desc := make([]bool, len(base.byItems))
			ctors := make([]collate.Collator, 0, len(base.byItems))
			for i, byItem := range base.byItems {
				desc[i] = byItem.Desc
				ctors = append(ctors, collate.GetCollator(byItem.Expr.GetType(ctx.GetEvalCtx()).GetCollate()))
			}

			return &groupConcatOrder{base, ctors, desc}
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

func buildNthValue(ctx AggFuncBuildContext, aggFuncDesc *aggregation.AggFuncDesc, ordinal int) AggFunc {
	base := baseAggFunc{
		args:    aggFuncDesc.Args,
		ordinal: ordinal,
	}
	// Already checked when building the function description.
	nth, _, _ := expression.GetUint64FromConstant(ctx.GetEvalCtx(), aggFuncDesc.Args[1])
	return &nthValue{baseAggFunc: base, tp: aggFuncDesc.RetTp, nth: nth}
}

func buildNtile(ctx AggFuncBuildContext, aggFuncDes *aggregation.AggFuncDesc, ordinal int) AggFunc {
	base := baseAggFunc{
		args:    aggFuncDes.Args,
		ordinal: ordinal,
	}
	n, _, _ := expression.GetUint64FromConstant(ctx.GetEvalCtx(), aggFuncDes.Args[0])
	return &ntile{baseAggFunc: base, n: n}
}

func buildPercentRank(ordinal int, orderByCols []*expression.Column) AggFunc {
	base := baseAggFunc{
		ordinal: ordinal,
	}
	return &percentRank{baseAggFunc: base, rowComparer: buildRowComparer(orderByCols)}
}

func buildLeadLag(ctx AggFuncBuildContext, aggFuncDesc *aggregation.AggFuncDesc, ordinal int) baseLeadLag {
	offset := uint64(1)
	if len(aggFuncDesc.Args) >= 2 {
		offset, _, _ = expression.GetUint64FromConstant(ctx.GetEvalCtx(), aggFuncDesc.Args[1])
	}
	var defaultExpr expression.Expression
	defaultExpr = expression.NewNull()
	if len(aggFuncDesc.Args) == 3 {
		defaultExpr = aggFuncDesc.Args[2]
		if et, ok := defaultExpr.(*expression.Constant); ok {
			evalCtx := ctx.GetEvalCtx()
			res, err1 := et.Value.ConvertTo(evalCtx.TypeCtx(), aggFuncDesc.RetTp)
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

func buildLead(ctx AggFuncBuildContext, aggFuncDesc *aggregation.AggFuncDesc, ordinal int) AggFunc {
	return &lead{buildLeadLag(ctx, aggFuncDesc, ordinal)}
}

func buildLag(ctx AggFuncBuildContext, aggFuncDesc *aggregation.AggFuncDesc, ordinal int) AggFunc {
	return &lag{buildLeadLag(ctx, aggFuncDesc, ordinal)}
}
