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

package evaluators

import (
	"github.com/pingcap/tidb/expression/aggregation"
)

func BuildAggEvaluator(agg aggregation.Aggregation, inputIdx, outputIdx []int) AggEvaluator {
	switch agg.GetFuncSig() {
	// avg, partial result (sum REAL, count INT)
	case "map_avg(real)":
		return &aggEvaluator4MapAvgReal{aggEvaluator4AvgReal{baseAggEvaluator{inputIdx, outputIdx}}}
	case "distinct_map_avg(real)":
		return &aggEvaluator4DistinctMapAvgReal{aggEvaluator4AvgReal{baseAggEvaluator{inputIdx, outputIdx}}}
	case "reduce_avg(int,real)":
		return &aggEvaluator4ReduceAvgReal{aggEvaluator4AvgReal{baseAggEvaluator{inputIdx, outputIdx}}}

	// avg, partial result (sum DECIMAL, count INT)
	case "map_avg(decimal)":
		return &aggEvaluator4MapAvgDecimal{aggEvaluator4AvgDecimal{baseAggEvaluator{inputIdx, outputIdx}}}
	case "map_avg(int)":
		return &aggEvaluator4MapAvgInt{aggEvaluator4AvgDecimal{baseAggEvaluator{inputIdx, outputIdx}}}
	case "distinct_map_avg(decimal)":
		return &aggEvaluator4DistinctMapAvgDecimal{aggEvaluator4AvgDecimal{baseAggEvaluator{inputIdx, outputIdx}}}
	case "reduce_avg(int,decimal)":
		return &aggEvaluator4ReduceAvgDecimal{aggEvaluator4AvgDecimal{baseAggEvaluator{inputIdx, outputIdx}}}

	// For "sum(real)"
	case "map_sum(real)":
		return nil
	case "distinct_map_sum(real)":
		return nil
	case "reduce_sum(real)":
		return nil

	// For "sum(decimal)"
	case "map_sum(decimal)":
		return nil
	case "distinct_map_sum(decimal)":
		return nil
	case "reduce_sum(decimal)":
		return nil

		// For "MAX"
	case "map_max(real)":
		return nil
	case "map_max(decimal)":
		return nil
	case "distinct_map_max(real)":
		return nil
	case "distinct_map_max(decimal)":
		return nil
	case "reduce_max(real)":
		return nil
	case "reduce_max(decimal)":
		return nil
	}
	return nil
}
