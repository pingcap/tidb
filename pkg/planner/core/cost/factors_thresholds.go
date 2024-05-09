// Copyright 2024 PingCAP, Inc.
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

package cost

import "github.com/pingcap/tidb/pkg/parser/ast"

// ******************************* Factors *******************************
const (
	// SelectionFactor is the default factor of the selectivity.
	// For example, If we have no idea how to estimate the selectivity
	// of a Selection or a JoinCondition, we can use this default value.
	SelectionFactor = 0.8

	DistinctFactor = 0.8
)

// AggFuncFactor is the basic factor for aggregation.
var AggFuncFactor = map[string]float64{
	ast.AggFuncCount:       1.0,
	ast.AggFuncSum:         1.0,
	ast.AggFuncAvg:         2.0,
	ast.AggFuncFirstRow:    0.1,
	ast.AggFuncMax:         1.0,
	ast.AggFuncMin:         1.0,
	ast.AggFuncGroupConcat: 1.0,
	ast.AggFuncBitOr:       0.9,
	ast.AggFuncBitXor:      0.9,
	ast.AggFuncBitAnd:      0.9,
	ast.AggFuncVarPop:      3.0,
	ast.AggFuncVarSamp:     3.0,
	ast.AggFuncStddevPop:   3.0,
	ast.AggFuncStddevSamp:  3.0,
	"default":              1.5,
}

// // ******************************* Thresholds *******************************
const (
	// SmallScanThreshold means: If the actual row count is much more
	// than the limit count, the unordered scan may cost much more than keep order.
	// So when a limit exists, we don't apply the DescScanFactor.
	SmallScanThreshold = 10000
)
