// Copyright 2021 PingCAP, Inc.
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

package types

var (
	// ExplainFormatBrief is the same as ExplainFormatRow, but it ignores explain ID suffix.
	ExplainFormatBrief = "brief"
	// ExplainFormatDOT indicates that using DotGraph to explain.
	ExplainFormatDOT = "dot"
	// ExplainFormatHint is to show hint information.
	ExplainFormatHint = "hint"
	// ExplainFormatJSON displays the information in JSON format.
	ExplainFormatJSON = "json"
	// ExplainFormatROW presents the output in tabular format.
	ExplainFormatROW = "row"
	// ExplainFormatVerbose display additional information regarding the plan.
	ExplainFormatVerbose = "verbose"
	// ExplainFormatTraditional is the same as ExplainFormatROW.
	ExplainFormatTraditional = "traditional"
	// ExplainFormatTrueCardCost indicates the optimizer to use true cardinality to calculate the cost.
	ExplainFormatTrueCardCost = "true_card_cost"
	// ExplainFormatBinary prints the proto for binary plan.
	ExplainFormatBinary = "binary"
	// ExplainFormatTiDBJSON warp the default result in JSON format
	ExplainFormatTiDBJSON = "tidb_json"
	// ExplainFormatCostTrace prints the cost and cost formula of each operator.
	ExplainFormatCostTrace = "cost_trace"
	// ExplainFormatPlanCache prints the reason why can't use non-prepared plan cache by warning
	ExplainFormatPlanCache = "plan_cache"

	// ExplainFormats stores the valid formats for explain statement, used by validator.
	ExplainFormats = []string{
		ExplainFormatBrief,
		ExplainFormatDOT,
		ExplainFormatHint,
		ExplainFormatJSON,
		ExplainFormatROW,
		ExplainFormatVerbose,
		ExplainFormatTraditional,
		ExplainFormatTrueCardCost,
		ExplainFormatBinary,
		ExplainFormatTiDBJSON,
		ExplainFormatCostTrace,
		ExplainFormatPlanCache,
	}
)
