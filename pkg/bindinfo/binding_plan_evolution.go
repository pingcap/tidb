// Copyright 2025 PingCAP, Inc.
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

package bindinfo

import (
	"sort"
)

// PlanGenerator is used to generate new Plan Candidates for this specified query.
type PlanGenerator interface {
	Generate(defaultSchema string, sql string) (plans []*BindingPlanInfo, err error)
}

// knobBasedPlanGenerator generates new plan candidates via adjusting knobs like cost factors, hints, etc.
type knobBasedPlanGenerator struct {
}

func (*knobBasedPlanGenerator) Generate(string, string) (plans []*BindingPlanInfo, err error) {
	// TODO: implement this
	return nil, nil
}

// PlanPerfPredictor is used to score these plan candidates, returns their scores and gives some explanations.
// If all scores are 0, means that no plan is recommended.
type PlanPerfPredictor interface {
	PerfPredicate(plans []*BindingPlanInfo) (scores []float64, explanations []string, err error)
}

// ruleBasedPlanPerfPredictor scores plans according to a set of rules.
// If any plan can hit any rule below, its score will be 1 and all others will be 0.
// Rules:
// 1. If any plan is a simple PointGet or BatchPointGet, recommend it.
// 2. If `ScanRowsPerReturnedRow` of a plan is 50% better than others', recommend it.
// 3. If `Latency`, `ScanRows` and `LatencyPerReturnRow` of a plan are 50% better than others', recommend it.
type ruleBasedPlanPerfPredictor struct {
}

func (*ruleBasedPlanPerfPredictor) PerfPredicate(plans []*BindingPlanInfo) (scores []float64, explanations []string, err error) {
	scores = make([]float64, len(plans))
	explanations = make([]string, len(plans))

	if len(plans) == 0 {
		return
	}
	if len(plans) == 1 {
		scores[0] = 1
		return
	}

	// rule-based recommendation
	// rule 1
	for i, cur := range plans {
		if IsSimplePointPlan(cur.Plan) {
			scores[i] = 1
			explanations[i] = "Simple PointGet or BatchPointGet is the best plan"
			return
		}
	}

	for _, p := range plans {
		if p.ExecTimes == 0 { // no execution info
			return
		}
	}

	// sort for rule 2 & 3.
	// only the first binding could be the candidate for rule 2 & 3.
	sort.Slice(plans, func(i, j int) bool {
		if plans[i].ScanRowsPerReturnRow == plans[j].ScanRowsPerReturnRow {
			return plans[i].AvgLatency < plans[j].AvgLatency &&
				plans[i].AvgScanRows < plans[j].AvgScanRows &&
				plans[i].LatencyPerReturnRow < plans[j].LatencyPerReturnRow
		}
		return plans[i].ScanRowsPerReturnRow < plans[j].ScanRowsPerReturnRow
	})

	// rule 2
	if plans[0].ScanRowsPerReturnRow < plans[1].ScanRowsPerReturnRow/2 {
		scores[0] = 1
		explanations[0] = "Plan's scan_rows_per_returned_row is 50% better than others'"
		return
	}

	// rule 3
	for i := 1; i < len(plans); i++ {
		hitRule3 := plans[0].AvgLatency <= plans[i].AvgLatency/2 &&
			plans[0].AvgScanRows <= plans[i].AvgScanRows/2 &&
			plans[0].LatencyPerReturnRow <= plans[i].LatencyPerReturnRow/2
		if !hitRule3 {
			break
		}
		if i == len(plans)-1 { // the last one
			scores[0] = 1
			explanations[0] = "Plan's latency, scan_rows and latency_per_returned_row are 50% better than others'"
			return
		}
	}

	return
}

// llmBasedPlanPerfPredictor leverages LLM to score plans.
type llmBasedPlanPerfPredictor struct {
}

func (*llmBasedPlanPerfPredictor) PerfPredicate(plans []*BindingPlanInfo) (scores []float64, explanations []string, err error) {
	scores = make([]float64, len(plans))
	explanations = make([]string, len(plans))
	// TODO: implement this
	return
}
