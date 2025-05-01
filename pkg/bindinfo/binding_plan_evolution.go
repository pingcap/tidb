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
	"encoding/json"
	"fmt"
	"github.com/pingcap/tidb/pkg/llmaccess"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util"
	"math/rand"
	"sort"
	"strings"
)

// PlanGenerator is used to generate new Plan Candidates for this specified query.
type PlanGenerator interface {
	Generate(defaultSchema string, sql string) (plans []*BindingPlanInfo, err error)
}

// knobBasedPlanGenerator generates new plan candidates via adjusting knobs like cost factors, hints, etc.
type knobBasedPlanGenerator struct {
	sPool util.DestroyableSessionPool
}

func (g *knobBasedPlanGenerator) Generate(defaultSchema string, sql string) (plans []*BindingPlanInfo, err error) {
	// sPool util.DestroyableSessionPool
	err = callWithSCtx(g.sPool, false, func(sctx sessionctx.Context) error {
		sql = strings.TrimSpace(sql)

		// reset cost factors
		sctx.GetSessionVars().CurrentDB = defaultSchema
		sctx.GetSessionVars().UsePlanBaselines = false
		sctx.GetSessionVars().CostModelVersion = 2
		sctx.GetSessionVars().IndexScanCostFactor = 1
		sctx.GetSessionVars().TableFullScanCostFactor = 1
		sctx.GetSessionVars().TableRangeScanCostFactor = 1
		sctx.GetSessionVars().TableRowIDScanCostFactor = 1
		sctx.GetSessionVars().IndexLookupCostFactor = 1
		sctx.GetSessionVars().IndexMergeCostFactor = 1
		sctx.GetSessionVars().SortCostFactor = 1
		sctx.GetSessionVars().TopNCostFactor = 1
		sctx.GetSessionVars().StreamAggCostFactor = 1
		sctx.GetSessionVars().HashAggCostFactor = 1
		sctx.GetSessionVars().MergeJoinCostFactor = 1
		sctx.GetSessionVars().HashJoinCostFactor = 1
		sctx.GetSessionVars().IndexJoinCostFactor = 1

		memorizedPlan := make(map[string]struct{})
		defaultPlan, err := generateBindingPlan(sctx, sql)
		if err != nil {
			return err
		}
		relatedCostFactors := collectRelatedCostFactors(sctx, defaultPlan.Plan)
		planHint, err := defaultPlan.Hint.Restore()
		if err != nil {
			return err
		}
		memorizedPlan[planHint] = struct{}{}
		plans = append(plans, defaultPlan)

		// change these related cost factors randomly to walk in the plan space and sample some plans
		if len(relatedCostFactors) > 0 {
			for walkStep := 0; walkStep < 500; walkStep++ {
				// each step, randomly change one cost factor
				idx := rand.Intn(len(relatedCostFactors))
				randomFactorValues := []float64{0.01, 0.1, 10, 100, 1000, 10000, 100000}
				*relatedCostFactors[idx] = randomFactorValues[rand.Intn(len(randomFactorValues))]

				// generate a new plan based on the modified cost factors
				bindingPlan, err := generateBindingPlan(sctx, sql)
				if err != nil {
					return err
				}
				planHint, err := bindingPlan.Hint.Restore()
				if err != nil {
					return err
				}
				if _, ok := memorizedPlan[planHint]; ok {
					// skip the same plan
					continue
				}
				memorizedPlan[planHint] = struct{}{}
				plans = append(plans, bindingPlan)
			}
		}
		return nil
	})
	return
}

func collectRelatedCostFactors(sctx sessionctx.Context, plan string) []*float64 {
	factors := make([]*float64, 0, 4)
	if strings.Contains(plan, "IndexFullScan") || strings.Contains(plan, "IndexRangeScan") {
		factors = append(factors, &sctx.GetSessionVars().IndexScanCostFactor)
	}
	if strings.Contains(plan, "IndexLookUp") {
		factors = append(factors, &sctx.GetSessionVars().IndexLookupCostFactor)
	}
	if strings.Contains(plan, "TableFullScan") {
		factors = append(factors, &sctx.GetSessionVars().TableFullScanCostFactor)
	}
	if strings.Contains(plan, "TableRangeScan") {
		factors = append(factors, &sctx.GetSessionVars().TableRangeScanCostFactor)
	}
	if strings.Contains(plan, "HashJoin") {
		factors = append(factors, &sctx.GetSessionVars().HashJoinCostFactor)
	}
	if strings.Contains(plan, "MergeJoin") {
		factors = append(factors, &sctx.GetSessionVars().MergeJoinCostFactor)
	}
	if strings.Contains(plan, "IndexJoin") {
		factors = append(factors, &sctx.GetSessionVars().IndexJoinCostFactor)
	}
	if strings.Contains(plan, "Sort") {
		factors = append(factors, &sctx.GetSessionVars().SortCostFactor,
			&sctx.GetSessionVars().TopNCostFactor)
	}
	if strings.Contains(plan, "HashAgg") {
		factors = append(factors, &sctx.GetSessionVars().HashAggCostFactor)
	}
	if strings.Contains(plan, "StreamAgg") {
		factors = append(factors, &sctx.GetSessionVars().StreamAggCostFactor)
	}
	return factors
}

func generateBindingPlan(sctx sessionctx.Context, sql string) (*BindingPlanInfo, error) {
	plan, hint, err := explainPlan(sctx, sql)
	if err != nil {
		return nil, err
	}

	bindingSQL := fmt.Sprintf("%v /*+ %v */ %v", sql[:6], hint, sql[6:]) // "select" + hint + ...
	binding := &Binding{
		OriginalSQL: sql, // TODO: normalize
		BindSQL:     bindingSQL,
		Db:          sctx.GetSessionVars().CurrentDB,
		Source:      "new-generated",
	}
	if err = prepareHints(sctx, binding); err != nil {
		return nil, err
	}

	return &BindingPlanInfo{
		Binding: binding,
		Plan:    plan,
	}, nil
}

func explainPlan(sctx sessionctx.Context, sql string) (plan, hint string, err error) {
	rows, _, err := execRows(sctx, "EXPLAIN "+sql)
	if err != nil {
		return "", "", err
	}

	/*
		+----------------------------+----------+-----------+---------------------+---------------------------------+
		| id                         | estRows  | task      | access object       | operator info                   |
		+----------------------------+----------+-----------+---------------------+---------------------------------+
		| StreamAgg_20               | 1.00     | root      |                     | funcs:count(Column#9)->Column#4 |
		| └─IndexReader_21           | 1.00     | root      |                     | index:StreamAgg_8               |
		|   └─StreamAgg_8            | 1.00     | cop[tikv] |                     | funcs:count(1)->Column#9        |
		|     └─IndexFullScan_19     | 10000.00 | cop[tikv] | table:t, index:a(a) | keep order:false, stats:pseudo  |
		+----------------------------+----------+-----------+---------------------+---------------------------------+
	*/
	for _, r := range rows {
		op := r.GetString(0)
		estRows := r.GetString(1)
		task := r.GetString(2)
		accObj := r.GetString(3)
		opInfo := r.GetString(4)
		plan = plan + "\n" + fmt.Sprintf("%s\t%s\t%s\t%s\t%s", op, estRows, task, accObj, opInfo)
	}

	rows, _, err = execRows(sctx, "EXPLAIN FORMAT='hint' "+sql)
	if err != nil {
		return "", "", err
	}
	hint = rows[0].GetString(0)
	return
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
	llmAccessor llmaccess.LLMAccessor
}

func (p *llmBasedPlanPerfPredictor) PerfPredicate(plans []*BindingPlanInfo) (scores []float64, explanations []string, err error) {
	scores = make([]float64, len(plans))
	explanations = make([]string, len(plans))

	if p.llmAccessor == nil || p.llmAccessor.IsAccessPointAvailable("tidb_spm") {
		return nil, nil, nil
	}

	prompt := p.prompt(plans)
	llmResp, err := p.llmAccessor.ChatCompletion("tidb_spm", prompt)
	if err != nil {
		return nil, nil, err
	}

	return p.parseLLMResp(llmResp, plans)
}

func (p *llmBasedPlanPerfPredictor) prompt(plans []*BindingPlanInfo) string {
	promptPattern := `You are a TiDB expert.
You are going to help me decide which hint should be used for a specified SQL.
Be careful with the escape characters.
Be careful that estRows might not be accurate.
You can take at most 20 seconds to think of this.
The SQL is "%v".
Here are these hinted SQLs and their plans:
%v
Please tell me which one is the best, and the reason.
The reason should be concise, not more than 200 words.
Please return a valid JSON object with the key "number" and "reason".
IMPORTANT: Don't put anything else in the response and return the raw json data directly, remove "` + "```" + `json".
Here is an example of output JSON:
    {"number": 2, "reason": "xxxxxxxxxxxxxxxxxxx"}`
	bindingPlanText := make([]string, 0, len(plans))
	for i, p := range plans {
		bindingPlanText = append(bindingPlanText, fmt.Sprintf("%d. %v\n%v\n", i, p.BindSQL, p.Plan))
	}
	prompt := fmt.Sprintf(promptPattern, plans[0].OriginalSQL, strings.Join(bindingPlanText, "\n"))
	return prompt
}

func (p *llmBasedPlanPerfPredictor) parseLLMResp(llmResp string, plans []*BindingPlanInfo) (scores []float64, explanations []string, err error) {
	if strings.HasPrefix(llmResp, "```json") {
		llmResp = strings.TrimPrefix(llmResp, "```json")
		llmResp = strings.TrimSuffix(llmResp, "```")
	}

	r := new(LLMRecommendation)
	err = json.Unmarshal([]byte(llmResp), r)
	if err != nil {
		return nil, nil, err
	}

	if r.Number < 0 || r.Number >= len(plans) {
		return nil, nil, fmt.Errorf("invalid result number: %d, should be in [0, %d)", r.Number, len(plans))
	}
	scores = make([]float64, len(plans))
	scores[r.Number] = 1
	explanations = make([]string, len(plans))
	explanations[r.Number] = r.Reason
	return
}

type LLMRecommendation struct {
	Number int    `json:"number"`
	Reason string `json:"reason"`
}
