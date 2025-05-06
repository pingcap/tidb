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
	"fmt"
	"slices"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/intest"
	utilparser "github.com/pingcap/tidb/pkg/util/parser"
	"go.uber.org/zap"
)

// PlanDigestFunc is used to get the plan digest of this SQL.
var PlanDigestFunc func(sctx sessionctx.Context, stmt ast.StmtNode) (planDigest string, err error)

// BindingPlanInfo contains the binding info and its corresponding plan execution info, which is used by
// "SHOW PLAN FOR <SQL>" to help users understand the historical plans for a specific SQL.
type BindingPlanInfo struct {
	*Binding

	// Info from StmtStats
	Plan                 string
	AvgLatency           float64
	ExecTimes            int64
	AvgScanRows          float64
	AvgReturnedRows      float64
	LatencyPerReturnRow  float64
	ScanRowsPerReturnRow float64

	// Recommendation
	Recommend string
	Reason    string
}

// BindingPlanEvolution represents a series of APIs that help manage bindings automatically.
type BindingPlanEvolution interface {
	// TODO: RecordHistPlansAsBindings records the history plans as bindings for qualified queries.

	// ShowPlansForSQL shows historical plans for a specific SQL.
	ShowPlansForSQL(currentDB, sqlOrDigest, charset, collation string) ([]*BindingPlanInfo, error)
}

type bindingAuto struct {
	sPool              util.DestroyableSessionPool
	planGenerator      PlanGenerator
	ruleBasedPredictor PlanPerfPredictor
	llmPredictor       PlanPerfPredictor
}

func newBindingAuto(sPool util.DestroyableSessionPool) BindingPlanEvolution {
	return &bindingAuto{
		sPool:              sPool,
		planGenerator:      new(knobBasedPlanGenerator),
		ruleBasedPredictor: new(ruleBasedPlanPerfPredictor),
		llmPredictor:       new(llmBasedPlanPerfPredictor),
	}
}

// ShowPlansForSQL evolves plans for the specified SQL.
// 1. get historical plan candidates.
// 2. generate new plan candidates.
// 3. score all historical and newly-generated plan candidates and recommend the best one.
func (ba *bindingAuto) ShowPlansForSQL(currentDB, sqlOrDigest, charset, collation string) ([]*BindingPlanInfo, error) {
	historicalPlans, err := ba.getHistoricalPlanInfo(currentDB, sqlOrDigest, charset, collation)
	if err != nil {
		return nil, err
	}

	generatedPlans, err := ba.planGenerator.Generate(currentDB, sqlOrDigest)
	if err != nil {
		return nil, err
	}

	planCandidates := append(historicalPlans, generatedPlans...)
	ok, err := ba.fillRecommendation(planCandidates, ba.ruleBasedPredictor, "rule-based")
	if err != nil || ok { // error or hit any rule
		return planCandidates, err
	}
	_, err = ba.fillRecommendation(planCandidates, ba.llmPredictor, "LLM")
	return planCandidates, err
}

func (ba *bindingAuto) getHistoricalPlanInfo(currentDB, sqlOrDigest, charset, collation string) ([]*BindingPlanInfo, error) {
	// parse and normalize sqlOrDigest
	// if the length is 64 and it has no " ", treat it as a digest.
	var whereCond string
	sqlOrDigest = strings.TrimSpace(sqlOrDigest)
	if len(sqlOrDigest) == 64 && !strings.Contains(sqlOrDigest, " ") {
		whereCond = "where sql_digest = %?"
	} else {
		p := parser.New()
		stmtNode, err := p.ParseOneStmt(sqlOrDigest, charset, collation)
		if err != nil {
			return nil, errors.NewNoStackErrorf("failed to normalize the SQL: %v", err)
		}
		db := utilparser.GetDefaultDB(stmtNode, currentDB)
		sqlOrDigest, _ = NormalizeStmtForBinding(stmtNode, db, false)
		whereCond = "where original_sql = %?"
	}
	bindings, err := readBindingsFromStorage(ba.sPool, whereCond, sqlOrDigest)
	if err != nil {
		return nil, err
	}

	// read plan info from information_schema.tidb_statements_stats
	bindingPlans := make([]*BindingPlanInfo, 0, len(bindings))
	for _, binding := range bindings {
		if binding.Status == StatusDeleted {
			continue
		}

		planDigest := binding.PlanDigest
		if planDigest == "" {
			if err := callWithSCtx(ba.sPool, false, func(sctx sessionctx.Context) error {
				planDigest = getBindingPlanDigest(sctx, binding.Db, binding.BindSQL)
				return nil
			}); err != nil {
				bindingLogger().Error("get plan digest failed",
					zap.String("bind_sql", binding.BindSQL), zap.Error(err))
			}
		}

		pInfo, err := ba.getPlanExecInfo(planDigest)
		if err != nil {
			bindingLogger().Error("get plan execution info failed", zap.String("plan_digest", binding.PlanDigest), zap.Error(err))
			continue
		}
		autoBinding := &BindingPlanInfo{Binding: binding}
		if pInfo != nil && pInfo.ExecCount > 0 { // pInfo could be nil when stmt_stats' data is incomplete.
			autoBinding.Plan = pInfo.Plan
			autoBinding.ExecTimes = pInfo.ExecCount
			autoBinding.AvgLatency = float64(pInfo.TotalTime) / float64(pInfo.ExecCount)
			autoBinding.AvgScanRows = float64(pInfo.ProcessedKeys) / float64(pInfo.ExecCount)
			autoBinding.AvgReturnedRows = float64(pInfo.ResultRows) / float64(pInfo.ExecCount)
			if autoBinding.AvgReturnedRows > 0 {
				autoBinding.LatencyPerReturnRow = autoBinding.AvgLatency / autoBinding.AvgReturnedRows
				autoBinding.ScanRowsPerReturnRow = autoBinding.AvgScanRows / autoBinding.AvgReturnedRows
			}
		}
		bindingPlans = append(bindingPlans, autoBinding)
	}
	return bindingPlans, nil
}

func (*bindingAuto) fillRecommendation(plans []*BindingPlanInfo, predictor PlanPerfPredictor, name string) (bool, error) {
	if len(plans) == 0 {
		return false, nil
	}
	scores, explanations, err := predictor.PerfPredicate(plans)
	if err != nil {
		return false, err
	}
	maxScore := slices.Max(scores)
	if maxScore == 0 {
		return false, nil
	}
	recommended := false
	for i := range plans {
		if scores[i] == maxScore && !recommended {
			plans[i].Recommend = fmt.Sprintf("YES (from %s)", name)
			plans[i].Reason = explanations[i]
			recommended = true
		} else {
			plans[i].Recommend = "NO"
		}
	}
	return recommended, nil
}

// getPlanExecInfo gets the plan execution info from information_schema.tidb_statements_stats table.
func (ba *bindingAuto) getPlanExecInfo(planDigest string) (plan *planExecInfo, err error) {
	if planDigest == "" {
		return nil, nil
	}
	stmtStatsTable := "information_schema.cluster_tidb_statements_stats"
	if intest.InTest { // don't need to access the cluster table in tests.
		stmtStatsTable = "information_schema.tidb_statements_stats"
	}
	stmtQuery := fmt.Sprintf(`select cast(sum(result_rows) as signed), cast(sum(exec_count) as signed),
       cast(sum(processed_keys) as signed), cast(sum(total_time) as signed), any_value(plan)
       from %v where plan_digest = '%v'`, stmtStatsTable, planDigest)

	var rows []chunk.Row
	err = callWithSCtx(ba.sPool, false, func(sctx sessionctx.Context) error {
		rows, _, err = execRows(sctx, stmtQuery)
		return err
	})
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 || rows[0].IsNull(0) {
		// TODO: read data from workload_schema.hist_stmt_stats in this case if it's enabled.
		return nil, nil
	}
	return &planExecInfo{
		ResultRows:    rows[0].GetInt64(0),
		ExecCount:     rows[0].GetInt64(1),
		ProcessedKeys: rows[0].GetInt64(2),
		TotalTime:     rows[0].GetInt64(3),
		Plan:          rows[0].GetString(4),
	}, nil
}

// planExecInfo represents the plan info from information_schema.tidb_statements_stats table.
type planExecInfo struct {
	Plan          string
	ResultRows    int64
	ExecCount     int64
	ProcessedKeys int64
	TotalTime     int64
}

// IsSimplePointPlan checks whether the plan is a simple point plan.
// Expose this function for testing.
func IsSimplePointPlan(plan string) bool {
	// if the plan only contains Point_Get, Batch_Point_Get, Selection and Projection, it's a simple point plan.
	lines := strings.Split(plan, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		operatorName := strings.Split(line, " ")[0]
		// TODO: these hard-coding lines are a temporary implementation, refactor this part later.
		if operatorName == "id" || // the first line with column names
			strings.Contains(operatorName, "Point_Get") ||
			strings.Contains(operatorName, "Batch_Point_Get") ||
			strings.Contains(operatorName, "Selection") ||
			strings.Contains(operatorName, "Projection") {
			continue
		}
		return false
	}
	return true
}
