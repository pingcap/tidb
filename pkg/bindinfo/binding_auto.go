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
	"sort"
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

// BindingAuto represents a series of APIs that help manage bindings automatically.
type BindingAuto interface {
	// TODO: RecordHistPlansAsBindings records the history plans as bindings for qualified queries.

	// ShowPlansForSQL shows historical plans for a specific SQL.
	ShowPlansForSQL(currentDB, sqlOrDigest, charset, collation string) ([]*BindingPlanInfo, error)
}

type bindingAuto struct {
	sPool util.DestroyableSessionPool
}

func newBindingAuto(sPool util.DestroyableSessionPool) BindingAuto {
	return &bindingAuto{
		sPool: sPool,
	}
}

// ShowPlansForSQL shows historical plans for a specific SQL.
func (ba *bindingAuto) ShowPlansForSQL(currentDB, sqlOrDigest, charset, collation string) ([]*BindingPlanInfo, error) {
	// parse and normalize sqlOrDigest
	// if the length is 64 and it has no " ", treat it as a digest.
	var whereCond, originalSQL string
	sqlOrDigest = strings.TrimSpace(sqlOrDigest)
	if len(sqlOrDigest) == 64 && !strings.Contains(sqlOrDigest, " ") {
		whereCond = "where sql_digest = %?"
	} else {
		originalSQL = sqlOrDigest
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
		binding.Source = "history plan"
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

	// explore new plans via cost factors.
	genedPlans, err := generateBindingPlans(ba.sPool, currentDB, originalSQL)
	if err != nil {
		return nil, fmt.Errorf("failed to generate new binding plans: %w", err)
	}
	bindingPlans = append(bindingPlans, genedPlans...)

	FillRecommendation(bindingPlans)

	return bindingPlans, nil
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

// FillRecommendation fills the recommendation field for each binding plan.
// Expose this function for testing.
// The recommendation is rule-based + LLM-based.
// Rules:
// 1. If any plan is a simple PointGet or BatchPointGet, recommend it.
// 2. If `ScanRowsPerReturnedRow` of a plan is 50% better than others', recommend it.
// 3. If `Latency`, `ScanRows` and `LatencyPerReturnRow` of a plan are 50% better than others', recommend it.
// LLM: TODO
func FillRecommendation(bindings []*BindingPlanInfo) {
	if len(bindings) < 2 {
		// if there is only 1 binding, don't need to recommend.
		return
	}

	for _, p := range bindings {
		if p.ExecTimes == 0 {
			// if we can't get plan info for any binding, we can't give any recommendation.
			FillRecommendationViaLLM(bindings)
			return
		}
	}

	// rule-based recommendation
	// rule 1
	for _, cur := range bindings {
		if IsSimplePointPlan(cur.Plan) {
			cur.Recommend = "YES (rule-based)"
			cur.Reason = "Simple PointGet or BatchPointGet is the best plan"
			return
		}
	}

	// sort for rule 2 & 3.
	// only the first binding could be the candidate for rule 2 & 3.
	sort.Slice(bindings, func(i, j int) bool {
		if bindings[i].ScanRowsPerReturnRow == bindings[j].ScanRowsPerReturnRow {
			return bindings[i].AvgLatency < bindings[j].AvgLatency &&
				bindings[i].AvgScanRows < bindings[j].AvgScanRows &&
				bindings[i].LatencyPerReturnRow < bindings[j].LatencyPerReturnRow
		}
		return bindings[i].ScanRowsPerReturnRow < bindings[j].ScanRowsPerReturnRow
	})

	// rule 2
	if bindings[0].ScanRowsPerReturnRow < bindings[1].ScanRowsPerReturnRow/2 {
		bindings[0].Recommend = "YES (rule-based)"
		bindings[0].Reason = "Plan's scan_rows_per_returned_row is 50% better than others'"
		return
	}

	// rule 3
	for i := 1; i < len(bindings); i++ {
		hitRule3 := bindings[0].AvgLatency <= bindings[i].AvgLatency/2 &&
			bindings[0].AvgScanRows <= bindings[i].AvgScanRows/2 &&
			bindings[0].LatencyPerReturnRow <= bindings[i].LatencyPerReturnRow/2
		if !hitRule3 {
			break
		}
		if i == len(bindings)-1 { // the last one
			bindings[0].Recommend = "YES (rule-based)"
			bindings[0].Reason = "Plan's latency, scan_rows and latency_per_returned_row are 50% better than others'"
			return
		}
	}

	FillRecommendationViaLLM(bindings)
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
