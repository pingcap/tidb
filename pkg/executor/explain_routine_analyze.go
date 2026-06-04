// Copyright 2026 PingCAP, Inc.
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

package executor

import (
	"context"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/execdetails"
)

type routineExplainObservation struct {
	runtimeSQLText string
	execCount      int
	totalTime      time.Duration
	rowsProduced   int64
	planVariants   map[string]struct{}
	explainable    bool
	drilldownRows  [][]string
}

type routineExplainAnalyzer struct {
	catalogByOrdinal          map[int]plannercore.ExplainRoutineCatalogEntry
	targetStmtOrdinal         int
	drilldownFormat           string
	observations              map[int]*routineExplainObservation
	drilldownRuntimeStatsColl *execdetails.RuntimeStatsColl
}

func newRoutineExplainAnalyzer(catalog []plannercore.ExplainRoutineCatalogEntry, targetStmtOrdinal int, drilldownFormat string) *routineExplainAnalyzer {
	catalogByOrdinal := make(map[int]plannercore.ExplainRoutineCatalogEntry, len(catalog))
	for _, entry := range catalog {
		catalogByOrdinal[entry.StmtOrdinal] = entry
	}
	return &routineExplainAnalyzer{
		catalogByOrdinal:          catalogByOrdinal,
		targetStmtOrdinal:         targetStmtOrdinal,
		drilldownFormat:           drilldownFormat,
		observations:              make(map[int]*routineExplainObservation, len(catalog)),
		drilldownRuntimeStatsColl: execdetails.NewRuntimeStatsColl(nil),
	}
}

func (a *routineExplainAnalyzer) hasTarget() bool {
	return a != nil && a.targetStmtOrdinal > 0
}

func (a *routineExplainAnalyzer) targetMatches(stmtOrdinal int) bool {
	return a != nil && a.targetStmtOrdinal == stmtOrdinal
}

func (a *routineExplainAnalyzer) drilldownRuntimeStatsCollForTarget() *execdetails.RuntimeStatsColl {
	if a == nil || !a.hasTarget() {
		return nil
	}
	return a.drilldownRuntimeStatsColl
}

func (a *routineExplainAnalyzer) prefersScalarSubQueryDrilldown(stmtOrdinal int) bool {
	if a == nil {
		return false
	}
	entry, ok := a.catalogByOrdinal[stmtOrdinal]
	if !ok {
		return false
	}
	return routineExplainStmtKindUsesExpressionWrapper(entry.StmtKind)
}

func (a *routineExplainAnalyzer) observe(stmtOrdinal int, runtimeSQLText string, rowsProduced int64, totalTime time.Duration, planKey string, explainable bool, drilldownRows [][]string) {
	if a == nil || stmtOrdinal <= 0 {
		return
	}
	if _, ok := a.catalogByOrdinal[stmtOrdinal]; !ok {
		return
	}
	observation, ok := a.observations[stmtOrdinal]
	if !ok {
		observation = &routineExplainObservation{
			planVariants: make(map[string]struct{}, 1),
		}
		a.observations[stmtOrdinal] = observation
	}
	observation.execCount++
	observation.totalTime += totalTime
	observation.rowsProduced += rowsProduced
	if runtimeSQLText != "" {
		observation.runtimeSQLText = runtimeSQLText
	}
	if explainable {
		observation.explainable = true
	}
	if planKey != "" {
		observation.planVariants[planKey] = struct{}{}
	}
	if a.targetMatches(stmtOrdinal) && len(drilldownRows) > 0 {
		observation.drilldownRows = drilldownRows
	}
}

func (a *routineExplainAnalyzer) runtimeStats(stmtOrdinal int) (plannercore.ExplainRoutineRuntimeStats, bool) {
	if a == nil {
		return plannercore.ExplainRoutineRuntimeStats{}, false
	}
	observation, ok := a.observations[stmtOrdinal]
	if !ok {
		return plannercore.ExplainRoutineRuntimeStats{}, false
	}
	return plannercore.ExplainRoutineRuntimeStats{
		RuntimeSQLText: observation.runtimeSQLText,
		ExecCount:      observation.execCount,
		TotalTime:      observation.totalTime,
		RowsProduced:   observation.rowsProduced,
		PlanVariants:   len(observation.planVariants),
		Explainable:    observation.explainable,
	}, true
}

func (a *routineExplainAnalyzer) drilldownRowsForTarget() ([][]string, error) {
	if a == nil || a.targetStmtOrdinal <= 0 {
		return nil, errors.New("missing target statement ordinal")
	}
	observation, ok := a.observations[a.targetStmtOrdinal]
	if !ok || observation.execCount == 0 {
		return nil, errors.Errorf("explain analyze routine STMT_ORDINAL %d was not executed", a.targetStmtOrdinal)
	}
	if len(observation.planVariants) > 1 {
		return nil, errors.Errorf("explain analyze routine STMT_ORDINAL %d observed multiple plan variants", a.targetStmtOrdinal)
	}
	if len(observation.drilldownRows) == 0 {
		return nil, errors.Errorf("explain analyze routine STMT_ORDINAL %d does not support drill-down", a.targetStmtOrdinal)
	}
	return observation.drilldownRows, nil
}

func routineExplainStmtText(stmt ast.StmtNode) string {
	if stmt == nil {
		return ""
	}
	if text := strings.TrimSpace(stmt.Text()); text != "" {
		return text
	}
	var sb strings.Builder
	_ = stmt.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &sb))
	return strings.TrimSpace(sb.String())
}

func routineExplainPrepareSQLText(stmt *ast.PrepareStmt, vars *variable.SessionVars) string {
	if stmt == nil {
		return ""
	}
	if stmt.SQLText != "" {
		return strings.TrimSpace(stmt.SQLText)
	}
	if stmt.SQLVar != nil && vars != nil {
		if datum, ok := vars.GetUserVarVal(stmt.SQLVar.Name); ok {
			str, err := datum.ToString()
			if err == nil {
				return strings.TrimSpace(str)
			}
		}
	}
	return routineExplainStmtText(stmt)
}

func resolveRoutineExplainRuntimeStmt(stmt ast.StmtNode, vars *variable.SessionVars) (ast.StmtNode, string, error) {
	switch x := stmt.(type) {
	case *ast.ExecuteStmt:
		prepared, err := plannercore.GetPreparedStmt(x, vars)
		if err != nil {
			return nil, "", err
		}
		runtimeStmt := prepared.PreparedAst.Stmt
		if plannercore.IsProcedurePlanCacheExecuteStmt(x) {
			return runtimeStmt, "", nil
		}
		return runtimeStmt, routineExplainStmtText(runtimeStmt), nil
	case *ast.PrepareStmt:
		return nil, routineExplainPrepareSQLText(x, vars), nil
	default:
		return stmt, routineExplainStmtText(stmt), nil
	}
}

func routineExplainIsScaffoldingStmt(stmt ast.StmtNode) bool {
	switch x := stmt.(type) {
	case *ast.PrepareStmt, *ast.DeallocateStmt:
		return true
	case *ast.SetStmt:
		if plannercore.ExplainRoutineSetStmtHasPlanBearingPath(x) {
			return false
		}
		for _, variable := range x.Variables {
			if variable != nil && variable.IsSystem {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func routineExplainPlanKey(plan base.Plan, preferScalarSubQuery bool) string {
	if plan == nil {
		return ""
	}
	if preferScalarSubQuery {
		if planKey := routineExplainScalarSubQueryPlanKey(plan); planKey != "" {
			return planKey
		}
	}
	_, digest := plannercore.NormalizePlan(plan)
	if digest != nil {
		if planKey := digest.String(); planKey != "" {
			return planKey
		}
	}
	return routineExplainScalarSubQueryPlanKey(plan)
}

func routineExplainScalarSubQueryPlanKey(plan base.Plan) string {
	flat := plannercore.FlattenPhysicalPlan(plan, true)
	if flat == nil || len(flat.ScalarSubQueries) == 0 {
		return ""
	}
	planKeys := make([]string, 0, len(flat.ScalarSubQueries))
	for _, subQuery := range flat.ScalarSubQueries {
		_, digest := plannercore.NormalizeFlatPlan(&plannercore.FlatPhysicalPlan{Main: subQuery})
		if digest == nil {
			continue
		}
		if planKey := digest.String(); planKey != "" {
			planKeys = append(planKeys, planKey)
		}
	}
	return strings.Join(planKeys, ";")
}

func routineExplainStmtKindUsesExpressionWrapper(stmtKind string) bool {
	switch stmtKind {
	case "condition", "declare_default", "return":
		return true
	default:
		return false
	}
}

func renderRoutineExplainAnalyzeRows(plan base.Plan, stmt ast.StmtNode, format string, preferScalarSubQuery bool) ([][]string, error) {
	if selectIntoPlan, ok := plan.(*plannercore.SelectInto); ok && selectIntoPlan.TargetPlan != nil {
		plan = selectIntoPlan.TargetPlan
	}
	explain := &plannercore.Explain{
		TargetPlan: plan,
		Format:     format,
		Analyze:    true,
		ExecStmt:   stmt,
	}
	explain.SetSCtx(plan.SCtx())
	if preferScalarSubQuery {
		flat := plannercore.FlattenPhysicalPlan(plan, true)
		if flat != nil && len(flat.ScalarSubQueries) > 0 {
			if err := explain.RenderFlatResult(&plannercore.FlatPhysicalPlan{
				CTEs:             flat.CTEs,
				ScalarSubQueries: flat.ScalarSubQueries,
			}); err != nil {
				return nil, err
			}
			return explain.Rows, nil
		}
	}
	if err := explain.RenderResult(); err != nil {
		return nil, err
	}
	return explain.Rows, nil
}

func executeRoutineAnalyze(ctx context.Context, procExec *ProcedureExec, analyzer *routineExplainAnalyzer) (err error) {
	previousAnalyzer := procExec.routineAnalyzer
	procExec.routineAnalyzer = analyzer
	procExec.cache = make([]plannercore.NeedCloseCur, 0, 10)
	defer func() {
		procExec.cache = plannercore.ReleaseAll(procExec.cache)
		procExec.routineAnalyzer = previousAnalyzer
	}()
	return procExec.executeCall(ctx)
}
