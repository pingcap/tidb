// Copyright 2015 PingCAP, Inc.
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

package core

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util/costusage"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tidb/pkg/util/texttree"
	"github.com/pingcap/tipb/go-tipb"
)

// Explain represents a explain plan.
type Explain struct {
	physicalop.SimpleSchemaProducer

	TargetPlan       base.Plan
	Format           string
	Analyze          bool
	Explore          bool   // EXPLAIN EXPLORE statement
	SQLDigest        string // "EXPLAIN EXPLORE <sql_digest>"
	ExecStmt         ast.StmtNode
	RuntimeStatsColl *execdetails.RuntimeStatsColl

	Rows            [][]string
	BriefBinaryPlan string
}

// GetBriefBinaryPlan returns the binary plan of the plan for explainfor.
func GetBriefBinaryPlan(p base.Plan) string {
	var plan base.Plan = p
	if plan == nil {
		return ""
	}
	// If the plan is a prepared statement, get execute.Plan.
	if exec, ok := p.(*Execute); ok {
		plan = exec.Plan
	}
	// Get plan context and statement context
	planCtx := plan.SCtx()
	if planCtx == nil {
		return ""
	}
	flat := FlattenPhysicalPlan(plan, true)
	return BinaryPlanStrFromFlatPlan(planCtx, flat, true)
}

// GetExplainAnalyzeRowsForPlan get explain rows for plan.
func GetExplainAnalyzeRowsForPlan(plan *Explain) (rows [][]string) {
	if err := plan.prepareSchema(); err != nil {
		return rows
	}
	if err := plan.RenderResult(); err != nil {
		return rows
	}
	return plan.Rows
}

// prepareSchema prepares explain's result schema.
func (e *Explain) prepareSchema() error {
	var fieldNames []string
	format := strings.ToLower(e.Format)
	if format == types.ExplainFormatTraditional {
		format = types.ExplainFormatROW
		e.Format = types.ExplainFormatROW
	}
	// Ensure StmtCtx.ExplainFormat is set early so shouldRemoveColumnNumbers can access it
	// This needs to be set before any ExplainInfo() calls are made
	// ResetContextOfStmt already set ExplainFormat to the original format (normalized to lowercase),
	// so we preserve that value instead of overwriting it with the converted format
	if e.SCtx() != nil {
		stmtCtx := e.SCtx().GetSessionVars().StmtCtx
		stmtCtx.InExplainStmt = true
		// Only set ExplainFormat if it's not already set (it should be set in ResetContextOfStmt)
		// This preserves the original format value for test compatibility
		if stmtCtx.ExplainFormat == "" {
			stmtCtx.ExplainFormat = format
		}
	}
	switch {
	case (format == types.ExplainFormatROW || format == types.ExplainFormatBrief || format == types.ExplainFormatPlanCache) && (!e.Analyze && e.RuntimeStatsColl == nil):
		fieldNames = []string{"id", "estRows", "task", "access object", "operator info"}
	case format == types.ExplainFormatPlanTree && (!e.Analyze && e.RuntimeStatsColl == nil):
		fieldNames = []string{"id", "task", "access object", "operator info"}
	case format == types.ExplainFormatVerbose:
		if e.Analyze || e.RuntimeStatsColl != nil {
			fieldNames = []string{"id", "estRows", "estCost", "actRows", "task", "access object", "execution info", "operator info", "memory", "disk"}
		} else {
			fieldNames = []string{"id", "estRows", "estCost", "task", "access object", "operator info"}
		}
	case format == types.ExplainFormatTrueCardCost:
		fieldNames = []string{"id", "estRows", "estCost", "costFormula", "actRows", "task", "access object", "execution info", "operator info", "memory", "disk"}
	case format == types.ExplainFormatCostTrace:
		if e.Analyze || e.RuntimeStatsColl != nil {
			fieldNames = []string{"id", "estRows", "estCost", "costFormula", "actRows", "task", "access object", "execution info", "operator info", "memory", "disk"}
		} else {
			fieldNames = []string{"id", "estRows", "estCost", "costFormula", "task", "access object", "operator info"}
		}
	case (format == types.ExplainFormatROW || format == types.ExplainFormatBrief || format == types.ExplainFormatPlanCache) && (e.Analyze || e.RuntimeStatsColl != nil):
		fieldNames = []string{"id", "estRows", "actRows", "task", "access object", "execution info", "operator info", "memory", "disk"}
	case format == types.ExplainFormatDOT:
		fieldNames = []string{"dot contents"}
	case format == types.ExplainFormatHint:
		fieldNames = []string{"hint"}
	case format == types.ExplainFormatBinary:
		fieldNames = []string{"binary plan"}
	case format == types.ExplainFormatTiDBJSON:
		fieldNames = []string{"TiDB_JSON"}
	case e.Explore:
		fieldNames = []string{"statement", "binding_hint", "plan", "plan_digest", "avg_latency", "exec_times", "avg_scan_rows",
			"avg_returned_rows", "latency_per_returned_row", "scan_rows_per_returned_row", "recommend", "reason",
			"explain_analyze", "binding"}
	default:
		if e.Analyze {
			return errors.Errorf("explain format '%s' with analyze is not supported now", e.Format)
		}
		return errors.Errorf("explain format '%s' is not supported now", e.Format)
	}

	cwn := &columnsWithNames{
		cols:  make([]*expression.Column, 0, len(fieldNames)),
		names: make([]*types.FieldName, 0, len(fieldNames)),
	}

	for _, fieldName := range fieldNames {
		cwn.Append(buildColumnWithName("", fieldName, mysql.TypeString, mysql.MaxBlobWidth))
	}
	e.SetSchema(cwn.col2Schema())
	e.SetOutputNames(cwn.names)
	return nil
}

func (e *Explain) renderResultForExplore() error {
	bindingHandle := domain.GetDomain(e.SCtx()).BindingHandle()
	sqlOrDigest := e.SQLDigest
	if sqlOrDigest == "" {
		sqlOrDigest = e.ExecStmt.Text()
	}
	plans, err := bindingHandle.ExplorePlansForSQL(e.SCtx(), sqlOrDigest, e.Analyze)
	if err != nil {
		return err
	}
	for _, p := range plans {
		hintStr, err := p.Binding.Hint.Restore()
		if err != nil {
			return err
		}

		e.Rows = append(e.Rows, []string{
			p.Binding.OriginalSQL,
			hintStr,
			p.Plan,
			p.PlanDigest,
			strconv.FormatFloat(p.AvgLatency, 'f', -1, 64),
			strconv.Itoa(int(p.ExecTimes)),
			strconv.FormatFloat(p.AvgScanRows, 'f', -1, 64),
			strconv.FormatFloat(p.AvgReturnedRows, 'f', -1, 64),
			strconv.FormatFloat(p.LatencyPerReturnRow, 'f', -1, 64),
			strconv.FormatFloat(p.ScanRowsPerReturnRow, 'f', -1, 64),
			p.Recommend,
			p.Reason,
			fmt.Sprintf("EXPLAIN ANALYZE %v", p.BindSQL),
			fmt.Sprintf("CREATE GLOBAL BINDING USING %v", p.BindSQL)})
	}
	return nil
}

// RenderResult renders the explain result as specified format.
func (e *Explain) RenderResult() error {
	if e.Explore {
		return e.renderResultForExplore()
	}

	if e.TargetPlan == nil {
		return nil
	}

	if e.Analyze && e.Format == types.ExplainFormatTrueCardCost {
		// true_card_cost mode is used to calibrate the cost model.
		pp, ok := e.TargetPlan.(base.PhysicalPlan)
		if ok {
			if _, err := getPlanCost(pp, property.RootTaskType,
				costusage.NewDefaultPlanCostOption().WithCostFlag(costusage.CostFlagRecalculate|costusage.CostFlagUseTrueCardinality|costusage.CostFlagTrace)); err != nil {
				return err
			}
			if pp.SCtx().GetSessionVars().CostModelVersion == modelVer2 {
				// output cost formula and factor costs through warning under model ver2 and true_card_cost mode for cost calibration.
				cost, _ := pp.GetPlanCostVer2(property.RootTaskType, costusage.NewDefaultPlanCostOption())
				if cost.GetTrace() != nil {
					trace := cost.GetTrace()
					pp.SCtx().GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackErrorf("cost formula: %v", trace.GetFormula()))
					data, err := json.Marshal(trace.GetFactorCosts())
					if err != nil {
						pp.SCtx().GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackErrorf("marshal factor costs error %v", err))
					}
					pp.SCtx().GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackErrorf("factor costs: %v", string(data)))

					// output cost factor weights for cost calibration
					factors := defaultVer2Factors.tolist()
					weights := make(map[string]float64)
					for _, factor := range factors {
						if factorCost, ok := trace.GetFactorCosts()[factor.Name]; ok && factor.Value > 0 {
							weights[factor.Name] = factorCost / factor.Value // cost = [factors] * [weights]
						}
					}
					if wstr, err := json.Marshal(weights); err != nil {
						pp.SCtx().GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackErrorf("marshal weights error %v", err))
					} else {
						pp.SCtx().GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackErrorf("factor weights: %v", string(wstr)))
					}
				}
			}
		} else {
			e.SCtx().GetSessionVars().StmtCtx.AppendWarning(errors.NewNoStackError("'explain format=true_card_cost' cannot support this plan"))
		}
	}
	// For explain for connection, we can directly decode the binary plan to get the explain rows.
	if e.BriefBinaryPlan != "" {
		if strings.ToLower(e.Format) != types.ExplainFormatBrief && strings.ToLower(e.Format) != types.ExplainFormatROW && strings.ToLower(e.Format) != types.ExplainFormatVerbose {
			return errors.Errorf("explain format '%s' for connection is not supported now", e.Format)
		}
		rows, err := plancodec.DecodeBinaryPlan4Connection(e.BriefBinaryPlan, strings.ToLower(e.Format), false)
		if err != nil {
			return err
		}
		e.Rows = rows
		return nil
	}
	// Ensure StmtCtx.ExplainFormat is set to match e.Format so shouldRemoveColumnNumbers can access it
	// e.Format is already normalized to lowercase in planbuilder.go and may have been converted from Traditional to ROW in prepareSchema()
	// prepareSchema() already set ExplainFormat (preserving the original from ResetContextOfStmt), so we just ensure InExplainStmt is set
	if e.SCtx() != nil {
		e.SCtx().GetSessionVars().StmtCtx.InExplainStmt = true
		// ExplainFormat should already be set correctly in prepareSchema() or ResetContextOfStmt
	}
	switch strings.ToLower(e.Format) {
	case types.ExplainFormatROW, types.ExplainFormatBrief, types.ExplainFormatVerbose, types.ExplainFormatTrueCardCost, types.ExplainFormatCostTrace, types.ExplainFormatPlanCache, types.ExplainFormatPlanTree:
		if e.Rows == nil || e.Analyze {
			flat := FlattenPhysicalPlan(e.TargetPlan, true)
			e.Rows = ExplainFlatPlanInRowFormat(flat, e.Format, e.Analyze, e.RuntimeStatsColl)
			if e.Analyze &&
				e.SCtx().GetSessionVars().MemoryDebugModeMinHeapInUse != 0 &&
				e.SCtx().GetSessionVars().MemoryDebugModeAlarmRatio > 0 {
				row := e.Rows[0]
				tracker := e.SCtx().GetSessionVars().MemTracker
				row[7] = row[7] + "(Total: " + tracker.FormatBytes(tracker.MaxConsumed()) + ")"
			}
		}
	case types.ExplainFormatDOT:
		if physicalPlan, ok := e.TargetPlan.(base.PhysicalPlan); ok {
			e.prepareDotInfo(physicalPlan)
		}
	case types.ExplainFormatHint:
		flat := FlattenPhysicalPlan(e.TargetPlan, false)
		hints := GenHintsFromFlatPlan(flat)
		hints = append(hints, hint.ExtractTableHintsFromStmtNode(e.ExecStmt, nil)...)
		e.Rows = append(e.Rows, []string{hint.RestoreOptimizerHints(hints)})
	case types.ExplainFormatBinary:
		flat := FlattenPhysicalPlan(e.TargetPlan, false)
		str := BinaryPlanStrFromFlatPlan(e.SCtx(), flat, false)
		e.Rows = append(e.Rows, []string{str})
	case types.ExplainFormatTiDBJSON:
		flat := FlattenPhysicalPlan(e.TargetPlan, true)
		encodes := e.explainFlatPlanInJSONFormat(flat)
		if e.Analyze && len(encodes) > 0 &&
			e.SCtx().GetSessionVars().MemoryDebugModeMinHeapInUse != 0 &&
			e.SCtx().GetSessionVars().MemoryDebugModeAlarmRatio > 0 {
			encodeRoot := encodes[0]
			tracker := e.SCtx().GetSessionVars().MemTracker
			encodeRoot.TotalMemoryConsumed = tracker.FormatBytes(tracker.MaxConsumed())
		}
		str, err := JSONToString(encodes)
		if err != nil {
			return err
		}
		e.Rows = append(e.Rows, []string{str})
	default:
		return errors.Errorf("explain format '%s' is not supported now", e.Format)
	}
	return nil
}

// ExplainFlatPlanInRowFormat returns the explain result in row format.
func ExplainFlatPlanInRowFormat(flat *FlatPhysicalPlan, format string, analyze bool,
	runtimeStatsColl *execdetails.RuntimeStatsColl) (rows [][]string) {
	if flat == nil || len(flat.Main) == 0 || flat.InExplain {
		return
	}
	for _, flatOp := range flat.Main {
		rows = prepareOperatorInfo(flatOp, format, analyze,
			runtimeStatsColl, rows)
	}
	for _, cte := range flat.CTEs {
		for _, flatOp := range cte {
			rows = prepareOperatorInfo(flatOp, format, analyze,
				runtimeStatsColl, rows)
		}
	}
	for _, subQ := range flat.ScalarSubQueries {
		for _, flatOp := range subQ {
			rows = prepareOperatorInfo(flatOp, format, analyze,
				runtimeStatsColl, rows)
		}
	}
	return
}

func (e *Explain) explainFlatPlanInJSONFormat(flat *FlatPhysicalPlan) (encodes []*ExplainInfoForEncode) {
	if flat == nil || len(flat.Main) == 0 || flat.InExplain {
		return
	}
	// flat.Main[0] must be the root node of tree
	encodes = append(encodes, e.explainOpRecursivelyInJSONFormat(flat.Main[0], flat.Main))

	for _, cte := range flat.CTEs {
		encodes = append(encodes, e.explainOpRecursivelyInJSONFormat(cte[0], cte))
	}
	for _, subQ := range flat.ScalarSubQueries {
		encodes = append(encodes, e.explainOpRecursivelyInJSONFormat(subQ[0], subQ))
	}
	return
}

func (e *Explain) explainOpRecursivelyInJSONFormat(flatOp *FlatOperator, flats FlatPlanTree) *ExplainInfoForEncode {
	taskTp := ""
	if flatOp.IsRoot {
		taskTp = "root"
	} else {
		taskTp = flatOp.ReqType.Name() + "[" + flatOp.StoreType.Name() + "]"
	}
	explainID := flatOp.ExplainID().String() + flatOp.Label.String()

	cur := e.prepareOperatorInfoForJSONFormat(flatOp.Origin, taskTp, explainID)

	for _, idx := range flatOp.ChildrenIdx {
		cur.SubOperators = append(cur.SubOperators,
			e.explainOpRecursivelyInJSONFormat(flats[idx], flats))
	}
	return cur
}

func getExplainIDAndTaskTp(flatOp *FlatOperator) (taskTp, textTreeExplainID string) {
	if flatOp.IsRoot {
		taskTp = "root"
	} else {
		taskTp = flatOp.ReqType.Name() + "[" + flatOp.StoreType.Name() + "]"
	}
	textTreeExplainID = texttree.PrettyIdentifier(flatOp.ExplainID().String()+flatOp.Label.String(),
		flatOp.TextTreeIndent,
		flatOp.IsLastChild)
	return
}

func getRuntimeInfoStr(ctx base.PlanContext, p base.Plan, runtimeStatsColl *execdetails.RuntimeStatsColl) (actRows, analyzeInfo, memoryInfo, diskInfo string) {
	if runtimeStatsColl == nil {
		runtimeStatsColl = ctx.GetSessionVars().StmtCtx.RuntimeStatsColl
		if runtimeStatsColl == nil {
			return
		}
	}
	rootStats, copStats, memTracker, diskTracker := getRuntimeInfo(ctx, p, runtimeStatsColl)
	actRows = "0"
	memoryInfo = "N/A"
	diskInfo = "N/A"
	if rootStats != nil {
		actRows = strconv.FormatInt(rootStats.GetActRows(), 10)
		analyzeInfo = rootStats.String()
	}
	if copStats != nil {
		if len(analyzeInfo) > 0 {
			analyzeInfo += ", "
		}
		analyzeInfo += copStats.String()
		actRows = strconv.FormatInt(copStats.GetActRows(), 10)
	}
	if memTracker != nil {
		memoryInfo = memTracker.FormatBytes(memTracker.MaxConsumed())
	}
	if diskTracker != nil {
		diskInfo = diskTracker.FormatBytes(diskTracker.MaxConsumed())
	}
	return
}

func getRuntimeInfo(ctx base.PlanContext, p base.Plan, runtimeStatsColl *execdetails.RuntimeStatsColl) (
	rootStats *execdetails.RootRuntimeStats,
	copStats *execdetails.CopRuntimeStats,
	memTracker *memory.Tracker,
	diskTracker *memory.Tracker,
) {
	if runtimeStatsColl == nil {
		runtimeStatsColl = ctx.GetSessionVars().StmtCtx.RuntimeStatsColl
	}
	explainID := p.ID()
	// There maybe some mock information for cop task to let runtimeStatsColl.Exists(p.ExplainID()) is true.
	// So check copTaskExecDetail first and print the real cop task information if it's not empty.
	if runtimeStatsColl != nil && runtimeStatsColl.ExistsRootStats(explainID) {
		rootStats = runtimeStatsColl.GetRootStats(explainID)
	}
	if runtimeStatsColl != nil && runtimeStatsColl.ExistsCopStats(explainID) {
		copStats = runtimeStatsColl.GetCopStats(explainID)
	}
	memTracker = ctx.GetSessionVars().StmtCtx.MemTracker.SearchTrackerWithoutLock(p.ID())
	diskTracker = ctx.GetSessionVars().StmtCtx.DiskTracker.SearchTrackerWithoutLock(p.ID())
	return
}

// prepareOperatorInfo generates the following information for every plan:
// operator id, estimated rows, task type, access object and other operator info.
func prepareOperatorInfo(flatOp *FlatOperator, format string, analyze bool,
	runtimeStatsColl *execdetails.RuntimeStatsColl, rows [][]string) [][]string {
	p := flatOp.Origin
	if p.ExplainID().String() == "_0" {
		return rows
	}
	taskType, id := getExplainIDAndTaskTp(flatOp)

	estRows, estCost, costFormula, accessObject, operatorInfo := getOperatorInfo(p, format)

	var row []string
	if analyze || runtimeStatsColl != nil {
		row = []string{id}
		if format != types.ExplainFormatPlanTree {
			row = append(row, estRows)
		}
		if format == types.ExplainFormatVerbose || format == types.ExplainFormatTrueCardCost || format == types.ExplainFormatCostTrace {
			row = append(row, estCost)
		}
		if format == types.ExplainFormatTrueCardCost || format == types.ExplainFormatCostTrace {
			row = append(row, costFormula)
		}
		actRows, analyzeInfo, memoryInfo, diskInfo := getRuntimeInfoStr(p.SCtx(), p, runtimeStatsColl)
		row = append(row, actRows, taskType, accessObject, analyzeInfo, operatorInfo, memoryInfo, diskInfo)
	} else {
		row = []string{id}
		if format != types.ExplainFormatPlanTree {
			row = append(row, estRows)
		}
		if format == types.ExplainFormatVerbose || format == types.ExplainFormatTrueCardCost ||
			format == types.ExplainFormatCostTrace {
			row = append(row, estCost)
		}
		if format == types.ExplainFormatCostTrace {
			row = append(row, costFormula)
		}
		row = append(row, taskType, accessObject, operatorInfo)
	}
	return append(rows, row)
}

func (e *Explain) prepareOperatorInfoForJSONFormat(p base.Plan, taskType, explainID string) *ExplainInfoForEncode {
	if p.ExplainID().String() == "_0" {
		return nil
	}

	estRows, _, _, accessObject, operatorInfo := getOperatorInfo(p, e.Format)
	jsonRow := &ExplainInfoForEncode{
		ID:           explainID,
		TaskType:     taskType,
		AccessObject: accessObject,
		OperatorInfo: operatorInfo,
		SubOperators: make([]*ExplainInfoForEncode, 0),
	}
	if e.Format != types.ExplainFormatPlanTree {
		jsonRow.EstRows = estRows
	}

	if e.Analyze || e.RuntimeStatsColl != nil {
		jsonRow.ActRows, jsonRow.ExecuteInfo, jsonRow.MemoryInfo, jsonRow.DiskInfo = getRuntimeInfoStr(e.SCtx(), p, e.RuntimeStatsColl)
	}
	return jsonRow
}

func getOperatorInfo(p base.Plan, format string) (estRows, estCost, costFormula, accessObject, operatorInfo string) {
	pp, isPhysicalPlan := p.(base.PhysicalPlan)
	estRows = "N/A"
	estCost = "N/A"
	costFormula = "N/A"
	sctx := p.SCtx()
	// Ensure StmtCtx.ExplainFormat is set before calling ExplainInfo() so shouldRemoveColumnNumbers can access it
	// format is already normalized to lowercase when passed from ExplainFlatPlanInRowFormat (which uses e.Format)
	if sctx != nil {
		stmtCtx := sctx.GetSessionVars().StmtCtx
		stmtCtx.InExplainStmt = true
		stmtCtx.ExplainFormat = format
	}
	if isPhysicalPlan {
		if format != types.ExplainFormatPlanTree {
			estRows = strconv.FormatFloat(pp.GetEstRowCountForDisplay(), 'f', 2, 64)
		}
		if sctx != nil && sctx.GetSessionVars().CostModelVersion == modelVer2 {
			costVer2, _ := pp.GetPlanCostVer2(property.RootTaskType, costusage.NewDefaultPlanCostOption())
			estCost = strconv.FormatFloat(costVer2.GetCost(), 'f', 2, 64)
			if costVer2.GetTrace() != nil {
				costFormula = costVer2.GetTrace().GetFormula()
			}
		} else {
			planCost, _ := getPlanCost(pp, property.RootTaskType, costusage.NewDefaultPlanCostOption())
			estCost = strconv.FormatFloat(planCost, 'f', 2, 64)
		}
	} else if si := p.StatsInfo(); si != nil {
		estRows = strconv.FormatFloat(si.RowCount, 'f', 2, 64)
	}

	if plan, ok := p.(base.DataAccesser); ok {
		accessObject = plan.AccessObject().String()
		operatorInfo = plan.OperatorInfo(false)
	} else {
		if pa, ok := p.(base.PartitionAccesser); ok && sctx != nil {
			accessObject = pa.AccessObject(sctx).String()
		}
		operatorInfo = p.ExplainInfo()
	}

	// Column numbers are now removed in column.ExplainInfo() when format is plan_tree
	return estRows, estCost, costFormula, accessObject, operatorInfo
}

// BinaryPlanStrFromFlatPlan generates the compressed and encoded binary plan from a FlatPhysicalPlan.
func BinaryPlanStrFromFlatPlan(explainCtx base.PlanContext, flat *FlatPhysicalPlan, briefBinaryPlan bool) string {
	binary := binaryDataFromFlatPlan(explainCtx, flat, briefBinaryPlan)
	if binary == nil {
		return ""
	}
	proto, err := binary.Marshal()
	if err != nil {
		return ""
	}
	str := plancodec.Compress(proto)
	return str
}

func binaryDataFromFlatPlan(explainCtx base.PlanContext, flat *FlatPhysicalPlan, briefBinaryPlan bool) *tipb.ExplainData {
	if len(flat.Main) == 0 {
		return nil
	}
	// Please see comments in EncodeFlatPlan() for this case.
	// We keep consistency with EncodeFlatPlan() here.
	if flat.InExecute {
		return nil
	}
	res := &tipb.ExplainData{}
	for _, op := range flat.Main {
		// We assume that runtime stats are available to this plan tree if any operator in the "Main" has runtime stats.
		rootStats, copStats, _, _ := getRuntimeInfo(explainCtx, op.Origin, nil)
		if rootStats != nil || copStats != nil {
			res.WithRuntimeStats = true
			break
		}
	}
	res.Main = binaryOpTreeFromFlatOps(explainCtx, flat.Main, briefBinaryPlan)
	for _, explainedCTE := range flat.CTEs {
		res.Ctes = append(res.Ctes, binaryOpTreeFromFlatOps(explainCtx, explainedCTE, briefBinaryPlan))
	}
	for _, subQ := range flat.ScalarSubQueries {
		res.Subqueries = append(res.Subqueries, binaryOpTreeFromFlatOps(explainCtx, subQ, briefBinaryPlan))
	}
	return res
}

func binaryOpTreeFromFlatOps(explainCtx base.PlanContext, ops FlatPlanTree, briefBinaryPlan bool) *tipb.ExplainOperator {
	operators := make([]tipb.ExplainOperator, len(ops))

	// First phase: Generate all operators with normal processing (including ID suffix)
	for i, op := range ops {
		binaryOpFromFlatOp(explainCtx, op, &operators[i])
	}

	// Second phase: If briefBinaryPlan is true, set IgnoreExplainIDSuffix and generate BriefName
	if briefBinaryPlan && len(ops) > 0 && ops[0].Origin.SCtx() != nil {
		stmtCtx := ops[0].Origin.SCtx().GetSessionVars().StmtCtx
		originalIgnore := stmtCtx.IgnoreExplainIDSuffix
		stmtCtx.IgnoreExplainIDSuffix = true

		for i, op := range ops {
			operators[i].BriefName = op.ExplainID().String()
			switch op.Origin.(type) {
			case *physicalop.PhysicalTableReader, *physicalop.PhysicalIndexReader, *physicalop.PhysicalHashJoin,
				*physicalop.PhysicalIndexJoin, *physicalop.PhysicalIndexHashJoin, *physicalop.PhysicalMergeJoin:
				operators[i].BriefOperatorInfo = op.Origin.ExplainInfo()
			}
		}

		stmtCtx.IgnoreExplainIDSuffix = originalIgnore
	}

	// Build the tree structure
	for i, op := range ops {
		for _, idx := range op.ChildrenIdx {
			operators[i].Children = append(operators[i].Children, &operators[idx])
		}
	}
	return &operators[0]
}

func binaryOpFromFlatOp(explainCtx base.PlanContext, fop *FlatOperator, out *tipb.ExplainOperator) {
	out.Name = fop.ExplainID().String()
	switch fop.Label {
	case BuildSide:
		out.Labels = []tipb.OperatorLabel{tipb.OperatorLabel_buildSide}
	case ProbeSide:
		out.Labels = []tipb.OperatorLabel{tipb.OperatorLabel_probeSide}
	case SeedPart:
		out.Labels = []tipb.OperatorLabel{tipb.OperatorLabel_seedPart}
	case RecursivePart:
		out.Labels = []tipb.OperatorLabel{tipb.OperatorLabel_recursivePart}
	}
	switch fop.StoreType {
	case kv.TiDB:
		out.StoreType = tipb.StoreType_tidb
	case kv.TiKV:
		out.StoreType = tipb.StoreType_tikv
	case kv.TiFlash:
		out.StoreType = tipb.StoreType_tiflash
	}
	if fop.IsRoot {
		out.TaskType = tipb.TaskType_root
	} else {
		switch fop.ReqType {
		case physicalop.Cop:
			out.TaskType = tipb.TaskType_cop
		case physicalop.BatchCop:
			out.TaskType = tipb.TaskType_batchCop
		case physicalop.MPP:
			out.TaskType = tipb.TaskType_mpp
		}
	}

	if fop.IsPhysicalPlan {
		p := fop.Origin.(base.PhysicalPlan)
		out.Cost, _ = getPlanCost(p, property.RootTaskType, costusage.NewDefaultPlanCostOption())
		out.EstRows = p.GetEstRowCountForDisplay()
	} else if statsInfo := fop.Origin.StatsInfo(); statsInfo != nil {
		out.EstRows = statsInfo.RowCount
	}

	// Runtime info
	rootStats, copStats, memTracker, diskTracker := getRuntimeInfo(explainCtx, fop.Origin, nil)
	if rootStats != nil {
		basic, groups := rootStats.MergeStats()
		if basic != nil {
			out.RootBasicExecInfo = basic.String()
		}
		for _, group := range groups {
			str := group.String()
			if len(str) > 0 {
				out.RootGroupExecInfo = append(out.RootGroupExecInfo, str)
			}
		}
		out.ActRows = uint64(rootStats.GetActRows())
	}
	if copStats != nil {
		out.CopExecInfo = copStats.String()
		out.ActRows = uint64(copStats.GetActRows())
	}
	if memTracker != nil {
		out.MemoryBytes = memTracker.MaxConsumed()
	} else {
		out.MemoryBytes = -1
	}
	if diskTracker != nil {
		out.DiskBytes = diskTracker.MaxConsumed()
	} else {
		out.DiskBytes = -1
	}

	// Operator info
	if plan, ok := fop.Origin.(base.DataAccesser); ok {
		out.OperatorInfo = plan.OperatorInfo(false)
	} else {
		out.OperatorInfo = fop.Origin.ExplainInfo()
	}
	// Access object
	switch p := fop.Origin.(type) {
	case base.DataAccesser:
		ao := p.AccessObject()
		if ao != nil {
			ao.SetIntoPB(out)
		}
	case base.PartitionAccesser:
		ao := p.AccessObject(explainCtx)
		if ao != nil {
			ao.SetIntoPB(out)
		}
	}
}

func (e *Explain) prepareDotInfo(p base.PhysicalPlan) {
	buffer := bytes.NewBufferString("")
	fmt.Fprintf(buffer, "\ndigraph %s {\n", p.ExplainID())
	e.prepareTaskDot(&pair{p, false}, "root", buffer)
	buffer.WriteString("}\n")

	e.Rows = append(e.Rows, []string{buffer.String()})
}

type pair struct {
	physicalPlan base.PhysicalPlan
	isChildOfINL bool
}

func (e *Explain) prepareTaskDot(pa *pair, taskTp string, buffer *bytes.Buffer) {
	fmt.Fprintf(buffer, "subgraph cluster%v{\n", pa.physicalPlan.ID())
	buffer.WriteString("node [style=filled, color=lightgrey]\n")
	buffer.WriteString("color=black\n")
	fmt.Fprintf(buffer, "label = \"%s\"\n", taskTp)

	if len(pa.physicalPlan.Children()) == 0 {
		if taskTp == "cop" {
			fmt.Fprintf(buffer, "\"%s\"\n}\n", pa.physicalPlan.ExplainID(pa.isChildOfINL))
			return
		}
		fmt.Fprintf(buffer, "\"%s\"\n", pa.physicalPlan.ExplainID(pa.isChildOfINL))
	}
	var copTasks []*pair
	var pipelines []string

	for planQueue := []*pair{pa}; len(planQueue) > 0; planQueue = planQueue[1:] {
		curPair := planQueue[0]
		switch copPlan := curPair.physicalPlan.(type) {
		case *physicalop.PhysicalTableReader:
			pipelines = append(pipelines, fmt.Sprintf("\"%s\" -> \"%s\"\n", copPlan.ExplainID(), copPlan.TablePlan.ExplainID()))
			copTasks = append(copTasks, &pair{physicalPlan: copPlan.TablePlan})
		case *physicalop.PhysicalIndexReader:
			pipelines = append(pipelines, fmt.Sprintf("\"%s\" -> \"%s\"\n", copPlan.ExplainID(), copPlan.IndexPlan.ExplainID()))
			copTasks = append(copTasks, &pair{physicalPlan: copPlan.IndexPlan})
		case *physicalop.PhysicalIndexLookUpReader:
			pipelines = append(pipelines, fmt.Sprintf("\"%s\" -> \"%s\"\n", copPlan.ExplainID(), copPlan.TablePlan.ExplainID()))
			pipelines = append(pipelines, fmt.Sprintf("\"%s\" -> \"%s\"\n", copPlan.ExplainID(), copPlan.IndexPlan.ExplainID()))
			copTasks = append(copTasks, &pair{physicalPlan: copPlan.TablePlan, isChildOfINL: true})
			copTasks = append(copTasks, &pair{physicalPlan: copPlan.IndexPlan})
		case *physicalop.PhysicalIndexMergeReader:
			for i := range copPlan.PartialPlansRaw {
				pipelines = append(pipelines, fmt.Sprintf("\"%s\" -> \"%s\"\n", copPlan.ExplainID(), copPlan.PartialPlansRaw[i].ExplainID()))
				copTasks = append(copTasks, &pair{physicalPlan: copPlan.PartialPlansRaw[i]})
			}
			if copPlan.TablePlan != nil {
				pipelines = append(pipelines, fmt.Sprintf("\"%s\" -> \"%s\"\n", copPlan.ExplainID(), copPlan.TablePlan.ExplainID()))
				copTasks = append(copTasks, &pair{physicalPlan: copPlan.TablePlan, isChildOfINL: true})
			}
		}
		for _, child := range curPair.physicalPlan.Children() {
			fmt.Fprintf(buffer, "\"%s\" -> \"%s\"\n", curPair.physicalPlan.ExplainID(curPair.isChildOfINL), child.ExplainID(curPair.isChildOfINL))
			// pass current pair isChildOfINL.
			planQueue = append(planQueue, &pair{physicalPlan: child, isChildOfINL: curPair.isChildOfINL})
		}
	}
	buffer.WriteString("}\n")

	for _, cop := range copTasks {
		e.prepareTaskDot(cop, "cop", buffer)
	}

	for i := range pipelines {
		buffer.WriteString(pipelines[i])
	}
}
