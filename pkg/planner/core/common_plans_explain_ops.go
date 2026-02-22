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
	"fmt"
	"strconv"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/planner/util/costusage"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/plancodec"
	"github.com/pingcap/tipb/go-tipb"
)
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
