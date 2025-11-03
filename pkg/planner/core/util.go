// Copyright 2017 PingCAP, Inc.
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
	"fmt"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/core/stats"
	"github.com/pingcap/tidb/pkg/planner/planctx"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/stringutil"
	"github.com/prometheus/client_golang/prometheus"
	"slices"
	"strings"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/set"
	"go.uber.org/zap"
)

// IsReadOnly check whether the ast.Node is a read only statement.
func IsReadOnly(node ast.Node, vars *variable.SessionVars) bool {
	return IsReadOnlyInternal(node, vars, true)
}

// IsReadOnlyInternal is that If checkGlobalVars is true, false will be returned when there are updates to global variables.
func IsReadOnlyInternal(node ast.Node, vars *variable.SessionVars, checkGlobalVars bool) bool {
	if execStmt, isExecStmt := node.(*ast.ExecuteStmt); isExecStmt {
		prepareStmt, err := GetPreparedStmt(execStmt, vars)
		if err != nil {
			logutil.BgLogger().Warn("GetPreparedStmt failed", zap.Error(err))
			return false
		}
		return ast.IsReadOnly(prepareStmt.PreparedAst.Stmt, checkGlobalVars)
	}
	return ast.IsReadOnly(node, checkGlobalVars)
}

// AggregateFuncExtractor visits Expr tree.
// It collects AggregateFuncExpr from AST Node.
type AggregateFuncExtractor struct {
	// skipAggMap stores correlated aggregate functions which have been built in outer query,
	// so extractor in sub-query will skip these aggregate functions.
	skipAggMap map[*ast.AggregateFuncExpr]*expression.CorrelatedColumn
	// AggFuncs is the collected AggregateFuncExprs.
	AggFuncs []*ast.AggregateFuncExpr
}

// Enter implements Visitor interface.
func (*AggregateFuncExtractor) Enter(n ast.Node) (ast.Node, bool) {
	switch n.(type) {
	case *ast.SelectStmt, *ast.SetOprStmt:
		return n, true
	}
	return n, false
}

// Leave implements Visitor interface.
func (a *AggregateFuncExtractor) Leave(n ast.Node) (ast.Node, bool) {
	//nolint: revive
	switch v := n.(type) {
	case *ast.AggregateFuncExpr:
		if _, ok := a.skipAggMap[v]; !ok {
			a.AggFuncs = append(a.AggFuncs, v)
		}
	}
	return n, true
}

// WindowFuncExtractor visits Expr tree.
// It converts ColumnNameExpr to WindowFuncExpr and collects WindowFuncExpr.
type WindowFuncExtractor struct {
	// WindowFuncs is the collected WindowFuncExprs.
	windowFuncs []*ast.WindowFuncExpr
}

// Enter implements Visitor interface.
func (*WindowFuncExtractor) Enter(n ast.Node) (ast.Node, bool) {
	switch n.(type) {
	case *ast.SelectStmt, *ast.SetOprStmt:
		return n, true
	}
	return n, false
}

// Leave implements Visitor interface.
func (a *WindowFuncExtractor) Leave(n ast.Node) (ast.Node, bool) {
	//nolint: revive
	switch v := n.(type) {
	case *ast.WindowFuncExpr:
		a.windowFuncs = append(a.windowFuncs, v)
	}
	return n, true
}

// extractStringFromStringSet helps extract string info from set.StringSet.
func extractStringFromStringSet(set set.StringSet) string {
	if len(set) < 1 {
		return ""
	}
	l := make([]string, 0, len(set))
	for k := range set {
		l = append(l, fmt.Sprintf(`"%s"`, k))
	}
	slices.Sort(l)
	return strings.Join(l, ",")
}

// extractStringFromStringSlice helps extract string info from []string.
func extractStringFromStringSlice(ss []string) string {
	if len(ss) < 1 {
		return ""
	}
	slices.Sort(ss)
	return strings.Join(ss, ",")
}

// extractStringFromUint64Slice helps extract string info from uint64 slice.
func extractStringFromUint64Slice(slice []uint64) string {
	if len(slice) < 1 {
		return ""
	}
	l := make([]string, 0, len(slice))
	for _, k := range slice {
		l = append(l, fmt.Sprintf(`%d`, k))
	}
	slices.Sort(l)
	return strings.Join(l, ",")
}

// extractStringFromBoolSlice helps extract string info from bool slice.
func extractStringFromBoolSlice(slice []bool) string {
	if len(slice) < 1 {
		return ""
	}
	l := make([]string, 0, len(slice))
	for _, k := range slice {
		l = append(l, fmt.Sprintf(`%t`, k))
	}
	slices.Sort(l)
	return strings.Join(l, ",")
}

func tableHasDirtyContent(ctx base.PlanContext, tableInfo *model.TableInfo) bool {
	pi := tableInfo.GetPartitionInfo()
	if pi == nil {
		return ctx.HasDirtyContent(tableInfo.ID)
	}
	// Currently, we add UnionScan on every partition even though only one partition's data is changed.
	// This is limited by current implementation of Partition Prune. It'll be updated once we modify that part.
	for _, partition := range pi.Definitions {
		if ctx.HasDirtyContent(partition.ID) {
			return true
		}
	}
	return false
}

// PlanMetricTestingKey is used for testing plan metrics.
var PlanMetricTestingKey stringutil.StringerStr = "_plan_metric_testing_key"

// PlanMetrics collects plan metrics from the given FlatPhysicalPlan.
func PlanMetrics(sctx sessionctx.Context, flatPlan *FlatPhysicalPlan) {
	flatPlanMetrics(sctx, flatPlan.Main)
	for _, cte := range flatPlan.CTEs {
		flatPlanMetrics(sctx, cte)
	}
	for _, sq := range flatPlan.ScalarSubQueries {
		flatPlanMetrics(sctx, sq)
	}
}

func flatPlanMetrics(sctx sessionctx.Context, planTree FlatPlanTree) {
	runtimeStats := sctx.GetSessionVars().StmtCtx.RuntimeStatsColl
	if runtimeStats == nil {
		return
	}
	for _, node := range planTree {
		var actRows int
		if node.IsRoot {
			nodeRuntimeStats := runtimeStats.GetRootStats(node.Origin.ID())
			if nodeRuntimeStats == nil {
				continue // no runtime info
			}
			actRows = int(nodeRuntimeStats.GetActRows())
		} else {
			nodeRuntimeInfo := runtimeStats.GetCopStats(node.Origin.ID())
			if nodeRuntimeInfo == nil {
				continue // no runtime info
			}
			actRows = int(nodeRuntimeInfo.GetActRows())
		}

		switch op := node.Origin.(type) {
		case *physicalop.PhysicalIndexScan:
			planScanMetric(op.SCtx(), op.Table, actRows, false)
		case *physicalop.PhysicalTableScan:
			planScanMetric(op.SCtx(), op.Table, actRows, op.IsFullScan())
		case *physicalop.PointGetPlan:
			planScanMetric(op.SCtx(), op.TblInfo, actRows, false)
		case *physicalop.BatchPointGetPlan:
			planScanMetric(op.SCtx(), op.TblInfo, actRows, false)
		case *physicalop.PhysicalIndexLookUpReader, *physicalop.PhysicalIndexMergeReader:
			planCopKVRequestsMetric(op.SCtx(), node, planTree, runtimeStats)
		case *physicalop.PhysicalIndexJoin, *physicalop.PhysicalIndexMergeJoin, *physicalop.PhysicalIndexHashJoin:
			planCopKVRequestsMetric(op.SCtx(), node, planTree, runtimeStats)
			planJoinMetric(op.SCtx(), actRows)
		case *physicalop.PhysicalHashJoin, *physicalop.PhysicalMergeJoin:
			planJoinMetric(op.SCtx(), actRows)
		}
	}
}

func planCopKVRequestsMetric(sctx planctx.PlanContext, curOp *FlatOperator, planTree FlatPlanTree, runtimeStats *execdetails.RuntimeStatsColl) {
	var sumKVReq int
	for _, idx := range curOp.ChildrenIdx {
		childOp := planTree[idx]
		copStats := runtimeStats.GetCopStats(childOp.Origin.ID())
		if copStats == nil {
			continue
		}
		sumKVReq += int(copStats.GetTasks())
	}

	kvReqLabel := getKVReqLabel(sumKVReq)
	var opLabel string
	switch curOp.Origin.(type) {
	case *physicalop.PhysicalIndexLookUpReader:
		opLabel = "index_lookup"
	case *physicalop.PhysicalIndexMergeReader:
		opLabel = "index_merge"
	default: // use the same label for all kinds of index join for simplicity.
		opLabel = "index_join"
	}

	incMetric(sctx, metrics.PlanKVReqCounter, opLabel, kvReqLabel)
}

func planJoinMetric(sctx planctx.PlanContext, joinRows int) {
	joinRowsLabel := getRowsLabel(joinRows)
	incMetric(sctx, metrics.PlanJoinRowsCounter, joinRowsLabel)
}

func planScanMetric(sctx planctx.PlanContext, tbl *model.TableInfo, scanRows int, isTableFullScan bool) {
	tableStats := stats.GetStatsTable(sctx, tbl, tbl.ID)
	if tableStats == nil || tableStats.Pseudo {
		return
	}
	totalRows := int(tableStats.RealtimeCount)
	if totalRows == 0 {
		return // ignore empty table
	}

	scanRowsLabel := getRowsLabel(scanRows)
	scanSelectivityLabel := getScanSelectivityLabel(float64(scanRows) / float64(totalRows))
	if isTableFullScan {
		incMetric(sctx, metrics.PlanTableFullScanCounter, scanRowsLabel)
	}
	// including table scan (or primary key scan) and index scan
	incMetric(sctx, metrics.PlanScanRowsCounter, scanRowsLabel)
	incMetric(sctx, metrics.PlanScanSelectivityCounter, scanSelectivityLabel)
}

func incMetric(sctx planctx.PlanContext, metric *prometheus.CounterVec, labels ...string) {
	if intest.InTest {
		if callback := sctx.Value(PlanMetricTestingKey); callback != nil {
			callback.(func(metric *prometheus.CounterVec, labels ...string))(metric, labels...)
		}
	}
	metric.WithLabelValues(labels...).Inc()
}

func getKVReqLabel(kvReq int) string {
	if kvReq < 100 {
		return "0-100"
	} else if kvReq < 1000 {
		return "100-1000"
	} else if kvReq < 10000 {
		return "1000-10000"
	}
	return "10000+"
}

func getScanSelectivityLabel(percentage float64) string {
	if percentage < 0.01 {
		return "0-1%"
	} else if percentage < 0.1 {
		return "1%-10%"
	} else if percentage < 0.5 {
		return "10%-50%"
	}
	return "50+%"
}

func getRowsLabel(scanRows int) string {
	if scanRows < 1000 {
		return "0-1000"
	} else if scanRows < 10000 {
		return "1000-10000"
	} else if scanRows < 100000 {
		return "10000-100000"
	} else if scanRows < 1000000 {
		return "100000-1000000"
	}
	return "1000000+"
}
