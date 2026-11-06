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

package core

import (
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/util/ranger"
)

// pkRangeProbeMinScanRows is the estimated number of rows a table scan must
// read before it gets PK range probes without a pk_filter hint. Two index
// reads can't pay for themselves on a smaller scan.
const pkRangeProbeMinScanRows = 1000

// attachPKRangeProbes runs after physical optimization. For each table scan
// in the chosen plan whose pushed-down filter fixes every column of a
// secondary index with equality conditions, it attaches PKRangeProbes so the
// executor can bound the scan to the handles that index holds for those
// values. It doesn't change the plan, its cost, or its shape.
//
// The probes are enabled by tidb_opt_generate_pk_filter, or per table by a
// pk_filter hint, which also lifts the scan size requirement.
func attachPKRangeProbes(sctx base.PlanContext, plan base.PhysicalPlan) {
	vars := sctx.GetSessionVars()
	if vars.InRestrictedSQL {
		return
	}
	// Writes and locking reads may read rows committed after the statement's
	// snapshot, which the probes can't see.
	sc := vars.StmtCtx
	if !sc.InSelectStmt || sc.InInsertStmt || sc.InUpdateStmt || sc.InDeleteStmt {
		return
	}
	attachPKRangeProbesWalk(sctx, plan)
}

func attachPKRangeProbesWalk(sctx base.PlanContext, p base.PhysicalPlan) {
	innerIdx := -1
	switch x := p.(type) {
	case *physicalop.PhysicalLock:
		// SELECT ... FOR UPDATE/SHARE.
		return
	case *physicalop.PhysicalUnionScan:
		// The reader's key ranges also bound the transaction's own writes.
		return
	case *physicalop.PhysicalTableReader:
		x.PKRangeProbes = buildPKRangeProbes(sctx, x)
		return
	case *physicalop.PhysicalIndexJoin:
		innerIdx = x.InnerChildIdx
	case *physicalop.PhysicalIndexHashJoin:
		innerIdx = x.InnerChildIdx
	case *physicalop.PhysicalIndexMergeJoin:
		innerIdx = x.InnerChildIdx
	}
	for i, child := range p.Children() {
		// The inner side of an index join reads ranges built from outer rows.
		if i == innerIdx {
			continue
		}
		attachPKRangeProbesWalk(sctx, child)
	}
}

func buildPKRangeProbes(sctx base.PlanContext, reader *physicalop.PhysicalTableReader) []*physicalop.PKRangeProbe {
	if reader.StoreType != kv.TiKV || reader.ReadReqType != physicalop.Cop || len(reader.TablePlans) < 2 {
		return nil
	}
	ts, ok := reader.TablePlans[0].(*physicalop.PhysicalTableScan)
	if !ok || (!ts.PKFilterHinted && !sctx.GetSessionVars().AllowGeneratePKFilter) {
		return nil
	}
	sel, ok := reader.TablePlans[1].(*physicalop.PhysicalSelection)
	if !ok {
		return nil
	}
	tbl := ts.Table
	if tbl.GetPartitionInfo() != nil || tbl.TempTableType != model.TempTableNone ||
		tbl.TableCacheStatusType != model.TableCacheStatusDisable {
		return nil
	}
	if !ts.PKFilterHinted && pkRangeProbeScanRows(ts) < pkRangeProbeMinScanRows {
		return nil
	}

	var probes []*physicalop.PKRangeProbe
	for _, idx := range tbl.Indices {
		if !pkRangeProbeIndexUsable(tbl, idx) || !pkRangeProbeIndexHinted(ts, idx) {
			continue
		}
		if probe := buildPKRangeProbe(tbl, idx, sel.Conditions); probe != nil {
			probes = append(probes, probe)
		}
	}
	return probes
}

// pkRangeProbeScanRows estimates how many rows the scan reads. A LIMIT may
// have lowered the scan's own estimate, but a full range scan can still read
// the whole table before it finds enough matching rows.
func pkRangeProbeScanRows(ts *physicalop.PhysicalTableScan) float64 {
	unsignedIntHandle := false
	if ts.Table.PKIsHandle {
		if pkCol := ts.Table.GetPkColInfo(); pkCol != nil {
			unsignedIntHandle = mysql.HasUnsignedFlag(pkCol.GetFlag())
		}
	}
	if ts.TblColHists != nil && ranger.HasFullRange(ts.Ranges, unsignedIntHandle) {
		return float64(ts.TblColHists.RealtimeCount)
	}
	return ts.StatsInfo().RowCount
}

// pkRangeProbeIndexUsable reports whether idx holds one entry per row, keyed
// by the column values followed by, or mapping to, the row's handle.
func pkRangeProbeIndexUsable(tbl *model.TableInfo, idx *model.IndexInfo) bool {
	if idx.State != model.StatePublic || idx.MVIndex || idx.Global || idx.IsColumnarIndex() {
		return false
	}
	// A clustered primary key is the table itself.
	return !idx.Primary || !tbl.HasClusteredIndex()
}

func pkRangeProbeIndexHinted(ts *physicalop.PhysicalTableScan, idx *model.IndexInfo) bool {
	if len(ts.PKFilterHintIndexes) == 0 {
		return true
	}
	for _, name := range ts.PKFilterHintIndexes {
		if name.L == idx.Name.L {
			return true
		}
	}
	return false
}

// buildPKRangeProbe returns a probe for idx when conds hold a
// `column = constant` condition for every column of idx.
func buildPKRangeProbe(tbl *model.TableInfo, idx *model.IndexInfo, conds []expression.Expression) *physicalop.PKRangeProbe {
	probe := &physicalop.PKRangeProbe{
		Index:      idx,
		EqConds:    make([]expression.Expression, 0, len(idx.Columns)),
		IdxCols:    make([]*expression.Column, 0, len(idx.Columns)),
		IdxColLens: make([]int, 0, len(idx.Columns)),
	}
	for _, idxCol := range idx.Columns {
		if idxCol.Offset < 0 || idxCol.Offset >= len(tbl.Columns) {
			return nil
		}
		colID := tbl.Columns[idxCol.Offset].ID
		cond, col := findColumnEqConstant(conds, colID)
		if cond == nil {
			return nil
		}
		probe.EqConds = append(probe.EqConds, cond)
		probe.IdxCols = append(probe.IdxCols, col)
		probe.IdxColLens = append(probe.IdxColLens, idxCol.Length)
	}
	return probe
}

// findColumnEqConstant finds a `column = constant` condition on the column
// with ID colID. The executor evaluates the constant again when it probes, so
// a constant with mutable effects such as rand() doesn't qualify.
func findColumnEqConstant(conds []expression.Expression, colID int64) (expression.Expression, *expression.Column) {
	for _, cond := range conds {
		sf, ok := cond.(*expression.ScalarFunction)
		if !ok || sf.FuncName.L != ast.EQ || expression.IsMutableEffectsExpr(cond) {
			continue
		}
		args := sf.GetArgs()
		for i := range 2 {
			col, isCol := args[i].(*expression.Column)
			_, isConst := args[1-i].(*expression.Constant)
			if isCol && isConst && col.ID == colID {
				return cond, col
			}
		}
	}
	return nil, nil
}
