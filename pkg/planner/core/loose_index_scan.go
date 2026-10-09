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
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/types"
)

// looseScanBatchSize is the number of rows a loose index scan reads per seek.
const looseScanBatchSize = 1

// attach2Task4LooseScan turns an index-only cop task, or a cop task over a
// clustered primary key, into a loose index scan under a complete-mode stream
// aggregation. The reader reads the first row of every distinct key prefix and
// the aggregation merges them, so this is only valid when every aggregate
// function can be computed from those rows. It returns base.InvalidTask when
// the cop task or the aggregation doesn't qualify.
func attach2Task4LooseScan(p *physicalop.PhysicalStreamAgg, t base.Task) base.Task {
	cop, ok := t.(*physicalop.CopTask)
	if !ok || cop.IndexJoinInfo != nil || len(cop.RootTaskConds) > 0 || len(cop.IdxMergePartPlans) > 0 ||
		cop.GetStoreType() != kv.TiKV {
		return base.InvalidTask
	}
	var scanPlan *base.PhysicalPlan
	switch {
	case cop.TablePlan == nil && cop.IndexPlan != nil:
		scanPlan = &cop.IndexPlan
	case cop.TablePlan != nil && cop.IndexPlan == nil:
		scanPlan = &cop.TablePlan
	default:
		return base.InvalidTask
	}
	// Only selections may sit between the scan and the limit we add: they keep
	// the first matching row of a group first.
	plan := *scanPlan
	for sel, ok := plan.(*physicalop.PhysicalSelection); ok; sel, ok = plan.(*physicalop.PhysicalSelection) {
		plan = sel.Children()[0]
	}
	var info *physicalop.LooseScanInfo
	switch x := plan.(type) {
	case *physicalop.PhysicalIndexScan:
		info = buildLooseScanInfo4IndexScan(p, x)
	case *physicalop.PhysicalTableScan:
		info = buildLooseScanInfo4TableScan(p, x)
	}
	if info == nil {
		return base.InvalidTask
	}

	limit := physicalop.PhysicalLimit{Count: info.BatchSize}.Init(p.SCtx(), p.StatsInfo(), p.QueryBlockOffset())
	limit.SetSchema((*scanPlan).Schema())
	limit.SetChildren(*scanPlan)
	*scanPlan = limit

	rt := cop.ConvertToRootTask(p.SCtx())
	switch reader := rt.Plan().(type) {
	case *physicalop.PhysicalIndexReader:
		reader.LooseScan = info
	case *physicalop.PhysicalTableReader:
		reader.LooseScan = info
	default:
		return base.InvalidTask
	}
	attachPlan2Task(p, rt)
	return rt
}

func buildLooseScanInfo4IndexScan(p *physicalop.PhysicalStreamAgg, is *physicalop.PhysicalIndexScan) *physicalop.LooseScanInfo {
	if !is.KeepOrder || is.Index == nil || is.Index.MVIndex || is.Index.IsColumnarIndex() ||
		is.Table.GetPartitionInfo() != nil || len(is.GroupByColIdxs) > 0 || len(is.ByItems) > 0 {
		return nil
	}
	return buildLooseScanInfo(p, is.IdxCols, is.IdxColLens, is.Schema(), is.Desc)
}

// buildLooseScanInfo4TableScan handles a scan over a clustered (common handle)
// primary key, whose row keys are ordered by the primary key columns. An
// integer handle is unique, so every group would have a single row.
func buildLooseScanInfo4TableScan(p *physicalop.PhysicalStreamAgg, ts *physicalop.PhysicalTableScan) *physicalop.LooseScanInfo {
	if !ts.KeepOrder || !ts.Table.IsCommonHandle || ts.Table.GetPartitionInfo() != nil ||
		len(ts.GroupByColIdxs) > 0 || len(ts.ByItems) > 0 || ts.HandleCols == nil {
		return nil
	}
	pk := ts.Table.GetPrimaryKey()
	if pk == nil || len(pk.Columns) != ts.HandleCols.NumCols() {
		return nil
	}
	keyCols := make([]*expression.Column, 0, len(pk.Columns))
	keyColLens := make([]int, 0, len(pk.Columns))
	for i, idxCol := range pk.Columns {
		keyCols = append(keyCols, ts.HandleCols.GetCol(i))
		keyColLens = append(keyColLens, idxCol.Length)
	}
	return buildLooseScanInfo(p, keyCols, keyColLens, ts.Schema(), ts.Desc)
}

// buildLooseScanInfo checks that the stream aggregation p can be computed from
// the first row of each distinct prefix of keyCols, the columns the scan's
// keys are ordered by, and returns the loose scan description, or nil if it
// can't. schema is the scan's output schema.
func buildLooseScanInfo(p *physicalop.PhysicalStreamAgg, keyCols []*expression.Column, keyColLens []int,
	schema *expression.Schema, desc bool) *physicalop.LooseScanInfo {
	keyPos := func(col *expression.Column) int {
		for i, keyCol := range keyCols {
			if keyCol != nil && keyCol.EqualColumn(col) {
				return i
			}
		}
		return -1
	}
	// The prefix ends at the last GROUP BY column in key order. Key columns
	// before it that aren't grouped on only split groups further, which the
	// aggregation above merges back.
	prefixLen := 0
	for _, item := range p.GroupByItems {
		col, ok := item.(*expression.Column)
		if !ok {
			return nil
		}
		pos := keyPos(col)
		if pos < 0 {
			return nil
		}
		prefixLen = max(prefixLen, pos+1)
	}
	usable := func(pos int) bool {
		if pos >= len(keyCols) || keyCols[pos] == nil || keyColLens[pos] != types.UnspecifiedLength {
			return false
		}
		col := keyCols[pos]
		if !schema.Contains(col) {
			return false
		}
		switch col.RetType.GetType() {
		case mysql.TypeJSON, mysql.TypeTiDBVectorFloat32, mysql.TypeGeometry:
			return false
		}
		return true
	}
	for i := range prefixLen {
		if !usable(i) {
			return nil
		}
	}
	info := &physicalop.LooseScanInfo{
		PrefixCols: make([]*expression.Column, 0, prefixLen),
		BatchSize:  looseScanBatchSize,
	}
	for i := range prefixLen {
		info.PrefixCols = append(info.PrefixCols, keyCols[i])
	}
	for _, aggFunc := range p.AggFuncs {
		switch aggFunc.Name {
		case ast.AggFuncFirstRow:
			// Any row of the group is a valid first row.
			continue
		case ast.AggFuncMin, ast.AggFuncMax:
		default:
			return nil
		}
		col, ok := aggFunc.Args[0].(*expression.Column)
		if !ok {
			return nil
		}
		pos := keyPos(col)
		if pos >= 0 && pos < prefixLen {
			// Constant within a group.
			continue
		}
		// The column right after the prefix is ordered within each group, so the
		// first row read has its smallest value (ascending scan) or largest
		// value (descending scan).
		if pos != prefixLen || !usable(pos) {
			return nil
		}
		if (aggFunc.Name == ast.AggFuncMin) == desc {
			return nil
		}
		if aggFunc.Name == ast.AggFuncMin && !mysql.HasNotNullFlag(col.RetType.GetFlag()) {
			// NULLs sort first in an ascending scan; descending MAX reads them
			// last, so only MIN needs the extra seek.
			info.NullSkipCol = keyCols[pos]
		}
	}
	return info
}
