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
	"github.com/pingcap/tidb/pkg/expression/aggregation"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	"github.com/pingcap/tidb/pkg/planner/property"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/types"
)

// markIndexJoinFirstRowPerRange finds index joins whose inner side computes
// MIN or MAX of one index column, grouped by the index columns before it, and
// makes the inner reader read only the first row of each lookup range.
//
// For example, with PRIMARY KEY (ns_id, id) on s:
//
//	SELECT t.ns_id, (SELECT MIN(s.id) FROM s WHERE s.ns_id = t.ns_id) FROM t
//
// becomes an index join whose inner side is MIN(s.id) GROUP BY s.ns_id over a
// range scan per ns_id. Each range is one group, and its first row in index
// order holds the minimum, so a Limit 1 per range gives the same result while
// reading one row per group instead of the whole group. The reader marked
// FirstRowPerRange sends every range as its own coprocessor task, batched per
// store, so the Limit applies to each range rather than to the whole request.
func markIndexJoinFirstRowPerRange(p base.PhysicalPlan) {
	for _, child := range p.Children() {
		markIndexJoinFirstRowPerRange(child)
	}
	switch x := p.(type) {
	case *physicalop.PhysicalIndexJoin:
		tryIndexJoinFirstRowPerRange(x)
	case *physicalop.PhysicalIndexHashJoin:
		tryIndexJoinFirstRowPerRange(&x.PhysicalIndexJoin)
	}
}

func tryIndexJoinFirstRowPerRange(join *physicalop.PhysicalIndexJoin) {
	// A range on the column after the join keys would come from CompareFilters
	// and is built per outer row; keep it simple and skip that case.
	if join.CompareFilters != nil {
		return
	}
	rootAgg := basePhysicalAggOf(join.Children()[join.InnerChildIdx])
	if rootAgg == nil || len(rootAgg.Children()) != 1 {
		return
	}
	var copRoot base.PhysicalPlan
	var setCopRoot func(base.PhysicalPlan)
	var markReader func()
	switch r := rootAgg.Children()[0].(type) {
	case *physicalop.PhysicalTableReader:
		if r.StoreType != kv.TiKV || r.ReadReqType != physicalop.Cop {
			return
		}
		copRoot = r.TablePlan
		setCopRoot = func(p base.PhysicalPlan) {
			r.TablePlan = p
		}
		markReader = func() {
			r.TablePlans = physicalop.FlattenListPushDownPlan(r.TablePlan)
			r.FirstRowPerRange = true
		}
	case *physicalop.PhysicalIndexReader:
		copRoot = r.IndexPlan
		setCopRoot = func(p base.PhysicalPlan) {
			r.IndexPlan = p
		}
		markReader = func() {
			r.IndexPlans = physicalop.FlattenListPushDownPlan(r.IndexPlan)
			r.FirstRowPerRange = true
		}
	default:
		return
	}

	// The aggregation whose arguments are scan columns is the partial one in
	// the coprocessor when the aggregation is pushed down, or the root one.
	argAgg := rootAgg
	limitChild := copRoot
	if copAgg := basePhysicalAggOf(copRoot); copAgg != nil {
		argAgg = copAgg
		limitChild = copAgg.Children()[0]
	}
	// Only a scan, optionally under one filter, may sit below the Limit. A
	// filter is fine: the first row that passes it still holds the minimum.
	scanNode := limitChild
	if sel, ok := scanNode.(*physicalop.PhysicalSelection); ok {
		scanNode = sel.Children()[0]
	}
	keyCols, tblInfo, colHists, setDesc := firstRowScanKeyColumns(scanNode)
	if keyCols == nil {
		return
	}

	// The GROUP BY must be exactly the index columns before the aggregated
	// column, and the join keys must fix all of them, so that each lookup
	// range is one group.
	prefixLen := len(argAgg.GroupByItems)
	if prefixLen == 0 || prefixLen >= len(keyCols) {
		return
	}
	prefixPos := make(map[int64]int, prefixLen)
	for i := range prefixLen {
		prefixPos[keyCols[i].ID] = i
	}
	seen := make(map[int]struct{}, prefixLen)
	for _, item := range argAgg.GroupByItems {
		col, ok := item.(*expression.Column)
		if !ok {
			return
		}
		pos, ok := prefixPos[col.ID]
		if !ok {
			return
		}
		seen[pos] = struct{}{}
	}
	if len(seen) != prefixLen {
		return
	}
	joinPos := make(map[int]struct{}, prefixLen)
	for _, idxOff := range join.KeyOff2IdxOff {
		if idxOff < 0 || idxOff >= prefixLen {
			return
		}
		joinPos[idxOff] = struct{}{}
	}
	if len(joinPos) != prefixLen {
		return
	}

	// The aggregated column is the next index column. It must be NOT NULL,
	// since MIN and MAX skip NULLs but a scan returns them, and of a type whose
	// index order is the order MIN and MAX compare in.
	target := keyCols[prefixLen]
	if !mysql.HasNotNullFlag(target.GetFlag()) || !firstRowOrderMatchesMinMax(target.GetType()) {
		return
	}
	isMax, ok := firstRowAggDirection(argAgg.AggFuncs, target.ID)
	if !ok || tblInfo.GetPartitionInfo() != nil {
		return
	}
	if !firstRowGroupsAreLarge(colHists, scanNode.Schema(), keyCols[:prefixLen]) {
		return
	}

	limit := physicalop.PhysicalLimit{Count: 1}.Init(join.SCtx(), property.DeriveLimitStats(limitChild.StatsInfo(), 1), limitChild.QueryBlockOffset())
	limit.SetChildren(limitChild)
	limit.SetSchema(limitChild.Schema())
	if argAgg == rootAgg {
		setCopRoot(limit)
	} else {
		argAgg.SetChildren(limit)
	}
	setDesc(isMax)
	markReader()

	// A range that spans regions is split into one task per region, so a
	// group can return a row from each, in any order. A stream aggregation
	// would then emit the group more than once; a hash aggregation merges it.
	if streamAgg, ok := join.Children()[join.InnerChildIdx].(*physicalop.PhysicalStreamAgg); ok {
		hashAgg := streamAgg.BasePhysicalAgg.InitForHash(streamAgg.SCtx(), streamAgg.StatsInfo(), streamAgg.QueryBlockOffset(), streamAgg.Schema())
		hashAgg.SetChildren(streamAgg.Children()...)
		join.Children()[join.InnerChildIdx] = hashAgg
		if join.InnerPlan == base.PhysicalPlan(streamAgg) {
			join.InnerPlan = hashAgg
		}
	}
}

// firstRowOrderMatchesMinMax reports whether index order for a column of type
// tp is the order MIN and MAX compare its values in, so that the first row of
// a range holds the minimum and the last the maximum. ENUM and SET are stored
// by member number and bitmask but compared by string value, so for
// ENUM('z','a') the index has 'z' first while MIN returns 'a'. Strings are
// left out because a collation can make distinct values compare equal, and
// floating point because -0 and +0 are distinct in the index but equal to MIN.
func firstRowOrderMatchesMinMax(tp byte) bool {
	switch tp {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong,
		mysql.TypeYear, mysql.TypeNewDecimal,
		mysql.TypeDate, mysql.TypeDatetime, mysql.TypeTimestamp, mysql.TypeDuration:
		return true
	}
	return false
}

// basePhysicalAggOf returns the aggregation in p, or nil if p isn't one.
func basePhysicalAggOf(p base.PhysicalPlan) *physicalop.BasePhysicalAgg {
	switch x := p.(type) {
	case *physicalop.PhysicalHashAgg:
		return &x.BasePhysicalAgg
	case *physicalop.PhysicalStreamAgg:
		return &x.BasePhysicalAgg
	}
	return nil
}

// firstRowScanKeyColumns returns the key columns of the scan in index order,
// the table, its column statistics, and a function that sets the scan
// direction. It returns nil key columns for scans the rewrite doesn't handle.
func firstRowScanKeyColumns(p base.PhysicalPlan) ([]*model.ColumnInfo, *model.TableInfo, *statistics.HistColl, func(bool)) {
	var tblInfo *model.TableInfo
	var idxInfo *model.IndexInfo
	var colHists *statistics.HistColl
	var setDesc func(bool)
	switch x := p.(type) {
	case *physicalop.PhysicalTableScan:
		if !x.Table.IsCommonHandle {
			return nil, nil, nil, nil
		}
		tblInfo, idxInfo, colHists = x.Table, tables.FindPrimaryIndex(x.Table), x.TblColHists
		setDesc = func(desc bool) { x.Desc, x.KeepOrder = desc, false }
	case *physicalop.PhysicalIndexScan:
		if x.Index.MVIndex || x.Index.Global {
			return nil, nil, nil, nil
		}
		tblInfo, idxInfo, colHists = x.Table, x.Index, x.TblColHists
		setDesc = func(desc bool) { x.Desc, x.KeepOrder = desc, false }
	default:
		return nil, nil, nil, nil
	}
	if idxInfo == nil {
		return nil, nil, nil, nil
	}
	cols := make([]*model.ColumnInfo, 0, len(idxInfo.Columns))
	for _, idxCol := range idxInfo.Columns {
		// A prefix-indexed column isn't ordered by its full value.
		if idxCol.Length != types.UnspecifiedLength {
			break
		}
		cols = append(cols, tblInfo.Columns[idxCol.Offset])
	}
	return cols, tblInfo, colHists, setDesc
}

// firstRowPerRangeMinGroupRows is the average group size from which reading
// one row per range pays off. TiKV reads its first batch of 32 rows before a
// Limit stops the scan, and sending each range as its own task costs more
// than scanning a few rows, so small groups are better read whole.
const firstRowPerRangeMinGroupRows = 256

// firstRowGroupsAreLarge reports whether the statistics show that the groups
// keyed by prefix average at least firstRowPerRangeMinGroupRows rows. The
// scan's TblColHists is keyed by the UniqueID of the scan's columns, not by
// the column ID, so each prefix column is looked up through the scan schema.
func firstRowGroupsAreLarge(colHists *statistics.HistColl, schema *expression.Schema, prefix []*model.ColumnInfo) bool {
	histIDs := make([]int64, 0, len(prefix))
	for _, c := range prefix {
		found := false
		for _, sc := range schema.Columns {
			if sc.ID == c.ID {
				histIDs = append(histIDs, sc.UniqueID)
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return avgGroupRowsAreLarge(colHists, histIDs)
}

// avgGroupRowsAreLarge reports whether the groups formed by the given columns
// average at least firstRowPerRangeMinGroupRows rows. histIDs are the keys of
// the columns in colHists.
func avgGroupRowsAreLarge(colHists *statistics.HistColl, histIDs []int64) bool {
	if colHists == nil || colHists.Pseudo || colHists.RealtimeCount <= 0 {
		return false
	}
	rows := float64(colHists.RealtimeCount)
	groups := 1.0
	for _, id := range histIDs {
		col := colHists.GetCol(id)
		if col == nil || !col.IsStatsInitialized() || col.Histogram.NDV <= 0 {
			return false
		}
		groups *= float64(col.Histogram.NDV)
	}
	return rows/min(groups, rows) >= firstRowPerRangeMinGroupRows
}

// firstRowAggDirection checks that the aggregate functions are MIN or MAX of
// the target column, all in the same direction, plus any FIRSTROW. It returns
// whether they are MAX.
func firstRowAggDirection(aggFuncs []*aggregation.AggFuncDesc, targetID int64) (isMax bool, ok bool) {
	found := false
	for _, f := range aggFuncs {
		switch f.Name {
		case ast.AggFuncFirstRow:
			continue
		case ast.AggFuncMin, ast.AggFuncMax:
		default:
			return false, false
		}
		if len(f.Args) != 1 {
			return false, false
		}
		col, isCol := f.Args[0].(*expression.Column)
		if !isCol || col.ID != targetID {
			return false, false
		}
		fIsMax := f.Name == ast.AggFuncMax
		if found && fIsMax != isMax {
			return false, false
		}
		found, isMax = true, fIsMax
	}
	return isMax, found
}
