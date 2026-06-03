// Copyright 2023 PingCAP, Inc.
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
	"slices"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/physicalop"
	plannerutil "github.com/pingcap/tidb/pkg/planner/util"
	h "github.com/pingcap/tidb/pkg/util/hint"
)

// GenHintsFromFlatPlan generates hints from a FlatPhysicalPlan.
func GenHintsFromFlatPlan(flat *FlatPhysicalPlan) []*ast.TableOptimizerHint {
	if len(flat.Main) == 0 {
		return nil
	}
	nodeTp := h.TypeSelect
	switch flat.Main[0].Origin.(type) {
	case *physicalop.Update:
		nodeTp = h.TypeUpdate
	case *physicalop.Delete:
		nodeTp = h.TypeDelete
	}
	var hints []*ast.TableOptimizerHint
	selectPlan, _ := flat.Main.GetSelectPlan()
	if len(selectPlan) == 0 || !selectPlan[0].IsPhysicalPlan {
		return nil
	}
	// To generate leading hint, we need to extract join group from the plan tree by traversing children of PhysicalJoin
	// operators. We use this map to avoid revisiting the same operator during this process.
	visitedPhysicalJoinIDs := make(map[int]struct{})
	for _, fop := range selectPlan {
		p := fop.Origin.(base.PhysicalPlan)
		hints = genHintsFromSingle(p, nodeTp, fop.StoreType, hints)
		if join, ok := p.(base.PhysicalJoin); ok {
			joinOrderHint := genJoinOrderHintFromRootPhysicalJoin(join, visitedPhysicalJoinIDs, nodeTp)
			if joinOrderHint != nil {
				hints = append(hints, joinOrderHint)
			}
		}
	}
	for _, cte := range flat.CTEs {
		for i, fop := range cte {
			if i == 0 || !fop.IsRoot {
				continue
			}
			p := fop.Origin.(base.PhysicalPlan)
			hints = genHintsFromSingle(p, nodeTp, fop.StoreType, hints)
			if join, ok := p.(base.PhysicalJoin); ok {
				joinOrderHint := genJoinOrderHintFromRootPhysicalJoin(join, visitedPhysicalJoinIDs, nodeTp)
				if joinOrderHint != nil {
					hints = append(hints, joinOrderHint)
				}
			}
		}
	}
	return h.RemoveDuplicatedHints(hints)
}

// GenHintsFromPhysicalPlan generates hints from physical plan.
func GenHintsFromPhysicalPlan(p base.Plan) []*ast.TableOptimizerHint {
	flat := FlattenPhysicalPlan(p, false)
	return GenHintsFromFlatPlan(flat)
}

func genHintsFromSingle(p base.PhysicalPlan, nodeType h.NodeType, storeType kv.StoreType, res []*ast.TableOptimizerHint) []*ast.TableOptimizerHint {
	qbName, err := h.GenerateQBName(nodeType, p.QueryBlockOffset())
	if err != nil {
		return res
	}
	switch pp := p.(type) {
	case *physicalop.PhysicalLimit, *physicalop.PhysicalTopN:
		if storeType == kv.TiKV {
			res = append(res, &ast.TableOptimizerHint{
				QBName:   qbName,
				HintName: ast.NewCIStr(h.HintLimitToCop),
			})
		}
	case *physicalop.PhysicalTableReader:
		tbl, ok := pp.TablePlans[0].(*physicalop.PhysicalTableScan)
		if !ok {
			return res
		}
		if tbl.StoreType == kv.TiFlash {
			res = append(res, &ast.TableOptimizerHint{
				QBName:   qbName,
				HintName: ast.NewCIStr(h.HintReadFromStorage),
				HintData: ast.NewCIStr(kv.TiFlash.Name()),
				Tables:   []ast.HintTable{{DBName: tbl.DBName, TableName: getTableName(tbl.Table.Name, tbl.TableAsName)}},
			})
		} else {
			res = append(res, &ast.TableOptimizerHint{
				QBName:   qbName,
				HintName: ast.NewCIStr(h.HintUseIndex),
				Tables:   []ast.HintTable{{DBName: tbl.DBName, TableName: getTableName(tbl.Table.Name, tbl.TableAsName)}},
			})
			if tbl.Table.PKIsHandle || tbl.Table.IsCommonHandle { // it's a primary key
				orderHint := h.HintOrderIndex
				if !tbl.KeepOrder {
					orderHint = h.HintNoOrderIndex
				}
				res = append(res, &ast.TableOptimizerHint{
					QBName:   qbName,
					HintName: ast.NewCIStr(orderHint),
					Tables:   []ast.HintTable{{DBName: tbl.DBName, TableName: getTableName(tbl.Table.Name, tbl.TableAsName)}},
					Indexes:  []ast.CIStr{ast.NewCIStr("primary")},
				})
			}
		}
	case *physicalop.PhysicalIndexLookUpReader:
		index := pp.IndexPlans[0].(*physicalop.PhysicalIndexScan)
		res = append(res, &ast.TableOptimizerHint{
			QBName:   qbName,
			HintName: ast.NewCIStr(h.HintUseIndex),
			Tables:   []ast.HintTable{{DBName: index.DBName, TableName: getTableName(index.Table.Name, index.TableAsName)}},
			Indexes:  []ast.CIStr{index.Index.Name},
		})
		orderHint := h.HintOrderIndex
		if !index.KeepOrder {
			orderHint = h.HintNoOrderIndex
		}
		res = append(res, &ast.TableOptimizerHint{
			QBName:   qbName,
			HintName: ast.NewCIStr(orderHint),
			Tables:   []ast.HintTable{{DBName: index.DBName, TableName: getTableName(index.Table.Name, index.TableAsName)}},
			Indexes:  []ast.CIStr{index.Index.Name},
		})
	case *physicalop.PhysicalIndexReader:
		index := pp.IndexPlans[0].(*physicalop.PhysicalIndexScan)
		res = append(res, &ast.TableOptimizerHint{
			QBName:   qbName,
			HintName: ast.NewCIStr(h.HintUseIndex),
			Tables:   []ast.HintTable{{DBName: index.DBName, TableName: getTableName(index.Table.Name, index.TableAsName)}},
			Indexes:  []ast.CIStr{index.Index.Name},
		})
		orderHint := h.HintOrderIndex
		if !index.KeepOrder {
			orderHint = h.HintNoOrderIndex
		}
		res = append(res, &ast.TableOptimizerHint{
			QBName:   qbName,
			HintName: ast.NewCIStr(orderHint),
			Tables:   []ast.HintTable{{DBName: index.DBName, TableName: getTableName(index.Table.Name, index.TableAsName)}},
			Indexes:  []ast.CIStr{index.Index.Name},
		})
	case *physicalop.PhysicalIndexMergeReader:
		indexs := make([]ast.CIStr, 0, 2)
		var tableName ast.CIStr
		var tableAsName *ast.CIStr
		for _, partialPlan := range pp.PartialPlans {
			if index, ok := partialPlan[0].(*physicalop.PhysicalIndexScan); ok {
				indexs = append(indexs, index.Index.Name)
				tableName = index.Table.Name
				tableAsName = index.TableAsName
			} else {
				indexName := ast.NewCIStr("PRIMARY")
				indexs = append(indexs, indexName)
			}
		}
		res = append(res, &ast.TableOptimizerHint{
			QBName:   qbName,
			HintName: ast.NewCIStr(h.HintIndexMerge),
			Tables:   []ast.HintTable{{TableName: getTableName(tableName, tableAsName)}},
			Indexes:  indexs,
		})
	case *physicalop.PhysicalHashAgg:
		res = append(res, &ast.TableOptimizerHint{
			QBName:   qbName,
			HintName: ast.NewCIStr(h.HintHashAgg),
		})
		if storeType == kv.TiKV {
			res = append(res, &ast.TableOptimizerHint{
				QBName:   qbName,
				HintName: ast.NewCIStr(h.HintAggToCop),
			})
		}
	case *physicalop.PhysicalStreamAgg:
		res = append(res, &ast.TableOptimizerHint{
			QBName:   qbName,
			HintName: ast.NewCIStr(h.HintStreamAgg),
		})
		if storeType == kv.TiKV {
			res = append(res, &ast.TableOptimizerHint{
				QBName:   qbName,
				HintName: ast.NewCIStr(h.HintAggToCop),
			})
		}
	case *physicalop.PhysicalApply:
		if pp.NoDecorrelate {
			children := pp.Children()
			if len(children) > 1 {
				innerOffset := children[1].QueryBlockOffset()
				if innerOffset > 0 && innerOffset != pp.QueryBlockOffset() {
					innerQB, err := h.GenerateQBName(nodeType, innerOffset)
					if err == nil {
						res = append(res, &ast.TableOptimizerHint{
							QBName:   innerQB,
							HintName: ast.NewCIStr(h.HintNoDecorrelate),
						})
					}
				}
			}
		}
	case *physicalop.PhysicalMergeJoin:
		hint := genJoinMethodHintForSinglePhysicalJoin(
			p.SCtx(),
			h.HintSMJ,
			p.QueryBlockOffset(),
			nodeType,
			false,
			pp.Children()...,
		)
		if hint != nil {
			res = append(res, hint)
		}
	case *physicalop.PhysicalHashJoin:
		// For semi join, hash_join_[build|probe] is not supported. See getHashJoins() for details.
		if pp.JoinType.IsSemiJoin() {
			hint := genJoinMethodHintForSinglePhysicalJoin(
				p.SCtx(),
				h.HintHJ,
				p.QueryBlockOffset(),
				nodeType,
				false,
				pp.Children()...,
			)
			if hint != nil {
				res = append(res, hint)
			}
			break
		}
		var buildSideChild, probeSideChild base.PhysicalPlan
		if pp.RightIsBuildSide() {
			buildSideChild = pp.Children()[1]
			probeSideChild = pp.Children()[0]
		} else {
			buildSideChild = pp.Children()[0]
			probeSideChild = pp.Children()[1]
		}
		hint := genJoinMethodHintForSinglePhysicalJoin(
			p.SCtx(),
			h.HintHashJoinBuild,
			p.QueryBlockOffset(),
			nodeType,
			true,
			buildSideChild,
			probeSideChild,
		)
		if hint != nil {
			res = append(res, hint)
		} else {
			// In case we failed to generate the hint for build side, we try to generate the hint for probe side.
			hint := genJoinMethodHintForSinglePhysicalJoin(
				p.SCtx(),
				h.HintHashJoinProbe,
				p.QueryBlockOffset(),
				nodeType,
				true,
				probeSideChild,
				buildSideChild,
			)
			if hint != nil {
				res = append(res, hint)
			}
		}
	case *physicalop.PhysicalIndexJoin:
		hint := genJoinMethodHintForSinglePhysicalJoin(
			p.SCtx(),
			h.HintINLJ,
			p.QueryBlockOffset(),
			nodeType,
			true,
			pp.Children()[pp.InnerChildIdx],
			pp.Children()[1-pp.InnerChildIdx],
		)
		if hint != nil {
			res = append(res, hint)
		}
	case *physicalop.PhysicalIndexMergeJoin:
		hint := genJoinMethodHintForSinglePhysicalJoin(
			p.SCtx(),
			h.HintINLMJ,
			p.QueryBlockOffset(),
			nodeType,
			true,
			pp.Children()[pp.InnerChildIdx],
			pp.Children()[1-pp.InnerChildIdx],
		)
		if hint != nil {
			res = append(res, hint)
		}
	case *physicalop.PhysicalIndexHashJoin:
		hint := genJoinMethodHintForSinglePhysicalJoin(
			p.SCtx(),
			h.HintINLHJ,
			p.QueryBlockOffset(),
			nodeType,
			true,
			pp.Children()[pp.InnerChildIdx],
			pp.Children()[1-pp.InnerChildIdx],
		)
		if hint != nil {
			res = append(res, hint)
		}
	}
	return res
}

func getTableName(tblName ast.CIStr, asName *ast.CIStr) ast.CIStr {
	if asName != nil && asName.L != "" {
		return *asName
	}
	return tblName
}

// genJoinMethodHintForSinglePhysicalJoin is the entry point of generating join method hint.
// It generates a join method hint for a single physical join operator according to the input joinType.
// Both children of the Join should be passed in as the children arguments, this is for correctly deriving the QB offset
// for the hint.
// For hints like merge_join(), we can generate hint using table name of any one of the two tables. But for hints like
// hash_join_build() and inl_join(), we want to generate hint using table name of a specific side. For this difference,
// we introduce the onlyFirstTbl argument. If onlyFirstTbl is true, we only try to generate hint using the table name of
// the children[0].
func genJoinMethodHintForSinglePhysicalJoin(
	sctx base.PlanContext,
	joinType string,
	parentQBOffset int,
	nodeType h.NodeType,
	onlyFirstTbl bool,
	children ...base.PhysicalPlan,
) *ast.TableOptimizerHint {
	if parentQBOffset == -1 {
		return nil
	}
	hintTbls, hintQBName := genHintTblForJoinNodes(sctx, children, parentQBOffset, nodeType)
	effectiveHintTbls := slices.DeleteFunc(slices.Clone(hintTbls), func(ht *ast.HintTable) bool {
		return ht == nil
	})
	if len(effectiveHintTbls) == 0 {
		return nil
	}

	if onlyFirstTbl && hintTbls[0] == nil {
		return nil
	}

	newHint := &ast.TableOptimizerHint{
		HintName: ast.NewCIStr(joinType),
		Tables:   []ast.HintTable{*effectiveHintTbls[0]},
	}

	if hintQBName != nil {
		newHint.QBName = *hintQBName
	} else if effectiveHintTbls[0].QBName.L != "" {
		// TODO: This fallback only preserves the existing replay behavior for join-method hints that finally export a
		// single effective inner query-block table. Mixed-QB replay correctness still needs a dedicated fix here,
		// instead of opportunistically suppressing the hint or generalizing table-level QB inference, because analogous
		// single-table inner-QB hints (for example LEADING(@sel_2 t3@sel_2)) can still produce warnings on replay.
		newHint.QBName = effectiveHintTbls[0].QBName
	}

	return newHint
}

// genHintTblForJoinNodes tries to generate ast.HintTable for each join node, and the QB name for the hint itself.
// (Join node here means the operators that are joined, not Join operator itself)
// If the return values is not (nil,nil), len(hintTbls) should be equal to len(joinedNodes). The invalid ones in the
// returned hintTbls slice will be nil.
// The hintQBNamePtr will be nil if it's not needed, or we failed to generate one.
func genHintTblForJoinNodes(
	sctx base.PlanContext,
	joinedNodes []base.PhysicalPlan,
	parentQBOffset int,
	nodeType h.NodeType,
) (hintTbls []*ast.HintTable, hintQBNamePtr *ast.CIStr) {
	// 1. Use genHintTblForSingleJoinNode() to generate QB offset and table name for each join node.

	// Note that if we failed to generate valid information for one element in joinedNodes, we append -1 and nil instead
	// of skipping.
	// So qbOffsets[x] is -1 if and only if hintTbls[x] is nil;
	// and qbOffsets[x] >=0 if and only if hintTbls[x] is not nil.
	hintTbls = make([]*ast.HintTable, 0, len(joinedNodes))
	qbOffsets := make([]int, 0, len(joinedNodes))
	guessQBOffsets := make(map[int]struct{})
	for _, plan := range joinedNodes {
		qbOffset, guessOffset, ht := genHintTblForSingleJoinNode(sctx, plan, parentQBOffset)
		if qbOffset < 0 || ht == nil {
			qbOffsets = append(qbOffsets, -1)
			hintTbls = append(hintTbls, nil)
			continue
		}
		// If we guessed the same QB offset for two different nodes, that's likely incorrect, and we stop use that.
		// This may happen for queries like ... FROM t1 join (select * from t2 join t3) derived ... . We will guess
		// derived@sel_1 for both t2 and t3, and that's incorrect. Besides, current leading hint also can't handle this
		// kind of hints.
		if guessOffset {
			if _, ok := guessQBOffsets[qbOffset]; ok {
				qbOffsets = append(qbOffsets, -1)
				hintTbls = append(hintTbls, nil)
				continue
			}
			guessQBOffsets[qbOffset] = struct{}{}
		}
		qbOffsets = append(qbOffsets, qbOffset)
		hintTbls = append(hintTbls, ht)
	}

	// 2. Add QB name for each table name in the hint.

	for i, hintTbl := range hintTbls {
		if hintTbl == nil {
			continue
		}
		// In quick binding, we always put the generated hints in the first valid place in the SQL.
		// That implies hintname(@del_1) and hintname(@upd_1) is unnecessary in UPDATE/DELETE statements, and
		// hintname(@sel_1) is unnecessary in SELECT statements.
		// We don't generate QB name for the table names in the hint in this case to make the result cleaner.
		if (qbOffsets[i] <= 1 && nodeType == h.TypeSelect) ||
			(qbOffsets[i] == 0 && (nodeType == h.TypeUpdate || nodeType == h.TypeDelete)) {
			continue
		}
		tblQBName, err := h.GenerateQBName(nodeType, qbOffsets[i])
		if err != nil {
			continue
		}
		hintTbls[i].QBName = tblQBName
	}

	// 3. Generate QB name for the hint itself based on the QB name of each join node from step 1.

	// Current join reorder will break QB offset of the join operator, e.g. setting them to -1.
	// So we are unable to get the correct QB offset for the hint from the join operator, now we use the minimum QB
	// offset among the tables.
	// Besides, genHintTblForSingleJoinNode() is not powerful enough to handle all cases, it may fail in some cases.
	// If we failed to get QB offset information from one join node, we don't generate QB name for the hint. Because
	// that may cause a wrong QB offset, leaving it blank is probably better.
	if slices.Contains(qbOffsets, -1) {
		return hintTbls, nil
	}
	minQBOffset := slices.Min(qbOffsets)

	// ditto. We don't generate unnecessary QB name for the hint itself.
	if (minQBOffset > 1 && nodeType == h.TypeSelect) ||
		(minQBOffset > 0 && (nodeType == h.TypeUpdate || nodeType == h.TypeDelete)) {
		hintQBName, err := h.GenerateQBName(nodeType, minQBOffset)
		if err != nil {
			return nil, nil
		}
		hintQBNamePtr = &hintQBName
	}
	return hintTbls, hintQBNamePtr
}

// genHintTblForSingleJoinNode tries to generate ast.HintTable and QB offset for a single join node.
// See the comments inside about the meaning of guessQBOffset.
func genHintTblForSingleJoinNode(
	sctx base.PlanContext,
	joinNode base.PhysicalPlan,
	parentOffset int,
) (
	qbOffset int,
	guessQBOffset bool,
	ht *ast.HintTable,
) {
	selfOffset := joinNode.QueryBlockOffset()
	qbOffset = selfOffset
	guessQBOffset = false
	var dbName, tableName *ast.CIStr
	// For sub-queries like `(select * from t) t1`, t1 should belong to its surrounding select block.
	if qbOffset >= 0 && qbOffset != parentOffset {
		var blockAsNames []ast.HintTable
		if p := sctx.GetSessionVars().PlannerSelectBlockAsName.Load(); p != nil {
			blockAsNames = *p
		}
		if qbOffset >= len(blockAsNames) {
			return -1, false, nil
		}
		hintTable := blockAsNames[qbOffset]
		dbName, tableName, qbOffset = &hintTable.DBName, &hintTable.TableName, parentOffset
		// Current join reorder will break QB offset of the join operator by setting them to -1. In this case, we will
		// get qbOffset == parentOffset == -1 when it comes here.
		// For this case, we add a temporary fix to guess the QB offset based on the parent offset. The idea is simple,
		// for the example above, we can easily notice that the QBOffset(t1) = QBOffset(t) - 1. This is not always true,
		// but it works in simple cases.
		if selfOffset > 1 && qbOffset == -1 {
			guessQBOffset = true
			qbOffset = selfOffset - 1
		}
	}
	if tableName == nil || tableName.L == "" {
		guessQBOffset = false
		qbOffset, dbName, tableName = extractHintTableByBlockOffset(sctx, joinNode, parentOffset)
		if tableName == nil || tableName.L == "" {
			qbOffset, dbName, tableName = extractHintTableByOutputNames(joinNode, parentOffset)
		}
		if tableName == nil || tableName.L == "" {
			qbOffset = joinNode.QueryBlockOffset()
			dbName, tableName = extractTableAsName(joinNode)
		}
	}
	if tableName == nil || tableName.L == "" {
		return -1, false, nil
	}
	return qbOffset, guessQBOffset, &ast.HintTable{DBName: *dbName, TableName: *tableName}
}

// extractHintTableByBlockOffset walks the physical sub-tree and tries to recover a
// single derived/CTE alias from descendant query-block metadata.
//
// Why this exists:
//   - genHintTblForSingleJoinNode() first tries joinNode.QueryBlockOffset() directly.
//   - That works only when the current node itself still carries the query-block
//     identity we need.
//   - In practice, the current node may be a wrapping Projection/Selection/Agg node,
//     while only one descendant still keeps the inner query-block offset that maps to
//     the derived-table alias registered in PlannerSelectBlockAsName.
//
// The function is intentionally conservative:
//   - if we cannot find any descendant alias, return nil;
//   - if we find more than one distinct candidate, return nil.
//
// When a unique candidate is found, we still return parentOffset (instead of the
// descendant's own offset) because the recovered alias is used as the outer query's
// visible join item in generated hints.
func extractHintTableByBlockOffset(
	sctx base.PlanContext,
	p base.PhysicalPlan,
	parentOffset int,
) (qbOffset int, db *ast.CIStr, table *ast.CIStr) {
	var blockAsNames []ast.HintTable
	if names := sctx.GetSessionVars().PlannerSelectBlockAsName.Load(); names != nil {
		blockAsNames = *names
	}
	if len(blockAsNames) == 0 {
		return -1, nil, nil
	}

	var (
		found      *ast.HintTable
		foundQbOff int
		ambiguous  bool
	)
	var walk func(base.PhysicalPlan)
	walk = func(cur base.PhysicalPlan) {
		if ambiguous {
			return
		}
		offset := cur.QueryBlockOffset()
		if offset >= 0 && offset < len(blockAsNames) && offset != parentOffset {
			hintTable := blockAsNames[offset]
			if hintTable.TableName.L != "" {
				if found == nil {
					copied := hintTable
					found = &copied
					foundQbOff = offset
				} else if found.DBName.L != hintTable.DBName.L || found.TableName.L != hintTable.TableName.L || foundQbOff != offset {
					// Multiple different descendant aliases means we cannot safely decide
					// which outer visible name this join node should use in a hint.
					ambiguous = true
					return
				}
			}
		}
		for _, child := range cur.Children() {
			walk(child)
		}
	}
	walk(p)
	if ambiguous || found == nil {
		return -1, nil, nil
	}
	if parentOffset >= 0 {
		// The alias may be discovered from an inner block, but the generated hint item
		// is attached to the current outer join group.
		return parentOffset, &found.DBName, &found.TableName
	}
	return -1, &found.DBName, &found.TableName
}

// extractHintTableByOutputNames recovers a hint table from the physical node's final
// output names.
//
// Why this exists:
//   - sometimes descendant query-block metadata is no longer sufficient or no longer
//     unique;
//   - however, the physical node may already expose a stable single alias in all of
//     its output columns (for example, a wrapped derived-table alias after Projection).
//
// This is also conservative because plannerutil.ExtractTableAlias() returns nil unless
// the output names consistently point to one alias. So this fallback may miss some
// cases, but it avoids inventing a possibly wrong hint table name.
func extractHintTableByOutputNames(p base.PhysicalPlan, parentOffset int) (qbOffset int, db *ast.CIStr, table *ast.CIStr) {
	tbl := plannerutil.ExtractTableAlias(p, parentOffset)
	if tbl == nil {
		return -1, nil, nil
	}
	return tbl.SelectOffset, &tbl.DBName, &tbl.TblName
}

func extractTableAsName(p base.PhysicalPlan) (db *ast.CIStr, table *ast.CIStr) {
	if len(p.Children()) > 1 {
		return nil, nil
	}
	switch x := p.(type) {
	case *physicalop.PhysicalTableReader:
		ts := x.TablePlans[0].(*physicalop.PhysicalTableScan)
		if ts.TableAsName != nil && ts.TableAsName.L != "" {
			return &ts.DBName, ts.TableAsName
		}
		return &ts.DBName, &ts.Table.Name
	case *physicalop.PhysicalIndexReader:
		is := x.IndexPlans[0].(*physicalop.PhysicalIndexScan)
		if is.TableAsName != nil && is.TableAsName.L != "" {
			return &is.DBName, is.TableAsName
		}
		return &is.DBName, &is.Table.Name
	case *physicalop.PhysicalIndexLookUpReader:
		is := x.IndexPlans[0].(*physicalop.PhysicalIndexScan)
		if is.TableAsName != nil && is.TableAsName.L != "" {
			return &is.DBName, is.TableAsName
		}
		return &is.DBName, &is.Table.Name
	case *physicalop.PhysicalSort, *physicalop.PhysicalSelection, *physicalop.PhysicalUnionScan, *physicalop.PhysicalProjection,
		*physicalop.PhysicalHashAgg, *physicalop.PhysicalStreamAgg:
		return extractTableAsName(p.Children()[0])
	}
	return nil, nil
}

// genJoinOrderHintFromRootPhysicalJoin is the entry point of generating join order hint.
func genJoinOrderHintFromRootPhysicalJoin(
	p base.PhysicalJoin,
	visitedIDs map[int]struct{},
	nodeType h.NodeType,
) *ast.TableOptimizerHint {
	if _, visited := visitedIDs[p.ID()]; visited {
		return nil
	}

	// 1. Get the joined operators in this join group with correct order in the slice.
	// Only mark a join group as visited after we successfully emit a LEADING hint for it.
	// Otherwise a larger unsupported/partially-unmappable group would suppress smaller valid sub-groups.
	localVisitedIDs := make(map[int]struct{})
	orderedJoinGroup := extractOrderedPhysicalJoinGroup(p, localVisitedIDs, 1)
	// If it only involves two tables, we don't need to generate the join order hint.
	if len(orderedJoinGroup) <= 2 {
		return nil
	}

	// 2. Generate the leading hint based on the ordered join nodes.
	hintTbls, hintQBName := genHintTblForJoinNodes(p.SCtx(), orderedJoinGroup, p.QueryBlockOffset(), nodeType)

	// For now, we generate the leading hint only if we successfully generate the names for all nodes.
	if slices.Contains(hintTbls, nil) {
		return nil
	}
	if len(hintTbls) <= 2 {
		return nil
	}
	for id := range localVisitedIDs {
		visitedIDs[id] = struct{}{}
	}

	hintTblVals := make([]ast.HintTable, 0, len(hintTbls))
	leadingItems := make([]any, 0, len(hintTbls))
	for _, ht := range hintTbls {
		hintTblVals = append(hintTblVals, *ht)
		leadingItems = append(leadingItems, ht)
	}
	res := &ast.TableOptimizerHint{
		HintName: ast.NewCIStr(h.HintLeading),
		Tables:   hintTblVals,
		// LEADING has special restore logic in parser/ast: HintData preserves the
		// structured LeadingList form so Restore() can emit the hint-level QBName
		// in the correct position and keep table-level QB annotations stable.
		HintData: &ast.LeadingList{Items: leadingItems},
	}
	if hintQBName != nil {
		res.QBName = *hintQBName
	}
	return res
}

func extractOrderedPhysicalJoinGroup(p base.PhysicalJoin, visitedIDs map[int]struct{}, depth uint) []base.PhysicalPlan {
	visitedIDs[p.ID()] = struct{}{}

	// 1. sanity checks

	// In our join reorder implementation, cartesian join will break the join relationship and make its two children
	// two independent join groups. So we don't need to handle it here.
	// Currently, index joins must match the index or PK of the inner table, so cartesian join must be a hash join.
	if hashJoin, ok := p.(*physicalop.PhysicalHashJoin); ok {
		if len(hashJoin.EqualConditions) == 0 && len(hashJoin.NAEqualConditions) == 0 {
			return nil
		}
	}

	jt := p.GetJoinType()
	// They are the only join types supported by current join reorder.
	if jt != base.InnerJoin && jt != base.LeftOuterJoin && jt != base.RightOuterJoin {
		return nil
	}

	// 2. Extract information from children according to whether the child is another Join, then construct the ordered
	// join group and return.

	var child0IsJoin, child1IsJoin bool
	var childJoin base.PhysicalJoin
	var childJoinGroup []base.PhysicalPlan
	if childJoin, child0IsJoin = p.Children()[0].(base.PhysicalJoin); child0IsJoin {
		childJoinGroup = extractOrderedPhysicalJoinGroup(childJoin, visitedIDs, depth+1)
	}
	if childJoin, child1IsJoin = p.Children()[1].(base.PhysicalJoin); child1IsJoin {
		childJoinGroup = extractOrderedPhysicalJoinGroup(childJoin, visitedIDs, depth+1)
	}

	// case 1 - bushy join: not supported now, also should not appear now
	if child0IsJoin && child1IsJoin {
		return nil
	}
	// case 2 - leaf join operator: initialize the join group with the two children
	if !child0IsJoin && !child1IsJoin {
		// preallocate the slice based on the number of join operators to avoid reallocations
		orderedJoinGroup := make([]base.PhysicalPlan, 0, depth+1)
		orderedJoinGroup = append(orderedJoinGroup, p.Children()[0], p.Children()[1])
		return orderedJoinGroup
	}
	// case 3 - non-leaf join operator: append the non-join child to the join group from the Join child
	if len(childJoinGroup) < 2 {
		return nil
	}
	var orderedJoinGroup []base.PhysicalPlan
	if child0IsJoin {
		orderedJoinGroup = append(childJoinGroup, p.Children()[1])
	} else {
		orderedJoinGroup = append(childJoinGroup, p.Children()[0])
	}
	return orderedJoinGroup
}
