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
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/planner/core/operator/logicalop"
	h "github.com/pingcap/tidb/pkg/util/hint"
)

// GenHintsFromFlatPlan generates hints from a FlatPhysicalPlan.
func GenHintsFromFlatPlan(flat *FlatPhysicalPlan) []*ast.TableOptimizerHint {
	if len(flat.Main) == 0 {
		return nil
	}
	nodeTp := h.TypeSelect
	switch flat.Main[0].Origin.(type) {
	case *Update:
		nodeTp = h.TypeUpdate
	case *Delete:
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
		if join, ok := p.(PhysicalJoin); ok {
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
			if join, ok := p.(PhysicalJoin); ok {
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
	case *PhysicalLimit, *PhysicalTopN:
		if storeType == kv.TiKV {
			res = append(res, &ast.TableOptimizerHint{
				QBName:   qbName,
				HintName: model.NewCIStr(h.HintLimitToCop),
			})
		}
	case *PhysicalTableReader:
		tbl, ok := pp.TablePlans[0].(*PhysicalTableScan)
		if !ok {
			return res
		}
		if tbl.StoreType == kv.TiFlash {
			res = append(res, &ast.TableOptimizerHint{
				QBName:   qbName,
				HintName: model.NewCIStr(h.HintReadFromStorage),
				HintData: model.NewCIStr(kv.TiFlash.Name()),
				Tables:   []ast.HintTable{{DBName: tbl.DBName, TableName: getTableName(tbl.Table.Name, tbl.TableAsName)}},
			})
		} else {
			res = append(res, &ast.TableOptimizerHint{
				QBName:   qbName,
				HintName: model.NewCIStr(h.HintUseIndex),
				Tables:   []ast.HintTable{{DBName: tbl.DBName, TableName: getTableName(tbl.Table.Name, tbl.TableAsName)}},
			})
			if tbl.Table.PKIsHandle || tbl.Table.IsCommonHandle { // it's a primary key
				orderHint := h.HintOrderIndex
				if !tbl.KeepOrder {
					orderHint = h.HintNoOrderIndex
				}
				res = append(res, &ast.TableOptimizerHint{
					QBName:   qbName,
					HintName: model.NewCIStr(orderHint),
					Tables:   []ast.HintTable{{DBName: tbl.DBName, TableName: getTableName(tbl.Table.Name, tbl.TableAsName)}},
					Indexes:  []model.CIStr{model.NewCIStr("primary")},
				})
			}
		}
	case *PhysicalIndexLookUpReader:
		index := pp.IndexPlans[0].(*PhysicalIndexScan)
		res = append(res, &ast.TableOptimizerHint{
			QBName:   qbName,
			HintName: model.NewCIStr(h.HintUseIndex),
			Tables:   []ast.HintTable{{DBName: index.DBName, TableName: getTableName(index.Table.Name, index.TableAsName)}},
			Indexes:  []model.CIStr{index.Index.Name},
		})
		orderHint := h.HintOrderIndex
		if !index.KeepOrder {
			orderHint = h.HintNoOrderIndex
		}
		res = append(res, &ast.TableOptimizerHint{
			QBName:   qbName,
			HintName: model.NewCIStr(orderHint),
			Tables:   []ast.HintTable{{DBName: index.DBName, TableName: getTableName(index.Table.Name, index.TableAsName)}},
			Indexes:  []model.CIStr{index.Index.Name},
		})
	case *PhysicalIndexReader:
		index := pp.IndexPlans[0].(*PhysicalIndexScan)
		res = append(res, &ast.TableOptimizerHint{
			QBName:   qbName,
			HintName: model.NewCIStr(h.HintUseIndex),
			Tables:   []ast.HintTable{{DBName: index.DBName, TableName: getTableName(index.Table.Name, index.TableAsName)}},
			Indexes:  []model.CIStr{index.Index.Name},
		})
		orderHint := h.HintOrderIndex
		if !index.KeepOrder {
			orderHint = h.HintNoOrderIndex
		}
		res = append(res, &ast.TableOptimizerHint{
			QBName:   qbName,
			HintName: model.NewCIStr(orderHint),
			Tables:   []ast.HintTable{{DBName: index.DBName, TableName: getTableName(index.Table.Name, index.TableAsName)}},
			Indexes:  []model.CIStr{index.Index.Name},
		})
	case *PhysicalIndexMergeReader:
		indexs := make([]model.CIStr, 0, 2)
		var tableName model.CIStr
		var tableAsName *model.CIStr
		for _, partialPlan := range pp.PartialPlans {
			if index, ok := partialPlan[0].(*PhysicalIndexScan); ok {
				indexs = append(indexs, index.Index.Name)
				tableName = index.Table.Name
				tableAsName = index.TableAsName
			} else {
				indexName := model.NewCIStr("PRIMARY")
				indexs = append(indexs, indexName)
			}
		}
		res = append(res, &ast.TableOptimizerHint{
			QBName:   qbName,
			HintName: model.NewCIStr(h.HintIndexMerge),
			Tables:   []ast.HintTable{{TableName: getTableName(tableName, tableAsName)}},
			Indexes:  indexs,
		})
	case *PhysicalHashAgg:
		res = append(res, &ast.TableOptimizerHint{
			QBName:   qbName,
			HintName: model.NewCIStr(h.HintHashAgg),
		})
		if storeType == kv.TiKV {
			res = append(res, &ast.TableOptimizerHint{
				QBName:   qbName,
				HintName: model.NewCIStr(h.HintAggToCop),
			})
		}
	case *PhysicalStreamAgg:
		res = append(res, &ast.TableOptimizerHint{
			QBName:   qbName,
			HintName: model.NewCIStr(h.HintStreamAgg),
		})
		if storeType == kv.TiKV {
			res = append(res, &ast.TableOptimizerHint{
				QBName:   qbName,
				HintName: model.NewCIStr(h.HintAggToCop),
			})
		}
	case *PhysicalMergeJoin:
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
	case *PhysicalHashJoin:
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
	case *PhysicalIndexJoin:
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
	case *PhysicalIndexMergeJoin:
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
	case *PhysicalIndexHashJoin:
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

func getTableName(tblName model.CIStr, asName *model.CIStr) model.CIStr {
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
		HintName: model.NewCIStr(joinType),
		Tables:   []ast.HintTable{*effectiveHintTbls[0]},
	}

	if hintQBName != nil {
		newHint.QBName = *hintQBName
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
) (hintTbls []*ast.HintTable, hintQBNamePtr *model.CIStr) {
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
	if qbOffset == -1 {
		return -1, false, nil
	}
	guessQBOffset = false
	var dbName, tableName *model.CIStr
	// For sub-queries like `(select * from t) t1`, t1 should belong to its surrounding select block.
	if qbOffset != parentOffset {
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
		qbOffset = joinNode.QueryBlockOffset()
		dbName, tableName = extractTableAsName(joinNode)
	}
	if tableName == nil || tableName.L == "" {
		return -1, false, nil
	}
	return qbOffset, guessQBOffset, &ast.HintTable{DBName: *dbName, TableName: *tableName}
}

func extractTableAsName(p base.PhysicalPlan) (*model.CIStr, *model.CIStr) {
	if len(p.Children()) > 1 {
		return nil, nil
	}
	switch x := p.(type) {
	case *PhysicalTableReader:
		ts := x.TablePlans[0].(*PhysicalTableScan)
		if ts.TableAsName != nil && ts.TableAsName.L != "" {
			return &ts.DBName, ts.TableAsName
		}
		return &ts.DBName, &ts.Table.Name
	case *PhysicalIndexReader:
		is := x.IndexPlans[0].(*PhysicalIndexScan)
		if is.TableAsName != nil && is.TableAsName.L != "" {
			return &is.DBName, is.TableAsName
		}
		return &is.DBName, &is.Table.Name
	case *PhysicalIndexLookUpReader:
		is := x.IndexPlans[0].(*PhysicalIndexScan)
		if is.TableAsName != nil && is.TableAsName.L != "" {
			return &is.DBName, is.TableAsName
		}
		return &is.DBName, &is.Table.Name
	case *PhysicalSort, *PhysicalSelection, *PhysicalUnionScan, *PhysicalProjection,
		*PhysicalHashAgg, *PhysicalStreamAgg:
		return extractTableAsName(p.Children()[0])
	}
	return nil, nil
}

// genJoinOrderHintFromRootPhysicalJoin is the entry point of generating join order hint.
func genJoinOrderHintFromRootPhysicalJoin(
	p PhysicalJoin,
	visitedIDs map[int]struct{},
	nodeType h.NodeType,
) *ast.TableOptimizerHint {
	if _, visited := visitedIDs[p.ID()]; visited {
		return nil
	}

	// 1. Get the joined operators in this join group with correct order in the slice.
	orderedJoinGroup := extractOrderedPhysicalJoinGroup(p, visitedIDs, 1)
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

	hintTblVals := make([]ast.HintTable, 0, len(hintTbls))
	for _, ht := range hintTbls {
		hintTblVals = append(hintTblVals, *ht)
	}
	res := &ast.TableOptimizerHint{
		HintName: model.NewCIStr(h.HintLeading),
		Tables:   hintTblVals,
	}
	if hintQBName != nil {
		res.QBName = *hintQBName
	}
	return res
}

func extractOrderedPhysicalJoinGroup(p PhysicalJoin, visitedIDs map[int]struct{}, depth uint) []base.PhysicalPlan {
	visitedIDs[p.ID()] = struct{}{}

	// 1. sanity checks

	// In our join reorder implementation, cartesian join will break the join relationship and make its two children
	// two independent join groups. So we don't need to handle it here.
	// Currently, index joins must match the index or PK of the inner table, so cartesian join must be a hash join.
	if hashJoin, ok := p.(*PhysicalHashJoin); ok {
		if len(hashJoin.EqualConditions) == 0 && len(hashJoin.NAEqualConditions) == 0 {
			return nil
		}
	}

	jt := p.GetJoinType()
	// They are the only join types supported by current join reorder.
	if jt != logicalop.InnerJoin && jt != logicalop.LeftOuterJoin && jt != logicalop.RightOuterJoin {
		return nil
	}

	// 2. Extract information from children according to whether the child is another Join, then construct the ordered
	// join group and return.

	var child0IsJoin, child1IsJoin bool
	var childJoin PhysicalJoin
	var childJoinGroup []base.PhysicalPlan
	if childJoin, child0IsJoin = p.Children()[0].(PhysicalJoin); child0IsJoin {
		childJoinGroup = extractOrderedPhysicalJoinGroup(childJoin, visitedIDs, depth+1)
	}
	if childJoin, child1IsJoin = p.Children()[1].(PhysicalJoin); child1IsJoin {
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
