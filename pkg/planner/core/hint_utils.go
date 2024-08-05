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
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/planner/core/base"
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

func getTableName(tblName model.CIStr, asName *model.CIStr) model.CIStr {
	if asName != nil && asName.L != "" {
		return *asName
	}
	return tblName
}

func extractTableAsName(p base.PhysicalPlan) (*model.CIStr, *model.CIStr) {
	if len(p.Children()) > 1 {
		return nil, nil
	}
	switch x := p.(type) {
	case *PhysicalTableReader:
		ts := x.TablePlans[0].(*PhysicalTableScan)
		if ts.TableAsName.L != "" {
			return &ts.DBName, ts.TableAsName
		}
		return &ts.DBName, &ts.Table.Name
	case *PhysicalIndexReader:
		is := x.IndexPlans[0].(*PhysicalIndexScan)
		if is.TableAsName.L != "" {
			return &is.DBName, is.TableAsName
		}
		return &is.DBName, &is.Table.Name
	case *PhysicalIndexLookUpReader:
		is := x.IndexPlans[0].(*PhysicalIndexScan)
		if is.TableAsName.L != "" {
			return &is.DBName, is.TableAsName
		}
		return &is.DBName, &is.Table.Name
	case *PhysicalSort, *PhysicalSelection, *PhysicalUnionScan, *PhysicalProjection,
		*PhysicalHashAgg, *PhysicalStreamAgg:
		return extractTableAsName(p.Children()[0])
	}
	return nil, nil
}

func extractHintTableForJoinNode(
	sctx base.PlanContext,
	joinNode base.PhysicalPlan,
	parentOffset int,
) (
	int,
	*ast.HintTable,
) {
	qbOffset := joinNode.QueryBlockOffset()
	if qbOffset == -1 {
		return -1, nil
	}
	var dbName, tableName *model.CIStr
	if qbOffset != parentOffset {
		var blockAsNames []ast.HintTable
		if p := sctx.GetSessionVars().PlannerSelectBlockAsName.Load(); p != nil {
			blockAsNames = *p
		}
		if qbOffset >= len(blockAsNames) {
			return -1, nil
		}
		hintTable := blockAsNames[qbOffset]
		// For sub-queries like `(select * from t) t1`, t1 should belong to its surrounding select block.
		dbName, tableName, qbOffset = &hintTable.DBName, &hintTable.TableName, parentOffset
	}
	if tableName == nil || tableName.L == "" {
		qbOffset = joinNode.QueryBlockOffset()
		dbName, tableName = extractTableAsName(joinNode)
	}
	if tableName == nil || tableName.L == "" {
		return -1, nil
	}
	return qbOffset, &ast.HintTable{DBName: *dbName, TableName: *tableName}
}

func getJoinMethodHintsForSinglePhysicalJoin(sctx base.PlanContext, joinType string, parentOffset int, nodeType h.NodeType, children ...base.PhysicalPlan) (res []*ast.TableOptimizerHint) {
	if parentOffset == -1 {
		return res
	}
	for _, child := range children {
		qbOffset, ht := extractHintTableForJoinNode(sctx, child, parentOffset)
		if qbOffset < 0 || ht == nil {
			return nil
		}
		qbName, err := h.GenerateQBName(nodeType, qbOffset)
		if err != nil {
			return nil
		}
		res = append(res, &ast.TableOptimizerHint{
			QBName:   qbName,
			HintName: model.NewCIStr(joinType),
			Tables:   []ast.HintTable{*ht},
		})
		break
	}
	return res
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
		res = append(res, getJoinMethodHintsForSinglePhysicalJoin(p.SCtx(), h.HintSMJ, p.QueryBlockOffset(), nodeType, pp.children...)...)
	case *PhysicalHashJoin:
		// TODO: support the hash_join_build and hash_join_probe hint for auto capture
		res = append(res, getJoinMethodHintsForSinglePhysicalJoin(p.SCtx(), h.HintHJ, p.QueryBlockOffset(), nodeType, pp.children...)...)
	case *PhysicalIndexJoin:
		res = append(res, getJoinMethodHintsForSinglePhysicalJoin(p.SCtx(), h.HintINLJ, p.QueryBlockOffset(), nodeType, pp.children[pp.InnerChildIdx])...)
	case *PhysicalIndexMergeJoin:
		res = append(res, getJoinMethodHintsForSinglePhysicalJoin(p.SCtx(), h.HintINLMJ, p.QueryBlockOffset(), nodeType, pp.children[pp.InnerChildIdx])...)
	case *PhysicalIndexHashJoin:
		res = append(res, getJoinMethodHintsForSinglePhysicalJoin(p.SCtx(), h.HintINLHJ, p.QueryBlockOffset(), nodeType, pp.children[pp.InnerChildIdx])...)
	}
	return res
}

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

	// 2. Generate the leading hint based on the ordered operators.
	minQBOffset := -1
	hintTbls := make([]ast.HintTable, 0, len(orderedJoinGroup))
	for _, plan := range orderedJoinGroup {
		qbOffset, ht := extractHintTableForJoinNode(p.SCtx(), plan, p.QueryBlockOffset())
		if qbOffset < 0 || ht == nil {
			return nil
		}
		if minQBOffset == -1 {
			minQBOffset = qbOffset
		} else {
			minQBOffset = min(minQBOffset, qbOffset)
		}
		tblQBNable, err := h.GenerateQBName(nodeType, qbOffset)
		if err != nil {
			return nil
		}
		ht.QBName = tblQBNable
		hintTbls = append(hintTbls, *ht)
	}
	// Current join reorder will break QB offset in the operator, e.g. setting them to -1.
	// So we are unable to get the correct QB offset for the join order hint from the join operator, now we use the
	// minimum QB offset among the tables.
	hintQBName, err := h.GenerateQBName(nodeType, minQBOffset)
	if err != nil {
		return nil
	}
	return &ast.TableOptimizerHint{
		QBName:   hintQBName,
		HintName: model.NewCIStr(h.HintLeading),
		Tables:   hintTbls,
	}
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
	if jt != InnerJoin && jt != LeftOuterJoin && jt != RightOuterJoin {
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
