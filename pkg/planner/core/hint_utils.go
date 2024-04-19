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
	for _, fop := range selectPlan {
		p := fop.Origin.(base.PhysicalPlan)
		hints = genHintsFromSingle(p, nodeTp, fop.StoreType, hints)
	}
	for _, cte := range flat.CTEs {
		for i, fop := range cte {
			if i == 0 || !fop.IsRoot {
				continue
			}
			p := fop.Origin.(base.PhysicalPlan)
			hints = genHintsFromSingle(p, nodeTp, fop.StoreType, hints)
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
	case *PhysicalSort, *PhysicalSelection, *PhysicalUnionScan, *PhysicalProjection:
		return extractTableAsName(p.Children()[0])
	}
	return nil, nil
}

func getJoinHints(sctx base.PlanContext, joinType string, parentOffset int, nodeType h.NodeType, children ...base.PhysicalPlan) (res []*ast.TableOptimizerHint) {
	if parentOffset == -1 {
		return res
	}
	for _, child := range children {
		qbOffset := child.QueryBlockOffset()
		if qbOffset == -1 {
			continue
		}
		var dbName, tableName *model.CIStr
		if qbOffset != parentOffset {
			var blockAsNames []ast.HintTable
			if p := sctx.GetSessionVars().PlannerSelectBlockAsName.Load(); p != nil {
				blockAsNames = *p
			}
			if qbOffset >= len(blockAsNames) {
				continue
			}
			hintTable := blockAsNames[qbOffset]
			// For sub-queries like `(select * from t) t1`, t1 should belong to its surrounding select block.
			dbName, tableName, qbOffset = &hintTable.DBName, &hintTable.TableName, parentOffset
		} else {
			dbName, tableName = extractTableAsName(child)
		}
		if tableName == nil || tableName.L == "" {
			continue
		}
		qbName, err := h.GenerateQBName(nodeType, qbOffset)
		if err != nil {
			continue
		}
		res = append(res, &ast.TableOptimizerHint{
			QBName:   qbName,
			HintName: model.NewCIStr(joinType),
			Tables:   []ast.HintTable{{DBName: *dbName, TableName: *tableName}},
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
		res = append(res, getJoinHints(p.SCtx(), h.HintSMJ, p.QueryBlockOffset(), nodeType, pp.children...)...)
	case *PhysicalHashJoin:
		// TODO: support the hash_join_build and hash_join_probe hint for auto capture
		res = append(res, getJoinHints(p.SCtx(), h.HintHJ, p.QueryBlockOffset(), nodeType, pp.children...)...)
	case *PhysicalIndexJoin:
		res = append(res, getJoinHints(p.SCtx(), h.HintINLJ, p.QueryBlockOffset(), nodeType, pp.children[pp.InnerChildIdx])...)
	case *PhysicalIndexMergeJoin:
		res = append(res, getJoinHints(p.SCtx(), h.HintINLMJ, p.QueryBlockOffset(), nodeType, pp.children[pp.InnerChildIdx])...)
	case *PhysicalIndexHashJoin:
		res = append(res, getJoinHints(p.SCtx(), h.HintINLHJ, p.QueryBlockOffset(), nodeType, pp.children[pp.InnerChildIdx])...)
	}
	return res
}
