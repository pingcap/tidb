// Copyright 2019 PingCAP, Inc.
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
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx"
	utilhint "github.com/pingcap/tidb/util/hint"
)

// GenHintsFromFlatPlan generates hints from a FlatPhysicalPlan.
func GenHintsFromFlatPlan(flat *FlatPhysicalPlan) []*ast.TableOptimizerHint {
	if len(flat.Main) == 0 {
		return nil
	}
	nodeTp := utilhint.TypeSelect
	switch flat.Main[0].Origin.(type) {
	case *Update:
		nodeTp = utilhint.TypeUpdate
	case *Delete:
		nodeTp = utilhint.TypeDelete
	}
	var hints []*ast.TableOptimizerHint
	selectPlan, _ := flat.Main.GetSelectPlan()
	if len(selectPlan) == 0 || !selectPlan[0].IsPhysicalPlan {
		return nil
	}
	for _, op := range selectPlan {
		if !op.IsRoot {
			continue
		}
		p := op.Origin.(PhysicalPlan)
		hints = genHintsFromSingle(p, nodeTp, hints)
	}
	for _, cte := range flat.CTEs {
		for i, op := range cte {
			if i == 0 || !op.IsRoot {
				continue
			}
			p := op.Origin.(PhysicalPlan)
			hints = genHintsFromSingle(p, nodeTp, hints)
		}
	}
	return hints
}

// GenHintsFromPhysicalPlan generates hints from physical plan.
// Deprecated: FlattenPhysicalPlan() + GenHintsFromFlatPlan() is preferred.
func GenHintsFromPhysicalPlan(p Plan) []*ast.TableOptimizerHint {
	var hints []*ast.TableOptimizerHint
	switch pp := p.(type) {
	case *Explain:
		return GenHintsFromPhysicalPlan(pp.TargetPlan)
	case *Update:
		hints = genHintsFromPhysicalPlan(pp.SelectPlan, utilhint.TypeUpdate)
	case *Delete:
		hints = genHintsFromPhysicalPlan(pp.SelectPlan, utilhint.TypeDelete)
	// For Insert, we only generate hints that would be used in select query block.
	case *Insert:
		hints = genHintsFromPhysicalPlan(pp.SelectPlan, utilhint.TypeSelect)
	case PhysicalPlan:
		hints = genHintsFromPhysicalPlan(pp, utilhint.TypeSelect)
	}
	return hints
}

func getTableName(tblName model.CIStr, asName *model.CIStr) model.CIStr {
	if asName != nil && asName.L != "" {
		return *asName
	}
	return tblName
}

func extractTableAsName(p PhysicalPlan) (*model.CIStr, *model.CIStr) {
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

func getJoinHints(sctx sessionctx.Context, joinType string, parentOffset int, nodeType utilhint.NodeType, children ...PhysicalPlan) (res []*ast.TableOptimizerHint) {
	if parentOffset == -1 {
		return res
	}
	for _, child := range children {
		blockOffset := child.SelectBlockOffset()
		if blockOffset == -1 {
			continue
		}
		var dbName, tableName *model.CIStr
		if blockOffset != parentOffset {
			blockAsNames := sctx.GetSessionVars().PlannerSelectBlockAsName
			if blockOffset >= len(blockAsNames) {
				continue
			}
			hintTable := blockAsNames[blockOffset]
			// For sub-queries like `(select * from t) t1`, t1 should belong to its surrounding select block.
			dbName, tableName, blockOffset = &hintTable.DBName, &hintTable.TableName, parentOffset
		} else {
			dbName, tableName = extractTableAsName(child)
		}
		if tableName == nil || tableName.L == "" {
			continue
		}
		qbName, err := utilhint.GenerateQBName(nodeType, blockOffset)
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

func genHintsFromPhysicalPlan(p PhysicalPlan, nodeType utilhint.NodeType) (res []*ast.TableOptimizerHint) {
	if p == nil {
		return res
	}
	for _, child := range p.Children() {
		res = append(res, genHintsFromPhysicalPlan(child, nodeType)...)
	}
	if phCte, ok := p.(*PhysicalCTE); ok {
		res = append(res, genHintsFromPhysicalPlan(phCte.CTE.seedPartPhysicalPlan, nodeType)...)
		res = append(res, genHintsFromPhysicalPlan(phCte.CTE.recursivePartPhysicalPlan, nodeType)...)
	}

	return genHintsFromSingle(p, nodeType, res)
}

func genHintsFromSingle(p PhysicalPlan, nodeType utilhint.NodeType, res []*ast.TableOptimizerHint) []*ast.TableOptimizerHint {
	qbName, err := utilhint.GenerateQBName(nodeType, p.SelectBlockOffset())
	if err != nil {
		return res
	}
	switch pp := p.(type) {
	case *PhysicalTableReader:
		tbl := pp.TablePlans[0].(*PhysicalTableScan)
		if tbl.StoreType == kv.TiFlash {
			res = append(res, &ast.TableOptimizerHint{
				QBName:   qbName,
				HintName: model.NewCIStr(HintReadFromStorage),
				HintData: model.NewCIStr(kv.TiFlash.Name()),
				Tables:   []ast.HintTable{{DBName: tbl.DBName, TableName: getTableName(tbl.Table.Name, tbl.TableAsName)}},
			})
		} else {
			res = append(res, &ast.TableOptimizerHint{
				QBName:   qbName,
				HintName: model.NewCIStr(HintUseIndex),
				Tables:   []ast.HintTable{{DBName: tbl.DBName, TableName: getTableName(tbl.Table.Name, tbl.TableAsName)}},
			})
		}
	case *PhysicalIndexLookUpReader:
		index := pp.IndexPlans[0].(*PhysicalIndexScan)
		res = append(res, &ast.TableOptimizerHint{
			QBName:   qbName,
			HintName: model.NewCIStr(HintUseIndex),
			Tables:   []ast.HintTable{{DBName: index.DBName, TableName: getTableName(index.Table.Name, index.TableAsName)}},
			Indexes:  []model.CIStr{index.Index.Name},
		})
	case *PhysicalIndexReader:
		index := pp.IndexPlans[0].(*PhysicalIndexScan)
		res = append(res, &ast.TableOptimizerHint{
			QBName:   qbName,
			HintName: model.NewCIStr(HintUseIndex),
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
			HintName: model.NewCIStr(HintIndexMerge),
			Tables:   []ast.HintTable{{TableName: getTableName(tableName, tableAsName)}},
			Indexes:  indexs,
		})
	case *PhysicalHashAgg:
		res = append(res, &ast.TableOptimizerHint{
			QBName:   qbName,
			HintName: model.NewCIStr(HintHashAgg),
		})
	case *PhysicalStreamAgg:
		res = append(res, &ast.TableOptimizerHint{
			QBName:   qbName,
			HintName: model.NewCIStr(HintStreamAgg),
		})
	case *PhysicalMergeJoin:
		res = append(res, getJoinHints(p.SCtx(), HintSMJ, p.SelectBlockOffset(), nodeType, pp.children...)...)
	case *PhysicalHashJoin:
		// TODO: support the hash_join_build and hash_join_probe hint for auto capture
		res = append(res, getJoinHints(p.SCtx(), HintHJ, p.SelectBlockOffset(), nodeType, pp.children...)...)
	case *PhysicalIndexJoin:
		res = append(res, getJoinHints(p.SCtx(), HintINLJ, p.SelectBlockOffset(), nodeType, pp.children[pp.InnerChildIdx])...)
	case *PhysicalIndexMergeJoin:
		res = append(res, getJoinHints(p.SCtx(), HintINLMJ, p.SelectBlockOffset(), nodeType, pp.children[pp.InnerChildIdx])...)
	case *PhysicalIndexHashJoin:
		res = append(res, getJoinHints(p.SCtx(), HintINLHJ, p.SelectBlockOffset(), nodeType, pp.children[pp.InnerChildIdx])...)
	}
	return res
}
