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
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/v4/kv"
	"github.com/pingcap/tidb/v4/sessionctx"
	utilhint "github.com/pingcap/tidb/v4/util/hint"
)

// GenHintsFromPhysicalPlan generates hints from physical plan.
func GenHintsFromPhysicalPlan(p Plan) []*ast.TableOptimizerHint {
	var hints []*ast.TableOptimizerHint
	switch pp := p.(type) {
	case *Explain:
		return GenHintsFromPhysicalPlan(pp.TargetPlan)
	case *Update:
		hints = genHintsFromPhysicalPlan(pp.SelectPlan, utilhint.TypeUpdate)
	case *Delete:
		hints = genHintsFromPhysicalPlan(pp.SelectPlan, utilhint.TypeDelete)
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
	}
	return nil, nil
}

func getJoinHints(sctx sessionctx.Context, joinType string, parentOffset int, nodeType utilhint.NodeType, children ...PhysicalPlan) (res []*ast.TableOptimizerHint) {
	for _, child := range children {
		blockOffset := child.SelectBlockOffset()
		if blockOffset == -1 {
			continue
		}
		var dbName, tableName *model.CIStr
		if child.SelectBlockOffset() != parentOffset {
			hintTable := sctx.GetSessionVars().PlannerSelectBlockAsName[child.SelectBlockOffset()]
			// For sub-queries like `(select * from t) t1`, t1 should belong to its surrounding select block.
			dbName, tableName, blockOffset = &hintTable.DBName, &hintTable.TableName, parentOffset
		} else {
			dbName, tableName = extractTableAsName(child)
		}
		if tableName == nil {
			continue
		}
		res = append(res, &ast.TableOptimizerHint{
			QBName:   utilhint.GenerateQBName(nodeType, blockOffset),
			HintName: model.NewCIStr(joinType),
			Tables:   []ast.HintTable{{DBName: *dbName, TableName: *tableName}},
		})
		break
	}
	return res
}

func genHintsFromPhysicalPlan(p PhysicalPlan, nodeType utilhint.NodeType) (res []*ast.TableOptimizerHint) {
	for _, child := range p.Children() {
		res = append(res, genHintsFromPhysicalPlan(child, nodeType)...)
	}
	switch pp := p.(type) {
	case *PhysicalTableReader:
		tbl := pp.TablePlans[0].(*PhysicalTableScan)
		res = append(res, &ast.TableOptimizerHint{
			QBName:   utilhint.GenerateQBName(nodeType, pp.blockOffset),
			HintName: model.NewCIStr(HintUseIndex),
			Tables:   []ast.HintTable{{DBName: tbl.DBName, TableName: getTableName(tbl.Table.Name, tbl.TableAsName)}},
		})
		if tbl.StoreType == kv.TiFlash {
			res = append(res, &ast.TableOptimizerHint{
				QBName:   utilhint.GenerateQBName(nodeType, pp.blockOffset),
				HintName: model.NewCIStr(HintReadFromStorage),
				HintData: model.NewCIStr(kv.TiFlash.Name()),
				Tables:   []ast.HintTable{{DBName: tbl.DBName, TableName: getTableName(tbl.Table.Name, tbl.TableAsName)}},
			})
		}
	case *PhysicalIndexLookUpReader:
		index := pp.IndexPlans[0].(*PhysicalIndexScan)
		res = append(res, &ast.TableOptimizerHint{
			QBName:   utilhint.GenerateQBName(nodeType, pp.blockOffset),
			HintName: model.NewCIStr(HintUseIndex),
			Tables:   []ast.HintTable{{DBName: index.DBName, TableName: getTableName(index.Table.Name, index.TableAsName)}},
			Indexes:  []model.CIStr{index.Index.Name},
		})
	case *PhysicalIndexReader:
		index := pp.IndexPlans[0].(*PhysicalIndexScan)
		res = append(res, &ast.TableOptimizerHint{
			QBName:   utilhint.GenerateQBName(nodeType, pp.blockOffset),
			HintName: model.NewCIStr(HintUseIndex),
			Tables:   []ast.HintTable{{DBName: index.DBName, TableName: getTableName(index.Table.Name, index.TableAsName)}},
			Indexes:  []model.CIStr{index.Index.Name},
		})
	case *PhysicalIndexMergeReader:
		Indexs := make([]model.CIStr, 0, 2)
		var tableName model.CIStr
		var tableAsName *model.CIStr
		for _, partialPlan := range pp.PartialPlans {
			if index, ok := partialPlan[0].(*PhysicalIndexScan); ok {
				Indexs = append(Indexs, index.Index.Name)
				tableName = index.Table.Name
				tableAsName = index.TableAsName
			} else {
				indexName := model.NewCIStr("PRIMARY")
				Indexs = append(Indexs, indexName)
			}
		}
		res = append(res, &ast.TableOptimizerHint{
			QBName:   utilhint.GenerateQBName(nodeType, pp.blockOffset),
			HintName: model.NewCIStr(HintIndexMerge),
			Tables:   []ast.HintTable{{TableName: getTableName(tableName, tableAsName)}},
			Indexes:  Indexs,
		})
	case *PhysicalHashAgg:
		res = append(res, &ast.TableOptimizerHint{
			QBName:   utilhint.GenerateQBName(nodeType, pp.blockOffset),
			HintName: model.NewCIStr(HintHashAgg),
		})
	case *PhysicalStreamAgg:
		res = append(res, &ast.TableOptimizerHint{
			QBName:   utilhint.GenerateQBName(nodeType, pp.blockOffset),
			HintName: model.NewCIStr(HintStreamAgg),
		})
	case *PhysicalMergeJoin:
		res = append(res, getJoinHints(p.SCtx(), HintSMJ, p.SelectBlockOffset(), nodeType, pp.children...)...)
	case *PhysicalHashJoin:
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
