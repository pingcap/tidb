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
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/bindinfo"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// BlockHintProcessor processes hints at different level of sql statement.
type BlockHintProcessor struct {
	QbNameMap        map[string]int                    // Map from query block name to select stmt offset.
	QbHints          map[int][]*ast.TableOptimizerHint // Group all hints at same query block.
	Ctx              sessionctx.Context
	selectStmtOffset int
}

// MaxSelectStmtOffset returns the current stmt offset.
func (p *BlockHintProcessor) MaxSelectStmtOffset() int {
	return p.selectStmtOffset
}

// Enter implements Visitor interface.
func (p *BlockHintProcessor) Enter(in ast.Node) (ast.Node, bool) {
	switch node := in.(type) {
	case *ast.UpdateStmt:
		p.checkQueryBlockHints(node.TableHints, 0)
	case *ast.DeleteStmt:
		p.checkQueryBlockHints(node.TableHints, 0)
	case *ast.SelectStmt:
		p.selectStmtOffset++
		node.QueryBlockOffset = p.selectStmtOffset
		p.checkQueryBlockHints(node.TableHints, node.QueryBlockOffset)
	}
	return in, false
}

// Leave implements Visitor interface.
func (p *BlockHintProcessor) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}

const hintQBName = "qb_name"

// checkQueryBlockHints checks the validity of query blocks and records the map of query block name to select offset.
func (p *BlockHintProcessor) checkQueryBlockHints(hints []*ast.TableOptimizerHint, offset int) {
	var qbName string
	for _, hint := range hints {
		if hint.HintName.L != hintQBName {
			continue
		}
		if qbName != "" {
			p.Ctx.GetSessionVars().StmtCtx.AppendWarning(errors.New(fmt.Sprintf("There are more than two query names in same query block,, using the first one %s", qbName)))
		} else {
			qbName = hint.QBName.L
		}
	}
	if qbName == "" {
		return
	}
	if p.QbNameMap == nil {
		p.QbNameMap = make(map[string]int)
	}
	if _, ok := p.QbNameMap[qbName]; ok {
		p.Ctx.GetSessionVars().StmtCtx.AppendWarning(errors.New(fmt.Sprintf("Duplicate query block name %s, only the first one is effective", qbName)))
	} else {
		p.QbNameMap[qbName] = offset
	}
}

const (
	defaultUpdateBlockName   = "upd_1"
	defaultDeleteBlockName   = "del_1"
	defaultSelectBlockPrefix = "sel_"
)

type nodeType int

const (
	typeUpdate nodeType = iota
	typeDelete
	typeSelect
)

// getBlockName finds the offset of query block name. It use 0 as offset for top level update or delete,
// -1 for invalid block name.
func (p *BlockHintProcessor) getBlockOffset(blockName model.CIStr, nodeType nodeType) int {
	if p.QbNameMap != nil {
		level, ok := p.QbNameMap[blockName.L]
		if ok {
			return level
		}
	}
	// Handle the default query block name.
	if nodeType == typeUpdate && blockName.L == defaultUpdateBlockName {
		return 0
	}
	if nodeType == typeDelete && blockName.L == defaultDeleteBlockName {
		return 0
	}
	if nodeType == typeSelect && strings.HasPrefix(blockName.L, defaultSelectBlockPrefix) {
		suffix := blockName.L[len(defaultSelectBlockPrefix):]
		level, err := strconv.ParseInt(suffix, 10, 64)
		if err != nil || level > int64(p.selectStmtOffset) {
			return -1
		}
		return int(level)
	}
	return -1
}

// getHintOffset gets the offset of stmt that the hints take effects.
func (p *BlockHintProcessor) getHintOffset(qbName model.CIStr, nodeType nodeType, currentOffset int) int {
	if qbName.L != "" {
		return p.getBlockOffset(qbName, nodeType)
	}
	return currentOffset
}

func (p *BlockHintProcessor) checkTableQBName(tables []ast.HintTable, nodeType nodeType) bool {
	for _, table := range tables {
		if table.QBName.L != "" && p.getBlockOffset(table.QBName, nodeType) < 0 {
			return false
		}
	}
	return true
}

// getCurrentStmtHints extracts all hints that take effects at current stmt.
func (p *BlockHintProcessor) getCurrentStmtHints(hints []*ast.TableOptimizerHint, nodeType nodeType, currentOffset int) []*ast.TableOptimizerHint {
	if p.QbHints == nil {
		p.QbHints = make(map[int][]*ast.TableOptimizerHint)
	}
	for _, hint := range hints {
		if hint.HintName.L == hintQBName {
			continue
		}
		offset := p.getHintOffset(hint.QBName, nodeType, currentOffset)
		if offset < 0 || !p.checkTableQBName(hint.Tables, nodeType) {
			hintStr := bindinfo.RestoreTableOptimizerHint(hint)
			p.Ctx.GetSessionVars().StmtCtx.AppendWarning(errors.New(fmt.Sprintf("Hint %s is ignored due to unknown query block name", hintStr)))
			continue
		}
		p.QbHints[offset] = append(p.QbHints[offset], hint)
	}
	return p.QbHints[currentOffset]
}

func restoreOptimizerHint(hint *ast.TableOptimizerHint) string {
	var sb strings.Builder
	ctx := format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)
	err := hint.Restore(ctx)
	// There won't be any error for optimizer hint.
	if err != nil {
		logutil.BgLogger().Warn("restore hint failed", zap.Error(err))
	}
	return sb.String()
}

// RestoreOptimizerHints restores these hints.
func RestoreOptimizerHints(hints []*ast.TableOptimizerHint) string {
	hintsStr := make([]string, 0, len(hints))
	hintsMap := make(map[string]struct{}, len(hints))
	for _, hint := range hints {
		hintStr := restoreOptimizerHint(hint)
		if _, ok := hintsMap[hintStr]; ok {
			continue
		}
		hintsMap[hintStr] = struct{}{}
		hintsStr = append(hintsStr, hintStr)
	}
	return strings.Join(hintsStr, ", ")
}

// GenHintsFromPhysicalPlan generates hints from physical plan.
func GenHintsFromPhysicalPlan(p Plan) []*ast.TableOptimizerHint {
	var hints []*ast.TableOptimizerHint
	switch pp := p.(type) {
	case *Explain:
		return GenHintsFromPhysicalPlan(pp.TargetPlan)
	case *Update:
		hints = genHintsFromPhysicalPlan(pp.SelectPlan, typeUpdate)
	case *Delete:
		hints = genHintsFromPhysicalPlan(pp.SelectPlan, typeDelete)
	case PhysicalPlan:
		hints = genHintsFromPhysicalPlan(pp, typeSelect)
	}
	return hints
}

// ExtractTableHintsFromStmtNode extracts table hints from this node.
func ExtractTableHintsFromStmtNode(node ast.Node) []*ast.TableOptimizerHint {
	switch x := node.(type) {
	case *ast.SelectStmt:
		return x.TableHints
	case *ast.UpdateStmt:
		return x.TableHints
	case *ast.DeleteStmt:
		return x.TableHints
	// TODO: support hint for InsertStmt
	case *ast.ExplainStmt:
		return ExtractTableHintsFromStmtNode(x.Stmt)
	default:
		return nil
	}
}

func generateQBName(nodeType nodeType, blockOffset int) model.CIStr {
	if nodeType == typeDelete && blockOffset == 0 {
		return model.NewCIStr(defaultDeleteBlockName)
	} else if nodeType == typeUpdate && blockOffset == 0 {
		return model.NewCIStr(defaultUpdateBlockName)
	}
	return model.NewCIStr(fmt.Sprintf("%s%d", defaultSelectBlockPrefix, blockOffset))
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

func getJoinHints(sctx sessionctx.Context, joinType string, parentOffset int, nodeType nodeType, children ...PhysicalPlan) (res []*ast.TableOptimizerHint) {
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
			QBName:   generateQBName(nodeType, blockOffset),
			HintName: model.NewCIStr(joinType),
			Tables:   []ast.HintTable{{DBName: *dbName, TableName: *tableName}},
		})
		break
	}
	return res
}

func genHintsFromPhysicalPlan(p PhysicalPlan, nodeType nodeType) (res []*ast.TableOptimizerHint) {
	for _, child := range p.Children() {
		res = append(res, genHintsFromPhysicalPlan(child, nodeType)...)
	}
	switch pp := p.(type) {
	case *PhysicalTableReader:
		tbl := pp.TablePlans[0].(*PhysicalTableScan)
		res = append(res, &ast.TableOptimizerHint{
			QBName:   generateQBName(nodeType, pp.blockOffset),
			HintName: model.NewCIStr(HintUseIndex),
			Tables:   []ast.HintTable{{DBName: tbl.DBName, TableName: getTableName(tbl.Table.Name, tbl.TableAsName)}},
		})
		if tbl.StoreType == kv.TiFlash {
			res = append(res, &ast.TableOptimizerHint{
				QBName:   generateQBName(nodeType, pp.blockOffset),
				HintName: model.NewCIStr(HintReadFromStorage),
				HintData: model.NewCIStr(kv.TiFlash.Name()),
				Tables:   []ast.HintTable{{DBName: tbl.DBName, TableName: getTableName(tbl.Table.Name, tbl.TableAsName)}},
			})
		}
	case *PhysicalIndexLookUpReader:
		index := pp.IndexPlans[0].(*PhysicalIndexScan)
		res = append(res, &ast.TableOptimizerHint{
			QBName:   generateQBName(nodeType, pp.blockOffset),
			HintName: model.NewCIStr(HintUseIndex),
			Tables:   []ast.HintTable{{DBName: index.DBName, TableName: getTableName(index.Table.Name, index.TableAsName)}},
			Indexes:  []model.CIStr{index.Index.Name},
		})
	case *PhysicalIndexReader:
		index := pp.IndexPlans[0].(*PhysicalIndexScan)
		res = append(res, &ast.TableOptimizerHint{
			QBName:   generateQBName(nodeType, pp.blockOffset),
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
			QBName:   generateQBName(nodeType, pp.blockOffset),
			HintName: model.NewCIStr(HintIndexMerge),
			Tables:   []ast.HintTable{{TableName: getTableName(tableName, tableAsName)}},
			Indexes:  Indexs,
		})
	case *PhysicalHashAgg:
		res = append(res, &ast.TableOptimizerHint{
			QBName:   generateQBName(nodeType, pp.blockOffset),
			HintName: model.NewCIStr(HintHashAgg),
		})
	case *PhysicalStreamAgg:
		res = append(res, &ast.TableOptimizerHint{
			QBName:   generateQBName(nodeType, pp.blockOffset),
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
