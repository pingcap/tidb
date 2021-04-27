// Copyright 2020 PingCAP, Inc.
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

package hint

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/format"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

var supportedHintNameForInsertStmt = map[string]struct{}{}

func init() {
	supportedHintNameForInsertStmt["memory_quota"] = struct{}{}
}

// HintsSet contains all hints of a query.
type HintsSet struct {
	tableHints [][]*ast.TableOptimizerHint // Slice offset is the traversal order of `SelectStmt` in the ast.
	indexHints [][]*ast.IndexHint          // Slice offset is the traversal order of `TableName` in the ast.
}

// GetFirstTableHints gets the first table hints.
func (hs *HintsSet) GetFirstTableHints() []*ast.TableOptimizerHint {
	if len(hs.tableHints) > 0 {
		return hs.tableHints[0]
	}
	return nil
}

// ContainTableHint checks whether the table hint set contains a hint.
func (hs *HintsSet) ContainTableHint(hint string) bool {
	for _, tableHintsForBlock := range hs.tableHints {
		for _, tableHint := range tableHintsForBlock {
			if tableHint.HintName.String() == hint {
				return true
			}
		}
	}
	return false
}

// setTableHints4StmtNode sets table hints for select/update/delete.
func setTableHints4StmtNode(node ast.Node, hints []*ast.TableOptimizerHint) {
	switch x := node.(type) {
	case *ast.SelectStmt:
		x.TableHints = hints
	case *ast.UpdateStmt:
		x.TableHints = hints
	case *ast.DeleteStmt:
		x.TableHints = hints
	}
}

// ExtractTableHintsFromStmtNode extracts table hints from this node.
func ExtractTableHintsFromStmtNode(node ast.Node, sctx sessionctx.Context) []*ast.TableOptimizerHint {
	switch x := node.(type) {
	case *ast.SelectStmt:
		return x.TableHints
	case *ast.UpdateStmt:
		return x.TableHints
	case *ast.DeleteStmt:
		return x.TableHints
	case *ast.InsertStmt:
		// check duplicated hints
		checkInsertStmtHintDuplicated(node, sctx)
		return x.TableHints
	case *ast.ExplainStmt:
		return ExtractTableHintsFromStmtNode(x.Stmt, sctx)
	default:
		return nil
	}
}

// checkInsertStmtHintDuplicated check whether existed the duplicated hints in both insertStmt and its selectStmt.
// If existed, it would send a warning message.
func checkInsertStmtHintDuplicated(node ast.Node, sctx sessionctx.Context) {
	switch x := node.(type) {
	case *ast.InsertStmt:
		if len(x.TableHints) > 0 {
			var supportedHint *ast.TableOptimizerHint
			for _, hint := range x.TableHints {
				if _, ok := supportedHintNameForInsertStmt[hint.HintName.L]; ok {
					supportedHint = hint
					break
				}
			}
			if supportedHint != nil {
				var duplicatedHint *ast.TableOptimizerHint
				for _, hint := range ExtractTableHintsFromStmtNode(x.Select, nil) {
					if hint.HintName.L == supportedHint.HintName.L {
						duplicatedHint = hint
						break
					}
				}
				if duplicatedHint != nil {
					hint := fmt.Sprintf("%s(`%v`)", duplicatedHint.HintName.O, duplicatedHint.HintData)
					err := dbterror.ClassUtil.NewStd(errno.ErrWarnConflictingHint).FastGenByArgs(hint)
					sctx.GetSessionVars().StmtCtx.AppendWarning(err)
				}
			}
		}
	default:
		return
	}
}

// RestoreOptimizerHints restores these hints.
func RestoreOptimizerHints(hints []*ast.TableOptimizerHint) string {
	hintsStr := make([]string, 0, len(hints))
	hintsMap := make(map[string]struct{}, len(hints))
	for _, hint := range hints {
		hintStr := RestoreTableOptimizerHint(hint)
		if _, ok := hintsMap[hintStr]; ok {
			continue
		}
		hintsMap[hintStr] = struct{}{}
		hintsStr = append(hintsStr, hintStr)
	}
	return strings.Join(hintsStr, ", ")
}

// RestoreTableOptimizerHint returns string format of TableOptimizerHint.
func RestoreTableOptimizerHint(hint *ast.TableOptimizerHint) string {
	var sb strings.Builder
	ctx := format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)
	err := hint.Restore(ctx)
	// There won't be any error for optimizer hint.
	if err != nil {
		logutil.BgLogger().Debug("restore TableOptimizerHint failed", zap.Error(err))
	}
	return strings.ToLower(sb.String())
}

// RestoreIndexHint returns string format of IndexHint.
func RestoreIndexHint(hint *ast.IndexHint) (string, error) {
	var sb strings.Builder
	ctx := format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)
	err := hint.Restore(ctx)
	if err != nil {
		logutil.BgLogger().Debug("restore IndexHint failed", zap.Error(err))
		return "", err
	}
	return strings.ToLower(sb.String()), nil
}

// Restore returns the string format of HintsSet.
func (hs *HintsSet) Restore() (string, error) {
	hintsStr := make([]string, 0, len(hs.tableHints)+len(hs.indexHints))
	for _, tblHints := range hs.tableHints {
		for _, tblHint := range tblHints {
			hintsStr = append(hintsStr, RestoreTableOptimizerHint(tblHint))
		}
	}
	for _, idxHints := range hs.indexHints {
		for _, idxHint := range idxHints {
			str, err := RestoreIndexHint(idxHint)
			if err != nil {
				return "", err
			}
			hintsStr = append(hintsStr, str)
		}
	}
	return strings.Join(hintsStr, ", "), nil
}

type hintProcessor struct {
	*HintsSet
	// bindHint2Ast indicates the behavior of the processor, `true` for bind hint to ast, `false` for extract hint from ast.
	bindHint2Ast bool
	tableCounter int
	indexCounter int
	blockCounter int
}

func (hp *hintProcessor) Enter(in ast.Node) (ast.Node, bool) {
	switch v := in.(type) {
	case *ast.SelectStmt, *ast.UpdateStmt, *ast.DeleteStmt:
		if hp.bindHint2Ast {
			if hp.tableCounter < len(hp.tableHints) {
				setTableHints4StmtNode(in, hp.tableHints[hp.tableCounter])
			} else {
				setTableHints4StmtNode(in, nil)
			}
			hp.tableCounter++
		} else {
			hp.tableHints = append(hp.tableHints, ExtractTableHintsFromStmtNode(in, nil))
		}
		hp.blockCounter++
	case *ast.TableName:
		// Insert cases.
		if hp.blockCounter == 0 {
			return in, false
		}
		if hp.bindHint2Ast {
			if hp.indexCounter < len(hp.indexHints) {
				v.IndexHints = hp.indexHints[hp.indexCounter]
			} else {
				v.IndexHints = nil
			}
			hp.indexCounter++
		} else {
			hp.indexHints = append(hp.indexHints, v.IndexHints)
		}
	}
	return in, false
}

func (hp *hintProcessor) Leave(in ast.Node) (ast.Node, bool) {
	switch in.(type) {
	case *ast.SelectStmt, *ast.UpdateStmt, *ast.DeleteStmt:
		hp.blockCounter--
	}
	return in, true
}

// CollectHint collects hints for a statement.
func CollectHint(in ast.StmtNode) *HintsSet {
	hp := hintProcessor{HintsSet: &HintsSet{tableHints: make([][]*ast.TableOptimizerHint, 0, 4), indexHints: make([][]*ast.IndexHint, 0, 4)}}
	in.Accept(&hp)
	return hp.HintsSet
}

// BindHint will add hints for stmt according to the hints in `hintsSet`.
func BindHint(stmt ast.StmtNode, hintsSet *HintsSet) ast.StmtNode {
	hp := hintProcessor{HintsSet: hintsSet, bindHint2Ast: true}
	stmt.Accept(&hp)
	return stmt
}

// ParseHintsSet parses a SQL string, then collects and normalizes the HintsSet.
func ParseHintsSet(p *parser.Parser, sql, charset, collation, db string) (*HintsSet, ast.StmtNode, []error, error) {
	stmtNodes, warns, err := p.Parse(sql, charset, collation)
	if err != nil {
		return nil, nil, nil, err
	}
	if len(stmtNodes) != 1 {
		return nil, nil, nil, errors.New(fmt.Sprintf("bind_sql must be a single statement: %s", sql))
	}
	hs := CollectHint(stmtNodes[0])
	processor := &BlockHintProcessor{}
	stmtNodes[0].Accept(processor)
	topNodeType := nodeType4Stmt(stmtNodes[0])
	for i, tblHints := range hs.tableHints {
		newHints := make([]*ast.TableOptimizerHint, 0, len(tblHints))
		curOffset := i + 1
		if topNodeType == TypeDelete || topNodeType == TypeUpdate {
			curOffset = curOffset - 1
		}
		for _, tblHint := range tblHints {
			if tblHint.HintName.L == hintQBName {
				continue
			}
			offset := processor.GetHintOffset(tblHint.QBName, curOffset)
			if offset < 0 || !processor.checkTableQBName(tblHint.Tables) {
				hintStr := RestoreTableOptimizerHint(tblHint)
				return nil, nil, nil, errors.New(fmt.Sprintf("Unknown query block name in hint %s", hintStr))
			}
			tblHint.QBName, err = GenerateQBName(topNodeType, offset)
			if err != nil {
				return nil, nil, nil, err
			}
			for i, tbl := range tblHint.Tables {
				if tbl.DBName.String() == "" {
					tblHint.Tables[i].DBName = model.NewCIStr(db)
				}
			}
			newHints = append(newHints, tblHint)
		}
		hs.tableHints[i] = newHints
	}
	return hs, stmtNodes[0], extractHintWarns(warns), nil
}

func extractHintWarns(warns []error) []error {
	for _, w := range warns {
		if parser.ErrWarnOptimizerHintUnsupportedHint.Equal(w) ||
			parser.ErrWarnOptimizerHintInvalidToken.Equal(w) ||
			parser.ErrWarnMemoryQuotaOverflow.Equal(w) ||
			parser.ErrWarnOptimizerHintParseError.Equal(w) ||
			parser.ErrWarnOptimizerHintInvalidInteger.Equal(w) {
			// Just one warning is enough, however we use a slice here to stop golint complaining
			// "error should be the last type when returning multiple items" for `ParseHintsSet`.
			return []error{w}
		}
	}
	return nil
}

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
			if p.Ctx != nil {
				p.Ctx.GetSessionVars().StmtCtx.AppendWarning(errors.New(fmt.Sprintf("There are more than two query names in same query block,, using the first one %s", qbName)))
			}
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
		if p.Ctx != nil {
			p.Ctx.GetSessionVars().StmtCtx.AppendWarning(errors.New(fmt.Sprintf("Duplicate query block name %s, only the first one is effective", qbName)))
		}
	} else {
		p.QbNameMap[qbName] = offset
	}
}

const (
	defaultUpdateBlockName   = "upd_1"
	defaultDeleteBlockName   = "del_1"
	defaultSelectBlockPrefix = "sel_"
)

// NodeType indicates if the node is for SELECT / UPDATE / DELETE.
type NodeType int

const (
	// TypeUpdate for Update.
	TypeUpdate NodeType = iota
	// TypeDelete for DELETE.
	TypeDelete
	// TypeSelect for SELECT.
	TypeSelect
	// TypeInvalid for unexpected statements.
	TypeInvalid
)

// nodeType4Stmt returns the NodeType for a statement. The type is used for SQL bind.
func nodeType4Stmt(node ast.StmtNode) NodeType {
	switch node.(type) {
	// This type is used by SQL bind, we only handle SQL bind for INSERT INTO SELECT, so we treat InsertStmt as TypeSelect.
	case *ast.SelectStmt, *ast.InsertStmt:
		return TypeSelect
	case *ast.UpdateStmt:
		return TypeUpdate
	case *ast.DeleteStmt:
		return TypeDelete
	}
	return TypeInvalid
}

// getBlockName finds the offset of query block name. It uses 0 as offset for top level update or delete,
// -1 for invalid block name.
func (p *BlockHintProcessor) getBlockOffset(blockName model.CIStr) int {
	if p.QbNameMap != nil {
		level, ok := p.QbNameMap[blockName.L]
		if ok {
			return level
		}
	}
	// Handle the default query block name.
	if blockName.L == defaultUpdateBlockName || blockName.L == defaultDeleteBlockName {
		return 0
	}
	if strings.HasPrefix(blockName.L, defaultSelectBlockPrefix) {
		suffix := blockName.L[len(defaultSelectBlockPrefix):]
		level, err := strconv.ParseInt(suffix, 10, 64)
		if err != nil || level > int64(p.selectStmtOffset) {
			return -1
		}
		return int(level)
	}
	return -1
}

// GetHintOffset gets the offset of stmt that the hints take effects.
func (p *BlockHintProcessor) GetHintOffset(qbName model.CIStr, currentOffset int) int {
	if qbName.L != "" {
		return p.getBlockOffset(qbName)
	}
	return currentOffset
}

func (p *BlockHintProcessor) checkTableQBName(tables []ast.HintTable) bool {
	for _, table := range tables {
		if table.QBName.L != "" && p.getBlockOffset(table.QBName) < 0 {
			return false
		}
	}
	return true
}

// GetCurrentStmtHints extracts all hints that take effects at current stmt.
func (p *BlockHintProcessor) GetCurrentStmtHints(hints []*ast.TableOptimizerHint, currentOffset int) []*ast.TableOptimizerHint {
	if p.QbHints == nil {
		p.QbHints = make(map[int][]*ast.TableOptimizerHint)
	}
	for _, hint := range hints {
		if hint.HintName.L == hintQBName {
			continue
		}
		offset := p.GetHintOffset(hint.QBName, currentOffset)
		if offset < 0 || !p.checkTableQBName(hint.Tables) {
			hintStr := RestoreTableOptimizerHint(hint)
			p.Ctx.GetSessionVars().StmtCtx.AppendWarning(errors.New(fmt.Sprintf("Hint %s is ignored due to unknown query block name", hintStr)))
			continue
		}
		p.QbHints[offset] = append(p.QbHints[offset], hint)
	}
	return p.QbHints[currentOffset]
}

// GenerateQBName builds QBName from offset.
func GenerateQBName(nodeType NodeType, blockOffset int) (model.CIStr, error) {
	if blockOffset == 0 {
		if nodeType == TypeDelete {
			return model.NewCIStr(defaultDeleteBlockName), nil
		}
		if nodeType == TypeUpdate {
			return model.NewCIStr(defaultUpdateBlockName), nil
		}
		return model.NewCIStr(""), errors.New(fmt.Sprintf("Unexpected NodeType %d when block offset is 0", nodeType))
	}
	return model.NewCIStr(fmt.Sprintf("%s%d", defaultSelectBlockPrefix, blockOffset)), nil
}
