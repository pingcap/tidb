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
	"github.com/pingcap/tidb/sessionctx"
)

// BlockHintProcessor processes hints at different level of sql statement.
type BlockHintProcessor struct {
	QbNameMap        map[string]int                    // Map from query block name to select stmt offset.
	QbHints          map[int][]*ast.TableOptimizerHint // Group all hints at same query block.
	Ctx              sessionctx.Context
	selectStmtOffset int
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
func (p *BlockHintProcessor) getHintOffset(hint *ast.TableOptimizerHint, nodeType nodeType, currentOffset int) int {
	if hint.QBName.L != "" {
		return p.getBlockOffset(hint.QBName, nodeType)
	}
	return currentOffset
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
		offset := p.getHintOffset(hint, nodeType, currentOffset)
		if offset < 0 {
			var sb strings.Builder
			ctx := format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)
			err := hint.Restore(ctx)
			// There won't be any error for optimizer hint.
			if err == nil {
				p.Ctx.GetSessionVars().StmtCtx.AppendWarning(errors.New(fmt.Sprintf("Hint %s is ignored due to unknown query block name", sb.String())))
			}
			continue
		}
		p.QbHints[offset] = append(p.QbHints[offset], hint)
	}
	return p.QbHints[currentOffset]
}
