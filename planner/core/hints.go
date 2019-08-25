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
	"math"
	"strconv"

	"github.com/cznic/mathutil"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
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
		p.checkQueryBlockHints(node.TableHints, p.selectStmtOffset)
	}
	return in, false
}

// Leave implements Visitor interface.
func (p *BlockHintProcessor) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}

// checkQueryBlockHints checks the validity of query blocks and records the map of query block name to select offset.
func (p *BlockHintProcessor) checkQueryBlockHints(hints []*ast.TableOptimizerHint, offset int) {
	var qbName string
	for _, hint := range hints {
		if hint.HintName.L != "qb_name" {
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
		p.Ctx.GetSessionVars().StmtCtx.AppendWarning(errors.New(fmt.Sprintf("Duplicate query block name %s, we will use the first one", qbName)))
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
	if nodeType == typeSelect && len(blockName.L) >= len(defaultSelectBlockPrefix) {
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
	topOffset := currentOffset
	if hint.QBName.L != "" {
		topOffset = p.getBlockOffset(hint.QBName, nodeType)
		if topOffset < 0 {
			return -1
		}
	}
	if len(hint.Tables) == 0 {
		return topOffset
	}
	// Handle the case of hint_name(t1@sel_1, t2@sel_2), the result offset should be the out most stmt, which has the
	// smallest block offset.
	minOffset := math.MaxInt64
	for _, tbl := range hint.Tables {
		if tbl.QBName.L != "" {
			level := p.getBlockOffset(tbl.QBName, nodeType)
			if level < 0 {
				return -1
			}
			minOffset = mathutil.Min(minOffset, level)
		} else {
			minOffset = mathutil.Min(minOffset, topOffset)
		}
	}
	return minOffset
}

func (p *BlockHintProcessor) getCurrentStmtHints(hints []*ast.TableOptimizerHint, nodeType nodeType, currentOffset int) []*ast.TableOptimizerHint {
	if p.QbHints == nil {
		p.QbHints = make(map[int][]*ast.TableOptimizerHint)
	}
	for _, hint := range hints {
		if hint.HintName.L == "qb_name" {
			continue
		}
		offset := p.getHintOffset(hint, nodeType, currentOffset)
		if offset < 0 {
			continue
		}
		p.QbHints[offset] = append(p.QbHints[offset], hint)
	}
	return p.QbHints[currentOffset]
}
