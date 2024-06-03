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

package hint

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/model"
)

// QBHintHandler is used to handle hints at different query blocks.
// See the 2 examples below:
// 1) `select /*+ use_index(@sel_2 t2, a) */ * from t1, (select a*2 as b from t2) tx where a>b`;
// 2) `select /*+ use_index(@qb_xxx t2, a) */ * from t1, (select /*+ qb_name(qb_xxx) */ a*2 as b from t2) tx where a>b`;
// In both cases, the `use_index` hint doesn't take effect directly, since a specific qb_name is specified, and this
// QBHintHandler is used to handle this cases.
type QBHintHandler struct {
	QBNameToSelOffset map[string]int                    // map[QBName]SelectOffset
	QBOffsetToHints   map[int][]*ast.TableOptimizerHint // map[QueryBlockOffset]Hints

	// Used for the view's hint
	ViewQBNameToTable map[string][]ast.HintTable           // map[QBName]HintedTable
	ViewQBNameToHints map[string][]*ast.TableOptimizerHint // map[QBName]Hints
	ViewQBNameUsed    map[string]struct{}                  // map[QBName]Used

	warnHandler      hintWarnHandler
	selectStmtOffset int
}

// hintWarnHandler is used to handle the warning when parsing hints.
type hintWarnHandler interface {
	SetHintWarning(warn string)
	SetHintWarningFromError(err error)
}

// NewQBHintHandler creates a QBHintHandler.
func NewQBHintHandler(warnHandler hintWarnHandler) *QBHintHandler {
	return &QBHintHandler{
		warnHandler: warnHandler,
	}
}

// MaxSelectStmtOffset returns the current stmt offset.
func (p *QBHintHandler) MaxSelectStmtOffset() int {
	return p.selectStmtOffset
}

// Enter implements Visitor interface.
func (p *QBHintHandler) Enter(in ast.Node) (ast.Node, bool) {
	switch node := in.(type) {
	case *ast.UpdateStmt:
		p.checkQueryBlockHints(node.TableHints, 0)
	case *ast.DeleteStmt:
		p.checkQueryBlockHints(node.TableHints, 0)
	case *ast.SelectStmt:
		p.selectStmtOffset++
		node.QueryBlockOffset = p.selectStmtOffset
		// Handle the view hints and update the left hint.
		node.TableHints = p.handleViewHints(node.TableHints, node.QueryBlockOffset)
		p.checkQueryBlockHints(node.TableHints, node.QueryBlockOffset)
	case *ast.ExplainStmt:
		return in, true
	case *ast.CreateBindingStmt:
		return in, true
	}
	return in, false
}

// Leave implements Visitor interface.
func (*QBHintHandler) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}

const hintQBName = "qb_name"

// checkQueryBlockHints checks the validity of query blocks and records the map of query block name to select offset.
func (p *QBHintHandler) checkQueryBlockHints(hints []*ast.TableOptimizerHint, offset int) {
	var qbName string
	for _, hint := range hints {
		if hint.HintName.L != hintQBName {
			continue
		}
		if qbName != "" {
			if p.warnHandler != nil {
				p.warnHandler.SetHintWarning(fmt.Sprintf("There are more than two query names in same query block, using the first one %s", qbName))
			}
		} else {
			qbName = hint.QBName.L
		}
	}
	if qbName == "" {
		return
	}
	if p.QBNameToSelOffset == nil {
		p.QBNameToSelOffset = make(map[string]int)
	}
	if _, ok := p.QBNameToSelOffset[qbName]; ok {
		if p.warnHandler != nil {
			p.warnHandler.SetHintWarning(fmt.Sprintf("Duplicate query block name %s, only the first one is effective", qbName))
		}
	} else {
		p.QBNameToSelOffset[qbName] = offset
	}
}

func (p *QBHintHandler) handleViewHints(hints []*ast.TableOptimizerHint, offset int) (leftHints []*ast.TableOptimizerHint) {
	if len(hints) == 0 {
		return
	}

	usedHints := make([]bool, len(hints))
	// handle the query block name hints for view
	for i, hint := range hints {
		if hint.HintName.L != hintQBName || len(hint.Tables) == 0 {
			continue
		}
		usedHints[i] = true
		if p.ViewQBNameToTable == nil {
			p.ViewQBNameToTable = make(map[string][]ast.HintTable)
			p.ViewQBNameUsed = make(map[string]struct{})
		}
		qbName := hint.QBName.L
		if qbName == "" {
			continue
		}
		if _, ok := p.ViewQBNameToTable[qbName]; ok {
			if p.warnHandler != nil {
				p.warnHandler.SetHintWarning(fmt.Sprintf("Duplicate query block name %s for view's query block hint, only the first one is effective", qbName))
			}
		} else {
			if offset != 1 {
				// If there are some qb_name hints for view are not defined in the first query block,
				// we should add the query block number where it is located to the first table in the view's qb_name hint table list.
				qbNum := hint.Tables[0].QBName.L
				if qbNum == "" {
					hint.Tables[0].QBName = model.NewCIStr(fmt.Sprintf("%s%d", defaultSelectBlockPrefix, offset))
				}
			}
			p.ViewQBNameToTable[qbName] = hint.Tables
		}
	}

	// handle the view hints
	for i, hint := range hints {
		if usedHints[i] || hint.HintName.L == hintQBName {
			continue
		}

		ok := false
		qbName := hint.QBName.L
		if qbName != "" {
			_, ok = p.ViewQBNameToTable[qbName]
		} else if len(hint.Tables) > 0 {
			// Only support to define the tables belong to the same query block in one view hint
			qbName = hint.Tables[0].QBName.L
			_, ok = p.ViewQBNameToTable[qbName]
			if ok {
				for _, table := range hint.Tables {
					if table.QBName.L != qbName {
						ok = false
						break
					}
				}
				if !ok {
					p.warnHandler.SetHintWarning("Only one query block name is allowed in a view hint, otherwise the hint will be invalid")
					usedHints[i] = true
				}
			}
		}

		if ok {
			if p.ViewQBNameToHints == nil {
				p.ViewQBNameToHints = make(map[string][]*ast.TableOptimizerHint)
			}
			usedHints[i] = true
			p.ViewQBNameToHints[qbName] = append(p.ViewQBNameToHints[qbName], hint)
		}
	}

	for i, hint := range hints {
		if !usedHints[i] {
			leftHints = append(leftHints, hint)
		}
	}
	return
}

// HandleUnusedViewHints handle the unused view hints.
func (p *QBHintHandler) HandleUnusedViewHints() {
	if p.ViewQBNameToTable != nil {
		for qbName := range p.ViewQBNameToTable {
			_, ok := p.ViewQBNameUsed[qbName]
			if !ok && p.warnHandler != nil {
				p.warnHandler.SetHintWarning(fmt.Sprintf("The qb_name hint %s is unused, please check whether the table list in the qb_name hint %s is correct", qbName, qbName))
			}
		}
	}
}

const (
	defaultUpdateBlockName   = "upd_1"
	defaultDeleteBlockName   = "del_1"
	defaultSelectBlockPrefix = "sel_"
)

// getBlockName finds the offset of query block name. It uses 0 as offset for top level update or delete,
// -1 for invalid block name.
func (p *QBHintHandler) getBlockOffset(blockName model.CIStr) int {
	if p.QBNameToSelOffset != nil {
		level, ok := p.QBNameToSelOffset[blockName.L]
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
func (p *QBHintHandler) GetHintOffset(qbName model.CIStr, currentOffset int) int {
	if qbName.L != "" {
		return p.getBlockOffset(qbName)
	}
	return currentOffset
}

func (p *QBHintHandler) checkTableQBName(tables []ast.HintTable) bool {
	for _, table := range tables {
		if table.QBName.L != "" && p.getBlockOffset(table.QBName) < 0 {
			return false
		}
	}
	return true
}

func (p *QBHintHandler) isHint4View(hint *ast.TableOptimizerHint) bool {
	if hint.QBName.L != "" {
		if p.ViewQBNameToTable != nil {
			_, ok := p.ViewQBNameToTable[hint.QBName.L]
			return ok
		}
		return false
	}
	allViewHints := true
	for _, table := range hint.Tables {
		qbName := table.QBName.L
		if _, ok := p.ViewQBNameToTable[qbName]; !ok {
			allViewHints = false
			break
		}
	}
	return allViewHints
}

// GetCurrentStmtHints extracts all hints that take effects at current stmt.
func (p *QBHintHandler) GetCurrentStmtHints(hints []*ast.TableOptimizerHint, currentOffset int) []*ast.TableOptimizerHint {
	if p.QBOffsetToHints == nil {
		p.QBOffsetToHints = make(map[int][]*ast.TableOptimizerHint)
	}
	for _, hint := range hints {
		if hint.HintName.L == hintQBName {
			continue
		}
		offset := p.GetHintOffset(hint.QBName, currentOffset)
		if offset < 0 || !p.checkTableQBName(hint.Tables) {
			if p.warnHandler != nil {
				hintStr := RestoreTableOptimizerHint(hint)
				p.warnHandler.SetHintWarning(fmt.Sprintf("Hint %s is ignored due to unknown query block name", hintStr))
			}
			continue
		}
		p.QBOffsetToHints[offset] = append(p.QBOffsetToHints[offset], hint)
	}
	return p.QBOffsetToHints[currentOffset]
}

// GenerateQBName builds QBName from offset.
func GenerateQBName(nodeType NodeType, qbOffset int) (model.CIStr, error) {
	if qbOffset == 0 {
		if nodeType == TypeDelete {
			return model.NewCIStr(defaultDeleteBlockName), nil
		}
		if nodeType == TypeUpdate {
			return model.NewCIStr(defaultUpdateBlockName), nil
		}
		return model.NewCIStr(""), fmt.Errorf("Unexpected NodeType %d when block offset is 0", nodeType)
	}
	return model.NewCIStr(fmt.Sprintf("%s%d", defaultSelectBlockPrefix, qbOffset)), nil
}
