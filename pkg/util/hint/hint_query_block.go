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
	"slices"
	"strconv"
	"strings"

	"github.com/pingcap/tidb/pkg/parser/ast"
)

// QBHintHandler is used to handle hints at different query blocks.
// See the 2 examples below:
// 1) `select /*+ use_index(@sel_2 t2, a) */ * from t1, (select a*2 as b from t2) tx where a>b`;
// 2) `select /*+ use_index(@qb_xxx t2, a) */ * from t1, (select /*+ qb_name(qb_xxx) */ a*2 as b from t2) tx where a>b`;
// In both cases, the `use_index` hint doesn't take effect directly, since a specific qb_name is specified, and this
// QBHintHandler is used to handle this cases.
type QBHintHandler struct {
	QBNameToSelOffset map[string]int // map[QBName]SelectOffset

	// Used for the view's hint
	ViewQBNameToTable map[string][]ast.HintTable           // map[QBName]HintedTable
	ViewQBNameToHints map[string][]*ast.TableOptimizerHint // map[QBName]Hints

	warnHandler      hintWarnHandler
	selectStmtOffset int

	// blockTables records the tables of each query block, see resolveHintDBName.
	blockTables map[int][]blockTable
	// offsetStack holds the offsets of the query blocks being visited.
	offsetStack []int
}

// blockTable is a table referenced in a query block, by its alias if it has one.
// dbName is empty when the table name is written without a database name.
type blockTable struct {
	dbName ast.CIStr
	name   ast.CIStr
}

// QBHintBuildState stores the per-build runtime state for a QBHintHandler.
// The handler itself only keeps AST-derived metadata and can be shared safely.
type QBHintBuildState struct {
	QBOffsetToHints map[int][]*ast.TableOptimizerHint // map[QueryBlockOffset]Hints
	ViewQBNameUsed  map[string]struct{}               // map[QBName]Used
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

// NewBuildState creates the per-build runtime state for the handler.
func (p *QBHintHandler) NewBuildState() *QBHintBuildState {
	if p == nil {
		return nil
	}

	state := &QBHintBuildState{}
	if len(p.ViewQBNameToTable) == 0 {
		return state
	}
	state.ViewQBNameUsed = make(map[string]struct{}, len(p.ViewQBNameToTable))
	return state
}

// MaxSelectStmtOffset returns the current stmt offset.
func (p *QBHintHandler) MaxSelectStmtOffset() int {
	return p.selectStmtOffset
}

// Enter implements ast.InPlaceVisitor.
func (p *QBHintHandler) Enter(in ast.Node) bool {
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
		p.offsetStack = append(p.offsetStack, node.QueryBlockOffset)
	case *ast.TableSource:
		p.recordBlockTable(node)
	case *ast.ExplainStmt:
		return true
	case *ast.CreateBindingStmt:
		return true
	}
	return false
}

// Leave implements ast.InPlaceVisitor.
func (p *QBHintHandler) Leave(in ast.Node) bool {
	if _, ok := in.(*ast.SelectStmt); ok && len(p.offsetStack) > 0 {
		p.offsetStack = p.offsetStack[:len(p.offsetStack)-1]
	}
	return true
}

// recordBlockTable records a table or derived table of the query block being visited.
// Tables outside any SELECT, e.g. the target tables of UPDATE and DELETE, belong to offset 0.
func (p *QBHintHandler) recordBlockTable(ts *ast.TableSource) {
	offset := 0
	if len(p.offsetStack) > 0 {
		offset = p.offsetStack[len(p.offsetStack)-1]
	}
	var tbl blockTable
	switch src := ts.Source.(type) {
	case *ast.TableName:
		tbl = blockTable{dbName: src.Schema, name: src.Name}
	default:
		// A derived table is in the current database for hint matching, like ExtractTableAlias.
		if ts.AsName.L == "" {
			return
		}
	}
	if ts.AsName.L != "" {
		tbl.name = ts.AsName
	}
	if p.blockTables == nil {
		p.blockTables = make(map[int][]blockTable)
	}
	p.blockTables[offset] = append(p.blockTables[offset], tbl)
}

// resolveHintDBName returns the database of a hint table name written without one, e.g. `t` in HASH_JOIN(t).
// It is the current database if the query block at offset has a table or alias of that name in the current
// database, or none at all, as before. Otherwise, if all the tables of that name in the block are in one
// other database, it is that database, so that `HASH_JOIN(t)` applies to `db1.t` when the current database is
// another one or empty. If they are in several other databases, the name is ambiguous and the current database
// is kept, so the hint does not match.
func (p *QBHintHandler) resolveHintDBName(offset int, tblName ast.CIStr, currentDB string) ast.CIStr {
	current := ast.NewCIStr(currentDB)
	if p == nil {
		return current
	}
	var resolved ast.CIStr
	found := false
	for _, tbl := range p.blockTables[offset] {
		if tbl.name.L != tblName.L {
			continue
		}
		dbName := tbl.dbName
		if dbName.L == "" || dbName.L == current.L {
			return current
		}
		if found && resolved.L != dbName.L {
			return current
		}
		resolved, found = dbName, true
	}
	if found {
		return resolved
	}
	return current
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
					hint.Tables[0].QBName = ast.NewCIStr(fmt.Sprintf("%s%d", defaultSelectBlockPrefix, offset))
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
func (p *QBHintHandler) HandleUnusedViewHints(state *QBHintBuildState, warn []string) []string {
	if state == nil {
		return warn
	}

	warn = warn[:0]
	if p.ViewQBNameToTable != nil {
		for qbName := range p.ViewQBNameToTable {
			_, ok := state.ViewQBNameUsed[qbName]
			if !ok && p.warnHandler != nil {
				warn = append(warn, fmt.Sprintf("The qb_name hint %s is unused, please check whether the table list in the qb_name hint %s is correct", qbName, qbName))
			}
		}
	}
	return warn
}

const (
	defaultUpdateBlockName   = "upd_1"
	defaultDeleteBlockName   = "del_1"
	defaultSelectBlockPrefix = "sel_"
)

// getBlockName finds the offset of query block name. It uses 0 as offset for top level update or delete,
// -1 for invalid block name.
func (p *QBHintHandler) getBlockOffset(blockName ast.CIStr) int {
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

// SetWarns set the warning from a list of strings.
func (p *QBHintHandler) SetWarns(warns []string) {
	if p == nil || p.warnHandler == nil || len(warns) == 0 {
		return
	}
	for _, one := range warns {
		p.warnHandler.SetHintWarning(one)
	}
}

// GetHintOffset gets the offset of stmt that the hints take effects.
func (p *QBHintHandler) GetHintOffset(qbName ast.CIStr, currentOffset int) int {
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
func (p *QBHintHandler) GetCurrentStmtHints(hints []*ast.TableOptimizerHint, currentOffset int, state *QBHintBuildState) []*ast.TableOptimizerHint {
	if state == nil {
		state = &QBHintBuildState{}
	}
	if state.QBOffsetToHints == nil {
		state.QBOffsetToHints = make(map[int][]*ast.TableOptimizerHint)
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
		if !slices.Contains(state.QBOffsetToHints[offset], hint) {
			state.QBOffsetToHints[offset] = append(state.QBOffsetToHints[offset], hint)
		}
	}
	return state.QBOffsetToHints[currentOffset]
}

// MarkViewQBNameUsed records that the named view qb hint was used in the current build.
func (*QBHintHandler) MarkViewQBNameUsed(qbName string, state *QBHintBuildState) {
	if state == nil || state.ViewQBNameUsed == nil {
		return
	}
	state.ViewQBNameUsed[qbName] = struct{}{}
}

// GenerateQBName builds QBName from offset.
func GenerateQBName(nodeType NodeType, qbOffset int) (ast.CIStr, error) {
	if qbOffset == 0 {
		if nodeType == TypeDelete {
			return ast.NewCIStr(defaultDeleteBlockName), nil
		}
		if nodeType == TypeUpdate {
			return ast.NewCIStr(defaultUpdateBlockName), nil
		}
		return ast.NewCIStr(""), fmt.Errorf("Unexpected NodeType %d when block offset is 0", nodeType)
	}
	return ast.NewCIStr(fmt.Sprintf("%s%d", defaultSelectBlockPrefix, qbOffset)), nil
}
