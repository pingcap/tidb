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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package hint

import (
	"fmt"
	"strings"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tidb/pkg/util/logutil"
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
func ExtractTableHintsFromStmtNode(node ast.Node, warnHandler hintWarnHandler) []*ast.TableOptimizerHint {
	switch x := node.(type) {
	case *ast.SelectStmt:
		return x.TableHints
	case *ast.UpdateStmt:
		return x.TableHints
	case *ast.DeleteStmt:
		return x.TableHints
	case *ast.InsertStmt:
		// check duplicated hints
		checkInsertStmtHintDuplicated(node, warnHandler)
		return x.TableHints
	case *ast.SetOprStmt:
		var result []*ast.TableOptimizerHint
		if x.SelectList == nil {
			return nil
		}
		for _, s := range x.SelectList.Selects {
			tmp := ExtractTableHintsFromStmtNode(s, warnHandler)
			if len(tmp) != 0 {
				result = append(result, tmp...)
			}
		}
		return result
	default:
		return nil
	}
}

// checkInsertStmtHintDuplicated check whether existed the duplicated hints in both insertStmt and its selectStmt.
// If existed, it would send a warning message.
func checkInsertStmtHintDuplicated(node ast.Node, warnHandler hintWarnHandler) {
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
					warnHandler.SetHintWarningFromError(plannererrors.ErrWarnConflictingHint.FastGenByArgs(hint))
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
	stmtNodes, warns, err := p.ParseSQL(sql,
		parser.CharsetConnection(charset),
		parser.CollationConnection(collation))
	if err != nil {
		return nil, nil, nil, err
	}
	if len(stmtNodes) != 1 {
		return nil, nil, nil, fmt.Errorf("bind_sql must be a single statement: %s", sql)
	}
	hs := CollectHint(stmtNodes[0])
	processor := NewQBHintHandler(nil)
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
				if len(tblHint.Tables) > 0 {
					newHints = append(newHints, tblHint)
				}
				continue
			}
			if processor.isHint4View(tblHint) {
				newHints = append(newHints, tblHint)
				continue
			}
			offset := processor.GetHintOffset(tblHint.QBName, curOffset)
			if offset < 0 || !processor.checkTableQBName(tblHint.Tables) {
				hintStr := RestoreTableOptimizerHint(tblHint)
				return nil, nil, nil, fmt.Errorf("Unknown query block name in hint %s", hintStr)
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
		if parser.ErrParse.Equal(w) ||
			parser.ErrWarnOptimizerHintUnsupportedHint.Equal(w) ||
			parser.ErrWarnOptimizerHintInvalidToken.Equal(w) ||
			parser.ErrWarnMemoryQuotaOverflow.Equal(w) ||
			parser.ErrWarnOptimizerHintParseError.Equal(w) ||
			parser.ErrWarnOptimizerHintInvalidInteger.Equal(w) ||
			parser.ErrWarnOptimizerHintWrongPos.Equal(w) {
			// Just one warning is enough, however we use a slice here to stop golint complaining
			// "error should be the last type when returning multiple items" for `ParseHintsSet`.
			return []error{w}
		}
	}
	return nil
}

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

// CheckBindingFromHistoryComplete checks whether the ast and hint string from history is complete.
// For these complex queries, the auto-generated binding might be not complete:
// 1. query use tiFlash engine
// 2. query with sub query
// 3. query with more than 2 table join
func CheckBindingFromHistoryComplete(node ast.Node, hintStr string) (complete bool, reason string) {
	// check tiflash
	contain := strings.Contains(hintStr, "tiflash")
	if contain {
		return false, "auto-generated hint for queries accessing TiFlash might not be complete, the plan might change even after creating this binding"
	}

	checker := bindableChecker{
		complete: true,
		tables:   make(map[model.CIStr]struct{}, 2),
	}
	node.Accept(&checker)
	return checker.complete, checker.reason
}

// bindableChecker checks whether a binding from history can be created.
type bindableChecker struct {
	complete bool
	reason   string
	tables   map[model.CIStr]struct{}
}

// Enter implements Visitor interface.
func (checker *bindableChecker) Enter(in ast.Node) (out ast.Node, skipChildren bool) {
	switch node := in.(type) {
	case *ast.ExistsSubqueryExpr, *ast.SubqueryExpr:
		checker.complete = false
		checker.reason = "auto-generated hint for queries with sub queries might not be complete, the plan might change even after creating this binding"
		return in, true
	case *ast.TableName:
		if _, ok := checker.tables[node.Schema]; !ok {
			checker.tables[node.Name] = struct{}{}
		}
		if len(checker.tables) >= 3 {
			checker.complete = false
			checker.reason = "auto-generated hint for queries with more than 3 table join might not be complete, the plan might change even after creating this binding"
			return in, true
		}
	}
	return in, false
}

// Leave implements Visitor interface.
func (checker *bindableChecker) Leave(in ast.Node) (out ast.Node, ok bool) {
	return in, checker.complete
}
