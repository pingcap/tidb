// Copyright 2025 PingCAP, Inc.
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

package bindinfo

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/util/fixcontrol"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
)

// adjustVar returns the new value of the variable for plan generation.
func adjustVar(varName string, varVal any) (newVarVal any, err error) {
	switch varName {
	case vardef.TiDBOptIndexScanCostFactor, vardef.TiDBOptIndexReaderCostFactor, vardef.TiDBOptTableReaderCostFactor,
		vardef.TiDBOptTableFullScanCostFactor, vardef.TiDBOptTableRangeScanCostFactor, vardef.TiDBOptTableRowIDScanCostFactor,
		vardef.TiDBOptTableTiFlashScanCostFactor, vardef.TiDBOptIndexLookupCostFactor, vardef.TiDBOptIndexMergeCostFactor,
		vardef.TiDBOptSortCostFactor, vardef.TiDBOptTopNCostFactor, vardef.TiDBOptLimitCostFactor,
		vardef.TiDBOptStreamAggCostFactor, vardef.TiDBOptHashAggCostFactor, vardef.TiDBOptMergeJoinCostFactor,
		vardef.TiDBOptHashJoinCostFactor, vardef.TiDBOptIndexJoinCostFactor:
		// for cost factors, we add add some penalties (5 tims of its current cost) in each step.
		v := varVal.(float64)
		if v >= 1e6 { // avoid too large penalty.
			return v, nil
		}
		return v * 5, nil
	case vardef.TiDBOptOrderingIdxSelRatio, vardef.TiDBOptRiskEqSkewRatio, vardef.TiDBOptRiskRangeSkewRatio, vardef.TiDBOptRiskGroupNDVSkewRatio, vardef.TiDBOptSelectivityFactor: // range [0, 1], "<=0" means disable
		v := varVal.(float64)
		if v <= 0 {
			return 0.1, nil
		} else if v+0.1 > 1 {
			return v, nil
		}
		// increase 0.1 each step
		return v + 0.1, nil
	case vardef.TiDBOptPreferRangeScan, vardef.TiDBOptEnableNoDecorrelateInSelect, vardef.TiDBOptAlwaysKeepJoinKey, vardef.TiDBOptEnableSemiJoinRewrite: // flip the switch
		return !varVal.(bool), nil
	}
	return nil, fmt.Errorf("unsupported variable %s in plan generation", varName)
}

// adjustFix returns the new value of the fix-control for plan generation.
func adjustFix(fixID uint64, fixVal string) (newFixVal string, err error) {
	switch fixID {
	case fixcontrol.Fix44855, fixcontrol.Fix52869: // flip the switch
		fixVal = strings.ToUpper(strings.TrimSpace(fixVal))
		if fixVal == vardef.Off {
			return vardef.On, nil
		}
		return vardef.Off, nil
	case fixcontrol.Fix45132:
		num, err := strconv.ParseInt(fixVal, 10, 64)
		if err != nil {
			return "", err
		}
		if num <= 10 {
			return fixVal, nil
		}
		// each time become 50% more aggressive.
		return fmt.Sprintf("%v", num/2), nil
	default:
		return "", fmt.Errorf("unsupported fix-control %d in plan generation", fixID)
	}
}

func getStartState(vars []string, fixes []uint64, indexHintCount int) (*state, error) {
	// use the default values of these vars and fix-controls as the initial state.
	s := &state{
		varNames:   vars,
		fixIDs:     fixes,
		indexHints: make([]*indexHint, indexHintCount),
	}
	for _, varName := range vars {
		switch varName {
		case vardef.TiDBOptIndexScanCostFactor:
			s.varValues = append(s.varValues, vardef.DefOptIndexScanCostFactor)
		case vardef.TiDBOptIndexReaderCostFactor:
			s.varValues = append(s.varValues, vardef.DefOptIndexReaderCostFactor)
		case vardef.TiDBOptTableReaderCostFactor:
			s.varValues = append(s.varValues, vardef.DefOptTableReaderCostFactor)
		case vardef.TiDBOptTableFullScanCostFactor:
			s.varValues = append(s.varValues, vardef.DefOptTableFullScanCostFactor)
		case vardef.TiDBOptTableRangeScanCostFactor:
			s.varValues = append(s.varValues, vardef.DefOptTableRangeScanCostFactor)
		case vardef.TiDBOptTableRowIDScanCostFactor:
			s.varValues = append(s.varValues, vardef.DefOptTableRowIDScanCostFactor)
		case vardef.TiDBOptTableTiFlashScanCostFactor:
			s.varValues = append(s.varValues, vardef.DefOptTableTiFlashScanCostFactor)
		case vardef.TiDBOptIndexLookupCostFactor:
			s.varValues = append(s.varValues, vardef.DefOptIndexLookupCostFactor)
		case vardef.TiDBOptIndexMergeCostFactor:
			s.varValues = append(s.varValues, vardef.DefOptIndexMergeCostFactor)
		case vardef.TiDBOptSortCostFactor:
			s.varValues = append(s.varValues, vardef.DefOptSortCostFactor)
		case vardef.TiDBOptTopNCostFactor:
			s.varValues = append(s.varValues, vardef.DefOptTopNCostFactor)
		case vardef.TiDBOptLimitCostFactor:
			s.varValues = append(s.varValues, vardef.DefOptLimitCostFactor)
		case vardef.TiDBOptStreamAggCostFactor:
			s.varValues = append(s.varValues, vardef.DefOptStreamAggCostFactor)
		case vardef.TiDBOptHashAggCostFactor:
			s.varValues = append(s.varValues, vardef.DefOptHashAggCostFactor)
		case vardef.TiDBOptMergeJoinCostFactor:
			s.varValues = append(s.varValues, vardef.DefOptMergeJoinCostFactor)
		case vardef.TiDBOptHashJoinCostFactor:
			s.varValues = append(s.varValues, vardef.DefOptHashJoinCostFactor)
		case vardef.TiDBOptIndexJoinCostFactor:
			s.varValues = append(s.varValues, vardef.DefOptIndexJoinCostFactor)
		case vardef.TiDBOptOrderingIdxSelRatio:
			s.varValues = append(s.varValues, vardef.DefTiDBOptOrderingIdxSelRatio)
		case vardef.TiDBOptRiskEqSkewRatio:
			s.varValues = append(s.varValues, vardef.DefOptRiskEqSkewRatio)
		case vardef.TiDBOptRiskRangeSkewRatio:
			s.varValues = append(s.varValues, vardef.DefOptRiskRangeSkewRatio)
		case vardef.TiDBOptRiskGroupNDVSkewRatio:
			s.varValues = append(s.varValues, vardef.DefOptRiskGroupNDVSkewRatio)
		case vardef.TiDBOptPreferRangeScan:
			s.varValues = append(s.varValues, vardef.DefOptPreferRangeScan)
		case vardef.TiDBOptEnableNoDecorrelateInSelect:
			s.varValues = append(s.varValues, vardef.DefOptEnableNoDecorrelateInSelect)
		case vardef.TiDBOptEnableSemiJoinRewrite:
			s.varValues = append(s.varValues, vardef.DefOptEnableSemiJoinRewrite)
		case vardef.TiDBOptAlwaysKeepJoinKey:
			s.varValues = append(s.varValues, vardef.DefOptAlwaysKeepJoinKey)
		case vardef.TiDBOptSelectivityFactor:
			s.varValues = append(s.varValues, vardef.DefOptSelectivityFactor)
		case vardef.TiDBOptCartesianJoinOrderThreshold:
			s.varValues = append(s.varValues, vardef.DefOptCartesianJoinOrderThreshold)
		default:
			return nil, fmt.Errorf("unsupported variable %s in plan generation", varName)
		}
	}

	for _, fixID := range fixes {
		switch fixID {
		case fixcontrol.Fix44855:
			s.fixValues = append(s.fixValues, "OFF")
		case fixcontrol.Fix45132:
			s.fixValues = append(s.fixValues, "1000")
		case fixcontrol.Fix52869:
			s.fixValues = append(s.fixValues, "OFF")
		default:
			return nil, fmt.Errorf("unsupported fix-control %d in plan generation", fixID)
		}
	}
	return s, nil
}

type tableNameExtractor struct {
	defaultSchema string
	tableNames    map[string]*tableName
}

// Enter implements ast.Visitor interface.
func (e *tableNameExtractor) Enter(in ast.Node) (node ast.Node, skipChildren bool) {
	if name, ok := in.(*ast.TableName); ok {
		t := &tableName{
			schema: name.Schema.L,
			name:   name.Name.L,
		}
		if t.schema == "" {
			t.schema = e.defaultSchema
		}
		if _, ok := e.tableNames[t.String()]; !ok {
			e.tableNames[t.String()] = t
		}
	}
	return in, false
}

// Leave implements ast.Visitor interface.
func (*tableNameExtractor) Leave(in ast.Node) (node ast.Node, ok bool) {
	return in, true
}

// extractSelectTableNames returns the table names in the SELECT statement.
func extractSelectTableNames(defaultSchema string, node ast.StmtNode) []*tableName {
	selStmt, isSel := node.(*ast.SelectStmt)
	if !isSel {
		return nil // only support SELECT statement for now
	}
	extractor := &tableNameExtractor{
		defaultSchema: defaultSchema,
		tableNames:    make(map[string]*tableName),
	}
	selStmt.Accept(extractor)

	names := make([]*tableName, 0, len(extractor.tableNames))
	for _, name := range extractor.tableNames {
		names = append(names, name)
	}
	sort.Slice(names, func(i, j int) bool {
		return names[i].String() < names[j].String()
	})
	return names
}

type predicateColumnExtractor struct {
	table         *tableName
	columns       map[string]struct{}
	allowAnyTable bool
}

func (e *predicateColumnExtractor) Enter(in ast.Node) (node ast.Node, skipChildren bool) {
	switch n := in.(type) {
	case *ast.SubqueryExpr:
		// Only consider predicates in the current SELECT, skip inner queries to avoid mixing scopes.
		return in, true
	case *ast.ColumnNameExpr:
		if n.Name == nil {
			return in, false
		}
		if !e.allowAnyTable {
			if n.Name.Table.L == "" {
				return in, false
			}
			if !matchesColumnTable(e.table, n.Name) {
				return in, false
			}
		} else if n.Name.Schema.L != "" && n.Name.Schema.L != e.table.schema {
			return in, false
		}
		e.columns[n.Name.Name.L] = struct{}{}
	}
	return in, false
}

func (*predicateColumnExtractor) Leave(in ast.Node) (node ast.Node, ok bool) {
	return in, true
}

func matchesColumnTable(target *tableName, name *ast.ColumnName) bool {
	if target == nil || name == nil {
		return false
	}
	if name.Schema.L != "" && name.Schema.L != target.schema {
		return false
	}
	if name.Table.L == "" {
		return false
	}
	if name.Table.L == target.name {
		return true
	}
	return target.alias != "" && name.Table.L == target.alias
}

func extractSelectTableNamesWithAlias(defaultSchema string, selStmt *ast.SelectStmt) []*tableName {
	if selStmt.From == nil || selStmt.From.TableRefs == nil {
		return nil
	}
	tables := make(map[string]*tableName)
	var collectTable func(node ast.ResultSetNode)
	collectTable = func(node ast.ResultSetNode) {
		switch n := node.(type) {
		case *ast.Join:
			collectTable(n.Left)
			collectTable(n.Right)
		case *ast.TableSource:
			alias := n.AsName.L
			switch src := n.Source.(type) {
			case *ast.TableName:
				t := &tableName{
					schema: src.Schema.L,
					name:   src.Name.L,
					alias:  alias,
				}
				if t.schema == "" {
					t.schema = defaultSchema
				}
				key := fmt.Sprintf("%s.%s:%s", t.schema, t.name, t.alias)
				tables[key] = t
			case *ast.Join:
				collectTable(src)
			}
		}
	}
	collectTable(selStmt.From.TableRefs)
	if len(tables) == 0 {
		return nil
	}
	names := make([]*tableName, 0, len(tables))
	for _, name := range tables {
		names = append(names, name)
	}
	sort.Slice(names, func(i, j int) bool {
		return names[i].String() < names[j].String()
	})
	return names
}

func collectJoinPredicates(node ast.ResultSetNode, extractor *predicateColumnExtractor) {
	switch n := node.(type) {
	case *ast.Join:
		if n.On != nil && n.On.Expr != nil {
			n.On.Expr.Accept(extractor)
		}
		collectJoinPredicates(n.Left, extractor)
		collectJoinPredicates(n.Right, extractor)
	case *ast.TableSource:
		if join, ok := n.Source.(*ast.Join); ok {
			collectJoinPredicates(join, extractor)
		}
	}
}

func extractSelectIndexHints(sctx sessionctx.Context, defaultSchema string, node ast.StmtNode) [][]*indexHint {
	selStmt, isSel := node.(*ast.SelectStmt)
	if !isSel {
		return nil
	}
	tableNames := extractSelectTableNamesWithAlias(defaultSchema, selStmt)
	if len(tableNames) == 0 {
		return nil
	}
	allowAnyTable := len(tableNames) == 1
	hintOptions := make([][]*indexHint, 0, len(tableNames))
	for _, target := range tableNames {
		options := make([]*indexHint, 0, 4)
		options = append(options, nil) // empty option to avoid forcing index paths
		extractor := &predicateColumnExtractor{
			table:         target,
			columns:       make(map[string]struct{}),
			allowAnyTable: allowAnyTable,
		}
		if selStmt.Where != nil {
			selStmt.Where.Accept(extractor)
		}
		if selStmt.From != nil && selStmt.From.TableRefs != nil {
			collectJoinPredicates(selStmt.From.TableRefs, extractor)
		}
		if len(extractor.columns) == 0 {
			hintOptions = append(hintOptions, options)
			continue
		}
		tblInfo, err := sctx.GetLatestInfoSchema().TableInfoByName(ast.NewCIStr(target.schema), ast.NewCIStr(target.name))
		if err != nil {
			hintOptions = append(hintOptions, options)
			continue
		}
		useInvisible := sctx.GetSessionVars().OptimizerUseInvisibleIndexes
		seen := make(map[string]struct{})
		for _, index := range tblInfo.Indices {
			if index.State != model.StatePublic {
				continue
			}
			if !useInvisible && index.Invisible {
				continue
			}
			if index.IsColumnarIndex() || index.InvertedInfo != nil {
				continue
			}
			if tblInfo.IsCommonHandle && index.Primary {
				continue
			}
			if len(index.Columns) == 0 {
				continue
			}
			if _, ok := extractor.columns[index.Columns[0].Name.L]; !ok {
				continue
			}
			hint := &indexHint{
				table: target,
				index: index.Name.O,
			}
			if _, ok := seen[hint.index]; ok {
				continue
			}
			seen[hint.index] = struct{}{}
			options = append(options, hint)
		}
		hintOptions = append(hintOptions, options)
	}
	return hintOptions
}
