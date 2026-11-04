// Copyright 2026 PingCAP, Inc.
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

package mviewutil

import (
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/opcode"
	"github.com/pingcap/tidb/pkg/util/dbterror"
)

// RewriteAvgToColumnTypes finalizes decimal AVG from SUM/COUNT using the physical
// MV column type. It mutates a fresh maintenance SELECT, never the definition.
// Computing the quotient directly avoids rounding AVG to its inferred query scale
// before rounding it again to the physical scale. FAST uses the same state and type.
func RewriteAvgToColumnTypes(sel *ast.SelectStmt, mv *model.TableInfo) (bool, error) {
	if sel == nil || sel.Fields == nil || mv == nil {
		return false, errors.New("materialized view has invalid select metadata")
	}
	changed := false
	for i, field := range sel.Fields.Fields {
		if field == nil {
			return false, errors.New("materialized view select field is missing")
		}
		agg, ok := field.Expr.(*ast.AggregateFuncExpr)
		if !ok || !strings.EqualFold(agg.F, ast.AggFuncAvg) {
			continue
		}
		if i >= len(mv.Columns) || mv.Columns[i] == nil {
			return false, errors.New("materialized view AVG output column is missing")
		}
		ft := &mv.Columns[i].FieldType
		if ft.GetType() != mysql.TypeNewDecimal {
			continue
		}
		if ft.GetDecimal() < 0 || ft.GetDecimal() > mysql.MaxDecimalScale ||
			ft.GetFlen() <= 0 || ft.GetFlen() < ft.GetDecimal() || ft.GetFlen() > mysql.MaxDecimalWidth {
			return false, errors.New("materialized view AVG has invalid physical DECIMAL type")
		}
		if agg.Distinct || len(agg.Args) != 1 {
			return false, errors.New("materialized view AVG requires one non-DISTINCT argument")
		}
		sum := &ast.AggregateFuncExpr{F: ast.AggFuncSum, Args: agg.Args}
		count := &ast.AggregateFuncExpr{F: ast.AggFuncCount, Args: agg.Args}
		// NULLIF preserves AVG(NULL) semantics without division-by-zero warnings.
		divisor := &ast.FuncCallExpr{
			Tp:     ast.FuncCallExprTypeGeneric,
			FnName: ast.NewCIStr("NULLIF"),
			Args:   []ast.ExprNode{count, ast.NewValueExpr(int64(0), "", "")},
		}
		quotient := &ast.BinaryOperationExpr{Op: opcode.Div, L: sum, R: divisor}
		field.Expr = &ast.FuncCastExpr{Expr: quotient, Tp: ft.Clone(), FunctionType: ast.CastFunction}
		changed = true
	}
	return changed, nil
}

// BuildMaintenanceSelectSQL restores the defining query with decimal AVG outputs
// converted to the physical MV column types. Queries without decimal AVG retain
// their original SQL text.
func BuildMaintenanceSelectSQL(mv *model.TableInfo) (string, error) {
	if mv == nil || mv.MaterializedView == nil || mv.MaterializedView.SQLContent == "" {
		return "", errors.New("materialized view has invalid select SQL")
	}
	p := parser.New()
	p.SetSQLMode(mv.MaterializedView.DefinitionSQLMode)
	node, err := p.ParseOneStmt(mv.MaterializedView.SQLContent, "", "")
	if err != nil {
		// Parser errors can echo literals from the stored user query.
		return "", errors.New("cannot parse materialized view maintenance query")
	}
	sel, ok := node.(*ast.SelectStmt)
	if !ok || sel.Fields == nil {
		return "", errors.New("materialized view definition is not a SELECT")
	}
	changed, err := RewriteAvgToColumnTypes(sel, mv)
	if err != nil {
		return "", err
	}
	if !changed {
		return mv.MaterializedView.SQLContent, nil
	}
	var sb strings.Builder
	err = sel.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags|format.RestoreStringWithoutCharset, &sb))
	return sb.String(), err
}

// CheckMaterializedViewSelect checks the SELECT clauses that are not supported
// by materialized views. It is called before the planner builds the SELECT so
// unsupported clauses do not get reported as unrelated planner errors.
func CheckMaterializedViewSelect(selectNode ast.ResultSetNode) error {
	sel, ok := selectNode.(*ast.SelectStmt)
	if !ok {
		return nil
	}
	if sel.With != nil {
		return dbterror.ErrGeneralUnsupportedDDL.GenWithStack("CREATE MATERIALIZED VIEW does not support common table expressions")
	}
	if sel.LockInfo != nil && sel.LockInfo.LockType != ast.SelectLockNone {
		return dbterror.ErrGeneralUnsupportedDDL.GenWithStack("CREATE MATERIALIZED VIEW does not support locking clauses")
	}
	if sel.SelectIntoOpt != nil {
		return dbterror.ErrGeneralUnsupportedDDL.GenWithStack("CREATE MATERIALIZED VIEW does not support SELECT INTO")
	}
	if sel.From == nil || sel.From.TableRefs == nil || sel.From.TableRefs.Right != nil {
		return nil
	}
	ts, ok := sel.From.TableRefs.Left.(*ast.TableSource)
	if !ok {
		return nil
	}
	tbl, ok := ts.Source.(*ast.TableName)
	if !ok {
		return nil
	}
	if tbl.AsOf != nil {
		return dbterror.ErrGeneralUnsupportedDDL.GenWithStack("CREATE MATERIALIZED VIEW does not support AS OF")
	}
	if tbl.TableSample != nil {
		return dbterror.ErrGeneralUnsupportedDDL.GenWithStack("CREATE MATERIALIZED VIEW does not support TABLESAMPLE")
	}
	return nil
}

// FindVisibleIndexWithPrefixCoveringColumns returns the first public visible key layout
// usable by MIN/MAX materialized-view refresh.
func FindVisibleIndexWithPrefixCoveringColumns(baseTableInfo *model.TableInfo, groupByCols []string) (string, bool) {
	indexNames := findIndexesWithPrefixCoveringColumns(baseTableInfo, groupByCols, "", true)
	if len(indexNames) == 0 {
		return "", false
	}
	return indexNames[0], true
}

// FindVisibleIndexesWithPrefixCoveringColumns returns all public visible key layouts
// usable by MIN/MAX materialized-view refresh.
func FindVisibleIndexesWithPrefixCoveringColumns(baseTableInfo *model.TableInfo, groupByCols []string) []string {
	return findIndexesWithPrefixCoveringColumns(baseTableInfo, groupByCols, "", true)
}

// HasIndexWithPrefixCoveringColumns reports whether the table has a key layout
// whose leading columns cover all group-by columns without prefix length.
// excludedIndexName is used by DDL checks to evaluate a post-DDL table shape
// where that index should not be considered.
func HasIndexWithPrefixCoveringColumns(
	baseTableInfo *model.TableInfo,
	groupByCols []string,
	excludedIndexName string,
	requireVisiblePublic bool,
) bool {
	return len(findIndexesWithPrefixCoveringColumns(baseTableInfo, groupByCols, excludedIndexName, requireVisiblePublic)) > 0
}

func findIndexesWithPrefixCoveringColumns(
	baseTableInfo *model.TableInfo,
	groupByCols []string,
	excludedIndexName string,
	requireVisiblePublic bool,
) []string {
	if baseTableInfo == nil {
		return nil
	}
	prefixLen := len(groupByCols)
	if prefixLen == 0 {
		return nil
	}
	groupBySet := make(map[string]struct{}, prefixLen)
	for _, col := range groupByCols {
		groupBySet[strings.ToLower(col)] = struct{}{}
	}

	indexNames := make([]string, 0, len(baseTableInfo.Indices))
	excludedIndexName = strings.ToLower(excludedIndexName)
	if baseTableInfo.PKIsHandle && prefixLen == 1 && excludedIndexName != strings.ToLower(mysql.PrimaryKeyName) {
		if pkCol := baseTableInfo.GetPkColInfo(); pkCol != nil {
			if _, ok := groupBySet[pkCol.Name.L]; ok {
				indexNames = append(indexNames, mysql.PrimaryKeyName)
			}
		}
	}

	for _, idx := range baseTableInfo.Indices {
		if idx == nil || len(idx.Columns) < prefixLen {
			continue
		}
		if requireVisiblePublic && (idx.State != model.StatePublic || idx.Invisible) {
			continue
		}
		if excludedIndexName != "" && idx.Name.L == excludedIndexName {
			continue
		}
		if indexPrefixCoversColumns(idx, prefixLen, groupBySet) {
			indexNames = append(indexNames, idx.Name.O)
		}
	}
	return indexNames
}

func indexPrefixCoversColumns(idx *model.IndexInfo, prefixLen int, groupBySet map[string]struct{}) bool {
	matched := make(map[string]struct{}, prefixLen)
	for i := range prefixLen {
		idxCol := idx.Columns[i]
		if idxCol.Length > 0 {
			return false
		}
		name := idxCol.Name.L
		if _, exists := groupBySet[name]; !exists {
			return false
		}
		if _, exists := matched[name]; exists {
			return false
		}
		matched[name] = struct{}{}
	}
	return len(matched) == prefixLen
}
