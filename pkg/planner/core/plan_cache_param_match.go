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

package core

import (
	"context"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	driver "github.com/pingcap/tidb/pkg/types/parser_driver"
	tidbutil "github.com/pingcap/tidb/pkg/util"
	plannererrors "github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
)

type paramMatchMode uint8

const (
	matchStrict paramMatchMode = iota
	matchIgnoreSourceType
)

// planCacheDMLExecutionConfig is immutable for one statement execution and is
// reused by parameter matching, cache-key construction and plan conversion.
type planCacheDMLExecutionConfig struct {
	applicable bool
	enabled    bool
}

// paramMatchInput is the complete parameter-matching contract passed through
// cache Get/Put. nil modes means the legacy strict contract; a non-nil all-
// strict slice is a distinct feature-ON contract.
type paramMatchInput struct {
	actualTypes []*types.FieldType
	modes       []paramMatchMode
}

func paramMatchInternalErrorf(format string, args ...any) error {
	return errors.Errorf("internal error: "+format, args...)
}

func currentPlanCacheDMLExecutionConfig(sctx sessionctx.Context, stmt *PlanCacheStmt, isNonPrepared bool) planCacheDMLExecutionConfig {
	if isNonPrepared || stmt == nil || !stmt.StmtCacheable || stmt.PreparedAst == nil {
		return planCacheDMLExecutionConfig{}
	}
	switch stmt.PreparedAst.Stmt.(type) {
	case *ast.UpdateStmt, *ast.InsertStmt:
	default:
		return planCacheDMLExecutionConfig{}
	}
	enabled := sctx.GetSessionVars().StmtCtx.PlanCacheDMLParamTypeMatchEnabled
	return planCacheDMLExecutionConfig{applicable: true, enabled: enabled}
}

func cloneParamMatchModes(input []paramMatchMode) []paramMatchMode {
	if input == nil {
		return nil
	}
	result := make([]paramMatchMode, len(input))
	copy(result, input)
	return result
}

func makeStrictParamMatchModes(count int) []paramMatchMode {
	return make([]paramMatchMode, count)
}

type directWriteBinding struct {
	markerOrder int
}

type paramMarkerCollector struct {
	markers []*driver.ParamMarkerExpr
}

func (c *paramMarkerCollector) Enter(node ast.Node) (ast.Node, bool) {
	if marker, ok := node.(*driver.ParamMarkerExpr); ok {
		c.markers = append(c.markers, marker)
		return node, true
	}
	return node, false
}

func (*paramMarkerCollector) Leave(node ast.Node) (ast.Node, bool) { return node, true }

func markersInWriteExpr(expr ast.ExprNode) []*driver.ParamMarkerExpr {
	if expr == nil {
		return nil
	}
	collector := &paramMarkerCollector{}
	expr.Accept(collector)
	return collector.markers
}

// hasUnsupportedParamMatchHints treats the explicitly rejected matching
// SET_VAR hint as absent. Any other hint still fails classification closed.
func isRejectedParamMatchSetVarHint(optimizerHint *ast.TableOptimizerHint) bool {
	if optimizerHint == nil || optimizerHint.HintName.L != "set_var" {
		return false
	}
	setVar, ok := optimizerHint.HintData.(ast.HintSetVar)
	return ok && strings.EqualFold(setVar.VarName, variable.TiDBEnablePlanCacheDMLParamTypeMatch)
}

func hasUnsupportedParamMatchHints(hints []*ast.TableOptimizerHint) bool {
	for _, optimizerHint := range hints {
		if !isRejectedParamMatchSetVarHint(optimizerHint) {
			return true
		}
	}
	return false
}

// appendRejectedParamMatchHintWarning preserves SET_VAR warning semantics on a
// cache hit, where normal optimization does not parse statement hints again.
func appendRejectedParamMatchHintWarning(sctx sessionctx.Context, stmt *PlanCacheStmt) {
	if stmt == nil || stmt.PreparedAst == nil {
		return
	}
	var hints []*ast.TableOptimizerHint
	switch node := stmt.PreparedAst.Stmt.(type) {
	case *ast.UpdateStmt:
		hints = node.TableHints
	case *ast.InsertStmt:
		hints = node.TableHints
	}
	for _, optimizerHint := range hints {
		if isRejectedParamMatchSetVarHint(optimizerHint) {
			sctx.GetSessionVars().StmtCtx.AppendWarning(
				plannererrors.ErrNotHintUpdatable.FastGenByArgs(variable.TiDBEnablePlanCacheDMLParamTypeMatch))
			return
		}
	}
}

func directParamMarker(expr ast.ExprNode) (*driver.ParamMarkerExpr, bool) {
	marker, ok := expr.(*driver.ParamMarkerExpr)
	return marker, ok
}

func singleBaseTable(refs *ast.TableRefsClause) (*ast.TableName, pmodel.CIStr, bool) {
	if refs == nil || refs.TableRefs == nil || refs.TableRefs.Right != nil {
		return nil, pmodel.CIStr{}, false
	}
	source, ok := refs.TableRefs.Left.(*ast.TableSource)
	if !ok || source == nil {
		return nil, pmodel.CIStr{}, false
	}
	tableName, ok := source.Source.(*ast.TableName)
	return tableName, source.AsName, ok
}

func resolveDirectWriteTable(stmt *PlanCacheStmt, is infoschema.InfoSchema, refs *ast.TableRefsClause) (table.Table, *ast.TableName, pmodel.CIStr, bool) {
	tableName, alias, ok := singleBaseTable(refs)
	if !ok || stmt.ResolveCtx == nil || is == nil || len(tableName.IndexHints) > 0 || len(tableName.PartitionNames) > 0 ||
		tableName.AsOf != nil || tableName.TableSample != nil || tableName.TableSplit != nil {
		return nil, nil, pmodel.CIStr{}, false
	}
	tableNameW := stmt.ResolveCtx.GetTableName(tableName)
	if tableNameW == nil || tableNameW.TableInfo == nil || tableNameW.DBInfo == nil ||
		tableNameW.TableInfo.IsView() || tableNameW.TableInfo.IsSequence() ||
		tableNameW.TableInfo.GetPartitionInfo() != nil ||
		tableNameW.TableInfo.TempTableType != model.TempTableNone ||
		tidbutil.IsMemDB(tableNameW.DBInfo.Name.L) {
		return nil, nil, pmodel.CIStr{}, false
	}
	tbl, found := is.TableByID(context.Background(), tableNameW.TableInfo.ID)
	if !found {
		return nil, nil, pmodel.CIStr{}, false
	}
	return tbl, tableName, alias, true
}

func columnQualifierMatches(column *ast.ColumnName, tableName *ast.TableName, alias pmodel.CIStr) bool {
	if column == nil || tableName == nil {
		return false
	}
	if column.Schema.L != "" && column.Schema.L != tableName.Schema.L {
		return false
	}
	if column.Table.L == "" {
		return true
	}
	if alias.L != "" {
		return column.Table.L == alias.L
	}
	return column.Table.L == tableName.Name.L
}

func collectUpdateDirectWriteBindings(stmt *PlanCacheStmt, is infoschema.InfoSchema, update *ast.UpdateStmt) ([]directWriteBinding, bool) {
	if update == nil || update.MultipleTable || update.IgnoreErr || update.With != nil || update.Order != nil || update.Limit != nil ||
		hasUnsupportedParamMatchHints(update.TableHints) || stmt.hasSubquery {
		return nil, false
	}
	tbl, tableName, alias, ok := resolveDirectWriteTable(stmt, is, update.TableRefs)
	if !ok {
		return nil, false
	}
	bindings := make([]directWriteBinding, 0, len(update.List))
	seenTargets := make(map[int64]struct{}, len(update.List))
	seenOrders := make(map[int]struct{}, len(update.List))
	for _, assignment := range update.List {
		// Every assignment participates in target uniqueness, even when its
		// value is a literal or DEFAULT and therefore creates no binding. A
		// duplicate target makes the write order ambiguous and must fail the
		// whole statement closed.
		if !columnQualifierMatches(assignment.Column, tableName, alias) {
			return nil, false
		}
		column := table.FindColLowerCase(tbl.Cols(), assignment.Column.Name.L)
		if column == nil {
			return nil, false
		}
		if _, duplicate := seenTargets[column.ID]; duplicate {
			return nil, false
		}
		seenTargets[column.ID] = struct{}{}

		markers := markersInWriteExpr(assignment.Expr)
		marker, direct := directParamMarker(assignment.Expr)
		if len(markers) > 0 && !direct {
			return nil, false
		}
		if !direct {
			continue
		}
		if marker.Order < 0 || marker.Order >= len(stmt.Params) {
			return nil, false
		}
		if _, duplicate := seenOrders[marker.Order]; duplicate {
			return nil, false
		}
		seenOrders[marker.Order] = struct{}{}
		bindings = append(bindings, directWriteBinding{markerOrder: marker.Order})
	}
	return bindings, true
}

func collectInsertDirectWriteBindings(stmt *PlanCacheStmt, is infoschema.InfoSchema, insert *ast.InsertStmt) ([]directWriteBinding, bool) {
	if insert == nil || insert.IsReplace || insert.IgnoreErr || insert.Select != nil || hasUnsupportedParamMatchHints(insert.TableHints) ||
		len(insert.PartitionNames) > 0 || stmt.hasSubquery {
		return nil, false
	}
	tbl, tableName, alias, ok := resolveDirectWriteTable(stmt, is, insert.Table)
	if !ok || alias.L != "" || len(insert.Lists) == 0 {
		return nil, false
	}
	insertPlan := &Insert{Table: tbl}
	affectedCols, err := (&PlanBuilder{}).getAffectCols(insert, insertPlan)
	if err != nil {
		// Existing planner returns the user-facing error later. Matching only
		// fails closed and must not replace that error.
		return nil, false
	}
	bindings := make([]directWriteBinding, 0, len(stmt.Params))
	seenOrders := make(map[int]struct{}, len(stmt.Params))
	seenValueTargets := make(map[int64]struct{}, len(affectedCols))
	for _, column := range affectedCols {
		if column == nil {
			return nil, false
		}
		if _, duplicate := seenValueTargets[column.ID]; duplicate {
			return nil, false
		}
		seenValueTargets[column.ID] = struct{}{}
	}
	appendBinding := func(expr ast.ExprNode, column *table.Column) bool {
		markers := markersInWriteExpr(expr)
		marker, direct := directParamMarker(expr)
		if len(markers) > 0 && !direct {
			return false
		}
		if !direct {
			return true
		}
		if column == nil || marker.Order < 0 || marker.Order >= len(stmt.Params) {
			return false
		}
		if _, duplicate := seenOrders[marker.Order]; duplicate {
			return false
		}
		seenOrders[marker.Order] = struct{}{}
		bindings = append(bindings, directWriteBinding{markerOrder: marker.Order})
		return true
	}
	for _, row := range insert.Lists {
		if len(row) != len(affectedCols) {
			return nil, false
		}
		for offset, expr := range row {
			if !appendBinding(expr, affectedCols[offset]) {
				return nil, false
			}
		}
	}
	seenOnDuplicateTargets := make(map[int64]struct{}, len(insert.OnDuplicate))
	for _, assignment := range insert.OnDuplicate {
		if !columnQualifierMatches(assignment.Column, tableName, alias) {
			return nil, false
		}
		column := table.FindColLowerCase(tbl.Cols(), assignment.Column.Name.L)
		if column == nil {
			return nil, false
		}
		if _, duplicate := seenOnDuplicateTargets[column.ID]; duplicate {
			return nil, false
		}
		seenOnDuplicateTargets[column.ID] = struct{}{}
		if !appendBinding(assignment.Expr, column) {
			return nil, false
		}
	}
	return bindings, true
}

func collectDirectWriteBindings(stmt *PlanCacheStmt, is infoschema.InfoSchema) ([]directWriteBinding, bool) {
	if stmt == nil || stmt.PreparedAst == nil || len(stmt.Params) == 0 {
		return nil, false
	}
	switch node := stmt.PreparedAst.Stmt.(type) {
	case *ast.UpdateStmt:
		return collectUpdateDirectWriteBindings(stmt, is, node)
	case *ast.InsertStmt:
		return collectInsertDirectWriteBindings(stmt, is, node)
	default:
		return nil, false
	}
}

func rebuildParamMatchModes(stmt *PlanCacheStmt, is infoschema.InfoSchema) []paramMatchMode {
	modes := makeStrictParamMatchModes(len(stmt.Params))
	bindings, ok := collectDirectWriteBindings(stmt, is)
	if !ok {
		return modes
	}
	for _, binding := range bindings {
		if binding.markerOrder < 0 || binding.markerOrder >= len(modes) {
			return makeStrictParamMatchModes(len(stmt.Params))
		}
		modes[binding.markerOrder] = matchIgnoreSourceType
	}
	return modes
}

// refreshParamMatchModes rebuilds the statement-scoped immutable matching
// contract. It must only be called after PREPARE preprocessing has established
// ResolveCtx, or after schema re-preprocessing has replaced that binding.
// The session serialization invariant guarantees this refresh completes before
// parameter matching begins. Ordinary EXECUTE paths borrow the published slice
// without rebuilding it; PlanCacheStmt does not support concurrent execution.
func refreshParamMatchModes(is infoschema.InfoSchema, stmt *PlanCacheStmt) {
	if stmt == nil || !stmt.StmtCacheable || stmt.PreparedAst == nil {
		return
	}
	switch stmt.PreparedAst.Stmt.(type) {
	case *ast.UpdateStmt, *ast.InsertStmt:
	default:
		return
	}
	modes := rebuildParamMatchModes(stmt, is)
	stmt.paramMatchModes = modes
}

// getParamMatchModes is a cloning inspection API used by tests. EXECUTE borrows
// the immutable statement-scoped slice directly.
func getParamMatchModes(stmt *PlanCacheStmt) []paramMatchMode {
	return cloneParamMatchModes(stmt.paramMatchModes)
}

func buildParamMatchInput(stmt *PlanCacheStmt, actualTypes []*types.FieldType,
	config planCacheDMLExecutionConfig) (paramMatchInput, error) {
	input := paramMatchInput{actualTypes: actualTypes}
	if !config.applicable || !config.enabled {
		return input, nil
	}
	if len(stmt.paramMatchModes) != len(actualTypes) {
		return paramMatchInput{}, paramMatchInternalErrorf(
			"parameter matching mode count %d differs from parameter count %d", len(stmt.paramMatchModes), len(actualTypes))
	}
	input.modes = stmt.paramMatchModes
	return input, nil
}
